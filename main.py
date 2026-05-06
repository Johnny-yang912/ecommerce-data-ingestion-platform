from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from schema import OrderIN, RawOut
import asyncio
import io
import pandas as pd
import json
import sqlite3
import logging
from database import SessionLocal, Base, engine
from models import Raw
from process import process_raw_event, scan_and_recover
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError, TimeoutError as SATimeoutError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

MAX_RAW_WRITE_RETRIES = 3
SCAN_INTERVAL_SECONDS = 300  # periodic scan 間隔（5 分鐘）

Base.metadata.create_all(bind=engine)


async def _periodic_scan():
    while True:
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)
        try:
            raw_ids = await asyncio.to_thread(scan_and_recover)
            for raw_id in raw_ids:
                asyncio.create_task(asyncio.to_thread(process_raw_event, raw_id))
            logger.info("periodic scan 完成，重新排程 %d 筆", len(raw_ids))
        except Exception as e:
            logger.error("periodic scan 失敗: %s", e, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    raw_ids = await asyncio.to_thread(scan_and_recover)
    for raw_id in raw_ids:
        asyncio.create_task(asyncio.to_thread(process_raw_event, raw_id))
    logger.info("startup recovery 完成，重新排程 %d 筆", len(raw_ids))
    asyncio.create_task(_periodic_scan())
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/orders")
async def create_order(order: OrderIN, background_tasks: BackgroundTasks):
    logger.info("收到訂單請求 order_id=%s", order.order_id)
    db = SessionLocal()
    try:
        payload_dict = order.model_dump()
        payload_text = json.dumps(payload_dict, ensure_ascii=False, default=str)

        raw = Raw(
            raw_payload=payload_text,
            order_id=order.order_id,
        )
        # Point 1: Raw 寫入 retry，僅對暫時性連線錯誤重試
        db.add(raw)
        for attempt in range(MAX_RAW_WRITE_RETRIES):
            try:
                db.commit()
                db.refresh(raw)
                break
            except OperationalError as e:
                db.rollback()
                if attempt < MAX_RAW_WRITE_RETRIES - 1:
                    logger.warning("raw 寫入失敗，第 %d 次重試 order_id=%s: %s", attempt + 1, order.order_id, e)
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    db.add(raw)  # rollback 後需重新 add
                else:
                    logger.error("raw 寫入失敗，已達最大重試次數 order_id=%s", order.order_id, exc_info=True)
                    raise

        background_tasks.add_task(process_raw_event, raw.id)
        logger.info("raw_id=%s 已建立，背景任務已排程", raw.id)
        return {"raw_id": raw.id, "status": "pending"}
    except SATimeoutError:
        logger.warning("connection pool 耗盡，order_id=%s", order.order_id)
        raise HTTPException(status_code=503, detail="Server busy, please retry later")
    finally:
        db.close()


@app.post("/process_raw/{raw_id}")
async def process_raw(raw_id: int, background_tasks: BackgroundTasks, force: bool = False):
    logger.info("觸發 replay raw_id=%s，force=%s", raw_id, force)
    if force:
        db = SessionLocal()
        try:
            db.execute(
                update(Raw)
                .where(Raw.id == raw_id)
                .values(status="pending", error_message=None)
            )
            db.commit()
        finally:
            db.close()

    background_tasks.add_task(process_raw_event, raw_id)
    return {"raw_id": raw_id, "triggered": True, "force": force}


@app.get("/raw/{raw_id}", response_model=RawOut)
async def get_raw(raw_id: int):
    db = SessionLocal()
    try:
        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()
        if not raw:
            raise HTTPException(status_code=404, detail="Raw not found")
        return raw
    finally:
        db.close()
