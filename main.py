from fastapi import FastAPI, UploadFile, File, HTTPException,BackgroundTasks
from schema import OrderIN, RawOut
import io
import pandas as pd
import json
import sqlite3
import logging
from database import SessionLocal, Base, engine
from models import Raw
from process import process_raw_event
from sqlalchemy import select, update

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)

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
        db.add(raw)
        try:
            db.commit()
            db.refresh(raw)
        except Exception as e:
            logger.error("raw 寫入失敗 order_id=%s: %s", order.order_id, e, exc_info=True)
            raise

        background_tasks.add_task(process_raw_event, raw.id)
        logger.info("raw_id=%s 已建立，背景任務已排程", raw.id)
        return {"raw_id": raw.id, "status": "pending"}
    finally:
        db.close()


@app.post("/process_raw/{raw_id}")
async def process_raw(raw_id: int, force: bool = False):
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

    process_raw_event(raw_id)
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