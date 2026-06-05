import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Depends
from starlette.middleware.base import BaseHTTPMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from schema import OrderIN, RawOut
import asyncio
import structlog
from structlog.contextvars import clear_contextvars, bind_contextvars
from database import SessionLocal
from models import Raw
from process import process_raw_event, scan_and_recover
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError, TimeoutError as SATimeoutError
from logging_config import configure_logging
from auth import verify_api_key
from config import settings

configure_logging()
logger = structlog.get_logger()

_key_func = get_remote_address  # 間接層：測試可替換此變數模擬不同 IP

def _limiter_key(request: Request) -> str:
    return _key_func(request)

limiter = Limiter(key_func=_limiter_key)

MAX_RAW_WRITE_RETRIES = 3  # 演算法常數：Raw 寫入重試上限，行為固定不隨環境變動

# Schema 由 Alembic 管理（alembic upgrade head），啟動時不再 create_all。


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        clear_contextvars()
        bind_contextvars(
            request_id=str(uuid.uuid4()),
            method=request.method,
            path=request.url.path,
        )
        return await call_next(request)


async def _periodic_scan():
    while True:
        await asyncio.sleep(settings.scan_interval_seconds)
        try:
            raw_ids = await asyncio.to_thread(scan_and_recover)
            for raw_id in raw_ids:
                asyncio.create_task(asyncio.to_thread(process_raw_event, raw_id))
            logger.info("periodic scan 完成", count=len(raw_ids))
        except Exception as e:
            logger.error("periodic scan 失敗", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    raw_ids = await asyncio.to_thread(scan_and_recover)
    for raw_id in raw_ids:
        asyncio.create_task(asyncio.to_thread(process_raw_event, raw_id))
    logger.info("startup recovery 完成", count=len(raw_ids))
    asyncio.create_task(_periodic_scan())
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestContextMiddleware)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
async def health():
    # liveness：行程活著即回 200。不掛 verify_api_key（healthcheck 不帶 key）、
    # 不掛 limiter（不消耗速率配額）。容器 healthcheck 與未來 LB/K8s 探針用。
    return {"status": "ok"}


@app.post("/orders")
@limiter.limit("60/minute")
async def create_order(request: Request, order: OrderIN, background_tasks: BackgroundTasks,
                       client_id: str = Depends(verify_api_key)):
    bind_contextvars(client_id=client_id)
    logger.info("收到訂單請求", order_id=order.order_id)
    db = SessionLocal()
    try:
        # landing 層語意：請求已通過 OrderIN 結構驗證（壞資料在邊界以 422 擋掉），
        # 此處逐字保留原始 request body，不做序列化/欄位過濾，避免上游 schema drift
        # 時靜默丟失未知欄位。order_id 另外抽出作為關鍵追溯欄位。
        payload_text = (await request.body()).decode("utf-8")

        raw = Raw(
            raw_payload=payload_text,
            order_id=order.order_id,
            source_client_id=client_id,  # 血緣起點：來自驗證層的來源端
        )
        db.add(raw)
        for attempt in range(MAX_RAW_WRITE_RETRIES):
            try:
                db.commit()
                db.refresh(raw)
                break
            except OperationalError as e:
                db.rollback()
                if attempt < MAX_RAW_WRITE_RETRIES - 1:
                    logger.warning("raw 寫入失敗", attempt=attempt + 1, order_id=order.order_id, error=str(e))
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    db.add(raw)
                else:
                    logger.error("raw 寫入失敗，已達最大重試次數", order_id=order.order_id, exc_info=True)
                    raise

        background_tasks.add_task(process_raw_event, raw.id)
        logger.info("raw 已建立，背景任務已排程", raw_id=raw.id)
        return {"raw_id": raw.id, "status": "pending"}
    except SATimeoutError:
        logger.warning("connection pool 耗盡", order_id=order.order_id)
        raise HTTPException(status_code=503, detail="Server busy, please retry later")
    finally:
        db.close()


@app.post("/process_raw/{raw_id}")
@limiter.limit("20/minute")
async def process_raw(request: Request, raw_id: int, background_tasks: BackgroundTasks, force: bool = False,
                      client_id: str = Depends(verify_api_key)):
    logger.info("觸發 replay", raw_id=raw_id, force=force)
    db = SessionLocal()
    try:
        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()
        if not raw:
            raise HTTPException(status_code=404, detail="Raw not found")
        if force:
            if raw.status not in ("error", "duplicate"):
                raise HTTPException(
                    status_code=400,
                    detail=f"force=True only allowed for error or duplicate records (current status: {raw.status})"
                )
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
@limiter.limit("120/minute")
async def get_raw(request: Request, raw_id: int, client_id: str = Depends(verify_api_key)):
    db = SessionLocal()
    try:
        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()
        if not raw:
            raise HTTPException(status_code=404, detail="Raw not found")
        return raw
    finally:
        db.close()
