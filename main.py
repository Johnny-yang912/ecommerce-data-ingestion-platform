import uuid
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import request_validation_exception_handler
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
from tasks import process_raw_event_task
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError, TimeoutError as SATimeoutError
from logging_config import configure_logging
from auth import verify_api_key
from circuit_breaker import CircuitBreaker, CircuitOpen
from config import settings
from telemetry import (
    RETRIES,
    configure_telemetry,
    instrument_fastapi,
    observe_circuit_breaker,
)

configure_logging()
# API 行程：uvicorn 的每個 worker 各自 import 本模組，因此這裡就是「fork 之後」，
# 可以直接在模組層初始化（worker 端的情況不同，見 telemetry.py 模組註解）。
configure_telemetry("order-api")
logger = structlog.get_logger()

def _client_id_key(request: Request) -> str:
    # 限流主體 = 認證身分（client_id，由 verify_api_key 落到 request.state），
    # 而非網路位置（IP）。client_id 是穩定的上游身分，免疫 NAT / proxy / 多 IP。
    # 取不到時（無認證路徑 / 設定漏失防呆）退回 IP。
    # 注意語意：以 client_id 為 key，限額是「每個上游（跨所有 key、所有 IP）合計」，
    # 非「每 IP」；上游若水平擴展成多實例，需重新校準限額（見 README）。
    client_id = getattr(request.state, "client_id", None)
    if client_id:
        return client_id
    return get_remote_address(request)


_key_func = _client_id_key  # 間接層：測試可替換此變數模擬不同 client_id

def _limiter_key(request: Request) -> str:
    return _key_func(request)

limiter = Limiter(
    key_func=_limiter_key,
    # 計數器儲存：空字串 → memory://（單行程 / pytest）；多行程部署指向 Redis。
    # 沒有共享儲存的話，`60/minute` 在 N 個 uvicorn worker 下會變成 60×N——
    # 限額的語意（每個上游合計）會隨部署形態靜默改變。
    storage_uri=settings.rate_limit_storage_uri,
    # 與 Celery broker 共用 Redis 實例但分開 DB index，再加一層 key 前綴，
    # 讓限流的 key 在任何情況下都不會與佇列的 key 混淆。
    key_prefix="ratelimit",
    # 限流檢查跑在 request 路徑上且是同步的，Redis 不可用時若不設逾時上限，
    # 會退回 OS 層的 DNS / TCP 逾時——實測單筆請求因此從 3.8s 惡化到 18.3s。
    # slowapi 在 storage 死掉後仍會以指數退避週期性 _storage.check() 重探，
    # 那次重探同樣受此上限保護。（與 celery_app 的 broker 逾時是同一個教訓。）
    # MemoryStorage 會忽略這些多餘參數，故預設路徑不受影響。
    storage_options={"socket_connect_timeout": 1, "socket_timeout": 1},
    # Redis 不可用時退回行程內計數，而不是整個放行。
    # 降級後限額變成「每 worker 60/min」（總量放寬 N 倍），仍遠好過完全不限流；
    # 與 _enqueue 吞掉 broker 故障是同一套原則：下游故障只降級、不失效。
    in_memory_fallback_enabled=True,
)

MAX_RAW_WRITE_RETRIES = 3  # 演算法常數：Raw 寫入重試上限，行為固定不隨環境變動

# 派工熔斷器的參數。與 MAX_*_RETRIES 同類——是程式行為而非環境設定，
# 故留在模組頂端、改動走 code review（見 config.py 對這條分界的說明）。
# 門檻 3：broker 掛掉時是全失敗，連續三次已足以區分「瞬斷」與「真的掛了」。
# 冷卻 30s：探測成本為每行程每 30 秒一筆慢請求（約 3.8s），可忽略；
#           同時讓 broker 復原後 30 秒內就能回到快路徑。
ENQUEUE_BREAKER_FAILURE_THRESHOLD = 3
ENQUEUE_BREAKER_RESET_SECONDS = 30

_enqueue_breaker = CircuitBreaker(
    failure_threshold=ENQUEUE_BREAKER_FAILURE_THRESHOLD,
    reset_timeout=ENQUEUE_BREAKER_RESET_SECONDS,
    name="celery_enqueue",
)

# 把熔斷器狀態暴露成 gauge。由這裡傳入 breaker 物件而非讓 telemetry 去 import
# main（會成環），也讓 circuit_breaker.py 維持零專案外依賴——見 telemetry.py。
observe_circuit_breaker(_enqueue_breaker)

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


def _enqueue(raw_id: int) -> bool:
    """
    把一筆 raw_id 派到 Celery 佇列，回傳是否成功送進 broker。

    刻意吞掉所有例外：呼叫此函式時 Raw 已經 commit 落地了，broker 掛掉不該讓
    POST /orders 回 500。回 500 會讓上游重送 → 同一個 order_id 灌出一批 Raw →
    全數走到 `duplicate` 終態變成雜訊，但資料其實早就收下了。正確的語意是
    「已受理（pending），處理會延遲」——Beat 的 tasks.scan_and_dispatch 每
    scan_interval_seconds 就會把這些沒派成功的 pending 撿回來重派。

    與 has_clean_error 非阻斷是同一套原則：**已經成立的事實，不因下游故障而回退**。

    ⚠️ 這是同步函式，且 broker 不可用時會走完連線逾時才返回。async 路徑上的呼叫者
    一律要用 `asyncio.to_thread` 包起來，否則會把整個 event loop 卡住——實測 broker
    停掉時單次派工阻塞達 19 秒（已由 celery_app 的 socket timeout 收斂，但仍非零）。

    熔斷器讓上面那個「非零」在持續故障下歸零：連續失敗三次後開路，之後直接回 False
    而不碰 Redis。少了它，實測 48 併發下有 47 筆在 120 秒內拿不到回應（見
    circuit_breaker.py 的模組註解）——而那些請求的 Raw **其實都已經寫進去了**，
    客戶端卻只看到逾時，於是重送、灌出一批同 order_id 的重複資料。
    """
    try:
        _enqueue_breaker.call(process_raw_event_task.delay, raw_id)
        return True
    except CircuitOpen:
        # 開路是預期中的降級狀態，不逐筆記 log：開路那一刻已經記過一次，
        # 事故期間每筆都記只會把真正的訊號淹掉（實測規模下每秒上千條）。
        return False
    except Exception:
        logger.error("派工失敗，改由 recovery scan 接手", raw_id=raw_id, exc_info=True)
        return False


# 恢復掃描（stale processing 重設 + pending 補派）已搬到 Celery Beat，見
# tasks.scan_and_dispatch_task。原本它是掛在 lifespan 上的 asyncio 迴圈——那是
# **行程內狀態**，API 一旦跑多個 uvicorn worker 就會每個行程各跑一份掃描。
# 移走之後 API 行程不再持有任何背景狀態，啟動時也不做任何恢復工作。
app = FastAPI()
app.add_middleware(RequestContextMiddleware)
# ⚠️ 必須在 RequestContextMiddleware 之後才掛：Starlette 的 add_middleware 是往外
# 疊的（後加的在外層），所以這個順序才能讓 OTel 的 span 涵蓋整個請求，
# 進而讓 RequestContextMiddleware 內與其後的每一行 log 都帶得到 trace_id。
# 反過來的話 span 會晚一步開始，最外層那段 log 就關聯不上。
instrument_fastapi(app)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.exception_handler(RequestValidationError)
async def _on_validation_error(request: Request, exc: RequestValidationError):
    # 攝入硬閘門（order_id 必填 / 型別不可強轉）擋下的請求不會落地 Raw，
    # 在此記一筆訊號，讓「上游開始送壞資料」也進得了可觀測性，而非只在 access log 浮現。
    logger.warning(
        "ingress_rejected",
        path=request.url.path,
        method=request.method,
        errors=[{"loc": e.get("loc"), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()],
    )
    return await request_validation_exception_handler(request, exc)


@app.get("/health")
async def health():
    # liveness：行程活著即回 200。不掛 verify_api_key（healthcheck 不帶 key）、
    # 不掛 limiter（不消耗速率配額）。容器 healthcheck 與未來 LB/K8s 探針用。
    return {"status": "ok"}


@app.post("/orders")
@limiter.limit("60/minute")
async def create_order(request: Request, order: OrderIN,
                       client_id: str = Depends(verify_api_key)):
    bind_contextvars(client_id=client_id)
    logger.info("收到訂單請求", order_id=order.order_id)
    db = SessionLocal()
    try:
        # landing 層語意：請求已通過 OrderIN 結構驗證（壞資料在邊界以 422 擋掉），
        # 此處逐字保留原始 request body，不做序列化/欄位過濾，避免上游 schema drift
        # 時靜默丟失未知欄位。order_id 另外抽出作為關鍵追溯欄位。
        # PostgreSQL 的 TEXT/VARCHAR 無法儲存 NUL byte（\x00），含 NUL 的 payload 直接寫入
        # 會在 commit 拋非 OperationalError 例外、不被下方 retry 攔截 → 500、訂單連 Raw 都進不來。
        # 此處先移除 NUL 讓資料得以落地，並記一筆 warning 作為上游異常訊號。
        raw_body = (await request.body()).decode("utf-8")
        payload_text = raw_body.replace("\x00", "")
        if len(payload_text) != len(raw_body):
            logger.warning("payload 含 NUL byte，已移除後落地", order_id=order.order_id)

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
                    RETRIES.add(1, {"stage": "raw_write"})
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    db.add(raw)
                else:
                    logger.error("raw 寫入失敗，已達最大重試次數", order_id=order.order_id, exc_info=True)
                    raise

        # db.refresh() 會開啟一個新交易取回 id，而它要到 db.close() 才結束。
        # 若讓它跨越下面的派工，broker 故障時整段阻塞期間連線都掛在
        # `idle in transaction`（實測 60 併發下 32 個 pool 槽位有 23 個是這狀態），
        # 同時壓住 Postgres 的 vacuum horizon。先取出 id 再明確收掉交易。
        raw_id = raw.id
        db.close()

        # 派工必須在 db.commit() 之後：worker 走的是另一條 DB 連線，
        # 先派工可能讓它讀不到還沒 commit 的 Raw（claim 直接落空）。
        # to_thread：broker 不可用時 _enqueue 會阻塞，不能卡在 event loop 上。
        queued = await asyncio.to_thread(_enqueue, raw_id)
        logger.info("raw 已建立", raw_id=raw_id, queued=queued)
        # 回應不受派工結果影響：資料已落地，未入列者由 recovery scan 接手（見 _enqueue）。
        return {"raw_id": raw_id, "status": "pending"}
    except SATimeoutError:
        logger.warning("connection pool 耗盡", order_id=order.order_id)
        raise HTTPException(status_code=503, detail="Server busy, please retry later")
    finally:
        db.close()


@app.post("/process_raw/{raw_id}")
@limiter.limit("20/minute")
async def process_raw(request: Request, raw_id: int, force: bool = False,
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

    # 與 /orders 的不對稱是刻意的：replay 是人工維運動作，操作者需要知道「到底派出去了沒」，
    # 所以 triggered 如實回報派工結果；/orders 面對的是自動化上游，回報派工失敗只會誘發
    # 無謂的重送（見 _enqueue）。
    triggered = await asyncio.to_thread(_enqueue, raw_id)
    return {"raw_id": raw_id, "triggered": triggered, "force": force}


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
