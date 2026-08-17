import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from database import SessionLocal
from models import Raw, ODS, QualityEvent
from schema import ODSOrder
from sqlalchemy import update, and_, select
import json
import time
from datetime import datetime, timedelta
from pytz import UTC
from clean import clean_order, detect_schema_drift, DQ_RULE_VERSION
from sqlalchemy.exc import OperationalError, IntegrityError, DataError
# 指標是 no-op-safe 的：OTel 關閉時這些是 proxy instrument，呼叫不做事也不拋錯，
# 所以下面的埋點一律不加 `if otel_enabled` 判斷（見 telemetry.py 模組註解）。
from telemetry import ORDERS_RESULT, PROCESSING_DURATION, RETRIES

logger = structlog.get_logger()

MAX_CLAIM_RETRIES = 3
MAX_PROCESS_RETRIES = 3
MAX_STATUS_RETRIES = 3
STALE_PROCESSING_MINUTES = 10

# 單輪掃描取回的上限。原本是「一次撈完所有 pending」——在攝入量大的情境下，
# 一次 broker 事故就會累積出數十萬筆，全部載進一個 Python list 再逐一派工，
# 等於把攝入層的崩潰原封不動搬到恢復路徑上。
SCAN_BATCH_SIZE = 5000

# 剛攝入的 pending 不由掃描接手：正常情況下攝入路徑會在毫秒內把它派出去，
# 掃描此時介入只會為同一筆多送一則訊息（CAS 擋得住，但純屬浪費）。
# 只有「躺了超過寬限期還是 pending」才代表快路徑真的失手了。
# ⚠️ 這裡用 received_at 是對的——問的正是「這筆資料躺了多久」；
#    而 stale 判定問的是「這次處理跑了多久」，故用 processing_started_at。
#    兩個問題不同，基準不同，別把它們混在一起（見 scan_and_recover 的說明）。
PENDING_GRACE_SECONDS = 60


def try_claim_raw(db, raw_id: int) -> bool:
    # 搶佔成功的同時蓋上 processing_started_at：stale 判定要問的是「這次處理跑了多久」，
    # 這裡是唯一能回答它的時點（也是唯一進入 processing 的路徑）。見 scan_and_recover。
    claim = (
        update(Raw)
        .where(and_(Raw.id == raw_id, Raw.status == "pending"))
        .values(status="processing", processing_started_at=datetime.now(UTC))
    )
    result = db.execute(claim)
    return result.rowcount == 1


def _commit_raw_status(db, raw_id: int, status: str, error_message=None) -> None:
    """Point 4: status 更新含 retry，防止 record 卡在 processing。"""
    for attempt in range(MAX_STATUS_RETRIES):
        try:
            db.execute(
                update(Raw).where(Raw.id == raw_id).values(
                    status=status,
                    error_message=error_message,
                    processed_at=datetime.now(UTC)
                )
            )
            db.commit()
            # 這裡是 error / duplicate 七個終態的唯一匯流點（只有 processed 走
            # 成功分支自己更新），所以結果計數埋在這一行就涵蓋全部失敗終態。
            ORDERS_RESULT.add(1, {"result": status})
            return
        except Exception as e:
            db.rollback()
            if attempt < MAX_STATUS_RETRIES - 1:
                logger.warning("status 更新失敗", attempt=attempt + 1)
                RETRIES.add(1, {"stage": "status_update"})
                time.sleep(0.5 * (2 ** attempt))
            else:
                logger.critical("status 更新失敗，record 可能永久卡在 processing", exc_info=True)


def process_raw_event(raw_id: int) -> None:
    clear_contextvars()
    bind_contextvars(raw_id=raw_id)
    logger.info("開始處理")
    db = SessionLocal()
    # 耗時量測起點。用 monotonic 而非 time.time()：後者會被 NTP 校正與睡眠回復
    # 拉扯，而這台機器有時鐘漂移的前科（見 TROUBLESHOOTING 的 WSL 時鐘一節），
    # 那會產生負的或荒謬的耗時，直接污染 histogram 而且無法回頭修。
    started = time.monotonic()
    # claimed 提到 try 之外：finally 要靠它判斷這一筆到底有沒有真的做事。
    claimed = False
    try:
        # Point 3: claim retry，區分 DB 例外 vs 正常搶佔失敗
        for attempt in range(MAX_CLAIM_RETRIES):
            try:
                claimed = try_claim_raw(db, raw_id)
                db.commit()
                break
            except OperationalError as e:
                db.rollback()
                if attempt < MAX_CLAIM_RETRIES - 1:
                    logger.warning("claim DB 例外", attempt=attempt + 1, error=str(e))
                    RETRIES.add(1, {"stage": "claim"})
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    logger.error("claim 失敗，已達最大重試次數，放棄", exc_info=True)
                    return

        if not claimed:
            logger.warning("claim 失敗，狀態不是 pending 或已被其他 worker 處理")
            return

        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()
        if not raw:
            logger.warning("claim 成功但查無此筆資料")
            return

        # Point 2: processing retry，僅對暫時性例外重試，資料錯誤直接 mark error
        ods = None
        for attempt in range(MAX_PROCESS_RETRIES):
            try:
                payload = json.loads(raw.raw_payload)
                ods_order = ODSOrder.from_nested(payload)
                ods_order, has_clean_error, clean_error_message = clean_order(ods_order)
                if has_clean_error:
                    logger.warning("資料品質問題", order_id=ods_order.order_id, clean_error_message=clean_error_message)

                # schema drift（上游契約漂移）：與 has_clean_error 平行的獨立非阻斷訊號，
                # 跑在原始 payload 上以看見原始型別與多餘欄位（log 延後到 success path，避免 retry 重複記）。
                has_schema_drift, schema_drift_message, unmapped_fields = detect_schema_drift(payload)

                ods = ODS(
                    raw_id=raw_id,
                    order_id=ods_order.order_id,
                    order_date=ods_order.order_date,
                    ship_mode=ods_order.ship_mode,
                    order_status=ods_order.order_status,
                    delivery_date=ods_order.delivery_date,
                    delivery_days=ods_order.delivery_days,
                    returned=ods_order.returned,

                    customer_id=ods_order.customer_id,
                    customer_name=ods_order.customer_name,
                    age=ods_order.age,
                    gender=ods_order.gender,
                    membership_tier=ods_order.membership_tier,
                    registration_date=ods_order.registration_date,
                    acquisition_channel=ods_order.acquisition_channel,
                    newsletter_subscribed=ods_order.newsletter_subscribed,
                    preferred_payment_method=ods_order.preferred_payment_method,
                    preferred_device=ods_order.preferred_device,

                    country=ods_order.country,
                    region=ods_order.region,
                    state=ods_order.state,
                    city=ods_order.city,
                    postal_code=ods_order.postal_code,

                    payment_method=ods_order.payment_method,
                    tax_pct=ods_order.tax_pct,

                    device_used=ods_order.device_used,
                    customer_rating=ods_order.customer_rating,
                    is_repeat_customer=ods_order.is_repeat_customer,

                    items=ods_order.items,

                    has_clean_error=has_clean_error,
                    clean_error_message=clean_error_message,
                    has_schema_drift=has_schema_drift,
                    schema_drift_message=schema_drift_message,
                    unmapped_fields=unmapped_fields,
                    dq_rule_version=DQ_RULE_VERSION,
                    # 血緣：從 raw 取（非 payload），隨錨點落地到 ODS
                    source_client_id=raw.source_client_id,
                )
                break  # 處理成功，跳出 retry loop

            except json.JSONDecodeError:
                # 防禦性：/orders 路徑的 payload 已通過 OrderIN 驗證、必為合法 JSON，
                # 此分支正常不會觸發；保留以涵蓋手動 replay 或直接寫入 DB 造成的髒資料。
                logger.error("JSON 解析失敗")
                db.rollback()
                _commit_raw_status(db, raw_id, "error", "Invalid JSON payload")
                return

            except ValueError as e:
                logger.error("資料驗證失敗", error=str(e))
                db.rollback()
                _commit_raw_status(db, raw_id, "error", str(e))
                return

            except Exception as e:
                db.rollback()
                if attempt < MAX_PROCESS_RETRIES - 1:
                    logger.warning("處理失敗", attempt=attempt + 1, error=str(e))
                    RETRIES.add(1, {"stage": "processing"})
                    time.sleep(0.5 * (2 ** attempt))
                    raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one()
                else:
                    logger.error("已達最大重試次數", exc_info=True)
                    _commit_raw_status(db, raw_id, "error", f"Max retries exceeded: {type(e).__name__}: {e}")
                    return

        # first-write-wins: 確認 order_id 尚未寫入 ODS
        existing_ods = db.execute(
            select(ODS).where(ODS.order_id == ods_order.order_id)
        ).scalar_one_or_none()
        if existing_ods:
            logger.warning("order_id 重複，標記 duplicate",
                           order_id=ods_order.order_id, existing_raw_id=existing_ods.raw_id)
            _commit_raw_status(db, raw_id, "duplicate",
                               f"order_id {ods_order.order_id} 已由 raw_id={existing_ods.raw_id} 寫入 ODS")
            return

        # Point 4 (success path): ODS + quality_event + status 一起 commit，含 retry
        quality_event = QualityEvent(
            raw_id=raw_id,
            order_id=ods_order.order_id,
            event_type="initial_evaluation",
            from_state=None,
            to_state="quarantined" if has_clean_error else "clean",
            rule_version=DQ_RULE_VERSION,
            reason=clean_error_message,
        )
        db.add(ods)
        db.add(quality_event)
        for status_attempt in range(MAX_STATUS_RETRIES):
            try:
                db.execute(
                    update(Raw).where(Raw.id == raw_id).values(
                        status="processed",
                        error_message=None,
                        processed_at=datetime.now(UTC)
                    )
                )
                db.commit()
                # processed 是唯一不走 _commit_raw_status 的終態，故單獨計一次。
                ORDERS_RESULT.add(1, {"result": "processed"})
                logger.info("處理完成", order_id=raw.order_id)
                logger.info("quality_metric",
                    rule_version=DQ_RULE_VERSION,
                    has_clean_error=has_clean_error,
                    order_id=ods_order.order_id,
                    error_fields=clean_error_message,
                )
                if has_schema_drift:
                    logger.warning("schema_drift",
                        order_id=ods_order.order_id,
                        schema_drift_message=schema_drift_message,
                        unmapped_fields=unmapped_fields,
                    )
                break
            except IntegrityError:
                db.rollback()
                existing_ods = db.execute(
                    select(ODS).where(ODS.order_id == ods_order.order_id)
                ).scalar_one_or_none()
                logger.warning("IntegrityError，race condition，標記 duplicate",
                               existing_raw_id=existing_ods.raw_id if existing_ods else None)
                _commit_raw_status(db, raw_id, "duplicate",
                                   f"race condition: order_id {ods_order.order_id} 已由 raw_id={existing_ods.raw_id if existing_ods else '?'} 寫入 ODS")
                return
            except DataError as e:
                # 欄位長度超限等資料層錯誤是 deterministic（重試必然再失敗），
                # 直接 fast-fail 到終態 error，避免卡在 processing 被 scan 反覆重排（poison-pill）。
                db.rollback()
                logger.error("ODS 寫入 DataError（如欄位長度超限），標記 error", error=str(e))
                _commit_raw_status(db, raw_id, "error", f"DataError: {type(e).__name__}: {e}")
                return
            except ValueError as e:
                # NUL（0x00）等「合法資料但儲存引擎不可存」的值：psycopg2 在參數轉換階段
                # 拋 bare ValueError（非 DBAPI Error → SQLAlchemy 不包裝）。與 DataError 同屬
                # deterministic（重試必敗），同樣 fast-fail 到終態 error，避免 poison-pill。
                # 典型來源：上游送 JSON `\u0000` escape，json.loads 後成真實 NUL 字元落到文字/JSONB 欄。
                db.rollback()
                logger.error("ODS 寫入 ValueError（如字串含 NUL），標記 error", error=str(e))
                _commit_raw_status(db, raw_id, "error", f"ValueError: {type(e).__name__}: {e}")
                return
            except Exception as e:
                db.rollback()
                if status_attempt < MAX_STATUS_RETRIES - 1:
                    logger.warning("commit 失敗", attempt=status_attempt + 1)
                    RETRIES.add(1, {"stage": "status_update"})
                    time.sleep(0.5 * (2 ** status_attempt))
                    db.add(ods)
                    db.add(quality_event)
                else:
                    logger.critical("commit 失敗達上限，ODS 未寫入，record 卡在 processing", exc_info=True)

    finally:
        # 只記「真的 claim 到」的那些。claim 落空是毫秒級的 no-op，把它們混進來
        # 會在有競爭時把整個分佈往左拉——P95 會愈忙看起來愈健康，正好相反。
        if claimed:
            PROCESSING_DURATION.record(time.monotonic() - started)
        clear_contextvars()
        db.close()


def scan_and_recover(limit: int = SCAN_BATCH_SIZE, after_id: int = 0) -> list[int]:
    """
    掃描 stuck records 並回傳**一頁**需要重新處理的 raw_id。

    Step 1: stale processing（> STALE_PROCESSING_MINUTES）→ 重設為 pending
    Step 2: 收集躺超過 PENDING_GRACE_SECONDS 的 pending（含剛重設的），
            以 id 為游標往後取一頁回傳給 caller 排程

    ── 為什麼要分頁，以及為什麼游標是 id ─────────────────────────────────────

    派工**不會改變 status**——記錄要等 worker 搶佔成功才離開 pending。所以單純加
    `LIMIT` 是不夠的：每一輪都會撈到同一批最前面的記錄，永遠到不了後面。必須用
    `id > after_id` 當游標往前推，caller 才能一頁一頁走完積壓（見
    tasks.scan_and_dispatch_task 的迴圈）。

    分頁的代價是「同一輪內，若 Step 1 把某筆 id < after_id 的記錄重設為 pending，
    這一輪會跳過它」——它會在下一輪掃描被撈到，延遲一個掃描間隔，可接受。

    ── 為什麼逾時基準是 processing_started_at 而不是 received_at ──────────────

    這裡要判定的是「**這次處理**跑太久了，worker 大概已經死了」。received_at 回答
    的卻是「這筆資料**躺了**多久」——兩者在平時幾乎相等（進來就處理），在積壓時
    相差極大，而積壓正是這個判定最常被觸發的時候。

    用 received_at 會炸的時間軸（實測可重現，見 QUEUE-TW.md §3.1／§5.4）：

        T-30min  訂單攝入，received_at = T-30min，因 broker 停機留在 pending
        T+0      broker 復原，掃描派工；worker A 搶佔成功 → status = processing
        T+0.01   worker A 正在清洗、組 ODS（尚未 commit）
        T+0.02   下一輪掃描：status='processing' ✓ 且 received_at < now()-10min ✓
                 → 判定 stale → 改回 pending → 再派一則新訊息
        T+0.03   worker B 搶佔：狀態現在是 pending，CAS 成功 ← 擋不住
                 同一個 raw_id 有兩個 worker 在跑
        T+0.05   A 先 commit：ods.raw_id 落地，raw.status = 'processed'
        T+0.06   B 撞到「自己」寫的 ODS，被判為 duplicate，蓋掉 processed

    注意 CAS 沒有失效——它防的是同一個狀態下的競爭，防不了狀態被第三方倒退回
    pending。結果是一筆其實處理成功的訂單頂著 duplicate，污染了「上游重送」這個
    刻意保留的監控語意（見 CLAUDE.md 架構約束）。資料本身不會壞（ODS 的 UNIQUE
    擋住重複寫入），壞的是訊號與白做的工。

    改用 processing_started_at 之後，計時從「搶佔成功」起算，與資料躺多久無關，
    上面 T+0.02 那步就不再成立——自我碰撞因此**不可達**（全專案只有 try_claim_raw
    會寫入 processing，也只有這裡會把它退回 pending）。

    不變式：status='processing' ⇒ processing_started_at 非空。由 try_claim_raw
    保證，並由 migration e5f6a7b8c9d0 對既有資料做 backfill 建立。
    """
    db = SessionLocal()
    try:
        threshold = datetime.now(UTC) - timedelta(minutes=STALE_PROCESSING_MINUTES)

        stale_ids = db.execute(
            select(Raw.id)
            .where(and_(Raw.status == "processing", Raw.processing_started_at < threshold))
            .order_by(Raw.id)
            .limit(limit)
        ).scalars().all()

        if stale_ids:
            db.execute(
                update(Raw)
                .where(Raw.id.in_(stale_ids))
                .values(status="pending")
            )
            db.commit()
            logger.warning(
                "stale processing 重設為 pending",
                count=len(stale_ids),
                ids=list(stale_ids),
            )

        pending_ids = db.execute(
            select(Raw.id)
            .where(
                and_(
                    Raw.status == "pending",
                    Raw.received_at < datetime.now(UTC) - timedelta(seconds=PENDING_GRACE_SECONDS),
                    Raw.id > after_id,
                )
            )
            .order_by(Raw.id)
            .limit(limit)
        ).scalars().all()

        logger.info("找到 pending 記錄待重新處理", count=len(pending_ids), after_id=after_id)

        return list(pending_ids)
    finally:
        db.close()
