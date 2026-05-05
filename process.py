from database import SessionLocal
from models import Raw, ODS
from schema import ODSOrder
from sqlalchemy import update, and_, select
import json
import logging
import time
from datetime import datetime, timedelta
from pytz import UTC
from clean import clean_order
from sqlalchemy.exc import OperationalError, IntegrityError

logger = logging.getLogger(__name__)

MAX_CLAIM_RETRIES = 3
MAX_PROCESS_RETRIES = 3
MAX_STATUS_RETRIES = 3
STALE_PROCESSING_MINUTES = 10


def try_claim_raw(db, raw_id: int) -> bool:
    claim = (update(Raw).where(and_(Raw.id == raw_id, Raw.status == "pending")).values(status="processing"))
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
            return
        except Exception as e:
            db.rollback()
            if attempt < MAX_STATUS_RETRIES - 1:
                logger.warning("raw_id=%s status 更新失敗，第 %d 次重試", raw_id, attempt + 1)
                time.sleep(0.5 * (2 ** attempt))
            else:
                logger.critical(
                    "raw_id=%s status 更新失敗，record 可能永久卡在 processing: %s",
                    raw_id, e, exc_info=True
                )


def process_raw_event(raw_id: int) -> None:
    logger.info("開始處理 raw_id=%s", raw_id)
    db = SessionLocal()
    try:
        # Point 3: claim retry，區分 DB 例外 vs 正常搶佔失敗
        claimed = False
        for attempt in range(MAX_CLAIM_RETRIES):
            try:
                claimed = try_claim_raw(db, raw_id)
                db.commit()
                break
            except OperationalError as e:
                db.rollback()
                if attempt < MAX_CLAIM_RETRIES - 1:
                    logger.warning("raw_id=%s claim DB 例外，第 %d 次重試: %s", raw_id, attempt + 1, e)
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    logger.error("raw_id=%s claim 失敗，已達最大重試次數，放棄", raw_id, exc_info=True)
                    return

        if not claimed:
            logger.warning("raw_id=%s claim 失敗，狀態不是 pending 或已被其他 worker 處理", raw_id)
            return

        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()
        if not raw:
            logger.warning("raw_id=%s claim 成功但查無此筆資料", raw_id)
            return

        # Point 2: processing retry，僅對暫時性例外重試，資料錯誤直接 mark error
        ods = None
        for attempt in range(MAX_PROCESS_RETRIES):
            try:
                payload = json.loads(raw.raw_payload)
                ods_order = ODSOrder.from_nested(payload)
                ods_order, has_clean_error, clean_error_message = clean_order(ods_order)
                if has_clean_error:
                    logger.warning("raw_id=%s order_id=%s 資料品質問題: %s", raw_id, ods_order.order_id, clean_error_message)

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

                    items=json.dumps(ods_order.items, ensure_ascii=False),

                    has_clean_error=has_clean_error,
                    clean_error_message=clean_error_message,
                )
                break  # 處理成功，跳出 retry loop

            except json.JSONDecodeError:
                logger.error("raw_id=%s JSON 解析失敗", raw_id)
                db.rollback()
                _commit_raw_status(db, raw_id, "error", "Invalid JSON payload")
                return

            except ValueError as e:
                logger.error("raw_id=%s 資料驗證失敗: %s", raw_id, e)
                db.rollback()
                _commit_raw_status(db, raw_id, "error", str(e))
                return

            except Exception as e:
                db.rollback()
                if attempt < MAX_PROCESS_RETRIES - 1:
                    logger.warning("raw_id=%s 處理失敗，第 %d 次重試: %s", raw_id, attempt + 1, e)
                    time.sleep(0.5 * (2 ** attempt))
                    raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one()
                else:
                    logger.error("raw_id=%s 已達最大重試次數: %s", raw_id, e, exc_info=True)
                    _commit_raw_status(db, raw_id, "error", f"Max retries exceeded: {type(e).__name__}: {e}")
                    return

        # first-write-wins: 確認 order_id 尚未寫入 ODS
        existing_ods = db.execute(
            select(ODS).where(ODS.order_id == ods_order.order_id)
        ).scalar_one_or_none()
        if existing_ods:
            logger.warning("raw_id=%s order_id=%s 重複，已由 raw_id=%s 處理，標記 duplicate",
                           raw_id, ods_order.order_id, existing_ods.raw_id)
            _commit_raw_status(db, raw_id, "duplicate",
                               f"order_id {ods_order.order_id} 已由 raw_id={existing_ods.raw_id} 寫入 ODS")
            return

        # Point 4 (success path): ODS + status 一起 commit，含 retry
        db.add(ods)
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
                logger.info("raw_id=%s order_id=%s 處理完成", raw_id, raw.order_id)
                break
            except IntegrityError:
                db.rollback()
                # TOCTOU：pre-check 後另一個 worker 搶先寫入
                existing_ods = db.execute(
                    select(ODS).where(ODS.order_id == ods_order.order_id)
                ).scalar_one_or_none()
                logger.warning("raw_id=%s IntegrityError，race condition，標記 duplicate", raw_id)
                _commit_raw_status(db, raw_id, "duplicate",
                                   f"race condition: order_id {ods_order.order_id} 已由 raw_id={existing_ods.raw_id if existing_ods else '?'} 寫入 ODS")
                return
            except Exception as e:
                db.rollback()
                if status_attempt < MAX_STATUS_RETRIES - 1:
                    logger.warning("raw_id=%s commit 失敗，第 %d 次重試", raw_id, status_attempt + 1)
                    time.sleep(0.5 * (2 ** status_attempt))
                    db.add(ods)  # rollback 後需重新 add
                else:
                    logger.critical(
                        "raw_id=%s commit 失敗達上限，ODS 未寫入，record 卡在 processing: %s",
                        raw_id, e, exc_info=True
                    )

    finally:
        db.close()


def scan_and_recover() -> list[int]:
    """
    掃描 stuck records 並回傳需要重新處理的 raw_id list。

    Step 1: stale processing（> STALE_PROCESSING_MINUTES）→ 重設為 pending
            ⚠️ 若 ODS 已寫入，重跑時 idempotency 保護會攔截重複寫入，標為 duplicate
    Step 2: 收集所有 pending（含剛重設的）回傳給 caller 排程
    """
    db = SessionLocal()
    try:
        threshold = datetime.now(UTC) - timedelta(minutes=STALE_PROCESSING_MINUTES)

        stale_ids = db.execute(
            select(Raw.id).where(
                and_(Raw.status == "processing", Raw.received_at < threshold)
            )
        ).scalars().all()

        if stale_ids:
            db.execute(
                update(Raw)
                .where(Raw.id.in_(stale_ids))
                .values(status="pending")
            )
            db.commit()
            logger.warning(
                "scan: %d 筆 stale processing 重設為 pending，重新排程（ODS 重複寫入已由 idempotency 保護）ids=%s",
                len(stale_ids), list(stale_ids)
            )

        pending_ids = db.execute(
            select(Raw.id).where(Raw.status == "pending")
        ).scalars().all()

        logger.info("scan: 找到 %d 筆 pending 記錄待重新處理", len(pending_ids))

        return list(pending_ids)
    finally:
        db.close()
