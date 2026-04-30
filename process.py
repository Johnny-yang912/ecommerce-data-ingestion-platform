from database import SessionLocal
from models import Raw , ODS
from schema import ODSOrder
from sqlalchemy import update, and_, select
import json
import logging
from datetime import datetime
from pytz import UTC
from clean import clean_order

logger = logging.getLogger(__name__)

def try_claim_raw(db,raw_id: int) -> bool:
        claim = (update(Raw).where(and_(Raw.id == raw_id, Raw.status == "pending")).values(status="processing"))
        result = db.execute(claim)
        return result.rowcount == 1

def process_raw_event(raw_id: int) -> None:
    logger.info("開始處理 raw_id=%s", raw_id)
    db = SessionLocal()
    try:
        claimed = try_claim_raw(db, raw_id)
        db.commit()
        if not claimed:
            logger.warning("raw_id=%s claim 失敗，狀態不是 pending 或已被其他 worker 處理", raw_id)
            return

        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()

        if not raw:
            logger.warning("raw_id=%s claim 成功但查無此筆資料", raw_id)
            return
        
        try:
            # 1. JSON 字串 → dict
            payload = json.loads(raw.raw_payload)

            # 2. 攤平 + 驗證
            ods_order = ODSOrder.from_nested(payload)

            # 步驟 2.5：清洗
            ods_order, has_clean_error, clean_error_message = clean_order(ods_order)
            if has_clean_error:
                logger.warning("raw_id=%s order_id=%s 資料品質問題: %s", raw_id, ods_order.order_id, clean_error_message)

            # 3. 寫入 ODS
            ods = ODS(
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
            db.add(ods)

            # 4. 更新 Raw 狀態
            raw.status = "processed"
            raw.error_message = None
            raw.processed_at = datetime.now(UTC)
            db.commit()
            logger.info("raw_id=%s order_id=%s 處理完成", raw_id, raw.order_id)

        except json.JSONDecodeError:
            logger.error("raw_id=%s JSON 解析失敗", raw_id)
            raw.status = "error"
            raw.error_message = "Invalid JSON payload"
            raw.processed_at = datetime.now(UTC)
            db.commit()

        except ValueError as e:
            logger.error("raw_id=%s 資料驗證失敗: %s", raw_id, e)
            raw.status = "error"
            raw.error_message = str(e)
            raw.processed_at = datetime.now(UTC)
            db.commit()

        except Exception as e:
            logger.error("raw_id=%s 未預期錯誤: %s", raw_id, e, exc_info=True)
            raw.status = "error"
            raw.error_message = f"{type(e).__name__}: {e}"
            raw.processed_at = datetime.now(UTC)
            db.commit()

    finally:
        db.close()
