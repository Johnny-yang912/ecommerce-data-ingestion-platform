from database import SessionLocal
from models import Raw, Measurement
from sqlalchemy import update, and_, select
import json
from datetime import datetime
from pytz import UTC
from clean import clean_and_validate

def try_claim_raw(db,raw_id: int) -> bool:
        claim = (update(Raw).where(and_(Raw.id == raw_id, Raw.status == "pending")).values(status="processing"))
        result = db.execute(claim)
        return result.rowcount == 1

def process_raw_event(raw_id: int) -> None:
    db = SessionLocal()
    try:
        claimed = try_claim_raw(db, raw_id)
        db.commit()
        if not claimed:
            return

        raw = db.execute(select(Raw).where(Raw.id == raw_id)).scalar_one_or_none()

        if not raw:
            return
        
        try:
            payload = json.loads(raw.payload_text)
            cleaned = clean_and_validate(payload)
            m=Measurement(name=cleaned["name"], value=cleaned["value"])
            db.add(m)
            raw.status = "processed"
            raw.error_message = None
            raw.processed_at = datetime.now(UTC)
            db.commit()
            return

        except json.JSONDecodeError:
            raw.status = "error"
            raw.error_message = "Invalid JSON payload"
            raw.processed_at = datetime.now(UTC)
            db.commit()
            return
        
        except ValueError as e:
            raw.status = "error"
            raw.error_message = str(e)
            raw.processed_at = datetime.now(UTC)
            db.commit()
            return
        
        except Exception as e:
            raw.status = "error"
            raw.error_message = f"{type(e).__name__}: {e}"
            raw.processed_at = datetime.now(UTC)
            db.commit()
            return
        
    finally:
        db.close()
