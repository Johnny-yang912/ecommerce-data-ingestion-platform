from fastapi import FastAPI, UploadFile, File, HTTPException,BackgroundTasks
from schema import UserIN, RawOut
import io
import pandas as pd
import json
import sqlite3
from database import SessionLocal, Base, engine
from models import Raw
from process import process_raw_event
from sqlalchemy import select, update

app = FastAPI()

Base.metadata.create_all(bind=engine)

@app.post("/users")
async def create_user(user: UserIN , background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        payload_dict =user.model_dump()
        payload_text = json.dumps(payload_dict, ensure_ascii=False)

        raw = Raw(payload_text=payload_text)
        db.add(raw)
        db.commit()
        db.refresh(raw)

        background_tasks.add_task(process_raw_event, raw.id)
        return {"raw_id": raw.id , "status": "pending"}
    finally:
        db.close()

@app.post("/process_raw/{raw_id}")
async def process_raw(raw_id: int, force: bool = False):
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
