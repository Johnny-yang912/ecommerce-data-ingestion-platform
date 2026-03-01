from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from pytz import UTC
from database import Base

class Raw(Base):
    __tablename__ = "raw"

    id = Column(Integer, primary_key=True, index=True)
    payload_text = Column(Text, nullable=False)
    received_at = Column(DateTime, nullable=False, default=datetime.now(UTC))
    status = Column(String(20), nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)



class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    value = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now(UTC))