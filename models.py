from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from pytz import UTC
from database import Base

class Raw(Base):
    __tablename__ = "raw"

    id = Column(Integer, primary_key=True, index=True)
    received_at = Column(DateTime, nullable=False, default=datetime.now(UTC))
    status = Column(String(20), nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)

    # 系統攤平欄位
    order_id = Column(String(50), nullable=True, index=True)

    # 整包原始資料
    raw_payload = Column(Text, nullable=False)  # SQLite 用 Text 存 JSON 字串


class ODS(Base):
    __tablename__ = "ods"

    id = Column(Integer, primary_key=True, index=True)
    received_at = Column(DateTime, nullable=False, default=datetime.now(UTC))

    # 訂單主體
    order_id = Column(String(50), nullable=False, index=True)
    order_date = Column(Date, nullable=True)
    ship_mode = Column(String(50), nullable=True)
    order_status = Column(String(20), nullable=True)
    delivery_date = Column(Date, nullable=True)
    delivery_days = Column(Integer, nullable=True)
    returned = Column(Boolean, nullable=True)

    # 顧客
    customer_id = Column(String(50), nullable=False, index=True)
    customer_name = Column(String(100), nullable=True)
    age = Column(Integer, nullable=True)
    gender = Column(String(20), nullable=True)
    membership_tier = Column(String(20), nullable=True)
    registration_date = Column(Date, nullable=True)
    acquisition_channel = Column(String(50), nullable=True)
    newsletter_subscribed = Column(Boolean, nullable=True)
    preferred_payment_method = Column(String(50), nullable=True)
    preferred_device = Column(String(50), nullable=True)

    # 地址
    country = Column(String(50), nullable=True)
    region = Column(String(50), nullable=True)
    state = Column(String(50), nullable=True)
    city = Column(String(50), nullable=True)
    postal_code = Column(String(20), nullable=True)

    # 金流
    payment_method = Column(String(50), nullable=True)
    tax_pct = Column(Float, nullable=True)

    # 行為
    device_used = Column(String(50), nullable=True)
    customer_rating = Column(Float, nullable=True)
    is_repeat_customer = Column(Boolean, nullable=True)

    # items 整包（SQLite 用 Text）
    items = Column(Text, nullable=True)
