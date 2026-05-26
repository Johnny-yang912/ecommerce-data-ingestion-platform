from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from database import Base
from sqlalchemy.dialects.postgresql import JSONB

class Raw(Base):
    __tablename__ = "raw"

    id = Column(Integer, primary_key=True, index=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    status = Column(String(20), nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)

    # 系統攤平欄位
    order_id = Column(String(50), nullable=True, index=True)

    # 整包原始資料
    raw_payload = Column(Text, nullable=False)  # 用Text保留完整JSON字串訊息，用以追蹤和除錯


class ODS(Base):
    __tablename__ = "ods"

    id = Column(Integer, primary_key=True, index=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    raw_id = Column(Integer, nullable=True, unique=True)

    # 訂單主體
    order_id = Column(String(50), nullable=False, index=True, unique=True)
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

    # items 整包 (清理與攤平後使用JSONB存儲，方便後續分析和查詢)
    items = Column(JSONB, nullable=True)

    # 清洗錯誤標籤
    has_clean_error = Column(Boolean, nullable=False, default=False)
    clean_error_message = Column(Text, nullable=True)

    # 品質評估版本（攝入時使用的規則版本，之後永遠不動）
    dq_rule_version = Column(String(10), nullable=True)


class QualityEvent(Base):
    __tablename__ = "quality_events"

    id = Column(Integer, primary_key=True, index=True)
    raw_id = Column(Integer, nullable=False, index=True)
    order_id = Column(String(50), nullable=False, index=True)

    # 事件類型：initial_evaluation | promotion | rejection
    event_type = Column(String(30), nullable=False)

    # 狀態機轉移
    from_state = Column(String(30), nullable=True)
    # to_state: clean | quarantined | promoted | permanently_rejected
    to_state = Column(String(30), nullable=False)

    rule_version = Column(String(10), nullable=False)
    event_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    reason = Column(Text, nullable=True)