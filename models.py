from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Date, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from database import Base
from sqlalchemy.dialects.postgresql import JSONB

class Raw(Base):
    __tablename__ = "raw"

    id = Column(Integer, primary_key=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    status = Column(String(20), nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)

    # 系統攤平欄位
    order_id = Column(String(50), nullable=True, index=True)

    # 資料血緣起點：驗證層解析出的來源端 client（非 payload 內容，由 API key 對應而來）
    # NULL 的語意：此筆非經認證 API 落地（手動 replay / backfill / 直接寫 DB），來源未建立。
    # Raw 作為不做假設的 landing 層，刻意保留「來源未知」這個可表達的狀態。
    source_client_id = Column(String(50), nullable=True, index=True)

    # 整包原始資料
    raw_payload = Column(Text, nullable=False)  # 用Text保留完整JSON字串訊息，用以追蹤和除錯


class ODS(Base):
    __tablename__ = "ods"

    id = Column(Integer, primary_key=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    raw_id = Column(Integer, ForeignKey("raw.id", ondelete="NO ACTION"), nullable=False, unique=True)

    # 訂單主體
    order_id = Column(String(50), nullable=False, index=True, unique=True)
    order_date = Column(Date, nullable=True)
    ship_mode = Column(String(64), nullable=True)
    order_status = Column(String(64), nullable=True)
    delivery_date = Column(Date, nullable=True)
    delivery_days = Column(Integer, nullable=True)
    returned = Column(Boolean, nullable=True)

    # 顧客
    # order_id 是唯一硬閘門；customer_id 缺失改為可落地（nullable）+ 由 schema drift 訊號標記
    customer_id = Column(String(50), nullable=True, index=True)
    customer_name = Column(String(255), nullable=True)
    age = Column(Integer, nullable=True)
    gender = Column(String(32), nullable=True)
    membership_tier = Column(String(64), nullable=True)
    registration_date = Column(Date, nullable=True)
    acquisition_channel = Column(String(64), nullable=True)
    newsletter_subscribed = Column(Boolean, nullable=True)
    preferred_payment_method = Column(String(64), nullable=True)
    preferred_device = Column(String(64), nullable=True)

    # 地址
    country = Column(String(64), nullable=True)
    region = Column(String(64), nullable=True)
    state = Column(String(64), nullable=True)
    city = Column(String(128), nullable=True)
    postal_code = Column(String(32), nullable=True)

    # 金流
    payment_method = Column(String(64), nullable=True)
    tax_pct = Column(Float, nullable=True)

    # 行為
    device_used = Column(String(64), nullable=True)
    customer_rating = Column(Float, nullable=True)
    is_repeat_customer = Column(Boolean, nullable=True)

    # items 整包 (清理與攤平後使用JSONB存儲，方便後續分析和查詢)
    items = Column(JSONB, nullable=True)

    # 清洗錯誤標籤（業務值品質）
    has_clean_error = Column(Boolean, nullable=False, default=False)
    clean_error_message = Column(JSONB, nullable=True)

    # Schema drift 標籤（上游契約漂移，與 has_clean_error 平行、互不混用的獨立訊號）
    # 非阻斷：drift 不會把乾淨訂單踢出下游 Gold，僅作監控訊號
    has_schema_drift = Column(Boolean, nullable=False, default=False)
    schema_drift_message = Column(JSONB, nullable=True)
    # 收容上游多送的未知欄位（保留路徑與值），讓「多欄位」也能落地、不靜默丟失
    unmapped_fields = Column(JSONB, nullable=True)

    # 品質評估版本（攝入時使用的規則版本，之後永遠不動）
    dq_rule_version = Column(String(10), nullable=True)

    # 資料血緣：攝入當下的來源端（與 dq_rule_version 同類的不可變 metadata，隨錨點走）
    source_client_id = Column(String(50), nullable=True, index=True)


class QualityEvent(Base):
    __tablename__ = "quality_events"

    id = Column(Integer, primary_key=True)
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
    reason = Column(JSONB, nullable=True)