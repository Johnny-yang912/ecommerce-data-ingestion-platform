from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime, date

class UserIN(BaseModel):
    name: Optional[Any] = None
    value: Optional[Any] = None

# --- 子群組 ---

class CustomerInfo(BaseModel):
    customer_id: str
    customer_name: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    membership_tier: Optional[str] = None
    registration_date: Optional[date] = None
    acquisition_channel: Optional[str] = None
    newsletter_subscribed: Optional[bool] = None
    preferred_payment_method: Optional[str] = None
    preferred_device: Optional[str] = None


class AddressInfo(BaseModel):
    country: Optional[str] = None
    region: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None


class ProductInfo(BaseModel):
    product_id: str
    product_name: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    brand: Optional[str] = None
    condition: Optional[str] = None


class PaymentInfo(BaseModel):
    payment_method: Optional[str] = None
    tax_pct: Optional[float] = None


class OrderItemInfo(BaseModel):
    product: ProductInfo
    quantity: int
    unit_price: float
    cost_price: Optional[float] = None
    discount_pct: Optional[float] = None
    shipping_fee: Optional[float] = None


class BehaviorInfo(BaseModel):
    device_used: Optional[str] = None
    is_repeat_customer: Optional[bool] = None
    customer_rating: Optional[float] = None


# --- 主體 ---

class OrderIN(BaseModel):
    order_id: str
    order_date: date
    ship_mode: Optional[str] = None
    order_status: Optional[str] = None
    delivery_date: Optional[date] = None
    delivery_days: Optional[int] = None
    returned: Optional[bool] = None

    customer: CustomerInfo
    address: AddressInfo
    items: list[OrderItemInfo]
    payment: PaymentInfo
    behavior: Optional[BehaviorInfo] = None



#攤平
class ODSOrder(BaseModel):
    # 訂單主體
    order_id: str
    order_date: date
    ship_mode: Optional[str] = None
    order_status: Optional[str] = None
    delivery_date: Optional[date] = None
    delivery_days: Optional[int] = None
    returned: Optional[bool] = None

    # 顧客
    customer_id: str
    customer_name: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    membership_tier: Optional[str] = None
    registration_date: Optional[date] = None
    acquisition_channel: Optional[str] = None
    newsletter_subscribed: Optional[bool] = None
    preferred_payment_method: Optional[str] = None
    preferred_device: Optional[str] = None

    # 地址
    country: Optional[str] = None
    region: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None

    # 金流
    payment_method: Optional[str] = None
    tax_pct: Optional[float] = None

    # 行為
    device_used: Optional[str] = None
    customer_rating: Optional[float] = None
    is_repeat_customer: Optional[bool] = None

    # items 整包存 jsonb
    items: Any = None

    @classmethod
    def from_nested(cls, payload: dict):
        customer = payload.get("customer", {})
        address = payload.get("address", {})
        payment = payload.get("payment", {})
        behavior = payload.get("behavior", {}) or {}

        return cls(
            # 訂單主體
            order_id=payload.get("order_id"),
            order_date=payload.get("order_date"),
            ship_mode=payload.get("ship_mode"),
            order_status=payload.get("order_status"),
            delivery_date=payload.get("delivery_date"),
            delivery_days=payload.get("delivery_days"),
            returned=payload.get("returned"),

            # 顧客
            customer_id=customer.get("customer_id"),
            customer_name=customer.get("customer_name"),
            age=customer.get("age"),
            gender=customer.get("gender"),
            membership_tier=customer.get("membership_tier"),
            registration_date=customer.get("registration_date"),
            acquisition_channel=customer.get("acquisition_channel"),
            newsletter_subscribed=customer.get("newsletter_subscribed"),
            preferred_payment_method=customer.get("preferred_payment_method"),
            preferred_device=customer.get("preferred_device"),

            # 地址
            country=address.get("country"),
            region=address.get("region"),
            state=address.get("state"),
            city=address.get("city"),
            postal_code=address.get("postal_code"),

            # 金流
            payment_method=payment.get("payment_method"),
            tax_pct=payment.get("tax_pct"),

            # 行為
            device_used=behavior.get("device_used"),
            customer_rating=behavior.get("customer_rating"),
            is_repeat_customer=behavior.get("is_repeat_customer"),

            # items 整包
            items=payload.get("items"),
        )

class RawOut(BaseModel):
    id: int
    status: str
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None
    payload_preview: Optional[str] = None  # 只回傳截斷內容（避免整包太大）

    class Config:
        from_attributes = True

class RawListItem(BaseModel):
    id: int
    status: str
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None

    class Config:
        from_attributes = True