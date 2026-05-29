from schema import ODSOrder
from datetime import date
from typing import Optional

DQ_RULE_VERSION = "v1"  # 每次規則改動時 bump，搭配 git tag 記錄變更內容


class DQCode:
    """business_clean 違規碼（常數枚舉）。

    下游（quality_events.reason / clean_error_message JSONB）以 code 作為穩定識別，
    與人類可讀訊息解耦：訊息措辭可調整，code 不變，避免下游字串比對隨文案漂移。
    """
    QUANTITY_NON_POSITIVE        = "quantity_non_positive"
    UNIT_PRICE_NEGATIVE          = "unit_price_negative"
    DISCOUNT_PCT_OUT_OF_RANGE    = "discount_pct_out_of_range"
    TAX_PCT_OUT_OF_RANGE         = "tax_pct_out_of_range"
    DELIVERY_BEFORE_ORDER        = "delivery_before_order"
    CUSTOMER_RATING_OUT_OF_RANGE = "customer_rating_out_of_range"
    AGE_OUT_OF_RANGE             = "age_out_of_range"


def format_clean(ods: ODSOrder) -> ODSOrder:

    # 統一小寫
    if ods.gender:
        ods.gender = ods.gender.strip().lower()
    if ods.ship_mode:
        ods.ship_mode = ods.ship_mode.strip().lower()
    if ods.membership_tier:
        ods.membership_tier = ods.membership_tier.strip().lower()
    if ods.payment_method:
        ods.payment_method = ods.payment_method.strip().lower()
    if ods.preferred_payment_method:
        ods.preferred_payment_method = ods.preferred_payment_method.strip().lower()
    if ods.preferred_device:
        ods.preferred_device = ods.preferred_device.strip().lower()

    # 去頭尾空白
    if ods.order_status:
        ods.order_status = ods.order_status.strip()
    if ods.country:
        ods.country = ods.country.strip()
    if ods.region:
        ods.region = ods.region.strip()
    if ods.state:
        ods.state = ods.state.strip()
    if ods.city:
        ods.city = ods.city.strip()
    if ods.postal_code:
        ods.postal_code = ods.postal_code.strip()
    if ods.customer_name:
        ods.customer_name = ods.customer_name.strip()

    if ods.order_date and ods.delivery_date:
        ods.delivery_days = (ods.delivery_date - ods.order_date).days

    return ods


def business_clean(ods: ODSOrder) -> ODSOrder:
    errors = []

    # quantity 不能是 0 或負數
    # items 是 list of dict，需要逐一檢查
    if ods.items:
        for i, item in enumerate(ods.items):
            qty = item.get("quantity")
            if qty is not None and qty <= 0:
                errors.append({"code": DQCode.QUANTITY_NON_POSITIVE, "field": "quantity", "value": qty, "index": i})

            unit_price = item.get("unit_price")
            if unit_price is not None and unit_price < 0:
                errors.append({"code": DQCode.UNIT_PRICE_NEGATIVE, "field": "unit_price", "value": unit_price, "index": i})

            discount_pct = item.get("discount_pct")
            if discount_pct is not None and not (0 <= discount_pct <= 100):
                errors.append({"code": DQCode.DISCOUNT_PCT_OUT_OF_RANGE, "field": "discount_pct", "value": discount_pct, "index": i})

    # tax_pct
    if ods.tax_pct is not None and not (0 <= ods.tax_pct <= 100):
        errors.append({"code": DQCode.TAX_PCT_OUT_OF_RANGE, "field": "tax_pct", "value": ods.tax_pct})

    # delivery_date 不能早於 order_date
    # value 存 isoformat 字串：clean_error_message / quality_events.reason 是 JSONB，date 物件無法直接序列化
    if ods.delivery_date and ods.order_date:
        if ods.delivery_date < ods.order_date:
            errors.append({"code": DQCode.DELIVERY_BEFORE_ORDER, "field": "delivery_date",
                           "value": ods.delivery_date.isoformat(), "order_date": ods.order_date.isoformat()})

    # customer_rating 1~5
    if ods.customer_rating is not None and not (1 <= ods.customer_rating <= 5):
        errors.append({"code": DQCode.CUSTOMER_RATING_OUT_OF_RANGE, "field": "customer_rating", "value": ods.customer_rating})

    # age 0~120
    if ods.age is not None and not (0 <= ods.age <= 120):
        errors.append({"code": DQCode.AGE_OUT_OF_RANGE, "field": "age", "value": ods.age})

    return ods, errors


def clean_order(ods: ODSOrder) -> tuple[ODSOrder, bool, Optional[list]]:
    ods = format_clean(ods)
    ods, business_errors = business_clean(ods)

    has_clean_error = len(business_errors) > 0
    clean_error_message = business_errors if business_errors else None

    return ods, has_clean_error, clean_error_message