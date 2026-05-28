from schema import ODSOrder
from datetime import date
from typing import Optional

DQ_RULE_VERSION = "v1"  # 每次規則改動時 bump，搭配 git tag 記錄變更內容


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
                errors.append(f"items[{i}].quantity 不能是 0 或負數（值：{qty}）")

            unit_price = item.get("unit_price")
            if unit_price is not None and unit_price < 0:
                errors.append(f"items[{i}].unit_price 不能是負數（值：{unit_price}）")

            discount_pct = item.get("discount_pct")
            if discount_pct is not None and not (0 <= discount_pct <= 100):
                errors.append(f"items[{i}].discount_pct 應在 0~100 之間（值：{discount_pct}）")

    # tax_pct
    if ods.tax_pct is not None and not (0 <= ods.tax_pct <= 100):
        errors.append(f"tax_pct 應在 0~100 之間（值：{ods.tax_pct}）")

    # delivery_date 不能早於 order_date
    if ods.delivery_date and ods.order_date:
        if ods.delivery_date < ods.order_date:
            errors.append(f"delivery_date（{ods.delivery_date}）不能早於 order_date（{ods.order_date}）")

    # customer_rating 1~5
    if ods.customer_rating is not None and not (1 <= ods.customer_rating <= 5):
        errors.append(f"customer_rating 應在 1~5 之間（值：{ods.customer_rating}）")

    # age 0~120
    if ods.age is not None and not (0 <= ods.age <= 120):
        errors.append(f"age 應在 0~120 之間（值：{ods.age}）")

    return ods, errors


def clean_order(ods: ODSOrder) -> tuple[ODSOrder, bool, Optional[list]]:
    ods = format_clean(ods)
    ods, business_errors = business_clean(ods)

    has_clean_error = len(business_errors) > 0
    clean_error_message = business_errors if business_errors else None

    return ods, has_clean_error, clean_error_message