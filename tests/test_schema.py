"""
schema.py 單元測試（新補）

ODSOrder.from_nested：把巢狀的 POST payload 攤平為 ODS 扁平 model。
這也是純函式，直接驗證輸出，不需要 mock。
"""

import pytest
from schema import ODSOrder


FULL_PAYLOAD = {
    "order_id": "ORD-001",
    "order_date": "2024-03-15",
    "ship_mode": "First Class",
    "order_status": "Shipped",
    "delivery_date": "2024-03-20",
    "delivery_days": 5,
    "returned": False,
    "customer": {
        "customer_id": "CUST-001",
        "customer_name": "Alice",
        "age": 30,
        "gender": "female",
        "membership_tier": "gold",
        "registration_date": "2020-06-15",
        "acquisition_channel": "organic",
        "newsletter_subscribed": True,
        "preferred_payment_method": "credit_card",
        "preferred_device": "mobile",
    },
    "address": {
        "country": "Taiwan",
        "region": "East Asia",
        "state": "Taipei",
        "city": "Zhongshan",
        "postal_code": "10001",
    },
    "items": [
        {"product": {"product_id": "PROD-001"}, "quantity": 2, "unit_price": 199.0},
    ],
    "payment": {
        "payment_method": "credit_card",
        "tax_pct": 5.0,
    },
    "behavior": {
        "device_used": "desktop",
        "is_repeat_customer": True,
        "customer_rating": 4.5,
    },
}


class TestODSOrderFromNested:

    def test_full_payload_all_fields_correctly_mapped(self):
        """完整 payload：所有欄位都要正確攤平到對應位置。"""
        ods = ODSOrder.from_nested(FULL_PAYLOAD)

        # 訂單主體
        assert ods.order_id == "ORD-001"
        assert str(ods.order_date) == "2024-03-15"
        assert ods.ship_mode == "First Class"
        assert ods.order_status == "Shipped"
        assert str(ods.delivery_date) == "2024-03-20"
        assert ods.delivery_days == 5
        assert ods.returned is False

        # 顧客（從 customer 子物件攤平）
        assert ods.customer_id == "CUST-001"
        assert ods.customer_name == "Alice"
        assert ods.age == 30
        assert ods.gender == "female"
        assert ods.membership_tier == "gold"
        assert ods.acquisition_channel == "organic"
        assert ods.newsletter_subscribed is True
        assert ods.preferred_payment_method == "credit_card"
        assert ods.preferred_device == "mobile"

        # 地址（從 address 子物件攤平）
        assert ods.country == "Taiwan"
        assert ods.region == "East Asia"
        assert ods.state == "Taipei"
        assert ods.city == "Zhongshan"
        assert ods.postal_code == "10001"

        # 金流（從 payment 子物件攤平）
        assert ods.payment_method == "credit_card"
        assert ods.tax_pct == 5.0

        # 行為（從 behavior 子物件攤平）
        assert ods.device_used == "desktop"
        assert ods.is_repeat_customer is True
        assert ods.customer_rating == 4.5

        # items 整包傳入，不轉換
        assert isinstance(ods.items, list) and len(ods.items) == 1

    def test_behavior_none_defaults_to_all_none(self):
        """behavior 欄位為 None → 三個行為欄位都是 None。"""
        payload = {**FULL_PAYLOAD, "behavior": None}
        ods = ODSOrder.from_nested(payload)
        assert ods.device_used is None
        assert ods.is_repeat_customer is None
        assert ods.customer_rating is None

    def test_behavior_key_missing_defaults_to_all_none(self):
        """payload 完全沒有 behavior key → 三個行為欄位都是 None。"""
        payload = {k: v for k, v in FULL_PAYLOAD.items() if k != "behavior"}
        ods = ODSOrder.from_nested(payload)
        assert ods.device_used is None
        assert ods.customer_rating is None

    def test_empty_address_produces_all_none(self):
        """address 為空 dict → 所有地址欄位都是 None。"""
        payload = {**FULL_PAYLOAD, "address": {}}
        ods = ODSOrder.from_nested(payload)
        assert ods.country is None
        assert ods.region is None
        assert ods.state is None
        assert ods.city is None
        assert ods.postal_code is None

    def test_minimal_customer_optional_fields_are_none(self):
        """customer 只有必填 customer_id → 其餘顧客欄位為 None。"""
        payload = {**FULL_PAYLOAD, "customer": {"customer_id": "CUST-MIN"}}
        ods = ODSOrder.from_nested(payload)
        assert ods.customer_id == "CUST-MIN"
        assert ods.customer_name is None
        assert ods.age is None
        assert ods.membership_tier is None

    def test_items_passed_through_unchanged(self):
        """items 整包傳入，from_nested 不做任何轉換或解析。"""
        items = [
            {"product": {"product_id": "A"}, "quantity": 1, "unit_price": 10.0},
            {"product": {"product_id": "B"}, "quantity": 3, "unit_price": 20.0},
        ]
        payload = {**FULL_PAYLOAD, "items": items}
        ods = ODSOrder.from_nested(payload)
        assert ods.items == items

    def test_optional_order_fields_default_to_none(self):
        """最小 payload（只有必填欄位）→ 所有可選訂單欄位為 None。"""
        payload = {
            "order_id": "ORD-MIN",
            "order_date": "2024-01-01",
            "customer": {"customer_id": "CUST-001"},
            "address": {},
            "items": [],
            "payment": {},
        }
        ods = ODSOrder.from_nested(payload)
        assert ods.ship_mode is None
        assert ods.order_status is None
        assert ods.delivery_date is None
        assert ods.returned is None
        assert ods.payment_method is None
        assert ods.tax_pct is None
