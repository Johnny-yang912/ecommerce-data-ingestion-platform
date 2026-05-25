"""
clean.py 單元測試（新補）

format_clean : 字串正規化（lowercase / strip）、delivery_days 計算
business_clean: 業務規則驗證，每條規則獨立測試，多錯誤同時累積
clean_order   : 整合路徑（format + business 都有被執行）

這些是純函式，不需要 mock，是最容易加的測試種類。
"""

import pytest
from datetime import date
from schema import ODSOrder
from clean import format_clean, business_clean, clean_order


# ─── Helper：建立最小合法 ODSOrder ────────────────────────────────────────────

def make_ods(**kwargs) -> ODSOrder:
    """
    建立 ODSOrder，只填入必填欄位（order_id, order_date, customer_id），
    其餘欄位透過 kwargs 傳入。
    """
    defaults = dict(
        order_id="ORD-001",
        order_date=date(2024, 1, 1),
        customer_id="CUST-001",
    )
    defaults.update(kwargs)
    return ODSOrder(**defaults)


# ─── format_clean ─────────────────────────────────────────────────────────────

class TestFormatClean:

    def test_lowercase_fields_are_lowercased_and_stripped(self):
        """gender / ship_mode / membership_tier / payment_method 等欄位：lowercase + strip。"""
        ods = make_ods(
            gender="  MALE  ",
            ship_mode="  First Class  ",
            membership_tier="  GOLD  ",
            payment_method="  Credit_Card  ",
            preferred_payment_method="  PayPal  ",
            preferred_device="  Mobile  ",
        )
        result = format_clean(ods)
        assert result.gender == "male"
        assert result.ship_mode == "first class"
        assert result.membership_tier == "gold"
        assert result.payment_method == "credit_card"
        assert result.preferred_payment_method == "paypal"
        assert result.preferred_device == "mobile"

    def test_strip_only_fields_preserve_case(self):
        """order_status / country / region / state / city / postal_code / customer_name：只 strip，不改大小寫。"""
        ods = make_ods(
            order_status="  Shipped  ",
            country="  Taiwan  ",
            region="  East Asia  ",
            state="  Taipei  ",
            city="  Test City  ",
            postal_code="  10001  ",
            customer_name="  John Doe  ",
        )
        result = format_clean(ods)
        assert result.order_status == "Shipped"
        assert result.country == "Taiwan"
        assert result.region == "East Asia"
        assert result.state == "Taipei"
        assert result.city == "Test City"
        assert result.postal_code == "10001"
        assert result.customer_name == "John Doe"

    def test_delivery_days_calculated_when_both_dates_present(self):
        """order_date 和 delivery_date 都有時，自動計算 delivery_days。"""
        ods = make_ods(
            order_date=date(2024, 1, 1),
            delivery_date=date(2024, 1, 8),
        )
        result = format_clean(ods)
        assert result.delivery_days == 7

    def test_delivery_days_not_recalculated_when_delivery_date_missing(self):
        """delivery_date 缺少時，delivery_days 維持原值（不計算）。"""
        ods = make_ods(order_date=date(2024, 1, 1), delivery_date=None, delivery_days=5)
        result = format_clean(ods)
        assert result.delivery_days == 5

    def test_none_optional_fields_do_not_raise(self):
        """所有可選欄位都是 None 時，不應觸發 AttributeError。"""
        ods = make_ods()  # 只有必填欄位
        result = format_clean(ods)  # 不應拋出例外
        assert result.gender is None
        assert result.ship_mode is None
        assert result.delivery_days is None


# ─── business_clean ───────────────────────────────────────────────────────────

class TestBusinessClean:

    def test_valid_data_produces_no_errors(self):
        ods = make_ods(
            items=[{"product": {"product_id": "P1"}, "quantity": 2, "unit_price": 50.0, "discount_pct": 10.0}],
            tax_pct=5.0,
            order_date=date(2024, 1, 1),
            delivery_date=date(2024, 1, 8),
            customer_rating=4.5,
            age=30,
        )
        _, errors = business_clean(ods)
        assert errors == []

    # ── items 欄位驗證 ────────────────────────────────────────────────────────

    def test_quantity_zero_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 0, "unit_price": 50.0}])
        _, errors = business_clean(ods)
        assert any("quantity" in e for e in errors)

    def test_quantity_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": -1, "unit_price": 50.0}])
        _, errors = business_clean(ods)
        assert any("quantity" in e for e in errors)

    def test_unit_price_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": -10.0}])
        _, errors = business_clean(ods)
        assert any("unit_price" in e for e in errors)

    def test_unit_price_zero_is_valid(self):
        """unit_price = 0 合法（免費商品）。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 0.0}])
        _, errors = business_clean(ods)
        assert not any("unit_price" in e for e in errors)

    def test_discount_pct_above_100_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": 101.0}])
        _, errors = business_clean(ods)
        assert any("discount_pct" in e for e in errors)

    def test_discount_pct_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": -1.0}])
        _, errors = business_clean(ods)
        assert any("discount_pct" in e for e in errors)

    @pytest.mark.parametrize("pct", [0.0, 100.0])
    def test_discount_pct_boundaries_are_valid(self, pct):
        """邊界值 0 和 100 都合法。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": pct}])
        _, errors = business_clean(ods)
        assert not any("discount_pct" in e for e in errors)

    # ── 訂單層級驗證 ──────────────────────────────────────────────────────────

    def test_tax_pct_above_100_is_invalid(self):
        ods = make_ods(tax_pct=101.0)
        _, errors = business_clean(ods)
        assert any("tax_pct" in e for e in errors)

    def test_tax_pct_negative_is_invalid(self):
        ods = make_ods(tax_pct=-1.0)
        _, errors = business_clean(ods)
        assert any("tax_pct" in e for e in errors)

    def test_delivery_date_before_order_date_is_invalid(self):
        ods = make_ods(
            order_date=date(2024, 1, 10),
            delivery_date=date(2024, 1, 5),
        )
        _, errors = business_clean(ods)
        assert any("delivery_date" in e for e in errors)

    def test_delivery_date_same_as_order_date_is_valid(self):
        """delivery_date == order_date 合法（當日出貨）。"""
        ods = make_ods(
            order_date=date(2024, 1, 1),
            delivery_date=date(2024, 1, 1),
        )
        _, errors = business_clean(ods)
        assert not any("delivery_date" in e for e in errors)

    @pytest.mark.parametrize("rating", [0.9, 5.1])
    def test_customer_rating_out_of_range_is_invalid(self, rating):
        ods = make_ods(customer_rating=rating)
        _, errors = business_clean(ods)
        assert any("customer_rating" in e for e in errors)

    @pytest.mark.parametrize("rating", [1.0, 5.0])
    def test_customer_rating_boundaries_are_valid(self, rating):
        ods = make_ods(customer_rating=rating)
        _, errors = business_clean(ods)
        assert not any("customer_rating" in e for e in errors)

    @pytest.mark.parametrize("age", [-1, 121])
    def test_age_out_of_range_is_invalid(self, age):
        ods = make_ods(age=age)
        _, errors = business_clean(ods)
        assert any("age" in e for e in errors)

    @pytest.mark.parametrize("age", [0, 120])
    def test_age_boundaries_are_valid(self, age):
        ods = make_ods(age=age)
        _, errors = business_clean(ods)
        assert not any("age" in e for e in errors)

    def test_multiple_violations_all_accumulated(self):
        """多個規則同時違反，全部錯誤都要被累積，不提前 return。"""
        ods = make_ods(
            items=[
                {"product": {"product_id": "P1"}, "quantity": 0, "unit_price": -1.0},
            ],
            customer_rating=10.0,
            age=200,
        )
        _, errors = business_clean(ods)
        # quantity + unit_price + customer_rating + age = 4 個錯誤
        assert len(errors) >= 4

    def test_none_items_produces_no_error(self):
        """items = None 時不應拋出例外，也不產生錯誤。"""
        ods = make_ods(items=None)
        _, errors = business_clean(ods)
        assert errors == []


# ─── clean_order ──────────────────────────────────────────────────────────────

class TestCleanOrder:

    def test_returns_three_tuple(self):
        result = clean_order(make_ods())
        assert isinstance(result, tuple) and len(result) == 3

    def test_clean_data_has_no_error_flag(self):
        ods = make_ods(gender="Male", customer_rating=4.0)
        _, has_error, msg = clean_order(ods)
        assert has_error is False
        assert msg is None

    def test_dirty_data_sets_error_flag_and_message(self):
        ods = make_ods(customer_rating=99.0)
        _, has_error, msg = clean_order(ods)
        assert has_error is True
        assert msg is not None
        assert "customer_rating" in msg

    def test_format_and_business_both_applied(self):
        """
        clean_order 必須同時執行 format_clean 和 business_clean。
        用一個 ods 同時帶有 format 問題和 business 問題來驗證兩者都有套用。
        """
        ods = make_ods(
            gender="  MALE  ",      # format_clean 應處理 → "male"
            customer_rating=99.0,  # business_clean 應標記
        )
        result_ods, has_error, _ = clean_order(ods)
        assert result_ods.gender == "male"  # format_clean 有套用
        assert has_error is True            # business_clean 有套用
