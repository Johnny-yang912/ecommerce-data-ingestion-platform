"""
clean.py 單元測試（新補）

format_clean : 字串正規化（lowercase / strip）、delivery_days 計算
business_clean: 業務規則驗證，每條規則獨立測試，多錯誤同時累積
clean_order   : 整合路徑（format + business 都有被執行）

這些是純函式，不需要 mock，是最容易加的測試種類。
"""

import pytest
from datetime import date, datetime, timezone, timedelta
from schema import ODSOrder
import typing
from clean import (
    format_clean, business_clean, clean_order,
    detect_schema_drift, DriftCode, _expected_kind, _type_compatible,
    DQCode, NON_REPRODUCIBLE_CODES, AGE_MIN, AGE_MAX,
)


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

    # ── sentinel 假空值正規化（#11）────────────────────────────────────────────

    def test_sentinel_values_normalized_to_none(self):
        """已知假空值（含被 strip 成空字串）→ None。"""
        ods = make_ods(country="N/A", city="null", gender="NONE", order_status="")
        result = format_clean(ods)
        assert result.country is None
        assert result.city is None
        assert result.gender is None
        assert result.order_status is None

    def test_conservative_set_preserves_ambiguous_values(self):
        """保守集合：'NA'（North America）/'-' 不應被誤殺。"""
        ods = make_ods(region="NA", state="-")
        result = format_clean(ods)
        assert result.region == "NA"
        assert result.state == "-"


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
        assert any(e["field"] == "quantity" for e in errors)

    def test_quantity_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": -1, "unit_price": 50.0}])
        _, errors = business_clean(ods)
        assert any(e["field"] == "quantity" for e in errors)

    def test_unit_price_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": -10.0}])
        _, errors = business_clean(ods)
        assert any(e["field"] == "unit_price" for e in errors)

    def test_unit_price_zero_is_valid(self):
        """unit_price = 0 合法（免費商品）。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 0.0}])
        _, errors = business_clean(ods)
        assert not any(e["field"] == "unit_price" for e in errors)

    def test_discount_pct_above_100_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": 101.0}])
        _, errors = business_clean(ods)
        assert any(e["field"] == "discount_pct" for e in errors)

    def test_discount_pct_negative_is_invalid(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": -1.0}])
        _, errors = business_clean(ods)
        assert any(e["field"] == "discount_pct" for e in errors)

    @pytest.mark.parametrize("pct", [0.0, 100.0])
    def test_discount_pct_boundaries_are_valid(self, pct):
        """邊界值 0 和 100 都合法。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 50.0, "discount_pct": pct}])
        _, errors = business_clean(ods)
        assert not any(e["field"] == "discount_pct" for e in errors)

    # ── 訂單層級驗證 ──────────────────────────────────────────────────────────

    def test_tax_pct_above_100_is_invalid(self):
        ods = make_ods(tax_pct=101.0)
        _, errors = business_clean(ods)
        assert any(e["field"] == "tax_pct" for e in errors)

    def test_tax_pct_negative_is_invalid(self):
        ods = make_ods(tax_pct=-1.0)
        _, errors = business_clean(ods)
        assert any(e["field"] == "tax_pct" for e in errors)

    def test_delivery_date_before_order_date_is_invalid(self):
        ods = make_ods(
            order_date=date(2024, 1, 10),
            delivery_date=date(2024, 1, 5),
        )
        _, errors = business_clean(ods)
        assert any(e["field"] == "delivery_date" for e in errors)

    def test_delivery_date_same_as_order_date_is_valid(self):
        """delivery_date == order_date 合法（當日出貨）。"""
        ods = make_ods(
            order_date=date(2024, 1, 1),
            delivery_date=date(2024, 1, 1),
        )
        _, errors = business_clean(ods)
        assert not any(e["field"] == "delivery_date" for e in errors)

    @pytest.mark.parametrize("rating", [0.9, 5.1])
    def test_customer_rating_out_of_range_is_invalid(self, rating):
        ods = make_ods(customer_rating=rating)
        _, errors = business_clean(ods)
        assert any(e["field"] == "customer_rating" for e in errors)

    @pytest.mark.parametrize("rating", [1.0, 5.0])
    def test_customer_rating_boundaries_are_valid(self, rating):
        ods = make_ods(customer_rating=rating)
        _, errors = business_clean(ods)
        assert not any(e["field"] == "customer_rating" for e in errors)

    @pytest.mark.parametrize("age", [-1, 131])
    def test_age_out_of_range_is_invalid(self, age):
        ods = make_ods(age=age)
        _, errors = business_clean(ods)
        assert any(e["field"] == "age" for e in errors)

    @pytest.mark.parametrize("age", [AGE_MIN, AGE_MAX])
    def test_age_boundaries_are_valid(self, age):
        ods = make_ods(age=age)
        _, errors = business_clean(ods)
        assert not any(e["field"] == "age" for e in errors)

    @pytest.mark.parametrize("age", [121, 125, 130])
    def test_v3_loosening_makes_the_old_band_valid(self, age):
        """v3 把上限從 120 放寬到 130。這一段區間在 v2 是髒的、在 v3 是乾淨的——
        它就是 Proposal B 回溯重評估第一次真的有東西可 promote 的來源
        （`seed_demo._dirty_age_out_of_range` 會注入 125）。"""
        _, errors = business_clean(make_ods(age=age))
        assert not any(e["field"] == "age" for e in errors)

    # ── 自由文字欄位軟性長度上限（2b）────────────────────────────────────────

    def test_customer_name_over_soft_limit_is_flagged(self):
        """customer_name 超過軟性上限（100）→ 標記 field_too_long，但資料仍落地。"""
        ods = make_ods(customer_name="x" * 101)
        _, errors = business_clean(ods)
        assert any(e["field"] == "customer_name" and e["code"] == "field_too_long" for e in errors)

    def test_customer_name_at_soft_limit_is_valid(self):
        """customer_name 等於軟性上限（100）→ 不標記。"""
        ods = make_ods(customer_name="x" * 100)
        _, errors = business_clean(ods)
        assert not any(e["field"] == "customer_name" for e in errors)

    def test_city_over_soft_limit_is_flagged(self):
        """city 超過軟性上限（80）→ 標記 field_too_long。"""
        ods = make_ods(city="x" * 81)
        _, errors = business_clean(ods)
        assert any(e["field"] == "city" and e["code"] == "field_too_long" for e in errors)

    # ── 未來日期防線（#15）────────────────────────────────────────────────────

    def test_future_order_date_is_flagged(self):
        ods = make_ods(order_date=date(2999, 1, 1))
        _, errors = business_clean(ods)
        assert any(e["code"] == "order_date_in_future" for e in errors)

    def test_order_date_within_tolerance_not_flagged(self):
        """今天 +1 天落在容差內，不標記。"""
        today = datetime.now(timezone.utc).date()
        ods = make_ods(order_date=today + timedelta(days=1))
        _, errors = business_clean(ods)
        assert not any(e["code"] == "order_date_in_future" for e in errors)

    def test_order_date_beyond_tolerance_is_flagged(self):
        today = datetime.now(timezone.utc).date()
        ods = make_ods(order_date=today + timedelta(days=2))
        _, errors = business_clean(ods)
        assert any(e["code"] == "order_date_in_future" for e in errors)

    # ── NaN / Infinity（#14）：非有限值只報一次 non_finite_number ──────────────

    def test_non_finite_item_unit_price_flagged_once(self):
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": float("nan")}])
        result, errors = business_clean(ods)
        codes = [e["code"] for e in errors]
        assert codes.count("non_finite_number") == 1
        assert "unit_price_negative" not in codes  # 不重複報
        # sanitize：非有限值正規化為 None，讓含 NaN 的 items 能寫進 JSONB
        assert result.items[0]["unit_price"] is None

    def test_negative_infinity_unit_price_only_non_finite(self):
        """-inf 雖 < 0，但有 isfinite 守衛 → 只報 non_finite，不報 unit_price_negative。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": float("-inf")}])
        _, errors = business_clean(ods)
        codes = [e["code"] for e in errors]
        assert "non_finite_number" in codes
        assert "unit_price_negative" not in codes

    def test_non_finite_tax_pct_only_non_finite(self):
        ods = make_ods(tax_pct=float("inf"))
        result, errors = business_clean(ods)
        codes = [e["code"] for e in errors]
        assert "non_finite_number" in codes
        assert "tax_pct_out_of_range" not in codes
        assert result.tax_pct is None  # sanitize

    def test_non_finite_customer_rating_only_non_finite(self):
        ods = make_ods(customer_rating=float("nan"))
        result, errors = business_clean(ods)
        codes = [e["code"] for e in errors]
        assert "non_finite_number" in codes
        assert "customer_rating_out_of_range" not in codes
        assert result.customer_rating is None  # sanitize

    def test_string_item_numeric_does_not_crash(self):
        """items 內數值送成字串（型別漂移由 B 偵測）→ business_clean 不崩潰、不誤報範圍違規。"""
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "discount_pct": "10"}])
        _, errors = business_clean(ods)  # 不應拋 TypeError
        assert not any(e["code"] == "discount_pct_out_of_range" for e in errors)

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


# ─── 重評估可重現性（Proposal B 前置）────────────────────────────────────────
#
# Proposal B 用【新版規則重跑 business_clean】決定要不要 promote。這只有在「同一筆
# ODS 值、同一版規則 → 同一個判定」時才成立。有兩類規則會破壞這個前提：
#   ① 時間相依（判定基準是 wall clock）→ 由 as_of 參數修好，本節前半驗證
#   ② 標記時把值正規化掉（證據消失）→ 無法修，只能排除，收在 NON_REPRODUCIBLE_CODES
# 兩者若不處理，後果相同且嚴重：規則一個字都沒放寬，資料卻自己流回 Gold（偽 promote）。

class TestReevaluationReproducibility:

    # ── ①：as_of 讓時間相依規則可重現 ────────────────────────────────────────

    def test_as_of_defaults_to_wall_clock(self):
        """不傳 as_of → 維持既有行為（回歸保護：攝入路徑一個字都不能變）。"""
        today = datetime.now(timezone.utc).date()
        _, errors = business_clean(make_ods(order_date=today + timedelta(days=2)))
        assert any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in errors)

    def test_as_of_overrides_wall_clock(self):
        """order_date 相對「今天」是過去、相對 as_of 是未來 → 以 as_of 為準被標記。"""
        as_of = datetime.now(timezone.utc).date() - timedelta(days=200)
        ods = make_ods(order_date=as_of + timedelta(days=10))
        _, errors = business_clean(ods, as_of=as_of)
        assert any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in errors)

    def test_as_of_accepts_datetime_and_normalizes_to_utc(self):
        """傳 tz-aware datetime → 先轉 UTC 再取日期。

        本例的 UTC 日期（01-02）與當地日期（01-01）不同：若誤用當地日期，
        cutoff 會少一天而把 01-03 誤標為未來日期。
        """
        as_of = datetime(2026, 1, 1, 23, 0, tzinfo=timezone(timedelta(hours=-8)))  # = 01-02 07:00 UTC
        _, errors = business_clean(make_ods(order_date=date(2026, 1, 3)), as_of=as_of)
        assert not any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in errors)

        _, errors = business_clean(make_ods(order_date=date(2026, 1, 4)), as_of=as_of)
        assert any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in errors)

    def test_future_date_verdict_is_reproducible_with_as_of(self):
        """核心案例：攝入當下被標記為未來日期的訂單，日後以 received_at 重評估仍成立。

        對照組（不傳 as_of）示範沒有這個參數時會發生什麼——同一筆資料憑空通過。
        """
        received_at = datetime.now(timezone.utc) - timedelta(days=200)
        order_date = (received_at + timedelta(days=10)).date()  # 相對攝入是未來、相對今天是過去

        _, at_ingest = business_clean(make_ods(order_date=order_date), as_of=received_at)
        assert any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in at_ingest)

        _, wall_clock = business_clean(make_ods(order_date=order_date))
        assert not any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in wall_clock)  # ← 偽 promote 的來源

        _, replayed = business_clean(make_ods(order_date=order_date), as_of=received_at)
        assert [e["code"] for e in replayed] == [e["code"] for e in at_ingest]

    # ── ②：值被正規化掉的碼，判定無法重現 ────────────────────────────────────

    def test_non_finite_verdict_is_not_reproducible(self):
        """NON_FINITE_NUMBER 標記的同時把值設成 None → 重跑時錯誤憑空消失。

        這正是它被列入 NON_REPRODUCIBLE_CODES 的原因：Proposal B 不得據此自動 promote，
        因為那個「通過」來自證據消失，不是規則放寬。原始值只在 Raw（→ Proposal C）。
        """
        ods = make_ods(items=[{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": float("nan")}])
        cleaned, first = business_clean(ods)
        assert [e["code"] for e in first] == [DQCode.NON_FINITE_NUMBER]

        _, second = business_clean(cleaned)   # 重評估：輸入已是被正規化後的值
        assert second == []

    def test_value_preserving_verdict_survives_reevaluation(self):
        """對照組：不改值的規則重跑仍成立（絕大多數碼屬於這類）。"""
        ods = make_ods(age=200)
        cleaned, first = business_clean(ods)
        _, second = business_clean(cleaned)
        assert [e["code"] for e in first] == [e["code"] for e in second] == [DQCode.AGE_OUT_OF_RANGE]

    def test_non_reproducible_codes_matches_observed_behaviour(self):
        """清單內容必須與上面兩支測到的實際行為一致（改規則時這裡要一起改）。"""
        assert DQCode.NON_FINITE_NUMBER in NON_REPRODUCIBLE_CODES
        assert DQCode.AGE_OUT_OF_RANGE not in NON_REPRODUCIBLE_CODES
        assert DQCode.ORDER_DATE_IN_FUTURE not in NON_REPRODUCIBLE_CODES  # 已由 as_of 修成可重現


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
        assert isinstance(msg, list) and len(msg) > 0
        assert any(e["field"] == "customer_rating" for e in msg)

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

    def test_as_of_is_passed_through_to_business_clean(self):
        """as_of 必須原樣透傳——Proposal C 從 Raw 重產值時重用的正是這條路徑，
        不透傳的話重建結果會與攝入當下不一致（DQ C-2 #3）。"""
        as_of = datetime.now(timezone.utc) - timedelta(days=200)
        order_date = (as_of + timedelta(days=10)).date()

        _, has_error, msg = clean_order(make_ods(order_date=order_date), as_of=as_of)
        assert has_error is True
        assert any(e["code"] == DQCode.ORDER_DATE_IN_FUTURE for e in msg)

        _, has_error_now, _ = clean_order(make_ods(order_date=order_date))
        assert has_error_now is False   # 同一筆、不傳 as_of → 判定不同


# ─── detect_schema_drift ────────────────────────────────────────────────────────

CLEAN_PAYLOAD = {
    "order_id": "A", "order_date": "2026-01-01",
    "customer": {"customer_id": "C1"}, "address": {},
    "items": [{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 9.9}],
    "payment": {"tax_pct": 5.0},
}


def _codes(messages):
    return [m["code"] for m in messages]


def _fields(messages):
    return [m["field"] for m in messages]


class TestDetectSchemaDrift:

    def test_clean_payload_has_no_drift(self):
        has, msg, unmapped = detect_schema_drift(CLEAN_PAYLOAD)
        assert has is False
        assert msg is None
        assert unmapped is None

    def test_extra_field_at_root_is_flagged_and_captured(self):
        d = {**CLEAN_PAYLOAD, "loyalty_points": 99}
        has, msg, unmapped = detect_schema_drift(d)
        assert has is True
        assert "loyalty_points" in _fields(msg)
        assert unmapped["loyalty_points"] == 99
        assert all(c == DriftCode.UNEXPECTED_FIELD for c in _codes(msg))

    def test_extra_field_in_nested_group_uses_dotted_path(self):
        d = {**CLEAN_PAYLOAD, "customer": {"customer_id": "C1", "sex": "X"}}
        has, msg, unmapped = detect_schema_drift(d)
        assert "customer.sex" in _fields(msg)
        assert unmapped["customer.sex"] == "X"

    def test_extra_field_in_item_uses_indexed_path(self):
        d = {**CLEAN_PAYLOAD, "items": [{"product": {"product_id": "P1"}, "quantity": 1, "unit_price": 9.9, "gift_wrap": True}]}
        has, msg, unmapped = detect_schema_drift(d)
        assert "items[0].gift_wrap" in _fields(msg)

    def test_type_drift_string_where_integer_expected(self):
        """age 送字串（Pydantic 會靜默強轉）→ 在原始 payload 偵測到型別漂移。"""
        d = {**CLEAN_PAYLOAD, "customer": {"customer_id": "C1", "age": "30"}}
        has, msg, _ = detect_schema_drift(d)
        drift = [m for m in msg if m["code"] == DriftCode.TYPE_DRIFT]
        assert any(m["field"] == "customer.age" and m["expected"] == "integer" and m["actual"] == "string" for m in drift)

    def test_type_drift_integer_where_boolean_expected(self):
        d = {**CLEAN_PAYLOAD, "returned": 1}
        has, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.TYPE_DRIFT and m["field"] == "returned" for m in msg)

    def test_integer_for_float_field_is_not_drift(self):
        """int 進 float 欄位（tax_pct=8）是合法 JSON 表示，不應誤報。"""
        d = {**CLEAN_PAYLOAD, "payment": {"tax_pct": 8}}
        has, _, _ = detect_schema_drift(d)
        assert has is False

    def test_null_value_is_not_drift(self):
        d = {**CLEAN_PAYLOAD, "ship_mode": None}
        has, _, _ = detect_schema_drift(d)
        assert has is False

    def test_non_object_nested_group_is_flagged(self):
        """customer 送成字串（#10）→ 標記 non_object_group，不崩潰。"""
        d = {**CLEAN_PAYLOAD, "customer": "just-a-string"}
        has, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.NON_OBJECT_GROUP and m["field"] == "customer" for m in msg)

    def test_non_object_item_element_is_flagged(self):
        d = {**CLEAN_PAYLOAD, "items": ["not-a-dict"]}
        has, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.NON_OBJECT_GROUP and m["field"] == "items[0]" for m in msg)

    def test_items_not_a_list_is_flagged(self):
        d = {**CLEAN_PAYLOAD, "items": {"product": {"product_id": "P1"}}}
        has, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.NON_OBJECT_GROUP and m["field"] == "items" for m in msg)

    def test_non_dict_payload_returns_no_drift(self):
        has, msg, unmapped = detect_schema_drift("not-a-dict")
        assert has is False and msg is None and unmapped is None

    def test_scalar_boolean_value_is_type_drift(self):
        """ship_mode 期望字串，送 bool → type drift（actual=boolean）。"""
        d = {**CLEAN_PAYLOAD, "ship_mode": True}
        _, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.TYPE_DRIFT and m["field"] == "ship_mode" and m["actual"] == "boolean" for m in msg)

    def test_scalar_array_value_is_type_drift(self):
        """order_status 期望字串，送陣列 → type drift（actual=array）。"""
        d = {**CLEAN_PAYLOAD, "order_status": ["a"]}
        _, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.TYPE_DRIFT and m["field"] == "order_status" and m["actual"] == "array" for m in msg)

    def test_null_item_element_is_non_object(self):
        """items 內含 None 元素 → non_object_group（actual=null）。"""
        d = {**CLEAN_PAYLOAD, "items": [None]}
        _, msg, _ = detect_schema_drift(d)
        assert any(m["code"] == DriftCode.NON_OBJECT_GROUP and m["field"] == "items[0]" and m["actual"] == "null" for m in msg)


class TestDriftHelpersDefensive:
    """直接覆蓋契約目前不會產生、但保留作未來防禦的分支。"""

    def test_unknown_annotation_maps_to_other(self):
        assert _expected_kind(typing.Any) == ("other", None)

    def test_unknown_expected_kind_is_compatible(self):
        assert _type_compatible("date", "string") is True
