"""`seed_demo.strip_optional_costs` 的不變式測試。

**為什麼 seed_demo 不在覆蓋門檻裡卻還是有這支測試**：pytest.ini 的排除理由是
「不可逆性」——seeding 寫錯重跑就好。那個理由管的是【覆蓋率要求】，不是
「這個檔沒有值得保護的契約」。

這裡保護的是一條**跨模組**的不變式：`strip_optional_costs` 產生的資料
【必須不是髒資料】。它靠的是 clean.py 目前不對 cost_price / shipping_fee 的
None 做任何檢查（schema.py 兩者皆為 `Optional[float] = None`）。那是別的模組的
行為，隨時可能被改——而一旦改了，這裡的失敗是**靜默**的：

  - seed_demo 的 expected_codes 對帳表會永遠差一項，且看不出差在哪
  - rpt_sales_daily_by_category 的 items_missing_* 會與品質違規混在一起
  - 「上游不完整」與「上游給了違規值」這兩個不同層級的訊號就此合流

所以這支測試的意義和 test_dags.py 的 `test_no_import_errors` 同構：
**把一個原本會靜默發生的破壞，變成一盞紅燈。**
"""
import random

import pytest

from clean import business_clean, detect_schema_drift, format_clean
from schema import ODSOrder, OrderIN
from seed_demo import (
    DIRTY_WEIGHTS,
    OPTIONAL_COST_FIELDS,
    make_order,
    strip_optional_costs,
)


def _clean_path(payload: dict):
    """把 payload 走完真實的攝入判定路徑，回傳 (has_drift, drift_msgs, dq_errors)。"""
    drift, msgs, _ = detect_schema_drift(payload)
    ods = ODSOrder(**OrderIN(**payload).model_dump(), raw_id=1)
    _, errors = business_clean(format_clean(ods))
    return drift, msgs, errors


class TestStripOptionalCostsIsNotDirtyData:

    def test_produces_no_dq_error(self):
        """⭐ 核心不變式：拿掉選填成本欄位【不得】觸發任何 DQCode。

        這是整個設計的前提——它是「上游給的資料本來就不完整」，不是違規。
        這條紅了代表 clean.py 新增了 cost_price / shipping_fee 的規則，
        屆時必須重新決定這個注入器該留在哪一層（見 seed_demo.py 該節註解）。
        """
        rng = random.Random(3)
        payload = make_order(rng, "T-0001", 45)
        strip_optional_costs(payload, rng, 1.0)          # 全部拿掉，確保打得到

        _, _, errors = _clean_path(payload)
        assert errors == [], f"選填成本欄位缺漏不該產生 DQCode，實得：{errors}"

    def test_produces_no_schema_drift(self):
        """JSON null 不得被誤判成契約漂移。

        注入器寫的是 `null`，而 2026-07-08 那批舊 demo 資料是【欄位整個不存在】。
        兩種形狀在 dbt 的 json_value() 下都回 SQL NULL，但漂移偵測是另一條路徑，
        必須各自確認——否則會產出一批 has_schema_drift=True 的資料而不自知。
        """
        rng = random.Random(3)
        payload = make_order(rng, "T-0001", 45)
        strip_optional_costs(payload, rng, 1.0)

        drift, msgs, _ = _clean_path(payload)
        assert drift is False, f"不該觸發 schema drift，實得訊息：{msgs}"


class TestStripOptionalCostsShape:

    def test_fields_are_drawn_independently(self):
        """⭐ 兩個欄位必須各自獨立抽，不能綁在一起。

        這正是要修正舊 demo 資料形狀的地方：2026-07-08 那 250 筆是 cost 與
        shipping 永遠同時缺，於是 rpt_sales_daily_by_category 的
        items_missing_cost 與 items_missing_shipping 在報表上【恆等】——
        兩個 counter 看起來像同一個訊號的複本，分開設計的意義完全看不出來。

        斷言「只缺其中一個」的兩格都非空，就是在保證這兩個 counter 真的會分岔。
        """
        rng = random.Random(7)
        only_cost = only_ship = 0
        for i in range(300):
            payload = make_order(rng, f"T-{i:04d}", 45)
            strip_optional_costs(payload, rng, 0.3)
            for item in payload["items"]:
                mc = item["cost_price"] is None
                ms = item["shipping_fee"] is None
                only_cost += mc and not ms
                only_ship += ms and not mc

        assert only_cost > 0, "沒有任何『只缺 cost_price』的 item——兩欄位被綁在一起了"
        assert only_ship > 0, "沒有任何『只缺 shipping_fee』的 item——兩欄位被綁在一起了"

    @pytest.mark.parametrize("field", OPTIONAL_COST_FIELDS)
    def test_rate_one_strips_everything(self, field):
        rng = random.Random(11)
        payload = make_order(rng, "T-0001", 45)
        strip_optional_costs(payload, rng, 1.0)
        assert all(item[field] is None for item in payload["items"])

    def test_rate_zero_is_a_complete_no_op(self):
        """預設值以外的既有行為不得改變：rate=0 必須連一個欄位都不碰。

        `--missing-cost-rate 0` 是「我要一批完全乾淨的對照組」的唯一手段。
        """
        rng = random.Random(11)
        payload = make_order(rng, "T-0001", 45)
        before = [dict(item) for item in payload["items"]]

        stripped = strip_optional_costs(payload, rng, 0.0)

        assert stripped == {}
        assert payload["items"] == before

    def test_does_not_touch_fields_owned_by_dirty_injectors(self):
        """正交性：本注入器只碰 cost_price / shipping_fee。

        DIRTY_WEIGHTS 裡的注入器負責 quantity / unit_price / discount_pct 等欄位
        （_dirty_non_finite_number 只打那三個）。兩邊碰同一個欄位的話，
        expected_codes 的對帳就會受 --missing-cost-rate 影響而不可解釋。
        """
        rng = random.Random(5)
        payload = make_order(rng, "T-0001", 45)
        untouched = ("quantity", "unit_price", "discount_pct")
        before = [{k: item[k] for k in untouched} for item in payload["items"]]

        strip_optional_costs(payload, rng, 1.0)

        assert [{k: item[k] for k in untouched} for item in payload["items"]] == before

    def test_returns_counts_matching_the_payload(self):
        """回傳的統計必須與 payload 實際狀態一致——run() 的總結報告靠它。"""
        rng = random.Random(13)
        payload = make_order(rng, "T-0001", 45)
        stripped = strip_optional_costs(payload, rng, 0.5)

        for field in OPTIONAL_COST_FIELDS:
            actual = sum(item[field] is None for item in payload["items"])
            assert stripped[field] == actual


def test_optional_cost_fields_are_not_dirty_injector_targets():
    """`OPTIONAL_COST_FIELDS` 不得與髒資料注入器的目標欄位重疊。

    以函式原始碼比對是刻意的粗略做法——它抓的是「有人在 DIRTY_WEIGHTS 新增了
    碰 cost_price 的注入器」這件事，而那正是會讓兩個軸靜默糾纏的改動。
    """
    import inspect

    for func, _weight in DIRTY_WEIGHTS:
        source = inspect.getsource(func)
        for field in OPTIONAL_COST_FIELDS:
            assert f'"{field}"' not in source, (
                f"{func.__name__} 碰了 {field}——該欄位屬於 strip_optional_costs 的職責，"
                f"兩邊都寫會讓 expected_codes 對帳失去意義"
            )
