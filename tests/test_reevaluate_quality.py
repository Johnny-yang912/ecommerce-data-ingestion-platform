"""reevaluate_quality.py（Proposal B 事件產生端）

這支程式是專案裡唯一會寫入 **append-only 稽核表** 的自動化程式：寫錯刪不掉，
且它的產出會直接決定資料流不流回 Gold。故測試重點放在「不該寫的時候有沒有寫」，
而不只是「該寫的時候有沒有寫」。

四組：
  1. 狀態轉移矩陣 —— 冪等與狀態機每條邊的可達性都收斂在 decide_target_state()
  2. 反序列化保真 —— 重評估看到的值必須與攝入當下逐字相同，否則評的不是同一筆資料
  3. 規劃 —— as_of 有沒有真的傳下去、不可重現碼有沒有擋住、事件欄位對不對
  4. CLI 閘門 —— 預設 dry-run、版本斷言、缺設定 fail-fast

不觸網、不連 DB：BQ client 與 SessionLocal 都被 patch 掉。
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import reevaluate_quality as rq
from clean import DQCode, DQ_RULE_VERSION, business_clean, clean_order
from schema import ODSOrder


# ─── Helper：模擬一列 BQ 讀回的候選 ───────────────────────────────────────────

def make_row(**overrides) -> dict:
    """BQ Row 支援 row["col"]，dict 也是——測試用 dict 即可。

    型別刻意比照 BQ client 讀回來的樣子：TIMESTAMP → tz-aware datetime、
    DATE → date、JSON → 已解析的物件。
    """
    row = {name: None for name in rq.ODS_FIELDS}
    row.update({
        "raw_id": 1,
        "received_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "clean_error_message": None,
        "order_id": "ORD-001",
        "order_date": date(2026, 1, 1),
        "customer_id": "CUST-001",
    })
    row.update(overrides)
    return row


def codes(*names) -> list:
    return [{"code": n, "field": "x"} for n in names]


# ─── 1. 狀態轉移矩陣 ──────────────────────────────────────────────────────────

class TestDecideTargetState:

    @pytest.mark.parametrize("current", [
        rq.STATE_QUARANTINED, rq.STATE_RE_QUARANTINED, None,
    ])
    def test_passing_gets_promoted(self, current):
        assert rq.decide_target_state(current, set(), []) == rq.STATE_PROMOTED

    @pytest.mark.parametrize("current", [
        rq.STATE_QUARANTINED, rq.STATE_RE_QUARANTINED, None,
    ])
    def test_still_failing_writes_nothing(self, current):
        """狀態沒變就不寫——這一條就是冪等的全部來源，不需要額外的狀態容器。"""
        assert rq.decide_target_state(current, set(), [{"code": "x"}]) is None

    def test_promoted_still_passing_writes_nothing(self):
        """重跑不得再 append 一次 promotion，否則 rpt_ 的 promotions 會被灌水，
        而那是〈歷史指標為何不會被追溯性改寫〉要保護的數字，且 append-only 刪不掉。"""
        assert rq.decide_target_state(rq.STATE_PROMOTED, set(), []) is None

    def test_promoted_now_failing_is_demoted(self):
        """規則變嚴 → promoted 降級為 re_quarantined（狀態機的邊緣情況，必須可達）。"""
        assert rq.decide_target_state(
            rq.STATE_PROMOTED, set(), [{"code": "x"}]) == rq.STATE_RE_QUARANTINED

    @pytest.mark.parametrize("new_errors", [[], [{"code": "x"}]])
    def test_permanently_rejected_is_never_touched(self, new_errors):
        """人工的終局決定，自動任務不得推翻——不論新版規則說什麼。"""
        assert rq.decide_target_state(
            rq.STATE_PERMANENTLY_REJECTED, set(), new_errors) is None

    def test_non_reproducible_code_blocks_promotion(self):
        """新版規則「通過」了，但原判定含不可重現碼 → 那個通過來自證據消失，不是規則放寬。"""
        assert rq.decide_target_state(
            rq.STATE_QUARANTINED, {DQCode.NON_FINITE_NUMBER}, []) is None

    def test_reproducible_code_does_not_block_promotion(self):
        """對照組：一般錯誤碼不擋 promote。"""
        assert rq.decide_target_state(
            rq.STATE_QUARANTINED, {DQCode.AGE_OUT_OF_RANGE}, []) == rq.STATE_PROMOTED

    def test_every_target_has_an_event_type(self):
        """新增目標狀態時，忘了配事件類型會在 plan_events 以 KeyError 炸掉——先在這裡擋。"""
        for state in (rq.STATE_PROMOTED, rq.STATE_RE_QUARANTINED):
            assert state in rq.EVENT_TYPE_BY_TARGET
        assert rq.STATE_PERMANENTLY_REJECTED not in rq.EVENT_TYPE_BY_TARGET


# ─── 2. 反序列化保真 ──────────────────────────────────────────────────────────

class TestDeserialization:

    def test_ods_fields_are_derived_from_the_schema(self):
        """欄位清單必須是推導來的，不是手寫的——手寫子集會讓未來的新規則靜默對 NULL 評估。"""
        assert set(rq.ODS_FIELDS) == set(ODSOrder.model_fields)

    def test_round_trip_preserves_every_field(self):
        """ODSOrder → 模擬 BQ 列 → 反序列化，逐欄位必須相同。"""
        original = ODSOrder(
            order_id="ORD-9", order_date=date(2026, 3, 4), customer_id="C9",
            customer_name="alice", age=30, tax_pct=5.0, customer_rating=4.5,
            city="taipei", returned=False, delivery_date=date(2026, 3, 6),
            items=[{"product": {"product_id": "P1"}, "quantity": 2, "unit_price": 9.5}],
        )
        row = make_row(**{f: getattr(original, f) for f in rq.ODS_FIELDS})
        assert rq.to_ods_order(row).model_dump() == original.model_dump()

    def test_items_accepts_json_string(self):
        """BQ client 對 JSON 欄位的回傳型別隨版本而異；字串路徑必須也解得開。

        ODSOrder.items 宣告為 Any → Pydantic 不會攔下字串，塞進去會一路飄到
        business_clean 逐字元迭代才炸，錯誤現場離根因很遠。
        """
        row = make_row(items='[{"quantity": 1, "unit_price": 2.0}]')
        assert rq.to_ods_order(row).items == [{"quantity": 1, "unit_price": 2.0}]

    def test_reevaluation_sees_the_same_values_as_ingestion(self):
        """端到端保真：攝入當下的判定，經 BQ 來回一趟後必須重現。"""
        ingested, has_error, message = clean_order(
            ODSOrder(order_id="ORD-1", order_date=date(2026, 1, 1),
                     customer_id="C1", age=200),
            as_of=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        assert has_error is True

        row = make_row(clean_error_message=message,
                       **{f: getattr(ingested, f) for f in rq.ODS_FIELDS})
        _, replayed = business_clean(rq.to_ods_order(row), as_of=row["received_at"])
        assert [e["code"] for e in replayed] == [e["code"] for e in message]

    @pytest.mark.parametrize("message, expected", [
        (None, set()),
        ([], set()),
        (codes("a", "b", "a"), {"a", "b"}),
        ('[{"code": "a"}]', {"a"}),        # 字串形式
        ([{"no_code": 1}], {None}),        # 防禦：格式不如預期時不崩潰
    ])
    def test_error_codes_extraction(self, message, expected):
        assert rq.error_codes(message) == expected


# ─── 3. 規劃 ──────────────────────────────────────────────────────────────────

class TestPlanEvents:

    EVENT_AT = datetime(2026, 8, 1, tzinfo=timezone.utc)

    def test_promotion_event_fields(self):
        row = make_row(raw_id=7, order_id="ORD-7", age=30,
                       clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
        events, stats = plan = rq.plan_events(
            [row], {7: rq.STATE_QUARANTINED}, self.EVENT_AT)
        assert stats["promoted"] == 1 and len(events) == 1
        assert events[0] == {
            "raw_id": 7, "order_id": "ORD-7",
            "event_type": rq.EVENT_PROMOTION,
            "from_state": rq.STATE_QUARANTINED,
            "to_state": rq.STATE_PROMOTED,
            "rule_version": DQ_RULE_VERSION,
            "event_at": self.EVENT_AT,
            "reason": None,          # promote 時無殘留訊息
        }
        assert plan is not None

    def test_demotion_event_records_why(self):
        """降級事件的 reason 記下「現在為什麼不過」，供 RCA。"""
        row = make_row(raw_id=8, age=200, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
        events, stats = rq.plan_events([row], {8: rq.STATE_PROMOTED}, self.EVENT_AT)
        assert stats["re_quarantined"] == 1
        assert events[0]["event_type"] == rq.EVENT_RE_QUARANTINATION
        assert events[0]["to_state"] == rq.STATE_RE_QUARANTINED
        assert [e["code"] for e in events[0]["reason"]] == [DQCode.AGE_OUT_OF_RANGE]

    def test_unchanged_rows_produce_no_events(self):
        row = make_row(raw_id=9, age=200, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
        events, stats = rq.plan_events([row], {9: rq.STATE_QUARANTINED}, self.EVENT_AT)
        assert events == [] and stats["unchanged"] == 1

    def test_blocked_rows_are_counted_even_though_nothing_is_written(self):
        """『因證據消失而無法自動判定』必須是可見的數字，否則沒人知道有一批卡在 B 與 C 之間。"""
        row = make_row(raw_id=10, clean_error_message=codes(DQCode.NON_FINITE_NUMBER))
        events, stats = rq.plan_events([row], {10: rq.STATE_QUARANTINED}, self.EVENT_AT)
        assert events == []
        assert stats["blocked_non_reproducible"] == 1 and stats["unchanged"] == 1

    def test_as_of_is_taken_from_received_at_not_wall_clock(self):
        """⭐ 核心：時間相依規則以 received_at 為基準。

        用 wall clock 的話，這筆「攝入當下是未來日期」的訂單會憑空通過 → 偽 promote。
        """
        received_at = datetime.now(timezone.utc) - timedelta(days=200)
        row = make_row(
            raw_id=11,
            received_at=received_at,
            order_date=(received_at + timedelta(days=10)).date(),
            clean_error_message=codes(DQCode.ORDER_DATE_IN_FUTURE),
        )
        events, stats = rq.plan_events([row], {11: rq.STATE_QUARANTINED}, self.EVENT_AT)
        assert events == [] and stats["unchanged"] == 1

    def test_missing_state_promotes_with_null_from_state(self):
        """事件在 PG 缺席（異常，但 BQ 60 天過期時看得到）→ 仍要記錄轉移，否則流不回 Gold。"""
        row = make_row(raw_id=12, age=30, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
        events, _ = rq.plan_events([row], {}, self.EVENT_AT)
        assert events[0]["from_state"] is None
        assert events[0]["to_state"] == rq.STATE_PROMOTED

    def test_v3_age_loosening_promotes_the_boundary_band(self):
        """⭐ v3 的實際回流案例：age=125 在 v2（上限 120）被隔離，v3（上限 130）通過。

        `seed_demo._dirty_age_out_of_range` 會注入 125，所以這條路徑在真實資料上
        走得通——它是 docs/zh-TW/runbooks/proposal-b-rollout.md demo 劇本的自動化版本。
        """
        row = make_row(raw_id=125, age=125,
                       clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
        events, stats = rq.plan_events([row], {125: rq.STATE_QUARANTINED}, self.EVENT_AT)
        assert stats["promoted"] == 1
        assert events[0]["to_state"] == rq.STATE_PROMOTED
        assert events[0]["rule_version"] == DQ_RULE_VERSION

    def test_v3_loosening_leaves_the_far_out_of_range_quarantined(self):
        """對照組：同一次放寬**不**該把 -3 / 150 / 999 一起放進來。
        放寬是有邊界的，不是把整條規則關掉。"""
        rows = [make_row(raw_id=i, age=age,
                         clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE))
                for i, age in enumerate([-3, 150, 999])]
        states = {i: rq.STATE_QUARANTINED for i in range(3)}
        events, stats = rq.plan_events(rows, states, self.EVENT_AT)
        assert events == [] and stats["unchanged"] == 3

    def test_stats_account_for_every_candidate(self):
        """promoted + re_quarantined + unchanged 必須等於候選數，不能有列憑空消失。"""
        rows = [
            make_row(raw_id=1, age=30, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE)),
            make_row(raw_id=2, age=200, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE)),
            make_row(raw_id=3, age=200, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE)),
        ]
        states = {1: rq.STATE_QUARANTINED, 2: rq.STATE_PROMOTED, 3: rq.STATE_QUARANTINED}
        events, stats = rq.plan_events(rows, states, self.EVENT_AT)
        assert stats["candidates"] == 3
        assert stats["promoted"] + stats["re_quarantined"] + stats["unchanged"] == 3
        assert len(events) == stats["promoted"] + stats["re_quarantined"]


# ─── 4. 候選查詢與 CLI 閘門 ───────────────────────────────────────────────────

class TestCandidateSQL:

    def test_covers_both_int_models(self):
        """promoted 的列住在 int_orders，漏掉它就讓 promoted → re_quarantined 永遠不可達。"""
        sql = rq.candidate_sql()
        assert "int_orders_quarantine" in sql
        assert "UNION ALL" in sql
        assert sql.count("has_clean_error") == 2

    def test_excludes_permanently_rejected_as_fast_path(self):
        assert rq.STATE_PERMANENTLY_REJECTED in rq.candidate_sql()

    def test_selects_every_field_the_evaluator_needs(self):
        sql = rq.candidate_sql()
        for name in (*rq.META_FIELDS, *rq.ODS_FIELDS):
            assert f"`{name}`" in sql

    def test_limit_is_applied(self):
        assert "LIMIT 5" in rq.candidate_sql(5)
        assert "LIMIT" not in rq.candidate_sql()


class TestFetchCandidates:

    def test_returns_query_rows(self):
        client = MagicMock()
        client.query.return_value.result.return_value = iter([{"raw_id": 1}])
        assert rq.fetch_candidates(client, limit=3) == [{"raw_id": 1}]
        assert "LIMIT 3" in client.query.call_args[0][0]


# ─── 現況查詢與寫入（PG）──────────────────────────────────────────────────────

def _session_returning(*batches):
    """MagicMock session；每次 .all() 回傳下一批列。"""
    session = MagicMock()
    session.query.return_value.filter.return_value.order_by.return_value.all.side_effect = batches
    return session


class TestFetchCurrentStates:

    def test_empty_input_does_not_open_a_session(self):
        with patch("reevaluate_quality.SessionLocal",
                   side_effect=AssertionError("空輸入不該開 session")):
            assert rq.fetch_current_states([]) == {}

    def test_latest_event_wins(self):
        """升冪查詢、後寫覆蓋前寫 → 等價於 int_ 層的 `order by event_at desc, id desc`。
        兩邊決勝鍵若不一致，重評估會基於一個 Gold 看不到的狀態做決定。"""
        session = _session_returning([(1, rq.STATE_QUARANTINED), (1, rq.STATE_PROMOTED)])
        with patch("reevaluate_quality.SessionLocal", return_value=session):
            assert rq.fetch_current_states([1]) == {1: rq.STATE_PROMOTED}
        session.close.assert_called_once()

    def test_missing_raw_id_is_absent_not_none(self):
        session = _session_returning([])
        with patch("reevaluate_quality.SessionLocal", return_value=session):
            assert rq.fetch_current_states([1, 2]) == {}

    def test_ids_are_queried_in_chunks(self, monkeypatch):
        """PG 的 IN (...) 有參數上限；候選在假想規模下可能上百萬筆。"""
        monkeypatch.setattr(rq, "_STATE_LOOKUP_CHUNK", 2)
        session = _session_returning([(1, "a")], [(3, "b")])
        with patch("reevaluate_quality.SessionLocal", return_value=session):
            assert rq.fetch_current_states([1, 2, 3]) == {1: "a", 3: "b"}
        assert session.query.return_value.filter.call_count == 2


class TestAppendEvents:

    EVENT = {"raw_id": 1, "order_id": "O1", "event_type": rq.EVENT_PROMOTION,
             "from_state": rq.STATE_QUARANTINED, "to_state": rq.STATE_PROMOTED,
             "rule_version": DQ_RULE_VERSION,
             "event_at": datetime(2026, 8, 1, tzinfo=timezone.utc), "reason": None}

    def test_empty_does_not_open_a_session(self):
        with patch("reevaluate_quality.SessionLocal",
                   side_effect=AssertionError("沒有事件時不該開 session")):
            assert rq.append_events([]) == 0

    def test_writes_whole_batch_in_one_transaction(self):
        """刻意不逐筆 commit：半套 append 會讓 backlog 對不上帳，
        而且沒有任何 pipeline 會自動補完（不像攝入層有 scan recovery）。"""
        session = MagicMock()
        with patch("reevaluate_quality.SessionLocal", return_value=session):
            assert rq.append_events([self.EVENT, self.EVENT]) == 2
        added = session.add_all.call_args[0][0]
        assert len(added) == 2 and added[0].to_state == rq.STATE_PROMOTED
        session.commit.assert_called_once()
        session.close.assert_called_once()

    def test_failure_rolls_back_and_reraises(self):
        session = MagicMock()
        session.commit.side_effect = RuntimeError("db down")
        with patch("reevaluate_quality.SessionLocal", return_value=session):
            with pytest.raises(RuntimeError, match="db down"):
                rq.append_events([self.EVENT])
        session.rollback.assert_called_once()
        session.close.assert_called_once()


class TestCLI:

    @pytest.fixture
    def stub(self, monkeypatch):
        """把 BQ 與 PG 都換掉；回傳被寫入的事件清單（None＝append_events 沒被呼叫）。"""
        written = {}
        monkeypatch.setattr(rq, "PROJECT", "fake-project")
        monkeypatch.setattr(rq, "get_bq_client", lambda: object())
        monkeypatch.setattr(rq, "fetch_candidates", lambda client, limit: [
            make_row(raw_id=1, age=30, clean_error_message=codes(DQCode.AGE_OUT_OF_RANGE)),
        ])
        monkeypatch.setattr(rq, "fetch_current_states",
                            lambda raw_ids: {1: rq.STATE_QUARANTINED})
        monkeypatch.setattr(rq, "append_events",
                            lambda events: written.setdefault("events", events) and 0)
        return written

    def test_dry_run_is_the_default(self, stub):
        """append-only 表寫錯刪不掉 → 寫入必須是顯式 opt-in。"""
        rq.main([])
        assert "events" not in stub

    def test_commit_writes(self, stub):
        rq.main(["--commit"])
        assert len(stub["events"]) == 1
        assert stub["events"][0]["to_state"] == rq.STATE_PROMOTED

    def test_rule_version_mismatch_aborts_before_reading_bq(self, monkeypatch):
        monkeypatch.setattr(rq, "PROJECT", "fake-project")
        monkeypatch.setattr(rq, "get_bq_client",
                            lambda: pytest.fail("版本不符時不該碰 BQ"))
        with pytest.raises(RuntimeError, match="規則版本不符"):
            rq.main(["--expect-rule-version", "v-nope"])

    def test_matching_rule_version_proceeds(self, stub):
        rq.main(["--commit", "--expect-rule-version", DQ_RULE_VERSION])
        assert len(stub["events"]) == 1

    def test_missing_bq_project_fails_fast(self, monkeypatch):
        monkeypatch.setattr(rq, "PROJECT", "")
        monkeypatch.setattr(rq, "get_bq_client",
                            lambda: pytest.fail("缺 BQ_PROJECT 時不該建立 client"))
        with pytest.raises(RuntimeError, match="BQ_PROJECT"):
            rq.main([])
