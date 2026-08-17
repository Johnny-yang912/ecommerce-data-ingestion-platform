"""
process.py Retry 測試（遷移自 test_process_retry.py）

Point 3 — Claim retry    : OperationalError retry / rowcount=0 不 retry
Point 2 — Processing retry: JSONDecodeError / ValueError 不 retry；Exception retry
Point 4 — Status commit   : _commit_raw_status retry；success path commit retry
Idempotency               : pre-check 攔截 / IntegrityError TOCTOU race

注意：每個測試的 exec_results 順序對應 process_raw_event 內的 db.execute() 呼叫序列。
正常成功路徑的 4 次 execute：
  1. try_claim_raw → UPDATE claim
  2. SELECT raw
  3. SELECT ODS（pre-check）
  4. UPDATE raw status（success）
正常成功路徑的 2 次 commit：
  1. claim commit
  2. success commit（ODS + status 同一個 commit）
"""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError, IntegrityError, DataError

import process
from process import (
    process_raw_event,
    _commit_raw_status,
    MAX_CLAIM_RETRIES,
    MAX_PROCESS_RETRIES,
    MAX_STATUS_RETRIES,
)
from models import QualityEvent, ODS
from helpers import (
    make_mock_db, claim_result, select_result,
    no_ods_result, existing_ods_result, make_mock_raw, VALID_PAYLOAD,
)


# ─── Point 3: Claim retry ─────────────────────────────────────────────────────

class TestClaimRetry:

    def test_claim_commit_fails_once_then_succeeds(self):
        """claim commit 第 1 次 OperationalError → 第 2 次 retry 成功。"""
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),        # claim attempt 1
                claim_result(1),        # claim attempt 2（retry）
                select_result(mock_raw),
                no_ods_result(),        # pre-check：無重複
                MagicMock(),            # UPDATE status（success commit）
            ],
            commit_effects=[
                OperationalError("connection lost", None, None),  # claim commit 1
                None,  # claim commit 2
                None,  # final success commit
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert mock_db.commit.call_count == 3
        assert mock_db.rollback.call_count == 1

    def test_claim_commit_fails_max_retries_then_returns(self):
        """claim commit 連續達到 MAX_CLAIM_RETRIES → 函式正常 return，不 crash。"""
        mock_db = make_mock_db(
            exec_results=[claim_result(1)] * MAX_CLAIM_RETRIES,
            commit_effects=[OperationalError("error", None, None)] * MAX_CLAIM_RETRIES,
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)  # 不應拋出例外

        assert mock_db.commit.call_count == MAX_CLAIM_RETRIES
        assert mock_db.rollback.call_count == MAX_CLAIM_RETRIES

    def test_claim_rowcount_zero_returns_immediately_without_retry(self):
        """rowcount=0（另一個 worker 搶到）→ 直接 return，不走 retry loop。"""
        mock_db = make_mock_db(
            exec_results=[claim_result(0)],
            commit_effects=[None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        # 只有 claim 的 1 次 commit，沒有進入後續處理
        assert mock_db.commit.call_count == 1
        assert mock_db.rollback.call_count == 0

    def test_claim_succeeds_but_raw_not_found_returns_early(self):
        """
        CAS 搶到（rowcount=1），但隨後 SELECT raw 查無此筆
        （極邊緣情況：record 在 claim 和 select 之間被刪）
        → log warning，不進行任何 ODS 操作，直接 return。
        """
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(None),  # SELECT raw → None
            ],
            commit_effects=[None],  # claim commit
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "warning") as mock_warning:
            process_raw_event(1)

        mock_warning.assert_called_once()
        assert mock_db.add.call_count == 0  # ODS 不應被寫入


# ─── B1 營運指標 ──────────────────────────────────────────────────────────────

class TestOperationalMetrics:
    """
    指標的埋點位置與 label。

    這裡驗的是「數字對不對」而不是「有沒有送出去」——後者是 SDK 的事。
    指標一旦上了 dashboard 與告警，錯誤的計數比沒有計數更危險：它看起來是健康的。
    """

    def test_duration_not_recorded_when_claim_lost(self):
        """
        claim 落空不計入耗時分佈。

        那是毫秒級的 no-op；把它混進來，愈多 worker 競爭、P95 看起來愈漂亮，
        而真實情況正好相反——這種指標會在事故當下給出反向訊號。
        """
        mock_db = make_mock_db(exec_results=[claim_result(0)], commit_effects=[None])

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch("process.PROCESSING_DURATION") as duration:
            process_raw_event(1)

        duration.record.assert_not_called()

    def test_duration_recorded_once_when_claimed(self):
        """claim 成功就記一次，且是秒為單位的正數。"""
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch("process.PROCESSING_DURATION") as duration:
            process_raw_event(1)

        duration.record.assert_called_once()
        assert duration.record.call_args.args[0] >= 0

    def test_processed_counted_with_result_label(self):
        """成功終態 → result=processed。processed 是唯一不走 _commit_raw_status 的。"""
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch("process.ORDERS_RESULT") as counter:
            process_raw_event(1)

        counter.add.assert_called_once_with(1, {"result": "processed"})

    @pytest.mark.parametrize("status", ["error", "duplicate"])
    def test_failure_terminals_counted_at_single_choke_point(self, status):
        """
        error / duplicate 的七個呼叫點全部匯流到 _commit_raw_status，
        所以那一行的計數就涵蓋全部失敗終態——這是埋點只有兩處的原因。
        """
        mock_db = make_mock_db(exec_results=[MagicMock()], commit_effects=[None])

        with patch("process.ORDERS_RESULT") as counter:
            _commit_raw_status(mock_db, 1, status, "boom")

        counter.add.assert_called_once_with(1, {"result": status})

    def test_no_result_counted_when_status_commit_never_succeeds(self):
        """
        status 更新用盡重試仍失敗 → **不能**計數。

        這筆記錄其實卡在 processing、沒有進入任何終態；計了就會讓
        「終態總數」與 raw.status 的真相對不起來，而對帳正是這個指標的驗收條件。
        """
        mock_db = make_mock_db(
            exec_results=[MagicMock()] * MAX_STATUS_RETRIES,
            commit_effects=[Exception("boom")] * MAX_STATUS_RETRIES,
        )

        with patch("process.time.sleep"), \
             patch("process.ORDERS_RESULT") as counter:
            _commit_raw_status(mock_db, 1, "error", "boom")

        counter.add.assert_not_called()

    def test_claim_retry_counted_with_stage_label(self):
        """retry 的 stage label 要能區分四層，否則分佈混在一起等於沒有資訊。"""
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1), claim_result(1), select_result(mock_raw),
                no_ods_result(), MagicMock(),
            ],
            commit_effects=[
                OperationalError("connection lost", None, None), None, None,
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch("process.RETRIES") as retries:
            process_raw_event(1)

        retries.add.assert_called_once_with(1, {"stage": "claim"})


# ─── Point 2: Processing retry ───────────────────────────────────────────────

class TestProcessingRetry:

    def test_json_decode_error_marks_error_without_retry(self):
        """JSONDecodeError：資料問題，不走 retry loop，直接 mark error。"""
        mock_raw = make_mock_raw()
        mock_raw.raw_payload = "{ invalid json !!!"

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                MagicMock(),   # _commit_raw_status: UPDATE status error
            ],
            commit_effects=[None, None],  # claim, _commit_raw_status
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        # 共 2 次 commit（claim + error status），1 次 rollback（JSONDecodeError 後）
        assert mock_db.commit.call_count == 2
        assert mock_db.rollback.call_count == 1

    def test_value_error_marks_error_without_retry(self):
        """ValueError（from_nested 拋出）：資料問題，不走 retry loop，直接 mark error。"""
        mock_raw = make_mock_raw()

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                MagicMock(),   # _commit_raw_status
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.ODSOrder.from_nested", side_effect=ValueError("invalid field")), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert mock_db.commit.call_count == 2
        assert mock_db.rollback.call_count == 1

    def test_transient_exception_retries_and_succeeds(self):
        """暫時性 Exception：第 1 次失敗 → retry → 第 2 次成功。"""
        mock_raw = make_mock_raw()
        original_from_nested = process.ODSOrder.from_nested

        call_count = [0]
        def flaky_from_nested(payload):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("transient db error")
            return original_from_nested(payload)

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),    # initial SELECT raw
                select_result(mock_raw),    # reload after attempt 0 fails
                no_ods_result(),            # pre-check
                MagicMock(),                # UPDATE status（success commit）
            ],
            commit_effects=[None, None],  # claim, final success
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.ODSOrder.from_nested", side_effect=flaky_from_nested), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert call_count[0] == 2
        assert mock_db.rollback.call_count == 1
        assert mock_db.commit.call_count == 2

    def test_persistent_exception_exhausts_retries_and_marks_error(self):
        """Exception 連續達到 MAX_PROCESS_RETRIES → mark error（Max retries exceeded）。"""
        mock_raw = make_mock_raw()

        # execute 呼叫：claim / initial select / reload × (MAX-1) / _commit_raw_status update
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),                           # initial
                *[select_result(mock_raw)] * (MAX_PROCESS_RETRIES - 1),  # reload
                MagicMock(),                                       # _commit_raw_status
            ],
            commit_effects=[None, None],  # claim, error status
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.ODSOrder.from_nested", side_effect=Exception("persistent error")), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert mock_db.rollback.call_count == MAX_PROCESS_RETRIES
        assert mock_db.commit.call_count == 2

    def test_has_clean_error_logs_warning_but_does_not_block_ods_write(self):
        """
        business_clean 標記錯誤（has_clean_error=True）→ 記 warning log，
        但不阻斷 ODS 寫入。has_clean_error 的設計是非阻塞的：
        有問題的資料仍寫入 ODS，下游自行決定如何處理。
        """
        dirty_payload = {
            **VALID_PAYLOAD,
            "behavior": {"customer_rating": 99.0},  # 超出 1~5 範圍，觸發 business error
        }
        mock_raw = make_mock_raw(dirty_payload)

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),  # pre-check：無重複
                MagicMock(),      # UPDATE status（success）
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "warning") as mock_warning:
            process_raw_event(1)

        assert mock_warning.called             # 有記 warning
        assert mock_db.add.call_count == 2     # ODS + quality_event（非阻塞）
        assert mock_db.commit.call_count == 2  # claim + success commit


# ─── Point 4: Status commit retry ────────────────────────────────────────────

class TestStatusCommitRetry:

    def test_commit_raw_status_retries_and_succeeds(self):
        """_commit_raw_status：commit 第 1 次失敗 → 第 2 次成功。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [Exception("db error"), None]

        with patch("process.time.sleep"):
            _commit_raw_status(mock_db, raw_id=1, status="error", error_message="test")

        assert mock_db.commit.call_count == 2
        assert mock_db.rollback.call_count == 1
        # 每次 retry 都重新 execute（UPDATE）
        assert mock_db.execute.call_count == 2

    def test_commit_raw_status_all_fail_logs_critical(self):
        """_commit_raw_status 全部失敗 → 記 CRITICAL log，函式正常返回（不 crash）。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [Exception("error")] * MAX_STATUS_RETRIES

        with patch("process.time.sleep"), \
             patch.object(process.logger, "critical") as mock_critical:
            _commit_raw_status(mock_db, raw_id=1, status="error")

        assert mock_db.commit.call_count == MAX_STATUS_RETRIES
        assert mock_critical.called

    def test_success_commit_fails_once_readds_ods_and_retries(self):
        """
        ODS + status 同一個 commit 失敗 → rollback 後重新 db.add(ods) → 第 2 次成功。

        db.add 應呼叫 2 次：initial add + re-add after rollback。
        """
        mock_raw = make_mock_raw()

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),        # pre-check
                MagicMock(),            # UPDATE status attempt 1（commit 失敗）
                MagicMock(),            # UPDATE status attempt 2（commit 成功）
            ],
            commit_effects=[
                None,                       # claim commit
                Exception("commit error"),  # success commit attempt 1
                None,                       # success commit attempt 2
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert mock_db.commit.call_count == 3
        assert mock_db.rollback.call_count == 1
        # (ods + quality_event) initial + (ods + quality_event) re-add after rollback
        assert mock_db.add.call_count == 4

    def test_success_commit_exhausts_all_retries_logs_critical(self):
        """
        success path commit 連續非 IntegrityError 失敗達 MAX_STATUS_RETRIES 次
        → 記 CRITICAL log，ODS 未完成寫入，record 永久卡在 processing。

        db.add 呼叫次數：initial(1) + re-add×(MAX-1) = MAX_STATUS_RETRIES
        （最後一次失敗不 re-add，直接記 CRITICAL）
        """
        mock_raw = make_mock_raw()

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),  # pre-check
                *[MagicMock() for _ in range(MAX_STATUS_RETRIES)],  # UPDATE status × MAX
            ],
            commit_effects=[
                None,  # claim commit
                *[Exception("persistent commit error")] * MAX_STATUS_RETRIES,
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "critical") as mock_critical:
            process_raw_event(1)

        assert mock_critical.called
        # (ods + quality_event) × initial(1) + re-add × (MAX-1) = MAX_STATUS_RETRIES * 2
        assert mock_db.add.call_count == MAX_STATUS_RETRIES * 2


# ─── DataError fast-fail（欄位長度超限等資料層錯誤）─────────────────────────

class TestDataErrorFastFail:

    def test_data_error_on_ods_commit_marks_error_without_retry(self):
        """
        ODS 寫入觸發 DataError（如欄位長度超限）→ fast-fail 終態 error，不重試。
        驗證：deterministic 失敗不會卡在 processing 被反覆重排（poison-pill）。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),   # pre-check
                MagicMock(),       # UPDATE status（success commit，將 DataError）
                MagicMock(),       # _commit_raw_status：UPDATE error
            ],
            commit_effects=[
                None,                                       # claim
                DataError("value too long", None, None),    # success commit → DataError
                None,                                       # _commit_raw_status
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "error") as mock_error:
            process_raw_event(1)

        # claim + 失敗的 success commit + error status = 3 次 commit
        assert mock_db.commit.call_count == 3
        assert mock_db.rollback.call_count == 1
        # ods + quality_event 各只 add 一次，DataError 後不重試、不 re-add
        assert mock_db.add.call_count == 2
        assert mock_error.called


# ─── ValueError fast-fail（字串含 NUL 等儲存引擎不可存的值）──────────────────

class TestValueErrorFastFail:

    def test_value_error_on_ods_commit_marks_error_without_retry(self):
        """
        ODS 寫入觸發 ValueError（如字串含 NUL：上游送 JSON ` ` escape，
        json.loads 後成真實 0x00 字元，psycopg2 在參數轉換階段拋 bare ValueError）
        → fast-fail 終態 error，不重試。
        驗證：此 deterministic 失敗不會卡在 processing 被反覆重排（poison-pill）。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),   # pre-check
                MagicMock(),       # UPDATE status（success commit，將 ValueError）
                MagicMock(),       # _commit_raw_status：UPDATE error
            ],
            commit_effects=[
                None,                                                       # claim
                ValueError("A string literal cannot contain NUL (0x00) characters."),  # success commit
                None,                                                       # _commit_raw_status
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "error") as mock_error:
            process_raw_event(1)

        # claim + 失敗的 success commit + error status = 3 次 commit
        assert mock_db.commit.call_count == 3
        assert mock_db.rollback.call_count == 1
        # ods + quality_event 各只 add 一次，ValueError 後不重試、不 re-add
        assert mock_db.add.call_count == 2
        assert mock_error.called


# ─── Idempotency: first-write-wins ───────────────────────────────────────────

class TestIdempotency:

    def test_pre_check_detects_duplicate_order_id(self):
        """
        ODS 已有此 order_id（pre-check 命中）→ raw 標記 duplicate，db.add 不應被呼叫。
        """
        mock_raw = make_mock_raw()

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),        # SELECT raw
                existing_ods_result(42),        # pre-check：ODS 已有此 order_id
                MagicMock(),                    # _commit_raw_status: UPDATE duplicate
            ],
            commit_effects=[None, None],  # claim, duplicate status
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        # ODS 不應被寫入
        assert mock_db.add.call_count == 0
        assert mock_db.commit.call_count == 2

    def test_integrity_error_on_toctou_race_marks_duplicate(self):
        """
        兩個 worker 同時通過 pre-check，其中一個 commit 時觸發 IntegrityError（TOCTOU race）
        → 不 retry，直接標記 duplicate。
        """
        mock_raw = make_mock_raw()

        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),    # SELECT raw
                no_ods_result(),            # pre-check：尚無重複（race 前）
                MagicMock(),                # UPDATE status in success commit（commit 將失敗）
                existing_ods_result(42),   # IntegrityError handler: SELECT ODS for log
                MagicMock(),               # _commit_raw_status: UPDATE duplicate
            ],
            commit_effects=[
                None,                                          # claim commit
                IntegrityError("duplicate key", None, None),  # success commit → TOCTOU
                None,                                          # _commit_raw_status
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        # claim + IntegrityError rollback + duplicate status
        assert mock_db.commit.call_count == 3
        assert mock_db.rollback.call_count == 1
        # ods + quality_event 各 1 次；IntegrityError 後不重試，不重新 add
        assert mock_db.add.call_count == 2


# ─── Quality Events ───────────────────────────────────────────────────────────

def _quality_events_from_add(mock_db) -> list:
    """從 db.add call list 中篩出 QualityEvent 物件。"""
    return [
        call[0][0]
        for call in mock_db.add.call_args_list
        if isinstance(call[0][0], QualityEvent)
    ]


class TestQualityEvents:

    def test_clean_record_writes_quality_event_clean_state(self):
        """
        正常路徑（無 has_clean_error）→ quality_event to_state="clean" 與 ODS 同一 commit。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),  # UPDATE status
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        events = _quality_events_from_add(mock_db)
        assert len(events) == 1
        assert events[0].event_type == "initial_evaluation"
        assert events[0].to_state == "clean"
        assert events[0].from_state is None
        assert events[0].raw_id == 1
        assert events[0].rule_version == process.DQ_RULE_VERSION

    def test_dirty_record_writes_quality_event_quarantined_state(self):
        """
        has_clean_error=True → quality_event to_state="quarantined"，reason 帶錯誤訊息。
        """
        dirty_payload = {
            **VALID_PAYLOAD,
            "behavior": {"customer_rating": 99.0},  # 超出 1~5 範圍
        }
        mock_raw = make_mock_raw(dirty_payload)
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "warning"):
            process_raw_event(1)

        events = _quality_events_from_add(mock_db)
        assert len(events) == 1
        assert events[0].to_state == "quarantined"
        assert events[0].reason is not None

    def test_commit_retry_readds_quality_event(self):
        """
        success commit 第 1 次失敗 → rollback 後 quality_event 與 ods 一起重新 add。
        quality_event 在 add list 中應出現 2 次（initial + re-add）。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),  # UPDATE status attempt 1
                MagicMock(),  # UPDATE status attempt 2
            ],
            commit_effects=[None, Exception("commit error"), None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        events = _quality_events_from_add(mock_db)
        assert len(events) == 2  # initial add + re-add after rollback

    def test_no_quality_event_on_pre_check_duplicate(self):
        """
        pre-check 攔截 duplicate → quality_event 不被寫入（ODS 未寫入）。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                existing_ods_result(42),  # pre-check 命中
                MagicMock(),              # _commit_raw_status
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        assert len(_quality_events_from_add(mock_db)) == 0

    def test_no_quality_event_committed_on_integrity_error(self):
        """
        TOCTOU IntegrityError → quality_event 雖已 add 到 session，
        但 rollback 後不重新 add（ODS 未成功寫入，不記錄品質事件）。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),              # success commit attempt（將失敗）
                existing_ods_result(42),  # IntegrityError handler SELECT
                MagicMock(),              # _commit_raw_status
            ],
            commit_effects=[
                None,
                IntegrityError("duplicate key", None, None),
                None,
            ],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"):
            process_raw_event(1)

        # quality_event 只被 add 一次，但 rollback 後未重新 add（代表未寫入 DB）
        events = _quality_events_from_add(mock_db)
        assert len(events) == 1
        assert mock_db.rollback.call_count == 1

    def _ods_from_add(self, mock_db):
        return [c[0][0] for c in mock_db.add.call_args_list if isinstance(c[0][0], ODS)]

    def test_schema_drift_columns_written_and_logged(self):
        """payload 有契約外欄位 → ODS 帶 has_schema_drift/unmapped_fields，並發 schema_drift log。"""
        drift_payload = {**VALID_PAYLOAD, "loyalty_points": 99}
        mock_raw = make_mock_raw(drift_payload)
        mock_db = make_mock_db(
            exec_results=[claim_result(1), select_result(mock_raw), no_ods_result(), MagicMock()],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "warning") as mock_warning:
            process_raw_event(1)

        ods = self._ods_from_add(mock_db)[0]
        assert ods.has_schema_drift is True
        assert ods.unmapped_fields == {"loyalty_points": 99}
        assert ods.schema_drift_message is not None
        assert any(c[0][0] == "schema_drift" for c in mock_warning.call_args_list)

    def test_clean_payload_has_no_schema_drift(self):
        """乾淨 payload → has_schema_drift=False、message/unmapped 為 None，不發 schema_drift log。"""
        mock_raw = make_mock_raw()  # VALID_PAYLOAD
        mock_db = make_mock_db(
            exec_results=[claim_result(1), select_result(mock_raw), no_ods_result(), MagicMock()],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "warning") as mock_warning:
            process_raw_event(1)

        ods = self._ods_from_add(mock_db)[0]
        assert ods.has_schema_drift is False
        assert ods.schema_drift_message is None
        assert ods.unmapped_fields is None
        assert not any(c[0][0] == "schema_drift" for c in mock_warning.call_args_list)

    def test_quality_metric_log_emitted_on_success(self):
        """
        成功 commit 後 logger.info("quality_metric", ...) 被呼叫，
        且帶有 rule_version 與 has_clean_error 欄位。
        """
        mock_raw = make_mock_raw()
        mock_db = make_mock_db(
            exec_results=[
                claim_result(1),
                select_result(mock_raw),
                no_ods_result(),
                MagicMock(),
            ],
            commit_effects=[None, None],
        )

        with patch("process.SessionLocal", return_value=mock_db), \
             patch("process.time.sleep"), \
             patch.object(process.logger, "info") as mock_info:
            process_raw_event(1)

        log_events = [c for c in mock_info.call_args_list if c[0][0] == "quality_metric"]
        assert len(log_events) == 1
        kwargs = log_events[0][1]
        assert "rule_version" in kwargs
        assert "has_clean_error" in kwargs
