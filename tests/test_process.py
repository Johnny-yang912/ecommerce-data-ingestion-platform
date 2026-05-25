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
from sqlalchemy.exc import OperationalError, IntegrityError

import process
from process import (
    process_raw_event,
    _commit_raw_status,
    MAX_CLAIM_RETRIES,
    MAX_PROCESS_RETRIES,
    MAX_STATUS_RETRIES,
)
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
        assert mock_db.add.call_count == 1     # ODS 仍被寫入（非阻塞）
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
        # initial add + re-add after rollback
        assert mock_db.add.call_count == 2

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
        # initial add(1) + re-add × (MAX-1) = MAX_STATUS_RETRIES
        assert mock_db.add.call_count == MAX_STATUS_RETRIES


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
        # ODS add 只有 1 次（不重試）
        assert mock_db.add.call_count == 1
