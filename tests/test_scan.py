"""
掃描 Recovery 測試（遷移自 test_scan_retry.py）

scan_and_recover : stale processing 重設 / pending 收集 / commit 行為

排程端（誰在什麼時候觸發掃描、掃到之後怎麼派工）已搬到 Celery Beat，
對應測試在 test_tasks.py::TestScanAndDispatchTask / TestBeatSchedule。
本檔只留 scan_and_recover 這個純函式本身的契約。
"""

from unittest.mock import MagicMock, patch

import process
from process import scan_and_recover, try_claim_raw, STALE_PROCESSING_MINUTES
from helpers import scalars_result, claim_result


# ─── scan_and_recover Helper ──────────────────────────────────────────────────

def make_scan_db(exec_results: list) -> MagicMock:
    """
    建立 scan 專用的 mock DB。
    scan_and_recover 使用 scalars().all()，與 process_raw_event 的 scalar_one_or_none 不同。
    """
    from collections import deque
    mock_db = MagicMock()
    queue = deque(exec_results)
    mock_db.execute.side_effect = lambda stmt: queue.popleft() if queue else MagicMock()
    return mock_db


# ─── scan_and_recover 單元測試 ────────────────────────────────────────────────
#
# execute 呼叫順序：
#   1. SELECT stale processing（processing_started_at < threshold）
#   2. [if stale] UPDATE stale → pending
#   3. SELECT all pending

class TestScanAndRecover:

    def test_no_stuck_records_returns_empty_list(self):
        """沒有任何 stuck 記錄 → 回傳 []，不 commit。"""
        mock_db = make_scan_db([
            scalars_result([]),  # stale query
            scalars_result([]),  # pending query
        ])

        with patch("process.SessionLocal", return_value=mock_db):
            result = scan_and_recover()

        assert result == []
        assert mock_db.commit.call_count == 0

    def test_only_pending_records_returned_without_commit(self):
        """只有 pending（沒有 stale processing）→ 直接回傳，不 commit。"""
        mock_db = make_scan_db([
            scalars_result([]),            # stale: 無
            scalars_result([1, 2, 3]),     # pending: 3 筆
        ])

        with patch("process.SessionLocal", return_value=mock_db):
            result = scan_and_recover()

        assert result == [1, 2, 3]
        assert mock_db.commit.call_count == 0

    def test_stale_processing_reset_to_pending_with_warning(self):
        """stale processing 超過門檻 → 重設為 pending，記 WARNING log，回傳所有 pending。"""
        mock_db = make_scan_db([
            scalars_result([5, 6]),  # stale: 2 筆
            MagicMock(),             # UPDATE stale → pending
            scalars_result([5, 6]), # pending（含剛重設的）
        ])

        with patch("process.SessionLocal", return_value=mock_db), \
             patch.object(process.logger, "warning") as mock_warning:
            result = scan_and_recover()

        assert result == [5, 6]
        assert mock_db.commit.call_count == 1   # 只有 stale reset 的 commit
        assert mock_warning.called

    def test_recent_processing_not_in_stale_result(self):
        """
        recent processing（< 門檻）→ DB 的 WHERE processing_started_at < threshold 條件
        已在查詢層過濾，stale query 回傳空，不被重設。
        """
        mock_db = make_scan_db([
            scalars_result([]),  # stale: 無（recent 不符條件）
            scalars_result([]),  # pending: 無
        ])

        with patch("process.SessionLocal", return_value=mock_db):
            result = scan_and_recover()

        assert result == []
        assert mock_db.commit.call_count == 0


# ─── 逾時基準：processing_started_at ⭐ ───────────────────────────────────────
#
# 這組守的是一個實測過的迴歸（見 QUEUE-TW.md §3.1／§5.4）：逾時基準若用 received_at，
# 積壓中的記錄會在「剛被搶佔、正在處理」時就符合逾時條件，被掃描收回改回 pending
# 並重新派工 → 同一個 raw_id 兩個 worker 並行 → 落敗方被誤標為 duplicate。
# CAS 擋不住這件事，因為狀態是被第三方倒退回 pending 的。

class TestStaleBasis:

    def test_stale_query_filters_on_processing_started_at(self):
        """
        逾時判定必須看「這次處理跑了多久」（processing_started_at），
        不能看「這筆資料躺了多久」（received_at）——積壓時兩者天差地遠。
        """
        captured = []

        mock_db = MagicMock()
        def _execute(stmt):
            captured.append(str(stmt))
            return scalars_result([])
        mock_db.execute.side_effect = _execute

        with patch("process.SessionLocal", return_value=mock_db):
            scan_and_recover()

        stale_query = captured[0]
        assert "processing_started_at" in stale_query
        assert "received_at" not in stale_query

    def test_claim_stamps_processing_started_at(self):
        """
        搶佔成功時必須蓋上 processing_started_at，否則上面那條查詢永遠比不到東西，
        stale 記錄會從「10 分鐘後恢復」變成「永久卡死」。
        """
        captured = []

        mock_db = MagicMock()
        def _execute(stmt):
            captured.append(str(stmt))
            return claim_result(1)
        mock_db.execute.side_effect = _execute

        assert try_claim_raw(mock_db, 1) is True
        assert "processing_started_at" in captured[0]
