"""
掃描 Recovery 測試（遷移自 test_scan_retry.py）

scan_and_recover : stale processing 重設 / pending 收集 / commit 行為

排程端（誰在什麼時候觸發掃描、掃到之後怎麼派工）已搬到 Celery Beat，
對應測試在 test_tasks.py::TestScanAndDispatchTask / TestBeatSchedule。
本檔只留 scan_and_recover 這個純函式本身的契約。
"""

from unittest.mock import MagicMock, patch

import process
from process import scan_and_recover, STALE_PROCESSING_MINUTES
from helpers import scalars_result


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
#   1. SELECT stale processing（received_at < threshold）
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
        recent processing（< 門檻）→ DB 的 WHERE received_at < threshold 條件
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
