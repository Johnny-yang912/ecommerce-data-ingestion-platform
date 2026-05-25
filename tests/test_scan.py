"""
掃描 Recovery 測試（遷移自 test_scan_retry.py）

scan_and_recover : stale processing 重設 / pending 收集 / commit 行為
lifespan startup : startup 時排程 pending records
_periodic_scan   : 單次迭代排程 / 例外不中斷迴圈

注意：
  - 所有 async 測試由 asyncio_mode=auto 自動處理，不需要 @pytest.mark.asyncio
  - test_startup_requeues_pending 使用 asyncio.sleep(0.1) 等待 to_thread tasks 完成
"""

import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

import process
import main
from main import lifespan, _periodic_scan, app
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


# ─── lifespan startup 測試 ────────────────────────────────────────────────────

class TestLifespanStartup:

    async def test_startup_requeues_all_pending_records(self):
        """Startup 找到 2 筆 pending → process_raw_event 各被呼叫一次。"""
        process_calls = []

        with patch("main.scan_and_recover", return_value=[10, 20]), \
             patch("main.process_raw_event", side_effect=lambda r: process_calls.append(r)), \
             patch("main._periodic_scan", new_callable=AsyncMock):

            async with lifespan(app):
                # 等 asyncio.to_thread tasks 在 thread pool 執行完
                await asyncio.sleep(0.1)

        assert sorted(process_calls) == [10, 20]

    async def test_startup_with_no_records_does_not_call_process(self):
        """Startup scan 沒有記錄 → process_raw_event 不被呼叫，scan 只呼叫一次。"""
        process_calls = []

        with patch("main.scan_and_recover", return_value=[]) as mock_scan, \
             patch("main.process_raw_event", side_effect=lambda r: process_calls.append(r)), \
             patch("main._periodic_scan", new_callable=AsyncMock):

            async with lifespan(app):
                await asyncio.sleep(0.1)

        mock_scan.assert_called_once()
        assert process_calls == []


# ─── _periodic_scan 測試 ─────────────────────────────────────────────────────
#
# 用 asyncio.CancelledError 終止迴圈：
#   mock_sleep 第 1 次立即回傳（進入 scan），第 2 次拋 CancelledError（結束）。

class TestPeriodicScan:

    async def test_periodic_scan_schedules_found_records(self):
        """Periodic scan 一輪：找到記錄 → process_raw_event 被呼叫。"""
        process_calls = []
        created_tasks = []
        orig_create_task = asyncio.create_task

        def capture_task(coro):
            t = orig_create_task(coro)
            created_tasks.append(t)
            return t

        sleep_count = [0]
        async def mock_sleep(n):
            sleep_count[0] += 1
            if sleep_count[0] > 1:
                raise asyncio.CancelledError()

        with patch("main.scan_and_recover", return_value=[7, 8]), \
             patch("main.process_raw_event", side_effect=lambda r: process_calls.append(r)), \
             patch("asyncio.sleep", side_effect=mock_sleep), \
             patch("asyncio.create_task", side_effect=capture_task):
            try:
                await _periodic_scan()
            except asyncio.CancelledError:
                pass

        # asyncio.sleep 已還原，等待 to_thread tasks 完成
        if created_tasks:
            await asyncio.gather(*created_tasks, return_exceptions=True)

        assert sorted(process_calls) == [7, 8]

    async def test_periodic_scan_exception_does_not_break_loop(self):
        """第 1 輪 scan 拋出例外 → 記 ERROR log，第 2 輪正常繼續。"""
        scan_count = [0]

        def mock_scan():
            scan_count[0] += 1
            if scan_count[0] == 1:
                raise Exception("db connection error")
            return []

        sleep_count = [0]
        async def mock_sleep(n):
            sleep_count[0] += 1
            if sleep_count[0] > 2:
                raise asyncio.CancelledError()

        with patch("main.scan_and_recover", side_effect=mock_scan), \
             patch("asyncio.sleep", side_effect=mock_sleep), \
             patch.object(main.logger, "error") as mock_error:
            try:
                await _periodic_scan()
            except asyncio.CancelledError:
                pass

        # 第 1 輪例外 + 第 2 輪正常 = 2 次 scan
        assert scan_count[0] == 2
        assert mock_error.called
