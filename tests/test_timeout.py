"""
Timeout 相關測試（遷移自 test_timeout_changes.py）

Pool 耗盡 → 503           : SATimeoutError 不走 retry，快速回 503
POST /process_raw/{raw_id}: force=False / force=True / 404 / 不直接執行
database.py 設定          : pool_size / max_overflow / pool_timeout / statement_timeout
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi import HTTPException
from sqlalchemy.exc import TimeoutError as SATimeoutError

from main import process_raw, create_order, get_raw, MAX_RAW_WRITE_RETRIES
import main
from helpers import make_sample_order


SAMPLE_ORDER = make_sample_order(order_id="TEST-503-001")


# ─── Pool 耗盡 → 503 ──────────────────────────────────────────────────────────

class TestPoolExhaustion:

    async def test_pool_exhausted_returns_503_without_retry(self, mock_request, raw_body):
        """SATimeoutError → 503 Service Unavailable，commit 只被呼叫 1 次（不走 retry loop）。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = SATimeoutError("pool timeout")

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"), \
             patch("main.time.sleep"), \
             pytest.raises(HTTPException) as exc_info:
            create_order(mock_request, SAMPLE_ORDER, raw_body=raw_body, client_id="test-client")

        assert exc_info.value.status_code == 503
        # pool 耗盡不應 retry
        assert mock_db.commit.call_count == 1
        # session 應被關閉（finally block）
        assert mock_db.close.call_count == 1


# ─── POST /process_raw/{raw_id} ───────────────────────────────────────────────

class TestProcessRawEndpoint:

    async def test_force_false_only_queries_existence(self, mock_request, mock_enqueue):
        """force=False：只 SELECT 確認記錄存在，不修改 DB，然後派工到 Celery。"""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock()

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"):
            result = process_raw(mock_request, raw_id=42, force=False)

        # 只有 1 次 SELECT，0 次 commit
        assert mock_db.execute.call_count == 1
        assert mock_db.commit.call_count == 0
        assert mock_db.close.call_count == 1

        # 派工到 Celery，帶正確 raw_id
        mock_enqueue.assert_called_once_with(42)

        assert result == {"raw_id": 42, "triggered": True, "force": False}

    async def test_force_true_resets_status_then_schedules(self, mock_request, mock_enqueue):
        """force=True：SELECT + UPDATE status（commit）→ 再派工到 Celery。"""
        mock_raw = MagicMock()
        mock_raw.status = "error"   # 明確設定，force=True 只允許 error / duplicate
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_raw

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"):
            result = process_raw(mock_request, raw_id=99, force=True)

        # SELECT + UPDATE = 2 次 execute，1 次 commit
        assert mock_db.execute.call_count == 2
        assert mock_db.commit.call_count == 1
        assert mock_db.close.call_count == 1
        mock_enqueue.assert_called_once_with(99)
        assert result == {"raw_id": 99, "triggered": True, "force": True}

    async def test_process_raw_event_not_executed_inline(self, mock_request, mock_enqueue):
        """
        process_raw_event 不應在 request 行程內執行，只透過 Celery 派工。
        這驗證 /process_raw 不 block event loop。
        """
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock()

        with patch("process.process_raw_event") as mock_fn, \
             patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"):
            process_raw(mock_request, raw_id=1, force=False)

        mock_fn.assert_not_called()
        mock_enqueue.assert_called_once_with(1)

    async def test_raw_id_not_found_returns_404(self, mock_request, mock_enqueue):
        """raw_id 不存在 → 404，不應派工。"""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None  # not found

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"), \
             pytest.raises(HTTPException) as exc_info:
            process_raw(mock_request, raw_id=99999, force=False)

        assert exc_info.value.status_code == 404
        mock_enqueue.assert_not_called()
        assert mock_db.close.call_count == 1

    async def test_force_true_on_processed_status_returns_400(self, mock_request, mock_enqueue):
        """
        force=True 但 raw.status = 'processed'（不允許的狀態）→ 400 Bad Request。

        force=True 的語意是「重試失敗記錄」，對已完成的記錄 force 會製造下游不一致，
        所以明確拒絕並回 400。
        """
        mock_raw = MagicMock()
        mock_raw.status = "processed"  # 不是 error 或 duplicate
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_raw

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"), \
             pytest.raises(HTTPException) as exc_info:
            process_raw(mock_request, raw_id=1, force=True)

        assert exc_info.value.status_code == 400
        mock_enqueue.assert_not_called()
        assert mock_db.close.call_count == 1


# ─── GET /raw/{raw_id} ────────────────────────────────────────────────────────

class TestGetRawEndpoint:

    async def test_get_raw_returns_found_record(self, mock_request):
        """raw_id 存在 → 回傳 raw 物件，session 正確關閉。"""
        mock_raw = MagicMock()
        mock_raw.id = 1
        mock_raw.status = "processed"
        mock_raw.error_message = None
        mock_raw.processed_at = None
        mock_raw.raw_payload = '{"order_id": "ORD-001"}'

        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_raw

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"):
            result = get_raw(mock_request, raw_id=1)

        assert result is mock_raw
        assert mock_db.close.call_count == 1

    async def test_get_raw_not_found_returns_404(self, mock_request):
        """raw_id 不存在 → 404，session 正確關閉（finally block 有執行）。"""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._key_func", return_value="test-ip"), \
             pytest.raises(HTTPException) as exc_info:
            get_raw(mock_request, raw_id=99999)

        assert exc_info.value.status_code == 404
        assert mock_db.close.call_count == 1


# ─── database.py 設定驗證 ─────────────────────────────────────────────────────

class TestDatabaseSettings:

    def test_connection_pool_settings(self):
        """pool_size=5 / max_overflow=10 / pool_timeout=30（與 SQLAlchemy 預設相同，但明確寫出）。"""
        from database import engine
        pool = engine.pool
        assert pool.size() == 5
        assert pool._max_overflow == 10
        assert pool._timeout == 30

    def test_statement_timeout_in_connect_args(self):
        """connect_args 應包含 statement_timeout=30000（PostgreSQL session-level timeout）。"""
        import inspect
        from database import engine

        cparams = inspect.getclosurevars(engine.pool._creator).nonlocals.get("cparams", {})
        options = cparams.get("options", "")
        assert "statement_timeout=30000" in options
