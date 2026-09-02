"""
Point 1 Retry 測試：POST /orders Raw 寫入 retry 機制（遷移自 test_retry.py）

驗證三條路徑：
  1. 一次成功（無 retry）
  2. 第 1 次 OperationalError → 第 2 次成功
  3. 連續 MAX_RAW_WRITE_RETRIES 次失敗 → 拋出 OperationalError

與舊版的差異：
  - async def test_* 由 pytest-asyncio 自動接管，不需要 asyncio.run()
  - 不需要 run_all() 也不需要 if __name__ == "__main__"
  - 斷言失敗時 pytest 自動展示 diff，不需要 print
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from sqlalchemy.exc import OperationalError
from fastapi.exceptions import RequestValidationError

import main
from main import create_order, MAX_RAW_WRITE_RETRIES


class TestRawWriteRetry:

    async def test_no_error_commits_once(self, mock_request, sample_order, raw_body):
        """正常路徑：第 1 次 commit 成功，共 1 次 commit，0 次 rollback。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [None]
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 999)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._enqueue", return_value=True), \
             patch("main._key_func", return_value="test-ip"), \
             patch("main.time.sleep"):
            result = create_order(mock_request, sample_order, raw_body=raw_body, client_id="test-client")

        assert mock_db.commit.call_count == 1
        assert mock_db.rollback.call_count == 0
        assert result == {"raw_id": 999, "status": "pending"}

    async def test_retry_succeeds_on_second_attempt(self, mock_request, sample_order, raw_body):
        """第 1 次 OperationalError → 第 2 次成功：共 2 次 commit，1 次 rollback。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [
            OperationalError("connection lost", None, None),
            None,
        ]
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 999)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._enqueue", return_value=True), \
             patch("main._key_func", return_value="test-ip"), \
             patch("main.time.sleep"):
            result = create_order(mock_request, sample_order, raw_body=raw_body, client_id="test-client")

        assert mock_db.commit.call_count == 2
        assert mock_db.rollback.call_count == 1
        assert result == {"raw_id": 999, "status": "pending"}

    async def test_all_retries_exhausted_raises_operational_error(self, mock_request, sample_order, raw_body):
        """連續 MAX_RAW_WRITE_RETRIES 次失敗 → 拋出 OperationalError，不吞掉例外。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [
            OperationalError("error", None, None)
        ] * MAX_RAW_WRITE_RETRIES
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 999)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._enqueue", return_value=True), \
             patch("main._key_func", return_value="test-ip"), \
             patch("main.time.sleep"), \
             pytest.raises(OperationalError):
            create_order(mock_request, sample_order, raw_body=raw_body, client_id="test-client")

        assert mock_db.commit.call_count == MAX_RAW_WRITE_RETRIES
        assert mock_db.rollback.call_count == MAX_RAW_WRITE_RETRIES


class TestNulByteHandling:

    async def test_nul_byte_stripped_before_raw_write(self, mock_request, sample_order):
        """payload 含 NUL byte（\\x00）→ 寫入前移除，資料得以落地、不回 500，並記 warning。"""
        mock_db = MagicMock()
        mock_db.commit.side_effect = [None]
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 1)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main._enqueue", return_value=True), \
             patch("main._key_func", return_value="test-ip"), \
             patch("main.time.sleep"), \
             patch.object(main.logger, "warning") as mock_warning:
            result = create_order(
                mock_request, sample_order,
                raw_body='{"order_id": "ORD-1\x00", "note": "a\x00b"}',
                client_id="c",
            )

        raw_obj = mock_db.add.call_args[0][0]
        assert "\x00" not in raw_obj.raw_payload
        assert mock_warning.called
        assert result == {"raw_id": 1, "status": "pending"}


class TestIngressRejectedHandler:

    async def test_validation_error_logged_and_returns_422(self, mock_request):
        """攝入硬閘門擋下的請求 → 記 ingress_rejected 訊號，且仍回 422。"""
        exc = RequestValidationError([
            {"loc": ("body", "order_id"), "msg": "field required", "type": "missing"},
        ])

        with patch.object(main.logger, "warning") as mock_warning:
            response = await main._on_validation_error(mock_request, exc)

        assert response.status_code == 422
        assert any(c[0][0] == "ingress_rejected" for c in mock_warning.call_args_list)


class TestHealth:

    async def test_health_returns_ok(self):
        assert await main.health() == {"status": "ok"}
