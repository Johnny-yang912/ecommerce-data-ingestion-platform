"""
Rate Limit 測試（遷移自 test_rate_limit.py）

per-IP 限流（slowapi）：同 IP 60/min，不同 IP 計數器獨立。

改進點（相對舊版）：
  - reset_limiter fixture 在每個測試前清空 in-memory 計數器，
    解決「同一分鐘內重跑測試，計數累積導致失敗」的已知問題。
  - TestClient 的 lifespan 依賴由 client fixture 統一 mock，
    不需要每個測試自己寫 patch。
  - IP 模擬仍使用 X-Forwarded-For + _xfwd_key，邏輯與舊版相同。
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

import main
from main import app


# ─── Module-level Setup ───────────────────────────────────────────────────────

def _xfwd_key(request):
    """讀取 X-Forwarded-For header 作為限流 key，讓測試可以模擬不同 IP。"""
    return request.headers.get("X-Forwarded-For", "testclient")

# main._key_func 是 _limiter_key 在請求時才讀取的 module 變數，
# 在模組載入時替換，讓所有測試都用 X-Forwarded-For 作為 key。
main._key_func = _xfwd_key


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _make_order_db():
    mock_db = MagicMock()
    mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 999)
    return mock_db


@pytest.fixture(autouse=True)
def reset_limiter():
    """
    每個測試前清空 slowapi 的 in-memory 計數器。
    根本解決「同一分鐘內重跑，計數累積」的問題。
    """
    from main import limiter
    limiter._storage.reset()
    yield


@pytest.fixture
def client():
    """提供已 mock lifespan 依賴的 TestClient，每個測試拿到乾淨的 client。"""
    with patch("main.scan_and_recover", return_value=[]), \
         patch("main.process_raw_event"), \
         patch("main.SessionLocal", return_value=_make_order_db()):
        with TestClient(app) as c:
            yield c


SAMPLE_ORDER = {
    "order_id": "RL-BASE",
    "order_date": "2024-01-01",
    "customer": {"customer_id": "CUST-001"},
    "address": {},
    "items": [{"product": {"product_id": "PROD-001"}, "quantity": 1, "unit_price": 100.0}],
    "payment": {},
}


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestRateLimiting:

    def test_request_within_limit_succeeds(self, client):
        """限額內的請求應回傳 200。"""
        resp = client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "RL-T1-001"},
            headers={"X-Forwarded-For": "10.1.0.1"},
        )
        assert resp.status_code == 200

    def test_requests_exceeding_per_ip_limit_return_429(self, client):
        """
        同一 IP 發 62 個請求（限額 60/minute）：
          前 60 個 → 200，第 61~62 個 → 429。
        """
        ip = "10.2.0.1"
        results = []

        for i in range(62):
            resp = client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"RL-T2-{i:04d}"},
                headers={"X-Forwarded-For": ip},
            )
            results.append(resp.status_code)

        assert results.count(200) == 60
        assert results.count(429) == 2

    def test_different_ips_have_independent_counters(self, client):
        """
        IP-A 填滿限額（60 次），IP-B 是第 1 次請求：
          IP-A 第 61 次 → 429，IP-B 第 1 次 → 200（計數器互不影響）。
        """
        ip_a = "10.3.0.1"
        ip_b = "10.3.0.2"

        for i in range(60):
            client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"RL-T3-A-{i:04d}"},
                headers={"X-Forwarded-For": ip_a},
            )

        resp_a = client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "RL-T3-A-FINAL"},
            headers={"X-Forwarded-For": ip_a},
        )
        resp_b = client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "RL-T3-B-001"},
            headers={"X-Forwarded-For": ip_b},
        )

        assert resp_a.status_code == 429  # IP-A 超出限額
        assert resp_b.status_code == 200  # IP-B 獨立計數，不受影響
