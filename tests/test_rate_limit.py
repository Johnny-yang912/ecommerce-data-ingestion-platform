"""
Rate Limit 測試（per-client_id 限流）

限流主體是「認證身分（client_id）」而非「網路位置（IP）」：同一 client_id 60/min，
不同 client_id 計數器獨立。client_id 由 verify_api_key 落到 request.state，
key_func 讀取它作為限流 key（取不到才退回 IP）。

設計重點：
  - reset_limiter fixture 在每個測試前清空 in-memory 計數器，
    解決「同一分鐘內重跑測試，計數累積導致失敗」的已知問題。
  - TestClient 的 lifespan 依賴由 client fixture 統一 mock。
  - 這裡測的是「限流」而非「auth」，故以 dependency_overrides 旁路驗證層；
    但旁路後 request.state.client_id 不會被設定，所以改用 _client_key 注入點，
    讓測試以 header（X-Test-Client）模擬不同 client_id，與舊版用 X-Forwarded-For
    模擬不同 IP 是同型手法。
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

import main
from main import app


# ─── Module-level Setup ───────────────────────────────────────────────────────

def _client_key(request):
    """讀取 X-Test-Client header 作為限流 key，讓測試可以模擬不同 client_id。"""
    return request.headers.get("X-Test-Client", "testclient")

# main._key_func 是 _limiter_key 在請求時才讀取的 module 變數，
# 在模組載入時替換，讓所有測試都用 X-Test-Client 作為 key（模擬 client_id）。
main._key_func = _client_key


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
    """提供已 mock lifespan 依賴的 TestClient，每個測試拿到乾淨的 client。

    驗證層以 dependency_overrides 旁路（這裡測的是限流，不是 auth），
    讓既有測試不必每個請求都塞 X-API-Key。
    """
    from auth import verify_api_key
    app.dependency_overrides[verify_api_key] = lambda: "test-client"
    try:
        with patch("main._enqueue", return_value=True), \
             patch("main.SessionLocal", return_value=_make_order_db()):
            with TestClient(app) as c:
                yield c
    finally:
        app.dependency_overrides.pop(verify_api_key, None)


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
            headers={"X-Test-Client": "client-a"},
        )
        assert resp.status_code == 200

    def test_requests_exceeding_per_client_limit_return_429(self, client):
        """
        同一 client_id 發 62 個請求（限額 60/minute）：
          前 60 個 → 200，第 61~62 個 → 429。
        """
        cid = "client-busy"
        results = []

        for i in range(62):
            resp = client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"RL-T2-{i:04d}"},
                headers={"X-Test-Client": cid},
            )
            results.append(resp.status_code)

        assert results.count(200) == 60
        assert results.count(429) == 2

    def test_different_clients_have_independent_counters(self, client):
        """
        client-A 填滿限額（60 次），client-B 是第 1 次請求：
          client-A 第 61 次 → 429，client-B 第 1 次 → 200（計數器互不影響）。

        這正是 per-client_id 相對 per-IP 的核心差異：兩個上游即使來自同一 NAT/IP，
        計數器也獨立，不會互相誤殺。
        """
        cid_a = "client-a"
        cid_b = "client-b"

        for i in range(60):
            client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"RL-T3-A-{i:04d}"},
                headers={"X-Test-Client": cid_a},
            )

        resp_a = client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "RL-T3-A-FINAL"},
            headers={"X-Test-Client": cid_a},
        )
        resp_b = client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "RL-T3-B-001"},
            headers={"X-Test-Client": cid_b},
        )

        assert resp_a.status_code == 429  # client-A 超出限額
        assert resp_b.status_code == 200  # client-B 獨立計數，不受影響


class TestClientIdKeyFunc:
    """_client_id_key：限流 key 來源的選取邏輯（client_id 優先，缺則退回 IP）。"""

    def _make_request(self, client_id=None, host="203.0.113.7"):
        from types import SimpleNamespace
        req = MagicMock()
        req.state = SimpleNamespace() if client_id is None else SimpleNamespace(client_id=client_id)
        req.client = SimpleNamespace(host=host)
        return req

    def test_returns_client_id_when_present(self):
        """request.state.client_id 存在 → 以 client_id 為 key（非 IP）。"""
        req = self._make_request(client_id="upstream-order-api", host="203.0.113.7")
        assert main._client_id_key(req) == "upstream-order-api"

    def test_falls_back_to_ip_when_client_id_absent(self):
        """request.state.client_id 不存在（無認證路徑/防呆）→ 退回 IP。"""
        req = self._make_request(client_id=None, host="203.0.113.7")
        assert main._client_id_key(req) == "203.0.113.7"


class TestRealAuthDrivesLimiter:
    """整合測試：真實 auth 路徑 → request.state.client_id → 限流以它為 key。

    與上面的 TestRateLimiting 不同，這裡**不旁路 auth、不覆寫 _key_func**——
    送真實 X-API-Key、跑真實 verify_api_key、用真實 _client_id_key，端到端確認
    「認證身分真的被餵給限流器」這條接線沒斷。
    """

    @pytest.fixture
    def auth_client(self, monkeypatch):
        import auth
        # 還原成真實 key_func（本模組載入時把它換成了 _client_key 模擬器）
        monkeypatch.setattr(main, "_key_func", main._client_id_key)
        # 真實 key→client_id 對應：client-a 兩把 key（輪替重疊期），client-b 一把
        monkeypatch.setattr(auth, "API_KEYS", {
            "key-a": "client-a",
            "key-a2": "client-a",
            "key-b": "client-b",
        })
        with patch("main._enqueue", return_value=True), \
             patch("main.SessionLocal", return_value=_make_order_db()):
            with TestClient(app) as c:
                yield c

    def test_real_auth_path_keys_limiter_on_client_id(self, auth_client):
        """
        真實認證下：client-a 灌滿 60 → 第 61 次 429；
        client-b 同時的第 1 次仍 200（限流確實 key 在 auth 解析出的 client_id 上，
        且不同 client 計數器獨立）。
        """
        for i in range(60):
            r = auth_client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"IT-A-{i:04d}"},
                headers={"X-API-Key": "key-a"},
            )
            assert r.status_code == 200, f"第 {i} 次不應被限流"

        over_a = auth_client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "IT-A-OVER"},
            headers={"X-API-Key": "key-a"},
        )
        first_b = auth_client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "IT-B-0001"},
            headers={"X-API-Key": "key-b"},
        )

        assert over_a.status_code == 429   # client-a 超額
        assert first_b.status_code == 200  # client-b 獨立計數

    def test_rotation_keys_share_one_client_counter(self, auth_client):
        """
        同一 client-a 的兩把 key（key-a / key-a2，輪替重疊期）共用同一個計數器：
        交錯送滿 60 次都 200，第 61 次（任一把 key）429。
        這正是 per-client_id 相對 per-api_key 的關鍵性質——輪替不會偷偷讓配額加倍。
        """
        codes = []
        for i in range(60):
            key = "key-a" if i % 2 == 0 else "key-a2"
            r = auth_client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"IT-ROT-{i:04d}"},
                headers={"X-API-Key": key},
            )
            codes.append(r.status_code)

        over = auth_client.post(
            "/orders",
            json={**SAMPLE_ORDER, "order_id": "IT-ROT-OVER"},
            headers={"X-API-Key": "key-a2"},
        )

        assert codes.count(200) == 60  # 兩把 key 合計 60 都通過
        assert over.status_code == 429  # 共用一桶，第 61 次被擋


# ─── 多行程下的限流儲存 ⭐ ────────────────────────────────────────────────────
#
# 這組守的是「限額語意不隨部署形態漂移」：slowapi 預設把計數器放行程記憶體，
# API 一開多個 uvicorn worker，60/minute 就會實質變成 60×workers——而且不會有
# 任何錯誤訊息，只會安靜地放行四倍流量。

class TestLimiterStorageConfig:

    def test_storage_uri_comes_from_settings(self):
        """儲存位置必須是環境設定，不能寫死——本機/pytest 用記憶體，部署用 Redis。"""
        from config import settings
        from main import limiter

        assert limiter._storage_uri == settings.rate_limit_storage_uri

    def test_defaults_to_in_memory_storage(self):
        """
        預設（設定為空字串）落回 memory://，讓單行程開發與 pytest 不必有 Redis。
        這也是 reset_limiter fixture 能直接 _storage.reset() 的前提。
        """
        from limits.storage import MemoryStorage
        from config import settings
        from main import limiter

        if settings.rate_limit_storage_uri:
            pytest.skip("環境已指定外部限流儲存，跳過記憶體預設斷言")
        assert isinstance(limiter._storage, MemoryStorage)

    def test_in_memory_fallback_enabled(self):
        """
        Redis 掛掉時退回行程內計數，而不是整個放行。降級後限額變鬆 N 倍，
        但仍遠好過完全不限流——與 _enqueue 吞掉 broker 故障是同一套原則。
        """
        from main import limiter

        assert limiter._in_memory_fallback_enabled is True
        assert limiter._fallback_limiter is not None

    def test_storage_wait_is_bounded(self):
        """
        限流檢查跑在 request 路徑上且同步。不設逾時上限，Redis 掛掉會退到 OS 層
        的 DNS / TCP 逾時——實測單筆請求從 3.8s 惡化到 18.3s。fallback 保住的是
        正確性，保不住延遲。
        """
        from main import limiter

        opts = limiter._storage_options
        assert 0 < opts["socket_connect_timeout"] <= 5
        assert 0 < opts["socket_timeout"] <= 5

    def test_key_prefix_isolates_from_broker_keys(self):
        """限流 key 與 Celery broker 共用 Redis 實例時不得混淆（另有 DB index 隔離）。"""
        from main import limiter

        assert limiter._key_prefix == "ratelimit"
