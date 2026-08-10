"""
API Key 驗證測試（Phase 3 - Option A：靜態 key + 多 key 輪替）

驗證 X-API-Key header 與血緣落地：
  - 缺 header        → 401
  - 無效 key         → 401
  - 有效 key         → 200，且 source_client_id 正確落地到 Raw
  - 同一 client 多把 key（輪替重疊期）→ 都有效，對應同一 client_id

說明：這裡走真實的驗證路徑（不像 test_rate_limit 旁路 dependency），
DB 以 mock 取代，透過攔截 db.add() 檢查 Raw.source_client_id。
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

import auth
from main import app


SAMPLE_ORDER = {
    "order_id": "AUTH-001",
    "order_date": "2024-01-01",
    "customer": {"customer_id": "CUST-001"},
    "address": {},
    "items": [{"product": {"product_id": "PROD-001"}, "quantity": 1, "unit_price": 100.0}],
    "payment": {},
}


@pytest.fixture
def added_objects():
    """收集所有被 db.add() 的 ORM 物件，供斷言 source_client_id。"""
    return []


@pytest.fixture
def client(added_objects, monkeypatch):
    # 固定一組測試用對應：同一 client 兩把 key（輪替），另一 client 一把
    monkeypatch.setattr(auth, "API_KEYS", {
        "key-new": "upstream-order-api",
        "key-old": "upstream-order-api",
        "key-partner": "partner-b",
    })

    def _make_db():
        db = MagicMock()
        db.add.side_effect = lambda obj: added_objects.append(obj)
        db.refresh.side_effect = lambda obj: setattr(obj, "id", 999)
        return db

    with patch("main._enqueue", return_value=True), \
         patch("main.SessionLocal", side_effect=_make_db):
        with TestClient(app) as c:
            yield c


class TestApiKeyAuth:

    def test_missing_api_key_returns_401(self, client):
        """無 X-API-Key header → 401。"""
        resp = client.post("/orders", json=SAMPLE_ORDER)
        assert resp.status_code == 401

    def test_invalid_api_key_returns_401(self, client):
        """錯誤的 key → 401。"""
        resp = client.post("/orders", json=SAMPLE_ORDER, headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401

    def test_valid_api_key_succeeds_and_persists_client_id(self, client, added_objects):
        """有效 key → 200，且 Raw.source_client_id = 對應的 client_id。"""
        resp = client.post("/orders", json=SAMPLE_ORDER, headers={"X-API-Key": "key-new"})
        assert resp.status_code == 200
        raws = [o for o in added_objects if hasattr(o, "source_client_id")]
        assert raws, "應有 Raw 物件被 add"
        assert raws[0].source_client_id == "upstream-order-api"

    def test_rotation_old_and_new_keys_map_to_same_client(self, client, added_objects):
        """輪替重疊期：同一 client 的新舊兩把 key 都有效，且對應同一 client_id。"""
        for key in ("key-new", "key-old"):
            resp = client.post(
                "/orders",
                json={**SAMPLE_ORDER, "order_id": f"AUTH-{key}"},
                headers={"X-API-Key": key},
            )
            assert resp.status_code == 200

        clients = {o.source_client_id for o in added_objects if hasattr(o, "source_client_id")}
        assert clients == {"upstream-order-api"}


class TestParseApiKeys:
    """_parse_api_keys：多 key、空值、格式錯誤項目的容錯。"""

    def test_none_and_empty_return_empty_dict(self):
        assert auth._parse_api_keys(None) == {}
        assert auth._parse_api_keys("") == {}

    def test_multiple_keys_same_client(self):
        result = auth._parse_api_keys("k1:client_a, k2:client_a , k3:client_b")
        assert result == {"k1": "client_a", "k2": "client_a", "k3": "client_b"}

    def test_malformed_entries_are_skipped(self):
        # 無冒號、空白項、缺 key、缺 client_id → 全部略過，只留合法項
        result = auth._parse_api_keys("no_colon, ,:client_x,key_only:,good:client_ok")
        assert result == {"good": "client_ok"}
