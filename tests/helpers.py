"""
Shared test helpers: mock factory functions and common test data.

這個模組放所有「會被多個測試檔引用」的純 Python 工具函式。
不是 pytest fixture，直接 import 使用。
"""

import json
from collections import deque
from datetime import date
from unittest.mock import MagicMock

from schema import OrderIN, CustomerInfo, AddressInfo, OrderItemInfo, ProductInfo, PaymentInfo


# ─── Common Test Data ─────────────────────────────────────────────────────────

VALID_PAYLOAD = {
    "order_id": "ORD-TEST",
    "order_date": "2024-01-01",
    "customer": {"customer_id": "CUST-001"},
    "address": {},
    "items": [{"product": {"product_id": "PROD-001"}, "quantity": 1, "unit_price": 100.0}],
    "payment": {},
}


def make_sample_order(**overrides) -> OrderIN:
    """建立一個最小合法的 OrderIN，可用 overrides 替換任意欄位。"""
    defaults = dict(
        order_id="TEST-001",
        order_date=date(2024, 1, 1),
        customer=CustomerInfo(customer_id="CUST-001"),
        address=AddressInfo(),
        items=[OrderItemInfo(
            product=ProductInfo(product_id="PROD-001"),
            quantity=1,
            unit_price=100.0,
        )],
        payment=PaymentInfo(),
    )
    defaults.update(overrides)
    return OrderIN(**defaults)


# ─── Mock DB Helpers ──────────────────────────────────────────────────────────

def make_mock_db(exec_results: list, commit_effects: list) -> MagicMock:
    """
    建立 mock DB session。

    exec_results  : list，依呼叫順序逐一作為 db.execute() 的回傳值。
    commit_effects: list，None 代表成功，Exception instance 代表拋出。
    """
    mock_db = MagicMock()
    queue = deque(exec_results)
    mock_db.execute.side_effect = lambda stmt: queue.popleft() if queue else MagicMock()
    mock_db.commit.side_effect = commit_effects
    return mock_db


def claim_result(rowcount: int = 1) -> MagicMock:
    """Mock CAS UPDATE 結果（try_claim_raw 用）。rowcount=1 代表搶到。"""
    r = MagicMock()
    r.rowcount = rowcount
    return r


def select_result(obj) -> MagicMock:
    """Mock SELECT 結果，scalar_one_or_none() 和 scalar_one() 都回傳 obj。"""
    r = MagicMock()
    r.scalar_one_or_none.return_value = obj
    r.scalar_one.return_value = obj
    return r


def no_ods_result() -> MagicMock:
    """Mock ODS pre-check：查無此 order_id（scalar_one_or_none → None）。"""
    r = MagicMock()
    r.scalar_one_or_none.return_value = None
    return r


def existing_ods_result(existing_raw_id: int = 42) -> MagicMock:
    """Mock ODS pre-check：order_id 已存在，回傳帶 raw_id 的 mock ODS。"""
    mock_ods = MagicMock()
    mock_ods.raw_id = existing_raw_id
    r = MagicMock()
    r.scalar_one_or_none.return_value = mock_ods
    return r


def scalars_result(ids: list) -> MagicMock:
    """Mock SELECT 結果，scalars().all() 回傳 ids list（scan 用）。"""
    r = MagicMock()
    r.scalars.return_value.all.return_value = list(ids)
    return r


def make_mock_raw(payload: dict = None) -> MagicMock:
    """建立 mock Raw ORM 物件，raw_payload 為 JSON 字串。"""
    if payload is None:
        payload = VALID_PAYLOAD
    r = MagicMock()
    r.id = 1
    r.order_id = "ORD-TEST"
    r.raw_payload = json.dumps(payload)
    return r
