"""
Pytest fixtures：所有測試共用的 setup。

pytest 會自動找到並注入這裡定義的 fixture，test 函式直接以參數名稱接收，
不需要 import。
"""

import pytest
from unittest.mock import MagicMock, AsyncMock
from fastapi import BackgroundTasks, Request

from helpers import make_sample_order


@pytest.fixture
def mock_request() -> MagicMock:
    """Mock FastAPI Request，client.host = 127.0.0.1。

    request.body() 明確回傳真實 bytes：MagicMock(spec=Request) 會把 body() 變成
    AsyncMock，其 .decode() 預設回傳 coroutine 而非字串，會讓 create_order 內對
    payload 字串的處理（NUL 移除、長度比對）失效。
    """
    r = MagicMock(spec=Request)
    r.client.host = "127.0.0.1"
    r.body = AsyncMock(return_value=b'{"order_id": "TEST-001"}')
    return r


@pytest.fixture
def mock_bg() -> MagicMock:
    """Mock FastAPI BackgroundTasks。"""
    return MagicMock(spec=BackgroundTasks)


@pytest.fixture
def sample_order():
    """最小合法 OrderIN，供 main.py endpoint 測試使用。"""
    return make_sample_order()
