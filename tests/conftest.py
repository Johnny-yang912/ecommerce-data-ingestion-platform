"""
Pytest fixtures：所有測試共用的 setup。

pytest 會自動找到並注入這裡定義的 fixture，test 函式直接以參數名稱接收，
不需要 import。

⚠️ 本檔【不得】在 top-level import 專案模組或 API 相依 ⭐
────────────────────────────────────────────────────────
與 orchestration/dags/ 那條「DAG 檔不得 top-level import 專案模組」是**同一條紀律
的另一半**，理由也同構：`tests/test_dags.py` 跑在一個【只裝 Airflow + pytest】的
獨立 CI job 裡（.github/workflows/dags.yml），那裡沒有 fastapi / slowapi / sqlalchemy。
conftest 只要在 top-level 碰到它們，那個 job 的每一支測試都會在 collection 階段就
ModuleNotFoundError——**而且是全部一起死，看起來像 DAG 壞了，實際上是測試環境的事**。

這件事已經真的發生過：`0f59b93` 加入 `reset_enqueue_breaker`（import main）之後，
dags.yml 就再也跑不過了。它沒有被立刻發現，是因為該 workflow 的 paths 過濾器只涵蓋
`orchestration/**` 與 `tests/test_dags.py`——改 conftest.py 不會觸發它。
**一個只在特定路徑被改動時才執行的 CI job，它的相依必須比它的觸發條件更保守。**
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


@pytest.fixture
def mock_request() -> MagicMock:
    """Mock FastAPI Request，client.host = 127.0.0.1。

    request.body() 明確回傳真實 bytes：MagicMock(spec=Request) 會把 body() 變成
    AsyncMock，其 .decode() 預設回傳 coroutine 而非字串，會讓 create_order 內對
    payload 字串的處理（NUL 移除、長度比對）失效。
    """
    from fastapi import Request      # 見檔頭：不得 top-level import

    r = MagicMock(spec=Request)
    r.client.host = "127.0.0.1"
    r.body = AsyncMock(return_value=b'{"order_id": "TEST-001"}')
    return r


@pytest.fixture(autouse=True)
def reset_enqueue_breaker():
    """
    每個測試前重置派工熔斷器。

    它是模組層級物件，失敗計數會跨測試累積——一個「模擬 broker 掛掉」的測試
    連跑三次就會讓後面無關的測試進入開路狀態。與 reset_limiter 是同型問題。

    ⚠️ autouse 表示【每一支測試】都會執行它，包含只裝了 Airflow 的 DAG 測試 job。
    那裡 import main 必然失敗，故 ImportError 時安靜略過——沒有 main 就沒有熔斷器
    狀態需要重置，這個 fixture 對那些測試本來就無事可做。
    捕捉範圍刻意只限 ImportError：其他例外代表熔斷器真的壞了，必須讓它炸出來。
    """
    try:
        from main import _enqueue_breaker
    except ImportError:
        yield
        return

    _enqueue_breaker.reset()
    yield


@pytest.fixture
def mock_enqueue():
    """
    Patch main._enqueue（派工到 Celery 的單一出口），回傳 True 表示成功入列。

    測試一律 patch 這一層而非 process_raw_event_task：`_enqueue` 是 endpoint 與
    佇列之間的唯一接縫，patch 它就不必碰 Celery 的 app 狀態，也不需要 broker。
    """
    with patch("main._enqueue", return_value=True) as m:
        yield m


@pytest.fixture
def sample_order():
    """最小合法 OrderIN，供 main.py endpoint 測試使用。"""
    from helpers import make_sample_order      # 見檔頭：helpers 會 import schema（pydantic）

    return make_sample_order()
