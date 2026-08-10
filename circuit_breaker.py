"""
熔斷器：把「下游故障」的成本從「每次呼叫都付一次逾時」降為零。

為什麼需要它——實測（Redis 全停、4 個 uvicorn worker、`POST /orders`）：

    併發 1  → 3.8s
    併發 8  → 每筆 12.8s
    併發 48 → 47/48 筆在 120 秒內沒有完成

退化是**超線性**的，不是「每筆固定加 3.8s」：kombu 的 producer pool 每行程上限 10，
broker 不可用時每次取用都要重付連線逾時，併發越高彼此排隊越久。

對攝入路徑而言 broker 是**可選的**（派不出去就落成 pending、交給恢復掃描），
而可選的下游不該有能力拖垮必要路徑。熔斷器要做的就是把「進入 fallback 的代價」
降到比 fallback 本身還低——否則系統寧可卡死也不會退化，那不叫降級。

狀態機（標準三態）：

    closed ──連續失敗達 failure_threshold──► open
      ▲                                        │
      │                                  冷卻 reset_timeout
      │                                        ▼
      └────────探測成功──────────  half_open ──┘
                                      │
                                探測失敗 → 回 open（重新計時）

設計取捨：

- **狀態刻意是行程內的**。共享狀態得放 Redis，而 Redis 正是掛掉的那一個。代價是
  每個行程各自學習：全叢集最多付 `failure_threshold × 行程數` 次慢呼叫，之後全開路。
- **half_open 單飛**：冷卻期滿後只放**一個**呼叫去探測，其餘照樣快速拒絕。否則所有
  執行緒會同時湧入探測、每條各付一次完整逾時，等於沒有熔斷。
- **鎖只保護狀態轉移，絕不跨越被包裝的呼叫**。持有時間是微秒級；若把網路呼叫也包
  進鎖裡，熔斷器自己就變成新的序列化瓶頸——正是它要解決的那個問題。
- **時間用 `time.monotonic()`**：冷卻計時不該被系統時鐘調整（NTP、DST）影響。

可觀測性：狀態轉移各記一條 log。事故期間的日誌量因此從「每筆失敗一條 traceback」
（實測規模下每秒上千條）降為「開路一條、關路一條」，訊號不再被自己淹掉。
"""

import threading
import time
from typing import Any, Callable

import structlog

logger = structlog.get_logger()

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class CircuitOpen(Exception):
    """
    電路開路中，呼叫**根本沒有被嘗試**。

    刻意與「嘗試了但失敗」用不同型別表達：呼叫端對兩者的處置可以不同
    （例如失敗要記 error log，開路則不必——狀態轉移時已經記過一次）。
    """


class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_timeout: float, name: str) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._lock = threading.Lock()
        self._state = CLOSED
        self._failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def reset(self) -> None:
        """回到初始狀態。測試用——模組層級的熔斷器會跨測試累積狀態。"""
        with self._lock:
            self._state = CLOSED
            self._failures = 0
            self._opened_at = 0.0

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        執行 fn。開路中則直接拋 CircuitOpen，完全不呼叫 fn。

        fn 的例外原樣往上拋（熔斷器只負責計數，不改變錯誤語意）。
        """
        self._before_call()
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        self._on_success()
        return result

    # ── 狀態轉移（都在鎖內，且都不含 I/O）──────────────────────────────────

    def _before_call(self) -> None:
        with self._lock:
            if self._state == CLOSED:
                return

            if self._state == OPEN:
                if time.monotonic() - self._opened_at < self.reset_timeout:
                    raise CircuitOpen(f"{self.name} 開路中")
                # 冷卻期滿：本次呼叫成為唯一的探測者，其餘仍被擋在 half_open。
                self._state = HALF_OPEN
                logger.info("circuit_half_open", circuit=self.name)
                return

            # HALF_OPEN：已經有人在探測，不要跟著一起去撞逾時。
            raise CircuitOpen(f"{self.name} 探測中")

    def _on_failure(self) -> None:
        with self._lock:
            if self._state == HALF_OPEN:
                # 探測失敗 → 回開路並「重新」計時，冷卻期不會因為探測而縮短。
                self._state = OPEN
                self._opened_at = time.monotonic()
                logger.warning("circuit_reopened", circuit=self.name)
                return

            self._failures += 1
            if self._state == CLOSED and self._failures >= self.failure_threshold:
                self._state = OPEN
                self._opened_at = time.monotonic()
                logger.error(
                    "circuit_opened", circuit=self.name, failures=self._failures
                )

    def _on_success(self) -> None:
        with self._lock:
            if self._state == HALF_OPEN:
                logger.info("circuit_closed", circuit=self.name)
            # 連續失敗語意：只要成功一次，計數就歸零。
            self._state = CLOSED
            self._failures = 0
