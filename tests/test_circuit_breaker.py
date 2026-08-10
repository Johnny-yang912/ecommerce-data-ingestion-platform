"""
熔斷器狀態機測試。

這裡不碰 Redis、不碰 endpoint——熔斷器是純狀態機，把它獨立出來測，
才能把「三態轉移是否正確」和「派工是否正確使用它」兩件事分開驗。

時間相關的斷言一律靠替換 reset_timeout（設 0 代表冷卻期已滿），
不用 sleep：測試不該為了驗一個時間比較而慢 30 秒。
"""

import threading

import pytest

from circuit_breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker, CircuitOpen


def make_breaker(threshold=3, reset_timeout=30.0):
    return CircuitBreaker(
        failure_threshold=threshold, reset_timeout=reset_timeout, name="test"
    )


def boom():
    raise ConnectionError("下游掛了")


# ─── closed ───────────────────────────────────────────────────────────────────

class TestClosed:

    def test_starts_closed_and_passes_through(self):
        b = make_breaker()
        assert b.state == CLOSED
        assert b.call(lambda x: x * 2, 21) == 42

    def test_failure_propagates_unchanged(self):
        """熔斷器只計數，不改變錯誤語意——原例外要原樣往上拋。"""
        b = make_breaker()
        with pytest.raises(ConnectionError):
            b.call(boom)

    def test_opens_only_after_threshold_consecutive_failures(self):
        b = make_breaker(threshold=3)
        for _ in range(2):
            with pytest.raises(ConnectionError):
                b.call(boom)
        assert b.state == CLOSED, "未達門檻不應開路"

        with pytest.raises(ConnectionError):
            b.call(boom)
        assert b.state == OPEN

    def test_success_resets_failure_count(self):
        """連續失敗語意：中間成功一次，計數就歸零，不會累積跳閘。"""
        b = make_breaker(threshold=3)
        for _ in range(2):
            with pytest.raises(ConnectionError):
                b.call(boom)
        b.call(lambda: "ok")

        for _ in range(2):
            with pytest.raises(ConnectionError):
                b.call(boom)
        assert b.state == CLOSED


# ─── open ─────────────────────────────────────────────────────────────────────

class TestOpen:

    def test_open_rejects_without_calling_fn(self):
        """
        開路的重點不是「回錯誤」，是**根本不去碰下游**——不然逾時照付，
        熔斷等於沒做。
        """
        b = make_breaker(threshold=1)
        with pytest.raises(ConnectionError):
            b.call(boom)
        assert b.state == OPEN

        calls = []
        with pytest.raises(CircuitOpen):
            b.call(lambda: calls.append(1))

        assert calls == [], "開路期間 fn 不得被呼叫"

    def test_still_open_before_cooldown_elapses(self):
        b = make_breaker(threshold=1, reset_timeout=30.0)
        with pytest.raises(ConnectionError):
            b.call(boom)
        with pytest.raises(CircuitOpen):
            b.call(lambda: "ok")
        assert b.state == OPEN


# ─── half_open ────────────────────────────────────────────────────────────────

class TestHalfOpen:

    def test_probe_allowed_after_cooldown_and_success_closes(self):
        b = make_breaker(threshold=1, reset_timeout=0.0)
        with pytest.raises(ConnectionError):
            b.call(boom)
        assert b.state == OPEN

        assert b.call(lambda: "ok") == "ok"
        assert b.state == CLOSED

    def test_failed_probe_reopens_and_restarts_cooldown(self):
        """探測失敗要重新計時，否則冷卻期會被反覆的探測侵蝕掉。"""
        b = make_breaker(threshold=1, reset_timeout=0.0)
        with pytest.raises(ConnectionError):
            b.call(boom)

        with pytest.raises(ConnectionError):
            b.call(boom)          # 這次是 half_open 的探測
        assert b.state == OPEN

        b.reset_timeout = 30.0
        with pytest.raises(CircuitOpen):
            b.call(lambda: "ok")

    def test_only_one_prober_others_still_rejected(self):
        """
        half_open 必須單飛。若所有執行緒一起衝進去探測，每條都付一次完整逾時，
        熔斷器就白做了——這正是它要消滅的行為。
        """
        b = make_breaker(threshold=1, reset_timeout=0.0)
        with pytest.raises(ConnectionError):
            b.call(boom)

        started = threading.Event()
        release = threading.Event()
        probe_calls = []

        def slow_probe():
            probe_calls.append(1)
            started.set()
            release.wait(timeout=5)
            return "ok"

        t = threading.Thread(target=lambda: b.call(slow_probe))
        t.start()
        assert started.wait(timeout=5), "探測執行緒未啟動"
        assert b.state == HALF_OPEN

        # 探測進行中，其他呼叫必須被擋下且不觸發下游
        other = []
        for _ in range(5):
            with pytest.raises(CircuitOpen):
                b.call(lambda: other.append(1))

        release.set()
        t.join(timeout=5)

        assert probe_calls == [1], "只應有一個探測者"
        assert other == []
        assert b.state == CLOSED


# ─── 併發安全 ─────────────────────────────────────────────────────────────────

class TestConcurrency:

    def test_threshold_holds_under_concurrent_failures(self):
        """
        _enqueue 跑在 asyncio.to_thread 裡，同一行程有多執行緒並行呼叫。
        大量併發失敗之後狀態必須是 open，不能因為競態卡在中間態。
        """
        b = make_breaker(threshold=3)

        def worker():
            for _ in range(10):
                try:
                    b.call(boom)
                except (ConnectionError, CircuitOpen):
                    pass

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert b.state == OPEN


# ─── 可觀測性 ─────────────────────────────────────────────────────────────────

class TestTransitionLogging:

    def test_logs_once_on_open_not_once_per_failure(self, monkeypatch):
        """
        事故期間的日誌量是實打實的成本。狀態轉移記一條、開路後不再逐筆記——
        否則每秒上千條 traceback 會把真正的訊號淹掉。
        """
        import circuit_breaker

        events = []
        monkeypatch.setattr(
            circuit_breaker.logger,
            "error",
            lambda ev, **kw: events.append(ev),
        )

        b = make_breaker(threshold=2)
        for _ in range(10):
            try:
                b.call(boom)
            except (ConnectionError, CircuitOpen):
                pass

        assert events == ["circuit_opened"]
