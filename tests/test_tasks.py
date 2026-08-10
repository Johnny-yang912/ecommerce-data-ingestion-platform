"""
Celery 佇列層測試：celery_app 設定契約 + tasks 薄包裝 + main._enqueue 的失敗語意。

這裡刻意不碰真的 Redis：Celery app 的建構是 lazy 的（import 時不連線），
而 `.delay()` 一律被 patch 掉。要驗的是「派工這個接縫的行為」，不是 Redis 本身。
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

import main
from main import create_order, _enqueue
from celery_app import celery_app
from config import settings
from tasks import process_raw_event_task, scan_and_dispatch_task


# ─── celery_app 設定契約 ──────────────────────────────────────────────────────
#
# 這些不是「測試設定檔有沒有被讀到」，而是把幾個**改壞了不會當場報錯、
# 但會在 crash 當下才發現**的決策釘住（見 celery_app.py 各項註解）。

class TestCeleryConfig:

    def test_late_ack_and_reject_on_worker_lost(self):
        """acks_late + reject_on_worker_lost：worker 崩潰時訊息要回到佇列而非消失。"""
        assert celery_app.conf.task_acks_late is True
        assert celery_app.conf.task_reject_on_worker_lost is True

    def test_prefetch_multiplier_is_one(self):
        """acks_late 的標配：不預抓，否則單一 worker 崩潰會拖住一整批訊息。"""
        assert celery_app.conf.worker_prefetch_multiplier == 1

    def test_json_only_serialization(self):
        """不得接受 pickle：反序列化等同執行任意程式碼。"""
        assert celery_app.conf.task_serializer == "json"
        assert list(celery_app.conf.accept_content) == ["json"]

    def test_no_result_backend(self):
        """任務狀態的真相是 PG 的 raw.status，不另開一份會漂移的 Redis 結果狀態。"""
        assert celery_app.conf.task_ignore_result is True
        assert not celery_app.conf.result_backend

    def test_visibility_timeout_exceeds_worst_case_task_duration(self):
        """
        Redis 靠 visibility timeout 模擬重投遞，必須大於最長任務耗時。
        process_raw_event 最壞情況是三層 exponential backoff（秒級），這裡要求 ≥60s
        作為安全下限——設成個位數秒會造成任務還在跑就被重投遞的重複執行風暴。
        """
        assert celery_app.conf.broker_transport_options["visibility_timeout"] >= 60

    def test_broker_wait_is_bounded(self):
        """
        broker 不可用時的等待必須有上限。不設 socket timeout 會退回 OS 層的
        DNS / TCP 逾時——實測單次派工阻塞 19 秒，等於讓可選的下游拖垮攝入延遲。
        """
        opts = celery_app.conf.broker_transport_options
        assert 0 < opts["socket_connect_timeout"] <= 5
        assert 0 < opts["socket_timeout"] <= 5
        assert 0 < celery_app.conf.broker_connection_timeout <= 5
        assert celery_app.conf.task_publish_retry_policy["max_retries"] <= 1

    def test_tasks_module_is_included(self):
        """worker 啟動時要 import 得到 tasks，否則 delay() 送出的訊息無人認領。"""
        assert "tasks" in celery_app.conf.include

    def test_stdout_not_hijacked_by_celery(self):
        """
        Celery 預設把 stdout 導進自己的 logger 並一律記為 WARNING，
        會讓 structlog 的 info 在 worker 端全部變成 WARNING、log level 失去意義。
        """
        assert celery_app.conf.worker_redirect_stdouts is False


class TestWorkerLogging:

    def test_setup_logging_signal_applies_project_config(self):
        """
        worker 端必須套用專案自己的 structlog 設定，否則 LOG_FORMAT=json 對 worker
        無效（configure_logging 原本只有 main.py 會呼叫）。
        """
        from celery_app import _configure_worker_logging

        with patch("celery_app.configure_logging") as mock_cfg:
            _configure_worker_logging()

        mock_cfg.assert_called_once()

    def test_setup_logging_receiver_is_connected(self):
        """
        接收者必須真的接上 setup_logging：Celery 只有偵測到有人接手，
        才會放棄自己那套 logging 設定、不搶 root logger。
        """
        from celery.signals import setup_logging as setup_logging_signal

        assert setup_logging_signal.receivers


class TestWorkerProcessInit:

    def test_engine_disposed_on_worker_process_init(self):
        """
        prefork 子行程啟動時必須 dispose 繼承來的 engine。
        漏了這步不會有任何錯誤訊息，只會在多行程共用 socket 時偶發資料串線。
        """
        from celery_app import _dispose_inherited_engine

        with patch("database.engine") as mock_engine:
            _dispose_inherited_engine()

        mock_engine.dispose.assert_called_once()


# ─── tasks 薄包裝 ─────────────────────────────────────────────────────────────

class TestProcessRawEventTask:

    def test_delegates_to_process_raw_event(self):
        """task 只做委派，不含任何業務邏輯。"""
        with patch("tasks.process_raw_event") as mock_fn:
            process_raw_event_task(123)

        mock_fn.assert_called_once_with(123)

    def test_task_name_is_pinned(self):
        """
        task 名稱是 broker 上的線路契約：若跟著函式名浮動，改名會讓佇列裡
        既有的訊息在 worker 端變成 NotRegistered。
        """
        assert process_raw_event_task.name == "tasks.process_raw_event"

    def test_no_celery_level_retry_configured(self):
        """
        不得疊 Celery retry：process.py 已有四層 retry，再加一層會變成重試放大，
        且 process_raw_event 不對外拋例外，Celery 根本看不到失敗。
        """
        assert process_raw_event_task.max_retries in (None, 0) or \
            not getattr(process_raw_event_task, "autoretry_for", ())


# ─── Beat 排程：週期恢復掃描 ──────────────────────────────────────────────────

class TestBeatSchedule:

    def test_recovery_scan_is_scheduled(self):
        """
        恢復掃描必須真的排進 Beat。漏了這條，佇列本身救不回來的那一半
        （crash 在 claim commit 之後、卡在 processing 的記錄）會永久卡死。
        """
        entry = celery_app.conf.beat_schedule["recovery-scan"]
        assert entry["task"] == "tasks.scan_and_dispatch"

    def test_schedule_interval_follows_settings(self):
        """掃描間隔是環境設定（scan_interval_seconds），不得寫死在排程裡。"""
        entry = celery_app.conf.beat_schedule["recovery-scan"]
        assert entry["schedule"] == float(settings.scan_interval_seconds)

    def test_beat_startup_triggers_immediate_scan(self):
        """
        Beat 的第一次 tick 要等滿一個間隔才發生。啟動時若不補一次掃描，
        上一輪殘留的 pending / stale processing 會多躺一個 scan_interval 沒人管。
        """
        from celery_app import _initial_recovery_scan

        with patch("tasks.scan_and_dispatch_task") as mock_task:
            _initial_recovery_scan()

        mock_task.delay.assert_called_once_with()


# ─── 掃描 → 派工 ──────────────────────────────────────────────────────────────

class TestScanAndDispatchTask:

    def test_dispatches_every_scanned_id(self):
        with patch("tasks.scan_and_recover", return_value=[10, 20, 30]), \
             patch("tasks.process_raw_event_task") as mock_task:
            count = scan_and_dispatch_task()

        assert count == 3
        assert [c[0][0] for c in mock_task.delay.call_args_list] == [10, 20, 30]

    def test_no_records_dispatches_nothing(self):
        with patch("tasks.scan_and_recover", return_value=[]), \
             patch("tasks.process_raw_event_task") as mock_task:
            count = scan_and_dispatch_task()

        assert count == 0
        mock_task.delay.assert_not_called()

    def test_dispatch_failure_propagates(self):
        """
        派工失敗不吞：記錄仍是 pending，下一輪掃描會原封不動再撈一次
        （scan_and_recover 冪等）。吞掉只會讓失敗變成看不見的靜默。
        """
        with patch("tasks.scan_and_recover", return_value=[1]), \
             patch("tasks.process_raw_event_task") as mock_task:
            mock_task.delay.side_effect = ConnectionError("redis is down")

            with pytest.raises(ConnectionError):
                scan_and_dispatch_task()


class TestApiProcessHoldsNoBackgroundState:

    def test_app_has_no_lifespan_recovery(self):
        """
        API 行程不得再持有背景掃描狀態——那是 lifespan 迴圈時代的東西，
        多開一個 uvicorn worker 就會多跑一份掃描，擋住水平擴展。
        """
        assert not hasattr(main, "_periodic_scan")
        assert not hasattr(main, "scan_and_recover")


# ─── main._enqueue 的失敗語意 ─────────────────────────────────────────────────

class TestEnqueue:

    def test_returns_true_on_success(self):
        with patch("main.process_raw_event_task") as mock_task:
            assert _enqueue(7) is True

        mock_task.delay.assert_called_once_with(7)

    def test_broker_failure_is_swallowed_and_logged(self):
        """broker 掛掉 → 回 False 並記 error，不得往上拋。"""
        with patch("main.process_raw_event_task") as mock_task, \
             patch.object(main.logger, "error") as mock_error:
            mock_task.delay.side_effect = ConnectionError("redis is down")
            result = _enqueue(7)

        assert result is False
        assert mock_error.called


class TestEnqueueCircuitBreaker:

    def test_breaker_opens_and_stops_touching_the_broker(self):
        """
        連續失敗達門檻後不得再呼叫 delay()。開路的價值全在這裡——碰了就要付逾時，
        實測 48 併發下那個逾時會讓 47 筆請求在 120 秒內拿不到回應。
        """
        threshold = main.ENQUEUE_BREAKER_FAILURE_THRESHOLD

        with patch("main.process_raw_event_task") as mock_task:
            mock_task.delay.side_effect = ConnectionError("redis is down")
            for _ in range(threshold + 20):
                assert _enqueue(1) is False

        assert mock_task.delay.call_count == threshold
        assert main._enqueue_breaker.state == "open"

    def test_open_circuit_does_not_log_per_request(self):
        """
        開路後逐筆記 error 等於把事故期間的日誌淹成雜訊（實測規模下每秒上千條）。
        開路那一刻已由熔斷器記過一次，之後應該安靜。
        """
        threshold = main.ENQUEUE_BREAKER_FAILURE_THRESHOLD

        with patch("main.process_raw_event_task") as mock_task:
            mock_task.delay.side_effect = ConnectionError("redis is down")
            for _ in range(threshold):
                _enqueue(1)

            with patch.object(main.logger, "error") as mock_error:
                for _ in range(50):
                    assert _enqueue(1) is False

        mock_error.assert_not_called()

    def test_recovers_after_cooldown(self):
        """broker 復原後要能自己回到快路徑，不需要重啟行程。"""
        with patch("main.process_raw_event_task") as mock_task:
            mock_task.delay.side_effect = ConnectionError("redis is down")
            for _ in range(main.ENQUEUE_BREAKER_FAILURE_THRESHOLD):
                _enqueue(1)
            assert main._enqueue_breaker.state == "open"

            main._enqueue_breaker.reset_timeout = 0.0   # 視為冷卻期已滿
            mock_task.delay.side_effect = None
            assert _enqueue(1) is True

        assert main._enqueue_breaker.state == "closed"


class TestCreateOrderEnqueueContract:

    async def test_still_returns_pending_when_broker_is_down(self, mock_request, sample_order):
        """
        broker 掛掉不得讓 POST /orders 回 500：Raw 已經 commit 落地了。
        回 500 會讓上游重送、灌出一批同 order_id 的 Raw 全變 duplicate 雜訊，
        而資料其實早就收下了。未入列者由 recovery scan 接手。
        """
        mock_db = MagicMock()
        mock_db.commit.side_effect = [None]
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 555)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main.process_raw_event_task") as mock_task, \
             patch("main._key_func", return_value="test-ip"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_task.delay.side_effect = ConnectionError("redis is down")
            result = await create_order(mock_request, sample_order, client_id="test-client")

        assert result == {"raw_id": 555, "status": "pending"}
        assert mock_db.commit.call_count == 1

    async def test_enqueue_happens_after_commit(self, mock_request, sample_order):
        """
        派工必須在 commit 之後：worker 走另一條 DB 連線，先派工可能讓它
        讀不到還沒 commit 的 Raw，claim 直接落空。
        """
        call_order = []
        mock_db = MagicMock()
        mock_db.commit.side_effect = lambda: call_order.append("commit")
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 1)

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main.process_raw_event_task") as mock_task, \
             patch("main._key_func", return_value="test-ip"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_task.delay.side_effect = lambda _: call_order.append("enqueue")
            await create_order(mock_request, sample_order, client_id="test-client")

        assert call_order == ["commit", "enqueue"]

    async def test_session_closed_before_enqueue(self, mock_request, sample_order):
        """
        DB session 必須在派工**之前**收掉。db.refresh() 會開一個新交易，若讓它跨越
        派工阻塞，broker 故障期間連線會整段掛在 `idle in transaction`（實測 60 併發
        下 32 個 pool 槽位有 23 個是這狀態），同時壓住 Postgres 的 vacuum horizon。
        """
        call_order = []
        mock_db = MagicMock()
        mock_db.commit.side_effect = lambda: call_order.append("commit")
        mock_db.refresh.side_effect = lambda obj: setattr(obj, "id", 1)
        mock_db.close.side_effect = lambda: call_order.append("close")

        with patch("main.SessionLocal", return_value=mock_db), \
             patch("main.process_raw_event_task") as mock_task, \
             patch("main._key_func", return_value="test-ip"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_task.delay.side_effect = lambda _: call_order.append("enqueue")
            await create_order(mock_request, sample_order, client_id="test-client")

        assert call_order.index("close") < call_order.index("enqueue")
