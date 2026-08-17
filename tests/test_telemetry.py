"""
telemetry.py 與 logging_config 的 trace 注入。

這裡測的**不是** OTel SDK 本身會不會運作（那是上游的責任），而是三件本專案自己
決定的事：
  1. `otel_enabled` 關閉時，**什麼都不做**——這是 pytest 與本機開發不受影響的前提。
  2. 開啟時，該掛的 instrumentor 一個都不能漏，特別是 **Celery 在派工端也要掛**：
     漏了它 worker 會開出一條新 trace，兩段各自看起來都正常、只是接不起來。
  3. trace_id 以 W3C 的 32/16 位十六進位注入 log——格式錯了在 Tempo 就查不到。

全程 patch 掉 exporter 與 `set_tracer_provider`：真的設定全域 provider 會洩漏到
同一個行程裡的其他測試（provider 是 process-global 且只能設定一次）。
"""

import pytest
from unittest.mock import MagicMock, patch
from opentelemetry.sdk.metrics.view import DropAggregation

import logging_config
import telemetry
from config import settings


@pytest.fixture
def otel_on(monkeypatch):
    """把 otel_enabled 暫時打開。"""
    monkeypatch.setattr(settings, "otel_enabled", True)


@pytest.fixture
def patched_sdk():
    """
    把所有會產生副作用的 OTel 進入點換成 mock。

    兩個 `set_*_provider` 都必須 patch：它們寫的是行程全域狀態，一旦設成真的
    provider 就無法還原——**而 metrics 那側更嚴重**：真的 MeterProvider 會啟動
    一條週期匯出執行緒，在測試行程裡對著不存在的 :4318 重試，直到 pytest 關掉
    stdout 之後還在寫 log（實測會噴一整頁 `I/O operation on closed file`）。
    """
    with patch.object(telemetry, "OTLPSpanExporter") as exporter, \
         patch.object(telemetry, "BatchSpanProcessor") as processor, \
         patch.object(telemetry.trace, "set_tracer_provider") as set_provider, \
         patch.object(telemetry, "OTLPMetricExporter") as metric_exporter, \
         patch.object(telemetry, "PeriodicExportingMetricReader") as reader, \
         patch.object(telemetry, "MeterProvider") as meter_provider, \
         patch.object(telemetry.metrics, "set_meter_provider") as set_meter_provider, \
         patch.object(telemetry, "CeleryInstrumentor") as celery_inst, \
         patch.object(telemetry, "SQLAlchemyInstrumentor") as sa_inst, \
         patch.object(telemetry, "RedisInstrumentor") as redis_inst:
        yield {
            "exporter": exporter,
            "processor": processor,
            "set_provider": set_provider,
            "metric_exporter": metric_exporter,
            "reader": reader,
            "meter_provider": meter_provider,
            "set_meter_provider": set_meter_provider,
            "celery": celery_inst,
            "sqlalchemy": sa_inst,
            "redis": redis_inst,
        }


# ── configure_telemetry ────────────────────────────────────────────────────

def test_configure_telemetry_disabled_does_nothing(patched_sdk):
    """
    關閉時完全不動作。

    這條保障的是「本機 pytest / uvicorn --reload 不需要 Collector 在跑」——
    只要這裡漏掉 early return，每個測試行程都會開一條背景執行緒去連 4318。
    """
    assert settings.otel_enabled is False      # 預設值，不是這支測試設的

    telemetry.configure_telemetry("order-api")

    patched_sdk["set_provider"].assert_not_called()
    patched_sdk["set_meter_provider"].assert_not_called()
    patched_sdk["celery"].assert_not_called()
    patched_sdk["sqlalchemy"].assert_not_called()
    patched_sdk["redis"].assert_not_called()


def test_configure_telemetry_enabled_sets_provider_with_service_name(otel_on, patched_sdk):
    """開啟時建立 provider，且 service.name 來自參數而非環境變數。"""
    telemetry.configure_telemetry("order-worker")

    patched_sdk["set_provider"].assert_called_once()
    provider = patched_sdk["set_provider"].call_args.args[0]
    assert provider.resource.attributes["service.name"] == "order-worker"


def test_configure_telemetry_enabled_instruments_all_libraries(otel_on, patched_sdk):
    """
    三個 instrumentor 都要掛上。

    Celery 這條特別重要且特別容易漏：多數教學只在 worker 掛，但派工端沒掛的話
    trace context 不會寫進訊息 header——這正是本階段的驗收條件。
    """
    telemetry.configure_telemetry("order-api")

    patched_sdk["celery"].return_value.instrument.assert_called_once()
    patched_sdk["redis"].return_value.instrument.assert_called_once()
    # SQLAlchemy 綁在既有 engine 上（非全域 patch），故必須帶 engine 參數
    assert "engine" in patched_sdk["sqlalchemy"].return_value.instrument.call_args.kwargs


def test_configure_telemetry_enabled_sets_meter_provider_with_views(otel_on, patched_sdk):
    """
    Metrics 與 traces 共用同一個 Resource，且帶上 View。

    View 是本專案控制序列數的**唯一**手段（Collector 的 filter 留作緊急拉桿），
    漏掉它的後果是額度被兩個 size histogram 吃掉約 2,160 條序列——而那不會報錯。
    """
    telemetry.configure_telemetry("order-api")

    patched_sdk["set_meter_provider"].assert_called_once()
    kwargs = patched_sdk["meter_provider"].call_args.kwargs
    assert kwargs["resource"].attributes["service.name"] == "order-api"
    assert kwargs["views"] is telemetry._VIEWS


def test_views_drop_useless_histograms_and_rebucket_duration():
    """
    View 的內容各有不可省的理由：

    - 兩個 size histogram：答不出任何人會問的問題，卻是最貴的兩項。
    - flower.task.runtime.seconds：CeleryInstrumentor 自動送，實測佔 27% 的序列，
      卻用毫秒級 bucket 量秒級的值 → 每筆都落第一桶，有資料零解析度。
    - duration 改 bucket：SDK 預設邊界是毫秒級的 (0, 5, 10, ... 10000)，
      而這個指標的單位是秒、實測值 0.0288——不改的話每一筆都落進第一個桶。

    ⚠️ 這份清單是【序列預算的唯一防線】。少一條不會有任何錯誤訊息，只會讓
    active series 悄悄長大，直到帳單或截流才發現。
    """
    dropped = {
        v._instrument_name for v in telemetry._VIEWS
        if isinstance(v._aggregation, DropAggregation)
    }
    assert dropped == {
        "http.server.request.size",
        "http.server.response.size",
        "flower.task.runtime.seconds",
    }

    duration_view = next(
        v for v in telemetry._VIEWS
        if v._instrument_name == "orders.processing.duration"
    )
    assert duration_view._aggregation._boundaries == telemetry.PROCESSING_DURATION_BUCKETS
    # 解析度必須落在實測值（0.0288 秒）附近，否則等於沒有分桶
    assert min(telemetry.PROCESSING_DURATION_BUCKETS) <= 0.0288
    assert max(telemetry.PROCESSING_DURATION_BUCKETS) >= 10.0


# ── observe_circuit_breaker ────────────────────────────────────────────────

def test_circuit_breaker_gauge_reports_one_hot_state():
    """
    三個狀態各一條序列，只有當前狀態是 1。

    形狀刻意不是「一條序列、值 0/1/2」：那要求查詢端記住哪個數字代表哪個狀態，
    而告警規則把數字寫錯是不會報錯的。
    """
    from circuit_breaker import CircuitBreaker, CLOSED, OPEN

    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=30, name="test_breaker")
    captured = {}
    with patch.object(telemetry._meter, "create_observable_gauge") as gauge:
        telemetry.observe_circuit_breaker(breaker)
    captured["cb"] = gauge.call_args.kwargs["callbacks"][0]

    observations = {
        obs.attributes["state"]: obs.value for obs in captured["cb"](None)
    }
    assert observations[CLOSED] == 1
    assert observations[OPEN] == 0
    assert sum(observations.values()) == 1, "任何時刻只能有一個狀態是 1"

    # 開路之後，1 必須跟著移動到 open
    breaker._on_failure()
    observations = {
        obs.attributes["state"]: obs.value for obs in captured["cb"](None)
    }
    assert observations[OPEN] == 1
    assert observations[CLOSED] == 0


# ── instrument_fastapi ─────────────────────────────────────────────────────

def test_instrument_fastapi_disabled_is_noop():
    """關閉時不掛 middleware——與 configure_telemetry 共用同一個開關語意。"""
    app = MagicMock()
    with patch("opentelemetry.instrumentation.fastapi.FastAPIInstrumentor") as inst:
        telemetry.instrument_fastapi(app)
    inst.instrument_app.assert_not_called()


def test_instrument_fastapi_enabled_instruments_app(otel_on):
    app = MagicMock()
    with patch("opentelemetry.instrumentation.fastapi.FastAPIInstrumentor") as inst:
        telemetry.instrument_fastapi(app)
    inst.instrument_app.assert_called_once_with(app)


# ── logging_config 的 trace 注入 ───────────────────────────────────────────

def test_add_trace_context_without_active_span_adds_nothing():
    """
    沒有有效 span 時不加欄位。

    這同時涵蓋「OTel 關閉」的情境：此時 get_current_span() 回傳 no-op span，
    其 context 的 is_valid 為 False——所以注入函式不需要自己判斷開關。
    """
    event = {"event": "test"}
    assert logging_config._add_trace_context(None, "info", event) == {"event": "test"}


def test_add_trace_context_injects_w3c_hex_ids():
    """
    有效 span 時注入 32/16 位補零小寫十六進位。

    格式是硬需求而非美觀問題：Tempo 查的是這個字串，直接輸出 int 會查不到。
    刻意挑一個高位為 0 的 id 來驗證補零沒有被省略。
    """
    from opentelemetry import trace as otel_trace

    ctx = otel_trace.SpanContext(
        trace_id=0x0000AABBCCDDEEFF00112233445566,
        span_id=0x00FF112233445566,
        is_remote=False,
        trace_flags=otel_trace.TraceFlags(otel_trace.TraceFlags.SAMPLED),
    )
    event = {"event": "test"}
    with otel_trace.use_span(otel_trace.NonRecordingSpan(ctx), end_on_exit=False):
        result = logging_config._add_trace_context(None, "info", event)

    assert result["trace_id"] == "000000aabbccddeeff00112233445566"
    assert result["span_id"] == "00ff112233445566"
    assert len(result["trace_id"]) == 32 and len(result["span_id"]) == 16
