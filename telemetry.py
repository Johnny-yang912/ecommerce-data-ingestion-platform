"""
OpenTelemetry 設定：遙測的單一初始化入口。

結構刻意與 logging_config.py 同構——一個設定模組、一個 `configure_*()`，由各行程
在自己的生命週期掛載點上顯式呼叫。理由不是「一致好看」，而是**生命週期控制**：

為什麼不用 `opentelemetry-instrument` CLI 包住啟動指令（zero-code 埋點）：
  那個做法在直譯器啟動時就初始化，對 Celery prefork 而言是**父行程**。而
  `BatchSpanProcessor` 是背景執行緒，**執行緒不會被 fork 繼承**——子行程會拿到
  一個有佇列卻沒有搬運工的 processor，span 堆在記憶體裡永遠送不出去，而且
  **不會有任何錯誤訊息**。這與 celery_app._dispose_inherited_engine 處理的是
  同一個成因（fork 繼承了不該繼承的資源），因此用同一個掛載點解決。

設定邊界（與 config.py 的分工）：
  - `settings.otel_enabled` 只回答「開不開」——那是專案自己的部署決定。
  - 「往哪送、怎麼取樣、資源屬性」一律走 OTel 規格的標準 `OTEL_*` 環境變數，
    由 SDK 自己讀。不在 config.py 複製一份，避免第二份會漂移的真相。
  - **例外是 service_name**：它由呼叫端以參數傳入，不走 `OTEL_SERVICE_NAME`。
    api / worker / beat 共用同一個映像與同一份 compose environment，靠環境變數
    區分的話，漏設就是三個服務在 Grafana 裡合併成一個——而那是靜默的、
    要等到看 service map 才發現。參數傳入則漏了就是 TypeError。
"""

import structlog
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (
    DropAggregation,
    ExplicitBucketHistogramAggregation,
    View,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from circuit_breaker import CLOSED, HALF_OPEN, OPEN
from config import settings

logger = structlog.get_logger()


# ── Metrics：instrument 定義 ───────────────────────────────────────────────
#
# 為什麼可以在模組層建立、而不必等 MeterProvider 設好：
#   provider 尚未設定時 `get_meter()` 回傳 `_ProxyMeter`，其 instrument 是
#   `_ProxyCounter` 等代理物件，會在 provider 設定後自動接上真正的實作（已實測）。
#   因此**埋點處完全不需要判斷 `otel_enabled`**——關閉時就是 no-op，跟
#   logging_config 的 trace 注入是同一個技巧。
#   ⚠️ 唯一代價：provider 設定【之前】的計數會被丟棄（實測確認）。實務影響為零——
#     worker 子行程在 worker_process_init 之後才收任務，API 在 import 期沒有業務事件。

_meter = metrics.get_meter("orders.ingestion")

# 處理結果。error / duplicate 的七個終態全部匯流到 process._commit_raw_status，
# 只有 processed 走另一條路（process.py 的成功分支），故埋點只有兩處。
ORDERS_RESULT = _meter.create_counter(
    "orders.raw.result",
    unit="1",
    description="Raw 記錄進入終態的次數，依 processed / error / duplicate 分",
)

# 單筆處理耗時。只在【真的 claim 到】時記錄——claim 落空是毫秒級的 no-op，
# 混進來會在有競爭時把分佈整個往左拉，讓 P95 失去意義。
PROCESSING_DURATION = _meter.create_histogram(
    "orders.processing.duration",
    unit="s",
    description="process_raw_event 單筆耗時（僅計 claim 成功者）",
)

# 四層 retry 各自的發生次數。這是「系統正在吃力」的最早訊號——它會在錯誤率
# 上升【之前】先動，因為 retry 成功的那些不會留下任何終態痕跡。
RETRIES = _meter.create_counter(
    "orders.retry",
    unit="1",
    description="重試次數，依 raw_write / claim / processing / status_update 分",
)

# 恢復掃描實際派出的筆數。持續 > 0 代表攝入路徑的即時派工正在漏，
# 而那在 raw.status 上看不出來（補派成功的話終態一樣是 processed）。
RECOVERY_DISPATCHED = _meter.create_counter(
    "recovery_scan.dispatched",
    unit="1",
    description="週期恢復掃描補派出去的筆數",
)


# 處理延遲的 bucket 邊界（秒）。
#
# ⚠️ 本檔最不可逆的常數：bucket 邊界會寫進歷史資料，事後調整等於讓調整前後的
#    分佈不可比。所以它是【量出來的】而不是挑的——A 段實測單筆 process_raw_event
#    為 0.0288 秒，故把解析度集中在 5ms–250ms，上界仍留到 10s 以涵蓋三層
#    exponential backoff 都用滿的最壞情況。
#    SDK 預設邊界是 (0, 5, 10, ... 10000)，那是給毫秒用的；對以秒為單位的值，
#    每一筆都會落進第一個桶——有資料但沒有任何解析度。
PROCESSING_DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)

# View：在【來源】就決定不算，而不是送出去再讓 Collector 丟。
#
# 砍掉兩個 size histogram 的理由不是省錢而是它們答不出任何人會問的問題——
# 真要看 payload 內容，Raw 表逐字留著原文。而它們很貴：每個 histogram 每組
# label 是 18 條序列（15 個預設邊界 + count + sum），乘上 method×route×status
# 再乘上 4 個 uvicorn worker，兩個加起來約 2,160 條。
#
# 選 View 而非 Collector 的 filter processor：只有 View 能改 bucket（見下方第三條），
# 而且「量什麼」是應用程式的決定。Collector 的 filter 留作額度告急時的緊急拉桿——
# 那條路不必重建映像。
#
# 第三條（flower.task.runtime.seconds）是 CeleryInstrumentor 自動送的，實測佔了
# 全部序列的 27%（108 條），而它用 SDK 預設的毫秒級 bucket 量一個以【秒】為單位、
# 實測 0.03 的值——每一筆都落進第一個桶，有資料但零解析度。與兩個 size histogram
# 是完全相同的毛病。它與 orders.processing.duration 也高度重疊，差別只在多含
# Celery 自身的開銷；真的需要那段開銷時，trace 裡有 span 可以看。
_VIEWS = [
    View(instrument_name="http.server.request.size", aggregation=DropAggregation()),
    View(instrument_name="http.server.response.size", aggregation=DropAggregation()),
    View(instrument_name="flower.task.runtime.seconds", aggregation=DropAggregation()),
    View(
        instrument_name="orders.processing.duration",
        aggregation=ExplicitBucketHistogramAggregation(
            boundaries=PROCESSING_DURATION_BUCKETS
        ),
    ),
]


def configure_telemetry(service_name: str) -> None:
    """
    建立 TracerProvider 並掛上各套件的 instrumentor。

    ⚠️ 呼叫時機必須在 **fork 之後**（見模組註解）。目前有三個呼叫點：
      - main.py 模組層（uvicorn worker 行程內）
      - celery_app 的 `worker_process_init`（prefork 子行程）
      - celery_app 的 `beat_init`（beat 不 fork，但掛在同一類啟動事件上語意一致）

    `otel_enabled` 關閉時直接返回，**不建立任何 provider**——此時
    `trace.get_current_span()` 會回傳 OTel API 內建的 no-op span，
    logging_config 的 trace 注入因此自動變成 no-op，不需要額外分支。
    """
    if not settings.otel_enabled:
        return

    # service.name 是 Grafana 區分服務的主鍵。其餘資源屬性（deployment.environment
    # 等）由 SDK 從 OTEL_RESOURCE_ATTRIBUTES 併入，Resource.create 會自動合併。
    resource = Resource.create({"service.name": service_name})

    provider = TracerProvider(resource=resource)
    # exporter 不帶參數：endpoint 走標準的 OTEL_EXPORTER_OTLP_ENDPOINT，
    # 在 compose 裡指向 http://otel-collector:4318（不是雲端——憑證不進 app）。
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

    # Metrics 與 traces 共用同一個 Resource，service.name 才對得起來。
    # PeriodicExportingMetricReader 同樣是背景執行緒 → 與 span processor 一樣受
    # fork 限制，所以本函式的呼叫時機（fork 之後）對 metrics 也是硬需求。
    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
            views=_VIEWS,
        )
    )

    # Celery：**api 與 worker 兩側都要**。多數範例只提 worker，但派工端沒掛的話，
    # trace context 不會被寫進訊息 header，worker 就會開出一條**新的 trace**——
    # 兩段各自看起來都正常，只是永遠接不起來。這正是本階段要驗證的東西。
    CeleryInstrumentor().instrument()

    # SQLAlchemy：綁在既有 engine 上而非全域 patch，讓 span 只涵蓋本專案的連線。
    # import 放在函式內：database.py 在 import 時就會建立 engine 並讀 DB_URL，
    # 而 telemetry 這個模組本身不該有那個副作用（測試會 import 它）。
    from database import engine

    SQLAlchemyInstrumentor().instrument(engine=engine)

    # Redis：涵蓋限流計數器與 broker 的 publish。這兩段延遲正是 circuit_breaker.py
    # 當初靠實測才定位到的東西（broker 不可用時單次派工阻塞 19 秒），有 span 之後
    # 不必再靠壓測重現。
    RedisInstrumentor().instrument()

    logger.info("OpenTelemetry 已啟用", service_name=service_name)


def instrument_fastapi(app) -> None:
    """
    掛上 FastAPI 的 ASGI middleware。

    與 configure_telemetry 分開，是因為它需要 app 物件——而 app 只有 main.py 有。
    開關判斷仍留在本模組，讓「otel_enabled 的語意」只有一個地方解讀。
    """
    if not settings.otel_enabled:
        return

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)


def observe_circuit_breaker(breaker) -> None:
    """
    把熔斷器目前狀態暴露成 gauge。

    形狀是「每個狀態一條序列，值 0 或 1」而不是「一條序列、值 0/1/2」：後者要在
    查詢端記住哪個數字代表哪個狀態，而告警規則寫錯數字是不會報錯的。
    三個狀態 × 一個熔斷器 = 3 條序列，成本可忽略。

    也不用「轉換次數 Counter」：告警要問的是「**現在**開著嗎」，Counter 答不了
    ——它只能告訴你曾經開過幾次，而事故當下最需要的正是當前狀態。

    ⚠️ 由呼叫端把 breaker 物件傳進來，而不是本模組去 import main：
      ① main 已經 import 了 telemetry，反向 import 會成環；
      ② circuit_breaker.py 目前沒有任何專案外依賴，值得維持——與 process.py
         刻意保持零 Celery 依賴是同一條紀律。
    不需要判斷 otel_enabled：關閉時 provider 從未設定，callback 不會被呼叫。
    """

    def _observe(_options):
        current = breaker.state
        return [
            Observation(
                1 if state == current else 0,
                {"name": breaker.name, "state": state},
            )
            for state in (CLOSED, OPEN, HALF_OPEN)
        ]

    _meter.create_observable_gauge(
        "circuit_breaker.state",
        callbacks=[_observe],
        unit="1",
        description="熔斷器狀態，每個狀態一條序列（1=目前處於該狀態）",
    )
