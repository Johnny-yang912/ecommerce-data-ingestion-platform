import logging
import sys
import structlog
from opentelemetry import trace

from config import settings


def _add_trace_context(_logger, _method_name, event_dict: dict) -> dict:
    """
    把目前 span 的 trace_id / span_id 注入每一條 log。

    這是三個 pillar 之間唯一的接縫：有了它，Grafana 才能從一條 log 跳到對應的
    trace，反之亦然。structlog 既有的 `request_id` 不能取代它——request_id 死在
    Celery 邊界（派工只送 raw_id 過去），而 trace_id 會隨訊息 header 傳到 worker。

    **不需要判斷 otel_enabled**：關閉時 `get_current_span()` 回傳 OTel API 內建的
    no-op span，其 context 的 `is_valid` 為 False，這裡就什麼都不加。讓開關的語意
    只在 telemetry.py 解讀一次，避免兩處判斷漂移。

    格式刻意用 32/16 位補零小寫十六進位，那是 W3C Trace Context 的線路格式，
    也是 Tempo 查詢時吃的格式——直接輸出 int 的話貼進 Grafana 查不到。
    """
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def configure_logging() -> None:
    log_format = settings.log_format

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        _add_trace_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = (
        structlog.processors.JSONRenderer()
        if log_format == "json"
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
