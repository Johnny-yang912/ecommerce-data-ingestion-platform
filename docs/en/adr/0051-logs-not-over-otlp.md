# ADR-0051: Logs are not routed over OTLP

**English** | [繁體中文](../../zh-TW/adr/0051-logs-not-over-otlp.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-17 |
| **Layer** | Observability |

---

## Context

OpenTelemetry has three pillars: traces, metrics, logs. With a Collector already in place (ADR-0050), routing logs through it too is the obvious completion — one pipeline, one backend, correlation for free.

Two things argue against doing it now.

**The logs pillar is the last to stabilise in Python.** Traces and metrics have settled APIs; the logging bridge has been the most volatile of the three, and adopting it means tracking that churn for a benefit that can be obtained more cheaply.

**Cross-pillar correlation needs exactly two fields.** Jumping from a log line to its trace requires `trace_id` and `span_id` — not a transport.

## Decision

**structlog injects `trace_id` and `span_id`** into every log record (W3C format: 32-hex and 16-hex). Logs stay on their existing path — stdout, JSON in deployed environments, console locally (`log_format`).

Correlation therefore works — a log line names its trace — without the logs pillar being in the path at all.

## Consequences

**Correlation is available today** at the cost of two fields, with no dependency on an unstable API.

**Log routing stays a container-runtime concern**, which is where it already was. `docker compose logs`, a log driver, or any collector-agnostic shipper continues to work unchanged.

**The cost is that logs are not in the same backend as traces and metrics.** Following a `trace_id` from Tempo to the log line is a manual step rather than a click. Acceptable at this scale; it would not be for a team on call.

**No structured log querying in the observability backend.** Searching logs means searching wherever the container runtime put them.

**This is not a rejection of the logs pillar, only of adopting it now.** The two injected fields are exactly what the eventual migration needs, so nothing done here has to be undone.

## Revisit when

The Python logs pillar is stable, **or** someone is actually on call and the manual hop between backends becomes a cost paid under time pressure.

## Alternatives considered

**Route logs over OTLP now.** One backend and one query surface, at the cost of tracking an unstable API and adding a failure mode where the Collector being down loses log lines that would otherwise have gone to stdout regardless.

**Ship logs with a separate agent (Promtail / Fluent Bit).** Would put logs in the same backend without the OTLP dependency — a real option, and one more component to run for a benefit nobody is currently waiting on.

**Skip `trace_id` injection entirely.** Saves nothing and gives up the only property that made deferring the pillar affordable.

## Related

- [ADR-0050](./0050-resident-otel-collector.md) — the pipeline this pillar is kept out of
- [ADR-0034](./0034-tier-1-tier-2-metrics.md) — the other "which signal belongs where" boundary
