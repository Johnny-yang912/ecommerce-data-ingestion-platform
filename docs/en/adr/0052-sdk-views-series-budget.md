# ADR-0052: SDK Views control the series budget — the expensive metrics are the automatic ones

**English** | [繁體中文](../../zh-TW/adr/0052-sdk-views-series-budget.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-17 |
| **Layer** | Observability |

---

## Context

Metrics backends bill by **active time series**, and a series is created for every unique combination of metric name and attribute values. Cardinality multiplies: three attributes with ten values each is a thousand series from one metric.

The free tier allows 10,000. The intuition is that the risk lies in custom instrumentation — the metrics you deliberately added, with the attributes you deliberately chose.

**That intuition is wrong, and measurement is what showed it.**

## Decision

**Control the series budget with SDK Views**, and verify the result rather than assuming it.

Measured: **320 active series — 3.2% of the free tier.**

Custom instrumentation is deliberately low-cardinality:

| Metric | What it answers |
|---|---|
| `orders.raw.result` | terminal-state distribution |
| `orders.processing.duration` | processing latency |
| `orders.retry` | retry pressure |
| `circuit_breaker.state` | is the breaker open |
| `recovery_scan.dispatched` | is the scan working |

Plus two supplied free by auto-instrumentation and genuinely worth keeping: `http.server.duration` (P50/95/99) and `db.client.connections.usage` (pool pressure).

### The counter-intuitive finding

**Three Drop views remove metrics nobody wrote**, and they accounted for **27% of the total**:

| Dropped | Why |
|---|---|
| `http.server.request.size` | Not a question anyone asks of this system |
| `http.server.response.size` | Same |
| `flower.task.runtime.seconds` | **Measured a second-scale value with millisecond-scale buckets** — data with zero resolution |

> **Instrumentation you did not write is still instrumentation you pay for.**

The `flower` histogram is the sharpest example: it was not merely unnecessary, it was **meaningless**. Every observation fell into the same bucket, so it consumed series to record nothing. Nobody chose that — it arrived with a library, and only a measurement found it.

## Consequences

**The budget is verified, not assumed.** "3.2% of the free tier" is a number that can be re-checked when instrumentation changes, rather than a hope.

**Adding a custom metric is cheap and predictable**, because the low-cardinality discipline is already established and the headroom is real.

**The audit has to be repeated after any dependency upgrade.** A library can add a histogram in a minor version, and nothing will announce it. This is the standing cost of the decision.

**Views are the right layer for this.** Dropping at the Collector would still pay the SDK's aggregation cost in-process; dropping at the SDK means the series is never created.

## Alternatives considered

**Drop nothing and rely on the free tier being generous.** 27% consumed by three metrics that answer no question, and no signal at all when the next dependency adds a fourth.

**Filter at the Collector instead.** Works, and pays the in-process aggregation cost anyway, and moves the decision away from the code that emits it.

**Add attributes freely and worry about cardinality later.** "Later" is a billing statement, and the same failure shape as the missing cost fuse in ADR-0021 — an expensive silent failure rather than a loud cheap one.

## Related

- [ADR-0034](./0034-tier-1-tier-2-metrics.md) — the tier boundary that keeps custom cardinality low
- [ADR-0050](./0050-resident-otel-collector.md) — the pipeline these Views sit in
- [ADR-0021](./0021-require-partition-filter-fuse.md) — the same "prevent, do not cap" stance on cost
