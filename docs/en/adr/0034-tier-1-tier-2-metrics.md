# ADR-0034: The boundary between tier-1 operational and tier-2 analytical metrics

**English** | [繁體中文](../../zh-TW/adr/0034-tier-1-tier-2-metrics.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — metrics |

---

## Context

"How is data quality doing?" is two questions wearing one sentence.

**"Is something breaking right now?"** needs an answer within minutes, at low cardinality, and it needs to be readable while the warehouse is down — because the warehouse being down is one of the things that might be breaking.

**"How has quality trended by category, by client, by rule version?"** needs high cardinality and arbitrary slicing, and nobody needs it within minutes.

Building one mechanism for both produces something that does neither well: either a metrics backend drowning in cardinality, or a warehouse query pretending to be an alert.

## Decision

Two tiers, with an explicit boundary.

| | Tier 1 — operational | Tier 2 — analytical |
|---|---|---|
| Latency | minute-level | daily / weekly batch |
| Cardinality | deliberately low | arbitrary |
| Where | OTel metrics + structlog `quality_metric` events | `rpt_quality_*` in the warehouse |
| Question | is something breaking *now*? | how has quality *trended*? |
| Survives a warehouse outage | yes | no |

**The rule: high-cardinality slicing belongs in the warehouse, by definition.** A metrics backend charges per time series; the warehouse charges per query. Slicing by customer, product category and rule version simultaneously is cheap in one and ruinous in the other.

## Consequences

**Tier 1 stays affordable.** The series budget is measured at 320 active series — 3.2% of the free tier. That number is only achievable because the boundary is enforced.

**The counter-intuitive part of that budget is worth recording**: the expensive metrics turned out to be the **automatic** ones, not the custom ones. Three Drop views remove `http.server.request/response.size` and `flower.task.runtime.seconds`, which together accounted for **27%** — and the latter measured a second-scale value with millisecond-scale buckets, i.e. data with zero resolution.

> Instrumentation you did not write is still instrumentation you pay for.

**Tier 1 answers survive the warehouse being unavailable**, which matters because "the analytics pipeline is broken" is precisely when someone is asking.

**Business / DQ metrics are currently deferred at tier 1**, and the reason is specific to this project: the simulated upstream picks its dirty rate deterministically from five values per day, so it is **constant within a day**. A minute-level error rate would say nothing `rpt_quality_events_daily` does not already say. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

**The cost is that the same concept is expressed twice**, in two systems with two definitions. They must be kept in agreement by discipline; nothing checks it automatically.

## Alternatives considered

**One tier: everything in the warehouse.** Cheap and simple, and it cannot answer anything during an incident — the outage and the diagnostic share a failure domain.

**One tier: everything in the metrics backend.** Would require either dropping the high-cardinality dimensions (losing the analysis) or paying for them (unaffordable — the series count multiplies with every dimension).

**Derive tier-1 metrics from tier-2 tables.** Inherits the warehouse's latency and its failure domain, which removes the only two properties tier 1 exists for.

## Related

- [ADR-0033](./0033-historical-metrics-never-rewritten.md) — the other metric boundary
- [ADR-0052](./0052-sdk-views-series-budget.md) — how the tier-1 budget is enforced
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — why business metrics are deferred at tier 1
