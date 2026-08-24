# ADR-0019: Batch load, not streaming

**English** | [繁體中文](../../zh-TW/adr/0019-batch-load-not-streaming.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

The ingestion path is real-time: an order is in ODS within milliseconds. It is tempting to carry that property downstream and stream rows into BigQuery as they land.

Two things argue against it, and only one of them is about cost.

**The downstream consumer is BI.** Dashboards and reports refresh on a T+1 or hourly cadence. Sub-minute freshness in the warehouse would be a property nobody reads.

**Batch is what makes windowed quality checks possible.** The Hard Gate asserts an error rate over the latest partition (ADR-0028). A rate over *what* is only answerable if there is a batch. Streaming has no batch boundary, so the run-level gate — the thing that stops a broken upstream from reaching Gold — has nothing to be scoped to.

Cost is the third reason and the least interesting: BigQuery batch load jobs are free, streaming inserts are not.

## Decision

**Batch load jobs only.** `client.load_table_from_json(...)` with `WRITE_APPEND`. No streaming inserts anywhere in the extraction path.

Nested `items` are landed as native JSON objects rather than serialised strings, so the shape survives into `stg_`.

## Consequences

**Windowed quality control works**, because there is a window: Hard Gate scope, freshness assertions, and the daily grain of `rpt_quality_events_daily` all rest on the batch boundary.

**Failures are re-runnable.** A batch either loaded or it did not; the watermark does not advance on failure, and the next run re-selects with `>=` (ADR-0023). A partially-streamed set has no such clean retry point.

**The ingestion and analytics layers stay decoupled.** The analytics layer's failure cannot back-pressure ingestion, because nothing in the ingestion path waits on it.

**The cost is latency**: an order is visible in the warehouse after the next scheduled extract, not immediately. That is the intended trade.

## Alternatives considered

**Streaming inserts.** Would give sub-minute freshness that nothing consumes, cost money per row, and — the decisive point — remove the batch boundary the quality gates depend on.

**Micro-batch (minute-level).** A middle ground that keeps a batch boundary. Deliberately not taken: the freshness requirement does not exist, and the partition budget and reporting grain both assume daily. `get_watermark()` is the seam if that ever changes (ADR-0023).

## Revisit when

A downstream consumer appears that needs sub-daily freshness — a real-time model, or an operational dashboard with an actual audience.

## Related

- [ADR-0023](./0023-watermark-approach-a.md) — the seam for changing this
- [ADR-0028](./0028-hard-gate-per-batch-scope.md) — the quality gate that depends on a batch boundary existing
- [Cloud layer design](../design/cloud-layer.md)
