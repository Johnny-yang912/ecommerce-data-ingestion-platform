# ADR-0024: One load job per table plus a gate; no cross-table transaction

**English** | [繁體中文](../../zh-TW/adr/0024-per-table-load-job-gate.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Cloud extraction |

---

## Context

Two tables are extracted: `orders` and `quality_events`. They are not independent — `int_` composes the effective quality state by joining them (ADR-0029). If `orders` lands and `quality_events` does not, downstream reads a consistent-looking but wrong picture: promoted records silently appear un-promoted.

BigQuery offers no cross-table transaction. A load job is atomic **for one table**. So "both or neither" cannot be obtained from the storage layer, and has to be constructed.

## Decision

Each table gets its own `TableSpec`, its own watermark, and its own load job. Consistency is built from two mechanisms rather than one transaction:

**① Per-table self-healing.** A failed load does not advance that table's watermark (ADR-0023), so the next run re-selects the same slice with `>=`. Append-only plus `stg_` dedup makes the retry harmless.

**② A gate before transformation.** If any table fails, the whole extraction fails with a non-zero exit. **Transformation must never start on half a dataset.**

The gate has two forms, with identical semantics:

| Mode | Where the gate lives |
|---|---|
| `--table all` (script-internal) | `main()` collects results and raises |
| `--table orders` / `--table quality_events` (Airflow) | The dependency edge: the dbt task's upstream is *both* extract tasks succeeding |

## Why one task per table in Airflow

This is the non-obvious half. Merging both extracts into one Airflow task would work, and it would break mechanism ①.

**Self-healing is per-table by construction.** If only `quality_events` fails, only `quality_events` should be retried. A combined task re-runs the successful `orders` extract as well — wasted work, and it obscures which table actually broke.

> Retry granularity should match failure granularity. When they differ, retries do collateral work and diagnosis loses information.

## Consequences

**"Both or neither" holds where it matters.** The tables can be transiently out of step *between* the two load jobs, but no consumer runs in that window — the gate is downstream of both.

**Failure is per-table and legible.** The Airflow UI shows which table failed; the retry touches only that table.

**The cost is that the invariant lives in orchestration, not in storage.** Nothing in BigQuery prevents someone from querying `int_` mid-extract. The guarantee is procedural, and it depends on the gate actually being upstream of every consumer.

**Independent watermarks mean the two tables can be at different points**, which is safe only because the gate exists. Removing the gate would not fail loudly — it would just start producing occasionally-wrong Gold data.

## Alternatives considered

**One load job for both tables.** Not possible; BigQuery load jobs are single-table.

**Stage to a temporary dataset and swap atomically.** Would give true all-or-nothing, at the cost of doubled storage, a swap step with its own failure mode, and a rebuild of the incremental model — for a guarantee the gate already provides at this cadence.

**One Airflow task doing both extracts.** Breaks per-table retry, per the argument above.

## Related

- [ADR-0023](./0023-watermark-approach-a.md) — the per-table self-healing this builds on
- [ADR-0022](./0022-quality-events-staging-diverges.md) — the second table, and why it must be there
- [ADR-0038](./0038-asymmetric-retries.md) — the retry policy that operates on this granularity
