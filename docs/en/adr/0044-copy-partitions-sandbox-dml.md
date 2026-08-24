# ADR-0044: `incremental` + `insert_overwrite` + `copy_partitions` to work around the sandbox DML ban

**English** | [繁體中文](../../zh-TW/adr/0044-copy-partitions-sandbox-dml.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `stg_` |

---

## Context

`stg_orders` is incremental, partitioned by `received_at` (DAY), with a lookback window. Routine runs recompute only recent partitions, so cost is proportional to recent data rather than total history.

Correctness rests on one invariant: **all duplicate copies of a given `raw_id` land in the same `received_at` partition.** Both `>=` re-extraction (ADR-0023) and Proposal C corrections preserve the original `received_at`, so whole-partition replacement misses nothing.

Then the sandbox intervenes. **This BigQuery project has billing disabled, which forbids DML** — and dbt's `insert_overwrite` defaults to `MERGE`, which is DML. Every routine incremental run failed with `DML queries are not allowed in the free tier`.

## Decision

`copy_partitions: true` inside `partition_by`. Partitions are replaced by **copy jobs** — storage-level, non-DML, free.

| | copy job (`copy_partitions=true`) | `MERGE` (default) |
|---|---|---|
| Operation level | Storage-level partition copy (`table$YYYYMMDD` + `WRITE_TRUNCATE`) | Query-engine DML (scan → delete → insert) |
| Is it DML | No | Yes |
| Billing | Free | Billed by bytes scanned |
| Sandbox | ✅ allowed | ❌ forbidden |
| Fits | producing *the full contents of a partition*, swapped wholesale | producing *a subset to upsert*, merged row by row |

**The constraint turned out to be aligned with the design, not merely worked around.** The dedup produces the full contents of a day, so whole-partition replacement is semantically the *correct* operation — `MERGE` was doing row-by-row work for a wholesale swap. Even with billing enabled there would be no reason to revert.

**⚠️ This constraint applies to every future incremental `int_`/`dim_`/`fct_` model too.** `--full-refresh` uses `CREATE OR REPLACE` (DDL) and is unaffected — which is also why `int_`'s full-rebuild materialisation (ADR-0046) sidesteps the problem entirely.

Two supporting parameters:

- **Lookback window** `var('stg_orders_lookback_days', 3)`, adjustable per invocation without editing the file. It must be ≥ the E/L `>=` re-extraction range plus a safety margin.
- **`on_schema_change='append_new_columns'`** — the dbt-side mirror of staging's additive-only policy (ADR-0025). Deliberately **not** `sync_all_columns`, which would `DROP` columns.

## The dedup key is `raw_id`, and that is a decision

Not `id`, not `order_id`:

- `ods.raw_id` is UNIQUE and 1:1 with the source row.
- A Proposal C migration-form correction arrives as **"new `id`, same `raw_id`"** — so only partitioning by `raw_id` lets the correction compete with the old copy already in staging.

The tiebreak is `received_at desc, id desc`. Current duplicates are byte-identical so ordering does not matter today — but **once Proposal C's `rebuild_batch_id` exists it must be prepended**: `rebuild_batch_id desc nulls last, received_at desc, id desc`.

## Consequences

**Incremental works on the sandbox at zero cost.**

**The lookback window couples two layers.** If E/L's re-extraction range ever widens beyond 3 days, this variable must widen with it — otherwise duplicates land outside the recomputed window and are never deduped.

**A future incremental Gold model must remember this.** The constraint is not local to `stg_`.

## Alternatives considered

**Enable billing and use `MERGE`.** Would remove the constraint and cost money, for an operation that is semantically worse for this workload.

**Full refresh every run.** Correct and unbounded — cost grows with total history, defeating the entire point of the layer.

**`--full-refresh` only, on a schedule.** Would work and abandons the incremental cost property, which is what the partitioning exists to deliver.

## Related

- [ADR-0043](./0043-stg-table-not-view.md) — the physical table this requires
- [ADR-0025](./0025-staging-additive-only.md) — the policy `on_schema_change` mirrors
- [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) — why `int_` does not inherit this
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — the sandbox's other consequences
