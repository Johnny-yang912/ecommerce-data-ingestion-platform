# ADR-0043: `stg_` builds a table, not a view

**English** | [繁體中文](../../zh-TW/adr/0043-stg-table-not-view.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `stg_` |

---

## Context

Mainstream dbt convention materialises staging models as **views**: a lightweight mirror, cheap storage, always reflecting the latest source. This project deliberately deviates.

Materialisation is really two decisions — *materialised vs virtual*, then *how to materialise*. This record covers the first; ADR-0044 covers the second.

## Decision

`stg_orders` is a physical table. Four forces, and the first is decisive:

**① It cuts fuse propagation.** Staging carries `require_partition_filter = True` as a cost fuse (ADR-0021). **A view is just stored SQL** — querying it without a `received_at` filter propagates down to that fuse and returns a 400. So a view would force *every* downstream consumer, including the Hard Gate test, to remember a partition filter. A physical table stops the fuse at the `stg_` layer.

**② Dedup is paid once.** The window-function dedup is `stg_`'s core work. A view makes every downstream re-compute it — N consumers, N executions per run. A table computes once and shares.

**③ A consistent snapshot at the DAG root.** `stg_` is the root of the transformation DAG. Materialising it means every downstream model stands on one snapshot frozen at run time, immune to a concurrent E/L load into the append-only staging table.

> This is **consistency**, not **durability**. The data anchor is still ODS — `stg_` can be rebuilt from staging at any time.

**④ It is the prerequisite for incremental.** "Cost does not scale with history" relies on partition-level `insert_overwrite`, which **requires** a physical partitioned table. A view has no physical partitions to swap, so it cannot support it structurally.

## Consequences

**Downstream models are free of the fuse**, which is what lets the Hard Gate assert over the whole table when scoped as a gauge (ADR-0028).

**The cost is storage and materialisation latency** — `stg_` reflects the source only after a run. Both acceptable: BigQuery storage is very cheap, and every downstream consumer already runs on the dbt batch cadence, so nobody needs a view's always-latest property.

**The deviation from convention is justified by local forces, not by principle.** There is no general rule here that "a mirror should be solid" — if the fuse were off and there were one downstream consumer, a view would be the better choice.

## Alternatives considered

**View (the convention).** Cheapest storage and always current, at the cost of fuse propagation to every consumer, repeated dedup, no run-level snapshot, and no path to incremental. Three of those four are properties of this project specifically.

**Ephemeral.** Inlined into each downstream, so it has all of the view's problems plus no queryability for ad-hoc inspection.

## Related

- [ADR-0021](./0021-require-partition-filter-fuse.md) — the fuse whose propagation this cuts
- [ADR-0044](./0044-copy-partitions-sandbox-dml.md) — how it is materialised
- [ADR-0023](./0023-watermark-approach-a.md) — the `>=` re-extraction whose duplicates the dedup absorbs
