# ADR-0021: `require_partition_filter` as a cost fuse

**English** | [繁體中文](../../zh-TW/adr/0021-require-partition-filter-fuse.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

BigQuery bills on bytes scanned. A partitioned table does not protect you from a full scan — it only makes a *filtered* query cheap. `SELECT * FROM staging.orders` with no `WHERE` clause scans every partition and bills for all of it.

The realistic failure is not malice or ignorance. It is a forgotten `WHERE` in an ad-hoc query, or a dbt model that loses its filter during a refactor. Nothing about that fails loudly — the query returns correct results, and the cost shows up on a bill later.

## Decision

`require_partition_filter = True` on the `orders` staging table.

A query without a partition filter now **fails immediately** with an explicit error, rather than succeeding expensively.

The setting is declared per table in `TableSpec`, not applied globally — because it is not universally correct (ADR-0022).

## Consequences

**The failure mode moves from "expensive and silent" to "free and loud."** An error at query time is recoverable in seconds; a bill is not.

**Downstream models must carry a partition filter, deliberately.** `stg_orders` reads with a lookback window, which satisfies the fuse naturally — the filter it needs for correctness is the same filter the fuse requires.

**`stg_orders` itself does not set the fuse.** It is a dbt-managed table, its consumers are `int_` models and the Hard Gate test, and the Hard Gate must be able to assert over the whole table when scoped as a gauge (ADR-0028). Applying the fuse there would block a query the design requires.

**The cost is friction on exploratory queries.** Someone poking at staging must write a filter every time. That is the intended friction — it is exactly the habit the fuse is there to enforce.

## Alternatives considered

**Rely on a project-level cost quota.** A quota stops the bleeding after it starts and is account-wide, so one careless query can exhaust the budget for everything else. The fuse prevents the query instead of capping the damage.

**Rely on review and convention.** Works until the one time it does not, and the feedback loop is a billing statement rather than an error message.

**Apply it to every table uniformly.** Rejected — see ADR-0022, where the main consumer's access pattern is inherently a full scan and the fuse would block correct behaviour.

## Related

- [ADR-0020](./0020-partition-on-received-at.md) — the partitioning this protects
- [ADR-0022](./0022-quality-events-staging-diverges.md) — where this decision is deliberately reversed
- [ADR-0044](./0044-copy-partitions-sandbox-dml.md) — the other cost-shaped constraint on this layer
