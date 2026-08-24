# 2026-08 — How much does partitioning actually save?

**English** | [繁體中文](../../zh-TW/verification/2026-08-partition-savings.md)

---

## What was being verified

Partitioning is universally recommended for cost. **How much does it actually save over clustering alone** — and is "partitioning saves a lot of money" the right justification for it?

## Environment

Two tables over **identical data** (540 rows), running the typical analyst query: a **last-30-day slice**. 2026-08.

## Observed

| | `totalBytesProcessed` | vs. full table |
|---|---|---|
| Full table | 68,856 B | 100% |
| `cluster by order_date` only | 12,474 B | **18%** |
| `partition by order_date` + cluster | 6,490 B | 9% |

**Clustering alone pruned 82%. Partitioning adds nine more percentage points.**

## Conclusion

The common justification needs correcting: **partitioning's value is not the pruning volume.** Clustering already did most of that work.

Its value is three things clustering cannot give:

**① Cost predictability.** Partition pruning is decided **from metadata before the query runs**, so `dry run` byte counts are exact. Clustering prunes at block level depending on data layout, so `dry run` over-estimates. **Cost governance depends on the former** — a fuse or a budget alarm built on an over-estimate is not a control.

**② The prerequisite for `require_partition_filter`.** Only a partitioned table can have it ([ADR-0021](../adr/0021-require-partition-filter-fuse.md)) — even though Gold chooses not to use it.

**③ Partition-level operations.** `insert_overwrite`'s atomic whole-partition replace, and single-partition targeted refresh. **The entire `stg_` runbook rests on this** ([ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)).

## The measurement's own boundary

> **BigQuery bills a 10 MB minimum per table per query.** At this project's data volume, all three variants cost **exactly the same**.

Partitioning's benefit only holds under the premise of tens to hundreds of millions of rows — **a premise this project declares rather than demonstrates.** For the extrapolation, see the per-order cost table in [design/transformation §3](../design/transformation.md).

Stating that boundary matters: a measurement at 540 rows cannot prove a cost argument, and presenting it as if it did would be the same error as tuning an index against generated data ([ADR-0018](../adr/0018-raw-status-no-index.md)).

## What this overturned

Not a written conclusion, but a **received justification**. "Partition your tables, it saves money" is true and it is the least important of the three reasons — and someone optimising on that basis alone would reasonably conclude that clustering is enough.

## Related

- [ADR-0020](../adr/0020-partition-on-received-at.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md)
- [2026-08-partition-expiry-measurement](./2026-08-partition-expiry-measurement.md) — the other side of the same feature
