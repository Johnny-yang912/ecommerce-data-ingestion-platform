# ADR-0046: `stg_` is incremental, `int_` is a full rebuild

**English** | [繁體中文](../../zh-TW/adr/0046-stg-incremental-int-full-rebuild.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt |

---

## Context

`stg_orders` is `incremental` + `insert_overwrite`, partitioned on `received_at` with a lookback window. Routine runs recompute only recent partitions, so cost scales with recent volume rather than total history. It is correct because of an invariant: **every copy of a given `raw_id` lands in the same `received_at` partition**, so overwriting a whole partition atomically leaves no stragglers.

The obvious next step is to materialise `int_orders` the same way. It reads `stg_orders`, which is already partitioned on `received_at`; a matching lookback window looks like free savings.

It is a trap, and the trap is silent.

**Proposal B writes events on a different time axis than the records they affect.** A promotion event has `event_at = now()` and lands in today's partition. But the order it rescues was ingested weeks ago and sits in an old `received_at` partition. If `int_orders` were incremental over a `received_at` lookback window, that old partition would never be recomputed — so the promoted record would never flow back into Gold.

Nothing would error. No test would fail. The record would simply remain in `int_orders_quarantine` forever, and the entire re-evaluation mechanism would be severed at this one layer while appearing to work everywhere else.

## Decision

| Model | Materialisation | Why |
|---|---|---|
| `stg_orders` | `incremental` + `insert_overwrite` + `copy_partitions` | Cost scales with recent data; the same-partition invariant makes it correct |
| `int_orders`, `int_orders_quarantine`, `int_order_items` | `table` (full rebuild) | A full rebuild has no time axis to get wrong |

`table` materialisation goes through `CREATE OR REPLACE`, which is DDL. That also sidesteps the BigQuery sandbox's DML ban — the same constraint that forced `copy_partitions` on the layer above.

## Consequences

**The flow-back path works with no special handling.** A promotion event takes effect on the next scheduled run, automatically. This is not a theoretical benefit: when the Proposal B event producer (`reevaluate_quality.py`) was finally implemented, **not one line of this layer changed**. The consumer side was already correct because it had been built without an incremental time axis to sever.

**The cost is that `int_` rebuild cost scales with total history**, not with recent volume. At current data volume this is comfortably acceptable; it will not always be.

**The exit is documented in the model itself**, because it is not obvious. When volume forces incrementalisation, the re-selection set must be *"lookback-window partitions ∪ the partitions of any `raw_id` with a recent quality event"* — and **whole partitions must be re-selected**. Selecting only the affected rows would let `insert_overwrite` wipe every other row in that partition.

## The general shape

This is structurally the same problem as late-arriving data, on a different axis:

| | What changes | Where the change lands |
|---|---|---|
| Late-arriving data | the **value** of a record | a partition other than the one being processed |
| Proposal B | the **quality state** of a record | a partition other than the one being processed |

Any incremental model whose correctness depends on a second time axis has this hazard. The general rule the two cases share: **an incremental window is only safe when everything that can change a row lands in the same partition as the row.**

## Alternatives considered

**`int_` incremental on `received_at`.** The trap described above. Rejected — and worth recording as rejected, because it is the option a reader will assume was simply overlooked.

**`int_` incremental on a separate "last touched" column.** Would require maintaining such a column across two models and keeping it synchronised with an append-only event log — more machinery, and more ways to be silently wrong, than a full rebuild costs at this volume.

**Rebuild only the affected partitions on demand.** This is the documented future path, not a rejected alternative. It is deferred until volume justifies the added complexity.

## Revisit when

Full rebuilds of the `int_` layer become noticeable in the daily run's duration or cost.

## Related

- [ADR-0044](./0044-copy-partitions-sandbox-dml.md) — the sandbox constraint that shapes the layer above
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — the mechanism this materialisation protects
- [ADR-0045](./0045-int-effective-state-duplication.md) — the other deliberate cost paid at this layer
- [Transformation layer design](../design/transformation.md)
- [Cloud layer design](../design/cloud-layer.md) — late-arriving data, the same shape on the value axis
