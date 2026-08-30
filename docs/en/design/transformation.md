# Transformation Layer (dbt)

**English** | [繁體中文](../../zh-TW/design/transformation.md)

`stg_` → `int_` → `dim_`/`fct_` → `rpt_`. Quickstart and commands: [`ecommerce_dbt/README.md`](../../../ecommerce_dbt/README.md).

---

## 1. Layers and naming

| Prefix | Grain | Responsibility |
|---|---|---|
| `stg_` | source grain | 1:1 mapping, rename, cast, dedup. **No business logic** |
| `int_` | source grain | joins, derived fields, **and the blocking point** |
| `dim_`/`fct_` | star schema | dimensions and facts for flexible analysis |
| `rpt_` | fixed | pre-aggregations for BI |

Models: `stg_orders`, `stg_quality_events`, `int_orders`, `int_orders_quarantine`, `int_order_items`, `dim_customer`, `dim_product`, `fct_orders`, `fct_order_items`, `rpt_quality_events_daily`, `rpt_quality_backlog`, `rpt_sales_daily_by_category`.

---

## 2. `stg_` layer

**A physical table, not a view.** Four forces, the first decisive: staging carries `require_partition_filter`, and **a view would propagate that fuse to every downstream consumer**. A table cuts the chain. It also pays dedup once, gives the DAG root a consistent snapshot, and is the prerequisite for incremental. [ADR-0043](../adr/0043-stg-table-not-view.md)

**Materialisation**: `incremental` + `insert_overwrite`, partitioned on `received_at` (DAY), with `var('stg_orders_lookback_days', 3)`.

Correctness rests on one invariant: **all copies of a given `raw_id` land in the same `received_at` partition**, so whole-partition replacement misses nothing.

⚠️ That invariant is **necessary but not sufficient**. It guarantees dedup is complete
*within* the window and says nothing at all about where the window's *boundary* falls —
while `insert_overwrite`'s atomic unit is a whole partition, and dbt overwrites exactly
the partitions that appear in the query result. A left boundary landing mid-day lets only
part of that day's rows into the window, so **half a day atomically overwrites a whole
day** and everything outside the window is silently deleted.

So the left boundary **must align to the day boundary**:

```sql
timestamp_sub(timestamp_trunc(current_timestamp(), day), interval N day)
--            └─ this layer is correctness, not style
```

Aligned, the edge day is either wholly inside the window or wholly outside it. "Half a
day" ceases to exist, and **the time of day a run happens stops being an implicit
precondition for correctness** — which was the real defect: the same model produced
different output at 20:38 than at 22:30.
[ADR-0055](../adr/0055-partition-aligned-incremental-window.md) ·
[2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md)

**Targeted backfill**: `stg_orders_backfill_start` / `_end` let the repair path name
partitions by date, independent of the clock. The routine path still uses the rolling
window. `assert_stg_orders_matches_staging` (per-partition reconciliation) guards the contract.

**`copy_partitions: true`** — the sandbox forbids DML and `insert_overwrite` defaults to `MERGE`. Copy jobs are storage-level, non-DML and free, and are *semantically* the better fit anyway: dedup produces the full contents of a day, so a wholesale swap is the correct operation. [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)

**Dedup**: `row_number() over (partition by raw_id order by received_at desc, id desc) = 1`.

The key is `raw_id`, not `id` or `order_id` — a Proposal C migration-form correction arrives as *"new `id`, same `raw_id`"*, so only partitioning by `raw_id` lets the correction compete with the old copy. When `rebuild_batch_id` exists it must be **prepended** to the `order by`.

**`on_schema_change='append_new_columns'`** — the dbt-side mirror of "staging only adds". Deliberately not `sync_all_columns`, which would `DROP`.

The **Hard Gate** hangs here: see [data-quality §3](./data-quality.md).

---

## 3. `int_` layer

Where blocking happens. Three models, and `int_orders` + `int_orders_quarantine` must be a **partition** of `stg_orders` — mutually exclusive and jointly exhaustive, asserted by `assert_orders_split_is_partition`.

### The effective-state block is duplicated deliberately

The composition logic is written **twice**, byte-identical, fenced with `═══` markers — not extracted into a shared model.

The clarifying fact: an **ephemeral** model is inlined into every downstream, so it creates no extra relation **and saves no JOIN executions**. Only a materialised shared table would. **So sharing vs duplicating is purely a maintenance trade-off here, not a cost one.**

Duplication was chosen because there are only two consumers today, each model file stays self-contained, and the price — complementarity dropping from a mechanical guarantee to a disciplinary one — **is bought back with one test**.

**Consolidation trigger: a third copy.** [ADR-0045](../adr/0045-int-effective-state-duplication.md)

### Alignment checklist

Walked on every change to either model:

| # | Check | If wrong |
|---|---|---|
| 1 | Both define `is_effectively_clean` identically; one `WHERE cond`, one `WHERE NOT cond` | rows in neither table (**silent loss**) or both |
| 2 | `coalesce(..., false)` not dropped | the row **vanishes from both tables at once** |
| 3 | Always `LEFT JOIN` | drops every row with no quality event |
| 4 | Window `partition by` / `order by` tiebreaks match | the two sides pick different events |
| 5 | `effective_quality_state` CASE branches match | `rpt_quality_*` miscounts |
| 6 | Same materialisation on both | partition breaks between runs |
| 7 | `assert_orders_split_is_partition` stays `severity: error` | the only automated safety net is gone |

### Materialisation: `table`, full rebuild

Deliberately **not** incremental on `received_at`. A Proposal B promotion event lands in **today's** partition while the order it rescues sits in an **old** one — an incremental window would never recompute that partition, and the promoted record would never flow back. Silently. [ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md)

`CREATE OR REPLACE` is DDL, so it also sidesteps the sandbox DML ban.

#### When to switch: watch observable numbers, not order counts

Measured baseline: one run of the `int_` layer scans **910 KB for 554 orders → ≈1.64 KB per order per run** (all three models combined; the ratio is essentially row width, so it holds as data grows).

| Total orders | Scan per run | Monthly cost on a daily batch (on-demand, $6.25/TiB) |
|---|---|---|
| 10M | 16 GB | ~$3 |
| 100M | 164 GB | ~$30 |
| 1B | 1.6 TB | ~$300 |

The sandbox's 1 TiB/month free tier lasts to roughly **15–20M orders** on a daily batch, before subtracting `stg_`'s and `dim_`/`fct_`'s usage.

**But cost is not the first bottleneck you hit.** Two things bite earlier:

1. **`job_execution_timeout_seconds: 300`** in `profiles.yml` — which makes runs *fail* rather than get expensive. The full rebuild currently takes **2.5s**.
2. **The batch window for the whole DAG**, once `int_`'s full rebuild stacks on top of equally full `dim_`/`fct_` rebuilds.

**Criterion**: track `bytes_billed` (monthly cumulative) and `execution_time` in `target/run_results.json`; start evaluating when either reaches **50%** of the quota or the timeout. Until then, the full rebuild is the option where **correctness is free and complexity is zero**.

> **One deliberately accepted asymmetry**: `stg_`'s incremental saves recomputation and writes, but `int_` still scans all of `stg_` every run — so **the pipeline's read cost still scales with total history.** A knowing trade-off, bought in exchange for `int_`'s correctness holding unconditionally.


**No partitioning, clustering on `order_id` only** — `int_` is consumed only inside the DAG, so partitioning buys nothing. That premise is exactly what [§5](#5-rpt_-layer) protects.

**`int_order_items`**: items flattened to item grain, with `safe_cast` and **strict NULL propagation** on derived amounts — no `coalesce`. `quarantined_at` takes the event time, not `CURRENT_TIMESTAMP()`, which would record when the run happened.

---

## 4. `dim_`/`fct_` layer

**Dual fact tables**: `fct_orders` (header) and `fct_order_items` (line).

**Measures roll up into the header**, with `assert_fct_orders_rollup_matches_items` asserting per-order equality. Same move as the `int_` layer: spend one test to convert a disciplinary guarantee into a mechanical one, and get single-table queryability back. `is distinct from`, not `=`, is mandatory — `NULL = NULL` is NULL, so `=` would silently filter out the rows most likely to be wrong.

> That test caught a defect it was not designed for: 39 rows differing by **1 ULP**, because `SUM()` over `FLOAT64` is not associative. The fix was moving money to `NUMERIC` — **not** loosening the test to a tolerance.

**`SUM()` ignores NULLs**, so one item failing `safe_cast` leaves the order's total short by exactly one item with no trace. The remedy is not `COALESCE` but making the incompleteness explicit: `fct_orders.items_missing_amount`. **We do not decide on the consumer's behalf that NULL means zero.**

Related: `item_count = 0` expresses "an order with no items" as a **value**, and `fct_orders` must `LEFT JOIN` the rollup — `INNER` would make that whole class vanish from Gold.

**Two dimensions only**, SCD1 with an explicit tiebreak. `dim_date`, `dim_geography` and a junk dimension are all degenerated onto the facts. SCD1's distortion is bought back by `fct_orders.membership_tier_at_order` — **the type-2 effect with zero infrastructure**. [ADR-0047](../adr/0047-measures-roll-up-to-header.md) · [ADR-0048](../adr/0048-two-dimensions-scd1.md)

**`dim_product` conflicts are flagged, not blocked**: the same `product_id` can arrive with different attributes. Measured 2026-08, 163 of 342 conflicted — root cause a generator bug, since fixed. Flagging meant the bug was **visible in the data** rather than hidden behind a failed build.

**Partitioning**: `fct_*` on `order_date` (DAY); `dim_*` none — dimensions are reached by key join, where a partition column prunes nothing.

### Business rules deliberately left undefined

Three things are undefined in the requirements and are **deliberately not assumed** — same principle as not building speculative models:

| Item | What is undefined | Current handling |
|---|---|---|
| `tax_amount` | Is the tax base `net`, or `net + shipping`? | Only `tax_pct` is exposed — **a ratio, non-additive, never `SUM` it**. No derived amount is built |
| Net revenue | Should `returned = TRUE` orders be subtracted? | `returned` stays on the fact as a flag; **downstream decides** |
| `profit_amount` | Does margin include shipping and tax? | Not built; downstream can compute `net_amount - cost_amount` |

> **A fabricated assumption makes a wrong number look like a fact.** Leaving the ratio and the flag on the fact table pushes the decision to whoever actually knows the answer — and makes the absence visible rather than papering over it with a plausible default.

---

## 5. `rpt_` layer

**Business reports read Gold only, never `int_` directly.** Four reasons, and the fourth is architectural: *"`int_` is consumed only inside the DAG"* is the **only** reason `int_` is not partitioned — `rpt_` reading it promotes it to a public contract and makes it un-refactorable.

**The legitimate exception is quality reporting**, since quarantined rows by definition never reach Gold.

> ⚠️ The denominator for quality rates is all of `stg_orders`, **dirty included** — not `fct_orders`. Use Gold as the denominator and `quarantine_rate` is identically zero.

### Quality reporting splits into two tables

| | `rpt_quality_events_daily` | `rpt_quality_backlog` |
|---|---|---|
| Axis | **event axis** (`event_at`) | **snapshot** |
| A row means | N events happened that day | N orders are stuck now |
| Rewritten retroactively | no (append-only) | yes — it *is* the current state |
| Incremental possible | ✅ | ❌ |

The backlog cannot be accumulated off the event axis: `quality_events` expires at 60 days, so **the starting point of the accumulation disappears and the distortion is one-directional** — the backlog would be systematically understated. [ADR-0049](../adr/0049-business-reports-read-gold.md)

**A note on honesty**: the textbook justification for `rpt_` is performance, and at this volume that is worth nothing. The real reasons are **one fixed definition per metric** and **BI not assembling joins itself**.

---

## 6. Testing

| Kind | Examples |
|---|---|
| generic | `not_null`, `unique`, `accepted_values`, `relationships` |
| custom generic | `error_rate_below` (the Hard Gate) |
| singular | `assert_orders_split_is_partition`, `assert_fct_orders_rollup_matches_items` |
| source | `dbt source freshness` — in its own DAG ([orchestration](./orchestration.md)) |

93 tests total. The two singular tests are the load-bearing ones: each converts a discipline into a mechanism.

---

## 7. Related

- [data-quality](./data-quality.md) — the layer contracts these models implement
- [cloud-layer](./cloud-layer.md) — where `stg_`'s source comes from
- [orchestration](./orchestration.md) — how these layers are executed
