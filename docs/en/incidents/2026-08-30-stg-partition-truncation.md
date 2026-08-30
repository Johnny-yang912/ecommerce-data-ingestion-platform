# Half a day overwrote a whole day: `stg_orders` silently lost 550 rows

**English** | [繁體中文](../../zh-TW/incidents/2026-08-30-stg-partition-truncation.md)

| | |
|---|---|
| **Date** | Occurred 2026-08-29 20:38 · found 2026-08-30 10:20 · repaired same day |
| **Impact** | `stg_orders`'s `2026-08-26` partition: 800 rows → **250 rows**; the gap propagated to Gold |
| **Alerting** | **None.** DAGs green, all 93 dbt tests green, upstream staging untouched |
| **Detection lag** | ~14 hours, and it was **noticed by eye on a BI dashboard** |
| **Root cause** | The incremental window's left boundary carried the run's wall-clock time; `insert_overwrite` overwrote a whole day with half a day |
| **Status** | Closed. Root cause fixed, data restored, detection added |

---

## Why this document is here

The other two incident reports in this project have root causes **outside** it — an editor extension, a development machine's clock. **This one's root cause is a line of SQL the project wrote itself.**

It is also the hardest of the three to see: no existing signal left green, not one row of upstream source data was missing, and afterwards there was no way to work out *what* had deleted them — the deleted rows left no trace at all.

---

## Symptom

The revenue line on Looker got shorter, but **no single day vanished** — the shortfall was smeared across roughly 45 `order_date` values, which reads as noise rather than a fault.

Following it into the warehouse showed it was actually one day's problem, and not the day it appeared on:

| Layer | Rows for `2026-08-26` |
|---|---|
| ODS (PostgreSQL) | 800 |
| `staging.orders` (BQ mirror) | 800 distinct `raw_id` ✅ |
| **`stg_orders` (Silver)** | **250** ❌ |

Scheduling on `2026-08-26` was **entirely normal**: all four seeding slots succeeded and every Raw row is `processed`. What broke was a run three days later.

The loss was highly regular — only the day's final slot survived:

```
Slot (Taipei)   staging   stg_orders
10:00            150    →      0     ✗
13:00            200    →      0     ✗
17:00            200    →      0     ✗
21:00            250    →    250     ✓
```

---

## Root cause

`stg_orders`'s incremental filter was:

```sql
where received_at >= timestamp_sub(current_timestamp(), interval 3 day)
```

`current_timestamp()` carries **the wall-clock time of the run**, so the left boundary cuts through the middle of a day. And `insert_overwrite`'s atomic unit is a **whole partition** — dbt overwrites the partitions that appear in the query result, with the query result itself.

So:

```
2026-08-26 partition:  |--02:00---05:00---09:00---13:00--|   800 rows (UTC)
                          150      200      200      250

cutoff (3 days back from the 2026-08-29 12:38 UTC run)  ↑ 12:38
inside the window:                                        [========]   250 rows
insert_overwrite replaces the whole partition with those 250  →  the other 550 deleted
```

The trigger was **a manual run at 2026-08-29 20:38** — the machine was being shut down early that evening, so the 22:30 scheduled run was executed by hand ahead of time.

**Why the scheduled run had always been fine**: 22:30 Taipei is 14:30 UTC, and the tail of the last seeding batch is 13:05 UTC. The cutoff lands at 14:30 on D-3, **later** than that day's final batch → the partition contributes **zero rows** to the query → dbt does not touch a partition with no rows → intact.

> **This pipeline's correctness had been resting on an 85-minute margin the whole time, and that dependency was never written down or pinned by a test.**

The danger window is **10:00–21:05 Taipei**. The manual run's 20:38 was inside it.

**The second time, by 26 minutes**: at boot the next morning (09:34) Airflow replayed the previous evening's missed runs. That run's cutoff landed at `2026-08-27 01:34 UTC`, **earlier** than that day's first batch (02:00 UTC), so 8/27 was wholly inside the window and got rewritten in full. Half an hour later a boot would have taken 8/27 out the same way.

---

## Why the existing defences did not catch it

| Defence | Why it stayed silent |
|---|---|
| DAG status | The task succeeded. It **did** do what it was asked to do |
| 20 dbt tests | All of them ask "is this row correct". Every one of the surviving 250 rows was perfectly correct |
| Hard Gate | A per-batch error rate. The deleted rows had a normal clean/dirty mix, so the ratio did not move |
| `source_freshness_watch` | Watches **how recent the newest row is**, not **how many there are** |
| `copy_partitions` ([ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)) | It prevents a mid-flight failure between delete and insert. It does **not** prevent a misaligned window boundary |
| The same-partition invariant ([ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md)) | It guarantees dedup is complete *within* the window and makes no claim about the window's *boundary* |

The last two rows are the point: **both existing correctness arguments hold, and neither of them covers this failure.**

### It had already been predicted twice

**1.** The [`dbt_packages` incident](./2026-08-dbt-deps-429.md)'s "Lessons" section already named this shape:

> A tool that clears state first and then fetches a replacement turns any download failure into data loss. That pattern is worth recognising generally — **it is the same shape as an `insert_overwrite` that deletes partitions before the new data is confirmed**

The conclusion at the time was that `copy_partitions` had handled it. **That handled half of it.**

**2.** Step 4 of the [`dag-failure-recovery` runbook](../runbooks/dag-failure-recovery.md), "Verify nothing was lost", is precisely a per-partition comparison of staging against `stg_orders`.

> **This project already knew that check mattered. It was just written as a block of SQL somebody had to remember to paste — and on the day it happened, nobody thought to run it.**

---

## Investigation

Worth recording, because "upstream is intact" breaks the usual order of attack: no error message, no failed task, and the only lead is a number smaller than expected.

1. **Count rows layer by layer**, from ODS outward (800 / 800 / 250) → the gap sits between staging and `stg_orders`, i.e. **inside dbt**, not in extraction
2. **Split those 250 by hour** → all of them in the 13:00 UTC batch → not random loss, a **clean cut**
3. **Work the cutoff back from the cut** → `2026-08-26 12:38 UTC`
4. **Search `dag_run` for 12:38** → the previous evening's manual run at 20:38

Step 2 was the turning point: **random loss points at dedup or a join; a clean cut points at a filter predicate.**

---

## Response

**1. Root cause**: align the left boundary to the day boundary.

```diff
- where received_at >= timestamp_sub(current_timestamp(), interval N day)
+ where received_at >= timestamp_sub(timestamp_trunc(current_timestamp(), day), interval N day)
```

**2. Repair path**: add `stg_orders_backfill_start` / `_end` to name partitions by date, independent of the run's clock.

**3. Detection**: add `assert_stg_orders_matches_staging`, comparing staging's `distinct raw_id` against `stg_orders`'s row count per partition. Window 7 days (**must exceed the lookback window**).

The decision is recorded in [ADR-0055](../adr/0055-partition-aligned-incremental-window.md).

### Data repair

Executed at 11:24 Taipei — **the middle of the old danger window**, which is itself the proof that the repair path no longer has a time condition:

```bash
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "2026-08-26"}'
dbt run -s stg_orders+ --exclude stg_orders
```

| | Before | After |
|---|---|---|
| `stg_orders`, 8/26 partition | 250 | **800** |
| `stg_orders` total | 14,037 | **14,587** |
| `fct_orders` | 12,935 | **13,434** (+499) |
| `int_orders_quarantine` | 986 | **1,037** (+51) |
| Reconciliation test | FAIL | **PASS** |
| Full dbt test suite | — | **94 / 94 PASS** |

`499 + 51 = 550`. Clean rows went to Gold, dirty ones to quarantine, in a ratio matching that batch's original dirty rate — they were **re-processed under the normal rules**, not forced back in.

`14,587 − 13,434 − 1,037 = 116`, identical to before the repair: that is the known `order_date` 60-day partition expiry, unaffected by this rebuild.

> The reconciliation test went **red then green**: before the repair it emitted `2026-08-26 / 800 / 250 / 550`, after it passed. **A test that has only ever been green is not a test.**

> **The record of this backfill is this document.** That is the current mechanism: the record is written by a person, not produced by the system — [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md) records why, and what would change it.

---

## Incidental finding: catch-up runs guarantee no ordering between DAGs

Established during the investigation. Self-healing, no action needed, but worth recording.

`orders_analytics_daily`'s file header documents an ordering contract: 84 minutes of slack between the last seeding batch (21:00) and extraction (22:30). **That contract exists only in the schedule table.**

At boot on 2026-08-30, all four DAGs' catch-up runs were queued in **the same second** (01:34:44 UTC), so extraction ran while seeding was still loading — ODS held 399 rows for the day and only **4** were extracted.

The next `>=` watermark pass picks them up automatically ([ADR-0023](../adr/0023-watermark-approach-a.md)), so nothing needs doing. But when reading the state: `catchup=False` does not mean "no catch-up" — it replays the most recent missed interval, and during a replay there is no ordering between DAGs at all.

---

## Lessons

> **A batch model that reads its own clock is not idempotent.** Same model, same code, different output at 20:38 than at 22:30 — that is the defect itself; "3 days was written as 3×24 hours" is only how it showed up.

> **Content tests only catch what still exists.** All 20 tests ask "is this value right". None asks "is the row still there". A suite made entirely of content tests is green while data is being deleted.

> **A check written as "remember to run this" is not a check.** That SQL went into the runbook three months ago. Its content was entirely correct; its cost was needing someone to think of it on the right day.

A fourth point, about why this was recoverable at all: **`staging` is append-only and had not been touched.** That is not something this incident fixed — it is the one thing that did not break, and **it is the sole reason all 550 rows could be restored exactly** ([ADR-0025](../adr/0025-staging-additive-only.md)). An immutable upstream is a cost right up until the day you need it.

---

## Related

- [ADR-0055](../adr/0055-partition-aligned-incremental-window.md) — the decision this produced
- [ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md) — the incomplete invariant
- [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md) — the defence that covered half of it
- [dbt-ops runbook](../runbooks/dbt-ops.md) — rebuilding specific partitions
- [`dbt_packages` incident](./2026-08-dbt-deps-429.md) — the document that predicted this shape
- [Design: transformation §2](../design/transformation.md) · [Design: testing strategy §6](../design/testing.md)
