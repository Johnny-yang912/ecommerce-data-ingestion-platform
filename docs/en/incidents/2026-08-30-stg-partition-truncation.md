# Half a day overwrote a whole day: one defect hiding in three models

**English** | [繁體中文](../../zh-TW/incidents/2026-08-30-stg-partition-truncation.md)

| | |
|---|---|
| **Date** | Occurred 2026-08-29 20:38 · found 2026-08-30 10:20 · **repaired same day in two phases** (11:24 / 23:10) |
| **Impact** | The `2026-08-26` partition of both `stg_orders` and `stg_quality_events`: 800 rows → **250 rows**; the gap propagated to Gold and to the quality reports |
| **Alerting** | **None.** DAGs green, all 93 dbt tests green, upstream staging untouched |
| **Detection lag** | Orders ~14 hours; **quality events ~26 hours — and only after phase one had been declared complete** |
| **Root cause** | The incremental window's left boundary carried the run's wall-clock time; `insert_overwrite` overwrote a whole day with half a day. **The same defect existed in three models** |
| **Status** | Closed. Root cause fixed in all three models, data restored, detection added (two reconciliation tests) |

> ⚠️ **The phase-one repair recorded in this report was incomplete.** That morning only one of the three models was fixed, and the incident was declared closed on that basis.
> The full account is in [Phase two: the repair was incomplete](#phase-two-the-repair-was-incomplete) — **that section is the part of this incident worth reading.**

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

⚠️ **The table above was taken as proof of closure at the time. It covers one of the three affected models.**

---

## Phase two: the repair was incomplete

The 11:24 repair drove `stg_orders`'s per-partition reconciliation to zero everywhere and left all 94 tests green. The judgement at the time was that the incident was closed.

At 23:00 that evening, a routine re-check found Looker's quality panel still showing 250 rows for `2026-08-26` — **and not one signal in the warehouse was red.**

Sweeping every table and every date column in both datasets for `2026-08-26`, exactly one came back as 250:

| Table | `2026-08-26` | Expected |
|---|---|---|
| `stg_orders` (fixed that morning) | 800 | 800 ✅ |
| **`stg_quality_events`** | **250** | 800 ❌ |
| **`rpt_quality_events_daily`** (what Looker actually reads) | **250** | 800 ❌ |

`stg_quality_events.sql` carried a **verbatim identical** unfixed boundary. Same 20:38 run, same cutoff, same 550 rows gone.

### Why the events side was harder to see than the orders side

The morning restored 550 orders, but their quality events did not come back with them. `int_orders` picks up `quality_state_at` through a **LEFT JOIN**, so those 550 rows returned to Gold **carrying a NULL quality state**:

| | `quality_state_at` IS NULL on `2026-08-26` |
|---|---|
| `int_orders` | 499 / 733 |
| `int_orders_quarantine` | 51 / 67 |

`499 + 51 = 550` — **the very same rows** the morning had restored to Gold.

⭐ The point: **the row count was complete** (all 800 present, none missing), so even the reconciliation test added that morning could not see it. **A LEFT JOIN translates "upstream lost rows" into "downstream has NULL columns"** — the same damage wearing a different disguise, and nothing in the project was watching for that disguise.

### The third model: one that had not broken, merely gone uncovered

After backfilling `stg_quality_events` and rebuilding downstream with `stg_quality_events+`, `rpt_quality_events_daily` reported `0 processed` — `stg_` was already 800 while the table Looker reads was still 250.

It is the **third instance** of the same defect (`insert_overwrite` + DAY partition + an unaligned `current_timestamp() - N day`), but it presented differently:

> **It had not been overwritten wrongly. It had simply never been covered by the window.** The old partition sits outside the lookback window, so backfilling upstream without backfilling it leaves BI showing the stale value — green all the way.

The reason it had not yet blown up was **luck**: ingestion batches all land before 13:00 UTC and the scheduled run is at 14:30, so the boundary day happened to select zero rows and the partition was therefore never added to the overwrite set. **Change the ingestion schedule and it breaks.** Correctness that holds only because of where the run time sits relative to the ingest time is not correctness.

### Phase-two response

All three boundaries aligned to the day edge; `stg_quality_events` and `rpt_quality_events_daily` each given targeted-backfill vars; a second reconciliation test added.

```bash
dbt run -s stg_quality_events   --vars '{stg_quality_events_backfill_start: "2026-08-26"}'
dbt run -s stg_quality_events+ --exclude stg_quality_events
# ⚠️ Downstream incremental models cannot see the old partition — backfill it with the same dates
dbt run -s rpt_quality_events_daily --vars '{rpt_quality_events_backfill_start: "2026-08-26"}'
```

| | Before | After |
|---|---|---|
| `stg_quality_events`, 8/26 partition | 250 | **800** |
| `rpt_quality_events_daily`, 8/26 | 250 | **800** |
| `int_orders`, 8/26 `quality_state_at` NULL | 499 | **0** |
| `int_orders_quarantine`, same | 51 | **0** |
| New reconciliation test | FAIL | **PASS** |
| Full dbt test suite | 94 / 94 | **95 / 95 PASS** |

> The second reconciliation test also went **red then green**: against the pre-repair state it emitted `2026-08-26 / 800 / 250 / 550`, and passed after the backfill.

**Idempotency, verified in practice**: a plain incremental run with no vars was executed at 23:15 Taipei — precisely the "manual run at an off-schedule hour" that caused the incident — and the `2026-08-26` and `2026-08-27` boundary partitions came through untouched. That is what this fix actually had to prove.

---

## Why phase one was incomplete

This was not "we forgot the other two". The repair's **scope was drawn from the wrong thing**:

> **Scope was drawn from "which table was observed to be wrong", not from "what does the defect look like".**

And the observation itself was biased, so only half of it surfaced:

| | Orders side | Events side |
|---|---|---|
| Any signal that morning? | Yes — the BI revenue line got shorter | No |
| Any test that would go red? | Yes (the new reconciliation test) | No (the test names `stg_orders` only) |
| Outcome | Fixed | Missed |

The third model was more hidden still: it **had not broken at all**, so no search for "things that are broken" could have found it. It surfaced only during the backfill, as a `0 processed`.

**The correct way to draw the scope is grep.** The defect lives at the level of *how the line is written*, so its extent equals "how many places copied that idiom" and has nothing to do with "how many places have failed yet". Phase two therefore began by sweeping the whole project for the `insert_overwrite` + time-window combination — three models, all handled — and confirming that the extraction path (`WRITE_APPEND`) and the other nine `table` models are structurally immune.

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

> **Fixing one model does not make another one better.** The defect lives at the level of the idiom, so it propagates to every model that copied the boundary. Repair scope must therefore be drawn by **grep**, not by symptom — "what is broken" is a *subset* of "what has this idiom", and a deceptive subset at that: the untouched ones are not safe, merely not yet reached. **By the same token every model needs its own reconciliation test; you cannot infer that this one is safe because that one is tested.**

> **"The row is still there" is not enough either.** The reconciliation test added that morning asks whether rows still exist, one level stronger than a content test — but all 800 rows in `int_orders` were present and what was missing was a column value. **A LEFT JOIN translates upstream row loss into downstream NULLs**, so the loss disguised itself as absence-of-value and walked straight past the defence built for row loss. Defences are shaped around the shape of the damage, and damage changes shape.

A fifth point, about why this was recoverable at all: **`staging` is append-only and had not been touched.** That is not something this incident fixed — it is the one thing that did not break, and **it is the sole reason all 550 rows could be restored exactly** ([ADR-0025](../adr/0025-staging-additive-only.md)). An immutable upstream is a cost right up until the day you need it.

---

## Related

- [ADR-0055](../adr/0055-partition-aligned-incremental-window.md) — the decision this produced
- [ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md) — the incomplete invariant
- [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md) — the defence that covered half of it
- [dbt-ops runbook](../runbooks/dbt-ops.md) — rebuilding specific partitions
- [`dbt_packages` incident](./2026-08-dbt-deps-429.md) — the document that predicted this shape
- [Design: transformation §2](../design/transformation.md) · [Design: testing strategy §6](../design/testing.md)
