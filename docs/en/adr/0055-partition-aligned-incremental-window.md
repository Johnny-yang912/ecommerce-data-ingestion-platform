# ADR-0055: An incremental window's boundary must be a partition boundary; backfills take dates, not day counts

**English** | [繁體中文](../../zh-TW/adr/0055-partition-aligned-incremental-window.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt |

---

## Context

[ADR-0046](./0046-stg-incremental-int-full-rebuild.md) rests `stg_orders`'s incremental correctness on one invariant: **all copies of a given `raw_id` land in the same `received_at` partition**, so atomic whole-partition replacement leaves nothing behind.

The invariant is true, but it **only describes the inside of the window**. It guarantees that whatever the window can see gets deduped completely; it says nothing about where the window's boundary falls. And `insert_overwrite`'s atomic unit is a **whole partition** — dbt overwrites exactly the partitions that appear in the query result. Multiply the two and you get a failure the invariant does not cover:

> If the left boundary lands **mid-day**, only part of that day's rows make it into the window, so **`insert_overwrite` atomically overwrites a whole day with half a day** and everything outside the window is silently deleted.

The original left boundary was `timestamp_sub(current_timestamp(), interval N day)` — carrying **the wall-clock time of the run**. That turned "this model is correct" into a conditional: true only when the run happens later in the day than the day's last batch arrives. The scheduled run is 22:30 Taipei; the tail of the last seeding batch is around 21:05. Those 85 minutes were the pipeline's only margin of safety — **and it was never written down, nor pinned by any test.**

A manual run at 2026-08-29 20:38 landed on the wrong side of that gap and cut the `2026-08-26` partition from 800 rows to 250 ([incident report](../incidents/2026-08-30-stg-partition-truncation.md)).

The pipeline also lacked a repair tool. The only way to rebuild an old partition was to **widen the lookback window** — a "count back N days from today" parameter that inherits the same floating boundary, and that drags every day after the target into the rebuild.

## Decision

**1. The incremental window's left boundary aligns to the day boundary.**

```sql
where received_at >= timestamp_sub(
    timestamp_trunc(current_timestamp(), day),   -- this layer is correctness, not style
    interval {{ var('stg_orders_lookback_days', 3) }} day
)
```

The edge day is now either wholly inside the window or wholly outside it. "Half a day" ceases to exist, and **the time of day a run happens stops being an implicit precondition for correctness**.

**2. The repair path is parameterised by date, independent of the clock.**

```bash
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "2026-08-26"}'
```

`stg_orders_backfill_end` is optional; omitted, it backfills `start`'s day alone. The routine path is unchanged and still uses the rolling window.

`_end` is deliberately optional: backfilling a single day while passing the same date as `_end` (rather than the day after) would silently select the empty set, and **a repair path is the worst place to leave a hidden trap** — the person typing the command during an incident often does not know this pipeline well.

**3. A per-partition reconciliation test** (`assert_stg_orders_matches_staging`) guards the contract. Its window must be **greater than** the lookback window.

## Why this right boundary does not contradict ADR-0037

[ADR-0037](./0037-catchup-false-structural.md) explicitly rejected a right boundary:

> Real backfillability would require slicing on `received_at >= data_interval_start AND < data_interval_end` … **that right boundary would cut off late-arriving rows**

The backfill's `_end` is also a right boundary. **The two hang on different axes:**

| | The right boundary ADR-0037 rejected | The backfill's `_end` |
|---|---|---|
| Bounds what | A **time interval owned by one scheduled run** | `received_at`, an **immutable attribute of a row** |
| Who decides it | Airflow's `data_interval` | The person doing the repair |
| Why it would cut off late arrivals | That run **happens exactly once**; rows loaded after the interval closes will never have another run look at them | It doesn't — a backfill reads staging's **entire current contents**, and a late-loaded row's `received_at` is unchanged, so it is still in the same partition |
| Used on | The routine path | The repair path only |

The decisive difference is **one-shot-ness**: the shape 0037 rejected ties "were these rows extracted" to a run that never repeats. A backfill has no such property — it is ad hoc and can simply be run again.

**The only reason this section exists is so that a reader who knows 0037 does not mistake this for a contradiction — or, in the other direction, carry this shape back to the extract layer where it does not belong.**

## Consequences

**The time of day disappears from the correctness conditions.** Manual runs, catch-up runs, a run at 3am by whoever is on call — same result. That is the main gain, and it trades an unwritten 85-minute gap for a structural guarantee.

**The repair path no longer has a time condition.** Repairs happen during incidents, and incidents do not pick their moment. A repair tool that comes with a "only safe between X and Y" caveat is guaranteed to be unusable exactly when it is needed.

**Cost 1: the window can span up to one extra day.** Day-alignment only ever moves the left boundary *earlier*, never later. Negligible at today's ~800 rows/day; the first thing to re-evaluate as volume grows.

**Cost 2: one more branch in the model.** The routine and backfill paths are two different `where` clauses and can drift. Mitigated by compiling all three branches, and by a reconciliation test that guards the *result* regardless of which path produced it.

**Cost 3: the detection window is 7 days, not unlimited.** The reconciliation test moves "how long until row loss is noticed" from **never** to **within a day**, but only inside 7 days. Rows arriving later than that (the Proposal C shape, see ADR-0046) are still silent — this ADR does not close that gap.

**Cost 4: repairs leave no automatic audit record.** A targeted backfill is one shell invocation — it fixes the data, but the system retains nothing saying `2026-08-26` was ever rebuilt. Answering "why does this day disagree with upstream?" three months from now depends on incident reports, the CHANGELOG and git history — **all three of which rely on a person remembering to write them**.

⚠️ This gap is narrower than it sounds: **the record is manual, the detection is not.** The next instance of row loss will be caught by `assert_stg_orders_matches_staging`, which names the day and the shortfall; nobody has to remember to run it. What is manual is only writing down that a repair happened.

The mitigation is to make "record it" a **step** in the [dbt-ops runbook](../runbooks/dbt-ops.md) rather than an expectation. Having the system produce that record is deferred; conditions in [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md).

## Alternatives considered

**Make `stg_orders` a full rebuild again.** This would eliminate the entire failure class, and at today's 14,587 rows the cost rounds to zero. Rejected not on cost but because [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) uses `stg_`'s incremental as the demonstration that *incremental is safe at this layer* — a conclusion that still stands; **it just needed a precondition it had not finished stating**. Abandoning incremental would delete the demonstration along with the problem.

**Switch to the `merge` strategy.** This removes the class at the root: `merge` combines row by row on `unique_key`, so a miscalculated window merely fails to update — **it does not delete**. The two failure modes are a full severity level apart. Technically unavailable: BigQuery sandbox forbids DML, the same constraint that forced `copy_partitions` ([ADR-0044](./0044-copy-partitions-sandbox-dml.md)). **This is the first choice once billing is enabled.**

**Let each Airflow run own its partition via `data_interval`.** A repair becomes "clear that day's run in the UI", and Airflow produces the audit record automatically.

**The difference from the chosen approach is not repair capability, it is the record.** Both can repair any partition, and both are independent of the run's clock; one leaves a run history, the other leaves a shell invocation. With a single operator and a very low repair rate, the record written by a person into an incident report and the CHANGELOG is actually richer — it can state **why** the backfill was needed, which a run history cannot.

What it would additionally cost: every run now *owns* an interval, `catchup` has to be reconsidered as a real decision, and there is one more contract to maintain between the DAG and dbt. **That is trading complexity for something a person can currently write by hand.**

⚠️ Generalising the same shape to the **routine path** (not just backfills) would additionally run into ADR-0037's reason for rejecting right boundaries, and the reason holds at this layer too: a row whose `received_at` falls inside an interval but which lands in staging after that interval's run has completed will never have a second run look at it. **The rolling window is correct on the routine path**, and that has not changed.

The deferral and its restart conditions are recorded in [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md).

**Widen the lookback window only, with no targeted backfill.** The pre-incident status quo. Safe now that the boundary is fixed, but it can still only express "N days back from today": rebuilding one old partition drags every day after it along, and the operator has to convert a date into a day count themselves.

## Revisit when

- **BigQuery billing is enabled** — switch to `merge` and reassess whether this ADR still needs to exist.
- **The `int_` layer goes incremental** — the same boundary rule must be applied there, and ADR-0046 already records that its reselection set is more complex (lookback partitions ∪ partitions of `raw_id`s with recent quality events).
- **Volume makes "up to one extra day" material.**
- **A second operator appears** — Cost 4's hand-written record stops being reliable; see [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md).

## Related

- [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) — the invariant this completes
- [ADR-0037](./0037-catchup-false-structural.md) — the right boundary that was rejected
- [ADR-0044](./0044-copy-partitions-sandbox-dml.md) — the constraint that forced `insert_overwrite`
- [ADR-0023](./0023-watermark-approach-a.md) — `>=` re-extraction, the source of the lookback window's lower bound
- [2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md)
- [Transformation design §2](../design/transformation.md) · [Testing strategy §6](../design/testing.md)
- [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md) — the deferred alternative
