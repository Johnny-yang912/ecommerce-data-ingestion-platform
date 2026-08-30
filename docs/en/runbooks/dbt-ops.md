# Runbook: dbt operations

**English** | [繁體中文](../../zh-TW/runbooks/dbt-ops.md)

---

## When to `--full-refresh`

Only these cases:

- changing partition or cluster configuration
- changing dedup logic
- recomputing history **outside** the lookback window
- first-time build

It uses DDL (`CREATE OR REPLACE`), so it is unaffected by the sandbox's DML ban.

**Applies to `stg_`'s incremental models only.** `int_` is a `table` full rebuild on every run anyway ([ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md)).

---

## ⚠️ Before changing `int_orders` or `int_orders_quarantine`

Walk the **alignment checklist** — the two models must remain a partition of `stg_orders`:

| # | Check |
|---|---|
| 1 | Both define `is_effectively_clean` identically; one `WHERE cond`, one `WHERE NOT cond` |
| 2 | `coalesce(..., false)` **not dropped** |
| 3 | Always `LEFT JOIN` |
| 4 | Window `partition by` / `order by` tiebreaks match |
| 5 | `effective_quality_state` CASE branches match |
| 6 | Same materialisation on both |
| 7 | `assert_orders_split_is_partition` stays `severity: error` |

Then:

```bash
dbt build --select intermediate+
```

and confirm `assert_orders_split_is_partition` is **green**. That test is the only automated safety net for the duplicated block ([ADR-0045](../adr/0045-int-effective-state-duplication.md)).

> #2 is the one people miss. With `has_clean_error=TRUE` and no event, `FALSE OR NULL = NULL` — and `WHERE NOT NULL` is also NULL, so the row **vanishes from both tables at once**, silently.

---

## Rebuilding specific partitions

Use this when you know which days are damaged. **Do not** widen the lookback window
instead — that window counts back from today, so covering one old partition drags
every day after it into the rebuild.

```bash
# one day
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "2026-08-26"}'

# a range (end is EXCLUSIVE)
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "2026-08-26", stg_orders_backfill_end: "2026-08-29"}'

# let downstream follow (int_/dim_/fct_ and rpt_sales/rpt_quality_backlog are table materialisations)
dbt run -s stg_orders+ --exclude stg_orders
```

`_end` is optional; omitting it backfills `start`'s day alone.

⚠️ **`--exclude stg_orders` in the second step is not optional.** Without it,
`stg_orders` runs again on the rolling window and pushes the partition you just
repaired back outside it — the fix is overwritten by its own next step.

### Backfilling the events side (`stg_quality_events`)

⚠️ **`stg_orders` is not the only model that breaks.** Quality events run a parallel
pipeline with their own vars:

```bash
dbt run -s stg_quality_events --vars '{stg_quality_events_backfill_start: "2026-08-26"}'
dbt run -s stg_quality_events+ --exclude stg_quality_events
```

### ⚠️ Downstream *incremental* models do not follow on their own

The `+` above only makes **table-materialised** downstream recompute. A downstream model
that is itself incremental **only recomputes partitions inside its own lookback window** —
the old partition you just backfilled falls outside it, so it keeps its stale value.

Today the only such downstream is **`rpt_quality_events_daily`** (the table Looker's
quality panel reads):

```bash
dbt run -s rpt_quality_events_daily --vars '{rpt_quality_events_backfill_start: "2026-08-26"}'
```

> Symptom of skipping this step: **upstream correct, downstream wrong, every test green,
> and BI still showing the old number.** The 2026-08-30 repair stalled exactly here —
> `stg_quality_events` was already back to 800 while Looker still showed 250.

**Rule of thumb**: after backfilling any partition, check whether anything downstream is
`materialized='incremental'`. If so, backfill it with the same dates.

**This path does not depend on the time of day**; it produces the same result whenever
you run it. That is not luck, it is what [ADR-0055](../adr/0055-partition-aligned-incremental-window.md)
bought.

### Record the backfill

⚠️ **Nothing reminds you to do this, and nothing turns red if you skip it.**

A backfill is one shell invocation — it repairs the data, but the system keeps no trace
that the partition was ever rebuilt. "Why does this day disagree with upstream?" three
months from now can only be answered by what a person wrote down.

The minimum: **which partition · row count before · row count after · why**.

| Situation | Where it goes |
|---|---|
| One-off data repair | An incident report (`docs/en/incidents/`); plus a CHANGELOG line if it changed a decision |
| Routine targeted refresh (e.g. Proposal C) | Whatever record that operation already has |

> Having the system produce this record automatically is feasible (let Airflow runs own
> their partitions); it is deliberately deferred — see [PORTFOLIO_SCOPE #13](../PORTFOLIO_SCOPE.md).

---

## Adjusting lookback windows

| Variable | Constraint |
|---|---|
| `stg_orders_lookback_days` | default 3; must be ≥ the E/L `>=` re-extraction range + margin |
| `stg_quality_events_lookback_days` | widen together with the above |
| `rpt_quality_events_lookback_days` | **must be ≥ `stg_quality_events_lookback_days`** |
| `stg_orders_recon_window_days` | default 7; **must be > `stg_orders_lookback_days`** |

> ⚠️ **Widen the reconciliation window alongside the lookback window.** If the
> reconciliation window is not larger, a damaged partition slides out of both before
> anyone sees it: scheduled runs stop rebuilding it and reconciliation stops checking it.
> The damage is frozen in place, under a green test.

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: 7}'
```

Values passed this way are per-invocation and are not persisted.

For recovery after consecutive failures, see [dag-failure-recovery](./dag-failure-recovery.md).

---

## Proposal C targeted refresh

Correction rows land in **old** partitions that the lookback window cannot see. The last
step of a repair is a targeted refresh of the affected partitions — use "Rebuilding
specific partitions" above and pass the dates the corrections fall on:

```bash
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "<earliest received_at day>", stg_orders_backfill_end: "<latest day + 1>"}'
```

⚠️ Earlier versions of this runbook said to widen `stg_orders_lookback_days`. Before the
[2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md) that was the
only tool available, but it rebuilds every day after the correction as well, and the
window's boundary used to float. **Pass dates, not a number of days.**

---

## ⚠️ Never split `dbt build` into `dbt run` + `dbt test`

That makes `int_`'s upstream *"staging's **run**"* instead of *"staging's **test**"* — and **the Hard Gate silently stops blocking** while dirty data flows into Gold. Nothing errors; the DAG is green; the gate is decorative.

Pinned by `tests/test_dags.py::test_dbt_never_splits_run_and_test`. [ADR-0040](../adr/0040-layered-dbt-execution.md)

---

## Reading a red layer

| Red task | Means | First check |
|---|---|---|
| `dbt_staging` | the **Hard Gate** fired, or a `stg_` test failed | is the latest partition genuinely >15% bad, or is this the false positive below? |
| `dbt_intermediate` | the **partition invariant** broke | walk the alignment checklist above |
| `dbt_marts` | rollup mismatch, or a Gold test | `assert_fct_orders_rollup_matches_items` |
| `dbt_test` (closing run) | a test that the layered runs skipped | compare which tests ran per layer |

⚠️ **Hard Gate false positives are expected at this configuration.** The simulated upstream's dirty rate is 0.12 and the gate threshold is 0.15; at n≈200 the batch standard deviation is ~2.3 percentage points, so **roughly one batch in ten trips the gate on random variation alone**. Rule that out before reading it as an upstream failure ([ADR-0028](../adr/0028-hard-gate-per-batch-scope.md)).

`dbt_*` tasks have `retries=0` deliberately — a red one has not been retried, so it means what it says.

---

## Related

- [design/transformation](../design/transformation.md) — what each layer does
- [schema-change](./schema-change.md) — adding or removing a column
