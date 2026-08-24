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

## Adjusting lookback windows

| Variable | Constraint |
|---|---|
| `stg_orders_lookback_days` | default 3; must be ≥ the E/L `>=` re-extraction range + margin |
| `stg_quality_events_lookback_days` | widen together with the above |
| `rpt_quality_events_lookback_days` | **must be ≥ `stg_quality_events_lookback_days`** |

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: 7}'
```

Values passed this way are per-invocation and are not persisted.

For recovery after consecutive failures, see [dag-failure-recovery](./dag-failure-recovery.md).

---

## Proposal C targeted refresh

Correction rows land in **old** partitions that the lookback window cannot see. The last step of a repair is a targeted refresh of the affected partitions:

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: <N covering the correction>}'
# or --full-refresh
```

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
