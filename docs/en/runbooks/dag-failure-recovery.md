# Runbook: Recovering from consecutive DAG failures

**English** | [繁體中文](../../zh-TW/runbooks/dag-failure-recovery.md)

---

## When this applies

`orders_analytics_daily` has failed **for more consecutive days than the lookback window** (default 3).

**A single failure is safe** and needs nothing: staging has already appended, the watermark has advanced, and `stg_`'s lookback window recomputes those days on the next run.

**Consecutive failures are the danger.** With a 3-day window, a DAG down for 4 days looks back only 3 days on recovery — so rows that landed in staging **before that boundary never reach `stg_orders`**. No error. No self-healing. **Silent data loss.**

---

## Procedure

### 1. Count the days down

```bash
docker compose exec airflow-db psql -U airflow -d airflow -c \
  "select max(run_after) from dag_run
    where dag_id='orders_analytics_daily' and state='success';"
```

`N = days since that timestamp + margin`.

### 2. Fix whatever broke the DAG

Do not proceed until the DAG can run. Widening the window on a still-broken pipeline just repeats the failure with a bigger query.

### 3. First run after the fix — widen the window

```bash
docker compose exec airflow-scheduler bash -c \
  "/home/airflow/venvs/dbt/bin/dbt build \
     --select path:models/staging \
     --vars '{stg_orders_lookback_days: N}'"
```

⚠️ **`stg_quality_events` and `rpt_quality_events_daily` must be widened together.** Their variables are `stg_quality_events_lookback_days` and `rpt_quality_events_lookback_days`, and the constraint is `rpt_ >= stg_`.

```bash
--vars '{stg_orders_lookback_days: N, stg_quality_events_lookback_days: N, rpt_quality_events_lookback_days: N}'
```

Alternatively, `--full-refresh` — correct, unconditional, and it costs a full-table rewrite.

### 4. Verify nothing was lost

```sql
-- staging vs stg_orders row counts over the affected range should agree
select count(*) from `<project>.staging.orders`
 where received_at >= '<start>' and received_at < '<end>';

select count(*) from `<project>.<dbt_dataset>.stg_orders`
 where received_at >= '<start>' and received_at < '<end>';
```

### 5. Return to the default window

The widened value is passed per-invocation and is **not** persisted — the next scheduled run is back to 3 days automatically. Nothing to undo.

---

## Prevention

> **The lookback window is really a declaration of how much unattended failure the pipeline tolerates**, not just a cost parameter.

Airflow failure alerts must be seen **before** cumulative downtime approaches the window. That is what the failure notification ([ADR-0042](../adr/0042-failure-notification-response-not-task.md)) is for — and it covers "ran and failed" only, so a machine that was simply off is invisible to it.

---

## Related

- [dbt-ops](./dbt-ops.md) — when to `--full-refresh` generally
- [design/transformation](../design/transformation.md) — how the lookback window works
