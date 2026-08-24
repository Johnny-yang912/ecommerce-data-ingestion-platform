# Orchestration Layer: Airflow

**English** | [繁體中文](../../zh-TW/design/orchestration.md)

---

## 1. Scope, and what Airflow is not

Airflow owns **batch scheduling**: ODS → BigQuery → dbt. It is **not** the ingestion path's task queue — that is Celery + Redis ([queue](./queue.md)), and the two deliberately do not share a Redis instance.

> ⚠️ **This is a portfolio project and the data source is simulated.** `seed_demo_daily` *is* the upstream. What that covers and does not cover: [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

Airflow 3.0.0, LocalExecutor, single host. No triggerer (only `BashOperator` is used), no Cosmos (13 models).

---

## 2. DAG topology

Six DAGs. Every schedule is declared explicitly in `Asia/Taipei`.

| DAG | Schedule | Does |
|---|---|---|
| `seed_demo_daily` | 10/13/17/21 | the simulated upstream — 800 orders/day |
| `raw_pending_watch` | 10:30/13:30/17:30/21:30 | dispatch-liveness probe, 30 min after each seeding slot |
| `orders_analytics_daily` | 22:30 | 2 extracts → 4 layered `dbt build`s → a full `dbt test` |
| `source_freshness_watch` | 08:00 | backstop for extract |
| `dq_reevaluation` | **manual** | Proposal B, dry-run by default |
| `seed_demo_gate_demo` | **manual** | the Hard Gate interception scenario |

**None of the four scheduled DAGs depends on another at the Airflow level — the ordering contract exists only in the time gaps.** That is deliberate: each one's red means a different thing, which is the entire reason they are separate.

---

## 3. Execution model

**Two venvs inside the image**, and nothing project-related installed into Airflow itself:

```
/home/airflow/venvs/analytics   ← requirements-analytics.txt
/home/airflow/venvs/dbt         ← dbt-core / dbt-bigquery 1.11
```

**DAG files import no project module at top level.** `config.py` instantiates `Settings` at import time with a mandatory `db_url`, and the dag-processor re-parses every file every few dozen seconds — a top-level import means that a parsing process without `DB_URL` makes **the entire DAG disappear from the UI**, with no red light at all.

Everything is a `BashOperator`, which pushes imports to execution time. Two side benefits: `tests/test_dags.py` can parse the DagBag with no database and no environment, and Airflow 3's separate dag-processor makes the discipline more important, not less. [ADR-0035](../adr/0035-two-venvs-dependency-isolation.md) · [ADR-0036](../adr/0036-dag-no-toplevel-import.md)

**`profiles.yml` lives in `orchestration/dbt_profiles/`**, not in `ecommerce_dbt/` — dbt's lookup puts the working directory ahead of `~/.dbt`, so putting it there would break local `dbt run`. It reuses the same environment variables as `config.py`, so producer and consumer cannot point at different datasets. [ADR-0041](../adr/0041-profiles-yml-structure-vs-values.md)

---

## 4. The analytics DAG

```
extract_orders ─┐
                ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test
extract_quality_events ─┘
```

**One extract task per table**, because retry granularity should match failure granularity — and the cross-table gate is the dependency edge itself (dbt's upstream = both extracts succeeded).

**Layered dbt execution with `--indirect-selection=buildable`.** The default `eager` would select the partition invariant test during the *staging* task, asserting against a half-rebuilt state; `cautious` would never run it. `buildable` lands it in the intermediate task with all inputs fresh.

**A full `dbt test` closes the DAG** — the per-layer tests are the *gate*, the closing run is *completeness*. **A silently skipped test is far worse than a duplicated one.**

> ⚠️ **Never split `dbt build` into `dbt run` + `dbt test`.** That makes `int_`'s upstream *"staging's run"* instead of *"staging's test"*, and the Hard Gate silently stops blocking while dirty data flows into Gold. Pinned by `tests/test_dags.py::test_dbt_never_splits_run_and_test`.

**Retries are deliberately asymmetric**: `extract_* = 2` (transient failures), `dbt_* = 0` (deterministic failures). BigQuery's transient errors are handled at the adapter layer by `job_retries: 1` — far more precise than re-running a whole `dbt build`. [ADR-0038](../adr/0038-asymmetric-retries.md) · [ADR-0040](../adr/0040-layered-dbt-execution.md)

---

## 5. Scheduling semantics

**`catchup=False` is structural.** The watermark is destination-derived, so a backfill run for a past date still extracts "the increment as of now" — N catch-up runs would do the same thing N times. **This is not a backfillable DAG**, and making it one would require a right-hand time bound that cuts off late-arriving rows, contradicting the `>=` semantics.

**`max_active_runs=1` is correctness**: concurrent dbt `insert_overwrite` on the same partitions would overwrite each other.

**`dq_reevaluation` has `schedule=None`.** Proposal B fires on a rule loosening — a human deploy event, not a period. **Schedules belong on things that change by themselves; rules do not.** [ADR-0037](../adr/0037-catchup-false-structural.md)

### ⚠️ Cross-timezone extraction: unresolved

Schedules are declared in `Asia/Taipei` (seeding at 10/13/17/21, extraction at 22:30), but `received_at` is a TIMESTAMP and BigQuery partitions by **UTC** day — `date()` rolls over eight hours apart.

**The mismatch is currently invisible**, because all four seeding slots fall between Taipei 08:00 and 24:00, mapping to UTC 00:00–16:00 on the same day. **That is a consequence of the slots that were picked, not a property of the system**: with round-the-clock ingestion, orders placed between Taipei 00:00 and 08:00 land in the *previous* UTC partition.

The blast radius splits along a meaningful line:

| | Time axis | Affected |
|---|---|---|
| `staging.orders` partition → the Hard Gate's "latest batch" verdict | `received_at` | ✅ yes |
| `rpt_quality_events_daily.event_date` | `event_at` | ✅ yes |
| source freshness's recent-window filter | `received_at` | ✅ yes |
| `fct_orders` / `fct_order_items` / `rpt_sales_daily_by_category` | `order_date` | ❌ no |

`order_date` comes from the payload and is already a `DATE` — it has no timezone. In other words:

> **The revenue numbers are right. "Which day the quality belongs to" is off by eight hours.**

That distinction governs whether and when to fix it: BI can read revenue as-is, while the DQ dashboard will attribute a boundary-crossing incident to the wrong day.

### The three candidate approaches

**None has been taken, and none is a purely technical decision:**

| | Approach | Cost |
|---|---|---|
| **a** | Partition staging on a business-timezone `DATE` | Most correct semantically; requires **rebuilding and backfilling** the table |
| **b** | Keep the UTC partition; derive a `business_date` in `stg_` for downstream use | Leaves partitions alone, but **adds a column whose definition must be maintained** |
| **c** | Declare explicitly that quality metrics are UTC-day-based, and put that in the report definitions | **Zero cost**, but asks report readers to accept a grain that disagrees with the operating day |

**Deliberately unchosen**: under the current ingestion pattern **all three produce identical output** — see "currently invisible" above. **No choice can be validated until there is real traffic that crosses the day boundary.** [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)

---

## 6. Observation signals

**An observation signal has neither the authority to block downstream nor to pollute another DAG's success rate.** A DAG run's state is the aggregate of its tasks, so an expected-red leaf task makes "main pipeline success rate" worthless.

Each DAG's red therefore means exactly one thing:

| DAG | Red means | Look at |
|---|---|---|
| `seed_demo_daily` | nothing is getting in | API, seeding script |
| `raw_pending_watch` | rows reach Raw, nobody claims them | redis / worker / beat |
| `orders_analytics_daily` | the pipeline is broken | extract or dbt |
| `source_freshness_watch` | staging was not moved forward | watermark and extract |

Pinned by `tests/test_dags.py::TestFreshnessIsolation`.

**Three timelines, one hop each** — merged, a single red would stand for two pipeline segments:

| Timeline | Hop | Watched by |
|---|---|---|
| `raw.received_at` | upstream + API | OTel (absent alerting not written) |
| `raw.received_at` → `ods.received_at` | dispatch | `raw_pending_watch` |
| `ods.received_at` in staging | extract | `source_freshness_watch` |

**`source_freshness_watch` runs at 08:00 because it is a backstop**: if extract reports success but moved nothing, the Hard Gate judges yesterday's partition and passes, `dbt test` is green — this is the only thing that speaks up before someone opens the report at 09:00. Its 26h/50h thresholds are one **loading cycle** plus two hours of grace.

⚠️ **"A Raw row with no matching ODS row" cannot be the definition of a fault** — `duplicate` and `error` are correct terminal states that produce no ODS row. `pending` age is the clean signal. [ADR-0039](../adr/0039-observation-signals-own-dag.md)

---

## 7. Failure notification

The four scheduled DAGs carry an `on_failure_callback` whose message states **the response**, not the task name — that information used to live only in docstrings, and the person handling an incident does not read docstrings.

**Attached at task level**, because downstream `upstream_failed` tasks do not fire the callback: a broken seven-task chain sends exactly one message, naming the task that actually broke.

**The transport defaults to a log line**; a real channel is one `NOTIFY_WEBHOOK_URL` away. Every message carries `channel=`, so `channel=log` says plainly that nobody was notified.

⚠️ **Covers "ran and failed" only.** Should-have-run-didn't (Airflow 3 removed SLAs), machine powered off, and freshness's `warn` (exit 0, task green) are all invisible to it. [ADR-0042](../adr/0042-failure-notification-response-not-task.md) · [liveness-alerting](./liveness-alerting.md)

---

## 8. Infrastructure

**Airflow's metadata DB is a separate instance** from the business DB. The reason is not fastidiousness: the business DB is the precondition for Proposal C's "rebuild from Raw", and restoring it should not roll back Airflow's execution history along with it.

**LocalExecutor** — single host, a handful of tasks. CeleryExecutor would add two containers and a broker, and that Redis would collide conceptually with the ingestion path's.

**Versions**: Airflow 3.0.0 (`apache/airflow:3.0.0-python3.12`), dbt-core / dbt-bigquery 1.11. When upgrading Airflow, `ARG AIRFLOW_VERSION` in `orchestration/Dockerfile` and `AIRFLOW_VERSION` in `.github/workflows/dags.yml` must change together — the constraints file is fetched by version.

---

## 9. Related

- [cloud-layer](./cloud-layer.md) · [transformation](./transformation.md) — what the DAG executes
- [data-quality](./data-quality.md) — the Proposal B mechanism `dq_reevaluation` drives
- Runbooks: `airflow-startup`, `airflow-silent-stall`, `proposal-b-rollout` (stage 4)
