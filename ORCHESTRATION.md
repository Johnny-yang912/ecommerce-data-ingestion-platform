# Orchestration Layer Architecture: Airflow

## Scope and Responsibility Boundary

This document records the design decisions of the **orchestration layer** — *when things run,
what follows what, and what happens on failure*. Each layer's own correctness contract lives
elsewhere: quality contracts in [DQ_ARCHITECTURE](./DQ_ARCHITECTURE.md), E/L and staging
infrastructure in [CLOUD_LAYER](./CLOUD_LAYER.md), transformations in
[ecommerce_dbt/README](./ecommerce_dbt/README.md).

```
ODS (PostgreSQL) ──[E/L]──► BQ staging ──[T: dbt]──► stg_/int_/dim_/fct_/rpt_
                     ▲                        ▲
                     └──────── Airflow ───────┘   ← you are here
```

**Airflow is not a task queue.** In the roadmap, "Airflow" and "Celery + Redis" are two
**orthogonal** items; conflating them warps the whole design:

| | Airflow | Celery + Redis |
|---|---|---|
| Replaces | the previously manual `extract_ods_to_bq.py` + `dbt build` | `BackgroundTasks` (`process_raw_event`) |
| Time scale | minutes to hours, batch | milliseconds to seconds, per record |
| Trigger | clock / human | the HTTP request path |
| Failure semantics | re-run the SQL, resume from the watermark | requeue one record |

Consequence: **`process_raw_event` must never move into Airflow.** Using a scheduler as a task
queue runs into DAG parse intervals (second-scale latency) and task startup overhead, and breaks
the real-time semantics of `POST /orders`. The only thing the two share is the word "Redis" —
and they should **not** share an instance (entangled failure domains).

---

## 1. DAG Topology

```
【orders_analytics_daily】  @daily, catchup=False, max_active_runs=1

  extract_orders ─────────┐
                          ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test_all
  extract_quality_events ─┘      (Hard Gate)                                                 (completeness)

【dq_reevaluation】  schedule=None (manual trigger)

  reevaluate ─► should_refresh (passes only when commit) ─► trigger orders_analytics_daily

【source_freshness_watch】  @daily

  dbt_source_freshness      ← rationale for it being its own DAG in §2.7
```

Files live in `orchestration/dags/`.

> **Why the directory is `orchestration/` and not `airflow/`**: `pytest.ini` sets
> `pythonpath = .`, so a root-level `airflow/` becomes a namespace package and **shadows the real
> Airflow package** — `import airflow.models` fails locally, and in CI it shadows a genuinely
> installed Airflow too. This was hit in practice, not named defensively.

---

## 2. Decision Record

### 2.1 Dependency isolation: two venvs, nothing installed into Airflow itself ⭐

Airflow and dbt-bigquery both depend heavily on `google-cloud-*` / `protobuf` / `jinja2`.
Installing them into one environment is a classic source of version conflicts, and every Airflow
upgrade risks breaking dbt.

| Option | Trade-off |
|---|---|
| `pip install` into the same env | Simplest; high conflict risk |
| **Separate venvs + BashOperator (chosen)** | Clean isolation, zero extra infrastructure, matches official guidance |
| DockerOperator with its own image | Cleanest, best prod-parity; requires mounting `docker.sock` |

Inside the image: `/home/airflow/venvs/analytics` (`requirements-analytics.txt`) and
`/home/airflow/venvs/dbt` (dbt-core / dbt-bigquery 1.11). This is also why
`requirements-analytics.txt` exists — the Airflow container must be able to *run the extraction
scripts* without pulling in pytest.

**No Cosmos**: the observability benefit of model-level tasks is out of proportion for a
13-model project, and the cost is a dependency that tracks both dbt and Airflow versions.

### 2.2 DAG files must not import project modules at top level ⭐

`config.py` instantiates `Settings` at import time and `db_url` is mandatory. DAG files are
re-parsed by the dag-processor every few dozen seconds — so a top-level import of a project
module means that whenever the parsing process lacks `DB_URL`, the result is **not "a failed
task" but a DAG import error: the entire DAG disappears from the UI.** No red light to look at,
which is worse than failing.

Hence `BashOperator` everywhere, pushing imports to task execution time. Two side benefits:

1. `tests/test_dags.py` can parse the DagBag **without a database and without any project
   environment variables**, so the DAGs are CI-protected;
2. Airflow 3 splits DAG parsing into a separate dag-processor process whose environment is
   distinct from task execution — making this discipline more important, not less.

### 2.3 One extract task per table

A single task calling `main()` would work, but throws away what the design was for: **each
table's watermark is independent and does not advance on failure**
([CLOUD_LAYER §3.2](./CLOUD_LAYER.md)) is the source of self-healing. Merging them into one task
makes a retry re-run the table that already succeeded, and hides which one broke.

The cross-table gate therefore moves from "aggregate then `raise` inside the script" to the
**DAG dependency edge** (dbt's upstream = both extracts succeeded) — identical semantics, just
relocated. The script keeps `--table all` unchanged, so the manual path is untouched.

### 2.4 Layered dbt execution — and why a full `dbt test` still runs at the end ⭐

Layering (staging → intermediate → marts → reports) makes the Hard Gate's interception point
visible in the UI and allows re-running from the failed layer down. But per-layer `--select`
runs into dbt's **indirect selection** semantics, and the cost lands on the project's most
important singular tests:

| Mode | What happens to `assert_orders_split_is_partition` |
|---|---|
| `eager` (default) | Selected during the staging task → asserts against a half-rebuilt state (`stg_` fresh, `int_` stale) → **spurious red** |
| `cautious` | Not all parents selected → **never runs** |
| **`buildable` (chosen)** | Parents must be "selected or ancestors of selected" → lands in the intermediate task with all inputs fresh → **correct** |

But that rule is subtle, and betting wrong means silently skipping a test the docs describe as
"the only automated safety net, never to be downgraded". So a full `dbt test` closes the DAG:
**a silently skipped test is far worse than a duplicated one.** The two have different jobs —
the per-layer tests are the **gate** (they stop downstream builds), the closing run is
**completeness**.

> ⚠️ **Never split `dbt build` into a `dbt run` task and a `dbt test` task.** That makes `int_`'s
> upstream "staging's **run**" instead of "staging's **test**", and the Hard Gate
> ([DQ mechanism 1](./DQ_ARCHITECTURE.md)) silently stops working while dirty data flows into
> Gold. Guarded by `tests/test_dags.py::test_dbt_never_splits_run_and_test`.

### 2.5 Scheduling semantics: `catchup=False` is structural ⭐

This pipeline's watermark is **destination-derived** (Plan A: from staging's
`MAX(partition_id)`), not execution-date-derived. A backfill run for 2026-07-01 still extracts
"the increment **as of now**", unrelated to the logical date → N backfill runs do the exact same
thing N times, plus N extra load jobs.

> **This is not a date-partitioned, backfillable DAG.** Making it genuinely backfillable would
> require slicing on `received_at >= data_interval_start AND < data_interval_end` (Airflow's
> idiomatic idempotent shape), but that right-hand bound would cut off late-arriving rows,
> directly conflicting with the existing "`>=`, rather re-fetch than miss" semantics. Deliberately
> not done — recorded here rather than left for a future reader to re-derive.

`max_active_runs=1` is correctness, not politeness: concurrent runs would read the same
`get_watermark` value (harmless — dbt `stg_` dedup absorbs it), but concurrent dbt
`insert_overwrite` on the same partitions would overwrite each other.

**Daily, not hourly**: Plan A's precision is capped by DAY partitioning, so an hourly batch
re-extracts the whole current day every run (the decision table in
[CLOUD_LAYER §2.2](./CLOUD_LAYER.md)). Going hourly requires HOUR partitioning or Plan B first —
a separate decision.

### 2.6 Deliberately asymmetric retries: extract=2, dbt=0

| Task | retries | Rationale |
|---|---|---|
| `extract_*` | 2 (exponential backoff) | Failures are mostly transient (PG connectivity, BQ 5xx / rateLimitExceeded) — the same philosophy as the ingestion layer's four retry points |
| `dbt_*` | **0** | Failures are mostly deterministic (bad SQL, a red test, the Hard Gate) — a retry just re-runs something doomed to fail |

BQ's transient errors are handled at the **adapter layer** by `profiles.yml`'s `job_retries: 1`,
which is far more precise than an Airflow task retry — the latter re-runs the entire `dbt build`.

This asymmetry is the same principle as the NUL byte poison-pill lesson: **treating a
deterministic error as transient and retrying it is how you manufacture a poison-pill.** The fix
there was an `except ValueError` fast-fail; the fix here is `retries=0`.

### 2.7 Freshness gets its own DAG ⭐

[CLOUD_LAYER §1.7.7](./CLOUD_LAYER.md) already established a hard rule: **never as a pre-check**.
Implementation revealed that the "side-channel task (its failure must not affect downstream)"
that section suggested is **not enough**:

> An Airflow DAG run's state is the aggregate of its tasks. An **expected-red** leaf task leaves
> `orders_analytics_daily` permanently failed → the "main pipeline success rate" signal is worth
> nothing, and real pipeline failures are buried under noise that is red every single day.

So the principle goes one step further: **freshness has neither the authority to block downstream
nor the authority to pollute another DAG's success rate.** Once separated, each DAG's red means
exactly one thing:

| DAG | Red means |
|---|---|
| `orders_analytics_daily` | The pipeline is broken |
| `source_freshness_watch` | The source is stale (under manual ingestion = **the expected state**) |

Guarded by `tests/test_dags.py::TestFreshnessIsolation`: if any DAG producing real output picks
up `dbt source freshness`, the test goes red.

### 2.8 The Proposal B DAG: `schedule=None` is the design, not an omission ⭐

Proposal B fires on a **rule loosening** — a human deploy event, not a period. With unchanged
rules, re-evaluation necessarily reproduces the previous result (same values, same rule version)
→ it emits no events while full-scanning the entire quarantine backlog. Scheduling it daily is
**364 days of wasted work for one day of effect**.

> Schedules belong on things that change by themselves. Rules do not change by themselves.

Three supporting choices:

- **Dry-run by default**; `commit` must be explicitly enabled — `quality_events` is append-only,
  a bad write cannot be deleted, and a manual-trigger UI makes it easy to click straight through.
- **`expect_rule_version` as a guard**: the most common accident is triggering against an
  environment that has not deployed the new rules yet, writing a batch of events stamped with the
  wrong version that cannot be revoked.
- **Trigger the main DAG afterwards, but only on `commit`**: re-evaluation only writes PG's
  `quality_events`; flowing back to Gold still needs `extract_quality_events` to ship them to BQ
  and `int_` to rebuild. Without that step the state is "I ran Proposal B and nothing happened" —
  the state most easily misread as a broken program.

Every parameter follows "omit the flag when empty": defaults live **only in the script** —
keeping a copy in the DAG as well is exactly how the two drift apart.

### 2.9 Metadata DB separate from the business DB

| Option | Trade-off |
|---|---|
| Share the business `db` instance (separate database) | Saves a container; but scheduler metadata is tied to the **data anchor** in the same backup/restore |
| **Standalone `airflow-db` (chosen)** | One more container, in exchange for clean failure-domain and backup semantics |

The reason is not fastidiousness: the business DB is the precondition for Proposal C's "rebuild
from Raw", and an operational component's history should not live or die with it. When restoring
the business DB, you also do not want to roll back Airflow's execution history along with it.

**LocalExecutor**: single host, a handful of tasks. CeleryExecutor would add two containers and a
broker — and that Redis would collide conceptually with the roadmap's "Celery + Redis replacing
BackgroundTasks".

**No triggerer**: only `BashOperator` is in use; there are no deferrable operators.

### 2.10 `profiles.yml`: structure in version control, values in the environment ⭐

It lives in `orchestration/dbt_profiles/`, pointed at explicitly by `DBT_PROFILES_DIR`.

> **Deliberately not in `ecommerce_dbt/`**: dbt's `profiles.yml` lookup puts the **current working
> directory ahead of `~/.dbt`**, so placing it in the dbt project directory would make a local
> `cd ecommerce_dbt && dbt run` suddenly consume that file and fail on unset environment
> variables. A dedicated directory leaves the existing local workflow untouched.

⭐ **It deliberately reuses the same environment variables as `config.py`** (`BQ_PROJECT` /
`BQ_DBT_DATASET` / `GOOGLE_APPLICATION_CREDENTIALS`). This is not just convenience: the
`int_orders` that `reevaluate_quality.py` reads *is* the table dbt writes — configured separately,
the two would **silently point at different datasets** and re-evaluation would scan a stale or
non-existent table without erroring. One shared variable makes that divergence impossible.

---

## 3. Runbook

### 3.1 Startup

```bash
# .env needs BQ_PROJECT and GOOGLE_APPLICATION_CREDENTIALS (host key path); AIRFLOW_UID recommended
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
```

UI at `http://localhost:8080` (SimpleAuthManager for local practice, no login).
The two compose files must be layered into one project so the DAGs can reach the business
database at the hostname `db`.

### 3.2 ⚠️ Consecutive DAG failures exceeding the lookback window → widen it on the first run after the fix

**A single failure is safe**: staging has appended, the watermark has advanced, and `stg_`'s
lookback window recomputes those days next run.

**Consecutive failures are the danger**: with the default 3-day window, a DAG down for 4 days
looks back only 3 days on recovery, so rows that landed in staging before that boundary
**never reach `stg_orders`** — no error, no self-healing, silent data loss.

```bash
dbt build --select path:models/staging --vars '{stg_orders_lookback_days: 10}'
```

`stg_quality_events` and `rpt_quality_events_daily` need widening together
(see [ecommerce_dbt/README §9](./ecommerce_dbt/README.md)).

> Which implies something: **the lookback window is really a declaration of how much unattended
> failure the pipeline tolerates**, not just a cost parameter. Airflow failure alerts must be seen
> *before* cumulative downtime approaches the window.

### 3.3 Full Proposal B demo script ⭐

⚠️ **A run today would promote 0 records.** v1→v2 was a **tightening**, and all existing data was
ingested under v2 — re-evaluating v2 against v2 is a tautology. Seeing flow-back requires a real
**rule loosening** first.

```
1. Load a batch with dirty data   python seed_demo.py --n 200 --dirty-rate 0.12
2. Run the main DAG               → confirm the record lands in int_orders_quarantine
                                    and is absent from fct_orders
3. Loosen a rule + bump to v3     → [DONE] age cap 120→130 (clean.AGE_MAX),
                                  DQ_RULE_VERSION=v3. The dirty-data injector emits
                                  age=125, which sits between the old and new caps
4. Dry-run to size the impact     dq_reevaluation (commit=off) → check would_write
5. Commit for real                dq_reevaluation (commit=on, expect_rule_version=v3)
                                  → triggers the main DAG automatically
6. Verify                         the record appears in fct_orders;
                                  rpt_quality_events_daily.promotions is no longer always 0
```

This script is itself the evidence that the DQ architecture's "rule evolution → retroactive
re-evaluation → data flows back" path has actually been walked end to end.

### 3.4 Manual write-off (`rejection`): a runbook, not a DAG

`permanently_rejected` is a **human's terminal decision** (the state machine has no outgoing
edge), and the automated task never writes it. To write a record off, append a `rejection` event
to PG's `quality_events` directly with a recorded reason. Deliberately not an endpoint or a DAG —
the same discipline as Proposal C's "never an HTTP endpoint": **irreversible decisions should not
have convenient buttons.**

---

## 4. Deliberately Not Done

| Item | Why not | Trigger |
|---|---|---|
| **Seeding DAG** | Would make a demo-data generator a permanent part of the system, and would **invert** the freshness stance already settled in [CLOUD_LAYER §1.7.7](./CLOUD_LAYER.md) | When BI charts need continuous data. Freshness then becomes a meaningful gate and §1.7.7's rule table must be updated in step |
| **Celery + Redis** | Orthogonal to Airflow (see *Scope*); it solves `BackgroundTasks` durability | When the API needs horizontal scaling |
| **OpenTelemetry** | Needs continuous traffic worth observing first | A separate roadmap Phase 5 item |
| **Cosmos (model-level tasks)** | 13 models; benefit is out of proportion to the dependency cost | When model count makes layer-level tasks too coarse to read |
| **triggerer / deferrable** | Only `BashOperator` today | When sensors are introduced |
| **Hourly batches** | Plan A's watermark precision is capped by DAY partitioning | When switching to HOUR partitioning or Plan B ([CLOUD_LAYER §2.2](./CLOUD_LAYER.md)) |
| **A backfillable DAG** | Conflicts with "`>=`, rather re-fetch than miss" (§2.5) | Re-evaluate when moving to a Plan B watermark |

---

## 5. Status and TODO

- ✅ `orders_analytics_daily` (2 extracts → 4 layered dbt builds → full `dbt test`)
- ✅ `dq_reevaluation` (manual, dry-run by default, chains into the main DAG on commit)
- ✅ `source_freshness_watch` (standalone observability)
- ✅ Image (two isolated venvs), compose overlay, env_var-driven `profiles.yml`
- ✅ `tests/test_dags.py` (20 tests) + a dedicated CI job (`.github/workflows/dags.yml`)
- ⬜ First real `docker compose up` verification (needs a GCP key and BQ project)
- ✅ The v3 rule loosening (`age` cap 120→130) — Proposal B now has genuine promote candidates
- ⬜ One live pass of the §3.3 demo script (needs a GCP key and BQ project)
- ⬜ Seeding DAG (see §4)
- ⬜ Celery + Redis, OpenTelemetry (other roadmap Phase 5 items)

## 6. Dependencies and Versions

- Airflow **3.0.0** (`apache/airflow:3.0.0-python3.12`), LocalExecutor
- dbt-core / dbt-bigquery **1.11** (aligned with [ecommerce_dbt/README §10](./ecommerce_dbt/README.md))
- When upgrading Airflow, `ARG AIRFLOW_VERSION` in `orchestration/Dockerfile` and
  `AIRFLOW_VERSION` in `.github/workflows/dags.yml` must change together (the constraints file is
  fetched by version)
