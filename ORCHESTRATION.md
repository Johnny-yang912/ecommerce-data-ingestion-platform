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

**Airflow is not a task queue.** "Airflow" and "Celery + Redis" are two
**orthogonal** items (the latter is implemented — see [QUEUE.md](./QUEUE.md)); conflating
them warps the whole design:

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

### ⚠️ This is a portfolio project; the data source is simulated

There is no real upstream here and there never will be. `seed_demo_daily` posts four batches a
day to `POST /orders`. **It simulates "there is data every day", not "realistic ingestion
behaviour".**

The gap is not in the data itself (that travels the real ingestion path through the real quality
rules) but in its **distribution over time**: a real system takes orders continuously around the
clock with peaks and troughs; this project fires at four discrete instants, each at a constant rate.

That gap has concrete consequences — this is not a disclaimer:

| Design that "happens to hold" because of the simulation | What real traffic would do to it |
|---|---|
| All seeding slots land in one UTC day partition | Round-the-clock ingestion cannot dodge the UTC day boundary; Taipei 00:00–08:00 falls into the previous day (see §2.11) |
| The Hard Gate uses "latest UTC day partition" as a proxy for "latest batch" | Under continuous ingestion it degrades into "today so far", replaying the dilution problem within a single day |
| Freshness needs no blocking authority (no data = nobody seeded = harmless) | An upstream outage is an incident; freshness should be restored as a gate |
| The 26h/50h freshness thresholds | Far too much slack at four batches a day — it cannot detect "the peak stopped for three hours" |

**But verification maturity differs sharply between the two paths, and the distinction matters:**

- **Ingestion path** (API → Redis → worker → ODS): **burst behaviour has been measured**, and it
  is the best-evidenced part of this project. See [QUEUE §5](./QUEUE.md) — multi-process rate
  limiting, degradation and circuit breaking under a broker outage (200 concurrent), and bounded
  recovery scans (60,000 backlogged rows cleared in one pass; 120,000 continued across two passes
  via cursor with 0 `duplicate`). **None of this is verified by seeding.**
- **Analytics path** (extract → BQ → dbt): **has never run on anything but small volumes.** The
  real cost of the `stg_` lookback window and `insert_overwrite`, how Hard Gate sensitivity shifts
  as data grows, and BigQuery storage/query cost are all unobserved.

Two things neither path covers: **sustained load** (load tests are a one-off burst, not weeks of
daily traffic — cumulative effects are unknown) and **peak shape** (an instantaneous concurrency
spike is not the same pressure as hours at a high plateau).

**Why write this down**: every one of these designs is correct under present conditions, but their
correctness rests on a premise that appears nowhere in the code. Without this note, the next person
to pick this up (including future me) will assume the pipeline has already faced continuous
traffic — **and every test in the repo will support that misreading.**

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
broker — and that Redis would collide conceptually with the ingestion path's "Celery + Redis replacing
BackgroundTasks" (implemented, see [QUEUE.md](./QUEUE.md); the two deliberately do not share an
instance, to keep their failure domains apart).

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

### 2.11 ⚠️ Cross-timezone extraction: the business "day" is not the partition "day" ⭐

Schedules are declared in `Asia/Taipei` (seeding at 09/13/17/21, extraction at 23:00), but
`received_at` is a TIMESTAMP and BigQuery partitions by **UTC** day — `date()` rolls over in UTC,
eight hours apart.

**The mismatch is currently invisible**, because all four seeding slots fall between Taipei 08:00
and 24:00, which maps to UTC 00:00–16:00 on the same day. **That is a consequence of the slots we
picked, not a property of the system**: with round-the-clock ingestion, orders placed between
Taipei 00:00 and 08:00 land in the *previous* UTC partition.

The blast radius splits along a meaningful line:

| | Time axis | Affected |
|---|---|---|
| `staging.orders` partition → Hard Gate's "latest batch" verdict | `received_at` | ✅ yes |
| `rpt_quality_events_daily.event_date` | `event_at` | ✅ yes |
| `source freshness` recent-window filter | `received_at` | ✅ yes |
| `fct_orders` / `fct_order_items` / `rpt_sales_daily_by_category` | `order_date` | ❌ no |

`order_date` comes from the payload and is already a `DATE` — it has no timezone. In other words:
**the revenue numbers are right, but "which day the quality belongs to" is off by eight hours.**

That distinction governs whether and when to fix it: revenue on the BI side can be read as-is,
while the DQ dashboard will attribute a boundary-crossing incident to the wrong day.

Options for an actual fix (**none taken yet, and none is a purely technical decision**):

| | Approach | Cost |
|---|---|---|
| a | Partition staging on a business-timezone `DATE` | Most correct semantically; requires rebuilding and backfilling the table |
| b | Keep the UTC partition, derive a `business_date` in `stg_` for downstream use | Leaves partitions alone, but adds a column whose definition must be maintained |
| c | Declare explicitly that quality metrics are UTC-day-based and put it in the report definitions | Zero cost, but asks report readers to accept a grain that disagrees with the operating day |

**Deliberately unchosen**: under the current ingestion pattern all three produce identical output
(see "currently invisible" above) — **no choice can be validated until there is real traffic that
crosses the day boundary.**

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

#### ⚠️ When the business DB is not in the compose project (found while running it for real)

The default above assumes **postgres also runs inside compose**. If your business DB lives on the
**host** (common in local development), `db` does not resolve inside the container and `localhost`
points at the container itself, so `extract_*` fails with
`OperationalError: could not translate host name`. Two options:

| Approach | Steps |
|---|---|
| **A. Put the business DB in compose too** (self-contained; the default path) | `docker compose -f docker-compose.yml -f docker-compose.airflow.yml up` brings `db` up as well. ⚠️ That is a **separate, empty database** — the `raw_id`s it mints overlap with the host DB's, so extracting both into the same BQ staging **collides on the dedup key**. Never mix them in one dataset |
| **B. Point back at the host postgres** | ① set `AIRFLOW_TASK_DB_URL=postgresql://user:pw@host.docker.internal:5432/<db>` in `.env`; ② the host postgres listens on **`127.0.0.1` only** by default, so `listen_addresses` in `postgresql.conf` and `pg_hba.conf` must be widened for the docker network, or it still cannot connect |

A's warning is worth internalising: **`raw_id` is a surrogate key minted by the landing layer, and
two independent ODS instances both start numbering at 1.** Extract them into one staging table and
`stg_`'s `raw_id`-grained dedup will collapse unrelated orders into "copies" of each other.

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
| **OpenTelemetry** | Needs continuous traffic worth observing first | A separate roadmap Phase 5 item |
| **Cosmos (model-level tasks)** | 13 models; benefit is out of proportion to the dependency cost | When model count makes layer-level tasks too coarse to read |
| **triggerer / deferrable** | Only `BashOperator` today | When sensors are introduced |
| **Hourly batches** | Plan A's watermark precision is capped by DAY partitioning | When switching to HOUR partitioning or Plan B ([CLOUD_LAYER §2.2](./CLOUD_LAYER.md)) |
| **A backfillable DAG** | Conflicts with "`>=`, rather re-fetch than miss" (§2.5) | Re-evaluate when moving to a Plan B watermark |

---

## 5. Live Verification Record (2026-08-05) ⭐

Two live runs, recorded here. **Every item below was measured**, not inferred at design time —
which is precisely why it deserves its own section: several decisions in the six sections around
it could only be settled by reasoning, and this is the first time they were confirmed or
overturned by data.

### 5.1 Full Proposal B flow-back (one pass of the §3.3 script)

Environment: ODS at 774 rows (57 dirty, 7.364%), BQ sandbox, dbt 1.11.

**The order is deliberate**: 20 `V3DEMO-*` records were ingested under **v2** first (15 with age
in 121/123/125/127/130, and a 5-record control group with age ∈ {-3, 150, 999}), and only then
was v3 switched on. Reversed, age=125 would be judged clean on arrival and never enter quarantine
at all — **only data ingested under the old rule is eligible to be pulled back by the new one.**

| Stage | Result |
|---|---|
| Ingested under v2 | all 20 `has_clean_error=TRUE`, `quality_events` → `quarantined`(v2) |
| Extraction | 220 rows orders / 220 rows quality_events (first pass re-extracts the current day's partition) |
| Layered dbt build | staging PASS=21 WARN=1 / intermediate PASS=27 WARN=1 / marts PASS=31 / reports PASS=24 |
| Before promotion | `int_orders_quarantine` 20, `fct_orders` **0**, `promotions` **0** |
| Re-evaluation dry-run | 57 candidates → `would_write=15`, `unchanged=42`, `blocked_non_reproducible=0` |
| Re-evaluation `--commit` | `written=15` |
| **Immediately run again** | **`promoted=0`, `unchanged=57`, `written=0`** |
| After flow-back | `int_orders` promoted **15**, quarantine down to **5**, `fct_orders` **15**, `promotions` **0→15** |
| Full `dbt test` | 93 tests: PASS=91 / WARN=2 / **ERROR=0** |

#### Four things the run actually proved

**① Idempotency went from "claimed" to "measured"** ⭐
Two consecutive runs; the second wrote 0 events. "Append only on an actual state change" really
does keep `promotions` — the figure 〈Why historical metrics are never retroactively rewritten〉
exists to protect — from being inflated by a re-run. Previously this had unit tests only; now it
has evidence from real data.

**② A loosening has an edge; it is not switching the rule off**
The 5 control records (age -3/150/999) stayed exactly where they were, and the flow-back landed
precisely as `age=121/123/125/127/130, 3 records each` in Gold.

**③ Bounded Writeback held — and left 15 live samples of the "permanent divergence"** ⭐
Those 20 ODS rows still read `dq_rule_version=v2, has_clean_error=TRUE`; not one column was
touched. The event chain is clean: `initial_evaluation(None→quarantined, v2)` →
`promotion(quarantined→promoted, v3)`. What
[DQ_ARCHITECTURE](./DQ_ARCHITECTURE.md)〈Permanent ODS/BQ quality-state divergence〉has argued at
length is now 15 rows you can point at: **ODS says dirty (v2), Gold says clean (v3), and
`dq_rule_version` + `quality_events` make it fully traceable.**

**④ The Hard Gate's severity tiers really are tiers**
7.364% made `error_rate_below_stg_orders_0_05` **WARN** while `_0_1` **PASSED** — alerting
without blocking, and `dbt build` carried on downstream. This is the first time the two-tier
threshold fired on a real ratio.

### 5.2 Verifying `--indirect-selection=buildable` ⭐

The §2.4 decision could originally **only be reasoned about** — the difference between the three
modes was describable, but there was no instance of it. Observed this time:

```
dbt build --select path:models/staging       22 nodes, all stg_ tests
                                             ← assert_orders_split_is_partition is NOT among them
dbt build --select path:models/intermediate  13 of 28 PASS assert_orders_split_is_partition
dbt build --select path:models/marts         assert_fct_orders_complete_projection PASS
                                             assert_fct_orders_rollup_matches_items PASS
```

Cross-layer singular tests land **exactly in the layer where all their inputs are fresh**: not
fired early in staging (where `int_` is still the previous table and they would go spuriously
red), and not skipped entirely the way `cautious` would. The reasoning holds.

> Aside: the closing full `dbt test` (93 tests) stays. Its value now is not "catching what was
> skipped" — nothing was — but that **it is the only thing that would notice if selector
> semantics change in a future version**.

### 5.3 Airflow container, run for real

| Item | Result |
|---|---|
| Image build | Success (`apache/airflow:3.0.0-python3.12` + two isolated venvs) |
| Services | `airflow-db` / `init` / `apiserver` / `scheduler` / `dag-processor` all healthy |
| **DAG parsing** | All 3 loaded; `list-import-errors` → **No data found** |
| analytics venv | `sqlalchemy` / `google.cloud.bigquery` / `structlog` / `pydantic` import fine |
| dbt venv | dbt-core **1.11.12**, dbt-bigquery **1.11.3** |
| env_var profile | `dbt debug` → `Connection test: [OK connection ok]` |
| `source_freshness_watch` | Full DAG run **success**, both sources PASS |
| `dbt_intermediate` | In-container `airflow tasks test` → PASS=27 WARN=1 ERROR=0, SUCCESS |
| `extract_orders` | **FAILED**: `OperationalError: could not translate host name` (see §5.4) |
| UI | `http://localhost:8080` HTTP 200 |

**The §2.2 discipline was validated against a real dag-processor** ⭐
The container had no usable `DB_URL` (the default points at a `db` service that was not started),
and all three DAGs still parsed with zero import errors. Had the DAG files carried a top-level
`from config import settings`, the screen at that moment would have shown **all three DAGs missing
from the UI** — not three red tasks, but nothing at all.

**The meaning of freshness was incidentally confirmed too**
Run 15 minutes after a data load, both sources **PASSED**. What
[CLOUD_LAYER §1.7.7](./CLOUD_LAYER.md) argued — "red means you have not fed it lately, not that
the pipeline is broken" — is no longer only an argument: **feed it and it goes green.**

### 5.4 The gap only a live run exposed: `raw_id` collides across two ODS instances ⭐

`extract_orders` failed in-container. The surface cause is that compose hard-codes the assumption
that the business DB runs inside the compose project, while this machine's postgres runs on the
host and listens on `127.0.0.1` only. The fix is an `AIRFLOW_TASK_DB_URL` override seam (see the
A/B options in §3.1).

**But the trap underneath option A is what deserves remembering**, and it is far worse than this
failure:

> Bringing compose's `db` service up gives you a **separate, empty database**. The `raw_id`s it
> mints start at 1 and **overlap completely** with the host ODS's. Extract both into the same BQ
> staging table and `stg_`'s `raw_id`-grained dedup will collapse **two unrelated orders into
> "copies" of each other**, dropping one. No error, no trace.

This is really a corollary of the [README](./README.md)〈`raw_id` is physical identity, `order_id`
is business identity〉principle, which the original text simply did not push all the way:
**`raw_id`'s uniqueness only holds within a single landing instance.** Choosing `raw_id` as the
dedup key is correct (physical dedup should use physical identity), but it also welds an implicit
premise into the pipeline — **one staging table can only correspond to one ODS**. If multi-instance
upstreams ever get their own landing layers, the dedup key must be upgraded to something like
`(source_instance, raw_id)`. Today there is a single instance, the premise holds, and nothing
changes; this is recorded so whoever expands it later knows where the line is.

---

## 6. Status and TODO

- ✅ `orders_analytics_daily` (2 extracts → 4 layered dbt builds → full `dbt test`)
- ✅ `dq_reevaluation` (manual, dry-run by default, chains into the main DAG on commit)
- ✅ `source_freshness_watch` (standalone observability)
- ✅ Image (two isolated venvs), compose overlay, env_var-driven `profiles.yml`
- ✅ `tests/test_dags.py` (20 tests) + a dedicated CI job (`.github/workflows/dags.yml`)
- ✅ Verified live (2026-08-05): image builds, all four services healthy, three DAGs parsed by the
  real dag-processor with **zero import errors**, both venvs working (dbt 1.11.12 / bigquery 1.11.3),
  the env_var profile connecting to BQ from inside the container, a full successful
  `source_freshness_watch` run, and `dbt_intermediate` passing in-container with PASS=27
- ⬜ `extract_*` executed in-container (blocked by "business DB on the host listening on 127.0.0.1
  only" — see the A/B options in §3.1)
- ✅ The v3 rule loosening (`age` cap 120→130) — Proposal B now has genuine promote candidates
- ✅ The §3.3 demo script walked end to end (2026-08-05): 20 records quarantined under v2 → v3
  loosening → re-evaluation promoted 15 → flowed back into `fct_orders`, `promotions` 0→15; the
  5 control records (age -3/150/999) correctly stayed quarantined; a second consecutive run wrote
  0 events (idempotency); ODS was never modified (Bounded Writeback)
- ⬜ Seeding DAG (see §4)
- ✅ Celery + Redis (implemented, orthogonal to this layer; see [QUEUE.md](./QUEUE.md))
- ⬜ OpenTelemetry (other roadmap Phase 5 items)

## 7. Dependencies and Versions

- Airflow **3.0.0** (`apache/airflow:3.0.0-python3.12`), LocalExecutor
- dbt-core / dbt-bigquery **1.11** (aligned with [ecommerce_dbt/README §10](./ecommerce_dbt/README.md))
- When upgrading Airflow, `ARG AIRFLOW_VERSION` in `orchestration/Dockerfile` and
  `AIRFLOW_VERSION` in `.github/workflows/dags.yml` must change together (the constraints file is
  fetched by version)
