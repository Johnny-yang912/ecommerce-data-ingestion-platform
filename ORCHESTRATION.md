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
| Freshness needs no blocking authority (no data = nobody seeded = harmless) | An upstream outage is an incident — **but freshness is not what detects it**. It measures `ods.received_at` = the extract hop, and is structurally blind to upstream. What a real system must add is measurement on the Raw side (§2.12, OTel), not wiring freshness up as a gate |
| The 26h/50h freshness thresholds | **Unchanged.** The thresholds come from the *loading* cadence (one extract per day → `24h + 2h grace`), not the ingestion cadence; under 24/7 ingestion the warehouse is still loaded nightly, so the thresholds stay in this range. What would change them is extract moving to hourly or streaming |
| Freshness detects "no data all day" | It cannot detect "the peak stopped for three hours" — but that is a question of **scope**, not of thresholds, and it is answered by §2.12 and OTel (see §2.7) |

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

Every schedule is declared explicitly in `Asia/Taipei` (rationale in §2.5).

```
【seed_demo_daily】  0 10,13,17,21 * * * (Taipei), catchup=False, max_active_runs=1

  seed_orders               ← the simulated upstream = this system's only data source

【raw_pending_watch】  30 10,13,17,21 * * * (Taipei)

  check_raw_pending         ← 30 min after each seeding slot; rationale in §2.12

【orders_analytics_daily】  30 22 * * * (Taipei), catchup=False, max_active_runs=1

  extract_orders ─────────┐
                          ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test_all
  extract_quality_events ─┘      (Hard Gate)                                                 (completeness)

【source_freshness_watch】  0 8 * * * (Taipei)

  dbt_source_freshness      ← rationale for it being its own DAG in §2.7

【dq_reevaluation】  schedule=None (manual trigger)

  reevaluate ─► should_refresh (passes only when commit) ─► trigger orders_analytics_daily

【seed_demo_gate_demo】  schedule=None (manual trigger)

  seed_dirty_batch          ← the Hard Gate interception scenario
```

**None of the four scheduled DAGs depends on another at the Airflow level** — the
`seed → probe → extract → freshness` ordering contract **exists only in the time gaps**
(21:00 finishes sending → 21:30 check → 22:30 extract → 08:00 next day backstop).
That is deliberate: wiring them with triggers would let an upstream red decide whether
downstream runs at all, while their reds stand for completely different responses
(see the table in §2.7). The price is that the gaps must be pinned by tests — see
`tests/test_dags.py::TestSeedDemoDaily::test_runs_before_the_analytics_dag` and
`TestRawPendingWatch::test_slot_hours_match_the_seeding_dag`.

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

| DAG | Red means | Where to look |
|---|---|---|
| `seed_demo_daily` | Nothing gets in (API refusing / the script itself broke) | API and the seeding script |
| `raw_pending_watch` | Rows reach Raw but nobody claims them | redis / worker / beat (§2.12) |
| `orders_analytics_daily` | The pipeline is broken (extract or dbt) | The pipeline |
| `source_freshness_watch` | staging was not moved forward (backstop for an empty extract) | The watermark and extract |

Guarded by `tests/test_dags.py::TestFreshnessIsolation`: if any DAG producing real output picks
up `dbt source freshness`, the test goes red.

**⚠️ Freshness covers exactly one hop, and that is deliberate** ⭐

`loaded_at_field` points at `ods.received_at` = **the ODS landing time, not the order-receipt
time** (full account in [CLOUD_LAYER §1.2.2](./CLOUD_LAYER.md)). And what this DAG checks is
extract, and what extract moves *is* ODS — **so reading ODS's own clock is the correct timeline,
not a compromise.**

The scope boundary that follows: **it cannot see ingestion outages that have already recovered**
(when a backlog is flushed by the recovery scan, those rows carry the catch-up write time, so the
gap does not exist on the ODS timeline). That is not a defect but somebody else's job — three
timelines, one hop each; merged, a single red would stand for two pipeline segments:

| Timeline | Which hop it answers | Who watches it |
|---|---|---|
| `raw.received_at` | Upstream + API: can orders get in? | OTel: the count of `http.server.duration{http_route="/orders"}` (live); **absent alerting not yet written**, see §4 |
| `raw.received_at` → `ods.received_at` | Dispatch: can workers claim them? | `raw_pending_watch` (§2.12) |
| `ods.received_at` in BQ staging | extract: did it reach the warehouse? | `source_freshness_watch` |

**Where 26h/50h comes from**: `26 = 24 + 2`, `50 = 48 + 2` — one **loading cycle** plus 2 hours of
grace. The source is the loading cadence, not the ingestion cadence: staging is pushed by extract
once a day, so the data is up to 24 hours old by design and the threshold must exceed 24h or it
would go red before every extract. Sampling point and threshold determine each other: sampled at
08:00 Taipei the healthy value is ~13h and one missed cycle is 37h, so 26h sits in the middle with
~10 hours of margin on both sides. **08:00 is chosen because this is a backstop** — if extract
reports success but moved nothing, the Hard Gate judges yesterday's partition and passes, `dbt test`
is green too, and this is the only thing that speaks up before the ops team opens the report at 09:00.

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

Schedules are declared in `Asia/Taipei` (seeding at 10/13/17/21, extraction at 22:30), but
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

### 2.12 `raw_pending_watch`: the observation-signal principle, applied a second time ⭐

The principle from §2.7 (an observation signal has neither blocking authority nor the authority to
pollute another DAG's success rate) applies here for the second time: `raw_pending_watch` is
likewise its own DAG and likewise upstream of nothing. But it brings two arguments §2.7 does not.

**① ⚠️ "a Raw row with no matching ODS row" cannot be the criterion**

Raw has three terminal states: `processed`, `duplicate`, `error`. **The latter two produce no ODS
row, and that is correct behaviour** (`duplicate` is a deliberately retained monitoring signal — see
the architecture constraints in CLAUDE.md). Built on that criterion, the probe would go red on
**every duplicate order**.

So it looks only at `status='pending'`: landed in Raw, but **not yet claimed by any worker**. That
state has no legitimate reason to persist, which makes it a clean fault signal. Whether a claimed
row ends up `processed` or `duplicate`/`error` is a question about data content, owned by the DQ
mechanisms.

This also means it measures the **root cause rather than the symptom**: when redis/worker dies the
visible downstream symptom is that ODS stops growing, but the cause is on the dispatch side —
measuring the cause is both earlier and less ambiguous than watching ODS row counts.

**② Sampling frequency follows *when the measured thing can change*, not "how often feels safe"**

The common magnitudes are **a reference, not a rule**: environments with a metrics stack sample every
15–60 seconds and require the breach to persist 2–10 minutes; a scheduler-only stopgap runs every
5–15 minutes and requires two consecutive breaches. This project checks four times a day.

The same principle yields opposite numbers, because under fixed-slot ingestion **pending cannot
structurally accumulate between slots** — measuring at 19:00 a queue that has been empty since 17:10
does not report "healthy", it reports nothing. The actual frequency should follow four things: the
arrival rate of the data, the cost of the check itself, the tolerable detection delay (set by the
**reversibility** of the loss — an order never received is irreversible, a late report is not), and
whether a suppression mechanism exists at all.

**③ The threshold is derived, not chosen**

Its lower bound is set by the **recovery path's own cadence**; below the self-healing time it would
alert on rows that are being handled correctly:

```
max(enqueue failed: PENDING_GRACE + scan_interval,
    worker died mid-flight: STALE_PROCESSING_MINUTES + scan_interval) + safety margin
```

Currently `max(360s, 900s) + 240s = 1140s` (19 minutes), and those three constants are **read at
runtime from `config` / `process`** rather than hardcoded — `SCAN_INTERVAL_SECONDS` is tunable via
`.env`, and freezing a number here would turn a derivation into a magic constant that the next
person to change it will not know to revisit. ⚠️ Corollary: that variable must be injected into the
worker/beat **and** the Airflow containers (both compose files carry it); tuning only one side lets
the recovery scan and the probe threshold diverge silently.

**④ It is a stopgap coarse filter, not an alert**

Real liveness monitoring is "sample by the second, require the breach to persist", and **Airflow
cannot express the second half** — every DAG run is an independent, memoryless sample. The other
limit is the failure domain: it lives in the same compose stack as the system it watches. Real
alerting waits for OTel (§4), and the first rule to write there is **absent** — "nothing happened at
all" is precisely the question metrics are worst at answering, and precisely what this is for.

---

## 3. Runbook

### 3.1 Startup

```bash
# .env needs BQ_PROJECT and GOOGLE_APPLICATION_CREDENTIALS (host key path); AIRFLOW_UID recommended
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

UI at `http://localhost:8080` (SimpleAuthManager for local practice, no login).
The two compose files must be layered into one project — that is what lets the DAGs reach `db`
and lets seeding reach `api`.

**The business DB is in compose too** (since 2026-08-11; an earlier "business DB on the host"
setup was supported and has been removed). With everything on one network,
`SEED_API_URL=http://api:8000/orders` and `DB_URL=postgresql://app:app@db:5432/orders` point at
the same system by construction.

> ⚠️ **`db` publishes on port 5433** (`DB_PUBLISH_PORT`). If another postgres already holds 5432
> on the host, a `5432:5432` mapping makes the service fail to bind outright. Containers talk over
> `db:5432` and never traverse this mapping — it exists only for `psql` from the host.

> ⚠️ **Host-side tooling (`seed_demo.py --verify`, `psql`) must connect to `localhost:5433/orders`.**
> `.env` already points there, but **`load_dotenv` defaults to `override=False`, so an environment
> variable beats `.env`** — if your shell has an older `DB_URL` exported, the script will quietly
> connect somewhere else. This is why `verify()` prints the database it actually reached: that line
> is the only place such a mistake surfaces on its own.

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

### 3.3 Deployment SOP for loosening a rule (Proposal B) ⭐

**Loosening** a rule gives existing quarantine records a chance to be pulled back into Gold. That
is what Proposal B is for, and the only situation needing this procedure — tightened rules apply
going forward only and need no retroaction.

```
1. Confirm candidates exist   check the value range of the target code in quarantine;
                              it must STRADDLE the old and new thresholds
2. Change rule + bump         the threshold in clean.py + DQ_RULE_VERSION, then git tag
3. ⚠️ Rebuild images          docker compose build api worker beat && docker compose up -d
4. ⚠️ Run the main DAG        orders_analytics_daily (candidates are read from BQ,
                              so the data must be up there first)
5. Dry-run                    dq_reevaluation (commit=off, expect_rule_version=<new>)
6. Commit for real            dq_reevaluation (commit=on, expect_rule_version=<new>)
                              → triggers the main DAG, flowing data back into Gold
7. Verify                     promoted rows enter fct_orders and leave quarantine;
                              promotions > 0; the control group stays quarantined;
                              ODS unmodified; a second run writes 0
```

#### ⚠️ Step 1: confirm candidates first — don't bump and then discover there are none

`promoted=0` looks **exactly like** "the rule didn't take effect" and "the code is broken". And
low-weight DQCodes accumulate slowly. Checking the value distribution before bumping is far cheaper
than diagnosing it afterwards.

#### ⚠️ Step 3: the two paths deliver code differently

```
api / worker / beat   code is BAKED INTO THE IMAGE     needs a build to take effect
Airflow containers    bind mount ./:/opt/project        takes effect IMMEDIATELY
```

Skip the rebuild and **re-evaluation (running in Airflow) is already on the new version while the
ingestion path is still on the old one** — two rule versions judging data in the same database at
once. And `--expect-rule-version` **cannot see that divergence**: it only compares the version
inside its own process, so the assertion passes.

> That guard protects against "running this against a deployment that hasn't got the new rules".
> It holds only if **the whole system has a single code-delivery mechanism** — and this compose
> topology breaks that premise.

#### ⚠️ Step 4: candidates come from BQ, state comes from PG

`dq_reevaluation`'s header note ④ records that "re-evaluation writes to PG's `quality_events`; an
extract is still needed to push events to BQ before data flows back into Gold". **The reverse holds
too, and is easier to miss:**

> **The candidate list comes from BQ's `int_orders_quarantine`. Newly accumulated data that has not
> been extracted to BQ is invisible to re-evaluation** — the symptom is a low `candidates` count
> and `would_write=0`.

Measured figures and the full record of both walkthroughs are in §5.

### 3.4 Manual write-off (`rejection`): a runbook, not a DAG

`permanently_rejected` is a **human's terminal decision** (the state machine has no outgoing
edge), and the automated task never writes it. To write a record off, append a `rejection` event
to PG's `quality_events` directly with a recorded reason. Deliberately not an endpoint or a DAG —
the same discipline as Proposal C's "never an HTTP endpoint": **irreversible decisions should not
have convenient buttons.**

### 3.5 ⚠️ Silent scheduling stall: `is_stale` is the first light to come on, and the easiest to miss ⭐

**Symptom**: a DAG that should have run didn't, yet **nothing in the UI turns red** — because no
run was ever created, and without a run there is no failed run to show. If the dag-processor
cannot finish parsing, DAGs get marked stale after `dag_stale_not_seen_duration` (600s by
default), and **the scheduler creates no runs at all for a stale DAG**. The pipeline stops
without a sound.

Triage order, fastest first:

```bash
# ① Is any DAG stale? (quickest, most direct signal)
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True   # non-zero = you have it

# ② When did parsing stop? (check this once is_stale=True)
docker exec api-airflow-apiserver-1 airflow dags details <dag_id> | grep -E "is_stale|last_parsed_time"

# ③ Is the dag-processor killing its parse subprocesses?
docker logs api-airflow-dag-processor-1 | grep -c "killing it"

# ④ Rule out genuine syntax/import problems
docker exec api-airflow-apiserver-1 airflow dags list-import-errors
```

> **The time-saver: a clean ④ does not prove the DAG file is fine, but if ② and ③ look wrong,
> the DAG file is probably not the problem.** Rule the code out directly by parsing it by hand
> inside the container:
>
> ```bash
> docker exec api-airflow-dag-processor-1 python -c \
>   "from airflow.models.dagbag import DagBag; d=DagBag('/opt/airflow/dags/<file>.py', include_examples=False); print(list(d.dags), d.import_errors)"
> ```
>
> If the manual parse **succeeds** while the dag-processor **fails**, the fault is in the
> processor's supervision machinery (timeout arithmetic, resources, subprocess lifecycle),
> not in the DAG code. That fork in the road saves a lot of time.

**Two independent knobs — don't conflate them**:

| Setting | Default | Governs |
|---|---|---|
| `[dag_processor] dag_file_processor_timeout` | 50 | How long a parse subprocess lives before it is killed and retried |
| `[scheduler] dag_stale_not_seen_duration` | 600 | How long without a successful parse before a DAG is marked stale |

Raising the former does **not** delay detection — that is the latter's job. The former only
changes how long a stuck parse waits before being killed and retried, and it does nothing for a
**persistent** hang (killing it just reruns the same file); it only matters for transient hangs
that a retry would clear.

> The observability gap this implies: **this failure mode has no built-in alerting**. Any detector
> for it must live *outside* Airflow — once every DAG is stale, a watchdog written as a DAG will
> not run either. Same principle as §4's conclusion on OpenTelemetry: **a liveness alert must not
> share its fate with the system it monitors.**

---

## 4. Deliberately Not Done

| Item | Why not | Trigger |
|---|---|---|
| **OTel absent alerting** | Traces and operational metrics went live on 2026-08-17 (see the README roadmap), **but the alert itself is still unwritten** — and what blocks it is no longer technical. The threshold must be derived from observation (same discipline as §2.12 ③), yet this machine is **powered off overnight** and seeding only runs at 10/13/17/21 Taipei; a rule written today would catch "the laptop is off" rather than a pipeline fault, firing every night until you learn to ignore it | After 2–3 days of real power-cycle and seeding cadence. The first rule is still **absent** ("how long since this source sent anything") rather than a business metric, and it must live on the cloud side — what it detects is precisely "my side can no longer speak". See §2.12 ④ |
| **Cosmos (model-level tasks)** | 13 models; benefit is out of proportion to the dependency cost | When model count makes layer-level tasks too coarse to read |
| **triggerer / deferrable** | Only `BashOperator` today | When sensors are introduced |
| **Hourly batches** | Plan A's watermark precision is capped by DAY partitioning | When switching to HOUR partitioning or Plan B ([CLOUD_LAYER §2.2](./CLOUD_LAYER.md)) |
| **A backfillable DAG** | Conflicts with "`>=`, rather re-fetch than miss" (§2.5) | Re-evaluate when moving to a Plan B watermark |

> **2026-08-11 update**: the **Seeding DAG has been implemented** (`seed_demo_daily`) and is
> therefore removed from the table above. The original reason for not doing it — "it would make a
> demo-data generator a permanent part of the system" — did come true, and is now **accepted
> deliberately**: this project has no real upstream, so seeding *is* the data source (see Scope and
> Responsibility Boundaries). The freshness stance in
> [CLOUD_LAYER §1.7.7](./CLOUD_LAYER.md) has been flipped in step.

---

## 5. Live Verification Record ⭐

Live runs, recorded here. **Every item below was measured**, not inferred at design time — which is
precisely why it deserves its own section: several decisions in the sections around it could only
be settled by reasoning, and this is where they were first confirmed or overturned by data.

> ⚠️ **These figures are point-in-time measurements, not current state.** The dataset was **rebuilt
> on 2026-08-11** (old ODS and both BQ datasets wiped, migrations re-run from zero), so the row
> counts cited in §5.1–§5.4 cannot be found in today's database. They are kept because **the design
> conclusions those measurements confirmed or overturned still hold** — they are evidence for the
> conclusions, not a snapshot of the present.

### 5.0 The dividing line between the two verifications

| | 2026-08-05 | 2026-08-11 |
|---|---|---|
| Environment | business DB on the host, Airflow in compose | fully in compose (§3.1) |
| `extract_*` in-container | ⬜ blocked | ✅ passing |
| Rule version | v3 | v4 |
| Dataset | accumulated over time (incl. manually loaded, non-reproducible rows) | rebuilt from zero, entirely produced by `seed_demo` |
| Ingestion mode | manual | scheduled via `seed_demo_daily` |

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

> ⚠️ **The test names in this item are outdated.** The Hard Gate later moved to a per-batch
> scope and is now `hard_gate_latest_batch_error_rate` (latest `received_at` partition, 0.15,
> **error**) plus `monitor_dataset_error_rate` (whole table, 0.1, warn) — see `stg_orders.yml`.
> The `_0_05` / `_0_1` pair no longer exists. The observation still holds; the thresholds and
> scope changed underneath it.

### 5.1.1 Rebuilding the fixture and walking the v2→v3→v4 SOP back to back (2026-08-12) ⭐

Migrating to the native Docker Engine replaces Docker's DataRoot, which in turn gives the
business DB a brand-new volume. **That cost was weighed and accepted before the migration** —
the fixture is simulated data that can simply be re-seeded, and keeping backups of it would mean
paying maintenance cost for something with no value. Since it had to be re-seeded anyway, the
rebuild was run as a full SOP exercise: a **deliberately shaped** batch of data walked through
§3.3 all the way from v2 to v4, filling in the branches §5.1 could not cover.

#### One unavoidable trade-off: replay the *rule state*, not the *commit state*

`git checkout dq-rules-v2` does not work — `business_clean`'s `(ods, as_of)` signature,
`NON_REPRODUCIBLE_CODES`, and the `AGE_MIN`/`AGE_MAX` constants were **all introduced in v3**,
so a genuine rollback makes `reevaluate_quality.py` fail at import. The approach instead was
**HEAD's code + that version's thresholds + that version's label**.

For the two violation codes this rebuild exercises (`age_out_of_range`, `field_too_long`) that is
behaviourally identical to the real older versions: the `as_of` parameter v2 lacked behaves the
same when the ingest path omits it, and `NON_REPRODUCIBLE_CODES` only matters during
re-evaluation — while the v2 batch is only ever ingested, never re-evaluated *as* v2.
**The v4 slot is exactly identical**: `git diff dq-rules-v4 HEAD -- clean.py` is empty, so the
end state is simply `git checkout clean.py` and the whole replay leaves zero code residue.

> For the same reason **no git tags were touched**. `dq-rules-v2/v3/v4` point at real historical
> commits; adding or moving them for a data replay would pollute a real record with a synthetic
> source. The "tag it" step in §3.3 is for actual rule changes.

#### Data shape

| Batch | Rows | Ingested under | Purpose |
|---|---|---|---|
| `SEED-*` main corpus | 1,000 (57 dirty, 5.7%) | v2 | Long-tail error-code distribution, report baseline |
| `V3DEMO-*` | 20 | v2 | 15 with `age ∈ {121,123,125,127,130}` + 5 controls `{-3,150,999}` |
| `V4DEMO-*` | 20 | **v3** | 15 with `customer_name` length `{110,120,130,140,150}` + 5 controls `{160,200,240}` |

**`V4DEMO` has to be ingested under v3** — the same reasoning as §5.1's "deliberate order": only
data judged dirty under the old rules is eligible to be rescued by the new ones.

#### Both flow-back rounds, measured

| | Round 1 (v2→v3) | Round 2 (v3→v4) |
|---|---|---|
| candidates | 77 | **97** |
| `would_write` / `written` | 16 | 15 |
| `unchanged` | 61 | 82 |
| `re_quarantined` | 0 | **0** |
| `blocked_non_reproducible` | **4** | **4** |
| `fct_orders` | 943 → **959** | 959 → **974** |
| quarantine | 77 → **61** | 66 → **66** (+20 new, −15 promoted, net +5 controls) |
| Idempotency re-run | `written=0`, `unchanged=77` | — |

Three observations worth recording:

**① Round 2's `candidates=97` proves why the `int_orders` half of the union is needed.**
97 = 61 (still quarantined) + 20 (newly arrived `V4DEMO`) + **16 (promoted in round 1)**.
`candidate_sql` unions in `int_orders WHERE has_clean_error` precisely so the
`promoted → re_quarantined` edge stays reachable. Here `re_quarantined=0` — v4 loosens rather
than tightens — but those 16 rows were genuinely re-checked, not excluded.

**② `blocked_non_reproducible=4` fills the gap §5.1 left.**
That run recorded 0, so the branch had only unit-test coverage. This time the main corpus landed
4 rows carrying `non_finite_number`, and `NON_REPRODUCIBLE_CODES` refusing to promote them was
measured on real data for the first time: **all 4 were candidates in both rounds, both rounds
judged them "clean now", and neither round promoted them.**

> ⚠️ Don't conflate this path with "an order-level field set to inf returns 500".
> `_dirty_non_finite_number` **deliberately targets `items` only** — items skip Pydantic
> coercion (`ODSOrder.items` is `Any`), so non-finite values get in, `business_clean` flags them
> and normalises them to None, and the path works (1,000 rows, 100% landed). The order-level
> `tax_pct` / `customer_rating` are Pydantic float fields and behave differently — that is the
> one that breaks. The injector picks items to keep the trigger path single and predictable.

**③ The one extra promotion is explainable, not noise.**
Round 1 wrote 16 while the targeted batch held only 15; the extra one is a main-corpus row with
`age=125` (drawn by `_dirty_age_out_of_range` from `[-3,125,150,999]`). The post-flow-back
distribution is `age=121×3, 123×3, 125×4, 127×3, 130×3`. **Before writing to an append-only
table, every discrepancy has to reconcile.**

#### End state

PG `ods` **1,040** = BQ `stg_orders` **1,040**; PG `quality_events` **1,071** =
BQ `stg_quality_events` **1,071**. The event chain:

```
v2  initial_evaluation  943 clean / 77 quarantined
v3  initial_evaluation   20 quarantined      +  promotion 16
v4                                              promotion 15
```

ODS's `dq_rule_version` distribution stays at v2 **1,020** / v3 **20** with not one row
rewritten — **35 live "permanent divergence" samples** (ODS says dirty, Gold says clean), a
fuller set than §5.1's 15 because it now spans two generations, v2→v3 and v3→v4.

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

### 5.4 `raw_id` collides across two ODS instances ⭐

**The situation at the time**: the business DB ran on the host while Airflow ran in containers, and
one way to connect them was "bring compose's `db` up too" — which gives you a **separate, empty
database** whose `raw_id`s start at 1 and **overlap completely** with the host ODS's. Extract both
into the same BQ staging table and `stg_`'s `raw_id`-grained dedup collapses **two unrelated orders
into "copies" of each other**, dropping one. No error, no trace.

**When to watch for it**: any time there is more than one landing instance — host and container,
blue/green deployments, several upstreams each with their own Raw table — this collision recurs.

**The fix**: upgrade the dedup key to something like `(source_instance, raw_id)`, or carry an
instance identifier from the extract onwards. **Until then, one staging table can only correspond
to one ODS** — never mix instances into a single dataset.

This is a corollary of the [README](./README.md)〈`raw_id` is physical identity, `order_id` is
business identity〉principle that the original text did not push all the way: **`raw_id`'s
uniqueness only holds within a single landing instance.** Choosing `raw_id` as the dedup key is
correct (physical dedup should use physical identity), but it welds the "single instance" premise
into the pipeline.

> **No longer a risk in this project**: it is a portfolio piece, and after moving fully into compose
> there is exactly one ODS, so the cause is gone (§3.1). This section stays because the premise is
> still welded in — it just happens to be permanently true.

### 5.5 Full-compose rebuild and the v4 flow-back (2026-08-11) ⭐

The environment moved fully into compose, the dataset was rebuilt from zero, and one rule-loosening
cycle was walked end to end under v4.

**Infrastructure**

| Item | Result |
|---|---|
| `alembic upgrade head` from zero | all 7 migrations passed — a path a long-lived dev database never exercises |
| Service health | 9 containers (db/redis/api/worker/beat + four Airflow services) all healthy |
| Airflow → `api:8000` / `db:5432` | both reachable, and reading the same database (`ods=8`) — clearing §6's longest-standing ⬜ |
| BQ rebuild after a full wipe | `extract_ods_to_bq.py`'s `create_dataset` / `create_table(exists_ok=True)` rebuilt everything with partitioning and `require_partition_filter` intact — **zero manual DDL** |
| Main DAG | 7/7 tasks success, ~**2.5 minutes** end to end |
| `source_freshness_watch` | both sources **PASS** — flipped from "expected red" to "expected green" |

**The landed-rows gate (`--require-landed-pct`), both directions**

With `worker` stopped, 3 records were posted:

| | ODS | exit code |
|---|---|---|
| without the flag (old behaviour) | 0 rows | **0** ← silent success, exactly what it must prevent |
| `--require-landed-pct 0.9` | 0 rows | **1** ← caught |

After restarting `worker`, all 13 `pending` rows were re-dispatched by `scan_and_dispatch` —
self-healing verified alongside.

**v4 rule loosening and flow-back**

3,015 rows, 265 in quarantine. Target: `customer_name` soft cap 100→150.

| Step | Result |
|---|---|
| Dry-run | `candidates=265 promoted=3 would_write=3` |
| Commit | `written=3`; `quality_events` = 3015 `initial_evaluation@v3` + 3 `promotion@v4` |
| **Bounded Writeback** | ODS fingerprint **identical before and after** (3015 rows, 265 dirty, unchanged) |
| Idempotency | second run: `promoted=0 written=0 unchanged=265` |
| Flow-back into Gold | `int_orders +3`, `quarantine 265→262`, `fct_orders +3`, `promotions 0→3` |
| Row-level check | all 3 show `fct_orders=1 / quarantine=0` |
| Control group | `customer_name` 157/164/176/188/199 and 5 `city` rows **all stayed quarantined** |

> The control group formed **naturally out of the same injector** (`_dirty_field_too_long` spreads
> lengths over 110–200 and targets `city` half the time), unlike v3 which needed one prepared
> separately. The boundary is tighter too: **146 promotes, 157 does not.**

#### Two inferences overturned ⭐

**① `--expect-rule-version` covers less than assumed**

Measured before rebuilding the images: `api`/`worker` reported `v3 {'customer_name': 100}` while
Airflow reported `v4 {'customer_name': 150}` — and `--expect-rule-version v4` **passed**. The guard
only compares the version inside its own process. **It holds only if the whole system has a single
code-delivery mechanism**, and this compose topology (baked image vs bind mount) breaks that
premise. Handling: §3.3 step 3.

**② The directionality of the candidate source was never written down**

`dq_reevaluation`'s header only recorded "re-evaluation writes to PG; an extract is needed for
flow-back into Gold". The reverse holds too: **candidates are read from BQ, so the data must reach
BQ first.** The first dry-run returned `candidates=26 / would_write=0` — not because the rule had
not taken effect, but because BQ still held the pre-accumulation state. Handling: §3.3 step 4.

#### Measured in passing

- **Unpausing a DAG immediately creates a scheduled run**: `staging.orders` therefore held
  398 = 199×2 rows while `stg_orders` held exactly 199 — an accidental live confirmation that
  append-only tolerance plus dedup in `stg_` works as designed.
- **Jinja template errors surface only at runtime**: DagBag parsing was clean, `dags list` normal,
  every structural test green — yet the task failed in 0.16s. All three variants hit (nested
  `{{ }}`, an f-string escaping `}}` down to `}`, and `data_interval_start` being absent in manual
  runs) are catchable **only by actually rendering the template**, so `tests/test_dags.py` gained
  render tests.
- **A cron `data_interval_start` is the *previous* fire point**: using it as a date seed would make
  each day's first slot pick up **yesterday's** value, breaking the single-dirty-rate-per-day
  invariant. Switched to `dag_run.run_after` (see `seed_demo_daily.py`'s header).

---

## 6. Status and TODO

- ✅ `orders_analytics_daily` (2 extracts → 4 layered dbt builds → full `dbt test`; 22:30 Taipei)
- ✅ `dq_reevaluation` (manual, dry-run by default, chains into the main DAG on commit)
- ✅ `source_freshness_watch` (backstop for extract; 08:00 Taipei; flipped from "expected red" to
  "expected green" on 2026-08-11)
- ✅ `seed_demo_daily` (simulated upstream; 10/13/17/21 Taipei, 800 orders/day)
- ✅ `raw_pending_watch` (dispatch liveness; 10:30/13:30/17:30/21:30 Taipei; threshold derived from
  the recovery path's own settings — see §2.12)
- ✅ `seed_demo_gate_demo` (Hard Gate interception script, manual)
- ✅ Image (two isolated venvs), compose overlay, env_var-driven `profiles.yml`
- ✅ Fully in compose (db/redis/api/worker/beat and Airflow in one project, on one dataset)
- ✅ `tests/test_dags.py` (47 tests) + a dedicated CI job (`.github/workflows/dags.yml`)
- ✅ Verified live (2026-08-05): image builds, all four services healthy, three DAGs parsed by the
  real dag-processor with **zero import errors**, both venvs working (dbt 1.11.12 / bigquery 1.11.3),
  the env_var profile connecting to BQ from inside the container, a full successful
  `source_freshness_watch` run, and `dbt_intermediate` passing in-container with PASS=27
- ✅ `extract_*` executed in-container (passing as of 2026-08-11 — moving fully into compose
  removed the whole class of obstacle; see §5.5)
- ✅ The v3 rule loosening (`age` cap 120→130) — Proposal B's first genuine promote candidates
- ✅ The v4 rule loosening (`customer_name` soft cap 100→150)
- ✅ The §3.3 SOP walked end to end **four times**: v3 (2026-08-05, 15 promoted), v4 (2026-08-11,
  3 promoted), and the back-to-back v2→v3 (16 promoted) and v3→v4 (15 promoted) during the
  2026-08-12 fixture rebuild; all four idempotent, ODS never modified, control group left
  quarantined. Full figures in §5.1, **§5.1.1**, and §5.5
- ✅ Celery + Redis (implemented, orthogonal to this layer; see [QUEUE.md](./QUEUE.md))
- 🟡 OpenTelemetry — traces + operational metrics are live (2026-08-17); Airflow integration and absent alerting are not (see §4)
- ⬜ A formal resolution for cross-timezone extraction (§2.11's a/b/c remain unchosen — none can be
  validated without real traffic crossing the day boundary)

## 7. Dependencies and Versions

- Airflow **3.0.0** (`apache/airflow:3.0.0-python3.12`), LocalExecutor
- dbt-core / dbt-bigquery **1.11** (aligned with [ecommerce_dbt/README §10](./ecommerce_dbt/README.md))
- When upgrading Airflow, `ARG AIRFLOW_VERSION` in `orchestration/Dockerfile` and
  `AIRFLOW_VERSION` in `.github/workflows/dags.yml` must change together (the constraints file is
  fetched by version)
