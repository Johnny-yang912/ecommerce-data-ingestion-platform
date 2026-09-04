# Changelog

**English** | [繁體中文](./CHANGELOG-TW.md)

How this system got here. **Two timelines**: Phases are the release unit and record how the architecture evolved; "Defects & Fixes" records what reality sent back once it was running. Every entry carries a link to the decision or the measurement.

**What is in scope**: changes to behaviour, contracts and architecture. Refactors and documentation are not listed unless they changed a decision.

---

## Defects & Fixes

**Defects that only appeared once the system was running.** They looked sound when designed, then broke one day during a real run, a load test or a routine build — a timeline running parallel to the Phases. Fixes forced by a new feature are recorded under that Phase's own "Fixed".

Each entry records: **when, in what setting it surfaced, the root cause, and what changed.** The account and the measurements live in the links.

---

### The validation-error response itself crashed, turning a 422 into a 500

`Found 2026-09-03 · fixed 09-04` · **`seed_demo` real run**

Pydantic puts the offending value into the error report's `input`, and `JSONResponse` serialises with `allow_nan=False` — so when `items[].quantity` received `NaN`/`Infinity`, **the verdict was correct (422) but that 422 died during render**, and an exception raised inside an exception handler has nowhere left to go → a bare 500. The root cause is that the parser at ingress is more permissive than the serialiser at egress (`json.loads` accepts `NaN` by default). The dividing line is the field's declared type: of four non-finite injections that day, the two that landed on `quantity` (int) each made an order vanish with a 500 without landing at all, while the two on `unit_price` (float) landed as usual and were flagged `NON_FINITE_NUMBER`. **The cost is not that one order** — a 500 means "my fault, try again", so an upstream retries a payload that can never succeed.

**Changed**: non-finite floats are sanitised recursively before serialisation; the response shape is unchanged. This brings the implementation in line with [ADR-0054](./docs/en/adr/0054-type-declaration-governance.md), which already specified hard type error → 422 + `ingress_rejected` — not a new contract. Same family as [ADR-0006](./docs/en/adr/0006-nul-byte-fast-fail.md), with JSON as the egress instead of PostgreSQL's TEXT.

### Endpoints changed from `async def` to `def`: synchronous DB calls no longer hold the event loop

`2026-09-02` · **load test** — inferred from the code during capacity and bottleneck work, then measured before/after

All three handlers (`/orders`, `/process_raw`, `/raw`) made blocking psycopg2 calls with no `await` anywhere in the connection-holding window, so a single stuck query froze **an entire uvicorn process**, not just that request. **Measured by pausing PostgreSQL for 8 seconds: a `/health` request — which touches no database at all — was held for 8.2s** (40ms after the change) — the freeze lasts as long as the database is stuck, bounded by `statement_timeout` (30s).

**Changed**: all three endpoints became `def`, moved by Starlette into the anyio thread pool. Under ordinary load `/health` p99 went from 167ms to 34ms; throughput +42%, and the worker curve no longer inverts at 8 (207 → 485 RPS). ⚠️ The cost: the API accepts faster while the worker drains slower during a burst (CPU contention on one machine), so the same 60,000-request burst peaked at a backlog of 36,526 instead of 5,453 — still zero errors, fully recovered in 119s. **The fix is for failure amplification; the performance change is a side effect.** → [measured](./docs/en/verification/2026-09-02-sync-handlers-before-after.md)

### The incremental window's left boundary was not aligned to the partition boundary — the same defect existed in three models

`Occurred 2026-08-29 20:38 · found 08-30 10:20 · repaired the same day in two phases` · **BI observation** — the revenue curve on Looker got shorter, the gap smeared across ~45 `order_date`s, looking like noise rather than a failure

**Nothing alerted**: the DAG was green, all 93 dbt tests were green, upstream staging was untouched. `insert_overwrite`'s atomic unit is a whole partition, and the left boundary carried the run's wall-clock time — one manual run two hours ahead of schedule let **half a day atomically overwrite a whole day**, cutting the `2026-08-26` partition of both `stg_orders` and `stg_quality_events` from **800 rows to 250**, with the gap propagating into Gold and the quality reports.

**Changed**: all three models (the third being `rpt_quality_events_daily`) now align to the day edge, each gained targeted-backfill vars, and two per-partition reconciliation tests landed. ⚠️ **The repair took two phases**: the first covered only `stg_orders` and was declared complete; the other two surfaced that evening from a BI discrepancy — the scope had been drawn from "which table broke" when the defect lives at the level of the idiom. → [ADR-0055](./docs/en/adr/0055-partition-aligned-incremental-window.md) · [incident](./docs/en/incidents/2026-08-30-stg-partition-truncation.md)

### Money moved from `FLOAT64` to `NUMERIC`

`2026-08-04` · **routine build** — `assert_fct_orders_rollup_matches_items` went red on its own, and it was not designed to catch this

`SUM()` over floats is not associative, and `fct_orders`' rollup and the test's re-aggregation take different execution plans, so 39 rows differed by about **1 ULP** (9.095e-13 absolute). It stayed latent so long because until then every order inside the 60-day window carried **exactly one item** — a single-value `SUM()` has no accumulation and therefore no ordering effect.

**Changed**: item amounts cast to `NUMERIC(38, 9)`; `quantity` stays `INT64` (it is a count). **The fix was the type, not a tolerance** — a tolerance hides the real defect, and "what tolerance, and does it need retuning as the data grows" is a question that comes back. → [ADR-0047](./docs/en/adr/0047-measures-roll-up-to-header.md) · [design/transformation](./docs/en/design/transformation.md)

### NUL-byte poison pill

`2026-06-16` · ⚠️ **no record of how it surfaced**

The same value has different representations at different stages of the pipeline, and the guard and the hazard lived in different representation spaces: a `\u0000` escape in an HTTP body is **six legal ASCII characters**, while the ingress guard strips real `0x00` bytes — of which there were none, so it stripped nothing. `json.loads` then decoded it into a real NUL. psycopg2 raises a **bare `ValueError`** during **parameter adaptation** — not a DBAPI error, so it never arrives as `DataError` — and it fell into the generic `except Exception`, **where it was retried as if transient**. It is deterministic: the record stayed in `processing` and the recovery scan re-dispatched it every interval, forever.

**Changed**: an `except ValueError` branch after `except DataError` in the commit loop, rolling back and fast-failing to a terminal `error`. **This failure was never really about NUL bytes — it was about a deterministic error classified as transient.** → [ADR-0006](./docs/en/adr/0006-nul-byte-fast-fail.md)

---

## Phase 5 — Automation & Queue Upgrade · 2026-08

### Added

- **Celery + Redis** replacing `BackgroundTasks`. `process.py` stays Celery-free, preserving the manual rescue path. → [ADR-0010](./docs/en/adr/0010-celery-replaces-backgroundtasks.md) · [ADR-0012](./docs/en/adr/0012-process-stays-celery-free.md)
- **Airflow 3.0.0**, six DAGs, every schedule declared in `Asia/Taipei`. No DAG depends on another at the Airflow level — the ordering contract lives in the time gaps. → [design/orchestration](./docs/en/design/orchestration.md)
- **Dispatch circuit breaker.** Degradation with the broker down is *super-linear* — 47 of 48 concurrent requests failed to complete within 120s, while their Raw rows **had in fact been written**. p50 under outage: timeout → **5ms**. → [ADR-0014](./docs/en/adr/0014-circuit-breaker-dispatch.md) · [measured](./docs/en/verification/2026-08-10-circuit-breaker-before-after.md)
- **Bounded recovery scan** — pagination + id cursor + per-run cap + Redis lock + grace period. Verified against a 120,000 backlog: ODS grew by exactly 120,000, zero duplicates. → [ADR-0017](./docs/en/adr/0017-bounded-recovery-scan.md) · [measured](./docs/en/verification/2026-08-10-bounded-scan-120k.md)
- **Proposal B event producer** (`reevaluate_quality.py`) — candidates from BQ, state decided against PG, events appended only on an actual state change. → [ADR-0030](./docs/en/adr/0030-proposal-b-event-driven-reevaluation.md)
- **OpenTelemetry** — traces across `api` → Celery → `worker`, plus operational metrics. **320 active series, 3.2% of the free tier.** → [ADR-0050](./docs/en/adr/0050-resident-otel-collector.md) · [ADR-0052](./docs/en/adr/0052-sdk-views-series-budget.md)
- **Failure notification** whose message states the **response**, not the task name. Attached at task level, so a broken seven-task chain sends exactly one message. → [ADR-0042](./docs/en/adr/0042-failure-notification-response-not-task.md)
- `raw.processing_started_at`, and `recovery_policy.py` as the thresholds' dependency-free home.

### Changed

- **Recovery scan moved from FastAPI's lifespan to Celery Beat.** That is what allows multiple API processes. → [ADR-0016](./docs/en/adr/0016-recovery-scan-in-beat.md)
- **Rate-limit counters moved to Redis db 1.** Across 4 uvicorn workers, `60/minute` had silently become `60 × workers` — measured, **91 of 100** requests got through instead of 60, **with no error raised anywhere**. → [measured](./docs/en/verification/2026-08-10-rate-limit-multiprocess.md)
- **Hard Gate rescoped from whole-table to per-batch.** The whole-table denominator dilutes single-batch anomalies and cannot heal; measured, four consecutive batches ran above 10% error (one at 100%) while the whole-table figure sat at 9.122% and the gate **never fired**. → [ADR-0028](./docs/en/adr/0028-hard-gate-per-batch-scope.md)
- **`source_freshness_watch` flipped from "expected red" to "expected green"** once seeding became the system's real data source.

### Fixed

- **Staleness judged by `processing_started_at`, not `received_at`.** Under backlog the old basis reclaimed records that were actively being processed, so one `raw_id` ran on two workers — reproduced, 2 occurrences across a 2,000-record backlog. **CAS did not fail**: it cannot stop a third party reverting the state. → [ADR-0015](./docs/en/adr/0015-staleness-from-processing-started-at.md) · [measured](./docs/en/verification/2026-08-10-staleness-basis-self-collision.md)
- **No DB transaction may span the dispatch.** `db.refresh()` did; measured at 60 concurrent, 23 of 32 pool slots stuck `idle in transaction`.

### Decided against

- **Business / DQ metrics at tier 1** — the simulated upstream's dirty rate is constant within a day, so a minute-level error rate says nothing the warehouse does not. → [PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md)
- **Absent alerting and dashboards** — writable today, but their value thresholds and response procedures need real traffic, and the rules would live in a UI that cannot be version-controlled. **The reasoning is the deliverable.** → [design/liveness-alerting](./docs/en/design/liveness-alerting.md)
- **Airflow → OTel integration** — technically ready all along; every consumer of it is itself deferred.

---

## Phase 4 — Analytics Pipeline · 2026-06 → 2026-08

### Added

- **`extract_ods_to_bq.py`** — batch load only, watermark derived from `INFORMATION_SCHEMA.PARTITIONS`, partitioned + clustered staging with `require_partition_filter` as a cost fuse. → [ADR-0019](./docs/en/adr/0019-batch-load-not-streaming.md) · [ADR-0023](./docs/en/adr/0023-watermark-approach-a.md)
- **dbt `stg_` layer** — dedup on `raw_id`, Hard Gate, source freshness; incremental with `copy_partitions` to work around the sandbox's DML ban. → [ADR-0044](./docs/en/adr/0044-copy-partitions-sandbox-dml.md)
- **dbt `int_` layer — where blocking happens.** The Row Filter keys on the **effective quality state**, not the literal flag, because ODS is immutable and a promoted record reads dirty forever. → [ADR-0029](./docs/en/adr/0029-effective-quality-state.md)
- **dbt `dim_`/`fct_`** — dual fact tables, two SCD1 dimensions, rollup consistency guarded by a singular test. → [ADR-0047](./docs/en/adr/0047-measures-roll-up-to-header.md) · [ADR-0048](./docs/en/adr/0048-two-dimensions-scd1.md)
- **dbt `rpt_`** — three tables; quality reporting split into two because an event axis and a snapshot have opposite mutability. → [ADR-0049](./docs/en/adr/0049-business-reports-read-gold.md)
- **`FIELDS` consistency test** — without it, adding an ODS column and forgetting `FIELDS` fails **silently**. → [ADR-0026](./docs/en/adr/0026-fields-single-source.md)

### Changed

- **`int_` materialisation kept as a full rebuild**, deliberately not incremental: a Proposal B promotion lands in today's partition while the order it rescues sits in an old one, so an incremental window would sever the flow-back path **silently**. → [ADR-0046](./docs/en/adr/0046-stg-incremental-int-full-rebuild.md)

### Retracted

- **The planned "legal partition range guard" before adopting `order_date` partitioning.** Measured: out-of-range dates do **not** fail the build — they land silently in `__UNPARTITIONED__`, and escape the 60-day reaper too. The assumed failure mode was the wrong one. → [measured](./docs/en/verification/2026-08-partition-expiry-measurement.md)

---

## Phase 3 — Operability · 2026-06

- **Service-to-service authentication** — static `X-API-Key`, `secrets.compare_digest`, multiple keys per client for rotation. The resolved `client_id` lands as `source_client_id`: **lineage comes free with authentication**. No user-facing JWT — there are no human users. → [ADR-0007](./docs/en/adr/0007-static-api-key-not-jwt.md)
- **Centralised configuration** with an explicit boundary: environment values only. Retry counts and stale thresholds stay at the top of their own modules, because **they are program behaviour, not environment**. → [ADR-0008](./docs/en/adr/0008-config-boundary.md)
- **Alembic as the single source of truth**; `create_all` removed entirely — it only creates and never alters, so it cannot carry schema evolution. `Base.metadata` carries a naming convention so constraint names are stable across environments. → [ADR-0009](./docs/en/adr/0009-alembic-single-source-of-truth.md)
- **Docker / docker-compose** — `db` (healthcheck) → `migrate` → `api`, secrets injected at runtime and never baked into the image.

---

## Phase 2 — Testability · 2026-05

- **Pytest suite** — unit and integration coverage of all four retry points, CAS claim, idempotency, crash recovery, cleaning rules and auth.
- **A handful of tests that pin decisions rather than behaviour** — the partition invariant, the rollup invariant, "never split `dbt build`", freshness isolation, schema consistency, probe dependency isolation. **Each converts a discipline into a mechanism.** → [design/testing](./docs/en/design/testing.md)
- **Data quality architecture** designed and the ODS layer implemented: `DQ_RULE_VERSION`, the `dq_rule_version` column, and the append-only `quality_events` state machine. → [ADR-0031](./docs/en/adr/0031-rule-versioning-quality-events.md)

---

## Phase 1 — Reliability · 2026-04 → 2026-05

- **Four-point retry** with exponential backoff — Raw write, claim, processing, status commit — and a `CRITICAL` log when retries are exhausted.
- **CAS claim** via `rowcount == 1`, with no external lock service: the state column that had to exist anyway does the work. Verified with 100 workers competing for one `raw_id` → ODS count **1**. → [ADR-0004](./docs/en/adr/0004-cas-claim-rowcount.md)
- **Idempotency** — `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`, first-write-wins, `IntegrityError` as the TOCTOU backstop. → [ADR-0005](./docs/en/adr/0005-first-write-wins-idempotency.md)
- **`duplicate` as a terminal status rather than a rejection**, because "the upstream sent twice" and "this system failed" demand different responses. → [ADR-0003](./docs/en/adr/0003-duplicate-terminal-status.md)
- **Raw layer does no business deduplication** — repeat submissions may carry complementary fields, and submission frequency is itself a signal. → [ADR-0001](./docs/en/adr/0001-raw-no-business-dedup.md)
- **`has_clean_error` is non-blocking** — the decision the entire quality architecture rests on. → [ADR-0002](./docs/en/adr/0002-has-clean-error-non-blocking.md)
- Timeouts, pool sizing, and per-client rate limiting.

---

## Conventions

- Phases are the release unit. There are no semantic versions — this is not a distributed package.
- **Where a fix goes depends on how it surfaced**: hit while doing a Phase's work and caused by that Phase's own additions, it belongs under that Phase's "Fixed"; surfaced only after the system had been running for a while — in a real run, a load test or a build — it belongs in "Defects & Fixes".
- An entry that **overturned a written conclusion** links to the verification record that overturned it. Six of those exist. → [verification/](./docs/en/verification/)
- "Decided against" is a first-class category. Each entry carries a trigger for revisiting it. → [PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md)
