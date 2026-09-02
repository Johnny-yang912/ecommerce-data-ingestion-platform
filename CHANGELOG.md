# Changelog

**English** | [繁體中文](./CHANGELOG-TW.md)

How this system got here. Phases are the release unit; within each, entries are one line plus a link to the decision or the measurement.

**What is in scope**: changes to behaviour, contracts and architecture. Refactors and documentation are not listed unless they changed a decision.

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
- **NUL-byte poison pill.** A `\u0000` escape on the wire is six legal ASCII characters, so the ingress guard stripped nothing; `json.loads` decoded it into a real NUL, and the resulting `ValueError` was retried as if transient — forever. → [ADR-0006](./docs/en/adr/0006-nul-byte-fast-fail.md)
- **No DB transaction may span the dispatch.** `db.refresh()` did; measured at 60 concurrent, 23 of 32 pool slots stuck `idle in transaction`.
- **Money moved from `FLOAT64` to `NUMERIC`** after a rollup test went red on 39 rows differing by **1 ULP** — `SUM()` over floats is not associative. The fix was the type, not a tolerance. → [design/transformation](./docs/en/design/transformation.md)
- **The incremental window's left boundary was not aligned to the partition boundary — the same defect existed in three models.** `insert_overwrite`'s atomic unit is a whole partition, and the left boundary carried the run's wall-clock time — one manual run two hours ahead of schedule let **half a day atomically overwrite a whole day**, cutting the `2026-08-26` partition of both `stg_orders` and `stg_quality_events` from **800 rows to 250**. The DAG was green, dbt tests were green, and upstream staging was untouched. All three models (the third being `rpt_quality_events_daily`) now align to the day edge, each gained targeted-backfill vars, and two per-partition reconciliation tests landed. **The repair took two phases**: the first covered only `stg_orders` and was declared complete; the other two surfaced that evening from a BI discrepancy — the scope had been drawn from "which table broke" when the defect lives at the level of the idiom. → [ADR-0055](./docs/en/adr/0055-partition-aligned-incremental-window.md) · [incident](./docs/en/incidents/2026-08-30-stg-partition-truncation.md)

- **Endpoints changed from `async def` to `def`: synchronous DB calls no longer hold the event loop.** All three handlers (`/orders`, `/process_raw`, `/raw`) made blocking psycopg2 calls with no `await` anywhere in the connection-holding window, so a single stuck query froze **an entire uvicorn process**, not just that request. **Measured by pausing PostgreSQL for 8 seconds: a `/health` request — which touches no database at all — was held for 8.2s (40ms after the change)** — the freeze lasts as long as the database is stuck, bounded by `statement_timeout` (30s). Under ordinary load `/health` p99 also went from 167ms to 34ms; throughput +42%, and the worker curve no longer inverts at 8 (207 → 485 RPS). ⚠️ The cost: the API accepts faster while the worker drains slower during a burst (CPU contention on one machine), so the same 60,000-request burst peaked at a backlog of 36,526 instead of 5,453 — still zero errors, fully recovered in 119s. **The fix is for failure amplification; the performance change is a side effect.** → [measured](./docs/en/verification/2026-09-02-sync-handlers-before-after.md)

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
- An entry that **overturned a written conclusion** links to the verification record that overturned it. Six of those exist. → [verification/](./docs/en/verification/)
- "Decided against" is a first-class category. Each entry carries a trigger for revisiting it. → [PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md)
