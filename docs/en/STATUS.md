# Implementation Status

**English** | [繁體中文](../zh-TW/STATUS.md)

Last reviewed: 2026-08-24

This is the single source of truth for what is built, what is not, and why. Design documents describe how the system works; they do not carry status. If a design document and this file disagree, this file is right.

---

## Status vocabulary

Four states, used consistently across every document in this repository.

**This project is complete as scoped.** Everything below that is not ✅ is a decision or a constraint, not unfinished work.

| Mark | Meaning | What it implies |
|---|---|---|
| ✅ **Implemented** | Built and exercised | — |
| ⛔ **Decided against** | Evaluated, and the answer was no | Would still be no with real traffic. Each carries a trigger for re-evaluation |
| ⏸ **Deferred** | Would be built in a real system; cannot be built meaningfully here | Blocked by the portfolio context — no real traffic, or a paid account. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| ⬜ **Open** | Should be built, can be built, not built yet | The only state that is genuine backlog |

The distinction that matters: **⛔ is "should not", ⏸ is "should but cannot", ⬜ is "should and will".** Reading all three as "not done" is the misreading this vocabulary exists to prevent.

**⬜ is currently empty**, because the project is complete as scoped. The category is kept for future expansion: if this system is extended, genuine backlog items would be listed there.

---

## Layer matrix

| Layer | State | Notes |
|---|---|---|
| Ingestion (API → Raw) | ✅ | Retry, rate limiting, API-key auth, pool guards |
| Processing (Raw → ODS) | ✅ | CAS claim, idempotency, quality flagging, `quality_events` |
| Task queue | ✅ | Celery + Redis, circuit breaker, bounded recovery scan |
| Extraction (ODS → BQ) | ✅ | Watermarked incremental load, partitioned + clustered staging |
| Transformation (dbt) | ✅ | `stg_` → `int_` → `dim_`/`fct_` → `rpt_`, 93 tests |
| Orchestration (Airflow) | ✅ | Six DAGs, two isolated venvs, failure callbacks |
| Observability (OTel) | ✅ | Traces + operational metrics; alerting deliberately absent |
| BI (Looker Studio) | ✅ | Demo dashboards reading the `rpt_` layer. No real audience, so nothing validates whether a given chart is *useful* — but the connection and the semantic layer are exercised |

---

## Ingestion and processing

| Item | State | Detail |
|---|---|---|
| Four-point retry with exponential backoff | ✅ | Raw write, claim, processing, status commit; `CRITICAL` log on exhaustion |
| Crash recovery scan | ✅ | Moved to Celery Beat in Phase 5; startup catch-up scan included |
| Timeouts and pool guards | ✅ | 30s statement timeout, explicit pool sizing, `503` on pool exhaustion |
| Idempotency | ✅ | `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`, first-write-wins, `IntegrityError` backstop |
| Per-client rate limiting | ✅ | slowapi keyed on `client_id`, counters shared via Redis db 1 |
| API-key authentication | ✅ | `X-API-Key`, `secrets.compare_digest`, resolved `client_id` persisted as lineage |
| Centralised configuration | ✅ | pydantic-settings `Settings` singleton; algorithmic constants deliberately excluded |
| Alembic as schema source of truth | ✅ | `create_all` removed; naming convention on `Base.metadata` |
| Docker / compose | ✅ | db → migrate → api/worker/beat, gated by healthchecks |
| NUL-byte poison pill | ✅ | `ValueError` fast-fail to terminal `error` state |
| Sanitising NUL instead of rejecting | ⛔ | Rejecting is consistent with `DataError`. **Trigger**: a decision that such orders must land flagged rather than be rejected — would require a new clean-error code and a `DQ_RULE_VERSION` bump |

## Task queue

| Item | State | Detail |
|---|---|---|
| Celery + Redis replacing `BackgroundTasks` | ✅ | `process.py` stays Celery-free, preserving the manual rescue path |
| No result backend | ✅ | `raw.status` is the source of truth |
| `acks_late` + `reject_on_worker_lost` | ✅ | Redelivery on worker loss |
| Staleness from `processing_started_at` | ✅ | Fixed a defect where backlog caused one `raw_id` to run on two workers |
| Dispatch circuit breaker | ✅ | p50 under broker outage: timeout → 5ms |
| Bounded recovery scan | ✅ | Pagination + id cursor + per-run cap + Redis lock + grace period |
| Index on `raw.status` | ⏸ | Pagination bounds memory and dispatch volume, but not query cost. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## Cloud extraction

| Item | State | Detail |
|---|---|---|
| `extract_ods_to_bq.py` | ✅ | Batch load, watermark Approach A, `ALLOW_FIELD_ADDITION` |
| Partitioned + clustered staging with cost fuse | ✅ | `received_at` DAY, cluster on `order_id` + `has_clean_error`, `require_partition_filter` |
| `FIELDS` single source of truth | ✅ | Guarded by `tests/test_schema_bq_consistency.py` |
| `get_watermark()` abstraction | ✅ | The single seam for switching to a micro-batch watermark. Batch is the architecture's choice (see ADR-0019); the seam **records the exit, it is not an unfinished feature**. Why this project will not take it — partition budget and reporting grain both assume daily — is in ADR-0023 |
| Gold `order_date` partition retention | ⏸ | Sandbox forces 60-day expiry. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## Transformation (dbt)

| Item | State | Detail |
|---|---|---|
| `stg_orders`, `stg_quality_events` | ✅ | Dedup + Hard Gate + freshness; incremental with `copy_partitions` |
| `int_orders`, `int_orders_quarantine`, `int_order_items` | ✅ | Row Filter on effective quality state; partition invariant test |
| `dim_customer`, `dim_product` | ✅ | SCD1 + unknown member |
| `fct_orders`, `fct_order_items` | ✅ | Rollup consistency and lossless projection both test-guarded |
| `rpt_quality_events_daily`, `rpt_quality_backlog`, `rpt_sales_daily_by_category` | ✅ | Two time axes for quality reporting |
| Scenario-specific `int_orders_*` | ⛔ | Designed, deliberately not built. Deciding which errors are irrelevant and what to impute requires knowing the analytical question — **building one before a scenario exists would be a guess dressed as a design, and that holds in production too**. **Trigger**: a real analytical scenario |
| SCD2 `dim_customer` | ⏸ | Designed; dbt snapshots need write permissions the sandbox does not grant. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| Incremental `rpt_sales_*` | ⏸ | The motive for going incremental is cost and volume; 800 simulated orders a day supply neither. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| Monetary exposure measures | ⏸ | Exposure is a business figure; computed over generated amounts it would be misleading, not merely imprecise. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## Orchestration

| Item | State | Detail |
|---|---|---|
| Six DAGs, all schedules declared in `Asia/Taipei` | ✅ | No DAG depends on another at the Airflow level; the ordering contract lives in the time gaps |
| Two isolated venvs | ✅ | Nothing installed into Airflow itself |
| DAG tests + dedicated CI job | ✅ | 52 tests; DAG files import no project module |
| Proposal B event producer | ✅ | `reevaluate_quality.py` + `dq_reevaluation` DAG |
| Failure notification wiring | ✅ | Task-level `on_failure_callback`; message states the response, not the task name |
| Failure notification transport | ⏸ | Defaults to a log line; every message carries `channel=` so `channel=log` means nobody was notified |
| Cross-timezone extraction | ⏸ | Three candidate approaches, none chosen — none can be validated without real traffic crossing the day boundary. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## Observability

| Item | State | Detail |
|---|---|---|
| Resident OTel Collector | ✅ | Cloud endpoint and credentials in exactly one place |
| Distributed traces across api → Celery → worker | ✅ | Same `trace_id` verified across both processes |
| Operational metrics | ✅ | 320 active series, 3.2% of the free tier |
| `trace_id` / `span_id` in structlog | ✅ | Logs deliberately not routed over OTLP |
| Business / DQ metrics | ⏸ | The simulated upstream's dirty rate is constant within a day, so a minute-level error rate says nothing the warehouse does not already say. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| Absent alerting and dashboards | ⏸ | The reasoning is the deliverable — see [liveness alerting principles](./design/liveness-alerting.md) |
| Airflow OTel integration | ⏸ | Technically ready — what blocks it is that every consumer of it is itself deferred. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## Testing and CI

| Item | State | Detail |
|---|---|---|
| Unit + integration suite | ✅ | 445 tests, 100% coverage of gated modules, Python 3.10/3.12 matrix |
| DAG parse suite | ✅ | 52 tests in a separate workflow, under official Airflow constraints |
| dbt tests | ✅ | 93 tests including custom generic and singular invariants |
| End-to-end against a real database | ⏸ | Container-startup flake maintenance costs more than the present risk |
| `check_migration_drift.py` in CI | ⏸ | Deterministic and low-flake, so it *could* run in CI today; kept manual given solo development and a stabilising schema. See [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

---

## Known risks

| Risk | Impact | Current mitigation |
|---|---|---|
| `raw.status` has no index | Recovery-scan query cost grows with table size | Pagination bounds memory and dispatch volume; the index's shape needs real traffic |
| Cross-timezone extraction unresolved | The business "day" and the partition "day" can diverge | Documented; no traffic crosses the day boundary today |
| Gold `order_date` partitions expire at 60 days | Older rows silently disappear from Gold | Known and measured; a billing upgrade removes it |
| "Should have run, didn't" is uninstrumented | A stopped scheduler produces no red — `on_failure_callback` needs a run that actually happened | **Not an independent gap**: it is the consequence of absent alerting and the Airflow→OTel integration both being deferred. The delivery seam is ready (`_deliver()`, one env var from a real channel); what is missing is the *detector*. Residual blind spot from the [August 2026 stall incidents](./incidents/2026-08-silent-scheduling-stalls.md) |
| CI does not exercise DB-layer contracts | A green check does not mean CAS/dedup/migration are verified | Manual scripts: `load_test.py`, `restart_test.sh`, `check_migration_drift.py` |

---

## Related

- [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) — every ⏸ item, with what a real system would do instead
- [CHANGELOG](../../CHANGELOG.md) — how the system got here
- [Architecture Decision Records](./adr/README.md) — why each decision was made
