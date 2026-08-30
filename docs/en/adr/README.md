# Architecture Decision Records

**English** | [繁體中文](../../zh-TW/adr/README.md)

Each record captures one decision: the forces at the time, what was chosen, what it cost, and what was rejected. Records are append-only — a decision that changes gets a new ADR that supersedes the old one, or a change-log entry if only the mechanism moved.

**What is here and what is not.** An ADR is for decisions that had real trade-offs and could plausibly have gone the other way. Mechanical implementation detail stays in the [design documents](../design/). If a reader would not ask "why did you do it that way?", it is not an ADR.

Status values: **Accepted** · **Superseded by ADR-NNNN** · **Proposed**. The ⛔/⏸ marks used in [STATUS](../STATUS.md) apply to *features*; ADRs record *decisions*, which are simply accepted or not.

Start with [ADR-0000](./0000-template.md) for the format.

---

> **Rollout in progress.** A title without a link is planned but not yet written. The full set is listed so the scope stays reviewable.

---

## Ingestion and data architecture

| # | Decision | Status |
|---|---|---|
| 0001 | [No business deduplication at the Raw layer](./0001-raw-no-business-dedup.md) | Accepted |
| 0002 | [`has_clean_error` is non-blocking](./0002-has-clean-error-non-blocking.md) | Accepted |
| 0003 | [`duplicate` is a terminal Raw status, not a rejection](./0003-duplicate-terminal-status.md) | Accepted |
| 0004 | [CAS claim via `rowcount == 1`, with no external queue](./0004-cas-claim-rowcount.md) | Accepted |
| 0005 | [Idempotency is first-write-wins: pre-check plus `IntegrityError` backstop](./0005-first-write-wins-idempotency.md) | Accepted |
| 0006 | [NUL bytes fast-fail rather than being sanitised](./0006-nul-byte-fast-fail.md) | Accepted |
| 0007 | [Service-to-service auth is a static API key, not JWT](./0007-static-api-key-not-jwt.md) | Accepted |
| 0008 | [Centralised config covers environment values only, not algorithmic constants](./0008-config-boundary.md) | Accepted |
| 0009 | [Alembic is the single source of truth for schema; `create_all` removed](./0009-alembic-single-source-of-truth.md) | Accepted |
| 0053 | [Raw stores the payload as `TEXT`; ODS stores structured fields as `JSONB`](./0053-raw-text-ods-jsonb.md) | Accepted |

## Task queue

| # | Decision | Status |
|---|---|---|
| 0010 | [Celery + Redis replaces `BackgroundTasks`](./0010-celery-replaces-backgroundtasks.md) | Accepted |
| 0011 | [No result backend — `raw.status` is the source of truth](./0011-no-result-backend.md) | Accepted |
| 0012 | [`process.py` stays Celery-free to preserve the manual rescue path](./0012-process-stays-celery-free.md) | Accepted |
| 0013 | [The broker wait must be bounded](./0013-bounded-broker-wait.md) | Accepted |
| 0014 | [Dispatch is circuit-broken, and no transaction may span it](./0014-circuit-breaker-dispatch.md) | Accepted |
| 0015 | [Staleness is judged by `processing_started_at`, not `received_at`](./0015-staleness-from-processing-started-at.md) | Accepted |
| 0016 | [The recovery scan lives in Beat, not the API process](./0016-recovery-scan-in-beat.md) | Accepted |
| 0017 | [The recovery scan itself must be bounded](./0017-bounded-recovery-scan.md) | Accepted |
| 0018 | [`raw.status` carries no index at current scale](./0018-raw-status-no-index.md) | Accepted |

## Cloud extraction and warehouse

| # | Decision | Status |
|---|---|---|
| 0019 | [Batch load, not streaming](./0019-batch-load-not-streaming.md) | Accepted |
| 0020 | [Partition on `received_at` — and it means two different instants in Raw and ODS](./0020-partition-on-received-at.md) | Accepted |
| 0021 | [`require_partition_filter` as a cost fuse](./0021-require-partition-filter-fuse.md) | Accepted |
| 0022 | [`quality_events` staging deliberately diverges from `orders`](./0022-quality-events-staging-diverges.md) | Accepted |
| 0023 | [Watermark Approach A, with `get_watermark()` as the only seam](./0023-watermark-approach-a.md) | Accepted |
| 0024 | [One load job per table plus a gate; no cross-table transaction](./0024-per-table-load-job-gate.md) | Accepted |
| 0025 | [Staging is additive-only; rename and cast are pushed down to dbt](./0025-staging-additive-only.md) | Accepted |
| 0026 | [`FIELDS` is the third schema declaration, guarded by a consistency test](./0026-fields-single-source.md) | Accepted |

## Data quality

| # | Decision | Status |
|---|---|---|
| 0027 | [Blocking happens at `int_`, not at ODS](./0027-blocking-at-int-layer.md) | Accepted |
| 0028 | [The Hard Gate is scoped per batch; whole-table is a gauge](./0028-hard-gate-per-batch-scope.md) | Accepted |
| 0029 | [The Row Filter reads effective quality state, not literal `has_clean_error`](./0029-effective-quality-state.md) | Accepted |
| 0030 | [Proposal B: event-driven re-evaluation without re-running the pipeline](./0030-proposal-b-event-driven-reevaluation.md) | Accepted |
| 0031 | [Rule versioning plus an append-only `quality_events` state machine](./0031-rule-versioning-quality-events.md) | Accepted |
| 0032 | [Bounded writeback: warehouse judgements do not flow back into ODS](./0032-bounded-writeback.md) | Accepted |
| 0033 | [Historical quality metrics are never retroactively rewritten](./0033-historical-metrics-never-rewritten.md) | Accepted |
| 0034 | [The boundary between tier-1 operational and tier-2 analytical metrics](./0034-tier-1-tier-2-metrics.md) | Accepted |
| 0054 | [Type coercion is governed by the declaration, not by coercion behaviour](./0054-type-declaration-governance.md) | Accepted |

## Orchestration

| # | Decision | Status |
|---|---|---|
| 0035 | [Dependency isolation: two venvs, nothing installed into Airflow itself](./0035-two-venvs-dependency-isolation.md) | Accepted |
| 0036 | [DAG files must not import project modules at top level](./0036-dag-no-toplevel-import.md) | Accepted |
| 0037 | [`catchup=False` is structural, not a convenience](./0037-catchup-false-structural.md) | Accepted |
| 0038 | [Deliberately asymmetric retries: extract = 2, dbt = 0](./0038-asymmetric-retries.md) | Accepted |
| 0039 | [Observation signals each get their own DAG](./0039-observation-signals-own-dag.md) | Accepted |
| 0040 | [Layered dbt execution, with a full `dbt test` still running at the end](./0040-layered-dbt-execution.md) | Accepted |
| 0041 | [`profiles.yml`: structure in version control, values in the environment](./0041-profiles-yml-structure-vs-values.md) | Accepted |
| 0042 | [Failure notification states the response, not the task name; the channel is left blank](./0042-failure-notification-response-not-task.md) | Accepted |

## Transformation (dbt)

| # | Decision | Status |
|---|---|---|
| 0043 | [`stg_` builds a table, not a view](./0043-stg-table-not-view.md) | Accepted |
| 0044 | [`incremental` + `insert_overwrite` + `copy_partitions` to work around the sandbox DML ban](./0044-copy-partitions-sandbox-dml.md) | Accepted |
| 0045 | [The `int_` layer duplicates effective-state logic deliberately, rather than sharing a model](./0045-int-effective-state-duplication.md) | Accepted |
| 0046 | [`stg_` is incremental, `int_` is a full rebuild](./0046-stg-incremental-int-full-rebuild.md) | Accepted |
| 0047 | [Measures roll up into the header, guarded by an invariant test](./0047-measures-roll-up-to-header.md) | Accepted |
| 0048 | [Build only two dimensions, SCD1; the fact table carries the at-the-time snapshot](./0048-two-dimensions-scd1.md) | Accepted |
| 0049 | [Business reports always read Gold; quality reporting splits into two tables](./0049-business-reports-read-gold.md) | Accepted |
| 0055 | [An incremental window's boundary must be a partition boundary; backfills take dates, not day counts](./0055-partition-aligned-incremental-window.md) | Accepted |

## Observability

| # | Decision | Status |
|---|---|---|
| 0050 | [A resident Collector, and why `.env` avoids the OTel standard endpoint name](./0050-resident-otel-collector.md) | Accepted |
| 0051 | [Logs are not routed over OTLP](./0051-logs-not-over-otlp.md) | Accepted |
| 0052 | [SDK Views control the series budget — the expensive metrics are the automatic ones](./0052-sdk-views-series-budget.md) | Accepted |

---

## Related

- [STATUS](../STATUS.md) — what is built, and the status vocabulary
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — what is deferred, and what a real system would do
- [Design documents](../design/) — how the system works, once the decisions are made
