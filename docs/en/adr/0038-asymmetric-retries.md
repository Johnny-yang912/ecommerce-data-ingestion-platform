# ADR-0038: Deliberately asymmetric retries — extract = 2, dbt = 0

**English** | [繁體中文](../../zh-TW/adr/0038-asymmetric-retries.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

A uniform retry policy across a DAG looks tidy and is wrong here, because the two kinds of task fail for opposite reasons.

## Decision

| Task | `retries` | Why |
|---|---|---|
| `extract_*` | **2**, exponential backoff | Failures are mostly **transient** — PostgreSQL connectivity, BigQuery 5xx, `rateLimitExceeded` |
| `dbt_*` | **0** | Failures are mostly **deterministic** — bad SQL, a red test, the Hard Gate firing |

A retry on a deterministic failure re-runs something guaranteed to fail again. It costs time, produces duplicate noise in the logs, and — worst — **delays the moment a human looks at it**.

This is the same principle as the NUL poison pill (ADR-0006), stated at a different layer:

> **Treating a deterministic error as transient and retrying it is how you manufacture a poison pill.** There the fix was an `except ValueError` fast-fail; here it is `retries=0`.

**BigQuery's genuinely transient errors are handled at the adapter layer instead**, by `job_retries: 1` in `profiles.yml`. That is far more precise than an Airflow task retry: it retries *the failed BigQuery job*, whereas an Airflow retry re-runs the entire `dbt build` — including every model that already succeeded.

> Retry at the layer that knows what actually failed.

## Consequences

**A red `dbt_*` task is immediately meaningful.** It has not been retried, so it means what it says: something is deterministically wrong, look at it.

**Extract's transient failures self-heal without waking anyone**, and they compose with the per-table watermark: a failed extract does not advance its watermark, so even a retry that fails twice is recovered by the next day's run re-selecting with `>=` (ADR-0023, ADR-0024).

**The Hard Gate's interception is not masked.** If the gate fires, the run stops and stays stopped — retrying it would re-run the gate against the same data and fail identically, while making the incident look intermittent.

**The cost is that a genuinely transient dbt failure — a BigQuery outage mid-build — requires a manual re-run.** Accepted: that class of failure is rare, and the adapter-level `job_retries` already covers its most common form.

## Alternatives considered

**Uniform `retries=2` everywhere.** Turns every deterministic failure into three identical failures, tripling log noise and delaying diagnosis.

**Uniform `retries=0` everywhere.** Would page a human for a transient PostgreSQL blip that would have resolved itself in thirty seconds.

**Airflow-level retry for BigQuery errors instead of `job_retries`.** Re-runs the whole `dbt build` to recover from one failed job. Strictly worse for the same outcome.

## Related

- [ADR-0006](./0006-nul-byte-fast-fail.md) — the same principle at the ingestion layer
- [ADR-0024](./0024-per-table-load-job-gate.md) — the retry granularity this operates on
- [ADR-0040](./0040-layered-dbt-execution.md) — why re-running a whole `dbt build` is expensive
