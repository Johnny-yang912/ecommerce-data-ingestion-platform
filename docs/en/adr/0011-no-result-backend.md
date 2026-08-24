# ADR-0011: No result backend — `raw.status` is the source of truth

**English** | [繁體中文](../../zh-TW/adr/0011-no-result-backend.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue |

---

## Context

Celery offers a result backend: a store where task state and return values are kept so callers can ask "did task X finish?".

This system already answers that question. Every record's fate lives in `raw.status` — `pending`, `processing`, `processed`, `error`, `duplicate` — in PostgreSQL, transactionally consistent with the ODS write that produced it, and already exposed through `GET /raw/{raw_id}`.

## Decision

`task_ignore_result = True`. No result backend is configured.

## Consequences

**One truth, not two.** A Redis result store would hold a second opinion about the same task, and the two can disagree: the DB commit succeeds and the result write fails, or the result expires while the row lives forever. When they disagree, the caller has no principled way to decide which is right.

**The authoritative answer is transactional.** `raw.status` is updated in the same commit as the ODS row (on the success path). A separate result backend is updated in a different store at a different moment, so it can never be more than eventually consistent with the thing it describes.

**No expiry policy to design.** Result backends need one — results accumulate otherwise. `raw` rows are business data with their own retention story, which already had to be decided.

**The cost: no `AsyncResult`, no `.get()`, no chords or chains keyed on results.** Nothing in this system needs them — the pipeline's coordination lives in Airflow (ADR-0035), not in Celery primitives. If a future workflow needed Celery-level task composition, this decision would need reopening.

## Alternatives considered

**Enable a result backend for observability.** Tempting, and answered by tracing instead: the `api` → Celery → `worker` span chain (ADR-0050) shows what happened to a task, without creating a second state store.

**Use the result backend as the source of truth and drop `raw.status`.** Would put the business state machine in an ephemeral cache with an expiry policy, outside the transaction that produces it. The state machine is business data, not task metadata.

## Revisit when

A workflow requires Celery-level composition (chords, chains that pass values) rather than the DB-state-machine coordination used today.

## Related

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — the queue this configures
- [ADR-0003](./0003-duplicate-terminal-status.md) — the state machine that is the truth
- [ADR-0008](./0008-config-boundary.md) — the same "do not create a second truth" reasoning, applied to config
