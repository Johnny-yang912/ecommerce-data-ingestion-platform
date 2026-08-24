# ADR-0010: Celery + Redis replaces `BackgroundTasks`

**English** | [繁體中文](../../zh-TW/adr/0010-celery-replaces-backgroundtasks.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue |

---

## Context

`BackgroundTasks` is an in-memory queue inside the API process. Task state is not persisted anywhere, which has two consequences that only look separate.

**Crashes lose work.** A `SIGKILL` mid-processing left 150 records in `pending` with no mechanism to reprocess them — the database rows existed, but nothing in the system knew they were owed a task. Recovery depended entirely on the periodic scan noticing them.

**The API cannot scale out.** Two pieces of in-process state pinned it to `--workers 1`: the background task pool, and the recovery scan running on an asyncio loop in FastAPI's lifespan. Any second process would duplicate the scan.

## Decision

Celery + Redis as the ingestion path's task queue, with:

- `acks_late = True` and `task_reject_on_worker_lost = True` — a message is acknowledged after the work, not before, so worker loss triggers redelivery.
- `worker_prefetch_multiplier = 1` — the standard companion to `acks_late`. Without it, a worker grabs a batch and a crash strands the whole batch until the visibility timeout expires.
- `visibility_timeout = 600` — Redis has no true ack, so redelivery is simulated by a visibility timeout. It must exceed the longest possible task, or a still-running task gets redelivered and the system stampedes. `process_raw_event`'s worst case is three layers of exponential backoff (seconds), so 600s is ample.
- JSON serialisation only, never pickle — deserialising pickle is arbitrary code execution, so a writable broker would be remote code execution.

## Consequences

**Verified: no loss under `SIGKILL`.** 800 records backlogged, worker killed after 225 were processed:

| Moment | `pending` | `processing` | `processed` |
|---|---|---|---|
| At `SIGKILL` | 537 | 2 | 261 |
| 30s after worker restart | 0 | **2** | 798 |
| One scan after the stale threshold | 0 | 0 | **800** |

Final ODS count 800, nothing lost.

**The middle row is the important one, and it overturned an assumption.** The 2 records already claimed into `processing` at `SIGKILL` **cannot be recovered by restarting the worker**. Redelivery arrives, fails the CAS check because the status is no longer `pending`, and returns immediately. Only the stale scan recovers them.

> A durable queue does not make the recovery scan redundant. It makes the scan **the complement of the queue's semantics** — the queue recovers what it still owns, and the scan recovers what was already claimed when the worker died.

**Multiple API processes become possible** — but only after the scan also moved out of the API process (ADR-0016). The queue alone was not sufficient.

**`acks_late` accepts that a message may be processed twice.** That is safe here precisely because idempotency already existed: the CAS claim (ADR-0004) and `UNIQUE(ods.order_id)` (ADR-0005) were built before the queue was.

## Alternatives considered

**Keep `BackgroundTasks` and rely on the scan.** The scan alone recovers everything eventually, but "eventually" is one scan interval, and the API stays single-process forever.

**RabbitMQ instead of Redis.** Real acks rather than a simulated visibility timeout, at the cost of another service to operate. Redis was already in the stack for rate-limit counters; the visibility-timeout semantics are well understood and bounded above.

## Related

- [ADR-0004](./0004-cas-claim-rowcount.md), [ADR-0005](./0005-first-write-wins-idempotency.md) — the idempotency `acks_late` relies on
- [ADR-0016](./0016-recovery-scan-in-beat.md) — the other half of making multi-process possible
- [Queue design](../design/queue.md)
