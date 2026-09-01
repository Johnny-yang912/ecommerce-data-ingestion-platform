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

### The three usual options

| | Kafka | RabbitMQ | Redis + Celery (chosen) |
|---|---|---|---|
| What it fundamentally is | a distributed commit log with a retention period | a message broker — real ack / nack / dead-letter | a data-structure server; queue semantics simulated on top by Celery |
| Delivery guarantee | at-least-once (exactly-once needs the transactional API) | at-least-once, **real ack** | at-least-once, **ack simulated by a visibility timeout** |
| When redelivery fires after loss | consumer group rebalance | immediate requeue on connection loss | when the visibility timeout expires (600s here) |
| Replayable history | ✅ any offset within retention | ❌ consumed is gone | ❌ |
| Dead-letter | build it yourself | ✅ native DLX | none (retries live in Celery) |
| Operational surface added | broker + partition planning + disk capacity | one service + vhost / queue declarations | **none** — Redis is already in the stack (rate-limit counters, db1) |
| What this system would actually use | dispatch | dispatch | dispatch |

The last row is the point of the table: **all three would be used here for exactly one thing.** The difference is therefore not capability but whether the extra capability is worth its cost.

**Why not Kafka.** Kafka's most expensive and most valuable property is that a retained event stream can be replayed and consumed independently by several consumer groups. Both are redundant here — a message has exactly one consumer and there is no second group; and **replay is not the queue's job, it is Raw's**: the payload is kept verbatim in Raw ([ADR-0053](./0053-raw-text-ods-jsonb.md)), and the entry points for replay are `force=true` and Proposal C, both of which start from Raw.

> If the queue also held a replayable history, the system would have **two sources of truth for what arrived** — exactly what the single-ingress invariant forbids. **Raw already is this system's commit log.**

Partition planning, disk capacity and an extra coordination service would therefore buy something this system already has and deliberately keeps only one copy of.

**Why not RabbitMQ.** Of the three it fits the shape of *dispatch* best: real acks, native dead-lettering, immediate requeue on loss, and no `visibility_timeout` — no parameter that **can be set wrong**. The reason for not choosing it is not that it is worse, but that **its advantage has already been bought here by something else**.

The weakness of `acks_late` is that a message may be processed twice. A real ack narrows that window but does not close it — no at-least-once system does. And redelivery here is **already a no-op**: the CAS claim ([ADR-0004](./0004-cas-claim-rowcount.md)) and `UNIQUE(ods.order_id)` ([ADR-0005](./0005-first-write-wins-idempotency.md)) were both built before the queue, so a redelivered message fails the CAS check and returns immediately. **The guarantee RabbitMQ would be bought for has already been paid for once, by idempotency.** What remains is one more service to operate, against a Redis that was in the stack anyway.

**The price of choosing Redis, stated.** What it buys is not the absence of a cost but a cost that is bounded and has been measured:

| Cost | Why it is acceptable |
|---|---|
| `visibility_timeout` is a parameter that can be set wrong — set it below the longest task and a still-running task gets redelivered, and the system stampedes | it is 600s, and `process_raw_event`'s worst case is three layers of exponential backoff (seconds). **This is a number to be known, not a number to be trusted** |
| Redis persistence (AOF / RDB) is weaker than the other two; a crash can lose the last few messages | the raw rows behind those messages are still sitting in `pending`, and the recovery scan picks them up — **the queue may lose messages; the database does not lose rows** |

The second row is the other face of this ADR's own conclusion: the queue and the scan are complements, so the queue's durability does not have to be absolute.

**And the decision is reversible.** For two of the three (Redis / RabbitMQ) Celery only needs a different `broker_url`; `acks_late`, prefetch and serialisation all stay as they are. **What is irreversible is having built idempotency outside the queue — and that is already done**, which is what keeps a broker change an operational decision rather than a rewrite.

## Related

- [ADR-0004](./0004-cas-claim-rowcount.md), [ADR-0005](./0005-first-write-wins-idempotency.md) — the idempotency `acks_late` relies on
- [ADR-0016](./0016-recovery-scan-in-beat.md) — the other half of making multi-process possible
- [Queue design](../design/queue.md)
