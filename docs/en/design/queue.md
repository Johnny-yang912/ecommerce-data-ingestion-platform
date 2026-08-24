# Task Queue: Celery + Redis

**English** | [繁體中文](../../zh-TW/design/queue.md)

The dispatch path between Raw and ODS, and what happens to it under failure.

---

## 1. Scope, and the boundary with Airflow

Two schedulers exist in this system and they are **orthogonal**:

| | Celery + Redis | Airflow |
|---|---|---|
| Granularity | one record | one batch |
| Latency | milliseconds–seconds | minutes–hours |
| Triggered by | an HTTP request | the clock |
| Owns | Raw → ODS | ODS → BigQuery → dbt |

They **deliberately do not share a Redis instance**, so their failure domains stay separate.

---

## 2. Configuration

| Setting | Value | Why |
|---|---|---|
| `task_serializer` / `accept_content` | `json` only | pickle deserialisation is arbitrary code execution — a writable broker would be RCE |
| `task_ignore_result` | `True` | `raw.status` is the source of truth ([ADR-0011](../adr/0011-no-result-backend.md)) |
| `task_acks_late` | `True` | acknowledge after the work, not before |
| `task_reject_on_worker_lost` | `True` | worker loss → redelivery |
| `worker_prefetch_multiplier` | `1` | the standard companion to `acks_late` |
| `visibility_timeout` | `600` | Redis has no true ack; must exceed the longest task |
| `socket_connect_timeout` / `socket_timeout` | `2` / `2` | bounded broker wait ([ADR-0013](../adr/0013-bounded-broker-wait.md)) |
| `task_publish_retry_policy.max_retries` | `1` | one retry for a blip, then fall through to the fallback |

`tasks.py` is a **thin wrapper**: no `autoretry_for`, no `max_retries`. `process.py` already has four retry layers, and `process_raw_event` never raises — every failure is already recorded in `raw.status`. [ADR-0012](../adr/0012-process-stays-celery-free.md)

---

## 3. Dispatch path and its degradation

```
Raw committed
    ↓
_enqueue(raw_id)
    ├── circuit CLOSED → publish (bounded at ~2s)
    └── circuit OPEN   → return False immediately, Redis untouched
    ↓
either way: 200 pending
```

**Nothing may hold a DB transaction across the dispatch.** `db.refresh()` used to, and at 60 concurrent that left 23 of 32 pool slots `idle in transaction`, held open for a network call to a dead service.

### Circuit breaker

Three consecutive failures open the circuit. Measured p50 under a broker outage: **timeout → 5ms**.

- **State is per-process** — sharing it would need Redis, which is the thing that is down. Cost: the cluster pays at most `threshold × process_count` slow calls.
- **`half_open` is single-flight** — one probe, everyone else still rejected fast.
- **The lock guards state transitions only**, never the wrapped call.
- **`time.monotonic()`** — cooldown unaffected by NTP or DST.

[ADR-0014](../adr/0014-circuit-breaker-dispatch.md)

---

## 4. How CAS claim interacts with redelivery

`acks_late` means a message can be delivered twice. That is safe because idempotency predates the queue — but the split of responsibility is worth stating precisely:

| Worker died | `raw.status` | Redelivery |
|---|---|---|
| **before** claim commit | still `pending` | CAS succeeds — recovered in seconds |
| **after** claim commit | already `processing` | **CAS fails, task returns immediately** |

The second row is the half the queue cannot recover. Only the stale scan does.

> A durable queue does not make the recovery scan redundant. It makes the scan **the complement of the queue's semantics**.

---

## 5. The recovery scan

Runs on Celery Beat every `scan_interval_seconds` (300s), plus one catch-up scan at startup. **Beat is a singleton and must never be `--scale`d.**

It handles two conditions, on deliberately different bases:

| Condition | Basis | Threshold | Action |
|---|---|---|---|
| stuck in `processing` | `processing_started_at` | `STALE_PROCESSING_MINUTES = 10` | reset to `pending` |
| stuck in `pending` | `received_at` | `PENDING_GRACE_SECONDS = 60` | re-dispatch |

Both thresholds live in `recovery_policy.py` — a module with **zero third-party dependencies**, so a read-only probe can import them without inheriting the write path's dependency tree ([ADR-0039](../adr/0039-observation-signals-own-dag.md)).

### Bounds

The circuit breaker keeps ingestion at full speed during an outage, so `pending` accumulates at full speed too. Five bounds:

| Bound | Value | Closes |
|---|---|---|
| Page size | `SCAN_BATCH_SIZE = 5000` | unbounded memory |
| Cursor | `WHERE id > :last_id ORDER BY id` | **`LIMIT` alone re-fetches forever** — dispatching does not change `status` |
| Rounds per run | `SCAN_MAX_ROUNDS = 20` | one task monopolising a worker slot |
| Redis lock | key + 300s TTL, Lua compare-and-delete | two scans overlapping |
| Grace period | 60s | competing with the ingestion fast path |

The scan is **deliberately imprecise** — it may re-dispatch something already queued. CAS makes the loser return immediately, so the cost is a wasted slot, never a double write.

⚠️ One bound is still open: **`raw.status` has no index**, so pagination bounds what is loaded, not what the database scans. [ADR-0018](../adr/0018-raw-status-no-index.md)

---

## 6. Rate-limit counters

slowapi keeps counters in process memory by default. Across N uvicorn workers `60/minute` silently becomes `60 × N` — measured, 4 workers let **91 of 100** requests through instead of 60, **with no error raised anywhere**.

Counters therefore live in **Redis db 1** (the broker uses db 0 — a `celery purge` must not touch rate limiting). If Redis is unavailable it degrades to per-process counting rather than disabling limiting entirely.

---

## 7. Related

- [ADR-0010](../adr/0010-celery-replaces-backgroundtasks.md) · [ADR-0016](../adr/0016-recovery-scan-in-beat.md) · [ADR-0017](../adr/0017-bounded-recovery-scan.md)
- [ingestion](./ingestion.md) — what happens after a task is claimed
- Runbook: `queue-ops` (stage 4)
