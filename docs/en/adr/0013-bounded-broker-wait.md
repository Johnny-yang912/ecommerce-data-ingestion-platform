# ADR-0013: The broker wait must be bounded

**English** | [繁體中文](../../zh-TW/adr/0013-bounded-broker-wait.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue |

---

## Context

`POST /orders` writes the Raw row, commits, then dispatches a task. If the dispatch is left with library defaults, the wait falls back to OS-level DNS and TCP timeouts.

Measured: after `docker compose stop redis`, a single `POST /orders` **blocked for 19 seconds** before responding.

The reason that is unacceptable is a property of this system's design rather than a general performance concern. **The broker is optional to the ingestion path.** If the dispatch fails, the Raw row is already committed as `pending`, and the recovery scan will pick it up. The request has already succeeded in every way that matters.

> An optional downstream must never be able to make the required path pay for its failure.

## Decision

Every wait in the dispatch path has an explicit bound:

| Setting | Value | Purpose |
|---|---|---|
| `socket_connect_timeout` | 2s | Cap on establishing the connection |
| `socket_timeout` | 2s | Cap on the operation itself |
| `retry_on_timeout` | `False` | A timeout is the answer, not a reason to wait again |
| `broker_connection_timeout` | 2s | Celery-level cap |
| `task_publish_retry_policy.max_retries` | 1 | One retry for a transient blip; then give up |

`_enqueue()` **swallows all exceptions** and returns a boolean. It never propagates a failure to the caller, because the Raw row is already committed — returning `500` would make the client resend and produce a batch of `duplicate` records for an order that was in fact accepted (ADR-0001, ADR-0003).

The response semantics are therefore: **`200` with status `pending`** — "accepted; processing will be delayed."

`task_publish_retry_policy` is capped at one attempt for a specific reason: **the longer the retry, the more it merely converts an already-handled failure into request latency.** There is a fallback; the point is to reach it quickly.

## Consequences

**A broker outage degrades throughput, not correctness.** Records land as `pending` and the recovery scan drains them when the broker returns.

**The same principle as `has_clean_error` being non-blocking**, applied to a different layer: *a fact that has already been established does not get rolled back because something downstream failed.* The Raw row exists; the client is told so.

**2 seconds is still not zero**, and under concurrency it compounds badly — which is what ADR-0014 exists to fix. This decision bounds the single-call cost; the circuit breaker removes it entirely under sustained failure.

**⚠️ `_enqueue()` is synchronous and can block for the full timeout.** Every caller on an async path must wrap it in `asyncio.to_thread`, or a broker outage stalls the entire event loop.

## Alternatives considered

**Leave the defaults and let the client time out.** The client cannot distinguish "your order was rejected" from "your order was accepted but the response was slow", so it resends — manufacturing duplicates for data that was already stored.

**Return `503` when the dispatch fails.** Truthful about the queue and false about the request: the order *was* accepted. A `503` invites exactly the resend that produces duplicate noise.

**Dispatch before committing Raw.** Would make the dispatch failure meaningful again — at the cost of dispatching tasks for rows that may not exist.

## Related

- [ADR-0014](./0014-circuit-breaker-dispatch.md) — what happens when the failure is sustained rather than momentary
- [ADR-0001](./0001-raw-no-business-dedup.md) — why manufactured duplicates are tolerable but not free
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — the same principle at the quality layer
- [ADR-0017](./0017-bounded-recovery-scan.md) — the fallback this decision leans on, and its own limits
