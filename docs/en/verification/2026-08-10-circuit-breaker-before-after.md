# 2026-08-10 — Dispatch degradation with the broker down

**English** | [繁體中文](../../zh-TW/verification/2026-08-10-circuit-breaker-before-after.md)

---

## What was being verified

Each dispatch was already bounded at ~2 seconds (ADR-0013). **Is a per-call bound sufficient under concurrency?**

## Environment

Redis **fully stopped**. api running 4 uvicorn workers. `POST /orders` at varying concurrency. 2026-08-10.

## Observed — before the circuit breaker

| Concurrency | Result |
|---|---|
| 1 | 3.8s |
| 8 | 12.8s each |
| 48 | **47 of 48 did not complete within 120s** |

**The degradation is super-linear**, not a fixed 3.8s added per request.

The cause: **kombu's producer pool is capped at 10 per process.** When the broker is unavailable, every acquisition pays the connection timeout again, and higher concurrency means longer queueing behind each other.

### The consequence was worse than slowness

Those 47 requests **had already written their Raw rows.** The client saw only a timeout, concluded failure, and resent — **manufacturing duplicates for orders that were already stored.**

### A second defect found in the same investigation

`db.refresh()` opened a transaction that **spanned the dispatch call**. Measured at 60 concurrent:

> **23 of 32 connection-pool slots stuck `idle in transaction`** — held open for the duration of a network call to a dead service.

## Observed — after

| Metric | Before | After |
|---|---|---|
| p50 under outage | timeout | **5ms** |
| Log volume | one traceback per failed request (thousands/s at the tested scale) | one line per state transition |

Three consecutive failures open the circuit; while open, Redis is not touched at all.

## Conclusion

A per-call bound is **not** sufficient. The bound addresses one call; pool contention is what produces the super-linear curve, and no per-call timeout addresses it.

The governing principle:

> **The cost of entering the fallback must be lower than the cost of the fallback itself.** Otherwise the system prefers to hang rather than degrade — and that is not degradation, it is failure with extra steps.

The log-volume result matters more than it looks. During an incident, one traceback per failed request means **the signal drowns in itself** at exactly the moment someone is trying to read it.

## What this overturned

Nothing previously written — but it corrected an intuition. Raising kombu's pool limit looks like the fix and is not: **more pool slots means more concurrent connection attempts to a dead service.**

## Related

- [ADR-0014](../adr/0014-circuit-breaker-dispatch.md) — the resulting decision
- [ADR-0013](../adr/0013-bounded-broker-wait.md) — the per-call bound this builds on
- [2026-08-10-bounded-scan-120k](./2026-08-10-bounded-scan-120k.md) — the load this decision relocated
