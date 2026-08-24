# ADR-0014: Dispatch is circuit-broken, and no transaction may span it

**English** | [繁體中文](../../zh-TW/adr/0014-circuit-breaker-dispatch.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue |

---

## Context

ADR-0013 bounded a single dispatch at ~2 seconds. Under concurrency that bound is not enough, and the reason is not obvious.

Measured with Redis fully stopped, 4 uvicorn workers, `POST /orders`:

| Concurrency | Result |
|---|---|
| 1 | 3.8s |
| 8 | 12.8s each |
| 48 | **47 of 48 did not complete within 120s** |

**The degradation is super-linear**, not "a fixed 3.8s added per request". kombu's producer pool is capped at 10 per process; when the broker is unavailable, every acquisition pays the connection timeout again, and higher concurrency means longer queueing behind each other.

The consequence was worse than slowness. Those 47 requests **had already written their Raw rows**. The client saw only a timeout, concluded failure, and resent — manufacturing duplicates for orders that were already stored.

A second defect surfaced in the same investigation: `db.refresh()` opened a transaction that **spanned the dispatch call**. Measured at 60 concurrent: 23 of 32 connection-pool slots stuck `idle in transaction`, held open for the duration of a network call to a dead service.

## Decision

**A circuit breaker around the dispatch.** Three consecutive failures open the circuit; while open, Redis is not touched at all and `_enqueue` returns `False` immediately. Measured p50 under outage: **timeout → 5ms**.

**No database transaction may span the dispatch.** The `db.refresh()` is completed and its transaction closed before `_enqueue` is called.

The governing principle:

> The cost of entering the fallback must be lower than the cost of the fallback itself. Otherwise the system prefers to hang rather than degrade — and that is not degradation, it is just failure with extra steps.

### Breaker design choices

**State is deliberately per-process.** Sharing it would require Redis, and Redis is the thing that is down. The cost is that each process learns independently: the cluster pays at most `failure_threshold × process_count` slow calls before all circuits are open.

**`half_open` is single-flight.** After the cooldown, exactly *one* call probes; the rest continue to be rejected fast. Letting every thread probe at once means every thread pays a full timeout — which is the same as having no breaker.

**The lock guards state transitions only, never the wrapped call.** Hold time is microseconds. Putting a network call inside the lock would make the breaker itself the serialisation bottleneck — precisely the problem it exists to remove.

**Timing uses `time.monotonic()`**, so cooldown is unaffected by NTP steps or DST.

## Consequences

**Ingestion stays at full speed during a broker outage**, which is the intent — and has a second-order effect that had to be handled separately: `pending` then accumulates at full speed too, which is what forced ADR-0017.

**Log volume collapses during an incident.** State transitions are logged once each, instead of one traceback per failed request (measured in the thousands per second at the tested scale). The signal stops drowning in itself.

**The breaker stays closed under low traffic**, since three *consecutive* failures are needed. That is acceptable: at low traffic the per-request cost is bounded by ADR-0013 and there is no queueing amplification.

## Alternatives considered

**Rely on the 2s timeout alone.** Insufficient, per the measurements above — the pool contention is what produces the super-linear curve, and a per-call timeout does not address it.

**Shared breaker state in Redis.** Would open all circuits at once, at the cost of depending on the failed component to know that it failed.

**Raise the kombu pool limit.** Treats the symptom. More pool slots means more concurrent connection attempts to a dead service.

## Related

- [ADR-0013](./0013-bounded-broker-wait.md) — the per-call bound this builds on
- [ADR-0017](./0017-bounded-recovery-scan.md) — the load this decision relocates onto the recovery path
- [Queue design](../design/queue.md) — the full before/after measurements
