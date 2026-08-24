# ADR-0004: CAS claim via `rowcount == 1`, with no external queue

**English** | [繁體中文](../../zh-TW/adr/0004-cas-claim-rowcount.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Ingestion — processing |

---

## Context

More than one worker can be handed the same `raw_id`. There are at least three routes to it: the recovery scan re-dispatching a record that is already queued, the broker redelivering after `acks_late`, and a manual `POST /process_raw/{id}`. Without mutual exclusion, two workers would each write an ODS row for the same order.

The reflex is to reach for a lock — an advisory lock, a Redis lock, or a queue that promises exactly-once delivery. All three add an external dependency to guarantee something the database row can already guarantee by itself.

## Decision

Claiming is a single conditional `UPDATE`:

```sql
UPDATE raw
   SET status = 'processing', processing_started_at = now()
 WHERE id = :raw_id AND status = 'pending'
```

The claim succeeds if and only if `rowcount == 1`. PostgreSQL row-locks the `UPDATE`, so among N concurrent attempts exactly one observes `pending` and transitions it; the other N−1 get `rowcount == 0` and return immediately without touching anything.

**This is also the only path into `processing`**, which is what makes the invariant `status='processing' ⇒ processing_started_at IS NOT NULL` hold (ADR-0015 depends on it).

## Consequences

**No lock service, no exactly-once queue.** The state column that already had to exist does the work.

**Verified under real concurrency**: 100 workers competing for one `raw_id` produced `raw.status = processed` and an ODS count of exactly 1.

**The losers are cheap.** A failed claim is one round trip with no transaction held open, so re-dispatch is wasteful but not dangerous — which is what lets the recovery scan be deliberately imprecise (ADR-0017).

**The boundary matters, and it is narrow.** CAS protects against concurrent contenders *in the same state*. It does **not** protect against a third party reverting the state to `pending` while a worker is mid-flight — the CAS then legitimately succeeds for a second worker. That is not a hypothetical: it is exactly the failure mode ADR-0015 exists to close.

> CAS guarantees "only one transition out of this state". It guarantees nothing about who else may transition *into* it.

## Alternatives considered

**`SELECT ... FOR UPDATE` then `UPDATE`.** Two round trips and a transaction held open across them, for a guarantee the single conditional `UPDATE` already provides atomically.

**Advisory lock or Redis lock.** Introduces a second source of truth for "who owns this record", which can disagree with `raw.status`. When they disagree, the wrong one is authoritative.

**A queue with exactly-once semantics.** The Redis broker does not offer it, and a broker that did would be a heavier operational dependency than this system's scale justifies — see the note in [CLAUDE.md](../../../CLAUDE.md) that this is a deliberate constraint at current scale.

## Revisit when

Scale requires a broker whose semantics change the picture, or the claim becomes a measured contention point.

## Related

- [ADR-0015](./0015-staleness-from-processing-started-at.md) — the boundary of this guarantee, and the defect that found it
- [ADR-0005](./0005-first-write-wins-idempotency.md) — the other half: CAS protects the claim, `UNIQUE` protects the write
- [ADR-0017](./0017-bounded-recovery-scan.md) — why cheap losers matter
