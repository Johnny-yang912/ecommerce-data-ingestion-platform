# ADR-0015: Staleness is judged by `processing_started_at`, not `received_at`

**English** | [繁體中文](../../zh-TW/adr/0015-staleness-from-processing-started-at.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue — recovery |

---

## Context

The recovery scan resets records stuck in `processing` back to `pending`, on the assumption that the worker died. It originally judged "stuck" using `received_at`, because that column already existed.

**`received_at` answers the wrong question.** The scan needs to know *how long this attempt has been running*. `received_at` says *how long this data has been lying around*. Under normal conditions the two are nearly equal — records are processed on arrival. Under backlog they diverge enormously — **and backlog is exactly when this check fires most often.**

The failure timeline, reproduced:

```
T-30min  Order ingested; received_at = T-30min.
         Broker is down, so it stays pending.
T+0      Broker recovers, scan dispatches. Worker A claims it → processing.
T+0.01   Worker A is cleaning and building the ODS row (not yet committed).
T+0.02   Next scan: status='processing' ✓ and received_at < now()-10min ✓
         → judged stale → reset to pending → a second message dispatched.
T+0.03   Worker B claims it: the status is now pending, so CAS succeeds.
         ← Nothing stops this. Two workers are now running the same raw_id.
T+0.05   A commits first: ODS row lands, raw.status = 'processed'.
T+0.06   B collides with the ODS row that "it" just wrote, is judged
         duplicate, and overwrites processed.
```

**Note that CAS did not fail.** It guarantees only one transition *out of* `pending`. It cannot prevent a third party transitioning the record back *into* `pending` mid-flight (ADR-0004).

The data was never corrupted — `UNIQUE(ods.order_id)` held. What broke was **the signal**: an order that had in fact succeeded ended up flagged `duplicate`, polluting the deliberately-preserved meaning of that status (ADR-0003). Reproduced twice across a 2,000-record backlog.

## Decision

Add `raw.processing_started_at`, stamped by `try_claim_raw` at the moment the claim succeeds. Staleness is measured from it:

```sql
WHERE status = 'processing' AND processing_started_at < now() - :stale_threshold
```

**Invariant:** `status = 'processing'` ⇒ `processing_started_at IS NOT NULL`. Guaranteed because `try_claim_raw` is the only path into `processing` (ADR-0004); established for existing rows by migration `e5f6a7b8c9d0`.

## The two thresholds ask two different questions

Both live in `recovery_policy.py`, on deliberately different bases:

| Constant | Basis | Question |
|---|---|---|
| `STALE_PROCESSING_MINUTES` (10) | `processing_started_at` | How long has **this attempt** been running? |
| `PENDING_GRACE_SECONDS` (60) | `received_at` | How long has **this data** been lying around? |

`PENDING_GRACE_SECONDS` correctly uses `received_at`: freshly-ingested `pending` rows are left to the ingestion path, because the fast path normally dispatches them within milliseconds and a scan intervening only sends a redundant message. Only a row still `pending` after the grace period means the fast path actually missed.

**Same module, different bases, and mixing them up is the bug this ADR is about.**

## Consequences

**Self-collision becomes unreachable, not merely unlikely.** Timing now starts at claim, so the `T+0.02` step cannot happen — the record has not been in `processing` for 10 minutes, whatever `received_at` says.

**The `duplicate` signal recovers its meaning**: it once again indicates upstream behaviour rather than this system's own.

**The cost is one column and one migration with a backfill.**

## Alternatives considered

**Raise the stale threshold.** Widens the window without closing it, and slows genuine crash recovery in exchange.

**Have the scan skip recently-dispatched records.** Requires tracking dispatch time somewhere — which is `processing_started_at` by another name, but held outside the transaction that sets the status.

## Related

- [ADR-0004](./0004-cas-claim-rowcount.md) — the guarantee whose boundary this defect found
- [ADR-0003](./0003-duplicate-terminal-status.md) — the signal that was being polluted
- [ADR-0017](./0017-bounded-recovery-scan.md) — the other correction to the same scan
