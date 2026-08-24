# 2026-08-10 — Staleness basis: `received_at` vs `processing_started_at`

**English** | [繁體中文](../../zh-TW/verification/2026-08-10-staleness-basis-self-collision.md)

---

## What was being verified

The recovery scan judged "stuck in `processing`" using `received_at`. **Does that basis cause the system to collide with itself under backlog?**

The hypothesis: `received_at` answers *"how long has this data been lying around"*, while the scan needs *"how long has this attempt been running"*. Under backlog the two diverge enormously — **and backlog is exactly when this check fires most often.**

## Environment

Same compose stack, worker×4, with `SCAN_INTERVAL_SECONDS=5` to tighten the scan cadence. 2026-08-10.

## Method

2,000 records inserted as `pending` with `received_at = now() - 30 minutes`, simulating the backlog left by a long broker outage. The same script over the same data; **only the basis column differs.**

Self-collisions are identified by `raw.status='duplicate'` ⋈ `ods.raw_id = raw.id` — a record marked duplicate against an ODS row that it wrote itself.

## Observed

| Basis | `processed` | `duplicate` | Self-collisions |
|---|---|---|---|
| `received_at` (before) | 1998 | **2** | **2** |
| `processing_started_at` (after) | **2000** | 0 | **0** |

The two offenders' `error_message` literally read `already written by raw_id=1998` — **where 1998 is their own id.** That `order_id` appears exactly once in `raw`, ruling out an upstream resend.

### The mechanism

```
T-30min  Ingested; received_at = T-30min. Broker down, stays pending.
T+0      Broker recovers; scan dispatches. Worker A claims it → processing.
T+0.01   Worker A is cleaning and building the ODS row (not yet committed).
T+0.02   Next scan: status='processing' ✓ AND received_at < now()-10min ✓
         → judged stale → reset to pending → a second message dispatched.
T+0.03   Worker B claims it: status is now pending, so CAS succeeds. ← nothing stops this
T+0.05   A commits: ODS row lands, raw.status = 'processed'.
T+0.06   B collides with the ODS row it just wrote, is judged duplicate,
         and overwrites processed.
```

## On magnitude

Records hit per scan tick ≈ records concurrently in `processing` ≈ worker concurrency, **independent of total backlog size** (a single record takes ~40ms, far below the scan interval).

So this is **"rare but real"**, not wholesale contamination — and it specifically strikes while the system is catching up, which is the worst possible time for a monitoring signal to be corrupted.

## Conclusion

The basis was wrong. Switching to `processing_started_at` makes self-collision **unreachable, not merely unlikely**: timing starts at the claim, so the `T+0.02` step cannot happen regardless of what `received_at` says.

The SIGKILL scenario was then re-run to confirm the recovery mechanism itself was not broken by the change: the 2 records stuck in `processing` were reclaimed as before, ending with all 2,900 records `processed`, 2,900 ODS rows, and **0 self-collisions**.

## What this overturned ⭐

**CAS was assumed sufficient for mutual exclusion.** It is not — and the boundary is narrower than it looks:

> CAS guarantees *"only one transition **out of** this state"*. It guarantees nothing about **who else may transition into it.**

The data was never corrupted — `UNIQUE(ods.order_id)` held throughout. What broke was **the signal**: an order that had in fact succeeded ended up flagged `duplicate`, polluting a status whose entire purpose is to distinguish "the upstream sent twice" from "this system failed".

**That signal's usefulness is what made the defect worth fixing.** Had `duplicate` been folded into `error`, the corruption would have been invisible.

## Related

- [ADR-0015](../adr/0015-staleness-from-processing-started-at.md) — the resulting decision
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) — the guarantee whose boundary this found
- [ADR-0003](../adr/0003-duplicate-terminal-status.md) — the signal being polluted
