# 2026-08-10 — SIGKILL recovery under Celery + Redis

**English** | [繁體中文](../../zh-TW/verification/2026-08-10-celery-sigkill-recovery.md)

---

## What was being verified

After replacing `BackgroundTasks` with a durable queue, **is crash recovery complete?** And specifically: **does the queue make the recovery scan redundant?**

## Environment

Full docker compose stack — api / worker×4 / beat / redis / postgres — with `SCAN_INTERVAL_SECONDS=20` to shorten the observation window. 2026-08-10.

## Method

800 records injected as `pending`. Once Beat had dispatched them and the worker had processed 225:

```bash
docker compose kill -s SIGKILL worker
```

Then: restart the worker, wait, observe. The third row backdates `processing_started_at` by 11 minutes to simulate crossing the 10-minute stale threshold rather than idling through it.

## Observed

| Moment | `pending` | `processing` | `processed` |
|---|---|---|---|
| At SIGKILL | 537 | 2 | 261 |
| 30s after worker restart | 0 | **2** | 798 |
| After one scan past the stale threshold | 0 | 0 | **800** |

Final ODS count: **800. Nothing lost.**

Two supporting observations from the same session:

- **Beat's startup catch-up works.** Beat started at `05:58:38`; the scan dispatched by `beat_init` reached the worker at `05:58:39`, while the first scheduled tick came at `05:58:58` (+20s). The catch-up does close the first interval's gap.
- **Ingestion survives a broker outage.** With Redis down, `POST /orders` returned HTTP 200 + `pending` (3.81s — this measurement predates the circuit breaker) and the data landed. `/health` answered in 1.7ms. When Redis came back, the stranded records were picked up by the scan and completed.

## Conclusion

Recovery is complete — **and the middle row is why this record exists.**

The 537 records still in the queue drained themselves via redelivery. The **2 that were mid-processing at SIGKILL could not be saved by restarting the worker**: redelivery arrived, failed the CAS check because the status was no longer `pending`, and returned immediately. Only the stale scan recovered them.

> **A durable queue does not make the recovery scan redundant. It makes the scan the complement of the queue's semantics** — the queue recovers what it still owns; the scan recovers what was already claimed when the worker died.

## What this overturned ⭐

README stress test #5 previously concluded: *"150 records stuck in `pending`, no automatic recovery after restart."*

That was true of `BackgroundTasks`, whose in-memory queue loses task state on death. It is no longer true — **but the fix was not "add a durable queue"**, which alone would still have left those 2 records stuck. It took the queue *and* the scan, each covering what the other cannot.

## Related

- [ADR-0010](../adr/0010-celery-replaces-backgroundtasks.md) — the queue this verifies
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) — why redelivery cannot save a claimed record
- [design/queue](../design/queue.md)
