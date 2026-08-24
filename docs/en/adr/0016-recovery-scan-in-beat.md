# ADR-0016: The recovery scan lives in Beat, not the API process

**English** | [繁體中文](../../zh-TW/adr/0016-recovery-scan-in-beat.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue — recovery |

---

## Context

The recovery scan originally ran on an asyncio loop started in FastAPI's `lifespan`. It worked, and it was **in-process state** — which meant the API was pinned to `--workers 1`. A second uvicorn process would have run a second scan loop, doubling the dispatch of every stuck record.

Moving to Celery (ADR-0010) removed one of the two things pinning the API to a single process. This is the other one.

## Decision

The scan is a Celery Beat schedule entry:

```python
beat_schedule = {
    "recovery-scan": {
        "task": "tasks.scan_and_dispatch",
        "schedule": float(settings.scan_interval_seconds),   # 300s
    },
}
```

**Beat also fires one catch-up scan at startup**, which closes a gap the interval alone leaves: without it, a restart means the first scan happens one full interval later, and anything stuck during the restart waits that long.

**⚠️ Beat must never be `--scale`d.** Two beat processes dispatch two of every scheduled scan. The API can scale, the worker can scale; beat is the singleton. `docker-compose.yml` runs all three from one image, differing only in the start command — which makes this constraint easy to violate by accident and worth stating loudly.

## Consequences

**The API process becomes stateless with respect to background work**, which is what finally allows `UVICORN_WORKERS > 1`.

**That immediately exposed a second piece of per-process state**: slowapi keeps rate-limit counters in process memory. Across N processes, `60/minute` silently becomes `60 × N` — measured, 4 workers let **91 of 100** requests through instead of 60, **with no error raised anywhere**. The counters had to move to Redis (db 1, separate from the broker's db 0) in the same change.

> Removing one piece of in-process state does not make a process stateless. It makes the *next* piece of state visible — and that one was silent.

**Scan timing is now decoupled from API deployments.** Restarting the API no longer restarts the scan clock.

**The cost is a third long-lived process** to run and monitor.

## Alternatives considered

**Leader election among API processes.** Would keep the scan in the API and allow scaling, at the cost of a coordination mechanism — and coordination that depends on Redis, which is the component whose failure the scan exists to recover from.

**A cron job outside the application.** Fewer moving parts inside the app, at the cost of a second scheduling mechanism alongside Airflow and Beat, and a deployment story that lives outside compose.

**Run the scan on every worker.** Every worker duplicating the dispatch — CAS makes it safe (ADR-0004) but it wastes worker slots proportional to worker count, precisely when workers are scarce.

## Related

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — the other half of removing in-process state
- [ADR-0017](./0017-bounded-recovery-scan.md) — what this scan had to become under load
- [ADR-0008](./0008-config-boundary.md) — why `scan_interval_seconds` is config but the thresholds are not
