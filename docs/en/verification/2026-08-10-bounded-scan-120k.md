# 2026-08-10 — Bounded recovery scan against a large backlog

**English** | [繁體中文](../../zh-TW/verification/2026-08-10-bounded-scan-120k.md)

---

## What was being verified

The circuit breaker keeps ingestion at full speed during a broker outage — so `pending` accumulates at full speed too. **Do the scan's bounds hold at scale, and does the cursor actually resume correctly?**

## Environment

Same compose stack, worker concurrency 4, **Beat disabled** so each scan run could be observed individually. 2026-08-10.

## Observed

### Throughput

60,000-record backlog, cleared in one run:

| Metric | Rate |
|---|---|
| Publishing | ~**2,140 msg/s** |
| Worker consumption | ~**305 records/s** |

All 60,000 ended `processed` with 60,000 ODS rows — reconciled exactly.

### Per-run cap and cursor resumption

120,000 backlog against a 100,000 per-run cap:

| | Dispatched | Remaining `pending` |
|---|---|---|
| Scan #1 | **100,000** (raised the "backlog not cleared" warning) | 20,000 |
| Scan #2 | **20,000** | 0 |

ODS ended up **exactly +120,000 with zero `duplicate` rows** — proving the cursor neither skipped records nor re-dispatched already-processed ones.

### Overlap protection

Two scan messages fired almost simultaneously against the same backlog. The second logged *"recovery scan skipped: previous round still running"* and dispatched nothing.

### Grace period

50 records with `received_at = now()` and 30 from five minutes ago, inserted together. The scan returned **only the 30** — the fresh ones are left to the ingestion fast path.

## Conclusion

All five bounds hold. The cursor is the non-obvious one: **`LIMIT` alone would re-fetch the same leading rows forever**, because dispatching does not change `status`. The `id` cursor is what makes pagination actually progress.

## What this overturned ⭐

**Batch publishing was assumed to be where the win came from. It is not.**

| Approach | Rate |
|---|---|
| `.delay()` | 2,332 msg/s |
| Shared producer | 2,563 msg/s |

**A mere 1.1×.** Celery's `.delay()` already reuses the producer pool, so acquisition is far cheaper than assumed.

The change stays — it costs nothing — but this record exists to say plainly that **it is not where the benefit comes from.** Pagination, the per-run cap and overlap protection are.

> An optimisation that was measured and found to be 1.1× is worth recording precisely because the next reader would otherwise assume it was load-bearing.

## Still open

**`raw.status` has no index.** Pagination bounds memory and dispatch volume — it does not bound how much the database scans to find the rows. See [ADR-0018](../adr/0018-raw-status-no-index.md).

## Related

- [ADR-0017](../adr/0017-bounded-recovery-scan.md) — the resulting decision
- [ADR-0014](../adr/0014-circuit-breaker-dispatch.md) — why this load arrives at all
- [design/queue](../design/queue.md)
