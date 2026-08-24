# Runbook: Queue operations

**English** | [繁體中文](../../zh-TW/runbooks/queue-ops.md)

---

## Inspect

```bash
# Queue backlog
docker compose exec redis redis-cli llen celery

# Current status distribution — the truth lives here, not in Redis
docker compose exec db psql -U app -d orders -c \
  "select status, count(*) from raw group by status order by status;"

# Oldest pending row — this is what raw_pending_watch checks
docker compose exec db psql -U app -d orders -c \
  "select min(received_at), count(*) from raw where status='pending';"

# Is the circuit breaker open?  (state transitions are logged once each)
docker compose logs api | grep circuit_breaker | tail -20
```

---

## Act

```bash
# Trigger a recovery scan by hand, without waiting for Beat
docker compose exec worker celery -A celery_app call tasks.scan_and_dispatch

# Reprocess a single record — the rescue path when the broker is down.
# No queue involved; this is why process.py stays Celery-free (ADR-0012).
docker compose exec worker python -c \
  "from process import process_raw_event; process_raw_event(123)"

# Shorten the scan interval to observe behaviour (default 300s)
SCAN_INTERVAL_SECONDS=20 docker compose up -d
```

---

## ⚠️ Records stuck in `processing`

**Do not edit `status` by hand.** Let the stale scan handle it after 10 minutes — that is exactly what it is for.

If immediate recovery is genuinely needed, backdate that record's **`processing_started_at`** beyond `STALE_PROCESSING_MINUTES` and wait one scan. Semantically that is *"declaring this attempt timed out"*, which is a statement the system already knows how to act on.

```sql
update raw
   set processing_started_at = now() - interval '20 minutes'
 where id = <raw_id> and status = 'processing';
```

> ⚠️ **Do not touch `received_at`.** It is the ingestion timestamp, part of the data's lineage, and has nothing to do with the timeout decision. The two columns answer different questions ([ADR-0015](../adr/0015-staleness-from-processing-started-at.md)).

---

## Records stuck in `pending`

Usually means the dispatch path failed and the scan has not caught up yet — which is **normal degraded behaviour**, not a fault, if the broker is down.

| First check | Then |
|---|---|
| Is redis up? | `docker compose ps redis` |
| Is the circuit open? | grep the logs above — if open, ingestion is deliberately not touching Redis |
| Is beat alive? | `docker compose logs beat \| tail` — beat must be a **singleton**, never `--scale`d |
| Is the worker consuming? | `docker compose exec redis redis-cli llen celery` — a growing queue with a live worker is a different problem |

Once the broker recovers, the scan drains the backlog on its own. Verified against 120,000 rows: two scans, ODS grew by exactly 120,000, zero duplicates.

---

## Broker down: what to expect

This is a designed degradation, not an outage:

| | Behaviour |
|---|---|
| `POST /orders` | still returns **`200 pending`** — the Raw row is committed |
| Dispatch | circuit opens after 3 consecutive failures; p50 drops to ~5ms |
| Log volume | one line per state transition, **not** one traceback per request |
| Recovery | the scan drains `pending` once the broker returns |

**Do not "fix" this by returning 500.** The client cannot distinguish rejection from a slow response, so it resends — manufacturing duplicates for orders that were in fact accepted ([ADR-0013](../adr/0013-bounded-broker-wait.md)).

---

## Beat

```bash
# Beat is a SINGLETON. Two beat processes dispatch duplicate scans.
docker compose ps beat        # must show exactly 1
```

Beat also fires one catch-up scan at startup, so a restart does not leave a full interval's gap.

---

## Related

- [design/queue](../design/queue.md) — how the scan's five bounds work
- [ADR-0017](../adr/0017-bounded-recovery-scan.md) — why the scan is deliberately imprecise
