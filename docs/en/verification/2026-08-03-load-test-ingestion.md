# 2026-08-03 — Ingestion load test

**English** | [繁體中文](../../zh-TW/verification/2026-08-03-load-test-ingestion.md)

---

## What was being verified

Concurrency behaviour and failure modes of the ingestion path: **does the CAS claim actually hold under real contention, and does ODS idempotency hold under a TOCTOU race?**

## Environment

`load_test.py` against a real server and real PostgreSQL. SQLAlchemy pool defaults: `pool_size=5`, `max_overflow=10` → 15 concurrent connections, `pool_timeout=30s`.

## Observed

### Test 1 — 1,000 unique orders, concurrency 50

All succeeded, **7.9s**, zero errors.

Each `POST /orders` performs a single fast INSERT and releases the connection immediately (hold time < 10ms). Concurrency 50 is well within the pool's capacity — no queuing occurs.

### Test 2 — 1,000 unique orders, concurrency 500

P99 latency **~14s**, **5 × HTTP 500**.

With 500 simultaneous requests, 485 queue for a connection. Any request exceeding `pool_timeout=30s` throws `QueuePool limit reached`. The 5 failures timed out **before** the INSERT — no Raw record was created, so nothing was half-written.

Handled since: `SATimeoutError` is caught and returns **503 Service Unavailable** immediately, letting the client retry against a truthful status code.

### Test 3 — 100 duplicate `order_id`s, concurrency 100

**100 Raw records written, 100 ODS records written** — all succeeded.

This is the designed behaviour, not a defect: `raw.order_id` is indexed but not unique ([ADR-0001](../adr/0001-raw-no-business-dedup.md)). Each duplicate is a new ingestion event with its own `raw_id`. **CAS protects the same `raw_id` from being processed twice; it is not business deduplication.**

### Test 4 — 100 workers competing for the same `raw_id`

`raw.status = processed`, **ODS COUNT = 1.**

`try_claim_raw` issues `UPDATE raw WHERE id=X AND status='pending'`. PostgreSQL row-locks the UPDATE — only the first worker gets `rowcount=1`; the remaining 99 get `rowcount=0` and return immediately.

### Test 6 — duplicate `order_id`, both orderings

| Scenario | Result |
|---|---|
| **Sequential** — same `order_id` submitted twice | First writes ODS. Second hits the pre-check, marked `duplicate`, ODS not written again |
| **TOCTOU race** — two workers both pass the pre-check | First commits ODS; the second hits `IntegrityError` on commit — caught **without retry**, marked `duplicate` |

ODS always contains **exactly one record per `order_id`**, and every subsequent duplicate reaches the `duplicate` terminal status, giving monitoring a clean signal.

## Conclusion

CAS and idempotency both hold under real contention. Test 4 and Test 6's race are the two that CI cannot reproduce — a mocked database has no row locks and no unique constraint to violate.

**Test 3 is included deliberately even though it "passed trivially"**: it documents that duplicate `order_id`s reaching ODS is the design, not an escape.

## What this overturned

Nothing at the time. **Test 5 (SIGKILL) from the same suite was later overturned** — see [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md).

## Related

- [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0005](../adr/0005-first-write-wins-idempotency.md)
- [design/testing](../design/testing.md) — why these are manual and not in CI
