# 2026-09-02 — Sync handlers: before and after unblocking the event loop

**English** | [繁體中文](../../zh-TW/verification/2026-09-02-sync-handlers-before-after.md)

---

## What was being verified

Three endpoints (`POST /orders`, `POST /process_raw/{raw_id}`, `GET /raw/{raw_id}`) were declared `async def` but their bodies made **synchronous blocking psycopg2 calls**, with no `await` anywhere inside the connection-holding window.

Three layers of assumption were under test:

1. Does this actually hold the event loop — can it be **measured**, not just inferred from the code?
2. Does switching to `def` (Starlette moves the handler into the anyio threadpool) resolve it, and at what cost?
3. Which conclusions of [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md), written earlier the same day, become invalid?

⚠️ This is a **correctness** fix, not a performance optimisation. The performance change is a side effect — see conclusion 6.

## Environment

Identical to [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) (WSL2, 16 cores, only `db` / `redis` kept, rate limiting disabled, service under test started via `docker run`), plus:

| Item | Setting |
|---|---|
| **before** | `api-api:latest` — the pre-change image (`async def` + blocking DB) |
| **after** | `api-a:latest` — the post-change image (`def` + threadpool) |
| Comparison | Same machine, same containers, **alternating runs**, to control for machine drift |

Code change: three endpoints switched to `def`, new async dependency `get_raw_body`, `asyncio.sleep`→`time.sleep`, `_enqueue` no longer wrapped in `to_thread`, `import asyncio` removed. 68 lines changed in `main.py`.

## Method

1. **Both images kept side by side**; `start_api` selects with `LT_IMAGE`, every other parameter held equal.
2. **Event-loop probe**: while load is running, poll `GET /health` (every 10ms for 12 seconds) and record its latency distribution.
   ⭐ `/health` touches no database, has no auth, and no rate limit — **the only resource it needs is the event loop**. Its latency is therefore a direct measurement of how long the loop is held, and it is this document's central evidence.
3. **Throughput**: `load_test.py`, 1200 requests each at concurrency 50, **three alternating rounds** (before→after→before→after…).
4. Tests 1/2/3 re-run with the original document's method, unchanged.
5. **Fault injection** (Test G): `docker pause api-db-1` freezes the database process for 8 seconds. The load must span the stall window, otherwise the measurement captures only silence — with no requests in flight during the stall, there is nothing to observe.
6. **Sustained load** (Test H): `redis-cli -n 0 flushdb` clears the celery queue before the run. Leftover messages make the previous round's `order_id`s arrive again and take the `duplicate` short path, which is far cheaper than a full ODS write and inflates the measured drain rate.
5. Between runs: `DELETE FROM ods` then `DELETE FROM raw` (FK order).

## Observed

### Test A — Event-loop responsiveness ⭐

workers=1, polling `/health` while under load:

| | **before** | **after** | Improvement |
|---|---:|---:|---:|
| samples | 534 | 725 | |
| p50 | 4.1 ms | 2.5 ms | 1.6× |
| p95 | 47.6 ms | 24.0 ms | 2.0× |
| **p99** | **167.2 ms** | **33.9 ms** | **4.9×** |
| **max** | **234.7 ms** | **53.0 ms** | **4.4×** |
| status codes | all 200 | all 200 | |

**This table is the defect itself.** `/health` touches no database at all, yet under load its p99 was dragged to 167ms — all of which is time spent queued behind someone else's `db.commit()`. After the change it is 34ms.

⚠️ workers=1 is used here to make the effect visible. In a multi-process deployment the other three workers can still answer, so the symptom is **masked**, not removed — and the cost of that masking is that each held process is still entirely stalled.

### Test B — Throughput (three alternating rounds)

1200 requests each, concurrency 50, workers=1, pool=5+10:

| Round | before | after |
|---|---:|---:|
| 1 | 130.7 | 183.5 |
| 2 | 130.1 | 180.9 |
| 3 | 126.7 | 186.8 |
| **mean** | **129.2** | **183.7** |

**+42%, with the two groups not overlapping across three alternating rounds.** Also: before had 1 failure across the three rounds (and 2 in the first comparison); after had zero.

### Test C — Latency decomposition re-run (conclusion unchanged)

| | before | after |
|---|---:|---:|
| wall-clock p50 (OTel off, n=3500) | 8.34 ms | 8.09 ms |
| wall-clock p50 (OTel on) | 9.87 ms | 8.60 ms |
| **server span p50** | 8.05 ms | **7.29 ms** |
| Residual (framework itself) | 6.48 ms (80.5%) | 5.87 ms (**80.6%**) |
| Dispatch: celery publish → Redis | 0.65 ms | 0.60 ms |
| `INSERT INTO raw` | 0.38 ms | 0.34 ms |
| `db.refresh()`'s SELECT | 0.32 ms | 0.28 ms |
| DB connection checkout ×2 | 0.09 ms | 0.08 ms |

**Single-request latency and the per-segment proportions barely moved** (residual still 80.6%, database still under a tenth). This is as expected — **the change does not make a single request faster; it stops concurrent requests from blocking one another.** The 0.76ms the server span lost comes mostly from the eliminated `to_thread` hop.

### Test D — Pool sweep re-run (conclusion reversed)

workers=1, 4 clients, total concurrency 52, n=2000:

| pool | before RPS (2 rounds) | **after RPS (2 rounds)** | before peak conns | **after peak conns** |
|---|---:|---:|---:|---:|
| 1 (1+0) | 312 / 285 | **167 / 153** | 11 | 6 / 8 |
| 2 (2+0) | 240 / 253 | 202 / 183 | 11 | 7 / 9 |
| 5 (5+0) | 180 / 234 | 205 / 186 | 12 | 11 / 12 |
| 15 (5+10) | 180 / 203 | 201 / 183 | 11 | 20 / 13 |
| 40 (30+10) | 248 / 222 | 194 / 180 | 11 | 19 / 20 |
| 80 (60+20) | 145 / 267 | 195 / 178 | 12 | 19 / 25 |

Two properties changed:

1. **`pool=1` is now clearly worse (about -18%), consistently across both rounds.** The "before" column jumps around with no trend at all — that is what "the pool is irrelevant" looks like.
2. **Peak connections now grow with pool size** (6 → 25). Before, it was 10–12 regardless of whether pool was 1 or 100.

The new shape is **1→2 matters, flat above 2**. The bottleneck has returned to the GIL: at workers=1 only one thread can run Python at a time, so the only overlappable part is the 0.62ms of DB I/O, requiring roughly 1.1 concurrent connections — **pool=2 already suffices**.

### Test E — Worker sweep re-run (conclusion reversed)

pool=3+5 (compose's actual value), 4 clients, total concurrency 52, n=2000, mean of two rounds:

| workers | before | **after** | Change | after peak conns |
|---:|---:|---:|---|---:|
| 1 | 130.8 | 160.5 | +23% | 13–14 |
| 2 | 201.0 | 260.2 | +29% | 21 |
| 4 | 298.1 | **367.3** | +23% | 29–32 |
| 8 | **207.1 (regression)** | **485.4** | **+134%** | 35–40 |

**The "8 workers regresses" conclusion is gone — after the change the curve rises all the way and 8 is the peak.**

Confirmed this is not a client-side ceiling:

| Configuration | Total RPS | Peak conns |
|---|---:|---:|
| workers=8, 4 clients, n=4000, concurrency 104 | 507.0 | 57 |
| workers=8, **8 clients**, n=4000, concurrency 104 | 534.6 | 61 |
| workers=4, 8 clients, n=4000, concurrency 104 | 334.5 | 34 |

Doubling client processes gains only 5% (507→535), so **510–535 RPS at workers=8 is a real server measurement, not the client.**

### Test F — Actual connection-budget utilisation ⚠️

| Configuration | Budget ceiling | **Measured peak** | Utilisation |
|---|---:|---:|---:|
| before, workers=4 | 32 | 10–12 | ~35% |
| **after, workers=4 (current compose)** | 32 | **29–34** | **~100%** |
| after, workers=8 | 64 | **57–61** | ~95% |

(Peaks include roughly 4–8 connections from the worker container and baseline.)

### Test G — Database stall injection ⭐

**This closes the one property this document had flagged as inferred rather than measured.**

Method: workers=1, pool=3+5; 6,000 requests sent to `/orders` at concurrency 20 while `/health` is polled every 200ms. Eight seconds into the load, `docker pause api-db-1` for 8 seconds, then unpause.

| | **before** (`async def`) | **after** (`def`) |
|---|---:|---:|
| `/health` p50 | 57.9 ms | 17.2 ms |
| `/health` p95 | 91.1 ms | 32.2 ms |
| **`/health` max** | **8,169.5 ms** | **40.4 ms** |
| requests over 1 second | **1 (8.2s)** | **0** |
| `/orders` | 6,000, all 200 | 6,000, all 200 |

**One `/health` request took 8.2 seconds under the old code — almost exactly the 8-second database stall.** An endpoint that touches no database was held for the entire duration of a database outage.

⭐ **The freeze lasts as long as the database is stuck.** This is the mechanism behind conclusion 1's "frozen for up to `statement_timeout` (30s)" — 8 seconds is simply a scaled-down version; lengthen the stall and the freeze lengthens with it.

⚠️ Neither version lost an order (6,000 requests all returned 200). The difference is **not data correctness but whether other requests can still be served**.

### Test H — Sustained load and backpressure, re-run

Same method as [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) Test 5: 4 client processes, `order_id` offsets, 15,000 requests each, 60,000 total.

| Metric | before | **after** |
|---|---:|---:|
| Injection rate | 313 /s | **487.8 /s** |
| **Peak backlog** | 5,453 | **36,526 (6.7×)** |
| **Worker drain (during injection)** | 270 /s | **186 /s** |
| Worker drain (after injection) | — | **299 /s** |
| Full drain after injection stopped | 0 s | **119 s** |
| ODS rows landed | 60,000 / 60,000 | **60,000 / 60,000** |
| Errors | 0 | **0** |

Backlog curve (after):

```
t=20s   backlog  7,372   processed  3,673
t=60s   backlog 22,556   processed 11,195
t=101s  backlog 36,526   processed 18,735   <- peak
t=121s  backlog 35,556   processed 23,892   <- injection ends (t=123s), recovery begins
t=181s  backlog 18,213   processed 41,787
t=241s  backlog    132   processed 59,868
```

⭐ **The worker's drain rate is not a constant**: 186/s while injection is running, back to 299/s the moment it stops.

## Conclusion

### 1. The defect is real, and it is failure amplification rather than a performance issue

Test A is the direct evidence: an endpoint that **touches no database at all** had its p99 dragged to 167ms under load. That time was spent with the event loop parked inside another request's `db.commit()`.

Taken to its limit: **Test G measured** an 8-second database stall holding one `/health` request for 8.2 seconds — **the freeze lasts as long as the database is stuck**. And `statement_timeout` is 30 seconds, so under a real lock wait a single stuck query can freeze an entire uvicorn process for up to 30 seconds — not just that request, but every request on that process, `/health` included. With four workers that is 25% of serving capacity gone.

⭐ **This property is completely invisible in normal operation and appears only during an incident — which is exactly when it is most needed alive.**

⚠️ The highest-risk of the three endpoints is `POST /process_raw/{raw_id}`, not the higher-traffic `/orders`: its `force=True` path issues an `UPDATE` on `raw`, while the worker's CAS claim (`try_claim_raw`) is concurrently issuing `UPDATE` on the same row. Lock waits can reach the full 30 seconds. **Low traffic does not mean low risk** — `/orders` does a 0.34ms INSERT, `/process_raw` does an UPDATE that may wait on a lock.

### 2. Single-request latency and decomposition proportions are unchanged

Server span 8.05 → 7.29 ms with near-identical proportions. **What was fixed is not per-request cost but mutual blocking under concurrency.**

### 3. The connection pool went from "irrelevant" to "two per process is enough"

Before, the pool sweep was a trendless noise band (each process only ever held one connection); after, there is a real 1→2 gap and peak connections track pool size.

Above 2 it is still flat — the GIL prevents each process's Python work from truly running in parallel, so only the DB I/O overlaps.

### 4. The worker curve no longer inverts

Before, it regressed at 8 (298→207); after, it climbs to 485. **The current `UVICORN_WORKERS=4` has gone from "the peak of the curve" to "a conservative choice".**

⚠️ Do not simply raise it to 8: this is a single-machine measurement with the load generator sharing the same 16 cores. The real deployment's core count — and the connection ceiling in the next section — are what should decide the value.

### 5. ⚠️ Retracting the previous document's actionable recommendation

[ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) conclusion 2 states:

> the API's connection budget can be cut from 32 (`4 × (3+5)`) to 8

**That recommendation would now cause 503s.** Test F shows the current 32 peaks at 29–34 under load — **essentially fully used**. Cutting to 8 would make requests queue on the pool, hit `pool_timeout=30s`, and return `SATimeoutError → 503`.

The recommendation was correct when written (only 4 connections were then in use). **It became invalid because the premise that produced it was removed by this change.** This is precisely why verification records carry dates and an overturn chain.

**The budget also now gives `UVICORN_WORKERS` an upper bound**: `max_connections=100` minus `superuser_reserved_connections=3`, minus 16 for the worker container, minus roughly 4 for Airflow and human sessions, leaves the API about **75**; at 8 per process (`3+5`), that is **about 9 uvicorn workers maximum**.

### 6. The gap between prediction and measurement, and why it happened

The pre-change estimate was **+10%**; measured **+42%**.

The model was wrong: it counted only the DB spans visible in the trace (0.79ms of 8.05ms ≈ 10%) as overlappable and treated everything else as GIL-bound and therefore not. In practice, once `def` moves the whole handler off the event loop, **the loop can continuously drive socket/ASGI work for 50 connections** — work that previously competed for the same thread as the handler, and which appears in no span at all.

⭐ **Lesson: estimating "how much removing a blocking call will save" from spans systematically underestimates, because the event loop's own work is not in any span.**

### 7. Three concrete implementation traps (all verified)

| Trap | Consequence | Outcome |
|---|---|---|
| No running loop inside a worker thread, so `asyncio.to_thread` / `asyncio.sleep` raise `RuntimeError` | **fails at runtime**, not import | Existing `tests/test_auth.py` catches it immediately |
| slowapi's decorator inspects the signature and raises if `request` is absent | **fails at import** | `request: Request` must stay in the signature even though the handler no longer uses it |
| contextvars (structlog's `request_id`/`client_id`, OTel spans) might not reach the thread | **silent failure**, logs lose correlation | **Measured: they propagate correctly** (Starlette's `run_in_threadpool` copies the context) — the only silently-failing item, and it is ruled out |

Separately: 8 unit tests that call the handler functions directly needed updating (drop `await`, pass `raw_body=`, `patch("asyncio.sleep")`→`patch("main.time.sleep")`). **They were affected because they bypass FastAPI's dependency resolution and call the function directly** — tests going through TestClient were entirely unaffected.

### 8. ⚠️ Burst backlog grows nearly 7× larger, and the worker slows down during bursts

This is the one consequence of the change that moves in the wrong direction, and it has to be recorded.

```
before: 313 in − 270 out = 43 /s gap   -> peak backlog  5,453
after : 488 in − 186 out = 302 /s gap  -> peak backlog 36,526
```

Both ends worsened at once: **the API accepts faster (+56%) while the worker drains slower during the burst (270 → 186).**

The worker did not change — `process.py` was not touched. **The API simply takes more CPU during a burst now**, and in this test the API, worker, PostgreSQL, Redis and 4 load-generator processes all share one 16-core machine. The evidence: the moment injection stops, the worker returns to 299/s.

**Backpressure behaviour remains entirely correct**: zero errors, zero 503s, zero lost orders, 60,000 rows all landed in ODS, fully recovered 119 seconds after load stopped. A larger backlog is not a failure — it is the queue absorbing a larger gap.

Three things that follow:

1. **How to read `raw_pending_watch`.** It measures how long the oldest row has waited, not how many rows there are, so it will not false-positive; but **the backlog figures you see will be an order of magnitude larger than the 5,453 in the earlier record** — do not read 36,526 as an anomaly.
2. **This strengthens rather than overturns [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) conclusion 4** (the ceiling is the worker, not the API) — the gap widened, so the conclusion holds more strongly. But **the 313 / 270 figures in its conclusions 4 and 6 are superseded by this document.**
3. **This worsening is an artefact of sharing one machine.** Deploy or scale the worker separately and the contention disappears — and `try_claim_raw`'s CAS guarantees workers scale horizontally without coordination.

**Updated citable capacity statement (must be quoted together with its environment):**

> Measured on a single development machine (16 cores; DB / Redis / worker / load generator co-located; rate limiting disabled): the ingestion API accepts ~488 orders/second. The Celery worker's drain rate depends on how hard the API is working — roughly 186/second during a burst, roughly 299/second when the API is idle. Beyond drain capacity the surplus is absorbed by the queue: a 60,000-request burst peaked at a backlog of 36,526 with zero errors and was fully consumed 119 seconds after load stopped, with every row landing in ODS.

### ⚠️ What this document does not represent

- **Only one kind of fault was injected.** Test G stalled PostgreSQL for 8 seconds with `docker pause`, directly verifying conclusion 1's freeze mechanism. But **broker outage, worker crash and dropped connections remain outside this document** — those are covered by [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) and [2026-08-10-circuit-breaker-before-after](./2026-08-10-circuit-breaker-before-after.md).
- **`docker pause` is not a slow query.** It freezes the whole database process, so the server-side `statement_timeout` never fires (the server is stopped too). Under a real lock wait or slow query, `statement_timeout` intervenes at 30 seconds and aborts the statement — **so the freeze is bounded at 30 seconds rather than unbounded**. That bound was not measured here.
- **Not production figures.** Single machine, load generator sharing 16 cores with the services, data volume around 17k rows.
- **Not a case for setting pool to 2.** Test D's flat region was measured at workers=1; per-process concurrency differs at other worker counts, and a degraded database raises the connections needed — **headroom exists for the abnormal case.**
- **Rate limiting was disabled throughout.** Production is `60/minute`.

## What this overturned

⭐ **Overturns conclusions 2 and 3 of [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md), written earlier the same day.**

| Overturned | Was | Is |
|---|---|---|
| Conclusion 2 (connection pool) | "pool from 1 to 100 affects neither throughput nor connection count; the budget can be cut from 32 to 8" | pool 1→2 is an 18% gap; peak connections track pool size; **the 32 budget is essentially fully used, and cutting it would cause 503s** |
| Conclusion 3 (worker count) | "1→4 near-linear, 8 regresses; the current `UVICORN_WORKERS=4` sits at the peak of the curve" | the curve no longer inverts, 8 reaches 485 RPS; 4 is a conservative value, not the peak |

**Conclusions 1 (the time is in the framework, not the database), 4 (the ceiling is the worker), 5 (Python is a cost, not a ceiling) and 6 (the four dimensions of "high concurrency") still hold** — test C's re-run left the proportions unchanged.

⚠️ Written and overturned on the same day, which is not a process failure: **the earlier document measured "what this system is"; this one measures "what it becomes once the defect it identified is fixed."** Both must exist, because anyone who acted on conclusion 2's recommendation needs to see why it no longer applies.

## Related

- [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) — this document overturns its conclusions 2 and 3
- [2026-08-03-load-test-ingestion](./2026-08-03-load-test-ingestion.md) — the cause of its Test 2 (`BackgroundTasks` dispatching synchronous processing into a 40-thread pool) is the mirror image of this mechanism: that time it was **too many threads for too small a pool**, this time **too few threads to use the pool at all**
- [design/queue](../design/queue.md) · [ADR-0004](../adr/0004-cas-claim-rowcount.md)
