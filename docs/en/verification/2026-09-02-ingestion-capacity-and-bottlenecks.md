# 2026-09-02 — Ingestion capacity and bottleneck location

**English** | [繁體中文](../../zh-TW/verification/2026-09-02-ingestion-capacity-and-bottlenecks.md)

---

## What was being verified

For `POST /orders → Raw → ODS`: **where the time goes, where the ceiling is, and what happens under overload.** Four questions:

1. A request takes roughly 8ms — what is that 8ms made of? How much is the database?
2. Does connection-pool size (15 → 100) affect throughput?
3. How does uvicorn worker count (1 → 8) affect throughput?
4. Under sustained overload, does the system queue or collapse?

## Environment

| Item | Setting |
|---|---|
| Host | WSL2, 16 cores. Load generator and services on the same machine |
| API under test | The same `api-api:latest` image compose uses, started via `docker run` on port 8001 |
| Parameter overrides | All through existing environment variables: `RATELIMIT_ENABLED` / `POOL_SIZE` / `MAX_OVERFLOW` / `UVICORN_WORKERS` / `OTEL_ENABLED` |
| Worker | `api-worker:latest`, `--concurrency=4`, `POOL_SIZE=2`, `MAX_OVERFLOW=2` (identical to compose) |
| Services kept | Only `db` / `redis`. api / worker / beat / otel-collector and the entire Airflow overlay were stopped |
| Observability | Local Jaeger (OTLP direct, never reaching Grafana Cloud) + py-spy (sidecar container sharing the PID namespace) |

⚠️ **Rate limiting was disabled throughout** (`RATELIMIT_ENABLED=false`). This document measures **capacity**, not the deployed policy — which is `60/minute`.

⚠️ **Zero code changes.** `RATELIMIT_ENABLED` is slowapi's own environment-variable contract (`Limiter.__init__` ends with `self.enabled = self.get_app_config("RATELIMIT_ENABLED", ...)`, via starlette's `Config`, where os.environ takes priority over `.env`).

## Method

1. **Isolation**: `docker compose stop api beat worker otel-collector` plus the whole Airflow overlay, keeping only `db` / `redis`, to remove neighbour noise and connection contention.
2. **Services under test**: `api-api:latest` (port 8001) and `api-worker:latest` started via `docker run`, all parameters injected with `-e`. Each parameter change means `docker rm -f` and a fresh start — environment changes require recreation; `restart` does not re-read them.
3. **Load**: throughput used the existing `load_test.py`. Two helper scripts were added to work around the two measurement traps in *Test 0*: a sequential probe (no semaphore, no `gather`) and a multi-process injector (`order_id` offset to avoid collisions). Both reuse `load_test.make_payload`, so the data shape is identical.
4. **Traces**: `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a local Jaeger container rather than the compose collector — nothing left the machine. Spans were pulled from Jaeger's HTTP API; only **direct children** of the server span are counted (to avoid double-counting grandchildren), and the residual = server span − sum of direct children.
5. **Profiling**: a py-spy sidecar with `--pid=container:api-lt --cap-add SYS_PTRACE` shares the PID namespace; `record -f raw --full-filenames` captured 30 seconds of on-CPU samples (without `--idle`, since blocking waits are already measured by the spans).
6. **Backlog sampling**: `raw` counts for `pending + processing` and `processed` every 1–2 seconds; `pg_stat_activity` sampled every 0.1 seconds for connection counts.
7. **Cleanup**: between runs, `DELETE FROM ods WHERE order_id LIKE 'LOAD-%'` then `DELETE FROM raw ...` (`ods.raw_id` has an FK to `raw.id`; the order cannot be reversed).

## Observed

### Test 0 — Two traps in the measurement tooling itself ⭐

**This has to come first, because it invalidated the first two rounds of this exercise — and the prior understanding of latency.**

#### Trap 1: `load_test.py` does not report latency, it reports queueing

`load_test.py:126` sets `t0 = time.perf_counter()` **outside** `async with sem`. `asyncio.gather` creates all N coroutines at once, so every request's stopwatch starts at t≈0 and only then queues on the semaphore. What it reports is **semaphore queueing + the real round trip**.

Decisive evidence: at C=1, n=200, p50 = 0.909s while total elapsed was 1.76s — **p50 is exactly half the total**, the mathematical signature of "N people queueing in order, the median one waited half the time." Nothing to do with the server.

| Measurement | p50 |
|---|---|
| `load_test.py`, C=1, n=200 | **909 ms** |
| Sequential probe (no semaphore, no gather), n=3500 | **8.34 ms** |

A factor of 109.

#### Trap 2: a single client process saturates at ~150 RPS

`load_test.py` re-seeds `random.Random(i)`, builds a nested payload and encodes JSON for every request. One process saturates one core.

| Client configuration | Total RPS |
|---|---|
| 1 process | 149.8 |
| 2 processes in parallel (2000 requests / 6.68s) | **299.4** |

**The load generator hit the wall before the service did.** Every sweep below therefore uses 4 client processes.

---

### Test 1 — Single-request latency decomposition

#### 1a. Span breakdown (Jaeger, 355 warm traces, OTel on)

| Segment | p50 | Share |
|---|---:|---:|
| **Residual (framework itself, see 1b)** | **6.48 ms** | **80.5%** |
| Dispatch: celery publish → Redis | 0.65 ms | 8.0% |
| **`INSERT INTO raw`** | **0.38 ms** | 4.8% |
| `db.refresh()`'s SELECT | 0.32 ms | 4.0% |
| Rate-limit counter, Redis EVALSHA | 0.20 ms | 2.4% |
| DB connection checkout ×2 | 0.09 ms | 1.1% |
| **Server span total** | **8.05 ms** | 100% |

**The database (INSERT + SELECT + checkout) totals 0.79 ms — under a tenth.**

#### 1b. What the residual is made of (py-spy, OTel off, 3,181 samples)

| Phase | CPU share |
|---|---:|
| **SQLAlchemy** | **40.3%** |
| asyncio event loop | 13.6% |
| uvicorn / HTTP parsing | 7.6% |
| redis client | 7.0% |
| `to_thread` thread pool | 6.4% |
| Starlette middleware / ASGI | 5.6% |
| kombu / celery dispatch serialisation | 5.2% |
| FastAPI dependency resolution / routing | 3.5% |
| stdlib logging | 3.4% |
| structlog | 2.8% |
| JSON encode/decode | 2.3% |
| **pydantic validation** | **1.1%** |
| Handler body `main.create_order` | 1.0% |

Splitting that 40.3%:

| | Share of SQLAlchemy |
|---|---:|
| Driver layer (`do_execute` / `do_commit` / `do_rollback`) | 25.8% |
| **ORM/Core machinery** (session, unit of work, cache key, identity map) | **74.2%** |

**⭐ Preparing to talk to the database, plus tidying up what it said back, costs three times as much as the talking.**

Two incidental findings:

- **Every request performs a `do_rollback`** (4.7% of SQLAlchemy samples). Its source is the transaction opened by `db.refresh()`, never committed, rolled back at `db.close()`.
- **pydantic accounts for only 1.1%.** The payload nests customer / address / items[] / payment / behavior five levels deep and intuitively should dominate — measurably it is negligible.

#### 1c. The cost of OpenTelemetry

| | OTel off | OTel on | Delta |
|---|---:|---:|---:|
| wall-clock p50 | 8.34 ms | 9.87 ms | **+1.53 ms (+18%)** |
| on-CPU samples (30s) | 3,181 | 4,986 | **+57%** |

---

### Test 2 — Connection-pool sweep (15 → 100)

workers=4, 4 clients, total concurrency 52, n=2000, two rounds each:

| pool (size+overflow) | Round 1 RPS | Round 2 RPS | Peak pg connections |
|---|---:|---:|---:|
| 1 (1+0) | 312.1 | 285.3 | 11 |
| 2 (2+0) | 239.9 | 252.9 | 11 |
| 5 (5+0) | 179.8 | 234.1 | 12 |
| 15 (5+10) | 179.5 | 203.2 | 11 |
| 40 (30+10) | 247.6 | 222.0 | 11 |
| 80 (60+20) | 145.2 | 266.5 | 12 |
| 100 (80+20) | 263.4 | 262.0 | 10 |

**RPS shows no trend; the same configuration varies by ±40% between rounds — host noise dwarfs any pool effect.**

**The decisive evidence is not RPS but the peak connection count: 10–12 for every configuration**, including `POOL_SIZE=80 / MAX_OVERFLOW=20 × 4 workers` (theoretical ceiling 400). `pool=1+0` (ceiling 4) also peaked at 11.

**Mechanism**: `create_order` is `async def`, but `db.commit()` and `db.refresh()` are synchronous psycopg2 calls with **no `await` between them**. The connection-holding window never yields the event loop → **each uvicorn process holds at most one connection at a time.**

⇒ compose currently budgets 32 connections for the API (`4 × (3+5)`); roughly 4 are actually used.

---

### Test 3 — uvicorn worker sweep

4 clients, total concurrency 52, n=2000, pool=5+10:

| workers | Total RPS | Relative |
|---:|---:|---:|
| 1 | 130.8 | 1.00× |
| 2 | 201.0 | 1.54× |
| 4 | **298.1** | **2.28×** |
| 8 | 207.1 | 1.58× (**regression**) |

1→4 is close to linear, consistent with Test 2's mechanism: **since each process can only have one DB operation in flight, the only way to add throughput is to add processes.**

The regression at 8: the 16-core host is simultaneously running 4 load-generator processes, the Celery worker, PostgreSQL and Redis. Process count exceeds what the cores can feed, and switching cost starts eating the gain. **compose's current `UVICORN_WORKERS=4` sits at the peak of this curve.**

---

### Test 4 — C=500 re-verification of [2026-08-03](./2026-08-03-load-test-ingestion.md) Test 2

| Scenario | Result | Peak pg connections |
|---|---|---:|
| **Faithful replica**: single client, C=500, n=1000, workers=4, pool=5+10 | **1000 / 1000 succeeded, 0 failures** | 12 |
| Same but workers=1 | 996 succeeded + 4 `RemoteProtocolError` | 9 |
| 4 clients, total concurrency 500, n=4000 | 3999 + 1 `RemoteProtocolError` | 12 |
| 4 clients, total concurrency 1000, n=4000 | 3979 + 21 `RemoteProtocolError` | 12 |
| Single client, C=1000, n=1000 (finished in 9.30s) | **1000 / 1000 succeeded, zero server-side warnings** | 11 |

**Zero HTTP 500s, zero 503s. The pool was never exhausted.**

Those `RemoteProtocolError`s are **client-side httpx errors, not server rejections**: they appear only in runs lasting more than ~15 seconds, and uvicorn's `timeout_keep_alive` defaults to 5 seconds. Keep-alive connections closed by the server after idling are then reused by httpx, which raises this. The C=1000 run that finished in 9.30s produced none.

---

### Test 5 — Sustained load and backpressure ⭐

4 client processes (`order_id` offset to avoid collisions), 15,000 requests each, 60,000 total:

```
t=20s   backlog 1,256   processed  4,778
t=41s   backlog 2,626   processed 10,163    ← in > out, backlog grows linearly
t=61s   backlog 3,831   processed 15,021
t=80s   backlog 5,183   processed 19,852    ← peak 5,453
t=121s  backlog 1,125   processed 31,029    ← one client finished, injection slows, recovery begins
t=140s  backlog     2   processed 35,275    ← caught up
t=200s  backlog     2   processed 45,315
t=280s  backlog     1   processed 58,138
Drain time after injection stopped: 0 seconds
```

| Metric | Value | Derivation |
|---|---:|---|
| Total injected | 60,000 / 292s | all HTTP 200 |
| **API intake capacity** | **~313 /s** | first 80s: (19,852 processed + 5,183 backlog) ÷ 80s |
| **Worker drain capacity** | **~270 /s** | saturated window t=80→121: 11,177 ÷ 41s |
| Peak backlog | 5,453 | t≈80s |
| Backlog recovery time | ~55s | 5,453 → 2 |
| ODS rows landed | 60,000 / 60,000 | zero loss |

**What happened during overload (first 85s, 313 in vs 270 out): nothing.** No errors, no 503s, no timeouts, no lost orders. The surplus of 43/s accumulated in the queue and the backlog grew **linearly**. Once load fell below capacity it was fully recovered in 55 seconds.

ODS grew from 17,380 to 77,380 rows during the test with **no observable write degradation**.

---

## Conclusion

### 1. The time is in the framework layer, not the database

Of 8.05 ms per request, the database (INSERT + refresh SELECT + connection checkout) totals 0.79 ms — **under a tenth**. Eighty percent is Python framework and ORM overhead, the largest single component being SQLAlchemy's ORM bookkeeping at roughly 30% of total CPU — three times the actual SQL execution.

The intuitively suspicious nested pydantic validation is 1.1%. **Optimising "the database" or "serialisation" would have missed entirely.**

### 2. The connection pool is not a tunable　⚠️ OVERTURNED

> **⚠️ This section was overturned later the same day by [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md).**
> The original text is kept below because it records what was measured at that moment; but
> **the actionable recommendation must not be followed** — once the three endpoints became `def`,
> the 32-connection budget peaks at 29–34 under load (essentially full), and cutting it to 8
> would cause `pool_timeout` expiries and 503s.

Synchronous DB calls inside an `async def` mean each process holds at most one connection; from pool 1 to pool 100 there is no observable effect on throughput or on actual connection count (peak stays 10–12).

Actionable: **the API's connection budget can be cut from 32 (`4 × (3+5)`) to 8.** `max_connections` is only 100, and the reclaimed headroom is what Airflow and human connections compete for.

### 3. uvicorn worker count is the only effective knob　⚠️ OVERTURNED

> **⚠️ This section was overturned later the same day by [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md).**
> The original text is kept below. After the endpoints became `def` the curve no longer
> inverts at 8 (207 → 485 RPS), so `UVICORN_WORKERS=4` went from "the peak of the curve"
> to "a conservative choice".

1→4 is close to linear (130.8 → 298.1 RPS); 8 regresses. The current `UVICORN_WORKERS=4` sits at the peak. This follows directly from point 2: if each process can only have one DB operation in flight, more throughput means more processes.

### 4. The capacity ceiling is the worker, not the API

This project's ingestion path is **"API accepts fast → Celery queue → worker drains."** So the answer to "how much can the system take" is **the worker's number (~270/s), not the API's (~313/s)**.

The gap is healthy: **a landing layer should accept faster than it processes**, so bursts are taken in rather than rejected.

Test 5 observed the overload behaviour directly: with 313 in against 270 out, the 43/s surplus accumulated in the queue and the backlog grew **linearly** to 5,453, with no errors, rejections or losses; once load fell, it fully recovered in 55 seconds. **The queue's job is to decouple upstream speed from downstream speed — a backlog is not a failure, it is the design working.**

⚠️ But "grows linearly" also means **a sustained overload will not converge on its own.** That is exactly why `raw_pending_watch` exists: it measures rows that landed in Raw but were never picked up by any worker, and this test gives that threshold an empirical reference — single digits when healthy, climbing linearly by tens per second under overload.

### 5. Python is a cost, not a ceiling

Eighty percent of the time is spent in Python (framework + ORM). A compiled language would reclaim most of that 7.26 ms — **but not the 0.79 ms of database time, which is PostgreSQL doing work.** Roughly speaking, Python + SQLAlchemy costs several times the CPU.

What matters is that **this cost is linearly purchasable**: Test 3 shows adding processes scales linearly. What actually makes a system fail to cope is a bottleneck where **adding machines does not help** — a single write ceiling, a global lock, in-process state that cannot scale horizontally. This project has none of those:

- API processes are stateless (recovery scanning moved to Celery Beat in `cf81d29`)
- Claiming relies on database row locks (`try_claim_raw`'s `rowcount == 1`)
- The queue lives in Redis; workers scale horizontally

**Conclusion: Python affects the hardware bill, not the architecture's scalability. At this project's volume it is not a problem to solve.**

### 6. Unpacking the phrase "handles high concurrency"

"High concurrency" collapses four distinct dimensions into one phrase. One measurement was taken for each:

| Dimension | In plain terms | Test result |
|---|---|---|
| Concurrent connections | do many simultaneous connections break it | 1000 concurrent connections all returned 200; peak pool 11; no server warnings. **Higher concurrency untested** |
| Throughput | how many per second can it take | API ~313/s, worker ~270/s. **Observed with the load generator and all services sharing the same 16 cores** |
| Endurance | does the rate degrade over time | 60,000 requests injected over 292 continuous seconds, no visible degradation, backlog at zero when injection stopped. **Beyond 5 minutes untested** |
| Backpressure | does it slow down or collapse past capacity | backlog grew linearly to 5,453 with zero errors, fully recovered in 55s. **Sustained overload over tens of minutes untested** |

⚠️ **The table above records observations in this document's environment. It is not an upper bound on the system's capability, nor a guarantee of any kind.** Each dimension was sampled at a single operating point; none was pushed to failure. So what this document can say is "everything measured here behaved correctly" — not "this is the limit." The actual limit can only be extrapolated from these observations (see point 8), and that extrapolation is unverified.

**Citable capacity statement (must be quoted together with its environment):**

> Measured on a single development machine (16 cores; DB / Redis / worker / load generator co-located; rate limiting disabled): the ingestion API accepts ~313 orders/second and the Celery worker drains ~270 orders/second. Beyond drain capacity the surplus is absorbed by the queue, with the backlog growing linearly and no errors; it is fully consumed within 55 seconds once load falls. 60,000 requests injected over 292 continuous seconds, all landed in ODS, zero failures.

**The statement that should not be used is "this system handles high concurrency"** — it has no boundaries, conflates the four dimensions above, and omits the environment that gives the numbers their meaning.

On whether this "passes": no SLO has been defined for this project, so strictly there is no criterion. What can be said is that against the ordinary scale of an **internal order-ingestion service**, 270/s is roughly 23 million orders per day and does not constitute a bottleneck; and that overload behaviour is queueing rather than collapse. **The latter is more worth recording than the capacity figure, because most systems fail not from insufficient capacity but from failing the moment capacity is exceeded.**

### 7. What this document and this test do not represent

This section matters as much as point 6's numbers, because whoever cites a figure does not automatically inherit the premises that produced it.

- **Not production performance.** Single machine throughout, no real network latency, data volume only reaching 77,380 rows. `ods.order_id` carries a unique index; writes slow down at tens of millions of rows, and 270 would need revising downward.
- **Not an upper bound.** See the ⚠️ in point 6. All four dimensions were sampled at one operating point; none was pushed to failure.
- **Not the behaviour under the deployed rate limit.** Tests ran with `RATELIMIT_ENABLED=false`; production is `60/minute`, i.e. one per second per upstream. **Capacity 270, policy locked at 1.** The two do not conflict, but must not be conflated — what this document means for a future limit increase is that the bottleneck will not be the API.
- **Not the behaviour under failure.** DB, Redis and worker were healthy throughout; no fault injection was performed. Disconnection, broker outage and worker crash are covered by [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) and [2026-08-10-circuit-breaker-before-after](./2026-08-10-circuit-breaker-before-after.md), and are **out of scope here**.
- **Not long-run stability.** The longest single load was 292 seconds. Memory leaks, connection leaks and queue fragmentation take hours to surface.
- **Not an SLA and not a commitment.** This is a measurement record whose validity expires with the code it measured — the way this document overturns 2026-08-03 is exactly the way it will itself be overturned.

### 8. Extrapolated ceiling on enterprise-grade hardware (unverified) ⚠️

**Everything below is extrapolation; none of it was measured.** It is recorded to provide a reasoning starting point and a checklist, not a number.

This measurement environment carries a structural contamination: **PostgreSQL, Redis, the Celery worker, 4 uvicorn processes and 4 load-generator processes all shared 16 cores** — and the load generator alone consumed roughly 4 of them (see Test 0, trap 2). In other words, **the system under test never had the machine to itself.**

Three axes can be reasoned about separately:

| Axis | Scales linearly? | Basis |
|---|---|---|
| **Moving the load generator off-box** | — | Not scaling, but removing measurement contamination. Direction is certainly up; magnitude unknown |
| **API horizontal scaling** | Yes | API processes are stateless (recovery scanning moved to Beat). Test 3 observed near-linear scaling from 1→4; the regression at 8 is **insufficient cores**, not an architectural limit, and that inflection moves right with more cores |
| **Worker horizontal scaling** | Yes | `try_claim_raw`'s CAS (`rowcount == 1`) guarantees a given `raw_id` is claimed by exactly one worker, so adding worker containers requires no coordination |

**The next bottleneck will almost certainly move to PostgreSQL.** Each order is roughly four writes:

1. `INSERT INTO raw` (API)
2. the claim's `UPDATE raw SET status='processing'` (worker)
3. `INSERT INTO ods` (worker)
4. `UPDATE raw SET status='processed'` (worker)

270 orders/s ≈ 1,080 writes/s. A single PostgreSQL primary on dedicated hardware (NVMe, adequate shared_buffers) typically handles simple writes in the thousands-to-tens-of-thousands per second range — **which translates to somewhere in the hundreds to low thousands of orders per second.**

⚠️ **How unreliable that range is, stated explicitly:**

- It assumes write cost does not grow with data volume, but maintaining `ods.order_id`'s unique index gets more expensive as the table grows, and `raw.raw_payload` is TEXT, so large payloads trigger TOAST
- It ignores autovacuum load — two of the four writes are `UPDATE`s, which in PostgreSQL produce dead tuples
- It assumes fsync cost is negligible, which depends entirely on the disk
- It accounts for no real network latency, connection establishment, or multi-AZ round trips

**Beyond that point, adding machines stops helping** and the write path itself has to change: batch writes (`COPY`), splitting `raw` and `ods` onto separate instances, or partitioning/archiving `raw` to bound index size. **That is a different class of design problem, and this project's volume is nowhere near it.**

**Turning any figure above into fact requires, at minimum: deploying the DB separately, moving the load generator to another machine, loading data to tens of millions of rows, then re-running Test 5 and watching when the backlog curve's slope starts degrading.** Until then, point 6's observations are the only part of this document backed by data.

---

## What this overturned

⚠️ **First, about this document itself**: conclusions 2 and 3 were overturned later the same day by [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) — that record measures the system after the `async def` + blocking-DB defect identified here was fixed. Conclusions 1, 4, 5 and 6 still hold.

**[2026-08-03](./2026-08-03-load-test-ingestion.md) Test 2 is overturned.** That record documented 5 HTTP 500s at C=500, attributed to pool exhaustion; re-run here with identical parameters, all 1000 requests succeeded with a peak of 12 connections.

**What overturned it is not measurement error but an architectural refactor.** At the time, `POST /orders` used FastAPI `BackgroundTasks`, and `process_raw_event` was synchronous — Starlette dispatches such functions into an anyio threadpool of 40 by default. That meant up to 40 full "Raw→ODS clean and write" operations running concurrently, **all sharing the API's 15-connection pool**; `db.close()` was in `finally`, so the transaction opened by `refresh` stayed held throughout.

After `8485f64` (2026-08-10, dispatch moved to Celery), the API process does one INSERT and explicitly returns its connection before dispatching. **The pressure source disappeared entirely.**

⭐ This is the same class of event as Test 5 (SIGKILL) being overturned by [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md): **a verification record's shelf life is set by the lifespan of the code it verified.**

## Related

- [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) — overturns conclusions 2 and 3 of this document
- [2026-08-03-load-test-ingestion](./2026-08-03-load-test-ingestion.md) — this document overturns its Test 2
- [design/queue](../design/queue.md) — how CAS claim and redelivery interact
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0005](../adr/0005-first-write-wins-idempotency.md)
