# Task Queue: Celery + Redis

## Scope

This document covers the **ingestion path's task queue** — how a Raw record gets handed to
background processing after `POST /orders` accepts it, what happens when something dies
mid-flight, and who is responsible for picking up the pieces.

Each layer's own contract lives elsewhere: quality contracts in
[DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md), analytics-pipeline orchestration in
[ORCHESTRATION.md](./ORCHESTRATION.md), E/L in [CLOUD_LAYER.md](./CLOUD_LAYER.md).

```
POST /orders ──► Raw (PostgreSQL) ──► Redis ──► Celery worker ──► ODS
                      ▲                            │
                      └──── Celery Beat ───────────┘   ← periodic recovery scan
                            (tasks.scan_and_dispatch)
```

**This is orthogonal to Airflow.** [ORCHESTRATION.md's *Scope*](./ORCHESTRATION.md) already
pins the distinction down: Airflow schedules minute-to-hour batches (extraction + dbt); this
handles millisecond-to-second single records. The only thing the two share is the word
"Redis" — and they deliberately **do not share an instance**, because sharing entangles the
failure domains: the analytics broker filling up must never stall live ingestion.

---

## 1. Why replace BackgroundTasks

`BackgroundTasks` is a **pure in-memory queue**. The README's stress test #5 measured its
floor: after SIGKILL, 150 records sat in `pending` forever, with nothing on restart aware
they needed reprocessing.

Four layers of retry and the recovery scan cover "processing failed"; they cannot cover
"the task itself vanished" — that is a property of process memory, not a matter of retry
counts. A durable queue is the actual fix.

It also unblocks horizontal scaling: `BackgroundTasks` and the periodic scan were both
in-process state, which pinned the API to `--workers 1` (each extra process would run its
own scan loop).

---

## 2. Core design decisions

| Decision | Choice | Why |
|---|---|---|
| Task vs. business logic | **Thin wrapper** (`tasks.py` wraps `process.process_raw_event`) | `process.py` stays Celery-free, so pytest, scripts, and the "broker is down, run it by hand" rescue path can call it directly; swapping queues touches only the wrapper |
| Result backend | **None** (`task_ignore_result=True`) | The source of truth for task state is PG's `raw.status`, queryable via `GET /raw/{raw_id}`. A Redis result store would be a second truth that drifts |
| Ack strategy | `acks_late` + `reject_on_worker_lost` | See the recovery matrix in §3: strictly better for "died before claim", neutral for "died after claim" |
| Prefetch | `worker_prefetch_multiplier=1` | Standard companion to `acks_late`. Prefetching means one worker's crash strands the whole batch it grabbed |
| Serialization | **JSON only** | Unpickling is arbitrary code execution; a writable broker would become RCE |
| Celery-level retry | **None** | `process.py` already has four retry layers; stacking another gives 3×3 retry amplification, and `process_raw_event` never raises, so Celery cannot see failure anyway |
| Redis persistence | **appendonly off** | The scan is already the database-level backstop; losing a message means "delayed to the next round", not lost. Paying fsync for a guarantee that already has a fallback is a bad trade |
| Ingestion when broker is down | **Return 200 `pending`, never 500** | The Raw is already committed. A 500 makes the upstream resend, flooding in Raws with the same `order_id` that all end up `duplicate` — while the data was accepted all along |

### 2.1 The broker wait must be bounded ⭐

`_enqueue` is a synchronous call sitting inside an async endpoint. Measured after
`docker compose stop redis`: a single `POST /orders` **blocked for 19 seconds** — kombu's
connection retry fell through to OS-level DNS/TCP timeouts, and it blocked the entire event
loop (under `--workers 1`, that is a full service stall).

Two fixes, both required:

1. **Bound the timeouts**: `socket_connect_timeout` / `socket_timeout` /
   `broker_connection_timeout` all 2s, `task_publish_retry_policy.max_retries=1`.
   19s → 3.81s (= 2s × 2 attempts).
2. **Get it off the event loop**: every async call site uses
   `await asyncio.to_thread(_enqueue, ...)`.

Verified: with the broker fully down, `POST /orders` still returns 200 (3.81s) while
`/health` answers in 1.7ms — the loop is no longer blocked.

**That 3.81s is the degraded-mode latency for a fully dead broker**, not the normal path.
Normally publishing is sub-millisecond.

### 2.2 Dispatch must be circuit-broken ⭐

That 3.81s is the **single-request** figure. Concurrency reveals the real shape:

| Concurrency | Without a breaker |
|---|---|
| 1 | 3.8s |
| 8 | 12.8s each |
| 48 | **47 of 48 requests did not complete within 120s** |

The degradation is **super-linear**: kombu's producer pool is capped at 10 per process, and
with the broker unavailable every acquisition re-pays the connection timeout, so higher
concurrency means longer queueing behind each other.

**The danger is not the latency — it is silent success.** The Raw commit happens before the
dispatch, so all 47 timed-out requests had in fact already persisted their data — while the
client saw only a timeout. A timeout is semantically *undefined*, so the upstream can only
resend, producing a second and third Raw for the same `order_id` that end up `duplicate` at
the ODS layer. The signal itself is accurate (the upstream really did resend), but we caused it.

The failure mode is **unbounded latency rather than fast rejection**: no 429, no 503, no
`Retry-After` — the upstream receives nothing it can act on.

`circuit_breaker.CircuitBreaker` closes this off: after a threshold of consecutive failures the
circuit opens and `_enqueue` returns False immediately without touching Redis. It changes no
external contract (still 200 `pending`, still picked up by the scan); it changes only the
**cost**, making "entering the fallback" cheaper than the fallback itself. The design
trade-offs (why the state is per-process, why half-open must be single-flight, why the lock
never spans the call) are documented in that module.

Measured results in §5.6.

### 2.3 No DB transaction may span the dispatch ⭐

In `create_order`, the `db.refresh(raw)` following `db.commit()` **opens a new transaction** to
fetch the id, and that transaction only ends at `db.close()`. The dispatch sat in between — so
during a broker outage every waiting request pinned a connection in `idle in transaction`
(measured: 23 of 32 pool slots at 60 concurrent).

Two costs: once the pool is saturated new requests can only wait out `pool_timeout`; and those
long transactions hold back PostgreSQL's vacuum horizon, accelerating bloat on a
high-write-volume table.

The fix is to read `raw.id` out and then close the session explicitly, ending the transaction
before the dispatch.

---

## 3. How CAS claim interacts with redelivery ⭐

This is the least intuitive part of the design, and the part most likely to be torn out by
someone reasoning "we have a durable queue now, so we don't need this".

At-least-once delivery means the same message may arrive twice. That is safe in itself —
`try_claim_raw`'s CAS (`UPDATE ... WHERE status='pending'`, checking `rowcount == 1`) plus
`UNIQUE(ods.order_id)` / `UNIQUE(ods.raw_id)` already prevent double processing.

**But redelivery does not recover every crash.** Contrast the two moments a worker can die:

| Crash point | `raw.status` | After redelivery | Who can recover it |
|---|---|---|---|
| **Before** claim commit | `pending` | CAS succeeds → processed normally | **The queue itself** (seconds) |
| **After** claim commit | `processing` | CAS fails (`rowcount == 0`) → task returns immediately | **Only the stale scan** (`STALE_PROCESSING_MINUTES` = 10 min) |

Two consequences, both important:

1. **`acks_late` is worth enabling.** It is strictly better in the first case (seconds vs.
   waiting for the next scan) and neutral in the second. Its cost — duplicate delivery — is
   exactly what the existing idempotency already absorbs.
2. **`scan_and_recover` must not be removed, and matters more than before.** Its role shifts
   from "the primary recovery mechanism" to "**the complement of the queue's semantics**" —
   it exists to handle the half the queue cannot recover on its own.

> ⚠️ If you are about to remove the periodic scan because "we have a durable queue now",
> read that table first. Without it, the second row stays stuck in `processing` **forever**.

### 3.1 Staleness must be judged by `processing_started_at`, not `received_at` ⭐

Recovering the second row above depends on the stale scan, and what that scan needs to ask is
**"how long has this attempt been running?"** `received_at` answers a different question —
**"how long has this record been sitting around?"** The two are nearly identical in normal
operation (ingest, then process immediately) and **wildly different under backlog — which is
exactly when this check matters most**.

Using `received_at` produces the following timeline (reproduced live; numbers in §5.4):

```
T-30min  Order ingested; received_at = T-30min; stays pending because the broker is down
T+0      Broker recovers, the scan dispatches; worker A claims it → status = processing
T+0.01   Worker A is cleaning and building the ODS row (not committed yet)
T+0.02   Next scan tick: status='processing' ✓ AND received_at < now()-10min ✓
         → declared stale → reset to pending → a fresh message is dispatched
T+0.03   Worker B claims it: the status is pending again, so CAS succeeds ← nothing blocks it
         Two workers are now running the same raw_id
T+0.05   A commits first: ods.raw_id lands, raw.status = 'processed'
T+0.06   B collides with the ODS row *it wrote itself*, is judged a duplicate,
         and overwrites 'processed'
```

Three points deserve emphasis:

1. **CAS did not fail.** It guards against contention *within one state*; it cannot guard
   against a third party rolling the state back to `pending`. Once the ticket is revoked
   mid-processing and reissued, a second claim is a perfectly legal move.
2. **The second worker did not come from Celery redelivery** — it came from a brand-new
   message the scan itself dispatched. Plain redelivery is in fact safe: CAS makes it
   `rowcount == 0` and the task returns immediately.
3. **The data stays correct; the signal does not.** ODS's `UNIQUE(raw_id)` /
   `UNIQUE(order_id)` block the double write, but an order that genuinely succeeded ends up
   labelled `duplicate` — contaminating the deliberately preserved "upstream resent it"
   monitoring semantic (see CLAUDE.md's architecture constraints). Plus one batch of wasted
   work, performed precisely while the system is trying to catch up.

Switching to `processing_started_at` (stamped by `try_claim_raw` on a successful claim) makes
the clock start at "processing began", independent of how long the record sat around, so step
`T+0.02` no longer holds.

**Self-collision therefore becomes unreachable**: `try_claim_raw` is the only thing in the
codebase that writes the `processing` state, and the stale scan is the only thing that rolls
it back to `pending` (`/process_raw?force=true` accepts only `error` / `duplicate`). Plug that
single reversal path and the symptom has no source left — which is why **no separate
"self-collision" signal is needed**; it would be a metric permanently pinned at zero.

> **Invariant**: `status='processing'` ⇒ `processing_started_at` is not null. Enforced by
> `try_claim_raw`, and established for pre-existing rows by migration `e5f6a7b8c9d0`'s
> backfill. If the invariant is broken (e.g. by hand-editing the DB), that row will never
> satisfy the stale condition and will hang forever.

---

## 4. Why the recovery scan lives in Beat, not the API

`_periodic_scan` used to be an asyncio loop attached to the FastAPI `lifespan`. That is
**in-process state**: once the API runs multiple uvicorn workers, every process runs its own
scan. Moving it to Beat leaves the API process holding no background state at all — which is
what unlocks going past `--workers 1`.

**Why not Airflow**: a 5-minute schedule is well within Airflow's reach, but the failure
domain would be wrong — the ingestion path's self-healing must not depend on the analytics
orchestrator being alive. And [ORCHESTRATION.md](./ORCHESTRATION.md) opens by stating that
Airflow is not a task queue.

**Beat fires one scan immediately on startup** (`beat_init` signal). Beat's first tick
otherwise waits a full interval (300s by default), leaving anything left over from the
previous round unattended for five minutes — precisely the gap the old lifespan startup
recovery was filling. It hangs off Beat rather than the API's lifespan because *the scheduler
restarting* is the moment a catch-up scan is warranted: an API restart implies nothing needs
recovery.

**Beat must run as a single instance.** Multiple beats each dispatch their own scans; the
result is harmless (`scan_and_recover` is idempotent, CAS blocks double processing) but
wasteful. Also avoid `celery worker -B`'s embedded beat — the docs explicitly call it
unsuitable for production.

### 4.1 The scan itself must also be bounded ⭐

The circuit breaker (§2.2) keeps ingestion at full speed through a broker outage — at the cost
of `pending` accumulating at full speed too. A ten-minute incident can pile up hundreds of
thousands of records. The scan used to be "select every pending row, dispatch them one by one",
which simply relocates the collapse ingestion just avoided onto the recovery path.

Three limits, all required:

| Mechanism | What it prevents |
|---|---|
| **`LIMIT` + an `id` cursor** (`SCAN_BATCH_SIZE`) | Memory is a function of batch size, not of total backlog |
| **Cap on pages per run** (`SCAN_MAX_ROUNDS`) | One incident cannot monopolise a worker slot; whatever is left waits for the next tick |
| **A Redis lock** | Beat dispatches on schedule regardless of whether the previous run finished; overlap sends two messages per record |

**Why the cursor is required and `LIMIT` alone is not**: dispatching does not change `status` —
a record only leaves `pending` once a worker claims it. So every round would fetch the same
leading rows and never reach the rest. The query must advance with `id > after_id`.

**The grace period** (`PENDING_GRACE_SECONDS`): freshly ingested `pending` rows are left alone.
Under normal conditions the ingestion path dispatches them within milliseconds, so the scan
stepping in merely adds a second message for the same record. `received_at` is the right basis
here — the question really is "how long has this record been sitting around"; do not confuse it
with §3.1's staleness check, which asks "how long has this attempt been running".

**The lock is an optimisation, not a correctness requirement**: if acquiring it fails (a Redis
hiccup) the scan runs anyway; only an explicitly held lock causes a skip — skipping leaves
records unattended, which is far worse than duplicate dispatch. If the scan task is SIGKILLed
the lock lingers until its TTL (one scan interval), costing one tick.

**Tuning the cap**: the upper bound is `SCAN_MAX_ROUNDS × SCAN_BATCH_SIZE`, which is effectively
**the recovery path's dispatch rate limit** (at most this many per scan interval). Tune it
against worker consumption rate — dispatching faster than workers consume only grows the queue;
it does not drain the backlog sooner.

---

## 5. Live verification (2026-08-10) ⭐

Every number below was measured, not inferred at design time. Environment: the full docker
compose stack (api / worker×4 / beat / redis / postgres) with `SCAN_INTERVAL_SECONDS=20` to
shorten the observation window.

### 5.1 SIGKILL: overturning README stress test #5

800 `pending` records injected; once Beat had dispatched them and the worker had processed
225, `docker compose kill -s SIGKILL worker`:

| Moment | `pending` | `processing` | `processed` |
|---|---|---|---|
| At SIGKILL | 537 | 2 | 261 |
| 30s after worker restart | 0 | **2** | 798 |
| `processing_started_at` backdated 11 min, one scan later | 0 | 0 | **800** |

Final ODS count: 800. **Nothing lost.**

That table is §3's recovery matrix made concrete: the 537 still in the queue drained
themselves via redelivery, while the **2 that were mid-processing at SIGKILL stayed stuck in
`processing` — restarting the worker could not save them.** Only the stale scan could. (The
third row backdates `processing_started_at` to simulate the 10-minute threshold rather than
idling through it.)

Against the old conclusion in README stress test #5 — "150 records stuck in pending forever,
no automatic recovery on restart" — this item is now overturned.

### 5.2 Beat startup catch-up

Beat started at `05:58:38`; the scan dispatched by `beat_init` reached the worker at
`05:58:39`, while the first scheduled tick came at `05:58:58` (+20s). The catch-up does close
the first interval's gap.

### 5.3 Ingestion with the broker down

See §2.1. `POST /orders` → HTTP 200 + `pending` (3.81s), data landed; `/health` 1.7ms. After
Redis came back, the 2 stranded records were picked up by the scan and completed.

### 5.4 Staleness basis: before and after

The same script over the same data; only §3.1's basis column differs. 2000 records inserted as
`pending` with `received_at = now() - 30 minutes` (simulating the backlog left by a long broker
outage), with `SCAN_INTERVAL_SECONDS=5` to tighten the scan cadence:

| | `processed` | `duplicate` | Self-collisions |
|---|---|---|---|
| Before (basis `received_at`) | 1998 | **2** | **2** |
| After (basis `processing_started_at`) | **2000** | 0 | **0** |

Self-collisions are identified by `raw.status='duplicate'` ⋈ `ods.raw_id = raw.id`. The two
offenders' `error_message` literally read "already written by raw_id=1998" — where 1998 *is*
their own id — and that `order_id` appears exactly once in `raw`, ruling out an upstream resend.

**On magnitude**: the number of records hit per scan tick ≈ the number concurrently in
`processing` ≈ worker concurrency, independent of total backlog size (a single record takes
~40ms, far below the scan interval). So this is "rare but real", not wholesale contamination —
but it specifically strikes while the system is catching up.

The §5.1 SIGKILL scenario was then re-run to confirm **the recovery mechanism itself was not
broken**: the 2 records stuck in `processing` were reclaimed as before, ending with all 2900
records `processed`, 2900 ODS rows, and 0 self-collisions.

### 5.5 Rate limiting across multiple processes

With the api running 4 uvicorn workers, 100 `POST /orders` sent against the same API key
(limit `60/minute`):

| Counter storage | 200 | 429 |
|---|---|---|
| Process memory (`RATE_LIMIT_STORAGE_URI=`) | **91** | 9 |
| Redis db 1 | **60** | 40 |

Memory mode let 91 through instead of 60 — each worker keeps its own counter, and the OS does
not distribute connections evenly (hence not a tidy 4×). **No error is raised anywhere**; the
limit simply fails silently, which is exactly what makes it dangerous. With shared storage it
lands exactly on 60.

While Redis is down slowapi logs `falling back to in-memory storage` and degrades to
per-process counting; on recovery it logs `Rate limit storage recovered` and the limit is back
to a precise 60/40.

### 5.6 Circuit breaker: before and after

Redis fully stopped, 4 uvicorn workers, same script:

| | Result |
|---|---|
| No breaker, 48 concurrent | **47 of 48 did not complete within 120s** |
| Breaker, 48 concurrent | 48/48 returned 200; batch took 18.4s |
| Breaker, sustained load of 200 | **p50 = 5ms, p90 = 7ms**; 14 of 200 exceeded 1s |

The first wave is still slow (18.4s) because with 48 requests arriving simultaneously the
breaker has not opened yet — none of the three failures has *returned*, so the whole wave is
already past the gate. That is inherent: a breaker protects everything after the first wave,
bounding the cost at "one concurrency wave per process" rather than per request. Under
sustained traffic that wave is a few seconds at the start of the incident.

The remaining 14/200 tail is **not the dispatch** — it is the rate-limit storage re-probe.
Broken out: after the circuit opens, `POST /orders` shows 4 requests at 0.02s (dispatch now
free) and 4 at 3.63s, while `GET /raw/1` (rate limiting only) shows all 8 at 3.75s. slowapi's
`_storage.check()` has no single-flight guard, so concurrent requests in one process all pile
into the same probe.

After the broker recovered, each of the 4 processes logged one `circuit_closed` and requests
returned to 10–51ms with no restart. Across the whole incident there were **0** `派工失敗`
error logs (pre-open failures are recorded as breaker state transitions instead), against one
traceback per failed request before the change.

### 5.7 Bounded scanning: measurements

Same environment, worker concurrency 4, Beat disabled so each run could be observed.

**Throughput** (60,000-record backlog, cleared in one run): publishing at roughly
**2,140 msg/s**, workers consuming roughly **305 records/s**. All 60,000 ended `processed`
with 60,000 ODS rows — reconciled exactly.

**Per-run cap and cursor resumption** (120,000 backlog against a 100,000 cap):

| | Dispatched | Remaining `pending` |
|---|---|---|
| Scan #1 | **100,000** (raised the "backlog not cleared" warning) | 20,000 |
| Scan #2 | **20,000** | 0 |

ODS ended up exactly +120,000 with **zero** `duplicate` rows — proving the cursor neither
skipped records nor re-dispatched already-processed ones.

**Overlap protection**: two scan messages fired almost simultaneously against the same backlog;
the second logged `recovery scan 略過：上一輪仍在進行` and dispatched nothing.

**Grace period**: 50 records with `received_at = now()` and 30 from five minutes ago inserted
together; the scan returned only the 30 — the fresh ones are left to the ingestion path.

**Batch publishing helps far less than expected**: measured, `.delay()` reached 2,332 msg/s and
a shared producer 2,563 msg/s — a mere **1.1×**. Celery's `.delay()` already reuses the producer
pool, so acquisition is much cheaper than assumed. The change stays (it costs nothing), but it
is **not** where this section's benefit comes from — pagination, the per-run cap and overlap
protection are.

---

## 6. Known boundaries and deliberate non-goals

| Item | Why not | Trigger |
|---|---|---|
| **Queue splitting** (separate ingest / replay queues) | One queue suffices at current volume; the routing seam (pinned task names) is already in place | When bulk replay starts pushing live ingestion latency |
| **Redis appendonly / clustering** | The scan is already the backstop, see §2 | When the broker stops being optional (e.g. if DB-level `pending` semantics are ever dropped) |
| **Celery-level retry / dead-letter queue** | Failure semantics already land in `raw.status` (`error` is terminal and carries `error_message`) | When "business failure" and "infrastructure failure" need separate retry policies |
| **Flower or similar UI** | With no result backend there is little for Flower to show; `raw.status` is the truth | Evaluate alongside OpenTelemetry (a separate Phase 5 roadmap item) |
| **Back-pressure to upstream when the broker is down** | Current semantics ("accepted, delayed") are correct and require no upstream change | When the DB write itself becomes the bottleneck under backlog |
| **Single-flight for the rate-limit storage probe** | slowapi's `_storage.check()` has no single-flight guard, so while Redis is down concurrent requests in one process all pile into the same probe (measured 3.75s). This is the sole cause of §5.6's latency tail | Once the degraded-mode tail becomes a practical problem; the cheapest first step is lowering the rate-limit storage's `socket_connect_timeout` below 1s |

---

## 7. Runbook

```bash
# Full stack (api / worker / beat / redis / db)
docker compose up -d --build

# Inspect queue backlog
docker compose exec redis redis-cli llen celery

# Current status distribution (the truth lives here, not in Redis)
docker compose exec db psql -U app -d orders -c \
  "select status, count(*) from raw group by status order by status;"

# Trigger a recovery scan by hand (without waiting for Beat)
docker compose exec worker celery -A celery_app call tasks.scan_and_dispatch

# Reprocess a single record (the rescue path when the broker is down: no queue involved)
docker compose exec worker python -c \
  "from process import process_raw_event; process_raw_event(123)"

# Shorten the scan interval to observe behaviour (default 300s)
SCAN_INTERVAL_SECONDS=20 docker compose up -d
```

**What to do about records stuck in `processing`**: do not edit `status` by hand. Let the
stale scan handle it after 10 minutes — that is exactly what it is for. If immediate recovery
is genuinely needed, backdate that record's `processing_started_at` beyond
`STALE_PROCESSING_MINUTES` and wait one scan; semantically that is "declaring this attempt
timed out". **Do not touch `received_at`** — it is the ingestion timestamp, part of the data's
lineage, and has nothing to do with the timeout decision (see §3.1).
