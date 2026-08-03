# ecommerce-data-ingestion-platform
### E-Commerce Order Data Pipeline — Data Lifecycle Management in Practice

**English** | [繁體中文](./README.zh-TW.md)

[![CI](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml)

A data pipeline for e-commerce orders, built around data lifecycle management as its core — ensuring data enters the pipeline as reliably as possible under high concurrency with potential failures and duplicate submissions, progressively transforming untrusted inbound data into trustworthy analytical data through per-layer quality contracts, and achieving cross-pipeline data quality governance and complete lifecycle traceability through rule versioning and quality event tracking.

This project is data engineering first, backend engineering second. The ingestion layer's fault-tolerant design (multi-point retry, crash recovery, CAS claim) ensures data gets in; the layered quality contracts (Raw → ODS → dbt stg/int/dim/fct) ensure data flows correctly; rule versioning and an append-only `quality_events` state machine ensure that the evolution of quality assessments is always auditable.

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Framework | FastAPI |
| ORM | SQLAlchemy |
| Database | PostgreSQL |
| Schema Validation | Pydantic |
| Environment Config | python-dotenv |
| Timezone | pytz |

---

## Architecture Overview

```
[Currently implemented]

POST /orders
    ↓
[Raw Table]  ←── persisted as-is, immutable                      status: pending
    ↓
[Background Task: process_raw_event]
    ├── try_claim_raw()         ← atomic UPDATE, claim the record (CAS)
    ├── JSON parse
    ├── ODSOrder.from_nested()  ← flatten nested payload
    ├── clean_order()           ← format normalisation + business-rule validation
    ├── first-write-wins idempotency check
    └── [ODS] + [quality_events]  ← immutable anchor, with quality flag and event log

[ODS] → incremental extraction (manual today, Airflow in Phase 5) → BigQuery staging
    ↓
dbt stg_*   Hard Gate tests                 ← Silver entry, all records retained
    ↓
dbt int_*   Row Filter                      ← Gold entry, blocking happens here
    ├── effective quality state = clean  → int_orders → int_order_items
    └── effective quality state ≠ clean  → int_orders_quarantine
        (effective state = ODS snapshot ⊕ latest quality_events event, not literal has_clean_error)

[Phase 4–5 target]

dbt int_*   scenario-specific int_orders_*  ← designed; enabled only when a real scenario appears
                                           (accepts errors irrelevant to the scenario, with imputation)
    ↓
dbt dim_*/fct_*/rpt_*
    ↓
Looker Studio (connected to BigQuery dim_*/fct_*/rpt_*)
```

---

## Quality Contract per Layer

Quality control responsibility tightens progressively as data flows downstream. ODS is the immutable anchor that retains all data including dirty records; blocking happens at the `dbt int_*` layer.

```
Raw (PostgreSQL)
  Responsibility : Persist every inbound request exactly as received, no quality assumptions
  Quality requirement : None
  Mutable : No

ODS (PostgreSQL)                               ← Bronze / Anchor
  Responsibility : Basic cleaning + business-rule validation, preserves all data including dirty records
  Quality requirement : Format normalisation applied; business issues flagged, never rejected
  Mutable : No

dbt stg_*                                      ← Silver entry
  Responsibility : 1:1 source mapping, type alignment, column renaming
  Quality requirement : Same as ODS — all records retained including dirty ones

─────────────────── Blocking happens here ───────────────────

dbt int_*                                      ← Gold entry
  Responsibility : Cross-table joins, derived fields, business logic
  Quality requirement : Only clean records pass (has_clean_error = FALSE)
  Destination for dirty records : int_orders_quarantine
  Scenario repair : Scenario-specific models may accept errors irrelevant to their use case, apply imputation, and pass records through for that scenario; repair logic is documented in the model SQL

dbt dim_*/fct_*                                ← Gold
  Quality requirement : Cleanest layer — no records with has_clean_error = TRUE

dbt rpt_*
  Responsibility : Fixed-grain pre-aggregations, optimised for BI dashboards
  Quality requirement : Same as dim_*/fct_*
```

For the full DQ architecture design (blocking mechanism, quarantine handling, remediation paths, historical metrics) see [DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md).

---

## Key Design Decisions

### Data Architecture Decisions

**No business deduplication at the Raw layer**
`Raw.order_id` intentionally has no UNIQUE constraint. The Raw table's responsibility is to record every inbound request as-is — including duplicate submissions — because different submissions may carry complementary fields, and abnormal submission frequency is itself a signal (attack detection, client-side bugs). Deduplication responsibility is delegated to the ODS layer.

**ODS items column uses JSONB; Raw payload uses TEXT**
The Raw layer is a landing zone: it stores the original request body **verbatim** (the raw bytes off the wire, not a re-serialised `model_dump()`), making no assumptions about structure and never silently dropping unknown fields when the upstream schema drifts. `order_id` is the only field **extracted as a key traceability column** for indexing and idempotency lookups; everything else lives untouched inside the payload text. TEXT is the semantically correct choice since the database neither parses nor validates the content. The ODS items field, by contrast, has already passed Pydantic validation and cleaning, so its structure is guaranteed; JSONB adds a second layer of format enforcement at the database level and preserves the option to query into items fields directly in SQL if needed.

**Two-stage cleaning pipeline (`format_clean` + `business_clean`)**
`format_clean()` handles format issues (lowercase normalisation, whitespace stripping). `business_clean()` validates business rules (quantity > 0, rating 1–5, delivery_date ≥ order_date, etc.). The two stages have distinct responsibilities — format standardisation versus business semantic validation — and are kept separate accordingly.

**`has_clean_error` is non-blocking**
Business-rule validation results are recorded in the `has_clean_error` flag rather than used to reject ODS writes. Blocking is delegated to `dbt int_*` for two concrete reasons: 
first, quarantine records are already format-cleaned and queryable by business fields (`format_clean` runs before business validation), so `int_orders_quarantine` supports direct field-level RCA without parsing raw payloads; 
second, Proposal B re-evaluation requires dirty records to exist in ODS — blocking at ingestion would make rule evolution apply only to new data, with no path to promote historical records.

**ODS-layer first-write-wins idempotency**
Only the first submission for a given `order_id` is written to ODS, enforced by **two guards** — the point is that each handles a **different source of duplicates**, not the same job done twice:

- **The UNIQUE constraints (`UNIQUE(ods.order_id)` + `UNIQUE(ods.raw_id)`) — responsible for *correctness***: this is the ultimate idempotency guarantee and **cannot be omitted**. Between the pre-check's `SELECT` and the subsequent `INSERT` there is an unavoidable TOCTOU window in which two concurrent workers both clear the pre-check. The pre-check inherently cannot close that window on its own, for two reasons: ① the project runs at the default **READ COMMITTED** isolation level, which has **no predicate locking** — there is no way to lock a *condition* to stop someone inserting a new row that matches it (a phantom); ② even `SELECT ... FOR UPDATE` cannot lock a row that *does not yet exist* — with no row, there is nothing to lock. Only a DB-level UNIQUE constraint can close this window.
- **The pre-check (query ODS for the `order_id` before committing) — responsible for *the efficiency and semantics of the common case***: for correctness this guard is redundant, but it handles the duplicate source that **dominates in practice and is non-concurrent** — chiefly `scan_and_recover` re-running a stale `processing` record after resetting it, or the scan re-queuing a record already sitting in the queue (see "Known Issues"). These are time-separated duplicates that are *expected* to recur.

Why not rely on UNIQUE alone? Because the success path commits **ODS + quality_event + the Raw `status` together in a single transaction**. Without the pre-check, every duplicate would have to "build the entire ODS object → attempt the commit → hit UNIQUE → abort the whole transaction → roll back → re-issue the status update separately." For the high-frequency scan re-runs, the pre-check intercepts the duplicate with a cheap `SELECT` *before* the object is even built — **taking the fast path instead of triggering a doomed-to-abort transaction every time** — and avoids using the `IntegrityError` exception as normal control flow, since `duplicate` is an *expected, meaningful monitoring signal* in this design, not an error.

The two guards are complementary — **fast path (pre-check) + backstop (UNIQUE)** — not an either/or. Subsequent duplicate Raw records are never rejected; they are written to a `duplicate` terminal status so monitoring can distinguish normal processing from intercepted duplicates.

**How the two guards relay under a TOCTOU race** (two workers concurrently processing the same `order_id`):

```
        Worker A (raw_id=1)              Worker B (raw_id=2)
          │                                │
 t1       │ SELECT order_id → not found    │
          │                                │
 t2       │                                │ SELECT order_id → not found
          │                                │   ← READ COMMITTED can't see
          │                                │     A's not-yet-committed row
          │ ── both clear the pre-check (the window) ──
          │                                │
 t3       │ INSERT ODS                     │
          │ commit ✔ (first write wins)    │
          │                                │
 t4       │                                │ INSERT ODS → hits unique index
          │                                │   blocks, waits for A's outcome
          │                                │
 t5       │                                │ A committed → IntegrityError
          │                                │   rollback → mark duplicate ✔
          ▼                                ▼
   exactly one row for that order_id in ODS; raw_id=2 → duplicate terminal
```

Note: the window stretches from t1 until A actually commits at t3 — any worker that runs its pre-check before then finds nothing and clears it, which is precisely why the UNIQUE backstop cannot be omitted. (If A rolls back at t5 instead of committing, B's INSERT succeeds and takes its place, still honoring first-write-wins.)

**Bypassing Raw to land directly in ODS is strictly forbidden (single-ingress invariant)**
This service is the ingestion unit of a data mesh; its callers are a few stable machine-to-machine upstreams with no human users (see *Service-to-service authentication decisions*), and onboarding a new source is a planned infrastructure event. That positioning removes the legitimate motive for ad-hoc "write straight to the DB" bypasses — so the system makes "all data enters through Raw" a hard invariant: **ODS is always a product of the pipeline and accepts no row that bypassed Raw**.
The rationale is not merely "feasible" but "necessary": ① `source_client_id` (the lineage origin) is resolved by the auth layer, so a row bypassing Raw has no way to establish its source; ② Raw's verbatim retention is the root of the "rebuildable" promise (Proposal C re-derives values from Raw), so a row with no Raw anchor cannot be rebuilt; ③ `has_clean_error` / schema drift / the `quality_events` initial evaluation are all produced by `process_raw_event`, so a direct ODS write would inject rows that appeared from nowhere into the quality state machine. Notably, even the "manual replay / backfill / direct DB write" that the schema tolerates is modeled at the **Raw layer** (`Raw.source_client_id` being NULL denotes a Raw row of unknown provenance), not at ODS — the design already confines "direct writes" to Raw.
This invariant has been promoted from policy to DB guarantee: `ods.raw_id` is **NOT NULL** + an **FK `ods.raw_id → raw.id` (`ON DELETE NO ACTION`)**. The former blocks "orphan rows with no anchor", the latter blocks "fabricating a non-existent raw_id and writing directly", and guarantees a raw row cannot be deleted ahead of its ODS child (incidentally protecting Proposal C's rebuild precondition).

**raw_id is physical identity, order_id is business identity — which drives the downstream dedup key choice**
ODS's two unique keys play different roles: **`raw_id` is the physical/surrogate identity** (a surrogate key minted by the landing layer, 1:1 with a single physically-landed record, immutable, the lineage anchor); **`order_id` is the business natural key** (its uniqueness is a *current business rule* that may evolve — order versioning, return line splits, SCD, etc.).
This distinction directly determines the **downstream dedup key**: dedup is fundamentally "collapse the multiple physical copies of the same physical record into one" — a physical-identity operation — so dbt `stg_` dedup keys on **`raw_id`** as its grain, not `order_id`. BQ staging is an append-only mirror; Proposal C correction rows and routine re-extraction both produce physical duplicates of the same record, and the basis for collapsing them is "the same physical row" = raw_id. Keying on order_id instead would couple physical dedup to a business constraint that may relax — the moment order_id uniqueness loosens, it would silently swallow rows that should coexist. Downstream `int_*` also keys on raw_id when composing the "effective quality state" by joining `quality_events`, consistent with the dedup grain.
This is also the implicit reason `raw_id` must be NOT NULL: `UNIQUE` in PostgreSQL **does not reject NULLs** (it allows arbitrarily many), so `UNIQUE + nullable` would let raw_id-grained dedup collapse multiple NULL rows into one and silently lose data; tightening to NOT NULL (+ the FK) closes that gap.

**`Raw.status` and `ODS.order_status` are unrelated**
`Raw.status` is the pipeline state machine (`pending → processing → processed / error / duplicate`), driven by `try_claim_raw` and `_commit_raw_status`. `ODS.order_status` is a business field carried in from the inbound payload — it describes the order's fulfillment state at the moment of ingestion (e.g. `"confirmed"`, `"pending_payment"`), not the pipeline's processing progress. This API handles order creation events only; status changes originating from other systems (payment, fulfillment, customer service) are out of scope and would be joined at the dbt layer.

**`force=True` semantic boundary: single-record retry, not backfill**
`POST /process_raw/{raw_id}?force=true` is only permitted on `error` or `duplicate` records — its semantics are "retry this failed record". Calling it on a `processed` record returns 400, because if downstream systems (Star Schema, aggregation tables) have already consumed that ODS record, deleting and rewriting it in isolation cannot cascade corrections downstream and would introduce inconsistencies instead. Quarantine records (`has_clean_error = TRUE`, `status = "processed"`) have a rule evaluation problem, not a pipeline failure — the correct remediation path is Airflow re-evaluation (Proposal B), not re-running the pipeline.

### Ingestion Layer Reliability Decisions

**Atomic claim logic (`try_claim_raw`)**
Uses `UPDATE ... WHERE status = 'pending'` and checks `rowcount == 1` so that only one worker can ever claim a given `raw_id` under concurrency, without pessimistic locking. Note it guards an **orthogonal dimension** to the ODS idempotency below: the idempotency guards (pre-check + `UNIQUE(order_id)`) key on **order_id (business identity)** and intercept "different raw_id, same order" business duplicates; CAS keys on **raw_id + status (physical identity)** and intercepts "the **same raw_id** scheduled to multiple workers" — precisely the duplicate source described in Known Issues, where the scan can't see the BackgroundTasks queue and may re-schedule an already-queued record. Its value has two sides:

- **Efficiency — the earliest, cheapest exit point**: the losing worker gets `rowcount=0` and returns at the very top of `process_raw_event`, *before* any JSON parse / flatten / clean / ODS-object build. This is earlier than the order_id pre-check (which must build the entire ODS object before it queries order_id), so for the high-frequency "same raw_id re-scheduled" case CAS is the lowest-cost interception point — what it saves is the whole pipeline's wasted work plus the DB round-trips.
- **Correctness — preventing racing writes to Raw.status, avoiding "self-marking as duplicate"**: the idempotency guards only guarantee order_id uniqueness in the **ODS table**; they say **nothing about the Raw.status state machine**. Without CAS, two workers concurrently process `raw_id=1`: A writes ODS and sets `processed`; B hits `UNIQUE(raw_id)`, enters the duplicate branch, looks up the existing row by order_id, and finds that `existing_raw_id` *is itself* — so it marks **itself as "duplicate of itself."** The `processed` and `duplicate` writes race and the final status is non-deterministic. The ODS row is correct (UNIQUE held the line), but the status semantics are broken. CAS makes "exactly one owner per raw_id" hold, so the status transition is deterministic and meaningful.

For pure "no two ODS rows" correctness, `UNIQUE(raw_id)` is already a sufficient backstop; CAS's irreplaceable value lies in the **state-machine correctness** and **efficiency** dimensions. This mirrors the pre-check's relationship to `UNIQUE(order_id)` exactly — CAS is to raw_id what the pre-check is to order_id: a "fast path + state semantics," with a DB-level UNIQUE backstopping it underneath.

**Per-client rate limiting (keyed on the authenticated `client_id`) — no global cap**
The limiter keys on the `client_id` resolved by auth, not on the source IP. The subject we actually want to bound is "a single upstream source" (abnormal submission frequency / a client-side bug), and `client_id` *is* that subject — a stable identity established by the auth layer, immune to network topology. IP is only a proxy for it and distorts the moment topology gets involved: multiple upstreams behind one NAT share a counter (false-positive throttling), one upstream scaled across many IPs gets `N × limit` (the per-client cap silently bypassed), and behind an LB everyone collapses onto the proxy IP (per-IP degenerates into a global limit). Keying on `client_id` sidesteps all three.

This is feasible because **auth runs before the limit check**: `@limiter.limit` wraps the endpoint and its check runs at the top of that wrapper — *after* FastAPI has resolved dependencies. So an unauthenticated request is rejected with `401` before the limiter ever counts it (it never enters a counter), and `verify_api_key` has already stashed `client_id` on `request.state` by the time the key function reads it. A side benefit of this ordering: only validated clients ever create counter entries, so a flood of spoofed source IPs can't grow the in-memory limiter storage.

**Limit semantics**: because the key is `client_id`, each limit is now **per upstream, aggregated across all of its keys and all of its IPs** — not per IP. For the current few-stable-single-instance upstreams this is effectively the same number; if an upstream later scales horizontally, the limit must be re-calibrated to "the whole upstream's fair share." A future refinement for per-instance containment is a composite `client_id × IP` key (see the deployment note below).

A global limit is deliberately omitted: it must be derived from "expected concurrent active clients × per-client limit" — without real traffic data this number is arbitrary and carries unclear semantics. More fundamentally, a `/minute` window cannot prevent instantaneous bursts, and pool exhaustion is already handled by `SATimeoutError → 503`. Rate limiting's responsibility is narrowed to defending against sustained single-client abuse; anonymous-flood DoS is delegated to the gateway/LB (the limiter sits after auth and never sees it).

**Pool exhaustion → fast fail (503)**
`POST /orders` separately catches `SATimeoutError` (pool failed to acquire a connection) and returns 503 Service Unavailable without entering the retry loop. Pool exhaustion is a resource contention issue, not a DB fault — retrying only makes it worse. Failing fast lets the client back off and retry.

### Service-to-service authentication decisions

**API Key, not user-facing JWT**
This service is positioned as an *ingestion unit* within the data mesh; its callers are a small, stable set of upstream services (machine-to-machine), with **no human users**. The JWT username/password login flow taught in most tutorials is designed for *users* and would be architecturally incoherent here. So we use service-to-service auth: the upstream holds an `X-API-Key`; a match lets it through. Comparison uses `secrets.compare_digest` (constant-time); the key is never logged and never enters `raw_payload`.

**Keys live in `.env` as static config, not a DB management table**
A DB management table (runtime key issuance/revocation) is driven by the "many, volatile clients" reality of public / multi-tenant platforms. Internal ingestion has few, stable trusted sources; onboarding a new source is a **deliberate infra event**, not a runtime concern. The only real churn is key rotation (security hygiene), handled by "one client mapping to multiple valid keys + an overlap window." If the platform later grows to multi-domain / multi-tenant, migrate to an `api_clients` table.

**Auth = the origin of data lineage**
The `client_id` resolved during auth is not just for access control — it lands on Raw and ODS as `source_client_id`, answering "which upstream sent this row." `source_client_id` is immutable ingestion-time metadata (same nature as `dq_rule_version`); it travels with the anchor into ODS so governance / quality analytics past the BQ extraction boundary can slice by source without joining back to Raw. It is determined by the auth layer — **not payload content** — so it is stored as its own column rather than mixed into `raw_payload`.

---

## Ingestion Layer Reliability

Ingestion-layer reliability is provided by three mechanisms working together: multi-point retry for transient failures, scan recovery for records stuck after a crash, and timeout settings to prevent system resources from being exhausted indefinitely.

### Multi-point Retry (exponential backoff, up to 3 attempts)

**Point 1 — Raw write (`main.py`)**
Catches `OperationalError` on `db.commit()`. Uses `asyncio.sleep` to avoid blocking the event loop.

**Point 2 — Processing (`process.py`)**
Retries the full pipeline (JSON parse → flatten → clean → ODS write) on generic `Exception`. `JSONDecodeError` and `ValueError` are not retried — these are data errors that will always fail regardless of retry count.

**Point 3 — Claim (`process.py`)**
Retries `try_claim_raw` on `OperationalError`. Distinguishes between a DB exception (retry) and `rowcount=0` (another worker claimed the record — expected behaviour, no retry).

**Point 4 — Status update (`process.py`)**
All Raw status updates go through `_commit_raw_status()`, which retries on any exception. The success-path commit (ODS + status together) re-adds the ODS object after rollback before retrying. Exhausting all retries logs `CRITICAL`.

### Crash Recovery Scan

**Startup scan** (`lifespan`): Runs once on server start. Queries all `pending` records and re-queues them via `asyncio.to_thread`.

**Periodic scan** (every 5 minutes): Two-step logic:
1. Reset stale `processing` records (stuck > 10 minutes) to `pending` — logs `WARNING` (duplicate ODS write risk is covered by idempotency protection)
2. Collect and re-queue all `pending` records

### Timeout and Rate Limiting

**DB statement timeout (`database.py`)**
Set via `connect_args={"options": "-c statement_timeout=30000"}` on each connection. Ensures any SQL that hangs beyond 30 seconds (e.g. a lock wait) raises `OperationalError`, allowing the retry mechanism to take over instead of leaving threads hanging indefinitely.

**Explicit connection pool settings (`database.py`)**
`pool_size=5, max_overflow=10, pool_timeout=30` — same as SQLAlchemy defaults, but now explicit for clarity and future tuning.

**`POST /process_raw/{raw_id}` converted to background task (`main.py`)**
Changed from calling `process_raw_event(raw_id)` directly to `background_tasks.add_task`, consistent with `/orders` and no longer blocking the event loop.

**Per-client rate limiting (`slowapi`)** — keyed on the authenticated `client_id`, with an IP fallback when no `client_id` is present (e.g. an unauthenticated path / a misconfiguration guard).

| Endpoint | Per-client limit | Reason |
|---|---|---|
| `POST /orders` | 60/minute | Guards against abnormal submission frequency from a single upstream; well above any legitimate order rate |
| `POST /process_raw/{raw_id}` | 20/minute | Manual replay — inherently low frequency |
| `GET /raw/{raw_id}` | 120/minute | Read-only, more lenient |

Requests exceeding the limit receive `429 Too Many Requests`. The limit is per upstream (across all of that upstream's keys and IPs), not per IP — see *Limit semantics* under Design Decisions.

**⚠️ Deployment note**
Keying on `client_id` makes the limiter **immune to the proxy-IP problem**: behind Nginx / an LB, `request.client.host` becomes the proxy IP and a per-IP limiter would collapse all callers onto one counter — but a per-`client_id` limiter is unaffected by network topology, because the key comes from the auth layer, not the transport. The IP fallback only engages when there is no authenticated `client_id`; if you ever rely on that path behind a proxy, the fallback's `get_remote_address` must be updated to read `X-Forwarded-For` with appropriate trusted-proxy configuration.

**Future direction — composite `client_id × IP` key**: a pure `client_id` key bounds the upstream's *aggregate* fair share but cannot contain a single rogue instance of a multi-instance upstream (all its instances share one bucket). The faithful key for "isolate the tenant *and* contain a single misbehaving instance" is a composite `client_id × IP`, typically as two stacked limits (a per-`client_id` aggregate cap plus a per-`client_id × IP` per-instance cap). This is deferred until there is a concrete multi-instance upstream that warrants it (YAGNI); the current `_key_func` seam extends to it without structural change.

Also, `X-API-Key` is sent in clear text in the header, so production **must run over HTTPS** (TLS terminated at the LB / reverse proxy is fine); otherwise the key travels in the clear.

### What retry handles vs. what it does not

| Scenario | Handled |
|---|---|
| Transient DB connection drop at any stage | ✅ |
| `pending` / `processing` records after a crash | ✅ (scan recovery) |
| Connection pool exhaustion (`TimeoutError`) | ✅ `SATimeoutError` caught, returns 503, client retries |
| SIGKILL mid-execution | ❌ Process is dead before any retry logic runs |
| Duplicate `order_id` submitted twice | ✅ Resolved by ODS idempotency |
| ODS duplicate when scan retries a `processing` record | ✅ Resolved by ODS idempotency |

---

## Load Test Results

Five scenarios were tested to validate concurrency behaviour and failure modes.

**Test 1 — 1,000 unique orders, concurrency=50**
Result: All succeeded, completed in 7.9s, zero errors.
Each `POST /orders` only performs a single fast INSERT and releases the connection immediately (hold time < 10ms). concurrency=50 is well within the DB pool's capacity — no queuing occurs.

**Test 2 — 1,000 unique orders, concurrency=500**
Result: P99 latency ~14s, 5 × HTTP 500 errors.
SQLAlchemy's default pool (pool_size=5, max_overflow=10) supports at most 15 concurrent connections. With 500 simultaneous requests, 485 queue for a connection. Any request exceeding `pool_timeout=30s` throws `QueuePool limit reached`. The 5 failures timed out before INSERT — no Raw record was created.

(Currently handled: `SATimeoutError` is caught and returns 503 Service Unavailable immediately, letting the client retry.)

Mitigation options: increase pool size, switch to async SQLAlchemy (asyncpg), or add rate limiting at the API gateway.

**Test 3 — 100 duplicate order_ids, concurrency=100**
Result: 100 Raw records written, 100 ODS records written — all succeeded.
`order_id` on the Raw table has an index but no UNIQUE constraint. Each duplicate is treated as a new ingestion event with its own `raw_id`. CAS lock protects against the same `raw_id` being processed twice, not against business-level deduplication — a known and intentional boundary (business-level deduplication is handled by ODS idempotency).

**Test 4 — 100 workers competing for the same raw_id (CAS lock)**
Result: `raw.status = processed`, ODS COUNT = 1.
`try_claim_raw` issues `UPDATE raw WHERE id=X AND status='pending'`. PostgreSQL row-locks on this UPDATE — only the first worker gets `rowcount=1`, the remaining 99 get `rowcount=0` and return immediately. ODS is written exactly once.

**Test 5 — Server crash (SIGKILL) mid-processing**
Result: 150 records stuck in `pending`, no automatic recovery after restart.
`BackgroundTasks` is an in-memory queue — task state is not persisted. After SIGKILL, pending DB records have no mechanism to trigger reprocessing.

Two stuck-state scenarios:

| Stuck status | Trigger condition |
|---|---|
| `pending` | Crash before background task runs, or `try_claim_raw` DB exception (transaction rollback) |
| `processing` | Crash after claim commit, before final status update |

Mitigation options: replace BackgroundTasks with Redis/Celery/Kafka (recovery scan is now in place, but a persistent queue remains the proper fix).

**Test 6 — Duplicate order_id submissions (ODS idempotency)**

Scenario A (sequential): the same `order_id` is submitted twice. The first write succeeds normally. When the second is processed, a pre-check finds the `order_id` already in ODS and marks the Raw record `duplicate` — ODS is not written again.

Scenario B (TOCTOU race): two workers both pass the pre-check simultaneously; the first commits ODS, the second hits an `IntegrityError` on commit — caught without retry, marked `duplicate`.
Result: ODS always contains exactly one record per `order_id`; all subsequent duplicate Raw records reach a `duplicate` terminal status, giving monitoring a clear signal distinct from normal processing.

---

## Continuous Integration (CI)

Every push to `main` and every Pull Request automatically triggers GitHub Actions (`.github/workflows/ci.yml`), which installs dependencies and runs the full test suite (248 tests, 100% source coverage) across a **Python 3.10 and 3.12** matrix.

- All tests are unit/integration level (mock DB) — no real database is required, and they finish in seconds.
- Test dependencies are pinned in `requirements-dev.txt` (`-r requirements.txt` + pytest, etc.).

### What CI covers and where it is blind (a deliberate trade-off)

CI automatically verifies **application logic and type contracts**. The **DB-layer contracts** — the CAS claim in `try_claim_raw`, UNIQUE deduplication of repeated `order_id`, post-crash recovery, and drift between Alembic migrations and `models.py` — are **outside CI's automated scope**, because the in-CI tests substitute a mock for the database.

These DB behaviors are currently exercised for reliability via **manual scripts**:

- `load_test.py`: throughput, CAS claim under real concurrency (`--cas-test`), and `order_id` deduplication (`--duplicate`), hitting a real server → real Postgres.
- `restart_test.sh`: `SIGKILL` to simulate a crash, verifying recovery of `pending` records.
- `check_migration_drift.py`: `alembic upgrade head` + `compare_metadata`, a one-command check comparing the schema produced by migrations against `models.py`; exits non-zero if they have drifted.

**Why the database is not wired into CI at this personal-project stage**: the project is currently a solo practice/portfolio piece with no real traffic or concurrency, and the value of DB logic such as CAS / recovery only materializes under genuine concurrency — introducing real-database concurrency integration tests now would cost test-authoring effort plus container-startup flake maintenance, **a cost higher than the present risk of not automating it**. `check_migration_drift.py` is the exception: it is deterministic, concurrency-free, and low-flake, so it *could* run in CI; it is kept manual for now as a deliberate trade-off given solo development, a stabilizing schema, and a low probability of drift — and with its exit-code interface already in place, it can be promoted straight into CI once there is real traffic, a second contributor, or a need to showcase engineering depth.

> ⚠️ **Do not read a green check as "everything is fine"**: a passing CI run only means there is no regression in the **logic layer** — it does **not** mean the dedup / CAS / migration DB contracts have been verified automatically. Those are corroborated by manual scripts requiring human inspection (migration drift now has a one-command check, `check_migration_drift.py`, but it too is not in CI). When changing this logic, re-corroborate with `load_test.py` / `restart_test.sh` / `check_migration_drift.py`.

---

## Known Issues

**Scan may re-schedule tasks that are already queued**
The periodic scan and startup scan collect all Raw records with `status='pending'` and re-schedule them, but the database has no visibility into whether a given record is already sitting in the BackgroundTasks queue waiting to be picked up. Under high traffic, if a burst of requests lands before the queue drains, the scan can re-schedule records that are already queued — resulting in multiple workers racing to process the same raw_id. The CAS claim (`try_claim_raw`) acts as the safety net at execution time: the losing worker receives `rowcount=0` and returns immediately, so ODS is never written twice and correctness is preserved. The cost is wasted thread-pool slots and extra DB round-trips, which adds pressure to the connection pool under load.

**Design direction when switching to a Queue**
The proper fix is to make "already enqueued" visible in the database by introducing a `queued` status into the state machine (`pending → queued → processing → processed/error/duplicate`) and coupling the enqueue action to an atomic status transition: after writing the Raw record, immediately CAS `pending → queued`; only push to the Queue if `rowcount == 1`. The scan then only collects `pending` records (meaning records that never successfully entered the queue) and skips anything already `queued`. The worker's CAS claim shifts accordingly to `queued → processing`. The one edge case this introduces is a failed Queue push after the DB write succeeds, leaving a record stuck in `queued` — the scan must also sweep for stale `queued` records (older than N minutes) and reset them to `pending` so they re-enter the enqueue flow.

**A NUL (0x00) inside a string value would leave a Raw record stuck in `processing` (fixed)**
During an end-to-end test, posting a payload like `{"order_status": "ok\u0000bad"}` to `/orders` produced a Raw record that was neither written to ODS nor moved to any terminal state — it stayed stuck in `processing` and was re-scheduled by the scan recovery every 10 minutes (a poison-pill).

The root cause is that the same NUL value has different representations at different stages of the pipeline, and the guard and the hazard live in different representation spaces. The ingestion guard in `main.py`, `raw_body.replace("\x00", "")`, only strips an **actual 0x00 byte** from the HTTP body; but `\u0000` on the wire is six legal ASCII characters (`\` `u` `0` `0` `0` `0`) with no 0x00 in it, so the guard strips nothing and `raw_payload` lands normally. The real NUL is only produced later, when `json.loads` in `process.py` **decodes** `\u0000` — at that point `order_status` becomes a string containing an actual 0x00, and writing it to an ODS text column makes PostgreSQL/psycopg2 raise `ValueError: A string literal cannot contain NUL (0x00) characters.` (NUL is storable in neither TEXT nor JSONB). Crucially, this `ValueError` is raised inside the success-path commit loop, which originally only special-cased `IntegrityError` (duplicate) and `DataError` (field overflow); the `ValueError` fell into the generic `except Exception` and was retried as if it were transient — but it is deterministic (retries always fail), so after retries were exhausted the record stayed stuck in `processing` and was repeatedly re-scheduled by the scan, forming a poison-pill.

**Current fix:** add an `except ValueError` branch after `DataError` in the commit loop, treating it the same way — fast-fail to the terminal `error` state (and replayable via `POST /process_raw/{id}?force=true`). This addresses the layer where "a deterministic error is misclassified as transient and retried forever": the record no longer sticks in `processing`, and the poison-pill is gone. The trade-off is that such data is **rejected and never reaches ODS**, semantically consistent with `DataError`.

**Future consideration:** the current behavior is "reject" rather than "accept and flag." If such orders should also land (consistent with this project's non-blocking `has_clean_error` quality philosophy), the alternative is to sanitize NUL out of text fields and nested `items` strings in `clean.py` before the write and flag it with a new clean-error code (e.g. `nul_in_text`), letting the data land tagged for downstream quarantine — directly analogous to the existing NaN/Inf sanitization on `items` (establishing the invariant that "ODS never stores a NUL"). That change touches the cleaning rules and would require bumping `DQ_RULE_VERSION`.

---

## API Endpoints

All endpoints require an `X-API-Key` header (see "Service-to-service authentication decisions"); a missing or invalid key returns `401`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Ingest a new order (writes Raw, triggers background task) |
| `POST` | `/process_raw/{raw_id}` | Manually replay a raw record (`?force=true` resets status) |
| `GET` | `/raw/{raw_id}` | Query raw record status and payload preview |
| `GET` | `/health` | Liveness probe — **no `X-API-Key` required**, returns `{"status": "ok"}` (used by the container healthcheck and future LB/K8s probes) |

---

## Data Flow

```
OrderIN (nested Pydantic)
    └── from_nested() → ODSOrder (flat Pydantic)
            └── clean_order() → ODS (SQLAlchemy model) + quality_events
```

Pydantic handles input validation and schema flattening. SQLAlchemy handles persistence. The two layers are intentionally decoupled.

---

## Project Structure

```
.
├── main.py        # FastAPI app, route handlers
├── process.py     # Background task, state machine, claim logic
├── clean.py       # format_clean, business_clean, clean_order
├── auth.py        # API Key authentication (X-API-Key → client_id)
├── schema.py      # Pydantic schemas (OrderIN, ODSOrder, RawOut...)
├── models.py      # SQLAlchemy models (Raw, ODS, QualityEvent)
├── database.py    # Engine, SessionLocal, Base
├── config.py      # Centralised config (pydantic-settings Settings singleton)
├── alembic/        # DB migrations (env.py wires settings.db_url + Base.metadata; versions/ holds scripts)
├── alembic.ini     # Alembic config (sqlalchemy.url left blank, injected by env.py)
├── pytest.ini     # Test configuration (asyncio_mode, coverage)
├── tests/
│   ├── conftest.py        # Shared fixtures
│   ├── helpers.py         # Mock factory functions and test data
│   ├── test_clean.py      # format_clean, business_clean, clean_order
│   ├── test_schema.py     # ODSOrder.from_nested
│   ├── test_raw_write.py  # Point 1: Raw write retry
│   ├── test_process.py    # Points 2–4: Claim / Processing / Status commit retry; Idempotency; Quality Events
│   ├── test_scan.py       # scan_and_recover, lifespan startup, periodic scan
│   ├── test_timeout.py    # Pool exhaustion, /process_raw, GET /raw, DB settings
│   ├── test_rate_limit.py # per-client rate limiting
│   └── test_auth.py       # API Key auth, rotation, source_client_id persistence
├── DQ_ARCHITECTURE.md     # Data Quality Control Architecture (English)
├── DQ_ARCHITECTURE-TW.md  # 資料品質控管架構設計文件（繁體中文）
├── CLOUD_LAYER.md         # Cloud Layer Architecture: ODS → BigQuery (English)
├── CLOUD_LAYER-TW.md      # 雲端層架構：ODS → BigQuery（繁體中文）
├── ecommerce_dbt/         # dbt transformation layer (stg_/int_/dim_/fct_/rpt_); ops & decisions in its README
├── .env           # DB_URL, API_KEYS (not committed)
├── .env.example   # Environment variable template (committed)
└── .gitignore
```

---

## 📄 Design Documents

| Document | Description |
|---|---|
| [Data Quality Control Architecture](./DQ_ARCHITECTURE.md) | Full DQ design: per-layer quality contracts, blocking mechanism (Hard Gate + Row Filter), scenario repair strategy, quarantine and remediation strategy, rule versioning with quality_events state machine, historical metrics architecture |
| [Cloud Layer Architecture](./CLOUD_LAYER.md) | ODS → BigQuery extraction and staging: partition/clustering/fuse design, watermark strategy (Approach A + the `get_watermark()` seam), batch-load and JSON landing decisions, and the ODS schema evolution strategy (additive staging + dbt absorption + `FIELDS` consistency test) |
| [Transformation Layer (dbt)](./ecommerce_dbt/README.md) | dbt transformation ops & implementation decisions: layering/naming conventions, materialization (table vs view, incremental + insert_overwrite + `copy_partitions` to bypass the sandbox DML ban), lookback window, dedup key & invariant, Hard Gate custom generic test, freshness fuse bypass; the `int_` layer's effective-quality-state composition (deliberate duplication + alignment checklist), why full rebuilds are required, the partition invariant test, and `int_order_items`'s `safe_cast` + strict NULL propagation. Layer contracts in DQ_ARCHITECTURE, staging infra in CLOUD_LAYER |

---

## Getting Started

```bash
# 1. Clone
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install dependencies (including test dependencies)
pip install -r requirements-dev.txt

# 4. Configure environment
cp .env.example .env
# Edit .env and set:
#   DB_URL=postgresql://user:password@localhost/dbname
#   API_KEYS=your_key:upstream-order-api   (format key:client_id, comma-separated for multiple)

# 5. Create the database schema (Alembic migration, not create_all)
alembic upgrade head

# 6. Run
uvicorn main:app --reload

# 7. Run tests (requires .env to be configured, or a dummy DB_URL)
pytest
```

API docs available at `http://localhost:8000/docs`

### Run with Docker (recommended)

The repository ships a `Dockerfile` + `docker-compose.yml` that bring up PostgreSQL, run the Alembic migration, then start the API — one command, no local Python/Postgres setup:

```bash
# 1. Set API_KEYS (and optionally POSTGRES_USER/PASSWORD/DB) in .env
cp .env.example .env

# 2. Build and start: db → migrate (alembic upgrade head) → api
docker compose up --build
```

- `db` (postgres:16) starts first; `pg_isready` healthcheck gates the rest.
- A one-shot `migrate` service runs `alembic upgrade head` and exits.
- `api` only starts once the DB is healthy **and** the migration has completed successfully (`service_completed_successfully`).
- `DB_URL` is injected by Compose pointing at the `db` service — it overrides the `.env` value (env vars outrank the `.env` file in pydantic-settings), so no code change is needed. `.env` is **not** baked into the image; secrets are injected at runtime.
- The API runs with `--workers 1` on purpose: `BackgroundTasks` and the periodic recovery scan are in-process state, so multiple workers would each run their own scan loop. Horizontal scaling waits on the Phase 5 queue (Redis/Celery).

API available at `http://localhost:8000` (docs at `/docs`, health at `/health`).

---

## Target Architecture

The current implementation covers the ingestion and processing layers. The full intended architecture is:

```
[Ingestion Layer]  Real-time
Simulate 1000 concurrent POST /orders
  ↓
API receives request → writes to Queue → responds immediately

[Processing Layer]  Real-time
Worker pulls from Queue
  ↓
Raw layer persists original payload (immutable)
  ↓
try_claim_raw atomically claims the record (pending → processing)
  ↓
Clean / Validate / Idempotency check
  ↓
Write to ODS (PostgreSQL, clean flat table) + quality_events
  ↓
Update status (processing → processed / error)

[Analytics Layer]  Batch, scheduled
Airflow DAG (local, scheduled)
  ↓
PostgreSQL ODS → BigQuery staging (incremental, watermarked by received_at)
  ↓
dbt Core transformations
  ├── stg_*  (1:1 source mapping, rename / cast / dedup only)
  ├── int_*  (cross-table joins, derived fields, business logic)
  ├── dim_* / fct_*  (Star Schema, flexible ad-hoc queries)
  └── rpt_*  (fixed-grain pre-aggregations, optimised for BI dashboards)
  ↓
Looker Studio (connected directly to BigQuery)
```

**Why batch over streaming for the analytics layer?**
The downstream consumer is BI — dashboards and reports where T+1 or hourly refresh is sufficient. Batch processing enables windowed data quality checks, supports re-runs on failure, and keeps the ingestion and analytics layers naturally decoupled. Streaming would only be motivated by a real-time prediction model downstream.

**dbt layer responsibilities**
- `stg_*`: Entry point for data from BigQuery staging; 1:1 source mapping, no business logic
- `int_*`: Intermediate building blocks; handles joins and complex derivations consumed by dim/fct models
- `dim_* / fct_*`: Star Schema dimension and fact tables for flexible analytical queries
- `rpt_*`: Pre-aggregated reporting tables built on top of dim/fct; fixed grain, optimised for dashboard performance and BigQuery query cost

---

## Roadmap

**Phase 1 — Reliability**
- [v] Retry mechanism — 4-point retry (Raw write, Claim, Processing, Status update), exponential backoff, `CRITICAL` log when all retries exhausted
- [v] Crash recovery scan — startup scan on restart + periodic scan every 5 minutes; stale `processing` (> 10 min) reset to `pending`; `WARNING` logged for potential duplicate ODS writes
- [v] Timeout — DB statement timeout (30s) prevents lock-wait hangs; explicit pool settings; `POST /orders` catches pool exhaustion and returns 503; `/process_raw` converted to background task to avoid blocking the event loop
- [v] Idempotency — `raw_id` column in ODS + `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`; first-write-wins: pre-check before commit + `IntegrityError` backstop for TOCTOU races; duplicate Raw records written to `duplicate` terminal status
- [v] Rate limiting — per-client limits via slowapi (keyed on the authenticated `client_id`, IP fallback): `POST /orders` 60/min, `POST /process_raw` 20/min, `GET /raw` 120/min; no global limit (see Design Decisions)

**Phase 2 — Testability**
- [v] Pytest — 248 tests, 100% coverage across all 8 source files (`pytest --cov`); unit tests cover all retry paths (Points 1–4), CAS claim, idempotency, crash recovery scan, `format_clean`, `business_clean`, `ODSOrder.from_nested`, quality_events write paths, API Key auth (missing/invalid/valid/rotation/parser fault-tolerance); `asyncio_mode=auto` replaces manual `asyncio.run()`; `reset_limiter` fixture eliminates cross-test rate-limit counter contamination; auth is bypassed via `dependency_overrides` so non-auth tests need not attach a header per request. Currently unit tests and integration tests (HTTP layer) only — no end-to-end tests; E2E tests against a real DB will be added once Phase 3 Docker / docker-compose is in place.
- [v] Data quality control architecture (ODS layer) — full design document (see [DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md)); ODS layer implemented: `DQ_RULE_VERSION` rule version constant, `dq_rule_version` column (ODS), `quality_events` table (append-only quality event log, state machine anchor), structlog `quality_metric` event; BQ Analytics layer (Hard Gate, Row Filter, `int_orders_quarantine`, Airflow re-evaluation, `rpt_quality_*`) deferred to Phase 4

**Phase 3 — Operability**
- [v] Service-to-service authentication (API Key) — static `X-API-Key` (`.env`-loaded `key:client_id` mapping, supporting multiple keys per client for rotation), constant-time `secrets.compare_digest` comparison; mounted on all endpoints, 401 on missing/invalid; the resolved `client_id` lands as `source_client_id` (Raw + ODS) as the origin of data lineage. **No user-facing JWT** — this is an internal ingestion unit with no human users (see "Service-to-service authentication decisions")
- [v] Centralised config management — `config.py` exposes a single `Settings` (pydantic-settings) as the source of truth, instantiated once at startup; modules read `from config import settings` instead of each calling `load_dotenv()` / `os.getenv`. **Decision boundary**: only values that vary by deployment environment are centralised — the required `DB_URL` (fail-fast on missing value instead of crashing late at first connection), `API_KEYS`, plus defaulted `pool_size` / `max_overflow` / `pool_timeout` / `statement_timeout_ms` / `scan_interval_seconds` / `log_format`. Algorithmic constants (`MAX_*_RETRIES`, `STALE_PROCESSING_MINUTES`) **deliberately stay at the top of their own modules** — they are part of program behaviour, not environment, so changing them should go through code review rather than an env var. Ships with a version-controlled `.env.example` template
- [v] Alembic migrations — Alembic is the single source of truth for schema; **`Base.metadata.create_all` removed** (`create_all` only creates, never alters — it cannot carry schema evolution). `env.py` pulls the connection from `settings.db_url` and `import models` to register `Base.metadata` for autogenerate; `alembic.ini`'s `sqlalchemy.url` is left blank so DB_URL stays a single source of truth. `Base.metadata` carries a **naming convention** (`ix/uq/ck/fk/pk_*`) so constraint names are stable and predictable, and future drop/rename won't break on environment-inconsistent names. The initial migration is generated with convention-native names; schema changes now flow through `alembic revision --autogenerate` → review → `alembic upgrade head`
- [v] Docker / docker-compose (API + PostgreSQL containerisation) — single-stage `python:3.12-slim` image (non-root, pinned `requirements.txt`), reused by both the `api` and one-shot `migrate` services; `docker compose up` orchestrates **db (healthcheck) → migrate (`alembic upgrade head`) → api** via `depends_on` conditions (`service_healthy` + `service_completed_successfully`); `DB_URL` injected at runtime pointing at the `db` service (no code change — env vars outrank `.env`), secrets never baked into the image; API pinned to `--workers 1` (in-process `BackgroundTasks` / periodic scan); `GET /health` liveness probe added for the container healthcheck

**Phase 4 — Analytics Pipeline**
- [v] ODS → BigQuery extraction script (`extract_ods_to_bq.py`) — incremental by `received_at` watermark (Approach A, derived from `INFORMATION_SCHEMA.PARTITIONS`, wrapped in `get_watermark()` as the seam for a future micro-batch Approach B); partitioned (`received_at` DAY) + clustered (`order_id`, `has_clean_error`) staging table with `require_partition_filter` fuse; batch load only (no streaming), JSON columns landed as native objects, `ALLOW_FIELD_ADDITION` for additive schema evolution; `FIELDS` single source of truth guarded by `tests/test_schema_bq_consistency.py` (see [CLOUD_LAYER.md](./CLOUD_LAYER.md))
- [v] dbt Core `stg_` layer (`stg_orders`: `raw_id` dedup + Hard Gate + source freshness; `stg_quality_events`: deduped at `id` grain, full state-machine history preserved; incremental + `insert_overwrite` + `copy_partitions`) — see [ecommerce_dbt/README.md](./ecommerce_dbt/README.md)
- [v] dbt Core `int_` layer (Gold entry, where blocking happens): `int_orders` (Row Filter keyed on the **effective quality state** composed from "ODS snapshot ⊕ latest `quality_events` event", not literal `has_clean_error`), `int_orders_quarantine` (with flattened `error_codes` and `quarantined_at` taken from the event time), `int_order_items` (items flattened to item grain). The **partition invariant** (the two tables are mutually exclusive and exhaustive over `stg_orders`) is guarded by a singular test; materialization is deliberately a `table` full rebuild rather than `received_at` incremental — a Proposal B promotion event lands in today's partition while the order it rescues sits in an old one, so incremental would silently sever the flow-back path
- [ ] dbt Core: dim_*/fct_* → rpt_*; includes `rpt_quality_*` (see [DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md)). Scenario-specific `int_orders_*` models are designed but enabled only when a real analytical scenario appears
- [ ] Looker Studio connected to BigQuery dim_*/fct_*/rpt_*

**Phase 5 — Automation + Queue Upgrade**
- [ ] Airflow (local) scheduled ODS → BigQuery extraction → dbt run/test（stg_* → int_* → dim_/fct_ → rpt_*）; includes Proposal B re-evaluation task (writes back to `quality_events`)
- [ ] Celery + Redis (replace BackgroundTasks)
- [ ] Docker extension: add Redis + Celery Worker and Airflow services
- [ ] OpenTelemetry — extend the existing structlog foundation to cover all three observability pillars:
  - **Logs**: route structlog output through the OTel Log Exporter; `trace_id` / `span_id` are injected into every log entry automatically, enabling cross-service log correlation
  - **Metrics**: quantify business signals via the OTel Metrics API — order ingestion throughput, ODS processed / error / duplicate rates, processing latency distribution (P50/P95/P99), DB pool pressure, retry attempt counts
  - **Traces**: add distributed tracing across the full chain (API → Worker → Airflow → BigQuery) to identify cross-service latency and bottlenecks
  - Exporter targets: Grafana Cloud (Loki + Prometheus + Tempo) or GCP Cloud Trace / Cloud Monitoring
