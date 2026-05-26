# ecommerce-data-ingestion-platform
### 電商資料平台寫入系統

**English** | [繁體中文](./README.zh-TW.md)

A backend data ingestion service built with FastAPI, designed to simulate a real-world e-commerce order pipeline — from raw payload ingestion to ODS (Operational Data Store) write with data cleaning and state management.

The focus of this project is demonstrating practical backend and data engineering skills. Each design decision is grounded in real-world concerns: preventing duplicate processing under high concurrency, compensating for database write failures, and handling inconsistent or dirty data at ingestion time.

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
POST /orders
    │
    ▼
[Raw Table]  ←── stores original JSON payload
    │  status: pending
    ▼
[Background Task: process_raw_event]
    │
    ├── try_claim_raw()         ← atomic UPDATE pending → processing
    ├── JSON parse
    ├── ODSOrder.from_nested()  ← flatten nested payload
    ├── clean_order()           ← format clean + business validation
    │
    ├── success → write ODS, Raw status = processed
    └── failure → Raw status = error, error_message logged
```

---

## Key Design Decisions

**Atomic claim logic (`try_claim_raw`)**
Uses `UPDATE ... WHERE status = 'pending'` and checks `rowcount == 1` to prevent duplicate processing — safe under concurrent workers without pessimistic locking.

**Two-layer data model (Raw → ODS)**
Raw table preserves the original JSON payload for auditability and replay. ODS stores flattened, cleaned data ready for analytics.

**Layered error handling**
Separates `JSONDecodeError`, `ValueError` (validation), and generic `Exception` — each mapped to a distinct error message on the Raw record.

**Data cleaning pipeline**
`format_clean()` normalises string casing and strips whitespace. `business_clean()` validates business rules (quantity > 0, rating 1–5, delivery_date ≥ order_date, etc.) and flags records with `has_clean_error` rather than rejecting them outright.

**ODS items column uses JSONB; Raw payload uses TEXT** 
The Raw layer's responsibility is to preserve every inbound request exactly as received, making no assumptions about structure — TEXT is the semantically correct choice since the database neither parses nor validates the content. The ODS items field, by contrast, has already passed Pydantic validation and cleaning, so its structure is guaranteed; JSONB adds a second layer of format enforcement at the database level and preserves the option to query into items fields directly in SQL if needed.

**No business deduplication at the Raw layer**
`Raw.order_id` intentionally has no UNIQUE constraint. The Raw table's responsibility is to record every inbound request as-is — including duplicate submissions — because different submissions may carry complementary fields, and abnormal submission frequency is itself a signal (attack detection, client-side bugs). Deduplication responsibility is delegated to the ODS layer.

**Per-IP rate limiting only — no global cap**
A global limit must be derived from "expected concurrent active IPs × per-IP limit", which is impossible to determine without real traffic data — an arbitrary number would only introduce a limit with unclear semantics. More fundamentally, a `/minute` window rate limit cannot prevent instantaneous bursts: 500 requests arriving simultaneously all fall within a 600/minute cap, yet the pool would still be exhausted. Pool exhaustion is already handled by `SATimeoutError → 503`. Rate limiting's responsibility is narrowed to defending against sustained abuse from a single IP; global protection is delegated to the existing 503 mechanism.

**ODS-layer first-write-wins idempotency**
Only the first submission for a given `order_id` is written to ODS, enforced by two guards: a pre-check (query ODS for the `order_id` before committing) and `UNIQUE(ods.order_id)` + `UNIQUE(ods.raw_id)` constraints as a backstop against TOCTOU races. Subsequent duplicate Raw records are not rejected — they are written to a `duplicate` terminal status so monitoring can distinguish normal processing from intercepted duplicates.

**`force=True` semantic boundary: single-record retry, not backfill**
`POST /process_raw/{raw_id}?force=true` is only permitted on `error` or `duplicate` records — its semantics are "retry this failed record". Calling it on a `processed` record returns 400. The reason: if downstream systems (Star Schema, aggregation tables) have already consumed that ODS record, deleting and rewriting it in isolation cannot cascade corrections downstream and would introduce inconsistencies instead. When a change to cleaning logic requires re-deriving historical data, the correct tool is a backfill batch job — starting from the immutable Raw layer, re-generating ODS in full, then re-running each downstream layer in sequence. That is a Phase 4 concern to be addressed when Airflow is introduced, and is outside the responsibility of `force=True`.

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
`order_id` on the Raw table has an index but no UNIQUE constraint. Each duplicate is treated as a new ingestion event with its own `raw_id`. CAS lock protects against the same `raw_id` being processed twice, not against business-level deduplication — a known and intentional boundary (business-level deduplication is now handled by ODS idempotency).

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

Mitigation options: ~~startup recovery scan (`SELECT * FROM raw WHERE status IN ('pending','processing')`)~~, replace BackgroundTasks with Redis/Celery/Kafka, ~~periodic sweep to reset stale `processing` records~~ (Recovery mechanism implemented).

**Test 6 — Duplicate order_id submissions (ODS idempotency)**

Scenario A (sequential): the same `order_id` is submitted twice. The first write succeeds normally. When the second is processed, a pre-check finds the `order_id` already in ODS and marks the Raw record `duplicate` — ODS is not written again.

Scenario B (TOCTOU race): two workers both pass the pre-check simultaneously; the first commits ODS, the second hits an `IntegrityError` on commit — caught without retry, marked `duplicate`.
Result: ODS always contains exactly one record per `order_id`; all subsequent duplicate Raw records reach a `duplicate` terminal status, giving monitoring a clear signal distinct from normal processing.

---

## Retry & Recovery

### Retry Mechanism

Four-layer retry strategy to handle transient failures. All retries use exponential backoff (0.5s → 1s → give up) capped at 3 attempts.

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
1. Reset stale `processing` records (stuck > 10 minutes) to `pending` — logs `WARNING` (duplicate ODS write risk is now covered by idempotency protection)
2. Collect and re-queue all `pending` records

### What retry handles vs. what it does not

| Scenario | Handled |
|---|---|
| Transient DB connection drop at any stage | ✅ |
| `pending` / `processing` records after a crash | ✅ (scan recovery) |
| Connection pool exhaustion (`TimeoutError`) | ~~❌ Different exception type — not caught by Point 1~~ → ✅ `SATimeoutError` is now caught and returns 503, letting the client retry |
| SIGKILL mid-execution | ❌ Process is dead before any retry logic runs |
| ~~Duplicate `order_id` submitted twice~~ | ~~❌ No `UNIQUE` constraint on `order_id`~~ → ✅ Resolved by ODS idempotency |
| ~~ODS duplicate when scan retries a `processing` record~~ | ~~❌ No idempotency guard~~ → ✅ Resolved by ODS idempotency |

---

## Timeout Settings

**DB statement timeout (`database.py`)**
Set via `connect_args={"options": "-c statement_timeout=30000"}` on each connection. Ensures any SQL that hangs beyond 30 seconds (e.g. a lock wait) raises `OperationalError`, allowing the retry mechanism to take over instead of leaving threads hanging indefinitely.

**Explicit connection pool settings (`database.py`)**
`pool_size=5, max_overflow=10, pool_timeout=30` — same as SQLAlchemy defaults, but now explicit for clarity and future tuning.

**Pool exhaustion → 503 (`main.py`)**
`POST /orders` separately catches `SATimeoutError` (pool failed to acquire a connection) and returns 503 Service Unavailable without entering the retry loop. Pool exhaustion is a resource contention issue, not a DB fault — retrying only makes it worse. Failing fast lets the client back off and retry.

**`POST /process_raw/{raw_id}` converted to background task (`main.py`)**
Changed from calling `process_raw_event(raw_id)` directly to `background_tasks.add_task`, consistent with `/orders` and no longer blocking the event loop.

---

## Rate Limiting

Per-IP rate limiting via `slowapi`. The count key is `request.client.host` (direct TCP connection IP).

| Endpoint | Per-IP limit | Reason |
|---|---|---|
| `POST /orders` | 60/minute | Guards against abnormal submission frequency from a single client; well above any legitimate order rate |
| `POST /process_raw/{raw_id}` | 20/minute | Manual replay — inherently low frequency |
| `GET /raw/{raw_id}` | 120/minute | Read-only, more lenient |

Requests exceeding the limit receive `429 Too Many Requests`.

**Why no global cap**

A global limit must be derived from "expected concurrent active IPs × per-IP limit" — without real traffic data this number is arbitrary and carries unclear semantics. More fundamentally, a `/minute` window cannot prevent instantaneous bursts: 500 simultaneous requests all fall within a 600/minute cap, yet the pool exhausts regardless. Pool exhaustion is already handled by `SATimeoutError → 503`; rate limiting's responsibility is limited to sustained single-IP abuse.

**⚠️ Deployment note**

Running directly under uvicorn, `request.client.host` is the real client IP and limiting behaves correctly. If deployed behind Nginx or a load balancer, `request.client.host` becomes the proxy IP — all requests share one counter and per-IP limiting breaks down. In that case, the key function must be updated to read the `X-Forwarded-For` header with appropriate trusted-proxy configuration.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Ingest a new order (writes Raw, triggers background task) |
| `POST` | `/process_raw/{raw_id}` | Manually replay a raw record (`?force=true` resets status) |
| `GET` | `/raw/{raw_id}` | Query raw record status and payload preview |

---

## Data Flow

```
OrderIN (nested Pydantic)
    └── from_nested() → ODSOrder (flat Pydantic)
            └── clean_order() → ODS (SQLAlchemy model)
```

Pydantic handles input validation and schema flattening. SQLAlchemy handles persistence. The two layers are intentionally decoupled.

---

## Project Structure

```
.
├── main.py        # FastAPI app, route handlers
├── process.py     # Background task, state machine, claim logic
├── clean.py       # format_clean, business_clean, clean_order
├── schema.py      # Pydantic schemas (OrderIN, ODSOrder, RawOut...)
├── models.py      # SQLAlchemy models (Raw, ODS, QualityEvent)
├── database.py    # Engine, SessionLocal, Base
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
│   └── test_rate_limit.py # per-IP rate limiting
├── DQ_ARCHITECTURE.md     # Data Quality Control Architecture (English)
├── DQ_ARCHITECTURE-TW.md  # 資料品質控管架構設計文件（繁體中文）
├── .env           # DB_URL (not committed)
└── .gitignore
```

---

## 📄 Design Documents

| Document | Description |
|---|---|
| [Data Quality Control Architecture](./DQ_ARCHITECTURE.md) | Full DQ design: per-layer quality contracts, blocking mechanism (Hard Gate + Row Filter), quarantine and remediation strategy, rule versioning with quality_events state machine, historical metrics architecture |

---

## Getting Started

```bash
# 1. Clone
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic python-dotenv pytz slowapi
pip install pytest pytest-asyncio pytest-cov  # test dependencies

# 4. Configure environment
cp .env.example .env
# Edit .env and set DB_URL=postgresql://user:password@localhost/dbname

# 5. Run
uvicorn main:app --reload

# 6. Run tests (requires .env to be configured)
pytest
```

API docs available at `http://localhost:8000/docs`

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
Write to ODS (PostgreSQL, clean flat table)
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
- ~~[ ] Idempotency — `UNIQUE` constraint on `Raw.order_id` + `IntegrityError` handling in `create_order`; `raw_id` column in ODS to prevent duplicate writes from scan retry~~
- [v] Idempotency — `raw_id` column in ODS + `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`; first-write-wins: pre-check before commit + `IntegrityError` backstop for TOCTOU races; duplicate Raw records written to `duplicate` terminal status
- [v] Rate limiting — per-IP limits via slowapi: `POST /orders` 60/min, `POST /process_raw` 20/min, `GET /raw` 120/min; no global limit (see Design Decisions)

**Phase 2 — Testability**
- [v] Pytest — 84 tests, 100% coverage across all 7 source files (`pytest --cov`); unit tests cover all retry paths (Points 1–4), CAS claim, idempotency, crash recovery scan, `format_clean`, `business_clean`, `ODSOrder.from_nested`, quality_events write paths; `asyncio_mode=auto` replaces manual `asyncio.run()`; `reset_limiter` fixture eliminates cross-test rate-limit counter contamination. Currently unit tests and integration tests (HTTP layer) only — no end-to-end tests; E2E tests against a real DB will be added once Phase 3 Docker / docker-compose is in place.
- [v] Data quality control architecture (ODS layer) — full design document (see [DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md)); ODS layer implemented: `DQ_RULE_VERSION` rule version constant, `dq_rule_version` column (ODS), `quality_events` table (append-only quality event log, state machine anchor), structlog `quality_metric` event; BQ Analytics layer (Hard Gate, Row Filter, `int_orders_quarantine`, Airflow re-evaluation, `rpt_quality_*`) deferred to Phase 4

**Phase 3 — Operability**
- [ ] JWT authentication
- [ ] Centralised config management
- [ ] Alembic migrations
- [ ] Docker / docker-compose (Queue + Worker + DB)

**Phase 4 — Analytics Layer**
- [ ] Celery + Redis (replace BackgroundTasks)
- [ ] Airflow (local) scheduled extraction: PostgreSQL ODS → BigQuery (incremental by `received_at`)
- [ ] dbt Core: stg_* → int_* → dim_*/fct_* (Star Schema in BigQuery) → rpt_* (fixed-grain pre-aggregations)
- [ ] Looker Studio connected to BigQuery dim_*/fct_*/rpt_* for dashboards and reports
- [ ] Data quality control architecture (BQ Analytics layer) — dbt stg_* Hard Gate tests; `int_orders` Row Filter (`WHERE has_clean_error = FALSE`) + `int_orders_quarantine`; Airflow re-evaluation task (Proposal B, writes back to `quality_events`); `rpt_quality_*` quality trend models (see [DQ_ARCHITECTURE.md](./DQ_ARCHITECTURE.md))
- [ ] OpenTelemetry — extend the existing structlog foundation to cover all three observability pillars:
  - **Logs**: route structlog output through the OTel Log Exporter; `trace_id` / `span_id` are injected into every log entry automatically, enabling cross-service log correlation
  - **Metrics**: quantify business signals via the OTel Metrics API — order ingestion throughput, ODS processed / error / duplicate rates, processing latency distribution (P50/P95/P99), DB pool pressure, retry attempt counts
  - **Traces**: once Celery and Airflow are in place, add distributed tracing across the full chain (API → Worker → Airflow → BigQuery) to identify cross-service latency and bottlenecks
  - Exporter targets: Grafana Cloud (Loki + Prometheus + Tempo) or GCP Cloud Trace / Cloud Monitoring

---

## Known Issues

**Scan may re-schedule tasks that are already queued**
The periodic scan and startup scan collect all Raw records with `status='pending'` and re-schedule them, but the database has no visibility into whether a given record is already sitting in the BackgroundTasks queue waiting to be picked up. Under high traffic, if a burst of requests lands before the queue drains, the scan can re-schedule records that are already queued — resulting in multiple workers racing to process the same raw_id. The CAS claim (`try_claim_raw`) acts as the safety net at execution time: the losing worker receives `rowcount=0` and returns immediately, so ODS is never written twice and correctness is preserved. The cost is wasted thread-pool slots and extra DB round-trips, which adds pressure to the connection pool under load.

**Design direction when switching to a Queue**
The proper fix is to make "already enqueued" visible in the database by introducing a `queued` status into the state machine (`pending → queued → processing → processed/error/duplicate`) and coupling the enqueue action to an atomic status transition: after writing the Raw record, immediately CAS `pending → queued`; only push to the Queue if `rowcount == 1`. The scan then only collects `pending` records (meaning records that never successfully entered the queue) and skips anything already `queued`. The worker's CAS claim shifts accordingly to `queued → processing`. The one edge case this introduces is a failed Queue push after the DB write succeeds, leaving a record stuck in `queued` — the scan must also sweep for stale `queued` records (older than N minutes) and reset them to `pending` so they re-enter the enqueue flow.
