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

---

## Load Test Results

Five scenarios were tested to validate concurrency behaviour and failure modes.

**Test 1 — 1,000 unique orders, concurrency=50**
Result: All succeeded, completed in 7.9s, zero errors.
Each `POST /orders` only performs a single fast INSERT and releases the connection immediately (hold time < 10ms). concurrency=50 is well within the DB pool's capacity — no queuing occurs.

**Test 2 — 1,000 unique orders, concurrency=500**
Result: P99 latency ~14s, 5 × HTTP 500 errors.
SQLAlchemy's default pool (pool_size=5, max_overflow=10) supports at most 15 concurrent connections. With 500 simultaneous requests, 485 queue for a connection. Any request exceeding `pool_timeout=30s` throws `QueuePool limit reached`. The 5 failures timed out before INSERT — no Raw record was created.

Mitigation options: increase pool size, switch to async SQLAlchemy (asyncpg), or add rate limiting at the API gateway.

**Test 3 — 100 duplicate order_ids, concurrency=100**
Result: 100 Raw records written, 100 ODS records written — all succeeded.
`order_id` on the Raw table has an index but no UNIQUE constraint. Each duplicate is treated as a new ingestion event with its own `raw_id`. CAS lock protects against the same `raw_id` being processed twice, not against business-level deduplication — a known and intentional boundary.

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

Mitigation options: startup recovery scan (`SELECT * FROM raw WHERE status IN ('pending','processing')`), replace BackgroundTasks with Redis/Celery/Kafka, periodic sweep to reset stale `processing` records.

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
├── models.py      # SQLAlchemy models (Raw, ODS)
├── database.py    # Engine, SessionLocal, Base
├── .env           # DB_URL (not committed)
└── .gitignore
```

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
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic python-dotenv pytz

# 4. Configure environment
cp .env.example .env
# Edit .env and set DB_URL=postgresql://user:password@localhost/dbname

# 5. Run
uvicorn main:app --reload
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
Write to ODS (clean, flat table)
  ↓
Update status (processing → processed / error)

[Analytics Layer]  Batch, scheduled
ODS
  ↓
Split into dim / fact tables (Star Schema)
  ↓
Aggregation
  ↓
Write to stats / query tables
  ↓
BI
```

**Why batch over streaming for the analytics layer?**
The downstream consumer is BI — dashboards and reports where T+1 or hourly refresh is sufficient. Batch processing enables windowed data quality checks, supports re-runs on failure, and keeps the ingestion and analytics layers naturally decoupled. Streaming would only be motivated by a real-time prediction model downstream.

---

## Roadmap

**Phase 1 — Reliability**
- [ ] Retry / timeout for background task workers
- [ ] Idempotency — complete implementation and test coverage
- [ ] Rate limiting on the ingestion layer

**Phase 2 — Testability**
- [ ] Pytest — unit tests for `try_claim_raw`, state transitions, edge cases
- [ ] Data quality / profiling checks before ODS write

**Phase 3 — Operability**
- [ ] JWT authentication
- [ ] Centralised config management
- [ ] Alembic migrations
- [ ] Docker / docker-compose (Queue + Worker + DB)

**Phase 4 — Analytics Layer**
- [ ] Celery + Redis (replace BackgroundTasks)
- [ ] Airflow for batch scheduling
- [ ] ODS → Star Schema → aggregation → stats tables
