# ecommerce-data-ingestion-platform

### E-Commerce Order Pipeline — Data Lifecycle Management in Practice

**English** | [繁體中文](./README-TW.md)

[![CI](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml)

---

## What this is

An e-commerce order ingestion and analytics pipeline, organised around **data lifecycle management**. Untrusted inbound data is progressively turned into trustworthy analytical data through per-layer quality contracts, and **the evolution of every quality judgement stays auditable**.

Data engineering first, backend engineering second:

- The ingestion layer's fault tolerance ensures **data gets in** — multi-point retry, CAS claim, crash recovery, a circuit-broken dispatch.
- Layered quality contracts ensure **data flows correctly** — Raw → ODS → `stg_` → `int_` → `dim_`/`fct_` → `rpt_`, blocking exactly once.
- Rule versioning and an append-only event log ensure **a judgement can be revised without rewriting history**.

## What this is not

It is a **portfolio project**. There is no real upstream and no real traffic; the data source is a simulator that posts through the real ingestion path. Some things a production system would have are deliberately absent — each one recorded, with what a real system does instead and what would trigger building it: **[PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md)**.

---

## Architecture

```
POST /orders                                    ← X-API-Key required
    ↓
[Raw]  persisted verbatim, immutable                       status: pending
    ↓  Celery dispatch (circuit-broken; failure → recovery scan)
[Worker]  CAS claim → parse → flatten → clean → idempotency check
    ↓
[ODS] + [quality_events]        ← immutable anchor + append-only judgement log
    ↓  Airflow, daily 22:30 Taipei
[BigQuery staging]  orders + quality_events, watermarked incremental
    ↓
dbt stg_*     1:1 mirror, dedup, Hard Gate            ← Silver, all rows kept
    ↓
─────────────────── blocking happens here ───────────────────
dbt int_*     Row Filter on effective quality state    ← Gold entry
    ├── effectively clean  → int_orders → int_order_items
    └── not clean          → int_orders_quarantine
    ↓
dbt dim_*/fct_*  Kimball star schema  →  dbt rpt_*  fixed-grain pre-aggregations
```

Full walkthrough: **[ARCHITECTURE](./docs/en/ARCHITECTURE.md)**

---

## Tech stack

| Layer | Technology |
|---|---|
| API | FastAPI · Pydantic · slowapi |
| Storage | PostgreSQL 16 · SQLAlchemy · Alembic |
| Queue | Celery · Redis (broker db0, rate-limit counters db1) |
| Warehouse | BigQuery (partitioned + clustered staging, cost fuse) |
| Transformation | dbt-core / dbt-bigquery 1.11 |
| Orchestration | Airflow 3.0.0, LocalExecutor, six DAGs |
| Observability | OpenTelemetry → resident Collector → Grafana Cloud |
| Runtime | Docker Compose (two layered files) |

---

## Quick start

```bash
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform
cp .env.example .env      # set API_KEYS at minimum
```

**Ingestion stack** — one command, no local Python or Postgres needed:

```bash
docker compose up -d --build
```

`db` + `redis` start first (healthchecks gate the rest) → a one-shot `migrate` runs `alembic upgrade head` → `api` / `worker` / `beat` start once both are healthy **and** the migration succeeded.

API at `http://localhost:8000` (docs `/docs`, health `/health`).

**Adding the analytics pipeline** — `.env` also needs `BQ_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS`:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

⚠️ **The two compose files must be layered into one project** — that is what lets the DAGs reach `db` and lets seeding reach `api`. Airflow UI at `http://localhost:8080`.

**Local development without Docker**:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
alembic upgrade head          # Alembic is the source of truth, not create_all
uvicorn main:app --reload
pytest
```

Full startup notes and the two host-side traps: **[runbooks/airflow-startup](./docs/en/runbooks/airflow-startup.md)**

---

## API

All endpoints require an `X-API-Key` header; a missing or invalid key returns `401`.

| Method | Path | Description | Limit |
|---|---|---|---|
| `POST` | `/orders` | Ingest an order. Returns `200` + `pending` — **even if the queue dispatch failed**, because the Raw row is already committed | 60/min |
| `POST` | `/process_raw/{id}` | Manually reprocess. `?force=true` accepts `error` and `duplicate` only — never `processed` | 20/min |
| `GET` | `/raw/{id}` | Inspect a Raw record and its current status | 120/min |
| `GET` | `/health` | Liveness probe for the container healthcheck | — |

Limits are **per authenticated client, with no global limit** — a global cap would let one noisy upstream deny service to every other.

---

## Status

| Layer | State |
|---|---|
| Ingestion · Processing · Task queue | ✅ |
| Extraction · Transformation (dbt) · Orchestration | ✅ |
| Observability (traces + operational metrics) | ✅ |
| BI (Looker Studio on `rpt_`) | ✅ |
| Alerting · monitoring dashboards | ⏸ deferred — thresholds need real traffic |

**393 unit + integration tests** (100% coverage of gated modules, Python 3.10/3.12 matrix) · **52 DAG tests** in a separate workflow · **93 dbt tests**.

⚠️ **A green CI check does not mean the DB-layer contracts are verified** — CAS, dedup and crash recovery use a mocked database in CI and are corroborated by manual scripts. See [design/testing](./docs/en/design/testing.md).

Full matrix and known risks: **[STATUS](./docs/en/STATUS.md)**

---

## Documentation

| Document | For | When |
|---|---|---|
| [ARCHITECTURE](./docs/en/ARCHITECTURE.md) | how the system fits together | first |
| [STATUS](./docs/en/STATUS.md) | what is built, what is not, and why | before judging a gap |
| [PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md) | every deferred item, with what a real system does instead | when something looks missing |
| [ADRs](./docs/en/adr/README.md) (54) | why each decision was made, and what was rejected | when a choice looks odd |
| [design/](./docs/en/design/) (8) | how each layer works | when changing one |
| [runbooks/](./docs/en/runbooks/) (8) | what to do when something breaks | during an incident |
| [verification/](./docs/en/verification/) (14) | what was measured, and what it overturned | when you doubt a claim |
| [incidents/](./docs/en/incidents/) (2) | what broke, and how it was diagnosed | — |
| [CHANGELOG](./CHANGELOG.md) | how the system got here | — |

### Suggested reading paths

| You have | Read |
|---|---|
| **5 minutes** | the architecture diagram above → [STATUS](./docs/en/STATUS.md) |
| **15 minutes** | + three ADRs — [0002](./docs/en/adr/0002-has-clean-error-non-blocking.md) (the central invariant), [0015](./docs/en/adr/0015-staleness-from-processing-started-at.md) (a defect and its fix), [0028](./docs/en/adr/0028-hard-gate-per-batch-scope.md) (a decision that was revised) |
| **30 minutes** | + two verification records — [SIGKILL recovery](./docs/en/verification/2026-08-10-celery-sigkill-recovery.md) and [staleness basis](./docs/en/verification/2026-08-10-staleness-basis-self-collision.md) |
| **looking for holes** | [PORTFOLIO_SCOPE](./docs/en/PORTFOLIO_SCOPE.md), then [incidents](./docs/en/incidents/2026-08-silent-scheduling-stalls.md) |

The commit history is also part of the record — 120+ commits with messages stating the reasoning, not just the change.

---

## License

[MIT](./LICENSE)
