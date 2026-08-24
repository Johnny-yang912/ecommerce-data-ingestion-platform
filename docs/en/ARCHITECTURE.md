# Architecture

**English** | [繁體中文](../zh-TW/ARCHITECTURE.md)

How the system is put together. **Why** each decision was made lives in the [ADRs](./adr/README.md); **what is built** lives in [STATUS](./STATUS.md).

---

## 1. What this system is

An e-commerce order ingestion and analytics pipeline, organised around **data lifecycle management**. Untrusted inbound data is progressively turned into trustworthy analytical data through per-layer quality contracts, and the evolution of every quality judgement stays auditable.

It is data engineering first, backend engineering second:

- The ingestion layer's fault tolerance ensures **data gets in**.
- The layered quality contracts ensure **data flows correctly**.
- Rule versioning and an append-only event log ensure **the evolution of quality judgements is always auditable**.

---

## 2. End-to-end data flow

```
POST /orders                                    ← X-API-Key required
    ↓
[Raw]  persisted verbatim, immutable                       status: pending
    ↓  Celery dispatch (circuit-broken; failure → recovery scan)
[Worker: process_raw_event]
    ├── try_claim_raw()        ← atomic UPDATE, CAS claim
    ├── JSON parse
    ├── ODSOrder.from_nested() ← flatten nested payload
    ├── clean_order()          ← format normalisation + business-rule validation
    ├── first-write-wins idempotency check
    └── [ODS] + [quality_events]   ← immutable anchor + quality event log
    ↓  Airflow, daily 22:30 Taipei
[BigQuery staging]  orders + quality_events, watermarked incremental
    ↓
dbt stg_*     1:1 mirror, dedup, Hard Gate            ← Silver entry, all rows kept
    ↓
─────────────────── blocking happens here ───────────────────
dbt int_*     Row Filter on effective quality state    ← Gold entry
    ├── effectively clean  → int_orders → int_order_items
    └── not clean          → int_orders_quarantine
    ↓
dbt dim_*/fct_*   Kimball star schema                  ← Gold
    ↓
dbt rpt_*         fixed-grain pre-aggregations
```

---

## 3. Layers and responsibility boundaries

| Layer | Responsibility | Mutable | Detail |
|---|---|---|---|
| **Raw** (PostgreSQL) | Persist every inbound request exactly as received. No quality assumptions | No | [ingestion](./design/ingestion.md) |
| **ODS** (PostgreSQL) | Format normalisation + business-rule validation. Retains everything, including dirty rows | No | [ingestion](./design/ingestion.md), [data-quality](./design/data-quality.md) |
| **Task queue** | Durable dispatch between Raw and ODS, with bounded degradation | — | [queue](./design/queue.md) |
| **staging** (BigQuery) | 1:1 landing of ODS. No cleaning, no renaming, no casting | append-only | [cloud-layer](./design/cloud-layer.md) |
| **`stg_`** | Type alignment, column renaming, dedup back to ODS grain | rebuilt | [transformation](./design/transformation.md) |
| **`int_`** | Cross-table joins, derived fields, and **the blocking point** | rebuilt | [transformation](./design/transformation.md) |
| **`dim_`/`fct_`** | Star schema for flexible analytical queries | rebuilt | [transformation](./design/transformation.md) |
| **`rpt_`** | Fixed-grain pre-aggregations for BI | rebuilt | [transformation](./design/transformation.md) |
| **Orchestration** | Scheduling, and the observation signals | — | [orchestration](./design/orchestration.md) |
| **Observability** | Traces + operational metrics | — | [liveness-alerting](./design/liveness-alerting.md) |

---

## 4. Quality contract per layer

Quality responsibility **tightens progressively downstream**. ODS is the immutable anchor that retains everything; blocking happens once, at `int_`.

```
Raw            no quality requirement                     dirty rows kept
ODS            flagged, never rejected                    dirty rows kept
stg_           same as ODS                                dirty rows kept
─────────────────── blocking ───────────────────
int_           only effectively-clean rows pass           dirty → int_orders_quarantine
dim_/fct_      cleanest layer                             no dirty rows present
rpt_           same as Gold                               no dirty rows present
```

Two mechanisms operate at different granularities:

- **Hard Gate** (run-level, on `stg_`) — *is the source broken as a whole?* Blocks the run. [ADR-0028](./adr/0028-hard-gate-per-batch-scope.md)
- **Row Filter** (record-level, in `int_`) — *is this row usable?* Routes to quarantine. [ADR-0029](./adr/0029-effective-quality-state.md)

---

## 5. Why blocking is at `int_` and not earlier

ODS is the **immutable anchor**: the one place where every accepted order exists exactly once, dirty or clean. That property is what makes three things possible:

1. Quality metrics have a real **denominator**.
2. Rule changes can be applied **retroactively** without re-ingesting — the basis of the whole re-evaluation mechanism.
3. Divergence between ODS and the warehouse is **explainable** rather than mysterious.

`stg_` is a 1:1 mirror, so filtering there would break the reconciliation that mirror property provides. `int_` is the first layer that already does semantic work, and "is this row usable" is a semantic judgement.

Full reasoning: [ADR-0002](./adr/0002-has-clean-error-non-blocking.md), [ADR-0027](./adr/0027-blocking-at-int-layer.md).

---

## 6. Why the analytics layer is batch, not streaming

The downstream consumer is BI, where T+1 refresh is sufficient. More decisively, **batch is what makes windowed quality control possible**: the Hard Gate asserts an error rate over a batch, and streaming has no batch boundary to scope it to.

Batch also makes failures re-runnable — the watermark does not advance on failure, so the next run re-selects the same slice. [ADR-0019](./adr/0019-batch-load-not-streaming.md)

---

## 7. Deployment topology

Two compose files, **layered into one project** so the DAGs can reach the business database at the hostname `db`:

```
docker-compose.yml                       docker-compose.airflow.yml (overlay)
├── db        postgres:16                ├── airflow-db    metadata, separate instance
├── redis     7-alpine, db0=broker       ├── airflow-apiserver
│                        db1=rate limit  ├── airflow-scheduler
├── migrate   one-shot, alembic          ├── airflow-dag-processor
├── api       4 uvicorn workers          └── two venvs inside the image:
├── worker    Celery, 4 prefork               /venvs/analytics  /venvs/dbt
├── beat      Celery Beat — singleton
└── otel-collector
```

Start order is gated by healthchecks and `service_completed_successfully`: `db` + `redis` → `migrate` → `api` / `worker` / `beat`.

Three process-level constraints worth knowing:

- **`beat` must never be `--scale`d** — two beats dispatch duplicate scans. [ADR-0016](./adr/0016-recovery-scan-in-beat.md)
- **Airflow's metadata DB is a separate instance** from the business DB, so restoring one does not roll back the other.
- **Airflow's two venvs do not update with a bind mount** — changing `requirements-analytics.txt` needs a rebuild. [ADR-0035](./adr/0035-two-venvs-dependency-isolation.md)

---

## 8. Where to read next

| You want to know | Read |
|---|---|
| How an order gets in, and what happens when it fails | [ingestion](./design/ingestion.md) |
| How dispatch degrades when the broker is down | [queue](./design/queue.md) |
| How ODS reaches BigQuery, and how schema changes are absorbed | [cloud-layer](./design/cloud-layer.md) |
| How quality is judged, and how a judgement can change later | [data-quality](./design/data-quality.md) |
| How the dbt layers are built and tested | [transformation](./design/transformation.md) |
| How everything is scheduled, and what each red light means | [orchestration](./design/orchestration.md) |
| What CI covers and where it is blind | [testing](./design/testing.md) |
| Why each decision was made | [ADRs](./adr/README.md) |
| What is not built, and why | [STATUS](./STATUS.md) · [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
