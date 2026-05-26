# Data Quality Control Architecture

## Design Goal

Ensure that data entering the analytics layer (Star Schema) is as clean as possible.  
ODS serves as an immutable anchor that preserves the complete state of all data. Quality control responsibility tightens progressively as data flows downstream.

---

## Quality Contract per Layer (Q0)

```
Raw (PostgreSQL)
  Responsibility : Persist every inbound request exactly as received, no quality assumptions
  Quality requirement : None
  Mutable : No

ODS (PostgreSQL)                               ← Bronze / Anchor
  Responsibility : Basic cleaning + business-rule validation, preserves all data including dirty records
  Quality requirement : Format normalisation applied; business issues flagged, never rejected
  Mutable : No — immutable snapshot at ingestion time, never modified by downstream

BQ staging (Airflow extraction)
  Responsibility : Mirror ODS, incremental upload
  Quality requirement : Same as ODS — pure mirror

dbt stg_*                                      ← Silver entry
  Responsibility : 1:1 source mapping, type alignment, column renaming
  Quality requirement : Same as ODS — all records retained including dirty ones
  Additional : source freshness tests, basic schema tests

─────────────────── Blocking happens here ───────────────────

dbt int_*                                      ← Gold entry
  Responsibility : Cross-table joins, derived fields, business logic
  Quality requirement : Only clean records pass (has_clean_error = FALSE)
  Destination for dirty records : int_orders_quarantine

dbt dim_*/fct_*                                ← Gold
  Responsibility : Star Schema, flexible ad-hoc queries for downstream consumers
  Quality requirement : Cleanest layer — no records with has_clean_error = TRUE

dbt rpt_*
  Responsibility : Fixed-grain pre-aggregations, optimised for BI dashboards
  Quality requirement : Same as dim_*/fct_*
```

---

## Blocking Mechanism (Q1)

Two mechanisms used together, each covering a different level of failure.

### Mechanism 1: Hard Gate (run-level)

Tests are attached to `dbt stg_*`. A test failure halts the entire dbt run — `int_*/dim_*/fct_*` are not updated and retain their last clean state.

```yaml
# stg_orders.yml
models:
  - name: stg_orders
    tests:
      # Critical: key field entirely null → entire batch is meaningless
      - not_null:
          column_name: order_id
          severity: error

      # Critical: batch error rate too high → likely a source system issue
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.1"
          severity: error     # > 10% → block run

      # Warning: error rate elevated but still acceptable → continue but alert
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.05"
          severity: warn      # > 5% → warn
```

### Mechanism 2: Row Filter (record-level)

Applied in `dbt int_*` SQL — isolates individual dirty records.

```sql
-- int_orders.sql  (clean data flow)
SELECT * FROM {{ ref('stg_orders') }}
WHERE has_clean_error = FALSE

-- int_orders_quarantine.sql  (quality-flagged records)
SELECT
    *,
    CURRENT_TIMESTAMP() AS quarantined_at
FROM {{ ref('stg_orders') }}
WHERE has_clean_error = TRUE
```

---

## Handling Quarantined Records (Q2)

### What ends up in quarantine

Records filtered out by the Row Filter: `Raw.status = "processed"` and `has_clean_error = TRUE`.  
These records **already exist in ODS** — they are not missing from the pipeline; they are simply isolated at the `int_*` layer and do not flow into `dim_*/fct_*`.

### Remediation: A + B together

A and B fix **different types of problems**. It is essential to know which path applies to which scenario:

| | What it fixes | Path |
|---|---|---|
| **A — force=True** | Records with `Raw.status = "error"` or `"duplicate"` (never successfully written to ODS) | `POST /process_raw/{raw_id}?force=true` → re-runs pipeline → flows downstream naturally |
| **B — Airflow re-evaluation** | Records with `Raw.status = "processed"` + `has_clean_error = TRUE` (in ODS, but quarantined at BQ layer) | Re-evaluate against updated rules → write to `quality_events` → promoted on next dbt run |

> **Important**: `force=True` does **not** work on quarantine records (`status = "processed"` → returns 400).  
> Quarantine records have a **rule evaluation problem**, not a pipeline failure — re-running the pipeline cannot fix them.

### State machine

```
initial_evaluation
  ├── passes all rules           → to_state: "clean"
  └── has_clean_error = TRUE     → to_state: "quarantined"

quarantined
  ├── Proposal B re-eval passes  → to_state: "promoted"
  └── manually written off       → to_state: "permanently_rejected"

promoted
  └── stricter rules re-eval fails → to_state: "re_quarantined"  (edge case)
```

---

## Rule Versioning and the quality_events Table (Q2 Extension)

### Rule version constant

```python
# clean.py
DQ_RULE_VERSION = "v1"    # bump on every rule change; pair with a git tag documenting what changed
```

### New column on ODS

```
ODS (new column)
└── dq_rule_version : String    ← rule version in effect at ingestion time; never updated afterward
```

### quality_events table (PostgreSQL, append-only)

Captures the full quality lifecycle of every record. Append-only — the state machine's event log.

```
quality_events
├── id:           Integer (PK)
├── raw_id:       Integer
├── order_id:     String
├── event_type:   String     "initial_evaluation" | "promotion" | "rejection"
├── from_state:   String?    null | "quarantined" | "promoted"
├── to_state:     String     "clean" | "quarantined" | "promoted" | "permanently_rejected"
├── rule_version: String     "v1" | "v2" | ...
├── event_at:     DateTime
└── reason:       Text?
```

**When records are written:**
- `process.py` after a successful ODS write → inserts one `initial_evaluation` event
- Airflow re-evaluation promotes or rejects a record → inserts one `promotion` or `rejection` event

---

## Data Consistency (Q2 Extension)

### Divergence between ODS and BQ

Divergence **will exist** — but with rule versioning and `quality_events` it becomes documented and traceable:

```
Without versioning:
  ODS has_clean_error = TRUE,  BQ dim_* is clean  →  "Why are they different?" — no answer

With versioning and quality_events:
  ODS has_clean_error = TRUE,  dq_rule_version = "v1"   ← truth at ingestion time
  quality_events: promoted under "v2" at 2026-03-01      ← truth after evolution
  → divergence is explained and auditable
```

### Bounded Writeback Principle

Any writeback from BQ (or Airflow after reading BQ) targets **only `quality_events`** — ODS itself is never modified.

```
❌ Prohibited : BQ → modify ODS columns  (breaks the immutable anchor contract)
✅ Allowed    : BQ → write to quality_events  (an audit log explicitly designed for this)
```

---

## Historical Quality Metrics (Q3)

### Tier 1: Real-time operational metrics (minute-level)

The structlog foundation is already in place. Extend it with a `quality_metric` log event:

```python
# process.py, after writing ODS
logger.info("quality_metric",
    rule_version=DQ_RULE_VERSION,
    has_clean_error=has_clean_error,
    order_id=ods_order.order_id,
    error_fields=clean_error_message,
)
```

Routes to Grafana via Phase 4 OTel/Loki → real-time error rate, Hard Gate trigger alerts.  
No new components required — structlog infrastructure already exists.

### Tier 2: Batch analytical metrics (daily / weekly)

`quality_events` is extracted to BQ by Airflow alongside ODS. dbt builds `rpt_quality_*` models on top:

```
rpt_quality_daily
├── date, rule_version
├── total_count, clean_count, quarantine_count, promoted_count
├── quarantine_rate, promotion_rate
└── sliceable by rule_version to compare quality across versions

rpt_quality_field_breakdown
├── which fields most frequently trigger has_clean_error
├── per-field error rate trend over time
└── source: parse clean_error_message (or a future JSONB quality_profile column)

rpt_quality_version_comparison
├── how many quarantined under v1 → promoted under v2 → still in quarantine
└── quantifies the real-world impact of each rule change
```

Connected to Looker Studio for long-term trend analysis.

### Why historical metrics are never retroactively rewritten

`quality_events` is append-only — historical events are permanent:

```sql
-- Initial quarantine rate under v1 (never changes)
SELECT countif(to_state = 'quarantined') / count(*) AS quarantine_rate
FROM quality_events
WHERE event_type = 'initial_evaluation' AND rule_version = 'v1'

-- How many records were promoted by v2 (independent metric, does not overwrite v1 numbers)
SELECT count(*) AS promoted_by_v2
FROM quality_events
WHERE event_type = 'promotion' AND rule_version = 'v2'
```

---

## Implementation Scope

### ODS / PostgreSQL Layer

| Component | Layer | Status |
|---|---|---|
| `format_clean()` + `business_clean()` | Before ODS write | ✅ Done |
| `has_clean_error` + `clean_error_message` | ODS | ✅ Done |
| `DQ_RULE_VERSION` constant (`clean.py`) | ODS | ✅ Done |
| `dq_rule_version` column (ODS model) | ODS | ✅ Done |
| `QualityEvent` model + `quality_events` table | ODS | ✅ Done |
| `quality_events` write logic (`process.py` success path) | ODS | ✅ Done |
| structlog `quality_metric` event (`process.py`) | ODS | ✅ Done |

**quality_events write semantics**

| Scenario | Written? | to_state |
|---|---|---|
| ODS successfully written, no quality issues | ✅ | `clean` |
| ODS successfully written, quality issues present | ✅ | `quarantined` |
| pre-check intercepts duplicate (ODS not written) | ❌ | — |
| TOCTOU IntegrityError (ODS not written) | ❌ | — |
| Pipeline failure → Raw status = error (ODS not written) | ❌ | — |

### BQ Analytics Layer

| Component | Layer | Status |
|---|---|---|
| dbt `stg_*` Hard Gate tests | BQ Analytics | ⬜ Phase 4 |
| `int_orders` Row Filter (`WHERE has_clean_error = FALSE`) | BQ Analytics | ⬜ Phase 4 |
| `int_orders_quarantine` dbt model | BQ Analytics | ⬜ Phase 4 |
| Airflow re-evaluation task (Proposal B) | BQ Analytics | ⬜ Phase 4 |
| `rpt_quality_*` dbt models | BQ Analytics | ⬜ Phase 4 |

---

## Known Boundaries and Design Decisions

**A/B remediation boundary must be explicit**  
`force=True` (Proposal A) is only valid for `Raw.status = "error"` or `"duplicate"` records.  
Quarantine records (`has_clean_error = TRUE`, `status = "processed"`) must go through Proposal B re-evaluation.  
Misuse results in a 400 from `force=True` with no clear diagnostic message — this boundary must be documented in operational runbooks.

**ODS and BQ quality state permanently diverge**  
ODS always reflects the quality assessment at ingestion time, with `dq_rule_version` recording which rule version was in effect.  
BQ `dim_*/fct_*` reflects the quality state under the latest evaluation.  
This divergence is an intentional design decision. Traceability is provided by `dq_rule_version` + `quality_events`.

**Hard Gate thresholds are business judgements**  
Suggested starting values: error rate > 10% blocks the run; > 5% warns.  
Actual thresholds should be calibrated once real traffic data is available. Initial values are conservative estimates.

**`quality_events` does not yet cover BQ-layer promotion events**  
Proposal B (Airflow re-evaluation) is not yet implemented. Currently `quality_events` only records `initial_evaluation` events written at ingestion time.  
When Airflow is introduced in Phase 4, the write logic for `promotion` and `permanently_rejected` events must be added alongside it.
