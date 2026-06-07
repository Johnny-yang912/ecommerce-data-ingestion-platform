# Data Quality Control Architecture

## Design Goal

Ensure that data entering the analytics layer (Star Schema) is as clean as possible.  
ODS serves as an immutable anchor that preserves the complete state of all data. Quality control responsibility tightens progressively as data flows downstream.

---

## Quality Contract per Layer (Q0)

```
Raw (PostgreSQL)                               ← Landing
  Responsibility : Persist the original request body verbatim; extract order_id only, as a key traceability field
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

## Ingestion Layer: Upstream Anomalies and Two-Signal Governance

The ODS ingestion boundary produces two quality signals that are **kept separate and carry different authority**. Core principle: **"is the value correct" and "has the upstream contract changed" are two different questions — flagged separately, handled separately.**

### Authority boundary of the two signals

| Aspect | `has_clean_error` | `has_schema_drift` |
|---|---|---|
| Meaning | The **value** of this record has a business problem | The **structure/contract** the upstream sent has changed |
| Typical sources | quantity ≤ 0, rating out of range, NaN/Inf, future date, over-long text, numeric sentinel | unexpected field, renamed field, type drift, non-object nested group |
| Message column | `clean_error_message` | `schema_drift_message` + `unmapped_fields` |
| **Authority over Gold** | **Can block**: `int_*` quarantines it via `WHERE has_clean_error=FALSE` | **Cannot block**: a clean order still flows to Gold even with drift; drift is purely a monitoring signal |
| Part of the `quality_events` state machine? | ✅ Yes (`initial_evaluation` → `clean`/`quarantined` → `promoted`…) | ❌ No (not a quality-state evolution; an ops signal) |
| Tied to rule version? | ✅ Evolves with `DQ_RULE_VERSION` | ❌ Unrelated to rule version; relates to how code maps the schema |
| Remediation path | **Proposal B** (re-evaluate under a new rule version) / `force=True` (re-run a failed pipeline) | **Engineering action**: align the contract with upstream / add a field mapping / update the Pydantic model — **not** rule re-evaluation |
| Observability | `quality_metric` log, `rpt_quality_*` | `schema_drift` log, `ingress_rejected` log, (Phase 4) drift-rate monitoring |

In one line: `has_clean_error` has the authority to keep data out of Gold; `has_schema_drift` **does not** — it can only alert and ask a human to realign the contract.

### Four quadrants: signal combination → action → result

| `has_clean_error` | `has_schema_drift` | Situation | Action | Result |
|:---:|:---:|---|---|---|
| FALSE | FALSE | Fully clean | Flows normally | Reaches Gold (`dim_*`/`fct_*`); `quality_events` → `clean` |
| TRUE | FALSE | Value has a business problem | `int_*` Row Filter blocks it | Goes to `int_orders_quarantine`; `quality_events` → `quarantined`; Proposal B |
| FALSE | TRUE | Contract changed but the **value is clean** | **Still flows to Gold** + drift alert | Reaches Gold (not blocked); engineering is notified to realign the contract / add a mapping |
| TRUE | TRUE | Value is bad **and** contract changed | Blocked by `has_clean_error` + drift alert | Goes to quarantine (value issue via Proposal B); drift handled separately by engineering. **The two paths are independent** |

The third quadrant is the key design point: **an otherwise-good order that merely carries an extra `loyalty_points` field is not kicked out of Gold** — exactly why a separate signal was chosen over overloading `has_clean_error`.

### Upstream anomalies and handling (15-item map)

| Anomaly | Signal / mechanism | Result |
|---|---|---|
| Unexpected new field | `has_schema_drift` (`UNEXPECTED_FIELD`) | Lands; new field stored in `unmapped_fields`, existing columns unaffected |
| Missing expected field | ingress relaxed (lands as NULL); detection deferred | Lands as NULL; missing-field detection via Phase 4 null-rate monitoring |
| Renamed field | new name → `UNEXPECTED_FIELD`; old name → NULL | New name flagged + captured; old name NULL |
| Changed type | coercible → `TYPE_DRIFT`; hard error → 422 | Coercible lands + flagged; hard type error 422 + `ingress_rejected` |
| Changed date format / timezone | format error → 422; timezone → contract | Format error 422 + log; timezone is a written contract (see boundaries) |
| Unseen enum value | lands; length handled by over-long path; detection deferred | New value lands; over-long no longer stalls; Phase 4 `accepted_values` (warn) |
| Semantic drift | — | Deferred to Phase 4–5 distribution monitoring (rules cannot catch it) |
| No data at all | — | Deferred to Phase 5 OTel volume/freshness alerting |
| Same order_id resent | existing idempotency | first-write-wins; duplicates marked `duplicate` |
| Non-object nested group | `has_schema_drift` (`NON_OBJECT_GROUP`) + defensive guard | No crash; flagged, that group lands as NULL |
| sentinel / fake nulls | `format_clean` normalization (strings); range check (numbers) | String sentinels → NULL; numeric sentinels flagged `has_clean_error` |
| Over-long string vs column cap | `has_clean_error` (`FIELD_TOO_LONG`) + generous DB wall + fast-fail | Moderately long → flagged + lands; egregious → terminal `error` (no more poison-pill) |
| NUL byte | stripped before write + warning | Stripped and landed; no more 500 / dropped order |
| NaN / Infinity | `has_clean_error` (`NON_FINITE_NUMBER`) | Flagged + lands; quarantined downstream; does not poison aggregates |
| Future date / clock skew | `has_clean_error` (`ORDER_DATE_IN_FUTURE`); extraction `>=` | Future date flagged; clock rollback mitigated by `>=` in incremental extraction |

### Relationship to `DQ_RULE_VERSION`

`DQ_RULE_VERSION` versions only the **business value-evaluation rules** (`business_clean`), not the schema mapping. The two are orthogonal axes:

> **An upstream contract change by itself does not bump `DQ_RULE_VERSION`**; it is bumped only when you modify `business_clean` (the value-evaluation rules) in response.

But there is an **indirect causal chain** between them: schema drift (an upstream change) often **forces a business-rule change** — e.g. a new enum value must now be validated, semantic drift requires tightening a range, a newly-mapped field needs a range check — and that is when you bump. So "schema drift indirectly causes a bump" is common in practice, but to be precise: the trigger is "**you changed `business_clean`**," not "the upstream changed."

Bump criterion: **if you re-run the same raw payload, would `has_clean_error` / `clean_error_message` come out different?**

| Change | Changes the value-evaluation result? | Bump? |
|---|---|---|
| Add/modify a `business_clean` rule (new check, changed threshold) | ✅ | **Yes** |
| A `format_clean` change that **affects later evaluation** (e.g. a new sentinel→NULL changes which values get flagged) | ✅ | **Yes** |
| Add a field mapping (`from_nested` picks up one more field) | ❌ | No (goes through code review / migration) |
| Renamed-field remapping | ❌ | No |
| Change to `detect_schema_drift` logic | ❌ (a different signal; does not touch `has_clean_error`) | No |

**This v1 → v2 bump**: the ingestion hardening added three `business_clean` rules — `FIELD_TOO_LONG`, `NON_FINITE_NUMBER`, `ORDER_DATE_IN_FUTURE` — and sentinel normalization that affects evaluation; re-running the same raw payload yields a different `has_clean_error`, hence the bump. These rules are **stricter** (they flag more), so they apply going forward only and need **no retroactive re-evaluation**; retroactive re-evaluation only applies when rules are loosened to promote old quarantined rows (the `re_quarantined` edge case in the state machine).

### Observability and alerting

- **Per record**: `quality_metric` (includes `has_clean_error`), `schema_drift` (drift detail), `ingress_rejected` (records blocked by the hard gate that never land).
- **Batch** (Phase 4): besides `rpt_quality_*`, a **drift-rate threshold alert** can be added (analogous to the Hard Gate, but an over-threshold drift rate only **alerts, never aborts** the run — because drift has no blocking authority).

### Design boundaries

- **order_id is the only hard gate**: missing order_id → 422 (does not land, logged as `ingress_rejected`); every other missing/type/structure problem always "lands + is flagged," pushing the judgment downstream.
- **Timezone semantics are a contract, not an algorithm**: a bare `order_date` cannot reveal timezone drift; it is resolved by an explicit (UTC) contract. The accompanying future-date guard is only a sanity check.
- **`quality_events` does not record schema drift**: keeping its semantics as a "business quality state machine" clean (consistent with the semantic boundary in *Rule Versioning and the quality_events Table*).

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

### Mechanism 3: Scenario-Specific Analysis Models (int_* layer)

Specific analytical scenarios can build dedicated models at the `int_*` layer. `clean_error_message` is a JSONB array of objects (`{"code", "field", "value", ...}`), so scenarios match on the stable `code` rather than human-readable wording, accept records that are globally quarantined but whose errors are irrelevant to the scenario, and apply field-level imputation where needed:

```sql
-- int_orders_shipping_analysis.sql
SELECT
    *,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM UNNEST(clean_error_message) AS e
            WHERE JSON_VALUE(e, '$.code') = 'customer_rating_out_of_range'
        )
        THEN NULL
        ELSE customer_rating
    END AS customer_rating_cleaned
FROM {{ ref('stg_orders') }}
WHERE
    has_clean_error = FALSE
    OR (
        -- only error is rating, which is irrelevant to shipping analysis
        ARRAY_LENGTH(clean_error_message) = 1
        AND JSON_VALUE(clean_error_message[0], '$.code') = 'customer_rating_out_of_range'
    )
```

**Audit trail**: Repair logic co-exists with transformation logic in the SQL file. The dbt model description records which errors the scenario accepts and why. The SQL itself is the audit trail, versioned in git.

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
DQ_RULE_VERSION = "v2"    # bump on every rule change; pair with a git tag documenting what changed
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
└── reason:       JSONB?     list[dict] {code, field, value, ...}, same format as ODS.clean_error_message
```

**When records are written:**
- `process.py` after a successful ODS write → inserts one `initial_evaluation` event
- Airflow re-evaluation promotes or rejects a record → inserts one `promotion` or `rejection` event

**Semantic boundary:**
This table is strictly a global state machine, recording only ingestion-layer events and cross-layer (PG → BQ) Proposal B evaluation events. Imputation decisions made inside scenario-specific BQ models are **not written back** to this table — scenario repair is an analytics-layer business logic decision, not a progression of data quality state.

---

## Data Consistency (Q2 Extension)

### Divergence between ODS and BQ

Divergence **will exist** — it has two distinct sources, each handled differently:

**Case 1: Divergence from rule version evolution**
Traceable via `dq_rule_version` + `quality_events`:

```
Without versioning:
  ODS has_clean_error = TRUE,  BQ dim_* is clean  →  "Why are they different?" — no answer

With versioning and quality_events:
  ODS has_clean_error = TRUE,  dq_rule_version = "v1"   ← truth at ingestion time
  quality_events: promoted under "v2" at 2026-03-01      ← truth after evolution
  → divergence is explained and auditable
```

**Case 2: Divergence from scenario-specific models**
Globally quarantined records may appear in specific scenarios' `dim_*/fct_*` tables when a scenario model accepts errors irrelevant to that scenario. This divergence is explained by reading the scenario model's SQL and dbt description — no separate tracking table is maintained.

This is an intentional design boundary: the explainability requirement for scenario repair is static (read the code), not a runtime auditing need. SQL documentation is sufficient.

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
└── source: clean_error_message (JSONB array of objects) — UNNEST and read e.field / e.code directly, no text parsing required

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
| Scenario-specific `int_orders_*` models (JSONB filter + imputation) | BQ Analytics | ⬜ Phase 4 |
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

**Scenario repair audit trail is SQL documentation**  
Repair logic in scenario-specific `int_*` models is documented in the dbt model SQL and model description — no separate tracking table is maintained. This assumes no runtime cross-system auditing requirement exists; if that need arises, a BQ-layer lifecycle table can be introduced at that point.

**`stg_*` boolean flags deferred**  
Scenario models currently match on the stable `code` inside the `clean_error_message` JSONB array directly (the `code` is decoupled from human-readable wording, so wording changes no longer break these queries). When the same `code` conditions need to be maintained across many scenario models, parse the JSONB into structured boolean columns (e.g. `has_rating_error`) in `stg_orders` to centralise the coupling in one place.
