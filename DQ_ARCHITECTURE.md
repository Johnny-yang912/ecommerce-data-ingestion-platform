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
| Renamed field | Decomposes into the two rows above: new name = "unexpected field"; old name = "missing field" | Same as above: new name captured in `unmapped_fields`; old name lands NULL |
| Changed type | coercible → `TYPE_DRIFT`; hard error → 422 (see "Changed type: from coercion behavior to declaration governance" below) | Coercible lands + flagged; hard type error 422 + `ingress_rejected` |
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

### Changed type: from coercion behavior to declaration governance (row 4 expanded)

How the ingress layer handles an upstream "changed type" depends on whether Pydantic's lax mode can coerce the value into the declared type — and this is **asymmetric in the two directions**:

| Direction | Example | Pydantic behavior | Result |
|---|---|---|---|
| Should be string, upstream sends a number | `customer_name: 123` | Does not coerce int→str | `ValidationError` → 422 + `ingress_rejected` (does not land) |
| Should be a number, upstream sends a coercible string | `age: "00501"` | Silently coerces `"00501"→501` | Passes, lands, value computes correctly downstream |

The first row is a "hard type error," rejected cleanly at the boundary. **The real blind spot is the second row**: `"00501"→501` conforms to the schema and computes correctly downstream, but the fact that "upstream sent an integer field as a string this time" is silently swallowed at the Pydantic layer. This is exactly why `TYPE_DRIFT` exists — `detect_schema_drift` bypasses Pydantic and runs on the **verbatim-preserved raw payload** (the landing layer deliberately does not re-serialize through `OrderIN`, see "Design boundaries"), comparing JSON-native types against the contract and recording the pre-coercion true type as `has_schema_drift` + `TYPE_DRIFT` (non-blocking).

Coercion itself also has a boundary — it is not "any string passes silently": only a **clean, integer-parseable string** passes (`"501"`, `" 501 "`; `"12.0"→12` truncates), while `"12.5"` and `"abc"` are still rejected as 422. So row 4 precisely means: **coercible** (value lands in the declared type) → lands + `TYPE_DRIFT` flag (observed); **hard type error** (value cannot convert) → 422 + `ingress_rejected` (does not land).

And since coercion is "alignment toward the declaration," **the declaration itself decides what gets silently rewritten** — pushing the problem up from "the value" to "the declaration." Identifier-like fields (`postal_code`, `customer_id`, `product_id`) are all declared `str` precisely to preserve leading zeros: declare one as `int` by mistake and `"00501"` gets silently truncated to `501`, semantics lost and hard to notice; conversely, only quantities that are "conceptually computable" (`age`, `delivery_days`, `tax_pct`) are declared `int/float`. So the discipline when setting a type is not a formatting concern — it **decides which deviations get silently swallowed and which get seen by `TYPE_DRIFT`**.

This also exposes the limit of `TYPE_DRIFT`: it can catch "the value type upstream sent ≠ the declaration," but **cannot judge whether the declaration itself is correct** — because its comparison baseline *is* that declaration, and a declaration cannot validate itself. If the baseline is wrong, `TYPE_DRIFT` just measures with the wrong ruler. The declaration therefore needs its own protection, in three layers — the first two automatable, the third necessarily human:

| Layer | Mechanism | Guards | Does not guard |
|---|---|---|---|
| 1 Cross-layer consistency | `tests/test_schema_db_consistency.py`: `ODSOrder` (Pydantic) ↔ `ODS` (SQLAlchemy), per-field `python_type` comparison | Changing schema.py but forgetting models.py (or vice versa); missing mappings | Both layers declared wrong together |
| 2 Contract snapshot | `tests/test_schema_snapshot.py`: `model_json_schema()` against a committed golden file | Any type-declaration change becomes a failing test + a reviewable diff | An intentional-but-wrong change (the snapshot updates with it) |
| 3 Human governance | CODEOWNERS (`schema.py` / `models.py` / `tests/snapshots/`) + an upstream data contract | "Is this type actually correct" | — (this layer is the final arbiter) |

The first two layers collapse "pure discipline" into "a test that goes red + a diff that gets seen," but they only answer **consistent / not silently altered**; **the correctness question "should `age` be an int in the first place" cannot be self-validated by any test**, because "correct" is defined as "matches the contract agreed with upstream," which needs a source of truth outside the declaration. So the final layer cannot escape human judgment: **CODEOWNERS** forces a designated data owner to review schema changes so the snapshot diff is actually looked at (mechanism 2 provides the hook, the human provides the judgment); a **data contract** writes down each field's agreed type and rationale so review has a comparable baseline; and the existing `TYPE_DRIFT` **drift rate** can be used in reverse — a field whose drift rate is chronically high is reasonable grounds to suspect not that upstream is persistently wrong, but that your own declaration is (see "Observability and alerting").

**Current status**: mechanisms 1 and 2 are in place (tests green); mechanism 3's CODEOWNERS and data contract are team-governance items.

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

Applied in `dbt int_*` SQL — isolates individual dirty records. The Row Filter's decision basis is **not the `has_clean_error` snapshot in ODS, but the "effective quality state"** — the ingestion-time verdict composed with any later evolution in `quality_events` (a Proposal B promotion).

```sql
-- int_orders.sql  (clean data flow, includes Proposal B re-evaluation flow-back)
WITH latest_quality_state AS (
    -- one row per raw_id: the latest quality event (append-only, so order by event_at and take the first)
    SELECT
        raw_id,
        to_state,
        ROW_NUMBER() OVER (
            PARTITION BY raw_id ORDER BY event_at DESC
        ) AS rn
    FROM {{ ref('stg_quality_events') }}
)
SELECT s.*
FROM {{ ref('stg_orders') }} s
LEFT JOIN latest_quality_state q
    ON s.raw_id = q.raw_id AND q.rn = 1
WHERE
    s.has_clean_error = FALSE      -- clean at ingestion time
    OR q.to_state = 'promoted'     -- or promoted by Proposal B re-evaluation (latest state = promoted)

-- int_orders_quarantine.sql  (records still isolated = effective state not clean)
WITH latest_quality_state AS (
    SELECT
        raw_id,
        to_state,
        ROW_NUMBER() OVER (
            PARTITION BY raw_id ORDER BY event_at DESC
        ) AS rn
    FROM {{ ref('stg_quality_events') }}
)
SELECT
    s.*,
    CURRENT_TIMESTAMP() AS quarantined_at
FROM {{ ref('stg_orders') }} s
LEFT JOIN latest_quality_state q
    ON s.raw_id = q.raw_id AND q.rn = 1
WHERE
    s.has_clean_error = TRUE
    AND (q.to_state IS NULL OR q.to_state != 'promoted')  -- never promoted, or promoted then re_quarantined
```

**Why can't the Row Filter just read `has_clean_error`?**
ODS is an immutable anchor — `has_clean_error` is frozen at the **ingestion-time** verdict (under that `dq_rule_version`), and a record promoted by Proposal B **still reads `has_clean_error=TRUE` in ODS**. If the Row Filter were written literally as `WHERE has_clean_error = FALSE`, promoted records would stay stuck in quarantine forever and never flow back to Gold. The "effective quality state" must therefore be **composed** by `int_*` on every dbt run, joining the ODS snapshot with the latest `quality_events` event — `has_clean_error` is the snapshot from the `initial_evaluation` event, while the latest `to_state` in `quality_events` is the current truth. This is the seam that lets "re-evaluate without modifying ODS, append `quality_events` only" (see *Bounded Writeback Principle*) actually flow data back. The `re_quarantined` edge case is covered automatically: as long as the latest event is not `promoted`, the record stays in quarantine, with no extra condition needed.

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

### Remediation: A + B + C together

A, B and C fix **different types of problems**. It is essential to know which path applies to which scenario:

| | What it fixes | Path |
|---|---|---|
| **A — force=True** | Records with `Raw.status = "error"` or `"duplicate"` (never successfully written to ODS) | `POST /process_raw/{raw_id}?force=true` → re-runs pipeline → flows downstream naturally |
| **B — Airflow re-evaluation** | Records with `Raw.status = "processed"` + `has_clean_error = TRUE` (in ODS, but quarantined at BQ layer) | Re-evaluate against updated rules → write to `quality_events` → promoted on next dbt run |
| **C — batch repair (backfill)** | Records with `Raw.status = "processed"` whose **values themselves were corrupted** by a value-production defect (`format_clean` / `from_nested` bug) — the pipeline did not fail and the rules did not misjudge | Batch re-derive from Raw under the fixed logic → land via one of two shapes (see *Proposal C* below) → cascade a downstream refresh |

> **Important**: `force=True` does **not** work on quarantine records (`status = "processed"` → returns 400).  
> Quarantine records have a **rule evaluation problem**, not a pipeline failure — re-running the pipeline cannot fix them.  
> And when the values themselves were corrupted by a production defect, neither A nor B can help: B's input *is* the corrupted ODS values, and Bounded Writeback forbids B from writing values. That is Proposal C's territory.

### Proposal B: re-evaluation without re-running the pipeline

B targets records with `Raw.status = "processed"` and `has_clean_error = TRUE` — **they already landed cleanly in ODS (and are mirrored to BQ staging)**; they were merely judged dirty by an older rule version and isolated at `int_*` by the Row Filter. B's job is to "re-evaluate against the new rules," not to re-run the pipeline.

**Why no pipeline re-run / no reading back the raw payload?**
Because the ingestion layer deliberately runs `format_clean` **before** `business_clean` — a quarantine record that reached ODS already has its **values normalized** (lowercased, trimmed, sentinel→NULL, type-aligned); only the business rules judged it non-compliant. And `DQ_RULE_VERSION` versions only `business_clean` (the value-evaluation rules), never the schema mapping. So "v2 re-evaluation" = **take that record's already-normalized ODS column values and re-run the new `business_clean` once**; all inputs already live in ODS — no raw payload, no re-flattening required.

> Contrast with A (`force=True`): an A record has `status=error`, so **ODS has no such row** — there is nothing to re-evaluate, and the only option is to re-walk the whole pipeline from Raw. This is exactly why `force=True` returns 400 on `processed` records: re-running the pipeline on a quarantine record is using the wrong tool for the wrong problem.

**Where the re-evaluation result is written — Bounded Writeback: append to `quality_events` only, never touch ODS.**
ODS stays frozen at the ingestion-time truth (`has_clean_error=TRUE, dq_rule_version="v1"`). Once re-evaluation passes, the new fact does not overwrite ODS — it appends one row to the append-only `quality_events`:

```
event_type:  "promotion"
from_state:  "quarantined"
to_state:    "promoted"
rule_version: "v2"
event_at:     <re-evaluation time>
reason:       null (or any residual, now non-blocking, message)
```

This keeps historical metrics from being retroactively rewritten: how many v1 blocked and how many v2 promoted are two independent, permanently-preserved metrics (see *Historical Quality Metrics*).

**How it flows back to Gold.**
On the next dbt run, `int_orders` composes the effective quality from "ODS snapshot + latest `quality_events` state" (see the JOIN version under *Mechanism 2: Row Filter*); a record whose latest `to_state='promoted'` is treated as clean and flows naturally into `int_orders → dim_*/fct_*`. The **permanent divergence** of ODS saying dirty (v1) while BQ Gold says clean (v2) is intentional by design, with traceability provided by `dq_rule_version` + `quality_events`.

**Full data flow:**

```
[ODS] has_clean_error=TRUE, dq_rule_version=v1   ← always this snapshot, never modified
   │
   │  Airflow re-evaluation task (Proposal B, Phase 5)
   ├─ 1. pull staging records with has_clean_error=TRUE (column values already normalized)
   ├─ 2. re-run v2 business_clean on those existing ODS column values   ← no raw, no pipeline re-run
   ├─ 3a. passes v2      → append quality_events: promotion (quarantined → promoted, v2)
   └─ 3b. still fails v2 / manually written off → append rejection (→ permanently_rejected)
   │
   ▼
[quality_events]  append-only; the new fact lives here (ODS is not changed)
   │
   ▼
next dbt run: int_orders composes effective quality from "ODS + latest quality_events state"
   → records whose latest state is promoted flow into int_orders → dim_*/fct_*
```

> **Applicability boundary**: this v1→v2 bump is **stricter** (flags more), so it applies going forward only and needs **no retroactive re-evaluation**. Retroactive re-evaluation (B's promote path) only triggers when rules are **loosened** to pull old quarantine back; tightening rules can instead push existing `promoted` records to `re_quarantined` on re-evaluation (the state-machine edge case).

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

## Proposal C: Batch Repair of Historical Value Defects (Q2 Extension — Directional Design)

> **Positioning.** A (`force=true`) and B (rule re-evaluation) cannot reach one class of problem: a **value-production defect** corrupting already-`processed` historical values — e.g. a `format_clean` sentinel list mistakenly treating a legitimate value as a fake null (`"na"` = North America → NULL), washing a field to NULL across thousands of `processed` records. B's input *is* the corrupted ODS values, and Bounded Writeback forbids B from writing values; A returns 400 on `processed`. If this path does not exist by design, the promise that Raw verbatim "enables rebuilding" can never be honored.
>
> This section therefore **pre-defines the shape of the repair path without pre-committing to a choice**. When an incident actually happens, the team should weigh blast-radius size, number of affected fields, downstream consumption, risk appetite toward operating on the anchor table, repair urgency, and operational capacity — and decide between the two shapes on the spot. This section provides direction, trade-offs, and the cautions that apply no matter which path is chosen. The two shapes are not mutually exclusive: different incidents may warrant different shapes, and a team may patch first to stop the bleeding, then rebuild later at leisure.
>
> Whichever shape is chosen, Proposal C is executed as an **offline, runbook-driven batch operation** (a deliberate infra event) — deliberately *not* an HTTP endpoint, so `force=true` remains the only runtime remediation surface and its semantic boundary stays undiluted.

### C-1 The two shapes

The **re-derivation side is identical** in both (re-produce values from Raw with the fixed logic); they diverge only in *where the corrected values land and how they take effect*:

| | Migration shape: Scoped Rebuild | Patch shape: Correction Overlay |
|---|---|---|
| Core semantics | Affected rows **replaced in place** in main ODS (retire old rows + write back new rows in one txn, tagged `rebuild_batch_id`) | Main ODS **untouched**; corrected rows land in a separate `ods_corrections` table |
| Post-repair truth | Main table is the single truth; old values retired into `ods_retired_<batch>` for audit | Truth split across two tables; correct value = main table + overlay, composed at read time |
| ODS immutability contract | Requires re-interpretation: what is forbidden is "single-row, unversioned, downstream-unaware" rewriting; a batch, versioned, retired-copy-keeping, downstream-cascading replacement is the legitimate escape hatch | Literally fully honored, zero re-interpretation |
| BQ landing | Corrected rows appended into the **same staging table** (original `received_at` partitions); `stg_` dedup tie-breaks on `rebuild_batch_id DESC` | Corrections become a second BQ table; `stg_` overlays via JOIN / COALESCE |
| `stg_` complexity | Existing dedup logic + one tie-breaker; no new JOIN seam | Adds a permanent JOIN seam; every upstream reader of `stg_` must know the overlay exists |
| Cloud cost | ≈ 0 (reuses the existing channel) | Also negligible in money (BQ bills by bytes scanned; a small-table JOIN rounds to zero) — the cost worry is a non-issue; the real price is semantic |
| Incident stacking (repeat repairs) | Each repair is just another appended batch + batch id; read logic unchanged | Each repair adds another correction batch; overlay must manage batch precedence; read-path complexity grows per incident |
| Future full re-extract of staging | Safe — main table already correct | Main-table wrong values get re-extracted verbatim; corrections must be re-pushed (see P3) |
| Other PG-side consumers | Automatically get correct values | Get wrong values unless they implement the overlay themselves |
| Rollback | A reverse batch (cover back from the retired table) — same mechanism, self-supporting | Simplest — void the correction batch; the main table was never touched |
| Point of no return | The PG commit (gated by dry-run diff + human confirmation) | None — naturally low-risk |
| Cost structure | Heavier operation, **paid once**; architecture returns to its original shape afterwards | Very light operation, fast to take effect, but complexity becomes **permanent** in the read path and future ops |
| Leaning factors | Large blast radius (systemic, multi-row multi-field corruption); a future staging full re-extract is expected; team values long-term single truth; an ops window exists for a rebuild | Small, targeted blast radius; risk appetite forbids touching the anchor table; need to stop the bleeding fast; or no staffing / window for a migration-style operation |

### C-2 Cautions both shapes must face (unavoidable either way)

| # | Caution | Content |
|---|---|---|
| 1 | Deployment order | **Deploy the fix first** to stop the bleeding — only then is the blast window's right edge frozen; repairing before deploying chases a moving target forever |
| 2 | Blast-window scoping | Scope by **`ODS.received_at`** (the moment values were produced), not `Raw.received_at` — records re-processed later by the recovery scan have Raw timestamps outside the window and would be missed |
| 3 | Re-derivation path | Reuse only the pure functions `from_nested → clean_order`; **never go through `process_raw_event`** — its first-write-wins pre-check would see the very row being replaced and mark the whole batch `duplicate` |
| 4 | Active push | Corrected rows carry old `received_at` values (old partitions); the watermark (forward-looking only) will never see them — pushing to the cloud is an explicit runbook step, not the scheduled extraction (see [CLOUD_LAYER.md](./CLOUD_LAYER.md) §7) |
| 5 | Batch version axis | `DQ_RULE_VERSION` versions evaluation semantics only — a `format_clean` value bug may change values **without** changing `has_clean_error`, escaping the bump criterion entirely; value production needs its own batch id (a `reprocess_batches` registry), which doubles as the functional tie-breaker for `stg_` dedup / overlay precedence — not merely an audit column |
| 6 | quality_events | Re-run `business_clean` on the corrected values and append a `re_evaluation` event (carrying the batch id), or the Row Filter's effective-state composition stops reconciling |
| 7 | Late-arriving | Corrected values land in old partitions; a `received_at`-incremental `stg_` run won't see them — the runbook's final step must be a targeted refresh of the affected partitions (see [CLOUD_LAYER.md](./CLOUD_LAYER.md) §7) |
| 8 | Divergence window | Between PG commit and BQ refresh, PG holds new values while BQ Gold still holds old ones — isomorphic to Proposal B's flow-back delay (acceptable under T+1 eventual consistency), but push + refresh must be bound runbook steps, never left half-done |

### C-3 Additional cautions when choosing the migration shape

| # | Caution | Content |
|---|---|---|
| M1 | Atomicity | Retired copy, main-table delete+insert, and the `quality_events` events go in **one transaction** — splitting them leaves a "values swapped but state machine unrecorded" crack |
| M2 | statement_timeout | The global 30s timeout is designed for short online transactions and will kill a ten-thousand-row batch — the rebuild connection must override it |
| M3 | Concurrency safety | Under MVCC the intermediate state is invisible: concurrent duplicate pre-checks read the old rows; a TOCTOU INSERT blocks until commit then hits `IntegrityError`, identical to normal behavior (no extra handling needed, but must be understood and documented) |

### C-4 Additional cautions when choosing the patch shape

| # | Caution | Content |
|---|---|---|
| P1 | Overlay precedence | When the same `raw_id` is corrected twice, the overlay must implement "latest batch wins" itself (the migration shape gets this for free from the main table) |
| P2 | A second extraction path | The corrections table needs its own `FIELDS` declaration, extraction logic, and a consistency guard on par with `test_schema_bq_consistency` |
| P3 | Full re-extract runbook | Any staging rebuild (e.g. repartitioning) must re-push corrections — write it into the rebuild steps explicitly, or the wrong values resurrect |
| P4 | Consumer contract | "Reading ODS requires applying the overlay" becomes a new implicit contract, needing documentation and guards against future direct reads of the main table |

### C-5 Factors to weigh at decision time (no pre-baked answer)

When the incident happens, walk through at least the following before picking a shape:

- **Blast radius and shape**: how many rows? how many fields? one contiguous window or scattered?
- **Downstream consumption**: which reports / models have consumed the wrong values? how urgent is the repair?
- **Risk appetite**: can the team accept a batch operation on the anchor table? is there a window and staffing for a dry-run review?
- **Long-term maintenance**: who owns the overlay seam? will the team remember it exists a year from now?
- **Recurrence**: is this class of incident one-off or expected to recur? (recurring → price in the compounding cost of a permanent seam)
- **Hybrid path**: the shapes compose — patch first to stop the bleeding, then converge back to single truth with a scoped rebuild when an ops window opens (the patch batch is valid input to the rebuild; the mechanisms are compatible)

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
| `stg_quality_events` dbt model (for `int_*` to JOIN the latest quality state) | BQ Analytics | ⬜ Phase 4 |
| `int_orders` Row Filter (JOIN latest `quality_events` state: `has_clean_error=FALSE OR to_state='promoted'`) | BQ Analytics | ⬜ Phase 4 |
| `int_orders_quarantine` dbt model | BQ Analytics | ⬜ Phase 4 |
| Scenario-specific `int_orders_*` models (JSONB filter + imputation) | BQ Analytics | ⬜ Phase 4 |
| Airflow re-evaluation task (Proposal B) | BQ Analytics | ⬜ Phase 4 |
| `rpt_quality_*` dbt models | BQ Analytics | ⬜ Phase 4 |

---

## Known Boundaries and Design Decisions

**A/B/C remediation boundaries must be explicit**  
`force=True` (Proposal A) is only valid for `Raw.status = "error"` or `"duplicate"` records.  
Quarantine records (`has_clean_error = TRUE`, `status = "processed"`) must go through Proposal B re-evaluation.  
Historical values corrupted by a value-production defect (pipeline succeeded, rules did not misjudge — the values themselves are wrong) are beyond both A and B; they go through Proposal C batch repair, a runbook-driven deliberate ops event, never an HTTP endpoint.  
Misuse results in a 400 from `force=True` with no clear diagnostic message — these boundaries must be documented in operational runbooks.

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
