# Data Quality Architecture

**English** | [繁體中文](../../zh-TW/design/data-quality.md)

How quality is judged, where it blocks, and how a judgement can change later without rewriting history.

---

## 1. Design goal

Turn untrusted inbound data into trustworthy analytical data, while keeping **every quality judgement auditable** — including judgements that were later revised.

Three properties make that possible, and each depends on the previous one:

1. **ODS is immutable and complete** — every accepted order exists exactly once, dirty or clean.
2. **Quality verdicts are events, not state** — an append-only log records transitions.
3. **Consumers compose the effective state** rather than reading a stored flag.

---

## 2. The two signals at ingestion

`clean_order()` and `detect_schema_drift()` produce two **independent, parallel, non-blocking** signals. They are never mixed.

### Authority boundary

| Aspect | `has_clean_error` | `has_schema_drift` |
|---|---|---|
| Meaning | the **value** of this record has a business problem | the **structure / contract** the upstream sent has changed |
| Typical sources | `quantity <= 0`, rating out of range, NaN/Inf, future date, over-long text, numeric sentinel | unexpected field, renamed field, type drift, non-object nested group |
| Message column | `clean_error_message` | `schema_drift_message` + `unmapped_fields` |
| **Authority over Gold** | **can block** — `int_*` quarantines it | **cannot block** — a clean order still flows to Gold even with drift |
| In the `quality_events` state machine? | ✅ yes | ❌ no — an ops signal, not a quality-state evolution |
| Tied to rule version? | ✅ evolves with `DQ_RULE_VERSION` | ❌ unrelated; it concerns how code maps the schema |
| Remediation path | Proposal B (re-evaluate) / `force=true` (re-run) | **an engineering action** — realign the contract, add a mapping, update the model. **Not** rule re-evaluation |
| Observability | `quality_metric` log, `rpt_quality_*` | `schema_drift` log, `ingress_rejected` log |

> In one line: **`has_clean_error` has the authority to keep data out of Gold; `has_schema_drift` does not.** It can only alert and ask a human to realign the contract.

### Four quadrants: signal combination → action → result

| `has_clean_error` | `has_schema_drift` | Situation | Action | Result |
|:---:|:---:|---|---|---|
| FALSE | FALSE | fully clean | flows normally | reaches Gold; `quality_events` → `clean` |
| TRUE | FALSE | the value has a business problem | `int_*` Row Filter blocks it | `int_orders_quarantine`; → `quarantined`; eligible for Proposal B |
| FALSE | TRUE | contract changed but **the value is clean** | **still flows to Gold** + drift alert | reaches Gold, not blocked; engineering is notified |
| TRUE | TRUE | value is bad **and** contract changed | blocked by `has_clean_error` + drift alert | quarantine (value issue → Proposal B); drift handled separately. **The two paths are independent** |

**The third quadrant is the key design point**: an otherwise-good order that merely carries an extra `loyalty_points` field **is not kicked out of Gold**. That is exactly why a separate signal was chosen over overloading `has_clean_error`.

### What is not a signal

Format normalisation is a third thing and does not set either flag: it **coerces** (trim, case, type alignment). Only business-rule violations set `has_clean_error`.

Unknown upstream fields are preserved in `unmapped_fields` rather than being silently dropped — so "the upstream sent something new" is recoverable rather than lost.

### The boundary of non-blocking

Non-blocking applies to **business-rule violations**, not to storage-level impossibilities. A field that overflows its column (`DataError`) or a string containing a NUL byte (`ValueError`) **cannot be written at all** — those fast-fail to the terminal `error` state and never reach ODS. [ADR-0006](../adr/0006-nul-byte-fast-fail.md)

The full inventory of upstream anomalies and how each is handled is in **[Appendix A](#appendix-a-upstream-anomalies-15-item-map)**.

---

## 3. Blocking: two mechanisms, two granularities

### Hard Gate — run-level, on `stg_`

Asks *"is the source broken as a whole?"* — mutation detection, not a cleanliness check. A failure halts the entire dbt run, leaving `int_`/`dim_`/`fct_` at their last clean state.

| | Scope | Threshold | Severity | Role |
|---|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | latest `received_at` partition | 15% | `error` | **gate** |
| `monitor_dataset_error_rate` | whole table | 10% | `warn` | **gauge** |

The gate is per-batch so its sensitivity does not decay as history accumulates, and so it can clear itself once upstream is fixed. The whole-table figure is kept for visibility and **deliberately given no blocking power**. [ADR-0028](../adr/0028-hard-gate-per-batch-scope.md)

Implemented as the custom generic test `macros/error_rate_below.sql` — a ratio assertion has to be made at the aggregate level via `HAVING`, because BigQuery rejects `COUNTIF` in a `WHERE` clause.

### Row Filter — record-level, in `int_`

Asks *"is this row usable?"* — dirty rows go to `int_orders_quarantine` rather than being dropped.

The criterion is the **effective quality state**, not the literal flag:

```sql
COALESCE(
    s.has_clean_error = FALSE      -- clean at ingestion time
    OR e.to_state = 'promoted',    -- or promoted by a later re-evaluation
    FALSE
) AS is_effectively_clean
```

ODS is immutable, so a promoted record reads `has_clean_error = TRUE` **forever**. Reading the flag literally would strand it in quarantine permanently.

**⚠️ Two things that must not be touched**: the `LEFT JOIN` (an inner join drops every record with no event — nearly all of them), and the `COALESCE` (`FALSE OR NULL = NULL`, and `WHERE NOT NULL` is also NULL, so the row **vanishes from both tables at once**). [ADR-0029](../adr/0029-effective-quality-state.md)

### Scenario-specific models — designed, not built

A scenario model may accept errors irrelevant to its question, apply imputation, and pass rows through for that scenario only. **Not built**, because deciding which errors are irrelevant requires knowing the analytical question — building one first would be a guess dressed as a design. [ADR-0027](../adr/0027-blocking-at-int-layer.md)

---

## 4. The quality event log

`quality_events` is append-only and records **transitions**, not state:

```
initial_evaluation
  ├── passes all rules          → clean
  └── has_clean_error = TRUE    → quarantined

quarantined / re_quarantined
  ├── re-evaluation passes      → promoted               (promotion)
  ├── re-evaluation fails       → no event written
  └── written off by a human    → permanently_rejected   (rejection)

promoted
  ├── stricter rules now fail   → re_quarantined         (re_quarantination)
  └── still passes              → no event written

permanently_rejected            ← terminal; no outgoing edge
```

Three properties are decisions in their own right:

- **"No event written" is deliberate** — appending only on an actual change makes the log its own idempotency gate.
- **`permanently_rejected` can only come from a human** — enforced at the PostgreSQL write target, not by a downstream filter.
- **`re_quarantination` was added after the fact and broke nothing** — because consumers count by `to_state`, not `event_type`.

Paired with `DQ_RULE_VERSION` (currently `v4`), stored per-row in `ods.dq_rule_version` and **never touched again**. [ADR-0031](../adr/0031-rule-versioning-quality-events.md)

### When does `DQ_RULE_VERSION` get bumped?

`DQ_RULE_VERSION` versions **only the business value-evaluation rules** (`business_clean`), not the schema mapping. The two are orthogonal axes.

> **An upstream contract change by itself does not bump the version.** It is bumped only when you modify `business_clean` in response.

There is an indirect chain — schema drift often *forces* a rule change, and that is when you bump — but the trigger is precisely **"you changed `business_clean`"**, not "the upstream changed".

**The criterion**: *if you re-ran the same raw payload, would `has_clean_error` / `clean_error_message` come out different?*

| Change | Changes the evaluation result? | Bump? |
|---|---|---|
| Add or modify a `business_clean` rule (new check, changed threshold) | ✅ | **Yes** |
| A `format_clean` change that **affects later evaluation** (a new sentinel→NULL changes which values get flagged) | ✅ | **Yes** |
| Add a field mapping (`from_nested` picks up one more field) | ❌ | No |
| Renamed-field remapping | ❌ | No |
| Change to `detect_schema_drift` logic | ❌ (a different signal; never touches `has_clean_error`) | No |
| Making a time-dependent rule take an injected `as_of` | ❌ (the ingestion path defaults to `as_of=None` = `now()`, so a payload's first evaluation is unchanged) | No |

**That last row exists because it fixes "does a re-run give the same answer?"** — it changes reproducibility, not the verdict, which is exactly why it must *not* bump.

### When is an event written at all?

An event exists only if an ODS row exists. The three "not written" rows are the ones people expect to see and do not:

| Scenario | Event written? | `to_state` |
|---|---|---|
| ODS written, no quality issues | ✅ | `clean` |
| ODS written, quality issues present | ✅ | `quarantined` |
| pre-check intercepts a duplicate (ODS not written) | ❌ | — |
| TOCTOU `IntegrityError` (ODS not written) | ❌ | — |
| Pipeline failure → `raw.status = error` (ODS not written) | ❌ | — |

> **`quality_events` is a log of quality judgements, not of ingestion attempts.** A record that never reached ODS was never judged — `raw.status` is where its fate is recorded ([ADR-0011](../adr/0011-no-result-backend.md)).

---

## 5. Remediation: three paths

| | Path | Reaches |
|---|---|---|
| **A** | `POST /process_raw/{id}?force=true` | a record stuck in `error`/`duplicate` — replay from Raw |
| **B** | Re-evaluation under new rules | records quarantined by rules that have since loosened |
| **C** | Batch correction from Raw | **value-production defects** — designed, not built |

### Proposal B: event-driven re-evaluation

`reevaluate_quality.py` reads candidates, re-runs the current rules, and appends **only on an actual state change**.

- **Candidates come from BigQuery's `int_` layer** — the same effective-state definition the Row Filter uses, so producer and consumer cannot disagree.
- **State is decided against PostgreSQL** — idempotency must not rest on a mirror that expires (the sandbox's 60-day limit).
- **Dry-run is the default**; commit is an explicit flag.

Two reproducibility guards:

| Guard | Prevents |
|---|---|
| `business_clean(as_of=...)` | a time-dependent rule giving a different answer purely because the clock moved |
| `NON_REPRODUCIBLE_CODES` | promoting a record because **the evidence disappeared** rather than because it passed |

> A re-evaluation that cannot see why a record failed must not conclude that it did not fail.

[ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md)

### Proposal C: what A and B cannot reach

A cleaning **bug** that corrupted values in already-`processed` records — for example a sentinel list treating `"na"` (North America) as a null, washing a column to NULL across thousands of rows.

B cannot help: **its input is the corrupted values**. Bounded writeback forbids it from writing values even if it could tell. A returns 400 on `processed`.

Two shapes were considered — a **migration** (new row, same `raw_id`, competing through the existing dedup) and a **patch** (a second table of corrections). The migration shape reuses machinery that already exists; the patch shape needs a hand-maintained second declaration and carries a re-extract landmine.

**If this path did not exist by design, the promise that "Raw kept verbatim enables rebuilding" would be unbacked.**

#### Cautions both shapes must face

| # | Caution | Content |
|---|---|---|
| 1 | Deployment order | **Deploy the fix first** to stop the bleeding — only then is the blast window's right edge frozen. Repairing before deploying chases a moving target forever |
| 2 | Blast-window scoping | Scope by **`ods.received_at`** (when values were produced), not `raw.received_at` — records reprocessed later by the recovery scan have Raw timestamps outside the window and would be missed |
| 3 | Re-derivation path | Reuse only the pure functions `from_nested → clean_order`. **Never go through `process_raw_event`** — its first-write-wins pre-check would see the very row being replaced and mark the whole batch `duplicate` |
| 4 | Active push | Corrected rows carry old `received_at` values, so the forward-only watermark will never see them. Pushing to the cloud is an **explicit runbook step** |
| 5 | Batch version axis | `DQ_RULE_VERSION` versions evaluation semantics only — a `format_clean` value bug can change values **without** changing `has_clean_error`, escaping the bump criterion entirely. Value production needs its own batch id, which doubles as the tie-breaker for `stg_` dedup — **not merely an audit column** |
| 6 | quality_events | Re-run `business_clean` on the corrected values and append an event carrying the batch id, or the Row Filter's effective-state composition stops reconciling |
| 7 | Late-arriving | Corrected values land in old partitions; a `received_at`-incremental `stg_` run will not see them. The runbook's final step must be a targeted refresh |
| 8 | Divergence window | Between the PG commit and the BQ refresh, PG holds new values while Gold still holds old ones — isomorphic to Proposal B's flow-back delay. Acceptable under T+1, but push + refresh must be **bound runbook steps, never left half-done** |

#### Additional cautions — migration shape

| # | Caution | Content |
|---|---|---|
| M1 | Atomicity | Retired copy, main-table delete+insert, and the events go in **one transaction** — splitting them leaves a "values swapped but state machine unrecorded" crack |
| M2 | `statement_timeout` | The global 30s timeout is designed for short online transactions and will kill a ten-thousand-row batch. The rebuild connection must override it |
| M3 | Concurrency safety | Under MVCC the intermediate state is invisible: concurrent duplicate pre-checks read the old rows; a TOCTOU INSERT blocks until commit then hits `IntegrityError`, identical to normal behaviour. **No extra handling needed — but it must be understood and documented** |

#### Additional cautions — patch shape

| # | Caution | Content |
|---|---|---|
| P1 | Overlay precedence | When the same `raw_id` is corrected twice, the overlay must implement "latest batch wins" itself — the migration shape gets this free from the main table |
| P2 | A second extraction path | The corrections table needs its own `FIELDS` declaration, extraction logic, and a consistency guard on par with `test_schema_bq_consistency` |
| P3 | Full re-extract runbook | Any staging rebuild must re-push corrections — write it into the rebuild steps explicitly, **or the wrong values resurrect** |
| P4 | Consumer contract | *"Reading ODS requires applying the overlay"* becomes a new implicit contract, needing documentation and guards against future direct reads |

#### The `raw_id` FK, and why it is not C's opposite

`ods.raw_id → raw.id` (`ON DELETE NO ACTION`, NOT NULL + UNIQUE = 1:1) is **the enforcement of a contract C already relies on**: C's core premise is "re-derive values from Raw", which already requires the parent row to exist. The FK turns *"we assume raw is there"* into *"the database guarantees raw is there"*.

| Shape | Impact |
|---|---|
| Migration | **FK-safe by construction** — the rebuilt row reuses the existing `raw_id` and raw is never deleted. Bonus: if the caution-3 manual INSERT path drops or fabricates `raw_id`, it escalates from a **silent orphan** to an immediate FK violation |
| Patch | The main-ODS FK does not touch the separate table. Recommendation: give `ods_corrections.raw_id` the same FK when it is eventually built |

| # | Runbook addition | Why |
|---|---|---|
| C-6.1 | Retired / archive tables **must not inherit the FK** (`LIKE ... EXCLUDING CONSTRAINTS`) | otherwise the retired copy pins raw rows, or archiving fails |
| C-6.2 | The dry-run gate asserts every blast-window row's `raw_id` still resolves in `raw` | the FK would block it anyway — catching it at dry-run avoids a half-done runbook |
| C-6.3 | FK lookup cost folds into M2's `statement_timeout` override | one PK index lookup per INSERT; negligible at batch scale |
| C-6.4 | The batch INSERT takes `FOR KEY SHARE` locks on raw — **no conflict** with the normal pipeline (`try_claim_raw` changes non-key columns, taking `FOR NO KEY UPDATE`, which is compatible); blast-window rows are all terminal, so real contention ≈ 0 | understand and document, in the spirit of M3 |

> **An adjacent precondition the FK formalises**: Raw must outlive its ODS row. If a Raw purge or TTL is ever introduced it must respect that ordering — the `NO ACTION` FK actively blocks deleting a raw row still referenced by ODS. Correct behaviour, but it changes purge semantics.

---

## 6. Consistency

**Bounded writeback**: any writeback from the warehouse targets **`quality_events` only**. ODS is never modified.

```
❌ warehouse → UPDATE an ODS column
✅ warehouse → INSERT into quality_events
```

Divergence between ODS and the warehouse is therefore expected and has exactly two explainable sources:

| Source | Explained by |
|---|---|
| rule-version evolution | `dq_rule_version` + `quality_events` — queryable, timestamped |
| a scenario model accepting irrelevant errors | reading the model's SQL and dbt description — static, no runtime tracking table |

[ADR-0032](../adr/0032-bounded-writeback.md)

---

## 7. Metrics

Two tiers with an explicit boundary — **high-cardinality slicing belongs in the warehouse, by definition**:

| | Tier 1 — operational | Tier 2 — analytical |
|---|---|---|
| Latency | minute-level | daily / weekly |
| Where | OTel metrics + structlog `quality_metric` | `rpt_quality_*` |
| Survives a warehouse outage | yes | no |

**Historical metrics are never retroactively rewritten.** If v2 promotes 15 records that v1 quarantined, the v1 quarantine rate stays what it was — promotions are counted as their own metric, on their own axis. A trend line that rewrites itself cannot support a conclusion. [ADR-0033](../adr/0033-historical-metrics-never-rewritten.md) · [ADR-0034](../adr/0034-tier-1-tier-2-metrics.md)

That is why quality reporting splits into two tables — see [transformation §5](./transformation.md).

---

## Appendix A: upstream anomalies (15-item map)

Every way an upstream can misbehave, and which mechanism absorbs it. **This is the concrete instantiation of the two-signal governance in §2** — each row resolves to one of the four quadrants.

| # | Anomaly | Signal / mechanism | Result |
|---|---|---|---|
| 1 | Unexpected new field | `has_schema_drift` (`UNEXPECTED_FIELD`) | lands; the new field is stored in `unmapped_fields`, existing columns unaffected |
| 2 | Missing expected field | ingress relaxed; detection deferred | lands as NULL; detected by null-rate monitoring, not at ingress |
| 3 | Renamed field | decomposes into rows 1 and 2 — new name = "unexpected", old name = "missing" | new name captured in `unmapped_fields`; old name lands NULL |
| 4 | **Changed type** | coercible → `TYPE_DRIFT`; hard error → 422. See [ADR-0054](../adr/0054-type-declaration-governance.md) | coercible lands + flagged; hard type error → 422 + `ingress_rejected` |
| 5 | Changed date format / timezone | format error → 422; timezone → a written contract | format error 422 + log; timezone is agreed, not detected |
| 6 | Unseen enum value | lands; length handled by the over-long path | new value lands; `accepted_values` (warn) catches it downstream |
| 7 | Semantic drift | — | **rules cannot catch this.** Deferred to distribution monitoring |
| 8 | No data at all | — | the OTel pipeline is live, but **absent alerting is unwritten** — see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) |
| 9 | Same `order_id` resent | existing idempotency | first-write-wins; duplicates marked `duplicate` ([ADR-0005](../adr/0005-first-write-wins-idempotency.md)) |
| 10 | Non-object nested group | `has_schema_drift` (`NON_OBJECT_GROUP`) + defensive guard | no crash; flagged, that group lands NULL |
| 11 | Sentinel / fake nulls | `format_clean` normalisation (strings); range check (numbers) | string sentinels → NULL; numeric sentinels flagged `has_clean_error` |
| 12 | Over-long string vs column cap | `has_clean_error` (`FIELD_TOO_LONG`) + a generous DB wall + fast-fail | moderately long → flagged and lands; egregious → terminal `error`, no poison pill |
| 13 | NUL byte | stripped before write + warning | stripped and landed; see [ADR-0006](../adr/0006-nul-byte-fast-fail.md) for the decoded-`\u0000` case |
| 14 | NaN / Infinity | `has_clean_error` (`NON_FINITE_NUMBER`) | flagged and lands; quarantined downstream — **does not poison aggregates** |
| 15 | Future date / clock skew | `has_clean_error` (`ORDER_DATE_IN_FUTURE`); extraction uses `>=` | future date flagged; clock rollback mitigated by `>=` in incremental extraction |

**Three rows worth reading together** — they are the ones no rule can catch:

- **#7 semantic drift** — the values are individually valid and collectively wrong. Only a distribution can see it.
- **#8 no data at all** — nothing arrives, so nothing is evaluated, so no signal fires. This is the same structural blind spot as [the silent scheduling stalls](../incidents/2026-08-silent-scheduling-stalls.md): **an absence produces no record.**
- **#2 missing field** — deliberately relaxed at ingress. Rejecting would trade a monitorable NULL for a lost order.

---

## 8. Related

- [ADR-0002](../adr/0002-has-clean-error-non-blocking.md) — the decision everything here rests on
- [ingestion](./ingestion.md) — where the signals are produced
- [transformation](./transformation.md) — where the filter runs
- [orchestration](./orchestration.md) — the Proposal B deployment SOP
