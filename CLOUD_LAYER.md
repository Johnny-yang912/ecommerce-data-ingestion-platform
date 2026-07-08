# Cloud Layer Architecture: ODS → BigQuery Extraction and Staging

## Scope and Responsibility Boundary

This document records the design decisions for the "cloud layer" — extraction (E/L) and landing (staging) once data leaves PostgreSQL (ODS) and enters BigQuery. The transformation layer's quality contract (dbt `stg_`→`int_`→`dim_/fct_`→`rpt_`) is covered in [DQ_ARCHITECTURE](./DQ_ARCHITECTURE.md).

```
ODS (PostgreSQL) ──[ E/L: Python ]──► BigQuery staging ──[ T: dbt ]──► stg_/int_/dim_/fct_/rpt_
```

**Why split E/L from T**: the two have different failure semantics. An E/L failure must resume from a watermark and be idempotent; a T failure only needs the SQL re-run. Mixing them entangles the two kinds of error.

---

## 1. Staging Table Design

### 1.1 Batch Load, Not Streaming

Staging is populated by the extraction script via a batch load job. BigQuery's streaming inserts are billed per volume, while batch load jobs are free; this project runs T+1 / hourly batches with no real-time requirement, so it always uses batch load. Streaming would only be motivated by a real-time prediction model downstream. Staging is therefore a physical table accumulated by batches (an append mirror of ODS).

### 1.2 Partitioning: `received_at` (DAY)

The rule for choosing a partition column is "pick the column that the table's most frequent, most expensive queries filter on." Staging's access pattern is **pipeline incremental** (both the extraction watermark and dbt incremental filter on `received_at`), so it partitions by `received_at`, letting each run scan only the new partition (partition pruning).

> **`received_at` vs `order_date`**: staging serves the pipeline, so it partitions by ingestion time `received_at`; downstream Gold (`dim_/fct_`) serves analysts whose monthly/weekly aggregates filter by the business time `order_date`, so that layer partitions by `order_date` instead. The partition column is chosen per table, according to its own access pattern.

DAY granularity over HOUR: batches are T+1 / hourly; and with a 4000-partition-per-table cap, DAY lasts ~11 years while HOUR lasts only 166 days.

### 1.3 Clustering: `order_id` + `has_clean_error`

Within each partition, data is sorted and co-located by the clustering columns, so filtering on them skips irrelevant blocks. `order_id` (downstream JOINs / dedup, high cardinality) comes first, `has_clean_error` (the Row Filter that every `int_` run applies) second.

### 1.4 Cost Fuse: `require_partition_filter=True`

Any query against staging without a `received_at` filter errors out immediately, blocking the "accidental full scan" cost surprise. Staging access always carries a `received_at` filter, so this has virtually no downside for it.

> **Knock-on effect**: the fuse blocks an unfiltered query like `SELECT MAX(received_at) FROM staging` — which directly shapes how the watermark is read (see §2).

### 1.5 Location Consistency: `US`

Each BigQuery dataset binds its location at creation time and cannot change it; a cross-location query errors out. All datasets (staging, dbt_dev, future dim/fct) are created in `US`, specified explicitly at dataset creation rather than relying on a default.

### 1.6 A Second Staging Table: `quality_events` (Deliberate Divergence from orders) ⭐

Besides `orders`, the extraction script also lands `quality_events` (the append-only quality-event log) to staging. **Why extract it**: when downstream `int_*` composes the "effective quality state," it JOINs the ODS snapshot with the latest `quality_events` event (a record promoted by Proposal B still reads `has_clean_error=TRUE` in ODS, so only the event lets it flow back to Gold) — without this table, the flow-back mechanism has no right-hand side (see [DQ_ARCHITECTURE §Mechanism 2: Row Filter](./DQ_ARCHITECTURE.md)).

Its table design **does not copy orders**, because the access pattern is the opposite. Every decision in §1.2–1.4 must be re-asked:

| Decision | orders | `quality_events` | Why different |
|---|---|---|---|
| Partition | `received_at` (DAY) | `event_at` (DAY) | Each table uses its own ingestion time axis; `event_at` also feeds the watermark (Approach A reads the latest partition) |
| Clustering | `order_id` + `has_clean_error` | `raw_id` + `to_state` | Downstream takes "the latest state per record" at **`raw_id`** grain (same key as dbt `stg_` dedup); `to_state` for state filtering |
| Cost fuse | ✅ on | ❌ **off** | **The key difference**: orders queries always carry a `received_at` filter, but `quality_events`'s main consumer is "latest per `raw_id` across all history" — inherently a non-partition-filtered full scan, which the fuse would block |

> **Cleaner flow-back than orders**: Proposal B's promotion events carry `event_at = now()`, landing in **today's** partition, so routine incremental `event_at >= watermark` picks them up naturally — unlike orders corrections that land back in **old** partitions and need an explicit runbook push (see §7.1). The append-only time semantics make `quality_events`'s E/L simpler.

> **Cross-table consistency: see §3.2** — the two tables extract independently, with independent watermarks and independent load jobs; how "orders landed but `quality_events` didn't" is prevented is covered there.

---

## 2. Watermark Strategy

### 2.1 Approach A: Derived from `INFORMATION_SCHEMA.PARTITIONS`

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `<project>.staging.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders' AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

Properties: **free** (metadata, scans no data), **exempt from the fuse** (it queries a metadata view, not the table body), and **stateless** (the watermark is derived from staging itself; after a load, the next read reflects new data, so there is no `advance_watermark()` step). The boundary uses `>=`, paired with dbt `stg_` dedup — re-pull rather than miss.

### 2.2 The `get_watermark()` Abstraction = the Only Seam for Switching to Approach B

Approach A's precision is bounded by the partition granularity (DAY → each run re-pulls the entire latest day). As batch frequency rises:

| Batch interval | Approach A | Remedy |
|---|---|---|
| Daily / T+1 | ✅ re-pull volume negligible | A |
| Hourly | ⚠️ re-pulls the day-so-far each run | switch to HOUR partitioning (bounded by the 4000 cap, needs expiration) |
| Sub-hourly micro-batch | ❌ re-pulls the same day hundreds of times | **Approach B** (separate watermark store, precise to the timestamp) |

Rule: **batch interval ≈ partition granularity → A; batch interval ≪ partition granularity → B.** Switching to B touches only `get_watermark()` plus an `advance_watermark()` after a successful load; `main()` is unchanged. B's cost is state management (where it lives, the load-then-advance ordering invariant, bootstrap, failure surface, concurrency); its monetary cost is still ≈ 0. A harder ceiling also exists: batch load jobs are capped at 1500 per table per day, which approaches a per-minute cadence.

---

## 3. Loading Strategy

### 3.1 Single-table landing semantics

- **WRITE_APPEND**: idempotency comes from append + dbt `stg_` dedup, with no MERGE at the E/L stage (preserving verbatim landing).
- **JSON columns pass native objects, not `json.dumps`** (verified empirically): psycopg2 parses JSONB into native list/dict, so passing it directly lets the client embed native JSON in the NDJSON, and BigQuery stores `JSON_TYPE=array/object`. Using `json.dumps` makes BigQuery store a JSON string scalar, breaking downstream `[0]` indexing.
- **`ALLOW_FIELD_ADDITION`**: supports additive evolution, see §5.

### 3.2 Cross-table consistency: per-table load job + gate (no cross-table transaction) ⭐

Multi-table extraction (orders + `quality_events`) raises a problem that doesn't exist on-prem: **a BigQuery load job is atomic only within a single table; there is no cross-table transaction** — you cannot wrap two tables' writes in one commit as you would on-prem Postgres. So we must guard against "orders landed but `quality_events` didn't," leaving dbt to build on a half-loaded pair.

Not by atomic landing, but by **two lines of defense**:

1. **Per-table watermark, not advanced on failure**: each table's watermark is derived from its own staging partitions (Approach A, §2). If one load fails, its watermark doesn't advance, and the next run re-extracts that batch via `event_at/received_at >= watermark` (append-only + `>=` + dbt `stg_` dedup). Whether orders succeeds has no bearing on `quality_events`'s watermark — that independence is the source of self-healing.
2. **The `main()` gate**: extract each table best-effort (one failure doesn't block the other from advancing), then reconcile — **any failure makes the whole run `raise` (non-zero exit)**, so downstream dbt (T) must not start. In the current manual phase this is "run dbt only if all succeeded"; in Phase 5 Airflow it becomes "the dbt task's upstream dependency = both extract tasks succeeded," the same gate semantics.

**The consistency model is eventual, not transactional**: skew (one table landed, the other not) causes only **delay** (a record due for flow-back is one dbt run late), never wrong data — provided downstream `int_*` composes its JOIN defensively (event absent → fall back to the ODS snapshot: clean flows, dirty stays quarantined). This is also the reason to keep "two independent load jobs" rather than forcing cross-table atomicity: independence is what makes each self-heal and retry.

---

## 4. Configuration and Security

- **`bq_project` is injected via `Settings`**, not hardcoded in the module: the project ID varies by deployment environment (dev/prod may be different projects), which falls under `config.py`'s established "environment config" boundary. It is **not a secret** (security rests on IAM, not obscurity), but as an infrastructure coordinate in a public repo, injection keeps the real ID out of version control.
- **Auth via ADC** (`bq.py`): locally it bridges a key-file path into the environment variable; in production the platform injects it, so the same code switches environments with zero changes (prod-parity).
- **Gotcha**: the BigQuery client needs the project **ID** (GCP often appends a numeric suffix, e.g. `-498602`), not the display name.

---

## 5. ODS Schema Evolution Strategy ⭐

### 5.1 Upstream Drift ≠ ODS Change

The ingestion layer **deliberately tolerates** upstream schema drift (see the two-signal governance in DQ): extra fields land in `unmapped_fields` + `has_schema_drift`; missing fields land NULL; type drift is flagged `TYPE_DRIFT`. **ODS's column structure does not change on its own** — drift is only a signal. ODS truly evolves only when an engineer **deliberately** adds/changes a column via an Alembic migration.

### 5.2 What BigQuery Can Migrate In Place

| ODS change | In place on BQ? | Cloud-layer handling |
|---|---|---|
| Add nullable column | ✅ `ALLOW_FIELD_ADDITION` | staging picks it up automatically |
| REQUIRED→NULLABLE | ✅ relaxation | staging relaxes |
| Drop column | ✅ DROP (but loses history) | **don't drop**, keep it, dbt `stg_` ignores |
| Rename | ✅ RENAME | **don't rename**, add a new column, dbt `stg_` renames |
| Incompatible type change | ❌ | add a new column + dbt cast |
| Change partition/clustering | ❌ | rebuild the table (CTAS) |

### 5.3 A Deliberate Discipline: Staging Is Additive-Only; Rename/Cast Goes to dbt

Even though BigQuery can DROP/RENAME, staging **deliberately** stays additive-only: ① preserve history; ② BigQuery DDL is unversioned, unlike Alembic — putting rename/cast into **dbt `stg_`** SQL gives it git versioning and review; ③ decouple physical evolution (rare, additive) from logical evolution (frequent, in SQL).

> **The asymmetry**: ODS has Alembic, a real migration framework; staging has **no equivalent** (dbt only takes over from `stg_` and does not own staging itself). In practice `ALLOW_FIELD_ADDITION` covers added columns and dbt absorbs the rest as the substitute. The one genuinely "must rebuild" case (changing partitioning) is cheap under Approach A: `drop + recreate + re-extract`, and the watermark resets itself.

### 5.4 Governance: One `FIELDS` per Table (the Third Schema Declaration)

Each staging table's `FIELDS` is the third hand-maintained declaration of that table's schema, after `schema.py` and `models.py`, and its worst drift mode is "silently dropping data from extraction." The extraction script folds each table's extraction contract into one `TableSpec` object — `table` / `model` / `time_col` / `fields` / partition / clustering / fuse — and there are two today: `ORDERS_SPEC` (mirroring `ODS`) and `QUALITY_EVENTS_SPEC` (mirroring `QualityEvent`, see §1.6).

Each `fields` drives three places (single source of truth): the BQ schema (`ensure_staging_table`), row serialization (`_to_bq_dict`), and the consistency test. `tests/test_schema_bq_consistency.py` parametrizes over `SPECS` per table, turning "changed `models.py` but forgot `fields`" into a failing test (column coverage, type, nullability). **Each new table only needs a spec added to `SPECS`; the consistency guard covers it automatically**, with no extra test to write.

### 5.5 End-to-End Examples: Add / Drop a Column (with the NULL follow-up) ⭐

§5.2's table is the static matrix of "which ODS change, can BQ do it in place"; this section is its **step-by-step walkthrough**, tracing the two most common changes from ODS all the way to dbt `stg_`, and then wiring in how to handle the NULLs each one produces.

Premise: "add/drop a column" here means an engineer **deliberately** changing ODS via Alembic (§5.1's deliberate evolution), **not upstream drift** (drift doesn't change ODS structure). `stg_orders` already sets `on_schema_change='append_new_columns'` (see [ecommerce_dbt/README §4.7](./ecommerce_dbt/README.md)).

The NULLs the two cases produce are **mirror images** on the time axis, so the handling philosophies are opposite:

| | Where the NULL grows | Meaning |
|---|---|---|
| Add | The past (historical partitions) | The column **simply didn't exist** in that history |
| Drop | The future (grows after collection stops) | The column is **no longer filled** from here on |

**The shared first step is always to decide which kind of NULL it is**, then choose accept / backfill / impute — get this wrong and you'll reach for the wrong tool.

#### 5.5.1 Add: the flow

| # | Checkpoint | Action |
|---|---|---|
| 1 | ODS | Alembic adds a **nullable** column (a NOT NULL add can't use `ALLOW_FIELD_ADDITION` — existing rows would violate it) |
| 2 | Consistency test | `test_no_ods_column_missing_from_fields` goes red — "ODS has it, `FIELDS` doesn't" is caught (else silent under-extraction) |
| 3 | `FIELDS` | Add the column (type/mode aligned, else the type/mode tests also go red); green = the three declarations realign |
| 4 | Extract + load | `ALLOW_FIELD_ADDITION` auto-adds the new column to the staging table; historical rows in old partitions are NULL, new rows have values |
| 5 | `stg_orders` (list not edited) | `source`'s `select *` pulls it in, but the **final explicit SELECT doesn't list it → dropped**; model output is unchanged, downstream can't see it — it just "rides along" in staging |
| 6 | `stg_orders` (surface via the list) | Add the column to the explicit SELECT (into git, reviewed) → the next **ordinary incremental run** suffices: dbt auto `ALTER ADD COLUMN` (metadata, free, old partitions NULL) + a copy job overwrites only the lookback-window partitions. **No `--full-refresh`, no full-table rewrite**; cost ∝ recent data |

#### 5.5.2 Add: handling the large historical NULL

First, a key fork: **is the column's history "nonexistent" or "under-extracted"?**

| Handling | Applies when | How | Why |
|---|---|---|---|
| A. Accept the NULL (default) | The value genuinely starts being collected now (a new program) | Don't fill; downstream slices by time or `WHERE col IS NOT NULL` | NULL honestly reflects "it didn't exist before"; force-filling = fabricating data. Cost 0 |
| B. Proposal C backfill | The value was always in Raw, ODS just never mapped it (under-extracted) | Re-produce from Raw with the new mapping → push corrected rows → targeted refresh of the affected partitions (see §7, DQ Proposal C) | A "missing value" class that A/B remediation can't touch — exactly Proposal C's domain. Heavy, but paid once |
| C. Downstream imputation | Analysis needs non-NULL (SUM/AVG shouldn't be diluted, a report must show 0) | `COALESCE(col, <default>)` in `int_/dim_`, record the semantics in the model description | `stg_` stays faithful (NULL); imputation is an analytics-layer business decision (DQ mechanism 3: SQL is the audit trail) |
| D. Default at ingestion | The value must always exist (e.g. `dq_rule_version`) | Set default/NOT NULL right in the ODS migration; historical rows are filled at migration time | Push the "NULL or not" decision to the cheapest, most upstream point; the cost is that a NOT NULL add must fill values in the migration, not via `ALLOW_FIELD_ADDITION` |

⚠️ **A blind spot of `append_new_columns`**: `ALTER ADD COLUMN` sets **all** old partitions to NULL, but an ordinary incremental only backfills the **lookback window**. If the column has existed in staging for a while (introduced ≪ the moment you add it to the `stg_` SELECT), the partitions in between — "staging has real values but they're outside the lookback window" — will **wrongly stay NULL** in `stg_`. The fix is a one-time targeted refresh of that range, temporarily widening `stg_orders_lookback_days`, or a single `--full-refresh` when the column first launches. So "no full-refresh" precisely means **no full-table rewrite on every future run**; a one-time backfill is still needed if there's a historical gap at first surfacing.

#### 5.5.3 Drop: the flow

| # | Checkpoint | Action |
|---|---|---|
| 1 | ODS | Alembic drops the column; `models.py` no longer has it |
| 2 | Consistency test | `test_no_stale_field_without_ods_column` goes red — the stale "`FIELDS` has it, ODS doesn't" is caught |
| 3 | `FIELDS` | Remove the column; green; `_to_bq_dict` no longer emits it |
| 4 | Extract + load | The staging physical column is **not dropped, kept** (§5.2); the load schema omits it → new rows NULL, historical rows keep their values |
| 5 | `stg_orders` | The explicit list still has the column → queries fine (staging still has it; new rows read NULL, old rows read values), **non-breaking**, becomes a legacy column |
| 6 | To remove it from the model | **Default: leave it as legacy, do nothing** — `append_new_columns` is add-only and **deliberately does not DROP** (aligning with "staging is additive-only; drops keep the legacy column" §5.2/§5.3). Only if you truly must remove it, `--full-refresh` rebuilds (rare, deliberate escape hatch; if downstream `int_/dim_` still references it, that run errors and is caught inside the DAG) |

#### 5.5.4 Drop: handling the large future NULL (the legacy column's NULL tail)

This column has real history but a NULL future that keeps growing; the question shifts from "how to fill" to "how to **not misuse** it".

| Handling | Applies when | How | Why |
|---|---|---|---|
| A. Freeze and keep (default) | Most cases | Let it sit: history queryable, future NULL; restrict to the historical range to use it | Aligns with §5.2/§5.3 "don't drop, keep for history"; BQ storage is dirt cheap, the NULL tail costs ≈ 0 |
| B. Mark the validity window, prevent misuse | A downstream will touch it | Model description / a note "stops being filled after X", or `int_/dim_` explicitly `WHERE order_date < cutoff` before referencing it | Prevents a future reader from `AVG`-ing a half-dead column and getting it diluted by the NULL tail (a consumer-contract issue, echoing DQ Proposal C-4 P4) |
| C. Actually remove it | Certain it's unneeded and history can be lost | Remove from the `stg_` explicit list + `--full-refresh` rebuild (`append_new_columns` won't DROP, so full-refresh is mandatory) | The only path that makes the column disappear. Rare, deliberate |
| D. Archive then remove | Want a clean mainline and retained audit | First snapshot the column (or history containing it) into an archive table, then remove it from the mainline | Balances "clean mainline" with "auditable history", analogous to the migration-form `ods_retired_<batch>`. Medium cost, one extra table |

#### 5.5.5 Which layer should NULL handling live in (`int_` vs `dim_/fct_`)

First split NULL handling into two kinds: **(a) consumer-invariant normalization** (objectively correct for all downstream, one answer) and **(b) consumer-specific analytical/presentation decisions** (NULL→0 for aggregation / keep NULL to count the miss-rate / NULL→'unknown' as a dimension bucket — the answer varies by question). The two structural NULLs above are almost always (b).

Core semantic principle: **NULL carries information ("didn't exist / stopped being collected"), and `COALESCE` is lossy and one-way** — once you collapse NULL→0 in `int_`, no downstream can tell "not collected" from "genuinely 0", and a `fct_` wanting a coverage rate can never compute it. Hence: **preserve NULL as long as possible; collapse only at the layer where the specific question makes the collapse correct**; filling a default is a business/presentation decision that belongs at the altitude of dim/fct/rpt, not the int_ plumbing (echoing "quality responsibility tightens downstream").

| Aspect | In `int_` (early, shared) | In `dim_/fct_` (late, close to the consumer) |
|---|---|---|
| Reversibility | Poor: NULL info dies here, downstream can't recover it | Good: local decision, small blast radius |
| Consistency | One answer for all downstream → only (a) benefits | Each takes what it needs → the natural home of (b) |
| Semantics | For (b), it "makes a decision for everyone that shouldn't be made for them" | Each question decides for itself |

An existing template in the docs: DQ mechanism 3's scenario imputation **does live in int_**, but via a **new column** (`customer_rating_cleaned`, not overwriting the original) + a **scenario-specific model** (not polluting the canonical `int_orders`) + **an audit trail in the description**. Copy that pattern.

**Recommendation**: for these two structural NULLs, **by default do not collapse them in `int_`** — carry them through int_ and handle them in `dim_/fct_/rpt_` per the question (aggregations already ignore NULL, so often no fill is needed; the drop-case NULL tail is best handled with `WHERE order_date < cutoff`). **Exception**: if a fill is proven (a) consumer-invariant and shared by many downstream, then move it into int_ — but **as a new column, never overwriting the canonical one** (the mechanism-3 pattern). **Iron rule: never `COALESCE` NULL away in place on a canonical `int_orders` column** — that makes a lossy, irreversible decision at the most-shared layer for the most consumers.

#### 5.5.6 Cross-cutting traps both cases hit

1. **Don't treat a structural NULL as a quality error.** DQ's `has_clean_error`/quarantine/Hard Gate are for "values with a business problem"; a NULL outside a column's existence window is not dirty data and does not go to quarantine. Hard Gate's `error_rate_below` looks at the `has_clean_error` ratio, so structural NULLs never feed it — **but** a `not_null` test on that column will blow up on the NULL tail. Design such tests around the validity window (assert not_null only over the valid range), or don't attach not_null at all.
2. **The null-rate monitor will false-alarm.** Phase 4's "missing fields via null-rate monitoring" will see the NULL spike from both cases. **Mark it as an expected structural NULL beforehand** (a migration/launch note, or a monitoring baseline exception), else you get a false alert every run.

#### 5.5.7 The decision rule

In one line: **first tell whether the NULL is "nonexistent / under-extracted / stopped-collecting"** — nonexistent → accept (5.5.2 A), under-extracted → Proposal C backfill (5.5.2 B), stopped-collecting → freeze-and-keep + prevent-misuse (5.5.4 A/B); and **push the fill decision to the DAG edge (dim/fct/rpt), never overwriting a canonical column** (5.5.5).

---

## 6. Live Verification Record (2026-06)

| Check | Result |
|---|---|
| partition/clustering/fuse/location | `received_at(DAY)` / `[order_id, has_clean_error]` / `True` / `US` |
| Fuse | a query without a `received_at` filter is blocked with 400 |
| JSON landing | `items` and `clean_error_message` both `JSON_TYPE=array`; downstream `JSON_VALUE(...[0],'$.code')` reads correctly |
| Additive load path | explicit schema + `ALLOW_FIELD_ADDITION` does not break the happy path |
| Consistency test | `test_schema_bq_consistency` all green |

---

## 7. Correction-Batch Flow-Back (Cloud Side of Proposal C)

[DQ_ARCHITECTURE](./DQ_ARCHITECTURE.md)'s Proposal C (batch repair of historical value defects — a directional design, not yet implemented) touches the cloud layer in four places:

### 7.1 The watermark never sees corrected rows — pushing is an explicit step

Corrected rows keep their original `received_at` (landing back in old partitions), while Approach A's watermark is `MAX(partition_id)` and only looks forward; the scheduled incremental extraction's `received_at >= latest partition` filter will never pick up new rows in old partitions. Pushing to staging must therefore be an explicit runbook step (select the corrected rows by batch id, call the existing `load_to_staging()` to append) — not the scheduled extraction. The watermark mechanism is neither involved nor modified.

### 7.2 Migration shape: reuse the append + dedup channel — no JOIN needed

Staging is append-only: after the corrected rows are appended, the same `raw_id` exists as two rows forever (wrong old + correct new), and the two rows share identical `received_at` / `raw_id` / `order_id` — "take the latest" has no natural sort key, so `stg_` dedup must tie-break on `rebuild_batch_id DESC NULLS LAST`. The batch id is thus a functional part of the flow-back mechanism, not merely an audit column. A bonus: if the blast window's right edge falls on today's partition, the scheduled extraction will pull the corrected rows a second time — harmless, since the duplicate rows are identical down to the batch id and dedup may keep either ("re-pull rather than miss" absorbs this by design). BQ load jobs are atomic per batch; no half-visible loads.

### 7.3 Patch shape: a second table, another hand-maintained declaration, and a re-extract landmine

If corrections land as a separate BQ table, it needs its own `FIELDS` declaration, extraction logic, and a consistency guard on par with `test_schema_bq_consistency` (§5.4's spirit applies equally). And any full staging rebuild (e.g. the repartitioning case in §5.3) re-extracts the main table's wrong values verbatim — the rebuild steps must explicitly include re-pushing corrections, or the wrong values resurrect.

### 7.4 Late-arriving: targeted refresh of the affected partitions

Corrected values land in old partitions; a `received_at`-incremental `stg_` run won't see them. The runbook's final step must be a targeted refresh of the affected partitions (insert_overwrite those partitions, or a one-off full-refresh of the single `stg_` model). A scheduled dbt run that races ahead of the push merely "hasn't taken effect yet" — it is not an inconsistent state.

---

## 8. Open Items and Future

- On micro-batch upgrade: swap `get_watermark()` to Approach B (+ `advance_watermark()`).
- Move into dbt layering (starting at `stg_`): started, see [ecommerce_dbt/README](./ecommerce_dbt/README.md).
