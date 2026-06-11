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

- **WRITE_APPEND**: idempotency comes from append + dbt `stg_` dedup, with no MERGE at the E/L stage (preserving verbatim landing).
- **JSON columns pass native objects, not `json.dumps`** (verified empirically): psycopg2 parses JSONB into native list/dict, so passing it directly lets the client embed native JSON in the NDJSON, and BigQuery stores `JSON_TYPE=array/object`. Using `json.dumps` makes BigQuery store a JSON string scalar, breaking downstream `[0]` indexing.
- **`ALLOW_FIELD_ADDITION`**: supports additive evolution, see §5.

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

### 5.4 Governance: `FIELDS` Is the Third Schema Declaration

`extract_ods_to_bq.FIELDS` is the third hand-maintained declaration of the ODS schema, after `schema.py` and `models.py`, and its worst drift mode is "silently dropping data from extraction." `tests/test_schema_bq_consistency.py` turns its consistency with `models.py` (column coverage, type, nullability) into a failing test — extending the spirit of DQ mechanism 1 to the extraction layer. `FIELDS` drives the BQ schema, the serialization, and this test alike (single source of truth).

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
- Move into dbt layering (starting at `stg_`).
