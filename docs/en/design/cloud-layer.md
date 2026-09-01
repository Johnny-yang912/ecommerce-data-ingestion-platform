# Cloud Layer: ODS → BigQuery

**English** | [繁體中文](../../zh-TW/design/cloud-layer.md)

Extraction and staging. **E/L only — the T is dbt's** ([transformation](./transformation.md)).

---

## 1. Scope

`extract_ods_to_bq.py` moves ODS rows into BigQuery staging **as they are**: no cleaning, no renaming, no casting. staging is a 1:1 mirror of ODS, which is what makes "compare staging to ODS" a meaningful reconciliation.

Two tables are extracted, each with its own `TableSpec`, watermark and load job.

---

## 2. Staging table design

| Decision | `orders` | `quality_events` |
|---|---|---|
| Partition (DAY) | `received_at` | `event_at` |
| Clustering | `order_id`, `has_clean_error` | `raw_id`, `to_state` |
| `require_partition_filter` | ✅ on | ❌ **off** |
| Location | `US` | `US` |

**The fuse difference is the important one.** `orders` queries always carry a time filter, so the fuse costs nothing. `quality_events`'s main consumer needs *the latest event per `raw_id` across all history* — inherently an unfiltered full scan, which the fuse would block. Copying the `orders` spec would have broken the flow-back path, and broken it months after the table was created. [ADR-0022](../adr/0022-quality-events-staging-diverges.md)

Location is pinned to `US` explicitly on the dataset rather than relying on a default, so cross-location query errors cannot appear later.

### ⚠️ `received_at` means two different instants

| Column | Stamped when | Means |
|---|---|---|
| `raw.received_at` | API writes Raw, in the request path | **order-receipt time** |
| `ods.received_at` | worker writes ODS | **ODS landing time** |

staging mirrors ODS, so partitioning on ODS's own clock answers exactly "did extract move ODS forward?".

**The scope boundary that follows:** when a backlog is flushed by the recovery scan, those rows carry the *catch-up write* time — so the ingestion gap **does not exist on the ODS timeline at all**. Anything built on `ods.received_at` sees only outages still in progress at sampling time.

**The name reads like receipt time and is not being renamed** — that is a migration rippling into `FIELDS` and every dbt reference. [ADR-0020](../adr/0020-partition-on-received-at.md)

---

## 3. Watermark

Approach A — derived from the destination:

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = @table
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

Free (metadata, not table data), unaffected by the cost fuse, and **self-consistent by construction** — the watermark *is* the loaded data.

There is deliberately **no `advance_watermark()`**: nothing to update means nothing to fail to update.

The slice boundary is `>=`, not `>` — **prefer re-extracting to missing rows**. Duplicates are absorbed by `stg_`'s dedup on `raw_id`.

`get_watermark()` is the **only seam** for switching to Approach B. [ADR-0023](../adr/0023-watermark-approach-a.md)

### When Approach A stops working

Approach A's precision is bounded by the partition granularity — DAY, so each run re-pulls the entire latest day:

| Batch interval | Approach A | Remedy |
|---|---|---|
| Daily / T+1 | ✅ re-pull volume negligible | stay on A |
| Hourly | ⚠️ re-pulls the day-so-far every run | switch to HOUR partitioning (bounded by the 4000-partition cap, needs expiration) |
| Sub-hourly micro-batch | ❌ re-pulls the same day hundreds of times | **Approach B** — a separate watermark store, precise to the timestamp |

> **The rule: batch interval ≈ partition granularity → A; batch interval ≪ partition granularity → B.**

Switching to B touches only `get_watermark()` plus an `advance_watermark()` after a successful load; `main()` is unchanged. B's cost is **state management** — where it lives, the load-then-advance ordering invariant, bootstrap, failure surface, concurrency — while its monetary cost stays ≈ 0.

A harder ceiling also exists: **batch load jobs are capped at 1500 per table per day**, which is roughly a per-minute cadence.

---

## 4. Loading

Batch load jobs only (`WRITE_APPEND`), never streaming inserts. Nested `items` land as native JSON objects.

**Cross-table consistency without a cross-table transaction**, because BigQuery has none:

1. **Per-table self-healing** — a failed load does not advance that table's watermark, so the next run re-selects with `>=`.
2. **A gate before transformation** — any table failing fails the whole extraction. In Airflow the gate is the dependency edge: the dbt task's upstream is *both* extract tasks succeeding.

One Airflow task per table, because **retry granularity should match failure granularity** — a combined task would re-run the table that already succeeded and hide which one broke. [ADR-0024](../adr/0024-per-table-load-job-gate.md)

---

## 5. Configuration and security

| Value | Source | Why |
|---|---|---|
| `PROJECT` | `settings.bq_project` | varies by environment; the real id stays out of version control |
| `DATASET`, `LOCATION` | module constants | structural, stable architecture decisions |
| credentials | `GOOGLE_APPLICATION_CREDENTIALS` | host path, mounted; never baked into an image |

`BQ_PROJECT` / `BQ_DBT_DATASET` / `GOOGLE_APPLICATION_CREDENTIALS` are **shared with dbt's `profiles.yml`** — configured separately, the producer and consumer could silently point at different datasets. [ADR-0041](../adr/0041-profiles-yml-structure-vs-values.md)

---

## 6. Schema evolution

**Upstream drift ≠ an ODS change.** Unknown fields from upstream land in `ods.unmapped_fields` and are flagged by `has_schema_drift`; they do not become ODS columns until someone decides they should.

### What BigQuery can migrate in place

The static matrix. The step-by-step walkthrough is [runbooks/schema-change](../runbooks/schema-change.md):

| ODS change | In place on BQ? | Cloud-layer handling |
|---|---|---|
| Add nullable column | ✅ `ALLOW_FIELD_ADDITION` | staging picks it up automatically |
| REQUIRED → NULLABLE | ✅ relaxation | staging relaxes |
| Drop column | ✅ DROP — **but it loses history** | **don't drop.** Keep it; `stg_` ignores it |
| Rename | ✅ RENAME | **don't rename.** Add a new column; `stg_` does the renaming |
| Incompatible type change | ❌ | add a new column + a cast in `stg_` |
| Change partition / clustering | ❌ | rebuild the table (CTAS) |

**Rows 3 and 4 are the interesting ones**: BigQuery *can* do it, and staging deliberately does not. Three reasons — ① preserve history; ② **BigQuery DDL is unversioned**, unlike Alembic, so putting rename/cast in `stg_` SQL gives it git versioning and review; ③ it decouples physical evolution (rare, additive) from logical evolution (frequent, in SQL).

> **The asymmetry worth naming**: ODS has Alembic, a real migration framework. **Staging has no equivalent** — dbt only takes over from `stg_` and does not own staging itself. `ALLOW_FIELD_ADDITION` covers additions and dbt absorbs the rest as the substitute. The one genuinely "must rebuild" case (repartitioning) is cheap under Approach A: drop, recreate, re-extract — and the watermark resets itself.

When ODS *does* change, staging only ever adds:

| Change | Handled where |
|---|---|
| Add a nullable column | `ALLOW_FIELD_ADDITION` on the load job — appears automatically |
| Drop a column | stays in staging as `NULL`-filled legacy; `stg_` stops selecting it |
| Rename | `stg_`'s explicit column list — staging keeps the old name |
| Type change | `stg_`'s cast, or a table rebuild |

`ensure_staging_table()` only **creates**; it never alters.

**`stg_` uses an explicit column list rather than `SELECT *` precisely because that list is both the rename seam and the gate**: a column grown in staging is invisible downstream until someone deliberately adds it — in a commit, in a review. [ADR-0025](../adr/0025-staging-additive-only.md)

### Which layer should NULL handling live in

Split NULL handling into two kinds first:

- **(a) consumer-invariant normalisation** — objectively correct for all downstream, one answer.
- **(b) consumer-specific analytical decisions** — NULL→0 for aggregation, keep NULL to count a miss-rate, NULL→`'unknown'` as a dimension bucket. **The answer varies by question.**

Structural NULLs from an add or a drop are almost always **(b)**.

> **The core principle: NULL carries information — "did not exist" / "stopped being collected" — and `COALESCE` is lossy and one-way.** Collapse NULL→0 in `int_` and no downstream can ever distinguish "not collected" from "genuinely 0"; a `fct_` wanting a coverage rate can never compute it.

| Aspect | In `int_` (early, shared) | In `dim_`/`fct_` (late, close to the consumer) |
|---|---|---|
| Reversibility | **poor** — the NULL's information dies here | good — a local decision, small blast radius |
| Consistency | one answer for all downstream → only **(a)** benefits | each takes what it needs → the natural home of **(b)** |
| Semantics | for **(b)**, it makes a decision *for everyone* that should not be made for them | each question decides for itself |

**Default: do not collapse structural NULLs in `int_`.** Carry them through and handle them per question at `dim_`/`fct_`/`rpt_` — aggregations already ignore NULL, so often no fill is needed at all.

**Exception**: if a fill is proven consumer-invariant and shared by many downstream, move it into `int_` — but **as a new column, never overwriting the canonical one**. That is the scenario-model pattern: a new column, a scenario-specific model, and an audit trail in the description.

> **Iron rule: never `COALESCE` a NULL away in place on a canonical `int_orders` column.** That makes a lossy, irreversible decision at the most-shared layer, on behalf of the most consumers.

Two traps both cases hit:

1. **A structural NULL is not a quality error.** `has_clean_error` / quarantine / the Hard Gate are for *values with a business problem*. A NULL outside a column's existence window is not dirty data. But **a `not_null` test on that column will blow up on the NULL tail** — design such tests around the validity window, or do not attach them.
2. **The null-rate monitor will false-alarm.** Mark it as an expected structural NULL beforehand — a migration note or a monitoring baseline exception — or you get a false alert every run.

### Three schema declarations, all guarded

| # | Declaration | Guarded by |
|---|---|---|
| 1 | `models.py` | — (the source) |
| 2 | Alembic migration | `check_migration_drift.py` (manual) |
| 3 | `FIELDS` per table | `tests/test_schema_bq_consistency.py` (in CI) |

Without guard 3, adding an ODS column and forgetting `FIELDS` fails **silently** — the extract runs, the load succeeds, the column simply is not there. [ADR-0026](../adr/0026-fields-single-source.md)

---

## 7. Correction batches (Proposal C, cloud side)

Directional design; not built. Four things the cloud side would have to handle:

1. **The watermark never sees corrected rows** — a correction preserves the original `received_at`, so pushing it is an explicit runbook step, not an incremental pickup.
2. **Migration shape**: reuses the existing append + dedup channel — no JOIN needed, because `stg_`'s dedup partitions on `raw_id` and a correction is "new `id`, same `raw_id`".
3. **Patch shape**: a second table and another hand-maintained declaration, plus a re-extract landmine.
4. **Late-arriving**: targeted refresh of the affected partitions.

See [data-quality](./data-quality.md) for what Proposal C is and why it exists; points 1 and 4 above are steps 4–5 of [runbooks/proposal-c-correction](../runbooks/proposal-c-correction.md).

---

## 8. The sandbox

This project runs on a BigQuery sandbox (billing disabled), which imposes two constraints that shaped real decisions:

- **DML is forbidden** → dbt's `insert_overwrite` needs `copy_partitions: true` ([ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)).
- **60-day partition expiry on every table**, account-level and not overridable → Gold tables partitioned on `order_date` lose older rows silently.

A related measurement **overturned an earlier conclusion**: dates outside BigQuery's legal partition range do **not** fail the build — they land silently in `__UNPARTITIONED__`. The planned "legal range guard" was retracted as unnecessary; the failure mode was not the one assumed.

Details and what a real system does: [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

---

## 9. Related

- [ADR-0019](../adr/0019-batch-load-not-streaming.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md)
- [transformation](./transformation.md) — what happens to staging next
- [orchestration](./orchestration.md) — how extraction is scheduled
