# ecommerce_dbt — Order Analytics Transformation Layer (dbt)

> Language: **English** ｜ [繁體中文](./README.zh-TW.md)

The **T (transformation) layer** after BQ staging: `stg_ → int_ → dim_/fct_ → rpt_`. This doc covers **only the dbt project's operations and implementation decisions**. The layer **quality contracts and semantics** (Hard Gate, Row Filter, quarantine, Proposal B/C, quality_events) live in [DQ_ARCHITECTURE](../DQ_ARCHITECTURE.md); the staging **infrastructure** (partitioning/clustering/partition-filter fuse/watermark, ODS→BQ E/L) lives in [CLOUD_LAYER](../CLOUD_LAYER.md).

## 1. Scope & Boundary

```
ODS (PostgreSQL) ──[E/L: Python]──► BQ staging ──[T: dbt (this project)]──► stg_/int_/dim_/fct_/rpt_
                    CLOUD_LAYER                     ← you are here →
```

This project **does T only**: it reads `staging.orders` (the 1:1 mirror already landed by E/L) and writes to `dbt_dev` (the dev target). It never touches extraction or ODS.

## 2. Quickstart

### Prerequisite: `~/.dbt/profiles.yml` (not version-controlled)

```yaml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/your/sa-key.json
      project: <your-gcp-project-id>   # real ID stays out of the repo (cf. CLOUD_LAYER §4)
      dataset: dbt_dev
      location: US                     # all datasets consistently in US
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1
```

### Common commands

```bash
dbt deps                              # install packages (dbt_utils)
dbt run    --select stg_orders        # build the model (incremental)
dbt run    --select stg_orders --full-refresh   # full rebuild (see §6)
dbt test   --select stg_orders        # run tests (incl. Hard Gate)
dbt source freshness                  # source freshness
dbt build  --select stg_orders        # run + test together
```

## 3. Layers & Naming Convention

| Layer | Prefix | Responsibility | Quality requirement |
|---|---|---|---|
| Silver entry | `stg_` | 1:1 to source, type alignment, name standardization, **dedup** | Keep all data incl. dirty; carries Hard Gate |
| Gold entry | `int_` | Cross-table joins, derived columns, **Row Filter interception** | Only clean data passes |
| Gold | `dim_`/`fct_` | Star Schema | No `has_clean_error=TRUE` |
| Reporting | `rpt_` | Fixed-grain pre-aggregation | Same as Gold |

- Naming uses `stg_orders` (consistent with existing project docs), not dbt's `stg_<source>__<entity>`.
- File layout: under `models/staging/` — `_staging__sources.yml` (source def), `stg_orders.sql`, `stg_orders.yml` (tests/descriptions).

## 4. Implementation Decisions (`stg_orders`)

### 4.1 Build a table, not a view

Materialization is a two-step decision: **first "materialized vs virtual", then "how to materialize".** This section covers the former (the latter is §4.2, incremental). `stg_orders` is a physical table for four reasons:

| Aspect | view (virtual) | table / incremental (physical, chosen) |
|---|---|---|
| Fuse propagation | A view is just stored SQL; querying it without a `received_at` filter **propagates** down to staging's `require_partition_filter` and 400s | A physical table **cuts** that chain: `stg_orders` has no fuse, downstream `int_`/Hard Gate tests query freely |
| Dedup cost | Every downstream query **recomputes** the dedup window function; N downstreams → the same dedup repeated N times per run | Dedup **computed once**, materialized; downstream reads the result |
| Consistent snapshot | Each downstream re-reads the append-only staging at query time; concurrent E/L loads can yield different states | All downstream stand on one snapshot frozen at run time |
| Incremental possible | Impossible — every query is a full recompute, no cost control | Only a physical, partitioned table supports partition-level `insert_overwrite` (see §4.2) |

1. **Cut fuse propagation** — the key point. staging's `require_partition_filter=True` is a deliberate cost fuse; if `stg_orders` were a view, that constraint would propagate to **every** downstream consumer, forcing each to remember a `received_at` filter. A physical table stops the fuse at the `stg_` layer, keeping downstream clean.
2. **Pay for dedup once** — dedup is `stg_`'s core work and multiple models read it. A view makes that computation repeat linearly with downstream count; materializing means "compute once, share."
3. **A consistent snapshot at the DAG root** — `stg_` is the root of the transformation DAG, read by many downstream models. Materializing as a table lets all downstream stand on one snapshot frozen at run time, immune to concurrent loads into the append-only staging, preserving within-run consistency and reproducibility. Note this is **consistency**, not **durability** — the data anchor is still ODS (see [DQ_ARCHITECTURE](../DQ_ARCHITECTURE.md)); `stg_` can be rebuilt from staging at any time.
4. **Prerequisite for incremental** — the "cost doesn't scale with history" property relies on partition-level incremental replacement, which **requires** a physical partitioned table; a view has no physical partitions to swap, so it structurally can't.

> Note: mainstream dbt convention actually materializes staging models as **views** (lightweight mirror, cheap storage). This project **deliberately deviates** to a table, justified by the local technical forces above — not by a generic "a mirror should be solid" principle.
>
> Trade-off: a physical table costs storage and has materialization latency (updated only after a run). Both are acceptable for `stg_` — BQ storage is very cheap, and downstream already consumes on the dbt-run batch cadence, so it doesn't need a view's "always reflects latest".

### 4.2 Materialization: `incremental` + `insert_overwrite`

Partitioned by `received_at(DAY)`; routine runs recompute only the recent partitions within a "lookback window", so cost is ∝ recent data, not total history. Correctness rests on the invariant: **all duplicate copies of a given `raw_id` land in the same `received_at` partition** (`>=` re-extraction and Proposal C corrections both keep the original `received_at`) → whole-partition replacement misses nothing.

### 4.3 `copy_partitions: true` ⭐ (working around the sandbox DML ban)

This BQ project is a **sandbox (no billing enabled)**, which **forbids DML**. `insert_overwrite` defaults to `MERGE` (DML) → routine incremental runs fail with `DML queries are not allowed in the free tier`. The fix is `copy_partitions: true` inside `partition_by`, which replaces whole partitions via **copy jobs (non-DML, free)**.

> ⚠️ **This constraint applies to every future incremental `int_/dim_/fct_` model too.** `--full-refresh` uses `CREATE OR REPLACE` (DDL) and is unaffected. Once billing is enabled you could drop this option and revert to MERGE, but copy jobs fit "whole-partition replace" better and are free, so there's no reason to.

**copy job vs MERGE**: both achieve "replace the affected partitions"; the difference is the layer at which replacement happens —

| | copy job (`copy_partitions=true`) | MERGE (default) |
|---|---|---|
| Operation level | Storage-level partition copy (`table$YYYYMMDD` + `WRITE_TRUNCATE`) | Query-engine DML (scan → delete → insert) |
| Is it DML | No | Yes |
| Billing | Free | Billed by bytes scanned |
| Sandbox | ✅ allowed | ❌ forbidden |
| Fits | Producing "the full contents of a partition", swap wholesale | Producing "a subset to upsert", merge row-by-row |

Our dedup produces "the full contents of a day", so whole-partition copy is semantically correct and cheaper.

### 4.4 Lookback window: `var('stg_orders_lookback_days', 3)`

Default 3 days; must be ≥ the E/L `>=` re-extraction range + safety margin. Adjust ad-hoc without editing the file:

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: 7}'
```

### 4.5 Dedup

`row_number() over (partition by raw_id order by received_at desc, id desc) = 1`.

- Partition key `raw_id` (not `id`/`order_id`): ODS `raw_id` is UNIQUE and 1:1, and a Proposal C migration-form correction row is "new `id`, same `raw_id`" — only partitioning by `raw_id` lets the correction compete with the old copy already in staging.
- Tiebreaker: current duplicates are byte-identical, so order doesn't matter. **Once Proposal C's `rebuild_batch_id` lands, prepend it** to the `order by`: `rebuild_batch_id desc nulls last, received_at desc, id desc`.

### 4.6 Contact points with the fuse (`require_partition_filter`)

1. **Full path** (full-refresh) needs the sentinel `where received_at >= timestamp('1970-01-01')` to avoid a 400.
2. **source freshness** underneath is `SELECT MAX(received_at)`, blocked by the fuse → bypass via the source's `freshness.filter` (a recent-window filter).
3. `stg_orders` itself **does not set** `require_partition_filter`, so downstream `int_` and Hard Gate tests query freely.

### 4.7 `on_schema_change: append_new_columns` ⭐ (add a column without a full rebuild)

**Problem**: `stg_orders`'s final projection is an explicit column list (§4.5, the "rename seam"), so adding a downstream column means editing the list. But under the default `on_schema_change='ignore'`, incremental runs won't add the new column to the existing table — the only way is `--full-refresh`, which on a large table is a full scan + full-partition rewrite, billed by bytes scanned. Paying a full-table cost to add one column isn't worth it.

**Fix**: use `append_new_columns`. On a column add, dbt first `ALTER TABLE ADD COLUMN` (metadata, free, existing rows auto-NULL), then overwrites only the lookback-window partitions — old partitions stay put at NULL. Cost is ∝ recent data. This mirrors staging's `ALLOW_FIELD_ADDITION` ([CLOUD_LAYER §5.2](../CLOUD_LAYER.md)): both layers symmetrically "add a nullable column, old data NULL, extend in place".

**Compatible with `insert_overwrite` + `copy_partitions`** (verified against dbt-bigquery 1.11 source): before running the copy job, the materialization builds a tmp table for `process_schema_changes`; on detecting the new column it ALTERs the target, then uses **the same** tmp table for the copy_partitions overwrite of the lookback-window partitions. copy_partitions already builds a tmp table every run, so the steady-state overhead of enabling this option is ≈ one metadata column comparison, negligible; no ALTER happens when the schema is unchanged.

**Why `append_new_columns` and not `sync_all_columns`**:

| | `append_new_columns` (chosen) | `sync_all_columns` |
|---|---|---|
| Add column | ✅ `ALTER ADD` | ✅ `ALTER ADD` |
| Drop column | Left alone, column kept | **`DROP` column** |
| Type change | Left alone | `ALTER` type |

`sync_all_columns`'s DROP conflicts with "staging is additive-only; drops keep the legacy column to preserve history" (§5.2/§5.3). `append_new_columns` is add-only by nature, which aligns exactly.

**The trigger gate is the explicit list, so it doesn't absorb drift**: `check_for_schema_changes` compares the model's produced columns (the explicit list), not the underlying staging. A column staging grew via `ALLOW_FIELD_ADDITION` is invisible until you add it to the explicit SELECT → no auto-ALTER. This option **fires only when you deliberately edit the list (into git, reviewed)**, leaving the explicit-list discipline intact.

**Boundaries** (what it can't solve — still go through the §6 runbook):
- **Historical backfill** (values needed in old partitions too, e.g. a Proposal C rebuild): it only NULL-fills old partitions and writes the lookback window → the true values in old partitions still need a targeted refresh.
- **Type change / rename / partition change**: still `--full-refresh` / table rebuild.

## 5. Testing Strategy

| Test | Target | Severity | Notes |
|---|---|---|---|
| `error_rate_below` (custom generic test) | Batch `has_clean_error` ratio | error @10% / warn @5% | **Hard Gate** (DQ mechanism 1). Can't use `dbt_utils.expression_is_true` (row-level, put in WHERE; aggregates error out) → custom `macros/error_rate_below.sql` uses `HAVING` for a whole-table aggregate |
| `unique` + `not_null` | `raw_id`/`id`/`order_id` | error | `unique(raw_id)` is the dedup check |
| `not_null` | `received_at`/`has_clean_error`/`has_schema_drift` | error | REQUIRED columns |
| source freshness | `staging.orders` | warn 26h / error 50h | with `filter` to bypass the fuse |

> Custom generic test arguments must be nested under `arguments:` (dbt 1.11 requirement, else `MissingArgumentsPropertyInGenericTestDeprecation`).

## 6. Operational Runbook

- **When to `--full-refresh`**: changing partition/cluster, changing dedup logic, recomputing history outside the lookback window, or first-time build. Uses DDL, unaffected by the sandbox.
- **Proposal C targeted refresh**: correction rows land in old partitions the lookback window can't see → the last step of the repair runbook does a targeted refresh of the affected partitions (`--full-refresh`, or a future single-partition `insert_overwrite`). See [CLOUD_LAYER §7.4](../CLOUD_LAYER.md), DQ C-2 #7.

## 7. Dependencies & Versions

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`: `dbt-labs/dbt_utils >=1.1.0,<2.0.0` (resolves to 1.4.1)

## 8. Status & TODO

- ✅ `stg_orders` (dedup + Hard Gate + freshness, incremental)
- ⬜ `stg_quality_events` (`quality_events` is now in E/L extraction to BQ; prerequisite cleared, this dbt model still pending)
- ⬜ `int_orders` + Row Filter, `int_orders_quarantine`, scenario-specific `int_orders_*`
- ⬜ `dim_/fct_`, `rpt_quality_*`
