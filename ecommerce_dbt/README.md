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
dbt run    --select stg_orders --full-refresh   # full rebuild (see §9)
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
| Reporting | `rpt_` | Fixed-grain pre-aggregation, consumed directly by BI | Business reports same as Gold; quality reports deliberately read quarantine |

- Naming uses `stg_orders` (consistent with existing project docs), not dbt's `stg_<source>__<entity>`.
- File layout: under `models/staging/` — `_staging__sources.yml` (source def), `stg_orders.sql`, `stg_orders.yml` (tests/descriptions); under `models/intermediate/` — the `int_*.sql` models plus a shared `_intermediate__models.yml`; under `models/marts/` — the `dim_*.sql`/`fct_*.sql` models plus `_marts__models.yml`; under `models/reports/` — the `rpt_*.sql` models plus `_reports__models.yml`.
- `reports/` sits **beside** `marts/`, not nested inside it: dbt's own convention would put `rpt_` under marts, but the table above already lists Reporting as its own layer, and the two quality reports draw from `int_`/`stg_` rather than marts — nesting them under marts would be semantically wrong.
- ⭐ `rpt_` spans **two data domains**: business reports always read `dim_`/`fct_`; quality reports read `int_orders_quarantine` and `stg_quality_events` — quarantined rows by definition **never reach Gold**, so that isn't bypassing the star schema, it's a different domain (see §7.1).

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

`sync_all_columns`'s DROP conflicts with "staging is additive-only; drops keep the legacy column to preserve history" ([CLOUD_LAYER §5.2/§5.3](../CLOUD_LAYER.md)). `append_new_columns` is add-only by nature, which aligns exactly.

**The trigger gate is the explicit list, so it doesn't absorb drift**: `check_for_schema_changes` compares the model's produced columns (the explicit list), not the underlying staging. A column staging grew via `ALLOW_FIELD_ADDITION` is invisible until you add it to the explicit SELECT → no auto-ALTER. This option **fires only when you deliberately edit the list (into git, reviewed)**, leaving the explicit-list discipline intact.

**Boundaries** (what it can't solve — still go through the §9 runbook):
- **Historical backfill** (values needed in old partitions too, e.g. a Proposal C rebuild): it only NULL-fills old partitions and writes the lookback window → the true values in old partitions still need a targeted refresh.
- **Type change / rename / partition change**: still `--full-refresh` / table rebuild.

## 5. Implementation Decisions (`int_` layer)

`int_` is the **Gold entry** — interception happens here ([DQ_ARCHITECTURE Q1, mechanism 2](../DQ_ARCHITECTURE.md)). DAG:

```
stg_orders ─────────┐
                    ├─► int_orders            (passes Row Filter) ──► int_order_items
stg_quality_events ─┘   int_orders_quarantine (intercepted)
```

### 5.1 Effective quality state: deliberate duplication, not a shared model

Both `int_orders` and `int_orders_quarantine` need the same logic — **synthesizing the "effective quality state"**:

> The criterion is **not** ODS's literal `has_clean_error` snapshot. ODS is an immutable anchor, so a record promoted by Proposal B is **forever** `has_clean_error=TRUE` in ODS; reading the snapshot alone would strand it in quarantine, never flowing back to Gold. The effective state must be synthesized on every run by `int_` from "ODS snapshot ⊕ latest `quality_events` event".

That logic (the `latest_event` → `resolved` → `classified` CTEs) is **deliberately written twice, once in each model file**, rather than extracted into a shared model. The decision:

| Option | How | Physical objects | JOIN executions | Complementarity guaranteed by |
|---|---|---|---|---|
| A | Shared ephemeral model emitting `is_effectively_clean`; downstream does `where flag` / `where not flag` | No increase (ephemeral builds no relation) | Once per downstream | **Mechanism** (one boolean + one negation) |
| B | Shared small table `int_quality_current` + a macro for the boolean | +1 small table | Once total | Mechanism (macro) |
| **C (chosen)** | Each model inlines the same CTE block | No increase | Once per downstream | **Discipline + a test** |

**Clearing up a common misconception**: A (ephemeral) and C compile to nearly identical SQL — an ephemeral model is inlined into every downstream, so it **neither creates extra tables nor saves JOIN executions**. Only B (materializing) actually reduces JOIN count. **So A vs C is purely a maintenance trade-off, not a cost one.**

The real reasons for choosing C:

1. **Only 2 consumers today**, so duplication costs less than the cognitive cost of one more `ref` indirection;
2. **Each model file is self-contained** — reading `int_orders.sql` shows the whole decision logic, matching the DQ doc's example verbatim, with no chasing across three files;
3. The price (complementarity drops from mechanism to discipline) **can be bought back with one test** — see §5.2.

### 5.2 Alignment checklist ⭐ (walk it on every change to either model)

Under option C, `int_orders` and `int_orders_quarantine` must remain a **complete partition** of `stg_orders` (mutually exclusive + exhaustive: every `raw_id` appears exactly once). The shared block is fenced with `═══` comments in both files and **must stay byte-identical**.

| # | Check | Consequence of getting it wrong |
|---|---|---|
| 1 | Both sides define `is_effectively_clean` **identically**, one uses `WHERE cond`, the other `WHERE NOT cond` | Non-complementary conditions → some rows in neither table (**silent data loss**) or in both (double counting downstream) |
| 2 | `coalesce(..., false)` **must not be dropped** | With `has_clean_error=TRUE` and no event, `FALSE OR NULL = NULL`; `WHERE NOT NULL` is also NULL → **the row vanishes from both tables at once** |
| 3 | Always `LEFT JOIN` | An accidental INNER drops every row without a quality event |
| 4 | The window's `partition by` / `order by` tiebreakers match (`partition by raw_id order by event_at desc, id desc`) | The two sides pick different events for the same row → the partition breaks |
| 5 | The `effective_quality_state` CASE branches match | Lineage labels disagree, `rpt_quality_*` miscounts |
| 6 | Both models use the **same materialization** (currently `table`, full rebuild) | One incremental, one full → the partition breaks between runs |
| 7 | `tests/assert_orders_split_is_partition.sql` stays `severity: error`, **never downgraded, never `--exclude`d** | It is the only automated safety net under option C |

> #2 is the easiest to miss: of `is_effectively_clean`'s three states (TRUE/FALSE/**NULL**), NULL makes a row disappear from **both** tables, silently. The partition test exists for exactly that.

### 5.3 Consolidation trigger, and options not yet enabled

- **Consolidation trigger**: when a **third copy** appears (e.g. enabling scenario-specific models), duplication starts to cost more than the indirection — at that point collapse the shared block into option A (ephemeral) or B (small table + macro).
- **Scenario-specific `int_orders_*` models ([DQ mechanism 3](../DQ_ARCHITECTURE.md)) — designed, deliberately not implemented yet.** Rationale: scenario-level imputation exists to answer a *specific* analytical question; without that question there is no correct answer to write, and building one anyway invents a fake requirement and pays permanent maintenance cost. Enable when a real scenario appears that can demonstrably tolerate a class of errors irrelevant to it. Enabling requires adding a `dq_has_only_error_codes(json_col, allowed_codes)` macro (asserting no code outside the allowed set exists) — **do not use `array_length(codes) = 1`**, since the same code can repeat (e.g. several items each raising `non_finite_number`), which would misjudge.

### 5.4 Materialization: `stg_` goes incremental, `int_` does full rebuilds ★

Two layers of the same project use opposite materialization strategies. That isn't an inconsistency — it's because **the shape of "what can change" differs** between them.

| | `stg_orders` | `int_orders` / `int_orders_quarantine` |
|---|---|---|
| Materialization | `incremental` + `insert_overwrite` + lookback window | `table` (full rebuild every run) |
| Sources of upstream change | One: rows added/re-extracted into staging | Two: new orders (`received_at` axis) **and** quality events (`event_at` axis) |
| Timeline of change | Aligned with the partition column, always recent | **Misaligned**: the event is today, the order it affects is long past |
| Can rows disappear? | No (append-only mirror) | **Yes** (a promoted row must leave quarantine) |
| Cost of missing a partition | Delay — the next run or a runbook fixes it | **Permanent error** — a ghost row stays in quarantine while also appearing in the other table |

The last row is the crux: a missed partition in `stg_` is a **delay**, in `int_` it's an **error** — one that raises no failure and never self-heals.

#### Why `int_` can't copy the lookback window

> A Proposal B promotion event has `event_at = now()` and lands in **today's** partition, while the order it rescues has a `received_at` in a **much older** partition. Under a `received_at` lookback window, that old partition would never be recomputed → the promoted record would **never flow back to Gold**, silently severing the reinstatement mechanism at this layer.

(Isomorphic to [CLOUD_LAYER §7.4](../CLOUD_LAYER.md)'s late-arriving problem but on a different axis: there the *value* changes (Proposal C corrections), here the *state* changes (Proposal B events).)

Hence the whole `int_` layer is `materialized='table'` (set as the folder default in `dbt_project.yml`). `table` uses `CREATE OR REPLACE` (DDL, atomic swap, unaffected by the sandbox DML ban — §4.3's constraint is currently moot for `int_`), and throws in two bonuses:

- **No logic drift**: the table always equals "the current SQL applied to the current upstream". An incremental table's old partitions can retain rows computed by an older version of the logic, fixable only via runbook (`stg_orders` carries exactly that burden — see §4.7, §9).
- **Changing the column list is free**: no `on_schema_change` to reason about, no judgment call on whether a `--full-refresh` is needed.

#### If it ever does go incremental, the hard part isn't the lookback window — it's these three

1. **Granularity mismatch**: the unit of change is a *row*, but `insert_overwrite` replaces a *partition*. Selecting only the affected rows would evaporate every other row in those partitions, so you must first expand "affected rows" into "affected partitions" and then reselect **all** rows in them — the reselected set is "lookback-window partitions **∪** partitions holding `raw_id`s with recent quality events". That needs an extra discovery query (the good news: BigQuery is columnar, so discovery only scans `received_at`/`raw_id` — a single-digit percentage of a full scan).
2. **It degrades to a full rebuild exactly when you need it most**: a typical Proposal B batch is "rules loosened → pull back old quarantine **across all history**", so the events' `raw_id`s are spread over every partition → affected partitions = all partitions, and that run costs *more* than a full rebuild (paying for discovery + a tmp table + N copy jobs on top). The very reason the incremental exists is the day it stops working.
3. **It forces the shared block to be consolidated too**: the two complementary tables must recompute the same set of partitions consistently, so that discovery logic would have to stay byte-identical across both files — the shared block grows from "three readable CTEs" into "three CTEs plus a fiddly dynamic-partition Jinja block". **The moment you go incremental is the moment §5.3's consolidation trigger fires**; the two changes must ship together.

#### When to switch: watch observable numbers, not order counts

Measured baseline: one run of the `int_` layer scans 910 KB for 554 orders → **≈1.64 KB per order per run** (all three models combined; the ratio is essentially row width, so it holds as data grows).

| Total orders | Scan per run | Monthly cost on a daily batch (on-demand, $6.25/TiB) |
|---|---|---|
| 10M | 16 GB | ~$3 |
| 100M | 164 GB | ~$30 |
| 1B | 1.6 TB | ~$300 |

The sandbox's 1 TiB/month free tier lasts to roughly **15–20M orders** on a daily batch (before subtracting `stg_`'s and future `dim_/fct_`'s usage).

But **cost is not the first bottleneck you hit**. Two things bite earlier: ① `profiles.yml`'s `job_execution_timeout_seconds: 300` — which makes runs *fail* rather than get expensive (the full rebuild currently takes 2.5s); ② the batch window for the whole DAG, once `int_`'s full rebuild stacks on top of equally full `dim_/fct_` rebuilds.

**Criterion**: track `bytes_billed` (monthly cumulative) and `execution_time` in `target/run_results.json`; start evaluating when either reaches **50%** of the quota / timeout. Until then, the full rebuild is the option where correctness is free and complexity is zero.

> **One deliberately accepted asymmetry**: `stg_`'s incremental saves recomputation and writes, but `int_` still scans all of `stg_` every run — so **the pipeline's read cost still scales with total history**. That's a knowing trade-off, bought in exchange for `int_`'s correctness holding unconditionally.

### 5.5 No partitioning; clustering on `order_id` only

- `int_` is consumed only inside the DAG (not by analysts doing ad-hoc queries), so partition pruning buys ≈ nothing; `order_date` partitioning belongs to `dim_/fct_` ([CLOUD_LAYER §1.2](../CLOUD_LAYER.md): "each table picks by its own access pattern").
- ⚠️ An earlier version recorded a second "landmine" here: `int_orders_quarantine` is where `ORDER_DATE_IN_FUTURE` rows land, and an absurd future date would fall outside BigQuery's legal partition range and fail the whole table build. **That reason was disproved by measurement in 2026-08** — values outside `1960-01-01 ~ 2159-12-31` do not fail the build; they land silently in the `__UNPARTITIONED__` partition (see [CLOUD_LAYER §1.7.3](../CLOUD_LAYER.md)). So `dim_/fct_` partitioning on `order_date` needs **no** legal-range guard (§6.2), and this layer's no-partition decision rests solely on the reason above.

### 5.6 `quarantined_at`: the event time, not `CURRENT_TIMESTAMP()`

An early DQ doc example wrote `CURRENT_TIMESTAMP() AS quarantined_at`. Under a `table` full rebuild that value **changes on every run** — it records "when this run happened", not "when this row was quarantined", distorting the `rpt_quality_*` timeline.

It is instead `coalesce(quality_state_at, received_at)`: prefer the `initial_evaluation` event's `event_at` (the real quarantine moment), falling back to ingestion time when the event is absent. If a run timestamp is ever needed, add a separate column with distinct semantics.

### 5.7 `int_order_items`: source, `safe_cast`, strict NULL propagation

Flattens `ODS.items` (a JSON array) to item grain via `unnest(json_query_array(items)) with offset`, for a future `fct_order_items`.

- **Sourced from `int_orders` (already filtered), not `stg_orders`**: item-level errors (`quantity_non_positive`, `unit_price_negative`, `discount_pct_out_of_range`, `non_finite_number`) already mark the **entire order** `has_clean_error=TRUE` at ingestion, so starting from `int_orders` naturally guarantees "no dirty data in Gold". For item-level RCA, build a separate model reading the quarantine table.
- **All numerics use `safe_cast`**: `clean.py` states explicitly that "values inside items are not coerced by Pydantic and may be strings" — items land as a whole JSONB blob, bypassing `ODSOrder`'s type coercion. A plain `cast` would let one dirty item **blow up the whole batch**; `safe_cast` yields NULL instead, matching the project's "mark, don't block" philosophy.
- ⭐ **All money is `NUMERIC`, never `FLOAT64`** (changed after measurement, 2026-08): `SUM()` over `FLOAT64` is **not associative** — the same values accumulated in a different order differ in the last bit, so `assert_fct_orders_rollup_matches_items`'s exact comparison is bound to fail at random (measured: 39 mismatched orders, max relative error **3.442e-16 ≈ 1 ULP**). `NUMERIC` is exact decimal (precision 38 / scale 9); sums are exact and order-independent, so the test can keep comparing exactly instead of retreating to a tolerance — and "what tolerance, and does it need retuning as data grows" is a question that comes back to haunt you. `safe_cast`'s fault tolerance holds under `NUMERIC` too (measured: uncastable → NULL, precision overflow → NULL, more than 9 decimal places → rounded, not an error). `quantity` stays `INT64`: it's a count, not money.
- **Derived amounts use strict NULL propagation, no `coalesce`**: `net_amount = quantity × unit_price × (1 - discount_pct/100)`, NULL if any input is NULL. The rationale is [CLOUD_LAYER §5.5.5](../CLOUD_LAYER.md)'s hard rule — NULL carries information ("no discount data" ≠ "discount is 0"), and `COALESCE` is lossy and one-way: collapse it to 0 at `int_` and no downstream can ever tell "not collected" from "genuinely zero" again. If "missing = no discount" is ever confirmed, add the imputation in `dim_/fct_`, **never retrofit this layer**.
- **`(raw_id, item_index)` is the item-grain key**: `items` is an immutable JSONB snapshot in ODS with fixed array order, so position is a stable identity; surrogate key `order_item_key = raw_id-item_index`.

## 6. Implementation Decisions (`dim_`/`fct_` layer — Star Schema)

Gold uses Kimball's **header/line dual fact tables**:

```
int_orders ──────┬──► dim_customer      (SCD1)
                 ├──► fct_orders        grain: order_id
int_order_items ─┼──► dim_product       (SCD1)
                 └──► fct_order_items   grain: (order_id, item_index)
```

> ⚠️ **Never join the two fact tables and aggregate measures from both** — the header's order total gets multiplied by the line count (double counting). For item detail query `fct_order_items`; for order totals query `fct_orders`.

### 6.1 Measure placement: roll up into the header + an invariant test ⭐

The biggest risk with dual fact tables isn't building them wrong, it's "the same number exists in two places and may disagree." Three options:

| | A: roll up into `fct_orders` | B: amounts live only on the line fact | **C (chosen)** |
|---|---|---|---|
| "Orders and revenue this month" | single-table query | must join + group by | single-table query |
| The two numbers disagreeing | possible (nothing guards it) | impossible (single source of truth) | **impossible (a test guards it)** |
| Extra cost | 0 | 0 | one singular test |

C it is: roll up into `fct_orders` and assert per-order equality via `assert_fct_orders_rollup_matches_items`. This is the **same move** as the `int_` layer's "deliberate duplication + `assert_orders_split_is_partition` to buy back the risk" (§5.1) — spend one test to upgrade a discipline guarantee into a mechanical one, and get single-table queryability in return.

In that test, `is distinct from` (not `=`) is mandatory: amounts propagate NULL strictly, and `NULL = NULL` yields NULL rather than TRUE, so `=` would let "both sides NULL" rows be silently filtered out by the `WHERE`.

> ⚠️ **This test doubles as a floating-point trap detector.** The day the first batch of genuinely multi-item orders landed (2026-08), it went red on 39 rows — `item_count` and `total_quantity` matched exactly; only the amounts differed, by 1 ULP. The cause is that `SUM()` over `FLOAT64` isn't associative, and the rollup and the test's re-aggregation took different execution plans.
> It stayed latent that long because until then every order inside the 60-day window had **exactly one item** — a single-value `SUM()` has no accumulation and therefore no ordering effect. The fix was switching money to `NUMERIC` (§5.7), **not** loosening the test to a tolerance comparison.

#### `SUM` silently swallows the NULLs you deliberately kept ⭐

The strict NULL propagation of `int_order_items` (§5.7) has a trap at the rollup: **BigQuery's `SUM()` ignores NULLs**. If a single item's `discount_pct` fails `safe_cast`, the order's `net_amount` is short by one item — **no error, no trace**.

The remedy deliberately is **not** `COALESCE` (that violates the hard rule in [CLOUD_LAYER §5.5.5](../CLOUD_LAYER.md), and is lossy and one-way). Instead it makes the incompleteness **explicit**: `fct_orders.items_missing_amount` records how many of that order's items have an uncomputable amount, letting consumers decide whether the sum is trustworthy. This is exactly what §5.5.5 means by "leave imputation to `dim_/fct_`, handled per question" — we don't decide for downstream that NULL should be 0; we give it the basis to decide.

Related: `item_count = 0` expresses "an order with no items" as a **value** rather than as an **absence**; `fct_orders` must `LEFT JOIN` the rollup — `INNER` would make that whole class of orders vanish from Gold.

### 6.2 Partitioning and retention

| Decision | `fct_*` | `dim_*` | Why |
|---|---|---|---|
| Partition | `order_date`(DAY) | **none** | Dimensions are reached **by key join**; a partition column prunes nothing there and only buys small-partition metadata overhead |
| Cluster | `customer_id` / `product_id, order_id` | dimension key | Matches the actual access pattern |
| Retention | 5 years (`var` gated) | — | DAY granularity is bounded by the 4000-partition cap (~11 years) → an explicit policy is mandatory |
| `require_partition_filter` | ❌ off | — | Gold serves analyst ad-hoc and BI exploratory queries; turning it on makes every unfiltered one a 400 |

Full reasoning — including the measured "clustering alone prunes 82%, partitioning adds 9pp" and the fuse-vs-custom-quota split — is in [CLOUD_LAYER §1.2.1](../CLOUD_LAYER.md).

**`partition_expiration_days` must be `var`-gated**: the BQ sandbox hard-locks it below 60 days, so hard-coding 1825 makes every `dbt run` fail ([§1.7.2](../CLOUD_LAYER.md)). Once billing is on:

```bash
dbt run --vars '{gold_partition_expiration_days: 1825, gold_projection_window_days: 1825}'
```

Materialization stays `table` (full rebuild) for the same reason as §5.4, only **stronger**: Gold's partition axis is `order_date` (business time), completely decoupled from "when the data changed" — a 2024 order promoted by Proposal B today can never be seen by any `order_date` lookback window.

### 6.3 Dimensions: build only two; SCD1 + the fact table carrying the at-the-time snapshot

**Which to build** (echoing §5.3, "no speculative models"): only `dim_customer` and `dim_product`. `dim_date` (no fiscal-year/holiday requirement yet; `order_date` itself can be `date_trunc`'d), `dim_geography` (no conformed geography master; extracting it just relocates columns and joins them back — and wide tables cost little in BigQuery's columnar storage) and a junk dimension (saving row storage isn't a problem in BigQuery) are all **degenerated onto the fact tables**.

**SCD strategy**: neither dimension has an independent master — attributes arrive with each order — so SCD1 with an explicit tiebreaker (without one, which of several same-day orders wins would drift with execution order).

SCD1's distortion — historical orders stamped with the *current* tier — is **bought back by the fact table**: `fct_orders.membership_tier_at_order` records the tier at order time. Customer attributes carried on an order are already a point-in-time snapshot, so letting the fact table carry them gives **the type-2 effect with zero infrastructure**:

- "Total spend of customers who are *currently* platinum" → join `dim_customer.membership_tier`
- "Orders that were placed *while* platinum" → read `fct_orders.membership_tier_at_order`

**SCD2 is designed but deliberately not enabled; the trigger is enabling billing.** Not because it's tedious, but because on the sandbox it **breaks**: a dbt snapshot is a stateful table, and once the 60-day table expiration eats it, it is gone for good — categorically unlike `fct_` full rebuilds, which self-heal. (Same discipline as §5.3: write the design down, implement when the trigger fires.)

### 6.4 `dim_product`'s attribute conflicts: flag, don't block

The same `product_id` can arrive with different `product_name`/`category`/`brand` on different orders. Measured 2026-08: **163 of 342** `product_id`s conflict, root cause being that `load_test.py` drew `product_id` and its attributes from two independent random draws (fixed — see `make_product()` in that file).

Handled in three layers:

1. The model uses an explicit tiebreaker to guarantee **determinism** — a conflict never breaks the grain, it just picks the latest
2. `fct_order_items.product_name_at_order` preserves the line-level truth for comparison against the dimension
3. `assert_product_attributes_stable` (**severity: warn**) tracks the conflict count

Warn rather than error, because this is an **upstream contract signal**, not a correctness defect in this layer — if `product_id` genuinely cannot determine product attributes, the fix belongs upstream or in the data contract, not in stopping the whole DAG. Same judgement as `has_schema_drift` in the DQ doc: drift has no interception authority, only alerting. Contrast: `assert_fct_orders_*` are errors, because those test **whether our own SQL is right**.

### 6.5 Key handling

**Dimension keys use natural keys directly** (`customer_id`/`product_id`), no hash surrogate key: BigQuery has no indexes, so a surrogate key brings no join-performance benefit — one less key-management layer, and analysts can read it. The switch trigger is precise: **the day §6.3 becomes SCD2**, `customer_id` stops being unique and a surrogate key goes from optional to mandatory.

**Fact tables carry no NULL FKs**: `customer_id`/`product_id` are both nullable in ODS, and a NULL FK makes INNER JOIN drop rows silently and LEFT JOIN render blanks in BI. So each dimension gets an unknown member (`'__UNKNOWN__'`) and the facts `coalesce` onto it.

This does **not** violate §5.5.5's NULL hard rule: that rule forbids lossy collapse of **measures** in a shared layer (after `NULL→0` you can no longer tell "not collected" from "genuinely 0"); here we act on a **key**, and `'__UNKNOWN__'` reverses cleanly back to "this row has no identifier" — it is **lossless**.

**`fct_order_items`' grain is `(order_id, item_index)`**, not the upstream `raw_id`: Gold faces analysts, and per the README, `raw_id` is physical identity while `order_id` is business identity. Both are UNIQUE and 1:1 in ODS, so switching loses no uniqueness; `raw_id` is retained as a lineage column but is **not** a key. The upstream surrogate key was renamed `int_order_item_key` and stops at the `int_` layer, avoiding same-name-different-value. The zero-padding in `format('%s-%03d', order_id, item_index)` makes lexical order equal numeric order (otherwise `A-10` sorts before `A-2`).

`fct_order_items` also carries `customer_id`/`order_date` **down from the header** so the line fact is queryable standalone — "unit sales of a product among platinum members" shouldn't be forced to scan two tables. Carrying a few low-cardinality columns costs nearly nothing in columnar storage.

### 6.6 Business rules deliberately left undefined

The following three are undefined in the docs and are **deliberately not assumed** (echoing §5.3) — a fabricated assumption makes a wrong number look like a fact:

| Item | What's undefined | Current handling |
|---|---|---|
| `tax_amount` | Is the tax base `net` or `net + shipping`? | Only `tax_pct` is exposed (**a ratio — non-additive, never SUM it**); no derived amount |
| Net revenue | Should `returned = TRUE` orders be subtracted? | `returned` stays on the fact as a flag; downstream decides |
| `profit_amount` | Does margin include shipping and tax? | Not built; downstream can compute `net_amount - cost_amount` |

## 7. Implementation Decisions (`rpt_` layer — Reporting)

Three tables, one per chart on the BI page:

```
int_orders_quarantine ──► rpt_quality_backlog          snapshot axis: what's still stuck
stg_quality_events ─────► rpt_quality_events_daily     event axis: what happened
fct_order_items ─┬──────► rpt_sales_daily_by_category  business aggregate
dim_product ─────┤
fct_orders ──────┘ (returned flag only)
```

### 7.1 Business reports always read Gold, never `int_` directly

Wiring `rpt_` straight onto `int_` as a canned query is an anti-pattern in mature teams. Four reasons; the last two are specific to this project:

| # | Reason | Consequence of bypassing `fct_` |
|---|---|---|
| 1 | One definition per metric | "Revenue" gets two lineages → two numbers, and nobody knows which is wrong |
| 2 | Don't redo the semantic decisions Gold already made | Unknown members, the `item_count=0` LEFT JOIN — all of it would have to be copied again |
| 3 | It invalidates existing tests | `assert_fct_orders_rollup_matches_items` guards `fct_orders`'s rollup; if `rpt_` recomputes from `int_`, that test **doesn't cover it at all** |
| 4 | ⭐ It overturns an architectural premise of `int_` | §5.5 "`int_` is consumed only inside the DAG" is the **only** reason `int_` isn't partitioned. `rpt_` reading `int_` promotes it from internal building material to a public contract → the partitioning decision needs revisiting, and `int_` becomes un-refactorable |

**The legitimate exception**: quality reports. Quarantined rows by definition never reach Gold, so `rpt_quality_*` must draw from `int_orders_quarantine` and `stg_quality_events`.

> ⚠️ This brings an easy mistake with it: **the denominator for quality rates is all of `stg_orders` (dirty included), not `fct_orders`**. Use Gold as the denominator and quarantine_rate is identically zero — that's exactly what the Row Filter does.

> A note on honesty: the textbook justification for `rpt_` is "pre-aggregate to buy performance and cost," and at this project's current volume that justification is worth nothing. The docs deliberately don't claim "for performance," because that would be false — the real reasons are **one fixed definition per metric** and **BI not having to assemble joins itself**. Let report authors aggregate `fct_` freely inside the BI tool and metric definitions drift into the BI tool; at this scale that's what `rpt_` actually prevents.

### 7.2 Three disciplines that span the layer

**① Never materialize a ratio — only the additive numerator and denominator** ⭐

Storing a rate in a pre-aggregate is the number-one trap: the moment BI rolls daily grain up to weekly, Looker Studio computes `AVG(daily_rate)` — **the average of ratios, not the ratio of sums**. Those agree only when every day's denominator is equal, and denominators are never equal. Leave the rate to a BI calculated field (which computes `SUM(num)/SUM(den)` and is correct at any grain).

> Why not follow `fct_orders.tax_pct` and "keep it but mark it non-additive": `tax_pct` is a **raw fact** — don't store it and it's gone. `quarantine_rate` is purely derived; keeping a column that's only correct at daily grain actively manufactures an opportunity for misuse.

**② `COUNT(DISTINCT)` is structurally non-additive in a pre-aggregate**

Summing `orders` across categories double-counts (one order's items span several categories); summing `customers` across days double-counts too. No naming convention fixes this — it can only be flagged. **Trigger point**: when a chart genuinely needs distinct counts rolled up across dimensions, switch to BigQuery's native HLL sketches (`HLL_COUNT.INIT` → BYTES, `HLL_COUNT.MERGE` upstream, ~1% error). The reason for not doing it now isn't effort — it's that Looker Studio's calculated fields **cannot call** `HLL_COUNT.MERGE`, so it needs another view on top, and that friction point doesn't exist yet (same discipline as §5.3).

**③ `rpt_` only does `GROUP BY` / window functions**

No new join semantics, no new cleaning, no new business definitions. If some `rpt_` needs a join that `dim_`/`fct_` can't provide, that's a **signal the star schema is missing something** — fix Gold, don't improvise here. (Same sense of direction as §5.7's "imputation belongs in `dim_/fct_`, don't go back and change `int_`.")

### 7.3 Quality reporting splits into two tables: two time axes, two kinds of mutability ⭐

The `rpt_quality_daily` originally sketched in the DQ doc conflated two things of opposite nature, so the implementation split them:

| | `rpt_quality_events_daily` | `rpt_quality_backlog` |
|---|---|---|
| Axis | **Event axis** (`event_at`) | **Snapshot** (current contents of quarantine) |
| What a row means | "N quality events happened that day" | "N orders are stuck right now" |
| Retroactively rewritten? | **No** (append-only) | Yes (it *is* the current state) |
| Incremental possible? | ✅ axis aligned with the source of change | ❌ inherently not |

**Why backlog can't just be accumulated off the event axis**: in theory `backlog(t) = cumulative quarantined − promoted − rejected`, since the event stream is the complete derivative of the state. But `quality_events` has 60-day partition expiration — **once it expires the starting point of that accumulation is gone, and the distortion is one-directional** (the start can only under-count quarantined → backlog is systematically understated). The snapshot table reads `int_orders_quarantine` directly and is immune to the event retention window.

**Why the event table isn't hung on the ingestion axis**: grouping by `received_at` means promoting a three-month-old order today **rewrites the composition of a row three months ago** — that's state, not an event, and it makes "how much did v1 intercept" drift over time, in direct conflict with [DQ_ARCHITECTURE](../DQ_ARCHITECTURE.md) "Why historical metrics are never retroactively rewritten."

### 7.4 Materialization: the one downstream model where incremental is inherently correct

| | Materialization | Partition | Reason |
|---|---|---|---|
| `rpt_quality_events_daily` | `incremental` + `insert_overwrite` + `copy_partitions` | `event_date`(DAY) | Event axis is append-only and the time axis aligns with what changes → a lookback window suffices; **none** of §5.4's affected-partition discovery is needed |
| `rpt_quality_backlog` | `table` | **none** | A state snapshot: one promotion and a row must leave the table → an incremental miss is a **permanent error**, not a delay. Partitioning only pays off for partition-level incremental replacement, which this layer will never do |
| `rpt_sales_daily_by_category` | `table` | `order_date`(DAY) | ⭐ Full rebuild for now, but **adding the partition column now is free; adding it later means rebuilding the table** |

> ⚠️ `rpt_quality_events_daily`'s `var('rpt_quality_events_lookback_days')` **must be ≥ `stg_quality_events_lookback_days`**. If the upstream window is wider than the downstream one, old events backfilled upstream today land outside the downstream window → they're never picked up, **and nothing errors** (the partition exists, it's just short on content). Change both vars together.

**Time zone**: `event_date = date(event_at)` is **UTC**, deliberately not `date(event_at, 'Asia/Taipei')` — a time-zone conversion breaks predicate pushdown for partition pruning. Landing UTC in the warehouse and leaving time-zone presentation to BI is the standard division of labor, but it does mean "that day" ends at UTC midnight, 8 hours off Taipei time — hence the column description says so as well.

**The path if `rpt_sales` ever goes incremental**: "daily incremental with an `order_date` lookback window **plus** a scheduled weekly `--full-refresh`" — **not** hand-written affected-partition discovery. The latter forces a purely business report to depend on `quality_events` as a change detector (coupling for a non-semantic reason), and on the day Proposal B promotes at scale it degrades to worse than a full rebuild (§5.4 point 2). The cost is a single documentable sentence: **retroactive corrections become visible in this report within ≤ 7 days**.

### 7.5 Handling fan-out: an additive measure for reconciliation

`rpt_quality_backlog` explodes to `error_code` (one order can carry several) → summing across codes double-counts orders. The fix puts **two measures with different meanings** in the same table:

- `orders_with_code`: orders carrying this code (**non-additive**, for the Top-N chart)
- `orders_primary_code`: orders whose *primary* code is this one (**additive**, for the "how many are stuck" KPI)

Primary code = the first entry of the deduplicated, sorted `error_codes` array. It exists **only** for determinism and reconciliation and does **not** express a severity ranking — severity priority is a business definition, none exists, and we don't invent one (same as §6.6).

> Rejected alternative: adding an `error_code = '__TOTAL__'` grand-total row. It turns "someone accidentally sums `__TOTAL__` too" into a new misuse surface, which is worse than the fan-out itself.

**Deduplicating the array is not optional**: the same code can repeat because several items each triggered it (same issue as §5.3's "don't test with `array_length(codes) = 1`"); without dedup, `orders_with_code` counts one order as several.

### 7.6 Deliberately not built

| Item | Why not | Trigger point |
|---|---|---|
| **Monetary exposure** (what the stuck orders are worth) | `int_order_items` sources from `int_orders` (the clean path); quarantine's items have **never been exploded** (§5.7 already flagged this) | Requires `int_order_items_quarantine`; enable when quality reporting needs business exposure figures |
| **HLL sketches** | Looker Studio calculated fields can't call `HLL_COUNT.MERGE` | A chart that needs distinct counts rolled up across dimensions |
| **Cell-by-cell amount reconciliation tests** | Under `table` full rebuilds it's a **tautology** (validating the same SQL against itself — always green, zero information) | ⭐ see §8 |
| `order_status` in the grain | The value domain is undefined; unknown whether it contains a "not placed" state | Confirm the domain, then decide whether to filter |

## 8. Testing Strategy

| Test | Target | Severity | Notes |
|---|---|---|---|
| `error_rate_below` (custom generic test) | `stg_orders` batch `has_clean_error` ratio | error @10% / warn @5% | **Hard Gate** (DQ mechanism 1). Can't use `dbt_utils.expression_is_true` (row-level, put in WHERE; aggregates error out) → custom `macros/error_rate_below.sql` uses `HAVING` for a whole-table aggregate |
| `unique` + `not_null` | `stg_`'s `raw_id`/`id`/`order_id`; `int_`'s `raw_id`/`order_id` | error | `stg_`'s `unique(raw_id)` is the dedup check |
| `not_null` | `received_at`/`has_clean_error`/`has_schema_drift` | error | REQUIRED columns |
| source freshness | `staging.orders`, `staging.quality_events` | warn 26h / error 50h | with `filter` to bypass the fuse |
| **`assert_orders_split_is_partition`** (singular) ⭐ | `int_orders` ∪ `int_orders_quarantine` vs `stg_orders` | error | **Partition invariant**: every `raw_id` appears exactly once. The only automated safety net under option C (§5.1 duplication), guarding checklist items #1–#4. **Never downgrade** |
| `assert_int_orders_no_unpromoted_dirty` (singular) | `int_orders` | error | **Gold contract**: no row with `has_clean_error=TRUE` that hasn't been promoted. Written as a singular test rather than a column test because it's a **conditional relation between two columns** — `has_clean_error=TRUE` is legal here (promoted records stay dirty in ODS) |
| `accepted_values` | `effective_quality_state` on `int_orders`/`int_orders_quarantine` | error | The two tables' state domains are disjoint (`clean`/`promoted` vs `quarantined`/`permanently_rejected`), cross-checking the partition from another angle |
| `dbt_utils.unique_combination_of_columns` + `relationships` | `int_order_items`'s `(raw_id, item_index)`, `raw_id → int_orders` | error | Item-grain uniqueness and lineage integrity |
| **`assert_fct_orders_rollup_matches_items`** (singular) ⭐ | `fct_orders` rollup measures vs `fct_order_items` aggregates | error | **Rollup consistency invariant** (the core of §6.1 option C). Compared per order with `is distinct from` — `=` would let "both sides NULL" rows be silently filtered out by the `WHERE`. Both tables share partition settings, so no time window is needed |
| **`assert_fct_orders_complete_projection`** (singular) ⭐ | `int_orders` (in-window) vs `fct_orders` | error | **Lossless-projection contract**: interception already happened in `int_`, so Gold must not drop a single row. Written as an anti-join over an `order_date` window rather than `count = count` — the two tables' 60-day clocks hang on different axes ([CLOUD §1.7.5](../CLOUD_LAYER.md)), so a count comparison would go flaky daily |
| `assert_product_attributes_stable` (singular) | `product_id` → attributes on `int_order_items` | **warn** | An upstream contract signal, not a defect in this layer — if `product_id` can't determine attributes, fix upstream rather than stopping the DAG (§6.4) |
| `unique` + `not_null` | `dim_customer`/`dim_product` dimension keys; `fct_orders.order_id`; `fct_order_items.order_item_key` | error | Dimension grain and fact surrogate-key uniqueness |
| `relationships` | `customer_id`/`product_id` on both `fct_` tables → `dim_*`; `fct_order_items.order_id` → `fct_orders` | error | Star-schema FK integrity. Paired with `not_null` (the unknown member guarantees FKs are never NULL, §6.5) |
| `dbt_utils.unique_combination_of_columns` | `fct_order_items`'s `(order_id, item_index)` | error | The declared grain |
| **`assert_rpt_sales_no_item_loss`** (singular) ⭐ | `rpt_sales`'s `sum(items)` vs `fct_order_items` row counts (in-window, per day) | error | `rpt_sales` introduces the only **new joins** in the whole DAG (× `dim_product`, × `fct_orders`). A join quietly turning INNER shows up as "revenue slowly shrinking" and raises nothing. The full outer join is what catches "too many" as well (dimension fan-out) |
| **`assert_rpt_quality_events_split`** (singular) ⭐ | `initial_clean + initial_quarantined = initial_evaluations` | error | **Domain-expansion alarm for the wide table**: the price of a wide table is "one more `to_state` upstream and the downstream needs a schema change to see it." A new state makes `count(*)` grow while the `countif`s don't → this goes red immediately instead of letting those events evaporate silently. It's what makes the wide table safe to use |
| `assert_rpt_backlog_primary_code_balances` (singular) | `sum(orders_primary_code)` vs actual order counts in `int_orders_quarantine` | error | The safety net for §7.5's reconciliation measure. When it breaks, the symptom is the backlog KPI in BI simply being wrong, with no self-healing |
| `dbt_utils.unique_combination_of_columns` + `not_null` | The declared grain of each of the three `rpt_` tables | error | A broken grain in a pre-aggregate doubles every number, silently |
| `dbt_utils.expression_is_true` | `orders <= items`, `items_missing_amount <= items`, `orders_with_code >= orders_primary_code` | error | Cheap sanity floors |

> Custom generic tests (and some built-in ones) need their arguments nested under `arguments:` (dbt 1.11 requirement, else `MissingArgumentsPropertyInGenericTestDeprecation`).

> ⚠️ **Deliberately not written**: `assert_rpt_sales_matches_fct`-style **cell-by-cell amount reconciliation**. Under `table` full rebuilds it's a tautology (`rpt_`'s sum *is* `fct_`'s columns added up) — always green, zero information. Its value only materializes the day the model goes incremental (catching missed partitions).
>
> **→ "make `rpt_sales` incremental" and "add cell-by-cell reconciliation" are two halves of one change; doing only the first is not allowed.** Structurally identical to §5.4's "the moment you go incremental is the moment the consolidation trigger fires."
>
> Contrast: `assert_rpt_sales_no_item_loss` *is* written now, because it tests **row counts across two joins** — independent of materialization strategy, and a genuinely possible failure.

## 9. Operational Runbook

- **When to `--full-refresh`**: changing partition/cluster, changing dedup logic, recomputing history outside the lookback window, or first-time build. Uses DDL, unaffected by the sandbox. (Applies to `stg_`'s incremental models only; `int_` is a `table` full rebuild on every run anyway.)
- **Proposal C targeted refresh**: correction rows land in old partitions the lookback window can't see → the last step of the repair runbook does a targeted refresh of the affected partitions (`--full-refresh`, or a future single-partition `insert_overwrite`). See [CLOUD_LAYER §7.4](../CLOUD_LAYER.md), DQ C-2 #7.
- **Before changing `int_orders` or `int_orders_quarantine`**: walk the §5.2 alignment checklist; afterwards run `dbt build --select intermediate+` and confirm `assert_orders_split_is_partition` is green.
- **Adjusting `rpt_quality_events_daily`'s lookback window**: `rpt_quality_events_lookback_days` must be ≥ `stg_quality_events_lookback_days`; change both vars together (§7.4).
- ⚠️ **If the DAG fails for more consecutive days than the lookback window, the first run after the fix must widen it** ⭐
  A single failure is safe: staging has already appended, the watermark has advanced, and `stg_`'s
  lookback window recomputes those days on the next run. **Consecutive failures are the danger** —
  with the default 3-day window, a DAG that has been down for 4 days looks back only 3 days on
  recovery, so rows that landed in staging before that boundary **never reach `stg_orders`**, with
  no error and no self-healing (silent data loss). On the first run after a fix, use
  `--vars '{stg_orders_lookback_days: N}'` (N ≥ days down + margin) or `--full-refresh`.
  The same applies to `stg_quality_events` and `rpt_quality_events_daily`; widen them together.
  Prevention: Airflow failure alerts must be seen *before* cumulative downtime approaches the
  window — which means **the lookback window is really a declaration of how much unattended
  failure the pipeline tolerates**, not just a cost parameter.

## 10. Dependencies & Versions

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`: `dbt-labs/dbt_utils >=1.1.0,<2.0.0` (resolves to 1.4.1)

## 11. Status & TODO

- ✅ `stg_orders` (dedup + Hard Gate + freshness, incremental)
- ✅ `stg_quality_events` (deduped at `id` grain, preserving the full state-machine history)
- ✅ `int_orders` + Row Filter, `int_orders_quarantine` (partition invariant guarded by a test)
- ✅ `int_order_items` (items flattened to item grain)
- ✅ `dim_customer`, `dim_product` (SCD1 + unknown member)
- ✅ `fct_orders`, `fct_order_items` (dual fact tables; rollup consistency and lossless projection both guarded by tests)
- ✅ `rpt_quality_events_daily` (event axis, incremental), `rpt_quality_backlog` (snapshot), `rpt_sales_daily_by_category`
- ⬜ Scenario-specific `int_orders_*` (designed; enable only when a real analytical scenario appears — see §5.3)
- ⬜ SCD2 `dim_customer` (designed; trigger = enabling billing — see §6.3)
- ⬜ Make `rpt_sales_*` incremental (path = daily incremental + weekly full refresh; **must land the cell-by-cell reconciliation test at the same time** — see §7.4, §8)
- ⬜ Monetary exposure measures (requires `int_order_items_quarantine` — see §7.6)
- ✅ Proposal B event producer (`reevaluate_quality.py` + the `dq_reevaluation` DAG) — **not one line changed in this layer**: `int_orders`'s effective-state composition was already test-guarded, so events take effect on the next run automatically. That is the payoff of having built the consumer side first
- ⚠️ `rpt_quality_events_daily`'s `promotions` / `re_quarantines` are **still 0**, but the reason has changed from "no event producer" to "no rule loosening has happened yet" — v1→v2 was a tightening, and re-evaluating v2 against v2 is a tautology. Non-zero values require a loosening v3 bump first (script in [ORCHESTRATION §3.3](../ORCHESTRATION.md))
