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
- File layout: under `models/staging/` — `_staging__sources.yml` (source def), `stg_orders.sql`, `stg_orders.yml` (tests/descriptions); under `models/intermediate/` — the `int_*.sql` models plus a shared `_intermediate__models.yml`.

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

**Boundaries** (what it can't solve — still go through the §7 runbook):
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

- **No logic drift**: the table always equals "the current SQL applied to the current upstream". An incremental table's old partitions can retain rows computed by an older version of the logic, fixable only via runbook (`stg_orders` carries exactly that burden — see §4.7, §7).
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
- The more concrete reason is a **landmine**: `int_orders_quarantine` is precisely where `ORDER_DATE_IN_FUTURE` rows land, and an absurd future date falls outside BigQuery's legal partition range, **failing the whole table build**. Partitioning it by `order_date` would be asking for failure. `dim_/fct_` must add a legal-range guard before enabling that partitioning.

### 5.6 `quarantined_at`: the event time, not `CURRENT_TIMESTAMP()`

An early DQ doc example wrote `CURRENT_TIMESTAMP() AS quarantined_at`. Under a `table` full rebuild that value **changes on every run** — it records "when this run happened", not "when this row was quarantined", distorting the `rpt_quality_*` timeline.

It is instead `coalesce(quality_state_at, received_at)`: prefer the `initial_evaluation` event's `event_at` (the real quarantine moment), falling back to ingestion time when the event is absent. If a run timestamp is ever needed, add a separate column with distinct semantics.

### 5.7 `int_order_items`: source, `safe_cast`, strict NULL propagation

Flattens `ODS.items` (a JSON array) to item grain via `unnest(json_query_array(items)) with offset`, for a future `fct_order_items`.

- **Sourced from `int_orders` (already filtered), not `stg_orders`**: item-level errors (`quantity_non_positive`, `unit_price_negative`, `discount_pct_out_of_range`, `non_finite_number`) already mark the **entire order** `has_clean_error=TRUE` at ingestion, so starting from `int_orders` naturally guarantees "no dirty data in Gold". For item-level RCA, build a separate model reading the quarantine table.
- **All numerics use `safe_cast`**: `clean.py` states explicitly that "values inside items are not coerced by Pydantic and may be strings" — items land as a whole JSONB blob, bypassing `ODSOrder`'s type coercion. A plain `cast` would let one dirty item **blow up the whole batch**; `safe_cast` yields NULL instead, matching the project's "mark, don't block" philosophy.
- **Derived amounts use strict NULL propagation, no `coalesce`**: `net_amount = quantity × unit_price × (1 - discount_pct/100)`, NULL if any input is NULL. The rationale is [CLOUD_LAYER §5.5.5](../CLOUD_LAYER.md)'s hard rule — NULL carries information ("no discount data" ≠ "discount is 0"), and `COALESCE` is lossy and one-way: collapse it to 0 at `int_` and no downstream can ever tell "not collected" from "genuinely zero" again. If "missing = no discount" is ever confirmed, add the imputation in `dim_/fct_`, **never retrofit this layer**.
- **`(raw_id, item_index)` is the item-grain key**: `items` is an immutable JSONB snapshot in ODS with fixed array order, so position is a stable identity; surrogate key `order_item_key = raw_id-item_index`.

## 6. Testing Strategy

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

> Custom generic tests (and some built-in ones) need their arguments nested under `arguments:` (dbt 1.11 requirement, else `MissingArgumentsPropertyInGenericTestDeprecation`).

## 7. Operational Runbook

- **When to `--full-refresh`**: changing partition/cluster, changing dedup logic, recomputing history outside the lookback window, or first-time build. Uses DDL, unaffected by the sandbox. (Applies to `stg_`'s incremental models only; `int_` is a `table` full rebuild on every run anyway.)
- **Proposal C targeted refresh**: correction rows land in old partitions the lookback window can't see → the last step of the repair runbook does a targeted refresh of the affected partitions (`--full-refresh`, or a future single-partition `insert_overwrite`). See [CLOUD_LAYER §7.4](../CLOUD_LAYER.md), DQ C-2 #7.
- **Before changing `int_orders` or `int_orders_quarantine`**: walk the §5.2 alignment checklist; afterwards run `dbt build --select intermediate+` and confirm `assert_orders_split_is_partition` is green.

## 8. Dependencies & Versions

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`: `dbt-labs/dbt_utils >=1.1.0,<2.0.0` (resolves to 1.4.1)

## 9. Status & TODO

- ✅ `stg_orders` (dedup + Hard Gate + freshness, incremental)
- ✅ `stg_quality_events` (deduped at `id` grain, preserving the full state-machine history)
- ✅ `int_orders` + Row Filter, `int_orders_quarantine` (partition invariant guarded by a test)
- ✅ `int_order_items` (items flattened to item grain)
- ⬜ Scenario-specific `int_orders_*` (designed; enable only when a real analytical scenario appears — see §5.3)
- ⬜ `dim_/fct_`, `rpt_quality_*`
- ⬜ Proposal B (Airflow re-evaluation writing `quality_events`) — the downstream reinstatement path is ready, only the event producer is missing
