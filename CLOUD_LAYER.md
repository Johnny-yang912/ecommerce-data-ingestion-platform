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

> **`received_at` vs `order_date`**: staging serves the pipeline, so it partitions by the ODS landing time `received_at` (⚠️ this column is *not* the order-receipt time — see §1.2.2); downstream Gold (`dim_/fct_`) serves analysts whose monthly/weekly aggregates filter by the business time `order_date`, so that layer partitions by `order_date` instead. The partition column is chosen per table, according to its own access pattern. **The full Gold-side decision is §1.2.1 below** — it is not this section copied over: each of the four decisions has its own reasoning, and two of them come out the opposite way.

DAY granularity over HOUR: batches are T+1 / hourly; and with a 4000-partition-per-table cap, DAY lasts ~11 years while HOUR lasts only 166 days.

### 1.2.2 ⚠️ `received_at` Means Two Different Instants in Raw and ODS ⭐

The column name means different things on the two tables, and three downstream mechanisms are built on it, so this has to be stated explicitly:

| Column | Stamped when | Meaning |
|---|---|---|
| `raw.received_at` | The API writes Raw synchronously in the request path (`models.py`, `server_default=func.now()`) | **Order-receipt time** |
| `ods.received_at` | The worker writes ODS (also `server_default`; `process.py` does not carry the Raw value over) | **ODS landing time** |

**The semantics are correct, not a compromise.** staging mirrors `extract_ods_to_bq.py`, and what extract moves *is* ODS — using ODS's own clock as the partition column and `loaded_at_field` answers exactly the question "did extract move ODS forward?". Switching to `raw.received_at` would fold "latency of the Raw→ODS hop" into the extract check, making one signal stand for two pipeline segments.

**But it carries a scope boundary you must know**: when a backlog is flushed by the recovery scan, those rows get an `ods.received_at` of the *catch-up write*, so the ingestion gap does not exist on the ODS timeline at all. Anything built on `ods.received_at` (partitioning, freshness, the day boundary in `rpt_quality_events_daily`) therefore only sees outages **still in progress at sampling time** — never ones that have already recovered.

The health of the dispatch hop is answered elsewhere: the oldest age of `raw.status='pending'` (owned by the `raw_pending_watch` DAG added in a follow-up change), and later the continuity of `raw.received_at` via OTel. **Three timelines, one hop each — none of them moonlights.**

⚠️ One easy-to-get-wrong criterion, spelled out: **"a Raw row with no matching ODS row" cannot be the definition of a fault.** Raw's terminal states are `processed` / `duplicate` / `error`; the latter two produce no ODS row *and that is correct behaviour*, so that definition would raise an alert on every duplicate order. `pending` is the clean signal — it means no worker has claimed the row yet.

**Why not change it to carry `raw.received_at` over**: the first reason is the one above — the semantics are already right, and changing them is what would make them wrong. The cost is only the secondary reason: changing the partition column's meaning requires rebuilding and backfilling the table, and shifts the Hard Gate's "latest UTC day partition" scope along with it.

⚠️ **The name reads like receipt time.** We are not renaming it (that is a migration, and it would ripple into the ODS→BQ `FIELDS` declaration and every dbt reference), but whoever reads `ods.received_at` next should take this section as authoritative rather than inferring from the name.

### 1.2.1 Partitioning Decisions for Gold (`dim_/fct_`) ⭐

Four decisions, each contrasted with staging:

| Decision | staging | Gold `fct_*` | Gold `dim_*` | Why they differ |
|---|---|---|---|---|
| Partition column | `received_at` | **`order_date`** | **none** | Facts serve analysts filtering on business time; dimensions are reached **by key join**, where a partition column prunes nothing and only buys small-partition metadata overhead |
| Granularity | DAY | DAY | — | Same as §1.2 |
| Retention | 60 days (sandbox-forced) | **5 years** (`var` gated) | — | Gold keeps full history, but DAY granularity is bounded by the 4000-partition cap (~11 years) → an explicit retention policy is mandatory, or year 11 hits the ceiling |
| `require_partition_filter` | ✅ on | ❌ **off** | — | See below |

#### Why Gold does not get the cost fuse

Staging turns it on at zero cost (§1.4: every access already carries `received_at`). But Gold serves analyst ad-hoc queries and Looker Studio's **exploratory** queries, and those frequently carry no date filter at all — with the fuse on, every one of them is a 400. This is the same tension as the "fuse contagion" argument in the dbt README §4.6, except it now lands at the very bottom of the DAG and faces humans rather than pipelines.

The cost is giving up "catch a single accidental full scan." The replacement is BigQuery **custom quota** (per-user / per-project daily scan cap). These two guard **different things**: the fuse guards a single mistake (every query is asked "did you filter?"), quota guards systematic abuse (only cumulative overrun blocks). For a layer opened up to analyst exploration, the latter is the right shape.

#### How much does partitioning actually save: measured (2026-08, 540 rows)

Two tables over identical data, running the typical analyst query "last-30-day slice":

| | totalBytesProcessed | vs. full table |
|---|---|---|
| Full table | 68,856 B | 100% |
| `cluster by order_date` only | 12,474 B | **18%** |
| `partition by order_date` + cluster | 6,490 B | 9% |

**Clustering alone pruned 82%; partitioning adds 9 more percentage points.** So the common claim "partitioning saves a lot of money" needs correcting — partitioning's value is not the pruning volume but the three things clustering cannot give:

1. **Cost predictability**: partition pruning is decided from metadata before the query runs, so `dry run` byte counts are exact; clustering prunes at block level depending on data layout, so `dry run` over-estimates. Cost governance depends on the former.
2. **The prerequisite for `require_partition_filter`**: only a partitioned table can have it (even though Gold chooses not to).
3. **Partition-level operations**: `insert_overwrite`'s atomic whole-partition replace, single-partition targeted refresh — the entire `stg_` runbook rests on this.

> Aside: BigQuery bills a **10 MB minimum per table per query**, so at this project's current data volume both variants cost exactly the same. Partitioning's benefit only holds under the "assume tens to hundreds of millions of rows" premise — a premise deliberately declared (this project is a practice simulation), not an empirical finding. For the extrapolation see the per-row cost table in the dbt README §5.4.

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

> **60-day expiration cap (sandbox limit)**: because this is a practice project with billing disabled, it runs on a BQ sandbox, which forces a 60-day partition + table expiration on the dataset that `quality_events` inherits; so the "latest across all history" assumption is in practice capped at 60 days on the sandbox — an account-level limit (setting `expiration=None` in the script is ignored by the sandbox), lifted only once billing is enabled. **Full measurements in §1.7**; the consequences of this limit are far more severe once Gold partitions on a business time axis.

### 1.7 Sandbox Partition Expiration: Measurement Record (2026-08) ⭐

§1.6 recorded only "`expiration=None` is ignored." But once Gold partitions on a **business time axis**, the consequences of this limit are entirely different, so here is the full measurement. **Every item below was measured, not inferred.**

#### 1.7.1 Expiration is computed from the partition's date value, not the build time

All datasets (`staging`, `dbt_dev`) carry `default_partition_expiration_ms = 5184000000` (60 days), and every existing partitioned table inherits it. Measured: create a `partition by order_date` table and CTAS five dates straddling the 60-day boundary in one statement —

| Partition | Result |
|---|---|
| 2024-01-01 | **rows=0, gone** |
| 2026-05-01 (94 days ago) | **rows=0, gone** |
| 2026-06-04 (on the boundary) | rows=1 ✅ |
| 2026-07-01 (33 days ago) | rows=1 ✅ |
| 2026-08-03 (today) | rows=1 ✅ |

Three key behaviours: **① the build does not fail** (`CREATE OR REPLACE` returns success); **② a "2024-01-01" partition is already past 60 days at the instant it is born**; **③ deletion is synchronous and immediate** — querying right after the CTAS returns, both old partitions are already absent from `INFORMATION_SCHEMA.PARTITIONS`, and even the `num_rows` metadata reads 3 rather than 5. No warning.

> `stg_orders` never hit this purely because it partitions on `received_at`, and ingestion time is always recent. **Switch to a business time axis and that protection disappears.**

#### 1.7.2 The expiration ceiling is hard-locked at 60 days; all four routes are closed

| Attempt | Result |
|---|---|
| DDL `options(partition_expiration_days = 3650)` | ❌ job fails |
| DDL `options(partition_expiration_days = NULL)` | ⚠️ **no error, silently rewritten to 60 days** |
| API: set `table.time_partitioning.expiration_ms` | ❌ 403 |
| API: set dataset `default_partition_expiration_ms` | ❌ 403 |

Verbatim error:

```
reason: billingNotEnabled
Partition expiration time must be less than 60 days while in sandbox mode.
```

**This is why `gold_partition_expiration_days` must be `var`-gated and emit nothing by default** — hard-coding 1825 makes every `dbt run` fail and skips everything downstream in a `dbt build`.

> **A leak worth knowing about but not using**: the `partition_expiration_days = 3650` DDL actually **half-succeeds** — the job is marked failed (`state=DONE`, `error_result.reason=billingNotEnabled`), yet the table is created, `expiration_ms` really is 3650 days, the old rows survive and are queryable, and they are still there 60 seconds later. Enforcement sits at the **job validation layer** and the DDL's side effect slips past it. Unusable: a failed job is a failed dbt run; and this is an enforcement gap rather than a supported path, so once Google closes it the table starts being reaped silently.

#### 1.7.3 Out-of-range dates land silently in `__UNPARTITIONED__` — they do **not** fail the build

**This item overturns the previous claim in §8 of this document** (originally: "absurd future dates fall outside BigQuery's acceptable partition range and fail the whole table build"). Measured:

```
partition_id=20260803           rows=1
partition_id=21591231           rows=1
partition_id=__UNPARTITIONED__  rows=3   ← 1959-12-31 / 2160-01-01 / 9999-12-31
build succeeded; all 5 rows survive and are queryable
```

Values outside `1960-01-01 ~ 2159-12-31` raise no error and go silently into `__UNPARTITIONED__`. Knock-on effect: those rows likewise **escape the 60-day reaper**, and can never be pruned by partition pruning.

So `dim_/fct_` partitioning on `order_date` needs **no** legal-range guard. (The decision to leave `int_orders_quarantine` unpartitioned still stands, but only on the "`int_` is consumed inside the DAG, partition benefit ≈ 0" reason.)

#### 1.7.4 The `__NULL__` partition escapes the reaper

`order_date` is nullable in ODS. NULLs land in BigQuery's `__NULL__` partition, which has no date and therefore no computable expiration, so it is **never reaped** — in the measurement it was written in the same batch as 2024-01-01, which vanished on the spot while it survived. Consequence: orders without an `order_date` outlive those with one in `fct_orders`. Current data has 0 NULLs, but the schema permits them.

#### 1.7.5 Two 60-day clocks on different axes ⭐

Of everything above, this is the one with the biggest impact on **test design**:

```
int_orders ← stg_orders ← staging.orders    expires by received_at
fct_orders / fct_order_items                expires by order_date
```

So the invariant "`fct_orders`' content == `int_orders`' content" **cannot be written as `count(*) = count(*)`**: the two tables have structurally different retention, so even a green result means "the two reapers happened to agree," not "the SQL is right." Worse, reaping is not synchronized — `fct_orders` is `CREATE OR REPLACE` and reaps synchronously; `stg_orders` is incremental and its old partitions are reaped asynchronously by BigQuery's background reaper — so boundary days are guaranteed to disagree, turning the test into one that goes red for a stretch every single day.

The fix is to **anchor the invariant on an `order_date` window** (anti-join; see `tests/assert_fct_orders_complete_projection.sql`) and to make `load_test.py` generate `order_date ≈ received_at` (see `ORDER_DATE_LOOKBACK_DAYS` in that file) so the two axes line up. **The old generator hard-coded `order_date` into 2024, an average of 410 days away from `received_at`; that would make every newly loaded row get reaped the moment it reaches Gold, leaving the test permanently red.**

#### 1.7.6 The BQ side is a rolling 60-day *ingestion* window; ODS cannot be backfilled ⭐

The five items above are about how partitions get reaped. This one is about what that does to the **shape of the whole pipeline** — **the most easily forgotten item here, and the one most likely to mislead a future decision.**

The extraction script writes ODS's `received_at` **verbatim** as the partition column (see `FIELDS` and `partition_field` in `extract_ods_to_bq.py`). Therefore:

> **Any ODS row whose `received_at` is older than 60 days will be reaped the moment it lands, no matter how many times it is re-extracted.**

ODS remains a permanent, complete anchor in PostgreSQL (`raw`/`ods`/`quality_events` lose nothing). But **on the BQ side the pipeline is structurally a rolling 60-day ingestion window** — not "keeps the last 60 days of data" but "can only hold data *ingested* in the last 60 days." The only route for historical data to reach BQ is **re-ingestion** (producing a fresh `received_at`), not re-extraction.

Three direct corollaries:

1. **"Just clear BQ staging and re-extract" is an ineffective shortcut.** `get_watermark()` will find no partitions, return `None` and trigger a full extract (§2.1), but the old rows evaporate on landing exactly as before — the end state is identical to before the clear. The only way to get data into BQ is to load new data.

2. **Once staging empties, extraction silently degrades to "full ODS scan every run" — and reports success.** The watermark stays `None`, so every run is a full extract; the load job reports `output_rows = <all ODS rows>`, and **the E/L gate only checks whether the load job raised, with no post-load `SELECT COUNT(*)` verification** (§3.2). The result is a **lying green light**: "loaded N rows successfully" every run while the table stays empty. Guarding against this needs a post-load row-count check in the gate (not implemented today).

3. **`dbt source freshness` goes red before anything else does — and it does not measure what you think it measures.**

   `loaded_at_field` points at `received_at`, which is **ODS ingestion time**; `ORDERS_FIELDS` contains **no extraction-time column at all** (the whole thing is a 1:1 mirror of ODS). So this check answers "how long ago did the newest order **enter ODS**", **not** "how long since the extraction job last ran."

   The consequence is that two completely different failures look identical:

   | Failure | Symptom |
   |---|---|
   | (a) Upstream stopped sending orders (business flow broke) | `max(received_at)` stops advancing → ERROR STALE |
   | (b) The extraction job died (pipeline broke) | `max(received_at)` stops advancing → ERROR STALE |

   **This also means: scheduling extraction in Airflow in Phase 5 will not turn it green.** What gets scheduled is the mover, not the producer — with no new orders in ODS there is no new `received_at`. The only way to keep it green is continuous ingestion (a batch at least every 26h). Telling (a) from (b) apart requires a second signal (e.g. an `_extracted_at` column added via §5.2's `ALLOW_FIELD_ADDITION`), which is post-Phase-5 work — extraction is manual today, so (b) cannot happen at all.

   **NULL degrades gracefully; it is not a hard error**: the `filter: received_at > now - 30 days` window becomes empty 30 days after ingestion stops, so `max()` returns NULL. dbt guards against this (`_create_freshness_response` in `dbt/adapters/base/impl.py`: `if last_modified is None → datetime(1,1,1)`, commented "Interpret missing value as infinitely long ago"), and the `loaded_at_field` path shares that same code, so it **does not crash** — `max_loaded_at` just renders as `0001-01-01` and the result is still ERROR STALE.

> Contrast with Proposal C in §7: there the problem is "corrected rows land in old partitions the watermark cannot see, so they must be pushed deliberately." Here it is one degree stronger — on the sandbox, **the old partition does not exist as a push target at all**. §7.1's conclusion ("pushing to the cloud is an explicit step") still holds, but on the sandbox its reachable range is compressed to 60 days.

#### 1.7.7 This project's stance: a red freshness check is the expected state ⭐

Ingestion in this project is **manual** (loaded through the API; `load_test.py` is a load-testing tool only), not a continuous stream. So between two loads, `dbt source freshness` is **necessarily ERROR STALE** — measured on 2026-08-03 it was 625 hours stale, 12.5× over the `error_after: 50h` threshold.

**This is an accepted state, not a defect awaiting a fix**, for two reasons:

1. **The threshold describes the SLA of the *simulated system*, not the habits of the *simulator*.** 26h/50h is a reasonable SLA for a real e-commerce system taking continuous orders; loosening it to 30 days would only make this configuration lie about how quickly the system is supposed to be fed. **Better a signal that is honestly red than a threshold slackened to look good.**
2. **It blocks nothing.** `dbt build` does not include freshness (`build` = run + test + snapshot + seed; freshness is a separate command), and the watermark reads partitions rather than freshness results — so the "load data → extract → `dbt build`" path is entirely unaffected.

⚠️ **But point 2 is a precondition, not a guarantee.** This stance holds precisely because freshness is **not wired as a blocking gate**. Therefore:

| Rule | Why |
|---|---|
| **Phase 5's Airflow DAG must not place `dbt source freshness` ahead of extraction / `dbt build` as a pre-check** | The very same red instantly turns from "an acceptable alert" into "a permanently blocked DAG" — while all it reflects is "you haven't hand-loaded data for a few days" |
| ~~If it goes into the DAG at all, it must be a **side-channel observability task** (its failure must not affect downstream), or have its `severity` lowered first~~ → **tightened one notch at implementation time: it becomes its own `source_freshness_watch` DAG** | A side-channel task is not enough — see 〈Implementation outcome〉 below |
| ~~If ingestion ever becomes **continuous**, this stance lapses and freshness should be restored as a meaningful gate~~ → **condition met on 2026-08-11** (`seed_demo_daily`, four batches a day); freshness has flipped from "expected red" to "expected green" | Red now genuinely does mean broken. **But it still is not wired up as a gate** — for a different reason; see below |

> **Measured on 2026-08-05**: fifteen minutes after a data load, the `source_freshness_watch` DAG
> ran `dbt source freshness` and both sources **PASSED**. So this section's stance is not "we
> loosened the standard" but "feed it and it is green, starve it and it is red" — **the signal has
> been honest all along**. When it goes red it really is saying something true; that something is
> just "you have not fed it lately" rather than "the pipeline is broken". Full verification in
> [ORCHESTRATION §5.3](./ORCHESTRATION.md).

> This is isomorphic to how the DQ architecture treats `has_schema_drift`: **a signal's value is not the same as the authority it should hold.** Drift may alert but never intercept; under the "manual ingestion" premise, freshness likewise may only alert. Authority comes from "is it actually broken when it goes red", not from "is this metric important."

Aside: continuous ingestion is simultaneously the fix for §1.7.6's "rolling 60-day ingestion window" problem — the two share **the same root cause** (no continuous ingestion), so a future decision to add seeding resolves both at once.

**⚠️ The condition was met, but only half the conclusion changed (2026-08-11)**

The table above anticipated "continuous ingestion → restore freshness as a gate". Continuous
ingestion arrived (`seed_demo_daily`), and the first half holds: red no longer merely means "you
haven't fed it". But **the second half is deliberately not carried out**, because the reason has
been replaced by a different one:

> **Seeding is this system's only data source. So the day seeding breaks *is* the day with no new
> data — running the analytics pipeline once over yesterday's data is harmless and correct, and
> blocking it buys nothing.**

Freshness therefore moved from "a signal that shouldn't have authority because its premise doesn't
hold" to "a signal whose premise holds, but for which **blocking itself has no value**". **Same
conclusion, different argument** — the distinction has to be written down, or the next reader will
assume that meeting the condition means wiring it up as a gate.

"Continuous ingestion" is also qualified here: **four batches a day, not round-the-clock**. The
26h/50h thresholds have enormous slack at that cadence (worst case is a 12-hour gap): they detect
"nothing was loaded all day" but not "the peak stopped for three hours". Genuinely continuous
ingestion would require re-deriving them — a question that cannot even be posed at the current
cadence.

**Implementation outcome (Phase 5): a side-channel task is not enough — it needs its own DAG** ⭐

The table above originally said "a side-channel observability task (its failure must not affect
downstream)". Implementing it revealed that this is still insufficient: **an Airflow DAG run's state
is the aggregate of its tasks**, so an expected-red leaf task leaves `orders_analytics_daily`
permanently failed — which zeroes out the value of "main pipeline success rate" as a signal and
buries real pipeline failures under noise that is red every single day.

So this section's principle goes one step further: **freshness has neither the authority to block
downstream nor the authority to pollute someone else's success rate.** The landed shape is a
standalone `source_freshness_watch` DAG (`orchestration/dags/`), so that each DAG's success rate
means exactly one thing:

| DAG | Red means |
|---|---|
| `orders_analytics_daily` | The pipeline is broken |
| `source_freshness_watch` | staging is stale = extract did not move ODS across (as of 2026-08-11 this flipped from "expected to be red" to "expected to be green" — see the update in the §1.7.7 table above) |

`tests/test_dags.py::TestFreshnessIsolation` pins this isolation down: if any DAG that produces
real output ever picks up `dbt source freshness`, the test goes red.

#### 1.7.8 The sandbox's 60-day partition expiration is **unconditional**, not just a cap on explicit settings ⭐

Measured against `dbt_dev`'s `INFORMATION_SCHEMA.TABLE_OPTIONS` on 2026-08-04:

| Table | Does the model set `partition_expiration_days`? | Actual value |
|---|---|---|
| `stg_orders` / `stg_quality_events` | ❌ never set in the model | **60** |
| `fct_orders` / `fct_order_items` | ❌ `gold_partition_expiration_days=null` → the option isn't emitted at all | **60** |
| `rpt_sales_daily_by_category` / `rpt_quality_events_daily` | ❌ same | **60** |

**Conclusion: `none` does not mean "never expires" — the sandbox fills in 60 days on every partitioned table.**
§1.7.2's "the sandbox hard-locks expiration below 60 days" only described half of it (explicit settings being rejected); the other half is that **not setting it gets you 60 days anyway**.

**This has already had real consequences** (measured the same day):

```
int_orders   540 rows (order_date 2024-01-01 ~ 2026-07-01; unpartitioned, so all retained)
     ↓ 333 rows dropped
fct_orders   207 rows (order_date 2026-06-05 ~ 2026-07-01 — exactly a 60-day window)
```

The entire gap is rows whose `order_date` fell outside the 60-day window — the vast majority
generated by the old `load_test.py` with its hard-coded `date(2024,1,1)`, reaped the moment they
reached Gold (that file's `ORDER_DATE_LOOKBACK_DAYS` comment already predicted this failure;
the fix has landed but **the data has not been regenerated**).

**This is also why `assert_fct_orders_complete_projection` must carry an `order_date` window**:
that isn't defensive design, it's **the only way the test can hold**. `int_` is unpartitioned and
therefore unaffected by the reaper while `fct_` is affected, so an unwindowed `count = count`
is guaranteed to stay red forever under the sandbox.

Enabling billing lifts this forced value, at which point `gold_partition_expiration_days` finally
takes effect (and `gold_projection_window_days` should be adjusted to match the retention policy).

#### 1.7.9 staging retention is not a free parameter — Proposal B imposes a hard floor ⭐

The sections above treat 60 days as "a limit the sandbox imposes". Implementing Proposal B
(`reevaluate_quality.py`) surfaced something else: **even with billing enabled and the limit
lifted, staging retention cannot be set arbitrarily short.**

Re-evaluation candidates come from BQ's `int_` layer, and `int_` ← `stg_` ← `staging.orders`.
Proposal B's canonical trigger is "rules loosened → pull back old quarantine **across all
history**" — the moment retention is shorter than "the oldest quarantine we might still want
back", those records simply do not exist on the BQ side. Re-evaluation cannot see them, and
**nothing errors**: the query returns fine, the task succeeds, a batch is just missing. Same
shape as the "green light lying" failure in §1.7.6.

Hence this project's stance: **staging is a mirror with a retention policy, but that policy is
determined by Proposal B's retroactive reach, not by storage cost.** The criterion when picking
the value is one sentence — "how far back are we willing to re-evaluate?" Retention must be at
least that.

> Contrast: ODS (PostgreSQL) is permanent and complete, so decisions that **never look back** —
> `permanently_rejected` and the like — are safe even beyond staging retention; only the records
> we might still want to pull back are at risk. This is also why re-evaluation reads its **state
> decision** from PG rather than BQ (see the Proposal B section in DQ_ARCHITECTURE) — idempotency
> cannot rest on a mirror that expires.

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
- dbt layering: `stg_` (`stg_orders`, `stg_quality_events`), `int_` (`int_orders`, `int_orders_quarantine`, `int_order_items`) and `dim_/fct_` (`dim_customer`, `dim_product`, `fct_orders`, `fct_order_items`) are in place — see [ecommerce_dbt/README](./ecommerce_dbt/README.md). §5.5.5's hard rule ("never overwrite canonical columns; push imputation to the DAG edge") is realized in `int_order_items` as **strict NULL propagation** for derived amounts (no `coalesce`), and in `fct_orders` as `items_missing_amount`, which surfaces rollup incompleteness explicitly (see §1.2).
- ~~If `dim_/fct_` adopt `order_date` partitioning, add a legal-range guard first~~ — **retracted; disproved by measurement in 2026-08, see §1.7.3.** Dates outside BigQuery's legal partition range do **not** fail the build; they land silently in `__UNPARTITIONED__`.
