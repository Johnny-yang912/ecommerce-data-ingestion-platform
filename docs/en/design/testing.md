# Testing Strategy

**English** | [繁體中文](../../zh-TW/design/testing.md)

What is tested, where, and — most importantly — **where the tests are blind**.

---

## 1. The layers

| Layer | Count | Where | Needs |
|---|---|---|---|
| Unit + integration (mock DB) | 445 | CI, `ci.yml` | nothing — seconds to run |
| DAG structure | 52 | CI, `dags.yml` | Airflow, no DB, no project env |
| dbt | 97 | in the DAG | BigQuery |
| Manual scripts | 3 | by hand | a real server + real Postgres |

Unit coverage is **100% of the 12 gated modules**, across a **Python 3.10 and 3.12** matrix. Test dependencies are pinned in `requirements-dev.txt`.

> **These counts go stale. Regenerate them rather than trusting them** — the numbers above were last verified 2026-08-30:
>
> ```bash
> pytest --collect-only -q | tail -1            # unit + integration
> pytest tests/test_dags.py --collect-only -q   # DAG (needs Airflow; auto-skips locally)
> python -c "import json;print(sum(1 for r in json.load(open('ecommerce_dbt/target/run_results.json'))['results'] if r['unique_id'].startswith('test.')))"
> ```
>
> A number written into a document has no mechanism keeping it true. **The command does.**

---

## 2. Two CI workflows, deliberately not merged

`ci.yml` runs the main suite with a mocked database — no real DB required, finishes in seconds.

`dags.yml` installs Airflow under the official constraints and parses `orchestration/dags/` with DagBag. **Folding it into the main job would destroy that job's "mock DB, done in seconds" property**, because Airflow's install is heavy and pins many package versions.

It needs no `DB_URL`, because DAG files deliberately import no project module ([ADR-0036](../adr/0036-dag-no-toplevel-import.md)) — the discipline is what makes the DAGs CI-testable at all.

---

## 3. What CI covers, and where it is blind

CI verifies **application logic and type contracts**. The **DB-layer contracts are outside its scope**, because the in-CI tests substitute a mock for the database:

| Not automated | Exercised by |
|---|---|
| CAS claim under real concurrency | `scripts/load_test.py --cas-test` |
| `order_id` deduplication | `scripts/load_test.py --duplicate` |
| Post-crash recovery | `scripts/restart_test.sh` (SIGKILL) |
| Alembic ↔ `models.py` drift | `check_migration_drift.py` |

> ⚠️ **Do not read a green check as "everything is fine".** A passing CI run means there is no regression in the **logic layer**. It does **not** mean the dedup / CAS / migration contracts have been verified. When changing that logic, re-corroborate with the manual scripts.

**Why the database is not wired into CI**: the value of CAS and recovery only materialises under genuine concurrency, and test-authoring effort plus container-startup flake maintenance currently costs more than the risk of not automating it. `check_migration_drift.py` is the exception — deterministic, concurrency-free, low-flake, and it *could* run in CI today. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

---

## 4. Tests that pin decisions, not behaviour

A handful of tests exist to stop a future change from silently removing a guarantee. They are worth knowing by name:

| Test | Pins |
|---|---|
| `assert_orders_split_is_partition` | `int_orders` + `int_orders_quarantine` are mutually exclusive and exhaustive — **the only automated safety net for the duplicated effective-state block** ([ADR-0045](../adr/0045-int-effective-state-duplication.md)) |
| `assert_fct_orders_rollup_matches_items` | the header rollup equals the line sum ([ADR-0047](../adr/0047-measures-roll-up-to-header.md)) |
| `test_dbt_never_splits_run_and_test` | splitting `dbt build` would silently disable the Hard Gate ([ADR-0040](../adr/0040-layered-dbt-execution.md)) |
| `TestFreshnessIsolation` | no output-producing DAG may pick up `dbt source freshness` ([ADR-0039](../adr/0039-observation-signals-own-dag.md)) |
| `test_schema_bq_consistency` | `FIELDS` matches `models.py` — otherwise a forgotten column fails **silently** ([ADR-0026](../adr/0026-fields-single-source.md)) |
| `test_script_deps` | the read-only probe does not inherit the write path's dependency tree ([ADR-0039](../adr/0039-observation-signals-own-dag.md)) |
| `test_seed_demo` | missing optional fields must not become dirty data — a cross-module invariant on the generator |
| `test_dag_param_injection` | a string-typed DAG param reaching `bash_command` must be **both** constrained (`pattern`) and quoted (`\| q`). The worker container holds `DB_URL`, `API_KEYS` and the GCP key, so a failure on this path is arbitrary code execution — not a bad parameter |

**These are not ordinary unit tests.** Each one converts a discipline into a mechanism, and downgrading any of them removes the justification for the design decision it protects.

---

## 5. Manual verification scripts

| Script | Verifies |
|---|---|
| `scripts/load_test.py` | throughput, CAS under real concurrency (`--cas-test`), dedup (`--duplicate`) |
| `scripts/restart_test.sh` | `SIGKILL` mid-processing, then recovery of `pending` rows |
| `check_migration_drift.py` | `alembic upgrade head` + `compare_metadata`; non-zero exit on drift |

All three hit a real server and a real PostgreSQL. Their results are recorded in `docs/*/verification/` (stage 4).

---

## 6. The dbt test inventory

97 tests. The full list, because "which test guards what" is not derivable from the model files:

| Test | Target | Severity | Notes |
|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | `has_clean_error` ratio over `stg_orders`'s **latest `received_at` partition** | error @15% | **Hard Gate** — the only test with blocking authority. Per-batch rather than whole-table: a whole-table denominator grows with history and dilutes single-batch anomalies away, and it cannot heal. Can't use `dbt_utils.expression_is_true` (row-level, folded into `WHERE`; aggregates error out) → custom `error_rate_below` uses `HAVING` |
| `monitor_dataset_error_rate` | whole-table ratio on `stg_orders` | warn @10% | A **gauge**, deliberately given no blocking power. Its denominator is a function of retention/backfill policy — sensitive to things other than quality |
| `unique` + `not_null` | `stg_`'s `raw_id`/`id`/`order_id`; `int_`'s `raw_id`/`order_id` | error | `stg_`'s `unique(raw_id)` **is** the dedup check |
| `not_null` | `received_at` / `has_clean_error` / `has_schema_drift` | error | REQUIRED columns |
| source freshness | `staging.orders`, `staging.quality_events` | warn 26h / error 50h | with `filter` to bypass the fuse |
| ⭐ `assert_stg_orders_matches_staging` | `staging.orders`'s `distinct raw_id` vs `stg_orders`'s row count, **per partition** | error | **Reconciliation, not content.** `stg_` only dedups staging, it never filters, so the two counts must agree partition by partition. This is one of the **two** tests in the list that ask whether rows are *still there* (the other is the row below) — in the [2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md) every surviving row was perfectly valid; 550 were simply gone, and every other test is structurally blind to that. Its window (7d) **must exceed the lookback window** (3d), or damage slides out of both before anyone sees it |
| ⭐ `assert_stg_quality_events_matches_staging` | `staging.quality_events`'s `distinct id` vs `stg_quality_events`'s row count, **per partition** | error | The twin of the row above. The dedup key is `id` (the event PK), not `raw_id` — one `raw_id` legitimately has many events, so reconciling on `raw_id` would read a normal event sequence as "extra rows". **Both must exist separately**: in phase two of the [2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md) the events side lost 550 rows too, and the test above caught none of it — it names `stg_orders` and nothing else |
| ⭐ `assert_orders_split_is_partition` | `int_orders` ∪ `int_orders_quarantine` vs `stg_orders` | error | **Partition invariant** — every `raw_id` appears exactly once. The only automated safety net under the duplicated block, guarding checklist items #1–#4. **Never downgrade** |
| `assert_int_orders_no_unpromoted_dirty` | `int_orders` | error | **Gold contract** — no `has_clean_error=TRUE` row that hasn't been promoted. A singular test rather than a column test because it is a **conditional relation between two columns**: `has_clean_error=TRUE` is legal here |
| `accepted_values` | `effective_quality_state` on both `int_` tables | error | The two domains are disjoint (`clean`/`promoted` vs `quarantined`/`permanently_rejected`) — **cross-checks the partition from another angle** |
| `unique_combination_of_columns` + `relationships` | `int_order_items`'s `(raw_id, item_index)`; `raw_id → int_orders` | error | Item-grain uniqueness and lineage integrity |
| ⭐ `assert_fct_orders_rollup_matches_items` | `fct_orders` rollup vs `fct_order_items` aggregates | error | **Rollup consistency invariant.** Compared per order with `is distinct from` — `=` would let "both sides NULL" rows be silently filtered out |
| ⭐ `assert_fct_orders_complete_projection` | `int_orders` (in-window) vs `fct_orders` | error | **Lossless projection** — interception already happened at `int_`, so Gold must not drop a row. An anti-join over an `order_date` window rather than `count = count`, because **the two tables' 60-day clocks hang on different axes** and a count comparison would go flaky daily |
| ⭐ `assert_int_orders_quality_state_resolved` | `quality_state_at` NULL on either `int_` table **and** `received_at` inside a 2–50 day window | **warn** | **Absence-of-value failure** — the third failure shape in the project. `int_orders` takes its quality state through a LEFT JOIN, so upstream row loss leaves this layer with a **perfectly correct row count and a NULL column**, which per-partition reconciliation (it only counts its own table) is structurally blind to. Phase two of the [2026-08-30 incident](../incidents/2026-08-30-stg-partition-truncation.md) was exactly this: all 800 rows present, 550 of them with an empty quality state, 94 tests green. **It asserts "must not stay NULL", not "must not be NULL"** — the event-absent fallback is deliberate and may only cause delay. The lower bound **must exceed one DAG cycle**; the upper bound excludes the retention-expiry race (a portfolio limitation). **`warn` is the final answer**, reasoned below |
| `assert_initial_event_shares_order_timestamp` | `initial_evaluation`'s `event_at` vs its order's `received_at` | **warn** | **A canary on an assumption**, not a correctness test — it protects the *derivation* behind the row above: the 2-day lower bound holds only because an order and its initial event are written in one transaction. Timestamp equality is the observable proxy for that structure; if the write path ever becomes asynchronous, this fires first. ⚠️ **Must be scoped to `initial_evaluation`**: promotion events are stamped `now()` and bear no relation to `received_at`, so the unscoped version is simply wrong (the 31 promotions today happen to share their order's partition — coincidence, not structure) |
| `assert_product_attributes_stable` | `product_id` → attributes on `int_order_items` | **warn** | An upstream contract signal, not a defect in this layer — if `product_id` can't determine attributes, fix upstream rather than stopping the DAG |
| `unique` + `not_null` | dimension keys; `fct_orders.order_id`; `fct_order_items.order_item_key` | error | Dimension grain and surrogate-key uniqueness |
| `relationships` | `customer_id`/`product_id` → `dim_*`; `fct_order_items.order_id` → `fct_orders` | error | Star-schema FK integrity, paired with `not_null` (the unknown member guarantees FKs are never NULL) |
| `unique_combination_of_columns` | `fct_order_items`'s `(order_id, item_index)` | error | The declared grain |
| ⭐ `assert_rpt_sales_no_item_loss` | `rpt_sales`'s `sum(items)` vs `fct_order_items` row counts | error | `rpt_sales` introduces the **only new joins in the whole DAG**. A join quietly turning INNER shows up as "revenue slowly shrinking" and raises nothing. The full outer join catches "too many" as well (dimension fan-out) |
| ⭐ `assert_rpt_quality_events_split` | `initial_clean + initial_quarantined = initial_evaluations` | error | **Domain-expansion alarm for a wide table.** The price of a wide table is "one more `to_state` upstream and downstream needs a schema change to see it". A new state makes `count(*)` grow while the `countif`s don't → this goes red immediately instead of letting those events evaporate. **It is what makes the wide table safe to use** |
| `assert_rpt_backlog_primary_code_balances` | `sum(orders_primary_code)` vs actual counts in quarantine | error | When it breaks, the symptom is the backlog KPI in BI simply being wrong, **with no self-healing** |
| `unique_combination_of_columns` + `not_null` | the declared grain of each `rpt_` table | error | **A broken grain in a pre-aggregate doubles every number, silently** |
| `expression_is_true` | `orders <= items`, `items_missing_amount <= items`, `orders_with_code >= orders_primary_code` | error | Cheap sanity floors |

> Custom generic tests (and some built-ins) need their arguments nested under `arguments:` — a dbt 1.11 requirement, else `MissingArgumentsPropertyInGenericTestDeprecation`.

### One test deliberately not written

**Cell-by-cell amount reconciliation** between `rpt_sales` and `fct_`. Under `table` full rebuilds it is a **tautology** — `rpt_`'s sum *is* `fct_`'s columns added up — so it would be always green and carry zero information.

Its value only materialises the day the model goes incremental, where it would catch a missed partition.

> **"Make `rpt_sales` incremental" and "add cell-by-cell reconciliation" are two halves of one change. Doing only the first is not allowed.**

Contrast `assert_rpt_sales_no_item_loss`, which *is* written now: it tests **row counts across two joins**, which is independent of materialisation strategy and a genuinely possible failure today.

### Reconciliation tests vs content tests

Every test above except the two `*_matches_staging` tests asks about **content**: is this
value within contract, does this relationship hold, is this grain right. They share one
precondition — **the row being checked has to be there**.

Row loss is an orthogonal failure, and a much quieter one:

| | Content failure | Row-loss failure |
|---|---|---|
| Symptom | Some value is wrong | Every value is right, there are just fewer of them |
| Who notices | A test | Someone's eyes on the BI dashboard, if anyone happens to look |
| Upstream evidence | Usually still there | **Upstream is untouched** — so you cannot work out afterwards what deleted them |

> **Content tests only catch what still exists.** A suite made entirely of content tests
> is green while data is being deleted.

A reconciliation test costs one `count(*)`, so whether to have one is not a cost question.
It is a question of whether anyone thought of it.

### A reconciliation test protects a *table*, not the category "row loss"

⚠️ **Every incremental model needs its own reconciliation test. You cannot infer that this
model is safe because that one is tested.**

That reads like a truism, but it was learned the hard way: after
`assert_stg_orders_matches_staging` was added on the morning of 2026-08-30, ADR-0055 stated
that "the next instance of row loss will be caught automatically". **Later the same day
`stg_quality_events` lost 550 rows and that test caught none of it** — because it names
`stg_orders`.

Coverage is not "do we have this kind of test". Coverage is **how many models each have one**.

### And one level beyond: row loss disguises itself as absence of value

A reconciliation test is one level stronger than a content test — it asks whether rows still
exist. But it has a precondition of its own: **it only sees its own table.**

In phase two of the same incident, `int_orders` held a complete 800 rows for `2026-08-26`,
none missing — and 550 of them had a NULL `quality_state_at`. The rows lost upstream in
`stg_quality_events` became **empty columns** downstream, through a **LEFT JOIN**.

| | Row loss | After a LEFT JOIN |
|---|---|---|
| Symptom | Some rows are missing | Row count is exactly right, one column goes NULL |
| What catches it | A reconciliation test | `assert_int_orders_quality_state_resolved` |

> **Defences are shaped around the shape of the damage, and damage changes shape.**

**This test asserts "must not stay NULL", not "must not be NULL".** That distinction is the
whole test:

`int_orders`'s LEFT JOIN has a deliberate fallback — when the event is absent it falls back
to the ODS snapshot (clean rows flow, dirty ones stay quarantined), and
[cloud-layer](./cloud-layer.md) states that this may only cause **delay, never dirty data**.
So a NULL is legitimate, and a plain `not_null` would go amber on the newest partition of
every single run — and **an amber light that is routine is not a light** (this project has
already paid for that lesson once: freshness was permanently red until it was forced into its
own DAG, [ADR-0039](../adr/0039-observation-signals-own-dag.md)).

What this test asserts is that fallback's **time bound**: delay is allowed, permanent absence
is not. The window is **2 to 50 days** — the lower bound **must exceed one full DAG cycle**
(or the normal self-healing process would itself turn the test red), and the upper bound keeps
the retention edge out of scope (see below).

Why the legitimate transient is so narrow is worth recording, because it is what the lower
bound is derived from: `process.py` writes the ODS row and the quality event in **one
transaction**, both timestamped `func.now()`.

> ⚠️ That equality holds **only for `initial_evaluation`**. Proposal B's promotion events are
> stamped `event_at = now()` and land in a **later** partition — so "an order always shares a
> partition with its events" is **wrong**. The accurate statement is: **an order always shares
> a partition with its `initial_evaluation`** (one transaction, one timestamp), while later
> events land later and therefore **expire later than the order**, never orphaning it.
> Expiry symmetry rests on the first half. Guarded by
> `assert_initial_event_shares_order_timestamp`.

The only gap is the two parallel extract tasks reading ODS seconds apart, and that gap is
**self-healing by construction**: skew can only drop rows at the *leading edge*, and the
leading edge is what sets the watermark (destination-derived `MAX(partition_id)`), so the
watermark can never advance past a dropped row and the next `>=` pass necessarily re-fetches it.

### Why `warn`, and why that is the final answer

**Severity encodes the correct operational response, not how confident we are.** The criterion
is the one already used by `assert_product_attributes_stable`: error means "our own SQL is
wrong", warn means a signal originating elsewhere.

1. **When it fires, Gold is not wrong.** The fallback guarantees delay, never dirty data — what
   is missed is a possible promotion (a false negative in the quality pipeline), not corruption
   in Gold. **Blocking authority exists to stop bad data reaching downstream, and there is no
   bad data here to stop.**
2. **It is not clearable by a re-run** (it needs a manual backfill). The DAG runs layered
   `dbt build` with `retries=0`, so error severity means every subsequent day's schedule fails
   until a human intervenes — **halting today's correct fresh data over an attribute that has
   already been missing for three days.** Same reasoning as the DAG header's point ⑤:
   deterministic failures should not be retried.
3. **The case where blocking *is* right is already blocked upstream**: both `*_matches_staging`
   tests are error severity and fire first for row loss inside the 7-day window. This test only
   adds value where those missed it — by definition the older, less urgent cases.

> ⚠️ The cost is **visibility**: warn → task succeeds → `on_failure_callback` never fires, and
> the signal lives only in the dbt log. The answer to that is a notification path, **not
> blocking authority** — [ADR-0039](../adr/0039-observation-signals-own-dag.md) already settled
> this question: observation signals get their own path rather than buying visibility by
> blocking the main line.
>
> And this project has **no real notification channel by decision, not by omission**:
> [PORTFOLIO_SCOPE #7](../PORTFOLIO_SCOPE.md) — there is no on-call target. Pointing a notifier
> at a connection that does not exist behaves as "red → callback fires → it raises → nobody
> receives anything", and **believing you have alerting when you don't is more dangerous than
> plainly having none**. So this test's visibility ceiling is the dbt log and
> `run_results.json` — **known, deliberate, and unchanged by the addition of this test**.

### The 50-day upper bound: a portfolio limitation

Partition expiry on the two tables runs as **independent background jobs**; even with the same
expiry date, they are not guaranteed to take effect at the same moment. If the events side
expires first, that batch of orders instantly becomes "order present, event absent" at an age
far beyond the lower bound — **a false positive against undamaged data**.

The BQ sandbox (billing not enabled) hard-caps partition expiry below 60 days, so this edge is
reached **every 60 days**. With billing enabled and 1825-day retention it sits five years out,
which is effectively never.

> This is a mark left by a **portfolio limitation**, but the upper bound itself is a **general
> solution**: any table with partition expiry has this edge. ⚠️ The bound and the retention
> policy are **maintained as a pair** — lowering retention requires lowering this bound, or the
> window collapses to empty, and **an empty window means the test silently does nothing and is
> green forever**, which is worse than not having it.

No coverage is lost: a row permanently missing its event is checked every single day while it
is between 2 and 50 days old.

---

## 7. Fixtures and conventions

- `asyncio_mode=auto` replaces manual `asyncio.run()`.
- A `reset_limiter` fixture eliminates cross-test rate-limit counter contamination.
- Auth is bypassed via `dependency_overrides`, so non-auth tests need not attach a header per request.
- `tests/helpers.py` holds mock factories and test data; `tests/conftest.py` holds shared fixtures.

---

## 8. Related

- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — E2E tests and `check_migration_drift.py` in CI, both deferred
- [transformation](./transformation.md) — the dbt test suite
- [orchestration](./orchestration.md) — where the dbt tests run
