# Testing Strategy

**English** | [繁體中文](../../zh-TW/design/testing.md)

What is tested, where, and — most importantly — **where the tests are blind**.

---

## 1. The layers

| Layer | Count | Where | Needs |
|---|---|---|---|
| Unit + integration (mock DB) | 445 | CI, `ci.yml` | nothing — seconds to run |
| DAG structure | 52 | CI, `dags.yml` | Airflow, no DB, no project env |
| dbt | 93 | in the DAG | BigQuery |
| Manual scripts | 3 | by hand | a real server + real Postgres |

Unit coverage is **100% of the 12 gated modules**, across a **Python 3.10 and 3.12** matrix. Test dependencies are pinned in `requirements-dev.txt`.

> **These counts go stale. Regenerate them rather than trusting them** — the numbers above were last verified 2026-08-24:
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
| CAS claim under real concurrency | `load_test.py --cas-test` |
| `order_id` deduplication | `load_test.py --duplicate` |
| Post-crash recovery | `restart_test.sh` (SIGKILL) |
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
| `load_test.py` | throughput, CAS under real concurrency (`--cas-test`), dedup (`--duplicate`) |
| `restart_test.sh` | `SIGKILL` mid-processing, then recovery of `pending` rows |
| `check_migration_drift.py` | `alembic upgrade head` + `compare_metadata`; non-zero exit on drift |

All three hit a real server and a real PostgreSQL. Their results are recorded in `docs/*/verification/` (stage 4).

---

## 6. The dbt test inventory

93 tests. The full list, because "which test guards what" is not derivable from the model files:

| Test | Target | Severity | Notes |
|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | `has_clean_error` ratio over `stg_orders`'s **latest `received_at` partition** | error @15% | **Hard Gate** — the only test with blocking authority. Per-batch rather than whole-table: a whole-table denominator grows with history and dilutes single-batch anomalies away, and it cannot heal. Can't use `dbt_utils.expression_is_true` (row-level, folded into `WHERE`; aggregates error out) → custom `error_rate_below` uses `HAVING` |
| `monitor_dataset_error_rate` | whole-table ratio on `stg_orders` | warn @10% | A **gauge**, deliberately given no blocking power. Its denominator is a function of retention/backfill policy — sensitive to things other than quality |
| `unique` + `not_null` | `stg_`'s `raw_id`/`id`/`order_id`; `int_`'s `raw_id`/`order_id` | error | `stg_`'s `unique(raw_id)` **is** the dedup check |
| `not_null` | `received_at` / `has_clean_error` / `has_schema_drift` | error | REQUIRED columns |
| source freshness | `staging.orders`, `staging.quality_events` | warn 26h / error 50h | with `filter` to bypass the fuse |
| ⭐ `assert_orders_split_is_partition` | `int_orders` ∪ `int_orders_quarantine` vs `stg_orders` | error | **Partition invariant** — every `raw_id` appears exactly once. The only automated safety net under the duplicated block, guarding checklist items #1–#4. **Never downgrade** |
| `assert_int_orders_no_unpromoted_dirty` | `int_orders` | error | **Gold contract** — no `has_clean_error=TRUE` row that hasn't been promoted. A singular test rather than a column test because it is a **conditional relation between two columns**: `has_clean_error=TRUE` is legal here |
| `accepted_values` | `effective_quality_state` on both `int_` tables | error | The two domains are disjoint (`clean`/`promoted` vs `quarantined`/`permanently_rejected`) — **cross-checks the partition from another angle** |
| `unique_combination_of_columns` + `relationships` | `int_order_items`'s `(raw_id, item_index)`; `raw_id → int_orders` | error | Item-grain uniqueness and lineage integrity |
| ⭐ `assert_fct_orders_rollup_matches_items` | `fct_orders` rollup vs `fct_order_items` aggregates | error | **Rollup consistency invariant.** Compared per order with `is distinct from` — `=` would let "both sides NULL" rows be silently filtered out |
| ⭐ `assert_fct_orders_complete_projection` | `int_orders` (in-window) vs `fct_orders` | error | **Lossless projection** — interception already happened at `int_`, so Gold must not drop a row. An anti-join over an `order_date` window rather than `count = count`, because **the two tables' 60-day clocks hang on different axes** and a count comparison would go flaky daily |
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
