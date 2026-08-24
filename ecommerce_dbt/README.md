# ecommerce_dbt — Order Analytics Transformation Layer

**English** | [繁體中文](./README-TW.md)

The dbt project for this pipeline. **This file is the entry point for working in this directory** — the design reasoning lives one level up.

| You want | Read |
|---|---|
| How the layers work and why they are shaped that way | [docs/en/design/transformation.md](../docs/en/design/transformation.md) |
| Why a specific decision was made | [ADR-0043 – 0049](../docs/en/adr/README.md) |
| What to do when a build goes red | [runbooks/dbt-ops](../docs/en/runbooks/dbt-ops.md) |
| Adding or removing a column | [runbooks/schema-change](../docs/en/runbooks/schema-change.md) |
| What each test guards | [design/testing §6](../docs/en/design/testing.md) |

---

## 1. Scope

This layer owns **T** only. Extraction into BigQuery staging is `extract_ods_to_bq.py`'s job ([design/cloud-layer](../docs/en/design/cloud-layer.md)); scheduling is Airflow's ([design/orchestration](../docs/en/design/orchestration.md)).

```
staging.orders  ──►  stg_*  ──►  int_*  ──►  dim_*/fct_*  ──►  rpt_*
staging.quality_events ──┘        ▲
                                  └── blocking happens here, and only here
```

---

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
      project: <your-gcp-project-id>   # the real ID stays out of the repo
      dataset: dbt_dev
      location: US                     # all datasets consistently in US
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1                   # adapter-level retry — see ADR-0038
```

> ⚠️ **Airflow uses a different profile**, in `orchestration/dbt_profiles/`, pointed at by `DBT_PROFILES_DIR`. It is deliberately not placed here: dbt's lookup puts the working directory ahead of `~/.dbt`, so putting it in this directory would make a local `dbt run` consume it and fail on unset environment variables. → [ADR-0041](../docs/en/adr/0041-profiles-yml-structure-vs-values.md)

### Common commands

```bash
dbt deps                                        # install packages (dbt_utils)
dbt run    --select stg_orders                  # build the model (incremental)
dbt run    --select stg_orders --full-refresh   # full rebuild
dbt test   --select stg_orders                  # run tests, incl. the Hard Gate
dbt source freshness                            # source freshness
dbt build  --select stg_orders                  # run + test together
```

> ⚠️ **Never split `dbt build` into a separate `dbt run` and `dbt test` in the DAG.** That makes `int_`'s upstream *"staging's run"* instead of *"staging's test"*, and the Hard Gate silently stops blocking while dirty data flows into Gold. Pinned by `tests/test_dags.py::test_dbt_never_splits_run_and_test`. → [ADR-0040](../docs/en/adr/0040-layered-dbt-execution.md)

---

## 3. Layers and naming

| Prefix | Grain | Responsibility | Quality requirement |
|---|---|---|---|
| `stg_` | source | 1:1 mapping, rename, cast, dedup. **No business logic** | same as ODS — all rows kept, including dirty |
| `int_` | source | joins, derived fields, **the blocking point** | only effectively-clean rows pass; the rest → quarantine |
| `dim_`/`fct_` | star schema | dimensions and facts for flexible analysis | cleanest layer — no dirty rows present |
| `rpt_` | fixed | pre-aggregations for BI | same as Gold |

**12 models**: `stg_orders` · `stg_quality_events` · `int_orders` · `int_orders_quarantine` · `int_order_items` · `dim_customer` · `dim_product` · `fct_orders` · `fct_order_items` · `rpt_quality_events_daily` · `rpt_quality_backlog` · `rpt_sales_daily_by_category`

---

## 4. Two things to know before editing

**① `int_orders` and `int_orders_quarantine` share a byte-identical CTE block**, fenced with `═══` markers, deliberately duplicated rather than shared. Changing either one means walking the **seven-item alignment checklist** first — see [runbooks/dbt-ops](../docs/en/runbooks/dbt-ops.md).

The one people miss: dropping the `coalesce(..., false)` makes a row **vanish from both tables at once**, silently, because `FALSE OR NULL = NULL` and `WHERE NOT NULL` is also NULL.

**② `assert_orders_split_is_partition` must never be downgraded or excluded.** It is the only automated safety net for that duplication. → [ADR-0045](../docs/en/adr/0045-int-effective-state-duplication.md)

---

## 5. Dependencies

- dbt-core **1.11** / dbt-bigquery **1.11**
- `packages.yml`: `dbt-labs/dbt_utils >=1.1.0,<2.0.0` (resolves to 1.4.1)

> ⚠️ If `dbt_packages/` turns up empty and `dbt deps` reports `not a gzip file`, an editor extension is probably running `dbt deps` in the background and hitting a rate limit. → [incidents/2026-08-dbt-deps-429](../docs/en/incidents/2026-08-dbt-deps-429.md)

---

## 6. Status

Everything above is built. What is designed but deliberately not enabled — scenario-specific `int_orders_*`, SCD2 `dim_customer`, incremental `rpt_sales_*`, monetary exposure measures — is in [STATUS](../docs/en/STATUS.md) and [PORTFOLIO_SCOPE](../docs/en/PORTFOLIO_SCOPE.md), each with its trigger.
