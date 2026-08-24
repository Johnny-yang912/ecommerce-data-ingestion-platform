# 2026-08-05 — Airflow commissioning, and `--indirect-selection=buildable`

**English** | [繁體中文](../../zh-TW/verification/2026-08-05-airflow-commissioning.md)

---

## What was being verified

Two things, in one session: **does the Airflow image actually work end to end**, and **does `--indirect-selection=buildable` put the cross-layer singular tests where the reasoning said it would?**

## Part 1 — the container, run for real

| Item | Result |
|---|---|
| Image build | Success (`apache/airflow:3.0.0-python3.12` + two isolated venvs) |
| Services | `airflow-db` / `init` / `apiserver` / `scheduler` / `dag-processor` all healthy |
| **DAG parsing** | All 3 loaded; `list-import-errors` → **No data found** |
| analytics venv | `sqlalchemy` / `google.cloud.bigquery` / `structlog` / `pydantic` import fine |
| dbt venv | dbt-core **1.11.12**, dbt-bigquery **1.11.3** |
| env_var profile | `dbt debug` → `Connection test: [OK connection ok]` |
| `source_freshness_watch` | Full DAG run **success**, both sources PASS |
| `dbt_intermediate` | In-container `airflow tasks test` → PASS=27 WARN=1 ERROR=0 |
| `extract_orders` | **FAILED**: `OperationalError: could not translate host name` |
| UI | `http://localhost:8080` HTTP 200 |

### ⭐ The no-top-level-import discipline was validated against a real dag-processor

**The container had no usable `DB_URL`** — the default pointed at a `db` service that was not started — and **all three DAGs still parsed with zero import errors.**

Had the DAG files carried a top-level `from config import settings`, the screen at that moment would have shown:

> **all three DAGs missing from the UI** — not three red tasks, but *nothing at all*.

That is the failure this discipline exists to prevent, and this run is the closest thing to seeing it not happen. [ADR-0036](../adr/0036-dag-no-toplevel-import.md)

### Freshness semantics confirmed incidentally

Run 15 minutes after a data load, both sources **PASSED**. The claim that *"red means you have not fed it lately, not that the pipeline is broken"* is no longer only an argument: **feed it and it goes green.**

### The one failure

`extract_orders` failed on hostname resolution — at the time the business DB ran on the host while Airflow ran in containers. That configuration also produced a subtler hazard; see [2026-08-raw-id-collision-two-ods](./2026-08-raw-id-collision-two-ods.md). Resolved by moving fully into compose ([2026-08-11-full-compose-rebuild-v4](./2026-08-11-full-compose-rebuild-v4.md)).

---

## Part 2 — `--indirect-selection=buildable`

The layered-execution decision could originally **only be reasoned about**. The difference between the three modes was describable, but there was no instance of it.

```
dbt build --select path:models/staging       22 nodes, all stg_ tests
                                             ← assert_orders_split_is_partition is NOT among them
dbt build --select path:models/intermediate  13 of 28 PASS assert_orders_split_is_partition
dbt build --select path:models/marts         assert_fct_orders_complete_projection    PASS
                                             assert_fct_orders_rollup_matches_items   PASS
```

Cross-layer singular tests land **exactly in the layer where all their inputs are fresh** — not fired early in staging (where `int_` is still the previous table and they would go spuriously red), and not skipped entirely the way `cautious` would.

**The reasoning holds.**

### Why the closing full `dbt test` stays

Its value is **not** "catching what was skipped" — nothing was skipped. It is that **it is the only thing that would notice if selector semantics change in a future version.**

That is a different job, and it is why the closing run was not removed once this measurement showed nothing was being missed.

## Related

- [ADR-0040](../adr/0040-layered-dbt-execution.md) — the decision this verifies
- [ADR-0035](../adr/0035-two-venvs-dependency-isolation.md) — the two venvs
- [design/orchestration](../design/orchestration.md)
