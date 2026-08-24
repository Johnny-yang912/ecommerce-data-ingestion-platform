# 2026-08-11 — Full-compose rebuild and the v4 flow-back

**English** | [繁體中文](../../zh-TW/verification/2026-08-11-full-compose-rebuild-v4.md)

---

## What was being verified

The environment moved fully into compose and the dataset was rebuilt from zero. **Does the whole system come up from nothing, and does a second rule-loosening cycle behave the same as the first?**

## Environment

9 containers — db / redis / api / worker / beat + four Airflow services. Dataset wiped and rebuilt. 3,015 rows, 265 in quarantine. 2026-08-11.

## Observed — infrastructure

| Item | Result |
|---|---|
| `alembic upgrade head` from zero | all **7 migrations** passed — a path a long-lived dev database never exercises |
| Service health | 9 containers all healthy |
| Airflow → `api:8000` / `db:5432` | both reachable, **reading the same database** (`ods=8`) |
| BQ rebuild after a full wipe | `create_dataset` / `create_table(exists_ok=True)` rebuilt everything with partitioning and `require_partition_filter` intact — **zero manual DDL** |
| Main DAG | **7/7 tasks success, ~2.5 minutes** end to end |
| `source_freshness_watch` | both sources **PASS** — flipped from "expected red" to "expected green" |

### The landed-rows gate, both directions

With `worker` stopped, 3 records were posted:

| | ODS | exit code |
|---|---|---|
| without `--require-landed-pct` (old behaviour) | 0 rows | **0** ← silent success, exactly what it must prevent |
| `--require-landed-pct 0.9` | 0 rows | **1** ← caught |

After restarting `worker`, all 13 `pending` rows were re-dispatched by `scan_and_dispatch` — self-healing verified alongside.

## Observed — v4 flow-back

Target: `customer_name` soft cap **100 → 150**.

| Step | Result |
|---|---|
| Dry-run | `candidates=265 promoted=3 would_write=3` |
| Commit | `written=3`; `quality_events` = 3015 `initial_evaluation@v3` + 3 `promotion@v4` |
| **Bounded writeback** | ODS fingerprint **identical before and after** (3015 rows, 265 dirty) |
| Idempotency | second run: `promoted=0 written=0 unchanged=265` |
| Flow-back | `int_orders` +3, quarantine 265→262, `fct_orders` +3, `promotions` 0→3 |
| Row-level check | all 3 show `fct_orders=1 / quarantine=0` |
| Control group | `customer_name` 157/164/176/188/199 and 5 `city` rows **all stayed quarantined** |

> The control group formed **naturally out of the same injector** — `_dirty_field_too_long` spreads lengths over 110–200 and targets `city` half the time — unlike v3, which needed one prepared separately. The boundary is tighter too: **146 promotes, 157 does not.**

## Two inferences overturned ⭐

### ① `--expect-rule-version` covers less than assumed

Measured **before** rebuilding the images: `api`/`worker` reported `v3 {'customer_name': 100}` while Airflow reported `v4 {'customer_name': 150}` — and **`--expect-rule-version v4` passed.**

The guard only compares the version **inside its own process**. It holds only if the whole system has a **single code-delivery mechanism**, and this compose topology breaks that premise:

```
api / worker / beat   code BAKED INTO THE IMAGE    needs a build
Airflow containers    bind mount ./:/opt/project    immediate
```

Handling: step 3 of [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md).

### ② The directionality of the candidate source was never written down

The DAG's header recorded *"re-evaluation writes to PG; an extract is needed for flow-back into Gold"*. **The reverse holds too**: candidates are read from BQ, so the data must reach BQ **first**.

The first dry-run returned `candidates=26 / would_write=0` — **not because the rule had not taken effect**, but because BQ still held the pre-accumulation state. That symptom is indistinguishable from a broken program without knowing this.

Handling: step 4 of the same runbook.

## Measured in passing

- **Unpausing a DAG immediately creates a scheduled run.** `staging.orders` therefore held 398 = 199×2 rows while `stg_orders` held exactly 199 — an accidental live confirmation that append-only tolerance plus `stg_` dedup works as designed.
- **Jinja template errors surface only at runtime.** DagBag parsing was clean, `dags list` normal, every structural test green — yet the task failed in **0.16s**. All three variants hit (nested `{{ }}`, an f-string escaping `}}` down to `}`, and `data_interval_start` being absent in manual runs) are catchable **only by actually rendering the template**, so `tests/test_dags.py` gained render tests.
- **A cron `data_interval_start` is the *previous* fire point.** Using it as a date seed would make each day's first slot pick up **yesterday's** value, breaking the single-dirty-rate-per-day invariant. Switched to `dag_run.run_after`.

## Related

- [2026-08-05-proposal-b-v3](./2026-08-05-proposal-b-v3.md) — the first cycle
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md) — the two warnings this record produced
- [ADR-0032](../adr/0032-bounded-writeback.md)
