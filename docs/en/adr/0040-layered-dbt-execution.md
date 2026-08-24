# ADR-0040: Layered dbt execution, with a full `dbt test` still running at the end

**English** | [繁體中文](../../zh-TW/adr/0040-layered-dbt-execution.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

A single `dbt build` task would work. Splitting it by layer (staging → intermediate → marts → reports) makes the Hard Gate's interception point visible in the UI and allows re-running from the failed layer down.

But per-layer `--select` runs into dbt's **indirect selection** semantics, and the cost lands on this project's most important test — the singular test asserting that `int_orders` and `int_orders_quarantine` form a partition of `stg_orders`:

| Mode | What happens to `assert_orders_split_is_partition` |
|---|---|
| `eager` (default) | Selected during the **staging** task → asserts against a half-rebuilt state (`stg_` fresh, `int_` stale) → **spurious red** |
| `cautious` | Not all parents selected → **never runs** |
| **`buildable`** | Parents must be selected or ancestors of selected → lands in the **intermediate** task with all inputs fresh → **correct** |

Both wrong answers are bad, and they are bad in opposite ways: one cries wolf, the other is silent. The silent one is worse — the documentation describes this test as the only automated safety net for the partition invariant, never to be downgraded.

## Decision

**`--indirect-selection=buildable` on every layered task, and a full `dbt test` closing the DAG.**

The closing run is not redundancy for its own sake:

> **A silently skipped test is far worse than a duplicated one.**

The two runs have different jobs:

- **Per-layer tests are the gate** — they stop downstream builds.
- **The closing run is completeness** — it proves nothing was skipped by a selection subtlety.

## ⚠️ Never split `dbt build` into `dbt run` + `dbt test`

This is the trap that motivated pinning the structure in a test.

Splitting them makes `int_`'s upstream *"staging's **run**"* instead of *"staging's **test**"*. The Hard Gate (ADR-0028) is a test on `stg_orders` — so with the split, **the gate stops blocking anything** while dirty data flows into Gold. Nothing errors. The DAG is green. The gate is decorative.

Guarded by `tests/test_dags.py::test_dbt_never_splits_run_and_test`.

## Consequences

**The interception point is visible in the UI.** A red staging task means the Hard Gate fired; a red intermediate task means the partition invariant broke. That distinction is legible before opening a log.

**Re-running is cheap and targeted** — from the failed layer down, rather than the whole project.

**The subtlety is recorded rather than rediscovered.** `--indirect-selection` is obscure enough that a future maintainer would likely change it without knowing what it protects. This record and the pinning test are what stop that.

**The cost is a longer DAG and duplicated test execution.** Both accepted — the tests are cheap relative to the builds, and the alternative failure mode is silent.

## Alternatives considered

**A single `dbt build` task.** No selection subtleties at all, and no visible interception point, no partial re-run, and a single opaque red for every possible failure.

**`--indirect-selection=eager` plus tolerating the spurious red.** Trains the operator to ignore a red — the same failure mode ADR-0028 rejected for the Hard Gate's whole-table scope.

**`cautious` plus relying on the closing `dbt test`.** The invariant would then only ever be checked *after* marts and reports were already built on top of a possibly-broken partition. The gate has to be upstream of what it protects.

## Related

- [ADR-0028](./0028-hard-gate-per-batch-scope.md) — the gate that the run/test split would silently disable
- [ADR-0029](./0029-effective-quality-state.md) — the partition invariant being asserted
- [ADR-0038](./0038-asymmetric-retries.md) — why these tasks do not retry
