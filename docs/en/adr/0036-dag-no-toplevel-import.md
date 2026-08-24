# ADR-0036: DAG files must not import project modules at top level

**English** | [繁體中文](../../zh-TW/adr/0036-dag-no-toplevel-import.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

`config.py` instantiates `Settings` at import time, and `db_url` is mandatory — a missing value raises immediately (ADR-0008). That fail-fast behaviour is correct for the API. It is dangerous in a DAG file.

The dag-processor re-parses every DAG file every few dozen seconds. If a DAG file imports a project module at top level and the parsing process lacks `DB_URL`, the result is **not a failed task**:

> **The entire DAG disappears from the UI.**

There is no red light to look at. A DAG that is not there cannot fail, cannot alert, and cannot be noticed by anyone who is not already looking for it. That is strictly worse than failing.

Airflow 3 makes this sharper, not softer: DAG parsing runs in a **separate dag-processor process** whose environment is distinct from task execution. A variable present for tasks is not necessarily present for parsing.

## Decision

**DAG files import no project module at top level.** Every task is a `BashOperator`, which pushes all imports to task execution time — where a missing environment variable produces a failed task with a traceback, which is a visible thing.

## Consequences

**A configuration problem becomes a red task instead of a vanished DAG.** The failure stays inside the failure-reporting system.

**DAG structure becomes CI-testable.** `tests/test_dags.py` parses the DagBag **with no database and no project environment variables at all** — 52 tests in a dedicated workflow. That is only possible because the DAG files are dependency-free.

The separate workflow is itself deliberate: Airflow's install is heavy and pins many package versions, and folding it into the main test job would destroy that job's "mock DB, done in seconds" property.

**The cost is that DAG files cannot share Python helpers with the project.** Anything a DAG needs must be passed as a command-line argument or read from the environment inside the task.

**One exception exists and it proves the rule.** `orchestration/dags/_notify.py` is imported by DAG files, and is bound by the same discipline — it imports nothing from the project. Its `_` prefix is also load-bearing: `tests/test_dags.py` globs `*.py` and excludes underscore-prefixed files before asserting "file count ≤ DAG count". Without the prefix, a module that produces no DAG would fail that assertion.

## Alternatives considered

**Give the dag-processor a `DB_URL`.** Would work today and hides the hazard rather than removing it — the DAG remains one environment change away from disappearing, in a process whose environment nobody thinks about.

**Make `Settings` lazy.** Would remove the fail-fast property that ADR-0008 exists for, trading a loud startup failure for a late one during a request.

**`PythonOperator` with imports inside the function body.** Achieves the same deferral and is more fragile — the discipline is invisible, and one refactor that hoists an import to the top of the file silently reintroduces the hazard. `BashOperator` makes it structurally impossible.

## Related

- [ADR-0008](./0008-config-boundary.md) — the fail-fast behaviour that makes this necessary
- [ADR-0035](./0035-two-venvs-dependency-isolation.md) — the other half of parse-time isolation
- [ADR-0042](./0042-failure-notification-response-not-task.md) — the one module DAG files do import
