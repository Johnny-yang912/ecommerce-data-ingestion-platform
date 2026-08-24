# ADR-0039: Observation signals each get their own DAG

**English** | [繁體中文](../../zh-TW/adr/0039-observation-signals-own-dag.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

`dbt source freshness` was first added as a side-channel task inside the main pipeline DAG, with its failure configured not to block downstream. That is not enough, and the reason is a property of Airflow rather than of the check:

> **A DAG run's state is the aggregate of its tasks.** An expected-red leaf task leaves `orders_analytics_daily` permanently failed — so "main pipeline success rate" becomes worthless, and real failures are buried under noise that is red every single day.

So the principle has to go one step further.

## Decision

**An observation signal has neither the authority to block downstream nor the authority to pollute another DAG's success rate.** Each gets its own DAG, so that each DAG's red means exactly one thing:

| DAG | Red means | Where to look |
|---|---|---|
| `seed_demo_daily` | Nothing is getting in | API, the seeding script |
| `raw_pending_watch` | Rows reach Raw but nobody claims them | redis / worker / beat |
| `orders_analytics_daily` | The pipeline is broken | extract or dbt |
| `source_freshness_watch` | staging was not moved forward | the watermark and extract |

Guarded by `tests/test_dags.py::TestFreshnessIsolation`: if any DAG that produces real output picks up `dbt source freshness`, the test goes red.

## Three timelines, one hop each

The signals do not overlap, deliberately — merged, a single red would stand for two pipeline segments:

| Timeline | Which hop | Watched by |
|---|---|---|
| `raw.received_at` | Upstream + API: can orders get in? | OTel (absent alerting not written — see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)) |
| `raw.received_at` → `ods.received_at` | Dispatch: can workers claim them? | `raw_pending_watch` |
| `ods.received_at` in BQ staging | extract: did it reach the warehouse? | `source_freshness_watch` |

**`source_freshness_watch` runs at 08:00 Taipei because it is a backstop.** If extract reports success but moved nothing, the Hard Gate judges yesterday's partition and passes, `dbt test` is green — and this is the only thing that speaks up before someone opens the report at 09:00.

Its 26h/50h thresholds are `24 + 2` and `48 + 2`: one **loading cycle** plus two hours of grace. The source is the loading cadence, not the ingestion cadence — staging is pushed once a day, so data is up to 24 hours old by design and a threshold below 24h would go red before every extract. Sampled at 08:00, the healthy value is ~13h and one missed cycle is 37h, so 26h sits in the middle with ~10 hours of margin either side.

## `raw_pending_watch`: the probe that must not import the write path

The dispatch probe derives its alert threshold from the recovery path's own settings rather than hardcoding a number. It originally imported those constants from `process.py` — and thereby inherited **the entire write path's dependency tree**.

When OTel was added, `process.py` gained `from telemetry import ...`, and the probe died at `ModuleNotFoundError: No module named 'opentelemetry'` **before checking anything**, because the analytics venv image had not been rebuilt (ADR-0035).

> **A single shared constant had coupled a read-only probe to a code path it never executes.**

Fixed by extracting the constants into `recovery_policy.py` — a module with zero third-party dependencies — and pinned by `tests/test_script_deps.py` so it does not rely on anyone remembering.

**⚠️ One easy-to-get-wrong criterion.** "A Raw row with no matching ODS row" **cannot** be the definition of a fault: `duplicate` and `error` are correct terminal states that produce no ODS row, so that definition would alarm on every duplicate order. `pending` age is the clean signal.

## Consequences

**Every red is diagnostic.** The DAG that is red tells you which hop to look at, before you read a single log line.

**A signal cannot be silenced by a noisy neighbour**, and cannot silence one.

**The cost is more DAGs to run and look at** — six rather than two — and the discipline that each new signal asks "which hop does this cover?" before it is placed.

## Alternatives considered

**Side-channel task with `trigger_rule` tricks.** The original attempt. Downstream is unblocked and the DAG's aggregate state is still ruined.

**One monitoring DAG with all the checks.** Restores exactly the problem: one red for four different meanings.

**Alert from the metrics backend instead of from DAGs.** The right long-term answer, and it needs alert rules that cannot be written meaningfully without real traffic — see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

## Related

- [ADR-0020](./0020-partition-on-received-at.md) — why each timeline covers exactly one hop
- [ADR-0035](./0035-two-venvs-dependency-isolation.md) — the rebuild requirement behind the probe incident
- [ADR-0042](./0042-failure-notification-response-not-task.md) — what happens when one of these goes red
