# ADR-0042: Failure notification states the response, not the task name; the channel is left blank

**English** | [繁體中文](../../zh-TW/adr/0042-failure-notification-response-not-task.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Layer** | Orchestration |

---

## Context

A default failure notification says `Task dbt_intermediate in DAG orders_analytics_daily failed`. The person receiving that at 07:00 still has to work out what it means and whether it matters.

The information they need — *"the analytics pipeline is broken; tomorrow morning's report is stuck on yesterday"* — existed, in each DAG file's docstring. **The person handling an incident does not read docstrings.**

The second problem is the channel. Pointing a notifier at a Slack connection that does not exist produces: task goes red → callback fires → callback raises → Airflow logs it → **nobody receives anything**.

> **Believing you have alerting when you do not is far more dangerous than plainly having none.**

## Decision

**The message states the response.** Each of the four scheduled DAGs carries an `on_failure_callback` whose text says what is now broken and what it means downstream — the docstring content, moved to where it is read.

**Attached at task level, not DAG level.** Downstream `upstream_failed` tasks do not fire the callback, so a broken seven-task chain sends **exactly one message**, and that message names the task that actually broke.

**The transport defaults to a log line.** A real channel is one `NOTIFY_WEBHOOK_URL` away. Deliberately not a notifier pointed at a connection that does not exist.

**Every message carries `channel=`.** Seeing `channel=log` tells you immediately that nobody was notified. The absence of alerting is itself reported.

**`_deliver()` is the only function that knows how to send.** Everything else knows only what to send. The webhook payload is `{"text": ...}` — Slack's Incoming Webhook shape, which most services accept; switching to Discord (`content`) or ntfy (plain body) changes one line.

**Environment variable, not an Airflow Connection.** A Connection would let Airflow mask secrets in logs, which is genuinely better — but it binds this module to a specific provider's notifier, and interchangeability is the whole point of the seam. The mitigation: **the URL never enters a log**, not even on failure, where only the status code is recorded. With that, the masking benefit approaches zero.

## ⚠️ Coverage: "ran and failed" only

`on_failure_callback` requires a task run that actually happened. Three things are therefore invisible to it:

| Not covered | Why |
|---|---|
| **Should have run, didn't** | No run means no failure. Airflow 3 removed SLAs (the `sla` parameter remains in `BaseOperator`'s signature — do not rely on it) |
| **Machine powered off / network down** | The callback lives on the same machine as the thing it watches |
| **`warn`-level results** | `dbt source freshness` warn exits 0; the task is green |

All three need cloud-side absent alerting, which is deferred — see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md). **The delivery seam is ready; the detector is what is missing.**

## Consequences

**A red now arrives with its own interpretation.** No cross-referencing a docstring to find out whether it matters.

**One incident produces one message**, not one per task in the failed chain.

**The gap is stated rather than implied.** `channel=log` in the message, and the coverage table above, mean nobody can mistake this for working alerting.

**⚠️ This module is imported by DAG files**, so it is bound by ADR-0036: it imports nothing from the project. Its `_` prefix is load-bearing for `tests/test_dags.py`'s file-count assertion.

## Alternatives considered

**Point a notifier at Slack now.** Would look complete and deliver nothing, which is the failure mode this decision is organised against.

**DAG-level `on_failure_callback`.** One message per DAG rather than per task — but it does not name the task that broke, and the message would have to be generic across every failure the DAG can have.

**Put the response text in the alerting tool.** Splits the explanation from the code that produces the failure, so they drift. Keeping it in the DAG file means a change to what the task does is in the same diff as a change to what its failure means.

## Related

- [ADR-0036](./0036-dag-no-toplevel-import.md) — the discipline this module is bound by
- [ADR-0039](./0039-observation-signals-own-dag.md) — why each red already means one thing
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — the three uncovered cases and what a real system does
