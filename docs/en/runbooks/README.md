# Runbooks

**English** | [繁體中文](../../zh-TW/runbooks/README.md)

Operational procedures. **What to do**, not why — the why lives in the [ADRs](../adr/README.md) and [design documents](../design/).

---

## Symptom → runbook

| What you are seeing | Open |
|---|---|
| Bringing the stack up for the first time, or after a reboot | [airflow-startup](./airflow-startup.md) |
| **A DAG should have run and didn't — and nothing is red** | [airflow-silent-stall](./airflow-silent-stall.md) |
| The analytics DAG has failed for several days in a row | [dag-failure-recovery](./dag-failure-recovery.md) |
| A quality rule is being loosened and quarantined rows should come back | [proposal-b-rollout](./proposal-b-rollout.md) |
| A record must be written off permanently | [quarantine-writeoff](./quarantine-writeoff.md) |
| Records stuck in `pending` or `processing`; the broker is down | [queue-ops](./queue-ops.md) |
| A dbt model needs rebuilding, or `int_` was changed | [dbt-ops](./dbt-ops.md) |
| A column is being added to or removed from ODS | [schema-change](./schema-change.md) |

---

## Before you touch anything

**Two rules that apply to every procedure here:**

1. **Do not edit `raw.status` by hand.** The state machine has recovery paths for every stuck state; editing it directly bypasses the invariants those paths rely on. [queue-ops](./queue-ops.md) explains what to do instead.

2. **Do not modify ODS.** ODS is the immutable anchor. Any correction goes through `quality_events` ([ADR-0032](../adr/0032-bounded-writeback.md)).

---

## What each red light means

The DAGs are deliberately separate so that each red means exactly one thing ([ADR-0039](../adr/0039-observation-signals-own-dag.md)):

| DAG red | Means | Look at |
|---|---|---|
| `seed_demo_daily` | nothing is getting in | API, seeding script |
| `raw_pending_watch` | rows reach Raw, nobody claims them | redis / worker / beat → [queue-ops](./queue-ops.md) |
| `orders_analytics_daily` | the pipeline is broken | extract or dbt → [dbt-ops](./dbt-ops.md) |
| `source_freshness_watch` | staging was not moved forward | the watermark and extract |

**No DAG red at all, but nothing ran** → [airflow-silent-stall](./airflow-silent-stall.md). That is the failure mode with no built-in alerting.
