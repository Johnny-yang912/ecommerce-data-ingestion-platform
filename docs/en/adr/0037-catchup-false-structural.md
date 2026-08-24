# ADR-0037: `catchup=False` is structural, not a convenience

**English** | [繁體中文](../../zh-TW/adr/0037-catchup-false-structural.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

`catchup=False` is often set to avoid a stampede of backfill runs when a DAG is first deployed — a convenience. Here it is a statement about what the pipeline *is*.

**This pipeline's watermark is destination-derived** (Approach A: `MAX(partition_id)` from staging — ADR-0023), not execution-date-derived. A backfill run for 2026-07-01 still extracts "the increment **as of now**", with no relation to its logical date.

So N catch-up runs do the exact same thing N times, plus N redundant load jobs. They are not backfills; they are repetitions.

## Decision

`catchup=False` on every DAG, plus `max_active_runs=1`.

**`max_active_runs=1` is correctness, not politeness.** Concurrent runs would read the same `get_watermark()` value — harmless in itself, since `stg_` dedup absorbs it — but concurrent dbt `insert_overwrite` on the same partitions would overwrite each other's output.

**This is not a date-partitioned, backfillable DAG, and making it one is a deliberate non-goal.** Genuine backfillability would require slicing on `received_at >= data_interval_start AND < data_interval_end` — Airflow's idiomatic idempotent shape. That right-hand bound would **cut off late-arriving rows**, directly contradicting the "`>=`, rather re-fetch than miss" semantics of ADR-0023.

**Daily, not hourly.** Approach A's precision is capped by DAY partitioning, so an hourly schedule re-extracts the entire current day on every run. Going hourly requires HOUR partitioning or Approach B first — a separate decision, and one ADR-0019 has already declined.

## The other half: `schedule=None` on the Proposal B DAG

`dq_reevaluation` has no schedule at all, and that is the same kind of statement.

Proposal B fires on a **rule loosening** — a human deploy event, not a period. With unchanged rules, re-evaluation necessarily reproduces the previous verdict (same values, same rule version), so it emits no events while full-scanning the entire quarantine backlog. Scheduling it daily would be **364 days of wasted work for one day of effect**.

> **Schedules belong on things that change by themselves. Rules do not change by themselves.**

Three supporting choices:

- **Dry-run by default.** `quality_events` is append-only; a bad write cannot be deleted, and a manual-trigger UI makes it easy to click straight through.
- **`expect_rule_version` as a guard.** The most likely accident is triggering against an environment that has not deployed the new rules yet — writing a batch of events stamped with the wrong version that cannot be revoked.
- **Trigger the main DAG afterwards, but only on `commit`.** Re-evaluation writes only PostgreSQL's `quality_events`; flowing back to Gold still needs `extract_quality_events` and an `int_` rebuild. Without that step the observed state is "I ran Proposal B and nothing happened" — the state most easily mistaken for a broken program.

Every parameter follows *omit the flag when empty*: defaults live **only in the script**. Keeping a second copy in the DAG is exactly how the two drift apart.

## Consequences

**Deploying or renaming a DAG does not trigger a burst of pointless runs.**

**The pipeline's non-backfillability is recorded rather than left to be re-derived.** A future reader who wonders "why isn't this backfillable?" finds the answer here instead of reconstructing it from the watermark implementation.

**The cost: a missed window is not automatically recovered.** If the machine is off at 22:30, that day's extract simply does not happen. The next day's run picks up everything since the watermark, so no data is lost — but the gap is real, and it is `source_freshness_watch`'s job to notice it (ADR-0039).

## Related

- [ADR-0023](./0023-watermark-approach-a.md) — the destination-derived watermark this follows from
- [ADR-0019](./0019-batch-load-not-streaming.md) — why daily rather than hourly
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — the job whose schedule is `None`
- [ADR-0039](./0039-observation-signals-own-dag.md) — what notices a missed window
