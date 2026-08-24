# ADR-0030: Proposal B — event-driven re-evaluation without re-running the pipeline

**English** | [繁體中文](../../zh-TW/adr/0030-proposal-b-event-driven-reevaluation.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Data Quality — re-evaluation |

---

## Context

Quality rules change. The `age` cap moved 120 → 130; the `customer_name` soft cap moved 100 → 150. Each loosening means some records quarantined under the old rules are legitimate under the new ones.

The naive fix is to re-run the pipeline: re-read Raw, re-clean, re-write. It cannot be done here, and should not be.

**It cannot**, because ODS is immutable (ADR-0002) — there is nothing to re-write. **It should not**, because re-running would destroy the audit trail: the record would no longer show that it was ever quarantined, or under which rule version.

## Decision

**Re-evaluation produces events, not mutations.** `reevaluate_quality.py` reads candidates, re-runs the current rules against them, and appends to `quality_events` when — and only when — the verdict actually changed.

Four decisions inside that:

**Candidates come from BigQuery's `int_` layer.** The same effective-quality-state definition the Row Filter uses (ADR-0029), so the producer and the consumer cannot disagree about who is quarantined.

**State is decided against PostgreSQL, not BigQuery.** Idempotency must not rest on a mirror that expires — the sandbox's 60-day partition expiry means BigQuery can lose history that PostgreSQL still has. Reading candidates from a mirror is fine; deciding whether an event already exists is not.

**Events are appended only on an actual state change.** "Re-evaluated and still quarantined" writes nothing. This makes the event table **its own idempotency gate**: running the job twice produces the same result as running it once, with no separate dedup key.

**Dry-run is the default.** Committing is an explicit flag, and the DAG's `schedule=None` (ADR-0037) means it only ever runs when a human triggers it.

## Reproducibility: two guards

Re-running a rule months later must reach the same verdict for the same reason. Two mechanisms enforce that:

**`business_clean` gained an `as_of` parameter.** Time-dependent rules (an age derived from a date, a window that has since closed) would otherwise produce a different answer purely because the clock moved. `as_of` pins evaluation to the record's own time.

**`NON_REPRODUCIBLE_CODES` blocks "the evidence disappeared" promotions.** Some error codes cannot be re-checked after the fact — the thing that justified the error is no longer available to inspect. Without this guard, absence of evidence would be read as evidence of cleanliness, and the record would be promoted **for the wrong reason**. Those codes are excluded from promotion entirely.

> A re-evaluation that cannot see why a record failed must not conclude that it did not fail.

## Consequences

**Rule evolution becomes a routine operation with a runbook**, exercised end to end four times: v3 (15 records promoted), v4 (3), and a back-to-back v2→v3 (16) and v3→v4 (15) during a fixture rebuild. All four idempotent, ODS never modified, control group left quarantined.

**Divergence between ODS and the warehouse becomes explainable rather than mysterious.** `dq_rule_version` records the truth at ingestion; `quality_events` records the truth after evolution. "Why does ODS say dirty and Gold say clean?" has a queryable answer.

**Tightening is symmetric but unexercised.** `re_quarantination` exists in the state machine and no scenario has produced one yet, so `rpt_quality_events_daily.re_quarantines` is still 0.

**`permanently_rejected` can only come from a human.** The automated task never writes it and never overrides it — enforced at the write target in PostgreSQL, not merely by a filter on the BigQuery side.

## Alternatives considered

**Re-run the pipeline from Raw.** Destroys the audit trail and contradicts the immutable anchor.

**Update `has_clean_error` in ODS.** Violates bounded writeback (ADR-0032); same loss of history.

**Schedule it automatically.** Rejected — a rule change is a human decision with a review step, and its blast radius should be inspected in dry-run before it is committed (ADR-0037).

## Related

- [ADR-0029](./0029-effective-quality-state.md) — the consumer, built first
- [ADR-0031](./0031-rule-versioning-quality-events.md) — the event log this writes to
- [ADR-0032](./0032-bounded-writeback.md) — the constraint that forces the event-based shape
- [ADR-0037](./0037-catchup-false-structural.md) — the DAG's manual-trigger semantics
