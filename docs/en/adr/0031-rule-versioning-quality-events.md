# ADR-0031: Rule versioning plus an append-only `quality_events` state machine

**English** | [繁體中文](../../zh-TW/adr/0031-rule-versioning-quality-events.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — audit |

---

## Context

`has_clean_error` records *that* a record failed. It does not record *under which rules*, or *when the verdict changed*.

Without that, a divergence between ODS and the warehouse is unanswerable:

```
ODS: has_clean_error = TRUE     Gold: the record is present and clean
→ "Why are these different?"    → no answer
```

A boolean has no history. Anything that needs to explain how a judgement evolved needs something else.

## Decision

Two mechanisms, together:

**A rule version constant.** `DQ_RULE_VERSION` in `clean.py` (currently `v4`), bumped on every rule change and paired with a git tag recording what changed. Each ODS row stores the version it was evaluated under, in `dq_rule_version` — **written once at ingestion and never touched again.**

**An append-only event log.** `quality_events` records every state transition:

```
initial_evaluation
  ├── passes all rules          → clean
  └── has_clean_error = TRUE    → quarantined

quarantined / re_quarantined
  ├── re-evaluation passes      → promoted               (promotion)
  ├── re-evaluation fails       → no event written
  └── written off by a human    → permanently_rejected   (rejection)

promoted
  ├── stricter rules now fail   → re_quarantined         (re_quarantination)
  └── still passes              → no event written

permanently_rejected            ← terminal; no outgoing edge
```

Three properties of this machine are decisions in their own right:

**"No event written" is deliberate.** Appending only on an actual change is what makes the log its own idempotency gate (ADR-0030).

**`permanently_rejected` can only come from a human.** The automated task never writes it and never overrides it, enforced at the PostgreSQL write target rather than by a downstream filter.

**`re_quarantination` was added after the fact**, and adding it broke nothing — because `rpt_quality_events_daily` counts by `to_state` rather than `event_type`, and the `int_` layer's `CASE` folds `re_quarantined` into `else 'quarantined'`. That the extension was safe is a property of how the consumers were written, not luck.

## Consequences

**Divergence becomes auditable rather than mysterious:**

```
ODS: has_clean_error = TRUE, dq_rule_version = 'v1'    ← truth at ingestion
quality_events: promoted under 'v2' on 2026-03-01      ← truth after evolution
→ explained, with a timestamp and a rule version
```

**A record's entire quality history is queryable**, not just its current state.

**Changing a cleaning rule is not a small change.** It requires bumping `DQ_RULE_VERSION` and running a re-evaluation — which is precisely why ADR-0006 chose the smaller fix for the NUL poison pill rather than the philosophically consistent one.

**The cost is one column, one table, and a discipline.** The discipline is the fragile part: nothing automatically detects a rule change that forgot to bump the version. It is held by review and by the deployment runbook.

## Alternatives considered

**A mutable `quality_status` column.** One row per record, updated on change. Smaller and faster to query — and it destroys the history, which is the entire point.

**Rely on git history for rule versions.** Git records when the *code* changed. It cannot tell you which version a *given row* was evaluated under, which is the actual question.

**Version rules per-rule rather than globally.** Finer-grained and more honest in principle. Rejected as premature: a global version is sufficient to explain any divergence, and per-rule versioning multiplies the bookkeeping without a demonstrated need.

## Related

- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — the mechanism that writes to this log
- [ADR-0029](./0029-effective-quality-state.md) — the mechanism that reads it
- [ADR-0033](./0033-historical-metrics-never-rewritten.md) — what append-only buys downstream
- [ADR-0006](./0006-nul-byte-fast-fail.md) — a decision shaped by the cost of a version bump
