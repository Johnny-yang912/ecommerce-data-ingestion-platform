# ADR-0032: Bounded writeback — warehouse judgements do not flow back into ODS

**English** | [繁體中文](../../zh-TW/adr/0032-bounded-writeback.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — consistency |

---

## Context

Re-evaluation happens downstream: candidates are identified in the warehouse, and the verdict is computed there or by a job reading from there. The result has to go somewhere.

The tempting destination is ODS itself — set `has_clean_error = FALSE` on the promoted record and everything downstream becomes correct with no special handling. No composition logic in `int_`, no `LEFT JOIN`, no `COALESCE` trap.

It would also dissolve the property the entire architecture rests on.

## Decision

**Writeback targets `quality_events` only. ODS is never modified.**

```
❌ Prohibited : warehouse → UPDATE an ODS column
✅ Allowed    : warehouse → INSERT into quality_events
```

`quality_events` is an audit log designed to receive exactly this. ODS is the immutable anchor.

## Consequences

**The anchor stays trustworthy.** "What did this system believe at ingestion time, under which rules?" has a permanent answer. Once ODS becomes writable from downstream, that question can never be answered again for any row — including rows nobody ever wrote back to, because the *possibility* is what destroys the guarantee.

**Divergence between ODS and the warehouse becomes expected and explainable**, rather than a symptom. It has exactly two sources, each handled differently:

| Source | How it is explained |
|---|---|
| Rule-version evolution | `dq_rule_version` + `quality_events` — queryable, with timestamps |
| Scenario-specific models accepting irrelevant errors | Read the model's SQL and its dbt description — static, no runtime tracking table |

The second is a deliberate boundary: scenario repair needs to be *explainable*, not *audited at runtime*. Documentation in SQL is sufficient, and a tracking table would be machinery serving no query anyone asks.

**Cost: every consumer must compose the effective state itself.** That is where the `LEFT JOIN` and the `COALESCE` trap in ADR-0029 come from — real complexity, paid deliberately in exchange for the anchor.

**A class of problem is placed permanently out of reach**, and this is the honest limitation. Neither `force=true` replay nor rule re-evaluation can fix a **value-production defect** — a cleaning bug that corrupted values in already-`processed` records (a sentinel list treating `"na"` = North America as a null, washing a column to NULL across thousands of rows).

Re-evaluation cannot help: its input *is* the corrupted values. Bounded writeback forbids it from writing values even if it could tell. That path needs a different mechanism entirely — a batch correction working from the Raw payload — which is designed as Proposal C and deliberately not built.

> If this boundary were not stated, the promise that "Raw kept verbatim enables rebuilding" would be unbacked. Proposal C is what backs it.

## Alternatives considered

**Update `has_clean_error` in ODS on promotion.** Simplifies every consumer and destroys the anchor, the audit trail, and the ability to answer what the system believed at ingestion.

**A separate mutable "current state" table alongside ODS.** Keeps ODS immutable and reintroduces the same problem one table over: two places holding a quality verdict, which can disagree. `quality_events` avoids it by being append-only — it holds *transitions*, not *state*.

**Let the warehouse be the sole source of quality truth.** Rejected for the reason in ADR-0030: the sandbox's 60-day partition expiry means the warehouse can lose history PostgreSQL still holds. Idempotency cannot rest on a mirror that expires.

## Related

- [ADR-0002](./0002-has-clean-error-non-blocking.md) — the anchor this protects
- [ADR-0029](./0029-effective-quality-state.md) — the complexity this decision imposes on consumers
- [ADR-0031](./0031-rule-versioning-quality-events.md) — the only legal writeback target
- [Data quality architecture](../design/data-quality.md) — Proposal C, the path this boundary leaves open
