# ADR-0000: Template

**English** | [繁體中文](../../zh-TW/adr/0000-template.md)

| | |
|---|---|
| **Status** | Template — not a decision |
| **Date** | — |
| **Layer** | — |

Copy this file, renumber it, and fill in the sections. Delete any section that has nothing to say — an empty heading is worse than an absent one.

---

## Context

The forces in play at the time of the decision. What made this a decision rather than an obvious default? State the constraint, the conflict, or the surprise that put the question on the table.

Write this so it stays true. Context does not change when the decision is later revised — that goes in the change log.

## Decision

What the system does, in the present tense. One paragraph, or a short list. No justification here; that is what the other sections are for.

## Consequences

What this buys **and** what it costs. An ADR with only benefits has not been thought through — every real decision gives something up.

## Alternatives considered

What else was on the table, and why each one lost. Include options that were technically impossible, and say why they were impossible — "we tried X and BigQuery rejects it" is more useful to the next reader than silence.

## Revisit when

The condition under which this decision should be reopened. Omit if the decision is unconditional.

This section is what separates a deliberate non-goal from a gap. See the status vocabulary in [STATUS](../STATUS.md).

## Change log

Only when the decision has been revised in place. Record what changed, when, and what forced it. A revision that changes the *mechanism* belongs here; a revision that changes the *decision* deserves a new ADR that supersedes this one.

## Related

Other ADRs, design documents, verification records, incident reports.
