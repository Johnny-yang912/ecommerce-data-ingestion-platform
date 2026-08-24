# ADR-0033: Historical quality metrics are never retroactively rewritten

**English** | [繁體中文](../../zh-TW/adr/0033-historical-metrics-never-rewritten.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — metrics |

---

## Context

Suppose 100 records were quarantined under rule version v1, and v2 later promotes 15 of them.

What was the v1 quarantine rate?

There are two defensible answers, and choosing wrongly makes every historical quality number meaningless:

- **"Still what it was."** v1 quarantined 100. That is what happened.
- **"Revised down to 85."** Fifteen of those were not really dirty, so the historical rate was overstated.

The second is tempting because it feels more accurate. It is the wrong answer, because it conflates *what the system judged* with *what we now believe*.

## Decision

**Historical metrics are never rewritten.** `quality_events` is append-only, so a metric computed over it is automatically stable — the numbers do not change because there is nothing to change.

Promotions are counted as **their own separate metric**, on their own time axis:

```sql
-- Initial quarantine rate under v1 — this number never changes
SELECT countif(to_state = 'quarantined') / count(*)
FROM quality_events
WHERE event_type = 'initial_evaluation' AND rule_version = 'v1'

-- How many records v2 promoted — an independent metric, does not overwrite the above
SELECT count(*)
FROM quality_events
WHERE event_type = 'promotion' AND rule_version = 'v2'
```

## Consequences

**A quality metric means "what the system judged at the time, under the rules of the time".** That is a fact about the system's operation, and it stays true.

**Rule-change impact becomes visible as its own quantity**, rather than being absorbed silently into a revised historical number. "v2 promoted 15 records" is a more useful statement than "the v1 rate was actually 85%", because it names the cause.

**Two time axes coexist**, which is why quality reporting splits into two tables (ADR-0049): `rpt_quality_events_daily` on the *event* axis (what happened that day, immutable once written) and `rpt_quality_backlog` as a *snapshot* (what is quarantined right now, changing every run).

**Nobody can quietly improve the past.** A report showing quality trending downward cannot be fixed by loosening a rule and letting history re-render. The loosening shows up as promotions, on the day it happened.

**The cost is that "how many records are dirty right now" needs a different query from "how many were judged dirty in March".** Two questions, two answers, deliberately not unified — see ADR-0034 for the same split at a different boundary.

## Alternatives considered

**Recompute historical rates against current rules.** Every historical number becomes a function of today's rules, so a chart's shape changes retroactively whenever a rule moves. A trend line that rewrites itself cannot support any conclusion.

**Store both — original and revised.** Doubles every metric and forces a choice at every point of use. In practice one of the two becomes the one people quote, and the other becomes noise.

**Snapshot the current state daily and report on that.** Loses the ability to distinguish "was judged dirty" from "is currently dirty", which is exactly the distinction that makes rule evolution auditable.

## Related

- [ADR-0031](./0031-rule-versioning-quality-events.md) — the append-only log that makes this automatic
- [ADR-0034](./0034-tier-1-tier-2-metrics.md) — the other metric boundary
- [ADR-0049](./0049-business-reports-read-gold.md) — the two-table split this produces
