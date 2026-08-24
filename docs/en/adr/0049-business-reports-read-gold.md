# ADR-0049: Business reports always read Gold; quality reporting splits into two tables

**English** | [繁體中文](../../zh-TW/adr/0049-business-reports-read-gold.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `rpt_` |

---

## Context

Wiring `rpt_` straight onto `int_` as a canned query is convenient and is an anti-pattern. Two of the four reasons are general; two are specific to this project's architecture.

| # | Reason | Consequence of bypassing `fct_` |
|---|---|---|
| 1 | One definition per metric | "Revenue" gets two lineages → two numbers, and nobody knows which is wrong |
| 2 | Do not redo the semantic decisions Gold already made | Unknown members, the `item_count=0` LEFT JOIN — all of it copied again |
| 3 | It invalidates existing tests | `assert_fct_orders_rollup_matches_items` guards `fct_orders`'s rollup; if `rpt_` recomputes from `int_`, **that test does not cover it at all** |
| 4 | ⭐ It overturns an architectural premise | "`int_` is consumed only inside the DAG" is the **only** reason `int_` is not partitioned. `rpt_` reading it promotes it from internal building material to a **public contract** → the partitioning decision needs revisiting, and `int_` becomes un-refactorable |

## Decision

**Business reports read `dim_`/`fct_` only.**

**The legitimate exception is quality reporting.** Quarantined rows by definition never reach Gold, so `rpt_quality_*` must draw from `int_orders_quarantine` and `stg_quality_events`.

> ⚠️ That exception carries an easy mistake: **the denominator for quality rates is all of `stg_orders`, dirty included — not `fct_orders`.** Use Gold as the denominator and `quarantine_rate` is identically zero, because removing those rows is exactly what the Row Filter did.

## Quality reporting splits into two tables

The originally sketched single `rpt_quality_daily` conflated two things of opposite nature:

| | `rpt_quality_events_daily` | `rpt_quality_backlog` |
|---|---|---|
| Axis | **event axis** (`event_at`) | **snapshot** (current quarantine contents) |
| A row means | "N quality events happened that day" | "N orders are stuck right now" |
| Retroactively rewritten? | **No** (append-only) | Yes — it *is* the current state |
| Incremental possible? | ✅ axis aligned with the source of change | ❌ inherently not |

**Why the backlog cannot just be accumulated off the event axis.** In theory `backlog(t) = cumulative quarantined − promoted − rejected`, since the event stream is the complete derivative of the state. But `quality_events` has a 60-day partition expiry — **once it expires the starting point of that accumulation is gone, and the distortion is one-directional**: the start can only under-count quarantined, so the backlog is systematically understated. The snapshot table reads `int_orders_quarantine` directly and is immune to the event retention window.

**Why the event table is not hung on the ingestion axis.** Grouping by `received_at` would mean that promoting a three-month-old order today **rewrites the composition of a row three months ago** — that is state, not an event, and it makes "how much did v1 intercept" drift over time, in direct conflict with ADR-0033.

## Consequences

**Every metric has one lineage**, and Gold's semantic decisions are made once.

**Existing tests actually cover the reports**, because the reports read the tables those tests guard.

**`int_` stays refactorable** — it remains internal building material, which is what keeps its no-partitioning decision valid.

## A note on honesty

The textbook justification for `rpt_` is "pre-aggregate to buy performance and cost", and **at this project's volume that justification is worth nothing**. The documentation deliberately does not claim it, because it would be false.

The real reasons are: **one fixed definition per metric**, and **BI not having to assemble joins itself**. Let report authors aggregate `fct_` freely inside the BI tool and metric definitions drift into the BI tool — at this scale, that is what `rpt_` actually prevents.

## Related

- [ADR-0033](./0033-historical-metrics-never-rewritten.md) — why the event axis cannot be the ingestion axis
- [ADR-0047](./0047-measures-roll-up-to-header.md) — the test that bypassing Gold would invalidate
- [ADR-0027](./0027-blocking-at-int-layer.md) — why quarantined rows are absent from Gold in the first place
