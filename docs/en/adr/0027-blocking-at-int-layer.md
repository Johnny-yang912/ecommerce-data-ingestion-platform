# ADR-0027: Blocking happens at `int_`, not at ODS

**English** | [繁體中文](../../zh-TW/adr/0027-blocking-at-int-layer.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Data Quality — architecture |

---

## Context

Dirty records have to be stopped somewhere before they reach a dashboard. The question is *where*, and every layer from ingestion down is a candidate.

Stopping them early is the intuitive answer — fail fast, keep the bad data out. ADR-0002 already rejected stopping at ODS, for reasons about the anchor and about rule evolution. But rejecting one location does not choose another, and the choice has consequences for every layer in between.

## Decision

**Quality control responsibility tightens progressively downstream. Blocking happens at exactly one place: the `int_` layer.**

| Layer | Quality requirement | Dirty records |
|---|---|---|
| Raw | none | kept verbatim |
| ODS (Bronze / Anchor) | flagged, never rejected | kept |
| `stg_` (Silver entry) | same as ODS | kept |
| **`int_` (Gold entry)** | **only effectively-clean records pass** | **→ `int_orders_quarantine`** |
| `dim_`/`fct_` (Gold) | cleanest layer | none present |
| `rpt_` | same as Gold | none present |

Two mechanisms operate at different granularities and must not be confused:

- **Hard Gate** (run-level, on `stg_`): does the source look broken *as a whole*? Blocks the entire run (ADR-0028).
- **Row Filter** (record-level, in `int_`): is *this record* usable? Routes it to quarantine (ADR-0029).

## Why `int_` and not `stg_`

`stg_` is a 1:1 mirror of the source: rename, cast, dedup, nothing else. Filtering there would make it stop being a mirror, and "compare `stg_` to ODS" would stop being a meaningful reconciliation.

`int_` is the first layer that already does semantic work — joins, derived fields, business logic. A record's *usability* is a semantic judgement, so it belongs at the first layer entitled to make one.

## Consequences

**Every layer above `int_` has a complete population**, so quality metrics have a denominator and reconciliation is arithmetic.

**Quarantine is a destination, not a deletion.** `int_orders_quarantine` holds the blocked records with their error codes, which is what makes Proposal B's flow-back possible at all (ADR-0030). A filter that dropped rows would make re-evaluation impossible.

**The split is guarded as a partition.** `int_orders` and `int_orders_quarantine` must be mutually exclusive and jointly exhaustive over `stg_orders`. A singular test asserts this — without it, a mistake in either model's `WHERE` clause silently loses or duplicates rows. This is not a theoretical risk: BigQuery's three-valued logic makes it easy for a row to fail *both* conditions and vanish from both tables (ADR-0029).

**The cost is that the contract is not enforced by the schema.** Nothing physically prevents a report from reading `stg_` directly and including dirty rows. It is held by the layer contract, by tests, and by ADR-0049.

## Alternatives considered

**Block at ODS.** ADR-0002 — loses the anchor and makes rule evolution unrecoverable.

**Block at `stg_`.** Breaks the 1:1 mirror property and the reconciliation that depends on it.

**Block at `dim_`/`fct_`.** Would mean every Gold model repeats the filter, and each is a place to get it wrong. Filtering once at the Gold *entry* means everything downstream can assume clean input.

**Filter in the BI tool.** Puts a data-quality contract in a place with no version control, no tests, and no review.

## Related

- [ADR-0002](./0002-has-clean-error-non-blocking.md) — why ODS does not block
- [ADR-0028](./0028-hard-gate-per-batch-scope.md) — the run-level mechanism
- [ADR-0029](./0029-effective-quality-state.md) — the record-level mechanism
- [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) — the materialisation this layer requires
