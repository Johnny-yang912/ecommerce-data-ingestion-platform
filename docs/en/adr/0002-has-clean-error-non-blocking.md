# ADR-0002: `has_clean_error` is non-blocking

**English** | [繁體中文](../../zh-TW/adr/0002-has-clean-error-non-blocking.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Data Quality — ODS |

---

## Context

The ODS layer applies two things to every order: format normalisation (coercion — trimming, casing, type alignment) and business-rule validation (is `age` within a plausible range, is `customer_name` within its soft length cap, and so on).

Format normalisation has an obvious answer: coerce and move on. Business-rule validation does not. When a rule fails, the layer has to choose between rejecting the record and recording the failure alongside it.

Three forces pushed against rejection:

**ODS is the immutable anchor.** It is the one place where every accepted order exists exactly once, flat and queryable. If dirty rows are rejected, the anchor is incomplete — and there is no denominator. "3% of orders failed validation" is unanswerable if the failures were never written.

**Raw is not a substitute.** The Raw table does hold the original payload, but as unparsed JSON. It cannot be reconciled, aggregated, or joined without redoing the whole flattening pass.

**Quality rules change.** `DQ_RULE_VERSION` is at v4; the `age` cap moved 120 → 130 and the `customer_name` soft cap moved 100 → 150. A record rejected under v2's rules may be perfectly clean under v4's. If rejection is the response, that record is unrecoverable without re-ingesting from the source — and the source is upstream, which may no longer have it.

## Decision

`clean_order()` returns `(ods, has_clean_error, clean_error_message)`. A business-rule violation sets `has_clean_error = True` and records the error codes. **The ODS write proceeds unconditionally.**

`has_schema_drift` is a parallel and independent signal with the same non-blocking property. The two are never mixed: one is about *values failing business rules*, the other about *the upstream contract changing shape*.

Blocking happens downstream, per-record, at the `int_` layer's Row Filter — not here.

## Consequences

**ODS stays complete.** Every accepted order is present exactly once regardless of quality, so every quality metric has a real denominator, and reconciliation between layers is arithmetic rather than guesswork.

**Rule changes become retroactively applicable.** Because the rows are present, they can be re-evaluated against a newer rule version without re-ingesting anything. This single property is what makes the entire Proposal B re-evaluation mechanism and the `quality_events` state machine possible — none of it would exist if dirty rows had been rejected.

**The cost is that no downstream consumer may read ODS or `stg_` directly for business reporting.** Those layers contain dirty rows by design. The rule is enforced by the layer contract and by tests, not by the schema — there is nothing physically stopping a careless query.

**The second cost is that row counts always need a quality qualifier.** "How many orders yesterday" has two correct answers, and any report that does not say which one it means is wrong.

## Boundary: what is *not* covered by this decision

Non-blocking applies to **business-rule violations**, not to storage-level impossibilities.

A field that overflows its column (`DataError`) or a string containing a NUL byte (`ValueError`) cannot be written at all. Those fast-fail to the terminal `error` state and never reach ODS. Treating them as "flag and continue" is not an option — there is nothing to continue to.

That boundary is its own decision; see ADR-0006.

## Alternatives considered

**Reject at ODS.** Loses the anchor property and makes rule evolution unrecoverable. Rejected on both counts.

**Block at the Raw layer.** Raw's responsibility is to record what arrived, including duplicates and garbage — see ADR-0001 and ADR-0003. Moving quality judgement there would collapse two layers with deliberately different jobs.

**Write dirty rows to a separate ODS table.** Every downstream query would need a `UNION`, and the "exactly once" invariant would be split across two tables — which means it could be violated in a way no single-table constraint could catch.

## Related

- [ADR-0006](./0006-nul-byte-fast-fail.md) — where the non-blocking rule stops
- [ADR-0027](./0027-blocking-at-int-layer.md) — where blocking happens instead
- [ADR-0029](./0029-effective-quality-state.md) — why the Row Filter reads effective state, not this flag
- [ADR-0031](./0031-rule-versioning-quality-events.md) — the mechanism this decision enables
- [Data quality architecture](../design/data-quality.md)
