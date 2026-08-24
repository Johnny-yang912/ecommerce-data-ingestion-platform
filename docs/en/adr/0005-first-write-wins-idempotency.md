# ADR-0005: Idempotency is first-write-wins — pre-check plus `IntegrityError` backstop

**English** | [繁體中文](../../zh-TW/adr/0005-first-write-wins-idempotency.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Ingestion — ODS |

---

## Context

ADR-0001 delegated business deduplication to ODS. ODS must therefore hold exactly one row per `order_id`, under concurrency, with no coordination between workers.

A pre-check — `SELECT` before `INSERT` — is the obvious implementation and is not sufficient on its own. Two workers can both run the `SELECT`, both find nothing, and both proceed. The window is small and it is real: it was reproduced deliberately.

## Decision

Two `UNIQUE` constraints, doing two different jobs:

| Constraint | Guarantees |
|---|---|
| `UNIQUE(ods.order_id)` | one row per business order |
| `UNIQUE(ods.raw_id)` | one Raw row produces at most one ODS row — a 1:1 lineage edge |

And two layers of enforcement:

1. **Pre-check** — `SELECT ODS WHERE order_id = ?` before commit. On a hit: mark the Raw record `duplicate`, do not write.
2. **`IntegrityError` backstop** — caught at commit, **without retry**. Re-read to find the winner, mark this record `duplicate`, return.

The first writer to commit wins. The second is not an error condition; it is a duplicate (ADR-0003).

## Consequences

**The constraint is the guarantee; the pre-check is an optimisation.** This distinction is worth stating because it determines what to do when they disagree: the pre-check exists to avoid doing pointless work in the common case, and the database is what makes the result correct in every case. Removing the pre-check would cost performance. Removing the constraint would cost correctness.

**`IntegrityError` is deliberately not retried.** A retry would re-execute a write that is now guaranteed to fail — the same category of mistake as retrying a deterministic error, which is what produced the poison pill in ADR-0006.

**Verified under both orderings:**

| Scenario | Result |
|---|---|
| Sequential — same `order_id` submitted twice | First writes ODS; second hits the pre-check and is marked `duplicate` |
| TOCTOU race — two workers both pass the pre-check | First commits; second gets `IntegrityError`, is marked `duplicate` |

In both cases ODS ends with exactly one row per `order_id`.

**The cost is wasted work.** The losing writer has already parsed, flattened and cleaned the payload before the constraint rejects it. That is accepted: the alternative is coordination, which is more expensive in the common case where there is no duplicate at all.

## Alternatives considered

**`INSERT ... ON CONFLICT DO NOTHING`.** Would collapse both layers into one statement, but silently — the losing writer could not tell whether it won, so `raw.status` could not be set correctly. The distinction between `processed` and `duplicate` (ADR-0003) is worth more than the round trip saved.

**Last-write-wins.** Would make ODS mutable, contradicting its role as the immutable anchor (ADR-0002) and breaking the assumption that `quality_events` is the only thing recording change over time.

**A distributed lock keyed on `order_id`.** External dependency, and it would still need the constraint as a backstop — see ADR-0004.

## Related

- [ADR-0001](./0001-raw-no-business-dedup.md) — where this responsibility was delegated from
- [ADR-0003](./0003-duplicate-terminal-status.md) — where the loser goes
- [ADR-0004](./0004-cas-claim-rowcount.md) — the same "let the database arbitrate" pattern, on the claim
- [ADR-0006](./0006-nul-byte-fast-fail.md) — the retry-classification mistake this decision avoids
