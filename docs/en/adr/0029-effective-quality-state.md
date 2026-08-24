# ADR-0029: The Row Filter reads effective quality state, not literal `has_clean_error`

**English** | [繁體中文](../../zh-TW/adr/0029-effective-quality-state.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — dbt `int_` |

---

## Context

The obvious Row Filter is `WHERE has_clean_error = FALSE`. It is wrong, and the reason is a direct consequence of two earlier decisions.

**ODS is immutable** (ADR-0002), so a record quarantined at ingestion reads `has_clean_error = TRUE` **forever** — even after a rule change makes it legitimate. **Bounded writeback** (ADR-0032) forbids updating that column from downstream.

So a literal read of the flag would leave every promoted record stuck in quarantine permanently. The re-evaluation mechanism would exist, write its events, and change nothing.

## Decision

The filter reads the **effective quality state**: the ingestion-time verdict composed with the latest event in `quality_events`.

```sql
COALESCE(
    s.has_clean_error = FALSE      -- clean at ingestion time
    OR e.to_state = 'promoted',    -- or promoted by a later re-evaluation
    FALSE
) AS is_effectively_clean
```

`int_orders` takes rows where this is true; `int_orders_quarantine` takes its negation. The two together are a **partition** of `stg_orders` — mutually exclusive, jointly exhaustive — asserted by a singular test.

The latest event per `raw_id` is taken by `ROW_NUMBER() OVER (PARTITION BY raw_id ORDER BY event_at DESC, id DESC)`. The `id DESC` tiebreak is not decorative: without it, two events sharing a timestamp make the result non-deterministic, and the two models could disagree about the same row.

## Two things that must not be touched

**⚠️ The `LEFT JOIN` must stay `LEFT`.** Most records have no quality event at all. An inner join would silently drop every record that was clean at ingestion and never re-evaluated — which is nearly all of them.

**⚠️ The `COALESCE` must not be dropped.** This is the subtle one. With `has_clean_error = TRUE` and no matching event:

```
FALSE OR NULL  =  NULL
WHERE NULL          → row excluded
WHERE NOT NULL      → NULL → row also excluded
```

**The row vanishes from *both* tables simultaneously.** No error, no failing row count on either side — just silent data loss, caused by SQL's three-valued logic doing exactly what it is specified to do. The partition test is what catches this class of mistake.

## Consequences

**The flow-back path works end to end.** When the Proposal B producer was implemented, **not one line of this layer changed** — the consumer was already correct. That is the payoff of building the consumer side first.

**The composition block is duplicated byte-for-byte across two models**, deliberately rather than shared. See ADR-0045 for that trade-off and its alignment checklist.

**Quarantine records carry `quarantined_at` from the event time, not `CURRENT_TIMESTAMP()`.** The model is a full rebuild, so `CURRENT_TIMESTAMP()` would record when *the run* happened rather than when the row was quarantined — and would change on every run.

## Alternatives considered

**`WHERE has_clean_error = FALSE`.** Severs the flow-back path, permanently and silently.

**Update `has_clean_error` in ODS when a record is promoted.** Violates the immutable anchor (ADR-0002) and bounded writeback (ADR-0032), and destroys the audit trail: the record would no longer show that it was ever quarantined, or under which rule version.

**Materialise the effective state into a column during extract.** Would move the composition upstream into E/L, where it would have to be recomputed and re-landed every time an event arrives — and staging is supposed to be a 1:1 mirror (ADR-0025).

## Related

- [ADR-0002](./0002-has-clean-error-non-blocking.md) — the immutability that makes this necessary
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — the producer of the events read here
- [ADR-0032](./0032-bounded-writeback.md) — why the flag cannot simply be updated
- [ADR-0045](./0045-int-effective-state-duplication.md) — why this block is duplicated
