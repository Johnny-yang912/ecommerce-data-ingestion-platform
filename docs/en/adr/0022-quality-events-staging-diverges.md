# ADR-0022: `quality_events` staging deliberately diverges from `orders`

**English** | [繁體中文](../../zh-TW/adr/0022-quality-events-staging-diverges.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

The extraction script lands a second table alongside `orders`: `quality_events`, the append-only quality-event log.

**Why it must be extracted at all:** the `int_` layer composes the *effective quality state* by joining the ODS snapshot with the latest `quality_events` event. A record promoted by Proposal B still reads `has_clean_error = TRUE` in ODS forever — ODS is immutable (ADR-0002). Only the event lets it flow back to Gold. Without this table in the warehouse, the flow-back mechanism has no right-hand side.

The tempting shortcut is to copy the `orders` table design. **Its access pattern is the opposite**, so every decision has to be re-asked.

## Decision

Every design choice is made independently, and three of them come out differently:

| Decision | `orders` | `quality_events` | Why different |
|---|---|---|---|
| Partition | `received_at` (DAY) | `event_at` (DAY) | Each table uses its own landing-time axis; `event_at` also feeds the watermark |
| Clustering | `order_id` + `has_clean_error` | `raw_id` + `to_state` | Downstream takes "latest state per record" at **`raw_id`** grain — the same key `stg_` dedups on |
| Cost fuse | ✅ on | ❌ **off** | **The decisive one** — see below |

**The fuse is off, and that is the point of this record.** `orders` queries always carry a `received_at` filter, so the fuse (ADR-0021) costs nothing. But `quality_events`'s main consumer needs *the latest event per `raw_id` across all history* — inherently a full scan with no partition filter. The fuse would block the one query the design requires.

## Consequences

**A copied design would have been silently wrong.** Not wrong at load time, not wrong in a test — wrong at the moment the flow-back path was first exercised, which is months after the table was created. That is the argument for re-asking every decision rather than inheriting one.

**Flow-back is cleaner here than for `orders`.** Promotion events carry `event_at = now()` and land in **today's** partition, so a routine `event_at >= watermark` incremental picks them up naturally. Corrections to `orders`, by contrast, land back in **old** partitions and need an explicit runbook push.

> The append-only time semantics make `quality_events`'s extraction strictly simpler than `orders`'s. That is not a coincidence — it is the payoff of an event log being append-only.

**⚠️ The "across all history" assumption is capped at 60 days on the sandbox.** The BigQuery sandbox forces a 60-day partition expiry that this table inherits; setting `expiration=None` in the script is ignored, because it is an account-level limit. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — the consequences are far more severe for Gold tables partitioned on a business axis.

**Cost is unbounded by design here**, since the fuse is off. Acceptable because the table is small (one row per quality state change, not per order) and the full scan is what the consumer needs.

## Alternatives considered

**Copy the `orders` spec.** Would break the effective-state composition, and break it late.

**Keep the fuse and materialise a "latest per `raw_id`" view.** Moves the full scan into a scheduled job instead of removing it, adds an object to maintain, and would need its own freshness guarantee for the flow-back path to be correct.

## Related

- [ADR-0021](./0021-require-partition-filter-fuse.md) — the decision this one deliberately reverses
- [ADR-0029](./0029-effective-quality-state.md) — the consumer whose access pattern drove all three differences
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — why ODS alone cannot answer the question
