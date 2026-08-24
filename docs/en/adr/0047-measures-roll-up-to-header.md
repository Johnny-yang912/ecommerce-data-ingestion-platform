# ADR-0047: Measures roll up into the header, guarded by an invariant test

**English** | [繁體中文](../../zh-TW/adr/0047-measures-roll-up-to-header.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `dim_`/`fct_` |

---

## Context

With dual fact tables — `fct_orders` (header grain) and `fct_order_items` (line grain) — the biggest risk is not building them wrong. It is that **the same number exists in two places and may disagree**.

| | A: roll up into `fct_orders` | B: amounts only on the line fact | **C (chosen)** |
|---|---|---|---|
| "Orders and revenue this month" | single-table query | must join + group by | single-table query |
| The two numbers disagreeing | possible, nothing guards it | impossible (single source) | **impossible (a test guards it)** |
| Extra cost | 0 | 0 | one singular test |

## Decision

Roll amounts up into `fct_orders`, and assert per-order equality with `assert_fct_orders_rollup_matches_items`.

This is the **same move** as ADR-0045: spend one test to upgrade a disciplinary guarantee into a mechanical one, and get single-table queryability in return.

**`is distinct from`, not `=`, is mandatory in that test.** Amounts propagate NULL strictly, and `NULL = NULL` yields NULL rather than TRUE — so `=` would let "both sides NULL" rows be silently filtered out by the `WHERE`, which is precisely the case most likely to be wrong.

## The test caught something it was not designed for

On the day the first batch of genuinely multi-item orders landed (2026-08), it went red on **39 rows**. `item_count` and `total_quantity` matched exactly; only the amounts differed — **by 1 ULP**.

The cause: `SUM()` over `FLOAT64` is not associative, and the rollup and the test's re-aggregation took different execution plans.

It stayed latent that long because until then **every order inside the 60-day window had exactly one item** — a single-value `SUM()` has no accumulation and therefore no ordering effect.

**The fix was switching money to `NUMERIC`, not loosening the test to a tolerance comparison.** A tolerance would have hidden the real defect: floating-point money.

## `SUM` silently swallows the NULLs you deliberately kept

`int_order_items` propagates NULL strictly — no `coalesce` on derived amounts. There is a trap at the rollup: **BigQuery's `SUM()` ignores NULLs.** If one item's `discount_pct` fails `safe_cast`, the order's `net_amount` is short by exactly one item, **with no error and no trace**.

The remedy is deliberately **not** `COALESCE` — that is lossy, one-way, and violates the rule that imputation belongs at the DAG edge. Instead the incompleteness is made **explicit**:

```
fct_orders.items_missing_amount   ← how many of this order's items have an uncomputable amount
```

Consumers decide whether the sum is trustworthy. **We do not decide on their behalf that NULL means zero; we give them the basis to decide.**

Related, and the same principle: `item_count = 0` expresses "an order with no items" as a **value** rather than an **absence**, and `fct_orders` must `LEFT JOIN` the rollup — an `INNER` would make that entire class of orders vanish from Gold.

## Consequences

**Single-table queryability for the most common question**, without the risk that normally accompanies it.

**A floating-point money bug was caught by a consistency test rather than by a user.** That was luck in timing and not in design — but it is an argument for asserting invariants that "obviously" hold.

**The cost is one singular test and a rollup that must stay in sync** — enforced mechanically, which is the whole point.

## Alternatives considered

**Amounts only on the line fact.** Disagreement becomes impossible by construction, and the most common business question needs a join and a group-by every time.

**Roll up with no test.** The two numbers can drift, and nothing says which is right. The floating-point defect would have shipped silently.

**Loosen the test to a tolerance.** Would have hidden a real bug behind an epsilon.

## Related

- [ADR-0045](./0045-int-effective-state-duplication.md) — the same "buy back a guarantee with a test" move
- [ADR-0048](./0048-two-dimensions-scd1.md) — the other Gold-layer modelling decision
- [ADR-0049](./0049-business-reports-read-gold.md) — the test this protects, and what bypassing Gold would cost
