# ADR-0048: Build only two dimensions, SCD1; the fact table carries the at-the-time snapshot

**English** | [繁體中文](../../zh-TW/adr/0048-two-dimensions-scd1.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `dim_`/`fct_` |

---

## Context

A textbook star schema invites a set of dimensions: date, geography, customer, product, plus a junk dimension for the low-cardinality flags. Most of them would be speculative here.

And a second question follows immediately: customer attributes change over time, so does the model need SCD2?

## Decision

**Only `dim_customer` and `dim_product` are built.** Everything else is degenerated onto the fact tables:

| Not built | Why |
|---|---|
| `dim_date` | No fiscal-year or holiday requirement; `order_date` can be `date_trunc`'d directly |
| `dim_geography` | No conformed geography master exists — extracting it just relocates columns and joins them back, and wide tables cost little in BigQuery's columnar storage |
| Junk dimension | Saving row storage is not a problem BigQuery has |

**SCD1, with an explicit tiebreak.** Neither dimension has an independent master — attributes arrive with each order — so there is no separate change feed to track. The tiebreak is required: without one, which of several same-day orders wins would drift with execution order, making the dimension non-deterministic.

## SCD1's distortion is bought back by the fact table

SCD1 stamps historical orders with the *current* attribute — a customer who was silver in March reads as platinum today. The fix does not need SCD2:

```
fct_orders.membership_tier_at_order   ← the tier at the moment of the order
```

**Customer attributes carried on an order are already a point-in-time snapshot.** Letting the fact table carry them gives **the type-2 effect with zero infrastructure**:

| Question | Read |
|---|---|
| Total spend of customers who are *currently* platinum | `dim_customer.membership_tier` |
| Orders placed *while* platinum | `fct_orders.membership_tier_at_order` |

## SCD2 is designed and deliberately not enabled

The trigger is **enabling billing**, and the reason is not effort — it is that on the sandbox SCD2 **breaks**.

A dbt snapshot is a **stateful** table. Once the 60-day table expiry eats it, **it is gone for good**. That is categorically unlike `fct_` full rebuilds, which self-heal from source on the next run.

> A stateful table under a forced expiry is not a slow-changing dimension. It is a dimension with amnesia.

## Consequences

**The model is small and every table earns its place.** No dimension exists to satisfy a diagram.

**Both temporal questions are answerable today**, without snapshot infrastructure.

**Wide fact tables**, which is the correct trade in a columnar store — unread columns cost nothing at query time.

**`dim_product` attribute conflicts are flagged, not blocked** (ADR-0044's sibling decision): the same `product_id` can arrive with different names or categories on different orders. Measured 2026-08, **163 of 342** `product_id`s conflicted — root cause being that `load_test.py` drew `product_id` and its attributes from two independent random draws, since fixed. Flagging rather than blocking meant the generator bug was **visible in the data** rather than hidden behind a failed build.

## Alternatives considered

**Full Kimball dimension set.** More textbook-conformant, and `dim_date` and `dim_geography` would exist to serve requirements nobody has stated — permanent maintenance for speculative benefit. Same principle as ADR-0027's stance on scenario models.

**SCD2 from the start.** Would answer the temporal question through infrastructure rather than through a column already available — and would break on the sandbox.

**SCD1 with no tiebreak.** Non-deterministic dimension contents between runs, in a way no test would obviously catch.

## Related

- [ADR-0047](./0047-measures-roll-up-to-header.md) — the fact-table design this pairs with
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — the SCD2 deferral and its trigger
- [Transformation design](../design/transformation.md)
