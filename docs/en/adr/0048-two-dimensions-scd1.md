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

**Customer attributes carried on an order are already a point-in-time snapshot.** Letting the fact table carry them gives **the type-2 effect with zero infrastructure**.

Walk one customer through it. Two orders in `int_orders` (the ODS is append-only, so both rows are permanent):

| order_id | customer_id | order_date | membership_tier | net_amount |
|---|---|---|---|---|
| O-1001 | A | 2026-05-01 | standard | 1,000 |
| O-2002 | A | 2026-09-01 | platinum | 3,000 |

After the tiebreak, `dim_customer` holds **a single row for A** (platinum, sourced from O-2002); `fct_orders` keeps **both rows**, each carrying the tier at the moment it was placed. The same customer is simultaneously "is platinum" and "was standard at the time" — that is not a contradiction but two definitions, and **both numbers are correct**:

| Question | Read | A's answer | Who wants it |
|---|---|---|---|
| Total spend of customers who are *currently* platinum | `dim_customer.membership_tier` | **4,000** | marketing, segmenting an audience |
| Orders placed *while* platinum | `fct_orders.membership_tier_at_order` | **3,000** | finance, costing the tier's benefits |

Dropping either side costs more than one column:

- **Without `_at_order`** → the second question is silently answered 4,000. May's standard-tier spend lands on the platinum ledger, and **any "did the upgrade work" analysis fails at the root** — both sides of the comparison become the same numbers. Meanwhile `unique` and `not_null` stay green.
- **Without `dim_customer`** → the first question has no answer. A carries two tiers in the fact table, so "which tier is A" is decided per query — **reimplementing the tiebreak at the query layer**.

> What this design buys is not data — the data was always in the ODS. It buys **one answer to "what tier is A", stable across runs.**

## SCD2 is designed and deliberately not enabled

The trigger is **enabling billing**, and the reason is not effort — it is that on the sandbox SCD2 **breaks**.

A dbt snapshot is a **stateful** table. Once the 60-day table expiry eats it, **it is gone for good**. That is categorically unlike `fct_` full rebuilds, which self-heal from source on the next run.

> A stateful table under a forced expiry is not a slow-changing dimension. It is a dimension with amnesia.

## Consequences

**The model is small and every table earns its place.** No dimension exists to satisfy a diagram.

**Both temporal questions are answerable today**, without snapshot infrastructure.

**Wide fact tables**, which is the correct trade in a columnar store — unread columns cost nothing at query time.

**`dim_product` attribute conflicts are flagged, not blocked** (ADR-0044's sibling decision): the same `product_id` can arrive with different names or categories on different orders. Measured 2026-08, **163 of 342** `product_id`s conflicted — root cause being that `scripts/load_test.py` drew `product_id` and its attributes from two independent random draws, since fixed. Flagging rather than blocking meant the generator bug was **visible in the data** rather than hidden behind a failed build.

## Alternatives considered

**Full Kimball dimension set.** More textbook-conformant, and `dim_date` and `dim_geography` would exist to serve requirements nobody has stated — permanent maintenance for speculative benefit. Same principle as ADR-0027's stance on scenario models.

**SCD2 from the start.** Would answer the temporal question through infrastructure rather than through a column already available — and would break on the sandbox.

**Deriving `valid_from` / `valid_to` with `lead()` over `fct_orders`.** Stateless, DDL-only, and not eaten by expiry, so it escapes the objection to snapshots above — but the interval boundaries it derives are **observation times, not change times** (a customer may have upgraded in July while the system only sees the September order), and `fct_orders` already carries `(customer_id, order_date, membership_tier_at_order)`, so whoever asks can window over it once. **Building a table buys a single definition, not answerability** — which makes the trigger a question being asked repeatedly, and the answer then is an `rpt_`, not a dimension.

**SCD1 with no tiebreak.** Non-deterministic dimension contents between runs, in a way no test would obviously catch.

## Related

- [ADR-0047](./0047-measures-roll-up-to-header.md) — the fact-table design this pairs with
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — the SCD2 deferral and its trigger
- [Transformation design](../design/transformation.md)
