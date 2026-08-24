# ADR-0045: The `int_` layer duplicates effective-state logic deliberately, rather than sharing a model

**English** | [繁體中文](../../zh-TW/adr/0045-int-effective-state-duplication.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Transformation — dbt `int_` |

---

## Context

`int_orders` and `int_orders_quarantine` need the same logic: composing the effective quality state (ADR-0029). The instinct is to extract it into a shared model — duplication is the thing one is trained to remove.

Three options were on the table, and one common belief about them is wrong:

| | How | Physical objects | JOIN executions | Complementarity guaranteed by |
|---|---|---|---|---|
| A | Shared **ephemeral** model emitting `is_effectively_clean` | no increase | once **per downstream** | mechanism (one boolean + its negation) |
| B | Shared small table `int_quality_current` + a macro | +1 table | once total | mechanism (macro) |
| **C (chosen)** | Each model inlines the same CTE block | no increase | once per downstream | **discipline + a test** |

**The misconception worth clearing up:** A and C compile to nearly identical SQL. An ephemeral model is *inlined into every downstream*, so it creates no extra relation **and saves no JOIN executions**. Only B actually reduces the JOIN count.

> **A vs C is purely a maintenance trade-off, not a cost one.**

## Decision

Option C — the block is written twice, fenced with `═══` comment markers in both files, and **must stay byte-identical**.

Three reasons:

1. **Only two consumers today**, so duplication costs less than the cognitive cost of one more `ref` indirection.
2. **Each model file is self-contained** — reading `int_orders.sql` shows the whole decision, matching the DQ document's example verbatim, with no chasing across three files.
3. **The price — complementarity drops from a mechanical guarantee to a disciplinary one — can be bought back with one test.**

That last point is the pattern: *spend one test to convert a discipline guarantee back into a mechanical one*. The same move appears at the Gold layer (ADR-0047).

## The alignment checklist

Walked on every change to either model. Each row is a way to break the partition invariant:

| # | Check | Consequence of getting it wrong |
|---|---|---|
| 1 | Both define `is_effectively_clean` identically; one uses `WHERE cond`, the other `WHERE NOT cond` | Non-complementary → rows in neither table (**silent loss**) or both (double counting) |
| 2 | `coalesce(..., false)` must not be dropped | `FALSE OR NULL = NULL`; `WHERE NOT NULL` is also NULL → **the row vanishes from both tables at once** |
| 3 | Always `LEFT JOIN` | An accidental INNER drops every row with no quality event |
| 4 | Window `partition by` / `order by` tiebreaks match | The two sides pick different events for the same row → partition breaks |
| 5 | The `effective_quality_state` CASE branches match | Lineage labels disagree; `rpt_quality_*` miscounts |
| 6 | Both use the **same materialisation** | One incremental, one full → partition breaks between runs |
| 7 | `assert_orders_split_is_partition` stays `severity: error` — never downgraded, never `--exclude`d | It is the only automated safety net under option C |

> **#2 is the easiest to miss.** Of `is_effectively_clean`'s three states — TRUE / FALSE / **NULL** — NULL makes a row disappear from *both* tables, silently. The partition test exists for exactly that.

## Consequences

**Each model is readable on its own**, which matters because this is the layer where the system's central quality decision is expressed.

**The safety net is a single test, and it is load-bearing.** Item 7 is not a style preference — downgrading that test removes the entire justification for option C.

**The cost is real and is paid on every change** to either model. It is bounded by the alignment checklist, not by good intentions.

## Revisit when

**A third copy appears** — for instance when scenario-specific `int_orders_*` models are enabled. At three consumers, duplication starts costing more than the indirection, and the block should collapse into option A or B.

## Related

- [ADR-0029](./0029-effective-quality-state.md) — the logic being duplicated
- [ADR-0047](./0047-measures-roll-up-to-header.md) — the same "buy back a guarantee with a test" move
- [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) — checklist item 6, as its own decision
