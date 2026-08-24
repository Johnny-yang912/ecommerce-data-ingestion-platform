# ADR-0025: Staging is additive-only; rename and cast are pushed down to dbt

**English** | [繁體中文](../../zh-TW/adr/0025-staging-additive-only.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

ODS schemas change: a column is added, a column falls out of use, a type turns out to be wrong. Staging has to absorb that without becoming a second place where transformation logic lives.

BigQuery makes some of these cheap and some expensive. Adding a nullable column is a metadata operation — free, instant, existing rows read as `NULL`. Renaming a column or changing its type is not: it means rewriting the table.

The temptation is to handle each case wherever it is most convenient at the time. That produces a staging layer that quietly does transformation, which is precisely what `stg_` exists to do.

## Decision

**Staging only ever adds.**

| Change | Where it is handled |
|---|---|
| Add a nullable column | `ALLOW_FIELD_ADDITION` on the load job — the column appears automatically |
| Drop a column | The column stays in staging as legacy, holding `NULL`; `stg_` stops selecting it |
| Rename a column | `stg_`'s explicit column list — staging keeps the old name |
| Change a type | `stg_`'s cast, or a table rebuild |

`ensure_staging_table()` only *creates*; it never alters. Evolution of an existing table happens through the load job's `schema_update_options`.

**The reason `stg_` uses an explicit column list rather than `SELECT *`** is exactly this: the explicit list is the rename seam, and it is also the gate. A column that `ALLOW_FIELD_ADDITION` grows in staging is invisible downstream until someone deliberately adds it to that list — **in a commit, in a review**. Drift cannot leak through on its own.

## Consequences

**Staging stays a faithful mirror of ODS**, which is what makes "compare staging to ODS" a meaningful reconciliation.

**Adding a column costs nothing and breaks nothing.** The load job absorbs it; downstream is unaffected until someone opts in.

**Dropped columns leave `NULL`-filled legacy columns behind.** Slightly untidy, and much cheaper than a rewrite. They are removed only when a rebuild happens for some other reason.

**The cost is that `stg_` accumulates the rename and cast logic.** That is where it belongs — dbt is version-controlled, tested, and reviewable, whereas a transformation buried in an extraction script is none of those.

**`on_schema_change='append_new_columns'` on `stg_orders` is the mirror of this at the dbt layer**, deliberately chosen over `sync_all_columns` — which would `DROP` columns and thereby contradict "staging only adds".

## Alternatives considered

**Full refresh on every schema change.** Correct and expensive: a full table scan plus a rewrite of every partition, for a change that is usually one nullable column.

**Handle renames in the extraction script.** Puts transformation in the E/L layer, splits the rename logic across two places, and makes staging stop mirroring ODS.

**`sync_all_columns` in dbt.** Would propagate drops automatically — and a drop propagated automatically is a drop nobody reviewed.

## Related

- [ADR-0026](./0026-fields-single-source.md) — the declaration that has to stay consistent for this to work
- [ADR-0043](./0043-stg-table-not-view.md) — where the absorbed logic lives
- [Cloud layer design](../design/cloud-layer.md) — end-to-end add/drop walkthroughs
