# ADR-0026: `FIELDS` is the third schema declaration, guarded by a consistency test

**English** | [繁體中文](../../zh-TW/adr/0026-fields-single-source.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

By the time a column reaches BigQuery, its shape has been declared three times:

| # | Declaration | Owner |
|---|---|---|
| 1 | `models.py` (SQLAlchemy) | The ODS table |
| 2 | The Alembic migration | The physical PostgreSQL schema |
| 3 | `ORDERS_FIELDS` / `QUALITY_EVENTS_FIELDS` | The BigQuery staging schema |

Declarations 1 and 2 already have a guard: `check_migration_drift.py` compares them (ADR-0009).

Declaration 3 had none. Add a column to ODS, forget to add it to `FIELDS`, and **nothing fails**. The extract runs, the load succeeds, and the column simply is not in the warehouse. There is no error to notice — only an absence, discovered whenever someone eventually looks for that column downstream.

## Decision

**One `FIELDS` list per table, used for everything**, so the three uses cannot diverge from each other:

- building the staging table (`ensure_staging_table`)
- the load job's schema
- the CLI's `--table` value set, derived from `SPECS` rather than maintained separately

And **`tests/test_schema_bq_consistency.py` compares `FIELDS` against `models.py`**, so declaration 3 is pinned to declaration 1 the way declaration 2 already is.

Adding a table means adding one `TableSpec` to `SPECS`. It then automatically gets a CLI entry and the consistency guard — there is no second list to remember.

## Consequences

**The silent-absence failure becomes a red test.** The feedback moves from "someone notices a missing column in a dashboard weeks later" to "CI fails on the pull request that caused it".

**All three declarations are now guarded**, though by two different mechanisms: `check_migration_drift.py` for 1↔2 (manual, see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)) and `test_schema_bq_consistency.py` for 1↔3 (in CI, because it needs no database).

**The cost is that a genuinely intentional divergence has to be expressed in the test.** If a column should exist in ODS but deliberately not in staging, that exception must be written down — which is the correct outcome, since an undocumented deliberate divergence is indistinguishable from a mistake.

**Three declarations still exist.** This decision does not remove the duplication; it makes the duplication *detectable*. Deriving the BQ schema from `models.py` at runtime would remove it — see below.

## Alternatives considered

**Derive the BQ schema from `models.py` automatically.** Would eliminate declaration 3 entirely. Rejected because the type mappings are not one-to-one and the differences are deliberate: `JSONB` → `JSON`, PostgreSQL `Date` → BigQuery `DATE`, nullable rules that differ between the two systems. An automatic mapping would need an override table for the exceptions — which is `FIELDS` again, with a layer of indirection on top.

**Rely on review.** The failure is an *absence*, and absences are the hardest thing to catch in a diff.

**Accept the gap.** The failure is silent, and silent failures in a data pipeline are the specific hazard this project is organised around.

## Related

- [ADR-0009](./0009-alembic-single-source-of-truth.md) — the guard on declarations 1↔2
- [ADR-0025](./0025-staging-additive-only.md) — the evolution policy `FIELDS` implements
- [Testing strategy](../design/testing.md)
