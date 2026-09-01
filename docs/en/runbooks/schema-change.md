# Runbook: Adding or removing an ODS column

**English** | [繁體中文](../../zh-TW/runbooks/schema-change.md)

---

## Scope

This is for an engineer **deliberately** changing ODS via Alembic. It is **not** for upstream drift — drift does not change ODS structure; unknown fields land in `unmapped_fields` and are flagged by `has_schema_drift`.

**First, decide which kind of NULL you are creating.** The two cases are mirror images on the time axis, so their handling is opposite:

| | Where the NULL grows | Meaning |
|---|---|---|
| **Add** | the past (historical partitions) | the column **did not exist** in that history |
| **Drop** | the future (grows after collection stops) | the column is **no longer filled** from here on |

Get this wrong and you reach for the wrong tool.

---

## Adding a column

| # | Checkpoint | Action |
|---|---|---|
| 1 | ODS | Alembic adds a **nullable** column. A `NOT NULL` add cannot use `ALLOW_FIELD_ADDITION` — existing rows would violate it |
| 2 | Consistency test | `test_no_ods_column_missing_from_fields` goes **red** — "ODS has it, `FIELDS` doesn't" is caught here rather than silently under-extracting |
| 3 | `FIELDS` | Add the column, type and mode aligned. Green = the three declarations realign |
| 4 | Extract + load | `ALLOW_FIELD_ADDITION` auto-adds it to staging; historical rows in old partitions are NULL, new rows have values |
| 5 | `stg_orders` (list untouched) | The final explicit `SELECT` does not list it → **dropped**. Model output unchanged; the column just rides along in staging |
| 6 | `stg_orders` (surface it) | Add it to the explicit `SELECT` — **into git, reviewed**. The next ordinary incremental run suffices: dbt `ALTER ADD COLUMN` (metadata, free) + a copy job overwriting only the lookback-window partitions |

**Step 5 is the gate, not an oversight.** A column grown in staging is invisible downstream until someone deliberately surfaces it — so drift cannot leak through on its own ([ADR-0025](../adr/0025-staging-additive-only.md)).

### ⚠️ The `append_new_columns` blind spot

`ALTER ADD COLUMN` sets **all** old partitions to NULL, but an ordinary incremental only backfills the **lookback window**.

If the column has existed in staging for a while — introduced ≪ the moment you add it to the `stg_` SELECT — the partitions in between have **real values in staging but stay NULL in `stg_`**.

Fix once, at first surfacing:

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: <N covering the gap>}'
# or a single --full-refresh
```

> "No full-refresh" means **no full-table rewrite on every future run**. A one-time backfill is still needed if there is a historical gap.

### Handling the historical NULL

Fork on **whether the history is "nonexistent" or "under-extracted"**:

| Option | Applies when | How |
|---|---|---|
| **A. Accept the NULL** (default) | the value genuinely starts being collected now | don't fill; downstream slices by time or `WHERE col IS NOT NULL` |
| **B. [Proposal C](./proposal-c-correction.md) backfill** | the value was always in Raw, ODS just never mapped it | re-produce from Raw → push corrected rows → targeted refresh |
| **C. Downstream imputation** | analysis needs non-NULL | `COALESCE` in `int_`/`dim_`, semantics recorded in the model description |
| **D. Default at ingestion** | the value must always exist | set default / NOT NULL in the migration; historical rows filled at migration time |

A is the default because **force-filling is fabricating data** — NULL honestly reflects "it did not exist before".

---

## Dropping a column

| # | Checkpoint | Action |
|---|---|---|
| 1 | ODS | Alembic drops it; `models.py` no longer has it |
| 2 | Consistency test | `test_no_stale_field_without_ods_column` goes **red** — the stale "`FIELDS` has it, ODS doesn't" is caught |
| 3 | `FIELDS` | Remove the column; green |
| 4 | Extract + load | The staging physical column is **kept**; the load schema omits it → new rows NULL, historical rows keep their values |
| 5 | `stg_orders` | The explicit list still has it → queries fine, **non-breaking**; it becomes a legacy column |
| 6 | Removing it from the model | **Default: leave it as legacy, do nothing.** `append_new_columns` is add-only and deliberately does not `DROP` |

If it genuinely must go, `--full-refresh` rebuilds. Rare, deliberate escape hatch — and if a downstream `int_`/`dim_` still references it, that run errors and is caught inside the DAG.

### Handling the growing future NULL

The column has real history and a NULL future that keeps growing. The question shifts from *"how to fill"* to **"how to not misuse it"**: any aggregate spanning the cut-off silently mixes two populations. Record the cut-off date in the model description, and prefer explicit time slicing over `COALESCE`.

---

## Why the consistency test matters

Without step 2 / 3's guard, adding an ODS column and forgetting `FIELDS` fails **silently**: the extract runs, the load succeeds, and the column simply is not in the warehouse. There is no error — only an absence, discovered whenever someone eventually looks for it. [ADR-0026](../adr/0026-fields-single-source.md)

---

## Related

- [design/cloud-layer](../design/cloud-layer.md) — the additive-only policy
- [dbt-ops](./dbt-ops.md) — lookback windows and `--full-refresh`
