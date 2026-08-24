# 2026-08 — BigQuery sandbox partition expiry

**English** | [繁體中文](../../zh-TW/verification/2026-08-partition-expiry-measurement.md)

---

## What was being verified

The sandbox forces a 60-day partition expiry. **What exactly does that do once Gold partitions on a business time axis, and can it be worked around?**

Every item below was measured, not inferred.

## ① Expiry is computed from the partition's date value, not the build time

All datasets carry `default_partition_expiration_ms = 5184000000` (60 days), inherited by every partitioned table. A `partition by order_date` table was created and five dates straddling the boundary were CTAS'd in **one statement**:

| Partition | Result |
|---|---|
| 2024-01-01 | **rows=0, gone** |
| 2026-05-01 (94 days ago) | **rows=0, gone** |
| 2026-06-04 (on the boundary) | rows=1 ✅ |
| 2026-07-01 (33 days ago) | rows=1 ✅ |
| 2026-08-03 (today) | rows=1 ✅ |

Three behaviours:

1. **The build does not fail** — `CREATE OR REPLACE` returns success.
2. **A "2024-01-01" partition is already past 60 days at the instant it is born.**
3. **Deletion is synchronous and immediate** — querying right after the CTAS returns, both old partitions are already absent from `INFORMATION_SCHEMA.PARTITIONS`, and even `num_rows` metadata reads 3 rather than 5. **No warning.**

> `stg_orders` never hit this purely because it partitions on `received_at`, and ingestion time is always recent. **Switch to a business time axis and that protection disappears.**

## ② The 60-day ceiling is hard-locked — all four routes closed

| Attempt | Result |
|---|---|
| DDL `options(partition_expiration_days = 3650)` | ❌ job fails |
| DDL `options(partition_expiration_days = NULL)` | ⚠️ **no error, silently rewritten to 60 days** |
| API: `table.time_partitioning.expiration_ms` | ❌ 403 |
| API: dataset `default_partition_expiration_ms` | ❌ 403 |

```
reason: billingNotEnabled
Partition expiration time must be less than 60 days while in sandbox mode.
```

**Consequence for the code**: `gold_partition_expiration_days` must be `var`-gated and emit nothing by default. Hard-coding 1825 makes every `dbt run` fail and skips everything downstream in a `dbt build`.

> **A leak worth knowing about but not using**: the `3650` DDL **half-succeeds** — the job is marked failed (`error_result.reason=billingNotEnabled`) yet the table is created, `expiration_ms` really is 3650 days, and the old rows survive and are queryable 60 seconds later. Enforcement sits at the **job validation layer** and the DDL's side effect slips past it. Unusable: a failed job is a failed dbt run, and once Google closes the gap the table starts being reaped silently.

## ③ Out-of-range dates land in `__UNPARTITIONED__` — they do **not** fail the build

```
partition_id=20260803           rows=1
partition_id=21591231           rows=1
partition_id=__UNPARTITIONED__  rows=3   ← 1959-12-31 / 2160-01-01 / 9999-12-31
build succeeded; all 5 rows survive and are queryable
```

Values outside `1960-01-01 ~ 2159-12-31` raise **no error** and go silently into `__UNPARTITIONED__`.

Knock-on effect: those rows likewise **escape the 60-day reaper**, and can never be pruned by partition pruning.

## ④ The `__NULL__` partition escapes the reaper

`order_date` is nullable in ODS. NULLs land in BigQuery's `__NULL__` partition, which has no date and therefore no computable expiration, so it is **never reaped**.

In the measurement it was written in the same batch as 2024-01-01 — which vanished on the spot while `__NULL__` survived.

Consequence: **orders without an `order_date` outlive those with one** in `fct_orders`. Current data has 0 NULLs, but the schema permits them.

## Conclusion

The limit is real, account-level, and unworkaroundable. Its important property is not that it deletes data but that **it deletes data silently, and its behaviour is non-uniform** — three different partition kinds (dated, out-of-range, NULL) are reaped on three different rules.

That non-uniformity is what makes it dangerous for test design: a test that counts rows in Gold has a result that depends on today's date.

## What this overturned ⭐

**The cloud-layer document previously claimed**: *"absurd future dates fall outside BigQuery's acceptable partition range and fail the whole table build."*

**They do not.** They land silently in `__UNPARTITIONED__` and the build succeeds.

The planned "legal-range guard" before adopting `order_date` partitioning was therefore **retracted as unnecessary** — the failure mode was not the one that had been assumed. The real hazard is the opposite of what was expected: not a loud build failure, but rows that quietly escape both partitioning and expiry.

## Related

- [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md) — the sandbox's other constraint
- [design/cloud-layer](../design/cloud-layer.md)
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — what a billed account changes
