# 2026-06 — Cloud extraction, first run

**English** | [繁體中文](../../zh-TW/verification/2026-06-cloud-extract-first-run.md)

---

## What was being verified

The staging table's design, on first contact with a real BigQuery dataset. **Do the partitioning, clustering, cost fuse and location settings actually take effect — and does the fuse actually block?**

## Environment

BQ sandbox, dataset `staging`, location `US`. First execution of `extract_ods_to_bq.py`. 2026-06.

## Observed

| Check | Result |
|---|---|
| partition / clustering / fuse / location | `received_at(DAY)` / `[order_id, has_clean_error]` / `True` / `US` |
| **Fuse** | a query without a `received_at` filter is **blocked with 400** |
| JSON landing | `items` and `clean_error_message` both `JSON_TYPE=array`; downstream `JSON_VALUE(...[0],'$.code')` reads correctly |
| Additive load path | explicit schema + `ALLOW_FIELD_ADDITION` does not break the happy path |
| Consistency test | `test_schema_bq_consistency` all green |

## Conclusion

All four table-design decisions took effect as declared. The two worth stating explicitly:

**The fuse actually blocks.** `require_partition_filter=True` is only worth anything if an unfiltered query fails rather than succeeding expensively. It returns 400 — so the failure mode is **free and loud** rather than expensive and silent ([ADR-0021](../adr/0021-require-partition-filter-fuse.md)).

**JSON lands as JSON, not as a string.** `items` arriving as `JSON_TYPE=array` is what lets `stg_` and `int_order_items` read into it without a parse step. Landing it as a serialised string would have worked at load time and cost a `PARSE_JSON` at every downstream read.

## What this overturned

Nothing. This is a commissioning record — its value is that the settings are **confirmed rather than assumed**, which matters because three of the four are silent when wrong:

| Setting | If it silently did not apply |
|---|---|
| partition | queries scan everything; only the bill says so |
| clustering | slower queries, no error |
| location | works until the first cross-location query, then fails confusingly |
| **fuse** | **the only one that announces itself** |

## Related

- [ADR-0020](../adr/0020-partition-on-received-at.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md) · [ADR-0026](../adr/0026-fields-single-source.md)
- [design/cloud-layer](../design/cloud-layer.md)
- [2026-08-partition-expiry-measurement](./2026-08-partition-expiry-measurement.md) — what the sandbox does to this table later
