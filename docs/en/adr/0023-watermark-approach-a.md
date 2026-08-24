# ADR-0023: Watermark Approach A, with `get_watermark()` as the only seam

**English** | [繁體中文](../../zh-TW/adr/0023-watermark-approach-a.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

An incremental extract needs to know where it stopped last time. The conventional answer is a watermark table: a small table holding the last-processed timestamp, updated after each successful load.

That introduces a second piece of state that can disagree with reality. If the load succeeds and the watermark update fails, the next run re-extracts. If the watermark advances and the load did not commit, **rows are silently skipped** — a data loss that no error reports.

## Decision

**Approach A: derive the watermark from the data that is already there.**

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = @table
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

Three properties make this work:

- **Free.** It reads a metadata view, not table data — no bytes scanned, no cost.
- **Not blocked by the cost fuse.** `require_partition_filter` (ADR-0021) applies to the table, not to `INFORMATION_SCHEMA`.
- **Self-consistent by construction.** The watermark *is* the loaded data. It cannot disagree with what landed, because it is derived from what landed.

**There is deliberately no `advance_watermark()`.** Nothing to update means nothing to fail to update. After a load, the next `get_watermark()` call naturally reflects the new data.

The slice boundary is `>=`, not `>`: **prefer re-extracting to missing rows.** Duplicates are handled by `stg_`'s dedup on `raw_id` (ADR-0043), which had to exist anyway.

## The seam

`get_watermark()` is the **only** place that knows how the watermark is obtained. Everything else asks it a question.

That is what makes Approach B — a dedicated watermark table, precise to the timestamp, for minute-level micro-batch — a contained change rather than a rewrite: it would replace this function's body and add an `advance_watermark()` step, and nothing else moves.

**This project will not take that path.** Batch is the architecture's choice (ADR-0019), and two further constraints point the same way: the partition budget under the sandbox's 60-day expiry, and a reporting grain that is daily throughout. The seam records the exit; it is **not an unfinished feature**.

## Consequences

**One less thing that can be wrong.** No watermark table means no drift between "what we think we loaded" and "what we loaded".

**Day granularity, not timestamp granularity.** The watermark resolves to a partition, so a re-run re-extracts up to a day of rows. That is affordable at daily cadence and is precisely what makes it insufficient for micro-batch.

**A failed load does not advance anything**, so the next run re-selects the same slice with `>=` and self-heals. This is the per-table self-healing that ADR-0024 relies on.

## Alternatives considered

**A watermark table (Approach B).** Timestamp-precise and micro-batch-capable, at the cost of a second state store with a failure mode whose worst case is silent data loss. The right choice *if* sub-daily freshness is ever required — which is why the seam exists.

**`MAX(received_at)` from the staging table itself.** Also self-consistent, but it scans table data — it costs money and, on `orders`, is blocked by the cost fuse. `INFORMATION_SCHEMA` gives the same answer for free.

## Revisit when

Sub-daily freshness becomes a real requirement — the same trigger as ADR-0019.

## Related

- [ADR-0019](./0019-batch-load-not-streaming.md) — the cadence decision this implements
- [ADR-0021](./0021-require-partition-filter-fuse.md) — why reading `INFORMATION_SCHEMA` rather than the table matters
- [ADR-0024](./0024-per-table-load-job-gate.md) — the self-healing this enables
- [ADR-0043](./0043-stg-table-not-view.md) — the dedup that makes `>=` safe
