# ADR-0020: Partition on `received_at` — and it means two different instants in Raw and ODS

**English** | [繁體中文](../../zh-TW/adr/0020-partition-on-received-at.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cloud extraction |

---

## Context

The `orders` staging table is partitioned by day on `received_at`, and clustered on `order_id` + `has_clean_error`. Partitioning by landing time is the standard choice for an append-only staging table: incremental loads touch one partition, and queries that filter by time scan one partition.

The part that needs recording is not the choice. It is that **the same column name means two different instants on two tables**, and at least four downstream mechanisms are built on it.

| Column | Stamped when | Means |
|---|---|---|
| `raw.received_at` | The API writes Raw synchronously in the request path | **Order-receipt time** |
| `ods.received_at` | The worker writes ODS (`process.py` does not carry the Raw value over) | **ODS landing time** |

## Decision

Partition `orders` staging on `ods.received_at` (DAY), cluster on `order_id` + `has_clean_error`.

**The semantics are correct, not a compromise.** What extract moves *is* ODS. Using ODS's own clock as both the partition column and the incremental cursor answers exactly the question "did extract move ODS forward?". Using `raw.received_at` instead would fold the latency of the Raw→ODS hop into the extract check, making **one signal stand for two pipeline segments**.

Clustering follows the access pattern: `order_id` is the join key downstream, `has_clean_error` is the Row Filter's predicate.

## Consequences

**Each timeline covers exactly one hop, and none of them moonlights:**

| Timeline | Answers |
|---|---|
| `ods.received_at` | Did extract move ODS forward? |
| `raw.status='pending'` oldest age | Is the dispatch hop alive? (`raw_pending_watch`, ADR-0039) |
| `raw.received_at` continuity via OTel | Is the upstream still sending? |

**⚠️ A scope boundary that must be known.** When a backlog is flushed by the recovery scan, those rows get an `ods.received_at` of the *catch-up write*. The ingestion gap therefore **does not exist on the ODS timeline at all**. Anything built on `ods.received_at` — partitioning, source freshness, the day boundary in `rpt_quality_events_daily` — only ever sees outages **still in progress at sampling time**, never ones that already recovered.

**⚠️ An easy-to-get-wrong criterion, spelled out.** "A Raw row with no matching ODS row" **cannot** be the definition of a fault. Raw's terminal states are `processed` / `duplicate` / `error`, and the latter two produce no ODS row *by correct behaviour*. That definition would alarm on every duplicate order. `pending` is the clean signal.

**⚠️ The name reads like receipt time, and it is not being renamed.** A rename is a migration that ripples into the `FIELDS` declaration (ADR-0026) and every dbt reference. Whoever reads `ods.received_at` next should treat this record as authoritative rather than inferring from the name.

## Alternatives considered

**Carry `raw.received_at` into ODS.** The primary objection is semantic, not practical: the current meaning is already the right one for what the column is used for, and changing it is what would make it wrong. The cost — rebuilding and backfilling the table, and shifting the Hard Gate's "latest UTC day partition" scope — is only the secondary reason.

**Add a second timestamp column carrying receipt time.** Would answer both questions, at the cost of two time columns whose difference is meaningful only for rows that went through a backlog. Not taken; the three-timeline split above already covers the questions, each at the layer that owns it.

## Related

- [ADR-0023](./0023-watermark-approach-a.md) — the watermark that reads this partition
- [ADR-0026](./0026-fields-single-source.md) — why renaming ripples
- [ADR-0039](./0039-observation-signals-own-dag.md) — the timeline that covers the hop this one cannot see
- [Cloud layer design](../design/cloud-layer.md) — Gold-layer partitioning, decided separately
