# ADR-0053: Raw stores the payload as `TEXT`; ODS stores structured fields as `JSONB`

**English** | [繁體中文](../../zh-TW/adr/0053-raw-text-ods-jsonb.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Ingestion — storage |

---

## Context

The same JSON travels through two layers, and PostgreSQL offers three ways to store it:

| Type | Stores | Queryable | Preserves the original bytes |
|---|---|---|---|
| `TEXT` | the exact string | only as text | ✅ |
| `JSON` | validated text | with operators, parsed per query | ✅ |
| `JSONB` | a decomposed binary form | with operators, indexable | ❌ |

The instinct is to use `JSONB` everywhere — it is the type PostgreSQL documentation steers you toward, and it is faster to query.

**That instinct is wrong for one of the two layers**, because of what `JSONB` does on the way in:

> `JSONB` **normalises**. It reorders keys, strips insignificant whitespace, and **collapses duplicate keys, keeping only the last**.

## Decision

| Column | Type | Because |
|---|---|---|
| `raw.raw_payload` | **`TEXT`** | Raw records what arrived, byte for byte |
| `ods.items` | **`JSONB`** | queried and joined downstream |
| `ods.clean_error_message`, `schema_drift_message`, `unmapped_fields` | **`JSONB`** | queried by the quality layer |

**The asymmetry is the decision.** The same data, two layers, opposite requirements.

## Why Raw must be `TEXT`

**① "Raw records every inbound request as-is" would otherwise be false.** ADR-0001 rests on Raw being a faithful record. A normalised payload is not the payload that arrived — it is a payload that agrees with the one that arrived on everything except the things normalisation removed.

**② Proposal C's promise depends on it.** "Raw kept verbatim enables rebuilding" ([ADR-0032](./0032-bounded-writeback.md)) is the backing for the entire correction path. Rebuilding from a normalised payload reproduces the normalisation, not the original.

**③ Duplicate keys are evidence, and `JSONB` destroys it.** `{"age": 30, "age": 31}` is legal JSON that an upstream bug can emit. `TEXT` keeps both; `JSONB` silently keeps `31` and the fact that the upstream contradicted itself is gone — **with no error, and no way to discover it later.**

**④ Drift detection reads the verbatim payload.** `detect_schema_drift` deliberately bypasses Pydantic and inspects the raw string, so it can record the **pre-coercion true type** ([ADR-0054](./0054-type-declaration-governance.md)). Key order and duplicates are part of what it may need to see.

## Why ODS must be `JSONB`

`items` is read by `int_order_items` on every run and flattened to item grain. With `TEXT`, every read pays a parse; with `JSONB` the structure is already decomposed and can be indexed with GIN if it ever needs to be.

ODS is the queryable layer. Raw is not — `GET /raw/{id}` returns the payload for a human to look at, and nothing joins on it.

## Consequences

**Raw costs more disk** — an uncompressed string with its original whitespace. Accepted: Raw is the cheapest place in the system to be wasteful, and the alternative gives up the property it exists for.

**Raw cannot be queried with JSON operators without a cast.** That is not a loss. Anything that wants to query the payload is asking a question ODS should answer, and reaching into Raw for it would create a second path to the same data.

**ODS loses the original byte form** — which is fine, because Raw has it, and `ods.raw_id` is the 1:1 edge back to it.

**A NUL byte still has to be stripped before the Raw write.** PostgreSQL `TEXT` cannot store `0x00` regardless of this decision — see [ADR-0006](./0006-nul-byte-fast-fail.md). `TEXT` preserves everything PostgreSQL is *able* to store, which is not quite everything.

## Alternatives considered

**`JSONB` everywhere.** Faster queries on Raw that nobody makes, in exchange for the verbatim property, the duplicate-key evidence, and Proposal C's backing.

**`TEXT` everywhere.** Keeps Raw correct and makes every downstream read of `items` pay a parse — for a column read on every `int_` rebuild.

**`JSON` (the non-binary type) for Raw.** Preserves the text *and* validates it as JSON. Rejected: validation at the storage layer is exactly what the landing layer must not do. A payload that is malformed JSON is still a record of what the upstream sent, and Raw must accept it so that `process.py` can classify the failure into a terminal state rather than the write failing.

## Related

- [ADR-0001](./0001-raw-no-business-dedup.md) — the "record what arrived" property this protects
- [ADR-0032](./0032-bounded-writeback.md) — Proposal C's promise, which rests on this
- [ADR-0054](./0054-type-declaration-governance.md) — drift detection reads the verbatim payload
- [ADR-0006](./0006-nul-byte-fast-fail.md) — what `TEXT` still cannot hold
