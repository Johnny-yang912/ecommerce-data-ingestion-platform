# Ingestion Layer

**English** | [繁體中文](../../zh-TW/design/ingestion.md)

The path from `POST /orders` to a row in ODS. Decisions live in the [ADRs](../adr/README.md); this describes how it works.

---

## 1. The state machine

```
pending ──try_claim_raw()──► processing ──► processed
                                        ├─► error
                                        └─► duplicate
```

`processed`, `error` and `duplicate` are terminal. Only `error` and `duplicate` are replayable, via `POST /process_raw/{id}?force=true`.

| Column | Set by | Answers |
|---|---|---|
| `raw.received_at` | API, in the request path | how long has this **data** been lying around |
| `raw.processing_started_at` | `try_claim_raw`, on a successful claim | how long has **this attempt** been running |

The two are never interchanged — that distinction is [ADR-0015](../adr/0015-staleness-from-processing-started-at.md).

**Invariant:** `status = 'processing'` ⇒ `processing_started_at IS NOT NULL`, guaranteed because `try_claim_raw` is the only path into `processing`.

---

## 2. Request path

```
X-API-Key → verify_api_key()          401 on missing/invalid; client_id → request.state
    ↓
slowapi rate limit                    keyed on client_id, counters in Redis db 1
    ↓
OrderIN validation                    malformed → 422 at the boundary
    ↓
raw_body.replace("\x00", "")          strips actual NUL bytes so the Raw write can succeed
    ↓
INSERT raw (retry ×3)                 pool exhaustion → 503
    ↓
COMMIT
    ↓
_enqueue(raw_id)                      circuit-broken; swallows all failures
    ↓
200 {"status": "pending"}
```

**The response is `200 pending` even if the dispatch failed.** The Raw row is committed; returning `500` would make the client resend and manufacture duplicates for an order that was in fact accepted. [ADR-0013](../adr/0013-bounded-broker-wait.md)

Rate limits are **per authenticated client, with no global limit** — a global cap would let one noisy upstream deny service to every other:

| Endpoint | Per-client limit | Reason |
|---|---|---|
| `POST /orders` | 60/min | the ingestion hot path; sized above expected upstream cadence so a legitimate burst is not throttled |
| `POST /process_raw` | 20/min | a manual rescue path — a human replaying records, not a machine. A high rate here means someone is looping, which is a mistake worth surfacing |
| `GET /raw` | 120/min | read-only inspection; cheap, so the limit only exists to bound accidental polling |

Counters live in Redis db 1, not process memory — see [ADR-0016](../adr/0016-recovery-scan-in-beat.md).

---

## 3. Processing path

```
try_claim_raw()          CAS: UPDATE ... WHERE id=? AND status='pending'
    ↓                    rowcount != 1 → someone else has it, return immediately
json.loads
    ↓
ODSOrder.from_nested()   flatten the nested payload
    ↓
clean_order()            → (ods, has_clean_error, clean_error_message)
detect_schema_drift()    → (has_schema_drift, message, unmapped_fields)
    ↓
pre-check ODS.order_id   hit → duplicate, do not write
    ↓
COMMIT ODS + quality_events + raw.status='processed'   ← one transaction
```

Two independent, parallel, non-blocking signals — never mixed:

| Signal | Means |
|---|---|
| `has_clean_error` | **values** violated business rules |
| `has_schema_drift` | the **upstream contract changed shape**; unknown fields land in `unmapped_fields` |

Neither aborts the ODS write. [ADR-0002](../adr/0002-has-clean-error-non-blocking.md)

---

## 4. Four retry points

| # | Where | On exhaustion |
|---|---|---|
| 1 | Raw write | `503` to the client |
| 2 | CAS claim | `error` |
| 3 | Processing | `error` |
| 4 | Status commit | `CRITICAL` log — the record may be stuck in `processing`, recovered by the scan |

All use exponential backoff. Counts (`MAX_*_RETRIES = 3`) live at the top of `process.py`, not in config — they are program behaviour, not environment. [ADR-0008](../adr/0008-config-boundary.md)

### What retry handles, and what it does not

| Failure | Handled by |
|---|---|
| Transient DB error, connection blip | retry |
| Deterministic error (`DataError`, `ValueError`/NUL) | **fast-fail to `error`** — retrying a deterministic error is how a poison pill is made ([ADR-0006](../adr/0006-nul-byte-fast-fail.md)) |
| Duplicate `order_id` (`IntegrityError`) | **no retry** → `duplicate` |
| Worker killed mid-processing | the recovery scan ([queue](./queue.md)) |
| Broker unavailable | circuit breaker + recovery scan |

---

## 5. Idempotency

Two constraints, two jobs:

| Constraint | Guarantees |
|---|---|
| `UNIQUE(ods.order_id)` | one row per business order |
| `UNIQUE(ods.raw_id)` | one Raw row produces at most one ODS row — a 1:1 lineage edge |

First-write-wins, enforced twice: a pre-check to avoid pointless work, and an `IntegrityError` backstop for the TOCTOU race. **The constraint is the guarantee; the pre-check is an optimisation.** [ADR-0005](../adr/0005-first-write-wins-idempotency.md)

---

## 6. Timeouts and pool

| Setting | Value | Purpose |
|---|---|---|
| `statement_timeout_ms` | 30000 | prevents a lock-wait hang |
| `pool_size` / `max_overflow` | 5 / 10 | 15 concurrent connections |
| `pool_timeout` | 30s | exhaustion raises `SATimeoutError` → caught → `503` |

`/process_raw` is a background task rather than inline work, so it cannot block the event loop.

⚠️ `_enqueue()` is synchronous and can block for its full timeout — every async caller must wrap it in `asyncio.to_thread`.

---

## 7. Two identities, and lineage

### `raw_id` is physical identity; `order_id` is business identity

Every row carries two identifiers that answer different questions, and **conflating them breaks a different thing at every layer**:

| | `raw_id` | `order_id` |
|---|---|---|
| Answers | *which ingestion event was this* | *which real-world order is this* |
| Assigned by | this system, on write | the upstream, in the payload |
| Unique in Raw | ✅ (primary key) | ❌ **deliberately not** — ADR-0001 |
| Unique in ODS | ✅ `UNIQUE(ods.raw_id)` | ✅ `UNIQUE(ods.order_id)` |
| Used for | 1:1 lineage; physical dedup in `stg_` | business dedup; joins across the warehouse |

The two `UNIQUE` constraints on ODS are therefore not redundant. They say different things:

- `UNIQUE(ods.raw_id)` — **one ingestion event produces at most one ODS row.** A lineage edge.
- `UNIQUE(ods.order_id)` — **one real order exists once.** A business invariant.

And that lineage edge is **carried the whole way down**. `raw_id` starts as Raw's primary key and is still there through ODS, staging, `stg_`, `int_`, all the way to `fct_orders` (where it is no longer a key, only a lineage column); `quality_events` records every state transition against it too. **Every stage of this record's life, from birth to end, hangs off the same identifier — any row at any layer can walk back to its own verbatim payload.**

So what it holds up is more than a join: `force=true` replay needs it to know which ingestion event to replay; [Proposal C](../runbooks/proposal-c-correction.md)'s premise of "re-derive values from Raw" does not stand without it; and [ADR-0053](../adr/0053-raw-text-ods-jsonb.md)'s promise that "Raw kept verbatim enables rebuilding" is redeemed through it — **a payload you cannot walk back to is a payload you did not keep.** The `FK → raw.id` (`NO ACTION`) turns *"we assume raw is there"* into *"the database guarantees raw is there"*, and with it requires that **Raw outlive its ODS row**.

> `raw.order_id` can be NULL — a payload that never parsed has no business identity at all, yet it still has `raw.id`, and it still has to be claimed, processed, and counted. **Business identity is a property of the data; physical identity is a property of the event — and the ingestion layer records events.**

Physical dedup uses physical identity, which is why `stg_orders` partitions its window function on `raw_id` and not on `order_id` ([Transformation design §2](./transformation.md)).

> ⚠️ **`raw_id`'s uniqueness holds only within a single landing instance.** Two ODS instances both start their sequences at 1, and extracting both into one staging table makes `stg_`'s dedup collapse unrelated orders into "copies" of each other — silently. See [verification/2026-08-raw-id-collision-two-ods](../verification/2026-08-raw-id-collision-two-ods.md).

### Source lineage: `source_client_id`

The edge above answers *which ingestion event this row came from*; this one answers *who that ingestion came from*.

The `client_id` resolved from the API key lands as `source_client_id` on both Raw and ODS. Because it comes from the verified key rather than the payload, an upstream cannot claim to be someone else.

**`NULL` is meaningful, not missing**: it marks a row that did not arrive through the authenticated API — a manual replay, a backfill, a direct DB write. Raw deliberately keeps "origin unknown" expressible.

---

## 8. Related

- [ADR-0001](../adr/0001-raw-no-business-dedup.md) · [ADR-0003](../adr/0003-duplicate-terminal-status.md) · [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0007](../adr/0007-static-api-key-not-jwt.md)
- [queue](./queue.md) — dispatch, recovery, degradation
- [data-quality](./data-quality.md) — what `clean_order` judges and how it can change later
