# Verification Records

**English** | [繁體中文](../../zh-TW/verification/README.md)

Each record answers one question: **did the design assumption actually hold?** They are written once and never updated — a record of what was measured, on the day it was measured.

These are not unit tests. A unit test asks *"is this code correct?"*; a verification record asks *"is this **architectural assumption** correct?"* — usually by breaking something on purpose, at a scale CI cannot reproduce.

---

## Index

| Date | Record | Key figure | Overturned something? |
|---|---|---|---|
| 2026-06 | [Cloud extraction, first run](./2026-06-cloud-extract-first-run.md) | fuse blocks unfiltered queries with 400 | — |
| 2026-08-03 | [Ingestion load test](./2026-08-03-load-test-ingestion.md) | 100 workers → ODS count **1** | — |
| 2026-08-05 | [Airflow commissioning](./2026-08-05-airflow-commissioning.md) | 3 DAGs parsed with **no usable `DB_URL`** | — |
| 2026-08-05 | [Proposal B v3 flow-back](./2026-08-05-proposal-b-v3.md) | 15 promoted, second run writes **0** | — |
| 2026-08-10 | [SIGKILL recovery](./2026-08-10-celery-sigkill-recovery.md) | 800 in, 800 out | ⭐ **yes** |
| 2026-08-10 | [Staleness basis](./2026-08-10-staleness-basis-self-collision.md) | self-collisions **2 → 0** | ⭐ **yes** |
| 2026-08-10 | [Rate limit across processes](./2026-08-10-rate-limit-multiprocess.md) | **91** of 100 got through instead of 60 | ⭐ **yes** |
| 2026-08-10 | [Circuit breaker](./2026-08-10-circuit-breaker-before-after.md) | 47 of 48 timed out → p50 **5ms** | — |
| 2026-08-10 | [Bounded scan](./2026-08-10-bounded-scan-120k.md) | 120,000 backlog, **zero** duplicates | ⭐ **yes** |
| 2026-08-11 | [Full-compose rebuild + v4](./2026-08-11-full-compose-rebuild-v4.md) | 7/7 tasks, ~2.5 min | ⭐ **yes** ×2 |
| 2026-08 | [Sandbox partition expiry](./2026-08-partition-expiry-measurement.md) | out-of-range dates **do not fail the build** | ⭐ **yes** |
| 2026-08 | [Partition savings](./2026-08-partition-savings.md) | clustering alone prunes **82%**; partitioning adds 9 | ⭐ **yes** |
| 2026-08-12 | [Proposal B v2→v3→v4](./2026-08-12-proposal-b-v2-to-v4.md) | 16 + 15 promoted, all idempotent | — |
| — | [`raw_id` collides across ODS instances](./2026-08-raw-id-collision-two-ods.md) | two unrelated orders deduped into one | — |

---

## The "overturned something" column

Six records changed a conclusion that had already been written down. That column exists because **a verification that can only confirm is not worth much** — the value is in the ones that came back with a different answer:

| Overturned | Was | Is |
|---|---|---|
| SIGKILL recovery | "150 records stuck forever, no automatic recovery" | zero loss — but the queue alone cannot do it |
| Staleness basis | CAS was assumed sufficient | CAS cannot stop a third party reverting the state |
| Rate limiting | `60/minute` | `60 × workers`, silently |
| Bounded scan | batch publishing was assumed to be the win | it is 1.1× — pagination is the win |
| Partition expiry | "out-of-range dates fail the build" | they land silently in `__UNPARTITIONED__` |
| `--expect-rule-version` | assumed to catch version divergence | only compares within its own process |

---

## Format

```
## What was being verified    the design assumption under test
## Environment                versions, data volume, where
## Method                     reproducible steps
## Observed                   figures, tables, before/after
## Conclusion                 holds / does not hold / holds conditionally
## What this overturned       ⭐ or "nothing"
## Related                    → ADR, design doc
```
