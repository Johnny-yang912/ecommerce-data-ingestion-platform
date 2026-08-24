# ADR-0017: The recovery scan itself must be bounded

**English** | [繁體中文](../../zh-TW/adr/0017-bounded-recovery-scan.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue — recovery |

---

## Context

The circuit breaker (ADR-0014) does its job: during a broker outage, ingestion continues at full speed. **Which means `pending` accumulates at full speed too.**

The scan's original implementation was "select every `pending` row, dispatch one by one". Under an outage that produces hundreds of thousands of rows, that loads the whole set into a Python list and iterates. **It did not remove the collapse — it relocated it from the ingestion path onto the recovery path**, which is worse, because the recovery path is what is supposed to clean up afterwards.

There is also a non-obvious trap in the naive fix. Adding `LIMIT` alone does not work: **dispatching does not change `status`.** The next page runs the same query against the same data and re-fetches the same leading rows, forever.

## Decision

Five bounds, each closing a different failure:

| Bound | Value | Closes |
|---|---|---|
| Page size | `SCAN_BATCH_SIZE = 5000` | Unbounded memory |
| Cursor | `WHERE id > :last_id ORDER BY id` | The `LIMIT`-alone re-fetch loop |
| Rounds per run | `SCAN_MAX_ROUNDS = 20` | One task monopolising a worker slot |
| Redis lock | key + 300s TTL | Two scans overlapping |
| Grace period | `PENDING_GRACE_SECONDS = 60` | Competing with the ingestion fast path |

The per-run cap is `20 × 5000 = 100,000`. Whatever is left over waits for the next tick — the scan is periodic by nature, and finishing in one pass was never a requirement.

**The lock is released with a Lua compare-and-delete**, not `GET` then `DEL`. Those two are not atomic: between them the lock can expire and be acquired by someone else, and the delete would then remove *their* lock — letting two scans run at once, which is the thing the lock exists to prevent.

The lock TTL is one scan interval: if a task dies without releasing it, at most one tick is lost.

## Consequences

**Verified against a 120,000-row backlog:**

| Scan | Dispatched |
|---|---|
| #1 | 100,000 (the per-run cap) |
| #2 | 20,000 |

ODS grew by exactly 120,000, with zero duplicates.

**The scan is now deliberately imprecise, and that is safe.** It may re-dispatch a record that is already queued. CAS (ADR-0004) makes the loser return immediately, so the cost is a wasted worker slot and a DB round trip, never a double write. Bounding memory and dispatch volume was the goal; exactness was not.

**⚠️ One bound is still missing: query cost.** `raw.status` has no index, so pagination bounds how much is loaded and dispatched, but not how much the database scans to find it. See ADR-0018.

## A related discipline: where the thresholds live

`STALE_PROCESSING_MINUTES` and `PENDING_GRACE_SECONDS` live in their own module, `recovery_policy.py`, rather than in `process.py` where they started.

The reason is a real incident. `check_raw_pending.py` is a **read-only probe** that derives its alert threshold from these constants rather than hardcoding one. It imported them from `process.py` — and therefore inherited the entire write path's dependency tree. When OTel was added, `process.py` gained `from telemetry import ...`, and the probe died at `ModuleNotFoundError: No module named 'opentelemetry'` **before checking anything**, because Airflow's `venvs/analytics` image had not been rebuilt.

> A single shared constant had coupled a probe to a code path it never executes.

Extracting the constants into a dependency-free module fixed it at the root; `tests/test_script_deps.py` pins it so it does not depend on anyone remembering.

They are deliberately **not** in `config.py`: that module's boundary is environment values only, and these are program behaviour (ADR-0008). `scan_interval_seconds` genuinely is environment config and stays there — so threshold derivation correctly reads from both.

## Alternatives considered

**`LIMIT` with no cursor.** The re-fetch loop described above.

**Mark rows `queued` when dispatched.** Would make "already enqueued" visible in the database and let the scan skip them — a cleaner design, and a larger one: it adds a state to the machine and a new failure mode (rows stuck in `queued` after a failed push, needing their own sweep). Documented as the direction if the queue is ever revisited.

**Drain everything in one run.** Monopolises a worker slot for an unbounded time and makes the lock TTL impossible to size.

## Related

- [ADR-0014](./0014-circuit-breaker-dispatch.md) — the decision that made this load arrive
- [ADR-0018](./0018-raw-status-no-index.md) — the bound that is still open
- [ADR-0015](./0015-staleness-from-processing-started-at.md) — the other correction to this scan
