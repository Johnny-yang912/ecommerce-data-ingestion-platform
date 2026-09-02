# 2026-09-02 — Worker scale-out: the ratio, the cost, and whether CAS ever fired

**English** | [繁體中文](../../zh-TW/verification/2026-09-02-worker-scale-out.md)

---

## What was being verified

Conclusion 8 of [sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md), written the same day, left three claims unmeasured. This record tests each:

1. **"The CAS in `try_claim_raw` lets workers scale horizontally with no coordination."** Until now this was a design argument only ([ADR-0004](../adr/0004-cas-claim-rowcount.md)) — **no measurement had ever run more than one worker against the same queue.**
2. Conclusion 4 of the earlier record says "the ceiling is the worker, not the API." So **does adding workers raise the ceiling, and by how much?**
3. Conclusion 8 says the worker slowing from 270 to 186 during a burst was "the API stealing CPU." That was **inferred from a correlation** (the rate recovers the moment injection stops). What happens if the same variable is manipulated instead of observed?

⚠️ **No code changed.** The only variables are `celery worker --concurrency` and the container count — both are launch parameters.

## Environment

Identical to [sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md):

| Item | Setting |
|---|---|
| Host | WSL2, 16 cores. Load-test clients share the machine with the services |
| API under test | `api-api:latest` (the **post-change** image, `def create_order`), started with `docker run` on port 8001 |
| API parameters | `UVICORN_WORKERS=4`, `POOL_SIZE=3`, `MAX_OVERFLOW=5`, OTel off, rate limiting off |
| Worker | `api-worker:latest`, `POOL_SIZE=2`, `MAX_OVERFLOW=2` (same as compose); **container count and `--concurrency` are this record's variables** |
| Services kept up | Only `db` / `redis`. api / worker / **beat** / otel-collector and the whole Airflow overlay stopped |
| Load | 4 client processes, `order_id` offset per process, 15,000 each = 60,000 per round. 11 throughput rounds = **660,000 records** |

⚠️ **beat must be stopped.** `scan_and_dispatch_task` re-dispatches the pending backlog page by page; with a backlog of 30,000+ a single tick emits tens of thousands of duplicate messages, corrupting both the drain rate and the CAS contention being measured.

## Method

### Method 1: burst load (a replication of test H)

The same script as test H in [sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md), run **alternating** W1 (1 container × 4) → W2 (2 containers × 4) → W1 → W2 to keep machine drift out of the comparison. `redis-cli -n 0 flushdb` before each round; `DELETE FROM ods` → `DELETE FROM raw` (FK order) between rounds.

### ⭐ Method 2: pure drain — why it was unavoidable

**Method 1 breaks down with two workers, and the breakdown is itself a finding.**

W2 drained at 360.6 records/s while injecting at 361.4 — nearly equal. The **worker was never saturated**, and the backlog was gone before injection ended. Two consequences:

- 360.6 is a **lower bound, not a ceiling**; it cannot produce a ratio.
- The clean primary metric originally planned ("drain rate after injection stops," measured in a window where the API and clients are idle) **does not exist under W2.**

Hence a second method: **stop all workers → fill the queue with 60,000 records → stop the API → only then start workers, and time the drain.** Zero API load, zero client load: what is measured is the true ceiling of worker + Postgres. Container start-up (~5 s from `docker run` to the first task) is reported both ways.

### ⭐ Method 3: CAS contention injection

**Methods 1 and 2 never put CAS to work** — dispatch is point-to-point, so two workers never contend for the same row. Verifying CAS requires manufacturing the contention:

With workers stopped, create 5,000 pending records → `flushdb` to discard the messages the API dispatched → **publish 4 duplicate messages per `raw_id` directly** (bypassing `main._enqueue` and `scan_and_dispatch`, since both paths exist precisely to avoid this situation) → start 2 containers × 8 children → count claim events from the worker logs.

## Observations

### Test A — burst load, five alternating rounds

| Metric | **W1** (4 children), 2 rounds | **W2** (8 children), 2 rounds | **C8** (1 container ×8) |
|---|---:|---:|---:|
| Injection rate | 447.7 / 550.4 (mean **499**) | 361.4 / 370.3 (mean **366**) | 368.0 |
| Drain during injection | 211.9 / 189.8 (mean **201**) | 360.6 / 369.3 (mean **365**) | 365.9 |
| Drain rate after injection | 301.0 / 302.4 (mean **302**) | — **no backlog left to drain** | — |
| **Peak backlog** | 33,037 / 39,605 (mean **36,321**) | 6,420 / 9,383 (mean **7,902**) | 8,944 |
| Seconds to drain after injection | 105 / 130 | **0 / 0** | **0** |
| **End-to-end, all 60,000 in ODS** | **239 / 239 s** | **166 / 162 s** | 163 s |
| Peak pg connections | 27 / 27 | 29 / 31 | 30 |
| ODS landed / errors | 60,000 / **0** | 60,000 / **0** | 60,000 / **0** |

### Test B — baseline replication ⭐

W1 is test H re-run in the same configuration. **Every figure brackets the original:**

| | Test H original | W1 here, 2 rounds |
|---|---:|---:|
| Peak backlog | 36,526 | 33,037 / 39,605 (mean 36,321) |
| Drain rate after injection | 299 | 301.0 / 302.4 |
| Seconds to drain after injection | 119 | 105 / 130 |
| Injection rate | 487.8 | 447.7 / 550.4 |

**Without this row, everything that follows is just two numbers from two different days.**

### Test C — the pure-drain scaling curve ⭐

Zero API load, zero client load; two rounds each:

| Children | Configuration | Pure drain (start-up excluded) | vs 4 | **Per child** | Peak pg connections |
|---:|---|---:|---:|---:|---:|
| 4 | 1 container × 4 | 309.2 / 298.5 (mean **303.9**) | 1.00× | 76.0 | 7 |
| 8 | 2 containers × 4 | 512.8 / 517.2 (mean **515.0**) | **1.69×** | 64.4 | 10 / 11 |
| 16 | 4 containers × 4 | 789.4 / 789.4 (mean **789.4**) | **2.60×** | **49.3** | 18 |

The two 16-child rounds are identical (76 s each) — no round-to-round drift.

**Three independent measurements agree**: pure drain 303.9, test A's W1 post-injection drain 302, test H's original 299 — within 1.6%. That also confirms method 1's post-injection window was genuinely clean.

### Test D — actual connection usage ⚠️

| Children | Budgeted | **Measured peak** | Usage |
|---:|---:|---:|---:|
| 4 (1 container) | 16 | **7** | ≈ 4/16 after subtracting ~3 baseline connections |
| 8 (2 containers) | 32 | **10–11** | ≈ 8/32 |
| 16 (4 containers) | 64 | **18** | ≈ 16/64 |

**Connections ≈ children + 2–3, independent of the configured pool size.**

### Test E — end-to-end correctness

11 rounds × 60,000 = **660,000 records**. Every round:

```
raw processed  = 60000        ODS landed        = 60000
distinct order_id = 60000     raw error message = (none)
```

`ods.raw_id` and `ods.order_id` **both carry UNIQUE constraints**, so any double write becomes an IntegrityError and pushes that record into the `error` terminal state. **Zero errors = no double write ever reached the database.**

⚠️ But this is **not the same as "CAS blocked the contention."** A failed claim at `process.py:99` writes one WARNING line and returns — **no ODS row, no status change, no terminal state.** Every field this test measures is blind to whether CAS ever fired. Zero errors is equally consistent with "CAS blocked contention" and with "contention never happened." Only test F can tell them apart.

### Test F — CAS contention injection ⭐

5,000 records × 4 duplicate messages = 20,000 messages, 2 containers × 8 children:

| | Measured | Expected |
|---|---:|---:|
| **Claim failures** | **15,000** | 15,000 (= 5,000 × 3) |
| **Processed** | **5,000** | 5,000 |
| Claim DB exceptions | 0 | 0 |
| "claim succeeded but record not found" | 0 | 0 |
| ODS landed / errors | 5,000 / **0** | 5,000 / 0 |

Both containers won and lost (failures 7,663 / 7,337; successes 2,471 / 2,529). The chance that all 4 copies of one record land in the same container is ~1/8, so **roughly 87% of records experienced cross-container contention.**

**Mechanism**: when two workers issue `UPDATE ... WHERE id=? AND status='pending'` concurrently, the later one blocks on the row lock; once the first commits, Postgres re-evaluates the predicate under READ COMMITTED → `rowcount = 0` → `try_claim_raw` returns False. **The outcome does not depend on timing, which is why the count is exactly 15,000 rather than a probabilistic figure.**

⚠️ **Instrument trap: grepping the logs for non-ASCII text silently matches nothing.** structlog's `JSONRenderer` defaults to `ensure_ascii=True`, so `claim 失敗` appears in the log as `claim \u5931\u6557\uff0c...`. The first count therefore returned "claim failures = 0" — indistinguishable from "contention never happened," and with no error. The events must be `json.loads`-ed before matching. (The same trap bit twice: the Python script written to fix it put `\uXXXX` inside a non-raw docstring and died with `SyntaxError`.)

## Conclusions

### 1. On the normal path contention never happens — and that is not CAS's doing

End-to-end correctness under multiple workers holds: up to 16 children across 4 containers, 660,000 records, zero duplicates, zero loss, nothing stuck. **This had never been measured** — the horizontal-scaling claim in [ADR-0004](../adr/0004-cas-claim-rowcount.md) and conclusion 8.3 had never been exercised with more than one worker.

⭐ **But the credit belongs to the shape of dispatch, not to CAS**: one message names one `raw_id`, and Celery delivers each message to exactly one consumer. In tests A–E **CAS almost certainly never fired once.**

⚠️ A failed claim leaves no trace in the database, so "zero errors" is blind to whether CAS ever arbitrated anything. **The two statements must be kept separate** — which is exactly why test F exists.

### 2. When contention does happen, CAS blocks it exactly — 15,000 / 15,000 ⭐

Test F manufactures 4× duplicate dispatch: claim failures exactly 15,000, processed exactly 5,000, ODS exactly 5,000, zero errors. **The `rowcount == 1` predicate is deterministic under cross-container concurrency, not probabilistic** — the row lock plus READ COMMITTED re-evaluation guarantees it.

In the real system this path is reached through `scan_and_dispatch` re-dispatch and `POST /process_raw/{raw_id}?force=true`. This record substitutes manual duplicate dispatch because neither of those fires on demand.

### 3. Workers do scale horizontally, but sub-linearly: 4× the children buys 2.60×

The gain per doubling decays: **1.69× → 1.53×**, and per-child throughput falls from 76.0 to 49.3.

**But the curve has not flattened.** That decides the next conclusion.

### 4. The bottleneck is CPU, not database concurrency

If the ceiling were Postgres, the 8→16 step would barely move; it still yields 1.53×. Three pieces of evidence point the same way:

1. **pg connections scale exactly linearly with children** (7 / 10–11 / 18) — a prefork child runs one task at a time and holds its connection synchronously, so there is **no connection queuing and no pool contention.**
2. **660,000 records, zero errors** — no sign of lock waits or write conflicts.
3. **Per-child throughput decays monotonically**, the signature of CPU oversubscription: 16 worker children plus Postgres backends plus Redis cannot each get a full core on a 16-core box.

⚠️ **Precisely stated, what is ruled out is a Postgres *concurrency* limit (locks, connections, write contention), not Postgres CPU consumption.** Worker and database CPU live on the same machine, and this method cannot separate them.

### 5. CPU is zero-sum: double the workers, the API accepts ~30% fewer

| | Injection rate |
|---|---:|
| No workers running (6 samples) | ~534 (range **441–577**, noisy) |
| 4 children | 499 |
| **8 children** | **366** |

Conclusion 8 said "the API steals the worker's CPU"; **the reverse holds too** — and this time by manipulating the variable rather than observing a correlation.

⚠️ **The "4 children cost the API 12%" step falls inside the noise and does not stand.** Only the 8-child step (~ −30%) exceeds the range, and it comes from the alternating W1/W2 pairs, the arrangement least exposed to machine drift.

### 6. End-to-end time drops 31% — the one metric needing no attribution

**239 s → 164 s.** Peak backlog, injection rate and drain rate are each contaminated by CPU redistribution (intake slows and drain speeds up simultaneously), but "how long until all 60,000 are in ODS" absorbs all three at once.

### 7. ⚠️ The worker connection budget is ~4× over-provisioned — do not cut it on that basis

Each child actually uses about one connection (test D). The budget is `POOL_SIZE=2 + MAX_OVERFLOW=2` = 4 per child, 16 per container.

This is the same shape as the API pool **before** the endpoint fix (see test 2 in [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md)): no yield point inside the connection-holding window, so one execution unit holds at most one connection. The difference is that **the API's cause was later removed, whereas the worker's cause is the prefork model itself and will not change.**

⚠️ Even so, **do not shrink the budget because of this** — that is precisely the trap recorded in conclusion 5 of [sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md): the previous "measured usage is 4, the budget can drop to 8" recommendation started causing 503s once the premise behind it was removed. **Headroom exists for the abnormal case**: when the database slows down, each child holds its connection longer and demand rises.

### 8. Container boundaries do not affect scalability

C8 (1 container × concurrency 8) and W2 (2 containers × 4) land in the same noise band (injection 368.0 vs 361–370, drain 365.9 vs 360–369, peak backlog 8,944 vs 6,420–9,383).

**The real variable is the total number of children; the container count contributes nothing by itself.** Prefork children are separate processes and share no GIL — which is the mechanism that makes the horizontal-scaling claim true, and it can be verified on a single machine.

**Updated capacity statement (always quote it together with the environment):**

> Measured on a single-host development machine (16 cores, with DB / Redis / worker / load-test clients all co-resident, rate limiting off): the Celery worker drains at 304 records/s with 4 children, 515/s with 8, and 789/s with 16 — sub-linear but still climbing (1.69× then 1.53× per doubling), bounded by single-host CPU oversubscription rather than database concurrency. During a burst the API and the worker compete for CPU: with 8 children, API intake falls from about 534 records/s to 366. End-to-end time for a 60,000-record burst to land fully in ODS drops from 239 s with 4 children to 164 s with 8; peak backlog falls from 36,321 to 7,902; both configurations produced zero errors and zero lost records.

## ⚠️ What this record does not say

- **These are not production figures.** Single host, 16 cores, clients co-resident with the services, ~17,000 rows of baseline data.
- **The worker ceiling was not located.** The curve is still rising at 16 children; 32 was not measured.
- **Worker CPU and Postgres CPU cannot be separated here.** Doing so requires the database on another host, or `--cpus` limits on the containers.
- **Injection rate is very noisy** (range 441–577 with no workers running). No 10%-level difference in injection rate is quotable from this record.
- **The two real sources of contention — `scan_and_dispatch` and `/process_raw?force=true` — were not exercised directly.** Test F reproduces the *situation* they create through manual duplicate dispatch, not their *code paths*.
- **No fault injection.** Recovery when one of several workers is SIGKILLed mid-flight is untested — [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) only ever ran a single worker.
- **Rate limiting was off throughout.** Production is `60/minute`.

## What this fills in

⭐ **This record overturns nothing.** It closes three kinds of gap, each corresponding to a different way an earlier claim was not yet solid:

- **Design argument → measurement**: the claim was written in an ADR, but no measurement had ever run more than one worker.
- **Correlation → manipulating the same variable**: what was an observation ("the rate recovers the moment injection stops") is now a reverse manipulation of the worker count, with a symmetric result.
- **An unqualified number → its missing premise**: the figure was not wrong; what was wrong was quoting it without the worker configuration.

Exactly one item counts as a correction, and it corrects **the reason, not the conclusion**: horizontal scaling is still safe, but the credit belongs elsewhere.

| Subject | What it was | What this record adds | Does the claim still hold |
|---|---|---|---|
| [sync-handlers](./2026-09-02-sync-handlers-before-after.md) conclusion 8.3<br>"the CAS in `try_claim_raw` lets workers scale horizontally with no coordination" | A design argument ([ADR-0004](../adr/0004-cas-claim-rowcount.md)); **no measurement had ever run more than one worker** | Tests A–E: up to 4 containers × 16 children, 660,000 records, zero duplicates and zero loss. Test F: 4× duplicate dispatch injected on purpose — 15,000 claim failures, 5,000 processed | ✅ Holds. ⭐ **But the reason changes**: on the normal path CAS almost certainly never fired; the credit belongs to point-to-point dispatch. CAS is the deterministic guarantee **for when contention actually happens** |
| Same record, conclusion 8's backlog figures<br>"peak backlog 36,526, fully drained 119 s after injection" | Correctly measured, but **with no worker configuration attached** | Those are the 1 container × 4 children values; with 8 children they are **7,902 and 0 s** | ✅ The original figures are unchanged. What is added is the citation condition: **a backlog figure must be quoted together with the worker configuration** |
| Same record, conclusion 8<br>"the worker slows during a burst because the API steals CPU" | **Inferred from a correlation** — the rate recovers the moment injection stops | The same variable manipulated in reverse: going from 4 to 8 children drops API intake from 499 to 366 (~ −30%) | ✅ Holds, and is upgraded from an observation to a manipulation of the same variable |
| Same record, conclusion 8.3<br>"deploy or scale the worker separately and the contention disappears" | An inference | Test C measured 304 / 515 / 789 records/s under zero API load, confirming the contention is an artefact of sharing one machine | ✅ Holds |
| [ingestion-capacity](./2026-09-02-ingestion-capacity-and-bottlenecks.md) conclusion 4<br>"the ceiling is the worker, not the API" | An observation, with **no measurement of whether that ceiling can be bought up** | The ratio: 4× children → 2.60×, sub-linear but still climbing at 16; bounded by single-host CPU rather than database concurrency | ✅ Holds, and **the ceiling is purchasable** |
| Same record, conclusion 8's extrapolation table<br>"Worker horizontal scaling / Yes / basis: CAS" | That table **states outright that none of it was verified** | The first of its three axes to be measured | ✅ Holds (sub-linearly). ⭐ **The basis column changes**: the mechanism is point-to-point dispatch, with CAS as the backstop under contention |

⚠️ **How `raw_pending_watch` should be read depends on the worker configuration.** It measures how long the oldest record has been waiting, not how many are waiting, so it will not misfire; but the *order of magnitude* of the backlog differs by 4.6× between 4 and 8 children.

## Related

- [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) — this record supplies the worker configuration its conclusion 8 backlog figures were missing, and turns its CAS scaling claim from a design argument into a measurement (with one attribution corrected)
- [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) — this record supplies the ratio its conclusion 4 lacked, and measures the worker axis of its conclusion 8 extrapolation table
- [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) — single-worker recovery; the multi-worker version is still unverified
- [design/queue](../design/queue.md) · [ADR-0004](../adr/0004-cas-claim-rowcount.md)
