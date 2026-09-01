# Runbook: batch-correcting historical values from Raw (Proposal C)

**English** | [繁體中文](../../zh-TW/runbooks/proposal-c-correction.md)

---

## When this applies

Only when all three answers are yes:

| Question | If no |
|---|---|
| Is `raw.status` = `processed`? | → `POST /process_raw/{id}?force=true` (path A) |
| Is the **value** wrong, rather than the judgement? | → [proposal-b-rollout](./proposal-b-rollout.md) (path B) |
| Is the original value in the Raw payload still correct? | → unrecoverable, [quarantine-writeoff](./quarantine-writeoff.md) |

The typical shape: a sentinel list in the cleaning rules treats `"na"` (North America) as a null, and 8,000 orders over three months have their region washed to NULL. **The pipeline did not fail, the rules did not misjudge — the values are wrong.**

---

## ⚠️ This is not a ready-made procedure

Proposal C is **designed, not built** — none of the parts it needs exist yet. What follows is *"once this has to happen, it happens in this order"*, not a script you can run today.

Deliberately not an HTTP endpoint and not a DAG — the same discipline as `permanently_rejected`:

> **Irreversible decisions should not have a convenient button.**

**This is also the one legitimate escape hatch from the [README](./README.md)'s "do not modify ODS".** What that rule forbids is the *single-row, unversioned, downstream-unaware* rewrite. Batched, versioned, keeping a retired copy, and forcing downstream through — that is this document.

---

## The two shapes

Steps 0–2 and 4–6 are identical for both shapes. They fork only at step 3 — **where the corrected values land**:

- **Migration shape**: replace in place in the main table, moving the old rows to a retired table. Afterwards the main table is the single truth again.
- **Patch shape**: leave the main table untouched, write corrections into a second table, overlay them at read time.

**Which one is not decided here.** Take the numbers from step 1 back to the comparison table and discretion checklist in [design/data-quality](../design/data-quality.md#c-1-the-two-shapes).

---

## Step 0: fix the code before touching any data

Change the cleaning logic that caused this, and deploy it.

If you repair data today without deploying, tomorrow's orders get washed too — the blast window keeps growing to the right and you never catch up. **Deploy first and the window's right edge freezes at that moment**, which is the only way you get a fixed range with both ends.

---

## Step 1: scope the blast window

This is read-only. Nothing is touched; redo it as often as you like.

**The only trap is which timestamp you use.** Use `ods.received_at` (when this row's values were produced), not `raw.received_at` (when the payload arrived). Some orders arrived six months ago, got stuck, and were only reprocessed last month by the recovery scan — their values were produced inside the blast window but their arrival time falls outside it. Scoping by arrival time misses them.

```sql
select id, raw_id, order_id, received_at, <the corrupted column>
from ods
where received_at >= '<window start>' and received_at < '<window end>'
  and <the condition identifying corruption>;
```

You come out with four numbers: **which orders, which columns, how many rows, and the date range.** Every later step uses them, and so does the decision gate.

---

## Step 2: recompute, but write nothing

Take each row's Raw payload and recompute with the fixed logic.

**This is not a pipeline re-run — it borrows exactly two pure functions: `from_nested` → `clean_order`.** The normal entry point `process_raw_event` has a first-write-wins pre-check: it sees that `order_id` already in ODS, marks the very row you are replacing as `duplicate`, and the whole batch dies. You are replacing, not re-ingesting, so you must go around the front door.

**Pass that row's original `received_at` as `as_of`.** Without it, time-dependent rules judge against *now* — that is re-judging, not recomputing (see the `clean_order` docstring in `clean.py`).

That single `clean_order` call returns **both** the corrected values **and** the quality verdict on them — the events in step 3 come from here for free, with no second pass.

**Do not write yet.** Print a diff for a human:

```
raw_id  order_id   column    old     new
1234    ORD-0001   region    NULL    na
...
8,000 rows total
```

While you are here, assert that every `raw_id` still resolves in `raw` — the FK would block it anyway, but catching it now avoids a half-done runbook.

**Up to this point you can walk away. Nothing has happened.**

---

## ⚠️ Decision gate — a human decision

Take step 1's numbers to [design/data-quality](../design/data-quality.md#c-1-the-two-shapes), pick a shape, and **write down why** (step 6 needs it).

There is no default answer here. The scale, what downstream has already consumed, and whether there is an operational window all move the answer.

---

## Parts that must exist before you start

| | Migration shape | Patch shape |
|---|---|---|
| Shared | A batch id (`reprocess_batches` table) — it is also the tie-breaker for cloud-side dedup, **not merely an audit column** | same |
| Tables | `ods.rebuild_batch_id` column, `ods_retired_<batch>` table | `ods_corrections` table (give `raw_id` the same FK) |
| Extraction | none (reuses the existing channel) | its own `FIELDS` declaration, extraction logic, and a guard on par with `test_schema_bq_consistency` |
| Other | a dedicated connection overriding `statement_timeout`; the batch id prepended to `stg_orders.sql`'s dedup tie-break | overlay logic and batch precedence on the read side |

The retired table **must not inherit the FK** (`LIKE ... EXCLUDING CONSTRAINTS`), or the retired copies pin the raw rows.

> **The per-row trace is `rebuild_batch_id`, not a quality event.** Every corrected row carries it, without exception; `quality_events` records only the rows whose **judgement** changed (next step). Two logs, two axes — neither stands in for the other.

---

## Step 3 (migration shape): one transaction, four things together

**This is the irreversible point.** Open one transaction and do four things:

1. Copy the **old** blast-window rows verbatim into `ods_retired_<batch>`
2. Delete those old rows from the main table
3. Write the recomputed rows back, each carrying the batch id
4. Append quality events (using the verdict from step 2)

**These four cannot be split.** If #3 commits and #4 fails, you get rows whose values were swapped while the quality state machine has no idea — downstream decides who to quarantine from a state that no longer matches the values, **and nothing reports an error**. And a backup done separately leaves, if you crash midway, an orphan copy nobody knows whether to trust. Either all four happen or none do.

**On #4**: this is not a human typing SQL. It is the same pattern as ingestion — the data and the event are written together in one commit by the program. The only difference is who presses run.

**What the event says**: reuse the existing `promotion` / `re_quarantination`, and **append only when the state actually changed** (the same rule the ingestion layer follows). What is being fixed here is a value defect we caused; the rules did not move, so what was clean stays clean and what was dirty stays dirty. Most rows do not change state, and writing nothing for them is correct — the event table is itself the idempotency gate. The rows that do produce events are those washed into breaking a rule and restored by the fix (`promotion`), and the rarer reverse (`re_quarantination`). Record the batch id in `reason`.

**Model the implementation on `fetch_current_states()` and `decide_target_state()` in `reevaluate_quality.py` — model on, do not import.** C is a separate script; B's decision logic should not grow a C-shaped exception parameter, or a future change to B's rules silently changes C's behaviour — and C is the path nobody has run in years. Two things must travel with the pattern:

1. **The tie-break must be `(event_at, id)`**, matching `int_`'s `order by event_at desc, id desc`. If it differs, C decides against a state Gold cannot see, and nothing reports it.
2. **The `NON_REPRODUCIBLE_CODES` block does not apply to C.** B treats those as "did not pass" because B's input is already-normalised ODS values, where passing means the evidence disappeared. **C's values are re-derived from Raw — the evidence is back**, and C is the one mechanism that lifts that block. Copying B's judgement verbatim silently skips exactly the rows you came to rescue.

Also, the global 30-second statement timeout is sized for short online transactions and will kill a ten-thousand-row batch — this connection must override it.

> **Rollback**: run another batch in reverse, restoring from the retired table. The same mechanism supports itself; no second path is needed.

---

## Step 3 (patch shape): write to another table, leave the main one alone

Write the recomputed values into `ods_corrections` with the batch id. Quality events are handled exactly as in the migration shape (same transaction, still only on state change, still modelled-on rather than imported).

The main table is never touched, so **there is no irreversible point** — to undo, void the batch.

The cost is that truth now lives in two tables: **the correct value = main table ⊕ corrections**. That seam is permanent, not one-off:

- When the same row is corrected twice, precedence must be implemented by hand (the migration shape gets it free from the main table)
- **After any full staging re-extract, the corrections must be re-pushed** — otherwise the wrong values resurrect
- *"Reading ODS requires applying the overlay"* becomes a new implicit contract, needing documentation and a guard against future direct reads

---

## Step 4: push the corrections to the cloud by hand

Cloud extraction runs off a forward-only watermark: each run picks up only what is newer than last time. The corrected rows **deliberately keep their original timestamps** (that is still when the order arrived; changing it would be a lie).

So that watermark **will never see them**, no matter how long you wait. A human has to push. Cloud-side detail: [design/cloud-layer](../design/cloud-layer.md).

---

## Step 5: tell downstream to recompute those old partitions

The data is in BigQuery staging now, but downstream models are incremental too and only compute the last few days. Corrected rows sit in partitions from three months ago, which routine runs do not see.

```bash
dbt run -s stg_orders --vars '{stg_orders_backfill_start: "<earliest corrected day>", stg_orders_backfill_end: "<latest day + 1>"}'
```

**Use dates, not day counts** (see [dbt-ops](./dbt-ops.md#proposal-c-targeted-refresh)). If quality events also land in old partitions, backfill `stg_quality_events` and `rpt_quality_events_daily` with the same dates.

> ⚠️ **Steps 4 and 5 are a bound pair.** Push without refresh and PostgreSQL holds the new values while Gold still holds the old ones — **and nothing turns red**. That is the most dangerous way to end this procedure: half-done, and looking successful.

---

## Step 6: write it down

The system leaves no trace that a batch of data was corrected by hand. Three months later, when someone asks why that day's numbers do not reconcile with upstream, this record is the only thing that answers.

Minimum set: **which range · rows before · rows after · why · which shape and why.** File it under `docs/en/incidents/`.

---

## Verification

| Check | Expected |
|---|---|
| Corrected values appear in `fct_orders` | yes |
| The old wrong values are in no Gold table | yes |
| Rows whose state changed have an event with the batch id in `reason` | yes |
| Rows whose state did not change have **no** new event | yes — events only on state change |
| Re-running the same correction writes **0** quality events | yes (idempotent, as in [proposal-b-rollout](./proposal-b-rollout.md) step 7) |
| (migration) `ods_retired_<batch>` row count == blast-window row count | equal |
| (patch) the full re-extract runbook now includes "re-push corrections" | yes |

---

## Never

| Do not | Why |
|---|---|
| Re-run through `process_raw_event` | the first-write-wins pre-check marks the row being replaced as `duplicate` and kills the batch |
| Split step 3 across transactions | leaves a "values swapped, state machine unrecorded" crack, and nothing reports it |
| Skip the targeted refresh after pushing | PG and Gold diverge permanently, all green |
| Scope the window by `raw.received_at` | misses rows reprocessed later by the recovery scan |
| Recompute without `as_of` | time-dependent rules judge against *now* — re-judging, not recomputing |
| Import `reevaluate_quality.py`'s decision functions | B grows a C-shaped exception; later changes to B's rules silently change C |
| Let the retired table inherit the FK | retired copies pin raw rows |

---

## Related

- [design/data-quality](../design/data-quality.md#c-1-the-two-shapes) — how to choose a shape, and the eight cautions
- [ADR-0032](../adr/0032-bounded-writeback.md) — why this path must exist
- [dbt-ops](./dbt-ops.md#proposal-c-targeted-refresh) — step 5
- [proposal-b-rollout](./proposal-b-rollout.md) — if the judgement is wrong rather than the value
