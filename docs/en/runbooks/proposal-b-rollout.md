# Runbook: Loosening a quality rule (Proposal B)

**English** | [繁體中文](../../zh-TW/runbooks/proposal-b-rollout.md)

---

## When this applies

A quality rule is being **loosened**, giving existing quarantined records a chance to be pulled back into Gold.

**Only loosening needs this procedure.** A tightened rule applies going forward and needs no retroaction.

---

## The seven steps

```
1. Confirm candidates exist   the target code's value range in quarantine must
                              STRADDLE the old and new thresholds
2. Change rule + bump         the threshold in clean.py + DQ_RULE_VERSION, then git tag
3. ⚠️ Rebuild images          docker compose build api worker beat && docker compose up -d
4. ⚠️ Run the main DAG        orders_analytics_daily — candidates are read from BQ,
                              so the data must be up there first
5. Dry-run                    dq_reevaluation (commit=off, expect_rule_version=<new>)
6. Commit                     dq_reevaluation (commit=on, expect_rule_version=<new>)
                              → triggers the main DAG, flowing data back into Gold
7. Verify                     promoted rows enter fct_orders and leave quarantine;
                              promotions > 0; the control group stays quarantined;
                              ODS unmodified; a second run writes 0
```

---

## ⚠️ Step 1 — confirm candidates *before* bumping

`promoted=0` looks **exactly like** "the rule didn't take effect" and **exactly like** "the code is broken". And low-weight error codes accumulate slowly, so an empty result is entirely plausible.

Checking the value distribution first is far cheaper than diagnosing it afterwards.

```sql
select min(<field>), max(<field>), count(*)
from `<project>.<dbt_dataset>.int_orders_quarantine`
where '<target_code>' in unnest(error_codes);
```

The range must straddle both thresholds. If every value is on the wrong side of the new threshold too, there are no candidates — stop here.

---

## ⚠️ Step 3 — the two paths deliver code differently

```
api / worker / beat   code is BAKED INTO THE IMAGE     needs a build to take effect
Airflow containers    bind mount ./:/opt/project        takes effect IMMEDIATELY
```

Skip the rebuild and **re-evaluation (running in Airflow) is already on the new rules while the ingestion path is still on the old ones** — two rule versions judging data in the same database at once.

**And `--expect-rule-version` cannot see that divergence**: it compares the version inside its own process, so the assertion passes.

> That guard protects against "running this against a deployment that hasn't got the new rules". It holds only if the whole system has a **single code-delivery mechanism** — and this compose topology breaks that premise.

---

## ⚠️ Step 4 — candidates come from BQ, state comes from PG

Re-evaluation writes to PostgreSQL's `quality_events`; an extract is still needed to push events to BQ before data flows back into Gold.

**The reverse holds too, and is easier to miss:**

> **The candidate list comes from BQ's `int_orders_quarantine`. Newly accumulated data that has not been extracted to BQ is invisible to re-evaluation.**

The symptom is a low `candidates` count and `would_write=0` — which looks like a broken program and is actually stale data upstream of the check.

---

## Step 7 — what to verify

| Check | Expected |
|---|---|
| `promotions` in `rpt_quality_events_daily` | `> 0` |
| Promoted `order_id`s in `fct_orders` | present |
| Same `order_id`s in `int_orders_quarantine` | absent |
| Control group (values outside the new threshold) | still quarantined |
| `ods.has_clean_error` for a promoted row | **still `TRUE`** — ODS is never modified |
| Running `dq_reevaluation` again with `commit=on` | writes **0** events (idempotent) |

The fifth row is the one people get wrong. ODS staying `TRUE` is **correct** — the record flows back through the effective-state composition, not by having its flag changed ([ADR-0029](../adr/0029-effective-quality-state.md)).

---

## Related

- [design/data-quality](../design/data-quality.md) — what Proposal B is
- [ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md) — why it is event-driven
- [quarantine-writeoff](./quarantine-writeoff.md) — the other terminal path
