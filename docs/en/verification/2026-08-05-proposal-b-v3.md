# 2026-08-05 — Proposal B: the full v3 flow-back

**English** | [繁體中文](../../zh-TW/verification/2026-08-05-proposal-b-v3.md)

---

## What was being verified

The first end-to-end walk of the rule-loosening SOP. **Does a promoted record actually reach Gold, is the operation idempotent, and does bounded writeback hold?**

## Environment

ODS at 774 rows (57 dirty, 7.364%). BQ sandbox, dbt 1.11. 2026-08-05.

## Method

**The order is deliberate.** 20 `V3DEMO-*` records were ingested under **v2** first — 15 with `age` in {121, 123, 125, 127, 130} and a 5-record control group with `age` ∈ {-3, 150, 999} — and **only then** was v3 switched on.

Reversed, `age=125` would be judged clean on arrival and never enter quarantine at all:

> **Only data ingested under the old rule is eligible to be pulled back by the new one.**

## Observed

| Stage | Result |
|---|---|
| Ingested under v2 | all 20 `has_clean_error=TRUE`, `quality_events` → `quarantined`(v2) |
| Extraction | 220 rows orders / 220 rows quality_events |
| Layered dbt build | staging PASS=21 WARN=1 / intermediate PASS=27 WARN=1 / marts PASS=31 / reports PASS=24 |
| Before promotion | quarantine **20**, `fct_orders` **0**, `promotions` **0** |
| Dry-run | 57 candidates → `would_write=15`, `unchanged=42`, `blocked_non_reproducible=0` |
| `--commit` | `written=15` |
| **Immediately again** | **`promoted=0`, `unchanged=57`, `written=0`** |
| After flow-back | `int_orders` +**15**, quarantine → **5**, `fct_orders` **15**, `promotions` 0→**15** |
| Full `dbt test` | 93 tests: PASS=91 / WARN=2 / **ERROR=0** |

## Four things this proved

**① Idempotency went from "claimed" to "measured".** Two consecutive runs; the second wrote 0 events. *"Append only on an actual state change"* really does keep `promotions` — the figure that "historical metrics are never rewritten" exists to protect — from being inflated by a re-run. Previously this had unit tests only.

**② A loosening has an edge; it is not switching the rule off.** The 5 control records (age −3/150/999) stayed exactly where they were, and the flow-back landed precisely as `age=121/123/125/127/130, 3 records each`.

**③ Bounded writeback held — and left 15 live samples of the permanent divergence.** Those 20 ODS rows still read `dq_rule_version=v2, has_clean_error=TRUE`; **not one column was touched.** The event chain is clean:

```
initial_evaluation(None → quarantined, v2)  →  promotion(quarantined → promoted, v3)
```

What the DQ document argues at length is now **15 rows you can point at**: ODS says dirty (v2), Gold says clean (v3), and `dq_rule_version` + `quality_events` make it fully traceable.

**④ The Hard Gate's severity tiers really are tiers.** 7.364% made the 0.05 assertion **WARN** while 0.1 **PASSED** — alerting without blocking, and `dbt build` carried on downstream.

## ⚠️ Historical note on ④

**The test names in this record are outdated.** The Hard Gate later moved to a per-batch scope and is now `hard_gate_latest_batch_error_rate` (latest `received_at` partition, 0.15, **error**) plus `monitor_dataset_error_rate` (whole table, 0.1, warn). The `_0_05` / `_0_1` pair no longer exists.

**The observation still holds; the thresholds and scope changed underneath it.** This note is left in place rather than edited — a verification record is a statement about a moment, and rewriting it would destroy the evidence that the design moved. See [ADR-0028](../adr/0028-hard-gate-per-batch-scope.md).

## Conclusion

The SOP works end to end. The three properties that matter — idempotency, bounded edges, and an untouched anchor — are all now backed by data rather than by argument.

## Related

- [ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md) · [ADR-0032](../adr/0032-bounded-writeback.md)
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md) — the SOP walked here
- [2026-08-12-proposal-b-v2-to-v4](./2026-08-12-proposal-b-v2-to-v4.md) — the branches this run could not cover
