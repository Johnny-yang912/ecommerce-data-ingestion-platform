# ADR-0028: The Hard Gate is scoped per batch; whole-table is a gauge, not a gate

**English** | [繁體中文](../../zh-TW/adr/0028-hard-gate-per-batch-scope.md)

| | |
|---|---|
| **Status** | Accepted — revised in place, see change log |
| **Date** | 2026-08 |
| **Layer** | Data Quality — dbt `stg_` |

---

## Context

The Hard Gate is the run-level blocking mechanism: a test attached to `stg_orders` whose failure halts the entire dbt run, leaving `int_`/`dim_`/`fct_` at their last clean state.

What it guards is easy to state wrongly. **It is not a cleanliness check.** Dirty records are handled per-record by the Row Filter at `int_` (ADR-0029) and land in `int_orders_quarantine`. The Hard Gate exists to answer a different question: *did the source break entirely?* It is a mutation detector, not a quality meter.

The gate was originally scoped to the whole table: error rate across all of `stg_orders`. Three problems surfaced with that scope, and only the first is obvious.

**It loses sensitivity as the dataset grows — measured, not predicted.** The whole-table denominator is cumulative history, so a single catastrophic batch is diluted. Four consecutive batches ran above 10% error (the 2026-08-05 batch was **100%** bad), and the whole-table figure over the same period was **9.122%**. The gate never fired once. A mechanism that guards against sudden failure gets worse at its job simply by the system running longer — it fails by growing old, silently.

**It cannot heal.** Once upstream is fixed, new data arrives clean — but the historical dirty rows stay in the denominator forever. The whole-table ratio never falls back below the threshold. The gate stays red after the problem is solved, and the only ways to clear it are raising the threshold or disabling the test. **Both of those train the operator to ignore the gate**, which is worse than having no gate.

**Its denominator can be moved by the calendar.** The sandbox expires partitions at 60 days. A projection showed that around 2026-09-05, a 300-row batch with 0% dirt would age out — and the whole-table ratio would jump from ~9% to **12%** as a result. The pipeline would have gone red and stayed blocked with nobody having changed anything and no new dirty data having arrived.

> **A metric that can be tripped by the calendar must not hold blocking authority.**

Retention policy, a backfill and a `--full-refresh` all move this denominator the same way, for the same reason: it is sensitive to things that are not quality.

## Decision

Two assertions, with different scopes and different authority:

| | Scope | Threshold | Severity | Role |
|---|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | latest `received_at` partition | 15% | `error` | **Gate** — blocks the run |
| `monitor_dataset_error_rate` | whole table | 10% | `warn` | **Gauge** — visibility only |

The gate's denominator is fixed at the size of one batch, so its sensitivity does not drift with history. The gauge keeps dataset-wide health visible and is **deliberately given no blocking power**.

Both are implemented by the custom generic test `macros/error_rate_below.sql`, which takes a `scope` argument.

## Consequences

**Sensitivity is stable for the life of the dataset.** A batch that is 20% bad trips the gate on day 1 and on day 1000 alike.

**The gate clears itself.** Once upstream is fixed, the next batch is clean and the gate opens with no human intervention and no threshold fiddling.

**A slow drip does not trip it.** Dirt that never exceeds 15% within any single batch passes the gate. This is accepted, not overlooked: those records are quarantined individually by the Row Filter, and the trend is tracked by `rpt_quality_events_daily`. The gate is not the mechanism for that failure mode.

**Two thresholds now have to stay meaningful, and one of them is uncomfortably tight.** 15% and 10% are placeholder values — there is no real upstream whose distribution they could be derived from (see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)). Concretely: the simulated upstream's `--dirty-rate` is 0.12, and at n≈200 the batch error rate has a standard deviation of about 2.3 percentage points. **Roughly one batch in ten will trip the gate on random variation alone** — one observed batch came in at 14.5%, half a point short. Pushing the false-positive rate down to ~1% would require a threshold near 18%.

> **When the gate goes red, first rule out this false positive.** Do not read it as an upstream failure by default.

**`latest_partition` is not the same thing as "this extract run".** Staging carries no load-batch id, so the day partition is a proxy. When one extract spans two days (the watermark is `>=`), only the newer day is asserted. Making it exact would mean writing a batch column during extract — which would make `stg_orders`'s `raw_id` dedup tiebreak non-deterministic. That is a separate decision, and it has not been made. Note also that the partition boundary is **UTC**, which rolls over at 08:00 Taipei.

## Alternatives considered

**`dbt_utils.expression_is_true`.** Technically impossible, and worth recording why: it is a row-level test that folds the condition into `WHERE NOT(...)`, while an error rate needs `countif()/count(*)` — an aggregate. BigQuery rejects it outright with `Aggregate function COUNTIF not allowed in WHERE clause`. A ratio assertion has to be made at the aggregate level, which is why the custom generic expresses it via `HAVING` with no `GROUP BY` (one value over the selected scope); returning a row means the threshold was breached.

**Keep the whole-table assertion blocking as well.** Rejected: not self-healing, for the reason above. Whichever assertion can block sets the ceiling on how good the mechanism can be.

**Block per-record at `stg_`.** Rejected: it would violate the layer contract that `stg_` retains everything including dirty rows (ADR-0002, ADR-0027). Per-record blocking has a designated home one layer down.

## Revisit when

A real upstream exists and its error-rate distribution can be observed. The thresholds should then be derived from that distribution rather than chosen.

## Change log

**2026-08 — scope changed from whole-table to per-batch.** The original gate asserted an error rate across all of `stg_orders` with blocking severity. It was replaced for the three reasons in Context; the whole-table assertion was kept but demoted to `warn`. The mechanism (a run-level aggregate assertion on `stg_orders`) did not change, which is why this is a revision rather than a new ADR.

## Related

- [ADR-0027](./0027-blocking-at-int-layer.md) — why per-record blocking lives at `int_`
- [ADR-0029](./0029-effective-quality-state.md) — the Row Filter this gate is deliberately not duplicating
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — why dirty rows are in `stg_` at all
- [Data quality architecture](../design/data-quality.md)
