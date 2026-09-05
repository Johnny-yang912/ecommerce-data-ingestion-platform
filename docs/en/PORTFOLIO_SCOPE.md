# Portfolio Scope: What Is Deliberately Not Built, and Why

**English** | [繁體中文](../zh-TW/PORTFOLIO_SCOPE.md)

Last reviewed: 2026-08-24

---

## Why this document exists

This system is a portfolio project. It has no real users, no real upstream, and runs on a BigQuery sandbox rather than a billed account. Some things a production system would have are therefore missing.

Scattering those gaps across seven design documents makes them read as omissions. Collecting them here makes them what they actually are: **a boundary that was measured, not one that was stumbled into.** Every row below states what a real system would do instead, and what would have to change for this system to do it.

Nothing here is unknown work. If it were unknown, it would be a risk in [STATUS](./STATUS.md), not a line in this table.

---

## The test: portfolio constraint, or technical debt?

Not everything unbuilt belongs here. The distinction:

| | Belongs here (⏸ Deferred) | Does not belong here |
|---|---|---|
| **Cause** | No real traffic, or a paid account | A design choice, or work not yet done |
| **If the constraint lifted** | It would be built, essentially as designed | It might still not be built (⛔), or it is simply backlog (⬜) |
| **What's missing** | Inputs — traffic distributions, thresholds, an audience | The decision itself, or the time to do it |

A rule that cannot be written because nobody knows the threshold is a portfolio constraint. A rule that could be written but was judged not worth writing is a decision, and lives in an ADR.

**This table therefore holds ⏸ items only.**

> One row is an exception worth naming: **#8 "should have run, didn't"** appears in [STATUS](./STATUS.md) as a **known risk**, not as a ⏸ line item — because it is not an independent gap but a *consequence* of #1 and #3 both being deferred. It is listed here anyway, because "what a real system does instead" (an external dead-man's switch) is an answer neither of those rows gives. So the counts differ by one, deliberately. Things this project decided against on their merits — and would decide the same way in production — are ⛔ in [STATUS](./STATUS.md), with the reasoning in their ADR. There are currently two of them: rejecting rather than sanitising NUL bytes, and not building scenario-specific `int_orders_*` models before a scenario exists.

---

## The table

| # | Deferred item | Category | Why it cannot be done meaningfully here | What a real system does | Trigger to build it |
|---|---|---|---|---|---|
| 1 | Absent / liveness alert rules | No real traffic | A liveness threshold comes from the schedule declaration and is writable today — but value thresholds (latency, error rate) and the response procedures around them both need real traffic to grow. Rules also live in the Grafana Cloud UI, where they can be neither version-controlled nor reviewed | Alert rules as code (Terraform / Grafana provisioning), thresholds derived from SLOs and observed distributions, each rule paired with a runbook | The day this system is actually used to monitor itself |
| 2 | Business / DQ metrics in OTel | Simulated upstream | The simulated upstream picks its dirty rate deterministically from five choices per day, so it is **constant within a day**. A minute-level error rate says nothing `rpt_quality_events_daily` does not already say | Real-time DQ metrics on the operational axis, high-cardinality slicing in the warehouse | A real upstream, where the dirty rate moves within a day |
| 3 | Airflow → OTel integration | Consumers all deferred | The Collector has been ready all along (plaintext to `otel-collector:4318`, same compose network). What blocks it is not technical: its strongest justification was supplying the pipeline's liveness signal, and no rule will be written | Airflow's OTel exporter enabled, feeding the same Collector | Writing liveness rules, or extract/dbt developing a real latency problem |
| 4 | Index on `raw.status` | No real traffic | The index's *shape* — which columns, in what order, partial or not — must be decided from real query patterns. Deriving it from `EXPLAIN` over self-generated data would encode the generator's biases, not the workload's | Read `pg_stat_statements`, find the real predicates, build a composite or partial index matching them | Real traffic, or a table large enough that the scan's query cost becomes visible |
| 5 | End-to-end tests against a real database | Cost exceeds risk | The DB-layer contracts (CAS, dedup, recovery) only produce value under genuine concurrency. Test-authoring effort plus container-startup flake maintenance currently costs more than the risk of not automating it | testcontainers or a service container in CI, running the real contracts | A second contributor, or real traffic |
| 6 | `check_migration_drift.py` in CI | Solo development | It is deterministic, concurrency-free, and low-flake — it *could* run in CI today. Kept manual given a solo developer, a stabilising schema, and a low probability of drift. Its exit-code interface is already in place | Run it on every PR | A second contributor, or real traffic |
| 7 | A real failure-notification channel | No on-call target | Pointing a notifier at a connection that does not exist behaves as "red → callback fires → it raises → nobody receives anything". **Believing you have alerting when you don't is more dangerous than plainly having none** | Slack / PagerDuty webhook, with escalation policy | Someone to actually notify |
| 8 | "Should have run, didn't" alerting | No on-call target | Airflow 3 removed SLAs. The residual blind spot from the [August 2026 stall incidents](./incidents/2026-08-silent-scheduling-stalls.md) | An external dead-man's switch that the DAG pings on success; it alarms on silence | Same as #7 |
| 9 | Gold `order_date` partition retention | BigQuery sandbox | The sandbox forces a 60-day partition expiry on every table. Rows older than that silently vanish from Gold — measured, not assumed | Set `partition_expiration_days` explicitly, or omit it | Enabling billing |
| 10 | SCD2 `dim_customer` | BigQuery sandbox | dbt snapshots need write permissions the sandbox does not grant. There is also no real attribute-change history to track | `dbt snapshot` with surrogate keys and validity windows | Enabling billing |
| 11 | Incremental `rpt_sales_*` | Simulated data | The motive for incremental aggregation is cost and volume. At 800 simulated orders a day there is no cost pressure to relieve — and the cell-by-cell reconciliation test that must land alongside it would be reconciling numbers that mean nothing | Daily incremental + weekly full refresh, **with the reconciliation test landed in the same change** | Real data volume, or query cost reaching a threshold |
| 12 | Monetary exposure measures for quality reporting | Simulated data | Exposure is a business figure — "how much revenue is sitting in quarantine". Computed over generated amounts it is not merely imprecise, it is **misleading**, because the number invites a business decision. (`int_order_items_quarantine` is also a missing prerequisite, but building it would not make the figure mean anything) | Item-grain quarantine, then exposure measures on top | Real order amounts |
| 13 | Partition backfill owned by Airflow runs | Solo development | Targeted backfill already repairs any partition today (`stg_orders_backfill_start`) — **the repair capability has no gap; the record of the repair does**. Owning partitions per run would turn a backfill into "re-run that day", with who, when, how long and the outcome retained by Airflow automatically. With one operator and a very low repair rate, an incident report plus a CHANGELOG line records more — it can state **why** the backfill happened, which a run history cannot; the cost is that it relies on self-discipline. Doing it also means every run *owns* an interval, `catchup` becomes a real decision again, and there is one more contract between the DAG and dbt. ⚠️ **Detection is not part of this** — row loss is caught by `assert_stg_orders_matches_staging` ([ADR-0055](./adr/0055-partition-aligned-incremental-window.md)) | Parameterise dbt's selection with `data_interval_start/end`; a backfill is clearing those runs in the UI, and the run history carries the audit trail | A second contributor, or a repair rate high enough that hand-written records start being missed |
| 13 | A formal resolution for cross-timezone extraction | No real traffic | Three approaches are documented; none can be validated without real traffic crossing the day boundary | Choose one, and prove it with data that straddles midnight | Real traffic in a second timezone |

---

## Three cases worth the detail

### A. Liveness alerting — the rules are writable; the thresholds are not

This is the clearest case of "the reasoning is the deliverable."

A liveness rule's threshold comes from the **schedule declaration**, not from a traffic distribution. `seed_demo_daily` runs at 10/13/17/21 Taipei; a rule saying "alarm if no run within 90 minutes of a scheduled slot" is writable today, with no traffic at all.

So the reason not to write it is not capability. It is that the surrounding apparatus — the value thresholds (latency, error rate), the response procedures, the escalation path — all need real traffic to grow, and the rules would live in Grafana Cloud's UI where they can be neither version-controlled nor reviewed.

What was built instead is the reasoning: six principles for setting liveness alerts, recorded in [design/liveness-alerting.md](./design/liveness-alerting.md). One of them is counter-intuitive enough to be worth stating here:

> Under **cumulative** temporality, a counter series never goes absent — the SDK keeps exporting the last value. So `absent()` detects **a dead process**, not a stopped upstream. Detecting "the upstream stopped" requires a rate-over-window rule, not an absence rule.

That distinction cost a measurement to find, and it does not depend on having real traffic.

### B. The `raw.status` index — why `EXPLAIN` over self-generated data is the wrong input

The recovery scan selects rows by `status='pending'`. There is no index on that column.

The bounded scan (pagination + id cursor + per-run cap) fixes the part that actually collapsed under load: memory and dispatch volume. It does **not** fix query cost, which still grows with table size.

The obvious move would be to run `EXPLAIN`, add the index it suggests, and call it done. That would be wrong here, and the reason is worth stating:

**The data in this system is generated by `scripts/seed_demo.py`.** Its status distribution, its arrival pattern, and the ratio of `pending` to terminal rows are all artifacts of the generator's parameters. An index tuned against that distribution encodes the generator, not a workload.

The specific decisions that need real data:

- **Partial or full?** If `pending` is a tiny fraction of the table in steady state, `WHERE status='pending'` as a partial index is far smaller — but that ratio is a property of throughput versus worker capacity, which does not exist yet.
- **Composite, and in what order?** The scan also filters on age and orders by `id`. Whether `(status, id)` or `(status, processing_started_at)` wins depends on which predicate is selective in practice.
- **Does it pay for itself?** Every index costs write throughput on the ingestion path — the hot path this system is built around.

So the deferral is not "we didn't get to it." It is that **the input required to make the decision correctly does not exist, and fabricating it would produce a confidently wrong answer.**

### C. The BigQuery sandbox's 60-day expiry, and how it reached back into Gold

The sandbox applies a mandatory 60-day partition expiry to every table. This is a billing limitation, but it changed a design decision rather than merely limiting one.

Measured in August 2026:

1. Gold tables partitioned on `order_date` lose rows older than 60 days — silently. No error, no warning; the row count simply drops.
2. A related measurement overturned an earlier conclusion recorded in the cloud-layer document: dates outside BigQuery's legal partition range do **not** fail the build. They land silently in `__UNPARTITIONED__`. The planned "legal range guard" was retracted as unnecessary — the failure mode is not the one that had been assumed.

The consequence for the design: **silent row loss and silent misfiling are the same class of hazard as the ingestion layer's `has_clean_error`** — a problem that does not announce itself. The response was the same in both cases: make it visible rather than make it impossible. `fct_orders` carries `items_missing_amount` to surface rollup incompleteness explicitly, rather than papering over it with `coalesce`.

Enabling billing removes the expiry. It does not remove the design principle it produced.

---

## The simulated data source: what it does and does not cover

The system's only data source is `scripts/seed_demo.py`, driven on a schedule by `seed_demo_daily`. Being explicit about its shape matters, because several deferrals above trace directly to it.

**What it covers:**

- Ingestion through the real path — every record goes through `POST /orders`, the queue, the CAS claim, and the cleaning rules. Nothing is inserted directly into ODS.
- Two orthogonal dirt axes: `--dirty-rate` emits business-rule violations; `--missing-cost-rate` emits incomplete upstream data. They are independent, which is what makes the quarantine and imputation paths testable.
- Enough volume for the analytics layer to be non-trivial: 800 orders/day across four slots.

**Designs that "happen to hold" because of the simulation** — this is not a disclaimer, each row has a concrete consequence:

| Design that happens to hold | What real traffic would do to it |
|---|---|
| All seeding slots land in one UTC day partition | Round-the-clock ingestion cannot dodge the UTC day boundary; Taipei 00:00–08:00 falls into the previous day |
| The Hard Gate uses "latest UTC day partition" as a proxy for "latest batch" | Under continuous ingestion it degrades into "today so far", **replaying the dilution problem within a single day** |
| Freshness needs no blocking authority (no data = nobody seeded = harmless) | An upstream outage is an incident — **but freshness is not what detects it.** It measures `ods.received_at` = the extract hop, and is structurally blind to upstream. A real system adds measurement on the Raw side, rather than wiring freshness up as a gate |
| The 26h/50h freshness thresholds | **Unchanged.** They come from the *loading* cadence (one extract per day → 24h + 2h grace), not the ingestion cadence. Under 24/7 ingestion the warehouse is still loaded nightly. What would change them is extract moving to hourly or streaming |

**What it does not cover:**

- **Intra-day variation in data quality.** The dirty rate is chosen deterministically from five values per day, so it is constant within a day. This is the direct cause of deferral #2.
- **Schema drift from upstream.** New or renamed fields never appear. The additive-staging and `on_schema_change` machinery is exercised by hand, not by the generator.
- **Adversarial or malformed traffic** beyond the specific cases already fixed (NUL bytes, NaN/Inf in items).
- **Traffic crossing the day boundary in a second timezone** — the direct cause of deferral #13.
- **Bursts and backpressure.** Load characteristics come from `scripts/load_test.py` on demand, not from the scheduled generator.

---

## Related

- [STATUS](./STATUS.md) — the full implementation matrix and known risks
- [Architecture Decision Records](./adr/README.md) — decisions, including the ⛔ ones with their triggers
- [August 2026 stall incidents](./incidents/2026-08-silent-scheduling-stalls.md) — where the "liveness vs progress" distinction came from
