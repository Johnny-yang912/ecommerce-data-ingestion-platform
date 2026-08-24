# Principles for Setting Liveness Alerts

**English** | [繁體中文](../../zh-TW/design/liveness-alerting.md)

**This project writes no alert rules.** What follows are the principles that govern writing them — none of which depend on traffic volume, and all of which apply directly to a real deployment.

> **The reasoning is the deliverable.** The rules would live in Grafana Cloud's UI, where they can be neither version-controlled nor reviewed; the principles can be. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) for why the rules themselves are deferred.

---

## ① Liveness and value thresholds come from different places

**Value thresholds** (latency, error rate) **must be derived from observation** — without the distribution of real traffic, any number is a guess.

**Liveness thresholds** — *"how long without a word from this source counts as wrong"* — **come from a declaration**: a batch's window is written in its cron expression; a continuous ingest's window comes from the upstream's SLA.

Applying "wait for real traffic" to a liveness rule is over-extending the argument. `orders_analytics_daily`'s window comes straight from `schedule="30 22 * * *"` and needs no measurement.

---

## ② "No data" satisfies no condition

An ordinary rule asks whether a value crossed a threshold. **When the data does not exist, the query returns an empty set, and an empty set satisfies no condition** — the rule sits quietly in Normal.

So a naive `latency > 500ms` rule stays silent **precisely when the whole system is dead**.

> Liveness must be its own rule. It cannot be expected to fall out of a value rule.

---

## ③ ⚠️ Cumulative temporality makes `absent()` catch the wrong thing

In cumulative mode the OTel SDK exports the **running total** on a fixed interval, **even when nothing happened**. So as long as the process is alive, the series always exists and is never absent.

Two failures therefore need two rules:

| Question | Form | Catches |
|---|---|---|
| The source stopped speaking | `absent(...)`, or Grafana's No Data handling | process dead, collector dead, machine off, network down |
| The source is alive but nothing is happening | `increase(<counter>[window]) == 0` | **upstream stopped — everything green, all containers Up, zero data** |

**The second is what a pipeline most needs, and it is exactly the one `absent()` cannot catch.**

Choosing wrong does not cost you a false alarm. It costs you **a rule that looks perfectly reasonable and will never fire.**

---

## ④ Mute timings suppress notification, not state transitions

If you choose "short window + mute the expected quiet period" over "a window long enough to span it", note that **a rule already Firing during the mute notifies the instant the mute lifts.**

The mute interval has to extend past the point where the first expected data actually lands — not to the nominal end of the quiet period.

---

## ⑤ Liveness rules must live outside the system they watch

What they detect is precisely *"my side can no longer speak"*.

This is also how they divide labour with the in-Airflow failure notification ([orchestration §7](./orchestration.md)):

| | Failure notification | Liveness rule |
|---|---|---|
| Runs in | Airflow's task process | the cloud |
| Dies with | the machine | nothing on this side |
| Covers | ran and failed | should have run, didn't |

**They are not substitutes but two independent paths in different failure domains** — either one alone has a structural blind spot.

---

## ⑥ An alert should carry the response, not just the number that moved

Whoever receives the alert needs to know **where to look first**, and that information never appears on its own in a metric name.

Same reasoning as the failure-notification message content: the docstring said what the red meant, and the person handling the incident does not read docstrings.

---

## Where this came from

These principles are not abstract. They were derived from four consecutive silent stalls in August 2026, in which the pipeline stopped and **every existing monitoring signal stayed green** — because all of them proved *"the thing exists"* and none proved *"the thing is working"*.

That distinction — **liveness signal vs progress signal** — is the root of principles ② and ③, and it is also why `raw_pending_watch` watches for orders stuck unclaimed rather than watching whether a process is alive.

Full account: [incidents/2026-08-silent-scheduling-stalls](../incidents/2026-08-silent-scheduling-stalls.md).

---

## Related

- [orchestration](./orchestration.md) — the in-system half of the coverage
- [data-quality §7](./data-quality.md) — the tier-1 / tier-2 metric boundary
- [ADR-0042](../adr/0042-failure-notification-response-not-task.md) · [ADR-0052](../adr/0052-sdk-views-series-budget.md)
