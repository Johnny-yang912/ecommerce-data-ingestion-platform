# Silent Scheduling Stalls: Four Incidents and One Architecture Migration

**English** | [繁體中文](../../zh-TW/incidents/2026-08-silent-scheduling-stalls.md)

| | |
|---|---|
| **Period** | 2026-08-11 → 2026-08-14 (fallback removed 08-17) |
| **Impact** | The analytics pipeline stalled four times; longest single stall 13h 22m. No data reached BigQuery during those windows |
| **Alerting** | **None, all four times.** Every existing monitoring signal stayed green throughout |
| **Status** | Closed. Three root causes remain unidentified; the handling and its cost are recorded at the end |

---

## Why this document is here

These four incidents occurred in the environment layer of a development machine — WSL2, Docker, the Windows boot sequence. Judged by cause alone, they are genuinely local problems.

But what they left behind is not local:

- **The failure mode generalises** — *"no task was created, so there is no failure to see, so monitoring stays green"* holds on any scheduling system.
- **They are the direct input to three design decisions** — the `raw_pending_watch` DAG, how the probe's threshold is derived, and the principles for setting liveness alerts all originate here.

In other words, this document does not record *"my computer broke"*. It records **"the standard I was using to judge system health was wrong, and how I corrected it."** The latter belongs to this project.

---

## Incident summary

| # | Incident | Root cause | Duration | Alerted |
|---|---|---|---|---|
| 1 | Clock drift | WSL kernel tick skew killed DAG parse subprocesses at birth | ~7h | none |
| 2 | Boot-time mount race | Container restore ran before the filesystem was ready; an unresolvable mount source **degrades to an empty one** | 2h 52m | none |
| 3 | Boot automation dies young | The wake command returned and the environment was reclaimed immediately | ~4h | none (exit code 0) |
| 4 | Zombie scheduler after sleep-resume | Unidentified; triggered by a non-cold power resume | 13h 22m | none |

**Four identical symptoms, four unrelated root causes, not one red light.**

---

## The common structure: why none of them alerted

During the stalls, all seven of these read normal:

| Signal | Value during the stall | What it proved |
|---|---|---|
| Container status | `Up` | the process exists |
| Restart count | `0` | no crash loop |
| Container error field | empty | no startup failure |
| Application log | clean | no exception was raised |
| DAG listing | lists fine | **(false signal)** it reads database residue, not the current parse result |
| Scheduler heartbeat | no gaps | the main process is still running |
| Healthcheck | alive | the port answers |

**What all seven have in common: every one of them proves "the thing exists". Not one proves "the thing is working".**

That is this incident sequence's central output — **a liveness signal is not a progress signal.** A scheduler can be entirely alive, heartbeating, and answering healthchecks while creating zero scheduled runs for thirteen hours.

---

## Incident 1 — Clock drift

### Symptom

The scheduled time arrived and the DAG did not run. Airflow raised nothing at all — no import errors, and parsing the DAG file by hand took one second and reported zero problems. All six DAGs were marked `stale`, with the last successful parse hours earlier.

### Root cause chain

```
WSL2 kernel tick suppressed by roughly 3.6%
  → the system's MONOTONIC clock falls steadily behind MONOTONIC_RAW
  → a second time-sync service corrects in the opposite direction; two controllers fight
  → a DAG parse subprocess is judged "already hundreds of seconds overdue" the instant it spawns,
    and is killed
  → the DAG never finishes parsing → permanently stale
  → the scheduler creates no runs for a stale DAG
  → no run means no failure, and no failure means no red light
```

### What actually helped

**Do not chase the DAG code.** A manual parse taking one second with zero errors already rules out syntax and import problems — an easy fact to overlook.

Three steps that worked:

1. Compare the two clocks (`MONOTONIC` vs `MONOTONIC_RAW`) — normally near-identical.
2. Measure cumulative drift since boot — that is the quantity that hits the wall.
3. Use the host's clock as an external reference — measured identical to `MONOTONIC_RAW`, so it can serve as ground truth.

### Handling and cost

Raise the DAG parse tolerance by **72×** and stop the service that was fighting (after confirming nothing else depended on it).

**This buys time; it does not fix the cause.** The root cause is unresolved, but the cost is calculable:

```
time until failure = tolerance ÷ drift rate
```

Daily shutdowns reset the accumulation, so the current usage pattern never reaches the limit. A 24-hour machine would hit it in **about 27 hours**. **Knowing what you bought matters more than pretending you solved it.**

Ruled out: writing the tick value back — the kernel overwrites it every 8 seconds, so it is gone within two minutes.

---

## Incident 2 — Boot-time mount race, and the migration it produced

The only one of the four that changed the architecture, and the one that produced the most method.

### Symptom

After a host reboot, Airflow was entirely dead. Containers showed `Exited (127)`, and **`docker logs` showed nothing at all** — the failure happened during container *creation*, before any log existed. The error lived only in the container's `State.Error` field.

### Root cause

After a host reboot, container restore ran before the filesystem was available. **When Docker cannot resolve a mount source it does not error — it gives you an empty one**: the bind mount silently degraded to tmpfs.

And because the failure happened at creation, **the container never entered `running`, so the restart policy never engaged** and the healthcheck never fired.

### The first fix pointed the wrong way

The first attempt changed a single-file mount to a directory mount, so the source would always exist at boot.

Result: **every indicator improved and the pipeline stalled exactly as before.**

A noisy failure (`Exited 127`, at least visible) had been traded for a silent empty directory (container `Up`, mount contents empty). That failure left a reversed reading:

> **All containers `Up` does not mean you are clear.** You have to go inside and check the mount type — `tmpfs` where a bind belongs is the tell; healthy is `ext4` or `virtiofs`.

### A zero-footprint experiment overturned the planned fix

After the first fix failed, a seven-step follow-up plan was drawn up. All seven rested on one assumption:

> **"A container whose mount is broken will re-mount if you restart it."**

Before touching anything, five controlled experiments (A–E) were run in throwaway containers, touching **no version-controlled file**.

**The assumption was false.** Mount mappings are registered at container **create** time; `start` does not re-register them. A broken container can be restarted ten thousand times and stay broken. The only fix is a forced recreate.

**None of the seven steps were performed.**

> **The judgement**: five throwaway experiments cost about twenty minutes with zero side effects; the seven steps required modifying three version-controlled files, with rollback and re-review costs if wrong. With confidence in the assumption below 80%, verifying was the cheaper path.

### From "fix the race" to "remove the precondition for the race"

Facing the problem a second time, the processing step was not taken on trust — **an explanation of the cause was requested first.**

The shape of the gap, derived from that explanation:

```
boot / login
  → Docker starts
  → WSL has not started yet
  → Docker cannot reach the environment it depends on
  → mount source fails to resolve → silently degrades to empty
```

That cause was then compared against a reference system known not to have the problem:

> **A native Linux environment has no such gap at all** — Docker and the files live in one system; there is no "wait for another system to start".

From which:

> **The gap is not Docker's problem. It is produced by the premise "Docker in system A, files in system B".**

So the final action was not to fix the race. It was to **remove the condition that produces it** — migrating Docker from Windows-side Desktop integration to a native Docker Engine inside WSL.

The reasoning has a repeatable shape:

> **Take a reference system known not to fail, and work backwards to find which premise is redundant.**

**A methodological note**: a fix can be applied on trust; a cause cannot. Asking for the cause forces you to obtain the raw material to reason further — following the fix directly keeps you inside the frame of *"how do I repair this race?"*.

### Acceptance and the cost of migrating

Ten acceptance items, the hardest being:

> **One complete reboot, with no manual start commands issued at any point — services must come up by themselves and the mount type must be correct.**

Supporting decisions: the old environment was **not uninstalled**, kept as a fallback; old data was not cleared.

When all ten passed, **completion was deliberately not declared** — only one boot cycle had been observed, and a boot race is intermittent, so a single sample is not evidence.

**That judgement was later proved right** — incident 3 occurred during the observation window.

The fallback was removed five days later (08-17) once stability was confirmed, reclaiming 18 GB.

---

## Incident 3 — Boot automation "succeeds", but the environment dies young

### Symptom

The boot automation task reported success (exit code `0`), but 15 minutes after login the containers did not exist at all.

### Root cause

Once the command that wakes WSL returned, the distro was reclaimed. The first version survived **1 second**.

After the first fix it survived **1–5 minutes** — **which is harder to notice**, because that is long enough to pass most manual checks.

### Handling

Two mines had to be cleared together; clearing one leaves the symptom almost unchanged:

1. Use a long-lived anchor so the command stays alive until docker is ready before returning.
2. Remove the scheduled task's execution time limit.

### By-product: a reversed health reading

After the fix, **exit code `0` — previously the sign of success — now means the anchor died.** The healthy state is the task still running.

This is the second reading reversal in the sequence (the first being incident 2's "container `Up` does not mean the mount worked").

---

## Incident 4 — Zombie scheduler after sleep-resume

### Symptom

After resuming from sleep, the scheduler woke as **the same process** and created no scheduled runs for 13 hours 22 minutes.

Heartbeat with no gaps, beat normal, container count correct, healthcheck alive — **all green.**

### Investigation

The only reliable signal: **does the scheduled-run table have new rows?** Heartbeat, container status and healthcheck are all blind to this.

The first assessment concluded this was a property of sleep mode, and proposed changing the script so the environment could survive a sleep-resume. That direction was not taken — instead the whole chain was laid out and re-checked from the start:

> I migrated Docker, wrote a boot script to bring the environment up automatically, and Docker starts the services. Is this chain correct? Which link is not being reached right now?

The answer: **the chain is correct. This machine had never actually rebooted.**

One more layer — *"I definitely pressed shut down, so what does 'restart' do differently?"* — surfaced the Windows boot-type distinction: **Fast Startup turns "shut down → power on" into a hybrid hibernation, not a cold boot.** Every previous "cold boot verification" had never taken that path.

### Handling and what remains open

Fast Startup disabled, so a shutdown genuinely is a cold boot.

**The zombie's root cause remains unidentified.** Subsequent stability came from removing the trigger, not from fixing it.

---

## Three revisions to the judgement criterion

The most valuable output of this sequence is not any single repair. It is that **the standard used to judge "is the scheduler alive" was overturned three times.**

| Version | Criterion | Overturned by |
|---|---|---|
| v1 | heartbeat is the cheapest liveness evidence | Incident 4 — heartbeat is perfectly normal in a zombie state |
| v2 | only count actual scheduled-run records | Same evening — **with no scheduling point inside the window, an empty result proves nothing** |
| v3 | first confirm a scheduling point should have occurred in the window; if not, manually trigger a read-only probe | current |

**Why v3 works**: a manual trigger writes one queued record and drives it through execution, dispatch and write-back — **all scheduler main-loop work, which is exactly what seizes up in a zombie state.** A heartbeat cannot reach that path, and the probe answers in a second or two.

---

## What these lessons became in the system

This is why this document belongs in this repository:

| Criterion learned here | What it became |
|---|---|
| A liveness signal is not a progress signal | the **`raw_pending_watch` DAG** — it does not watch whether a process is alive; it watches whether orders are stuck unclaimed. See [ADR-0039](../adr/0039-observation-signals-own-dag.md) |
| An empty record proves nothing without a scheduling point | the probe's threshold is **derived from the recovery path's own settings**, not hard-coded |
| Green can be a lie; believing you have alerting is worse than knowing you don't | **failure notifications state the response, not the task name**, and every message carries `channel=` — `channel=log` says plainly that nobody was notified. See [ADR-0042](../adr/0042-failure-notification-response-not-task.md) |
| A criterion must be able to overturn itself | the **six principles for liveness alerts** — including the counter-intuitive result that under cumulative temporality a series never goes absent, so `absent()` catches a dead process rather than a stopped upstream. See [design/liveness-alerting](../design/liveness-alerting.md) |
| Verify key assumptions with a zero-cost experiment before touching version-controlled files | the default approach to environment-layer changes in this project |

---

## Closing state

### Verified (2026-08-14, a full unattended day)

- 5 scheduling points, all within **< 1 second** of target
- 16 tasks, all success
- 800 orders reached the cloud on time
- 93 dbt data-quality tests all green

### Still unresolved (three)

1. **The clock drift's root cause is unidentified.** Handling raised the tolerance 72×; the cost is quantified (a 24-hour machine fails after ~27 hours).
2. **The zombie's root cause is unidentified.** Handling removed the trigger (Fast Startup disabled); it is not a repair.
3. **The "scheduler is not running at all" blind spot remains.** The fix is known — a liveness rule — but its threshold needs real traffic to be meaningful. Deliberately deferred; see [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

---

## Related

- [runbooks/airflow-silent-stall](../runbooks/airflow-silent-stall.md) — the triage procedure for next time
- [ADR-0039](../adr/0039-observation-signals-own-dag.md) — the `raw_pending_watch` design
- [design/liveness-alerting](../design/liveness-alerting.md) — the six principles
