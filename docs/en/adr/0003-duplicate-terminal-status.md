# ADR-0003: `duplicate` is a terminal Raw status, not a rejection

**English** | [繁體中文](../../zh-TW/adr/0003-duplicate-terminal-status.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Ingestion — Raw state machine |

---

## Context

ADR-0001 lets repeat `order_id` submissions into Raw. They therefore reach processing, and processing has to put them somewhere. Three options were available: drop them silently, mark them `error`, or give them a status of their own.

`error` is the tempting default, and it is wrong for a specific reason: **`error` and `duplicate` demand different responses.** `error` means the system failed to process something it should have processed — someone needs to look at it. `duplicate` means the system worked exactly as designed and the upstream sent the same order twice — nobody needs to do anything, unless the *rate* changes. Folding them together makes the `error` count useless as an alert signal.

## Decision

`duplicate` is one of the terminal statuses in the Raw state machine:

```
pending → processing → processed | error | duplicate
```

It is reached by two paths, both in `process_raw_event`:

1. **Pre-check hit** — `order_id` already present in ODS before commit.
2. **`IntegrityError` on commit** — two workers both passed the pre-check; the second loses the race (see ADR-0005).

Both record which `raw_id` won, in `error_message`.

## Consequences

**Monitoring gets a clean, separately countable signal.** A rise in `duplicate` points at the upstream or at the network; a rise in `error` points at this system.

**`duplicate` is replayable.** `POST /process_raw/{id}?force=true` accepts `error` and `duplicate` only — the two terminal states where a human might legitimately want a second attempt. `processed` is not replayable, which prevents a stray force from overwriting good data.

**The cost is that this signal can be polluted by the system's own behaviour.** A `duplicate` does not always mean the upstream sent twice — it can also mean *this system* processed the same `raw_id` on two workers. That actually happened: a stale-detection defect reverted a record from `processing` to `pending` mid-flight, letting a second worker claim it, and the record that had in fact succeeded ended up flagged `duplicate` (ADR-0015).

The signal's usefulness is precisely what made that defect worth fixing. If `duplicate` had been folded into `error`, the corruption would have been invisible.

## Alternatives considered

**Silently drop.** Leaves the Raw row in a non-terminal state or requires a hidden fourth outcome. Either way the count of "orders we received twice" becomes unobtainable.

**Reuse `error`.** Destroys the alerting distinction described above. The whole reason for a separate terminal state is that the two conditions call for different human responses.

**Reject at the API layer before writing Raw.** That is ADR-0001's rejected alternative, for reasons that live one layer up.

## Related

- [ADR-0001](./0001-raw-no-business-dedup.md) — why duplicates reach this layer at all
- [ADR-0005](./0005-first-write-wins-idempotency.md) — the two paths into this status
- [ADR-0015](./0015-staleness-from-processing-started-at.md) — the defect that polluted this signal, and why that mattered
