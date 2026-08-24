# ADR-0001: No business deduplication at the Raw layer

**English** | [繁體中文](../../zh-TW/adr/0001-raw-no-business-dedup.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05 |
| **Layer** | Ingestion — Raw |

---

## Context

`POST /orders` writes the request body to the Raw table verbatim. The obvious defensive move is a `UNIQUE` constraint on `raw.order_id`: reject a repeat submission at the door, and the rest of the pipeline never has to think about duplicates.

Three things argue against it.

**Repeat submissions are not necessarily redundant.** Two posts of the same `order_id` may carry different fields — an upstream that retries after enriching a record sends strictly more information the second time. Rejecting the second submission discards data the first one did not have.

**Submission frequency is itself a signal.** An `order_id` arriving forty times in a minute means something: a client-side retry loop, a misconfigured upstream, or an attack. A `UNIQUE` constraint converts that signal into a constraint violation and throws it away.

**The system itself manufactures duplicates.** `_enqueue()` deliberately swallows broker failures and still returns `200 pending`, because the Raw row is already committed (see ADR-0013). But a client that times out at the HTTP layer will resend. Rejecting those at Raw would hide the consequences of the system's own degraded behaviour from the system's own operators.

## Decision

`raw.order_id` is **indexed but not unique**. Every accepted `POST /orders` produces exactly one Raw row, whatever has been seen before.

Deduplication responsibility is delegated downstream to ODS (ADR-0005).

## Consequences

**Raw's row count exceeds ODS's by design**, and the difference is a monitoring quantity rather than an error. It answers "how much redundant submission is this pipeline absorbing?" — a question that cannot be asked if the duplicates were rejected.

**Raw stays a true landing layer.** It records what arrived. It makes no judgement about what arrived, which is what lets every judgement downstream be revised later without re-ingesting.

**The cost is storage**, and a downstream layer that must actually enforce uniqueness. If ODS's constraints were wrong, nothing upstream would catch it.

## Alternatives considered

**`UNIQUE` on `raw.order_id`.** Loses complementary fields, loses the frequency signal, and returns the client a `409` that it cannot distinguish from a genuine failure — so a well-behaved client retries anyway.

**Read-before-write in the endpoint.** Same semantic problems, plus a `SELECT` on the hot ingestion path for every request, against a table that only grows.

## Related

- [ADR-0003](./0003-duplicate-terminal-status.md) — what happens to the duplicates that this decision lets through
- [ADR-0005](./0005-first-write-wins-idempotency.md) — where deduplication actually happens
- [ADR-0013](./0013-bounded-broker-wait.md) — why the system produces duplicates of its own
