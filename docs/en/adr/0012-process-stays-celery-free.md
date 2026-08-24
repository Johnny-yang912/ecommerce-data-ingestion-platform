# ADR-0012: `process.py` stays Celery-free to preserve the manual rescue path

**English** | [繁體中文](../../zh-TW/adr/0012-process-stays-celery-free.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Task queue |

---

## Context

The direct way to make `process_raw_event` a Celery task is to decorate it: `@celery_app.task` on the function itself. One line, no wrapper module.

It also couples the core processing logic to the transport, and the coupling bites in the exact situation where it matters most — when the transport is what has failed.

## Decision

`process.py` has **zero Celery imports**. A thin wrapper in `tasks.py` holds the decorator:

```python
# tasks.py
@celery_app.task
def process_raw_event_task(raw_id): ...   # calls process.process_raw_event
```

Three things this buys:

**The manual rescue path stays open.** When the broker is down, a record can still be processed with `python -c "from process import process_raw_event; ..."` — no queue involved, no broker required. That path exists precisely for the case where the queue is the problem.

**Tests and scripts call the logic directly.** pytest does not need a broker; neither does `reevaluate_quality.py` or any ad-hoc script.

**Swapping the queue touches one file.** If Celery is ever replaced, `tasks.py` changes and the processing logic does not.

## The wrapper is deliberately thin

`tasks.py` sets **no `autoretry_for` and no `max_retries`**, and that is a decision rather than an omission.

`process.py` already has four retry layers: the Raw write, the claim, processing, and the status commit. Adding Celery-level retry on top produces **3 × 3 retry amplification**, and worse, it blurs what the `error` terminal state means — a record could be in `error` and simultaneously scheduled for another attempt by the transport layer.

`process_raw_event` is designed **not to raise**. Every failure has already been recorded in `raw.status` by the time it returns. Celery does not need to judge the outcome, and should not.

> The transport layer's job is delivery. Once the business layer has recorded a terminal state, the transport has nothing left to decide.

## Consequences

**One extra module and one extra indirection** for a reader tracing the call path. That is the whole cost.

**The rescue path has to actually work**, which means `process.py` must not acquire a Celery import by accident. Nothing enforces this automatically today — it is a discipline held by this ADR and by the module docstring.

That is not hypothetical: the same class of accident happened to `check_raw_pending.py`, where a single shared constant coupled a read-only probe to the write path's entire dependency tree and broke it during an unrelated deploy (see ADR-0039). There, the fix was extracting the constants into `recovery_policy.py` and pinning it with `tests/test_script_deps.py`.

## Alternatives considered

**Decorate `process_raw_event` directly.** Saves a module; costs the rescue path, and makes every test and script import Celery.

**Put the wrapper in `main.py`.** The worker process would then have to import the entire FastAPI app — middleware, rate limiter, lifespan — none of which background processing needs. `celery -A celery_app` is a cleaner single entry point for worker and beat.

## Related

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — the queue being wrapped
- [ADR-0013](./0013-bounded-broker-wait.md) — the broker-down scenario this path serves
- [ADR-0039](./0039-observation-signals-own-dag.md) — the same coupling accident, and how it was pinned
