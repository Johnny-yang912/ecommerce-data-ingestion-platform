# 2026-08 — `raw_id` collides across two ODS instances

**English** | [繁體中文](../../zh-TW/verification/2026-08-raw-id-collision-two-ods.md)

---

## What was found

`stg_`'s dedup partitions on `raw_id` ([ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)). That is correct — physical dedup should use physical identity — **and it welds a premise into the pipeline that was never written down.**

## The situation at the time

The business DB ran on the host while Airflow ran in containers. One way to connect them was "bring compose's `db` up too" — which gives you a **separate, empty database** whose `raw_id`s start at 1 and **overlap completely** with the host ODS's.

Extract both into the same BigQuery staging table, and `stg_`'s `raw_id`-grained dedup collapses **two unrelated orders into "copies" of each other**, dropping one.

> **No error. No trace.** The dedup is doing exactly what it was told; the input violated an assumption nobody had stated.

## When to watch for it

Any time there is **more than one landing instance**:

- host and container (this case)
- blue/green deployments
- several upstreams, each with its own Raw table

The collision recurs in all of them.

## The premise this exposed

The project already stated: *"`raw_id` is physical identity, `order_id` is business identity."* That is true and it does not go far enough:

> **`raw_id`'s uniqueness only holds within a single landing instance.**

Choosing `raw_id` as the dedup key is right — but it welds the **single-instance** premise into the pipeline, and an unstated premise is one nobody checks.

## The fix, if it ever becomes necessary

Upgrade the dedup key to `(source_instance, raw_id)`, or carry an instance identifier from the extract onwards.

**Until then: one staging table can only correspond to one ODS.** Never mix instances into a single dataset.

## Conclusion

**No longer a live risk in this project.** After moving fully into compose there is exactly one ODS, so the cause is gone.

This record stays because **the premise is still welded in** — it just happens to be permanently true here. A future change that introduces a second landing instance reintroduces the hazard, and there is nothing in the code that would announce it.

## Why this is filed as a verification rather than an incident

It was found while verifying something else, and it **never produced bad data in this system** — the two-instance configuration was corrected before it ran at any volume. It is a discovered constraint, not a failure that happened.

## Related

- [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md) — the dedup key decision
- [2026-08-05-airflow-commissioning](./2026-08-05-airflow-commissioning.md) — the session this was found in
- [2026-08-11-full-compose-rebuild-v4](./2026-08-11-full-compose-rebuild-v4.md) — the change that removed the cause
