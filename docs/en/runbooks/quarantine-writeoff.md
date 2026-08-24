# Runbook: Writing a record off permanently

**English** | [繁體中文](../../zh-TW/runbooks/quarantine-writeoff.md)

---

## When this applies

A quarantined record will never be usable, and should stop appearing as backlog.

`permanently_rejected` is a **human's terminal decision**. The state machine has no outgoing edge from it, and **the automated task never writes it and never overrides it.**

---

## ⚠️ Deliberately not an endpoint and not a DAG

There is no `POST /reject` and no `write_off` DAG. That is a decision, not an omission — the same discipline as Proposal C never becoming an HTTP endpoint:

> **Irreversible decisions should not have convenient buttons.**

The event is appended by hand, against PostgreSQL, with a recorded reason.

---

## Procedure

### 1. Confirm the record is genuinely unrecoverable

Check that it is not simply waiting for a rule that is about to loosen:

```sql
select raw_id, order_id, error_codes, quarantined_at
from `<project>.<dbt_dataset>.int_orders_quarantine`
where order_id = '<order_id>';
```

If the failure is a threshold that could reasonably move, use [proposal-b-rollout](./proposal-b-rollout.md) instead.

### 2. Confirm the current state in PostgreSQL

**PostgreSQL is authoritative for state**, not BigQuery — the warehouse mirror expires at 60 days.

```sql
select event_type, from_state, to_state, rule_version, event_at, reason
from quality_events
where order_id = '<order_id>'
order by event_at desc, id desc;
```

The latest `to_state` must be `quarantined` or `re_quarantined`. If it is already `permanently_rejected`, stop — the state is terminal.

### 3. Append the rejection event

```sql
insert into quality_events
  (raw_id, order_id, event_type, from_state, to_state, rule_version, reason)
values
  (<raw_id>, '<order_id>', 'rejection', '<current_state>', 'permanently_rejected',
   '<current DQ_RULE_VERSION>',
   '{"decided_by": "<name>", "why": "<the actual reason>", "ticket": "<ref>"}'::jsonb);
```

**`reason` is not optional.** It is the only record of why a human gave up on this row, and there is no path that would ever produce it again.

### 4. Flow it through

The event has to reach BigQuery and be picked up by the `int_` rebuild:

```bash
docker exec api-airflow-apiserver-1 airflow dags trigger orders_analytics_daily
```

### 5. Verify

| Check | Expected |
|---|---|
| `int_orders_quarantine` | the row is **gone** |
| `int_orders` / `fct_orders` | the row is **not** there either |
| `rpt_quality_backlog` | count decreased by one |
| `ods` row | **unchanged** — ODS is never modified |

A written-off record leaves both Gold and quarantine. It remains in ODS and in the event log forever, which is the point: **the decision is auditable even though it is final.**

---

## What must never be done

| Don't | Why |
|---|---|
| `UPDATE ods SET has_clean_error = FALSE` | violates the immutable anchor and bounded writeback ([ADR-0032](../adr/0032-bounded-writeback.md)) |
| `DELETE FROM ods` | destroys the denominator for every historical quality metric |
| `DELETE FROM quality_events` | the log is append-only; removing history is exactly what it exists to prevent |
| Write `permanently_rejected` from a script | the state is reserved for a human decision, enforced at the write target |

---

## Related

- [design/data-quality](../design/data-quality.md) — the state machine
- [proposal-b-rollout](./proposal-b-rollout.md) — the recovery path, if the record is not hopeless
