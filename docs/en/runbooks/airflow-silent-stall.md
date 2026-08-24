# Runbook: Silent scheduling stall

**English** | [繁體中文](../../zh-TW/runbooks/airflow-silent-stall.md)

---

## Symptom

A DAG that should have run didn't — **and nothing in the UI is red.**

No run was ever created, and without a run there is no failed run to show. If the dag-processor cannot finish parsing, DAGs are marked stale after `dag_stale_not_seen_duration` (600s), and **the scheduler creates no runs at all for a stale DAG.**

> ⚠️ **This failure mode has no built-in alerting.** Any detector for it must live outside Airflow — once every DAG is stale, a watchdog written as a DAG will not run either.

---

## Triage order — fastest signal first

```bash
# ① Is any DAG stale?  (quickest, most direct)
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True
#    non-zero = you have it

# ② When did parsing stop?  (once is_stale=True)
docker exec api-airflow-apiserver-1 airflow dags details <dag_id> \
  | grep -E "is_stale|last_parsed_time"

# ③ Is the dag-processor killing its parse subprocesses?
docker logs api-airflow-dag-processor-1 | grep -c "killing it"

# ④ Rule out genuine syntax / import problems
docker exec api-airflow-apiserver-1 airflow dags list-import-errors
```

### The time-saver

**A clean ④ does not prove the DAG file is fine** — but if ② and ③ look wrong, the DAG file is probably not the problem.

Rule the code out directly by parsing it by hand inside the container:

```bash
docker exec api-airflow-dag-processor-1 python -c \
  "from airflow.models.dagbag import DagBag; \
   d=DagBag('/opt/airflow/dags/<file>.py', include_examples=False); \
   print(list(d.dags), d.import_errors)"
```

> If the manual parse **succeeds** while the dag-processor **fails**, the fault is in the processor's supervision machinery — timeout arithmetic, resources, subprocess lifecycle — **not in the DAG code.** That fork in the road saves a lot of time.

---

## Two knobs — do not conflate them

| Setting | Default | Governs |
|---|---|---|
| `[dag_processor] dag_file_processor_timeout` | 50 | how long a parse subprocess lives before it is killed and retried |
| `[scheduler] dag_stale_not_seen_duration` | 600 | how long without a successful parse before a DAG is marked stale |

**Raising the former does not delay detection** — that is the latter's job. The former only changes how long a stuck parse waits before being killed and retried, and it does nothing for a **persistent** hang (killing it just reruns the same file). It matters only for transient hangs that a retry would clear.

---

## If containers are Up but nothing works

⚠️ **Container `Up` does not mean the mount succeeded.** A bind mount whose source was unavailable at container-create time degrades to an empty tmpfs, silently.

```bash
# Inside the container — "type tmpfs" on a path that should be a bind = you have it
docker exec <container> mount | grep -E "/opt/(airflow|project)"
#   healthy: ext4 or virtiofs
#   broken:  tmpfs
```

**A restart will not fix it.** Bind mappings are registered at container **create** time; `start` does not re-register them. Force-recreate:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --force-recreate
```

---

## If everything is green and there are still no runs

Heartbeats, container counts and healthchecks can all be normal while the scheduler creates nothing. **The only reliable signal is whether the `dag_run` table has new rows.**

```bash
docker compose exec airflow-db psql -U airflow -d airflow -c \
  "select dag_id, max(run_after) from dag_run group by dag_id order by 2 desc;"
```

⚠️ **An empty result is only evidence if there was a scheduling point inside the window you are looking at.** If there wasn't, force the question:

```bash
# Manually trigger the read-only probe — pushes a run through the full dispatch path
docker exec api-airflow-apiserver-1 airflow dags trigger raw_pending_watch
```

It writes one queued row and drives it to execution, dispatch and write-back — all scheduler main-loop work, which is exactly what is stuck in a zombie state. An answer arrives in a second or two.

---

## Related

- [incidents/2026-08-silent-scheduling-stalls](../incidents/2026-08-silent-scheduling-stalls.md) — four occurrences, four unrelated root causes, none of them red
- [design/liveness-alerting](../design/liveness-alerting.md) — why a detector must live outside the system it watches
