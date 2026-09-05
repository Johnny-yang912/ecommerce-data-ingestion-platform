# Runbook: Starting the stack

**English** | [繁體中文](../../zh-TW/runbooks/airflow-startup.md)

---

## Prerequisites

`.env` must contain:

```
DB_URL, API_KEYS                          # always
BQ_PROJECT, GOOGLE_APPLICATION_CREDENTIALS  # for the analytics path (host path to the key)
AIRFLOW_UID                               # recommended, see below
```

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

---

## Start

```bash
# Ingestion stack only
docker compose up -d --build

# Ingestion + Airflow
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

**The two compose files must be layered into one project.** That is what lets the DAGs reach the business database at `db`, and lets seeding reach `api`. Running them as two separate projects puts them on different networks and both of those break.

| Service | URL |
|---|---|
| API | `http://localhost:8000` (docs at `/docs`, health at `/health`) |
| Airflow | `http://localhost:8080` (SimpleAuthManager locally, no login) |

Start order is gated automatically: `db` + `redis` (healthchecks) → `migrate` (`alembic upgrade head`, one-shot) → `api` / `worker` / `beat`.

---

## ⚠️ Two host-side traps

### `db` publishes on **5433**, not 5432

Controlled by `DB_PUBLISH_PORT`. If another PostgreSQL already holds 5432 on the host, a `5432:5432` mapping makes the service fail to bind outright.

Containers talk over `db:5432` and never traverse this mapping — **it exists only for `psql` from the host.**

### An exported `DB_URL` silently beats `.env`

Host-side tooling (`scripts/seed_demo.py --verify`, `psql`) must connect to `localhost:5433/orders`. `.env` already points there — but **`load_dotenv` defaults to `override=False`, so an environment variable in your shell wins.**

If an older `DB_URL` is exported, the script quietly connects somewhere else and reports on the wrong database.

```bash
# Check what your shell is actually exporting
echo "$DB_URL"
```

`verify()` prints the database it actually reached. **That line is the only place this mistake surfaces on its own** — read it.

---

## Verify it came up

```bash
# All services healthy
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps

# DAGs parsed, no import errors
docker exec api-airflow-apiserver-1 airflow dags list-import-errors

# ⚠️ No DAG marked stale — see airflow-silent-stall if this is non-zero
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True

# The ingestion path answers
curl -s localhost:8000/health
```

---

## Stop

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down

# Including volumes — destroys the database
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v
```

---

## Related

- [airflow-silent-stall](./airflow-silent-stall.md) — if DAGs parsed but nothing schedules
- [design/orchestration](../design/orchestration.md) — what each container is for
