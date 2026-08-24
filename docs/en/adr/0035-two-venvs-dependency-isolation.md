# ADR-0035: Dependency isolation — two venvs, nothing installed into Airflow itself

**English** | [繁體中文](../../zh-TW/adr/0035-two-venvs-dependency-isolation.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

Airflow and dbt-bigquery both depend heavily on `google-cloud-*`, `protobuf` and `jinja2`. Installing them into one environment is a well-known source of version conflicts, and it creates a standing hazard: **every Airflow upgrade risks breaking dbt**, and vice versa, with the breakage appearing at DAG parse time rather than at install time.

| Option | Trade-off |
|---|---|
| `pip install` into the same env | Simplest; high conflict risk |
| **Separate venvs + `BashOperator`** | Clean isolation, zero extra infrastructure, matches official guidance |
| `DockerOperator` with its own image | Cleanest, best prod parity; requires mounting `docker.sock` |

## Decision

Two virtualenvs inside the Airflow image, and **nothing project-related installed into Airflow's own environment**:

```
/home/airflow/venvs/analytics   ← requirements-analytics.txt
/home/airflow/venvs/dbt         ← dbt-core / dbt-bigquery 1.11
```

Tasks are `BashOperator`s invoking the right interpreter.

This is also why `requirements-analytics.txt` exists as a separate file: the Airflow container must be able to *run the extraction scripts* without pulling in pytest and the rest of the development toolchain.

**No Cosmos.** Model-level task granularity would improve observability, and for a 13-model project that benefit is out of proportion to the cost — a dependency that must track both dbt's and Airflow's versions, i.e. exactly the coupling this decision exists to avoid.

## Consequences

**Airflow upgrades and dbt upgrades are independent.** Each venv is resolved separately; neither can constrain the other.

**⚠️ The image's dependencies do not update with a bind mount.** DAG *files* are mounted and picked up on the next parse, but the venvs live in the image. Changing `requirements-analytics.txt` requires a **rebuild** — a restart only starts the old container, and does not rebuild or recreate it.

This has bitten in practice: when OTel was added, `process.py` gained a `telemetry` import, and a read-only probe running in the analytics venv died at `ModuleNotFoundError` because the image had not been rebuilt (ADR-0039).

**The cost is image size and build time** — two dependency trees instead of one — and one more thing to remember when adding a dependency: *which* venv.

## Alternatives considered

**One shared environment.** Simplest until the first conflict, at which point the resolution is to pin something that breaks something else.

**`DockerOperator`.** Genuinely cleaner and closer to production shape, at the cost of mounting `docker.sock` into the Airflow container — a meaningful privilege escalation for a single-host setup.

**Cosmos.** Per-model tasks and lineage in the UI, for a dependency coupled to both upgrade cycles. Revisit if the model count grows enough that "which model failed" stops being obvious from the log.

## Related

- [ADR-0036](./0036-dag-no-toplevel-import.md) — the other half of keeping DAG parsing dependency-free
- [ADR-0040](./0040-layered-dbt-execution.md) — how dbt is invoked from these venvs
- [ADR-0039](./0039-observation-signals-own-dag.md) — the incident that showed the rebuild requirement
