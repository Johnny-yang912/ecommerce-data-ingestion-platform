# ADR-0041: `profiles.yml` — structure in version control, values in the environment

**English** | [繁體中文](../../zh-TW/adr/0041-profiles-yml-structure-vs-values.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08 |
| **Layer** | Orchestration |

---

## Context

dbt needs a `profiles.yml`. It contains the connection's *shape* — which adapter, which target, which retry settings — and its *values* — project id, dataset, credentials path. The shape should be reviewed like code. The values must not be committed.

Where the file lives is a second question, and it has a non-obvious answer.

## Decision

The file lives in `orchestration/dbt_profiles/`, pointed at explicitly by `DBT_PROFILES_DIR`. Its structure is in version control; every value is an `env_var()` reference.

**⚠️ Deliberately not in `ecommerce_dbt/`.** dbt's `profiles.yml` lookup puts the **current working directory ahead of `~/.dbt`**. Placing it in the dbt project directory would mean that a local `cd ecommerce_dbt && dbt run` suddenly consumes the orchestration profile and fails on unset environment variables. A dedicated directory leaves the existing local workflow untouched.

**⭐ It deliberately reuses the same environment variables as `config.py`** — `BQ_PROJECT`, `BQ_DBT_DATASET`, `GOOGLE_APPLICATION_CREDENTIALS`.

That is not merely convenience. **The `int_orders` that `reevaluate_quality.py` reads *is* the table dbt writes.** Configured separately, the two would silently point at different datasets, and re-evaluation would scan a stale or non-existent table **without erroring** — producing "no candidates found" and looking exactly like a healthy run with nothing to do.

> One shared variable makes that divergence impossible to express.

## Consequences

**The connection's shape is reviewable.** `job_retries: 1` — the adapter-level retry that ADR-0038 relies on instead of an Airflow retry — is visible in a diff.

**Credentials never enter the repository**, and the same file works across environments by changing the environment rather than the file.

**Producer and consumer cannot drift apart on the dataset.** This is the substantive win, and it closes a failure that would have been silent rather than loud.

**The local dbt workflow is unaffected.** `~/.dbt/profiles.yml` continues to serve interactive work; the orchestration profile is only used when `DBT_PROFILES_DIR` points at it.

**The cost is one more directory and one more environment variable to set correctly**, and a missing `env_var()` fails at dbt start-up rather than at parse time — loud, but later than ideal.

## Alternatives considered

**`~/.dbt/profiles.yml` inside the image.** Not in version control, so the shape is unreviewable and undiscoverable; and it would have to be baked in or mounted, both of which put credentials somewhere awkward.

**Put it in `ecommerce_dbt/`.** Breaks the local workflow, per the lookup-order trap above.

**Separate environment variables for dbt and for the analytics scripts.** More explicit, and it reintroduces exactly the silent dataset divergence this decision closes.

## Related

- [ADR-0038](./0038-asymmetric-retries.md) — the `job_retries` setting this file carries
- [ADR-0008](./0008-config-boundary.md) — the same environment variables, on the Python side
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — the consumer that must agree on the dataset
