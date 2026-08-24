# ADR-0008: Centralised config covers environment values only, not algorithmic constants

**English** | [繁體中文](../../zh-TW/adr/0008-config-boundary.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Cross-cutting — configuration |

---

## Context

Before centralisation, each module called `load_dotenv()` and `os.getenv()` for itself. That is several sources of truth for the same value, each with its own default, each free to drift.

Centralising is easy. The hard part is deciding **where to stop** — because the natural end state of "put settings in the config file" is that every constant in the codebase becomes an environment variable, and at that point the system's behaviour is no longer described by its source code.

## Decision

A single `Settings` (pydantic-settings) instantiated once at startup. Modules do `from config import settings`; nothing else reads the environment.

**The boundary: only values that vary by deployment environment.**

| In `Settings` | Stays in its own module |
|---|---|
| `db_url`, `api_keys` | `MAX_CLAIM_RETRIES`, `MAX_PROCESS_RETRIES`, `MAX_STATUS_RETRIES` |
| `pool_size`, `max_overflow`, `pool_timeout`, `statement_timeout_ms` | `STALE_PROCESSING_MINUTES`, `PENDING_GRACE_SECONDS` |
| `celery_broker_url`, `rate_limit_storage_uri` | `SCAN_BATCH_SIZE` |
| `bq_project`, `bq_dbt_dataset`, `google_application_credentials` | |
| `log_format`, `otel_enabled`, `scan_interval_seconds` | |

The rule: **a retry count is program behaviour, not environment.** Changing it changes what the system does, so it should go through code review — not through an operator editing a `.env` on a Friday evening.

Three specific choices inside that boundary:

**`db_url` has no default.** A missing value raises at instantiation, so the process dies at startup rather than at the first connection attempt in the middle of a request.

**`api_keys` stays a raw string, not a dict.** Two reasons: it sidesteps pydantic-settings' automatic JSON parsing of dict-typed fields, and it keeps "how to interpret the key string" as auth-domain logic in `auth.py`, where it belongs.

**`otel_enabled` controls on/off only — not the endpoint, not the sampler.** Those go through the standard `OTEL_*` environment variables, which the SDK reads itself. Declaring them here would copy another domain's configuration surface into this file and create **a second truth that can drift from the first**. The reasoning is identical to the `api_keys` case: do not re-declare what another layer already owns.

## Consequences

**One place to look for anything environment-dependent**, and a `.env.example` in version control that documents it.

**Behavioural constants stay visible where the code that uses them lives.** A reader of `process.py` sees `MAX_CLAIM_RETRIES = 3` at the top of the file, rather than having to guess what an operator set.

**The cost is that "where is this configured?" has two answers**, and someone unfamiliar with the boundary has to learn it. That is the price of the boundary existing at all — and it is cheaper than the alternative, where the answer is "somewhere in the environment, good luck".

## Alternatives considered

**Centralise everything.** Every constant becomes an env var. Maximum flexibility, and the system's behaviour stops being reviewable — a production incident could be caused by a value that appears nowhere in the repository.

**Centralise nothing; keep `os.getenv` at call sites.** The original state. Multiple defaults for the same value, no fail-fast, no single template for deployment.

## Related

- [ADR-0007](./0007-static-api-key-not-jwt.md) — why `api_keys` is parsed in `auth.py`, not here
- [ADR-0050](./0050-resident-otel-collector.md) — the OTel endpoint decision this defers to
- [ADR-0009](./0009-alembic-single-source-of-truth.md) — the same "one source of truth" pattern, applied to schema
