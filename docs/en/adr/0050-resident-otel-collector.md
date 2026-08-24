# ADR-0050: A resident Collector, and why `.env` avoids the OTel standard endpoint name

**English** | [繁體中文](../../zh-TW/adr/0050-resident-otel-collector.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-17 |
| **Layer** | Observability |

---

## Context

Applications can export telemetry straight to a cloud backend. Doing so puts the endpoint, the credentials and the export configuration into every process that emits anything — the API, the Celery worker, Beat, and eventually Airflow.

That is four copies of a secret and four places to change when the backend moves.

## Decision

**A resident OpenTelemetry Collector** (contrib 0.158.0, `otel/collector-config.yaml`). Applications always export to the local Collector in plaintext; **the cloud endpoint and credentials exist in exactly one place.**

Exporter: Grafana Cloud (`ap-southeast-1`, Tempo + Prometheus).

### ⚠️ `.env` deliberately avoids `OTEL_EXPORTER_OTLP_ENDPOINT`

This is the part worth recording, because the failure it prevents is silent.

**Any SDK that sees `OTEL_EXPORTER_OTLP_ENDPOINT` exports straight to whatever it names** — bypassing the Collector entirely. No error is raised, and *the data still arrives*, so nothing looks wrong. The Collector is simply no longer in the path, and with it go the credential centralisation, the processing pipeline, and the Views that control the series budget (ADR-0052).

The standard name is therefore **reserved for "app → Collector"**, and the cloud destination is configured under a different name that no SDK will pick up by accident.

This is the same reasoning as ADR-0008's refusal to re-declare OTel settings in `Settings`: **do not put a value where another layer's convention will silently consume it.**

## Consequences

**One place to rotate credentials or change backends.**

**Applications carry no cloud configuration**, so a local run or a test needs nothing beyond `otel_enabled=False` (the default).

**Instrumentation is no-op-safe.** With OTel disabled, the metric instruments are proxies — calling them does nothing and raises nothing. That is why the instrumentation points in `process.py` carry no `if otel_enabled` guards.

**Airflow was always able to join** — plaintext to `otel-collector:4318`, both already on the same compose network. **What blocks that integration is not technical**; every consumer of it has been deferred. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md).

**Trace context is stitched across processes.** The `api` → Celery → `worker` chain shares one `trace_id`, verified across both processes. SDK init hangs off `worker_process_init` because **`BatchSpanProcessor` is a background thread and threads are not inherited across `fork`** — the same root cause, and the same hook, as `_dispose_inherited_engine`.

**The cost is one more container** that must be running for telemetry to leave the host.

## Alternatives considered

**Direct export from each application.** Four copies of the credential, four places to change the backend, and no place to apply processing or sampling uniformly.

**Sidecar Collector per service.** More isolation, more containers, and the credential is back in N places.

**Use the standard `OTEL_EXPORTER_OTLP_ENDPOINT` for the cloud destination.** The silent-bypass hazard above. The convenience of a conventional name is not worth a failure mode that produces correct-looking data through the wrong path.

## Related

- [ADR-0008](./0008-config-boundary.md) — why `Settings` declares only `otel_enabled`
- [ADR-0051](./0051-logs-not-over-otlp.md) — the pillar deliberately left out of this path
- [ADR-0052](./0052-sdk-views-series-budget.md) — what the Collector path makes possible
