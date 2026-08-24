# ADR-0007: Service-to-service auth is a static API key, not JWT

**English** | [繁體中文](../../zh-TW/adr/0007-static-api-key-not-jwt.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06 |
| **Layer** | Ingestion — authentication |

---

## Context

This service is an ingestion unit inside an internal data mesh. Its callers are a small, stable set of upstream services — machine-to-machine. **There are no human users**, so there is no login, no session, and no user identity to carry.

JWT is the reflexive answer to "add authentication", and most of what it buys is irrelevant here: stateless verification of claims issued by a separate identity provider, short-lived tokens, and a user subject. There is no identity provider, nothing to expire against a user session, and no subject other than "which upstream service is this".

## Decision

A static `X-API-Key` header, resolved against a mapping loaded from `.env`:

```
API_KEYS = key1:client_a,key2:client_a,key3:client_b
```

- **Multiple keys may map to one `client_id`**, which is how rotation works: overlap the old and new key, redeploy, then drop the old one.
- Comparison uses `secrets.compare_digest` — constant-time, so a byte-by-byte timing leak cannot be used to guess a key.
- Missing or invalid key returns **401** (`auto_error=False` on the header dependency, because FastAPI's default is 403, and 403 means "authenticated but not permitted" — a different thing).
- A malformed entry in the mapping is skipped rather than fatal, so one bad pair cannot take down the whole configuration.
- Only the **first six characters** of a rejected key are ever logged.

The resolved `client_id` does double duty:

1. It lands on Raw and ODS as `source_client_id` — **the origin of data lineage**, answering "which upstream sent this row".
2. It is set on `request.state` and becomes the rate-limiting key, so limits apply per authenticated identity rather than per network address.

The second one has a timing constraint worth recording: **dependency resolution is the only moment early enough.** slowapi's rate-limit check runs at the very front of its wrapper, before the endpoint body executes — setting `client_id` inside the endpoint would be too late for `key_func` to read it.

## Consequences

**Adding a source is a planned infrastructure event**, not a runtime operation: edit `.env`, redeploy. For a fixed set of internal upstreams this matches how the change actually happens anyway.

**Revoking a key requires a deploy.** This is the real cost. At this scale the exposure window is acceptable; with external or self-service clients it would not be.

**Lineage comes free with authentication.** Because `source_client_id` is derived from the verified key rather than from the payload, an upstream cannot claim to be someone else by putting a different value in the body.

**`NULL` in `source_client_id` is meaningful, not missing.** It marks a row that did not arrive through the authenticated API — a manual replay, a backfill, a direct DB write. Raw deliberately preserves "origin unknown" as an expressible state rather than inventing a placeholder.

## Alternatives considered

**JWT.** Buys stateless claim verification and expiry against an identity provider that does not exist here, in exchange for key management, clock-skew handling, and a rotation story that is more complex than "overlap two static keys".

**A DB-backed `api_clients` table.** Would give runtime key management and instant revocation. Rejected for now because it adds a database read to the hot ingestion path and an admin surface to maintain, for a caller set that changes on the order of never.

## Revisit when

The service expands to multiple domains or multiple tenants, or acquires callers that are not internal — at which point runtime key management stops being optional and the `api_clients` table becomes the right shape.

## Related

- [ADR-0008](./0008-config-boundary.md) — why `api_keys` stays a raw string in `Settings`
- [Ingestion design](../design/ingestion.md) — rate limiting, and why there is no global limit
