# 2026-08-10 — Rate limiting across multiple processes

**English** | [繁體中文](../../zh-TW/verification/2026-08-10-rate-limit-multiprocess.md)

---

## What was being verified

Moving the recovery scan out of the API process (ADR-0016) allowed the API to run multiple uvicorn workers. **Does anything else in the API still hold per-process state?**

slowapi keeps rate-limit counters in process memory by default. The question was whether that mattered.

## Environment

api running **4 uvicorn workers**. Rate limit on `POST /orders` configured as `60/minute`, keyed on the authenticated `client_id`. 2026-08-10.

## Method

100 `POST /orders` sent against the **same API key**, counting responses.

Run twice — once with counters in process memory (`RATE_LIMIT_STORAGE_URI=` empty), once pointed at Redis db 1.

## Observed

| Counter storage | `200` | `429` |
|---|---|---|
| Process memory | **91** | 9 |
| Redis db 1 | **60** | 40 |

## Conclusion

With counters in process memory, `60/minute` is effectively **`60 × workers`**. The observed 91 rather than a clean 240 is simply because requests did not distribute evenly across the four workers — the ceiling is what moved, not the arithmetic.

Pointing the counters at Redis restores the limit to its stated meaning: **60 per authenticated client, in total.**

Redis db 1 is used deliberately — the broker occupies db 0, and a `celery purge` must not touch rate-limit counters. If Redis is unavailable, slowapi degrades to per-process counting rather than disabling limiting entirely.

## What this overturned ⭐

Not a stated conclusion, but something worse: **an unstated assumption that nothing announced was wrong.**

> The limit was silently 50% higher than configured, and **no error was raised anywhere.** Nothing logged, nothing alarmed, no test failed. The only way to find it was to count the responses.

The general lesson generalises past rate limiting:

> **Removing one piece of in-process state does not make a process stateless. It makes the *next* piece visible** — and that one was silent.

Making the API multi-process was a two-part change, and only the first part (the scan) was obvious in advance.

## Related

- [ADR-0016](../adr/0016-recovery-scan-in-beat.md) — the change that exposed this
- [ADR-0007](../adr/0007-static-api-key-not-jwt.md) — why the limit is keyed on `client_id`
- [design/queue](../design/queue.md)
