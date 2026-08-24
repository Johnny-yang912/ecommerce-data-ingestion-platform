# 2026-08-12 — Rebuilding the fixture and walking v2→v3→v4 back to back

**English** | [繁體中文](../../zh-TW/verification/2026-08-12-proposal-b-v2-to-v4.md)

---

## What was being verified

Migrating to the native Docker Engine replaced Docker's DataRoot, which gave the business DB a brand-new volume. **That cost was weighed and accepted before the migration** — the fixture is simulated data that can simply be re-seeded, and keeping backups of it would mean paying maintenance cost for something with no value.

Since it had to be re-seeded anyway, the rebuild was run as a **full SOP exercise**: a deliberately shaped batch of data walked from v2 all the way to v4, filling in the branches the earlier runs could not cover.

## The one unavoidable trade-off

**Replay the *rule state*, not the *commit state*.**

`git checkout dq-rules-v2` does not work: `business_clean`'s `(ods, as_of)` signature, `NON_REPRODUCIBLE_CODES`, and the `AGE_MIN`/`AGE_MAX` constants were **all introduced in v3**, so a genuine rollback makes `reevaluate_quality.py` fail at import.

The approach instead: **HEAD's code + that version's thresholds + that version's label.**

For the two violation codes this exercises (`age_out_of_range`, `field_too_long`) that is behaviourally identical to the real older versions:

- the `as_of` parameter v2 lacked behaves the same when the ingest path omits it;
- `NON_REPRODUCIBLE_CODES` only matters during re-evaluation — and the v2 batch is only ever *ingested*, never re-evaluated *as* v2.

**The v4 slot is exactly identical**: `git diff dq-rules-v4 HEAD -- clean.py` is empty, so the end state is simply `git checkout clean.py` and the whole replay leaves **zero code residue**.

> **No git tags were touched.** `dq-rules-v2/v3/v4` point at real historical commits; adding or moving them for a data replay would pollute a real record with a synthetic one.

## Observed

| Cycle | Promoted |
|---|---|
| v2 → v3 | **16** |
| v3 → v4 | **15** |

Both cycles: idempotent on re-run, **ODS never modified**, control group left quarantined.

Combined with the two earlier walks, the SOP has now been exercised **four times**:

| Date | Cycle | Promoted |
|---|---|---|
| 2026-08-05 | v2 → v3 | 15 |
| 2026-08-11 | v3 → v4 | 3 |
| 2026-08-12 | v2 → v3 (rebuild) | 16 |
| 2026-08-12 | v3 → v4 (rebuild) | 15 |

## Conclusion

The SOP is repeatable across rule versions and across a full environment rebuild. The back-to-back run is the one that mattered: it exercises **v3 → v4 on data that was already promoted once**, which is the only path that touches the `promoted → re_quarantined` edge's neighbourhood.

## A note on what this record is not

This was **not** a disaster recovery test. The volume loss was a *planned consequence* of a migration decision, accepted in advance because the data was reproducible. Recording it as recovery would overstate it.

What it does demonstrate is the property that made the decision cheap: **a fixture that can be regenerated from a seeding script is not something you need to back up.** That is a design property of the simulated upstream, not a lucky escape.

## Related

- [2026-08-05-proposal-b-v3](./2026-08-05-proposal-b-v3.md) · [2026-08-11-full-compose-rebuild-v4](./2026-08-11-full-compose-rebuild-v4.md)
- [incidents/2026-08-silent-scheduling-stalls](../incidents/2026-08-silent-scheduling-stalls.md) — the migration that necessitated this
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md)
