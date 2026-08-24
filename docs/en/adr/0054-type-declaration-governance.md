# ADR-0054: Type coercion is governed by the declaration, not by coercion behaviour

**English** | [繁體中文](../../zh-TW/adr/0054-type-declaration-governance.md)

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07 |
| **Layer** | Data Quality — ingress |

---

## Context

An upstream changes a field's type. What the ingress layer does depends on whether Pydantic's lax mode can coerce the value into the declared type — **and that is asymmetric in the two directions**:

| Direction | Example | Pydantic behaviour | Result |
|---|---|---|---|
| Should be a string, upstream sends a number | `customer_name: 123` | does **not** coerce int→str | `ValidationError` → 422 + `ingress_rejected` (does not land) |
| Should be a number, upstream sends a coercible string | `age: "00501"` | **silently coerces** `"00501"` → `501` | passes, lands, computes correctly downstream |

The first row is a hard type error, rejected cleanly at the boundary. **The second row is the real blind spot**: the value conforms to the schema and computes correctly, but the fact that *"upstream sent an integer field as a string this time"* is silently swallowed at the Pydantic layer.

## Decision

**`TYPE_DRIFT` exists, and it deliberately does not go through Pydantic.**

`detect_schema_drift` runs on the **verbatim-preserved raw payload** ([ADR-0053](./0053-raw-text-ods-jsonb.md)) — the landing layer does not re-serialise through `OrderIN` — comparing JSON-native types against the contract and recording the **pre-coercion true type** as `has_schema_drift` + `TYPE_DRIFT`. Non-blocking ([ADR-0002](./0002-has-clean-error-non-blocking.md)).

### Coercion has a boundary; it is not "any string passes"

Only a **clean, integer-parseable** string passes:

| Input | Result |
|---|---|
| `"501"`, `" 501 "` | coerced → lands + `TYPE_DRIFT` |
| `"12.0"` | truncates to `12` → lands + `TYPE_DRIFT` |
| `"12.5"`, `"abc"` | **422**, does not land |

So the "changed type" row of the anomaly map means precisely: **coercible** → lands, flagged, observed; **hard type error** → 422 + `ingress_rejected`.

## The escalation: the declaration decides what gets silently rewritten

Coercion is *alignment toward the declaration*. Which pushes the problem up a level — from **the value** to **the declaration itself**.

Identifier-like fields are declared `str` **precisely to preserve leading zeros**:

```python
customer_id: Optional[str]     postal_code: Optional[str]     product_id: Optional[str]
age: Optional[int]             delivery_days: Optional[int]   tax_pct: Optional[float]
```

Declare `postal_code` as `int` by mistake and `"00501"` is silently truncated to `501` — semantics lost, and hard to notice. Conversely, only quantities that are **conceptually computable** are declared numeric.

> **Setting a type is not a formatting concern. It decides which deviations get silently swallowed and which get seen by `TYPE_DRIFT`.**

## The limit: a declaration cannot validate itself

`TYPE_DRIFT` catches *"the type upstream sent ≠ the declaration"*. It **cannot judge whether the declaration is correct** — because its comparison baseline **is** that declaration.

> If the baseline is wrong, `TYPE_DRIFT` measures accurately with the wrong ruler.

The declaration therefore needs its own protection. Three layers — the first two automatable, **the third necessarily human**:

| Layer | Mechanism | Guards | Does not guard |
|---|---|---|---|
| **1** Cross-layer consistency | `tests/test_schema_db_consistency.py` — `ODSOrder` (Pydantic) ↔ `ODS` (SQLAlchemy), per-field `python_type` comparison | changing `schema.py` but forgetting `models.py` (or the reverse); missing mappings | both layers declared wrong together |
| **2** Contract snapshot | `tests/test_schema_snapshot.py` — `model_json_schema()` against a committed golden file (`tests/snapshots/`) | any type-declaration change becomes a failing test **and a reviewable diff** | an intentional-but-wrong change (the snapshot updates with it) |
| **3** Human governance | CODEOWNERS on `schema.py` / `models.py` / `tests/snapshots/`, plus an upstream data contract | *"is this type actually correct"* | — this layer is the final arbiter |

Layers 1 and 2 collapse "pure discipline" into "a test goes red and a diff gets seen". But they only answer **consistent / not silently altered**.

**The correctness question — "should `age` be an `int` in the first place" — cannot be self-validated by any test**, because "correct" is defined as "matches the contract agreed with upstream", and that needs a source of truth outside the declaration.

So the final layer cannot escape human judgement:

- **CODEOWNERS** forces a designated data owner to review schema changes, so the snapshot diff is actually *looked at* — mechanism 2 provides the hook, the human provides the judgement.
- **A data contract** writes down each field's agreed type and rationale, so review has a comparable baseline.
- **`TYPE_DRIFT`'s drift rate can be used in reverse**: a field whose drift rate is chronically high is reasonable grounds to suspect **not that upstream is persistently wrong, but that your own declaration is.**

## Status

| Layer | State |
|---|---|
| 1 — cross-layer consistency test | ✅ in place, green |
| 2 — contract snapshot | ✅ in place, green (`tests/snapshots/ods_order.schema.json`, `order_in.schema.json`) |
| 3 — CODEOWNERS + data contract | ⏸ **not in place** — team-governance items with no team. See [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) |

## Alternatives considered

**Rely on Pydantic's validation as the governance point.** It is the obvious answer and it is the thing with the blind spot — coercion succeeds silently, so the layer that would report the drift is the layer that erased it.

**Turn off lax mode (strict types).** Every coercible-but-drifted payload becomes a 422 and does not land. That converts a monitoring signal into data loss, contradicting ADR-0002 — a value that computes correctly is not a reason to reject an order.

**Make `TYPE_DRIFT` blocking.** Would give the drift signal authority over Gold, which the two-signal boundary explicitly denies it: a clean order that merely arrived with a stringified number is not a bad order.

## Related

- [ADR-0053](./0053-raw-text-ods-jsonb.md) — the verbatim payload drift detection reads
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — the two-signal authority boundary
- [design/data-quality](../design/data-quality.md) — the 15-item anomaly map this row expands
