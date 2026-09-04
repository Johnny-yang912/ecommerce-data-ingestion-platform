"""
OpenAPI 的文字內容：服務描述與各端點的錯誤回應說明。

之所以獨立成一個模組而不是寫在 main.py 的裝飾器裡：這些字串是給【串接方】看的
規格，而 main.py 的註解是給【維護者】看的設計理由。兩者的讀者不同、變動頻率也不同，
混在一起會讓 main.py 那些解釋「為什麼是 def 不是 async def」的長註解被樣板稀釋。

內容一律用英文：規格的讀者是串接方，不必然讀中文。這是本 repo 唯一單語的文件，
其餘（README／ADR／design）維持 EN + TW 並行。

⚠️ 這裡的任何改動都會改變 openapi.json，因而讓 tests/test_openapi_snapshot.py 變紅。
   確認改動是有意的之後執行：UPDATE_OPENAPI=1 pytest tests/test_openapi_snapshot.py
"""

API_TITLE = "Order Ingestion API"

# OpenAPI 的 info.version 是必填欄位，所以這裡必須有一個值。
# ⚠️ 它【不】代表本專案採用語意化版本——見 CHANGELOG 的〈慣例〉：Phase 才是發布單位。
#    這個值只滿足 OpenAPI 的格式要求。
API_VERSION = "1.0.0"

API_DESCRIPTION = """\
Ingests e-commerce orders into a two-stage pipeline: a verbatim **Raw** record first,
then a flattened, cleaned **ODS** row written asynchronously by a worker.

## Authentication

Every endpoint except `GET /health` requires an `X-API-Key` header. A missing or
invalid key returns `401`. The key identifies the calling client, and that identity is
stored with every record it submits.

## The response you get is not the result

`POST /orders` returns `200` with `status: "pending"` as soon as the Raw record is
committed. **This is not a statement that the order was processed** — processing happens
afterwards, on a worker. Two consequences for a client:

- Do not treat `200` as success of the pipeline. It means *accepted and durable*.
- To learn the outcome, poll `GET /raw/{raw_id}` with the `raw_id` you were returned.

A `200` is returned even when the queue is unavailable, because the record is already
committed and a recovery scan will pick it up. Queue health is never the client's problem.

### Raw status values

| Status | Meaning | Terminal |
|---|---|---|
| `pending` | Accepted and stored, not yet claimed | no |
| `processing` | Claimed by a worker | no |
| `processed` | Written to ODS | yes |
| `error` | Processing failed; see `error_message` | yes |
| `duplicate` | The `order_id` already exists in ODS | yes |

## What is rejected and what is not

Only `order_id` is required. **Every other field may be missing, and the order will still
be accepted and stored** — incomplete records are flagged for downstream quality
reporting rather than refused. Unknown fields are preserved verbatim.

Two things are rejected at the door with `422`: a missing `order_id`, and a value whose
declared type cannot be converted (for example `NaN` in the integer field
`items[].quantity`).

**Duplicate `order_id` submissions are not rejected.** They are accepted with `200` and
reach the terminal status `duplicate`. A client that resubmits will not receive a `409`,
and should not treat the absence of one as confirmation that the order was new.

## Status codes and what to do about them

| Code | Meaning | What the client should do |
|---|---|---|
| `200` | Accepted and durable | Not a processing result — poll `GET /raw/{raw_id}` |
| `400` | Replay refused for this record's current status | Do not retry |
| `401` | API key missing or invalid | Do not retry; check `X-API-Key` |
| `404` | No record with this `raw_id` | Do not retry |
| `422` | Payload is invalid | **Do not retry** — the same payload will always fail. Fix it and resend |
| `429` | Per-client rate limit exceeded | Back off, then retry |
| `503` | Server busy (connection pool exhausted) | Retry with exponential backoff |

Error responses carry a `detail` field. For `422` it is a list of per-field errors; for
every other code it is a string.

## Rate limits

Limits are **per authenticated client**, not global — a global ceiling would let one noisy
upstream deny service to every other one. `GET /health` is unlimited.
"""

# --- 各端點的錯誤回應 -----------------------------------------------------------
#
# 只宣告端點【實際會回】的碼。
#
# 422 三個端點都會有（FastAPI 對任何帶驗證的端點自動加），但成因不同：/orders 是
# payload 不合規，另外兩個是路徑參數 raw_id 不是整數。預設描述一律是 "Validation
# Error"，那句話沒說出最重要的事——這個錯誤重試永遠不會成功。這裡逐端點覆寫掉它。
# 這正是本專案剛修過的那個缺陷的另一面：422 一旦變成 500，上游就會去重送一個
# 永遠不可能成功的 payload（見 CHANGELOG〈缺陷與修正〉）。

_UNAUTHORIZED = {
    "description": "API key missing or invalid. Do not retry.",
    "content": {"application/json": {"example": {"detail": "Invalid API key"}}},
}

_RATE_LIMITED = {
    "description": "Per-client rate limit exceeded. Back off, then retry.",
    "content": {
        "application/json": {"example": {"detail": "Rate limit exceeded: 60 per 1 minute"}}
    },
}

_NOT_FOUND = {
    "description": "No Raw record with this id. Do not retry.",
    "content": {"application/json": {"example": {"detail": "Raw not found"}}},
}



ORDERS_RESPONSES = {
    200: {
        "description": (
            "Accepted and durably stored. Not a statement that the order was processed — "
            "poll `GET /raw/{raw_id}` for the outcome."
        )
    },
    401: _UNAUTHORIZED,
    429: _RATE_LIMITED,
    503: {
        "description": "Server busy (connection pool exhausted). Retry with exponential backoff.",
        "content": {
            "application/json": {"example": {"detail": "Server busy, please retry later"}}
        },
    },
}

PROCESS_RAW_RESPONSES = {
    200: {
        "description": (
            "Replay dispatched. `triggered` is false when the record was already in a "
            "state that needs no replay."
        )
    },
    400: {
        "description": (
            "Replay refused for this record's current status. `force=true` accepts only "
            "`error` and `duplicate`; a `processed` record is never replayed. Do not retry."
        ),
        "content": {
            "application/json": {
                "example": {"detail": "Cannot force replay a processed record"}
            }
        },
    },
    401: _UNAUTHORIZED,
    404: _NOT_FOUND,
    429: _RATE_LIMITED,
}

RAW_RESPONSES = {
    200: {"description": "The Raw record and its current status."},
    401: _UNAUTHORIZED,
    404: _NOT_FOUND,
    429: _RATE_LIMITED,
}

HEALTH_RESPONSES = {
    200: {"description": "The process is alive. Requires no API key and consumes no rate limit."}
}


# --- 422 的描述 -----------------------------------------------------------------
#
# 422 不在上面那些 dict 裡，因為在 responses= 直接宣告它會【取代】FastAPI 產生的
# 那一份，連 HTTPValidationError 的 schema 一起蓋掉——而 422 是唯一 body 有結構的
# 錯誤（一個逐欄位的錯誤列表），把形狀弄丟了就等於沒說。
#
# 改成事後只覆寫描述：FastAPI 產生的 schema 完整保留，被換掉的只有那句沒有資訊量的
# "Validation Error"。

_PAYLOAD_INVALID = (
    "Payload is invalid: `order_id` is missing, or a value cannot be converted to its "
    "declared type (for example `NaN` in the integer field `items[].quantity`). "
    "**Do not retry** — the same payload will always fail. Fix it and resend."
)

_BAD_RAW_ID = "`raw_id` is not an integer. Do not retry."

VALIDATION_ERROR_DESCRIPTIONS = {
    ("/orders", "post"): _PAYLOAD_INVALID,
    ("/process_raw/{raw_id}", "post"): _BAD_RAW_ID,
    ("/raw/{raw_id}", "get"): _BAD_RAW_ID,
}


def build_openapi(app):
    """產生 OpenAPI 規格，並把每個 422 的描述換成帶重試指引的版本。

    先呼叫 FastAPI 的原始實作（它會把結果快取進 app.openapi_schema 並回傳同一個
    物件），再就地改寫描述——所以這裡不需要自己做快取。
    """
    from fastapi import FastAPI

    if app.openapi_schema:
        return app.openapi_schema

    spec = FastAPI.openapi(app)
    for (path, method), description in VALIDATION_ERROR_DESCRIPTIONS.items():
        response = spec["paths"][path][method]["responses"]["422"]
        response["description"] = description
    return spec
