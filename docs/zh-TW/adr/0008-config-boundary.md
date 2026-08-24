# ADR-0008：集中式設定只涵蓋環境值，不涵蓋演算法常數

[English](../../en/adr/0008-config-boundary.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 橫切 — 設定 |

---

## 背景

集中化之前，每個模組各自呼叫 `load_dotenv()` 與 `os.getenv()`。那是同一個值有好幾個真相來源，各有各的預設值，各自可以漂移。

集中化本身很容易。難的是決定**在哪裡停下來**——因為「把設定放進設定檔」的自然終點，是程式庫裡的每一個常數都變成環境變數，而到了那一步，**系統的行為就不再由它的原始碼所描述。**

## 決策

一個 `Settings`（pydantic-settings），啟動時實例化一次。模組一律 `from config import settings`；其他地方不讀環境變數。

**邊界：只放會因部署環境而異的值。**

| 放進 `Settings` | 留在各自模組 |
|---|---|
| `db_url`、`api_keys` | `MAX_CLAIM_RETRIES`、`MAX_PROCESS_RETRIES`、`MAX_STATUS_RETRIES` |
| `pool_size`、`max_overflow`、`pool_timeout`、`statement_timeout_ms` | `STALE_PROCESSING_MINUTES`、`PENDING_GRACE_SECONDS` |
| `celery_broker_url`、`rate_limit_storage_uri` | `SCAN_BATCH_SIZE` |
| `bq_project`、`bq_dbt_dataset`、`google_application_credentials` | |
| `log_format`、`otel_enabled`、`scan_interval_seconds` | |

判準是：**重試次數是程式行為，不是環境。** 改它就是改變系統做什麼事，所以它應該走 code review——而不是讓維運者在星期五晚上編輯一個 `.env`。

這條邊界之內有三個具體選擇：

**`db_url` 不給預設值。** 缺值時在實例化就 raise，所以行程在啟動時就死掉，而不是在某個請求中途第一次連線時才炸。

**`api_keys` 維持原始字串，不做成 dict。** 兩個理由：它避開 pydantic-settings 對 dict 型欄位的 JSON 自動解析，而且把「這個 key 字串該怎麼解讀」留在 `auth.py` 作為 auth 領域的邏輯，那才是它該在的地方。

**`otel_enabled` 只管開關——不管端點，不管取樣器。** 那些走 OTel 規格的標準 `OTEL_*` 環境變數，由 SDK 自己讀。在這裡再宣告一份，等於把別人領域的設定表面複製過來，並**製造出一份會與原本那份漂移的第二真相**。理由與 `api_keys` 的案例完全相同：**不要重新宣告另一層已經擁有的東西。**

## 後果

**任何與環境相關的東西只有一個地方可找**，而且版控裡有一份 `.env.example` 記錄它們。

**行為常數留在使用它的程式碼旁邊、看得見的地方。** 讀 `process.py` 的人會在檔案開頭看到 `MAX_CLAIM_RETRIES = 3`，而不必去猜某個維運者設了什麼。

**代價是「這個東西在哪裡設定？」有兩個答案**，而不熟悉這條邊界的人必須先學會它。那是「邊界存在」本身的價碼——而它比另一種情況便宜，在那種情況下答案是「在環境裡的某個地方，祝你好運」。

## 考慮過的替代方案

**全部集中。** 每個常數都變成環境變數。最大的彈性，而系統的行為不再可被審查——一次生產事故的成因，可能是一個在版控裡完全不存在的值。

**完全不集中，在呼叫點保留 `os.getenv`。** 原本的狀態。同一個值有多份預設、沒有 fail-fast、也沒有單一的部署範本。

## 相關

- [ADR-0007](./0007-static-api-key-not-jwt.md) — 為何 `api_keys` 在 `auth.py` 解析而不在這裡
- [ADR-0050](./0050-resident-otel-collector.md) — 這裡所讓渡的 OTel 端點決策
- [ADR-0009](./0009-alembic-single-source-of-truth.md) — 同一個「單一真相」模式，用在 schema 上
