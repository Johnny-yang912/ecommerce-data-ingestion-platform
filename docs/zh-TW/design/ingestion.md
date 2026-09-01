# 攝入層

[English](../../en/design/ingestion.md) | **繁體中文**

從 `POST /orders` 到 ODS 一列的路徑。決策住在 [ADR](../adr/README.md)；這裡描述它如何運作。

---

## 1. 狀態機

```
pending ──try_claim_raw()──► processing ──► processed
                                        ├─► error
                                        └─► duplicate
```

`processed`、`error`、`duplicate` 是終端狀態。只有 `error` 與 `duplicate` 可重放，經 `POST /process_raw/{id}?force=true`。

| 欄位 | 由誰設定 | 回答 |
|---|---|---|
| `raw.received_at` | API，在請求路徑中 | 這筆**資料**躺了多久 |
| `raw.processing_started_at` | `try_claim_raw`，認領成功時 | **這次嘗試**跑了多久 |

兩者永不互換——那個區分就是 [ADR-0015](../adr/0015-staleness-from-processing-started-at.md)。

**不變式：** `status = 'processing'` ⇒ `processing_started_at IS NOT NULL`，之所以成立是因為 `try_claim_raw` 是進入 `processing` 的唯一路徑。

---

## 2. 請求路徑

```
X-API-Key → verify_api_key()          缺失/無效回 401；client_id → request.state
    ↓
slowapi 限流                          以 client_id 為鍵，計數器在 Redis db 1
    ↓
OrderIN 驗證                          格式錯誤 → 在邊界回 422
    ↓
raw_body.replace("\x00", "")          剝除真正的 NUL 位元組，讓 Raw 寫得進去
    ↓
INSERT raw（重試 ×3）                  連線池耗盡 → 503
    ↓
COMMIT
    ↓
_enqueue(raw_id)                      有斷路器；吞掉所有失敗
    ↓
200 {"status": "pending"}
```

**即使派工失敗，回應仍是 `200 pending`。** Raw 那一列已經 commit 了；回 `500` 會讓客戶端重送，為一筆其實已被接受的訂單製造重複。[ADR-0013](../adr/0013-bounded-broker-wait.md)

限流是**逐已驗證客戶的，沒有全域限額**——全域上限會讓一個吵鬧的上游有能力對其他所有上游造成阻斷服務：

| 端點 | 每客戶限額 | 理由 |
|---|---|---|
| `POST /orders` | 60/分 | 攝入熱路徑；額度設在預期上游節奏之上，好讓一次合法的突發不會被節流 |
| `POST /process_raw` | 20/分 | 手動救援路徑——是人在重放記錄，不是機器。這裡速率一高就代表有人在跑迴圈，**那是一個值得被浮現出來的錯誤** |
| `GET /raw` | 120/分 | 唯讀檢視；很便宜，所以限額存在的唯一目的是限制住意外的輪詢 |

計數器住在 Redis db 1，不在行程記憶體——見 [ADR-0016](../adr/0016-recovery-scan-in-beat.md)。

---

## 3. 處理路徑

```
try_claim_raw()          CAS：UPDATE ... WHERE id=? AND status='pending'
    ↓                    rowcount != 1 → 別人拿走了，立即返回
json.loads
    ↓
ODSOrder.from_nested()   攤平巢狀 payload
    ↓
clean_order()            → (ods, has_clean_error, clean_error_message)
detect_schema_drift()    → (has_schema_drift, message, unmapped_fields)
    ↓
預檢 ODS.order_id        命中 → duplicate，不寫入
    ↓
COMMIT ODS + quality_events + raw.status='processed'   ← 同一個交易
```

兩個獨立、平行、非阻斷的訊號——絕不混用：

| 訊號 | 意義 |
|---|---|
| `has_clean_error` | **值**違反了業務規則 |
| `has_schema_drift` | **上游契約的形狀改變了**；未知欄位落進 `unmapped_fields` |

兩者都不中止 ODS 寫入。[ADR-0002](../adr/0002-has-clean-error-non-blocking.md)

---

## 4. 四個重試點

| # | 位置 | 耗盡時 |
|---|---|---|
| 1 | Raw 寫入 | 對客戶端回 `503` |
| 2 | CAS 認領 | `error` |
| 3 | 處理 | `error` |
| 4 | 狀態提交 | 記 `CRITICAL` log——該筆可能卡在 `processing`，由掃描恢復 |

全部使用指數退避。次數（`MAX_*_RETRIES = 3`）住在 `process.py` 開頭而非設定檔——它們是程式行為，不是環境。[ADR-0008](../adr/0008-config-boundary.md)

### 重試處理得了什麼、處理不了什麼

| 失效 | 由什麼處理 |
|---|---|
| 暫時性 DB 錯誤、連線瞬斷 | 重試 |
| 確定性錯誤（`DataError`、`ValueError`／NUL） | **快速失敗到 `error`**——重試確定性錯誤正是製造毒藥丸的方式（[ADR-0006](../adr/0006-nul-byte-fast-fail.md)） |
| 重複 `order_id`（`IntegrityError`） | **不重試** → `duplicate` |
| worker 在處理中途被殺 | 恢復掃描（[queue](./queue.md)） |
| broker 不可用 | 斷路器 + 恢復掃描 |

---

## 5. 冪等性

兩條約束，兩件事：

| 約束 | 保證 |
|---|---|
| `UNIQUE(ods.order_id)` | 每筆業務訂單一列 |
| `UNIQUE(ods.raw_id)` | 一列 Raw 最多產生一列 ODS——一條 1:1 血緣邊 |

First-write-wins，執行兩層：預檢以避免白做工，`IntegrityError` 作為 TOCTOU 競賽的後盾。**約束才是保證；預檢是最佳化。** [ADR-0005](../adr/0005-first-write-wins-idempotency.md)

---

## 6. Timeout 與連線池

| 設定 | 值 | 目的 |
|---|---|---|
| `statement_timeout_ms` | 30000 | 防止鎖等待掛死 |
| `pool_size` / `max_overflow` | 5 / 10 | 15 個併發連線 |
| `pool_timeout` | 30s | 耗盡時拋 `SATimeoutError` → 被捕捉 → `503` |

`/process_raw` 是背景任務而非同步工作，所以它無法卡住 event loop。

⚠️ `_enqueue()` 是同步的，可能阻塞完整的逾時時間——每一個 async 呼叫者都必須用 `asyncio.to_thread` 包起來。

---

## 7. 兩種身分，以及血緣

### `raw_id` 是物理身分；`order_id` 是業務身分

每一列都帶著兩個回答不同問題的識別碼，**而混淆它們會在每一層弄壞不同的東西**：

| | `raw_id` | `order_id` |
|---|---|---|
| 回答 | *這是哪一次攝入事件* | *這是現實世界的哪一筆訂單* |
| 由誰指派 | 這個系統，在寫入時 | 上游，在 payload 裡 |
| 在 Raw 唯一 | ✅（主鍵） | ❌ **刻意不唯一**——ADR-0001 |
| 在 ODS 唯一 | ✅ `UNIQUE(ods.raw_id)` | ✅ `UNIQUE(ods.order_id)` |
| 用於 | 1:1 血緣；`stg_` 的物理去重 | 業務去重；倉庫裡的 join |

因此 ODS 上那兩條 `UNIQUE` 約束並不冗餘。**它們說的是不同的事**：

- `UNIQUE(ods.raw_id)`——**一次攝入事件最多產生一列 ODS。** 一條血緣邊。
- `UNIQUE(ods.order_id)`——**一筆真實訂單只存在一次。** 一條業務不變式。

而那條血緣邊是**一路帶到底的**。`raw_id` 從 Raw 的主鍵出發，經 ODS、staging、`stg_`、`int_`，到 `fct_orders` 都還在（在 Gold 已不是鍵，只作血緣欄位）；`quality_events` 也以它為軸記錄每一次狀態轉移。**這筆資料從出生到終局的每一段，都掛在同一個識別碼上——任一層的任一列，都能靠它走回自己那份逐字的 payload。**

因此它撐著的不只是一條 join：`force=true` 重放靠它知道要重放哪一次攝入；[Proposal C](../runbooks/proposal-c-correction.md) 的前提「從 Raw 重產值」沒有它就不成立；[ADR-0053](../adr/0053-raw-text-ods-jsonb.md) 那句「Raw 逐字保留使重建成為可能」也是靠它兌現——**payload 留著卻走不回去，等於沒留。** `FK → raw.id`（`NO ACTION`）把「我們假設 raw 還在」變成「資料庫保證 raw 還在」，並順帶要求 **Raw 必須活得比它的 ODS 列久**。

> `raw.order_id` 可以是 NULL——沒解析成功的 payload 根本沒有業務身分，但它仍然有 `raw.id`，仍然要被搶佔、被處理、被計數。**業務身分是資料的屬性；物理身分是事件的屬性——而攝入層記的是事件。**

**物理去重要用物理身分**，那正是 `stg_orders` 的 window function 以 `raw_id` 而非 `order_id` 分組的原因（[ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)）。

> ⚠️ **`raw_id` 的唯一性只在單一落地實例之內成立。** 兩個 ODS 實例的序列都從 1 開始，把兩者抽進同一張 staging 表，`stg_` 的去重就會把毫無關係的訂單摺疊成彼此的「副本」——而且是靜默的。見 [verification/2026-08-raw-id-collision-two-ods](../verification/2026-08-raw-id-collision-two-ods.md)。

### 來源血緣：`source_client_id`

上面那條邊回答「這一列來自哪一次攝入」；這一條回答「那一次攝入來自誰」。

由 API key 解析出的 `client_id` 以 `source_client_id` 落在 Raw 與 ODS 上。因為它來自已驗證的 key 而非 payload，上游無法假冒他人。

**`NULL` 是有意義的，不是缺值**：它標記一列不是經由已驗證 API 進來的——手動重放、backfill、直接寫 DB。Raw 刻意讓「來源未知」保持可表達。

---

## 8. 相關

- [ADR-0001](../adr/0001-raw-no-business-dedup.md) · [ADR-0003](../adr/0003-duplicate-terminal-status.md) · [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0007](../adr/0007-static-api-key-not-jwt.md)
- [queue](./queue.md) — 派工、恢復、降級
- [data-quality](./data-quality.md) — `clean_order` 判定什麼，以及它日後如何改變
