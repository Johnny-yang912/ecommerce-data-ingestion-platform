# ecommerce-data-ingestion-platform
### 電商訂單資料管線 — 資料生命週期管理實踐

[English](./README.md) | **繁體中文**

[![CI](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml)

以資料生命週期管理為核心，設計一條電商訂單資料管線——在可能發生故障與重複提交的高併發環境中，確保資料盡可能可靠地進入管線作為源頭基礎，透過各層品質合約讓資料從不可信的原始輸入逐步轉化為可信的分析資料，並以規則版本化與品質事件追蹤，實現跨 pipeline 的資料品質治理與完整的生命週期可追溯性。

此專案以資料工程為主軸，後端工程為地基。攝取層的容錯設計（多層 retry、crash recovery、CAS claim）確保資料進得來；資料架構的分層品質合約（Raw → ODS → dbt stg/int/dim/fct）確保資料流得正確；規則版本化與 append-only 的 `quality_events` 狀態機，確保品質評估的演進有跡可查。

---

## 技術棧

| 層級 | 技術 |
|---|---|
| API 框架 | FastAPI |
| ORM | SQLAlchemy |
| 資料庫 | PostgreSQL |
| Schema 驗證 | Pydantic |
| 環境設定 | python-dotenv |
| 時區處理 | pytz |

---

## 整體架構

```
【目前已實作】

POST /orders
    ↓
[Raw Table]  ←── 原樣落地，不可修改                      status: pending
    ↓
[Background Task: process_raw_event]
    ├── try_claim_raw()         ← 原子性 UPDATE，搶佔這筆資料（CAS）
    ├── JSON 解析
    ├── ODSOrder.from_nested()  ← 把巢狀結構攤平
    ├── clean_order()           ← 格式清洗 + 業務規則驗證
    ├── first-write-wins idempotency check
    └── [ODS] + [quality_events]  ← 不可變錨點，含品質標記與事件日誌

[ODS] → 增量抽取（Airflow 每日排程）→ BigQuery staging
    ↓
dbt stg_*   Hard Gate tests                 ← Silver 入口，仍含全部資料
    ↓
dbt int_*   Row Filter                      ← Gold 入口，攔截在這裡
    ├── 有效品質狀態＝乾淨 → int_orders → int_order_items
    └── 有效品質狀態≠乾淨 → int_orders_quarantine
        （有效狀態＝ODS 快照 ⊕ quality_events 最新事件，非 has_clean_error 字面值）

【Phase 4–5 目標】

dbt int_*   場景專用 int_orders_*            ← 設計已備妥，待真實場景才啟用
                                  （接受場景無關的欄位錯誤，補值後流入）
    ↓
dbt dim_*/fct_*/rpt_*
    ↓
Looker Studio（接 BigQuery dim_*/fct_*/rpt_*）
```

---

## 各層品質合約

品質管控責任隨資料往下游流動而逐步收緊。ODS 是不可變的錨點，保留完整資料（含髒的）；攔截發生在 `dbt int_*` 這一層。

```
Raw (PostgreSQL)
  職責：完整落地所有進來的請求，不做任何品質假設
  品質要求：無
  可修改：否

ODS (PostgreSQL)                               ← Bronze / 錨點
  職責：基本清洗與業務規則驗證，保留完整資料狀況
  品質要求：格式標準化；業務問題只標記，不攔截
  可修改：否

dbt stg_*                                      ← Silver 入口
  職責：1:1 對應來源，型別對齊、欄位命名標準化
  品質要求：與 ODS 相同，仍保留所有資料含髒的

────────────── 攔截發生在這裡 ──────────────

dbt int_*                                      ← Gold 入口
  職責：跨表 join、衍生欄位、業務邏輯
  品質要求：只讓乾淨資料通過（has_clean_error = FALSE）
  髒資料去向：int_orders_quarantine
  場景補值：場景專用模型可接受與該場景無關的欄位錯誤，補值後供特定分析使用，補值邏輯記錄在 SQL 文件

dbt dim_*/fct_*                                ← Gold
  品質要求：最乾淨，不含任何 has_clean_error = TRUE 的記錄

dbt rpt_*
  職責：固定粒度預聚合，BI Dashboard 直接使用
  品質要求：同 dim_*/fct_*
```

完整 DQ 架構設計（攔截機制、Quarantine 處理、Remediation 路徑、歷史指標）見 [DQ_ARCHITECTURE-TW.md](./DQ_ARCHITECTURE-TW.md)。

---

## 設計決策

### 資料架構決策

**Raw 層不做業務去重**
`Raw.order_id` 刻意不加 UNIQUE 約束。Raw 的職責是完整記錄所有進來的請求，包含重複提交——不同提交之間可能欄位互補，異常的提交頻率本身也是訊號（攻擊偵測、用戶端 bug）。去重的責任下放到 ODS 層。

**ODS 層 items 欄位使用 JSONB，Raw 層使用 TEXT**
Raw 是 landing 層：**逐字保留原文**（直接存原始 request body，而非重新序列化的 `model_dump()`），不對其結構做任何假設，上游 schema drift 時也不會靜默丟失未知欄位。只有 `order_id` 被**抽取為關鍵追溯欄位**（供索引與 idempotency 查詢），其餘欄位原封不動留在 payload 文字內。TEXT 語義最貼切——資料庫不解析、不驗證，原樣存入。ODS 的 items 已經過 Pydantic 驗證與清洗，結構有保證，JSONB 讓資料庫在寫入時多一道格式驗證，且保留未來在 SQL 層直接查詢 items 內部欄位的彈性。

**資料清洗分層（`format_clean` + `business_clean`）**
`format_clean()` 處理格式問題（統一小寫、去空白）。`business_clean()` 驗證業務規則（數量不能為負、評分要在 1–5 之間、出貨日不能早於訂單日等）。兩者職責分離，分別對應「格式標準化」與「業務語意驗證」兩個不同層次的問題。

**`has_clean_error` 非阻斷**
業務規則驗證結果只標記在 `has_clean_error` 欄位，不拒絕寫入 ODS。攔截責任下放到 `dbt int_*`，理由來自兩個具體機制：
一、Quarantine 是可分析的業務層——進入 ODS 的髒資料已完成格式標準化（`format_clean` 先於業務驗證執行），`int_orders_quarantine` 能直接以業務欄位切片做 RCA，無需回溯解析 Raw payload；
二、支援規則演進後的 Proposal B 重評估——當規則升版，ODS 中的 quarantine 記錄才有對象可以重評估並 promote，若在攝入時就攔截，規則演進只能對新資料有效。

**ODS 層 first-write-wins idempotency**
同一 `order_id` 只有第一筆能寫入 ODS，由**兩道防線**共同實現——關鍵是它們各自負責**不同來源的重複**，不是同一件事做兩遍：

- **UNIQUE 約束（`UNIQUE(ods.order_id)` + `UNIQUE(ods.raw_id)`）—— 負責「正確性」**：這是冪等性的最終保證，**不可省**。因為 pre-check 的 `SELECT` 與後續 `INSERT` 之間必然存在一個 TOCTOU 窗口，讓並發的兩個 worker 同時通過 pre-check。pre-check 先天無法靠自己關掉這個窗口，根因有二：① 本專案用預設的 **READ COMMITTED** 隔離級，**沒有謂詞鎖（predicate lock）**，無法對「條件」上鎖去阻止別人插入符合條件的新列（phantom）；② 就算改用 `SELECT ... FOR UPDATE`，也鎖不到「尚不存在的列」——沒有列就沒有東西可鎖。唯有 DB 層的 UNIQUE 約束能關上這個窗口。
- **pre-check（commit 前先查 ODS 是否已有此 order_id）—— 負責「常見情況的效率與語意」**：就正確性而言這道是多餘的，但它處理的是系統裡**占多數、且非並發**的重複來源——主要是 `scan_and_recover` 把 stale `processing` 重設後重跑、或 scan 把已在佇列中的記錄又排一次（見「Known Issues」）。這些是時序錯開、預期會反覆發生的重複。

為什麼不只靠 UNIQUE？因為 success path 是把 **ODS + quality_event + Raw `status` 綁在同一個 transaction 一起 commit**。若沒有 pre-check，每一筆重複都得「建好整個 ODS 物件 → 嘗試 commit → 撞 UNIQUE → 整個交易 abort → rollback → 再單獨補一次 status」。對高頻的 scan 重跑而言，pre-check 用一個便宜的 `SELECT` 就在建物件前攔下，**走快路、不必每次觸發一次注定 abort 的交易**，也避免拿 `IntegrityError` 這個 exception 當正常控制流——畢竟 `duplicate` 在本設計裡是**預期且有意義的監控訊號**，不是錯誤。

兩道防線是 **快路（pre-check）+ 兜底（UNIQUE）** 的互補關係，不是二選一。後進的重複 Raw 一律不報錯，而是寫入 `duplicate` 終態，讓監控能明確區分正常處理與重複攔截。

**TOCTOU race 下兩道防線如何接力**（兩個 worker 並發處理同一 order_id）：

```
        Worker A (raw_id=1)              Worker B (raw_id=2)
          │                                │
 t1       │ SELECT order_id → 查無          │
          │                                │
 t2       │                                │ SELECT order_id → 查無
          │                                │   ← READ COMMITTED 看不到
          │                                │     A 尚未 commit 的列
          │ ── pre-check 兩邊皆通過（空窗）──│
          │                                │
 t3       │ INSERT ODS                     │
          │ commit ✔ (first write wins)    │
          │                                │
 t4       │                                │ INSERT ODS → 撞 unique index
          │                                │   被阻塞，等 A 的結局
          │                                │
 t5       │                                │ A 已 commit → IntegrityError
          │                                │   rollback → 標記 duplicate ✔
          ▼                                ▼
       ODS 內該 order_id 僅一列；raw_id=2 進入 duplicate 終態
```

註：空窗從 t1 一直延續到 A 在 t3 真正 commit 為止；在那之前進來做 pre-check 的 worker 全都查無、全都會通過——這正是 UNIQUE 兜底不可省的原因。（若 A 在 t5 是 rollback 而非 commit，B 的 INSERT 會改為成功遞補，仍符合 first-write-wins。）

**嚴格禁止繞過 Raw 直接落地 ODS（single-ingress invariant）**
本服務是資料網格的攝取單元、呼叫者是少數且穩定的 machine-to-machine 上游、無人類使用者（見〈服務對服務驗證決策〉），新增來源是有計畫的基建事件。這個定位抽掉了「臨時人工直寫 DB」這類繞過的正當動機——因此本系統把「所有資料一律經 Raw 進入」立為硬不變式：**ODS 永遠是 pipeline 的產物，不接受任何繞過 Raw 的直落列**。
理由不只「可行」，更是「必要」：① `source_client_id`（資料血緣起點）由驗證層解析，繞過 Raw 的列無從建立來源；② Raw 逐字保留是「可重建」承諾的根（Proposal C 從 Raw 重產值），無 Raw 錨點的列無法被重建；③ `has_clean_error` / schema drift / `quality_events` 初評全由 `process_raw_event` 產生，直寫 ODS 會讓品質狀態機出現無中生有的列。值得一提的是，連 schema 容忍的「手動 replay / backfill / 直接寫 DB」也是建模在 **Raw 層**（`Raw.source_client_id` 可為 NULL 代表來源未知的 Raw 列），而非 ODS——設計本就一致地把「直寫」收束在 Raw。
此不變式已從「政策」升級為「DB 保證」：`ods.raw_id` 為 **NOT NULL** + **FK `ods.raw_id → raw.id`（`ON DELETE NO ACTION`）**。前者擋掉「無錨點的孤兒列」，後者擋掉「捏造一個不存在的 raw_id 直寫」，並保證 raw 不得先於其 ODS 子列被刪（連帶保護 Proposal C 的重建前提）。

**raw_id 是物理身分、order_id 是業務身分——連帶下游去重鍵的選擇**
ODS 兩把唯一鍵分工不同：**`raw_id` 是物理／代理身分**（landing 層發的代理鍵，與一筆物理落地記錄 1:1、immutable、血緣錨點）；**`order_id` 是業務自然鍵**（其唯一性是「當前的業務約束」，可能隨業務演進而變——訂單版本化、退貨拆列、SCD 等）。
這個區分直接決定**下游去重鍵的選擇**：去重的本質是「把同一筆物理記錄的多份副本收斂成一份」，屬物理身分操作，故 dbt `stg_` 去重以 **`raw_id`** 為 grain，而非 `order_id`。BQ staging 是 append-only 鏡射，Proposal C 修正列與例行重抽都會產生同一筆記錄的物理重複，收斂它們的依據是「同一物理列」＝ raw_id；若改用 order_id，等於把物理去重耦合到一條可能放寬的業務約束上，一旦 order_id 唯一性鬆動就會默默吃掉本該並存的列。下游 `int_*` 合成「有效品質狀態」時 JOIN `quality_events` 也以 raw_id 為鍵，與去重 grain 一致。
⚠️ **但 `raw_id` 的唯一性只在「單一 landing 實例」內成立。** 兩個獨立的 ODS 各自從 1 開始編號，抽進同一張 BQ staging 會讓以 `raw_id` 為 grain 的去重把不相干的訂單當成彼此的副本收斂掉——不報錯、不留痕跡。這等於把「一張 staging 只對應一個 ODS」焊成了管線的隱含前提；多實例上游若各自有 landing，去重鍵需升級為 `(source_instance, raw_id)` 之類的複合鍵。實例見 [ORCHESTRATION-TW §5.4](./ORCHESTRATION-TW.md)。

這也是 `raw_id` 必須 NOT NULL 的隱性理由：`UNIQUE` 在 PostgreSQL **不拒絕 NULL**（允許多筆 NULL 並存），`UNIQUE + nullable` 會讓「以 raw_id 為 grain 的去重」把多筆 NULL 列收斂成一列而靜默漏資料；收成 NOT NULL（+ FK）後此破口閉合。

**`Raw.status` 與 `ODS.order_status` 無關**
`Raw.status` 是 pipeline 狀態機（`pending → processing → processed / error / duplicate`），由 `try_claim_raw` 與 `_commit_raw_status` 驅動。`ODS.order_status` 是業務欄位，從進來的 payload 攤平而來，描述訂單在攝入當下的履約狀態（如 `"confirmed"`、`"pending_payment"`），與 pipeline 的處理進度無關。此 API 只負責接收訂單建立事件；後續狀態變更（付款、出貨、取消）來自其他系統，超出此 pipeline 的 scope，由 dbt 層 JOIN 其他來源表組合。

**`force=True` 的語意邊界：單筆重試，而非 Backfill**
`POST /process_raw/{raw_id}?force=true` 只允許對 `error` 或 `duplicate` 狀態的記錄使用，語意是「重試這筆處理失敗的記錄」。對 `processed` 的記錄呼叫會直接回 400——因為若下游（Star Schema、聚合統計表）已消費過此筆 ODS，單獨刪除再重寫 ODS 無法 cascade 修正下游，反而製造不一致。Quarantine 記錄（`has_clean_error=TRUE`，`status="processed"`）的問題是規則評估而非 pipeline 失敗，正確的 remediation 路徑是 Airflow 重評估（Proposal B），不是重跑 pipeline。

### 攝取層可靠性決策

**原子性 claim（`try_claim_raw`）**
用 `UPDATE ... WHERE status = 'pending'` 再檢查 `rowcount == 1`，確保同一筆 `raw_id` 在多個 worker 並發時只有一個能搶到，不需要悲觀鎖。要注意它與下方 ODS 冪等性守的是**正交的兩個維度**：冪等守護（pre-check + `UNIQUE(order_id)`）key 在 **order_id（業務身分）**上，攔的是「不同 raw_id、相同訂單」的業務重複；CAS key 在 **raw_id + status（物理身分）**上，攔的是「**同一筆 raw_id** 被排給多個 worker」——這正是 Known Issues 裡 scan 看不到 BackgroundTasks 佇列、可能把已排隊記錄重複排程所製造的重複來源。它的價值有兩面：

- **效率——最早、最便宜的退出點**：敗者拿到 `rowcount=0` 後在 `process_raw_event` 一開頭就直接 return，發生在任何 JSON parse / flatten / clean / 建 ODS 物件**之前**。這比 order_id 的 pre-check 還早（pre-check 要先把整個 ODS 物件建好才查 order_id），所以對「同 raw_id 被重複排程」這個高頻情境，CAS 是成本最低的攔截點，省下的是整段 pipeline 的白工與 DB round-trip。
- **正確性——避免 Raw.status 被競寫、避免「自我標記 duplicate」**：冪等守護只保證 **ODS 表**裡 order_id 唯一，它**管不到 Raw.status 狀態機**。若沒有 CAS，兩個 worker 同時處理 `raw_id=1`：A 寫入 ODS 並設 `processed`，B 撞到 `UNIQUE(raw_id)` 後進 duplicate 分支、用 order_id 反查卻發現 existing_raw_id 正是自己，於是把**自己標成「duplicate of 自己」**——`processed` 與 `duplicate` 兩個寫入互相競爭，最終狀態不可決定。ODS 那一列是對的（UNIQUE 擋住了），但狀態語意是壞的。CAS 讓「一個 raw_id 只有一個擁有者」，狀態轉換因此確定且有意義。

單就「ODS 不會有兩列」而言，`UNIQUE(raw_id)` 已是足夠的 backstop；CAS 不可取代的價值在**狀態機正確性**與**效率**這兩個維度。這恰好對稱於 pre-check 之於 `UNIQUE(order_id)`——CAS 之於 raw_id，就是「快路徑 + 狀態語意」，背後一樣有一道 DB 層 UNIQUE 兜底。

**以認證身分（`client_id`）為 key 做 per-client 限流，不加全域上限**
限流的 key 是 auth 解析出的 `client_id`，而非來源 IP。我們真正想框住的主體是「單一上游來源」（異常送單頻率 / client 端 bug），而 `client_id` 就是這個主體——由 auth 層建立的穩定身分，免疫網路拓樸。IP 只是它的代理，一旦牽涉拓樸就失真：多個上游共用同一 NAT 會共用計數器（誤殺）、單一上游散在多 IP 會拿到 `N × 上限`（單客戶上限被悄悄繞過）、在 LB 後面所有人縮成 proxy IP（per-IP 退化成全域上限）。改 key 在 `client_id` 上一次解掉這三個問題。

這之所以可行，是因為 **auth 跑在限流檢查之前**：`@limiter.limit` 包住 endpoint，其檢查跑在 wrapper 最前面——也就是 FastAPI 解析完依賴*之後*。所以未認證請求會先以 `401` 被擋下、根本不進計數器；而 `verify_api_key` 早已把 `client_id` 落到 `request.state`，等 key_func 來讀時已就緒。這個順序還有個附帶好處：只有通過認證的 client 才會建立計數項，所以偽造大量來源 IP 的洪水無法撐爆 in-memory 的限流 storage。

**限額語意**：因為 key 是 `client_id`，每條限額現在是**每個上游、跨它所有 key 與所有 IP 的合計**，而非每 IP。對目前「少數穩定、單實例」的上游而言數字實質相同；若某上游日後水平擴展,限額需重新校準為「該上游整體的公平份額」。針對單實例圍堵的進一步細化，是複合 `client_id × IP` key（見下方部署注意事項）。

全域上限刻意不加：它的數字必須從「預期同時活躍 client 數 × per-client 上限」推導，沒有真實流量資料時無從決定。更根本地，`/minute` 視窗無法防瞬間 burst，而 pool 耗盡已由 `SATimeoutError → 503` 妥善處理。因此 rate limiting 的職責只保留在「防單一 client 持續性濫用」；匿名洪水 DoS 交給 gateway/LB（限流在 auth 之後，根本看不到它）。

**Pool 耗盡快速失敗（503）**
`POST /orders` 額外 catch `SATimeoutError`（pool 等不到連線），直接回傳 503 Service Unavailable，不走 retry loop。Pool 耗盡是資源競爭問題而非 DB 故障，retry 無法改善，應快速失敗讓 client 自行退讓。

### 服務對服務驗證決策

**用 API Key，而非 JWT 使用者登入**
本服務的定位是資料網格中的**攝取單元**，呼叫者是少數且穩定的上游服務（machine-to-machine），**沒有人類使用者**。教學常見的 JWT 帳密登入流程是為「使用者」設計的，套到這裡會造成架構不協調。故採服務對服務驗證：上游持有 `X-API-Key`，命中即放行。比對用 `secrets.compare_digest`（constant-time），key 不進 log、不進 `raw_payload`。

**Key 存 `.env` 靜態對應，而非 DB 管理表**
DB 管理表（執行期發/撤 key）的需求來自「來源多又常變」的對外 / 多租戶平台；內部攝取的信賴來源少且穩定，新增來源是**有計畫的基建事件**，不需要執行期管理。唯一真實的變動是 key 輪替（安全衛生），以「同一 client 對應多把有效 key + 重疊期」處理。若未來擴張為多 domain / 多租戶，再遷移到 `api_clients` 表。

**驗證 = 資料血緣起點**
驗證解析出的 `client_id` 不只用於放行，也以 `source_client_id` 落地到 Raw 與 ODS，回答「這筆資料是哪個上游送的」。`source_client_id` 是攝入當下的不可變 metadata（與 `dq_rule_version` 同類），隨錨點走進 ODS，讓 BQ 抽取邊界後的治理 / 品質分析能 by 來源端切片，無需回 JOIN Raw。它由驗證層判定、**非 payload 內容**，故存為獨立欄位而不混入 `raw_payload`。

---

## 攝取層可靠性

攝取層的可靠性由三個機制共同保障：多層 retry 應對暫時性故障、掃描 recovery 應對 crash 後的卡住記錄、timeout 設定防止系統資源被無限耗盡。

### 多層 Retry（指數退避，最多 3 次）

**Point 1 — Raw 寫入（`main.py`）**
攔截 `db.commit()` 的 `OperationalError`，使用 `asyncio.sleep` 避免 block event loop。

**Point 2 — 背景任務處理（`process.py`）**
對完整 processing pipeline（JSON 解析 → 攤平 → 清洗 → ODS 寫入）在 `Exception` 時重試。`JSONDecodeError` 與 `ValueError` 屬資料本身的問題，不重試，直接 mark error。

**Point 3 — Claim（`process.py`）**
在 `OperationalError` 時重試 `try_claim_raw`。明確區分 DB 例外（重試）與 `rowcount=0`（另一個 worker 搶走，正常行為，不重試）。

**Point 4 — Status 更新（`process.py`）**
所有 Raw status 更新統一走 `_commit_raw_status()`，任何例外均 retry。Success path 的 ODS + status commit 在 rollback 後重新 `db.add(ods)` 再重試。耗盡所有重試機會時記 `CRITICAL` log。

### 掃描 Recovery

**Startup scan**（`lifespan`）：Server 啟動時立即執行一次，掃描所有 `pending` 記錄並透過 `asyncio.to_thread` 重新排程。

**Periodic scan**（每 5 分鐘）：兩步驟邏輯：
1. 將卡住超過 10 分鐘的 `processing` 記錄重設為 `pending`，記 `WARNING`（ODS 重複寫入風險已透過 idempotency 保護）
2. 收集所有 `pending` 記錄重新排程

### Timeout 與 Rate Limiting

**DB statement timeout（`database.py`）**
透過 `connect_args={"options": "-c statement_timeout=30000"}` 在每條連線上設定 PostgreSQL session-level timeout。確保任何 SQL 超過 30 秒（如 lock wait 導致的掛住）會拋 `OperationalError`，讓 retry 機制能正常接管，而不是讓 thread 永久掛住。

**Connection pool 明確設定（`database.py`）**
`pool_size=5, max_overflow=10, pool_timeout=30`，與 SQLAlchemy 預設值相同，但明確寫出來方便日後調整。

**`POST /process_raw/{raw_id}` 改為 background task（`main.py`）**
從直接呼叫 `process_raw_event(raw_id)` 改為 `background_tasks.add_task`，與 `/orders` 設計一致，不再 block event loop。

**per-client 限流（`slowapi`）**——以認證的 `client_id` 為 key，無 `client_id` 時（如未認證路徑 / 設定漏失防呆）退回 IP。

| Endpoint | per-client 限制 | 理由 |
|---|---|---|
| `POST /orders` | 60/minute | 防單一上游異常頻率，正常下單行為遠不到此上限 |
| `POST /process_raw/{raw_id}` | 20/minute | 人工 replay 操作，頻率天然低 |
| `GET /raw/{raw_id}` | 120/minute | Read-only，較寬鬆 |

超出限制時回傳 `429 Too Many Requests`。限額是「每個上游（跨其所有 key 與 IP）合計」而非每 IP——見設計決策的*限額語意*。

**⚠️ 部署注意事項**
以 `client_id` 為 key 讓限流**免疫 proxy-IP 問題**：在 Nginx / LB 後面 `request.client.host` 會變成 proxy IP，per-IP 限流會把所有呼叫者縮成同一個計數器——但 per-`client_id` 限流不受網路拓樸影響，因為 key 來自 auth 層而非傳輸層。IP fallback 只在沒有認證 `client_id` 時才啟動；若你在 proxy 後面會依賴這條 fallback 路徑，需將其 `get_remote_address` 改為讀取 `X-Forwarded-For` 並搭配適當的 trusted proxy 設定。

**未來方向——複合 `client_id × IP` key**：純 `client_id` key 框住的是上游的*合計*份額，無法圍堵多實例上游中單一抓狂的實例（它所有實例共用一桶）。「既隔離租戶、又圍堵單一失控實例」最忠實的 key 是複合 `client_id × IP`，通常做成兩道疊加限流（一道 per-`client_id` 合計上限 + 一道 per-`client_id × IP` 單實例上限）。此項延後到真的出現需要它的多實例上游再做（YAGNI）；目前的 `_key_func` 接縫可無結構性改動地擴充過去。

此外，`X-API-Key` 以明文置於 header，正式環境**必須走 HTTPS**（TLS 終結於 LB / reverse proxy 亦可），否則 key 會在傳輸中裸奔。

### 能應對 vs. 不能應對

| 情況 | 能應對？ |
|---|---|
| 任何階段的 DB 連線短暫中斷 | ✅ |
| Crash 後遺留的 `pending` / `processing` 記錄 | ✅（掃描 Recovery）|
| Connection pool 打爆（`TimeoutError`）| ✅ catch `SATimeoutError` 回傳 503，快速失敗，由 client 自行重試 |
| SIGKILL 正在執行時 | ❌ 程序已死，任何 retry 都無法觸發 |
| 同一 `order_id` 重複送入 | ✅ 已透過 ODS idempotency 解決 |
| Scan retry 對已寫入 ODS 的記錄重新處理 | ✅ 已透過 ODS idempotency 解決 |

---

## 壓測結果

針對六個場景進行壓測，驗證併發行為與故障模式。

**測試一：1,000 筆不同訂單，concurrency=50**
結果：全部成功，耗時 7.9 秒，無錯誤。
`POST /orders` 只做一次快速 INSERT 就釋放連線，每筆持有連線時間 < 10ms。concurrency=50 遠低於 DB pool 能承載的吞吐量，排隊現象不存在。

**測試二：1,000 筆不同訂單，concurrency=500**
結果：P99 延遲約 14 秒，5 筆 HTTP 500 錯誤。
SQLAlchemy 預設 pool_size=5、max_overflow=10，最多 15 條連線。500 個請求同時湧入，485 個排隊等連線，超過 pool_timeout=30 秒的請求直接拋出 `QueuePool limit reached`。那 5 筆錯誤是在 INSERT 之前就 timeout，raw table 裡沒有對應記錄。

（目前已 catch `SATimeoutError` 回傳 503，快速失敗，由 client 自行重試。）

應對方向：調大 pool、改用 async SQLAlchemy（asyncpg）、或在 API 前端加 rate limiting。

**測試三：100 筆相同 order_id，concurrency=100**
結果：raw 寫入 100 筆，ODS 寫入 100 筆，全部成功。
raw table 的 order_id 欄位只有 index，沒有 UNIQUE 約束，100 筆重複訂單全被當作合法資料處理，各自拿到不同 raw_id。CAS lock 保護的是「同一個 raw_id 不被重複消費」，業務層去重是另一層的問題，這是設計上已知的邊界（業務去重已透過 ODS idempotency 解決）。

**測試四：100 個 worker 同時搶同一個 raw_id（CAS lock）**
結果：raw.status = processed，ODS COUNT = 1。
`try_claim_raw` 使用 `UPDATE raw WHERE id=X AND status='pending'` 作為 CAS 操作。PostgreSQL 對這條 UPDATE 加行鎖，只有第一個到達的 worker 能讓 rowcount=1，其餘 99 個 rowcount=0，直接 return。ODS 因此只寫入一次，無重複。

**測試五：server crash 重啟（SIGKILL）**
結果：150 筆永久卡在 pending，重啟後無自動 recovery。
`BackgroundTasks` 是純記憶體 queue，任務狀態不持久化。SIGKILL 後任務消失，DB 裡的 pending 記錄沒有任何機制知道要重新處理。

兩種卡住情況的對照：

| 卡住的 status | 觸發條件 |
|---|---|
| `pending` | server crash 在背景任務執行前，或 try_claim_raw 的 DB 例外（transaction rollback） |
| `processing` | server crash 在 claim commit 之後、狀態更新之前（Phase 5 後：由 `processing_started_at` 起算逾時，見 [QUEUE-TW §3.1](./QUEUE-TW.md)）|

**⭐ 此結論已於 Phase 5 翻案。** 換上 Celery + Redis 後重跑同型情境（800 筆積壓、處理到 225 筆時 SIGKILL worker）：

| 時點 | `pending` | `processing` | `processed` |
|---|---|---|---|
| SIGKILL 當下 | 537 | 2 | 261 |
| worker 重啟後 30s | 0 | **2** | 798 |
| stale 逾時後一輪掃描 | 0 | 0 | **800** |

最終 ODS 800 筆、零遺失。值得注意的是中間那列：SIGKILL 當下已 claim 成 `processing` 的 2 筆，**重啟 worker 完全救不回來**——訊息重投遞會 CAS 失敗直接 return，只有 stale 掃描能救。這說明持久化佇列並沒有讓恢復掃描變成冗餘，反而讓它成為「佇列語意的補集」。完整分析見 [QUEUE-TW.md](./QUEUE-TW.md)。

**測試六：同一 order_id 重複提交（ODS idempotency）**

情境一（sequential）：同一 order_id 先後送入兩次。第一筆正常寫入 ODS；第二筆處理時，pre-check 查到 ODS 已有此 order_id，直接將 Raw 標為 `duplicate`，ODS 不重複寫入。

情境二（TOCTOU race）：兩個 worker 同時通過 pre-check，第一個搶先 commit ODS，第二個 commit 時觸發 `IntegrityError`，catch 後不 retry，直接標為 `duplicate`。
結果：ODS 始終只有一筆，後進的重複 Raw 均為 `duplicate` 終態，下游與監控能明確區分正常處理與重複攔截。

---

## 持續整合（CI）

每次 push 到 `main` 與所有 Pull Request 會自動觸發 GitHub Actions（`.github/workflows/ci.yml`），在 **Python 3.10 與 3.12** 雙版本矩陣下安裝依賴並執行完整測試套件（393 個測試，受管模組 100% 覆蓋）。

另有一條**獨立的 DAG workflow**（`.github/workflows/dags.yml`，47 個測試）：以官方 constraints 安裝 Airflow 並用 DagBag 解析 `orchestration/dags/`。刻意不併進主 job——Airflow 安裝很重且 pin 了大量套件版本，會毀掉主 job「mock DB、數秒跑完」的速度優勢。它不需要 `DB_URL`，因為 DAG 檔刻意不 top-level import 專案模組（見 [ORCHESTRATION-TW §2.2](./ORCHESTRATION-TW.md)）。

- 測試全為單元/整合層（mock DB），不需真實資料庫即可執行，數秒內完成。
- 測試依賴集中於 `requirements-dev.txt`（`-r requirements.txt` + pytest 等）。

### CI 的涵蓋範圍與盲區（刻意的取捨）

CI 自動驗證的是**程式邏輯與型別契約**。**DB 層契約**——`try_claim_raw` 的 CAS claim、重複 `order_id` 的 UNIQUE 去重、crash 後的 recovery，以及 Alembic migration 與 `models.py` 的漂移——因 CI 內測試以 mock 取代資料庫，**不在 CI 的自動驗證範圍內**。

這些 DB 邏輯目前改以**手動腳本**驗證可靠性：

- `load_test.py`：吞吐量、真實併發下的 CAS claim（`--cas-test`）與 `order_id` 去重（`--duplicate`），打真 server → 真 Postgres。
- `restart_test.sh`：`SIGKILL` 模擬 crash，驗證 pending 記錄的 recovery 行為。
- `check_migration_drift.py`：`alembic upgrade head` + `compare_metadata`，一鍵比對「migration 產生的 schema」與 `models.py` 是否漂移；偵測到不一致即以非零 exit code 報錯。

**為何個人專案階段先不把資料庫接進 CI**：本專案目前定位為單人練習/作品，無真實流量與併發；CAS / recovery 等 DB 邏輯的價值只有在真實併發下才會兌現，導入真連資料庫的併發整合測試需付出撰寫成本與容器啟動的 flake 維護，**其成本高於「不自動化」在現階段的風險**。其中 `check_migration_drift.py` 屬例外——它確定性、無併發、低 flake，本可進 CI；目前仍維持手動，是基於單人開發、schema 已趨穩、漂移發生機率低的權衡，並已預留 exit code 介面，待有真實流量、第二位協作者、或需展示工程深度時即可直接升級進 CI。

> ⚠️ **不要把綠燈當成「全部沒問題」**：CI 通過僅代表**邏輯層**無回歸，**不代表去重 / CAS / migration 等 DB 契約已被自動驗證**。這些目前靠手動腳本佐證、需人工判讀（migration 漂移已有 `check_migration_drift.py` 可一鍵檢查，但同樣未進 CI）。修改相關邏輯時，仍須以 `load_test.py` / `restart_test.sh` / `check_migration_drift.py` 重新佐證。

---

## 已知問題

**Scan 可能對已排程的任務重複排程**
Periodic scan 與 startup scan 會撈出所有 `status='pending'` 的 Raw 記錄並重新排程，但 DB 無法感知某筆記錄是否已經在 BackgroundTasks 佇列中等待執行。在高流量情境下，若大量請求寫入後尚未被消化，scan 就可能對這些「已排隊但還沒 claim 的記錄」再排一次，造成同一筆 raw_id 有多個 worker 同時嘗試處理。目前靠 CAS claim（`try_claim_raw`）在執行層兜底——後到的 worker 拿到 `rowcount=0` 會直接 return，不會重複寫入 ODS，正確性沒有問題，但會浪費 thread 資源，在高負載時會加重 pool 壓力。

**未來換 Queue 時的修正方向**
根本解法是讓「已排進 Queue」這件事對 DB 可見，具體做法是在狀態機中加入 `queued` 狀態（`pending → queued → processing → processed/error/duplicate`），並把入隊動作與狀態轉移綁在一起：寫入 Raw 後立刻以 CAS 原子性地將 `pending` 轉為 `queued`，成功才 push 進 Queue；scan 只撈 `pending`（即「從未成功入隊」的記錄），`queued` 的記錄一律跳過。Worker 的 CAS claim 對象也對應改為 `queued → processing`。唯一需要額外處理的邊界情況是：`pending → queued` 寫入 DB 成功、但 Queue push 失敗，此時記錄會卡在 `queued`——scan 需另外掃描超時的 stale `queued` 記錄並重設為 `pending`，讓它重新走一遍入隊流程。

**字串內含 NUL（0x00）會導致 Raw 卡在 `processing`（已修）**
在一次端對端測試中，以 `{"order_status": "ok\u0000bad"}` 這類 payload 打進 `/orders`，結果該筆 Raw 既沒成功寫入 ODS、也沒落到任何終態，而是一直卡在 `processing`，並被 scan recovery 每 10 分鐘撈回來重排一次（poison-pill）。

問題的根本是「同一個 NUL 值在管線不同階段有不同的表示形式，而防護與危害落在不同的表示空間」。`main.py` 的攝取防護 `raw_body.replace("\x00", "")` 只清除 HTTP body 中**真實的 0x00 byte**；但 `\u0000` 在 body 裡是 6 個合法的 ASCII 字元（`\` `u` `0` `0` `0` `0`），裡面根本沒有 0x00，所以防護什麼都沒清、raw_payload 也正常落地。真正的 NUL 是後段 `process.py` 的 `json.loads` 把 `\u0000` **解碼**後才生出來的——此時 `order_status` 才變成含真實 0x00 的字串，寫入 ODS 文字欄時 PostgreSQL/psycopg2 拋出 `ValueError: A string literal cannot contain NUL (0x00) characters.`（NUL 在 TEXT 與 JSONB 都無法儲存）。更關鍵的是，這個 `ValueError` 發生在 success-path 的 commit loop，而該處原本只特判 `IntegrityError`（duplicate）與 `DataError`（欄位超限），`ValueError` 落入泛用的 `except Exception` 被當成「暫時性錯誤」一再重試——但它其實是 deterministic（重試必然再失敗），於是重試耗盡後卡在 `processing`、被 scan 反覆重排，形成 poison-pill。

**目前的解法**：在 commit loop 的 `DataError` 之後補上 `except ValueError` 分支，把它比照 `DataError` 一樣 fast-fail 到終態 `error`（並可用 `POST /process_raw/{id}?force=true` replay）。這治的是「deterministic 錯誤被誤判為暫時性而無限重試」這一層：資料不再卡在 `processing`，poison-pill 消除。代價是這類資料**會被拒絕、不進 ODS**，語意與 `DataError` 一致。

**未來考量**：目前是「拒絕」而非「接受後標記」。若希望這類訂單也能落地（符合本專案 `has_clean_error` 的非阻斷品質哲學），可改在 `clean.py` 於寫入前 sanitize 文字欄與 items 巢狀字串中的 NUL、標記成一個新的清洗錯誤碼（如 `nul_in_text`），讓資料帶標籤落地、由下游 quarantine 處理——做法可直接類比現有 items 的 NaN/Inf sanitize（建立「ODS 永不儲存 NUL」的不變量）。此變更會動到清洗規則，需 bump `DQ_RULE_VERSION`。

---

## API 端點

所有端點均需帶 `X-API-Key` header（見下方〈服務對服務驗證〉），缺失或無效回 `401`。

| Method | Path | 說明 |
|---|---|---|
| `POST` | `/orders` | 寫入新訂單（存 Raw，觸發背景任務） |
| `POST` | `/process_raw/{raw_id}` | 手動 replay 指定 raw（加 `?force=true` 可重置狀態） |
| `GET` | `/raw/{raw_id}` | 查詢 raw 的處理狀態和 payload 預覽 |
| `GET` | `/health` | liveness 探針 — **不需 `X-API-Key`**，回 `{"status": "ok"}`（供容器 healthcheck 與未來 LB/K8s 探針用） |

---

## 資料流

```
OrderIN（巢狀 Pydantic）
    └── from_nested() → ODSOrder（攤平的 Pydantic）
            └── clean_order() → ODS（SQLAlchemy model）+ quality_events
```

Pydantic 負責驗證和攤平，SQLAlchemy 負責存資料，兩層刻意解耦，各自獨立。

---

## 專案結構

```
.
├── main.py        # FastAPI app、路由
├── process.py     # 背景任務、狀態機、claim 邏輯
├── clean.py       # format_clean、business_clean、clean_order
├── auth.py        # API Key 驗證（X-API-Key → client_id）
├── schema.py      # Pydantic schemas（OrderIN、ODSOrder、RawOut...）
├── models.py      # SQLAlchemy models（Raw、ODS、QualityEvent）
├── database.py    # Engine、SessionLocal、Base
├── config.py      # 設定集中管理（pydantic-settings Settings 單例）
├── alembic/        # DB migration（env.py 接 settings.db_url + Base.metadata；versions/ 為遷移腳本）
├── alembic.ini     # Alembic 設定（sqlalchemy.url 留空，由 env.py 注入）
├── pytest.ini     # 測試設定（asyncio_mode、coverage）
├── tests/
│   ├── conftest.py        # 共用 fixtures
│   ├── helpers.py         # Mock 工廠函式與測試資料
│   ├── test_clean.py      # format_clean、business_clean、clean_order
│   ├── test_schema.py     # ODSOrder.from_nested
│   ├── test_raw_write.py  # Point 1：Raw 寫入 retry
│   ├── test_process.py    # Point 2–4：Claim / Processing / Status retry；Idempotency；Quality Events
│   ├── test_scan.py       # scan_and_recover、lifespan startup、periodic scan
│   ├── test_timeout.py    # Pool 耗盡、/process_raw、GET /raw、DB 設定
│   ├── test_rate_limit.py # per-client 限流
│   ├── test_auth.py       # API Key 驗證、輪替、source_client_id 落地
│   ├── test_extract_cli.py        # 抽取腳本的 --table 分派與 gate
│   ├── test_reevaluate_quality.py # Proposal B：狀態轉移矩陣、反序列化保真、CLI 閘門
│   ├── test_dags.py       # DAG 結構（需 Airflow，跑在獨立 CI job；本機自動 skip）
│   └── test_seed_demo.py  # 造資料器的跨模組不變式（缺漏選填欄位不得變成髒資料）
├── extract_ods_to_bq.py   # E/L：ODS → BQ staging（--table orders|quality_events|all）
├── check_raw_pending.py   # 派工存活探針（raw.status='pending' 年齡；門檻由恢復路徑的
│                          #   設定推導而非寫死）。由 raw_pending_watch DAG 執行
├── reevaluate_quality.py  # Proposal B 事件產生端（候選讀 BQ int_、狀態讀 PG、預設 dry-run）
├── seed_demo.py           # 走真實攝入路徑產生 BI 展示資料（--dirty-rate 造違規、
│                          #   --missing-cost-rate 造上游不完整，兩者正交）
├── orchestration/         # Airflow：Dockerfile、dags/、env_var 版 dbt profiles
├── docker-compose.airflow.yml     # Airflow overlay（與 docker-compose.yml 疊加）
├── requirements-analytics.txt     # 分析管線執行期依賴（Airflow 容器裝這個）
├── DQ_ARCHITECTURE-TW.md  # 資料品質控管架構設計文件（繁體中文）
├── DQ_ARCHITECTURE.md     # Data Quality Control Architecture（English）
├── CLOUD_LAYER-TW.md      # 雲端層架構：ODS → BigQuery（繁體中文）
├── CLOUD_LAYER.md         # Cloud Layer Architecture: ODS → BigQuery（English）
├── ORCHESTRATION-TW.md    # 編排層架構：Airflow（繁體中文）
├── ORCHESTRATION.md       # Orchestration Layer Architecture: Airflow（English）
├── ecommerce_dbt/         # dbt 轉換層（stg_/int_/dim_/fct_/rpt_）；操作與實作決策見其 README
├── .env           # DB_URL、API_KEYS（不進版控）
├── .env.example   # 環境變數範本（進版控）
└── .gitignore
```

---

## 📄 設計文件

| 文件 | 說明 |
|---|---|
| [資料品質控管架構](./DQ_ARCHITECTURE-TW.md) | 完整 DQ 設計：各層品質合約、攔截機制（Hard Gate + Row Filter）、場景補值策略、Quarantine 與 Remediation 策略、版本號與 quality_events 狀態機、歷史指標架構 |
| [雲端層架構](./CLOUD_LAYER-TW.md) | ODS → BigQuery 抽取與 staging：分區/叢集/保險絲設計、watermark 策略（方案 A 與 `get_watermark()` 接縫）、批次載入與 JSON 落地決策、ODS schema 演進策略（staging 只做加法 + dbt 吸收 + `FIELDS` 一致性測試）|
| [任務佇列](./QUEUE-TW.md) | 攝入路徑的 Celery + Redis：與 Airflow 的正交邊界、薄包裝與不開 result backend 的理由、`acks_late` 與 CAS claim 的交互作用（佇列救不回來的那一半）、stale 逾時基準為何是 `processing_started_at`、broker 掛掉時的降級語意；含 SIGKILL／限流／降級延遲的實機驗證數據與 runbook |
| [編排層架構](./ORCHESTRATION-TW.md) | Airflow 的六條 DAG 與決策：Airflow ≠ 任務佇列的邊界、DAG 檔不得 import 專案模組、extract 一表一 task、dbt 分層執行與 `--indirect-selection=buildable`、`catchup=False` 的結構性理由、retry 不對稱、觀測訊號（freshness 與派工探針）為何各自獨立成 DAG、Proposal B 的手動觸發語意；含 runbook 與 Proposal B 完整 demo 劇本 |
| [轉換層（dbt）](./ecommerce_dbt/README.zh-TW.md) | dbt 轉換層的操作與實作決策：分層與命名慣例、物化策略（table vs view、incremental + insert_overwrite + `copy_partitions` 繞過 sandbox 禁 DML）、回看窗、去重鍵與不變式、Hard Gate 自訂 generic test、freshness 繞保險絲；`int_` 層的有效品質狀態合成（刻意複製 + 對齊清單）、全量重建的必要性、劃分不變式測試、`int_order_items` 的 `safe_cast` 與嚴格 NULL 傳播。層契約見 DQ_ARCHITECTURE、staging 基建見 CLOUD_LAYER |

---

## 怎麼跑起來

```bash
# 1. Clone
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform

# 2. 建虛擬環境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. 裝套件（含測試依賴）
pip install -r requirements-dev.txt

# 4. 設定環境變數
cp .env.example .env
# 編輯 .env，填入：
#   DB_URL=postgresql://user:password@localhost/dbname
#   API_KEYS=your_key:upstream-order-api   （格式 key:client_id，逗號分隔多組）

# 5. 建立資料庫 schema（Alembic migration，非 create_all）
alembic upgrade head

# 6. 啟動
uvicorn main:app --reload

# 7. 執行測試（需先設定 .env，或設一個假的 DB_URL）
pytest
```

跑起來之後 API 文件在 `http://localhost:8000/docs`

### 用 Docker 跑（推薦）

專案附 `Dockerfile` + `docker-compose.yml`，會依序拉起 PostgreSQL、跑 Alembic migration、再啟動 API —— 一個指令，本機不必裝 Python/Postgres：

```bash
# 1. 在 .env 設好 API_KEYS（POSTGRES_USER/PASSWORD/DB 可選）
cp .env.example .env

# 2. build 並啟動：db + redis → migrate（alembic upgrade head）→ api + worker + beat
docker compose up --build
```

- `db`（postgres:16）與 `redis`（7-alpine）先起，各自的 healthcheck 把關後續服務。
- 一次性 `migrate` 服務跑完 `alembic upgrade head` 後退出。
- `api` / `worker` / `beat` 都要等 DB 與 Redis 健康 **且** migration 成功完成（`service_completed_successfully`）才啟動。
- `worker`（Celery，4 個 prefork 子行程）消費攝入任務；`beat` 只負責按時派出恢復掃描，**不可 `--scale`**（多個 beat 會重複派工）。三者共用同一個映像，只換啟動指令。
- `DB_URL` 由 Compose 注入、指向 `db` 服務，會覆蓋 `.env` 內的值（pydantic-settings 中 env var 優先序高於 `.env` 檔），故**不需改任何程式碼**。`.env` **不會**烤進映像，secrets 於執行期注入。
- API 預設以 `--workers 4` 啟動（`UVICORN_WORKERS` 可調）。能開多行程的前提是 Phase 5 把兩份行程內狀態都移走了：處理任務交給 Celery worker、恢復掃描交給 Celery Beat，API 行程本身不再持有背景狀態。
- 連帶必須處理的是**限流計數器**：slowapi 預設放在行程記憶體，多行程下 `60/minute` 會實質變成 `60 × workers`（實測 4 workers 放行 91/100 筆而非 60），且不會有任何錯誤訊息。compose 因此把計數器指向 Redis db 1（broker 用 db 0），限額回到精準的 60。Redis 不可用時退回行程內計數而非整個放行。見 [QUEUE-TW §5.5](./QUEUE-TW.md)。

API 位於 `http://localhost:8000`（文件 `/docs`，健康檢查 `/health`）。

### 加上 Airflow（分析管線排程）

```bash
# .env 需有 BQ_PROJECT 與 GOOGLE_APPLICATION_CREDENTIALS（主機上的金鑰路徑）
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
```

兩個 compose 檔**必須疊加成同一個 project**，DAG 才能以 `db` 這個 hostname 連到業務資料庫。
Airflow UI 在 `http://localhost:8080`。完整決策與 runbook 見 [ORCHESTRATION-TW.md](./ORCHESTRATION-TW.md)。

---

## 預期完整架構

目前實作涵蓋攝取層與處理層，完整的目標架構如下：

```
【接收層】即時
模擬腳本同時打 1000 個 POST /orders
  ↓
API 收到請求 → 寫入 Queue → 立刻回應「收到」

【處理層】即時
Worker 從 Queue 拿任務
  ↓
Raw 層落地（原始資料，不動）
  ↓
try_claim_raw 搶狀態（pending → processing）
  ↓
清理 / 驗證 / Idempotency 檢查
  ↓
寫入 ODS（PostgreSQL，乾淨原始大表）+ quality_events
  ↓
狀態更新（processing → processed / error）

【分析層】批次，排程觸發
Airflow DAG（本地，定期執行）
  ↓
PostgreSQL ODS → BigQuery staging（incremental，以 received_at 為 watermark）
  ↓
dbt Core 執行轉換
  ├── stg_*（1:1 對應來源，只做 rename / cast / dedup）
  ├── int_*（跨表 join、衍生欄位、業務邏輯）
  ├── dim_* / fct_*（Star Schema，供彈性查詢）
  └── rpt_*（固定粒度預聚合，BI Dashboard 直接使用）
  ↓
Looker Studio（直連 BigQuery）
```

**分析層為什麼用批次而不用 Streaming？**
下游是 BI，消費模式是報表與 Dashboard，T+1 或小時級的更新頻率已足夠。批次可以用 window 做資料品質檢查、出錯可重跑，穩定性更高。接收層與分析層也因此天然解耦，批次排程不影響即時寫入路徑。若未來下游接即時預測模型，才有換 Streaming 的動機。

**dbt 各層職責說明**
- `stg_*`：資料清洗的入口，1:1 對應 BigQuery staging，不做業務邏輯
- `int_*`：中間建材層，處理跨表 join 與複雜衍生邏輯，供 dim/fct 引用
- `dim_* / fct_*`：Star Schema 的維度與事實表，適合彈性的 ad-hoc 查詢
- `rpt_*`：在 dim/fct 之上進一步聚合，粒度固定，專為 Dashboard 效能與成本最佳化。分**兩個資料域**：業務報表上游一律走 `dim_`/`fct_`（口徑單一、不繞過 Gold 的語意決定與測試）；品質報表讀 `int_orders_quarantine` 與 `stg_quality_events`——被隔離的列按定義永遠不在 Gold。比率一律不落地、`COUNT(DISTINCT)` 標死不可加，理由見 [ecommerce_dbt/README.zh-TW §7](./ecommerce_dbt/README.zh-TW.md)

---

## 開發藍圖

**Phase 1 — 系統可靠性**
- [v] Retry 機制 — 四層 retry（Raw 寫入、Claim、Processing、Status 更新），exponential backoff，耗盡後記 `CRITICAL` log
- [v] 掃描 Recovery — 啟動時掃描一次 + 每 5 分鐘 periodic scan；stale `processing`（>10分鐘）重設為 pending；potential duplicate ODS 記 `WARNING`
- [v] Timeout — DB statement timeout（30s）防 lock wait 掛住 thread；pool 設定明確化；`POST /orders` catch pool 耗盡回 503；`/process_raw` 改為 background task 不 block event loop
- [v] Idempotency — ODS 加 `raw_id` 欄位 + `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`；first-write-wins 策略：pre-check + IntegrityError 兜底；重複記錄標為 `duplicate` 終態
- [v] Rate Limiting — per-client 限流（slowapi，以認證 `client_id` 為 key、IP fallback），`POST /orders` 60/min、`POST /process_raw` 20/min、`GET /raw` 120/min；不加全域上限（見設計決策）

**Phase 2 — 可驗證性**
- [v] Pytest — 393 個測試，12 個受管模組全部 100% 覆蓋（`pytest --cov`；另有 47 個 DAG 測試跑在獨立 CI job）；涵蓋所有 retry 路徑（Point 1–4）、CAS claim、idempotency、crash recovery scan、`format_clean`、`business_clean`、`ODSOrder.from_nested`、quality_events 各寫入路徑、API Key 驗證（缺失/無效/有效/輪替/parser 容錯）；`asyncio_mode=auto` 取代手寫 `asyncio.run()`；`reset_limiter` fixture 解決 rate limit 計數器跨測試污染問題；驗證以 `dependency_overrides` 旁路，讓非 auth 測試不必每個請求塞 header。目前僅單元測試與整合測試（HTTP 層），無端到端測試；待 Phase 3 Docker / docker-compose 建立後，再補上真實 DB 的 E2E 測試。
- [v] 資料品質控管架構（ODS 層）— 完整設計文件（見 [DQ_ARCHITECTURE-TW.md](./DQ_ARCHITECTURE-TW.md)）；ODS 層已實作：`DQ_RULE_VERSION` 規則版本常數、`dq_rule_version` 欄位（ODS）、`quality_events` 表（append-only 品質事件日誌，狀態機起點）、structlog `quality_metric` 事件；BQ Analytics 層（Hard Gate、Row Filter、`int_orders_quarantine`、Airflow 重評估、`rpt_quality_*`）除 Airflow 重評估外皆已實作

**Phase 3 — 工程化**
- [v] 服務對服務驗證（API Key）— 靜態 `X-API-Key`（`.env` 載入 `key:client_id` 對應，支援同 client 多把 key 做輪替），`secrets.compare_digest` constant-time 比對；掛載於全部端點，缺失/無效回 401；驗證出的 `client_id` 落地為 `source_client_id`（Raw + ODS），作為資料血緣起點。**未採 JWT 使用者登入**——本服務是內部攝取單元、無人類使用者（見〈服務對服務驗證〉設計決策）
- [v] 環境變數集中管理 — `config.py` 以 pydantic-settings 的 `Settings` 為單一真相來源，啟動時實例化一次，各模組統一 `from config import settings`，不再各自 `load_dotenv()` / `os.getenv`。**決策邊界**：只集中「會因部署環境而異」的值——必填的 `DB_URL`（缺值即 fail-fast，不再延遲到連線才炸）、`API_KEYS`，以及帶預設值的 `pool_size` / `max_overflow` / `pool_timeout` / `statement_timeout_ms` / `scan_interval_seconds` / `log_format`；演算法常數（`MAX_*_RETRIES`、`STALE_PROCESSING_MINUTES`）**刻意留在各自模組開頭**——它們是程式行為的一部分、不隨環境變動，改動應走 code review 而非環境變數。附 `.env.example` 範本進版控
- [v] Alembic DB migration — 以 Alembic 作為 schema 唯一真相來源，**移除 `Base.metadata.create_all`**（`create_all` 只建不改，無法承載 schema 演進）。`env.py` 從 `settings.db_url` 取連線、`import models` 註冊 `Base.metadata` 供 autogenerate；`alembic.ini` 的 `sqlalchemy.url` 留空，DB_URL 維持單一真相。`Base.metadata` 掛 **naming convention**（`ix/uq/ck/fk/pk_*`），讓約束命名穩定可預期、未來 drop/rename 不因環境命名不一致而出錯。初始 migration 以 convention 原生命名生成；schema 變更流程改為 `alembic revision --autogenerate` → review → `alembic upgrade head`
- [v] Docker / docker-compose（API + PostgreSQL 容器化）— 單階段 `python:3.12-slim` 映像（非 root、鎖版 `requirements.txt`），由 `api` 與一次性 `migrate` 服務共用；`docker compose up` 以 `depends_on` 條件（`service_healthy` + `service_completed_successfully`）編排 **db（healthcheck）→ migrate（`alembic upgrade head`）→ api**；`DB_URL` 於執行期注入、指向 `db` 服務（不需改程式碼 —— env var 優先序高於 `.env`），secrets 不烤進映像；API 固定 `--workers 1`（行程內 `BackgroundTasks` / 週期掃描）；新增 `GET /health` liveness 探針供容器 healthcheck

**Phase 4 — 分析層 Pipeline**
- [v] ODS → BigQuery 抽取腳本（`extract_ods_to_bq.py`）— 以 `received_at` 為 watermark 做增量抽取（方案 A，由 `INFORMATION_SCHEMA.PARTITIONS` 推導，封裝在 `get_watermark()` 作為未來微批方案 B 的接縫）；staging 表分區（`received_at` DAY）+ 叢集（`order_id`、`has_clean_error`）+ `require_partition_filter` 保險絲；只走 batch load（不串流）、JSON 欄位以原生物件落地、`ALLOW_FIELD_ADDITION` 支援 additive schema 演進；`FIELDS` 單一真相來源由 `tests/test_schema_bq_consistency.py` 把關（見 [CLOUD_LAYER-TW.md](./CLOUD_LAYER-TW.md)）
- [v] dbt Core `stg_` 層（`stg_orders`：`raw_id` 去重 + Hard Gate + source freshness；`stg_quality_events`：以 `id` 為 grain 去重、保留完整狀態機歷史；incremental + `insert_overwrite` + `copy_partitions`）— 見 [ecommerce_dbt/README.zh-TW.md](./ecommerce_dbt/README.zh-TW.md)
- [v] dbt Core `int_` 層（Gold 入口，攔截發生於此）：`int_orders`（Row Filter，判定基準為「ODS 快照 ⊕ `quality_events` 最新事件」合成的**有效品質狀態**，非 `has_clean_error` 字面值）、`int_orders_quarantine`（含 `error_codes` 攤平、`quarantined_at` 取事件時間）、`int_order_items`（items 攤平到 item 粒度）。**劃分不變式**（兩表對 `stg_orders` 互斥 + 窮盡）由 singular test 把關；物化刻意採 `table` 全量重建而非 `received_at` 增量——Proposal B 的 promotion 事件落當天分區、受影響訂單卻在舊分區，增量會靜默切斷回流路徑
- [v] dbt Core：dim_*/fct_*（Kimball header/line 雙事實表 + 兩張 SCD1 維度，見 [ecommerce_dbt/README.zh-TW](./ecommerce_dbt/README.zh-TW.md) §6）
- [v] dbt Core：rpt_*（三張：`rpt_quality_events_daily` 事件軸增量、`rpt_quality_backlog` 快照、`rpt_sales_daily_by_category` 業務聚合；見 [DQ_ARCHITECTURE-TW.md](./DQ_ARCHITECTURE-TW.md) 與 [ecommerce_dbt/README.zh-TW §7](./ecommerce_dbt/README.zh-TW.md)）。⚠️ Proposal B 事件產生端未實作，故回流相關欄位目前恆為 0。場景專用 `int_orders_*`、SCD2 `dim_customer`、`rpt_sales_*` 切增量、品質報表的金額曝險度量，設計皆已備妥，各有明確觸發點才啟用
- [ ] Looker Studio 接 BigQuery dim_*/fct_*/rpt_*

**Phase 5 — 自動化 + Queue 升級**
- [v] Airflow（本地，3.0.0 + LocalExecutor）六條 DAG，排程一律以 `Asia/Taipei` 顯式宣告——`seed_demo_daily`（模擬上游＝本系統唯一的資料來源，10/13/17/21 各一批共 800 筆/天）、`raw_pending_watch`（派工存活探針，每個 seeding 時段後 30 分鐘；門檻由恢復路徑的設定推導而非寫死）、`orders_analytics_daily`（22:30，2 個 extract task → 4 層 `dbt build` → 完整 `dbt test`）、`source_freshness_watch`（08:00，extract 的 backstop）、`dq_reevaluation`（Proposal B，手動觸發、預設 dry-run、commit 後自動接主 DAG）、`seed_demo_gate_demo`（Hard Gate 攔截劇本，手動觸發）。**四條有排程的 DAG 之間沒有 Airflow 層級的相依，時序契約只存在於時間差裡**，且各自的紅代表不同處置——這是它們被拆開的全部理由。dbt 與分析腳本各自獨立 venv；DAG 檔不 import 專案模組故可進 CI；決策見 [ORCHESTRATION-TW.md](./ORCHESTRATION-TW.md)
- [v] Proposal B 事件產生端（`reevaluate_quality.py`）——候選讀 BQ `int_` 層（與 Row Filter 同一份有效品質狀態定義）、狀態判定讀 PG（冪等不能建立在有保留期的鏡射上）、只在狀態改變時 append；`business_clean` 加 `as_of` 讓時間相依規則可重現，`NON_REPRODUCIBLE_CODES` 擋掉「證據消失式」的偽 promote
- [v] Celery + Redis（取代 BackgroundTasks）——`process_raw_event` 以薄包裝進 Celery task（`process.py` 零 Celery 依賴，保留手動補跑的救援路徑）；不開 result backend（任務狀態的真相是 `raw.status`）；`acks_late` + `reject_on_worker_lost`；broker 掛掉時 `POST /orders` 仍回 200 `pending`，由恢復掃描接手。設計與實測見 [QUEUE-TW.md](./QUEUE-TW.md)
- [v] 恢復掃描搬到 Celery Beat——原本掛在 FastAPI lifespan 的 asyncio 迴圈是行程內狀態，移走後 API 才得以多行程；Beat 啟動時另補一次掃描，填住第一個排程間隔的空窗
- [v] `raw.processing_started_at`——stale 逾時改由「開始處理」起算而非「攝入」。用 `received_at` 會在積壓時把正在處理的記錄誤判為逾時、收回重派，造成同一 `raw_id` 被兩個 worker 並行處理（實測 2000 筆積壓下重現 2 筆）。見 [QUEUE-TW §3.1](./QUEUE-TW.md)
- [v] 派工熔斷器（`circuit_breaker.py`）——broker 不可用時的退化是**超線性**的（kombu producer pool 每行程上限 10），實測 48 併發下 47 筆在 120 秒內拿不到回應，而它們的 Raw **其實都已寫入**，客戶端只看到逾時、於是重送。連續失敗三次即開路，之後不再碰 Redis：p50 從「逾時」降到 **5ms**。連帶修掉 `db.refresh()` 開啟的交易跨越派工的問題（實測 60 併發時 32 個 pool 槽位有 23 個卡在 `idle in transaction`）
- [v] 有界的恢復掃描——熔斷器讓事故期間攝入維持全速，代價是 `pending` 也全速累積，原本「一次撈完所有 pending 逐一派工」等於把剛避開的崩潰搬到恢復路徑。改為 `LIMIT` + `id` 游標分頁（派工不改 status，光有 LIMIT 每輪會撈到同一批）、單輪頁數上限、Redis 鎖擋重疊，並加寬限期讓剛攝入的 pending 留給攝入路徑。實測 120,000 積壓：掃描 #1 送出 100,000（上限）、#2 送出 20,000，ODS 恰好 +120,000、重複 0 筆。⚠️ **前提未完備**：`raw.status` 目前沒有索引，分頁收斂的是記憶體與派工量而非查詢成本；索引形狀需由真實流量決定，不能用自造資料的 `EXPLAIN` 下結論（見 [QUEUE-TW §6.1](./QUEUE-TW.md)）
- [v] Docker 擴展：補上 Redis + Celery Worker + Beat；api 開 4 個 uvicorn worker，限流計數器改走 Redis
- [v] OpenTelemetry — Traces + 營運 Metrics（2026-08-17）
  - **Collector 常駐**（contrib 0.158.0，`otel/collector-config.yaml`）：app 一律送本機 Collector，雲端 endpoint 與憑證只存在那一處。⚠️ `.env` 刻意**不用** OTel 標準名 `OTEL_EXPORTER_OTLP_ENDPOINT`——任何 SDK 看到那個名字就會直連雲端、靜默繞過 Collector（沒有錯誤、資料照樣進得去），標準名保留給「app → Collector」
  - **Traces**：`api` → Celery → `worker` 全鏈路串通（實測同一 `trace_id` 跨行程出現），自動涵蓋 SQLAlchemy / Redis。SDK 初始化掛在 `worker_process_init`，因為 `BatchSpanProcessor` 是背景執行緒、**不會被 fork 繼承**——與 `_dispose_inherited_engine` 是同一個成因、同一個掛載點
  - **Logs**：structlog 注入 `trace_id` / `span_id`（W3C 32/16 位十六進位）。**不走 OTLP**：Python 的 logs pillar 最晚穩定，而跨 pillar 關聯需要的只有這兩個欄位
  - **Metrics（營運）**：`orders.raw.result`、`orders.processing.duration`、`orders.retry`、`circuit_breaker.state`、`recovery_scan.dispatched`；另有 `http.server.duration`（P50/95/99）與 `db.client.connections.usage`（pool 壓力）由 instrumentation 免費提供。序列預算由 SDK View 控制——實測 **320 active series（免費額度 10k 的 3.2%）**。⚠️ 反直覺的結論是：**貴的是自動指標不是自訂的**。三條 Drop view 砍掉的 `http.server.request/response.size` 與 `flower.task.runtime.seconds` 原本佔 27%，而後者用毫秒級 bucket 量秒級的值，等於有資料零解析度
  - Exporter：Grafana Cloud（`ap-southeast-1`，Tempo + Prometheus）
- [ ] OpenTelemetry 未竟項
  - **業務 / DQ Metrics**：刻意緩做。`seed_demo_daily` 的髒率是每日決定性五選一 → **日內恆定**，分鐘級 error rate 答不出 `rpt_quality_events_daily` 沒說過的事；高基數切片按定義屬倉裡（見 [DQ_ARCHITECTURE-TW.md](./DQ_ARCHITECTURE-TW.md) 的層次一/二邊界）
  - **Airflow 接入**：Collector 已備妥（明文送本機即可），待查證 Airflow 內建 OTel 輸出的認證能力
  - **absent 告警與 dashboard**：門檻必須從觀測推導，而本機夜間關機會讓規則天天誤報——需先累積 2–3 天真實節奏
