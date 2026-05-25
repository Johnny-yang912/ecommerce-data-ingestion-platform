# ecommerce-data-ingestion-platform
### 電商資料平台寫入系統

[English](./README.md) | **繁體中文**

一個用 FastAPI 寫的後端資料寫入服務，模擬真實電商訂單的資料流程——從接收原始 payload、狀態管理、資料清洗，到最後寫入 ODS（操作型資料倉儲）。

此專案著重展示後端工程與資料工程的實務能力，設計決策盡量貼近真實場景會遇到的問題——例如高併發下的重複處理、資料庫寫入失敗的補償機制、格式不一致的資料清洗策略等，並在每個環節思考如何正確應對與處理。

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
POST /orders
    │
    ▼
[Raw Table]  ←── 先把原始 JSON 整包存進來
    │  status: pending
    ▼
[Background Task: process_raw_event]
    │
    ├── try_claim_raw()         ← 原子性 UPDATE，搶佔這筆資料
    ├── JSON 解析
    ├── ODSOrder.from_nested()  ← 把巢狀結構攤平
    ├── clean_order()           ← 格式清洗 + 業務規則驗證
    │
    ├── 成功 → 寫入 ODS，Raw status 改成 processed
    └── 失敗 → Raw status 改成 error，錯誤訊息寫回去
```

---

## 幾個設計決策

**原子性 claim（`try_claim_raw`）**
用 `UPDATE ... WHERE status = 'pending'` 再檢查 `rowcount == 1`，確保同一筆資料在多個 worker 並發時不會被重複處理，不需要悲觀鎖。

**Raw → ODS 兩層資料模型**
Raw table 保留原始 JSON，方便除錯和 replay。ODS 存的是攤平、清洗過的版本，直接供下游分析用。兩層職責分開，不互相污染。

**分層錯誤處理**
`JSONDecodeError`、`ValueError`（Pydantic 驗證失敗）、`Exception`（未預期錯誤）分開 catch，各自寫不同的 error_message 回 Raw，方便事後排查是哪種問題。

**資料清洗 pipeline**
`format_clean()` 處理格式問題（統一小寫、去空白）。`business_clean()` 驗證業務規則（數量不能為負、評分要在 1–5 之間、出貨日不能早於訂單日等）。有問題的資料不直接拒絕，而是用 `has_clean_error` 標記，讓下游自己決定怎麼處理。

**ODS 層 items 欄位使用 JSONB，Raw 層使用 TEXT**　
Raw 的職責是保留所有進來的原始資料，不對其結構做任何假設，TEXT 語義最貼切——資料庫不解析、不驗證，原樣存入。ODS 的 items 已經過 Pydantic 驗證與清洗，結構有保證，JSONB 讓資料庫在寫入時多一道格式驗證，且保留未來在 SQL 層直接查詢 items 內部欄位的彈性。

**Raw 層不做業務去重**
`Raw.order_id` 刻意不加 UNIQUE 約束。Raw 的職責是完整記錄所有進來的請求，包含重複提交——不同提交之間可能欄位互補，異常的提交頻率本身也是訊號（攻擊偵測、用戶端 bug）。去重的責任下放到 ODS 層。

**只做 per-IP 限流，不加全域上限**
全域上限的數字必須從「預期同時活躍 IP 數 × per-IP 上限」推導，但這個數字在沒有真實流量資料時無從決定——隨意填寫的全域數字反而會帶來語義不明確的限制。更根本的問題是：`/minute` 視窗的 rate limit 無法防止瞬間 burst（如 concurrency=500 同時打入），而 pool 耗盡已由 `SATimeoutError → 503` 妥善處理。因此 rate limiting 的職責只保留在「防單一 IP 持續性濫用」，全域保護交給既有的 503 機制。

**ODS 層 first-write-wins idempotency**
同一 `order_id` 只有第一筆能寫入 ODS，透過兩道防線實現：pre-check（commit 前查 ODS 是否已有此 order_id）和 `UNIQUE(ods.order_id)` + `UNIQUE(ods.raw_id)` 約束作為 TOCTOU race 的兜底。後進的重複 Raw 不報錯，而是寫入 `duplicate` 終態，讓監控能明確區分正常處理與重複攔截。

**`force=True` 的語意邊界：單筆重試，而非 Backfill**
`POST /process_raw/{raw_id}?force=true` 只允許對 `error` 或 `duplicate` 狀態的記錄使用，語意是「重試這筆處理失敗的記錄」。對 `processed` 的記錄呼叫會直接回 400——因為若下游（Star Schema、聚合統計表）已消費過此筆 ODS，單獨刪除再重寫 ODS 無法 cascade 修正下游，反而製造不一致。若清洗邏輯改動後需要重新 derive 歷史資料，正確的工具是 Backfill batch job：從不可變的 Raw 層出發，全量重新產生 ODS 再逐層重跑下游——這是 Phase 4 引入 Airflow 時要解決的問題，不在 `force=True` 的責任範圍內。

---

## 壓測結果

針對五個場景進行壓測，驗證併發行為與故障模式。

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
raw table 的 order_id 欄位只有 index，沒有 UNIQUE 約束，100 筆重複訂單全被當作合法資料處理，各自拿到不同 raw_id，ODS 出現 100 筆相同訂單。CAS lock 保護的是「同一個 raw_id 不被重複消費」，業務層去重是另一層的問題，這是設計上已知的邊界（業務去重已透過 ODS idempotency 解決）。

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
| `processing` | server crash 在 claim commit 之後、狀態更新之前 |

應對方向：~~啟動時掃描 recovery（`SELECT * FROM raw WHERE status IN ('pending','processing')`）~~、改用 Redis/Celery/Kafka 取代 BackgroundTasks、~~定期掃描超過 N 分鐘仍是 processing 的記錄並重設為 pending~~（已補上 Recovery 機制）。

**測試六：同一 order_id 重複提交（ODS idempotency）**

情境一（sequential）：同一 order_id 先後送入兩次。第一筆正常寫入 ODS；第二筆處理時，pre-check 查到 ODS 已有此 order_id，直接將 Raw 標為 `duplicate`，ODS 不重複寫入。

情境二（TOCTOU race）：兩個 worker 同時通過 pre-check，第一個搶先 commit ODS，第二個 commit 時觸發 `IntegrityError`，catch 後不 retry，直接標為 `duplicate`。
結果：ODS 始終只有一筆，後進的重複 Raw 均為 `duplicate` 終態，下游與監控能明確區分正常處理與重複攔截。

---

## Retry 與 Recovery 機制

### Retry 機制

四層 Retry 策略，應對暫時性故障。所有 retry 採 exponential backoff（0.5s → 1s → 放棄），最多重試 3 次。

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

### 能應對 vs. 不能應對

| 情況 | 能應對？ |
|---|---|
| 任何階段的 DB 連線短暫中斷 | ✅ |
| Crash 後遺留的 `pending` / `processing` 記錄 | ✅（掃描 Recovery）|
| Connection pool 打爆（`TimeoutError`）| ~~❌ 例外型別不同，Point 1 catch 不到~~ → ✅ 目前已 catch `SATimeoutError` 回傳 503，快速失敗，由 client 自行重試 |
| SIGKILL 正在執行時 | ❌ 程序已死，任何 retry 都無法觸發 |
| ~~同一 `order_id` 重複送入~~ | ~~❌ `order_id` 無 UNIQUE 約束~~ → ✅ 已透過 ODS idempotency 解決 |
| ~~Scan retry 對已寫入 ODS 的記錄重新處理~~ | ~~❌ 無 idempotency 保護，ODS 會重複寫入~~ → ✅ 已透過 ODS idempotency 解決 |

---

## Timeout 設定

**DB statement timeout（`database.py`）**
透過 `connect_args={"options": "-c statement_timeout=30000"}` 在每條連線上設定 PostgreSQL session-level timeout。確保任何 SQL 超過 30 秒（如 lock wait 導致的掛住）會拋 `OperationalError`，讓 retry 機制能正常接管，而不是讓 thread 永久掛住。

**Connection pool 明確設定（`database.py`）**
`pool_size=5, max_overflow=10, pool_timeout=30`，與 SQLAlchemy 預設值相同，但明確寫出來方便日後調整。

**Pool 耗盡 → 503（`main.py`）**
`POST /orders` 額外 catch `SATimeoutError`（pool 等不到連線），直接回傳 503 Service Unavailable，不走 retry loop。Pool 耗盡是資源競爭問題而非 DB 故障，retry 無法改善，應快速失敗讓 client 自行退讓。

**`POST /process_raw/{raw_id}` 改為 background task（`main.py`）**
從直接呼叫 `process_raw_event(raw_id)` 改為 `background_tasks.add_task`，與 `/orders` 設計一致，不再 block event loop。

---

## Rate Limiting

使用 `slowapi` 對三個 endpoint 實施 per-IP 限流，計數 key 為 `request.client.host`（TCP 直連 IP）。

| Endpoint | per-IP 限制 | 理由 |
|---|---|---|
| `POST /orders` | 60/minute | 防單一 client 異常頻率，正常用戶下單行為遠不到此上限 |
| `POST /process_raw/{raw_id}` | 20/minute | 人工 replay 操作，頻率天然低 |
| `GET /raw/{raw_id}` | 120/minute | Read-only，較寬鬆 |

超出限制時回傳 `429 Too Many Requests`。

**為什麼不加全域上限**

全域上限的數字必須從「預期同時活躍 IP 數 × per-IP 上限」推導，在沒有真實流量資料時無從決定。更根本的問題是 rate limit（`/minute` 視窗）無法防止瞬間 burst——500 個請求在同一秒同時打入，全部都在 600/minute 上限內，pool 照樣會被打爆。pool 耗盡已由 `SATimeoutError → 503` 處理，rate limiting 的職責只保留在「防單一 IP 持續性濫用」。

**⚠️ 部署注意事項**

目前直接跑 uvicorn，`request.client.host` 是真實的 client IP，限流行為正確。若未來在 Nginx / Load Balancer 後面部署，`request.client.host` 會變成 proxy 的 IP，所有請求共用同一個計數器，per-IP 限流將失效。屆時需將 key_func 改為讀取 `X-Forwarded-For` header，並搭配適當的 trusted proxy 設定。

---

## API 端點

| Method | Path | 說明 |
|---|---|---|
| `POST` | `/orders` | 寫入新訂單（存 Raw，觸發背景任務） |
| `POST` | `/process_raw/{raw_id}` | 手動 replay 指定 raw（加 `?force=true` 可重置狀態） |
| `GET` | `/raw/{raw_id}` | 查詢 raw 的處理狀態和 payload 預覽 |

---

## 資料流

```
OrderIN（巢狀 Pydantic）
    └── from_nested() → ODSOrder（攤平的 Pydantic）
            └── clean_order() → ODS（SQLAlchemy model）
```

Pydantic 負責驗證和攤平，SQLAlchemy 負責存資料，兩層刻意解耦，各自獨立。

---

## 專案結構

```
.
├── main.py        # FastAPI app、路由
├── process.py     # 背景任務、狀態機、claim 邏輯
├── clean.py       # format_clean、business_clean、clean_order
├── schema.py      # Pydantic schemas（OrderIN、ODSOrder、RawOut...）
├── models.py      # SQLAlchemy models（Raw、ODS）
├── database.py    # Engine、SessionLocal、Base
├── pytest.ini     # 測試設定（asyncio_mode、coverage）
├── tests/
│   ├── conftest.py        # 共用 fixtures
│   ├── helpers.py         # Mock 工廠函式與測試資料
│   ├── test_clean.py      # format_clean、business_clean、clean_order
│   ├── test_schema.py     # ODSOrder.from_nested
│   ├── test_raw_write.py  # Point 1：Raw 寫入 retry
│   ├── test_process.py    # Point 2–4：Claim / Processing / Status retry；Idempotency
│   ├── test_scan.py       # scan_and_recover、lifespan startup、periodic scan
│   ├── test_timeout.py    # Pool 耗盡、/process_raw、GET /raw、DB 設定
│   └── test_rate_limit.py # per-IP 限流
├── .env           # DB_URL（不進版控）
└── .gitignore
```

---

## 怎麼跑起來

```bash
# 1. Clone
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform

# 2. 建虛擬環境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. 裝套件
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic python-dotenv pytz slowapi
pip install pytest pytest-asyncio pytest-cov  # 測試依賴

# 4. 設定環境變數
cp .env.example .env
# 編輯 .env，填入你的 DB_URL=postgresql://user:password@localhost/dbname

# 5. 啟動
uvicorn main:app --reload

# 6. 執行測試（需先設定 .env）
pytest
```

跑起來之後 API 文件在 `http://localhost:8000/docs`

---

## 預期完整架構

目前實作涵蓋接收層與處理層，完整的目標架構如下：

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
寫入 ODS（PostgreSQL，乾淨原始大表）
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
- `rpt_*`：在 dim/fct 之上進一步聚合，粒度固定，專為 Dashboard 效能與成本最佳化

---

## 開發藍圖

**Phase 1 — 系統可靠性**
- [v] Retry 機制 — 四層 retry（Raw 寫入、Claim、Processing、Status 更新），exponential backoff，耗盡後記 `CRITICAL` log
- [v] 掃描 Recovery — 啟動時掃描一次 + 每 5 分鐘 periodic scan；stale `processing`（>10分鐘）重設為 pending；potential duplicate ODS 記 `WARNING`
- [v] Timeout — DB statement timeout（30s）防 lock wait 掛住 thread；pool 設定明確化；`POST /orders` catch pool 耗盡回 503；`/process_raw` 改為 background task 不 block event loop
- ~~[ ] Idempotency — `Raw.order_id` 加 UNIQUE 約束 + `create_order` 的 `IntegrityError` handling；ODS 加 `raw_id` 欄位防止 scan retry 重複寫入~~
- [v] Idempotency — ODS 加 `raw_id` 欄位 + `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`；first-write-wins 策略：pre-check + IntegrityError 兜底；重複記錄標為 `duplicate` 終態
- [v] Rate Limiting — per-IP 限流（slowapi），`POST /orders` 60/min、`POST /process_raw` 20/min、`GET /raw` 120/min；不加全域上限（見設計決策）

**Phase 2 — 可驗證性**
- [v] Pytest — 78 個測試，7 個原始碼檔案全部 100% 覆蓋（`pytest --cov`）；涵蓋所有 retry 路徑（Point 1–4）、CAS claim、idempotency、crash recovery scan、`format_clean`、`business_clean`、`ODSOrder.from_nested`；`asyncio_mode=auto` 取代手寫 `asyncio.run()`；`reset_limiter` fixture 解決 rate limit 計數器跨測試污染問題。目前僅單元測試與整合測試（HTTP 層），無端到端測試；待 Phase 3 Docker / docker-compose 建立後，再補上真實 DB 的 E2E 測試。
- [ ] ODS 寫入前的資料品質驗證與 Profiling

**Phase 3 — 工程化**
- [ ] JWT 身份驗證
- [ ] 環境變數集中管理
- [ ] Alembic DB migration
- [ ] Docker / docker-compose（Queue + Worker + DB 一鍵啟動）

**Phase 4 — 分析層完整實作**
- [ ] 升級成 Celery + Redis（取代目前的 BackgroundTasks）
- [ ] Airflow（本地）定期抽取 ODS → BigQuery（incremental，以 `received_at` 為 watermark）
- [ ] dbt Core：stg_* → int_* → dim_*/fct_*（Star Schema in BigQuery）→ rpt_*（固定粒度預聚合）
- [ ] Looker Studio 接 BigQuery dim_*/fct_*/rpt_* 做報表與 Dashboard
- [ ] OpenTelemetry — 在現有 structlog 基礎上接入 OTel SDK，補全可觀測性的三個 pillar：
  - **Logs**：structlog 輸出接 OTel Log Exporter，與 Metrics / Traces 共用同一套 context（`trace_id` / `span_id` 自動注入每條 log，跨服務 log 可關聯）
  - **Metrics**：透過 OTel Metrics API 量化業務指標——訂單寫入量、ODS 處理成功 / 失敗 / duplicate 比率、processing 延遲分佈（P50/P95/P99）、DB pool 壓力、Retry 次數分佈
  - **Traces**：在 Celery + Airflow 引入後，對「API 接單 → Worker 處理 → Airflow 抽取 → BigQuery 寫入」全鏈路做分散式追蹤，定位跨服務延遲與瓶頸
  - Exporter 目標：Grafana Cloud（Loki + Prometheus + Tempo）或 GCP Cloud Trace / Cloud Monitoring

---

## 已知問題

**Scan 可能對已排程的任務重複排程**
Periodic scan 與 startup scan 會撈出所有 `status='pending'` 的 Raw 記錄並重新排程，但 DB 無法感知某筆記錄是否已經在 BackgroundTasks 佇列中等待執行。在高流量情境下，若大量請求寫入後尚未被消化，scan 就可能對這些「已排隊但還沒 claim 的記錄」再排一次，造成同一筆 raw_id 有多個 worker 同時嘗試處理。目前靠 CAS claim（`try_claim_raw`）在執行層兜底——後到的 worker 拿到 `rowcount=0` 會直接 return，不會重複寫入 ODS，正確性沒有問題，但會浪費 thread 資源，在高負載時會加重 pool 壓力。

**未來換 Queue 時的修正方向**
根本解法是讓「已排進 Queue」這件事對 DB 可見，具體做法是在狀態機中加入 `queued` 狀態（`pending → queued → processing → processed/error/duplicate`），並把入隊動作與狀態轉移綁在一起：寫入 Raw 後立刻以 CAS 原子性地將 `pending` 轉為 `queued`，成功才 push 進 Queue；scan 只撈 `pending`（即「從未成功入隊」的記錄），`queued` 的記錄一律跳過。Worker 的 CAS claim 對象也對應改為 `queued → processing`。唯一需要額外處理的邊界情況是：`pending → queued` 寫入 DB 成功、但 Queue push 失敗，此時記錄會卡在 `queued`——scan 需另外掃描超時的 stale `queued` 記錄並重設為 `pending`，讓它重新走一遍入隊流程。
