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

---

## 壓測結果

針對五個場景進行壓測，驗證併發行為與故障模式。

**測試一：1,000 筆不同訂單，concurrency=50**
結果：全部成功，耗時 7.9 秒，無錯誤。
`POST /orders` 只做一次快速 INSERT 就釋放連線，每筆持有連線時間 < 10ms。concurrency=50 遠低於 DB pool 能承載的吞吐量，排隊現象不存在。

**測試二：1,000 筆不同訂單，concurrency=500**
結果：P99 延遲約 14 秒，5 筆 HTTP 500 錯誤。
SQLAlchemy 預設 pool_size=5、max_overflow=10，最多 15 條連線。500 個請求同時湧入，485 個排隊等連線，超過 pool_timeout=30 秒的請求直接拋出 `QueuePool limit reached`。那 5 筆錯誤是在 INSERT 之前就 timeout，raw table 裡沒有對應記錄。

應對方向：調大 pool、改用 async SQLAlchemy（asyncpg）、或在 API 前端加 rate limiting。

**測試三：100 筆相同 order_id，concurrency=100**
結果：raw 寫入 100 筆，ODS 寫入 100 筆，全部成功。
raw table 的 order_id 欄位只有 index，沒有 UNIQUE 約束，100 筆重複訂單全被當作合法資料處理，各自拿到不同 raw_id，ODS 出現 100 筆相同訂單。CAS lock 保護的是「同一個 raw_id 不被重複消費」，業務層去重是另一層的問題，這是設計上已知的邊界。

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

應對方向：啟動時掃描 recovery（`SELECT * FROM raw WHERE status IN ('pending','processing')`）、改用 Redis/Celery/Kafka 取代 BackgroundTasks、定期掃描超過 N 分鐘仍是 processing 的記錄並重設為 pending。

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
1. 將卡住超過 10 分鐘的 `processing` 記錄重設為 `pending`，記 `WARNING`（存在 ODS 重複寫入風險，尚未實作 idempotency）
2. 收集所有 `pending` 記錄重新排程

### 能應對 vs. 不能應對

| 情況 | 能應對？ |
|---|---|
| 任何階段的 DB 連線短暫中斷 | ✅ |
| Crash 後遺留的 `pending` / `processing` 記錄 | ✅（掃描 Recovery）|
| Connection pool 打爆（`TimeoutError`）| ❌ 例外型別不同，Point 1 catch 不到 |
| SIGKILL 正在執行時 | ❌ 程序已死，任何 retry 都無法觸發 |
| 同一 `order_id` 重複送入 | ❌ `order_id` 無 UNIQUE 約束 |
| Scan retry 對已寫入 ODS 的記錄重新處理 | ❌ 無 idempotency 保護，ODS 會重複寫入 |

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
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic python-dotenv pytz

# 4. 設定環境變數
cp .env.example .env
# 編輯 .env，填入你的 DB_URL=postgresql://user:password@localhost/dbname

# 5. 啟動
uvicorn main:app --reload
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
寫入 ODS（乾淨原始大表）
  ↓
狀態更新（processing → processed / error）

【分析層】批次，排程觸發
ODS
  ↓
拆分寫入 dim / fact 表（Star Schema）
  ↓
聚合運算
  ↓
寫入統計 / 查詢表
  ↓
BI
```

**分析層為什麼用批次而不用 Streaming？**
下游是 BI，消費模式是報表與 Dashboard，T+1 或小時級的更新頻率已足夠。批次可以用 window 做資料品質檢查、出錯可重跑，穩定性更高。接收層與分析層也因此天然解耦，批次排程不影響即時寫入路徑。若未來下游接即時預測模型，才有換 Streaming 的動機。

---

## 開發藍圖

**Phase 1 — 系統可靠性**
- [v] Retry 機制 — 四層 retry（Raw 寫入、Claim、Processing、Status 更新），exponential backoff，耗盡後記 `CRITICAL` log
- [v] 掃描 Recovery — 啟動時掃描一次 + 每 5 分鐘 periodic scan；stale `processing`（>10分鐘）重設為 pending；potential duplicate ODS 記 `WARNING`
- [ ] Idempotency — `Raw.order_id` 加 UNIQUE 約束 + `create_order` 的 `IntegrityError` handling；ODS 加 `raw_id` 欄位防止 scan retry 重複寫入
- [ ] 接收層 Rate Limiting

**Phase 2 — 可驗證性**
- [ ] Pytest — 覆蓋 `try_claim_raw`、狀態機流轉、邊界條件
- [ ] ODS 寫入前的資料品質驗證與 Profiling

**Phase 3 — 工程化**
- [ ] JWT 身份驗證
- [ ] 環境變數集中管理
- [ ] Alembic DB migration
- [ ] Docker / docker-compose（Queue + Worker + DB 一鍵啟動）

**Phase 4 — 分析層完整實作**
- [ ] 升級成 Celery + Redis（取代目前的 BackgroundTasks）
- [ ] Airflow 做分析層排程
- [ ] ODS → Star Schema → 聚合 → 統計表
