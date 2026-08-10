# 任務佇列：Celery + Redis

## 範圍與職責邊界

本文件記錄**攝入路徑的任務佇列**——`POST /orders` 收下訂單之後，那筆 Raw 是怎麼被
交給背景處理的、中途死掉會怎樣、以及誰負責把死掉的撿回來。

各層自己的契約不在這裡：品質契約見 [DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md)，
分析管線的編排見 [ORCHESTRATION-TW](./ORCHESTRATION-TW.md)，E/L 見
[CLOUD_LAYER-TW](./CLOUD_LAYER-TW.md)。

```
POST /orders ──► Raw (PostgreSQL) ──► Redis ──► Celery worker ──► ODS
                      ▲                            │
                      └──── Celery Beat ───────────┘   ← 週期恢復掃描
                            (tasks.scan_and_dispatch)
```

**這與 Airflow 正交**。[ORCHESTRATION-TW〈範圍與職責邊界〉](./ORCHESTRATION-TW.md)
已經把兩者的差異寫死：Airflow 排的是分鐘～小時級的批次（抽取 + dbt），這裡處理的是
毫秒～秒級的單筆。兩者唯一共用的只有「Redis」這個字，而且**刻意不共用同一個實例**
——共用會讓故障域糾纏：分析管線的 broker 塞爆，不該讓線上攝入停擺。

---

## 1. 為什麼要換掉 BackgroundTasks

`BackgroundTasks` 是 **純記憶體佇列**。README 的壓力測試「測試五」量過它的下限：
SIGKILL 之後 150 筆永久卡在 `pending`，重啟沒有任何機制知道要重跑。

四層 retry 與掃描 recovery 補得了「處理失敗」，補不了「任務本身消失」——那是行程
記憶體的性質，不是重試次數的問題。持久化佇列才是根解。

順帶解掉的是水平擴展：`BackgroundTasks` 與週期掃描都是行程內狀態，API 因此被釘在
`--workers 1`（多開一個行程就多跑一份掃描迴圈）。

---

## 2. 核心設計決策

| 決策 | 選擇 | 為什麼 |
|---|---|---|
| task 與業務邏輯的關係 | **薄包裝**（`tasks.py` 包 `process.process_raw_event`） | `process.py` 保持零 Celery 依賴，才能被 pytest、腳本、以及「broker 掛了手動補跑」直接呼叫；換佇列時只有包裝層要改 |
| result backend | **不開**（`task_ignore_result=True`） | 任務狀態的真相是 PG 的 `raw.status`，且有 `GET /raw/{raw_id}` 可查。再開一份 Redis 結果狀態＝製造第二份會漂移的真相 |
| ack 策略 | `acks_late` + `reject_on_worker_lost` | 見 §3 的恢復矩陣：在「崩在 claim 之前」嚴格更好，在「崩在 claim 之後」中性 |
| prefetch | `worker_prefetch_multiplier=1` | `acks_late` 的標配。預抓會讓單一 worker 崩潰時，拖住它抓走的一整批訊息 |
| 序列化 | **只收 JSON** | pickle 反序列化等同執行任意程式碼，broker 一旦被寫入即等同 RCE |
| Celery 層 retry | **不設** | `process.py` 已有四層 retry；再疊一層是 3×3 的重試放大，且 `process_raw_event` 不對外拋例外，Celery 根本看不到失敗 |
| Redis 持久化 | **不開 appendonly** | 掃描已是資料庫層的兜底，丟訊息的後果只是「延遲到下一輪」而非遺失。用 fsync 成本去換一個已有備援的保證不划算 |
| broker 掛掉時的攝入 | **回 200 `pending`，不回 500** | Raw 已 commit 落地。回 500 會讓上游重送、灌出一批同 `order_id` 的 Raw 全變 `duplicate` 雜訊，但資料早就收下了 |

### 2.1 broker 等待必須有上限 ⭐

`_enqueue` 是同步呼叫、又坐在 async endpoint 上。實測 `docker compose stop redis` 後，
單次 `POST /orders` **阻塞 19 秒**——kombu 的連線重試退到了 OS 層的 DNS / TCP 逾時，
而且卡的是整個 event loop（`--workers 1` 下等於全服務停擺）。

兩層修法，缺一不可：

1. **收斂逾時**：`socket_connect_timeout` / `socket_timeout` / `broker_connection_timeout`
   皆設 2s，`task_publish_retry_policy.max_retries=1`。19s → 3.81s（= 2s × 2 次嘗試）。
2. **移出 event loop**：所有 async 呼叫點一律 `await asyncio.to_thread(_enqueue, ...)`。

驗證：broker 全停時 `POST /orders` 仍回 200（3.81s），同時 `/health` 只要 1.7ms
——loop 沒有被卡住。

**這條 3.81s 是「broker 全掛」的降級延遲，不是常態**。常態下 publish 是次毫秒級。

⚠️ **`_enqueue` 沒有「記住 broker 已死」的狀態**，所以 Redis 全掛期間**每一筆請求**
都會重付這個逾時。這與限流儲存的行為形成對比——slowapi 有 `_storage_dead` 旗標加
指數退避，偵測到一次之後就直接走 fallback（實測：首筆 3.77s，之後 2.5ms）。
兩者疊加後，Redis 全掛時 `POST /orders` 實測落在 7～18s。

語意仍然正確（回 200 `pending`、資料落地、掃描接手），但這個延遲在持續流量下會讓
上游 client 端逾時、進而重送 → 灌出一批同 `order_id` 的 Raw。若要壓低，可以把
`socket_connect_timeout` 降到 1s 並把 `task_publish_retry_policy.max_retries` 設為 0
（換取「瞬斷不再有即時重試，改等下一輪掃描」），或替 `_enqueue` 加一個熔斷旗標。
**目前刻意不做**——尚未有持續流量會踩到它，見 §6。

---

## 3. CAS claim 與重新投遞的交互作用 ⭐

這是整套設計裡最不直觀的一段，也是最容易在「已經有持久化佇列了」的直覺下被拆掉的一段。

至少一次投遞（at-least-once）意味著同一則訊息可能被送兩次。這件事本身是安全的——
`try_claim_raw` 的 CAS（`UPDATE ... WHERE status='pending'`，靠 `rowcount==1`）加上
`UNIQUE(ods.order_id)` / `UNIQUE(ods.raw_id)` 早就擋住了重複處理。

**但重新投遞救不回全部的崩潰。** 對照 worker 死掉的兩個時點：

| 崩潰時點 | `raw.status` | 訊息重投遞後 | 誰救得回來 |
|---|---|---|---|
| claim commit **之前** | `pending` | CAS 成功 → 正常處理 | **佇列自己**（秒級） |
| claim commit **之後** | `processing` | CAS 失敗（`rowcount==0`）→ task 直接 return | **只有 stale 掃描**（`STALE_PROCESSING_MINUTES` = 10 分鐘） |

推論有兩個，都很重要：

1. **`acks_late` 值得開**。它在第一種情況嚴格更好（秒級恢復 vs 等下一輪掃描），
   第二種情況中性。代價（重複投遞）是既有冪等性本來就承接的。
2. **`scan_and_recover` 不能刪，而且比以前更關鍵**。它的角色從「主要恢復機制」
   變成「**佇列語意的補集**」——專門處理佇列自己救不回來的那一半。

> ⚠️ 未來看到「都已經有持久化佇列了」就想拿掉週期掃描的人，請先讀完這張表。
> 拿掉之後，第二列那些記錄會**永久**卡在 `processing`。

### 3.1 逾時基準必須是 `processing_started_at`，不是 `received_at` ⭐

上表第二列的恢復靠 stale 掃描，而 stale 掃描要問的是
**「這次處理跑了多久」**。`received_at` 回答的卻是**「這筆資料躺了多久」**——
平時兩者幾乎相等（進來就處理），**積壓時相差極大，而積壓正是這個判定最常被觸發的
時候**。

用 `received_at` 當基準會炸出下面這條時間軸（已實測重現，數據見 §5.4）：

```
T-30min  訂單攝入，received_at = T-30min，因 broker 停機留在 pending
T+0      broker 復原，掃描派工；worker A 搶佔成功 → status = processing
T+0.01   worker A 正在清洗、組 ODS（尚未 commit）
T+0.02   下一輪掃描：status='processing' ✓ 且 received_at < now()-10min ✓
         → 判定 stale → 改回 pending → 再派一則新訊息
T+0.03   worker B 搶佔：狀態現在是 pending，CAS 成功 ← 擋不住
         同一個 raw_id 有兩個 worker 在跑
T+0.05   A 先 commit：ods.raw_id 落地，raw.status = 'processed'
T+0.06   B 撞到「自己」寫的 ODS，被判為 duplicate，蓋掉 processed
```

三件事值得特別指出：

1. **CAS 沒有失效**。它防的是「同一個狀態下的競爭」，防不了「狀態被第三方倒退回
   `pending`」。號碼牌在處理中途被收回重發，第二次搶奪就成了合法動作。
2. **第二個 worker 不是 Celery 重投遞來的**，是掃描自己 `.delay()` 出來的一則全新
   訊息。單純的重投遞反而是安全的——CAS 會讓它 `rowcount==0` 直接 return。
3. **資料不會壞，壞的是訊號**。ODS 的 `UNIQUE(raw_id)` / `UNIQUE(order_id)` 擋住了
   重複寫入，但那筆其實處理成功的訂單最終頂著 `duplicate`——污染了「上游重送」這個
   刻意保留的監控語意（見 CLAUDE.md 架構約束）。加上白做的一份工，發生在系統
   正在追進度的時候。

改用 `processing_started_at`（由 `try_claim_raw` 在搶佔成功時蓋上）之後，計時從
「開始處理」起算，與資料躺多久無關，`T+0.02` 那一步不再成立。

**自我碰撞因此不可達**：全專案只有 `try_claim_raw` 會把狀態寫成 `processing`，
也只有 stale 掃描會把它退回 `pending`（`/process_raw?force=true` 只接受 `error` /
`duplicate`）。堵住這唯一的倒退路徑，症狀就沒有來源了——所以**不需要**再為
「自我碰撞」另外加一個訊號，那會是一個永遠為 0 的指標。

> **不變式**：`status='processing'` ⇒ `processing_started_at` 非空。
> 由 `try_claim_raw` 保證，並由 migration `e5f6a7b8c9d0` 對既有資料 backfill 建立。
> 若這個不變式被破壞（例如手動寫 DB），該筆會永遠不符合 stale 條件而卡死。

---

## 4. 恢復掃描為什麼在 Beat 而不在 API

原本 `_periodic_scan` 是掛在 FastAPI `lifespan` 上的 asyncio 迴圈。它是**行程內狀態**：
API 一旦跑多個 uvicorn worker，每個行程都會各跑一份掃描。搬到 Beat 之後 API 行程不再
持有任何背景狀態，這才解開了 `--workers 1` 的鎖。

**為什麼不是 Airflow**：5 分鐘一次的排程 Airflow 當然做得到，但故障域錯了——攝入路徑
的自我修復不該依賴分析編排器活著。而且 [ORCHESTRATION-TW](./ORCHESTRATION-TW.md)
開宗明義寫了「Airflow 不是任務佇列」。

**Beat 啟動時會立刻補一次掃描**（`beat_init` signal）。理由是 Beat 的第一次 tick 要
等滿一個間隔（預設 300s），中間若有上一輪殘留的 pending 就會多躺 5 分鐘沒人管——
這正是原本 lifespan startup recovery 在填的洞。掛在 Beat 而非 API 的 lifespan，是因為
「排程器重啟」才是該補掃的時機：API 重啟不代表有東西需要恢復。

**Beat 只能有一個實例**。多個 beat 會各自按時派掃描，後果無害（`scan_and_recover`
冪等、CAS 擋住重複處理）但純屬浪費。也不要用 `celery worker -B` 的內嵌 beat，
官方明示非生產用法。

---

## 5. 實機驗證記錄（2026-08-10）⭐

下面每個數字都是量出來的，不是設計時的推論。環境：docker compose 全棧
（api / worker×4 / beat / redis / postgres），`SCAN_INTERVAL_SECONDS=20` 以縮短觀察窗。

### 5.1 SIGKILL：README 測試五的翻案

灌入 800 筆 `pending`，等 Beat 派工、worker 處理到 225 筆時 `docker compose kill -s SIGKILL worker`：

| 時點 | `pending` | `processing` | `processed` |
|---|---|---|---|
| SIGKILL 當下 | 537 | 2 | 261 |
| worker 重啟後 30s | 0 | **2** | 798 |
| `processing_started_at` 回推 11 分鐘、等一輪掃描 | 0 | 0 | **800** |

最終 ODS 800 筆，**零遺失**。

這張表就是 §3 恢復矩陣的實體：537 筆在佇列裡的靠重新投遞自行排空，而 SIGKILL 當下
正在處理的那 **2 筆卡在 `processing`，重啟 worker 完全救不了它們**——只有 stale 掃描
能。（第三列用回推 `processing_started_at` 模擬滿 10 分鐘，避免實測乾等。）

對照 README 壓力測試「測試五」的舊結論——「150 筆永久卡在 pending，重啟後無自動
recovery」——這一項到此翻案。

### 5.2 Beat 啟動補掃

Beat 於 `05:58:38` 啟動，`beat_init` 派出的掃描在 `05:58:39` 被 worker 收到，
首次排程 tick 則在 `05:58:58`（+20s）。補掃確實填住了第一個間隔的空窗。

### 5.3 broker 停機下的攝入

見 §2.1。`POST /orders` → HTTP 200 + `pending`（3.81s），資料落地；`/health` 1.7ms。
Redis 復原後，積壓的 2 筆由掃描撿回並處理完成。

### 5.4 逾時基準：修正前後對照

同一組腳本、同一份資料，只差 §3.1 的基準欄位。灌入 2000 筆
`received_at = now() - 30 分鐘` 的 pending（模擬 broker 長時間停機後的積壓），
`SCAN_INTERVAL_SECONDS=5` 加密掃描頻率：

| | `processed` | `duplicate` | 自我碰撞 |
|---|---|---|---|
| 修正前（基準 `received_at`） | 1998 | **2** | **2** |
| 修正後（基準 `processing_started_at`） | **2000** | 0 | **0** |

自我碰撞以 `raw.status='duplicate'` ⋈ `ods.raw_id = raw.id` 判定。修正前那 2 筆的
`error_message` 直接寫著「已由 raw_id=1998 寫入 ODS」——而 1998 就是它自己；
且該 `order_id` 在 `raw` 表只出現一次，排除上游重送的可能。

**量級說明**：每輪掃描命中的筆數 ≈ 當下併發處理中的筆數 ≈ worker concurrency，
與積壓總量無關（單筆處理約 40ms，掃描間隔遠大於它）。所以是「罕見但真實」，
不是大規模污染——但它專挑系統正在追進度的時候發生。

接著重跑 §5.1 的 SIGKILL 情境確認**沒有修壞恢復機制本身**：2 筆卡在
`processing` 的記錄照樣被回收，最終 2900 筆全數 `processed`、ODS 2900 筆、
自我碰撞 0。

### 5.5 多行程下的限流

`SCAN_INTERVAL_SECONDS` 不變，api 開 4 個 uvicorn worker，對同一把 API key
連送 100 筆 `POST /orders`（限額 `60/minute`）：

| 計數器儲存 | 200 | 429 |
|---|---|---|
| 行程記憶體（`RATE_LIMIT_STORAGE_URI=`） | **91** | 9 |
| Redis db 1 | **60** | 40 |

記憶體模式放行 91 筆而非 60——每個 worker 各記各的，且 OS 對連線的分派並不平均
（所以不是整齊的 4 倍）。**沒有任何錯誤訊息**，限額就這樣安靜地失守，這正是它
危險的地方。改用共享儲存後精準落在 60。

Redis 停機期間 slowapi 記一筆 `falling back to in-memory storage` 並降級為每行程
計數；復原後記 `Rate limit storage recovered`，限額回到精準的 60/40。

---

## 6. 已知邊界與刻意先不做

| 項目 | 為什麼 | 觸發點 |
|---|---|---|
| **佇列分流**（ingest / replay 各一條 queue） | 目前量級下單一 queue 夠用，路由接縫（task 名稱已固定）留著隨時可切 | replay 大量灌入開始擠壓線上攝入延遲時 |
| **Redis appendonly / 叢集** | 掃描已是兜底，見 §2 決策表 | broker 從「可選」變成「唯一真相」時（例如未來拿掉 DB 層 pending 語意） |
| **Celery 層 retry / 死信佇列** | 失敗語意已完整落在 `raw.status`（`error` 是終態，帶 `error_message`） | 需要區分「業務失敗」與「基礎設施失敗」並分別重試時 |
| **Flower 等監控 UI** | 沒有 result backend，Flower 能看的東西有限；`raw.status` 才是真相 | 接 OpenTelemetry 時一併評估（藍圖 Phase 5 獨立項）|
| **broker 掛掉時回壓上游** | 現在是「收下但延遲」，語意正確且上游不需要改 | 積壓到 DB 寫入本身成為瓶頸時 |
| **`_enqueue` 的熔斷旗標** | Redis 全掛時每筆請求都重付連線逾時（見 §2.1）。目前沒有持續流量會踩到，加了反而多一份要維護的狀態 | 有持續攝入流量、且一次 Redis 事故會讓上游因逾時而重送時 |

---

## 7. Runbook

```bash
# 全棧啟動（api / worker / beat / redis / db）
docker compose up -d --build

# 觀察佇列積壓
docker compose exec redis redis-cli llen celery

# 目前狀態分佈（真相在這裡，不在 Redis）
docker compose exec db psql -U app -d orders -c \
  "select status, count(*) from raw group by status order by status;"

# 手動觸發一次恢復掃描（不必等 Beat）
docker compose exec worker celery -A celery_app call tasks.scan_and_dispatch

# 單筆補跑（broker 掛掉時的救援路徑：完全不經佇列）
docker compose exec worker python -c \
  "from process import process_raw_event; process_raw_event(123)"

# 縮短掃描間隔以觀察行為（預設 300s）
SCAN_INTERVAL_SECONDS=20 docker compose up -d
```

**卡在 `processing` 的記錄怎麼辦**：不要手動改 status。等 stale 掃描（10 分鐘）自動
處理即可——這正是它存在的理由。若確定要立刻恢復，把該筆 `processing_started_at`
回推超過 `STALE_PROCESSING_MINUTES` 再等一輪掃描，語意上等同於「宣告這次處理已經
逾時」。**不要改 `received_at`**：它是攝入時刻、屬於資料血緣，與逾時判定無關（原因見 §3.1）。
