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
| `received_at` 回推 11 分鐘、等一輪掃描 | 0 | 0 | **800** |

最終 ODS 800 筆，**零遺失**。

這張表就是 §3 恢復矩陣的實體：537 筆在佇列裡的靠重新投遞自行排空，而 SIGKILL 當下
正在處理的那 **2 筆卡在 `processing`，重啟 worker 完全救不了它們**——只有 stale 掃描
能。（第三列用回推 `received_at` 模擬滿 10 分鐘，避免實測乾等。）

對照 README 壓力測試「測試五」的舊結論——「150 筆永久卡在 pending，重啟後無自動
recovery」——這一項到此翻案。

### 5.2 Beat 啟動補掃

Beat 於 `05:58:38` 啟動，`beat_init` 派出的掃描在 `05:58:39` 被 worker 收到，
首次排程 tick 則在 `05:58:58`（+20s）。補掃確實填住了第一個間隔的空窗。

### 5.3 broker 停機下的攝入

見 §2.1。`POST /orders` → HTTP 200 + `pending`（3.81s），資料落地；`/health` 1.7ms。
Redis 復原後，積壓的 2 筆由掃描撿回並處理完成。

---

## 6. 已知邊界與刻意先不做

| 項目 | 為什麼 | 觸發點 |
|---|---|---|
| **佇列分流**（ingest / replay 各一條 queue） | 目前量級下單一 queue 夠用，路由接縫（task 名稱已固定）留著隨時可切 | replay 大量灌入開始擠壓線上攝入延遲時 |
| **Redis appendonly / 叢集** | 掃描已是兜底，見 §2 決策表 | broker 從「可選」變成「唯一真相」時（例如未來拿掉 DB 層 pending 語意） |
| **Celery 層 retry / 死信佇列** | 失敗語意已完整落在 `raw.status`（`error` 是終態，帶 `error_message`） | 需要區分「業務失敗」與「基礎設施失敗」並分別重試時 |
| **Flower 等監控 UI** | 沒有 result backend，Flower 能看的東西有限；`raw.status` 才是真相 | 接 OpenTelemetry 時一併評估（藍圖 Phase 5 獨立項）|
| **broker 掛掉時回壓上游** | 現在是「收下但延遲」，語意正確且上游不需要改 | 積壓到 DB 寫入本身成為瓶頸時 |

### 6.1 已知缺陷：stale 判定用的是 `received_at` ⚠️

`scan_and_recover` 判定 stale 的條件是
`status = 'processing' AND received_at < now() - STALE_PROCESSING_MINUTES`，
而 `received_at` 是**攝入時間**，不是**開始處理的時間**。

在長積壓的情境下這會誤判：broker 停機 15 分鐘累積的記錄，復原後被 worker claim 成
`processing`，下一輪掃描立刻認定它們「已 stale」並重設為 `pending` ——即使 worker
正在處理。結果是同一筆被並行處理兩次，落敗方吃 `IntegrityError` 後把 `raw.status`
覆寫成 `duplicate`，讓一筆其實處理成功的訂單帶著誤導性的狀態。

資料不會壞（ODS 的 UNIQUE 約束擋住重複寫入），壞的是**監控訊號**——而 `duplicate`
在本專案是刻意保留的監控語意（見 CLAUDE.md 架構約束）。

這個缺陷在 `BackgroundTasks` 時代很難踩到（任務會直接消失而非積壓），持久化佇列
讓它變得可達。**修法需要 schema 變更，尚未實作**，選項見 README 的 Phase 5 待辦。

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
處理即可——這正是它存在的理由。若確定要立刻恢復，把該筆 `received_at` 回推超過
`STALE_PROCESSING_MINUTES` 再等一輪掃描，語意上等同於「宣告它已經逾時」。
