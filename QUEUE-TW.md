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

### 2.2 派工必須熔斷 ⭐

上面那個 3.81s 是**單筆**的數字。實測併發之後才看得到真正的形狀：

| 併發 | 無熔斷器 |
|---|---|
| 1 | 3.8s |
| 8 | 每筆 12.8s |
| 48 | **47/48 筆在 120 秒內沒有完成** |

退化是**超線性**的：kombu 的 producer pool 每行程上限 10，broker 不可用時每次取用
都要重付連線逾時，併發越高彼此排隊越久。

**最危險的不是慢，是「靜默成功」。** Raw 的 commit 在派工之前，所以那 47 筆逾時的
請求資料其實都已經寫進去了——客戶端卻只看到逾時。逾時在語意上是**未定**，上游只能
選擇重送，於是同一個 `order_id` 灌出第二筆、第三筆 Raw，最後在 ODS 端變成
`duplicate`。訊號本身正確（上游確實重送了），但成因是我方自己造成的。

現在的失敗模式是**無界延遲**而非快速拒絕：沒有 429、沒有 503、沒有 `Retry-After`，
上游拿不到任何可據以決策的資訊。

`circuit_breaker.CircuitBreaker` 把這件事收斂掉——連續失敗達門檻即開路，之後直接
回 False 而不碰 Redis。它不改變任何對外契約（仍回 200 `pending`、仍由掃描接手），
改變的只有**成本**：讓「進入 fallback」變得比 fallback 本身還便宜。設計取捨（狀態
為何是行程內的、half_open 為何要單飛、鎖為何不能跨越呼叫）見該模組的註解。

實測效果見 §5.6。

### 2.3 DB 交易不得跨越派工 ⭐

`create_order` 裡 `db.commit()` 之後那行 `db.refresh(raw)` 會**開啟一個新交易**取回
id，而它要到 `db.close()` 才結束。派工原本就夾在中間——於是 broker 故障期間，
每個等待中的請求都把一條連線掛在 `idle in transaction`（實測 60 併發時 32 個 pool
槽位有 23 個是這狀態）。

兩層代價：連線池被佔滿後新請求只能等 `pool_timeout`；同時這些長交易會壓住
PostgreSQL 的 vacuum horizon，在高寫入量的表上加速膨脹。

修法是先取出 `raw.id` 再明確 `db.close()`，讓交易在派工之前就結束。

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

### 4.1 掃描本身也必須有界 ⭐

熔斷器（§2.2）讓攝入在 broker 故障期間維持全速——代價是 `pending` 也以全速累積。
一次十分鐘的事故就能堆出數十萬筆。原本的掃描是「一次撈完所有 pending，逐一派工」，
那等於把攝入層剛避開的崩潰，原封不動搬到恢復路徑上。

三個限制，缺一不可：

| 機制 | 擋掉什麼 |
|---|---|
| **`LIMIT` + `id` 游標**（`SCAN_BATCH_SIZE`） | 記憶體佔用由批次大小決定，與積壓總量無關 |
| **單輪頁數上限**（`SCAN_MAX_ROUNDS`） | 單一 worker 槽位不會被一次事故佔住太久；清不完留給下一輪 tick |
| **Redis 鎖** | Beat 不管上一輪跑完沒有就會再派；重疊會讓同一批記錄送出兩份訊息 |

**為什麼游標是必要的，光有 `LIMIT` 不夠**：派工不會改變 `status`——記錄要等 worker
搶佔成功才離開 `pending`。所以每一輪都會撈到同一批最前面的記錄，永遠到不了後面。
必須 `id > after_id` 往前推。

**寬限期**（`PENDING_GRACE_SECONDS`）：剛攝入的 pending 不由掃描接手。正常情況下攝入
路徑會在毫秒內把它派出去，掃描此時介入只是為同一筆多送一則訊息。這裡用 `received_at`
是對的——問的正是「這筆資料躺了多久」；別和 §3.1 的 stale 判定搞混，那問的是
「這次處理跑了多久」。

**鎖是最佳化，不是正確性要求**：取鎖本身失敗（Redis 抽風）時照常執行，只有「明確被
別人持有」才略過——跳過掃描的代價是記錄卡著沒人管，比重複派工嚴重得多。掃描 task
若被 SIGKILL，鎖會留到 TTL 過期（一個掃描間隔），代價是犧牲一輪 tick。

**單輪上限的取捨**：上界 = `SCAN_MAX_ROUNDS × SCAN_BATCH_SIZE`。這個值實質上是
**恢復路徑的派工速率上限**（每個掃描間隔最多送出這麼多），調整時要對照 worker 的
消費速率——送得比消費快只會讓佇列堆積，並不會讓積壓更早清完。

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

### 5.6 派工熔斷：情況、解法與前後對照 ⭐

先把角色對上：

| 系統 | 比喻 |
|---|---|
| API 端點 | 餐廳櫃檯 |
| PostgreSQL 的 `raw` 表 | 訂單簿——**寫下去就算數** |
| Redis | 把單子送進廚房的傳送軌道 |
| Celery worker | 廚房 |
| 恢復掃描 | 定期翻訂單簿、補送漏單的人 |

#### 情況

觸發條件是 Redis 連不上（掛掉、重啟、OOM、網路斷）。資料庫還活著，壞掉的只是
「通知廚房」這條路。

櫃檯不知道軌道壞了，每接一張單還是走過去試著塞，塞不進去就站著等到逾時。人一多
就卡在走道上排隊，而且是**超線性**地惡化——1 筆等 3.8s、8 筆各等 12.8s、48 筆時
有 47 筆超過 120 秒沒有回應（完整曲線與成因見 §2.2）。

**最糟的不是慢，是「其實成功了但客人不知道」。** 程式的順序是「先寫訂單簿，再送
軌道」，所以那 47 筆的資料**早就寫進去了**，客人卻只看到逾時。逾時在語意上是未定
的，他唯一能做的就是再送一次——同一筆訂單於是進來第二次、第三次，下游變成一堆
`duplicate`。在每天上億筆的規模下，一次十分鐘的事故會製造出數百萬筆事後分不出
真假的重複資料。

#### 解法

跟家裡的無熔絲開關一樣——與其反覆去撞一個壞掉的東西，不如先切斷。連續三次塞不
進去就貼張紙條「別再試」，之後只寫訂單簿、完全不碰軌道；每 30 秒派**一個**請求
去試軌道通了沒，通了就撕掉紙條恢復正常。

之所以可以放心不送，是因為**本來就有人會翻訂單簿**：單子不會消失，只是晚幾分鐘
進廚房。這套備援一直都在，問題出在**進入備援的代價比備援本身還貴**——熔斷器把
這個代價變成零。設計取捨（狀態為何是行程內的、half_open 為何要單飛）見 §2.2 與
`circuit_breaker.py` 的模組註解。

#### 前後對照

Redis 全停，4 個 uvicorn worker，同一支腳本：

| | 結果 |
|---|---|
| 無熔斷器，48 併發 | **47/48 筆在 120 秒內沒有完成** |
| 有熔斷器，48 併發 | 48/48 全回 200，整批 18.4s |
| 有熔斷器，持續負載 200 筆 | **p50 = 5ms、p90 = 7ms**，>1s 者 14/200 |

第一波之所以仍慢（18.4s），是因為 48 筆同時到達時熔斷器還沒開路——三次失敗都還沒
**返回**，整波都已經越過閘門。這是熔斷器的固有性質：保護的是第一波之後的所有請求，
成本上界是「每行程一個併發波次」，而非每筆請求。持續流量下這一波只佔開頭數秒。

剩下的 14/200 長尾**不是派工**，是限流儲存的重探：拆開量，`POST /orders` 在開路後
出現 4 筆 0.02s（派工已零成本）與 4 筆 3.63s，而只有限流的 `GET /raw/1` 是 8 筆全
3.75s。slowapi 的 `_storage.check()` 沒有 single-flight，同一行程的併發請求會一起去撞。

broker 復原後 4 個行程各記一次 `circuit_closed`，請求回到 10–51ms，無需重啟。
事故全程 `派工失敗` 的 error log 為 **0 條**（開路前的失敗記在熔斷器的狀態轉移裡），
對照修正前每筆失敗一條 traceback。

#### 副作用：問題會搬到後門

櫃檯恢復全速接單後，訂單簿上待處理的量也開始全速累積——一次十分鐘的事故能堆出
數十萬張。翻訂單簿的人若想一口氣全部搬完，換他被壓垮，問題只是從前門搬到後門。
所以掃描本身也必須有界：設計見 §4.1，實測見下一節 **§5.7**。

### 5.7 有界掃描：實測

環境同上，worker concurrency 4，Beat 停用以便逐輪觀察。

**吞吐**（60,000 筆積壓，單輪清完）：發佈約 **2,140 msg/s**，worker 消費約
**305 筆/s**。60,000 筆全數 `processed`、ODS 60,000 筆，對帳一致。

**單輪上限與游標續傳**（120,000 筆積壓 > 上限 100,000）：

| | 派工筆數 | 剩餘 pending |
|---|---|---|
| 掃描 #1 | **100,000**（觸發「積壓未清完」warning） | 20,000 |
| 掃描 #2 | **20,000** | 0 |

最終 ODS 恰好 +120,000、`duplicate` **0 筆**——證明游標既沒漏掉記錄，也沒有把
已處理的重新送一次。

**重疊保護**：對同一批積壓幾乎同時送出兩則掃描訊息，第二則記下
`recovery scan 略過：上一輪仍在進行`，不重複派工。

**寬限期**：同時灌入 50 筆 `received_at = now()` 與 30 筆五分鐘前的 pending，
掃描只取回 30 筆——新鮮的那批留給攝入路徑自己處理。

**批次發佈的效益比預期小得多**：實測 `.delay()` 2,332 msg/s、共用 producer
2,563 msg/s，只快 **1.1 倍**。Celery 的 `.delay()` 本來就會重用 producer pool，
取用成本遠低於原先估計。這項改動保留（幾乎零成本），但它**不是**本節效益的來源——
真正解決問題的是分頁、單輪上限與重疊保護。

---

## 6. 已知邊界與刻意先不做

| 項目 | 為什麼 | 觸發點 |
|---|---|---|
| **佇列分流**（ingest / replay 各一條 queue） | 目前量級下單一 queue 夠用，路由接縫（task 名稱已固定）留著隨時可切 | replay 大量灌入開始擠壓線上攝入延遲時 |
| **Redis appendonly / 叢集** | 掃描已是兜底，見 §2 決策表 | broker 從「可選」變成「唯一真相」時（例如未來拿掉 DB 層 pending 語意） |
| **Celery 層 retry / 死信佇列** | 失敗語意已完整落在 `raw.status`（`error` 是終態，帶 `error_message`） | 需要區分「業務失敗」與「基礎設施失敗」並分別重試時 |
| **Flower 等監控 UI** | 沒有 result backend，Flower 能看的東西有限；`raw.status` 才是真相 | 接 OpenTelemetry 時一併評估（藍圖 Phase 5 獨立項）|
| **broker 掛掉時回壓上游** | 現在是「收下但延遲」，語意正確且上游不需要改 | 積壓到 DB 寫入本身成為瓶頸時 |
| **限流儲存重探的 single-flight** | slowapi 的 `_storage.check()` 沒有單飛保護，Redis 停機期間同一行程的併發請求會一起撞那次探測（實測 3.75s）。這是 §5.6 那條長尾的唯一成因 | 降級延遲的長尾成為實際問題時；最省力的第一步是把限流儲存的 `socket_connect_timeout` 從 1s 再調小 |

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
