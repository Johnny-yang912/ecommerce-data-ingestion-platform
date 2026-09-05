# 2026-09-02 — 攝入路徑容量與瓶頸定位

[English](../../en/verification/2026-09-02-ingestion-capacity-and-bottlenecks.md) | **繁體中文**

---

## 驗證的假設

`POST /orders → Raw → ODS` 這段的**時間花在哪、容量上限在哪、超載時會怎樣**。四個問題：

1. 單筆請求約 8ms，這 8ms 的組成是什麼？資料庫佔多少？
2. 連線池大小（15 → 100）影響吞吐嗎？
3. uvicorn worker 數（1 → 8）如何影響吞吐？
4. 持續超載時，系統是排隊還是崩潰？

## 環境

| 項目 | 設定 |
|---|---|
| 主機 | WSL2，16 核。壓測客戶端與服務同機 |
| 受測 API | 與 compose 同一個 `api-api:latest`，以 `docker run` 起在 8001 埠 |
| 參數覆寫 | 全部走既有環境變數：`RATELIMIT_ENABLED` / `POOL_SIZE` / `MAX_OVERFLOW` / `UVICORN_WORKERS` / `OTEL_ENABLED` |
| Worker | `api-worker:latest`，`--concurrency=4`、`POOL_SIZE=2`、`MAX_OVERFLOW=2`（與 compose 一致）|
| 保留服務 | 僅 `db` / `redis`。api / worker / beat / otel-collector 與整組 Airflow overlay 全部停掉 |
| 觀測 | 本機 Jaeger（OTLP 直送，不經 Grafana Cloud）＋ py-spy（sidecar 容器共享 PID namespace）|

⚠️ **限流全程關閉**（`RATELIMIT_ENABLED=false`）。本文量的是**容量**，不是線上政策——線上是 `60/minute`。

⚠️ **零程式碼改動。** `RATELIMIT_ENABLED` 是 slowapi 自己的環境變數契約（`Limiter.__init__` 尾端 `self.enabled = self.get_app_config("RATELIMIT_ENABLED", ...)`，經 starlette `Config`，os.environ 優先於 `.env`）。

## 方法

1. **隔離**：`docker compose stop api beat worker otel-collector` 加上整組 Airflow overlay，只保留 `db` / `redis`，消除鄰居噪音與連線競爭。
2. **受測服務**：以 `docker run` 起 `api-api:latest`（8001 埠）與 `api-worker:latest`，全部參數由 `-e` 注入。每換一組參數就 `docker rm -f` 重起——環境變數的改變必須 recreate，`restart` 不會重讀。
3. **負載**：吞吐量用既有的 `scripts/load_test.py`。另加兩支輔助腳本繞開〈測試 0〉的兩個量測陷阱：一支序列化探針（無訊號量、無 `gather`）、一支多行程注入器（`order_id` 平移避免撞號）。兩者都沿用 `load_test.make_payload`，資料形狀完全一致。
4. **Trace**：`OTEL_EXPORTER_OTLP_ENDPOINT` 指向本機 Jaeger 容器而非 compose 的 collector，全程不出網。span 由 Jaeger HTTP API 取出後，只計 server span 的**直接子 span**（避免與孫層重複計算），殘差 = server span − 直接子 span 總和。
5. **Profile**：py-spy sidecar 容器以 `--pid=container:api-lt --cap-add SYS_PTRACE` 共享 PID namespace，`record -f raw --full-filenames` 錄 30 秒 on-CPU 樣本（不開 `--idle`，因為阻塞等待已由 span 量過）。
6. **積壓觀測**：每 1–2 秒查一次 `raw` 的 `pending + processing` 與 `processed` 筆數；連線數則每 0.1 秒取樣 `pg_stat_activity`。
7. **清理**：每輪之間 `DELETE FROM ods WHERE order_id LIKE 'LOAD-%'` 再 `DELETE FROM raw ...`（`ods.raw_id` 對 `raw.id` 有 FK，順序不可反）。

## 觀測

### 測試 0 — 量測工具本身的兩個陷阱 ⭐

**必須先講，因為它作廢了本次前兩組數字，也作廢了先前對延遲的認知。**

#### 陷阱 1：`scripts/load_test.py` 報的不是延遲，是排隊

`scripts/load_test.py:126` 的 `t0 = time.perf_counter()` 在 `async with sem` **之外**。`asyncio.gather` 一次建立全部 N 個 coroutine，每一筆的碼表都在 t≈0 起跑，然後才去排訊號量。它報的是**訊號量排隊 + 真實往返**。

決定性證據：C=1、n=200 時 p50 = 0.909s，而總耗時 1.76s——**p50 恰為總時長的一半**，那是「N 人依序排隊，中位數那人等了一半」的數學特徵，與伺服器無關。

| 量法 | p50 |
|---|---|
| `scripts/load_test.py`，C=1，n=200 | **909 ms** |
| 序列化探針（無訊號量、無 gather），n=3500 | **8.34 ms** |

差了 109 倍。

#### 陷阱 2：單一客戶端行程在 ~150 RPS 就飽和

`scripts/load_test.py` 每筆都要 `random.Random(i)` 重新播種、生一份巢狀 payload、編碼 JSON。單行程吃滿一顆核心。

| 客戶端配置 | 總 RPS |
|---|---|
| 1 個行程 | 149.8 |
| 2 個行程並行（2000 筆 / 6.68s）| **299.4** |

**壓力產生器比被壓的服務先撞牆。** 本文所有 sweep 因此改用 4 個客戶端行程。

---

### 測試 1 — 單筆延遲拆解

#### 1a. Span 拆解（Jaeger，355 筆暖 trace，OTel on）

| 區段 | p50 | 佔比 |
|---|---:|---:|
| **殘差（框架自身，見 1b）** | **6.48 ms** | **80.5%** |
| 派工 celery publish → Redis | 0.65 ms | 8.0% |
| **`INSERT INTO raw`** | **0.38 ms** | 4.8% |
| `db.refresh()` 的 SELECT | 0.32 ms | 4.0% |
| 限流計數器 Redis EVALSHA | 0.20 ms | 2.4% |
| DB 連線 checkout ×2 | 0.09 ms | 1.1% |
| **server span 總計** | **8.05 ms** | 100% |

**資料庫（INSERT + SELECT + checkout）合計 0.79 ms，不到一成。**

#### 1b. 殘差的 CPU 組成（py-spy，OTel off，3,181 樣本）

| 階段 | 佔 CPU |
|---|---:|
| **SQLAlchemy** | **40.3%** |
| asyncio event loop | 13.6% |
| uvicorn / HTTP 解析 | 7.6% |
| redis 客戶端 | 7.0% |
| `to_thread` 執行緒池 | 6.4% |
| Starlette middleware / ASGI | 5.6% |
| kombu / celery 派工序列化 | 5.2% |
| FastAPI 依賴解析 / 路由 | 3.5% |
| stdlib logging | 3.4% |
| structlog | 2.8% |
| JSON 編解碼 | 2.3% |
| **pydantic 驗證** | **1.1%** |
| handler 本體 `main.create_order` | 1.0% |

SQLAlchemy 那 40.3% 再拆：

| | 佔 SQLAlchemy |
|---|---:|
| 驅動層（`do_execute` / `do_commit` / `do_rollback`）| 25.8% |
| **ORM/Core 機制**（session、unit of work、cache key、identity map）| **74.2%** |

**⭐ 「準備跟資料庫講話」與「整理講完的話」的成本，是講話本身的三倍。**

兩個附帶發現：

- **每筆請求都有一次 `do_rollback`**（佔 SQLAlchemy 樣本 4.7%）。來源是 `db.refresh()` 開啟的交易未被 commit，於 `db.close()` 時回滾。
- **pydantic 只佔 1.1%。** 該 payload 有 customer / address / items[] / payment / behavior 五層巢狀，直覺上應為主要成本——實測可忽略。

#### 1c. OpenTelemetry 的成本

| | 無 OTel | 有 OTel | 差 |
|---|---:|---:|---:|
| wall clock p50 | 8.34 ms | 9.87 ms | **+1.53 ms（+18%）** |
| on-CPU 樣本（30 秒）| 3,181 | 4,986 | **+57%** |

---

### 測試 2 — 連線池 sweep（15 → 100）

workers=4、4 客戶端、總併發 52、n=2000、各跑兩輪：

| pool（size+overflow）| 第 1 輪 RPS | 第 2 輪 RPS | pg 連線峰值 |
|---|---:|---:|---:|
| 1（1+0）| 312.1 | 285.3 | 11 |
| 2（2+0）| 239.9 | 252.9 | 11 |
| 5（5+0）| 179.8 | 234.1 | 12 |
| 15（5+10）| 179.5 | 203.2 | 11 |
| 40（30+10）| 247.6 | 222.0 | 11 |
| 80（60+20）| 145.2 | 266.5 | 12 |
| 100（80+20）| 263.4 | 262.0 | 10 |

**RPS 無趨勢，同組態兩輪差達 ±40%——主機雜訊遠大於任何 pool 效應。**

**決定性證據不是 RPS，是連線峰值：所有組態都是 10–12**，包含 `POOL_SIZE=80 / MAX_OVERFLOW=20 × 4 workers`（理論上限 400 條）。`pool=1+0`（上限 4 條）同樣是 11。

**機制**：`create_order` 是 `async def`，但 `db.commit()` 與 `db.refresh()` 是同步 psycopg2 呼叫，兩者之間**沒有任何 `await`**。連線持有窗口不會讓出 event loop → **每個 uvicorn 行程同一時間最多持有 1 條連線**。

⇒ compose 目前給 API 編列 32 條連線預算（`4 × (3+5)`），實測僅用到約 4 條。

---

### 測試 3 — uvicorn worker sweep

4 客戶端、總併發 52、n=2000、pool=5+10：

| workers | 總 RPS | 相對 |
|---:|---:|---:|
| 1 | 130.8 | 1.00× |
| 2 | 201.0 | 1.54× |
| 4 | **298.1** | **2.28×** |
| 8 | 207.1 | 1.58×（**反轉**）|

1→4 接近線性，這與測試 2 的機制一致：**既然每個行程同時只能有一條 DB 作業，增加吞吐的唯一方法就是增加行程。**

8 個反轉：16 核主機上同時還有 4 個壓測客戶端、Celery worker、PostgreSQL、Redis 在競爭，行程數超過核心能餵飽的量，切換成本開始吃掉收益。**compose 現行的 `UVICORN_WORKERS=4` 是這條曲線的最高點。**

---

### 測試 4 — C=500 複驗（[2026-08-03](./2026-08-03-load-test-ingestion.md) 測試 2）

| 情境 | 結果 | pg 連線峰值 |
|---|---|---:|
| **忠實複刻**：單客戶端 C=500、n=1000、workers=4、pool=5+10 | **1000 / 1000 成功，0 失敗** | 12 |
| 同上但 workers=1 | 996 成功 + 4 `RemoteProtocolError` | 9 |
| 4 客戶端、總併發 500、n=4000 | 3999 + 1 `RemoteProtocolError` | 12 |
| 4 客戶端、總併發 1000、n=4000 | 3979 + 21 `RemoteProtocolError` | 12 |
| 單客戶端 C=1000、n=1000（9.30 秒跑完）| **1000 / 1000 成功，伺服器 log 零警告** | 11 |

**零個 HTTP 500、零個 503。連線池從未耗盡。**

那些 `RemoteProtocolError` 是 **httpx 客戶端側的錯誤，不是伺服器退件**：只出現在總時長超過約 15 秒的輪次，而 uvicorn 的 `timeout_keep_alive` 預設為 5 秒。閒置逾時被伺服器關閉的 keep-alive 連線，被 httpx 拿去重用即拋此例外。9.30 秒內跑完的 C=1000 那輪完全沒有。

---

### 測試 5 — 持續負載與背壓 ⭐

4 個客戶端行程（`order_id` 平移避免撞號）、各 15,000 筆、總計 60,000 筆：

```
t=20s   積壓 1,256   已處理  4,778
t=41s   積壓 2,626   已處理 10,163    ← 進 > 出，積壓線性成長
t=61s   積壓 3,831   已處理 15,021
t=80s   積壓 5,183   已處理 19,852    ← 峰值 5,453
t=121s  積壓 1,125   已處理 31,029    ← 一個客戶端跑完，注入降速，開始回收
t=140s  積壓     2   已處理 35,275    ← 追平
t=200s  積壓     2   已處理 45,315
t=280s  積壓     1   已處理 58,138
注入結束後仍需消化：0 秒
```

| 指標 | 值 | 推導 |
|---|---:|---|
| 注入總量 | 60,000 筆 / 292 秒 | 全數 HTTP 200 |
| **API 收單能力** | **約 313 筆/秒** | 前 80 秒：(19,852 已處理 + 5,183 積壓) ÷ 80s |
| **Worker 消化能力** | **約 270 筆/秒** | t=80→121 飽和期：11,177 筆 ÷ 41s |
| 積壓峰值 | 5,453 筆 | t≈80s |
| 積壓回收時間 | 約 55 秒 | 5,453 → 2 |
| ODS 落地 | 60,000 / 60,000 | 零遺失 |

**超載期間（前 85 秒，進 313 > 出 270）發生了什麼：什麼都沒發生。** 無錯誤、無 503、無逾時、無掉單。差額每秒 43 筆堆進佇列，積壓**線性**成長。負載回落後 55 秒完全回收。

測試期間 ODS 由 17,380 筆成長至 77,380 筆，**未觀察到寫入效能衰退**。

---

## 結論

### 一、時間都在框架層，不在資料庫

單筆 8.05 ms 中，資料庫（INSERT + refresh SELECT + 連線 checkout）合計 0.79 ms，**不到一成**。八成是 Python 框架與 ORM 開銷，其中最大宗是 SQLAlchemy 的 ORM 簿記，約佔總 CPU 的 30%——是實際 SQL 執行的三倍。

直覺上最可疑的 pydantic 巢狀驗證只有 1.1%。**優化方向若從「資料庫」或「序列化」下手，都會落空。**

### 二、連線池不是可調參數　⚠️ 已被推翻

> **⚠️ 本節已於同日被 [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) 推翻。**
> 原文保留於下，因為它記錄了「在那個當下量到什麼」；但**下面那個可執行建議不可照做**——
> 三個端點改成 `def` 之後，32 條在負載下實測峰值 29–34（幾乎用滿），砍到 8 會造成 `pool_timeout` 逾時與 503。

`async def` 內的同步 DB 呼叫使每行程同時只持有 1 條連線；pool 從 1 到 100，對吞吐與實際連線數皆無可觀測影響（連線峰值恆為 10–12）。

可執行結論：**API 的連線預算可由 32 條（`4 × (3+5)`）縮減至 8 條**。`max_connections` 只有 100，回收的額度是 Airflow 與人工連線的空間。

### 三、擴展的唯一旋鈕是 uvicorn worker 數　⚠️ 已被推翻

> **⚠️ 本節已於同日被 [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) 推翻。**
> 原文保留於下。端點改 `def` 之後曲線不再於 8 反轉（207 → 485 RPS），
> `UVICORN_WORKERS=4` 因此從「曲線最高點」變成「保守的選擇」。

1→4 接近線性（130.8 → 298.1 RPS），8 反轉。現行 `UVICORN_WORKERS=4` 位於曲線最高點。這是第二點的直接推論：既然每行程只能有一條 DB 作業在飛，增加吞吐就只能增加行程。

### 四、容量天花板在 worker，不在 API

本專案的攝入路徑是**「API 快速收下 → Celery 佇列 → worker 消化」**。因此「系統能承受多少」這個問題的答案是 **worker 的數字（約 270 筆/秒），不是 API 的數字（約 313 筆/秒）**。

這個差距是健康的：**landing 層應該收得比處理得快**，突發流量才會被收下而非被拒絕。

測試 5 直接觀測到超載時的行為：注入 313 > 消化 270 時，差額每秒 43 筆堆進佇列，積壓**線性成長**至 5,453 筆，期間無任何錯誤、退件或掉單；負載回落後 55 秒完全回收。**佇列的作用是把上游速度與下游速度解耦——積壓不是故障，是設計在運作。**

⚠️ 但「線性成長」同時意味著**持續超載時積壓不會自行收斂**。這正是 `raw_pending_watch` 存在的理由：它量的是「已落到 Raw 但沒有任何 worker 取走」的筆數，而本次測試給了那道門檻一個實測的參照基準——健康時積壓穩定在個位數，超載時以每秒數十筆線性爬升。

⭐ **後續補充（同日稍晚，非推翻）**：本節的 313 / 270 兩個數字已被 [sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) 取代，但「天花板在 worker」這個結論本身成立，而 [worker-scale-out](./2026-09-02-worker-scale-out.md) 補上了本節缺的那一半——**那個天花板能不能買**。零 API 負載下 4／8／16 個 worker 子行程的消化速率為 304／515／789 筆/秒（次線性，每次加倍 1.69×、1.53×，曲線在 16 仍未平），瓶頸為單機 CPU 超額訂閱而非資料庫併發。**天花板在 worker，而那個天花板是可以用子行程買的。** 同時，本節的積壓數字（5,453、55 秒）與該文的一樣，只在 worker 組態被一起引用時才有意義。

### 五、Python 是成本，不是天花板

八成的時間花在 Python 層（框架 + ORM）。換成編譯語言可省下的是這 7.26 ms 的大部分——**但 0.79 ms 的資料庫時間省不掉，那是 PostgreSQL 在做事**。粗略地說，使用 Python + SQLAlchemy 的代價是數倍的 CPU。

關鍵在於**這個代價是線性可買的**：測試 3 證明加行程即線性成長。真正會讓系統撐不住的，是那種**加機器也沒用**的瓶頸——單點寫入上限、全域鎖、無法水平擴展的行程內狀態。這些本專案都沒有：

- API 行程無狀態（恢復掃描已於 `cf81d29` 移至 Celery Beat）
- 認領靠資料庫列鎖（`try_claim_raw` 的 `rowcount == 1`）
- 佇列在 Redis，worker 可水平擴展

**結論：Python 影響的是硬體帳單，不是架構的擴展性。在本專案的量級，它不是需要解決的問題。**

### 六、「能處理高併發」這句話拆開來看

「高併發」實為四個不同面向的合稱。本文各測了一組，觀測值如下：

| 面向 | 白話 | 測試結果 |
|---|---|---|
| 併發連線數 | 同時很多連線會不會爆 | 1000 個併發連線全數回 200，連線池峰值 11，伺服器 log 無警告。**未測更高併發** |
| 吞吐量 | 每秒能吃下幾筆 | API 約 313 筆/秒、worker 約 270 筆/秒。**此為壓測客戶端與全部服務共用同 16 核時的觀測值** |
| 持續力 | 這個速度跑久了會不會衰退 | 連續 292 秒注入 60,000 筆，速率無明顯衰退，注入結束時積壓為 0。**未測超過 5 分鐘** |
| 背壓 | 超過負荷時是變慢還是崩潰 | 超載期間積壓線性成長至 5,453 筆，全程零錯誤；負載回落後 55 秒完全回收。**未測持續超載數十分鐘以上的行為** |

⚠️ **上表是「在本文環境下量到的觀測值」，不是系統能力的上界，也不是任何形式的保證。** 四項都只取樣了一個工作點，沒有一項被推到失效為止——所以本文能說的是「測到這裡都正常」，不能說「上限就是這裡」。真正的上限只能由這些觀測值往上推估（見第八點），而該推估未經驗證。

**可陳述的容量描述（必須連同環境一起引用）：**

> 於單機開發環境（16 核，DB / Redis / worker / 壓測客戶端同機，限流關閉）實測：攝入 API 收單約每秒 313 筆，Celery worker 消化約每秒 270 筆。超過消化能力時，超額由佇列吸收，積壓線性成長且無錯誤；負載回落後 55 秒內完全消化。連續 292 秒注入 60,000 筆，全數落地 ODS，零失敗。

**不應使用的陳述是「本系統能處理高併發」**——該句沒有邊界，會把上表四項混為一談，且省略了「這些數字產生於什麼環境」這個決定其意義的前提。

至於「合格與否」：本專案尚未定義 SLO，嚴格說沒有判準。可說的是——以**內部訂單攝入服務**的常識量級衡量，270 筆/秒約等於每日 2,300 萬筆，對此場景不構成瓶頸；且超載時的行為是排隊而非崩潰。**後者比容量數字更值得記錄，因為多數系統的問題不是容量不足，而是超過容量即失效。**

### 七、這份文件與本次測試不代表什麼

本節與第六點的觀測值同等重要，因為引用數字的人不會自動繼承產生數字時的前提。

- **不代表生產環境效能。** 全程單機、無真實網路延遲、資料量僅到 77,380 筆。`ods.order_id` 有 unique index，資料到千萬級時寫入會變慢，270 這個數字屆時需下修。
- **不代表容量上界。** 見第六點的 ⚠️。四個面向都只取樣一個工作點，沒有任何一項被推到失效。
- **不代表限流政策下的行為。** 測試全程 `RATELIMIT_ENABLED=false`；線上是 `60/minute`，即每個上游每秒 1 筆。**能力 270，政策鎖在 1。** 兩者不衝突，但不可混談——若未來需放寬限額，本文的意義是「瓶頸不會在 API」。
- **不代表故障下的行為。** 本次全程 DB、Redis、worker 均健康，未做任何故障注入。斷線、broker 中斷、worker 崩潰等路徑由 [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) 與 [2026-08-10-circuit-breaker-before-after](./2026-08-10-circuit-breaker-before-after.md) 涵蓋，**不在本文範圍**。
- **不代表長時間穩定性。** 最長單次負載 292 秒。記憶體洩漏、連線洩漏、佇列碎片化都需數小時才會浮現。
- **不是 SLA、不是承諾。** 這是一次量測記錄，有效期由被量測的那段程式碼的壽命決定——本文推翻 2026-08-03 的方式，正是它自己將來會被推翻的方式。

### 八、換到企業級硬體的上限推估（未經驗證）⚠️

**以下全部是推估，沒有任何一項被實測過。** 記錄它的目的是提供一個推理起點與檢查清單，不是提供數字。

本次的量測環境有一個結構性污染：**16 核上同時跑著 PostgreSQL、Redis、Celery worker、4 個 uvicorn 行程，以及 4 個壓測客戶端行程**——而壓測客戶端本身就吃掉約 4 顆核（見測試 0 陷阱 2）。也就是說，**被測系統從來沒有拿到整台機器**。

若要往上擴展，三個軸可分別推理：

| 軸 | 是否可線性擴展 | 依據 |
|---|---|---|
| **移走壓測客戶端** | — | 不是擴展，是消除量測污染。方向確定向上，幅度未知 |
| **API 水平擴展** | 可 | API 行程無狀態（恢復掃描已移至 Beat）。測試 3 在 1→4 觀測到接近線性；8 反轉是**核心數不足**而非架構限制，換更多核心後該反轉點會右移 |
| **Worker 水平擴展** | ⭐ **可——已實測（次線性）** | ⚠️ **本欄原寫「`try_claim_raw` 的 CAS（`rowcount == 1`）保證同一 `raw_id` 只會被一個 worker 取走」，該歸屬已修正。** [worker-scale-out](./2026-09-02-worker-scale-out.md) 實測 4→16 子行程得 2.60×：真正讓加開 worker 不需協調的是**每筆一則指名 `raw_id` 的點對點派工**，CAS 是重複派工發生時的兜底（測試 F：15,000／15,000）。**本表三軸中唯一已被實測的一軸** |

**下一個瓶頸幾乎確定會轉移到 PostgreSQL。** 每筆訂單在資料庫上是約四次寫入：

1. `INSERT INTO raw`（API）
2. claim 的 `UPDATE raw SET status='processing'`（worker）
3. `INSERT INTO ods`（worker）
4. `UPDATE raw SET status='processed'`（worker）

270 筆/秒 ≈ 每秒約 1,080 次寫入。單一 PostgreSQL primary 在專用硬體（NVMe、足夠 shared_buffers）上處理簡單寫入的常見量級是每秒數千至上萬次——**換算成訂單，量級落在每秒數百到一兩千筆**。

⭐ **一個實測參照點（仍不足以驗證上段推估）**：[worker-scale-out](./2026-09-02-worker-scale-out.md) 在 16 個 worker 子行程下量到 789 筆/秒 ≈ 每秒約 3,156 次寫入，而 PostgreSQL 未顯示任何**併發**瓶頸的跡象（連線數與子行程數精確線性、零鎖等待、零寫入衝突、66 萬筆零錯誤）——當時的限制是單機 CPU。**也就是說本段推估的區間在 789 筆/秒這一點上尚未被觸及**；但那仍是同一台機器、同一份約 1.7 萬筆的基底資料，不能當成上段推估被驗證。

⚠️ **這個區間有多不可靠，必須講清楚：**

- 它假設寫入成本不隨資料量成長，但 `ods.order_id` 的 unique index 維護會隨表變大而變貴，且 `raw.raw_payload` 是 TEXT，大 payload 會觸發 TOAST
- 它沒有計入 autovacuum 的負擔——四次寫入裡有兩次是 `UPDATE`，在 PostgreSQL 是產生死列的操作
- 它假設 fsync 成本可忽略，但那完全取決於磁碟
- 它完全沒有考慮真實網路延遲、連線建立成本、以及多可用區部署的往返

**再往上就不是加機器能解決的**，需要改變寫入路徑本身：批次寫入（`COPY`）、把 `raw` 與 `ods` 拆到不同實例、或對 `raw` 做分區/歸檔以控制索引大小。**那是另一個層級的設計題，而本專案目前的量級離它很遠。**

**要把上面任何一個數字變成事實，需要的最小實驗是：把 DB 獨立部署、壓測客戶端移到另一台機器、把資料灌到千萬級，然後重跑測試 5 並觀察積壓曲線的斜率何時開始惡化。** 在那之前，第六點的觀測值是本文唯一有數據支撐的部分。

---

## 這推翻了什麼

⚠️ **先說本文自己**：結論二與三已於同日被 [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) 推翻——
那份記錄的是「把本文指出的 `async def` + 阻塞 DB 這個缺陷修掉之後」的量測。結論一、四、五、六仍然成立。

**另外兩處是被補上而非被推翻**：結論四的「天花板在 worker」與結論八推估表的 worker 軸，已由 [2026-09-02-worker-scale-out](./2026-09-02-worker-scale-out.md) 補上實測——前者補上倍率（4× 子行程 → 2.60×），後者是該表三軸中第一個離開「未經驗證」狀態的一軸（並修正了其依據欄的歸屬）。

**[2026-08-03](./2026-08-03-load-test-ingestion.md) 的測試 2 已被推翻。** 該次記錄 C=500 時有 5 筆 HTTP 500，歸因於連線池耗盡；本次以相同參數複跑，1000 筆全過、連線池峰值 12。

**推翻它的不是量測誤差，是一次架構重構。** 當時 `POST /orders` 使用 FastAPI `BackgroundTasks`，而 `process_raw_event` 是同步函式——Starlette 會將其投入預設 40 條的 anyio threadpool。也就是最多 40 個「完整的 Raw→ODS 清洗與寫入」同時執行，**且共用 API 那個僅 15 條的連線池**；`db.close()` 亦在 `finally`，`refresh` 開啟的交易全程掛著。

`8485f64`（2026-08-10，派工改為 Celery）之後，API 行程僅剩一次 INSERT，且連線在派工前明確歸還。**壓力源整個消失。**

⭐ 這與測試 5（SIGKILL）被 [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md) 推翻是同一類事件：**驗證文件的有效期，由被驗證的那段程式碼的壽命決定。**

## 相關

- [2026-09-02-sync-handlers-before-after](./2026-09-02-sync-handlers-before-after.md) — 推翻本文結論二與三
- [2026-09-02-worker-scale-out](./2026-09-02-worker-scale-out.md) — 為本文結論四補上倍率，並實測結論八推估表的 worker 軸
- [2026-08-03-load-test-ingestion](./2026-08-03-load-test-ingestion.md) — 本文推翻其測試 2
- [design/queue](../design/queue.md) — CAS claim 與重新投遞的交互作用
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0005](../adr/0005-first-write-wins-idempotency.md)
