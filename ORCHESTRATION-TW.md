# 編排層架構：Airflow

## 範圍與職責邊界

本文件記錄「編排層」的設計決策——即**什麼時候跑、跑完接什麼、失敗了怎麼辦**。
各層自己的正確性契約不在這裡：品質契約見 [DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md)，
E/L 與 staging 基建見 [CLOUD_LAYER-TW](./CLOUD_LAYER-TW.md)，轉換層見
[ecommerce_dbt/README.zh-TW](./ecommerce_dbt/README.zh-TW.md)。

```
ODS (PostgreSQL) ──[E/L]──► BQ staging ──[T：dbt]──► stg_/int_/dim_/fct_/rpt_
                     ▲                        ▲
                     └──────── Airflow ───────┘   ← 你在這裡
```

**Airflow 不是任務佇列**。「Airflow」與「Celery + Redis」是兩個**正交**的項目
（後者已實作，見 [QUEUE-TW.md](./QUEUE-TW.md)），
混淆會讓整個設計走歪：

| | Airflow | Celery + Redis |
|---|---|---|
| 取代什麼 | 過去手動跑的 `extract_ods_to_bq.py` + `dbt build` | `BackgroundTasks`（`process_raw_event`）|
| 時間尺度 | 分鐘～小時級批次 | 毫秒～秒級、單筆 |
| 觸發源 | 時鐘 / 人 | HTTP 請求路徑 |
| 失敗語意 | 重跑整段 SQL、從 watermark 續傳 | 單筆 requeue |

推論：**`process_raw_event` 永遠不該進 Airflow**。用排程器當任務佇列會撞上 DAG 解析間隔
（秒級延遲）與 task 啟動開銷，並破壞 `POST /orders` 的即時語意。兩者唯一共用的只有
「Redis」這個字，而且**不該共用同一個實例**（故障域會糾纏）。

### ⚠️ 這是作品，資料來源是模擬的

本專案沒有、也不會有真實上游。`seed_demo_daily` 每天分四批打進 `POST /orders`，
**它模擬的是「每天都有資料」，不是「真實的攝入行為」**。

差別不在資料內容（那些走的是真實攝入路徑、真實品質規則），而在**時間分佈**：
真實系統 24 小時連續進單且有尖離峰，本專案是一天四個離散時點、每點等速率。

這個落差有具體後果，不是免責聲明：

| 因為模擬而「剛好成立」的設計 | 真實流量下會怎樣 |
|---|---|
| 所有 seeding 時段落在同一個 UTC 日分區 | 24 小時進單無法迴避 UTC 日界，台北 00:00–08:00 的資料會落到前一天（見 §2.11） |
| Hard Gate 以「最新 UTC 日分區」代理「最近一批」 | 連續攝入下退化成「今天到目前為止」，稀釋問題在一天內重演 |
| freshness 不需要阻斷權（沒資料＝沒灌，無害） | 上游停送是事故，freshness 應恢復為 gate |
| 26h/50h 的 freshness 閾值 | 一天四批下餘裕過大，偵測不到「峰期停了三小時」 |

**但驗證程度要分兩條路徑講，不可一概而論**：

- **攝入路徑**（API → Redis → worker → ODS）：**爆量行為已經量過**，是本專案證據最完整的一段。見 [QUEUE-TW §5](./QUEUE-TW.md)——多行程限流、broker 停機下的降級與熔斷（200 併發）、有界恢復掃描（6 萬筆積壓單輪清完；12 萬筆游標續傳兩輪、`duplicate` 0 筆）。**這段不是靠 seeding 驗證的。**
- **分析路徑**（extract → BQ → dbt）：**從未在小量以外的資料上跑過**。`stg_` 回看窗與 `insert_overwrite` 的實際成本、Hard Gate 靈敏度隨資料成長的變化、BQ 的儲存與查詢成本，都未被觀察。

兩邊都沒涵蓋的還有兩件：**持續性**（壓測是一次性爆量，不是連續數週的日常負載，累積效應未知）與**峰期形狀**（瞬間併發峰值 ≠ 持續數小時的高檔，對連線池週轉是不同壓力）。

**寫下來的理由**：這些設計在目前的條件下都是對的，但它們的正確性依賴一個
不會出現在程式碼裡的前提。不寫的話，下一個接手的人（包括未來的我）會以為
這條管線已經面對過連續流量——**而它的所有測試都會支持那個誤解**。

---

## 1. DAG 拓撲

```
【orders_analytics_daily】  @daily, catchup=False, max_active_runs=1

  extract_orders ─────────┐
                          ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test_all
  extract_quality_events ─┘      (Hard Gate)                                                 (completeness)

【dq_reevaluation】  schedule=None（手動觸發）

  reevaluate ─► should_refresh（commit 才通過）─► trigger orders_analytics_daily

【source_freshness_watch】  @daily

  dbt_source_freshness      ← 獨立成一條 DAG 的理由見 §2.7
```

檔案位置：`orchestration/dags/`。

> **為什麼目錄叫 `orchestration/` 而不是 `airflow/`**：`pytest.ini` 設了 `pythonpath = .`，
> repo 根目錄下的 `airflow/` 會變成 namespace package 並**遮蔽真正的 Airflow 套件**——
> 本機 `import airflow.models` 直接失敗，CI 裡就算裝了真的 Airflow 也一樣被遮掉。
> 這是實測踩到的，不是預防性命名。

---

## 2. 決策記錄

### 2.1 依賴隔離：兩個 venv，不裝進 Airflow 本體 ⭐

Airflow 與 dbt-bigquery 都重度依賴 `google-cloud-*` / `protobuf` / `jinja2`，同環境安裝是
典型的版本衝突來源，且每次升 Airflow 都可能炸掉 dbt。

| 選項 | 取捨 |
|---|---|
| 同環境 `pip install` | 最簡；衝突風險高 |
| **獨立 venv + BashOperator（選用）** | 隔離乾淨、零額外基建、與官方建議一致 |
| DockerOperator 跑獨立映像 | 最乾淨、prod-parity 最佳；但要掛 `docker.sock` |

映像內：`/home/airflow/venvs/analytics`（`requirements-analytics.txt`）與
`/home/airflow/venvs/dbt`（dbt-core / dbt-bigquery 1.11）。這也是 `requirements-analytics.txt`
存在的理由——Airflow 容器要「跑得動抽取腳本」但不該裝 pytest。

**不用 Cosmos**：模型級 task 的可觀測性收益，對 13 個 model 的專案不成比例，代價是一個
會跟著 dbt/Airflow 版本走的相依。

### 2.2 DAG 檔不得 top-level import 專案模組 ⭐

`config.py` 在 import 當下就實例化 `Settings` 且 `db_url` 必填。DAG 檔會被 dag-processor
每隔數十秒重新解析一次——top-level import 專案模組的話，解析行程只要缺 `DB_URL`，
結果**不是「task 失敗」而是 DAG import error，整條 DAG 從 UI 消失**。沒有紅燈可看，
比失敗更危險。

故一律用 `BashOperator` 把 import 推到 task 執行期。附帶收益有兩個：

1. `tests/test_dags.py` 能在**不連 DB、不設任何專案環境變數**的情況下用 DagBag 解析成功，
   因此 DAG 有 CI 保護；
2. Airflow 3 把 DAG 解析拆成獨立的 dag-processor 行程，它的環境與 task 執行環境本就分開——
   這條紀律在 3.x 上更重要。

### 2.3 extract 拆成一表一 task

單一 task 呼叫 `main()` 也能跑，但會丟掉設計本來的價值：**各表 watermark 獨立、失敗不推進**
（[CLOUD_LAYER-TW §3.2](./CLOUD_LAYER-TW.md)）是自癒的來源，合成單一 task 會讓重試連帶
重跑已經成功的那張，也看不出是哪張壞了。

跨表 gate 因此從「腳本內彙整後 `raise`」搬到 **DAG 的依賴邊**（dbt 的上游＝兩個 extract
都 success）——語意完全相同，只是判斷點移了位置。腳本保留 `--table all` 的原行為，手動路徑不變。

### 2.4 dbt 分層執行，以及為什麼結尾還要再跑一次 `dbt test` ⭐

分層（staging → intermediate → marts → reports）讓 Hard Gate 的攔截點在 UI 上看得見，
也能從失敗層往下重跑。但逐層 `--select` 會踩到 dbt 的 **indirect selection** 語意，
而代價落在整個專案最重要的那幾支 singular test 上：

| 模式 | `assert_orders_split_is_partition` 的下場 |
|---|---|
| `eager`（預設） | 在 staging task 就被選中 → 對「`stg_` 剛重建、`int_` 還是舊表」的中間狀態斷言 → **誤紅** |
| `cautious` | 父節點沒有全部被選中 → **永遠不跑** |
| **`buildable`（選用）** | 父節點「被選中或是被選中節點的祖先」→ 落在 intermediate task，所有輸入都新鮮 → **正確** |

但這條規則細微，賭錯的代價是文件裡寫著「唯一的自動化安全網、永不降級」的測試被**無聲跳過**。
故結尾補一個完整 `dbt test`：**測試被靜默跳過，比多跑一次測試糟糕得多。**
兩者職責不同——逐層的測試是 **gate**（擋住下游建置），結尾那次是 **completeness**。

> ⚠️ **絕對不可以把 `dbt build` 拆成 `dbt run` 一個 task、`dbt test` 另一個。** 那會讓
> `int_` 的上游變成「staging 的 **run**」而非「staging 的 **test**」，
> Hard Gate（[DQ 機制一](./DQ_ARCHITECTURE-TW.md)）就此失效，髒資料照樣流進 Gold。
> 由 `tests/test_dags.py::test_dbt_never_splits_run_and_test` 把關。

### 2.5 排程語意：`catchup=False` 是結構性的 ⭐

本管線的 watermark 是 **destination-derived**（方案 A：從 staging 的 `MAX(partition_id)`
推導），不是 execution-date-derived。補跑 2026-07-01 的 backfill run，抽的仍然是
「**現在**的增量」，與 logical date 無關 → N 個 backfill run 做 N 次一模一樣的事，
還多產 N 個 load job。

> **這不是 date-partitioned 的可回填 DAG。** 要讓它真的可回填，得改成
> `received_at >= data_interval_start AND < data_interval_end` 的切片抽取（Airflow 慣用的
> 冪等形狀），但那個右邊界會切掉遲到列，與既有「`>=` 寧可重抓不漏抓」的語意直接衝突。
> 刻意不做，理由記在這裡而不是留給未來的人重新推導。

`max_active_runs=1` 是正確性而非禮貌：並行 run 的 `get_watermark` 會讀到同一個值（無害，
dbt `stg_` 去重吃掉），但 dbt 對同一批分區並行 `insert_overwrite` 會互相覆寫。

**日批而非小時批**：方案 A 的精度被 DAY 分區卡死，小時批每跑一次都重抽當天至今
（[CLOUD_LAYER-TW §2.2](./CLOUD_LAYER-TW.md) 的判準表）。要上小時批得先換 HOUR 分區或
方案 B——那是另一個獨立決策。

### 2.6 retry 刻意不對稱：extract=2、dbt=0

| Task | retries | 理由 |
|---|---|---|
| `extract_*` | 2（指數退避） | 失敗多為暫時性（PG 連線、BQ 5xx / rateLimitExceeded），與攝取層四層 retry 同一個哲學 |
| `dbt_*` | **0** | 失敗多為 deterministic（SQL 錯、測試紅、Hard Gate 擋下），retry 只是重跑一次注定失敗的東西 |

BQ 的暫時性錯誤由 `profiles.yml` 的 `job_retries: 1` 在 **adapter 層**處理，比 Airflow 層的
task retry 精準得多——後者會把整個 `dbt build` 重跑一次。

這個不對稱與 NUL byte poison-pill 的教訓是同一條原則：**把 deterministic 錯誤當成暫時性
而重試，就是在製造 poison-pill。** 當時的解法是加 `except ValueError` fast-fail，這裡的
對應解法是 `retries=0`。

### 2.7 freshness 獨立成一條 DAG ⭐

[CLOUD_LAYER-TW §1.7.7](./CLOUD_LAYER-TW.md) 早已立了硬規則：**不得當前置檢查**。
實作時發現該節建議的「旁路 task（失敗不影響下游）」**還不夠**：

> Airflow 的 DAG run 狀態是所有 task 的彙總。一個**預期會紅**的 leaf task 會讓
> `orders_analytics_daily` 恆為 failed → 「主管線成功率」這個訊號的價值歸零，
> 真正的管線故障被淹沒在每天都紅的噪音裡。

所以那條原則要再推一步：**freshness 不只沒有「阻斷下游」的權限，也沒有「污染別人成功率」
的權限。** 獨立後兩條 DAG 的紅各自代表一件事：

| DAG | 紅代表 |
|---|---|
| `orders_analytics_daily` | 管線壞了 |
| `source_freshness_watch` | 資料源不新鮮（目前的手動攝入下＝**預期狀態**）|

由 `tests/test_dags.py::TestFreshnessIsolation` 把關：任何有實際產出的 DAG 混進
`dbt source freshness`，測試就紅。

### 2.8 Proposal B 的 DAG：`schedule=None` 是設計，不是還沒設好 ⭐

Proposal B 的觸發條件是「**規則放寬了**」——那是人為的部署事件，不是週期。規則沒變時，
重評估必然與上次得到相同結果（同一批值、同一版規則）→ 產不出任何事件，卻要對全歷史
quarantine 做一次完整掃描。排成日批＝**364 天的白工換 1 天的效果**。

> 排程的正確對象是「資料會自己變的東西」；規則不會自己變。

三個配套：

- **預設 dry-run**，`commit` 必須顯式打開——`quality_events` 是 append-only，寫錯刪不掉，
  而手動觸發的 UI 很容易一路點下去。
- **`expect_rule_version` 防呆**：最常見的意外是「打在還沒部署新規則的環境上」，那會寫出
  一批標著錯版本號的事件，無法撤銷。
- **跑完觸發主 DAG，但只在 `commit` 時**：重評估只寫 PG 的 `quality_events`；要真的回流
  Gold 還需要 `extract_quality_events` 送上 BQ、`int_` 全量重建。少了這一步，狀態會是
  「我跑了 Proposal B 但什麼都沒發生」——最容易被誤判成程式壞掉的狀態。

參數一律「留空就不加旗標」，預設值**只存在於腳本裡**——DAG 與腳本各留一份預設值就是漂移的來源。

### 2.9 metadata DB 獨立於業務 DB

| 選項 | 取捨 |
|---|---|
| 與業務 `db` 共用實例（另開 database） | 省一個容器；但排程器的 metadata 與**資料錨點**綁在同一次備份／還原裡 |
| **獨立 `airflow-db`（選用）** | 多一個容器，換故障域與備份語意的清爽 |

理由不是潔癖：業務 DB 是 Proposal C「從 Raw 重建」的前提，不該讓一個運維元件的歷史
與它同生共死。要 restore 業務 DB 時，也不會想連 Airflow 的執行歷史一起回滾。

**Executor 用 LocalExecutor**：本機單機、task 數個位數，CeleryExecutor 只多兩個容器與一個
broker——而那個 Redis 會與攝入路徑的「Celery + Redis 取代 BackgroundTasks」在概念上打架
（後者已實作，見 [QUEUE-TW.md](./QUEUE-TW.md)；兩者刻意不共用實例，以免故障域糾纏）。

**不起 triggerer**：目前只用 `BashOperator`，沒有 deferrable operator。

### 2.10 `profiles.yml`：結構入版控、值留環境 ⭐

放在 `orchestration/dbt_profiles/`，由 `DBT_PROFILES_DIR` 明示指定。

> **刻意不放 `ecommerce_dbt/`**：dbt 找 `profiles.yml` 的順序把**當前工作目錄排在 `~/.dbt` 之前**，
> 放進 dbt 專案目錄會讓你本機 `cd ecommerce_dbt && dbt run` 突然改吃那一份、並因為沒設
> 環境變數而炸掉。放專用目錄則既有工作流完全不受影響。

⭐ **刻意重用 `config.py` 的同一組環境變數**（`BQ_PROJECT` / `BQ_DBT_DATASET` /
`GOOGLE_APPLICATION_CREDENTIALS`）。這不只是省事：`reevaluate_quality.py` 讀的 `int_orders`
就是 dbt 寫出來的那張表——兩邊若各自設定，會**安靜地指向不同 dataset**，重評估掃到一個
過期或不存在的表而不報錯。共用同一個變數讓這種分歧不可能發生。

### 2.11 ⚠️ 跨時區抽取：業務的「日」與分區的「日」不是同一個日 ⭐

排程以 `Asia/Taipei` 宣告（seeding 09/13/17/21、抽取 23:00），但 `received_at` 是 TIMESTAMP、
BQ 按 **UTC** 日分區——`date()` 在 UTC 換日，兩者差 8 小時。

**目前這個錯位看不出來**，因為 seeding 的四個時段都落在台北 08:00–24:00，恰好對應
UTC 00:00–16:00 的同一日。**但那是挑時段挑出來的結果，不是系統的性質**：真實系統
24 小時進單時，台北 00:00–08:00 的訂單會落進前一個 UTC 分區。

影響範圍剛好沿著一條有意義的線切開：

| | 時間軸 | 是否受影響 |
|---|---|---|
| `staging.orders` 分區 → Hard Gate 的「最新一批」判定 | `received_at` | ✅ 受影響 |
| `rpt_quality_events_daily.event_date` | `event_at` | ✅ 受影響 |
| `source freshness` 的近窗過濾 | `received_at` | ✅ 受影響 |
| `fct_orders` / `fct_order_items` / `rpt_sales_daily_by_category` | `order_date` | ❌ 不受影響 |

`order_date` 來自 payload、本來就是 `DATE`，沒有時區概念。換句話說：
**營收數字是對的，但「哪一天的品質」會偏移 8 小時。**

這個區別決定了修不修、以及什麼時候修——BI 上的營收可以照看，而 DQ 儀表在跨日界的
事故裡會把責任歸錯天。

要真正修掉，選項是（**都尚未做，且都不是純技術決策**）：

| | 做法 | 代價 |
|---|---|---|
| a | staging 改用業務時區的 `DATE` 當分區欄位 | 語意最正確，但要重建表與回填 |
| b | 保留 UTC 分區，在 `stg_` 層轉出 `business_date` 供下游用 | 不動既有分區，但多一個欄位要維護口徑 |
| c | 明確宣告「品質指標以 UTC 日為準」並寫進報表定義 | 零成本，但要求看報表的人接受與營運日不一致的口徑 |

**先不選**，是因為在目前的攝入模式下三者的產出完全相同（見上方「碰巧對齊」）——
**沒有真實跨日界流量之前，任何選擇都無法被驗證。**

---

## 3. Runbook

### 3.1 啟動

```bash
# .env 需有 BQ_PROJECT、GOOGLE_APPLICATION_CREDENTIALS（主機金鑰路徑），建議加 AIRFLOW_UID
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose -f docker-compose.yml -f docker-compose.airflow.yml up --build
```

UI 在 `http://localhost:8080`（本機練習用 SimpleAuthManager，免登入）。
兩個 compose 檔必須疊加成同一個 project，DAG 才能以 `db` 這個 hostname 連到業務資料庫。

#### ⚠️ 業務 DB 不在 compose 裡的話（實跑踩到的）

上面的預設假設 **postgres 也跑在 compose 內**。若你的業務 DB 在**主機**上（本機開發常見），
容器內 `db` 解析不到、`localhost` 又指向容器自己，`extract_*` 會以
`OperationalError: could not translate host name` 失敗。兩個選擇：

| 做法 | 步驟 |
|---|---|
| **A. 業務 DB 也進 compose**（自給自足，預設路徑） | `docker compose -f docker-compose.yml -f docker-compose.airflow.yml up` 連 `db` 一起起。⚠️ 那是一個**獨立的空資料庫**——它產生的 `raw_id` 會與主機 DB 的重疊，抽到同一個 BQ staging 會**撞去重鍵**，切勿與既有資料混用同一個 dataset |
| **B. 指回主機 postgres** | ① `.env` 設 `AIRFLOW_TASK_DB_URL=postgresql://user:pw@host.docker.internal:5432/<db>`；② 主機 postgres 預設**只監聽 `127.0.0.1`**，必須放寬 `postgresql.conf` 的 `listen_addresses` 與 `pg_hba.conf` 允許 docker 網段，否則仍連不到 |

A 的那個警告值得記住：**`raw_id` 是 landing 層發的代理鍵，兩個獨立的 ODS 各自從 1 開始編號。**
把它們抽進同一張 staging，`stg_` 以 `raw_id` 為 grain 的去重會把不同訂單當成同一筆的副本收斂掉。

### 3.2 ⚠️ DAG 連續失敗超過回看窗 → 修好後第一次跑必須放大回看窗

**單次失敗是安全的**：staging 已 append、watermark 已推進，`stg_` 的回看窗下輪會把那幾天重算。

**危險的是連續失敗**：回看窗預設 3 天，DAG 掛了 4 天再修好的話，那次跑批只回看 3 天，
第 4 天前已在 staging 的列**永遠不會進 `stg_orders`**——不報錯、不自癒，靜默漏資料。

```bash
dbt build --select path:models/staging --vars '{stg_orders_lookback_days: 10}'
```

`stg_quality_events` 與 `rpt_quality_events_daily` 的窗同理，要一起放大
（見 [ecommerce_dbt/README.zh-TW §9](./ecommerce_dbt/README.zh-TW.md)）。

> 由此反推一件事：**回看窗的天數其實是在宣告「可容忍多久的無人值守失敗」**，不只是成本參數。
> Airflow 的失敗告警必須在累積中斷天數逼近回看窗**之前**就被看見。

### 3.3 Proposal B 完整 demo 劇本 ⭐

⚠️ **現在直接跑會 promote 0 筆。** v1→v2 是**變嚴**，而現有資料都是 v2 攝入的——拿 v2
重評估 v2 是同義反覆。要看到回流，必須先有一次真實的**規則放寬**。

```
1. 灌一批含髒資料          python seed_demo.py --n 200 --dirty-rate 0.12
2. 跑主 DAG                → 確認該筆落 int_orders_quarantine、fct_orders 看不到它
3. 放寬一條規則 + bump v3  → 【已落地】age 上限 120→130（clean.AGE_MAX），DQ_RULE_VERSION=v3
                           髒資料注入器會產生 age=125，正好落在新舊上限之間
4. dry-run 確認影響範圍    dq_reevaluation（commit=off）→ 看 would_write 筆數
5. 真的寫                  dq_reevaluation（commit=on, expect_rule_version=v3）
                           → 自動觸發主 DAG
6. 驗收                    該筆出現在 fct_orders；
                           rpt_quality_events_daily.promotions 不再恆為 0
```

這條劇本本身就是整套 DQ 架構「規則演進 → 回溯重評估 → 資料回流」真的被走通一次的證據。

### 3.4 人工放棄（`rejection`）：runbook，不做成 DAG

`permanently_rejected` 是**人工的終局決定**（狀態機沒有出邊），自動任務永不寫入。
需要放棄某筆時，直接對 PG 的 `quality_events` append 一筆 `rejection` 事件並記錄理由。
刻意不做成 endpoint 或 DAG——與 Proposal C「永不做成 HTTP endpoint」同一個紀律：
**不可逆的決定不該有方便的按鈕。**

---

## 4. 刻意先不做的

| 項目 | 為什麼不做 | 觸發點 |
|---|---|---|
| **Seeding DAG** | 會讓「示範資料產生器」變成常駐系統的一部分；且會**翻轉** [CLOUD_LAYER-TW §1.7.7](./CLOUD_LAYER-TW.md) 已寫定的 freshness 立場 | 需要 BI 圖表持續有資料時。屆時 freshness 恢復為有意義的 gate，§1.7.7 的規則表要同步改 |
| **OpenTelemetry** | 需要先有值得觀測的持續流量 | 藍圖 Phase 5 的獨立項 |
| **Cosmos（模型級 task）** | 13 個 model，收益與相依成本不成比例 | model 數量成長到層級 task 看不清依賴時 |
| **triggerer / deferrable** | 目前只有 `BashOperator` | 引入 sensor 時 |
| **小時批** | 方案 A 的 watermark 精度被 DAY 分區卡死 | 改 HOUR 分區或換方案 B 時（[CLOUD_LAYER-TW §2.2](./CLOUD_LAYER-TW.md)）|
| **可回填的 DAG** | 與「`>=` 寧可重抓不漏抓」衝突（§2.5） | 換方案 B watermark 時重新評估 |

---

## 5. 實機驗證記錄（2026-08-05）⭐

本節記錄兩次實跑。**下面每一項都是量出來的**，不是設計時的推論——這正是它值得單獨成節的
理由：前面六節有好幾個決策當初只能靠推理定案，這裡是它們第一次被資料驗證或推翻。

### 5.1 Proposal B 完整回流（§3.3 劇本走一次）

環境：ODS 774 筆（髒 57，7.364%）、BQ sandbox、dbt 1.11。

**刻意的順序**：先以 **v2** 灌 20 筆 `V3DEMO-*`（15 筆 age 落在 121/123/125/127/130，
5 筆對照組 age ∈ {-3, 150, 999}），再切 v3。順序顛倒的話 age=125 會直接被判乾淨、
永遠不會進 quarantine——**只有在舊規則下攝入的資料，才有被新規則撈回的資格。**

| 階段 | 結果 |
|---|---|
| v2 攝入 | 20 筆全部 `has_clean_error=TRUE`、`quality_events` → `quarantined`(v2) |
| 抽取 | orders 220 列 / quality_events 220 列（首次含當日分區重抽）|
| dbt 分層 build | staging PASS=21 WARN=1／intermediate PASS=27 WARN=1／marts PASS=31／reports PASS=24 |
| promote 前 | `int_orders_quarantine` 20、`fct_orders` **0**、`promotions` **0** |
| 重評估 dry-run | 候選 57 → `would_write=15`、`unchanged=42`、`blocked_non_reproducible=0` |
| 重評估 `--commit` | `written=15` |
| **緊接著再跑一次** | **`promoted=0`、`unchanged=57`、`written=0`** |
| 回流後 | `int_orders` promoted **15**、quarantine 剩 **5**、`fct_orders` **15**、`promotions` **0→15** |
| 完整 `dbt test` | 93 支：PASS=91 / WARN=2 / **ERROR=0** |

#### 被實測證實的四件事

**① 冪等從「宣稱」變成「量到的」** ⭐
連跑兩次，第二次 `written=0`。「只在狀態改變時 append」確實讓 `promotions` 這個
〈歷史指標為何不會被追溯性改寫〉要保護的數字不會被重跑灌水。這條當初只有單元測試，
現在有真實資料的證據。

**② 放寬是有邊界的，不是把規則關掉**
對照組 5 筆（age -3/150/999）一動不動留在 quarantine，回流後精確分佈為
`age=121/123/125/127/130 各 3 筆` 進 Gold。

**③ Bounded Writeback 守住了，而且留下 15 筆活的「永久分歧」樣本** ⭐
ODS 那 20 筆至今仍是 `dq_rule_version=v2, has_clean_error=TRUE`，一個欄位都沒被改；
事件鏈是乾淨的 `initial_evaluation(None→quarantined, v2)` → `promotion(quarantined→promoted, v3)`。
[DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md)〈ODS 與 BQ 品質狀態永久分歧〉講了很久的東西，
現在資料庫裡有 15 筆可以直接指給人看：**ODS 說髒（v2）、Gold 說乾淨（v3），
靠 `dq_rule_version` + `quality_events` 完全可追溯。**

**④ Hard Gate 的分級真的是分級**
7.364% 讓 `error_rate_below_stg_orders_0_05` **WARN**、`_0_1` **PASS**——告警但不阻斷，
`dbt build` 照常往下跑。這是「閾值分兩級」第一次在真實比率上被觸發。

### 5.2 `--indirect-selection=buildable` 的行為驗證 ⭐

§2.4 那個決策當初**只能靠推理**——三種模式的差異寫得出來，但沒有實例。這次觀察到：

```
dbt build --select path:models/staging       22 個節點，全是 stg_ 的測試
                                             ← assert_orders_split_is_partition 不在其中
dbt build --select path:models/intermediate  13 of 28 PASS assert_orders_split_is_partition
dbt build --select path:models/marts         assert_fct_orders_complete_projection PASS
                                             assert_fct_orders_rollup_matches_items PASS
```

跨層的 singular test **精準落在「所有輸入都新鮮」的那一層**：不在 staging 誤觸發
（那時 `int_` 還是舊表，會誤紅），也沒有被 `cautious` 那樣整支跳過。推理成立。

> 附帶：結尾那個完整 `dbt test`（93 支）仍然保留。它現在的價值不是「補跑漏掉的」——
> 這次沒有漏——而是**當 selector 語意在未來版本改變時，它是唯一會發現的東西**。

### 5.3 Airflow 容器實跑

| 項目 | 結果 |
|---|---|
| 映像建置 | 成功（`apache/airflow:3.0.0-python3.12` + 兩個隔離 venv）|
| 服務 | `airflow-db` / `init` / `apiserver` / `scheduler` / `dag-processor` 全部 healthy |
| **DAG 解析** | 3 條全部載入，`list-import-errors` → **No data found** |
| analytics venv | `sqlalchemy` / `google.cloud.bigquery` / `structlog` / `pydantic` 匯入正常 |
| dbt venv | dbt-core **1.11.12**、dbt-bigquery **1.11.3** |
| env_var profile | `dbt debug` → `Connection test: [OK connection ok]` |
| `source_freshness_watch` | 完整 DAG run **success**，兩個 source 皆 PASS |
| `dbt_intermediate` | 容器內 `airflow tasks test` → PASS=27 WARN=1 ERROR=0，SUCCESS |
| `extract_orders` | **FAILED**：`OperationalError: could not translate host name`（見 §5.4）|
| UI | `http://localhost:8080` HTTP 200 |

**§2.2 那條紀律在真實 dag-processor 上被驗證了** ⭐
容器裡沒有可用的 `DB_URL`（預設指向未啟動的 `db`），三條 DAG 仍全部解析成功、零 import
錯誤。若當初在 DAG 檔頂層寫了 `from config import settings`，此刻的畫面會是**三條 DAG
全部從 UI 消失**——不是三個紅色 task，是什麼都沒有。

**freshness 的語意也順帶被證實**
灌完料 15 分鐘後跑，兩個 source 皆 **PASS**。[CLOUD_LAYER-TW §1.7.7](./CLOUD_LAYER-TW.md)
論證的「紅代表你最近沒餵它，不代表管線壞掉」不再只是論證——**餵了就綠**。

### 5.4 實跑才發現的落差：`raw_id` 在兩個 ODS 之間會碰撞 ⭐

`extract_orders` 在容器內失敗，表面原因是 compose 把「業務 DB 跑在 compose 內」寫死成
假設，而本機的 postgres 在主機上、且只監聽 `127.0.0.1`。修法是開一個
`AIRFLOW_TASK_DB_URL` 覆寫接縫（見 §3.1 的 A/B 選項）。

**但真正值得記住的是選項 A 底下那個陷阱**，它比這次的失敗嚴重得多：

> 讓 compose 的 `db` 服務一起起來，那是一個**獨立的空資料庫**。它 mint 出來的 `raw_id`
> 從 1 開始編號，與主機 ODS 的**完全重疊**。兩者抽進同一張 BQ staging，
> `stg_` 以 `raw_id` 為 grain 的去重會把**兩筆不相干的訂單當成彼此的副本**收斂掉一筆。
> 不報錯、不留痕跡。

這其實是 [README](./README.zh-TW.md)〈`raw_id` 是物理身分、`order_id` 是業務身分〉那條原則
的一個推論，只是原文沒有把它推到底：**`raw_id` 的唯一性只在「單一 landing 實例」內成立。**
去重鍵選 `raw_id` 是對的（物理去重就該用物理身分），但它同時把一個隱含前提焊進了管線——
**一張 staging 表只能對應一個 ODS**。多實例上游若各自有 landing，去重鍵必須升級為
`(source_instance, raw_id)` 之類的複合鍵。目前是單實例，前提成立，故不動；
記在這裡是為了讓未來要擴展的人知道這條線在哪。

---

## 6. 現況與待辦

- ✅ `orders_analytics_daily`（2 extract → 4 層 dbt build → 完整 `dbt test`）
- ✅ `dq_reevaluation`（手動觸發，預設 dry-run，commit 後自動接主 DAG）
- ✅ `source_freshness_watch`（獨立觀測）
- ✅ 映像（兩個隔離 venv）、compose overlay、env_var 版 `profiles.yml`
- ✅ `tests/test_dags.py`（20 支）+ 獨立 CI job（`.github/workflows/dags.yml`）
- ✅ 實機驗證（2026-08-05）：映像建成、四個服務健康、3 條 DAG 由真實 dag-processor 解析且**零 import 錯誤**、
  兩個 venv 可用（dbt 1.11.12 / bigquery 1.11.3）、env_var 版 profile 在容器內連上 BQ、
  `source_freshness_watch` 完整 run 成功、`dbt_intermediate` 於容器內 PASS=27
- ⬜ `extract_*` 於容器內實跑（受阻於「業務 DB 在主機且只監聽 127.0.0.1」，見 §3.1 的 A/B 選項）
- ✅ v3 規則放寬（`age` 上限 120→130）——Proposal B 第一次有真的可 promote 的對象
- ✅ §3.3 demo 劇本實機走完（2026-08-05）：20 筆以 v2 落 quarantine → v3 放寬 → 重評估 promote 15
  → 回流 `fct_orders`，`promotions` 0→15；對照組 5 筆（age -3/150/999）正確留在 quarantine；
  連跑兩次第二次 `written=0`（冪等）；ODS 全程未被修改（Bounded Writeback）
- ⬜ Seeding DAG（見 §4）
- ✅ Celery + Redis（已實作，與本層正交；見 [QUEUE-TW.md](./QUEUE-TW.md)）
- ⬜ OpenTelemetry（藍圖 Phase 5 的其他項）

## 7. 相依與版本

- Airflow **3.0.0**（`apache/airflow:3.0.0-python3.12`），LocalExecutor
- dbt-core / dbt-bigquery **1.11**（對齊 [ecommerce_dbt/README.zh-TW §10](./ecommerce_dbt/README.zh-TW.md)）
- 升級 Airflow 時：`orchestration/Dockerfile` 的 `ARG AIRFLOW_VERSION` 與
  `.github/workflows/dags.yml` 的 `AIRFLOW_VERSION` 必須一起改（constraints 檔按版本號取得）
