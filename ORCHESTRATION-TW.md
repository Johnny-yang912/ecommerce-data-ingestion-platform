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
| freshness 不需要阻斷權（沒資料＝沒灌，無害） | 上游停送是事故，**但不由 freshness 偵測**——它量的是 `ods.received_at`＝extract 那一跳，結構上看不見上游。真實系統要補的是 Raw 那一側的量測（§2.12、OTel），不是把 freshness 接成 gate |
| 26h/50h 的 freshness 閾值 | **不變**。閾值來自【載入節奏】（一天一次 extract → `24h + 2h 寬限`），不是攝入節奏；24/7 攝入下倉儲仍是夜間批次，閾值就仍是這個量級。會改變它的是 extract 改成小時批或串流 |
| freshness 偵測得到「整天沒資料」 | 偵測不到「峰期停了三小時」——但那是**範圍**而非閾值的問題，該由 §2.12 與 OTel 回答（見 §2.7） |

**但驗證程度要分兩條路徑講，不可一概而論**：

- **攝入路徑**（API → Redis → worker → ODS）：**爆量行為已經量過**，是本專案證據最完整的一段。見 [QUEUE-TW §5](./QUEUE-TW.md)——多行程限流、broker 停機下的降級與熔斷（200 併發）、有界恢復掃描（6 萬筆積壓單輪清完；12 萬筆游標續傳兩輪、`duplicate` 0 筆）。**這段不是靠 seeding 驗證的。**
- **分析路徑**（extract → BQ → dbt）：**從未在小量以外的資料上跑過**。`stg_` 回看窗與 `insert_overwrite` 的實際成本、Hard Gate 靈敏度隨資料成長的變化、BQ 的儲存與查詢成本，都未被觀察。

兩邊都沒涵蓋的還有兩件：**持續性**（壓測是一次性爆量，不是連續數週的日常負載，累積效應未知）與**峰期形狀**（瞬間併發峰值 ≠ 持續數小時的高檔，對連線池週轉是不同壓力）。

**寫下來的理由**：這些設計在目前的條件下都是對的，但它們的正確性依賴一個
不會出現在程式碼裡的前提。不寫的話，下一個接手的人（包括未來的我）會以為
這條管線已經面對過連續流量——**而它的所有測試都會支持那個誤解**。

---

## 1. DAG 拓撲

全部排程皆以 `Asia/Taipei` 顯式宣告（理由見 §2.5）。

```
【seed_demo_daily】  0 10,13,17,21 * * *（台北），catchup=False, max_active_runs=1

  seed_orders               ← 模擬上游＝本系統唯一的資料來源

【raw_pending_watch】  30 10,13,17,21 * * *（台北）

  check_raw_pending         ← 每個 seeding 時段後 30 分鐘；理由見 §2.12

【orders_analytics_daily】  30 22 * * *（台北），catchup=False, max_active_runs=1

  extract_orders ─────────┐
                          ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test_all
  extract_quality_events ─┘      (Hard Gate)                                                 (completeness)

【source_freshness_watch】  0 8 * * *（台北）

  dbt_source_freshness      ← 獨立成一條 DAG 的理由見 §2.7

【dq_reevaluation】  schedule=None（手動觸發）

  reevaluate ─► should_refresh（commit 才通過）─► trigger orders_analytics_daily

【seed_demo_gate_demo】  schedule=None（手動觸發）

  seed_dirty_batch          ← Hard Gate 攔截劇本
```

**四條有排程的 DAG 之間沒有任何 Airflow 層級的相依**——`seed → 探針 → 抽取 → freshness`
的時序契約**只存在於時間差裡**（21:00 送完 → 21:30 檢查 → 22:30 抽取 → 隔日 08:00 backstop）。
這是刻意的：用 Trigger 串起來會讓上游的紅連帶決定下游跑不跑，而它們的紅各自代表
完全不同的處置（見 §2.7 的表）。代價是那個時間差必須由測試釘住，見
`tests/test_dags.py::TestSeedDemoDaily::test_runs_before_the_analytics_dag`
與 `TestRawPendingWatch::test_slot_hours_match_the_seeding_dag`。

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
的權限。** 拆開之後，每條 DAG 的紅各自對應一種處置——這就是拆開的全部收益：

| DAG | 紅代表 | 處置方向 |
|---|---|---|
| `seed_demo_daily` | 灌不進去（API 拒收 / 腳本本身壞了） | 看 API 與腳本 |
| `raw_pending_watch` | Raw 進得來但沒人來取 | 看 redis / worker / beat（§2.12）|
| `orders_analytics_daily` | 管線壞了（extract 或 dbt） | 看管線 |
| `source_freshness_watch` | staging 沒被推進（extract 空搬的 backstop） | 看 watermark 與 extract |

由 `tests/test_dags.py::TestFreshnessIsolation` 把關：任何有實際產出的 DAG 混進
`dbt source freshness`，測試就紅。

**⚠️ freshness 的範圍剛好是一跳，而且那是刻意的** ⭐

`loaded_at_field` 指向 `ods.received_at`＝**ODS 的落地時刻，不是收單時刻**
（完整說明見 [CLOUD_LAYER-TW §1.2.2](./CLOUD_LAYER-TW.md)）。而本 DAG 檢查的是 extract，
extract 搬的正是 ODS——**所以看 ODS 自己的時鐘是正確的時間軸，不是妥協**。

由此得到的範圍邊界：**它看不見已經恢復的攝入中斷**（積壓被恢復掃描沖出去時，那批列的
`received_at` 是回補當下的寫入時刻，斷層在 ODS 時間軸上不存在）。這不是缺陷，是別人的
職責——三個時間軸各管一段，混在一起的話一個紅會同時代表兩段管線：

| 時間軸 | 回答哪一段 | 誰在看 |
|---|---|---|
| `raw.received_at` | 上游 + API：收得到單嗎 | （OTel 之後）|
| `raw.received_at` → `ods.received_at` | 派工：worker 取得到件嗎 | `raw_pending_watch`（§2.12）|
| BQ staging 上的 `ods.received_at` | extract：搬進倉儲了嗎 | `source_freshness_watch` |

**26h/50h 從哪來**：`26 = 24 + 2`、`50 = 48 + 2`——一個**載入週期** + 2 小時寬限。
來源是載入節奏而非攝入節奏：staging 一天只被 extract 推一次，資料設計上就有最多 24 小時的
年齡，閾值必須大於 24h 否則每天抽取前會自己紅。取樣點與閾值互相決定：台北 08:00 取樣時
健康值約 13h、一個週期沒進資料是 37h，26h 落在正中間、兩邊各約 10 小時餘裕。
**排 08:00 是因為它是 backstop**——extract 若回報成功卻沒搬東西，Hard Gate 判的是舊分區
會通過、`dbt test` 也全綠，此時它是唯一會在營運團隊 09:00 看報表前叫的東西。

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

排程以 `Asia/Taipei` 宣告（seeding 10/13/17/21、抽取 22:30），但 `received_at` 是 TIMESTAMP、
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

### 2.12 `raw_pending_watch`：觀測訊號原則的第二個案例 ⭐

§2.7 立的原則（觀測訊號沒有阻斷權、也沒有污染別人成功率的權）在這裡第二次適用：
`raw_pending_watch` 同樣是獨立 DAG、同樣不是任何東西的上游。但它帶來兩個 §2.7
沒有的論點。

**① ⚠️ 不能用「Raw 有列但沒有對應的 ODS 列」當判準**

Raw 的終態有三種：`processed`、`duplicate`、`error`。**後兩者不會產生 ODS 列，
而且那是正確行為**（`duplicate` 是刻意保留的監控訊號，見 CLAUDE.md 架構約束）。
照那個判準做探針，**每一筆重複訂單都會讓它紅**。

所以它只看 `status='pending'`：已落到 Raw、但**還沒有任何 worker 把它取走**。
這個狀態沒有正當理由長期存在，是一個乾淨的故障訊號。取走之後變成 processed 還是
duplicate/error，是資料內容的事，由 DQ 機制負責。

這也決定了它量的是**根因而非症狀**：redis/worker 掛掉時下游看得到的症狀是 ODS 不再
成長，但根因在派工端——量根因比盯著 ODS 有沒有變多更早、也更明確。

**② 量測頻率由【被量的東西何時會變化】決定，不是由「多久看一次比較安心」**

常見量級是**參考不是準則**：有 metrics stack 的環境是 15–60 秒取樣 + 要求連續超標
2–10 分鐘；只有排程器的過渡期是 5–15 分鐘 + 連續兩次。本專案是一天四次。

同一條原則推出相反的數字，因為固定時段攝入下**時段之間 pending 在結構上不可能累積**
——19:00 去量一個從 17:10 起就空的佇列，量到的不是「健康」，是沒有資訊。
實際頻率應由四項共同決定：資料抵達頻率、檢查本身的成本、可容忍的偵測延遲
（由損失的**可逆性**決定——訂單沒收到不可逆，報表晚了可逆）、以及有沒有抑制機制。

**③ 門檻是推導的，不是選的**

下界由**恢復路徑自己的週期**決定，低於自癒時間會對「正在被正確處理的列」告警：

```
max(派工失敗 PENDING_GRACE + scan_interval,
    worker 猝死 STALE_PROCESSING_MINUTES + scan_interval) + 安全邊際
```

現值 `max(360s, 900s) + 240s = 1140s`（19 分鐘），且**在執行時從 `config` / `process`
讀那三個常數**而非寫死——`SCAN_INTERVAL_SECONDS` 是 `.env` 可調的，寫死一個數字等於
把推導結果凍成魔術數字，而下一個調它的人不會知道要回來改這裡。
⚠️ 連帶要求：該變數必須同時注入 worker/beat **與** Airflow 容器（兩份 compose 都有），
只調一邊會讓恢復掃描與探針門檻無聲分岔。

**④ 它是過渡期的粗篩，不是告警**

實務上的存活監控是「秒級取樣 + 要求連續超標」，而 **Airflow 表達不了後者**——每個
DAG run 都是獨立、無記憶的取樣。另一個限制是故障域：它與被監控的系統住在同一組
compose。真正的告警要等 OTel（§4），而屆時第一條該寫的規則是 **absent**——
「什麼都沒發生」正是 metrics 最不擅長回答的問題，而那正是這裡要抓的東西。

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

**業務 DB 也在 compose 裡**（2026-08-11 起；此前曾支援「業務 DB 在主機」的設定，已移除）。
全套同網路之後，`SEED_API_URL=http://api:8000/orders` 與
`DB_URL=postgresql://app:app@db:5432/orders` 天然指向同一套系統。

> ⚠️ **`db` 的對外埠是 5433**（`DB_PUBLISH_PORT`）。主機上若另有 postgres 佔著 5432，
> `5432:5432` 會讓服務直接 bind 失敗。容器之間走 `db:5432`，不經過這個映射——
> 它只給主機端 `psql` 除錯用。

> ⚠️ **主機端工具（`seed_demo.py --verify`、`psql`）要連 `localhost:5433/orders`。**
> `.env` 已指向那裡，但 **`load_dotenv` 的 `override=False` 讓環境變數勝過 `.env`**——
> shell 裡若 export 過舊的 `DB_URL`，腳本會安靜地連到別的地方。`verify()` 因此會印出
> 實際連到的資料庫，那是唯一會讓這類錯誤自己現形的地方。

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

### 3.3 規則放寬的部署 SOP（Proposal B）⭐

規則**放寬**會讓既有 quarantine 記錄有機會被撈回 Gold。這是 Proposal B 的用途，也是
唯一需要這套流程的場合——變嚴的規則只往後生效，不需要回溯。

```
1. 確認有候選         查 quarantine 裡目標 code 的值域，確認【跨越】新舊閾值
2. 改規則 + bump      clean.py 的閾值 + DQ_RULE_VERSION，打 git tag
3. ⚠️ 重建映像        docker compose build api worker beat && docker compose up -d
4. ⚠️ 跑主 DAG        orders_analytics_daily（候選讀 BQ，資料必須先上去）
5. dry-run            dq_reevaluation（commit=off, expect_rule_version=<新版本>）
6. 真的寫             dq_reevaluation（commit=on, expect_rule_version=<新版本>）
                      → 自動觸發主 DAG 回流 Gold
7. 驗收               promote 的列進 fct_orders、離開 quarantine；
                      promotions > 0；對照組仍在 quarantine；
                      ODS 未被修改；再跑一次 written=0
```

#### ⚠️ 第 1 步：先確認有候選，不要 bump 完才發現沒有

`promoted=0` 與「規則沒生效」「程式壞了」在畫面上**長得一模一樣**。而權重低的 DQCode
累積很慢——bump 之前先查一次值域分佈，不夠就先灌，比事後排查省事得多。

#### ⚠️ 第 3 步：兩條路徑的程式碼交付方式不同

```
api / worker / beat   程式碼【烤進映像】           改檔案後要 build 才生效
Airflow 容器          bind mount ./:/opt/project    改檔案【立刻】生效
```

漏掉重建的話，**重評估（跑在 Airflow）已是新版本、攝入路徑仍是舊版本**——資料庫裡
同時存在兩個版本的判定，而 `--expect-rule-version` **看不到這個分歧**：它只比對自己
行程裡的 `DQ_RULE_VERSION`，斷言會通過。

> 那個守衛防的是「打在還沒部署新規則的環境上」。它成立的前提是**整個系統只有一套
> 程式碼交付機制**——這個 compose 拓撲打破了那個前提。

#### ⚠️ 第 4 步：候選讀 BQ，狀態讀 PG

`dq_reevaluation` 檔頭 ④ 記的是「重評估寫 PG 的 `quality_events`，要回流 Gold 還需要
extract 把事件送上 BQ」。**反向同樣成立，而且更容易漏**：

> **候選清單來自 BQ 的 `int_orders_quarantine`。新累積的資料若還沒抽上 BQ，重評估就
> 看不見它們**——症狀是 `candidates` 偏低、`would_write=0`。

實測數據與兩次走完的完整記錄見 §5。

### 3.4 人工放棄（`rejection`）：runbook，不做成 DAG

`permanently_rejected` 是**人工的終局決定**（狀態機沒有出邊），自動任務永不寫入。
需要放棄某筆時，直接對 PG 的 `quality_events` append 一筆 `rejection` 事件並記錄理由。
刻意不做成 endpoint 或 DAG——與 Proposal C「永不做成 HTTP endpoint」同一個紀律：
**不可逆的決定不該有方便的按鈕。**

### 3.5 ⚠️ 排程靜默停擺：`is_stale` 是最先亮起、也最容易被忽略的燈 ⭐

**症狀**：該跑的 DAG 沒跑，但 **UI 上看不到任何紅色**——因為根本沒有 run 被建立，
沒有 run 就沒有 failed run 可以變紅。dag-processor 若無法完成解析，
DAG 會在 `dag_stale_not_seen_duration`（預設 600 秒）之後被標記為 stale，
而 **scheduler 不會為 stale 的 DAG 建立任何 run**。整條管線就此無聲停止。

排查順序，由快到慢：

```bash
# ① 有沒有 DAG 是 stale 的（最快、最直接的判準）
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True   # 非 0 = 中獎

# ② 解析停在什麼時候（is_stale=True 時看這個）
docker exec api-airflow-apiserver-1 airflow dags details <dag_id> | grep -E "is_stale|last_parsed_time"

# ③ dag-processor 是否在殺解析子行程
docker logs api-airflow-dag-processor-1 | grep -c "killing it"

# ④ 排除真正的語法/import 問題
docker exec api-airflow-apiserver-1 airflow dags list-import-errors
```

> **省時間的關鍵：④ 乾淨不代表 DAG 檔沒問題，但②③有異常時，八成不是 DAG 檔的問題。**
> 可以直接在容器裡手動解析來把程式碼排除在外：
>
> ```bash
> docker exec api-airflow-dag-processor-1 python -c \
>   "from airflow.models.dagbag import DagBag; d=DagBag('/opt/airflow/dags/<file>.py', include_examples=False); print(list(d.dags), d.import_errors)"
> ```
>
> 手動解析**成功**、dag-processor 卻**失敗**，代表問題在 dag-processor 的監督機制
> （逾時判定、資源、子行程生命週期），不在 DAG 程式碼裡。這個分岔點能省下大量時間。

**兩條獨立的線，別搞混**：

| 參數 | 預設 | 管什麼 |
|---|---|---|
| `[dag_processor] dag_file_processor_timeout` | 50 | 解析子行程活多久會被砍掉重試 |
| `[scheduler] dag_stale_not_seen_duration` | 600 | 多久沒被成功解析會被標記 stale |

調大前者**不會**延後故障被發現的時間——那是後者決定的。前者只影響「卡住的解析多久
才會被砍掉重試」，而且對**持續性**卡死無效（砍掉只是重跑同一個檔），只對
「重試就會過」的暫時性卡住有意義。

> 由此反推的觀測缺口：**這種故障沒有任何內建告警**。要補的話，偵測器必須活在
> Airflow 之外——DAG 全部 stale 時，寫成 DAG 的看門狗自己也不會跑。這與 §4 對
> OpenTelemetry 的結論是同一個原則：**存活告警不能與被監控的系統同生共死。**

---

## 4. 刻意先不做的

| 項目 | 為什麼不做 | 觸發點 |
|---|---|---|
| **OpenTelemetry** | ⚠️ 原本的理由是「需要先有值得觀測的持續流量」——`seed_demo_daily` 上線後**那個條件已經成立**。真正還缺的是一個**活在本機之外**的 backend：存活告警不能與被監控的系統同生共死 | 已可著手。第一條該寫的規則是 **absent**（「這個來源多久沒送資料了」），不是業務指標——那條必須寫在雲端側，因為它要偵測的正是「我這側已經沒辦法說話了」。見 §2.12 ④ |
| **Cosmos（模型級 task）** | 13 個 model，收益與相依成本不成比例 | model 數量成長到層級 task 看不清依賴時 |
| **triggerer / deferrable** | 目前只有 `BashOperator` | 引入 sensor 時 |
| **小時批** | 方案 A 的 watermark 精度被 DAY 分區卡死 | 改 HOUR 分區或換方案 B 時（[CLOUD_LAYER-TW §2.2](./CLOUD_LAYER-TW.md)）|
| **可回填的 DAG** | 與「`>=` 寧可重抓不漏抓」衝突（§2.5） | 換方案 B watermark 時重新評估 |

> **2026-08-11 更新**：**Seeding DAG 已實作**（`seed_demo_daily`），故從上表移除。
> 當初列為「不做」的理由是「會讓示範資料產生器變成常駐系統的一部分」——那個顧慮成真了，
> 而且是**刻意接受的**：這個專案沒有真實上游，seeding 就是資料來源（見〈範圍與職責邊界〉）。
> 連帶 [CLOUD_LAYER-TW §1.7.7](./CLOUD_LAYER-TW.md) 的 freshness 立場已同步翻轉。

---

## 5. 實機驗證記錄 ⭐

本節記錄歷次實跑。**下面每一項都是量出來的**，不是設計時的推論——這正是它值得單獨成節的
理由：前面幾節有好些決策當初只能靠推理定案，這裡是它們第一次被資料驗證或推翻。

> ⚠️ **本節數字是特定時點的量測，不是當前狀態。** 資料集已於 **2026-08-11 重建**
> （舊 ODS 與兩個 BQ dataset 全清、從零重跑 migration），§5.1–§5.4 引用的筆數在現在的
> 資料庫裡查不到。保留它們是因為**那幾次量測驗證或推翻的設計結論仍然成立**——
> 它們是結論的證據，不是現況的快照。

### 5.0 兩次驗證的分界

| | 2026-08-05 | 2026-08-11 |
|---|---|---|
| 環境 | 業務 DB 在主機、Airflow 在 compose | 全 compose（§3.1） |
| `extract_*` 於容器內 | ⬜ 受阻 | ✅ 通過 |
| 規則版本 | v3 | v4 |
| 資料集 | 累積而來（含手動灌入、不可重現的資料） | 從零重建，全部由 `seed_demo` 產生 |
| 攝入模式 | 手動 | `seed_demo_daily` 排程 |

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

### 5.4 `raw_id` 在兩個 ODS 之間會碰撞 ⭐

**當時的狀況**：業務 DB 在主機、Airflow 在容器，兩邊要接起來時有一個選項是「讓 compose 的
`db` 也跑起來」——那是一個**獨立的空資料庫**，它 mint 的 `raw_id` 從 1 開始，與主機 ODS
**完全重疊**。兩者抽進同一張 BQ staging，`stg_` 以 `raw_id` 為 grain 的去重會把**兩筆不相干
的訂單當成彼此的副本**收斂掉一筆。不報錯、不留痕跡。

**什麼時候要注意**：只要出現「多個 landing 實例」——主機與容器各一份、藍綠部署兩份、
多個上游各自有自己的 Raw 表——這個碰撞就會重現。

**解法**：去重鍵升級為 `(source_instance, raw_id)` 之類的複合鍵；或在抽取時就帶上實例
識別欄位。**在那之前，一張 staging 表只能對應一個 ODS**，不同實例的資料切勿混進同一個
dataset。

這是 [README](./README.zh-TW.md)〈`raw_id` 是物理身分、`order_id` 是業務身分〉那條原則的
推論，只是原文沒推到底：**`raw_id` 的唯一性只在「單一 landing 實例」內成立。** 去重鍵選
`raw_id` 是對的（物理去重就該用物理身分），但它同時把「單實例」這個前提焊進了管線。

> **本專案已無此風險**：作品性質、全 compose 之後只有一個 ODS，成因隨之消失（§3.1）。
> 保留本節是因為那個前提仍焊在管線裡——只是目前恆為真。

### 5.5 全 compose 重建與 v4 回流（2026-08-11）⭐

環境改為全 compose、資料集從零重建，並以 v4 走完一次規則放寬。

**基建**

| 項目 | 結果 |
|---|---|
| `alembic upgrade head` 從零 | 7 個 migration 全數通過——長壽的開發資料庫從沒測過這條路徑 |
| 服務健康 | 9 個容器（db/redis/api/worker/beat + Airflow 四件）全 healthy |
| Airflow → `api:8000` / `db:5432` | 皆通，且讀到同一個資料庫（`ods=8`）——`§6` 卡最久的 ⬜ 就此解除 |
| BQ 全清後自動重建 | `extract_ods_to_bq.py` 的 `create_dataset/create_table(exists_ok=True)` 帶著分區與 `require_partition_filter` 設定重建，**零手工 DDL** |
| 主 DAG | 7/7 task success，全程約 **2.5 分鐘** |
| `source_freshness_watch` | 兩個 source 皆 **PASS**——由「預期恆紅」轉為「預期常綠」 |

**落地閘門（`--require-landed-pct`）正反向**

停掉 `worker` 後灌 3 筆：

| | ODS | exit code |
|---|---|---|
| 不給旗標（舊行為） | 0 筆 | **0** ← 靜默成功，正是要防的 |
| `--require-landed-pct 0.9` | 0 筆 | **1** ← 擋下 |

重啟 `worker` 後，13 筆 `pending` 由 `scan_and_dispatch` 全數補派完成——自癒一併驗到。

**v4 規則放寬回流**

資料 3,015 筆、quarantine 265 筆。目標：`customer_name` 軟性上限 100→150。

| 步驟 | 結果 |
|---|---|
| dry-run | `candidates=265 promoted=3 would_write=3` |
| commit | `written=3`；`quality_events` 3015 `initial_evaluation@v3` + 3 `promotion@v4` |
| **Bounded Writeback** | ODS 指紋前後**完全一致**（3015 筆、髒 265 不變） |
| 冪等 | 再跑一次 `promoted=0 written=0 unchanged=265` |
| Gold 回流 | `int_orders +3`、`quarantine 265→262`、`fct_orders +3`、`promotions 0→3` |
| 逐筆確認 | 3 筆皆 `fct_orders=1 / quarantine=0` |
| 對照組 | `customer_name` 157/164/176/188/199 與 `city` ×5 **全數留在 quarantine** |

> 對照組是**同一個注入器自然形成的**（`_dirty_field_too_long` 長度散佈 110~200、
> 且有一半打 `city`），不像 v3 那次要另外準備。邊界也更緊：**146 promote、157 不 promote**。

#### 被推翻的兩個推論 ⭐

**① `--expect-rule-version` 的覆蓋範圍比原本以為的窄**

重建映像前實測：`api`/`worker` 回報 `v3 {'customer_name': 100}`、Airflow 回報
`v4 {'customer_name': 150}`，而 `--expect-rule-version v4` **通過**。
該守衛只比對自己行程裡的版本——**它成立的前提是整個系統只有一套程式碼交付機制**，
而這個 compose 拓撲（映像 vs bind mount）打破了那個前提。處置見 §3.3 第 3 步。

**② 候選來源的方向性沒被寫下來**

`dq_reevaluation` 檔頭只記了「重評估寫 PG，要回流 Gold 需要 extract」。反向同樣成立：
**候選讀 BQ，所以資料必須先上 BQ**。第一次 dry-run 得到 `candidates=26 / would_write=0`
——不是規則沒生效，是 BQ 還停在累積前的狀態。處置見 §3.3 第 4 步。

#### 順帶量到的

- **unpause 一條 DAG 會立刻產生一個 scheduled run**：`staging.orders` 因此是 398 = 199×2，
  而 `stg_orders` 正好 199——append-only 容忍重複、去重交給 `stg_` 的設計被意外驗證了一次。
- **Jinja 模板錯誤只在 runtime 出現**：DagBag 解析乾淨、`dags list` 正常、結構測試全綠，
  task 卻在 0.16 秒內失敗。三種踩法（巢狀 `{{ }}`、f-string 把 `}}` 跳脫成 `}`、
  `data_interval_start` 在手動 run 不存在）都只有**真的渲染一次**才抓得到，
  故 `tests/test_dags.py` 補了渲染測試。
- **cron 的 `data_interval_start` 是上一個觸發點**：用它當日期種子會讓每天第一個時段取到
  **前一天**，當日髒率不再一致。改用 `dag_run.run_after`（見 `seed_demo_daily.py` 檔頭）。

---

## 6. 現況與待辦

- ✅ `orders_analytics_daily`（2 extract → 4 層 dbt build → 完整 `dbt test`；台北 22:30）
- ✅ `dq_reevaluation`（手動觸發，預設 dry-run，commit 後自動接主 DAG）
- ✅ `source_freshness_watch`（extract 的 backstop；台北 08:00；2026-08-11 起由「預期恆紅」轉為「預期常綠」）
- ✅ `seed_demo_daily`（模擬上游，台北 10/13/17/21，共 800 筆/天）
- ✅ `raw_pending_watch`（派工存活；台北 10:30/13:30/17:30/21:30；門檻由恢復路徑的設定推導，見 §2.12）
- ✅ `seed_demo_gate_demo`（Hard Gate 攔截劇本，手動觸發）
- ✅ 映像（兩個隔離 venv）、compose overlay、env_var 版 `profiles.yml`
- ✅ 全 compose 化（db/redis/api/worker/beat 與 Airflow 同一個 project、同一套資料）
- ✅ `tests/test_dags.py`（47 支）+ 獨立 CI job（`.github/workflows/dags.yml`）
- ✅ 實機驗證（2026-08-05）：映像建成、四個服務健康、3 條 DAG 由真實 dag-processor 解析且**零 import 錯誤**、
  兩個 venv 可用（dbt 1.11.12 / bigquery 1.11.3）、env_var 版 profile 在容器內連上 BQ、
  `source_freshness_watch` 完整 run 成功、`dbt_intermediate` 於容器內 PASS=27
- ✅ `extract_*` 於容器內實跑（2026-08-11 通過——全 compose 之後阻礙整類消失，見 §5.5）
- ✅ v3 規則放寬（`age` 上限 120→130）——Proposal B 第一次有真的可 promote 的對象
- ✅ v4 規則放寬（`customer_name` 軟性上限 100→150）
- ✅ §3.3 SOP 實機走完兩次：v3（2026-08-05，promote 15）與 v4（2026-08-11，promote 3）；
  兩次皆冪等、ODS 未被修改、對照組留在 quarantine。完整數據見 §5.1 與 §5.5
- ✅ Celery + Redis（已實作，與本層正交；見 [QUEUE-TW.md](./QUEUE-TW.md)）
- ⬜ OpenTelemetry（藍圖 Phase 5 的其他項）
- ⬜ 跨時區抽取的正式處置（§2.11 的 a/b/c 尚未選——沒有真實跨日界流量之前無法驗證）

## 7. 相依與版本

- Airflow **3.0.0**（`apache/airflow:3.0.0-python3.12`），LocalExecutor
- dbt-core / dbt-bigquery **1.11**（對齊 [ecommerce_dbt/README.zh-TW §10](./ecommerce_dbt/README.zh-TW.md)）
- 升級 Airflow 時：`orchestration/Dockerfile` 的 `ARG AIRFLOW_VERSION` 與
  `.github/workflows/dags.yml` 的 `AIRFLOW_VERSION` 必須一起改（constraints 檔按版本號取得）
