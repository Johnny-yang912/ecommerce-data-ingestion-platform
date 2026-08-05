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

**Airflow 不是任務佇列**。開發藍圖裡「Airflow」與「Celery + Redis」是兩個**正交**的項目，
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
broker——而那個 Redis 會與藍圖裡「Celery + Redis 取代 BackgroundTasks」在概念上打架。

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
3. 放寬一條規則 + bump v3  → clean.py（例如放寬 age 上限），依 bump 判準這是要 bump 的改動
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
| **Celery + Redis** | 與 Airflow 正交（見〈範圍與職責邊界〉），解的是 `BackgroundTasks` 的持久化問題 | 需要 API 水平擴展時 |
| **OpenTelemetry** | 需要先有值得觀測的持續流量 | 藍圖 Phase 5 的獨立項 |
| **Cosmos（模型級 task）** | 13 個 model，收益與相依成本不成比例 | model 數量成長到層級 task 看不清依賴時 |
| **triggerer / deferrable** | 目前只有 `BashOperator` | 引入 sensor 時 |
| **小時批** | 方案 A 的 watermark 精度被 DAY 分區卡死 | 改 HOUR 分區或換方案 B 時（[CLOUD_LAYER-TW §2.2](./CLOUD_LAYER-TW.md)）|
| **可回填的 DAG** | 與「`>=` 寧可重抓不漏抓」衝突（§2.5） | 換方案 B watermark 時重新評估 |

---

## 5. 現況與待辦

- ✅ `orders_analytics_daily`（2 extract → 4 層 dbt build → 完整 `dbt test`）
- ✅ `dq_reevaluation`（手動觸發，預設 dry-run，commit 後自動接主 DAG）
- ✅ `source_freshness_watch`（獨立觀測）
- ✅ 映像（兩個隔離 venv）、compose overlay、env_var 版 `profiles.yml`
- ✅ `tests/test_dags.py`（20 支）+ 獨立 CI job（`.github/workflows/dags.yml`）
- ⬜ 首次 `docker compose up` 的實機驗證（需要 GCP 金鑰與 BQ 專案）
- ⬜ v3 規則放寬 + §3.3 demo 劇本走一次
- ⬜ Seeding DAG（見 §4）
- ⬜ Celery + Redis、OpenTelemetry（藍圖 Phase 5 的其他項）

## 6. 相依與版本

- Airflow **3.0.0**（`apache/airflow:3.0.0-python3.12`），LocalExecutor
- dbt-core / dbt-bigquery **1.11**（對齊 [ecommerce_dbt/README.zh-TW §10](./ecommerce_dbt/README.zh-TW.md)）
- 升級 Airflow 時：`orchestration/Dockerfile` 的 `ARG AIRFLOW_VERSION` 與
  `.github/workflows/dags.yml` 的 `AIRFLOW_VERSION` 必須一起改（constraints 檔按版本號取得）
