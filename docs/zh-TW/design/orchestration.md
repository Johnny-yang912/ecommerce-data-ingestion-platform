# 編排層：Airflow

[English](../../en/design/orchestration.md) | **繁體中文**

---

## 1. 範圍，以及 Airflow 不是什麼

Airflow 擁有的是**批次排程**：ODS → BigQuery → dbt。它**不是**攝入路徑的任務佇列——那是 Celery + Redis（[queue](./queue.md)），而兩者刻意不共用 Redis 實例。

> ⚠️ **這是一個作品專案，而資料源是模擬的。** `seed_demo_daily` **就是**上游。它涵蓋什麼、不涵蓋什麼：[PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

Airflow 3.0.0、LocalExecutor、單機。沒有 triggerer（只用 `BashOperator`），沒有 Cosmos（13 個 model）。

---

## 2. DAG 拓撲

六個 DAG。每一個排程都以 `Asia/Taipei` 明確宣告。

| DAG | 排程 | 做什麼 |
|---|---|---|
| `seed_demo_daily` | 10/13/17/21 | 模擬上游——每天 800 筆訂單 |
| `raw_pending_watch` | 10:30/13:30/17:30/21:30 | 派工存活探針，每個灌資料時段後 30 分鐘 |
| `orders_analytics_daily` | 22:30 | 2 個 extract → 4 個分層 `dbt build` → 一次完整 `dbt test` |
| `source_freshness_watch` | 08:00 | extract 的後備 |
| `dq_reevaluation` | **手動** | Proposal B，預設 dry-run |
| `seed_demo_gate_demo` | **手動** | Hard Gate 攔截情境 |

**四個排程 DAG 沒有任何一個在 Airflow 層依賴另一個——排序契約只存在於時間間隔中。** 那是刻意的：**每一個的紅燈代表不同的事，那正是它們分開的全部理由。**

---

## 3. 執行模型

**映像內兩個 venv**，而且專案相關的東西一個都不裝進 Airflow 自己：

```
/home/airflow/venvs/analytics   ← requirements-analytics.txt
/home/airflow/venvs/dbt         ← dbt-core / dbt-bigquery 1.11
```

**DAG 檔在 top-level 不 import 任何專案模組。** `config.py` 在 import 時就實例化 `Settings` 且 `db_url` 必填，而 dag-processor 每隔幾十秒重新解析每一個檔案——top-level import 代表一個缺 `DB_URL` 的解析行程會讓**整個 DAG 從 UI 消失**，而且完全沒有紅燈。

一切都是 `BashOperator`，把 import 推到執行時。兩個附帶好處：`tests/test_dags.py` 能在沒有資料庫、沒有環境變數的情況下解析 DagBag，而 Airflow 3 的獨立 dag-processor 讓這條紀律更重要而非更不重要。[ADR-0035](../adr/0035-two-venvs-dependency-isolation.md) · [ADR-0036](../adr/0036-dag-no-toplevel-import.md)

**`profiles.yml` 住在 `orchestration/dbt_profiles/`**，不在 `ecommerce_dbt/`——dbt 的尋找順序把工作目錄排在 `~/.dbt` 之前，放那裡會破壞本機 `dbt run`。它重用與 `config.py` 相同的環境變數，所以產生者與消費者不可能指向不同的 dataset。[ADR-0041](../adr/0041-profiles-yml-structure-vs-values.md)

---

## 4. 分析 DAG

```
extract_orders ─┐
                ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test
extract_quality_events ─┘
```

**一表一個 extract task**，因為重試粒度應該與失效粒度相符——而跨表 gate 就是那條依賴邊本身（dbt 的上游 = 兩個 extract 都成功）。

**分層 dbt 執行，加 `--indirect-selection=buildable`。** 預設的 `eager` 會在 **staging** task 就選中劃分不變式測試，對一個重建到一半的狀態斷言；`cautious` 則永遠不會執行它。`buildable` 讓它落在 intermediate task、所有輸入都是新的。

**一次完整的 `dbt test` 收尾整個 DAG**——逐層的測試是**閘門**，收尾那次是**完整性**。**一個被靜默跳過的測試，遠比一個被重複執行的測試糟糕。**

> ⚠️ **絕不可把 `dbt build` 拆成 `dbt run` + `dbt test`。** 那會讓 `int_` 的上游變成「staging 的 **run**」而非「staging 的 **test**」，於是 Hard Gate 靜默地停止阻擋，而髒資料流進 Gold。由 `tests/test_dags.py::test_dbt_never_splits_run_and_test` 釘住。

**重試刻意不對稱**：`extract_* = 2`（暫時性失敗）、`dbt_* = 0`（確定性失敗）。BigQuery 的暫時性錯誤由 adapter 層的 `job_retries: 1` 處理——**比重跑整個 `dbt build` 精確得多**。[ADR-0038](../adr/0038-asymmetric-retries.md) · [ADR-0040](../adr/0040-layered-dbt-execution.md)

---

## 5. 排程語意

**`catchup=False` 是結構性的。** watermark 由目的地推導，所以為過去某日期而跑的補跑，抽取的仍是「截至現在的增量」——N 次補跑會做同一件事 N 次。**這不是一個可補跑的 DAG**，而要把它變成那樣，需要一個會切掉遲到列的右時間界，牴觸 `>=` 的語意。

**`max_active_runs=1` 是正確性**：併發的 dbt `insert_overwrite` 對同一批分區會互相覆蓋。

**`dq_reevaluation` 的 `schedule=None`。** Proposal B 由一次規則放寬觸發——那是人為的部署事件，不是週期。**排程屬於會自己改變的東西；規則不會。** [ADR-0037](../adr/0037-catchup-false-structural.md)

### ⚠️ 跨時區抽取：未解

排程以 `Asia/Taipei` 宣告（灌資料在 10/13/17/21，抽取在 22:30），但 `received_at` 是 TIMESTAMP 而 BigQuery 按 **UTC** 日分區——`date()` 的換日點差八小時。

**這個錯位目前是不可見的**，因為四個灌資料時段全都落在台北 08:00 到 24:00 之間，對應到同一天的 UTC 00:00–16:00。**那是「我們挑的時段」的後果，不是系統的性質**：在全天候攝入下，台北 00:00 到 08:00 之間下的訂單會落進**前一個** UTC 分區。

影響半徑沿著一條有意義的線分開：

| | 時間軸 | 受影響 |
|---|---|---|
| `staging.orders` 分區 → Hard Gate 的「最新批次」判定 | `received_at` | ✅ 是 |
| `rpt_quality_events_daily.event_date` | `event_at` | ✅ 是 |
| source freshness 的近期窗口過濾 | `received_at` | ✅ 是 |
| `fct_orders` / `fct_order_items` / `rpt_sales_daily_by_category` | `order_date` | ❌ 否 |

`order_date` 來自 payload 而且本來就是 `DATE`——它沒有時區。換句話說：

> **營收數字是對的。「品質屬於哪一天」差了八小時。**

那個區分決定了要不要修、什麼時候修：BI 那側的營收可以照讀，**而 DQ 儀表板會把一次跨邊界的事故歸給錯誤的日期。**

### 三個候選方案

**沒有一個被採用，而且沒有一個是純技術決策：**

| | 方案 | 代價 |
|---|---|---|
| **a** | 用業務時區的 `DATE` 為 staging 分區 | 語意上最正確；需要**重建並回填**整張表 |
| **b** | 保留 UTC 分區，在 `stg_` 推導一個 `business_date` 供下游使用 | 不動分區，但**多一個定義需要被維護的欄位** |
| **c** | 明確宣告品質指標是以 UTC 日為基準，並把它寫進報表定義 | **零成本**，但要求報表讀者接受一個與營運日不一致的粒度 |

**刻意未選**：在目前的攝入模式下**三者產出完全相同**——見上面的「目前是不可見的」。**在有跨日界的真實流量之前，任何選擇都無法被驗證。** [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)

---

## 6. 觀察訊號

**一個觀察訊號既沒有阻斷下游的權限，也沒有污染另一個 DAG 成功率的權限。** 一次 DAG run 的狀態是它所有 task 的聚合，所以一個預期會紅的葉節點會讓「主管線成功率」變得毫無價值。

因此每個 DAG 的紅燈只代表一件事：

| DAG | 紅燈代表 | 去看 |
|---|---|---|
| `seed_demo_daily` | 什麼都進不來 | API、seeding 腳本 |
| `raw_pending_watch` | 資料進得了 Raw，沒人認領 | redis／worker／beat |
| `orders_analytics_daily` | 管線壞了 | extract 或 dbt |
| `source_freshness_watch` | staging 沒被往前推 | watermark 與 extract |

由 `tests/test_dags.py::TestFreshnessIsolation` 釘住。

**三條時間線，各覆蓋一跳**——合併之後，一個紅燈會代表兩段管線：

| 時間線 | 跳 | 誰在看 |
|---|---|---|
| `raw.received_at` | 上游 + API | OTel（absent 告警未寫） |
| `raw.received_at` → `ods.received_at` | 派工 | `raw_pending_watch` |
| staging 裡的 `ods.received_at` | extract | `source_freshness_watch` |

**`source_freshness_watch` 排在 08:00 因為它是後備**：如果 extract 回報成功卻什麼都沒搬，Hard Gate 判的是昨天的分區、會通過，`dbt test` 也是綠的——這是唯一一個會在有人 09:00 打開報表之前出聲的東西。它的 26h／50h 門檻是一個**載入週期**加兩小時寬限。

⚠️ **「有 Raw 卻沒有對應的 ODS」不能作為故障的定義**——`duplicate` 與 `error` 是正確的終端狀態、本來就不產生 ODS 列。**`pending` 的年齡才是乾淨的訊號。** [ADR-0039](../adr/0039-observation-signals-own-dag.md)

---

## 7. 失敗通知

四個排程 DAG 各帶一個 `on_failure_callback`，其訊息陳述的是**該做什麼**而非任務名——那個資訊以前只住在 docstring 裡，**而處理事故的人不會去讀 docstring**。

**掛在 task 層級**，因為下游 `upstream_failed` 的 task 不會觸發 callback：一條斷掉的七任務鏈恰好送出一則訊息，並點名真正壞掉的那個 task。

**傳輸預設是一行 log**；真實通道離這裡只有一個 `NOTIFY_WEBHOOK_URL`。每則訊息都帶 `channel=`，所以 `channel=log` 明白地說出沒有人被通知。

⚠️ **只涵蓋「跑了而且失敗」。** 該跑沒跑（Airflow 3 移除了 SLA）、機器關機、以及 freshness 的 `warn`（exit 0、task 是綠的），對它全都不可見。[ADR-0042](../adr/0042-failure-notification-response-not-task.md) · [liveness-alerting](./liveness-alerting.md)

---

## 8. 基礎設施

**Airflow 的 metadata DB 是與業務 DB 分離的實例。** 理由不是潔癖：業務 DB 是 Proposal C「從 Raw 重建」的前提，而還原它時不該連帶回滾 Airflow 的執行歷史。

**LocalExecutor**——單機、少量 task。CeleryExecutor 會多兩個容器加一個 broker，而那個 Redis 會在概念上與攝入路徑的撞在一起。

**版本**：Airflow 3.0.0（`apache/airflow:3.0.0-python3.12`）、dbt-core／dbt-bigquery 1.11。升級 Airflow 時，`orchestration/Dockerfile` 的 `ARG AIRFLOW_VERSION` 與 `.github/workflows/dags.yml` 的 `AIRFLOW_VERSION` 必須一起改——constraints 檔是按版本抓的。

---

## 9. 相關

- [cloud-layer](./cloud-layer.md) · [transformation](./transformation.md) — DAG 執行的是什麼
- [data-quality](./data-quality.md) — `dq_reevaluation` 驅動的 Proposal B 機制
- Runbook：`airflow-startup`、`airflow-silent-stall`、`proposal-b-rollout`（第 4 階段）
