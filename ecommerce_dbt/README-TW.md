# ecommerce_dbt —— 訂單分析轉換層

[English](./README.md) | **繁體中文**

這條管線的 dbt 專案。**這份檔案是在這個目錄工作時的入口**——設計論述住在上一層。

| 你想要 | 讀 |
|---|---|
| 各層如何運作、為何是那個形狀 | [docs/zh-TW/design/transformation.md](../docs/zh-TW/design/transformation.md) |
| 某個決策為何如此 | [ADR-0043 – 0049](../docs/zh-TW/adr/README.md) |
| build 紅掉時該做什麼 | [runbooks/dbt-ops](../docs/zh-TW/runbooks/dbt-ops.md) |
| 加或刪一個欄位 | [runbooks/schema-change](../docs/zh-TW/runbooks/schema-change.md) |
| 每個測試守什麼 | [design/testing §6](../docs/zh-TW/design/testing.md) |

---

## 1. 範圍

這一層只擁有 **T**。抽取進 BigQuery staging 是 `extract_ods_to_bq.py` 的職責（[design/cloud-layer](../docs/zh-TW/design/cloud-layer.md)）；排程是 Airflow 的（[design/orchestration](../docs/zh-TW/design/orchestration.md)）。

```
staging.orders  ──►  stg_*  ──►  int_*  ──►  dim_*/fct_*  ──►  rpt_*
staging.quality_events ──┘        ▲
                                  └── 阻斷發生在這裡，也只發生在這裡
```

---

## 2. Quickstart

### 前置：`~/.dbt/profiles.yml`（不進版控）

```yaml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/your/sa-key.json
      project: <your-gcp-project-id>   # 真實 ID 不進版控
      dataset: dbt_dev
      location: US                     # 所有 dataset 一致在 US
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1                   # adapter 層重試——見 ADR-0038
```

> ⚠️ **Airflow 用的是另一份 profile**，在 `orchestration/dbt_profiles/`，由 `DBT_PROFILES_DIR` 指向。它刻意不放在這裡：dbt 的尋找順序把工作目錄排在 `~/.dbt` 之前，放在這個目錄會讓本機 `dbt run` 吃進它，並因環境變數未設而失敗。→ [ADR-0041](../docs/zh-TW/adr/0041-profiles-yml-structure-vs-values.md)

### 常用指令

```bash
dbt deps                                        # 安裝套件（dbt_utils）
dbt run    --select stg_orders                  # 建置模型（增量）
dbt run    --select stg_orders --full-refresh   # 全量重建
dbt test   --select stg_orders                  # 跑測試，含 Hard Gate
dbt source freshness                            # source freshness
dbt build  --select stg_orders                  # run + test 一起
```

> ⚠️ **絕不可在 DAG 裡把 `dbt build` 拆成獨立的 `dbt run` 與 `dbt test`。** 那會讓 `int_` 的上游變成「staging 的 **run**」而非「staging 的 **test**」，於是 Hard Gate 靜默地停止阻擋，而髒資料流進 Gold。由 `tests/test_dags.py::test_dbt_never_splits_run_and_test` 釘住。→ [ADR-0040](../docs/zh-TW/adr/0040-layered-dbt-execution.md)

---

## 3. 分層與命名

| 前綴 | 粒度 | 職責 | 品質要求 |
|---|---|---|---|
| `stg_` | 來源 | 1:1 對應、改名、轉型、去重。**不含業務邏輯** | 與 ODS 相同——保留所有列，含髒的 |
| `int_` | 來源 | join、衍生欄位、**阻斷點** | 只有有效乾淨的列通過；其餘 → 隔離區 |
| `dim_`／`fct_` | 星型結構 | 供彈性分析的維度與事實 | 最乾淨的一層——不存在髒資料 |
| `rpt_` | 固定 | 供 BI 的預先聚合 | 與 Gold 相同 |

**12 個模型**：`stg_orders` · `stg_quality_events` · `int_orders` · `int_orders_quarantine` · `int_order_items` · `dim_customer` · `dim_product` · `fct_orders` · `fct_order_items` · `rpt_quality_events_daily` · `rpt_quality_backlog` · `rpt_sales_daily_by_category`

---

## 4. 編輯之前要知道的兩件事

**① `int_orders` 與 `int_orders_quarantine` 共用一段逐位元組相同的 CTE 區塊**，以 `═══` 標記圍住，刻意重複而非共用。改動任一個之前，必須先走那份**七項對齊清單**——見 [runbooks/dbt-ops](../docs/zh-TW/runbooks/dbt-ops.md)。

最多人漏掉的那一項：拿掉 `coalesce(..., false)` 會讓一列**同時從兩張表消失**，而且是靜默的，因為 `FALSE OR NULL = NULL`，而 `WHERE NOT NULL` 也是 NULL。

**② `assert_orders_split_is_partition` 絕不可被降級或 `--exclude`。** 它是那段重複唯一的自動化安全網。→ [ADR-0045](../docs/zh-TW/adr/0045-int-effective-state-duplication.md)

---

## 5. 依賴

- dbt-core **1.11** / dbt-bigquery **1.11**
- `packages.yml`：`dbt-labs/dbt_utils >=1.1.0,<2.0.0`（解析為 1.4.1）

> ⚠️ 若 `dbt_packages/` 變成空的、而 `dbt deps` 回報 `not a gzip file`，很可能是編輯器擴充在背景執行 `dbt deps` 並撞上速率限制。→ [incidents/2026-08-dbt-deps-429](../docs/zh-TW/incidents/2026-08-dbt-deps-429.md)

---

## 6. 現況

上述全部已建置。已設計但刻意未啟用的——情境專用 `int_orders_*`、SCD2 `dim_customer`、`rpt_sales_*` 增量化、金額曝險度量——在 [STATUS](../docs/zh-TW/STATUS.md) 與 [PORTFOLIO_SCOPE](../docs/zh-TW/PORTFOLIO_SCOPE.md)，各自附有 trigger。
