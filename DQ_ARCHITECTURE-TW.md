# 資料品質控管架構

## 設計目標

確保進入分析層（Star Schema）的資料是最乾淨的。  
ODS 作為不可變的錨點，保留完整資料狀況；品質管控的責任隨資料往下游流動而逐步收緊。

---

## 各層品質合約（Q0）

```
Raw (PostgreSQL)
  職責：完整落地所有進來的請求，不做任何品質假設
  品質要求：無
  可修改：否

ODS (PostgreSQL)                               ← Bronze / 錨點
  職責：基本清洗與業務規則驗證，保留完整資料狀況
  品質要求：基本格式標準化；業務問題只標記，不攔截
  可修改：否（攝入當下的快照，永遠不被下游修改）

BQ staging（Airflow 抽取）
  職責：搬運 ODS，增量上傳
  品質要求：與 ODS 相同，純鏡像

dbt stg_*                                      ← Silver 入口
  職責：1:1 對應來源，型別對齊、欄位命名標準化
  品質要求：與 ODS 相同，仍保留所有資料含髒的
  附加：source freshness test、schema 基本測試

────────────── 攔截發生在這裡 ──────────────

dbt int_*                                      ← Gold 入口
  職責：跨表 join、衍生欄位、業務邏輯
  品質要求：只讓乾淨資料通過（has_clean_error = FALSE）
  髒資料去向：int_orders_quarantine

dbt dim_*/fct_*                                ← Gold
  職責：Star Schema，供下游彈性查詢
  品質要求：最乾淨，不含任何 has_clean_error=TRUE 的記錄

dbt rpt_*
  職責：固定粒度預聚合，BI Dashboard 直接使用
  品質要求：同 dim_*/fct_*
```

---

## 攔截機制（Q1）

兩個機制並用，覆蓋不同層次的問題。

### 機制一：Hard Gate（run-level）

在 `dbt stg_*` 掛載測試。測試失敗 → 整個 dbt run 中止，`int_*/dim_*/fct_*` 不更新，保留上一次的乾淨狀態。

```yaml
# stg_orders.yml
models:
  - name: stg_orders
    tests:
      # 嚴重：關鍵欄位全空 → 整批資料無意義，直接擋
      - not_null:
          column_name: order_id
          severity: error

      # 嚴重：整批錯誤率過高 → 可能是 source 系統異常
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.1"
          severity: error     # > 10% 擋住

      # 警告：錯誤率偏高但尚可接受 → 繼續跑但發告警
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.05"
          severity: warn      # > 5% 告警
```

### 機制二：Row Filter（record-level）

在 `dbt int_*` 的 SQL 中過濾，逐筆隔離。

```sql
-- int_orders.sql（乾淨資料流）
SELECT * FROM {{ ref('stg_orders') }}
WHERE has_clean_error = FALSE

-- int_orders_quarantine.sql（品質問題資料）
SELECT
    *,
    CURRENT_TIMESTAMP() AS quarantined_at
FROM {{ ref('stg_orders') }}
WHERE has_clean_error = TRUE
```

### 機制三：場景專用分析模型（int_* 層）

特定分析場景可在 `int_*` 層建立場景專用模型，透過查詢 `clean_error_message` JSONB，接受全局 quarantine 中與該場景無關的錯誤，並在模型內對問題欄位補值：

```sql
-- int_orders_shipping_analysis.sql
SELECT
    *,
    CASE
        WHEN clean_error_message @> '["customer_rating 應在 1~5 之間"]'
        THEN NULL
        ELSE customer_rating
    END AS customer_rating_cleaned
FROM {{ ref('stg_orders') }}
WHERE
    has_clean_error = FALSE
    OR (
        -- 唯一問題是 rating，對出貨分析無關
        ARRAY_LENGTH(clean_error_message) = 1
        AND clean_error_message @> '["customer_rating 應在 1~5 之間"]'
    )
```

**審計軌跡**：補值邏輯與轉換邏輯共存於 SQL 檔案，dbt model description 記錄該場景接受哪些錯誤及原因。SQL 本身即審計軌跡，進 version control。

---

## 被攔截資料的處理（Q2）

### Quarantine 的對象

Row Filter 過濾出的記錄：`Raw.status = "processed"` 且 `has_clean_error = TRUE`。  
這些記錄**已在 ODS 中**，只是在 `int_*` 層被隔離，不流入 `dim_*/fct_*`。

### Remediation：A + B 並用

A 和 B 修的是**不同類型**的問題，需明確知道用哪個：

| | 修什麼 | 路徑 |
|---|---|---|
| **A：force=True** | `Raw.status = "error"` 或 `"duplicate"` 的記錄（從未成功寫入 ODS） | `POST /process_raw/{raw_id}?force=true` → 重跑 pipeline → 自然流入下游 |
| **B：Airflow 重評估** | `Raw.status = "processed"` + `has_clean_error = TRUE` 的記錄（在 ODS，但被 quarantine） | 用新版規則重評估 → 寫 `quality_events` → 下次 dbt run 自然流入 `int_*` |

> **注意**：`force=True` 對 quarantine 記錄無效（status="processed" → 回 400）。  
> Quarantine 記錄的問題是**規則評估**，不是 pipeline 失敗，不需要重跑 pipeline。

### 狀態機

```
initial_evaluation
  ├── 通過所有規則           → to_state: "clean"
  └── has_clean_error=TRUE  → to_state: "quarantined"

quarantined
  ├── B 重評估，新規則通過   → to_state: "promoted"
  └── 人工決定放棄           → to_state: "permanently_rejected"

promoted
  └── 規則變嚴後重評估失敗   → to_state: "re_quarantined"（邊緣情況）
```

---

## 版本號與 quality_events 表（Q2 延伸）

### 規則版本號

```python
# clean.py
DQ_RULE_VERSION = "v1"    # 每次規則改動時 bump，搭配 git tag 記錄變更內容
```

### ODS 新增欄位

```
ODS（新增）
├── dq_rule_version: String    ← 攝入時使用的規則版本，之後永遠不動
```

### quality_events 表（PostgreSQL，append-only）

記錄每筆資料的品質生命週期。只增不改，是狀態機的事件日誌。

```
quality_events
├── id:           Integer (PK)
├── raw_id:       Integer
├── order_id:     String
├── event_type:   String     "initial_evaluation" | "promotion" | "rejection"
├── from_state:   String?    null | "quarantined" | "promoted"
├── to_state:     String     "clean" | "quarantined" | "promoted" | "permanently_rejected"
├── rule_version: String     "v1" | "v2" | ...
├── event_at:     DateTime
└── reason:       JSONB?     list[str]，與 ODS.clean_error_message 相同格式
```

**寫入時機：**
- `process.py` 成功寫 ODS 後 → 寫一筆 `initial_evaluation` 事件
- Airflow 重評估促進記錄後 → 寫一筆 `promotion` 或 `rejection` 事件

**語意邊界：**
此表嚴格限定為全局狀態機，只記錄攝入層事件與跨層（PG → BQ）的 Proposal B 評估事件。BQ 內場景專用模型的補值決策**不寫回**此表——場景補值屬於分析層內部的業務邏輯，不是資料品質狀態的演進。

---

## 資料一致性處理

### ODS 與 BQ 的品質狀態分歧

分歧**會存在**，來源有兩種，處理方式不同：

**情境一：規則版本演進造成的分歧**
透過 `dq_rule_version` + `quality_events` 可追溯：

```
無版本號時：
  ODS has_clean_error=TRUE，BQ dim_* 是乾淨的 → 「為什麼不一樣？」無從解釋

有版本號與 quality_events 時：
  ODS has_clean_error=TRUE, dq_rule_version="v1"   ← 攝入當下的真實
  quality_events: promoted under "v2" at 2026-03-01 ← 後續演進的真實
  → 分歧有解釋，可追溯
```

**情境二：場景專用模型造成的分歧**
全局 quarantined 的記錄可能出現在特定場景的 `dim_*/fct_*` 中（因場景模型接受了與該場景無關的錯誤）。此分歧透過閱讀對應場景模型的 SQL 與 dbt description 說明，不建立獨立追蹤表。

此為有意識的設計邊界：場景補值的可解釋性需求屬靜態審計（讀 code），非運行時稽核需求，SQL 文件就夠。

### Bounded Writeback 原則

BQ（或 Airflow 讀 BQ 後）的寫回對象**只有 `quality_events`**，不修改 ODS 本身。

```
❌ 禁止：BQ → 修改 ODS 欄位（破壞錨點語意）
✅ 允許：BQ → 寫 quality_events（品質演進的 audit log，為此設計）
```

---

## 歷史品質指標（Q3）

### 層次一：即時運維指標（分鐘級）

structlog 已有基礎，延伸以下 log 事件：

```python
# process.py，寫 ODS 後
logger.info("quality_metric",
    rule_version=DQ_RULE_VERSION,
    has_clean_error=has_clean_error,
    order_id=ods_order.order_id,
    error_fields=clean_error_message,
)
```

接 Grafana（Phase 4 OTel/Loki）→ 即時 error rate、Hard Gate 觸發告警。  
不需要新元件，structlog 基礎已在。

### 層次二：批次分析指標（日/週）

`quality_events` 由 Airflow 與 ODS 一起抽取進 BQ，dbt 建立 `rpt_quality_*` 模型：

```
rpt_quality_daily
├── date, rule_version
├── total_count, clean_count, quarantine_count, promoted_count
├── quarantine_rate, promotion_rate
└── 可按 rule_version 切片，比較版本間的品質差異

rpt_quality_field_breakdown
├── 哪些欄位最常觸發 has_clean_error
├── 每個欄位的 error rate 趨勢
└── 來源：clean_error_message（JSONB array），直接 UNNEST 取欄位名稱，不需 parse 文字

rpt_quality_version_comparison
├── v1 攔截了多少 → v2 促進了多少 → 目前仍在 quarantine 多少
└── 規則改動的實際效果量化
```

接 Looker Studio，供長期趨勢分析。

### 歷史指標為何不會被追溯性改寫

`quality_events` 是 append-only，歷史事件永遠存在：

```sql
-- v1 下的初始 quarantine rate（永遠不變）
SELECT countif(to_state = 'quarantined') / count(*) AS quarantine_rate
FROM quality_events
WHERE event_type = 'initial_evaluation' AND rule_version = 'v1'

-- v2 促進了多少（另一個獨立指標，不覆蓋 v1 數字）
SELECT count(*) AS promoted_by_v2
FROM quality_events
WHERE event_type = 'promotion' AND rule_version = 'v2'
```

---

## 實作範圍對應

### ODS / PostgreSQL 層

| 元件 | 所在層 | 狀態 |
|---|---|---|
| `format_clean()` + `business_clean()` | ODS 寫入前 | ✅ 已完成 |
| `has_clean_error` + `clean_error_message` | ODS | ✅ 已完成 |
| `DQ_RULE_VERSION` 常數（`clean.py`） | ODS | ✅ 已完成 |
| `dq_rule_version` 欄位（ODS model） | ODS | ✅ 已完成 |
| `QualityEvent` model + `quality_events` 表 | ODS | ✅ 已完成 |
| `quality_events` 寫入邏輯（`process.py` success path） | ODS | ✅ 已完成 |
| structlog `quality_metric` 事件（`process.py`） | ODS | ✅ 已完成 |

**quality_events 寫入語意**

| 情況 | 寫入？ | to_state |
|---|---|---|
| ODS 成功寫入，無品質問題 | ✅ | `clean` |
| ODS 成功寫入，有品質問題 | ✅ | `quarantined` |
| pre-check 攔截 duplicate（ODS 未寫） | ❌ | — |
| TOCTOU IntegrityError（ODS 未寫） | ❌ | — |
| pipeline 失敗 → Raw status=error（ODS 未寫） | ❌ | — |

### BQ Analytics 層

| 元件 | 所在層 | 狀態 |
|---|---|---|
| dbt `stg_*` Hard Gate tests | BQ Analytics | ⬜ Phase 4 |
| `int_orders` Row Filter（`WHERE has_clean_error = FALSE`） | BQ Analytics | ⬜ Phase 4 |
| `int_orders_quarantine` dbt model | BQ Analytics | ⬜ Phase 4 |
| 場景專用 `int_orders_*` 模型（JSONB 過濾 + 補值） | BQ Analytics | ⬜ Phase 4 |
| Airflow 重評估 task（Proposal B） | BQ Analytics | ⬜ Phase 4 |
| `rpt_quality_*` dbt 模型 | BQ Analytics | ⬜ Phase 4 |

---

## 已知邊界與設計決策

**A/B Remediation 的邊界必須明確**  
`force=True`（A）只能用於 `Raw.status = "error"` 或 `"duplicate"` 的記錄。  
Quarantine 記錄（`has_clean_error=TRUE`，`status="processed"`）只能走 Proposal B 重評估路徑。  
混用會導致 `force=True` 回 400 且無法診斷原因，需在操作文件中明確說明。

**ODS 與 BQ 品質狀態永久分歧**  
ODS 永遠反映攝入當下的品質評估（以 `dq_rule_version` 記錄使用的規則版本）。  
BQ `dim_*/fct_*` 反映目前最新評估下的乾淨狀態。  
這個分歧是有意的設計決策，透過 `dq_rule_version` + `quality_events` 提供追溯能力。

**Hard Gate 閾值為業務判斷**  
目前建議值：error rate > 10% 擋住，> 5% 告警。  
實際閾值應在有真實流量資料後調整，初始值為保守估計。

**`quality_events` 目前不覆蓋 BQ 層的促進事件**  
Proposal B（Airflow 重評估）尚未實作。目前 `quality_events` 只有攝入時的 `initial_evaluation` 事件。  
Phase 4 Airflow 建立後，促進（`promotion`）與永久拒絕（`rejection`）的事件寫入邏輯需一併補上。

**場景補值審計軌跡為 SQL 文件**  
場景專用 `int_*` 模型的補值邏輯記錄在 dbt 模型 SQL 與 model description，不建立獨立追蹤表。前提是無跨系統運行時稽核的實務需求；若未來出現此需求，再評估是否引入 BQ 層生命週期表。

**`stg_*` boolean 欄位延後**  
目前場景模型直接查詢 `clean_error_message` JSONB 字串。當錯誤條件需跨多個場景模型維護、或 `clean_error_message` 措辭異動的維護成本明顯上升時，再於 `stg_orders` 拆解為結構化 boolean 欄位（如 `has_rating_error`），將格式耦合集中到單一地方。
