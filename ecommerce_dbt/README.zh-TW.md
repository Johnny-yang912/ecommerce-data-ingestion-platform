# ecommerce_dbt — 訂單分析轉換層（dbt）

> 語言：**繁體中文** ｜ [English](./README.md)

BQ staging 之後的 **T（轉換）層**：`stg_ → int_ → dim_/fct_ → rpt_`。本文件只談 **dbt 專案的操作與實作決策**；分層的**品質契約與語意**（Hard Gate、Row Filter、quarantine、Proposal B/C、quality_events）見 [DQ_ARCHITECTURE-TW](../DQ_ARCHITECTURE-TW.md)，staging **基建**（分區/叢集/保險絲/watermark、ODS→BQ E/L）見 [CLOUD_LAYER-TW](../CLOUD_LAYER-TW.md)。

## 1. 定位與邊界

```
ODS (PostgreSQL) ──[E/L：Python]──► BQ staging ──[T：dbt（本專案）]──► stg_/int_/dim_/fct_/rpt_
                    CLOUD_LAYER                     ← 你在這裡 →
```

本專案**只做 T**：讀 `staging.orders`（E/L 已落地的 1:1 鏡射），輸出到 `dbt_dev`（dev target）。不碰抽取、不碰 ODS。

## 2. 快速上手

### 前置：`~/.dbt/profiles.yml`（不入版控）

```yaml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/your/sa-key.json
      project: <your-gcp-project-id>   # 真實 ID 不寫進 repo（比照 CLOUD_LAYER §4）
      dataset: dbt_dev
      location: US                     # 所有 dataset 一致在 US
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1
```

### 常用指令

```bash
dbt deps                              # 安裝套件（dbt_utils）
dbt run    --select stg_orders        # 建模型（增量）
dbt run    --select stg_orders --full-refresh   # 全量重建（見 §6）
dbt test   --select stg_orders        # 跑測試（含 Hard Gate）
dbt source freshness                  # source 新鮮度
dbt build  --select stg_orders        # run + test 一起
```

## 3. 分層與命名慣例

| 層 | 前綴 | 職責 | 品質要求 |
|---|---|---|---|
| Silver 入口 | `stg_` | 1:1 對應來源、型別對齊、命名標準化、**去重** | 保留所有資料含髒的；掛 Hard Gate |
| Gold 入口 | `int_` | 跨表 join、衍生欄位、**Row Filter 攔截** | 只讓乾淨資料通過 |
| Gold | `dim_`/`fct_` | Star Schema | 不含 `has_clean_error=TRUE` |
| 報表 | `rpt_` | 固定粒度預聚合 | 同 Gold |

- 命名採 `stg_orders`（沿用專案既有文件），非 dbt 官方 `stg_<source>__<entity>`。
- 檔案組織：`models/staging/` 內 `_staging__sources.yml`（source 定義）、`stg_orders.sql`、`stg_orders.yml`（測試/描述）。

## 4. 實作決策（`stg_orders`）

### 4.1 建 table（實體表）而非 view

物化決策分兩步：**先決定「實體化 vs 虛擬」，再決定「實體化怎麼切」**。這一節談前者（後者見 §4.2 incremental）。`stg_orders` 選實體表，四個理由：

| 面向 | view（虛擬） | table / incremental（實體，本專案選用） |
|---|---|---|
| 保險絲傳染 | view 只是存起來的 SQL，下游查它、若沒帶 `received_at` 過濾，會**穿透**到底層 staging 的 `require_partition_filter` 而被 400 | 實體表**切斷**這條鏈：`stg_orders` 是不帶保險絲的物理表，下游 `int_`／Hard Gate 測試可自由查 |
| 去重成本 | 每個下游查詢都**重算一次**去重視窗函數；多個下游 → 同一份去重每次 run 重複 N 次 | 去重**只算一次**、落地，下游讀現成結果 |
| 一致快照 | 各下游查詢時各自即時重讀 append-only staging，遇 E/L 併發載入可能看到不同狀態 | 全下游站在同一份跑批當下凍結的快照上 |
| 增量可能性 | 無法增量——每次查詢都等於全表重算，成本控制無從談起 | 只有實體、分區表能做 partition 級 `insert_overwrite`（見 §4.2）|

1. **切斷保險絲傳染**：這是最關鍵的一點。staging 的 `require_partition_filter=True` 是刻意設的燒錢保險絲；若 `stg_orders` 是 view，這個約束會沿著 view 傳染給**每一個**下游消費者，逼他們都得記得帶 `received_at` 過濾。實體表把保險絲擋在 `stg_` 這一層，下游回歸乾淨。
2. **去重只付一次錢**：去重是 `stg_` 的核心工作，且下游會有多個模型讀它。view 會讓這份計算隨下游數量線性重複；實體化是「算一次、大家共用」。
3. **DAG 根節點的一致快照**：`stg_` 是轉換 DAG 的根、被多個下游共讀。物化成 table 讓所有下游站在同一份跑批當下凍結的快照上，免疫於 append-only staging 的併發載入，保住單次 run 的內部一致與可重現。注意：這是「**一致性**」而非「**耐久性**」——資料的錨點仍是 ODS（見 [DQ_ARCHITECTURE-TW](../DQ_ARCHITECTURE-TW.md)），`stg_` 隨時可從 staging 重建。
4. **增量的前提**：本專案要的「成本不隨歷史成長」靠 partition 級增量替換，而增量**必須**是實體分區表——view 沒有實體分區可換，天生做不到。

> 附帶說明：dbt 主流慣例其實是把 staging 模型物化成 **view**（輕量鏡射、省儲存）。本專案**刻意偏離**慣例改用 table，靠的正是上述局部技術壓力，而非「鏡射就該穩固」這種一般原則。
>
> 代價：實體表要佔儲存、且資料有物化延遲（跑批後才更新）。對 `stg_` 都可接受——BQ 儲存極廉價，且下游本就在 dbt run 的批次節奏上消費，不需要 view 的「即時反映最新」。

### 4.2 物化：`incremental` + `insert_overwrite`

依 `received_at(DAY)` 分區，例行跑批只重算「回看窗」內的近期分區，成本 ∝ 近期資料、不隨歷史總量成長。正確性靠不變式：**同一 `raw_id` 的所有重複副本都落在同一 `received_at` 分區**（`>=` 重抽與 Proposal C 修正列都保留原 `received_at`）→ 整分區替換不會漏。

### 4.3 `copy_partitions: true` ⭐（sandbox 禁 DML 的繞道）

本 BQ 專案為 **sandbox（未啟用帳單）**，**禁止 DML**。`insert_overwrite` 預設用 `MERGE`（DML）→ 例行增量會報 `DML queries are not allowed in the free tier`。解法是在 `partition_by` 內加 `copy_partitions: true`，改用 **copy job（非 DML、免費）**整分區覆寫。

> ⚠️ **這條限制對未來所有 `int_/dim_/fct_` 增量模型同樣適用**。`--full-refresh` 走 `CREATE OR REPLACE`（DDL）不受限。啟用帳單後可移除此選項改回 MERGE，但 copy job 對「整分區替換」語意更貼切且免費，無移除誘因。

**copy job vs MERGE**：兩者都達成「替換受影響的分區」，差別在替換發生的層級——

| | copy job（`copy_partitions=true`） | MERGE（預設） |
|---|---|---|
| 操作層級 | 儲存層分區複製（`表$YYYYMMDD` + `WRITE_TRUNCATE`）| 查詢引擎 DML（掃描→刪→插）|
| 是不是 DML | 否 | 是 |
| 計費 | 免費 | 按掃描量計費 |
| sandbox | ✅ 可 | ❌ 禁 |
| 適用 | 產出「該分區完整內容」、整塊換 | 產出「要 upsert 的子集」、逐列併入 |

我們去重後產出的是「該日完整內容」，故整分區複製語意正確且更省。

### 4.4 回看窗：`var('stg_orders_lookback_days', 3)`

預設 3 天，須 ≥ E/L 的 `>=` 重抽範圍 + 安全邊際。臨時調整不需改檔：

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: 7}'
```

### 4.5 去重

`row_number() over (partition by raw_id order by received_at desc, id desc) = 1`。

- 分組鍵 `raw_id`（非 `id`/`order_id`）：ODS `raw_id` 為 UNIQUE、1:1，且 Proposal C 遷移式修正列為「新 `id`、同 `raw_id`」，唯有以 `raw_id` 分組才能讓修正列與 staging 舊副本競爭。
- 決勝鍵：目前重複列 byte-identical，順序無所謂。**Proposal C 的 `rebuild_batch_id` 落地後，插到 `order by` 最前**：`rebuild_batch_id desc nulls last, received_at desc, id desc`。

### 4.6 保險絲（`require_partition_filter`）的接觸點

1. **全量路徑**（full-refresh）需帶哨兵 `where received_at >= timestamp('1970-01-01')` 才不被保險絲 400。
2. **source freshness** 底層是 `SELECT MAX(received_at)`，會被保險絲擋 → source 的 `freshness.filter` 用近窗過濾繞過。
3. `stg_orders` 本身**不設** `require_partition_filter`，下游 `int_` 與 Hard Gate 測試可自由查詢。

### 4.7 `on_schema_change: append_new_columns` ⭐（加欄免全量重抽）

**問題**：`stg_orders` 最終投影是顯式清單（§4.5「改名接縫」），新增一個下游欄位得改清單。但預設 `on_schema_change='ignore'` 下，增量跑批不會把新欄加進既有表——唯一辦法是 `--full-refresh`，在大表上就是全表掃 + 全分區重寫、按掃描量計費。加一欄付一次全表的錢，不划算。

**解法**：改用 `append_new_columns`。加欄時 dbt 先 `ALTER TABLE ADD COLUMN`（metadata、免費、既有列自動 NULL），再只覆寫回看窗分區——舊分區原地不動、停在 NULL。成本 ∝ 近期資料。這是 staging 端 `ALLOW_FIELD_ADDITION`（[CLOUD_LAYER-TW §5.2](../CLOUD_LAYER-TW.md)）的鏡像：兩層對稱地「加 nullable 欄、舊資料 NULL、原地擴充」。

**與 `insert_overwrite` + `copy_partitions` 相容**（已對 dbt-bigquery 1.11 原始碼查證）：materialization 在跑 copy job 前先建 tmp table 做 `process_schema_changes`，偵測到新欄即 ALTER 目標表，再用**同一張** tmp table 走 copy_partitions 覆寫回看窗分區。copy_partitions 本來每次就要建 tmp table，故啟用此選項的穩態額外成本 ≈ 一次 metadata 欄位比對，可忽略；無 schema 變動時不 ALTER。

**為何 `append_new_columns` 而非 `sync_all_columns`**：

| | `append_new_columns`（本專案選用） | `sync_all_columns` |
|---|---|---|
| 加欄 | ✅ `ALTER ADD` | ✅ `ALTER ADD` |
| 刪欄 | 不動，欄留著 | **`DROP` 欄** |
| 改型別 | 不動 | `ALTER` 型別 |

`sync_all_columns` 的 DROP 會牴觸「staging 只做加法、刪欄留 legacy 欄保歷史」（§5.2/§5.3）。`append_new_columns` 天生只加不刪，正好對齊。

**觸發閘門＝顯式清單，故不吃 drift**：`check_for_schema_changes` 比對的是 model 產出的欄位（顯式清單），不是底層 staging。staging 靠 `ALLOW_FIELD_ADDITION` 自動長的欄，在你把它加進顯式 SELECT 前偵測不到 → 不會自己 ALTER。此選項**只在刻意改清單（進 git、被 review）時觸發**，顯式紀律原封不動。

**界線**（救不了的，仍走 §6 runbook）：
- **歷史回填**（值要連舊分區一起有，如 Proposal C 重建）：它只 NULL 補舊分區、只寫回看窗 → 舊分區真值仍需 targeted refresh。
- **改型別 / 改名 / 改分區**：仍 `--full-refresh` / 重建表。

## 5. 測試策略

| 測試 | 對象 | severity | 說明 |
|---|---|---|---|
| `error_rate_below`（自訂 generic test）| 整批 `has_clean_error` 比率 | error @10% / warn @5% | **Hard Gate**（DQ 機制一）。不能用 `dbt_utils.expression_is_true`（逐列、塞 WHERE，聚合會報錯）→ 自訂 `macros/error_rate_below.sql` 用 `HAVING` 做全表聚合 |
| `unique` + `not_null` | `raw_id`/`id`/`order_id` | error | `unique(raw_id)` 即去重驗證 |
| `not_null` | `received_at`/`has_clean_error`/`has_schema_drift` | error | REQUIRED 欄位 |
| source freshness | `staging.orders` | warn 26h / error 50h | 帶 `filter` 繞保險絲 |

> 自訂 generic test 的參數需巢狀在 `arguments:` 下（dbt 1.11 要求，否則 `MissingArgumentsPropertyInGenericTestDeprecation`）。

## 6. 常見操作 runbook

- **何時 `--full-refresh`**：改分區/叢集、改去重邏輯、回看窗外的歷史需重算、或首次建表。走 DDL、不受 sandbox 限制。
- **Proposal C targeted refresh**：修正列落在舊分區、回看窗看不到 → 修復 runbook 最後一步對災區分區做 targeted refresh（`--full-refresh` 或未來對單分區 `insert_overwrite`）。見 [CLOUD_LAYER-TW §7.4](../CLOUD_LAYER-TW.md)、DQ C-2 #7。

## 7. 相依與版本

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`：`dbt-labs/dbt_utils >=1.1.0,<2.0.0`（實裝 1.4.1）

## 8. 現況與待辦

- ✅ `stg_orders`（去重 + Hard Gate + freshness，增量）
- ⬜ `stg_quality_events`（需先把 `quality_events` 納入 E/L 抽取到 BQ）
- ⬜ `int_orders` + Row Filter、`int_orders_quarantine`、場景專用 `int_orders_*`
- ⬜ `dim_/fct_`、`rpt_quality_*`
