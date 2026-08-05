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
dbt run    --select stg_orders --full-refresh   # 全量重建（見 §9）
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
| 報表 | `rpt_` | 固定粒度預聚合，BI 直接消費 | 業務報表同 Gold；品質報表刻意讀 quarantine |

- 命名採 `stg_orders`（沿用專案既有文件），非 dbt 官方 `stg_<source>__<entity>`。
- 檔案組織：`models/staging/` 內 `_staging__sources.yml`（source 定義）、`stg_orders.sql`、`stg_orders.yml`（測試/描述）；`models/intermediate/` 內各 `int_*.sql` 與共用的 `_intermediate__models.yml`；`models/marts/` 內各 `dim_*.sql`/`fct_*.sql` 與 `_marts__models.yml`；`models/reports/` 內各 `rpt_*.sql` 與 `_reports__models.yml`。
- `reports/` 與 `marts/` **平行而非巢狀在其下**：dbt 官方慣例會把 `rpt_` 放進 marts，但上表已把「報表」列為獨立一層，且兩張品質報表的上游是 `int_`/`stg_` 而非 marts，塞進 marts 反而語意錯。
- ⭐ `rpt_` 有**兩個資料域**：業務報表上游一律走 `dim_`/`fct_`；品質報表讀 `int_orders_quarantine` 與 `stg_quality_events`——被隔離的列按定義**永遠不會出現在 Gold**，那不是繞過星狀模型，是不同的資料域（見 §7.1）。

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

`sync_all_columns` 的 DROP 會牴觸「staging 只做加法、刪欄留 legacy 欄保歷史」（[CLOUD_LAYER-TW §5.2/§5.3](../CLOUD_LAYER-TW.md)）。`append_new_columns` 天生只加不刪，正好對齊。

**觸發閘門＝顯式清單，故不吃 drift**：`check_for_schema_changes` 比對的是 model 產出的欄位（顯式清單），不是底層 staging。staging 靠 `ALLOW_FIELD_ADDITION` 自動長的欄，在你把它加進顯式 SELECT 前偵測不到 → 不會自己 ALTER。此選項**只在刻意改清單（進 git、被 review）時觸發**，顯式紀律原封不動。

**界線**（救不了的，仍走 §9 runbook）：
- **歷史回填**（值要連舊分區一起有，如 Proposal C 重建）：它只 NULL 補舊分區、只寫回看窗 → 舊分區真值仍需 targeted refresh。
- **改型別 / 改名 / 改分區**：仍 `--full-refresh` / 重建表。

## 5. 實作決策（`int_` 層）

`int_` 是 **Gold 入口**——攔截發生在這一層（[DQ_ARCHITECTURE-TW Q1 機制二](../DQ_ARCHITECTURE-TW.md)）。DAG：

```
stg_orders ─────────┐
                    ├─► int_orders            (Row Filter 通過) ──► int_order_items
stg_quality_events ─┘   int_orders_quarantine (被隔離)
```

### 5.1 有效品質狀態：刻意複製，不抽共用模型

`int_orders` 與 `int_orders_quarantine` 都需要同一段邏輯——**合成「有效品質狀態」**：

> 判定基準【不是】ODS 的 `has_clean_error` 字面快照。ODS 是不可變錨點，被 Proposal B promote 的記錄在 ODS 裡**永遠**是 `has_clean_error=TRUE`；只讀快照它會永遠卡在 quarantine、流不回 Gold。有效狀態必須由 `int_` 在每次 run 時把「ODS 快照 ⊕ `quality_events` 最新事件」合成出來。

這段邏輯（`latest_event` → `resolved` → `classified` 三個 CTE）**刻意在兩個模型檔各寫一份**，而非抽成共用模型。決策過程：

| 方案 | 做法 | 實體物件 | JOIN 執行次數 | 互補性保證 |
|---|---|---|---|---|
| A | 共用 ephemeral model，輸出 `is_effectively_clean`；下游 `where flag` / `where not flag` | 不增加（ephemeral 不建 relation）| 每個下游各一次 | **機制**（同一布林 + 一次否定）|
| B | 共用小表 `int_quality_current` + macro 定義布林 | +1 張小表 | 只算一次 | 機制（macro）|
| **C（選用）** | 兩個模型各自 inline 同一段 CTE | 不增加 | 每個下游各一次 | **紀律 + 測試** |

**釐清一個常見誤解**：A（ephemeral）與 C 編譯後的 SQL 幾乎逐字相同——ephemeral 會被 inline 進每個下游，**既不會多建表、也不會少跑 JOIN**。真要減少 JOIN 次數只有 B（物化）做得到。**所以 A 與 C 的差別純粹在維護面，不在成本面。**

選 C 的實際理由：

1. **消費者只有 2 個**，複製成本低於「多一層 `ref` 間接跳轉」的認知成本；
2. **模型檔自我完備**——讀 `int_orders.sql` 就看得到全部判定邏輯，且與 DQ 文件的範例逐字對應，不必在三個檔案之間追邏輯；
3. 代價（互補性從機制保證降為紀律保證）**可以用一支測試買回來**——見 §5.2。

### 5.2 對齊清單 ⭐（改動任一模型時逐條核對）

C 方案下，`int_orders` 與 `int_orders_quarantine` 必須維持對 `stg_orders` 的**完整劃分**（互斥 + 窮盡：每個 `raw_id` 恰好出現一次）。共用區塊在兩檔中以 `═══` 註解框出，**必須逐字相同**。

| # | 檢查項 | 改錯的後果 |
|---|---|---|
| 1 | 兩邊 `is_effectively_clean` 的**定義逐字相同**，且一邊 `WHERE cond`、一邊 `WHERE NOT cond` | 條件不互補 → 某些列兩邊都不在（**靜默漏資料**）或兩邊都在（下游重複計數）|
| 2 | `coalesce(..., false)` **不可省** | `has_clean_error=TRUE` 且無事件時 `FALSE OR NULL = NULL`；`WHERE NOT NULL` 也是 NULL → **該列從兩張表同時消失** |
| 3 | JOIN 一律 `LEFT JOIN` | 誤寫 INNER → 所有沒有品質事件的列整批消失 |
| 4 | 視窗的 `partition by` / `order by` 決勝鍵兩邊一致（`partition by raw_id order by event_at desc, id desc`）| 同一列在兩邊取到不同事件 → 劃分破裂 |
| 5 | `effective_quality_state` 的 CASE 分支兩邊一致 | 血緣標籤對不上帳，`rpt_quality_*` 統計錯 |
| 6 | 兩模型的**物化策略**一致（目前皆 `table` 全量重建）| 一邊增量一邊全量 → 劃分在跑批之間破裂 |
| 7 | `tests/assert_orders_split_is_partition.sql` 維持 `severity: error`，**永不降級、永不 `--exclude`** | 這是 C 方案下唯一的自動化安全網 |

> 第 2 項是最容易漏的：`is_effectively_clean` 的三態（TRUE/FALSE/**NULL**）中，NULL 會讓列從兩張表**同時**消失，而且不報任何錯。劃分測試正是為它而寫。

### 5.3 收斂觸發點與尚未啟用的選項

- **收斂觸發點**：當出現**第 3 份複製**時（例如日後啟用場景專用模型），複製成本開始超過間接層成本，屆時應把共用區塊收斂成方案 A（ephemeral）或 B（小表 + macro）。
- **場景專用 `int_orders_*` 模型（[DQ 機制三](../DQ_ARCHITECTURE-TW.md)）——設計已備妥，刻意暫不實作**。理由：場景補值的本質是「回答某個具體分析問題」，沒有那個問題就沒有正確答案可寫，先建等於憑空造一個假需求並付永久維護成本。啟用時機＝出現真實分析場景、且該場景明確能接受某類與它無關的錯誤。啟用時需一併補一個 `dq_has_only_error_codes(json_col, allowed_codes)` macro（判斷「不存在任何 allowed 以外的 code」）——**不可用 `array_length(codes) = 1` 判斷**，因同一個 code 可能重複出現（如多個 item 各觸發一次 `non_finite_number`），數量比對會誤判。

### 5.4 物化：`stg_` 用增量、`int_` 用全量重建 ★

同一個專案裡兩層用了相反的物化策略，不是不一致——是因為兩層**「什麼會變」的形狀不同**。

| | `stg_orders` | `int_orders` / `int_orders_quarantine` |
|---|---|---|
| 物化 | `incremental` + `insert_overwrite` + 回看窗 | `table`（每次 run 全量重建）|
| 上游變更的來源 | 一個：staging 新增／重抽的列 | 兩個：新訂單（`received_at` 軸）**＋** 品質事件（`event_at` 軸）|
| 變更的時間軸 | 與分區欄位一致，都在近期 | **錯位**：事件在今天，它影響的訂單在很久以前 |
| 列會不會消失 | 不會（append-only 鏡射）| **會**（promote 後要離開 quarantine）|
| 漏算一個分區的後果 | 延遲——下輪或 runbook 補得回來 | **永久錯誤**——幽靈列留在 quarantine，且同時出現在兩張表 |

最後一列是關鍵：`stg_` 的增量失誤是**延遲**，`int_` 的增量失誤是**錯誤**，而且不報錯、不自癒。

#### 為什麼 `int_` 不能照抄回看窗

> Proposal B 的 promotion 事件 `event_at = now()`、落**當天**分區，但它救的那筆訂單 `received_at` 在**很久以前**的舊分區。若按 `received_at` 回看窗增量，那個舊分區永遠不會被重算 → 被 promote 的記錄**永遠流不回 Gold**，回流機制在此層被靜默切斷。

（與 [CLOUD_LAYER-TW §7.4](../CLOUD_LAYER-TW.md) 的 late-arriving 同構但軸不同：那裡是「值」變（Proposal C 修正列），這裡是「狀態」變（Proposal B 事件）。）

因此 `int_` 層一律 `materialized='table'`（在 `dbt_project.yml` 設為該資料夾預設）。`table` 走 `CREATE OR REPLACE`（DDL、原子替換、不受 sandbox 禁 DML 限制，§4.3 的限制對 `int_` 目前不適用），並順帶換到兩個好處：

- **不會有邏輯漂移**：表永遠等於「現在的 SQL 套在現在的上游」。增量表的舊分區可能留著用舊版邏輯算出的列，得靠 runbook 補（`stg_orders` 就有這個包袱，見 §4.7、§9）。
- **改欄位清單零成本**：不需要 `on_schema_change`、不需要判斷該不該 `--full-refresh`。

#### 真要改增量，難的不是回看窗，是這三件事

1. **粒度錯配**：受影響的單位是「列」，`insert_overwrite` 換的是「分區」。只 SELECT 受影響的列會讓同分區其他列蒸發，所以必須先把「受影響的列」反推成「受影響的分區」，再重選那些分區的**全部**列——重選集合＝「回看窗分區 **∪** 近期有品質事件的 `raw_id` 所屬分區」。這需要一趟額外的 discovery 查詢（好消息：BQ 是列式，discovery 只掃 `received_at`／`raw_id` 兩欄，成本是全表掃的個位數百分比）。
2. **在最需要它的那天退化成全量**：Proposal B 的典型形態是「規則放寬 → 撈回**跨全歷史**的舊 quarantine」，事件的 `raw_id` 散布在每個分區上 → 受影響分區＝全部分區，那次 run 比全量還貴（多付 discovery + tmp table + N 個 copy job）。增量存在的理由，正好發生在它失效的那天。
3. **必須連帶收斂共用區塊**：兩張互補的表要對同一批分區做一致重算，那段 discovery 邏輯必須在兩檔逐字相同——共用區塊會從「三個直觀的 CTE」膨脹成「三個 CTE + 一段容易寫錯的動態分區 Jinja」。**改增量的那一刻就是 §5.3 收斂觸發點被觸發的時刻**，兩件事必須一起做。

#### 何時該改：看可觀測數字，不看訂單筆數

實測基準：`int_` 層一次 run 掃 910 KB / 554 筆訂單 → **每筆訂單每次 run 約 1.64 KB**（三個模型加總；此比率就是列寬，隨規模基本不變）。

| 訂單總數 | 單次 run 掃描 | 日批月成本（on-demand $6.25/TiB）|
|---|---|---|
| 1,000 萬 | 16 GB | ~$3 |
| 1 億 | 164 GB | ~$30 |
| 10 億 | 1.6 TB | ~$300 |

sandbox 的 1 TiB/月免費額度，在日批下約撐到 **1,500–2,000 萬筆訂單**（還要扣 `stg_` 與未來 `dim_/fct_` 的用量）。

但**成本不是第一個撞到的瓶頸**，會先撞到的是：① `profiles.yml` 的 `job_execution_timeout_seconds: 300`——它會讓 run 直接失敗而不是變貴（目前全量重建 2.5 秒）；② 整條 DAG 的批次窗口，`int_` 全量疊上未來同樣全量的 `dim_/fct_`。

**判準**：監控 `target/run_results.json` 的 `bytes_billed` 月累計與 `execution_time`，任一項到額度／timeout 的 **50%** 就開始評估。在那之前，全量重建是「正確性免費、複雜度為零」的選擇。

> **一個刻意接受的架構不對稱**：`stg_` 做增量省下的是重算與寫入，但 `int_` 每次仍全掃 `stg_`——所以**整條管線的讀取成本依然 ∝ 歷史總量**。這是明知並接受的取捨，換來 `int_` 層的正確性無條件成立。

### 5.5 不分區，叢集只掛 `order_id`

- `int_` 只被 DAG 內部消費（非分析師 ad-hoc 查詢），分區收益 ≈ 0；`order_date` 分區留給 `dim_/fct_`（[CLOUD_LAYER-TW §1.2](../CLOUD_LAYER-TW.md)「每張表依自己的 access pattern 各自選」）。
- ⚠️ 早期版本在此另記了一個「地雷」：`int_orders_quarantine` 是 `ORDER_DATE_IN_FUTURE` 髒列的收容處，離譜的未來日期會超出 BQ 分區合法區間、讓整張表建立失敗。**該理由已於 2026-08 實測推翻**——超出 `1960-01-01 ~ 2159-12-31` 的值不會炸表，會靜默落進 `__UNPARTITIONED__` 分區（見 [CLOUD_LAYER-TW §1.7.3](../CLOUD_LAYER-TW.md)）。故 `dim_/fct_` 採 `order_date` 分區**不需要**合法區間守衛（§6.2），而本層不分區的決定只剩上面那一條理由。

### 5.6 `quarantined_at`：取事件時間，非 `CURRENT_TIMESTAMP()`

DQ 文件早期範例寫 `CURRENT_TIMESTAMP() AS quarantined_at`。在 `table` 全量重建下這個值**每次跑批都會變**——它記錄的是「這次 run 的時間」而非「這筆被隔離的時刻」，會讓 `rpt_quality_*` 的時間軸失真。

改為 `coalesce(quality_state_at, received_at)`：優先取 `initial_evaluation` 事件的 `event_at`（真正的隔離時刻），事件缺席時退回攝入時間。若日後需要跑批時戳，另開欄位、語意分開。

### 5.7 `int_order_items`：來源、`safe_cast`、嚴格 NULL 傳播

把 `ODS.items`（JSON 陣列）以 `unnest(json_query_array(items)) with offset` 攤平到 item 粒度，供未來的 `fct_order_items`。

- **來源選 `int_orders`（已過濾）而非 `stg_orders`**：item 層的錯誤（`quantity_non_positive`、`unit_price_negative`、`discount_pct_out_of_range`、`non_finite_number`）在攝入層就會讓**整張訂單** `has_clean_error=TRUE` 被隔離，所以從 `int_orders` 出發天然保證「Gold 不含髒資料」。要做 item 層 RCA 時另建讀 quarantine 的模型。
- **數值一律 `safe_cast`**：`clean.py` 明載「items 內的值未經 Pydantic 強轉，可能是字串」——items 整包以 JSONB 落地，欄位值不經 `ODSOrder` 型別強轉。用 `cast` 會讓一筆髒 item **炸掉整批**；`safe_cast` 轉不動 → NULL，符合本專案「標記不阻斷」的哲學。
- ⭐ **金額一律 `NUMERIC`，不用 `FLOAT64`**（2026-08 實測後改）：`FLOAT64` 的 `SUM()` **不滿足結合律**，同一組數字換個累加順序尾數就差一個 bit，於是 `assert_fct_orders_rollup_matches_items` 的精確比對必然隨機失敗（實測 39 筆不一致，最大相對誤差 **3.442e-16 ≈ 1 ULP**）。`NUMERIC` 是精確十進位（precision 38 / scale 9），加總精確且與順序無關，測試因此可以維持精確比對，不必退讓成容差比對——容差要設多少、隨資料量成長要不要調，那是個會回來找你的決定。`safe_cast` 的容錯在 `NUMERIC` 下同樣成立（已實測：轉不動→NULL、超出 precision→NULL、小數超過 scale 9→四捨五入而非報錯）。`quantity` 維持 `INT64`：它是計數不是金額。
- **衍生金額採嚴格 NULL 傳播，不 `coalesce`**：`net_amount = quantity × unit_price × (1 - discount_pct/100)`，任一輸入為 NULL 則結果 NULL。理由是 [CLOUD_LAYER-TW §5.5.5](../CLOUD_LAYER-TW.md) 的鐵律——NULL 帶資訊（「沒有折扣資料」≠「折扣為 0」），`COALESCE` 有損且單向，一旦在 `int_` 壓成 0，全下游再也分不出「沒收集」與「真的是 0」。若日後確認「缺值＝無折扣」，補值加在 `dim_/fct_`，**不回頭改本層**。
- **`(raw_id, item_index)` 是 item 粒度鍵**：`items` 在 ODS 內是不可變的 JSONB 快照、陣列順序固定，故位置可作為穩定身分；代理鍵 `order_item_key = raw_id-item_index`。

## 6. 實作決策（`dim_`/`fct_` 層 — 星狀模型）

Gold 採 **Kimball header/line 雙事實表**：

```
int_orders ──────┬──► dim_customer      (SCD1)
                 ├──► fct_orders        grain: order_id
int_order_items ─┼──► dim_product       (SCD1)
                 └──► fct_order_items   grain: (order_id, item_index)
```

> ⚠️ **不得 join 兩張事實表後同時聚合兩邊的度量**——header 的訂單總額會被 line 的列數放大（重複計算）。要 item 明細查 `fct_order_items`，要訂單總額查 `fct_orders`。

### 6.1 度量分配：rollup 進 header + 不變式測試 ⭐

雙事實表最大的風險不是建錯，是「同一個數字存在兩處、可能不一致」。三個方案：

| | A：rollup 進 `fct_orders` | B：金額只放 line fact | **C（選用）** |
|---|---|---|---|
| 「本月訂單數 × 總營收」 | 單表查 | 必須 join + group by | 單表查 |
| 兩處數字不一致 | 可能（無人把關） | 不可能（單一事實來源） | **不可能（測試把關）** |
| 額外成本 | 0 | 0 | 一支 singular test |

選 C：rollup 進 `fct_orders`，並以 `assert_fct_orders_rollup_matches_items` 逐單斷言。這與 `int_` 層「刻意複製共用區塊 + `assert_orders_split_is_partition` 買回風險」（§5.1）是**同一個手法**——用一支測試把紀律保證升級成機制保證，換取查詢便利。

測試中 `is distinct from`（而非 `=`）不可省：金額是嚴格 NULL 傳播的，`NULL = NULL` 結果是 NULL 而非 TRUE，用 `=` 會讓「兩邊都 NULL」的列被 `WHERE` 靜默濾掉。

> ⚠️ **這支測試同時是浮點數陷阱的偵測器**。2026-08 第一批「每張訂單多個品項」的資料進來當天，它就紅了 39 筆——`item_count` 與 `total_quantity` 完全相符，只有金額差 1 ULP。根因是 `FLOAT64` 的 `SUM()` 不滿足結合律，rollup 與測試端的重新聚合走了不同執行計畫。
> 潛伏這麼久才浮現，是因為在那之前 60 天窗內的資料**每張訂單剛好一個品項**，單值 `SUM()` 沒有累加就沒有順序問題。處置是把金額改成 `NUMERIC`（§5.7），**不是**把測試放寬成容差比對。

#### `SUM` 會靜默吃掉刻意保留的 NULL ⭐

`int_order_items` 的嚴格 NULL 傳播（§5.7）到了 rollup 這裡有個陷阱：**BQ 的 `SUM()` 忽略 NULL**。一筆訂單裡只要有一個 item 的 `discount_pct` `safe_cast` 失敗，`net_amount` 就少算一個品項，**不報錯、不留痕跡**。

處置刻意**不** `COALESCE`（那違反 [CLOUD_LAYER-TW §5.5.5](../CLOUD_LAYER-TW.md) 的鐵律，且有損單向），改成把不完整性**顯性化**：`fct_orders.items_missing_amount` 記錄該訂單有幾個品項的金額算不出來，讓下游自己判斷這個加總可不可信。這正是 §5.5.5 所謂「填值決定留到 `dim_/fct_` 依問題處理」的正確落點——我們不替下游決定 NULL 該當 0，而是給它判斷的依據。

附帶：`item_count = 0` 讓「沒有任何品項的訂單」以**值**表達而非以**缺席**表達；`fct_orders` 對 `item_rollup` 必須 `LEFT JOIN`，`INNER` 會讓那類訂單整批從 Gold 消失。

### 6.2 分區與保留政策

| 決策 | `fct_*` | `dim_*` | 理由 |
|---|---|---|---|
| 分區 | `order_date`(DAY) | **不分區** | 維度是**按鍵 join** 進來的，分區對 join 沒有裁切作用，只換來小分區與 metadata 開銷 |
| 叢集 | `customer_id` / `product_id, order_id` | 維度鍵 | 對齊實際 access pattern |
| 保留 | 5 年（`var` gated） | — | DAY 粒度受 4000 分區上限約束（約 11 年）→ 必須有明確政策 |
| `require_partition_filter` | ❌ 不上 | — | Gold 服務分析師 ad-hoc 與 BI 探索式查詢，開了就是一律 400 |

完整論證（含「clustering 單獨已裁掉 82%、分區再拿 9pp」的實測，以及保險絲 vs custom quota 的分工）見 [CLOUD_LAYER-TW §1.2.1](../CLOUD_LAYER-TW.md)。

**`partition_expiration_days` 必須 `var` gated**：BQ sandbox 硬鎖 < 60 天，寫死 1825 會讓每次 `dbt run` 失敗（[§1.7.2](../CLOUD_LAYER-TW.md)）。啟用帳單後：

```bash
dbt run --vars '{gold_partition_expiration_days: 1825, gold_projection_window_days: 1825}'
```

物化仍是 `table` 全量重建，理由同 §5.4 **且更強**：Gold 的分區軸是 `order_date`（業務時間），與「資料何時變動」完全脫鉤——一筆 2024 年的訂單今天被 Proposal B promote，任何按 `order_date` 的回看窗都永遠看不到它。

### 6.3 維度：只建兩張，SCD1 + 事實表承載當下快照

**建哪些**（呼應 §5.3「不建投機性 model」）：只建 `dim_customer` 與 `dim_product`。`dim_date`（目前沒有財年/節慶需求，`order_date` 本身可 `date_trunc`）、`dim_geography`（沒有 conformed 地理主檔，抽出來只是把欄位搬家再 join 回來；BQ 列式儲存下寬表不吃虧）、junk dimension（省 row storage 在 BQ 不是問題）**一律 degenerate 到事實表**。

**SCD 策略**：兩張維度都沒有獨立主檔，屬性隨訂單帶進來，故 SCD1 + 明確決勝鍵（少了決勝鍵，同日多筆訂單誰勝出會隨執行順序漂移）。

SCD1 的失真——歷史訂單被貼上「現在的」等級——**用事實表補回來**：`fct_orders.membership_tier_at_order` 記錄下單當時的等級。訂單裡帶的顧客屬性本來就是下單當下的快照，用事實表承載它等於**零基建拿到 type-2 的效果**：

- 「白金會員**現在**的總消費」→ join `dim_customer.membership_tier`
- 「下單**當時**是白金的訂單」→ 直接讀 `fct_orders.membership_tier_at_order`

**SCD2 為備妥但未啟用的設計，觸發點＝啟用帳單。** 不是因為麻煩，是因為在 sandbox 上它**會壞掉**：dbt snapshot 是有狀態表，被 60 天表過期吃掉就再也回不來，與 `fct_` 全量重建能自癒的性質完全不同。（同 §5.3 的紀律：設計寫下來，等觸發點到了才實作。）

### 6.4 `dim_product` 的屬性衝突：標記，不阻擋

同一個 `product_id` 可能在不同訂單帶著不同的 `product_name`/`category`/`brand`。2026-08 實測 342 個 `product_id` 中 **163 個衝突**，根因是 `load_test.py` 對 `product_id` 與其屬性各抽一次獨立亂數（已修，見該檔 `make_product()`）。

處置分三層：

1. 模型用明確決勝鍵保證**確定性**——衝突不會讓 grain 破裂，只會挑最新的
2. `fct_order_items.product_name_at_order` 保留 line 級真值，可與維度對照
3. `assert_product_attributes_stable`（**severity: warn**）監控衝突數

warn 而非 error，因為這是**上游契約訊號**而非本層的正確性缺陷——`product_id` 若真的無法唯一決定商品屬性，該修的是上游或 data contract，不是讓整條 DAG 停下來。判斷邏輯與 DQ 文件的 `has_schema_drift`「沒有攔截權限、只能告警」一致。對照組：`assert_fct_orders_*` 是 error，因為那些測的是**我們自己的 SQL 對不對**。

### 6.5 鍵的處理

**維度鍵用 natural key 直連**（`customer_id`/`product_id`），不做 hash surrogate key：BQ 沒有 index，surrogate key 不帶 join 效能收益，少一層 key 管理、分析師直接看得懂。切換觸發點很明確——**§6.3 改成 SCD2 的那天**，`customer_id` 不再唯一，surrogate key 就從可選變成必要。

**事實表不留 NULL FK**：`customer_id`/`product_id` 在 ODS 皆 nullable，而 NULL FK 會讓 INNER JOIN 靜默掉列、LEFT JOIN 讓 BI 顯示空白。故維度各補一筆 unknown member（`'__UNKNOWN__'`），事實表 `coalesce` 到它。

這**不**牴觸 §5.5.5 的 NULL 鐵律：鐵律禁止的是在共享層對**度量**做有損 collapse（`NULL→0` 之後分不出「沒收集」與「真的是 0」）；這裡動的是**鍵**，且 `'__UNKNOWN__'` 可完整反查回「這筆沒有識別」，是**無損**的。

**`fct_order_items` 的 grain 用 `(order_id, item_index)`**，不沿用上游的 `raw_id`：Gold 面向分析師，README〈`raw_id` 是物理身分、`order_id` 是業務身分〉。兩者在 ODS 皆 UNIQUE、1:1，改用 `order_id` 不損失唯一性；`raw_id` 保留為血緣欄位但**不是鍵**。上游那支代理鍵已改名 `int_order_item_key` 並停在 `int_` 層，避免同名不同值。代理鍵 `format('%s-%03d', order_id, item_index)` 的補零讓字典序＝數值序（否則 `A-10` 會排在 `A-2` 前）。

`fct_order_items` 另把 order 層的 `customer_id`/`order_date` **一路帶下來**，讓 line fact 能獨立查詢——「某商品在白金會員間的銷量」不該被迫掃兩張表。BQ 列式下多帶低基數欄位近乎零成本。

### 6.6 尚未定義的業務規則（刻意不做假設）

以下三項文件裡沒有定義，**刻意不憑空假設**（呼應 §5.3）——憑空假設會讓一個錯誤的數字看起來像事實：

| 項目 | 未定義的是什麼 | 目前處置 |
|---|---|---|
| `tax_amount` | 稅基是 `net` 還是 `net + shipping`？ | 只放 `tax_pct`（**比率，不可加，不要 SUM**），不衍生金額 |
| 淨營收 | `returned = TRUE` 的訂單要不要扣掉？ | `returned` 留在事實表當 flag，由下游決定 |
| `profit_amount` | 毛利要不要含運費與稅？ | 不做；`net_amount - cost_amount` 下游自行計算 |

## 7. 實作決策（`rpt_` 層 — 報表）

三張表，對應 BI 上三張圖：

```
int_orders_quarantine ──► rpt_quality_backlog          快照軸：現在還剩什麼
stg_quality_events ─────► rpt_quality_events_daily     事件軸：發生了什麼
fct_order_items ─┬──────► rpt_sales_daily_by_category  業務聚合
dim_product ─────┤
fct_orders ──────┘（只取 returned 旗標）
```

### 7.1 業務報表的上游一律走 Gold，不從 `int_` 直接拉

`rpt_` 直接讀 `int_` 寫成固定查詢，在成熟團隊是 anti-pattern。四個理由，後兩個是本專案特有的：

| # | 理由 | 繞過 `fct_` 的後果 |
|---|---|---|
| 1 | 口徑單一 | 「營收」有兩條血緣 → 兩個數字，且沒人知道哪個錯 |
| 2 | 不重做 Gold 已經做過的語意決定 | unknown member、`item_count=0` 的 LEFT JOIN 全得再抄一遍 |
| 3 | 會讓既有測試失效 | `assert_fct_orders_rollup_matches_items` 保的是 `fct_orders` 的 rollup；`rpt_` 若自己從 `int_` 重算，那支測試**完全不覆蓋它** |
| 4 | ⭐ 會推翻 `int_` 層的架構前提 | §5.5「`int_` 只被 DAG 內部消費」是 `int_` **不分區的唯一理由**。`rpt_` 讀 `int_` 等於把 `int_` 從內部建材升格成對外契約 → 分區決策要重審，且 `int_` 從此改不動 |

**合法的例外**：品質報表。被隔離的列按定義永遠不在 Gold，所以 `rpt_quality_*` 的上游必然是 `int_orders_quarantine` 與 `stg_quality_events`。

> ⚠️ 這帶出一個容易寫錯的地方：**品質率的分母是 `stg_orders` 全體（含髒），不是 `fct_orders`**。用 Gold 當分母，quarantine_rate 恆為 0——那正是 Row Filter 的作用。

> 附帶說明：`rpt_` 的教科書理由是「預聚合換效能與成本」，而在本專案目前的資料量上這個理由是零。文件裡不寫「為了效能」，因為那會是假的——真正的理由是**固定口徑 + BI 端不必自己組 join**。讓報表作者在 BI 裡自由拉 `fct_` 做聚合，指標定義就漂到 BI 裡去了，那才是 `rpt_` 在這個規模上要防的事。

### 7.2 三個貫穿全層的紀律

**① 比率一律不落地，只落地可加的分子與分母** ⭐

預聚合層存 rate 是頭號陷阱：BI 一旦把日粒度 roll up 到週，Looker Studio 算的是 `AVG(daily_rate)`——**「比率的平均」而非「總和的比率」**，兩者只在每日分母相等時才一致，而分母永遠不相等。rate 交給 BI 計算欄位（它做 `SUM(分子)/SUM(分母)`，任何粒度都對）。

> 為何不比照 `fct_orders.tax_pct` 那樣「留著並註明不可加」：`tax_pct` 是**原始事實**，不存就沒了；`quarantine_rate` 是純衍生值，留一個「只在日粒度正確」的欄位等於主動製造誤用機會。

**② `COUNT(DISTINCT)` 在預聚合層結構性不可加**

跨 category 加總 `orders` 會重複（一張訂單的品項橫跨多分類），跨日加總 `customers` 也會重複。這無法靠命名補救，只能標死。**觸發點**：需要跨維度正確彙總 distinct 時，改存 BQ 原生 HLL sketch（`HLL_COUNT.INIT` → BYTES，上層 `HLL_COUNT.MERGE`，誤差 ~1%）。現在不做的理由不是麻煩，是 Looker Studio 的計算欄位**呼叫不了** `HLL_COUNT.MERGE`，得再包一層 view——那個摩擦點目前不存在（同 §5.3 的紀律）。

**③ `rpt_` 只做 `GROUP BY` / window**

不做新的 join 語意、不做新的清洗、不引入新的業務定義。若某張 `rpt_` 需要 `dim_`/`fct_` 給不出的 join，那是**星狀模型缺東西的訊號**，該補的是 Gold，不是在這裡湊。（與 §5.7「填值加在 `dim_/fct_`，不回頭改 `int_`」同一個方向感。）

### 7.3 品質報表拆成兩張：兩條時間軸，兩種可變性 ⭐

DQ 文件原規劃的 `rpt_quality_daily` 混了兩件性質相反的事，實作時拆開：

| | `rpt_quality_events_daily` | `rpt_quality_backlog` |
|---|---|---|
| 軸 | **事件軸**（`event_at`） | **快照**（讀 quarantine 當下內容） |
| 一列的語意 | 「當天發生了 N 個品質事件」 | 「現在還有 N 筆卡著」 |
| 會被追溯改寫嗎 | **不會**（append-only） | 會（本來就是現況） |
| 可否增量 | ✅ 軸與變更源對齊 | ❌ 天生不可增量 |

**為什麼 backlog 不能從事件軸累加算出來**：理論上 `backlog(t) = 累計 quarantined − promoted − rejected`，事件流是狀態的完整導數。但 `quality_events` 有 60 天分區過期——**過期後累加的起點就丟了，而且失真是單向的**（起點只會少算 quarantined → backlog 被系統性低估）。快照表直讀 `int_orders_quarantine`，不受事件保留期影響。

**為什麼事件表不掛攝入軸**：若按 `received_at` 分組，今天 promote 一筆三個月前的訂單會**改掉三個月前那一列的組成**——那是狀態不是事件，且會讓「v1 攔了多少」這個數字隨時間漂移，直接牴觸 [DQ_ARCHITECTURE-TW](../DQ_ARCHITECTURE-TW.md)〈歷史指標為何不會被追溯性改寫〉。

### 7.4 物化：本專案唯一一個增量天生正確的下游模型

| | 物化 | 分區 | 理由 |
|---|---|---|---|
| `rpt_quality_events_daily` | `incremental` + `insert_overwrite` + `copy_partitions` | `event_date`(DAY) | 事件軸 append-only，時間軸與「什麼會變」對齊 → 回看窗就夠，**不需要** §5.4 那套受影響分區 discovery |
| `rpt_quality_backlog` | `table` | **不分區** | 狀態快照，一筆被 promote 就要從表裡消失 → 增量失誤是**永久錯誤**不是延遲。分區的價值在分區級增量替換，本層永遠不會增量 |
| `rpt_sales_daily_by_category` | `table` | `order_date`(DAY) | ⭐ 現在全量，但**分區欄位現在加是免費的、事後補要重建表** |

> ⚠️ `rpt_quality_events_daily` 的 `var('rpt_quality_events_lookback_days')` **必須 ≥ `stg_quality_events_lookback_days`**。上游窗比下游窗大時，上游今天才補進來的舊事件會落在下游窗外 → 永遠撈不到，**且不報錯**（分區存在，只是內容少算）。兩個 var 要一起調。

**時區**：`event_date = date(event_at)` 走 **UTC**，刻意不用 `date(event_at, 'Asia/Taipei')`——時區轉換會讓分區裁切的謂詞下推失效。倉儲層落地 UTC、時區呈現交給 BI 是標準分工，但這代表「當日」的邊界是 UTC 午夜，與台北差 8 小時，故 column description 也寫了一次。

**`rpt_sales` 未來切增量的路徑**：「日 incremental + `order_date` 回看窗 ＋ 排程每週一次 `--full-refresh`」，**不是**自己寫受影響分區 discovery。後者會讓一張純業務報表被迫依賴 `quality_events` 當變更偵測器（為非語意的理由建立耦合），且在 Proposal B 大規模回流那天會退化成比全量還貴（§5.4 第 2 點）。代價是可寫進文件的一句話：**追溯性修正在本報表的可見延遲 ≤ 7 天**。

### 7.5 fan-out 的處置：配平用的可加度量

`rpt_quality_backlog` 攤平到 `error_code`（一張訂單可能多碼）→ 跨 code 加總會重複計數。處置是同一張表放**兩個語意不同的度量**：

- `orders_with_code`：帶此 code 的訂單數（**不可加**，Top N 圖用）
- `orders_primary_code`：以此 code 為主要碼的訂單數（**可加**，「總共卡幾筆」KPI 用）

主要碼＝`error_codes` 去重排序後的第一個。**只**為確定性與配平，**不**代表嚴重性排序——嚴重性優先級是業務定義，目前沒有，不憑空造（同 §6.6）。

> 被否決的替代方案：加一列 `error_code = '__TOTAL__'` 的彙總列。它讓「不小心把 `__TOTAL__` 也加進去」變成新的誤用面，比 fan-out 本身更危險。

陣列**去重不可省**：同一個 code 可能因多個 item 各觸發一次而重複（同 §5.3 那條「不可用 `array_length(codes) = 1` 判斷」），不去重會讓 `orders_with_code` 把一張訂單算成多張。

### 7.6 刻意先不做的

| 項目 | 為什麼不做 | 觸發點 |
|---|---|---|
| **金額曝險**（被卡住的訂單值多少錢） | `int_order_items` 的來源是 `int_orders`（乾淨路徑），quarantine 的 items **從未被攤平**（§5.7 已預告） | 需要 `int_order_items_quarantine`；啟用時機＝品質報表需要業務曝險金額 |
| **HLL sketch** | Looker Studio 計算欄位呼叫不了 `HLL_COUNT.MERGE` | 出現需要跨維度彙總 distinct 的圖表 |
| **逐格金額對帳測試** | 在 `table` 全量重建下是**同義反覆**（用同一段 SQL 驗自己，恆綠、零資訊） | ⭐ 見 §8 |
| `order_status` 進 grain | 值域未定義，不知道有沒有「未成立」狀態 | 確認值域後決定要不要過濾 |

## 8. 測試策略

| 測試 | 對象 | severity | 說明 |
|---|---|---|---|
| `error_rate_below`（自訂 generic test）| `stg_orders` 整批 `has_clean_error` 比率 | error @10% / warn @5% | **Hard Gate**（DQ 機制一）。不能用 `dbt_utils.expression_is_true`（逐列、塞 WHERE，聚合會報錯）→ 自訂 `macros/error_rate_below.sql` 用 `HAVING` 做全表聚合 |
| `unique` + `not_null` | `stg_` 的 `raw_id`/`id`/`order_id`；`int_` 的 `raw_id`/`order_id` | error | `stg_` 的 `unique(raw_id)` 即去重驗證 |
| `not_null` | `received_at`/`has_clean_error`/`has_schema_drift` | error | REQUIRED 欄位 |
| source freshness | `staging.orders`、`staging.quality_events` | warn 26h / error 50h | 帶 `filter` 繞保險絲 |
| **`assert_orders_split_is_partition`**（singular）⭐ | `int_orders` ∪ `int_orders_quarantine` vs `stg_orders` | error | **劃分不變式**：每個 `raw_id` 恰好出現一次。C 方案（§5.1 複製）下唯一的自動化安全網，守 §5.2 對齊清單的 #1–#4。**永不降級** |
| `assert_int_orders_no_unpromoted_dirty`（singular）| `int_orders` | error | **Gold 契約**：不得含 `has_clean_error=TRUE` 且未被 promote 的列。寫成 singular 而非欄位測試，因為它是**兩欄之間的條件關係**——`has_clean_error=TRUE` 本身在此表合法（promoted 記錄在 ODS 仍是髒的）|
| `accepted_values` | `int_orders`/`int_orders_quarantine` 的 `effective_quality_state` | error | 兩表的狀態值域互斥（`clean`/`promoted` vs `quarantined`/`permanently_rejected`），等於從另一角度覆核劃分 |
| `dbt_utils.unique_combination_of_columns` + `relationships` | `int_order_items` 的 `(raw_id, item_index)`、`raw_id → int_orders` | error | item 粒度唯一性與血緣完整性 |
| **`assert_fct_orders_rollup_matches_items`**（singular）⭐ | `fct_orders` 的 rollup 度量 vs `fct_order_items` 聚合 | error | **rollup 一致性不變式**（§6.1 方案 C 的核心）。逐單以 `is distinct from` 比對——用 `=` 會讓「兩邊都 NULL」的列被 `WHERE` 靜默濾掉。兩表分區設定相同，故不需加時間窗 |
| **`assert_fct_orders_complete_projection`**（singular）⭐ | `int_orders`（窗內）vs `fct_orders` | error | **無損投影契約**：攔截已在 `int_` 做完，Gold 不得再掉任何一列。寫成反向 join + `order_date` 窗而非 `count = count`——兩表的 60 天時鐘掛在不同軸上（[CLOUD §1.7.5](../CLOUD_LAYER-TW.md)），count 比對會變成每天 flaky |
| `assert_product_attributes_stable`（singular）| `int_order_items` 的 `product_id` → 屬性 | **warn** | 上游契約訊號，非本層缺陷——`product_id` 無法唯一決定屬性時該修上游，不該停 DAG（§6.4）|
| `unique` + `not_null` | `dim_customer`/`dim_product` 的維度鍵；`fct_orders.order_id`；`fct_order_items.order_item_key` | error | 維度 grain 與事實表代理鍵唯一性 |
| `relationships` | 兩張 `fct_` 的 `customer_id`/`product_id` → `dim_*`；`fct_order_items.order_id` → `fct_orders` | error | 星狀模型的 FK 完整性。配 `not_null`（unknown member 保證 FK 不為 NULL，§6.5）|
| `dbt_utils.unique_combination_of_columns` | `fct_order_items` 的 `(order_id, item_index)` | error | 宣告的 grain |
| **`assert_rpt_sales_no_item_loss`**（singular）⭐ | `rpt_sales` 的 `sum(items)` vs `fct_order_items` 列數（窗內，逐日） | error | `rpt_sales` 引入了整條 DAG 唯一一組**新的 join**（× `dim_product`、× `fct_orders`）。join 悄悄變 INNER 的表現是「營收慢慢變小」，不報錯。用 full outer join 才抓得到「多了」（維度扇出） |
| **`assert_rpt_quality_events_split`**（singular）⭐ | `initial_clean + initial_quarantined = initial_evaluations` | error | **寬表的值域擴張警報器**：寬表的代價是「上游多一個 `to_state`，下游要改 schema 才看得到」。多出來的狀態會讓 `count(*)` 漲而 `countif` 不漲 → 立刻紅，而不是靜默蒸發。寬表能安心用靠的就是這支 |
| `assert_rpt_backlog_primary_code_balances`（singular） | `sum(orders_primary_code)` vs `int_orders_quarantine` 實際訂單數 | error | §7.5 配平度量的安全網。失效的症狀是 BI 上 backlog 總數直接錯掉，不自癒 |
| `dbt_utils.unique_combination_of_columns` + `not_null` | 三張 `rpt_` 各自宣告的 grain | error | 預聚合表 grain 破裂＝所有數字直接翻倍，且無聲 |
| `dbt_utils.expression_is_true` | `orders <= items`、`items_missing_amount <= items`、`orders_with_code >= orders_primary_code` | error | 便宜的合理性下限 |

> 自訂 generic test 與部分內建測試的參數需巢狀在 `arguments:` 下（dbt 1.11 要求，否則 `MissingArgumentsPropertyInGenericTestDeprecation`）。

> ⚠️ **刻意先不寫的測試**：`assert_rpt_sales_matches_fct` 這類**逐格金額對帳**。在 `table` 全量重建下它是同義反覆（`rpt_` 的 sum 就是把 `fct_` 的欄位加起來），恆綠、零資訊，價值要到切增量那天才兌現（抓漏分區）。
>
> **→「`rpt_sales` 改增量」與「加逐格對帳測試」是同一件事的兩半，不得只做前者。** 與 §5.4「改增量的那一刻就是收斂觸發點被觸發的時刻」同構。
>
> 對照組：`assert_rpt_sales_no_item_loss` 現在就寫，因為它測的是**列數跨兩個 join**，與物化策略無關——那是真實可能發生的失效。

## 9. 常見操作 runbook

- **何時 `--full-refresh`**：改分區/叢集、改去重邏輯、回看窗外的歷史需重算、或首次建表。走 DDL、不受 sandbox 限制。（僅適用 `stg_` 的增量模型；`int_` 為 `table` 全量重建，每次 run 本就重建。）
- **Proposal C targeted refresh**：修正列落在舊分區、回看窗看不到 → 修復 runbook 最後一步對災區分區做 targeted refresh（`--full-refresh` 或未來對單分區 `insert_overwrite`）。見 [CLOUD_LAYER-TW §7.4](../CLOUD_LAYER-TW.md)、DQ C-2 #7。
- **改動 `int_orders` 或 `int_orders_quarantine` 前**：先過一遍 §5.2 對齊清單；改完跑 `dbt build --select intermediate+`，確認 `assert_orders_split_is_partition` 為綠。
- **調 `rpt_quality_events_daily` 的回看窗**：`rpt_quality_events_lookback_days` 必須 ≥ `stg_quality_events_lookback_days`，兩個 var 一起調（§7.4）。
- ⚠️ **DAG 連續失敗超過回看窗天數 → 修好後第一次跑必須放大回看窗** ⭐
  單次失敗是安全的：staging 已 append、watermark 已推進，而 `stg_` 的回看窗下輪會把那幾天重算。
  **危險的是連續失敗**——回看窗預設 3 天，DAG 掛了 4 天再修好的話，那次跑批只回看 3 天，
  第 4 天前已在 staging 的列**永遠不會進 `stg_orders`**，而且不報錯、不自癒（靜默漏資料）。
  修好後首次執行請用 `--vars '{stg_orders_lookback_days: N}'`（N ≥ 中斷天數 + 安全邊際）
  或 `--full-refresh`。`stg_quality_events` 與 `rpt_quality_events_daily` 的窗同理，要一起放大。
  預防面：Airflow 的失敗告警必須在「累積中斷天數逼近回看窗」之前就被看見——
  換句話說，**回看窗的天數其實是在宣告「可容忍多久的無人值守失敗」**，不只是成本參數。

## 10. 相依與版本

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`：`dbt-labs/dbt_utils >=1.1.0,<2.0.0`（實裝 1.4.1）

## 11. 現況與待辦

- ✅ `stg_orders`（去重 + Hard Gate + freshness，增量）
- ✅ `stg_quality_events`（以 `id` 為 grain 去重，保留完整狀態機歷史）
- ✅ `int_orders` + Row Filter、`int_orders_quarantine`（劃分不變式有測試把關）
- ✅ `int_order_items`（items 攤平到 item 粒度）
- ✅ `dim_customer`、`dim_product`（SCD1 + unknown member）
- ✅ `fct_orders`、`fct_order_items`（雙事實表；rollup 一致性與無損投影皆有測試把關）
- ✅ `rpt_quality_events_daily`（事件軸，增量）、`rpt_quality_backlog`（快照）、`rpt_sales_daily_by_category`
- ⬜ 場景專用 `int_orders_*`（設計已備妥，待真實分析場景出現才啟用——見 §5.3）
- ⬜ SCD2 `dim_customer`（設計已備妥，觸發點＝啟用帳單——見 §6.3）
- ⬜ `rpt_sales_*` 切增量（路徑＝日增量 + 週全量；**必須同時補逐格對帳測試**——見 §7.4、§8）
- ⬜ 金額曝險度量（需 `int_order_items_quarantine`——見 §7.6）
- ⬜ Proposal B（Airflow 重評估寫 `quality_events`）——下游回流路徑已就緒，只等事件產生端
- ⚠️ Proposal B 事件產生端未實作 → `rpt_quality_events_daily` 的 `promotions` / `rejections` / `re_quarantines` 目前**恆為 0**（欄位已備妥，事件一產生即有值）
