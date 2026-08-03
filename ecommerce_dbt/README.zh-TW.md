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
- 檔案組織：`models/staging/` 內 `_staging__sources.yml`（source 定義）、`stg_orders.sql`、`stg_orders.yml`（測試/描述）；`models/intermediate/` 內各 `int_*.sql` 與共用的 `_intermediate__models.yml`。

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

**界線**（救不了的，仍走 §7 runbook）：
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

- **不會有邏輯漂移**：表永遠等於「現在的 SQL 套在現在的上游」。增量表的舊分區可能留著用舊版邏輯算出的列，得靠 runbook 補（`stg_orders` 就有這個包袱，見 §4.7、§7）。
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
- 更實際的理由是一個**地雷**：`int_orders_quarantine` 正是 `ORDER_DATE_IN_FUTURE` 髒列的收容處，離譜的未來日期會超出 BQ 分區合法區間、**讓整張表建立失敗**。按 `order_date` 分區等於自找失敗。`dim_/fct_` 啟用該分區時需先做合法區間守衛。

### 5.6 `quarantined_at`：取事件時間，非 `CURRENT_TIMESTAMP()`

DQ 文件早期範例寫 `CURRENT_TIMESTAMP() AS quarantined_at`。在 `table` 全量重建下這個值**每次跑批都會變**——它記錄的是「這次 run 的時間」而非「這筆被隔離的時刻」，會讓 `rpt_quality_*` 的時間軸失真。

改為 `coalesce(quality_state_at, received_at)`：優先取 `initial_evaluation` 事件的 `event_at`（真正的隔離時刻），事件缺席時退回攝入時間。若日後需要跑批時戳，另開欄位、語意分開。

### 5.7 `int_order_items`：來源、`safe_cast`、嚴格 NULL 傳播

把 `ODS.items`（JSON 陣列）以 `unnest(json_query_array(items)) with offset` 攤平到 item 粒度，供未來的 `fct_order_items`。

- **來源選 `int_orders`（已過濾）而非 `stg_orders`**：item 層的錯誤（`quantity_non_positive`、`unit_price_negative`、`discount_pct_out_of_range`、`non_finite_number`）在攝入層就會讓**整張訂單** `has_clean_error=TRUE` 被隔離，所以從 `int_orders` 出發天然保證「Gold 不含髒資料」。要做 item 層 RCA 時另建讀 quarantine 的模型。
- **數值一律 `safe_cast`**：`clean.py` 明載「items 內的值未經 Pydantic 強轉，可能是字串」——items 整包以 JSONB 落地，欄位值不經 `ODSOrder` 型別強轉。用 `cast` 會讓一筆髒 item **炸掉整批**；`safe_cast` 轉不動 → NULL，符合本專案「標記不阻斷」的哲學。
- **衍生金額採嚴格 NULL 傳播，不 `coalesce`**：`net_amount = quantity × unit_price × (1 - discount_pct/100)`，任一輸入為 NULL 則結果 NULL。理由是 [CLOUD_LAYER-TW §5.5.5](../CLOUD_LAYER-TW.md) 的鐵律——NULL 帶資訊（「沒有折扣資料」≠「折扣為 0」），`COALESCE` 有損且單向，一旦在 `int_` 壓成 0，全下游再也分不出「沒收集」與「真的是 0」。若日後確認「缺值＝無折扣」，補值加在 `dim_/fct_`，**不回頭改本層**。
- **`(raw_id, item_index)` 是 item 粒度鍵**：`items` 在 ODS 內是不可變的 JSONB 快照、陣列順序固定，故位置可作為穩定身分；代理鍵 `order_item_key = raw_id-item_index`。

## 6. 測試策略

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

> 自訂 generic test 與部分內建測試的參數需巢狀在 `arguments:` 下（dbt 1.11 要求，否則 `MissingArgumentsPropertyInGenericTestDeprecation`）。

## 7. 常見操作 runbook

- **何時 `--full-refresh`**：改分區/叢集、改去重邏輯、回看窗外的歷史需重算、或首次建表。走 DDL、不受 sandbox 限制。（僅適用 `stg_` 的增量模型；`int_` 為 `table` 全量重建，每次 run 本就重建。）
- **Proposal C targeted refresh**：修正列落在舊分區、回看窗看不到 → 修復 runbook 最後一步對災區分區做 targeted refresh（`--full-refresh` 或未來對單分區 `insert_overwrite`）。見 [CLOUD_LAYER-TW §7.4](../CLOUD_LAYER-TW.md)、DQ C-2 #7。
- **改動 `int_orders` 或 `int_orders_quarantine` 前**：先過一遍 §5.2 對齊清單；改完跑 `dbt build --select intermediate+`，確認 `assert_orders_split_is_partition` 為綠。

## 8. 相依與版本

- dbt-core 1.11 / dbt-bigquery 1.11
- `packages.yml`：`dbt-labs/dbt_utils >=1.1.0,<2.0.0`（實裝 1.4.1）

## 9. 現況與待辦

- ✅ `stg_orders`（去重 + Hard Gate + freshness，增量）
- ✅ `stg_quality_events`（以 `id` 為 grain 去重，保留完整狀態機歷史）
- ✅ `int_orders` + Row Filter、`int_orders_quarantine`（劃分不變式有測試把關）
- ✅ `int_order_items`（items 攤平到 item 粒度）
- ⬜ 場景專用 `int_orders_*`（設計已備妥，待真實分析場景出現才啟用——見 §5.3）
- ⬜ `dim_/fct_`、`rpt_quality_*`
- ⬜ Proposal B（Airflow 重評估寫 `quality_events`）——下游回流路徑已就緒，只等事件產生端
