# 轉換層（dbt）

[English](../../en/design/transformation.md) | **繁體中文**

`stg_` → `int_` → `dim_`／`fct_` → `rpt_`。Quickstart 與指令見 [`ecommerce_dbt/README.md`](../../../ecommerce_dbt/README.md)。

---

## 1. 分層與命名

| 前綴 | 粒度 | 職責 |
|---|---|---|
| `stg_` | 來源粒度 | 1:1 對應、改名、轉型、去重。**不含業務邏輯** |
| `int_` | 來源粒度 | join、衍生欄位，**以及阻斷點** |
| `dim_`／`fct_` | 星型結構 | 供彈性分析的維度與事實 |
| `rpt_` | 固定 | 供 BI 的預先聚合 |

模型：`stg_orders`、`stg_quality_events`、`int_orders`、`int_orders_quarantine`、`int_order_items`、`dim_customer`、`dim_product`、`fct_orders`、`fct_order_items`、`rpt_quality_events_daily`、`rpt_quality_backlog`、`rpt_sales_daily_by_category`。

---

## 2. `stg_` 層

**實體表，不是 view。** 四股力量，第一個是決定性的：staging 帶著 `require_partition_filter`，而 **view 會把那道保險絲傳播給每一個下游消費者**。表切斷了那條鏈。它同時讓去重只付一次、給 DAG 根部一個一致的快照，並且是增量的前提。[ADR-0043](../adr/0043-stg-table-not-view.md)

**物化**：`incremental` + `insert_overwrite`，依 `received_at`（DAY）分區，帶 `var('stg_orders_lookback_days', 3)`。

正確性倚賴一條不變式：**同一個 `raw_id` 的所有副本都落在同一個 `received_at` 分區**，所以整分區替換不會漏掉任何東西。

⚠️ 那條不變式**必要但不充分**。它保證窗**內**的去重完整，對窗的**邊界**一句話都沒說——
而 `insert_overwrite` 的原子單位是整個分區，dbt 只覆寫「查詢結果裡出現過的分區」。
左邊界若落在某天中間，邊界那天就只有一部分的列進得了窗，於是**半天被原子覆寫成整天**，
窗外的列被靜默刪除。

所以左邊界**必須對齊日界**：

```sql
timestamp_sub(timestamp_trunc(current_timestamp(), day), interval N day)
--            └─ 這一層是正確性，不是風格
```

對齊後，邊界那天只有「整天在窗內」或「整天在窗外」兩種狀態，「半天」不再存在，
**跑批時刻也就不再是正確性的隱含前提**——那才是原本那個缺陷的本體：同一支模型
在 20:38 與 22:30 跑會產出不同結果。[ADR-0055](../adr/0055-partition-aligned-incremental-window.md) ·
[2026-08-30 事故](../incidents/2026-08-30-stg-partition-truncation.md)

⚠️ **這條規則適用於全部三支 `insert_overwrite` 模型**，不是只有 `stg_orders`：

| 模型 | 分區欄 | 回看窗 var | 對帳測試 |
|---|---|---|---|
| `stg_orders` | `received_at` | `stg_orders_lookback_days` | `assert_stg_orders_matches_staging` |
| `stg_quality_events` | `event_at` | `stg_quality_events_lookback_days` | `assert_stg_quality_events_matches_staging` |
| `rpt_quality_events_daily` | `event_date` | `rpt_quality_events_lookback_days` | （上游兩支覆蓋） |

2026-08-30 第一次套用這條規則時只改了 `stg_orders`，另外兩支照原樣留著、同一天稍晚才補完。
**缺陷是「寫法」層級的**——新增任何 `incremental` + `insert_overwrite` 模型時，這條規則與
它的對帳測試一併適用，而這件事目前靠 code review，沒有自動檢查（[ADR-0055](../adr/0055-partition-aligned-incremental-window.md) 代價五）。

**定點回填**：`<model>_backfill_start` / `_end` 讓修復路徑用日期指定分區，與跑批時刻無關。
例行路徑仍走滾動窗。守門的是逐分區對帳測試。

⚠️ **回填舊分區時，下游的增量模型必須用同一組日期跟著補。** `table` 物化的下游會自動跟上，
但下游的**增量**模型只重算自己回看窗內的分區——被補的舊分區落在窗外，於是它照舊維持舊值：

```bash
dbt run -s stg_quality_events   --vars '{stg_quality_events_backfill_start: "2026-08-26"}'
dbt run -s stg_quality_events+ --exclude stg_quality_events
dbt run -s rpt_quality_events_daily --vars '{rpt_quality_events_backfill_start: "2026-08-26"}'
```

失敗模式是最惡劣的那種：**上游正確、下游錯誤、所有測試全綠、BI 繼續顯示舊數字。**

**`copy_partitions: true`**——sandbox 禁止 DML，而 `insert_overwrite` 預設用 `MERGE`。Copy job 是儲存層級、非 DML、免費的，而且它在**語意上**本來就更合適：去重產出的是一天的完整內容，所以整批替換才是正確的操作。[ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)

**去重**：`row_number() over (partition by raw_id order by received_at desc, id desc) = 1`。

鍵是 `raw_id`，不是 `id` 或 `order_id`——Proposal C 的遷移形修正會以「新 `id`、同 `raw_id`」抵達，所以只有以 `raw_id` 分組，修正才能與舊副本競爭。當 `rebuild_batch_id` 存在時，它必須被**放到 `order by` 最前面**。

**`on_schema_change='append_new_columns'`**——「staging 只做加法」在 dbt 側的鏡像。刻意不用 `sync_all_columns`，因為它會 `DROP`。

**Hard Gate** 掛在這裡：見 [data-quality §3](./data-quality.md)。

---

## 3. `int_` 層

阻斷發生的地方。三個模型，而 `int_orders` + `int_orders_quarantine` 必須是 `stg_orders` 的一個**劃分**——互斥且窮盡，由 `assert_orders_split_is_partition` 斷言。

### 有效狀態區塊刻意重複

那段合成邏輯被寫了**兩次**、逐位元組相同、用 `═══` 標記圍起來——不抽成共用模型。

澄清性的事實：**ephemeral** 模型會被 inline 進每一個下游，所以它不建立額外關聯，**也不省下任何 JOIN 執行**。只有物化的共用表才會。**所以在這裡，共用 vs 重複純粹是維護面的取捨，不是成本面的。**

選擇重複是因為今天只有兩個消費者、每個模型檔保持自給自足，而代價——互補性從機制保證降為紀律保證——**用一個測試買回來了**。

**收斂 trigger：第三份副本。** [ADR-0045](../adr/0045-int-effective-state-duplication.md)

### 對齊清單

任一模型變更時都要走一遍：

| # | 檢查 | 弄錯的話 |
|---|---|---|
| 1 | 兩邊 `is_effectively_clean` 定義相同；一邊 `WHERE cond`、一邊 `WHERE NOT cond` | 有些列兩張表都不在（**靜默遺失**）或都在 |
| 2 | `coalesce(..., false)` 不可拿掉 | 該列**同時從兩張表消失** |
| 3 | 一律 `LEFT JOIN` | 丟掉每一列沒有品質事件的記錄 |
| 4 | window 的 `partition by` / `order by` 決勝鍵一致 | 兩邊挑到不同事件 |
| 5 | `effective_quality_state` 的 CASE 分支一致 | `rpt_quality_*` 算錯 |
| 6 | 兩者物化方式相同 | 劃分在 run 之間破裂 |
| 7 | `assert_orders_split_is_partition` 維持 `severity: error` | 唯一的自動化安全網消失 |

### 物化：`table`，全量重建

刻意**不**依 `received_at` 增量。Proposal B 的 promotion 事件落在**今天的**分區，而它救的那筆訂單坐在一個**舊的**分區——增量窗口永遠不會重算那個分區，被 promote 的記錄也就永遠流不回去。而且是靜默的。[ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md)

`CREATE OR REPLACE` 是 DDL，所以它也繞開了 sandbox 的 DML 禁令。

#### 何時該切換：看可觀測的數字，不看訂單數

實測基準：`int_` 層跑一次掃描 **910 KB 處理 554 筆訂單 → 每筆訂單每次執行約 1.64 KB**（三個模型合計；這個比例本質上是列寬，所以資料成長後仍然成立）。

| 訂單總量 | 每次執行掃描量 | 日批次的月成本（on-demand，$6.25/TiB） |
|---|---|---|
| 1000 萬 | 16 GB | 約 $3 |
| 1 億 | 164 GB | 約 $30 |
| 10 億 | 1.6 TB | 約 $300 |

Sandbox 每月 1 TiB 的免費額度，在日批次下大約撐到 **1500–2000 萬筆訂單**，還沒扣掉 `stg_` 與 `dim_`／`fct_` 的用量。

**但成本不是你先撞到的瓶頸。** 有兩件事更早咬人：

1. **`profiles.yml` 的 `job_execution_timeout_seconds: 300`**——它讓執行**失敗**，而不是讓它變貴。目前全量重建耗時 **2.5 秒**。
2. **整個 DAG 的批次窗口**，一旦 `int_` 的全量重建疊在同樣全量的 `dim_`／`fct_` 重建之上。

**判準**：追蹤 `target/run_results.json` 裡的 `bytes_billed`（月累計）與 `execution_time`；任一達到配額或逾時的 **50%** 時就開始評估。在那之前，全量重建是那個**正確性免費、複雜度為零**的選項。

> **一個刻意接受的不對稱**：`stg_` 的增量省下重算與寫入，但 `int_` 每次執行仍然掃過整個 `stg_`——所以**這條管線的讀取成本仍然隨歷史總量成長。** 那是一個知情的取捨，換來的是 `int_` 的正確性無條件成立。


**不分區，只以 `order_id` 叢集**——`int_` 只在 DAG 內部被消費，分區買不到任何東西。**那個前提正是 [§5](#5-rpt_-層) 所保護的東西。**

**`int_order_items`**：items 攤平到 item 粒度，衍生金額用 `safe_cast` 並**嚴格傳播 NULL**——不做 `coalesce`。`quarantined_at` 取事件時間，而非 `CURRENT_TIMESTAMP()`（那會記錄成 run 發生的時間）。

---

## 4. `dim_`／`fct_` 層

**雙事實表**：`fct_orders`（表頭）與 `fct_order_items`（明細）。

**度量上捲進表頭**，並由 `assert_fct_orders_rollup_matches_items` 斷言逐訂單相等。與 `int_` 層是同一個手法：花一個測試把紀律保證換成機制保證，並換回單表可查詢性。必須用 `is distinct from` 而非 `=`——`NULL = NULL` 是 NULL，所以 `=` 會靜默濾掉那些最可能出錯的列。

> 那個測試抓到了它並非為此而設計的缺陷：39 列差 **1 ULP**，因為 `FLOAT64` 上的 `SUM()` 不具結合律。修法是把金額移到 `NUMERIC`——**而不是**把測試放寬成容差。

**`SUM()` 會忽略 NULL**，所以一個品項 `safe_cast` 失敗，就會讓該訂單的總額少了恰好一個品項而毫無痕跡。補救不是 `COALESCE`，而是讓不完整明確化：`fct_orders.items_missing_amount`。**我們不代替消費者決定 NULL 等於零。**

相關：`item_count = 0` 把「一筆沒有品項的訂單」表達成一個**值**，而 `fct_orders` 必須 `LEFT JOIN` 那個上捲——`INNER` 會讓那整類從 Gold 消失。

**只建兩個維度**，SCD1 帶明確決勝鍵。`dim_date`、`dim_geography` 與 junk dimension 全部退化到事實表上。SCD1 的失真由 `fct_orders.membership_tier_at_order` 買回來——**以零基礎設施得到 type-2 的效果**。[ADR-0047](../adr/0047-measures-roll-up-to-header.md) · [ADR-0048](../adr/0048-two-dimensions-scd1.md)

**`dim_product` 的衝突是標記而非阻斷**：同一個 `product_id` 可能帶著不同屬性抵達。2026-08 實測 342 個中有 163 個衝突——根因是產生器的 bug，之後已修正。標記代表那個 bug **在資料裡是可見的**，而不是被藏在一次失敗的 build 後面。

**分區**：`fct_*` 依 `order_date`（DAY）；`dim_*` 不分區——維度是靠鍵 join 觸及的，分區欄位在那裡剪不掉任何東西。

### 刻意未定義的業務規則

有三件事在需求中未定義，而且**刻意不做假設**——與「不建投機性模型」是同一條原則：

| 項目 | 未定義的是什麼 | 目前的處理 |
|---|---|---|
| `tax_amount` | 稅基是 `net`，還是 `net + shipping`？ | 只暴露 `tax_pct`——**那是一個比率，非可加，絕不可 `SUM`**。不建任何衍生金額 |
| 淨營收 | `returned = TRUE` 的訂單該不該被扣掉？ | `returned` 以旗標留在事實表上；**由下游決定** |
| `profit_amount` | 毛利含不含運費與稅？ | 不建；下游可自行計算 `net_amount - cost_amount` |

> **一個捏造出來的假設，會讓一個錯誤的數字看起來像事實。** 把比率與旗標留在事實表上，等於把決定推給真正知道答案的人——**並讓那個缺席可見，而不是用一個看似合理的預設把它糊掉。**

---

## 5. `rpt_` 層

**業務報表只讀 Gold，絕不直接讀 `int_`。** 四個理由，而第四個是架構性的：**「`int_` 只在 DAG 內部被消費」是 `int_` 不分區的唯一理由**——`rpt_` 去讀它，等於把它升級為公開契約，並讓它變得無法重構。

**合法的例外是品質報告**，因為被隔離的列按定義永遠不會抵達 Gold。

> ⚠️ 品質比率的分母是整個 `stg_orders`，**含髒資料**——不是 `fct_orders`。用 Gold 當分母，`quarantine_rate` 恆等於零。

### 品質報告拆成兩張表

| | `rpt_quality_events_daily` | `rpt_quality_backlog` |
|---|---|---|
| 軸 | **事件軸**（`event_at`） | **快照** |
| 一列代表 | 那天發生了 N 次事件 | 現在有 N 筆卡著 |
| 會被回溯改寫 | 否（append-only） | 是——它**就是**當前狀態 |
| 可增量 | ✅ | ❌ |

Backlog 不能從事件軸累加：`quality_events` 60 天過期，所以**累加的起點會消失，而失真是單向的**——backlog 會被系統性低估。[ADR-0049](../adr/0049-business-reports-read-gold.md)

**關於誠實的一段話**：教科書給 `rpt_` 的正當理由是效能，而在這個資料量下那一文不值。真正的理由是**每個指標一份固定定義**與 **BI 不必自己組 join**。

---

## 6. 測試

| 類型 | 例子 |
|---|---|
| generic | `not_null`、`unique`、`accepted_values`、`relationships` |
| 自訂 generic | `error_rate_below`（Hard Gate） |
| singular | `assert_orders_split_is_partition`、`assert_fct_orders_rollup_matches_items` |
| source | `dbt source freshness`——在自己的 DAG 裡（[orchestration](./orchestration.md)） |

共 93 個測試。那兩個 singular test 是承重的：**每一個都把一份紀律轉成一個機制。**

---

## 7. 相關

- [data-quality](./data-quality.md) — 這些模型所實作的層契約
- [cloud-layer](./cloud-layer.md) — `stg_` 的來源從哪裡來
- [orchestration](./orchestration.md) — 這些層如何被執行
