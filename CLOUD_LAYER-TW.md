# 雲端層架構：ODS → BigQuery 抽取與 staging

## 範圍與職責邊界

本文件記錄「雲端層」的設計決策——即資料離開 PostgreSQL（ODS）、進入 BigQuery 之後的抽取（E/L）與落地（staging）。轉換層（dbt `stg_`→`int_`→`dim_/fct_`→`rpt_`）的品質契約見 [DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md)。

```
ODS (PostgreSQL) ──[ E/L：Python ]──► BigQuery staging ──[ T：dbt ]──► stg_/int_/dim_/fct_/rpt_
```

**為什麼 E/L 與 T 切兩段**：兩者「失敗語意」不同。E/L 失敗要從 watermark 續傳、要冪等；T 失敗只要重跑 SQL。混在一起會讓兩種錯誤糾纏。

---

## 1. Staging 表設計

### 1.1 批次載入，不用串流

staging 由抽取腳本以 batch load job 灌入。BQ 的 streaming insert 按量計費，而 batch load job 免費；本專案是 T+1／小時級批次，沒有即時性需求，故一律走 batch load。串流只有在下游接即時預測模型時才有動機。staging 因此是一張由批次累積而成的實體表（ODS 的 append 鏡射）。

### 1.2 分區：`received_at`（DAY）

分區欄位選法的原則是「選那張表最常、最貴的查詢拿來過濾的欄位」。staging 的 access pattern 是**管線增量**（抽取 watermark、dbt incremental 都過濾 `received_at`），所以分區用 `received_at`，讓每次跑批只掃新增的分區（partition pruning）。

> **`received_at` vs `order_date`**：staging 服務管線，故用攝入時間 `received_at` 分區；下游 Gold（`dim_/fct_`）服務分析師，月/週平均按業務時間 `order_date` 過濾，那一層才改用 `order_date` 分區。分區欄位是「每張表依自己的 access pattern 各自選」。

粒度選 DAY 不選 HOUR：批次是 T+1／小時級；且單表上限 4000 分區，DAY 可撐約 11 年、HOUR 僅 166 天。

### 1.3 叢集：`order_id` + `has_clean_error`

分區內再依叢集欄位排序聚集，過濾這些欄位時跳過不相關區塊。選 `order_id`（下游 JOIN／去重，高基數）優先、`has_clean_error`（`int_` 的 Row Filter 每次必走）次之。

### 1.4 保險絲：`require_partition_filter=True`

任何查 staging、沒帶 `received_at` 過濾的查詢直接報錯，擋掉「不小心全表掃」的燒錢意外。staging 的存取一律帶 `received_at`，故對它幾乎零副作用。

> **連帶效應**：保險絲會擋掉 `SELECT MAX(received_at) FROM staging` 這種無過濾查詢——直接影響 watermark 讀法（見 §2）。

### 1.5 location 一致：`US`

BQ 每個 dataset 建立當下綁定 location 不可改；跨 location 查詢會直接報錯。所有 dataset（staging、dbt_dev、未來 dim/fct）統一建在 `US`，建 dataset 時明確指定、不靠預設。

---

## 2. Watermark 策略

### 2.1 方案 A：從 `INFORMATION_SCHEMA.PARTITIONS` 推導

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `<project>.staging.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders' AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

特性：**免費**（metadata，不掃資料）、**不受保險絲限制**（查的是 metadata view 非表本體）、**無狀態**（watermark 由 staging 自身推導，load 完下次讀即反映新資料，故沒有 `advance_watermark()` 步驟）。邊界用 `>=`，配 dbt `stg_` 去重 → 寧可重抓不漏抓。

### 2.2 `get_watermark()` 抽象＝換方案 B 的唯一接縫

方案 A 的精度被分區粒度卡死（DAY → 每次重抽「最新分區整天」）。批次頻率拉高時：

| 批次間隔 | 方案 A | 對策 |
|---|---|---|
| 日批 / T+1 | ✅ 重抽量微不足道 | A |
| 小時批 | ⚠️ 每跑重抽當天至今 | 改 HOUR 分區（受 4000 上限約束，需設過期）|
| 分鐘級微批 | ❌ 同日重抽數百次 | **方案 B**（獨立 watermark 表，精確到 timestamp）|

判準：**批次間隔 ≈ 分區粒度 → A；批次間隔 ≪ 分區粒度 → B。** 換 B 時只改 `get_watermark()`、並在 load 成功後新增 `advance_watermark()`，`main()` 不動。方案 B 的代價是狀態管理（存放位置、load-then-advance 順序不變式、bootstrap、失敗面、併發），金錢成本仍 ≈ 0。另有更硬的天花板：batch load job 每表每天 1500 個，逼近每分鐘批次。

---

## 3. 載入策略

- **WRITE_APPEND**：冪等靠 append + dbt `stg_` 去重，不在 E/L 做 MERGE（保持原樣落地）。
- **JSON 欄位傳原生物件，非 `json.dumps`**（實機驗證結論）：psycopg2 解 JSONB 本就是 list/dict，直接傳，client 寫 NDJSON 時嵌成原生 JSON，BQ 存成 `JSON_TYPE=array/object`。若用 `json.dumps` → BQ 存成 JSON 字串純量，下游 `[0]` 索引失效。
- **`ALLOW_FIELD_ADDITION`**：支援 additive evolution，見 §5。

---

## 4. 設定與安全

- **`bq_project` 由 `Settings` 注入**，不寫死在模組：project ID 會隨部署環境而異（dev/prod 不同專案），屬 `config.py` 既定的「環境設定」邊界。它**不是機密**（安全靠 IAM 不靠隱密），但作為公開 repo 的基礎設施座標，注入可讓真實 ID 不入版控。
- **認證走 ADC**（`bq.py`）：本機讀金鑰路徑橋接環境變數，正式環境由平台注入，同程式碼零修改換環境（prod-parity）。
- **陷阱**：BQ client 要的是 project **ID**（GCP 常自動補數字後綴，如 `-498602`），不是顯示名稱。

---

## 5. ODS Schema 演進策略 ⭐

### 5.1 上游漂移 ≠ ODS 改動

攝入層**刻意容忍**上游 schema 漂移（見 DQ 兩訊號治理）：多送的欄位進 `unmapped_fields` + `has_schema_drift`；少送的落 NULL；型別漂移標 `TYPE_DRIFT`。**ODS 的欄位結構不會自己跟著變**——drift 只是訊號。ODS 真正演進，是工程師**刻意**經 Alembic migration 加/改欄。

### 5.2 BQ 能就地 migrate 什麼

| ODS 變更 | BQ 能就地? | 雲端層做法 |
|---|---|---|
| 加 nullable 欄 | ✅ `ALLOW_FIELD_ADDITION` | staging 自動接 |
| REQUIRED→NULLABLE | ✅ relaxation | staging 放寬 |
| 刪欄 | ✅ DROP（但丟歷史）| **不刪**，留著，dbt `stg_` 忽略 |
| 改名 | ✅ RENAME | **不改**，加新欄，dbt `stg_` rename |
| 型別不相容 | ❌ | 加新欄 + dbt cast |
| 改分區/叢集 | ❌ | 重建表（CTAS）|

### 5.3 刻意的紀律：staging 只做加法，改名/轉型丟給 dbt

即使 BQ 做得到 DROP/RENAME，staging 仍**刻意**選「只加不動」：① 保留歷史；② BQ DDL 無版控、不像 Alembic，把改名/轉型放進 **dbt `stg_`** SQL 才有 git 版控與 review；③ 解耦實體演進（罕見、只加）與邏輯演進（頻繁、在 SQL）。

> **不對稱**：ODS 有 Alembic 這個正式 migration 框架；staging **沒有對應框架**（dbt 從 `stg_` 才接手，不管 staging 本身）。實務上以 `ALLOW_FIELD_ADDITION` 補加欄、其餘交 dbt 作為替代。唯一「必須重建」的情況（改分區）在方案 A 下成本很低：`drop + recreate + 重抽`，watermark 自動歸零。

### 5.4 治理：`FIELDS` 是 schema 的第三份宣告

`extract_ods_to_bq.FIELDS` 是 ODS schema 繼 `schema.py`、`models.py` 之後的第三份手維護宣告，且漂移時最糟是「靜默漏抽資料」。以 `tests/test_schema_bq_consistency.py` 把它和 `models.py` 的一致性（欄位齊備、型別、可空性）變成會紅的測試——延伸 DQ 文件機制 1 的精神到抽取層。`FIELDS` 同時驅動 BQ schema、序列化、與這份測試（單一真相來源）。

---

## 6. 實機驗證記錄（2026-06）

| 驗證項 | 結果 |
|---|---|
| 分區/叢集/保險絲/location | `received_at(DAY)` / `[order_id, has_clean_error]` / `True` / `US` |
| 保險絲 | 無 `received_at` 過濾的查詢被 400 擋下 |
| JSON 落地 | `items`、`clean_error_message` 皆 `JSON_TYPE=array`；下游 `JSON_VALUE(...[0],'$.code')` 取值正確 |
| additive load 路徑 | explicit schema + `ALLOW_FIELD_ADDITION` 不破壞 happy path |
| 一致性測試 | `test_schema_bq_consistency` 全綠 |

---

## 7. 修正批次的回流路徑（Proposal C 的雲端側）

[DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md) 的 Proposal C（歷史值缺陷的批次修復，方向性設計、尚未實作）在雲端層有四件事要知道：

### 7.1 watermark 永遠看不到修正列——上雲是主動步驟

修正列保留原本的 `received_at`（落回舊分區），而方案 A 的 watermark 是 `MAX(partition_id)`、只往前看；例行增量抽取的 `received_at >= 最新分區` 條件永遠撈不到舊分區裡的新列。所以「推上 staging」必須是修復 runbook 的主動步驟（按 batch id 圈出修正列、呼叫既有 `load_to_staging()` append），不是等例行排程。watermark 機制從頭到尾不參與、也不需要動。

### 7.2 遷移式形態：複用 append + dedup 通道，不需要 JOIN

staging 是 append-only：修正列 append 後，同一 `raw_id` 會永遠存在兩列（錯的舊列 + 對的新列），且兩列的 `received_at` / `raw_id` / `order_id` 完全相同——「取最新」沒有現成的排序依據，`stg_` 去重必須以 `rebuild_batch_id DESC NULLS LAST` 決勝。batch id 因此是回流機制的功能性零件，不只是稽核欄位。額外紅利：若災區右邊界落在當天分區，例行抽取會把修正列再抽一次——無害，重複的兩列連 batch id 都相同，去重取誰都一樣（「寧可重抓不漏抓」的既有設計直接吸收）。BQ load job 整批原子，不會出現半批可見。

### 7.3 補丁式形態：第二張表、另一份手維護宣告、一個重抽地雷

corrections 若另成一張 BQ 表：需要自己的 `FIELDS` 宣告、抽取邏輯、與 `test_schema_bq_consistency` 同級的一致性守衛（§5.4 的精神同樣適用）；且任何 staging 全量重建（如 §5.3 的改分區情境）會把主表錯值原樣重抽上去——**重建步驟必須明文包含補推 corrections**，否則錯值復活。

### 7.4 late-arriving：災區分區需 targeted refresh

修正值落在舊分區，按 `received_at` 增量的 `stg_` 例行跑批看不到。runbook 最後一步必須對災區分區做 targeted refresh（insert_overwrite 該批分區，或對 `stg_` 單一模型一次性 full-refresh）。在 push 完成前搶跑的例行 dbt run 只是「尚未生效」，不是錯誤狀態。

---

## 8. 待辦與未來

- 微批升級時：`get_watermark()` 換方案 B（+ `advance_watermark()`）。
- 進 dbt 分層（`stg_` 起）：已起，見 [ecommerce_dbt/README.zh-TW](./ecommerce_dbt/README.zh-TW.md)。
