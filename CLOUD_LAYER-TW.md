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

### 1.6 第二張 staging 表：`quality_events`（與 orders 的刻意差異）⭐

`orders` 之外，抽取腳本同時把 `quality_events`（append-only 品質事件日誌）抽上 staging。**為什麼要抽**：下游 `int_*` 合成「有效品質狀態」時，要把 ODS 快照與 `quality_events` 最新事件 JOIN 起來（Proposal B promote 的記錄在 ODS 仍是 `has_clean_error=TRUE`，靠事件才流得回 Gold）——沒有這張表，回流機制的右表就不存在（見 [DQ_ARCHITECTURE-TW〈機制二：Row Filter〉](./DQ_ARCHITECTURE-TW.md)）。

它的表設計**不照抄 orders**，因為 access pattern 相反。§1.2–1.4 的每個決定都要重問一次：

| 決策 | orders | `quality_events` | 為什麼不同 |
|---|---|---|---|
| 分區 | `received_at`（DAY） | `event_at`（DAY） | 各表用自己的攝入時間軸；`event_at` 同時餵 watermark（方案 A 讀最新分區）|
| 叢集 | `order_id` + `has_clean_error` | `raw_id` + `to_state` | 下游以 **`raw_id`** 為 grain 取「每筆記錄的最新狀態」（與 dbt `stg_` 去重同鍵）；`to_state` 供狀態過濾 |
| 保險絲 | ✅ 開 | ❌ **關** | **關鍵差異**：orders 的查詢永遠帶 `received_at` 過濾；但 `quality_events` 的主消費者是「跨全歷史按 `raw_id` 取最新」，本質是**非分區過濾的全掃描**，開保險絲會直接擋掉這個必然查詢 |

> **回流比 orders 乾淨**：Proposal B 補的 promotion 事件 `event_at = now()`，落**當天**分區，例行增量 `event_at >= watermark` 自然撈得到；不像 orders 修正列落回**舊**分區、需要修復 runbook 主動補推（見 §7.1）。append-only 的時間語意讓 `quality_events` 的 E/L 反而更單純。

> **跨表一致性見 §3.2**：兩張表獨立抽取、獨立 watermark、獨立 load job；「orders 上了但 `quality_events` 沒上」怎麼防，見〈跨表一致性〉。

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

### 3.1 單表落地語意

- **WRITE_APPEND**：冪等靠 append + dbt `stg_` 去重，不在 E/L 做 MERGE（保持原樣落地）。
- **JSON 欄位傳原生物件，非 `json.dumps`**（實機驗證結論）：psycopg2 解 JSONB 本就是 list/dict，直接傳，client 寫 NDJSON 時嵌成原生 JSON，BQ 存成 `JSON_TYPE=array/object`。若用 `json.dumps` → BQ 存成 JSON 字串純量，下游 `[0]` 索引失效。
- **`ALLOW_FIELD_ADDITION`**：支援 additive evolution，見 §5。

### 3.2 跨表一致性：per-table load job + gate（無跨表交易）⭐

多表抽取（orders + `quality_events`）帶出一個地端沒有的問題：**BQ 的 load job 只保證「單表」原子，跨表沒有交易**——無法像地端 Postgres 把兩張表的寫入包進同一個 commit。於是要防「orders 上去了、`quality_events` 沒上」導致 dbt 建在半套資料上。

不靠原子落地，靠**兩層防線**：

1. **各表獨立 watermark、失敗不推進**：每張表的 watermark 由自己的 staging 分區推導（方案 A，§2）。一張 load 失敗，它的 watermark 就沒前進，下輪 `event_at/received_at >= watermark` 自動把那批補抽（append-only + `>=` + dbt `stg_` 去重）。orders 成不成功，完全不影響 `quality_events` 的 watermark——這個獨立性正是自癒的來源。
2. **`main()` 的 gate**：逐表盡力抽取（一張失敗不擋另一張各自推進），最後彙整——**任一張失敗即整體 `raise`（非零 exit）**，下游 dbt(T) 不得開跑。現在手動階段是「全成功才接著跑 dbt」；Phase 5 Airflow 則落成「dbt task 的上游依賴＝兩個 extract task 都 success」，同一條 gate 語意。

**一致性模型是「最終一致」，非交易**：skew（一張到、一張沒到）只會造成**延遲**（某筆該回流的髒單晚一個 dbt run），不會造成髒資料——前提是下游 `int_*` 的 JOIN 寫成保守合成（事件缺席 → fall back 到 ODS 快照：乾淨照流、髒的續留 quarantine）。這也是維持「兩張獨立 load job」而非硬湊跨表原子的理由：獨立才好各自自癒、各自重試。

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

### 5.4 治理：每張表一份 `FIELDS`（schema 的第三份宣告）

每張 staging 表的 `FIELDS` 是該表 schema 繼 `schema.py`、`models.py` 之後的第三份手維護宣告，且漂移時最糟是「靜默漏抽資料」。抽取腳本用一個 `TableSpec` 把每張表的抽取契約收成一份物件——`table` / `model` / `time_col` / `fields` / 分區 / 叢集 / 保險絲——目前有兩份：`ORDERS_SPEC`（鏡射 `ODS`）與 `QUALITY_EVENTS_SPEC`（鏡射 `QualityEvent`，見 §1.6）。

每份 `fields` 同時驅動三處（單一真相來源）：BQ schema（`ensure_staging_table`）、列序列化（`_to_bq_dict`）、與一致性測試。`tests/test_schema_bq_consistency.py` 以 `SPECS` 逐表參數化，把「改了 `models.py` 卻忘了改 `fields`」變成會紅的測試（欄位齊備、型別、可空性四類）——延伸 DQ 文件機制 1 的精神到抽取層。**每加一張表只需在 `SPECS` 掛一份 spec，一致性守衛自動涵蓋**，不必另寫測試。

### 5.5 端到端範例：加欄 / 刪欄（含後續 NULL 處理）⭐

§5.2 的對照表是「哪種 ODS 變更、BQ 能不能就地」的靜態矩陣；本節是它的**逐步走查版**，把兩個最常見的變更從 ODS 一路追到 dbt `stg_`，並接上各自產生的 NULL 該怎麼處理。

前提：這裡的「加/刪欄」＝工程師**刻意**經 Alembic 改 ODS（§5.1 的刻意演進），**不是上游 drift**（drift 不動 ODS 結構）。`stg_orders` 已設 `on_schema_change='append_new_columns'`（見 [ecommerce_dbt/README.zh-TW §4.7](./ecommerce_dbt/README.zh-TW.md)）。

兩例產生的 NULL 在時間軸上是**鏡像**，處理哲學因此相反：

| | NULL 長在哪 | 語意 |
|---|---|---|
| 加欄 | 過去（歷史分區） | 這欄在那段歷史**根本不存在** |
| 刪欄 | 未來（停收後往後長） | 這欄之後**不再被填** |

**共同第一步永遠是先判斷 NULL 屬於哪一種**，再決定接受 / 回填 / 補值——判錯就會用錯工具。

#### 5.5.1 加欄：流程

| # | 關卡 | 動作 |
|---|---|---|
| 1 | ODS | Alembic 加一個 **nullable** 欄（NOT NULL 加欄無法走 `ALLOW_FIELD_ADDITION`，既有列會違反）|
| 2 | 一致性測試 | `test_no_ods_column_missing_from_fields` 變紅——「ODS 有、`FIELDS` 沒有」被擋下（否則靜默漏抽）|
| 3 | `FIELDS` | 補上該欄（型別/mode 對齊，否則型別/mode 測試也紅），測試轉綠＝三份宣告重新對齊 |
| 4 | 抽取＋載入 | `ALLOW_FIELD_ADDITION` 自動把新欄加進 staging 實體表；舊分區歷史列 NULL、新列有值 |
| 5 | `stg_orders`（未改清單）| `source` 的 `select *` 撈進來，但**最終顯式 SELECT 不列它 → 丟掉**；模型產出不變、下游看不到，只是「靜默搭車」躺在 staging |
| 6 | `stg_orders`（改清單顯現）| 把欄加進顯式 SELECT（進 git、被 review）→ 下次**一般增量跑批**即可：dbt 自動 `ALTER ADD COLUMN`（metadata、免費、舊分區 NULL）+ copy job 只覆寫回看窗分區。**免 `--full-refresh`、免全表重寫**，成本 ∝ 近期資料 |

#### 5.5.2 加欄：歷史大量 NULL 的後續處理

先分岔關鍵判斷：**這欄的歷史是「不存在」還是「漏抽」？**

| 處理 | 適用 | 做法 | Why |
|---|---|---|---|
| A. 接受 NULL（預設）| 值真的從現在才開始收集（新制上線）| 不填；下游按時間切或 `WHERE col IS NOT NULL` | NULL 誠實反映「過去沒有」，硬填＝製造假資料。成本 0 |
| B. Proposal C 回填 | 值其實一直在 Raw 裡，只是 ODS 之前沒對映（漏抽）| 從 Raw 用新對映批次重產 → push 修正列 → 災區分區 targeted refresh（見 §7、DQ Proposal C）| 「值缺漏」類，A/B remediation 管不到，正是 Proposal C 領域。重、但一次付清 |
| C. 下游補值 | 分析需要非 NULL（SUM/AVG 不想被稀釋、報表要顯示 0）| `int_/dim_` 層 `COALESCE(col, <default>)`，model description 記錄語意 | `stg_` 保持忠實（NULL），補值屬分析層業務決策（DQ 機制三：SQL 即審計）|
| D. 攝入時給 default | 業務上必然有值（如 `dq_rule_version`）| ODS migration 就設 default/NOT NULL，歷史列當下填滿 | 把「要不要 NULL」推到最上游最便宜的時點；代價是 NOT NULL 加欄需 migration 內填值，不走 `ALLOW_FIELD_ADDITION` |

⚠️ **`append_new_columns` 的盲區**：`ALTER ADD COLUMN` 把**所有**舊分區設 NULL，但一般增量只回填**回看窗**那幾天。若這欄在 staging 已存在一陣子（欄位引入時點 ≪ 加進 `stg_` SELECT 的時點），中間「staging 有真值、但在回看窗外」的分區，`stg_` 會**錯誤停在 NULL**。補救＝對那段區間一次性 targeted refresh、臨時放大 `stg_orders_lookback_days`、或該欄首次上線單獨 `--full-refresh` 一次。故「免 full-refresh」精確講是**免「未來每次」全表重寫**，首次若有歷史落差仍要一次性補。

#### 5.5.3 刪欄：流程

| # | 關卡 | 動作 |
|---|---|---|
| 1 | ODS | Alembic drop 欄，`models.py` 不再有它 |
| 2 | 一致性測試 | `test_no_stale_field_without_ods_column` 變紅——「`FIELDS` 有、ODS 沒有」的殘欄被擋下 |
| 3 | `FIELDS` | 移除該欄，測試轉綠；`_to_bq_dict` 不再吐它 |
| 4 | 抽取＋載入 | staging 實體欄**不刪、留著**（§5.2）；load schema 少該欄 → 新列 NULL、歷史列保留原值 |
| 5 | `stg_orders` | 顯式清單仍含該欄 → 照常查（staging 還在，新列讀 NULL、舊列讀原值）、**不 breaking**，變成 legacy 欄 |
| 6 | 要從模型移除 | **預設：留 legacy、不動**——`append_new_columns` 只加不刪，**刻意不介入 DROP**（對齊「staging 只做加法、刪欄留 legacy」§5.2/§5.3）。真要拿掉才 `--full-refresh` 重建（罕見、刻意的 escape hatch；若下游 `int_/dim_` 仍引用它，會在那次 `dbt run` 報錯，於 DAG 內被抓）|

#### 5.5.4 刪欄：未來大量 NULL（legacy 欄的 NULL 尾）的後續處理

這欄有真實歷史、未來 NULL 越長越長；問題從「怎麼填」變成「怎麼**不被誤用**」。

| 處理 | 適用 | 做法 | Why |
|---|---|---|---|
| A. 凍結留存（預設）| 大多數情況 | 讓它躺著：歷史可查、未來 NULL；要用就限歷史區間 | 對齊 §5.2/§5.3「不刪、留著保歷史」；BQ 儲存極廉，NULL 尾成本 ≈ 0 |
| B. 標記有效期，防誤用 | 有下游會碰它 | model description / 註記「X 日後停填」，或 `int_/dim_` 明確 `WHERE order_date < 停用日` 才引用 | 防止未來的人對半死欄做 `AVG` 被 NULL 尾稀釋（消費者契約問題，呼應 DQ Proposal C-4 P4）|
| C. 真的清掉 | 確定不需要、可接受丟歷史 | 從 `stg_` 顯式清單移除 + `--full-refresh` 重建（`append_new_columns` 不 DROP，故必須 full-refresh）| 唯一能讓欄「消失」的路。罕見、刻意 |
| D. 歸檔後移除 | 要主線乾淨又要留稽核 | 先把含該欄的歷史快照另存 archive 表，再從主線移除 | 兼顧「主線乾淨」與「歷史可稽核」，類比遷移式 `ods_retired_<batch>`。中成本、多一張表 |

#### 5.5.5 NULL 處理該落在哪一層（`int_` vs `dim_/fct_`）

先分兩種 NULL 處理：**(a) 消費者無關的正規化**（對所有下游客觀正確、答案唯一）與 **(b) 消費者相關的分析/呈現決定**（NULL→0 好聚合／保留 NULL 以計缺漏率／NULL→'unknown' 當維度桶——答案隨問題而變）。上面兩個結構性 NULL 幾乎都是 (b)。

核心語意原則：**NULL 帶資訊（「不存在 / 停止收集」），`COALESCE` 是有損且單向**——一旦在 `int_` 把 NULL 壓成 0，全下游再也分不出「沒收集」與「真的是 0」，想算涵蓋率的 `fct_` 就永遠算不出來。故：**保留 NULL 越久越好，只在「那個具體問題讓 collapse 變正確」的那層才 collapse**；填預設值是業務/呈現決定，屬 dim/fct/rpt 的高度，不是 int_ 管線（呼應「品質責任往下游收緊」）。

| 面向 | 放 `int_`（早、共享）| 放 `dim_/fct_`（晚、貼近消費者）|
|---|---|---|
| 可逆性 | 差：NULL 資訊在此消失、下游救不回 | 好：局部決定、爆炸半徑小 |
| 一致性 | 全下游同一解 → 只有 (a) 受惠 | 各取所需 → (b) 的天然歸屬 |
| 語意 | 對 (b) 是「替所有人做了不該替他做的決定」| 每個問題自己決定 |

文件既有樣板：DQ 機制三的場景補值**已放 int_**，但用**新欄**（`customer_rating_cleaned`，不覆寫原欄）＋**場景專用模型**（不污染正典 `int_orders`）＋**description 留痕**。照抄這套。

**建議**：這兩個結構性 NULL **預設不要在 `int_` collapse**，保留穿過 int_、在 `dim_/fct_/rpt_` 依問題處理（聚合本就忽略 NULL，常常不用填；刪欄的 NULL 尾用 `WHERE order_date < 停用日` 限有效期即可）。**例外**：某填值被證明 (a) 消費者無關且被多下游共用 → 才移進 int_，且**加新欄、不覆寫正典欄**（機制三那套）。**鐵律：永不在 `int_orders` 正典欄就地 `COALESCE` 掉 NULL**——那是在最共享的層、對最多消費者、做有損不可逆的決定。

#### 5.5.6 兩例都會踩的橫向陷阱

1. **別把結構性 NULL 當成品質錯誤**。DQ 的 `has_clean_error`/quarantine/Hard Gate 是給「值有業務問題」用的；欄位存在期外的 NULL 不是髒資料，不進 quarantine。Hard Gate 的 `error_rate_below` 看 `has_clean_error` 比率，結構 NULL 不會灌進去——**但**若在該欄掛了 `not_null` 測試，NULL 尾會讓測試爆。這類欄的測試要按「有效期」設計（只對有效區間斷言 not_null），或不掛 not_null。
2. **null-rate 監控會誤報**。Phase 4「少欄位由 null-rate 監控」會看到這兩例的 NULL 暴增。要**事先把它標為「預期的結構性 NULL」**（migration/上線 note、或監控 baseline 例外），否則每次假警報。

#### 5.5.7 判準

一句話收束：**先分辨 NULL 是「不存在 / 漏抽 / 停止收集」**——不存在→接受（5.5.2 A）、漏抽→Proposal C 回填（5.5.2 B）、停止收集→凍結留存+防誤用（5.5.4 A/B）；而**填值決定往 DAG 邊緣（dim/fct/rpt）推、正典欄永不覆寫**（5.5.5）。

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
