# 雲端層：ODS → BigQuery

[English](../../en/design/cloud-layer.md) | **繁體中文**

抽取與 staging。**只做 E/L——T 是 dbt 的職責**（[transformation](./transformation.md)）。

---

## 1. 範圍

`extract_ods_to_bq.py` 把 ODS 的列**原樣**搬進 BigQuery staging：不清洗、不改名、不轉型。staging 是 ODS 的 1:1 鏡像，**那正是讓「拿 staging 對 ODS」成為有意義對帳的原因**。

抽取兩張表，各有自己的 `TableSpec`、watermark 與 load job。

---

## 2. Staging 表設計

| 決策 | `orders` | `quality_events` |
|---|---|---|
| 分區（DAY） | `received_at` | `event_at` |
| 叢集 | `order_id`、`has_clean_error` | `raw_id`、`to_state` |
| `require_partition_filter` | ✅ 開 | ❌ **關** |
| Location | `US` | `US` |

**保險絲的差異才是重點。** `orders` 的查詢永遠帶時間過濾，所以保險絲不花任何代價。`quality_events` 的主要消費者需要的是**跨全歷史、按 `raw_id` 取最新事件**——本質上就是一次不帶過濾的全掃描，而保險絲會擋掉它。照抄 `orders` 的 spec 會弄壞回流路徑，**而且是在表建立好幾個月之後才壞**。[ADR-0022](../adr/0022-quality-events-staging-diverges.md)

Location 在 dataset 上明確釘成 `US` 而非依賴預設，好讓跨 location 查詢錯誤不會在日後出現。

### ⚠️ `received_at` 指的是兩個不同時刻

| 欄位 | 何時蓋上 | 意義 |
|---|---|---|
| `raw.received_at` | API 在請求路徑中寫入 Raw | **訂單接收時間** |
| `ods.received_at` | worker 寫入 ODS | **ODS 落地時間** |

staging 鏡像的是 ODS，所以用 ODS 自己的時鐘分區，恰好回答「extract 有沒有把 ODS 往前推？」。

**隨之而來的範圍邊界：** 當恢復掃描把積壓排空時，那些列帶的是**補寫當下**的時間——所以攝入的空窗**在 ODS 時間線上根本不存在**。任何建立在 `ods.received_at` 上的東西，只看得見取樣當下仍在進行中的中斷。

**這個名字讀起來像接收時間，而我們不改名**——改名是一次 migration，會波及 `FIELDS` 與每一處 dbt 引用。[ADR-0020](../adr/0020-partition-on-received-at.md)

---

## 3. Watermark

方案 A——從目的地推導：

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = @table
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

免費（metadata，不是表資料）、不受成本保險絲影響，而且**在建構上自我一致**——watermark **就是**已載入的資料。

刻意**沒有 `advance_watermark()`**：沒有東西要更新，就沒有東西會更新失敗。

切片邊界是 `>=` 而非 `>`——**寧可重抓，不要漏抓**。重複由 `stg_` 依 `raw_id` 去重吸收。

`get_watermark()` 是切換到方案 B 的**唯一接縫**。[ADR-0023](../adr/0023-watermark-approach-a.md)

### 方案 A 什麼時候會失效

方案 A 的精度被分區粒度封頂——DAY，所以每次執行都會重抽整個最新那一天：

| 批次間隔 | 方案 A | 補救 |
|---|---|---|
| 每日／T+1 | ✅ 重抽量可忽略 | 留在 A |
| 每小時 | ⚠️ 每次執行都重抽「今天到目前為止」 | 改用 HOUR 分區（受 4000 分區上限約束，需要過期設定） |
| 次小時級 micro-batch | ❌ 同一天被重抽數百次 | **方案 B**——獨立的 watermark 儲存，精確到時間戳 |

> **判準：批次間隔 ≈ 分區粒度 → A；批次間隔 ≪ 分區粒度 → B。**

切換到 B 只動 `get_watermark()`，外加載入成功後的一個 `advance_watermark()`；`main()` 不變。**B 的代價是狀態管理**——它住在哪、「先載入再推進」的順序不變式、bootstrap、失效面、併發——而它的金錢成本仍然 ≈ 0。

還有一道更硬的天花板：**batch load job 每張表每天上限 1500 次**，那大約是每分鐘一次的節奏。

---

## 4. 載入

只用批次 load job（`WRITE_APPEND`），絕不用 streaming insert。巢狀 `items` 以原生 JSON 物件落地。

**在沒有跨表交易的情況下達成跨表一致性**，因為 BigQuery 沒有跨表交易：

1. **逐表自癒**——失敗的載入不推進那張表的 watermark，下一輪用 `>=` 重新選取。
2. **轉換前的一道 gate**——任一張表失敗即整體失敗。在 Airflow 裡，gate 就是依賴邊：dbt task 的上游是**兩個** extract task 都成功。

一表一個 Airflow task，因為**重試粒度應該與失效粒度相符**——合併的 task 會重跑已經成功的那張表，並掩蓋是哪一張壞了。[ADR-0024](../adr/0024-per-table-load-job-gate.md)

---

## 5. 設定與安全

| 值 | 來源 | 為何 |
|---|---|---|
| `PROJECT` | `settings.bq_project` | 隨環境而異；真實 id 不進版控 |
| `DATASET`、`LOCATION` | 模組常數 | 結構性、穩定的架構決定 |
| 憑證 | `GOOGLE_APPLICATION_CREDENTIALS` | 主機路徑，掛載進來；絕不烤進映像 |

`BQ_PROJECT` / `BQ_DBT_DATASET` / `GOOGLE_APPLICATION_CREDENTIALS` **與 dbt 的 `profiles.yml` 共用**——分開設定的話，產生者與消費者可能靜默指向不同的 dataset。[ADR-0041](../adr/0041-profiles-yml-structure-vs-values.md)

---

## 6. Schema 演進

**上游漂移 ≠ ODS 變更。** 上游來的未知欄位落進 `ods.unmapped_fields` 並由 `has_schema_drift` 標記；在有人決定之前，它們不會變成 ODS 欄位。

### BigQuery 能就地遷移什麼

靜態矩陣。逐步的操作流程見 [runbooks/schema-change](../runbooks/schema-change.md)：

| ODS 的變更 | BQ 能就地做嗎？ | 雲端層的處理 |
|---|---|---|
| 加 nullable 欄位 | ✅ `ALLOW_FIELD_ADDITION` | staging 自動撿走 |
| REQUIRED → NULLABLE | ✅ 放寬 | staging 跟著放寬 |
| 刪欄位 | ✅ DROP——**但會失去歷史** | **不要刪。** 留著；`stg_` 忽略它 |
| 改名 | ✅ RENAME | **不要改名。** 加一個新欄位；由 `stg_` 做改名 |
| 不相容的型別變更 | ❌ | 加新欄位 + 在 `stg_` 做 cast |
| 改分區／叢集 | ❌ | 重建表（CTAS） |

**第 3、4 列才是有意思的**：BigQuery **做得到**，而 staging 刻意不做。三個理由——① 保留歷史；② **BigQuery 的 DDL 沒有版本控制**，不像 Alembic，所以把改名／轉型放進 `stg_` 的 SQL 才有 git 版控與 review；③ 把物理演進（罕見、只加）與邏輯演進（頻繁、在 SQL 裡）解耦。

> **值得點名的不對稱**：ODS 有 Alembic，一套真正的 migration 框架。**staging 沒有對等物**——dbt 只從 `stg_` 開始接手，它並不擁有 staging。`ALLOW_FIELD_ADDITION` 涵蓋新增，其餘由 dbt 作為替代品吸收。唯一真正「非重建不可」的情況（改分區）在方案 A 之下很便宜：drop、重建、重抽——**而 watermark 會自己重設。**

當 ODS **確實**變更時，staging 只做加法：

| 變更 | 在哪裡處理 |
|---|---|
| 加一個 nullable 欄位 | load job 的 `ALLOW_FIELD_ADDITION`——自動出現 |
| 刪一個欄位 | 以 `NULL` 填滿的 legacy 形式留在 staging；`stg_` 不再 select 它 |
| 改名 | `stg_` 的顯式欄位清單——staging 保留舊名 |
| 改型別 | `stg_` 的 cast，或重建表 |

`ensure_staging_table()` 只**建立**，從不變更。

**`stg_` 用顯式欄位清單而非 `SELECT *`，正是因為那份清單既是改名接縫也是閘門**：在 staging 長出來的欄位，在有人刻意加進去之前對下游都是不可見的——**而那次刻意會是一個 commit、一次 review**。[ADR-0025](../adr/0025-staging-additive-only.md)

### NULL 的處理該住在哪一層

先把 NULL 處理拆成兩類：

- **(a) 與消費者無關的正規化**——對所有下游都客觀正確，只有一個答案。
- **(b) 因消費者而異的分析決策**——為了聚合而 NULL→0、保留 NULL 以計算缺失率、NULL→`'unknown'` 當成一個維度桶。**答案隨問題而變。**

由加欄或刪欄產生的結構性 NULL，**幾乎永遠是 (b)**。

> **核心原則：NULL 攜帶資訊——「不曾存在」／「停止收集」——而 `COALESCE` 是有損且單向的。** 在 `int_` 把 NULL 收成 0，任何下游就再也分不出「沒收集」與「真的是 0」；一個想算涵蓋率的 `fct_` 永遠算不出來。

| 面向 | 放在 `int_`（早、共用） | 放在 `dim_`／`fct_`（晚、貼近消費者） |
|---|---|---|
| 可逆性 | **差**——NULL 的資訊在這裡死掉 | 好——局部決策，影響半徑小 |
| 一致性 | 所有下游同一個答案 → 只有 **(a)** 受益 | 各取所需 → **(b)** 的天然歸屬 |
| 語意 | 對 **(b)** 而言，它**替所有人**做了一個不該替他們做的決定 | 每個問題自己決定 |

**預設：不要在 `int_` 收掉結構性 NULL。** 讓它們穿過去，在 `dim_`／`fct_`／`rpt_` 按問題處理——聚合本來就會忽略 NULL，所以往往根本不需要填。

**例外**：若某個填值被證明與消費者無關、且被許多下游共用，就把它移進 `int_`——**但要作為一個新欄位，絕不覆蓋正典欄位。** 那就是情境模型的模式：一個新欄位、一個情境專用模型，以及 description 裡的稽核軌跡。

> **鐵律：絕不可在正典的 `int_orders` 欄位上就地 `COALESCE` 掉 NULL。** 那是在最共用的一層、代表最多消費者，做出一個有損且不可逆的決定。

兩個案例都會踩到的陷阱：

1. **結構性 NULL 不是品質錯誤。** `has_clean_error`／隔離區／Hard Gate 是給*有業務問題的值*用的。落在某欄位存在窗口之外的 NULL 不是髒資料。但**那個欄位上的 `not_null` 測試會被 NULL 尾巴炸掉**——把這類測試設計成只在有效區間內斷言，或者乾脆不掛。
2. **null-rate 監控會誤報。** 事前把它標記為預期的結構性 NULL——一則 migration 註記或一個監控基線例外——否則每次執行都會得到一次假警報。

### 三份 schema 宣告，全部有守衛

| # | 宣告 | 由什麼把關 |
|---|---|---|
| 1 | `models.py` | —（來源） |
| 2 | Alembic migration | `check_migration_drift.py`（手動） |
| 3 | 每張表的 `FIELDS` | `tests/test_schema_bq_consistency.py`（在 CI） |

沒有守衛 3 的話，加了 ODS 欄位卻忘記 `FIELDS` 會**靜默失敗**——抽取照跑、載入成功，那個欄位就是不在。[ADR-0026](../adr/0026-fields-single-source.md)

---

## 7. 修正批次（Proposal C 的雲端側）

方向性設計，未實作。雲端側需要處理四件事：

1. **watermark 永遠看不到被修正的列**——修正保留原本的 `received_at`，所以推送它是一個明確的 runbook 步驟，不是一次增量撿取。
2. **遷移形**：重用既有的 append + 去重通道——不需要 JOIN，因為 `stg_` 的去重以 `raw_id` 分組，而修正是「新 `id`、同 `raw_id`」。
3. **補丁形**：第二張表與另一份手工維護的宣告，外加一個重抽的地雷。
4. **Late-arriving**：對受影響分區做定向刷新。

Proposal C 是什麼、為何存在，見 [data-quality](./data-quality.md)。

---

## 8. Sandbox

這個專案跑在 BigQuery sandbox（未啟用帳單）上，它施加了兩條形塑了真實決策的限制：

- **禁止 DML** → dbt 的 `insert_overwrite` 需要 `copy_partitions: true`（[ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)）。
- **每張表 60 天分區過期**，帳號層級、無法覆寫 → 以 `order_date` 分區的 Gold 表會靜默失去較舊的列。

一項相關量測**推翻了先前的結論**：超出 BigQuery 合法分區範圍的日期**不會**讓 build 失敗——它們會靜默落進 `__UNPARTITIONED__`。原本計畫的「合法區間 guard」因此被撤回；實際的失效模式不是原先假設的那一種。

細節與真實系統的做法：[PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

---

## 9. 相關

- [ADR-0019](../adr/0019-batch-load-not-streaming.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md)
- [transformation](./transformation.md) — staging 之後發生什麼
- [orchestration](./orchestration.md) — 抽取如何被排程
