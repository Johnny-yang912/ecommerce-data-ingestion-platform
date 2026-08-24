# ADR-0044：用 `incremental` + `insert_overwrite` + `copy_partitions` 繞開 sandbox 的 DML 禁令

[English](../../en/adr/0044-copy-partitions-sandbox-dml.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 轉換 — dbt `stg_` |

---

## 背景

`stg_orders` 是增量的，依 `received_at`（DAY）分區，帶一個回看窗。例行 run 只重算近期分區，所以成本正比於近期資料而非歷史總量。

正確性倚賴一條不變式：**同一個 `raw_id` 的所有重複副本都落在同一個 `received_at` 分區。** `>=` 重抽（ADR-0023）與 Proposal C 的修正都保留原本的 `received_at`，所以整分區替換不會漏掉任何東西。

然後 sandbox 介入了。**這個 BigQuery 專案未啟用帳單，因而禁止 DML**——而 dbt 的 `insert_overwrite` 預設用 `MERGE`，那是 DML。每一次例行增量 run 都以 `DML queries are not allowed in the free tier` 失敗。

## 決策

在 `partition_by` 裡加 `copy_partitions: true`。分區改由 **copy job** 替換——儲存層級、非 DML、免費。

| | copy job（`copy_partitions=true`） | `MERGE`（預設） |
|---|---|---|
| 操作層級 | 儲存層分區複製（`table$YYYYMMDD` + `WRITE_TRUNCATE`） | 查詢引擎的 DML（掃描 → 刪除 → 插入） |
| 是否為 DML | 否 | 是 |
| 計費 | 免費 | 按掃描位元組計費 |
| Sandbox | ✅ 允許 | ❌ 禁止 |
| 適合 | 產出「一個分區的完整內容」，整批替換 | 產出「一個要 upsert 的子集」，逐列合併 |

**這條限制最後證明是與設計對齊的，不只是被繞過。** 去重產出的是「一天的完整內容」，所以整分區替換在語意上是**正確**的操作——`MERGE` 是在為一次整批替換做逐列的工。**就算啟用帳單，也沒有理由改回去。**

**⚠️ 這條限制對未來每一個增量的 `int_`／`dim_`／`fct_` model 同樣適用。** `--full-refresh` 用 `CREATE OR REPLACE`（DDL），不受影響——那也正是 `int_` 的全量重建物化（ADR-0046）完全繞過這個問題的原因。

兩個配套參數：

- **回看窗** `var('stg_orders_lookback_days', 3)`，可逐次調整而不必改檔案。它必須 ≥ E/L 的 `>=` 重抽範圍加安全餘裕。
- **`on_schema_change='append_new_columns'`**——staging「只做加法」政策在 dbt 側的鏡像（ADR-0025）。刻意**不用** `sync_all_columns`，因為它會 `DROP` 欄位。

## 去重鍵是 `raw_id`，而那是一個決策

不是 `id`，也不是 `order_id`：

- `ods.raw_id` 是 UNIQUE 且與來源列 1:1。
- Proposal C 的遷移形修正會以**「新的 `id`、相同的 `raw_id`」**抵達——**所以只有以 `raw_id` 分組，那筆修正才能與 staging 裡既有的舊副本競爭。**

決勝鍵是 `received_at desc, id desc`。目前的重複是逐位元組相同的，所以順序今天無關緊要——但**一旦 Proposal C 的 `rebuild_batch_id` 存在，必須把它放到最前面**：`rebuild_batch_id desc nulls last, received_at desc, id desc`。

## 後果

**增量在 sandbox 上以零成本運作。**

**回看窗耦合了兩層。** 若 E/L 的重抽範圍哪天超過 3 天，這個變數必須跟著放大——否則重複會落在重算窗口之外，永遠不會被去重。

**未來的增量 Gold model 必須記得這件事。** 這條限制不是 `stg_` 的局部性質。

## 考慮過的替代方案

**啟用帳單並使用 `MERGE`。** 會移除這條限制並花錢，換一個對這個工作負載語意上更差的操作。

**每次 run 都 full refresh。** 正確且無界——成本隨歷史總量成長，完全打消這一層存在的意義。

**只用 `--full-refresh`，排程執行。** 可行，而它放棄了增量的成本性質，**而那正是分區存在要交付的東西**。

## 相關

- [ADR-0043](./0043-stg-table-not-view.md) — 這件事所要求的實體表
- [ADR-0025](./0025-staging-additive-only.md) — `on_schema_change` 所鏡像的政策
- [ADR-0046](./0046-stg-incremental-int-full-rebuild.md) — 為何 `int_` 不繼承這條限制
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — sandbox 的其他後果
