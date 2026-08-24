# ADR-0025：Staging 只做加法；改名與轉型下推給 dbt

[English](../../en/adr/0025-staging-additive-only.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 雲端抽取 |

---

## 背景

ODS 的 schema 會變：加一個欄位、某個欄位不再使用、某個型別後來發現是錯的。Staging 必須吸收這些變化，同時不變成第二個住著轉換邏輯的地方。

BigQuery 讓其中一些變得便宜、另一些昂貴。加一個 nullable 欄位是 metadata 操作——免費、瞬間、既有列讀成 `NULL`。改名或改型別不是：那代表重寫整張表。

誘惑是「哪裡當下方便就在哪裡處理」。**那會產生一個悄悄在做轉換的 staging 層，而那正是 `stg_` 存在的職責。**

## 決策

**Staging 永遠只做加法。**

| 變更 | 在哪裡處理 |
|---|---|
| 加一個 nullable 欄位 | load job 的 `ALLOW_FIELD_ADDITION`——欄位自動出現 |
| 刪一個欄位 | 欄位以 legacy 形式留在 staging、值為 `NULL`；`stg_` 不再 select 它 |
| 改欄位名稱 | `stg_` 的顯式欄位清單——staging 保留舊名 |
| 改型別 | `stg_` 的 cast，或重建表 |

`ensure_staging_table()` 只**建立**，從不變更。既有表的演進透過 load job 的 `schema_update_options` 發生。

**`stg_` 之所以用顯式欄位清單而非 `SELECT *`，理由正是這個**：顯式清單既是改名接縫，也是閘門。一個經由 `ALLOW_FIELD_ADDITION` 在 staging 長出來的欄位，在有人**刻意**把它加進那份清單之前，對下游都是不可見的——**而那次刻意會是一個 commit、一次 review**。漂移無法自己滲進來。

## 後果

**Staging 保持為 ODS 的忠實鏡像**，這才讓「拿 staging 對 ODS」成為有意義的對帳。

**加欄位零成本、零破壞。** load job 吸收它；下游在有人主動採用之前不受影響。

**刪掉的欄位會留下 `NULL` 的 legacy 欄位。** 稍微不整齊，而且比重寫便宜得多。它們只在因其他理由重建時才被移除。

**代價是 `stg_` 會累積改名與轉型邏輯。** **那正是它該在的地方**——dbt 有版控、有測試、可審查，而埋在抽取腳本裡的轉換一樣都沒有。

**`stg_orders` 的 `on_schema_change='append_new_columns'` 是這條在 dbt 層的鏡像**，刻意不選 `sync_all_columns`——後者會 `DROP` 欄位，因而牴觸「staging 只做加法」。

## 考慮過的替代方案

**每次 schema 變更都 full refresh。** 正確且昂貴：一次全表掃描加上每個分區的重寫，換一個通常只是「一個 nullable 欄位」的變更。

**在抽取腳本裡處理改名。** 把轉換放進 E/L 層、讓改名邏輯散在兩個地方，並讓 staging 不再是 ODS 的鏡像。

**dbt 用 `sync_all_columns`。** 會自動傳播刪除——**而自動傳播的刪除，是一次沒有人審查過的刪除。**

## 相關

- [ADR-0026](./0026-fields-single-source.md) — 讓這件事成立所必須保持一致的那份宣告
- [ADR-0043](./0043-stg-table-not-view.md) — 被吸收的邏輯住在哪裡
- [雲端層設計](../design/cloud-layer.md) — 加欄／刪欄的端到端演練
