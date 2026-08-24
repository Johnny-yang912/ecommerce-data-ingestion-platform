# ADR-0029：Row Filter 讀有效品質狀態，不讀字面的 `has_clean_error`

[English](../../en/adr/0029-effective-quality-state.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-07 |
| **層** | 資料品質 — dbt `int_` |

---

## 背景

顯而易見的 Row Filter 是 `WHERE has_clean_error = FALSE`。**它是錯的**，而理由是前面兩個決策的直接後果。

**ODS 是不可變的**（ADR-0002），所以一筆在攝入時被隔離的記錄，**永遠**讀到 `has_clean_error = TRUE`——即使規則變更後它已經合法。**Bounded writeback**（ADR-0032）禁止從下游更新那個欄位。

所以字面讀取那個旗標，會讓每一筆被 promote 的記錄永久卡在隔離區。**再評估機制會存在、會寫出它的事件，然後什麼都不改變。**

## 決策

過濾器讀的是**有效品質狀態**：攝入當下的判定，與 `quality_events` 最新事件的合成。

```sql
COALESCE(
    s.has_clean_error = FALSE      -- 攝入時就乾淨
    OR e.to_state = 'promoted',    -- 或被後來的再評估 promote
    FALSE
) AS is_effectively_clean
```

`int_orders` 取這個為真的列；`int_orders_quarantine` 取它的否定。兩者合起來是 `stg_orders` 的一個**劃分**——互斥且窮盡——由 singular test 斷言。

每個 `raw_id` 的最新事件由 `ROW_NUMBER() OVER (PARTITION BY raw_id ORDER BY event_at DESC, id DESC)` 取得。`id DESC` 這個決勝鍵不是裝飾：沒有它，同一時戳的兩筆事件會讓結果非決定性，**而兩個模型可能對同一列有不同意見。**

## 兩個不可以動的東西

**⚠️ `LEFT JOIN` 必須維持 `LEFT`。** 大多數記錄根本沒有品質事件。inner join 會靜默丟掉每一筆「攝入時乾淨且從未被再評估」的記錄——那幾乎是全部。

**⚠️ `COALESCE` 不可以拿掉。** 這是隱微的那一個。當 `has_clean_error = TRUE` 且沒有對應事件時：

```
FALSE OR NULL  =  NULL
WHERE NULL          → 該列被排除
WHERE NOT NULL      → NULL → 該列也被排除
```

**那一列會同時從兩張表消失。** 沒有錯誤、兩邊的列數都不會失敗——只有靜默的資料遺失，成因是 SQL 的三值邏輯精確地做了它被規定要做的事。劃分測試就是抓這類錯誤的東西。

## 後果

**回流路徑端到端可運作。** 當 Proposal B 的產生器被實作出來時，**這一層一行都沒有改**——消費端本來就是對的。那是先建消費端的紅利。

**合成區塊在兩個模型之間逐字重複**，是刻意的而非共用。取捨與對齊清單見 ADR-0045。

**隔離區記錄的 `quarantined_at` 取自事件時間，不是 `CURRENT_TIMESTAMP()`。** 這個模型是全量重建，所以 `CURRENT_TIMESTAMP()` 記錄的會是**這次 run 發生的時間**而非該列被隔離的時間——而且每次 run 都會變。

## 考慮過的替代方案

**`WHERE has_clean_error = FALSE`。** 切斷回流路徑，永久且靜默。

**promote 時更新 ODS 的 `has_clean_error`。** 違反不可變錨點（ADR-0002）與 bounded writeback（ADR-0032），並摧毀稽核軌跡：那筆記錄將不再顯示它曾經被隔離過，也不再顯示是在哪個規則版本下。

**在 extract 時把有效狀態物化成一個欄位。** 會把合成往上游搬進 E/L，而它得在每次有事件抵達時重算並重新落地——而 staging 應該是 1:1 鏡像（ADR-0025）。

## 相關

- [ADR-0002](./0002-has-clean-error-non-blocking.md) — 讓這件事成為必要的那個不可變性
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — 這裡所讀事件的產生者
- [ADR-0032](./0032-bounded-writeback.md) — 為何不能直接更新那個旗標
- [ADR-0045](./0045-int-effective-state-duplication.md) — 為何這個區塊是重複的
