# ADR-0023：Watermark 採方案 A，`get_watermark()` 是唯一接縫

[English](../../en/adr/0023-watermark-approach-a.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 雲端抽取 |

---

## 背景

增量抽取需要知道上一次停在哪裡。慣例的答案是一張 watermark 表：一張小表存著最後處理到的時間戳，每次載入成功後更新。

**那引入了第二份可能與現實不一致的狀態。** 如果載入成功而 watermark 更新失敗，下一輪會重抽。如果 watermark 推進了而載入沒有 commit，**列會被靜默跳過**——那是一次沒有任何錯誤回報的資料遺失。

## 決策

**方案 A：從已經在那裡的資料推導 watermark。**

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = @table
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

有三個性質讓它成立：

- **免費。** 它讀的是 metadata view，不是表本體——零掃描位元組、零成本。
- **不受成本保險絲限制。** `require_partition_filter`（ADR-0021）作用於表，不作用於 `INFORMATION_SCHEMA`。
- **在建構上就自我一致。** **watermark 就是已載入的資料本身。** 它不可能與落地的東西不一致，因為它是從落地的東西推導出來的。

**刻意沒有 `advance_watermark()`。** 沒有東西要更新，就沒有東西會更新失敗。載入之後，下一次呼叫 `get_watermark()` 自然就反映新資料。

切片邊界用 `>=` 而非 `>`：**寧可重抓，不要漏抓。** 重複由 `stg_` 依 `raw_id` 去重處理（ADR-0043），而那本來就必須存在。

## 那個接縫

`get_watermark()` 是**唯一**知道 watermark 怎麼取得的地方。其他所有東西只是向它提問。

正是這一點，讓方案 B——一張獨立的 watermark 表、精確到時間戳、供分鐘級 micro-batch 使用——成為一次**收斂的變更**而非重寫：它會替換這個函式的內容並新增一個 `advance_watermark()` 步驟，其餘什麼都不動。

**這個專案不會走那條路。** 批次是架構的選擇（ADR-0019），而另外兩個限制指向同一個方向：sandbox 60 天過期下的分區預算，以及一個全程以日為單位的報表口徑。**這個接縫記錄的是出口，它不是一個未完成的功能。**

## 後果

**少一件會出錯的事。** 沒有 watermark 表，就沒有「我們以為載入了什麼」與「實際載入了什麼」之間的漂移。

**粒度是日，不是時間戳。** watermark 解析到一個分區，所以重跑會重抽最多一天的列。在日排程下這負擔得起，**而那也正是它不足以支撐 micro-batch 的原因。**

**失敗的載入不會推進任何東西**，所以下一輪用 `>=` 重新選取同一個切片並自癒。這就是 ADR-0024 所倚賴的逐表自癒。

## 考慮過的替代方案

**watermark 表（方案 B）。** 時間戳精確、支援 micro-batch，代價是第二份狀態儲存，而它最壞的失效是**靜默的資料遺失**。**如果**哪天真的需要次日級新鮮度，它才是對的選擇——那正是這個接縫存在的原因。

**從 staging 表本身取 `MAX(received_at)`。** 同樣自我一致，但它掃描表資料——會花錢，而且在 `orders` 上會被成本保險絲擋住。`INFORMATION_SCHEMA` 免費給出同一個答案。

## 何時重新檢視

次日級新鮮度成為真實需求時——與 ADR-0019 相同的 trigger。

## 相關

- [ADR-0019](./0019-batch-load-not-streaming.md) — 這條所實作的節奏決策
- [ADR-0021](./0021-require-partition-filter-fuse.md) — 為何「讀 `INFORMATION_SCHEMA` 而非表」很重要
- [ADR-0024](./0024-per-table-load-job-gate.md) — 這條所啟用的自癒
- [ADR-0043](./0043-stg-table-not-view.md) — 讓 `>=` 安全的那個去重
