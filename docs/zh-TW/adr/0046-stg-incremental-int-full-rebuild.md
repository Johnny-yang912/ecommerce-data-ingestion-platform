# ADR-0046：`stg_` 走增量，`int_` 走全量重建

[English](../../en/adr/0046-stg-incremental-int-full-rebuild.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 轉換 — dbt |

---

## 背景

`stg_orders` 採 `incremental` + `insert_overwrite`，依 `received_at` 分區並帶一個回看窗。例行跑批只重算近期分區，因此成本正比於近期資料量而非歷史總量。它之所以正確，靠的是一條不變式：**同一個 `raw_id` 的所有副本都落在同一個 `received_at` 分區**，所以整分區原子覆寫不會留下漏網之魚。

顯而易見的下一步，是讓 `int_orders` 比照辦理。它讀的是 `stg_orders`，而後者本來就依 `received_at` 分區；配一個相同的回看窗，看起來是免費的節省。

**那是一個陷阱，而且是無聲的。**

**Proposal B 寫事件的時間軸，和它所影響的記錄不在同一條軸上。** 一個 promotion 事件的 `event_at = now()`，落在今天的分區。但它所拯救的那筆訂單是幾週前攝入的，坐在一個舊的 `received_at` 分區裡。如果 `int_orders` 依 `received_at` 回看窗增量，那個舊分區永遠不會被重算——所以被 promote 的記錄永遠流不回 Gold。

**不會有任何錯誤，不會有任何測試失敗。** 那筆記錄就只是永遠留在 `int_orders_quarantine`，而整套再評估機制會在這一層被切斷，同時在其他每一層看起來都運作正常。

## 決策

| 模型 | 物化方式 | 理由 |
|---|---|---|
| `stg_orders` | `incremental` + `insert_overwrite` + `copy_partitions` | 成本正比於近期資料；同分區不變式使其正確 |
| `int_orders`、`int_orders_quarantine`、`int_order_items` | `table`（全量重建） | 全量重建沒有時間軸可以搞錯 |

`table` 物化走 `CREATE OR REPLACE`，那是 DDL。這同時繞開了 BigQuery sandbox 禁止 DML 的限制——正是逼上一層改用 `copy_partitions` 的同一條限制。

## 後果

**回流路徑不需要任何特殊處理就能運作。** 一個 promotion 事件會在下一次排程跑批時自動生效。這不是理論上的好處：當 Proposal B 的事件產生器（`reevaluate_quality.py`）終於實作出來時，**這一層一行都沒有改**。消費端本來就是正確的，因為它當初就沒有一條可被切斷的增量時間軸。

**代價是 `int_` 的重建成本正比於歷史總量**，而非近期資料量。以目前的資料量而言這綽綽有餘地可以接受；但不會永遠如此。

**出口寫在模型檔本身裡**，因為它並不顯而易見。當資料量逼得非增量化不可時，重選集合必須是**「回看窗分區 ∪ 近期有品質事件的 `raw_id` 所屬分區」**——而且**必須整分區重選**。只選那幾列會讓 `insert_overwrite` 把同分區的其他每一列都洗掉。

## 這個問題的通用形狀

這在結構上與 late-arriving data 是同一個問題，只是軸不同：

| | 改變的是什麼 | 改變落在哪 |
|---|---|---|
| Late-arriving data | 記錄的**值** | 不是正在處理的那個分區 |
| Proposal B | 記錄的**品質狀態** | 不是正在處理的那個分區 |

任何一個「正確性取決於第二條時間軸」的增量模型都有這個危害。兩個案例共有的通則是：**增量窗口只有在「所有能改變一列的事情，都落在該列所屬的同一個分區」時才安全。**

## 考慮過的替代方案

**`int_` 依 `received_at` 增量。** 就是上面描述的陷阱。否決——而且值得明確記錄它被否決過，**因為讀者會假設這個選項只是被忽略了**。

**`int_` 依一個獨立的「最後異動」欄位增量。** 需要在兩個模型間維護這樣一個欄位，並讓它與一份 append-only 事件日誌保持同步——以目前的資料量而言，這比全量重建帶來更多機械結構，也帶來更多無聲出錯的方式。

**只按需重建受影響的分區。** 這是已記錄的未來路徑，不是被否決的替代方案。它被延後到資料量足以正當化那份額外複雜度為止。

## 何時重新檢視

當 `int_` 層的全量重建在每日跑批的時間或成本上開始有感時。

## 相關

- [ADR-0044](./0044-copy-partitions-sandbox-dml.md) — 形塑上一層的那條 sandbox 限制
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — 這個物化方式所保護的機制
- [ADR-0045](./0045-int-effective-state-duplication.md) — 這一層付出的另一個刻意代價
- [轉換層設計](../design/transformation.md)
- [雲端層設計](../design/cloud-layer.md) — late-arriving data，值軸上的同一個形狀
