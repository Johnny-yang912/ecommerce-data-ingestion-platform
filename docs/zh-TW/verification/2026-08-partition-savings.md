# 2026-08 — 分區實際上省多少？

[English](../../en/verification/2026-08-partition-savings.md) | **繁體中文**

---

## 驗證的假設

分區在成本上被普遍推薦。**相對於只用叢集，它實際上省多少**——而「分區省很多錢」是不是採用它的正確理由？

## 環境

兩張表、**完全相同的資料**（540 列），跑典型的分析師查詢：**最近 30 天的切片**。2026-08。

## 觀測

| | `totalBytesProcessed` | 對全表 |
|---|---|---|
| 全表 | 68,856 B | 100% |
| 只有 `cluster by order_date` | 12,474 B | **18%** |
| `partition by order_date` + 叢集 | 6,490 B | 9% |

**光是叢集就剪掉了 82%。分區再多加九個百分點。**

## 結論

那個常見的正當理由需要修正：**分區的價值不在剪枝的量。** 叢集已經做掉了其中的大部分。

它的價值是三件叢集給不了的事：

**① 成本的可預測性。** 分區剪枝是**在查詢執行之前、由 metadata 決定的**，所以 `dry run` 的位元組數是精確的。叢集是在 block 層級剪枝、取決於資料布局，所以 `dry run` 會高估。**成本治理倚賴前者**——一個建立在高估之上的保險絲或預算告警，不是一種控制。

**② `require_partition_filter` 的前提。** 只有分區表能設它（[ADR-0021](../adr/0021-require-partition-filter-fuse.md)）——即使 Gold 選擇不用。

**③ 分區層級的操作。** `insert_overwrite` 的整分區原子替換，以及單分區的定向刷新。**整份 `stg_` runbook 都建立在這件事上**（[ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md)）。

## 這次量測自身的邊界

> **BigQuery 對每張表每次查詢有 10 MB 的計費下限。** 在這個專案的資料量下，三種版本的成本**完全相同**。

分區的效益只在「數千萬到數億列」這個前提之下成立——**而那是這個專案宣告的前提，不是它證明出來的。** 外推見 [design/transformation §3](../design/transformation.md) 的逐訂單成本表。

**把那條邊界講出來很重要**：一次 540 列的量測無法證明一個成本論證，而把它當成證明了，會與「拿產生的資料去調索引」是同一種錯誤（[ADR-0018](../adr/0018-raw-status-no-index.md)）。

## 這推翻了什麼

不是一個被寫下來的結論，而是一個**約定俗成的正當理由**。「把表分區，它省錢」是對的，**而它是三個理由中最不重要的一個**——一個只基於它做最佳化的人，會合理地得出「叢集就夠了」的結論。

## 相關

- [ADR-0020](../adr/0020-partition-on-received-at.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md)
- [2026-08-partition-expiry-measurement](./2026-08-partition-expiry-measurement.md) — 同一個功能的另一面
