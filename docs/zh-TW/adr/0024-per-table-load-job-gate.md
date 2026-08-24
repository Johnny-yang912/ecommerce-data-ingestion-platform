# ADR-0024：每表一個 load job 加一道 gate；不做跨表交易

[English](../../en/adr/0024-per-table-load-job-gate.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 雲端抽取 |

---

## 背景

有兩張表被抽取：`orders` 與 `quality_events`。它們並不獨立——`int_` 靠 join 兩者來合成有效品質狀態（ADR-0029）。如果 `orders` 落地了而 `quality_events` 沒有，下游讀到的是一幅**看起來一致但錯誤**的畫面：被 promote 的記錄會靜默地顯示為未被 promote。

BigQuery 不提供跨表交易。一個 load job **對單張表**是原子的。所以「要嘛都有、要嘛都沒有」無法從儲存層取得，必須被建構出來。

## 決策

每張表有自己的 `TableSpec`、自己的 watermark、自己的 load job。一致性由兩個機制而非一個交易構成：

**① 逐表自癒。** 失敗的載入不推進那張表的 watermark（ADR-0023），所以下一輪用 `>=` 重新選取同一個切片。append-only 加上 `stg_` 去重，讓重試無害。

**② 轉換前的一道 gate。** 任一張表失敗，整體抽取即以非零碼失敗。**轉換絕不可在半套資料上開跑。**

這道 gate 有兩種形式，語意完全相同：

| 模式 | gate 住在哪 |
|---|---|
| `--table all`（腳本內） | `main()` 彙整結果後 raise |
| `--table orders` / `--table quality_events`（Airflow） | 依賴邊：dbt task 的上游是**兩個** extract task 都成功 |

## 為何 Airflow 裡一表一個 task

這是不明顯的那一半。把兩個抽取合併成一個 Airflow task 是可行的，**而它會破壞機制 ①**。

**自癒在建構上就是逐表的。** 如果只有 `quality_events` 失敗，就只有 `quality_events` 該被重試。合併的 task 會連已經成功的 `orders` 抽取也一起重跑——白做工，而且看不出實際是哪張表壞了。

> **重試的粒度應該與失效的粒度相符。兩者不同時，重試會做連帶工作，而診斷會遺失資訊。**

## 後果

**「要嘛都有、要嘛都沒有」在重要的地方成立。** 兩張表可以在兩個 load job **之間**短暫不同步，但那個窗口內沒有消費者在跑——gate 在兩者的下游。

**失效是逐表且可讀的。** Airflow UI 顯示哪張表失敗；重試只碰那張表。

**代價是這個不變式住在編排層，不在儲存層。** BigQuery 裡沒有任何東西阻止有人在抽取進行中查詢 `int_`。這個保證是程序性的，而且它依賴 gate 確實位於每一個消費者的上游。

**獨立的 watermark 代表兩張表可以停在不同的點**，而那之所以安全，只因為 gate 存在。**移除 gate 不會大聲失敗——它只會開始產出偶爾錯誤的 Gold 資料。**

## 考慮過的替代方案

**一個 load job 載兩張表。** 不可能；BigQuery 的 load job 是單表的。

**先落到暫存 dataset 再原子替換。** 能提供真正的 all-or-nothing，代價是雙倍儲存、一個有自己失效模式的替換步驟，以及重建增量模型——換取的保證在目前節奏下 gate 已經提供了。

**一個 Airflow task 做兩個抽取。** 依上述論證，破壞逐表重試。

## 相關

- [ADR-0023](./0023-watermark-approach-a.md) — 這條所建立於的逐表自癒
- [ADR-0022](./0022-quality-events-staging-diverges.md) — 第二張表，以及它為何必須存在
- [ADR-0038](./0038-asymmetric-retries.md) — 在這個粒度上運作的重試政策
