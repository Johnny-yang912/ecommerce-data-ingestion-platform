# ADR-0040：dbt 分層執行，但結尾仍跑一次完整 `dbt test`

[English](../../en/adr/0040-layered-dbt-execution.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

一個單一的 `dbt build` task 是可行的。按層拆分（staging → intermediate → marts → reports）能讓 Hard Gate 的攔截點在 UI 上可見，並允許從失敗的那一層往下重跑。

但逐層 `--select` 會撞上 dbt 的 **indirect selection** 語意，而代價正好落在這個專案最重要的那個測試上——斷言 `int_orders` 與 `int_orders_quarantine` 構成 `stg_orders` 一個劃分的 singular test：

| 模式 | `assert_orders_split_is_partition` 會怎樣 |
|---|---|
| `eager`（預設） | 在 **staging** task 就被選中 → 對一個重建到一半的狀態斷言（`stg_` 新、`int_` 舊）→ **假性紅燈** |
| `cautious` | 並非所有 parent 都被選中 → **永遠不會執行** |
| **`buildable`** | parent 必須是「被選中的，或被選中者的祖先」→ 落在 **intermediate** task、所有輸入都是新的 → **正確** |

兩個錯誤答案都很糟，而且是以相反的方式糟：一個在狼來了，另一個是沉默的。**沉默的那個更糟**——文件把這個測試描述為分割不變式唯一的自動化安全網，絕不可降級。

## 決策

**每個分層 task 都加 `--indirect-selection=buildable`，並讓一次完整的 `dbt test` 收尾整個 DAG。**

收尾那一次不是為了冗餘而冗餘：

> **一個被靜默跳過的測試，遠比一個被重複執行的測試糟糕。**

兩次執行的職責不同：

- **逐層的測試是閘門**——它們阻止下游 build。
- **收尾那一次是完整性**——它證明沒有任何東西因為選取的微妙之處而被跳過。

## ⚠️ 絕不可把 `dbt build` 拆成 `dbt run` + `dbt test`

這是那個促使我們用測試把結構釘住的陷阱。

拆開之後，`int_` 的上游會變成「staging 的 **run**」而非「staging 的 **test**」。Hard Gate（ADR-0028）是掛在 `stg_orders` 上的一個測試——所以拆開之後，**閘門就不再阻擋任何東西**，而髒資料流進 Gold。沒有任何東西報錯。DAG 是綠的。**閘門變成裝飾品。**

由 `tests/test_dags.py::test_dbt_never_splits_run_and_test` 把關。

## 後果

**攔截點在 UI 上可見。** staging task 紅了代表 Hard Gate 觸發；intermediate task 紅了代表分割不變式壞了。這個區分在打開 log 之前就是可讀的。

**重跑便宜且精準**——從失敗那一層往下，而不是整個專案。

**那個微妙之處被記錄下來，而不是被重新發現。** `--indirect-selection` 冷僻到未來的維護者很可能在不知道它保護什麼的情況下改掉它。**這份記錄與那個釘住的測試，就是阻止那件事的東西。**

**代價是更長的 DAG 與重複執行的測試。** 兩者都接受——相對於 build，測試很便宜，而另一種失效模式是靜默的。

## 考慮過的替代方案

**單一個 `dbt build` task。** 完全沒有選取的微妙之處，也沒有可見的攔截點、沒有部分重跑，而且每一種可能的失敗都只有一個不透明的紅燈。

**用 `--indirect-selection=eager` 並容忍那個假性紅燈。** 訓練維運者忽略一個紅燈——與 ADR-0028 為 Hard Gate 全表口徑所否決的，是同一種失效模式。

**用 `cautious` 並倚賴收尾的 `dbt test`。** 那樣的話，這個不變式只會在 marts 與 reports **已經**建立在一個可能損壞的劃分之上**之後**才被檢查。**閘門必須在它所保護的東西的上游。**

## 相關

- [ADR-0028](./0028-hard-gate-per-batch-scope.md) — 會被 run/test 拆分靜默停用的那道閘門
- [ADR-0029](./0029-effective-quality-state.md) — 被斷言的那個分割不變式
- [ADR-0038](./0038-asymmetric-retries.md) — 為何這些 task 不重試
