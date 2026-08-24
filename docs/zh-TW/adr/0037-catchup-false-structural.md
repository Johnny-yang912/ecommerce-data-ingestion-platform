# ADR-0037：`catchup=False` 是結構性的，不是為了省事

[English](../../en/adr/0037-catchup-false-structural.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

`catchup=False` 經常是為了避免 DAG 首次部署時湧出一堆補跑而設的——一種便利。**在這裡它是一句關於「這條管線是什麼」的陳述。**

**這條管線的 watermark 是由目的地推導的**（方案 A：從 staging 的 `MAX(partition_id)`——ADR-0023），不是由執行日期推導的。一個為 2026-07-01 而跑的補跑，抽取的仍然是「**截至現在**的增量」，與它的邏輯日期無關。

所以 N 次補跑會做同一件事 N 次，外加 N 個多餘的 load job。**它們不是補跑，它們是重複。**

## 決策

每個 DAG 都設 `catchup=False`，加上 `max_active_runs=1`。

**`max_active_runs=1` 是正確性，不是禮貌。** 併發的 run 會讀到同一個 `get_watermark()` 值——這本身無害，因為 `stg_` 去重會吸收它——但**併發的 dbt `insert_overwrite` 對同一批分區會互相覆蓋**。

**這不是一個按日期分區、可補跑的 DAG，而把它變成那樣是一個刻意的非目標。** 真正的可補跑需要以 `received_at >= data_interval_start AND < data_interval_end` 切片——Airflow 慣用的冪等形狀。**那個右界會切掉遲到的列**，直接牴觸 ADR-0023「用 `>=`，寧可重抓不漏抓」的語意。

**是每日，不是每小時。** 方案 A 的精度被 DAY 分區封頂，所以每小時排程會在每次 run 重抽整個當天。要改每小時，得先有 HOUR 分區或方案 B——那是另一個決策，而 ADR-0019 已經婉拒了它。

## 另一半：Proposal B DAG 的 `schedule=None`

`dq_reevaluation` 完全沒有排程，而那是同一類陳述。

Proposal B 由一次**規則放寬**觸發——那是一個人為的部署事件，不是一個週期。規則沒變時，再評估必然重現前一次的判定（同樣的值、同樣的規則版本），所以它**不會產生任何事件，卻會全掃整個隔離區積壓**。每天排程它等於**364 天的白工換一天的效果**。

> **排程屬於會自己改變的東西。規則不會自己改變。**

三個配套選擇：

- **預設 dry-run。** `quality_events` 是 append-only 的；一次錯誤的寫入無法刪除，而手動觸發的 UI 讓人很容易一路點過去。
- **`expect_rule_version` 作為守衛。** 最可能發生的意外，是對一個還沒部署新規則的環境觸發——寫出一批標著錯誤版本、且無法撤銷的事件。
- **事後觸發主 DAG，但僅在 `commit` 時。** 再評估只寫 PostgreSQL 的 `quality_events`；要流回 Gold 還需要 `extract_quality_events` 與一次 `int_` 重建。少了那一步，觀察到的狀態會是「我跑了 Proposal B，然後什麼都沒發生」——**最容易被誤讀成程式壞掉的那個狀態。**

每個參數都遵循「空值就省略旗標」：預設值**只住在腳本裡**。在 DAG 裡也留一份副本，正是兩者開始分歧的方式。

## 後果

**部署或改名一個 DAG 不會觸發一串無意義的 run。**

**這條管線的不可補跑性被記錄下來，而不是留給人重新推導。** 未來想問「為什麼這個不能補跑？」的讀者，會在這裡找到答案，而不必從 watermark 的實作重建它。

**代價是：錯過的窗口不會自動恢復。** 如果機器在 22:30 是關的，那天的 extract 就是沒發生。隔天的 run 會撿走 watermark 之後的一切，所以資料不會遺失——但那個空窗是真的，而**注意到它是 `source_freshness_watch` 的工作**（ADR-0039）。

## 相關

- [ADR-0023](./0023-watermark-approach-a.md) — 這條所源自的「目的地推導 watermark」
- [ADR-0019](./0019-batch-load-not-streaming.md) — 為何是每日而非每小時
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — 那個 `schedule` 為 `None` 的工作
- [ADR-0039](./0039-observation-signals-own-dag.md) — 誰來注意錯過的窗口
