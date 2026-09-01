# Runbooks

[English](../../en/runbooks/README.md) | **繁體中文**

維運程序。**該做什麼**，不是為什麼——為什麼住在 [ADR](../adr/README.md) 與[設計文件](../design/)。

---

## 症狀 → runbook

| 你看到的狀況 | 打開 |
|---|---|
| 第一次啟動，或重開機之後 | [airflow-startup](./airflow-startup.md) |
| **DAG 該跑卻沒跑——而且沒有任何東西是紅的** | [airflow-silent-stall](./airflow-silent-stall.md) |
| 分析 DAG 已經連續失敗好幾天 | [dag-failure-recovery](./dag-failure-recovery.md) |
| 要放寬一條品質規則，讓被隔離的列回來 | [proposal-b-rollout](./proposal-b-rollout.md) |
| 某筆記錄必須被永久註銷 | [quarantine-writeoff](./quarantine-writeoff.md) |
| 已 `processed` 的歷史值被清洗 bug 洗壞了 | [proposal-c-correction](./proposal-c-correction.md) |
| 記錄卡在 `pending` 或 `processing`；broker 掛了 | [queue-ops](./queue-ops.md) |
| 某個 dbt model 需要重建，或 `int_` 被改動 | [dbt-ops](./dbt-ops.md) |
| 要對 ODS 加或刪一個欄位 | [schema-change](./schema-change.md) |

---

## 動手之前

**兩條適用於這裡每一個程序的規則：**

1. **不要手動編輯 `raw.status`。** 狀態機對每一種卡住的狀態都有恢復路徑；直接改它會繞過那些路徑所倚賴的不變式。該怎麼做見 [queue-ops](./queue-ops.md)。

2. **不要修改 ODS。** ODS 是不可變錨點。任何修正都走 `quality_events`（[ADR-0032](../adr/0032-bounded-writeback.md)）。
   唯一的例外是 [proposal-c-correction](./proposal-c-correction.md) 的遷移形——**批次、有版本、留退役副本、強制連動下游**。被這條規則禁止的是「單筆、無版本、下游不知情」的改寫。

---

## 每個紅燈代表什麼

DAG 是刻意分開的，好讓每個紅燈只代表一件事（[ADR-0039](../adr/0039-observation-signals-own-dag.md)）：

| DAG 紅燈 | 代表 | 去看 |
|---|---|---|
| `seed_demo_daily` | 什麼都進不來 | API、seeding 腳本 |
| `raw_pending_watch` | 資料進得了 Raw，沒人認領 | redis／worker／beat → [queue-ops](./queue-ops.md) |
| `orders_analytics_daily` | 管線壞了 | extract 或 dbt → [dbt-ops](./dbt-ops.md) |
| `source_freshness_watch` | staging 沒被往前推 | watermark 與 extract |

**完全沒有紅燈，但什麼都沒跑** → [airflow-silent-stall](./airflow-silent-stall.md)。**那是唯一沒有內建告警的失效模式。**
