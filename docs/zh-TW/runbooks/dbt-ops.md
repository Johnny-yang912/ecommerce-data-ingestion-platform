# Runbook：dbt 維運

[English](../../en/runbooks/dbt-ops.md) | **繁體中文**

---

## 何時該 `--full-refresh`

只有這幾種情況：

- 改變分區或叢集設定
- 改變去重邏輯
- 重算回看窗**之外**的歷史
- 首次建置

它用 DDL（`CREATE OR REPLACE`），所以不受 sandbox 的 DML 禁令影響。

**只適用於 `stg_` 的增量模型。** `int_` 本來每次執行就是 `table` 全量重建（[ADR-0046](../adr/0046-stg-incremental-int-full-rebuild.md)）。

---

## ⚠️ 改動 `int_orders` 或 `int_orders_quarantine` 之前

走一遍**對齊清單**——兩個模型必須維持為 `stg_orders` 的一個劃分：

| # | 檢查 |
|---|---|
| 1 | 兩邊 `is_effectively_clean` 定義相同；一邊 `WHERE cond`、一邊 `WHERE NOT cond` |
| 2 | `coalesce(..., false)` **不可拿掉** |
| 3 | 一律 `LEFT JOIN` |
| 4 | window 的 `partition by` / `order by` 決勝鍵一致 |
| 5 | `effective_quality_state` 的 CASE 分支一致 |
| 6 | 兩者物化方式相同 |
| 7 | `assert_orders_split_is_partition` 維持 `severity: error` |

然後：

```bash
dbt build --select intermediate+
```

並確認 `assert_orders_split_is_partition` 是**綠的**。那個測試是那段重複區塊唯一的自動化安全網（[ADR-0045](../adr/0045-int-effective-state-duplication.md)）。

> #2 是最多人漏掉的。當 `has_clean_error=TRUE` 且無事件時，`FALSE OR NULL = NULL`——而 `WHERE NOT NULL` 也是 NULL，所以該列會**同時從兩張表消失**，而且是靜默的。

---

## 調整回看窗

| 變數 | 約束 |
|---|---|
| `stg_orders_lookback_days` | 預設 3；必須 ≥ E/L 的 `>=` 重抽範圍 + 餘裕 |
| `stg_quality_events_lookback_days` | 與上面一起放大 |
| `rpt_quality_events_lookback_days` | **必須 ≥ `stg_quality_events_lookback_days`** |

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: 7}'
```

這樣傳入的值是逐次呼叫的，不會被持久化。

連續失敗後的恢復見 [dag-failure-recovery](./dag-failure-recovery.md)。

---

## Proposal C 的定向刷新

修正列落在回看窗**看不到的舊分區**。修復的最後一步是對受影響分區做定向刷新：

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: <涵蓋修正的 N>}'
# 或 --full-refresh
```

---

## ⚠️ 絕不可把 `dbt build` 拆成 `dbt run` + `dbt test`

那會讓 `int_` 的上游變成「staging 的 **run**」而非「staging 的 **test**」——而 **Hard Gate 會靜默地停止阻擋**，髒資料流進 Gold。沒有東西報錯；DAG 是綠的；閘門變成裝飾品。

由 `tests/test_dags.py::test_dbt_never_splits_run_and_test` 釘住。[ADR-0040](../adr/0040-layered-dbt-execution.md)

---

## 讀懂一個紅掉的層

| 紅掉的 task | 代表 | 先檢查 |
|---|---|---|
| `dbt_staging` | **Hard Gate** 觸發，或某個 `stg_` 測試失敗 | 最新分區真的超過 15% 髒，還是下面那個誤觸？ |
| `dbt_intermediate` | **劃分不變式**壞了 | 走上面的對齊清單 |
| `dbt_marts` | 上捲不符，或某個 Gold 測試 | `assert_fct_orders_rollup_matches_items` |
| `dbt_test`（收尾那次） | 某個被分層執行跳過的測試 | 比對各層各跑了哪些測試 |

⚠️ **在目前的設定下，Hard Gate 的誤觸是預期之內的。** 模擬上游的髒資料率是 0.12 而閘門門檻是 0.15；n≈200 時批次標準差約 2.3 個百分點，所以**大約每十批就有一批純因隨機波動而觸發**。在把它讀成上游異常之前，先排除這一項（[ADR-0028](../adr/0028-hard-gate-per-batch-scope.md)）。

`dbt_*` task 刻意設 `retries=0`——一個紅掉的 task 沒有被重試過，所以**它就是字面意思**。

---

## 相關

- [design/transformation](../design/transformation.md) — 每一層做什麼
- [schema-change](./schema-change.md) — 加或刪一個欄位
