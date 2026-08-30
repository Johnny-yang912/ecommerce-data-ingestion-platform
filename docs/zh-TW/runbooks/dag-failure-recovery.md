# Runbook：從連續 DAG 失敗中恢復

[English](../../en/runbooks/dag-failure-recovery.md) | **繁體中文**

---

## 何時適用

`orders_analytics_daily` **連續失敗的天數超過了回看窗**（預設 3 天）。

**單次失敗是安全的**，什麼都不用做：staging 已經 append 了、watermark 已經推進了，而 `stg_` 的回看窗會在下一輪重算那幾天。

**連續失敗才是危險的。** 回看窗 3 天、DAG 掛了 4 天，恢復時只往回看 3 天——所以**落在那個邊界之前的列永遠不會進到 `stg_orders`**。沒有錯誤、沒有自癒。**靜默的資料遺失。**

---

## 程序

### 1. 數出掛了幾天

```bash
docker compose exec airflow-db psql -U airflow -d airflow -c \
  "select max(run_after) from dag_run
    where dag_id='orders_analytics_daily' and state='success';"
```

`N = 距離那個時戳的天數 + 餘裕`。

### 2. 先修好把 DAG 弄壞的東西

在 DAG 能跑起來之前不要往下走。對一條仍然壞掉的管線放大窗口，只是用更大的查詢重複同一次失敗。

### 3. 修好後的第一次執行——放大窗口

```bash
docker compose exec airflow-scheduler bash -c \
  "/home/airflow/venvs/dbt/bin/dbt build \
     --select path:models/staging \
     --vars '{stg_orders_lookback_days: N}'"
```

⚠️ **`stg_quality_events` 與 `rpt_quality_events_daily` 必須一起放大。** 它們的變數是 `stg_quality_events_lookback_days` 與 `rpt_quality_events_lookback_days`，而約束是 `rpt_ >= stg_`。

```bash
--vars '{stg_orders_lookback_days: N, stg_quality_events_lookback_days: N, rpt_quality_events_lookback_days: N}'
```

或者用 `--full-refresh`——正確、無條件，代價是一次全表重寫。

⚠️ **放大回看窗時 `stg_orders_recon_window_days` 也要一起放大**，否則對帳測試會看不到
你正在修的那些天（約束：對帳窗 > 回看窗）。

**這一節適用於「連續掛了 N 天」——你真的想要最近 N 天全部重算。**
如果壞的是某幾個特定日期（而不是「最近都壞」），放大窗口是錯的工具：它會連帶重算
那些天之後的每一天。改用 [dbt-ops §重建特定分區](./dbt-ops.md#重建特定分區)。

### 4. 驗證沒有東西遺失

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/project/ecommerce_dbt && /home/airflow/venvs/dbt/bin/dbt test \
     -s assert_stg_orders_matches_staging \
     --vars '{stg_orders_recon_window_days: N}'"
```

這條測試逐分區比對 staging 的 `distinct raw_id` 與 `stg_orders` 的列數，失敗時直接吐出
是哪一天、少幾筆：

```
received_day               rows_expected   rows_actual   missing
2026-08-26 00:00:00+00              800           250        550
```

⚠️ `N` 要涵蓋整個受影響範圍，預設的 7 天通常不夠。

> 這一步曾經是一段要人記得手動貼上去的 SQL。它現在是每晚跑的測試——
> 而 [2026-08-30 事故](../incidents/2026-08-30-stg-partition-truncation.md)發生時，
> 這條檢查已經寫在這份 runbook 裡了，只是沒有人在那天想到要跑它。

### 5. 回到預設窗口

放大的值是逐次呼叫傳入的，**不會**被持久化——下一次排程執行會自動回到 3 天。沒有東西需要復原。

---

## 預防

> **回看窗其實是一份「這條管線能容忍多少無人值守的失敗」的宣告**，不只是一個成本參數。

Airflow 的失敗告警必須在累積停機逼近窗口**之前**被看見。那正是失敗通知（[ADR-0042](../adr/0042-failure-notification-response-not-task.md)）的用途——**而它只涵蓋「跑了而且失敗」，所以一台單純關著的機器對它是不可見的。**

---

## 相關

- [dbt-ops](./dbt-ops.md) — 一般情況下何時該 `--full-refresh`
- [design/transformation](../design/transformation.md) — 回看窗如何運作
