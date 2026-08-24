# Runbook：永久註銷一筆記錄

[English](../../en/runbooks/quarantine-writeoff.md) | **繁體中文**

---

## 何時適用

一筆被隔離的記錄永遠不會變得可用，而且應該停止出現在 backlog 裡。

`permanently_rejected` 是**人的終端決定**。狀態機沒有從它出去的邊，而**自動任務永不寫入它、也永不覆蓋它。**

---

## ⚠️ 刻意不是 endpoint，也不是 DAG

沒有 `POST /reject`，也沒有 `write_off` DAG。那是一個決策而非疏漏——與 Proposal C 永不成為 HTTP endpoint 是同一份紀律：

> **不可逆的決定不該有方便的按鈕。**

事件是手動 append 的，對 PostgreSQL 寫入，並附上記錄下來的理由。

---

## 程序

### 1. 確認這筆記錄真的無法救回

確認它不是只是在等一條即將放寬的規則：

```sql
select raw_id, order_id, error_codes, quarantined_at
from `<project>.<dbt_dataset>.int_orders_quarantine`
where order_id = '<order_id>';
```

如果失敗的原因是一個有可能移動的門檻，請改用 [proposal-b-rollout](./proposal-b-rollout.md)。

### 2. 在 PostgreSQL 確認當前狀態

**狀態以 PostgreSQL 為權威**，不是 BigQuery——倉庫的鏡像 60 天就過期。

```sql
select event_type, from_state, to_state, rule_version, event_at, reason
from quality_events
where order_id = '<order_id>'
order by event_at desc, id desc;
```

最新的 `to_state` 必須是 `quarantined` 或 `re_quarantined`。若已經是 `permanently_rejected`，就停手——那個狀態是終端的。

### 3. Append rejection 事件

```sql
insert into quality_events
  (raw_id, order_id, event_type, from_state, to_state, rule_version, reason)
values
  (<raw_id>, '<order_id>', 'rejection', '<current_state>', 'permanently_rejected',
   '<當前的 DQ_RULE_VERSION>',
   '{"decided_by": "<姓名>", "why": "<真正的理由>", "ticket": "<單號>"}'::jsonb);
```

**`reason` 不是選填的。** 它是「一個人為何放棄這一列」的唯一記錄，而且沒有任何路徑會再產生它一次。

### 4. 讓它流下去

事件必須抵達 BigQuery 並被 `int_` 的重建撿走：

```bash
docker exec api-airflow-apiserver-1 airflow dags trigger orders_analytics_daily
```

### 5. 驗證

| 檢查 | 預期 |
|---|---|
| `int_orders_quarantine` | 該列**不見了** |
| `int_orders` / `fct_orders` | 該列**也不在**那裡 |
| `rpt_quality_backlog` | 計數少一 |
| `ods` 那一列 | **未變**——ODS 從不被修改 |

一筆被註銷的記錄會同時離開 Gold 與隔離區。它永遠留在 ODS 與事件日誌裡，**而那正是重點：這個決定即使是最終的，仍然是可稽核的。**

---

## 絕不可做的事

| 不要 | 為何 |
|---|---|
| `UPDATE ods SET has_clean_error = FALSE` | 違反不可變錨點與 bounded writeback（[ADR-0032](../adr/0032-bounded-writeback.md)） |
| `DELETE FROM ods` | 摧毀每一個歷史品質指標的分母 |
| `DELETE FROM quality_events` | 日誌是 append-only 的；移除歷史正是它存在要防止的事 |
| 用腳本寫入 `permanently_rejected` | 那個狀態保留給人的決定，在寫入目標上強制 |

---

## 相關

- [design/data-quality](../design/data-quality.md) — 狀態機
- [proposal-b-rollout](./proposal-b-rollout.md) — 若這筆記錄還有救，走那條路
