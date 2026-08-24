# 任務佇列：Celery + Redis

[English](../../en/design/queue.md) | **繁體中文**

Raw 與 ODS 之間的派工路徑，以及它在故障下會發生什麼。

---

## 1. 範圍，以及與 Airflow 的邊界

這個系統裡有兩個排程器，而它們是**正交的**：

| | Celery + Redis | Airflow |
|---|---|---|
| 粒度 | 一筆記錄 | 一個批次 |
| 延遲 | 毫秒～秒 | 分鐘～小時 |
| 觸發者 | 一個 HTTP 請求 | 時鐘 |
| 擁有 | Raw → ODS | ODS → BigQuery → dbt |

它們**刻意不共用 Redis 實例**，讓故障域保持分離。

---

## 2. 設定

| 設定 | 值 | 為何 |
|---|---|---|
| `task_serializer` / `accept_content` | 只有 `json` | 反序列化 pickle 等同執行任意程式碼——可被寫入的 broker 就等同 RCE |
| `task_ignore_result` | `True` | `raw.status` 是唯一真相（[ADR-0011](../adr/0011-no-result-backend.md)） |
| `task_acks_late` | `True` | 在工作**之後**才確認，不是之前 |
| `task_reject_on_worker_lost` | `True` | worker 失聯 → 重新投遞 |
| `worker_prefetch_multiplier` | `1` | `acks_late` 的標配 |
| `visibility_timeout` | `600` | Redis 沒有真正的 ack；必須大於最長任務 |
| `socket_connect_timeout` / `socket_timeout` | `2` / `2` | 有界的 broker 等待（[ADR-0013](../adr/0013-bounded-broker-wait.md)） |
| `task_publish_retry_policy.max_retries` | `1` | 給瞬斷一次重試，然後落到兜底 |

`tasks.py` 是一層**薄包裝**：不設 `autoretry_for`、不設 `max_retries`。`process.py` 已經有四層重試，而 `process_raw_event` 從不拋例外——每一種失敗都已經被記進 `raw.status`。[ADR-0012](../adr/0012-process-stays-celery-free.md)

---

## 3. 派工路徑與它的降級

```
Raw 已 commit
    ↓
_enqueue(raw_id)
    ├── 電路 CLOSED → publish（上界約 2s）
    └── 電路 OPEN   → 立即回 False，完全不碰 Redis
    ↓
兩種情況都：200 pending
```

**不得有任何 DB 交易跨越派工。** `db.refresh()` 曾經如此，在 60 併發下讓 32 個連線池槽位中的 23 個卡在 `idle in transaction`，被一個對已死服務的網路呼叫持有著。

### 斷路器

連續三次失敗開路。broker 中斷期間實測 p50：**逾時 → 5ms**。

- **狀態是行程內的**——共享它需要 Redis，而 Redis 正是掛掉的那一個。代價：全叢集最多付 `threshold × 行程數` 次慢呼叫。
- **`half_open` 單飛**——一個探測，其餘照樣快速拒絕。
- **鎖只保護狀態轉移**，絕不跨越被包裝的呼叫。
- **`time.monotonic()`**——冷卻不受 NTP 或 DST 影響。

[ADR-0014](../adr/0014-circuit-breaker-dispatch.md)

---

## 4. CAS 認領與重新投遞的互動

`acks_late` 代表訊息可能被投遞兩次。那之所以安全，是因為冪等性早於佇列存在——但責任的劃分值得精確陳述：

| worker 死在 | `raw.status` | 重新投遞 |
|---|---|---|
| 認領 commit **之前** | 仍是 `pending` | CAS 成功——秒級恢復 |
| 認領 commit **之後** | 已是 `processing` | **CAS 失敗，task 立即返回** |

第二列是佇列救不回的那一半。只有 stale 掃描能救。

> **持久化佇列不會讓恢復掃描變得多餘。它讓掃描成為「佇列語意的補集」。**

---

## 5. 恢復掃描

跑在 Celery Beat 上，每 `scan_interval_seconds`（300 秒）一次，外加啟動時一次補掃。**Beat 是單例，絕不可被 `--scale`。**

它處理兩種情況，基準刻意不同：

| 情況 | 基準 | 門檻 | 動作 |
|---|---|---|---|
| 卡在 `processing` | `processing_started_at` | `STALE_PROCESSING_MINUTES = 10` | 退回 `pending` |
| 卡在 `pending` | `received_at` | `PENDING_GRACE_SECONDS = 60` | 重新派工 |

兩個門檻都住在 `recovery_policy.py`——一個**零第三方依賴**的模組，好讓一支唯讀探針能 import 它們而不繼承寫入路徑的依賴樹（[ADR-0039](../adr/0039-observation-signals-own-dag.md)）。

### 界限

斷路器讓中斷期間攝入維持全速，所以 `pending` 也以全速累積。五道界限：

| 界限 | 值 | 關掉什麼 |
|---|---|---|
| 每頁大小 | `SCAN_BATCH_SIZE = 5000` | 無界的記憶體 |
| 游標 | `WHERE id > :last_id ORDER BY id` | **光加 `LIMIT` 會永遠重撈**——派工不改變 `status` |
| 每輪頁數 | `SCAN_MAX_ROUNDS = 20` | 單一 task 獨佔一個 worker 槽位 |
| Redis 鎖 | key + 300s TTL，Lua compare-and-delete | 兩輪掃描重疊 |
| 寬限期 | 60s | 與攝入快路徑競爭 |

掃描是**刻意不精確的**——它可能重派一筆已在佇列裡的記錄。CAS 讓輸家立即返回，所以代價是一個浪費掉的槽位，絕不是重複寫入。

⚠️ 還有一道界限開著：**`raw.status` 沒有索引**，所以分頁限制的是載入量，不是資料庫掃描量。[ADR-0018](../adr/0018-raw-status-no-index.md)

---

## 6. 限流計數器

slowapi 預設把計數器放在行程記憶體裡。跨 N 個 uvicorn worker，`60/minute` 會靜默變成 `60 × N`——實測 4 個 worker 讓 100 個請求中的 **91 個**通過而非 60，**而且沒有任何地方報錯**。

因此計數器住在 **Redis db 1**（broker 用 db 0——`celery purge` 不該波及限流）。若 Redis 不可用，它降級為逐行程計數，而不是完全停用限流。

---

## 7. 相關

- [ADR-0010](../adr/0010-celery-replaces-backgroundtasks.md) · [ADR-0016](../adr/0016-recovery-scan-in-beat.md) · [ADR-0017](../adr/0017-bounded-recovery-scan.md)
- [ingestion](./ingestion.md) — 任務被認領之後發生什麼
- Runbook：`queue-ops`（第 4 階段）
