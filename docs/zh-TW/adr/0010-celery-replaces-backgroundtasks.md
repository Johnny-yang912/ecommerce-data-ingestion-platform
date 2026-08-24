# ADR-0010：以 Celery + Redis 取代 `BackgroundTasks`

[English](../../en/adr/0010-celery-replaces-backgroundtasks.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 任務佇列 |

---

## 背景

`BackgroundTasks` 是 API 行程內的記憶體佇列。任務狀態不被持久化到任何地方，這造成兩個看起來各自獨立的後果。

**崩潰會遺失工作。** 處理中途的 `SIGKILL` 留下 150 筆停在 `pending` 的記錄，而系統沒有任何機制重新處理它們——資料庫的列存在，但系統裡沒有任何東西知道它們還欠一個任務。恢復完全依賴週期性掃描注意到它們。

**API 無法橫向擴充。** 有兩份行程內狀態把它釘死在 `--workers 1`：背景任務池，以及跑在 FastAPI `lifespan` asyncio 迴圈上的恢復掃描。任何第二個行程都會重複執行掃描。

## 決策

以 Celery + Redis 作為攝入路徑的任務佇列，並：

- `acks_late = True` 與 `task_reject_on_worker_lost = True`——訊息在工作**之後**才被確認，所以 worker 失聯會觸發重新投遞。
- `worker_prefetch_multiplier = 1`——`acks_late` 的標配。沒有它，一個 worker 抓走一批訊息後崩潰，整批都要等到 visibility timeout 到期才重見天日。
- `visibility_timeout = 600`——Redis 沒有真正的 ack，重新投遞靠 visibility timeout 模擬。**它必須大於最長可能的任務耗時**，否則還在跑的任務會被重投遞，系統就會踩踏。`process_raw_event` 最壞情況是三層指數退避（秒級），600 秒綽綽有餘。
- 只用 JSON 序列化，絕不用 pickle——反序列化 pickle 等同執行任意程式碼，所以一個可被寫入的 broker 就等同 RCE。

## 後果

**已驗證：`SIGKILL` 下零遺失。** 積壓 800 筆，處理到 225 筆時砍掉 worker：

| 時點 | `pending` | `processing` | `processed` |
|---|---|---|---|
| `SIGKILL` 當下 | 537 | 2 | 261 |
| worker 重啟 30 秒後 | 0 | **2** | 798 |
| 過了 stale 門檻掃描一次後 | 0 | 0 | **800** |

ODS 最終計數 800，什麼都沒丟。

**中間那一列才是重點，而且它推翻了一個假設。** `SIGKILL` 當下那 2 筆已經被認領進 `processing` 的記錄，**重啟 worker 救不回來**。重投遞會抵達、因為狀態已不是 `pending` 而 CAS 失敗、立刻返回。只有 stale 掃描能救它們。

> **持久化佇列並不會讓恢復掃描變得多餘。它讓掃描成為「佇列語意的補集」**——佇列恢復它仍然擁有的，掃描恢復那些在 worker 死去時已被認領的。

**多個 API 行程因此成為可能**——但只有在掃描也搬出 API 行程之後（ADR-0016）。光有佇列並不夠。

**`acks_late` 接受「同一則訊息可能被處理兩次」。** 這在這裡是安全的，正是因為冪等性早就存在：CAS 認領（ADR-0004）與 `UNIQUE(ods.order_id)`（ADR-0005）都建立在佇列之前。

## 考慮過的替代方案

**保留 `BackgroundTasks`，靠掃描兜底。** 掃描確實最終能恢復一切，但「最終」是一個掃描間隔，而且 API 永遠是單行程。

**用 RabbitMQ 而非 Redis。** 有真正的 ack 而非模擬的 visibility timeout，代價是多一個要維運的服務。Redis 本來就在技術棧裡（限流計數器用它），而 visibility timeout 的語意是清楚且有上界的。

## 相關

- [ADR-0004](./0004-cas-claim-rowcount.md)、[ADR-0005](./0005-first-write-wins-idempotency.md) — `acks_late` 所依賴的冪等性
- [ADR-0016](./0016-recovery-scan-in-beat.md) — 讓多行程成為可能的另一半
- [佇列設計](../design/queue.md)
