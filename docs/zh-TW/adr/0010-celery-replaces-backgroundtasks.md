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

### 三個常見選項

| | Kafka | RabbitMQ | Redis + Celery（採用） |
|---|---|---|---|
| 本質上是什麼 | 有保留期的分散式 commit log | 訊息 broker——真 ack／nack／死信 | 資料結構伺服器；佇列語意由 Celery 在上層模擬 |
| 投遞保證 | at-least-once（exactly-once 需交易 API） | at-least-once，**真 ack** | at-least-once，**ack 靠 visibility timeout 模擬** |
| 失聯後何時重投遞 | consumer group rebalance | 連線中斷即時 requeue | 等 visibility timeout 到期（此處 600s） |
| 可重播歷史 | ✅ 保留期內任意 offset | ❌ 消費即消失 | ❌ |
| 死信 | 自行實作 | ✅ 原生 DLX | 無（重試在 Celery 層） |
| 新增的維運表面 | broker + 分區規劃 + 磁碟容量 | 一個服務 + vhost／queue 宣告 | **零**——Redis 已在棧內（限流計數器，db1） |
| 這個系統實際會用到的 | 派工 | 派工 | 派工 |

最後一列是整張表的重點：**三者在這裡都只會被用來做同一件事。** 差別因此不在能力，而在「多買到的能力值不值得它的成本」。

**為什麼不是 Kafka。** Kafka 最貴、也最有價值的能力是「保留的事件流可以被重播，並被多個獨立消費者群各自取用」。這兩件事在這裡都是重複的——一則訊息只有一個消費者，沒有第二個消費者群；而**重播的職責根本不在佇列，在 Raw 表**：payload 逐字保留於 Raw（[ADR-0053](./0053-raw-text-ods-jsonb.md)），重放的入口是 `force=true` 與 Proposal C，兩者都從 Raw 出發。

> 若佇列也保留一份可重播的歷史，系統就有了**兩個「到達了什麼」的真相**——而那正是 single-ingress 不變式禁止的事。**Raw 已經是這個系統的 commit log 了。**

於是分區規劃、磁碟容量與一個額外的協調服務，買到的是一個這裡已經有、而且刻意只留一份的東西。

**為什麼不是 RabbitMQ。** 三者裡它技術上最貼合「派工」這個形狀：真 ack、原生死信、失聯即時 requeue，而且沒有 `visibility_timeout` 這種**可以設錯**的參數。不選它的理由不是它比較差，而是**它的優勢在這個系統裡已經被別的東西買走了**。

`acks_late` 的弱點是「同一則訊息可能被處理兩次」。真 ack 能縮小那個窗口，但縮不到零——任何 at-least-once 系統都一樣。而這裡的重複投遞**本來就是無操作**：CAS 認領（[ADR-0004](./0004-cas-claim-rowcount.md)）與 `UNIQUE(ods.order_id)`（[ADR-0005](./0005-first-write-wins-idempotency.md)）都建立在佇列之前，重投遞抵達時 CAS 失敗、立刻返回。**要用 RabbitMQ 買的那個保證，冪等性已經先付過一次錢了。** 剩下的差額是一個要維運的新服務，而 Redis 本來就在棧裡。

**選 Redis 的代價，寫清楚。** 它換來的不是「沒有代價」，是「一個有上界、且已被量測過的代價」：

| 代價 | 為何可接受 |
|---|---|
| `visibility_timeout` 是可以設錯的參數——設得比最長任務短，任務還在跑就被重投遞，形成重複執行風暴 | 取 600s，而 `process_raw_event` 最壞情況是三層指數退避（秒級）。**這是一個需要被知道、而非需要被信任的數字** |
| Redis 的持久化（AOF／RDB）弱於另外兩者，崩潰時可能丟掉最後幾則訊息 | 丟掉的訊息對應的 raw 列仍停在 `pending`，恢復掃描會撿回來——**佇列可以丟訊息，資料庫不會丟列** |

第二列正是本文開頭那個結論的另一面：佇列與掃描是互補的，所以佇列的耐久性不必是絕對的。

**而且這個決定是可逆的。** Celery 對其中兩者（Redis／RabbitMQ）都只是換 `broker_url`，`acks_late`、`prefetch`、序列化那些設定一律不動。**真正不可逆的是把冪等性建在佇列之外——而那件事已經做完了**，所以換 broker 永遠只是一次維運決定，不會變成一次重寫。

## 相關

- [ADR-0004](./0004-cas-claim-rowcount.md)、[ADR-0005](./0005-first-write-wins-idempotency.md) — `acks_late` 所依賴的冪等性
- [ADR-0016](./0016-recovery-scan-in-beat.md) — 讓多行程成為可能的另一半
- [佇列設計](../design/queue.md)
