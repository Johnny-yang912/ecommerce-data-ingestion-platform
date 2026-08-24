# 2026-08-10 — Celery + Redis 下的 SIGKILL 恢復

[English](../../en/verification/2026-08-10-celery-sigkill-recovery.md) | **繁體中文**

---

## 驗證的假設

把 `BackgroundTasks` 換成持久化佇列之後，**崩潰恢復是完整的嗎？** 而且具體地說：**佇列讓恢復掃描變得多餘了嗎？**

## 環境

完整的 docker compose 環境——api / worker×4 / beat / redis / postgres——並設 `SCAN_INTERVAL_SECONDS=20` 以縮短觀察窗口。2026-08-10。

## 方法

注入 800 筆 `pending`。等 Beat 派完工、worker 處理了 225 筆之後：

```bash
docker compose kill -s SIGKILL worker
```

然後：重啟 worker、等待、觀察。第三列把 `processing_started_at` 往回調 11 分鐘，用來模擬跨過 10 分鐘的 stale 門檻，而不是真的空等。

## 觀測

| 時點 | `pending` | `processing` | `processed` |
|---|---|---|---|
| SIGKILL 當下 | 537 | 2 | 261 |
| worker 重啟 30 秒後 | 0 | **2** | 798 |
| 過了 stale 門檻掃描一次後 | 0 | 0 | **800** |

ODS 最終計數：**800。什麼都沒丟。**

同一次工作階段的兩項佐證觀測：

- **Beat 的啟動補掃有效。** Beat 在 `05:58:38` 啟動；`beat_init` 派出的掃描在 `05:58:39` 抵達 worker，而第一次排程 tick 是 `05:58:58`（+20 秒）。補掃確實關掉了第一個間隔的缺口。
- **攝入在 broker 中斷時存活。** Redis 停掉時，`POST /orders` 回 HTTP 200 + `pending`（3.81 秒——這次量測早於斷路器），資料有落地。`/health` 在 1.7ms 內回應。Redis 回來後，滯留的記錄被掃描撿走並完成。

## 結論

恢復是完整的——**而中間那一列正是這則記錄存在的理由。**

還在佇列裡的 537 筆靠重新投遞自行排空。但 **SIGKILL 當下正在處理中的那 2 筆，重啟 worker 救不回來**：重新投遞抵達了、因為狀態已不是 `pending` 而 CAS 失敗、立即返回。只有 stale 掃描救得回它們。

> **持久化佇列不會讓恢復掃描變得多餘。它讓掃描成為「佇列語意的補集」**——佇列恢復它仍然擁有的，掃描恢復那些在 worker 死去時已被認領的。

## 這推翻了什麼 ⭐

README 壓測 #5 先前的結論是：*「150 筆卡在 `pending`，重啟後沒有自動恢復。」*

那對 `BackgroundTasks` 而言是真的，它的記憶體佇列在死亡時遺失任務狀態。現在不再為真——**但修法不是「加一個持久化佇列」**，那樣做仍然會讓那 2 筆卡住。**它需要佇列**與**掃描兩者**，各自覆蓋對方做不到的部分。

## 相關

- [ADR-0010](../adr/0010-celery-replaces-backgroundtasks.md) — 這裡驗證的那個佇列
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) — 為何重新投遞救不了一筆已被認領的記錄
- [design/queue](../design/queue.md)
