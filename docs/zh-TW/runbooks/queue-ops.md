# Runbook：佇列維運

[English](../../en/runbooks/queue-ops.md) | **繁體中文**

---

## 檢視

```bash
# 佇列積壓
docker compose exec redis redis-cli llen celery

# 當前狀態分布 —— 真相住在這裡，不在 Redis
docker compose exec db psql -U app -d orders -c \
  "select status, count(*) from raw group by status order by status;"

# 最舊的 pending —— 這正是 raw_pending_watch 檢查的東西
docker compose exec db psql -U app -d orders -c \
  "select min(received_at), count(*) from raw where status='pending';"

# 斷路器開路了嗎？（狀態轉移各記一次）
docker compose logs api | grep circuit_breaker | tail -20
```

---

## 動作

```bash
# 手動觸發一次恢復掃描，不必等 Beat
docker compose exec worker celery -A celery_app call tasks.scan_and_dispatch

# 重新處理單一筆記錄 —— broker 掛掉時的救援路徑。
# 不涉及佇列；這正是 process.py 保持 Celery-free 的原因（ADR-0012）。
docker compose exec worker python -c \
  "from process import process_raw_event; process_raw_event(123)"

# 縮短掃描間隔以觀察行為（預設 300 秒）
SCAN_INTERVAL_SECONDS=20 docker compose up -d
```

---

## ⚠️ 卡在 `processing` 的記錄

**不要手動編輯 `status`。** 讓 stale 掃描在 10 分鐘後處理它——**那正是它的用途。**

如果真的需要立即恢復，把那筆記錄的 **`processing_started_at`** 往回調到 `STALE_PROCESSING_MINUTES` 之外，然後等一次掃描。語意上那是*「宣告這次嘗試逾時了」*，而那是系統本來就知道怎麼處理的陳述。

```sql
update raw
   set processing_started_at = now() - interval '20 minutes'
 where id = <raw_id> and status = 'processing';
```

> ⚠️ **不要碰 `received_at`。** 它是攝入時戳、是資料血緣的一部分，與逾時判定毫無關係。**這兩個欄位回答的是不同的問題**（[ADR-0015](../adr/0015-staleness-from-processing-started-at.md)）。

---

## 卡在 `pending` 的記錄

通常代表派工路徑失敗了而掃描還沒追上——**若 broker 掛著，那是正常的降級行為，不是故障。**

| 先檢查 | 然後 |
|---|---|
| redis 起來了嗎？ | `docker compose ps redis` |
| 電路開著嗎？ | grep 上面那條 log——若開路，攝入是刻意不碰 Redis 的 |
| beat 活著嗎？ | `docker compose logs beat \| tail`——beat 必須是**單例**，絕不可 `--scale` |
| worker 有在消費嗎？ | `docker compose exec redis redis-cli llen celery`——佇列成長但 worker 活著，是另一種問題 |

broker 恢復之後，掃描會自己把積壓排空。已對 120,000 列驗證過：兩次掃描、ODS 恰好成長 120,000、零重複。

---

## Broker 掛掉時該預期什麼

這是被設計出來的降級，不是一次中斷：

| | 行為 |
|---|---|
| `POST /orders` | 仍然回 **`200 pending`**——Raw 那一列已經 commit |
| 派工 | 連續三次失敗後開路；p50 降到約 5ms |
| Log 量 | 每次狀態轉移一行，**不是**每個請求一則 traceback |
| 恢復 | broker 回來後掃描排空 `pending` |

**不要用「改回 500」來「修」這件事。** 客戶端無法區分拒絕與慢回應，所以它會重送——為其實已被接受的訂單製造重複（[ADR-0013](../adr/0013-bounded-broker-wait.md)）。

---

## Beat

```bash
# Beat 是【單例】。兩個 beat 行程會派出重複的掃描。
docker compose ps beat        # 必須恰好顯示 1 個
```

Beat 也會在啟動時放一次補掃，所以重啟不會留下一整個間隔的空窗。

---

## 相關

- [design/queue](../design/queue.md) — 掃描的五道界限如何運作
- [ADR-0017](../adr/0017-bounded-recovery-scan.md) — 為何掃描是刻意不精確的
