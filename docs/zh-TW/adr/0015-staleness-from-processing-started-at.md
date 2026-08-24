# ADR-0015：逾時以 `processing_started_at` 判定，非 `received_at`

[English](../../en/adr/0015-staleness-from-processing-started-at.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 任務佇列 — 恢復 |

---

## 背景

恢復掃描把卡在 `processing` 的記錄退回 `pending`，前提假設是 worker 已經死了。它原本用 `received_at` 判定「卡住」，因為那個欄位本來就存在。

**`received_at` 回答的是錯的問題。** 掃描需要知道的是**這次嘗試跑了多久**；`received_at` 說的是**這筆資料躺了多久**。正常情況下兩者幾乎相等——資料一到就被處理。**積壓時兩者相差極大——而積壓正是這個判定最常被觸發的時候。**

被重現出來的失效時間軸：

```
T-30min  訂單攝入，received_at = T-30min。
         broker 停機，所以它留在 pending。
T+0      broker 復原，掃描派工。worker A 搶佔成功 → processing。
T+0.01   worker A 正在清洗、組 ODS（尚未 commit）。
T+0.02   下一輪掃描：status='processing' ✓ 且 received_at < now()-10min ✓
         → 判定 stale → 退回 pending → 再派一則新訊息。
T+0.03   worker B 搶佔：狀態現在是 pending，CAS 成功。
         ← 沒有任何東西擋得住。同一個 raw_id 現在有兩個 worker 在跑。
T+0.05   A 先 commit：ODS 那一列落地，raw.status = 'processed'。
T+0.06   B 撞到「自己」剛寫的那列 ODS，被判為 duplicate，蓋掉 processed。
```

**注意 CAS 並沒有失效。** 它只保證「從 `pending` 只會有一次轉出」。它無法防止第三方在處理進行中把記錄轉**回** `pending`（ADR-0004）。

**資料從未損壞**——`UNIQUE(ods.order_id)` 守住了。壞掉的是**訊號**：一筆其實成功的訂單頂著 `duplicate`，污染了那個刻意保留的狀態語意（ADR-0003）。在 2,000 筆積壓中重現了兩次。

## 決策

新增 `raw.processing_started_at`，由 `try_claim_raw` 在認領成功的那一刻蓋上。逾時從它起算：

```sql
WHERE status = 'processing' AND processing_started_at < now() - :stale_threshold
```

**不變式：** `status = 'processing'` ⇒ `processing_started_at IS NOT NULL`。之所以成立，是因為 `try_claim_raw` 是進入 `processing` 的唯一路徑（ADR-0004）；既有資料由 migration `e5f6a7b8c9d0` 做 backfill 建立。

## 兩個門檻問的是兩個不同的問題

兩者都住在 `recovery_policy.py`，而基準刻意不同：

| 常數 | 基準 | 問題 |
|---|---|---|
| `STALE_PROCESSING_MINUTES`（10） | `processing_started_at` | **這次嘗試**跑了多久？ |
| `PENDING_GRACE_SECONDS`（60） | `received_at` | **這筆資料**躺了多久？ |

`PENDING_GRACE_SECONDS` 用 `received_at` 是正確的：剛攝入的 `pending` 留給攝入路徑，因為快路徑正常會在毫秒內把它派出去，掃描此時介入只會多送一則冗餘訊息。只有「過了寬限期還是 `pending`」才代表快路徑真的失手了。

**同一個模組、不同的基準——而把它們搞混，正是這條 ADR 在講的那個 bug。**

## 後果

**自我碰撞變成不可達，而不只是不太可能。** 計時現在從認領起算，所以 `T+0.02` 那一步不可能發生——不論 `received_at` 說什麼，那筆記錄都還沒在 `processing` 待滿 10 分鐘。

**`duplicate` 訊號恢復了它的意義**：它重新只指向上游行為，而不是這個系統自己的行為。

**代價是一個欄位與一次帶 backfill 的 migration。**

## 考慮過的替代方案

**調高 stale 門檻。** 把窗口拉寬但沒有關上它，同時換來真實崩潰恢復變慢。

**讓掃描跳過剛派工過的記錄。** 需要在某個地方追蹤派工時間——那就是 `processing_started_at` 的另一個名字，只是被放在設定狀態的那個交易之外。

## 相關

- [ADR-0004](./0004-cas-claim-rowcount.md) — 這個缺陷所找到的那個保證的邊界
- [ADR-0003](./0003-duplicate-terminal-status.md) — 被污染的那個訊號
- [ADR-0017](./0017-bounded-recovery-scan.md) — 對同一支掃描的另一項修正
