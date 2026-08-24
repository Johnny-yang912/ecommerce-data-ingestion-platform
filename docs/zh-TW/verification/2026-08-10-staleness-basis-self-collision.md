# 2026-08-10 — 逾時判定基準：`received_at` vs `processing_started_at`

[English](../../en/verification/2026-08-10-staleness-basis-self-collision.md) | **繁體中文**

---

## 驗證的假設

恢復掃描原本用 `received_at` 判定「卡在 `processing`」。**那個基準會不會讓系統在積壓時與自己碰撞？**

假設是：`received_at` 回答的是*「這筆資料躺了多久」*，而掃描需要的是*「這次嘗試跑了多久」*。積壓時兩者相差極大——**而積壓正是這個判定最常被觸發的時候。**

## 環境

同一個 compose 環境、worker×4，並設 `SCAN_INTERVAL_SECONDS=5` 以縮緊掃描節奏。2026-08-10。

## 方法

插入 2,000 筆 `pending`，`received_at = now() - 30 分鐘`，模擬一次長時間 broker 中斷所留下的積壓。同一支腳本、同一份資料；**只有基準欄位不同。**

自我碰撞的辨識方式：`raw.status='duplicate'` ⋈ `ods.raw_id = raw.id`——一筆被標記為 duplicate 的記錄，撞到的是它自己寫的那列 ODS。

## 觀測

| 基準 | `processed` | `duplicate` | 自我碰撞 |
|---|---|---|---|
| `received_at`（之前） | 1998 | **2** | **2** |
| `processing_started_at`（之後） | **2000** | 0 | **0** |

那兩筆的 `error_message` 字面上寫著 `already written by raw_id=1998`——**而 1998 就是它們自己的 id。** 那個 `order_id` 在 `raw` 裡恰好只出現一次，排除了上游重送。

### 機制

```
T-30min  攝入；received_at = T-30min。broker 停機，留在 pending。
T+0      broker 復原；掃描派工。worker A 搶佔成功 → processing。
T+0.01   worker A 正在清洗、組 ODS（尚未 commit）。
T+0.02   下一輪掃描：status='processing' ✓ 且 received_at < now()-10min ✓
         → 判定 stale → 退回 pending → 再派一則新訊息。
T+0.03   worker B 搶佔：狀態現在是 pending，CAS 成功。 ← 沒有東西擋得住
T+0.05   A 先 commit：ODS 落地，raw.status = 'processed'。
T+0.06   B 撞到它自己剛寫的那列 ODS，被判為 duplicate，蓋掉 processed。
```

## 關於規模

每次掃描 tick 命中的記錄數 ≈ 同時處於 `processing` 的數量 ≈ worker 併發度，**與積壓總量無關**（單筆約 40ms，遠低於掃描間隔）。

所以這是**「罕見但真實」**，不是全面汙染——**而它專挑系統正在追趕的時候下手**，那正是監控訊號最不該被汙染的時刻。

## 結論

基準是錯的。改用 `processing_started_at` 讓自我碰撞變成**不可達，而非只是不太可能**：計時從認領起算，所以不論 `received_at` 說什麼，`T+0.02` 那一步都不可能發生。

接著重跑了 SIGKILL 場景，確認恢復機制本身沒有被這次改動弄壞：那 2 筆卡在 `processing` 的記錄如常被收回，最終 2,900 筆全部 `processed`、2,900 列 ODS、**0 次自我碰撞**。

## 這推翻了什麼 ⭐

**先前假設 CAS 對互斥而言已經足夠。** 它不是——而且邊界比看起來窄：

> CAS 保證的是*「從這個狀態只會有一次轉出」*。它對**還有誰可能轉入這個狀態**沒有任何保證。

**資料從未損壞**——`UNIQUE(ods.order_id)` 全程守住了。壞掉的是**訊號**：一筆其實成功的訂單頂著 `duplicate`，污染了一個存在目的就是要區分「上游送了兩次」與「這個系統失敗了」的狀態。

**那個訊號的價值，正是這個缺陷值得修的原因。** 如果 `duplicate` 當初被併進 `error`，那次汙染會是不可見的。

## 相關

- [ADR-0015](../adr/0015-staleness-from-processing-started-at.md) — 由此產生的決策
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) — 這次找到的那個保證的邊界
- [ADR-0003](../adr/0003-duplicate-terminal-status.md) — 被污染的那個訊號
