# ADR-0004：CAS 認領以 `rowcount == 1` 判定，不外接佇列

[English](../../en/adr/0004-cas-claim-rowcount.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-05 |
| **層** | 攝入 — 處理 |

---

## 背景

同一個 `raw_id` 可能被交給不只一個 worker。至少有三條路徑：恢復掃描重派一筆已經在佇列裡的記錄、`acks_late` 之後 broker 的重新投遞、以及手動 `POST /process_raw/{id}`。沒有互斥的話，兩個 worker 會各自為同一筆訂單寫一列 ODS。

反射動作是去拿一把鎖——advisory lock、Redis lock，或一個承諾 exactly-once 的佇列。**這三者都是為了保證一件資料庫的那一列自己就能保證的事，而引入一個外部依賴。**

## 決策

認領是一句條件式 `UPDATE`：

```sql
UPDATE raw
   SET status = 'processing', processing_started_at = now()
 WHERE id = :raw_id AND status = 'pending'
```

當且僅當 `rowcount == 1` 時認領成功。PostgreSQL 對這句 `UPDATE` 做列鎖，所以 N 個併發嘗試中恰好一個觀察到 `pending` 並完成轉移；其餘 N−1 個拿到 `rowcount == 0` 並立即返回，不碰任何東西。

**這同時也是進入 `processing` 的唯一路徑**，這正是不變式 `status='processing' ⇒ processing_started_at IS NOT NULL` 得以成立的原因（ADR-0015 依賴它）。

## 後果

**不需要鎖服務，不需要 exactly-once 佇列。** 那個本來就必須存在的狀態欄位完成了這項工作。

**在真實併發下驗證過**：100 個 worker 競爭同一個 `raw_id`，結果是 `raw.status = processed` 且 ODS 計數恰好為 1。

**輸家很便宜。** 一次失敗的認領是一次來回、且不持有任何開啟的交易，所以重派是浪費但不危險——**正是這一點讓恢復掃描可以刻意不精確**（ADR-0017）。

**邊界很重要，而且很窄。** CAS 防的是**同一個狀態下**的併發競爭者。它**不**防止第三方在 worker 處理進行中把狀態退回 `pending`——此時第二個 worker 的 CAS 會合法地成功。這不是假設性的：那正是 ADR-0015 存在的目的。

> CAS 保證的是「從這個狀態只會有一次轉出」。它對「還有誰可能轉**入**這個狀態」沒有任何保證。

## 考慮過的替代方案

**`SELECT ... FOR UPDATE` 之後再 `UPDATE`。** 兩次來回，而且交易要橫跨兩者，換取單句條件式 `UPDATE` 本來就原子提供的保證。

**Advisory lock 或 Redis lock。** 引入「誰擁有這筆記錄」的第二個真相來源，而它可以與 `raw.status` 不一致。當兩者不一致時，錯的那個具有權威。

**具 exactly-once 語意的佇列。** Redis broker 不提供；而一個提供它的 broker，其維運依賴的重量超過這個系統目前規模所能正當化的——見 [CLAUDE.md](../../../CLAUDE.md) 中關於這是現階段刻意限制的說明。

## 何時重新檢視

規模成長到需要一個語意不同的 broker，或認領本身成為一個被量測到的爭用點。

## 相關

- [ADR-0015](./0015-staleness-from-processing-started-at.md) — 這個保證的邊界，以及發現它的那個缺陷
- [ADR-0005](./0005-first-write-wins-idempotency.md) — 另一半：CAS 保護認領，`UNIQUE` 保護寫入
- [ADR-0017](./0017-bounded-recovery-scan.md) — 為什麼「輸家便宜」很重要
