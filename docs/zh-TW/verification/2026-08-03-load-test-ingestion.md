# 2026-08-03 — 攝入層壓測

[English](../../en/verification/2026-08-03-load-test-ingestion.md) | **繁體中文**

---

## 驗證的假設

攝入路徑的併發行為與失效模式：**CAS 認領在真實爭用下真的成立嗎？ODS 冪等性在 TOCTOU 競賽下真的成立嗎？**

## 環境

`scripts/load_test.py` 打真實 server 與真實 PostgreSQL。SQLAlchemy 池預設：`pool_size=5`、`max_overflow=10` → 15 個併發連線，`pool_timeout=30s`。

## 觀測

### 測試 1 — 1,000 筆不重複訂單，併發 50

全部成功，**7.9 秒**，零錯誤。

每個 `POST /orders` 只做一次快速 INSERT 並立即釋放連線（持有時間 < 10ms）。併發 50 遠在池的容量之內——不會排隊。

### 測試 2 — 1,000 筆不重複訂單，併發 500

P99 延遲 **約 14 秒**，**5 個 HTTP 500**。

500 個同時請求下，485 個要排隊等連線。任何超過 `pool_timeout=30s` 的請求都會拋 `QueuePool limit reached`。那 5 個失敗**在 INSERT 之前**就逾時了——沒有 Raw 記錄被建立，所以沒有東西是寫到一半的。

之後已處理：`SATimeoutError` 被捕捉並立即回 **503 Service Unavailable**，讓客戶端對著一個誠實的狀態碼重試。

### 測試 3 — 100 筆重複 `order_id`，併發 100

**寫入 100 筆 Raw、100 筆 ODS**——全部成功。

**這是被設計出來的行為，不是缺陷**：`raw.order_id` 有索引但沒有 unique（[ADR-0001](../adr/0001-raw-no-business-dedup.md)）。每一筆重複都是一次帶著自己 `raw_id` 的新攝入事件。**CAS 保護的是同一個 `raw_id` 不被處理兩次；它不是業務去重。**

### 測試 4 — 100 個 worker 競爭同一個 `raw_id`

`raw.status = processed`，**ODS COUNT = 1。**

`try_claim_raw` 送出 `UPDATE raw WHERE id=X AND status='pending'`。PostgreSQL 對這句 UPDATE 做列鎖——只有第一個 worker 拿到 `rowcount=1`；其餘 99 個拿到 `rowcount=0` 並立即返回。

### 測試 6 — 重複 `order_id`，兩種順序

| 場景 | 結果 |
|---|---|
| **循序** — 同一個 `order_id` 送兩次 | 第一筆寫入 ODS。第二筆在預檢命中、標記 `duplicate`，ODS 不再被寫 |
| **TOCTOU 競賽** — 兩個 worker 都通過預檢 | 第一個 commit ODS；第二個在 commit 時拿到 `IntegrityError`——**不重試**地被捕捉、標記 `duplicate` |

ODS 永遠**每個 `order_id` 恰好一筆**，而後續每一筆重複都到達 `duplicate` 終端狀態，給監控一個乾淨的訊號。

## 結論

CAS 與冪等性在真實爭用下都成立。測試 4 與測試 6 的競賽是 CI 重現不了的兩項——**mock 的資料庫沒有列鎖，也沒有唯一約束可以違反。**

**測試 3 刻意被收錄，即使它「輕鬆通過」**：它記錄了「重複 `order_id` 抵達 ODS」是設計，不是漏網。

## 這推翻了什麼

當時沒有。**但這套測試自己後來被推翻了兩次：**

- **測試 2（併發 500 的 5 筆失敗）已於 2026-09-02 失效**——見 [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md)。以相同參數複跑，1000 筆全過、連線池峰值 12。⚠️ **上面「485 個要排隊等連線」那段推論在今天的程式碼上不成立**：當時的壓力來自 `BackgroundTasks` 把同步的 `process_raw_event` 丟進 40 條 anyio threadpool、共用 API 那 15 條連線池；`8485f64` 把派工改成 Celery 之後，API 行程只剩一次 INSERT，且連線在派工前就歸還。
- **測試 5（SIGKILL）已於 2026-08-10 失效**——見 [2026-08-10-celery-sigkill-recovery](./2026-08-10-celery-sigkill-recovery.md)。

測試 1、3、4、6 仍然成立：那幾項驗的是 CAS 與冪等性，而那段程式碼沒有變。

## 相關

- [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) — 推翻本文測試 2
- [ADR-0004](../adr/0004-cas-claim-rowcount.md) · [ADR-0005](../adr/0005-first-write-wins-idempotency.md)
- [design/testing](../design/testing.md) — 為何這些是手動而不在 CI 裡
