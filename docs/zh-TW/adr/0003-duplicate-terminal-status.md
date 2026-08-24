# ADR-0003：`duplicate` 是 Raw 的終端狀態，不是拒絕

[English](../../en/adr/0003-duplicate-terminal-status.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-05 |
| **層** | 攝入 — Raw 狀態機 |

---

## 背景

ADR-0001 讓重複的 `order_id` 提交進入 Raw。它們因此會走到處理階段，而處理階段必須把它們放在某個地方。有三個選項：靜默丟棄、標記為 `error`、或給它們一個自己的狀態。

`error` 是最誘人的預設，而它錯在一個具體的地方：**`error` 與 `duplicate` 要求不同的回應。** `error` 代表系統沒能處理它本該處理的東西——有人得去看。`duplicate` 代表系統完全照設計運作，而上游把同一筆訂單送了兩次——沒有人需要做任何事，除非**頻率**改變了。把兩者合在一起，會讓 `error` 的計數作為告警訊號變得沒有用。

## 決策

`duplicate` 是 Raw 狀態機中的終端狀態之一：

```
pending → processing → processed | error | duplicate
```

它有兩條到達路徑，都在 `process_raw_event` 裡：

1. **預檢命中**——commit 之前 `order_id` 已存在於 ODS。
2. **commit 時的 `IntegrityError`**——兩個 worker 都通過了預檢，第二個輸掉競賽（見 ADR-0005）。

兩者都會把「哪個 `raw_id` 贏了」記進 `error_message`。

## 後果

**監控得到一個乾淨、可獨立計數的訊號。** `duplicate` 上升指向上游或網路；`error` 上升指向這個系統。

**`duplicate` 可以被重放。** `POST /process_raw/{id}?force=true` 只接受 `error` 與 `duplicate`——這兩個終端狀態是人類可能合理想要再試一次的地方。`processed` 不可重放，這防止一次失手的 force 覆蓋掉好資料。

**代價是這個訊號可能被系統自己的行為污染。** 一筆 `duplicate` 不總是代表上游送了兩次——它也可能代表**這個系統**在兩個 worker 上處理了同一個 `raw_id`。這確實發生過：一個逾時判定的缺陷在處理進行中把記錄從 `processing` 退回 `pending`，讓第二個 worker 得以認領，結果一筆其實已經成功的記錄頂著 `duplicate`（ADR-0015）。

**這個訊號的價值，正是那個缺陷值得修的原因。** 如果 `duplicate` 當初被併進 `error`，那次污染會是不可見的。

## 考慮過的替代方案

**靜默丟棄。** 會讓 Raw 那一列停在非終端狀態，或需要一個隱藏的第四種結果。不論哪一種，「我們收到幾筆重複訂單」這個計數都變得取不到。

**沿用 `error`。** 破壞上述的告警區分。設一個獨立終端狀態的全部理由，就是這兩種情況要求不同的人類回應。

**在寫入 Raw 之前於 API 層拒絕。** 那是 ADR-0001 的被否決方案，理由住在上一層。

## 相關

- [ADR-0001](./0001-raw-no-business-dedup.md) — 重複為何會到達這一層
- [ADR-0005](./0005-first-write-wins-idempotency.md) — 進入此狀態的兩條路徑
- [ADR-0015](./0015-staleness-from-processing-started-at.md) — 污染這個訊號的缺陷，以及那為何重要
