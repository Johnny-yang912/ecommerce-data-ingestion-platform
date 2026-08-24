# ADR-0011：不用 result backend——`raw.status` 是唯一真相

[English](../../en/adr/0011-no-result-backend.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 任務佇列 |

---

## 背景

Celery 提供 result backend：一個儲存任務狀態與回傳值的地方，讓呼叫者可以問「任務 X 完成了嗎？」。

**這個系統已經能回答那個問題。** 每一筆記錄的下場都住在 `raw.status`——`pending`、`processing`、`processed`、`error`、`duplicate`——存在 PostgreSQL 裡，與產生它的 ODS 寫入具有交易一致性，而且已經由 `GET /raw/{raw_id}` 對外暴露。

## 決策

`task_ignore_result = True`。不設定任何 result backend。

## 後果

**一份真相，不是兩份。** Redis 的結果儲存會對同一個任務持有第二種意見，而兩者可以不一致：DB commit 成功但結果寫入失敗，或者結果過期了而那一列永遠存在。當兩者不一致時，呼叫者沒有任何有原則的方式決定哪個是對的。

**有權威的那個答案具有交易性。** `raw.status` 在成功路徑上與 ODS 那一列在同一次 commit 中更新。獨立的 result backend 是在不同的儲存、不同的時刻更新的，所以它與它所描述的東西之間，永遠不可能超過最終一致。

**沒有過期策略要設計。** result backend 需要一份——否則結果會不斷累積。`raw` 的列是業務資料，有自己的保留期敘事，而那本來就必須決定。

**代價：沒有 `AsyncResult`、沒有 `.get()`、沒有以結果為鍵的 chord 或 chain。** 這個系統裡沒有東西需要它們——管線的協調住在 Airflow（ADR-0035），不在 Celery 原語裡。如果未來有工作流程需要 Celery 層級的任務組合，這個決策就需要重新打開。

## 考慮過的替代方案

**為了可觀測性而啟用 result backend。** 很誘人，而答案是改用追蹤：`api` → Celery → `worker` 的 span 鏈（ADR-0050）能顯示一個任務發生了什麼，而不製造第二個狀態儲存。

**把 result backend 當作唯一真相並拿掉 `raw.status`。** 那會把業務狀態機放進一個有過期策略的暫時性快取裡，而且在產生它的交易之外。**狀態機是業務資料，不是任務中繼資料。**

## 何時重新檢視

當某個工作流程需要 Celery 層級的組合（chord、會傳遞值的 chain），而不是今天使用的「DB 狀態機協調」時。

## 相關

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — 這裡所設定的佇列
- [ADR-0003](./0003-duplicate-terminal-status.md) — 作為真相的那個狀態機
- [ADR-0008](./0008-config-boundary.md) — 同一套「不要製造第二份真相」的推理，用在設定上
