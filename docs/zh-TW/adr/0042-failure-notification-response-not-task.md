# ADR-0042：失敗通知寫「該做什麼」而非任務名；通道刻意留空

[English](../../en/adr/0042-failure-notification-response-not-task.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08-24 |
| **層** | 編排 |

---

## 背景

預設的失敗通知會說 `Task dbt_intermediate in DAG orders_analytics_daily failed`。早上七點收到它的人，仍然得自己推敲那是什麼意思、以及重不重要。

他們需要的資訊——**「分析管線壞了；明天早上的報表會停在昨天」**——是存在的，寫在每個 DAG 檔的 docstring 裡。**而處理事故的人不會去讀 docstring。**

第二個問題是通道。把 notifier 指向一個不存在的 Slack connection，產生的是：task 變紅 → callback 觸發 → callback 拋錯 → Airflow 記進 log → **沒有任何人收到東西**。

> **相信自己有告警、實際上沒有，遠比坦白地沒有告警危險。**

## 決策

**訊息陳述的是該做什麼。** 四個排程 DAG 各自帶一個 `on_failure_callback`，其文字說明「現在什麼壞了、它對下游代表什麼」——把 docstring 的內容，搬到它會被讀到的地方。

**掛在 task 層級，不是 DAG 層級。** 下游 `upstream_failed` 的 task 不會觸發 callback，所以一條斷掉的七任務鏈**恰好送出一則訊息**，而那則訊息點名的是真正壞掉的那個 task。

**傳輸預設是一行 log。** 真實通道離這裡只有一個 `NOTIFY_WEBHOOK_URL`。刻意不做一個指向不存在連線的 notifier。

**每則訊息都帶 `channel=`。** 看到 `channel=log` 就立刻知道沒有人被通知。**告警的缺席本身被回報了出來。**

**`_deliver()` 是唯一知道「怎麼送」的函式。** 其餘程式碼只知道「送什麼」。webhook payload 用 `{"text": ...}`——Slack Incoming Webhook 的形狀，多數服務也吃這個；換成 Discord（`content`）或 ntfy（純文字 body）只改那一行。

**用環境變數，不用 Airflow Connection。** Connection 能讓 Airflow 在 log 裡遮蔽祕密，那確實比較好——但它把這個模組綁死在特定 provider 的 notifier 上，**而可替換性正是這個接縫的全部重點**。緩解措施是：**URL 永遠不進 log**，連失敗時也只記 status code。有了那一條，遮蔽的價值趨近於零。

## ⚠️ 涵蓋範圍：只有「跑了而且失敗」

`on_failure_callback` 需要一次真的發生過的 task run。因此有三件事對它是不可見的：

| 未涵蓋 | 為何 |
|---|---|
| **該跑卻沒跑** | 沒有 run 就沒有 failure。Airflow 3 移除了 SLA（`sla` 參數還留在 `BaseOperator` 簽章裡——別依賴它） |
| **機器關機／斷網** | callback 與它所監看的東西住在同一台機器上 |
| **`warn` 等級的結果** | `dbt source freshness` 的 warn 是 exit 0；task 是綠的 |

這三者都需要雲端側的 absent 告警，而那被暫緩——見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。**傳輸接縫已就緒；缺的是偵測器。**

## 後果

**一個紅燈現在自帶它的解讀。** 不必去對照 docstring 才知道重不重要。

**一次事故產生一則訊息**，而不是失敗鏈上每個 task 各一則。

**缺口被明說而非暗示。** 訊息裡的 `channel=log`，加上上面那張涵蓋範圍表，讓任何人都不會把這個誤認為可運作的告警。

**⚠️ 這個模組會被 DAG 檔 import**，所以它受 ADR-0036 約束：它不 import 任何專案模組。它的 `_` 前綴對 `tests/test_dags.py` 的檔案數斷言而言是承重的。

## 考慮過的替代方案

**現在就把 notifier 指向 Slack。** 會看起來很完整而什麼都不送達，**那正是這個決策所針對的失效模式**。

**DAG 層級的 `on_failure_callback`。** 每個 DAG 一則而非每個 task 一則——但它不會點名壞掉的那個 task，而且訊息得對這個 DAG 所有可能的失敗都通用。

**把「該做什麼」的文字放在告警工具裡。** 把說明與產生失敗的程式碼拆開，於是它們會漂移。**留在 DAG 檔裡，代表「task 做什麼」的變更與「它的失敗代表什麼」的變更會在同一個 diff 裡。**

## 相關

- [ADR-0036](./0036-dag-no-toplevel-import.md) — 這個模組所受的紀律
- [ADR-0039](./0039-observation-signals-own-dag.md) — 為何每個紅燈已經只代表一件事
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — 那三個未涵蓋的情況，以及真實系統的做法
