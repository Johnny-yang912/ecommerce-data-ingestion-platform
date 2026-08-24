# ADR-0039：觀察訊號各自獨立成一個 DAG

[English](../../en/adr/0039-observation-signals-own-dag.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

`dbt source freshness` 最初是被加成主管線 DAG 裡的一個側通道 task，並設定成它的失敗不阻斷下游。**那並不足夠**，理由是 Airflow 的一個性質，而非這個檢查的性質：

> **一次 DAG run 的狀態是它所有 task 的聚合。** 一個預期會紅的葉節點 task，會讓 `orders_analytics_daily` 永久處於失敗——於是「主管線成功率」變得毫無價值，而真正的失效被埋在每天都紅的雜訊底下。

所以這條原則必須再往前一步。

## 決策

**一個觀察訊號既沒有阻斷下游的權限，也沒有污染另一個 DAG 成功率的權限。** 各自獨立成一個 DAG，好讓每個 DAG 的紅燈只代表一件事：

| DAG | 紅燈代表 | 該去哪裡看 |
|---|---|---|
| `seed_demo_daily` | 什麼都進不來 | API、seeding 腳本 |
| `raw_pending_watch` | 資料進得了 Raw，但沒人認領 | redis／worker／beat |
| `orders_analytics_daily` | 管線壞了 | extract 或 dbt |
| `source_freshness_watch` | staging 沒有被往前推 | watermark 與 extract |

由 `tests/test_dags.py::TestFreshnessIsolation` 把關：若任何一個產出真實輸出的 DAG 撿走了 `dbt source freshness`，這個測試就變紅。

## 三條時間線，各覆蓋一跳

這些訊號刻意不重疊——合併之後，一個紅燈會代表兩段管線：

| 時間線 | 哪一跳 | 誰在看 |
|---|---|---|
| `raw.received_at` | 上游 + API：訂單進得來嗎？ | OTel（absent 告警未寫——見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)） |
| `raw.received_at` → `ods.received_at` | 派工：worker 認領得到嗎？ | `raw_pending_watch` |
| BQ staging 裡的 `ods.received_at` | extract：有抵達倉庫嗎？ | `source_freshness_watch` |

**`source_freshness_watch` 排在台北時間 08:00，因為它是後備。** 如果 extract 回報成功卻什麼都沒搬，Hard Gate 判的是昨天的分區、會通過，`dbt test` 也是綠的——**而這是唯一一個會在有人 09:00 打開報表之前出聲的東西。**

它的 26h／50h 門檻是 `24 + 2` 與 `48 + 2`：一個**載入週期**加兩小時寬限。來源是**載入節奏**，不是攝入節奏——staging 每天被推一次，所以資料按設計最舊可達 24 小時，門檻低於 24h 就會在每次 extract 之前變紅。在 08:00 取樣時，健康值約 13h、錯過一個週期是 37h，所以 26h 落在中間，兩側各留約 10 小時餘裕。

## `raw_pending_watch`：那支不可以 import 寫入路徑的探針

派工探針從恢復路徑自己的設定推導告警門檻，而不寫死一個數字。它原本從 `process.py` import 那些常數——因而繼承了**整條寫入路徑的依賴樹**。

OTel 上線時 `process.py` 多了 `from telemetry import ...`，於是探針在**檢查任何東西之前**就死在 `ModuleNotFoundError: No module named 'opentelemetry'`，因為 analytics venv 的映像還沒重建（ADR-0035）。

> **單一個共用常數，把一支唯讀探針耦合到了它根本不執行的程式碼路徑上。**

修法是把常數抽進 `recovery_policy.py`——一個零第三方依賴的模組——並由 `tests/test_script_deps.py` 釘住，讓它不必倚賴任何人記得。

**⚠️ 一個很容易搞錯的判準。** 「有 Raw 卻沒有對應的 ODS」**不能**作為故障的定義：`duplicate` 與 `error` 是正確的終端狀態、本來就不產生 ODS 列，所以那個定義會對每一筆重複訂單告警。**`pending` 的年齡才是乾淨的訊號。**

## 後果

**每一個紅燈都是診斷性的。** 哪個 DAG 紅了，就告訴你該看哪一跳，在你讀第一行 log 之前。

**一個訊號不會被吵鬧的鄰居消音，也不會消音別人。**

**代價是更多要運行與查看的 DAG**——六個而非兩個——以及一份紀律：每個新訊號在被放置之前都要問「它覆蓋的是哪一跳？」。

## 考慮過的替代方案

**側通道 task 搭配 `trigger_rule` 技巧。** 最初的嘗試。下游確實不被阻斷，而 DAG 的聚合狀態照樣被毀掉。

**一個包含所有檢查的監控 DAG。** 精確地把問題放回來：一個紅燈代表四種不同的意思。

**改由指標後端告警，而非由 DAG 告警。** **那是長期上正確的答案**，而它需要的告警規則在沒有真實流量時寫不出有意義的版本——見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

## 相關

- [ADR-0020](./0020-partition-on-received-at.md) — 為何每條時間線恰好覆蓋一跳
- [ADR-0035](./0035-two-venvs-dependency-isolation.md) — 探針事故背後的「必須重建」
- [ADR-0042](./0042-failure-notification-response-not-task.md) — 當其中一個變紅時會發生什麼
