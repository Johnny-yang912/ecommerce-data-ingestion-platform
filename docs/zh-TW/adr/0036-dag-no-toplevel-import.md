# ADR-0036：DAG 檔不得在 top-level import 專案模組

[English](../../en/adr/0036-dag-no-toplevel-import.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

`config.py` 在 import 時就實例化 `Settings`，而 `db_url` 是必填的——缺值立即 raise（ADR-0008）。那個 fail-fast 行為對 API 而言是正確的。**在 DAG 檔裡它是危險的。**

dag-processor 每隔幾十秒就重新解析每一個 DAG 檔。如果 DAG 檔在 top-level import 專案模組，而解析行程缺 `DB_URL`，結果**不是一個失敗的 task**：

> **整個 DAG 從 UI 上消失。**

**沒有紅燈可以看。** 一個不在那裡的 DAG 不會失敗、不會告警，也不會被任何沒在專門找它的人注意到。**那嚴格比失敗更糟。**

Airflow 3 讓這件事更尖銳而非更緩和：DAG 解析跑在一個**獨立的 dag-processor 行程**裡，它的環境與 task 執行不同。對 task 存在的變數，不必然對解析存在。

## 決策

**DAG 檔在 top-level 不 import 任何專案模組。** 每個 task 都是 `BashOperator`，把所有 import 推到 task 執行時——在那裡，缺少環境變數會產生一個帶 traceback 的失敗 task，**而那是一個看得見的東西**。

## 後果

**設定問題變成一個紅色 task，而不是一個消失的 DAG。** 失效留在失效回報系統的內部。

**DAG 結構變成可被 CI 測試。** `tests/test_dags.py` 解析 DagBag 時**完全不需要資料庫、也不需要任何專案環境變數**——52 個測試，跑在專屬 workflow 裡。**那只有在 DAG 檔零依賴時才可能。**

那個獨立 workflow 本身也是刻意的：Airflow 的安裝很重、會釘住許多套件版本，把它併進主測試 job 會摧毀主 job「mock DB、幾秒跑完」的性質。

**代價是 DAG 檔無法與專案共用 Python 輔助函式。** DAG 需要的任何東西，都必須以命令列參數傳入，或在 task 內部從環境讀取。

**有一個例外，而它恰好證明了規則。** `orchestration/dags/_notify.py` 會被 DAG 檔 import，而它受同一條紀律約束——它不 import 任何專案模組。它的 `_` 前綴也是承重的：`tests/test_dags.py` 以 `glob("*.py")` 排除底線開頭的檔案，再斷言「檔案數 ≤ DAG 數」。沒有前綴的話，這個不產出 DAG 的模組會讓那條斷言變紅。

## 考慮過的替代方案

**給 dag-processor 一個 `DB_URL`。** 今天能work，而它是**隱藏危害而非移除危害**——那個 DAG 仍然距離「消失」只有一次環境變更的距離，而且發生在一個沒有人會去想它環境的行程裡。

**讓 `Settings` 變成 lazy。** 會移除 ADR-0008 存在的那個 fail-fast 性質，把一次大聲的啟動失敗換成一次請求中途的遲來失敗。

**`PythonOperator` 搭配函式內部的 import。** 達成同樣的延後，而且更脆弱——紀律是不可見的，一次把 import 提到檔案頂端的重構就會靜默地把危害放回來。**`BashOperator` 讓它在結構上不可能發生。**

## 相關

- [ADR-0008](./0008-config-boundary.md) — 讓這件事成為必要的那個 fail-fast 行為
- [ADR-0035](./0035-two-venvs-dependency-isolation.md) — 解析期隔離的另一半
- [ADR-0042](./0042-failure-notification-response-not-task.md) — DAG 檔唯一會 import 的那個模組
