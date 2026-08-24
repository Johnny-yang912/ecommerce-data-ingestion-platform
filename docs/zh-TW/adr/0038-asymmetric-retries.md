# ADR-0038：刻意不對稱的重試——extract = 2、dbt = 0

[English](../../en/adr/0038-asymmetric-retries.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

整個 DAG 套用統一的重試政策看起來整齊，**而在這裡是錯的**，因為兩類 task 失敗的理由是相反的。

## 決策

| Task | `retries` | 理由 |
|---|---|---|
| `extract_*` | **2**，指數退避 | 失敗大多是**暫時性的**——PostgreSQL 連線、BigQuery 5xx、`rateLimitExceeded` |
| `dbt_*` | **0** | 失敗大多是**確定性的**——SQL 寫錯、測試變紅、Hard Gate 觸發 |

對確定性失敗做重試，等於重跑一件保證會再次失敗的事。它花時間、在 log 裡製造重複雜訊，而且——最糟的——**延後了人去看它的那一刻**。

這與 NUL 毒藥丸（ADR-0006）是同一條原則，只是在不同的層陳述：

> **把確定性錯誤當成暫時性錯誤重試，正是製造毒藥丸的方式。** 那裡的修法是 `except ValueError` 快速失敗；這裡的修法是 `retries=0`。

**BigQuery 真正暫時性的錯誤改由 adapter 層處理**，靠 `profiles.yml` 的 `job_retries: 1`。**那比 Airflow 的 task 重試精確得多**：它重試的是**失敗的那個 BigQuery job**，而 Airflow 的重試會重跑整個 `dbt build`——包含每一個已經成功的 model。

> **在知道「實際是什麼失敗了」的那一層重試。**

## 後果

**一個紅色的 `dbt_*` task 立即具有意義。** 它沒有被重試過，所以它就是字面意思：有東西確定性地壞了，去看它。

**Extract 的暫時性失敗自癒而不吵醒任何人**，而且它與逐表 watermark 相互組合：失敗的 extract 不推進它的 watermark，所以就算重試兩次都失敗，隔天的 run 用 `>=` 重新選取也會恢復（ADR-0023、ADR-0024）。

**Hard Gate 的攔截不會被掩蓋。** 若閘門觸發，run 就停下並保持停止——重試它會對同一批資料重跑閘門並以完全相同的方式失敗，同時讓這次事故看起來像是間歇性的。

**代價是：一次真正暫時性的 dbt 失敗——build 途中 BigQuery 中斷——需要手動重跑。** 接受：那類失敗罕見，而 adapter 層的 `job_retries` 已經涵蓋它最常見的形式。

## 考慮過的替代方案

**全部統一 `retries=2`。** 把每一次確定性失敗變成三次一模一樣的失敗，log 雜訊三倍，診斷延後。

**全部統一 `retries=0`。** 會為了一次 30 秒後就會自己好的 PostgreSQL 瞬斷而呼叫一個人。

**用 Airflow 層重試處理 BigQuery 錯誤，而不用 `job_retries`。** 為了從一個失敗的 job 恢復而重跑整個 `dbt build`。**同樣的結果，嚴格更差。**

## 相關

- [ADR-0006](./0006-nul-byte-fast-fail.md) — 攝入層的同一條原則
- [ADR-0024](./0024-per-table-load-job-gate.md) — 這條所運作的重試粒度
- [ADR-0040](./0040-layered-dbt-execution.md) — 為何重跑整個 `dbt build` 很貴
