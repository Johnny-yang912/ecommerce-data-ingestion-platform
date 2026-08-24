# ADR-0035：依賴隔離——兩個 venv，什麼都不裝進 Airflow 本身

[English](../../en/adr/0035-two-venvs-dependency-isolation.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

Airflow 與 dbt-bigquery 都重度依賴 `google-cloud-*`、`protobuf` 與 `jinja2`。把它們裝進同一個環境是版本衝突的經典來源，而且它製造一個常駐危害：**每一次 Airflow 升級都有弄壞 dbt 的風險**，反之亦然，而且壞掉會在 DAG 解析時而非安裝時才浮現。

| 選項 | 取捨 |
|---|---|
| `pip install` 到同一個環境 | 最簡單；衝突風險高 |
| **獨立 venv + `BashOperator`** | 乾淨隔離、零額外基礎設施、符合官方建議 |
| 自帶映像的 `DockerOperator` | 最乾淨、與生產最一致；需要掛載 `docker.sock` |

## 決策

Airflow 映像內兩個 virtualenv，而且**專案相關的東西一個都不裝進 Airflow 自己的環境**：

```
/home/airflow/venvs/analytics   ← requirements-analytics.txt
/home/airflow/venvs/dbt         ← dbt-core / dbt-bigquery 1.11
```

Task 是 `BashOperator`，呼叫對應的直譯器。

這也是 `requirements-analytics.txt` 存在為獨立檔案的原因：Airflow 容器必須能夠**執行抽取腳本**，而不必連帶拉進 pytest 與其餘的開發工具鏈。

**不用 Cosmos。** model 層級的 task 粒度會改善可觀測性，而對一個 13 個 model 的專案而言，那個好處與代價不成比例——**代價是一個必須同時追蹤 dbt 與 Airflow 版本的依賴，也就是這個決策存在要避免的那種耦合。**

## 後果

**Airflow 升級與 dbt 升級互相獨立。** 兩個 venv 各自解析依賴；誰都無法約束誰。

**⚠️ 映像的依賴不隨 bind mount 更新。** DAG **檔案**是掛載進去的、下次解析就會生效，但 venv 住在映像裡。改 `requirements-analytics.txt` 需要**重新 build**——重啟只會啟動舊容器，既不 build 也不 recreate。

這實際咬過人：OTel 上線時 `process.py` 多了一個 `telemetry` import，而一支跑在 analytics venv 裡的唯讀探針死在 `ModuleNotFoundError`，因為映像還沒重建（ADR-0039）。

**代價是映像大小與 build 時間**——兩棵依賴樹而非一棵——以及新增依賴時多一件要記得的事：**裝進哪一個 venv**。

## 考慮過的替代方案

**共用一個環境。** 在第一次衝突之前最簡單，而衝突發生時的解法是釘住某個東西，然後弄壞另一個東西。

**`DockerOperator`。** 確實更乾淨、也更接近生產形狀，代價是把 `docker.sock` 掛進 Airflow 容器——對單機環境而言那是一次有意義的權限提升。

**Cosmos。** UI 裡有逐 model 的 task 與 lineage，代價是一個同時與兩個升級週期耦合的依賴。若 model 數成長到「哪個 model 壞了」不再能從 log 一眼看出時，值得重新檢視。

## 相關

- [ADR-0036](./0036-dag-no-toplevel-import.md) — 讓 DAG 解析保持零依賴的另一半
- [ADR-0040](./0040-layered-dbt-execution.md) — dbt 如何從這些 venv 被呼叫
- [ADR-0039](./0039-observation-signals-own-dag.md) — 顯示出「必須重建」的那次事故
