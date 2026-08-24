# 架構

[English](../en/ARCHITECTURE.md) | **繁體中文**

系統如何組成。每個決策**為何如此**住在 [ADR](./adr/README.md)；**做了什麼**住在 [STATUS](./STATUS.md)。

---

## 1. 這個系統是什麼

一條電商訂單攝入與分析管線，圍繞**資料生命週期管理**組織。不可信的入站資料透過分層品質契約逐步轉為可信的分析資料，而每一次品質判斷的演進都保持可稽核。

它是資料工程優先、後端工程其次：

- 攝入層的容錯確保**資料進得來**。
- 分層品質契約確保**資料流得正確**。
- 規則版本化與 append-only 事件日誌確保**品質判斷的演進永遠可稽核**。

---

## 2. 端到端資料流

```
POST /orders                                    ← 需要 X-API-Key
    ↓
[Raw]  逐字保留、不可變                                    status: pending
    ↓  Celery 派工（有斷路器；失敗 → 恢復掃描）
[Worker: process_raw_event]
    ├── try_claim_raw()        ← 原子 UPDATE，CAS 認領
    ├── JSON 解析
    ├── ODSOrder.from_nested() ← 攤平巢狀 payload
    ├── clean_order()          ← 格式正規化 + 業務規則驗證
    ├── first-write-wins 冪等檢查
    └── [ODS] + [quality_events]   ← 不可變錨點 + 品質事件日誌
    ↓  Airflow，每日台北時間 22:30
[BigQuery staging]  orders + quality_events，watermark 增量
    ↓
dbt stg_*     1:1 鏡像、去重、Hard Gate            ← Silver 入口，保留所有列
    ↓
─────────────────── 阻斷發生在這裡 ───────────────────
dbt int_*     依有效品質狀態的 Row Filter           ← Gold 入口
    ├── 有效乾淨  → int_orders → int_order_items
    └── 非乾淨    → int_orders_quarantine
    ↓
dbt dim_*/fct_*   Kimball 星型結構                  ← Gold
    ↓
dbt rpt_*         固定粒度的預先聚合
```

---

## 3. 分層與責任邊界

| 層 | 職責 | 可變 | 詳見 |
|---|---|---|---|
| **Raw**（PostgreSQL） | 逐字保存每一個入站請求。不做任何品質假設 | 否 | [ingestion](./design/ingestion.md) |
| **ODS**（PostgreSQL） | 格式正規化 + 業務規則驗證。保留一切，含髒資料 | 否 | [ingestion](./design/ingestion.md)、[data-quality](./design/data-quality.md) |
| **任務佇列** | Raw 與 ODS 之間的持久化派工，降級有界 | — | [queue](./design/queue.md) |
| **staging**（BigQuery） | ODS 的 1:1 落地。不清洗、不改名、不轉型 | append-only | [cloud-layer](./design/cloud-layer.md) |
| **`stg_`** | 型別對齊、欄位改名、去重回 ODS 粒度 | 重建 | [transformation](./design/transformation.md) |
| **`int_`** | 跨表 join、衍生欄位，以及**阻斷點** | 重建 | [transformation](./design/transformation.md) |
| **`dim_`／`fct_`** | 供彈性分析查詢的星型結構 | 重建 | [transformation](./design/transformation.md) |
| **`rpt_`** | 供 BI 的固定粒度預先聚合 | 重建 | [transformation](./design/transformation.md) |
| **編排** | 排程，以及觀察訊號 | — | [orchestration](./design/orchestration.md) |
| **可觀測性** | Traces + 營運指標 | — | [liveness-alerting](./design/liveness-alerting.md) |

---

## 4. 分層品質契約

品質責任**隨下游逐步收緊**。ODS 是保留一切的不可變錨點；阻斷只發生一次，在 `int_`。

```
Raw            無品質要求                              保留髒資料
ODS            標記，絕不拒絕                          保留髒資料
stg_           與 ODS 相同                             保留髒資料
─────────────────── 阻斷 ───────────────────
int_           只有有效乾淨的列通過                     髒的 → int_orders_quarantine
dim_/fct_      最乾淨的一層                            不存在髒資料
rpt_           與 Gold 相同                            不存在髒資料
```

兩個機制運作在不同粒度：

- **Hard Gate**（run 層級，掛在 `stg_`）——*source 是不是整個壞了？* 擋下整個 run。[ADR-0028](./adr/0028-hard-gate-per-batch-scope.md)
- **Row Filter**（逐筆層級，在 `int_` 內）——*這一列能用嗎？* 導向隔離區。[ADR-0029](./adr/0029-effective-quality-state.md)

---

## 5. 為何阻斷在 `int_` 而不更早

ODS 是**不可變錨點**：唯一一個「每一筆被接受的訂單都恰好存在一次」的地方，不論髒或乾淨。那個性質讓三件事成為可能：

1. 品質指標有真實的**分母**。
2. 規則變更可以在不重新攝入的前提下**回溯套用**——整套再評估機制的基礎。
3. ODS 與倉庫之間的分歧是**可解釋的**，而非神祕的。

`stg_` 是 1:1 鏡像，在那裡過濾會破壞鏡像性質所提供的對帳。`int_` 是第一個已經在做語意工作的層，而「這一列能不能用」是一個語意判斷。

完整推理：[ADR-0002](./adr/0002-has-clean-error-non-blocking.md)、[ADR-0027](./adr/0027-blocking-at-int-layer.md)。

---

## 6. 為何分析層是批次而非串流

下游消費者是 BI，T+1 更新已經足夠。更具決定性的是：**批次才讓窗口式品質控管成為可能**——Hard Gate 斷言的是一個批次的錯誤率，而串流沒有批次邊界可以界定它。

批次也讓失敗可重跑——失敗時 watermark 不推進，下一輪重新選取同一個切片。[ADR-0019](./adr/0019-batch-load-not-streaming.md)

---

## 7. 部署拓撲

兩份 compose 檔，**疊加成同一個 project**，好讓 DAG 能以主機名 `db` 觸及業務資料庫：

```
docker-compose.yml                       docker-compose.airflow.yml（overlay）
├── db        postgres:16                ├── airflow-db    metadata，獨立實例
├── redis     7-alpine, db0=broker       ├── airflow-apiserver
│                        db1=限流         ├── airflow-scheduler
├── migrate   一次性，alembic             ├── airflow-dag-processor
├── api       4 個 uvicorn worker        └── 映像內兩個 venv：
├── worker    Celery，4 prefork               /venvs/analytics  /venvs/dbt
├── beat      Celery Beat —— 單例
└── otel-collector
```

啟動順序由 healthcheck 與 `service_completed_successfully` 把關：`db` + `redis` → `migrate` → `api` / `worker` / `beat`。

三個值得知道的行程層級限制：

- **`beat` 絕不可被 `--scale`**——兩個 beat 會派出重複的掃描。[ADR-0016](./adr/0016-recovery-scan-in-beat.md)
- **Airflow 的 metadata DB 是獨立實例**，還原其中一個不會連帶回滾另一個。
- **Airflow 的兩個 venv 不隨 bind mount 更新**——改 `requirements-analytics.txt` 需要重新 build。[ADR-0035](./adr/0035-two-venvs-dependency-isolation.md)

---

## 8. 接下來讀哪裡

| 你想知道 | 讀 |
|---|---|
| 一筆訂單怎麼進來，失敗時怎麼辦 | [ingestion](./design/ingestion.md) |
| broker 掛掉時派工如何降級 | [queue](./design/queue.md) |
| ODS 如何抵達 BigQuery，schema 變更如何被吸收 | [cloud-layer](./design/cloud-layer.md) |
| 品質如何被判定，判定日後如何改變 | [data-quality](./design/data-quality.md) |
| dbt 各層如何建置與測試 | [transformation](./design/transformation.md) |
| 一切如何排程，每個紅燈代表什麼 | [orchestration](./design/orchestration.md) |
| CI 涵蓋什麼、對什麼是盲的 | [testing](./design/testing.md) |
| 每個決策為何如此 | [ADR](./adr/README.md) |
| 什麼沒做，以及為什麼 | [STATUS](./STATUS.md) · [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
