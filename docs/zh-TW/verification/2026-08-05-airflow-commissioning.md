# 2026-08-05 — Airflow 啟用驗收，以及 `--indirect-selection=buildable`

[English](../../en/verification/2026-08-05-airflow-commissioning.md) | **繁體中文**

---

## 驗證的假設

同一次工作階段裡的兩件事：**Airflow 映像端到端真的能運作嗎**，以及 **`--indirect-selection=buildable` 真的把跨層 singular test 放在推理所說的位置嗎？**

## 第一部分——容器實地執行

| 項目 | 結果 |
|---|---|
| 映像 build | 成功（`apache/airflow:3.0.0-python3.12` + 兩個隔離 venv） |
| 服務 | `airflow-db` / `init` / `apiserver` / `scheduler` / `dag-processor` 全部健康 |
| **DAG 解析** | 3 個全部載入；`list-import-errors` → **No data found** |
| analytics venv | `sqlalchemy` / `google.cloud.bigquery` / `structlog` / `pydantic` 皆可 import |
| dbt venv | dbt-core **1.11.12**、dbt-bigquery **1.11.3** |
| env_var profile | `dbt debug` → `Connection test: [OK connection ok]` |
| `source_freshness_watch` | 完整 DAG run **成功**，兩個 source 皆 PASS |
| `dbt_intermediate` | 容器內 `airflow tasks test` → PASS=27 WARN=1 ERROR=0 |
| `extract_orders` | **FAILED**：`OperationalError: could not translate host name` |
| UI | `http://localhost:8080` HTTP 200 |

### ⭐ 「不在 top-level import」的紀律，對著一個真實的 dag-processor 被驗證了

**那個容器沒有可用的 `DB_URL`**——預設值指向一個沒有啟動的 `db` 服務——而**三個 DAG 仍然全部解析成功、零 import error。**

如果 DAG 檔帶著 top-level 的 `from config import settings`，當下畫面會顯示的是：

> **三個 DAG 全部從 UI 消失**——不是三個紅色 task，而是**什麼都沒有**。

那正是這條紀律存在要防止的失效，**而這次執行是最接近「親眼看見它沒有發生」的一次。** [ADR-0036](../adr/0036-dag-no-toplevel-import.md)

### Freshness 的語意順帶被確認

在一次資料載入 15 分鐘後執行，兩個 source 皆 **PASS**。*「紅燈代表你最近沒餵它，不代表管線壞了」*這個主張不再只是論證：**餵它，它就會綠。**

### 唯一的失敗

`extract_orders` 因主機名解析失敗——當時業務 DB 跑在主機上而 Airflow 跑在容器裡。那個配置還產生了一個更隱微的危害；見 [2026-08-raw-id-collision-two-ods](./2026-08-raw-id-collision-two-ods.md)。已由完整搬進 compose 解決（[2026-08-11-full-compose-rebuild-v4](./2026-08-11-full-compose-rebuild-v4.md)）。

---

## 第二部分——`--indirect-selection=buildable`

分層執行那個決策原本**只能被推理**。三種模式的差異描述得出來，但沒有一個實例。

```
dbt build --select path:models/staging       22 個節點，全部是 stg_ 測試
                                             ← assert_orders_split_is_partition 不在其中
dbt build --select path:models/intermediate  28 個中的第 13 個 PASS assert_orders_split_is_partition
dbt build --select path:models/marts         assert_fct_orders_complete_projection    PASS
                                             assert_fct_orders_rollup_matches_items   PASS
```

跨層 singular test **恰好落在它們所有輸入都是新鮮的那一層**——不會在 staging 提早觸發（那時 `int_` 還是上一份表，它們會假性變紅），也不會像 `cautious` 那樣完全被跳過。

**推理成立。**

### 為何收尾的完整 `dbt test` 仍然保留

它的價值**不是**「抓到被跳過的東西」——這次什麼都沒被跳過。而是**它是唯一一個會在未來版本改變 selector 語意時注意到的東西。**

那是另一份職責，**也是為何這次量測顯示沒有東西被遺漏之後，收尾那一輪並沒有被移除。**

## 相關

- [ADR-0040](../adr/0040-layered-dbt-execution.md) — 這裡驗證的那個決策
- [ADR-0035](../adr/0035-two-venvs-dependency-isolation.md) — 那兩個 venv
- [design/orchestration](../design/orchestration.md)
