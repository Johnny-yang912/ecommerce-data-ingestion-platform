# 2026-08-11 — 全 compose 重建與 v4 回流

[English](../../en/verification/2026-08-11-full-compose-rebuild-v4.md) | **繁體中文**

---

## 驗證的假設

環境完整搬進 compose，資料集從零重建。**整套系統能從無到有起來嗎？第二輪規則放寬的行為與第一輪相同嗎？**

## 環境

9 個容器——db / redis / api / worker / beat + 四個 Airflow 服務。資料集清空重建。3,015 列，265 筆在隔離區。2026-08-11。

## 觀測——基礎設施

| 項目 | 結果 |
|---|---|
| 從零跑 `alembic upgrade head` | **7 個 migration** 全部通過——一條長期存在的開發資料庫永遠不會演練到的路徑 |
| 服務健康 | 9 個容器全部健康 |
| Airflow → `api:8000` / `db:5432` | 兩者皆可觸及，且**讀的是同一個資料庫**（`ods=8`） |
| 全部清空後的 BQ 重建 | `create_dataset` / `create_table(exists_ok=True)` 把分區與 `require_partition_filter` 原封不動地重建回來——**零手動 DDL** |
| 主 DAG | **7/7 tasks 成功，端到端約 2.5 分鐘** |
| `source_freshness_watch` | 兩個 source 皆 **PASS**——從「預期會紅」翻轉成「預期會綠」 |

### 落地列數 gate，兩個方向

停掉 `worker`，送出 3 筆記錄：

| | ODS | exit code |
|---|---|---|
| 不加 `--require-landed-pct`（舊行為） | 0 列 | **0** ← 靜默成功，正是它必須防止的 |
| `--require-landed-pct 0.9` | 0 列 | **1** ← 被抓到 |

重啟 `worker` 之後，全部 13 筆 `pending` 被 `scan_and_dispatch` 重新派工——**自癒也一併驗證了。**

## 觀測——v4 回流

目標：`customer_name` 軟性上限 **100 → 150**。

| 步驟 | 結果 |
|---|---|
| Dry-run | `candidates=265 promoted=3 would_write=3` |
| Commit | `written=3`；`quality_events` = 3015 筆 `initial_evaluation@v3` + 3 筆 `promotion@v4` |
| **Bounded writeback** | ODS 指紋**前後完全相同**（3015 列，265 髒） |
| 冪等性 | 第二次執行：`promoted=0 written=0 unchanged=265` |
| 回流 | `int_orders` +3、隔離區 265→262、`fct_orders` +3、`promotions` 0→3 |
| 逐列檢查 | 3 筆全部顯示 `fct_orders=1 / quarantine=0` |
| 對照組 | `customer_name` 157/164/176/188/199 與 5 筆 `city` **全部維持隔離** |

> 對照組是**從同一個注入器裡自然形成的**——`_dirty_field_too_long` 把長度散布在 110–200 之間、且有一半機率打 `city`——不像 v3 需要另外準備一組。邊界也更緊：**146 會被 promote，157 不會。**

## 兩個被推翻的推論 ⭐

### ① `--expect-rule-version` 涵蓋的範圍比假設的少

在重建映像**之前**量測：`api`／`worker` 回報 `v3 {'customer_name': 100}`，而 Airflow 回報 `v4 {'customer_name': 150}`——**而 `--expect-rule-version v4` 通過了。**

那道守衛只比對**它自己行程內的**版本。它成立的前提是整個系統只有**單一的程式碼遞送機制**，而這個 compose 拓撲打破了那個前提：

```
api / worker / beat   程式碼【烤進映像】      需要 build
Airflow 容器          bind mount ./:/opt/project   立即生效
```

處置：[runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md) 的步驟 3。

### ② 候選來源的方向性從未被寫下來

那個 DAG 的檔頭只記了*「再評估寫入 PG；要回流進 Gold 還需要一次 extract」*。**反過來也成立**：候選是從 BQ 讀的，所以資料必須**先**上到 BQ。

第一次 dry-run 回傳 `candidates=26 / would_write=0`——**不是因為規則沒生效**，而是因為 BQ 還停在累積之前的狀態。**在不知道這件事的前提下，那個症狀與程式壞掉無法區分。**

處置：同一份 runbook 的步驟 4。

## 順帶量測到的

- **取消暫停一個 DAG 會立刻建立一次排程 run。** 於是 `staging.orders` 有 398 = 199×2 列，而 `stg_orders` 恰好 199 列——**一次意外的實地確認**：append-only 容忍度加上 `stg_` 去重確實如設計般運作。
- **Jinja 樣板錯誤只在執行時浮現。** DagBag 解析乾淨、`dags list` 正常、每個結構測試都綠——而 task 在 **0.16 秒**內失敗。踩到的三個變體（巢狀 `{{ }}`、f-string 把 `}}` 逃逸成 `}`、以及手動執行時 `data_interval_start` 不存在）**只能靠真的去 render 樣板才抓得到**，所以 `tests/test_dags.py` 加上了 render 測試。
- **cron 的 `data_interval_start` 是「上一個」觸發點。** 拿它當日期種子，會讓每天的第一個時段取到**昨天的**值，破壞「每天單一髒資料率」的不變式。已改用 `dag_run.run_after`。

## 相關

- [2026-08-05-proposal-b-v3](./2026-08-05-proposal-b-v3.md) — 第一輪
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md) — 這則記錄所產生的兩個警告
- [ADR-0032](../adr/0032-bounded-writeback.md)
