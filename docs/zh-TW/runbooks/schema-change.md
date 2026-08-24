# Runbook：對 ODS 加欄或刪欄

[English](../../en/runbooks/schema-change.md) | **繁體中文**

---

## 適用範圍

這是給工程師**刻意**經由 Alembic 改動 ODS 用的。它**不是**給上游漂移用的——漂移不改變 ODS 結構；未知欄位落進 `unmapped_fields` 並由 `has_schema_drift` 標記。

**第一步永遠是判定你正在製造哪一種 NULL。** 兩種情況在時間軸上互為鏡像，所以處理哲學是相反的：

| | NULL 往哪裡長 | 意義 |
|---|---|---|
| **加欄** | 過去（歷史分區） | 那段歷史裡這個欄位**根本不存在** |
| **刪欄** | 未來（停止收集後持續成長） | 從此以後這個欄位**不再被填** |

判錯就會拿錯工具。

---

## 加一個欄位

| # | 檢查點 | 動作 |
|---|---|---|
| 1 | ODS | Alembic 加一個 **nullable** 欄位。`NOT NULL` 的新增無法使用 `ALLOW_FIELD_ADDITION`——既有列會違反它 |
| 2 | 一致性測試 | `test_no_ods_column_missing_from_fields` 變**紅**——「ODS 有、`FIELDS` 沒有」在這裡被抓到，而不是靜默少抽 |
| 3 | `FIELDS` | 加上該欄位，型別與 mode 對齊。綠了 = 三份宣告重新對齊 |
| 4 | Extract + load | `ALLOW_FIELD_ADDITION` 自動把它加進 staging；舊分區的歷史列是 NULL，新列有值 |
| 5 | `stg_orders`（未改清單） | 最終的顯式 `SELECT` 沒有列出它 → **被丟掉**。模型輸出不變；那個欄位只是在 staging 裡搭便車 |
| 6 | `stg_orders`（讓它浮上來） | 把它加進顯式 `SELECT`——**進 git、被 review**。下一次普通的增量執行就夠了：dbt 自動 `ALTER ADD COLUMN`（metadata、免費）+ 一個 copy job 只覆寫回看窗分區 |

**步驟 5 是閘門，不是疏漏。** 一個在 staging 長出來的欄位，在有人刻意讓它浮上來之前對下游都是不可見的——**所以漂移無法自己滲進來**（[ADR-0025](../adr/0025-staging-additive-only.md)）。

### ⚠️ `append_new_columns` 的盲點

`ALTER ADD COLUMN` 會把**所有**舊分區設成 NULL，但普通的增量只回填**回看窗**。

如果那個欄位已經在 staging 存在一段時間了——引入的時間點 ≪ 你把它加進 `stg_` SELECT 的時間點——中間那些分區會是**staging 裡有真實值、而 `stg_` 裡卻停在 NULL**。

在首次浮現時修一次：

```bash
dbt run --select stg_orders --vars '{stg_orders_lookback_days: <涵蓋缺口的 N>}'
# 或單次 --full-refresh
```

> 「不用 full-refresh」的意思精確地是**未來每次執行都不必全表重寫**。若首次浮現時有歷史缺口，一次性的回填仍然需要。

---

### 處理歷史的 NULL

判準是**這段歷史是「不存在」還是「少抽了」**：

| 選項 | 適用於 | 做法 |
|---|---|---|
| **A. 接受 NULL**（預設） | 這個值確實是從現在才開始被收集 | 不填；下游按時間切片或 `WHERE col IS NOT NULL` |
| **B. Proposal C 回填** | 值一直在 Raw 裡，只是 ODS 從未對映它 | 用新對映從 Raw 重新產生 → 推送修正列 → 定向刷新 |
| **C. 下游補值** | 分析需要非 NULL | 在 `int_`／`dim_` 用 `COALESCE`，語意記在 model description |
| **D. 攝入時給預設** | 這個值必須永遠存在 | 在 migration 裡設 default／NOT NULL；歷史列在 migration 當下被填 |

A 是預設，因為**強制填值就是捏造資料**——NULL 誠實地反映「它之前不存在」。

---

## 刪一個欄位

| # | 檢查點 | 動作 |
|---|---|---|
| 1 | ODS | Alembic 刪除它；`models.py` 不再有它 |
| 2 | 一致性測試 | `test_no_stale_field_without_ods_column` 變**紅**——「`FIELDS` 有、ODS 沒有」這種過期狀態被抓到 |
| 3 | `FIELDS` | 移除該欄位；綠 |
| 4 | Extract + load | staging 的實體欄位**保留不刪**；load schema 省略它 → 新列 NULL，歷史列保有它們的值 |
| 5 | `stg_orders` | 顯式清單裡還有它 → 查詢正常，**不會壞**；它成為一個 legacy 欄位 |
| 6 | 要從模型移除它 | **預設：留著當 legacy，什麼都不做。** `append_new_columns` 是只加不減、刻意不 `DROP` |

如果真的非移除不可，`--full-refresh` 會重建。罕見、刻意的逃生口——而且若下游 `int_`／`dim_` 仍然引用它，那次執行會報錯並在 DAG 內被抓到。

### 處理持續成長的未來 NULL

這個欄位有真實的歷史，卻有一條持續成長的 NULL 尾巴。問題從*「怎麼填」*轉成**「怎麼不誤用它」**：任何橫跨切斷點的聚合都會靜默地混合兩個母體。把切斷日期記在 model description 裡，並優先用明確的時間切片而非 `COALESCE`。

---

## 為何一致性測試很重要

沒有步驟 2 / 3 的守衛，加了 ODS 欄位卻忘記 `FIELDS` 會**靜默**失敗：抽取照跑、載入成功，那個欄位就是不在倉庫裡。沒有錯誤——**只有一個缺席**，直到某天有人去找它。[ADR-0026](../adr/0026-fields-single-source.md)

---

## 相關

- [design/cloud-layer](../design/cloud-layer.md) — 只做加法的政策
- [dbt-ops](./dbt-ops.md) — 回看窗與 `--full-refresh`
