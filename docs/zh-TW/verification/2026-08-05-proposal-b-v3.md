# 2026-08-05 — Proposal B：完整的 v3 回流

[English](../../en/verification/2026-08-05-proposal-b-v3.md) | **繁體中文**

---

## 驗證的假設

規則放寬 SOP 的第一次端到端演練。**一筆被 promote 的記錄真的會抵達 Gold 嗎？這個操作冪等嗎？bounded writeback 守得住嗎？**

## 環境

ODS 774 列（57 髒，7.364%）。BQ sandbox、dbt 1.11。2026-08-05。

## 方法

**順序是刻意的。** 先在 **v2** 之下攝入 20 筆 `V3DEMO-*`——15 筆 `age` 落在 {121, 123, 125, 127, 130}，加上 5 筆 `age` ∈ {-3, 150, 999} 的對照組——**然後才**切換到 v3。

順序反過來的話，`age=125` 在抵達時就會被判為乾淨，根本不會進入隔離區：

> **只有在舊規則下攝入的資料，才有資格被新規則拉回來。**

## 觀測

| 階段 | 結果 |
|---|---|
| 在 v2 下攝入 | 20 筆全部 `has_clean_error=TRUE`，`quality_events` → `quarantined`(v2) |
| 抽取 | orders 220 列 / quality_events 220 列 |
| 分層 dbt build | staging PASS=21 WARN=1 / intermediate PASS=27 WARN=1 / marts PASS=31 / reports PASS=24 |
| promote 之前 | 隔離區 **20**、`fct_orders` **0**、`promotions` **0** |
| Dry-run | 57 個候選 → `would_write=15`、`unchanged=42`、`blocked_non_reproducible=0` |
| `--commit` | `written=15` |
| **立刻再跑一次** | **`promoted=0`、`unchanged=57`、`written=0`** |
| 回流之後 | `int_orders` +**15**、隔離區 → **5**、`fct_orders` **15**、`promotions` 0→**15** |
| 完整 `dbt test` | 93 個測試：PASS=91 / WARN=2 / **ERROR=0** |

## 這次證明的四件事

**① 冪等性從「宣稱」變成「量測」。** 連續兩次執行；第二次寫入 0 筆事件。*「只在狀態確實改變時 append」*確實讓 `promotions`——那個「歷史指標永不改寫」所要保護的數字——不會因為重跑而被灌水。先前這件事只有單元測試。

**② 放寬是有邊界的，不是把規則關掉。** 那 5 筆對照組（age −3/150/999）原封不動留在原地，而回流精準地落成 `age=121/123/125/127/130，各 3 筆`。

**③ Bounded writeback 守住了——並留下 15 個「永久分歧」的活樣本。** 那 20 列 ODS 仍然讀到 `dq_rule_version=v2, has_clean_error=TRUE`；**沒有一個欄位被碰過。** 事件鏈很乾淨：

```
initial_evaluation(None → quarantined, v2)  →  promotion(quarantined → promoted, v3)
```

DQ 文件長篇論述的東西，現在是**15 列你可以指著看的資料**：ODS 說髒（v2）、Gold 說乾淨（v3），而 `dq_rule_version` + `quality_events` 讓它完全可追溯。

**④ Hard Gate 的嚴重度分級真的是分級。** 7.364% 讓 0.05 那條斷言 **WARN**、而 0.1 **PASS**——告警但不阻斷，`dbt build` 繼續往下游走。

## ⚠️ 關於 ④ 的歷史註記

**這則記錄裡的測試名稱已經過時。** Hard Gate 後來改為逐批口徑，現在是 `hard_gate_latest_batch_error_rate`（最新 `received_at` 分區、0.15、**error**）加上 `monitor_dataset_error_rate`（全表、0.1、warn）。`_0_05` / `_0_1` 這一對已經不存在。

**觀察本身仍然成立；改變的是它底下的閾值與口徑。** 這則註記是加上去而非改掉原文——**一則驗證記錄是關於某個時刻的陳述，改寫它會摧毀「設計曾經移動過」的證據。** 見 [ADR-0028](../adr/0028-hard-gate-per-batch-scope.md)。

## 結論

SOP 端到端可運作。三個關鍵性質——冪等性、有邊界、錨點未被觸碰——現在全部由資料背書，而不是由論證背書。

## 相關

- [ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md) · [ADR-0032](../adr/0032-bounded-writeback.md)
- [runbooks/proposal-b-rollout](../runbooks/proposal-b-rollout.md) — 這裡走過的那份 SOP
- [2026-08-12-proposal-b-v2-to-v4](./2026-08-12-proposal-b-v2-to-v4.md) — 這一輪覆蓋不到的分支
