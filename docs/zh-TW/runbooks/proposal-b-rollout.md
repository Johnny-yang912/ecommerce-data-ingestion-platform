# Runbook：放寬一條品質規則（Proposal B）

[English](../../en/runbooks/proposal-b-rollout.md) | **繁體中文**

---

## 何時適用

一條品質規則正在被**放寬**，讓既有被隔離的記錄有機會被拉回 Gold。

**只有放寬需要這個程序。** 收緊的規則往後適用，不需要回溯。

---

## 七個步驟

```
1. 確認候選存在        目標 code 在隔離區內的值域必須「跨越」新舊兩個門檻
2. 改規則 + bump       clean.py 的門檻 + DQ_RULE_VERSION，然後打 git tag
3. ⚠️ 重建映像         docker compose build api worker beat && docker compose up -d
4. ⚠️ 跑主 DAG         orders_analytics_daily —— 候選是從 BQ 讀的，
                       所以資料必須先上去
5. Dry-run             dq_reevaluation（commit=off、expect_rule_version=<新版>）
6. 正式 commit         dq_reevaluation（commit=on、expect_rule_version=<新版>）
                       → 觸發主 DAG，讓資料回流進 Gold
7. 驗證                被 promote 的列進入 fct_orders 並離開隔離區；
                       promotions > 0；對照組仍被隔離；
                       ODS 未被修改；第二次執行寫入 0 筆
```

---

## ⚠️ 步驟 1——在 bump **之前**確認候選

`promoted=0` 看起來**跟**「規則沒生效」**一模一樣**，也**跟**「程式壞了」**一模一樣**。而低權重的 error code 累積得很慢，所以空的結果完全有可能。

**先檢查值的分布，遠比事後診斷它便宜。**

```sql
select min(<field>), max(<field>), count(*)
from `<project>.<dbt_dataset>.int_orders_quarantine`
where '<target_code>' in unnest(error_codes);
```

值域必須跨越新舊兩個門檻。如果每一個值在新門檻下也在錯的那一邊，就沒有候選——到此為止。

---

## ⚠️ 步驟 3——兩條路徑遞送程式碼的方式不同

```
api / worker / beat   程式碼【烤進映像】       需要 build 才會生效
Airflow 容器          bind mount ./:/opt/project   立即生效
```

跳過重建，**再評估（跑在 Airflow 裡）已經在新規則上，而攝入路徑還在舊規則上**——同一個資料庫裡有兩個規則版本在同時判定資料。

**而 `--expect-rule-version` 看不見那個分歧**：它比對的是它自己行程內的版本，所以斷言會通過。

> 那道守衛防的是「對一個還沒部署新規則的環境執行這件事」。**它成立的前提是整個系統只有單一的程式碼遞送機制**——而這個 compose 拓撲打破了那個前提。

---

## ⚠️ 步驟 4——候選來自 BQ，狀態來自 PG

再評估只寫 PostgreSQL 的 `quality_events`；要讓資料流回 Gold，還需要一次 extract 把事件推上 BQ。

**反過來也成立，而且更容易被忽略：**

> **候選清單來自 BQ 的 `int_orders_quarantine`。尚未被抽取到 BQ 的新資料，對再評估是不可見的。**

症狀是 `candidates` 數量很低且 `would_write=0`——**那看起來像程式壞了，實際上是檢查上游的資料過期了。**

---

## 步驟 7——要驗證什麼

| 檢查 | 預期 |
|---|---|
| `rpt_quality_events_daily` 的 `promotions` | `> 0` |
| 被 promote 的 `order_id` 在 `fct_orders` | 存在 |
| 同樣的 `order_id` 在 `int_orders_quarantine` | 不存在 |
| 對照組（值在新門檻之外） | 仍被隔離 |
| 被 promote 那列的 `ods.has_clean_error` | **仍然是 `TRUE`**——ODS 從不被修改 |
| 再跑一次 `dq_reevaluation`（commit=on） | 寫入 **0** 筆事件（冪等） |

第五列是最多人搞錯的。**ODS 維持 `TRUE` 是正確的**——記錄是透過有效狀態的合成流回去的，不是靠改它的旗標（[ADR-0029](../adr/0029-effective-quality-state.md)）。

---

## 相關

- [design/data-quality](../design/data-quality.md) — Proposal B 是什麼
- [ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md) — 為何它是事件驅動的
- [quarantine-writeoff](./quarantine-writeoff.md) — 另一條終端路徑
