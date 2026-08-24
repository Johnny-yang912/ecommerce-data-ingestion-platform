# 2026-08 — BigQuery sandbox 分區過期

[English](../../en/verification/2026-08-partition-expiry-measurement.md) | **繁體中文**

---

## 驗證的假設

Sandbox 強制 60 天分區過期。**一旦 Gold 以業務時間軸分區，它究竟會做什麼？能不能繞過？**

以下每一項都是量測出來的，不是推論的。

## ① 過期是從分區的日期值算的，不是從 build 時間

所有 dataset 都帶著 `default_partition_expiration_ms = 5184000000`（60 天），並被每一張分區表繼承。建了一張 `partition by order_date` 的表，用**一句 CTAS** 寫入五個跨越邊界的日期：

| 分區 | 結果 |
|---|---|
| 2024-01-01 | **rows=0，不見了** |
| 2026-05-01（94 天前） | **rows=0，不見了** |
| 2026-06-04（邊界上） | rows=1 ✅ |
| 2026-07-01（33 天前） | rows=1 ✅ |
| 2026-08-03（當天） | rows=1 ✅ |

三個行為：

1. **build 不會失敗**——`CREATE OR REPLACE` 回傳成功。
2. **一個「2024-01-01」的分區在它誕生的那一瞬間就已經超過 60 天了。**
3. **刪除是同步且立即的**——CTAS 一返回就去查，兩個舊分區已經從 `INFORMATION_SCHEMA.PARTITIONS` 消失，連 `num_rows` 的 metadata 都讀到 3 而不是 5。**沒有任何警告。**

> `stg_orders` 之所以從未撞到這件事，純粹是因為它以 `received_at` 分區，而攝入時間永遠是近期的。**換到業務時間軸，那層保護就消失了。**

## ② 60 天上限是硬鎖的——四條路徑全部關閉

| 嘗試 | 結果 |
|---|---|
| DDL `options(partition_expiration_days = 3650)` | ❌ job 失敗 |
| DDL `options(partition_expiration_days = NULL)` | ⚠️ **不報錯，靜默被改寫成 60 天** |
| API：`table.time_partitioning.expiration_ms` | ❌ 403 |
| API：dataset 的 `default_partition_expiration_ms` | ❌ 403 |

```
reason: billingNotEnabled
Partition expiration time must be less than 60 days while in sandbox mode.
```

**對程式碼的後果**：`gold_partition_expiration_days` 必須用 `var` 把關，且預設不輸出任何東西。寫死 1825 會讓每一次 `dbt run` 失敗，並在 `dbt build` 中跳過所有下游。

> **一個值得知道但不該使用的漏洞**：`3650` 那句 DDL 其實**半成功**——job 被標記為失敗（`error_result.reason=billingNotEnabled`），但表被建出來了、`expiration_ms` 真的是 3650 天，而舊的列存活下來、60 秒後仍可查詢。強制執行位在 **job 驗證層**，而 DDL 的副作用溜過了它。不可用：失敗的 job 就是失敗的 dbt run，而且一旦 Google 把這個縫補起來，表就會開始被靜默回收。

## ③ 超出範圍的日期落進 `__UNPARTITIONED__`——它們**不會**讓 build 失敗

```
partition_id=20260803           rows=1
partition_id=21591231           rows=1
partition_id=__UNPARTITIONED__  rows=3   ← 1959-12-31 / 2160-01-01 / 9999-12-31
build 成功；五列全部存活且可查詢
```

超出 `1960-01-01 ~ 2159-12-31` 的值**不報任何錯**，靜默進入 `__UNPARTITIONED__`。

連帶效果：那些列同樣**逃過 60 天的回收**，而且永遠無法被 partition pruning 剪掉。

## ④ `__NULL__` 分區逃過回收

`order_date` 在 ODS 是 nullable 的。NULL 落進 BigQuery 的 `__NULL__` 分區，它沒有日期、因此沒有可計算的過期時間，所以**永遠不會被回收**。

量測中它與 2024-01-01 在同一批寫入——後者當場蒸發，而 `__NULL__` 存活。

後果：在 `fct_orders` 裡，**沒有 `order_date` 的訂單活得比有的更久**。目前資料有 0 個 NULL，但 schema 允許它們。

## 結論

這條限制是真的、帳號層級的、無法繞過。它重要的性質不是「它刪資料」，而是**它靜默地刪資料，而且行為不一致**——三種分區（有日期、超出範圍、NULL）依三套不同的規則被回收。

**那種不一致正是它對測試設計危險的地方**：一個計算 Gold 列數的測試，結果取決於今天是幾號。

## 這推翻了什麼 ⭐

**雲端層文件先前主張**：*「荒謬的未來日期落在 BigQuery 可接受的分區範圍之外，會讓整張表的 build 失敗。」*

**它們不會。** 它們靜默落進 `__UNPARTITIONED__`，而 build 成功。

因此，採用 `order_date` 分區前那道計畫中的「合法區間 guard」**被撤回為不必要**——實際的失效模式不是原先假設的那一種。**真正的危害與預期相反**：不是一次大聲的 build 失敗，而是一些悄悄逃過了分區與過期兩者的列。

## 相關

- [ADR-0044](../adr/0044-copy-partitions-sandbox-dml.md) — sandbox 的另一條限制
- [design/cloud-layer](../design/cloud-layer.md)
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — 開通計費會改變什麼
