# ADR-0022：`quality_events` staging 刻意與 `orders` 分歧

[English](../../en/adr/0022-quality-events-staging-diverges.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 雲端抽取 |

---

## 背景

抽取腳本在 `orders` 之外還落地第二張表：`quality_events`，那份 append-only 的品質事件日誌。

**為什麼非抽不可：** `int_` 層合成**有效品質狀態**的方式，是把 ODS 快照與最新的 `quality_events` 事件 join 起來。一筆被 Proposal B promote 的記錄，在 ODS 裡永遠讀到 `has_clean_error = TRUE`——ODS 是不可變的（ADR-0002）。**只有事件能讓它流回 Gold。** 倉庫裡沒有這張表，回流機制就沒有右手邊。

誘人的捷徑是照抄 `orders` 的表設計。**它的存取模式是相反的**，所以每一個決定都必須重新問一次。

## 決策

每一個設計選擇都獨立做出，其中三個得到不同的答案：

| 決策 | `orders` | `quality_events` | 為何不同 |
|---|---|---|---|
| 分區 | `received_at`（DAY） | `event_at`（DAY） | 每張表用自己的落地時間軸；`event_at` 同時餵給 watermark |
| 叢集 | `order_id` + `has_clean_error` | `raw_id` + `to_state` | 下游取的是「每筆記錄的最新狀態」，粒度在 **`raw_id`**——與 `stg_` 去重的鍵相同 |
| 成本保險絲 | ✅ 開 | ❌ **關** | **決定性的那一個**——見下 |

**保險絲是關的，而那正是這條記錄的重點。** `orders` 的查詢永遠帶 `received_at` 過濾，所以保險絲（ADR-0021）不花任何代價。但 `quality_events` 的主要消費者需要的是**跨全歷史、按 `raw_id` 取最新事件**——本質上就是一次不帶分區過濾的全掃描。**保險絲會擋掉設計唯一要求的那個查詢。**

## 後果

**照抄的設計會是靜默錯誤的。** 不是在載入時錯、不是在測試裡錯——而是在回流路徑第一次被實際走過的那一刻才錯，**而那是表建立之後好幾個月的事**。這就是「每個決定都要重新問一次」而非「繼承一份」的論據。

**這裡的回流比 `orders` 乾淨。** Promotion 事件帶著 `event_at = now()`，落在**今天的**分區，所以例行的 `event_at >= watermark` 增量自然就撿得到。反觀 `orders` 的修正會落回**舊的**分區，需要一次明確的 runbook 推送。

> **append-only 的時間語意，讓 `quality_events` 的抽取嚴格比 `orders` 簡單。** 那不是巧合——那是「事件日誌是 append-only」這件事的紅利。

**⚠️「跨全歷史」這個假設在 sandbox 上被壓到 60 天。** BigQuery sandbox 強制一個 60 天分區過期，這張表繼承了它；在腳本裡設 `expiration=None` 會被忽略，因為那是帳號層級的限制。見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)——對以業務時間軸分區的 Gold 表而言，後果嚴重得多。

**這裡的成本按設計是無界的**，因為保險絲關著。可以接受，因為這張表很小（每次品質狀態改變一列，不是每筆訂單一列），而全掃描正是消費者需要的。

## 考慮過的替代方案

**照抄 `orders` 的 spec。** 會弄壞有效狀態的合成，而且是很晚才壞。

**保留保險絲，另外物化一個「每個 `raw_id` 的最新」視圖。** 把全掃描搬進一個排程作業而不是消除它，多一個要維護的物件，而且它自己需要一份新鮮度保證，回流路徑才會正確。

## 相關

- [ADR-0021](./0021-require-partition-filter-fuse.md) — 這條刻意反轉的那個決策
- [ADR-0029](./0029-effective-quality-state.md) — 驅動這三個差異的那個消費者存取模式
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — 為何光靠 ODS 回答不了這個問題
