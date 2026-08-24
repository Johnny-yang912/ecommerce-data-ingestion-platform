# ADR-0026：`FIELDS` 是第三份 schema 宣告，由一致性測試把關

[English](../../en/adr/0026-fields-single-source.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 雲端抽取 |

---

## 背景

一個欄位抵達 BigQuery 時，它的形狀已經被宣告了三次：

| # | 宣告 | 擁有者 |
|---|---|---|
| 1 | `models.py`（SQLAlchemy） | ODS 表 |
| 2 | Alembic migration | PostgreSQL 的實體 schema |
| 3 | `ORDERS_FIELDS` / `QUALITY_EVENTS_FIELDS` | BigQuery staging schema |

宣告 1 與 2 之間已經有守衛：`check_migration_drift.py` 比對它們（ADR-0009）。

**宣告 3 沒有。** 在 ODS 加一個欄位、忘了加進 `FIELDS`，**什麼都不會失敗**。抽取照跑、載入照樣成功，那個欄位就只是不在倉庫裡。沒有錯誤可以注意——**只有一個缺席**，直到某天有人在下游找那個欄位時才會發現。

## 決策

**每張表一份 `FIELDS` 清單，所有用途共用它**，讓那三種用途無法彼此分歧：

- 建立 staging 表（`ensure_staging_table`）
- load job 的 schema
- CLI 的 `--table` 值域，由 `SPECS` 推導而非另外維護

並且 **`tests/test_schema_bq_consistency.py` 拿 `FIELDS` 對 `models.py` 比對**，讓宣告 3 像宣告 2 一樣被釘在宣告 1 上。

新增一張表 = 在 `SPECS` 掛一份 `TableSpec`。它會自動獲得 CLI 項目與一致性守衛——**沒有第二份清單需要記得。**

## 後果

**「靜默缺席」的失效變成一個紅色的測試。** 回饋從「幾週後有人在儀表板上發現少了一個欄位」提前到「造成它的那個 PR 在 CI 上失敗」。

**三份宣告現在全部有守衛**，但機制有兩套：1↔2 靠 `check_migration_drift.py`（手動，見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)），1↔3 靠 `test_schema_bq_consistency.py`（在 CI 裡，因為它不需要資料庫）。

**代價是：真正刻意的分歧必須寫進測試裡。** 若某個欄位該存在於 ODS 但刻意不存在於 staging，那個例外必須被寫下來——**而那正是正確的結果**，因為一個沒有被記錄的刻意分歧，與一個錯誤無法區分。

**三份宣告仍然存在。** 這個決策沒有消除重複；**它讓重複變得可偵測。** 在執行期從 `models.py` 推導 BQ schema 才會消除它——見下。

## 考慮過的替代方案

**自動從 `models.py` 推導 BQ schema。** 能完全消除宣告 3。否決，因為型別對映不是一對一的，而且那些差異是刻意的：`JSONB` → `JSON`、PostgreSQL `Date` → BigQuery `DATE`、兩個系統之間不同的 nullable 規則。自動對映會需要一份例外覆寫表——**那就是 `FIELDS` 本身，只是上面多包了一層間接。**

**靠 review。** 這個失效是一個**缺席**，而缺席是 diff 裡最難抓到的東西。

**接受這個缺口。** 這個失效是靜默的，而資料管線裡的靜默失效，正是這整個專案所圍繞的那個特定危害。

## 相關

- [ADR-0009](./0009-alembic-single-source-of-truth.md) — 宣告 1↔2 的守衛
- [ADR-0025](./0025-staging-additive-only.md) — `FIELDS` 所實作的演進政策
- [測試策略](../design/testing.md)
