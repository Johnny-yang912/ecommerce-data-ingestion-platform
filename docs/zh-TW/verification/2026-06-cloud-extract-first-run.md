# 2026-06 — 雲端抽取首次執行

[English](../../en/verification/2026-06-cloud-extract-first-run.md) | **繁體中文**

---

## 驗證的假設

Staging 表設計首次接觸真實 BigQuery dataset。**分區、叢集、成本保險絲與 location 設定真的生效了嗎——保險絲真的會擋嗎？**

## 環境

BQ sandbox，dataset `staging`，location `US`。`extract_ods_to_bq.py` 首次執行。2026-06。

## 觀測

| 檢查 | 結果 |
|---|---|
| 分區／叢集／保險絲／location | `received_at(DAY)` / `[order_id, has_clean_error]` / `True` / `US` |
| **保險絲** | 不帶 `received_at` 過濾的查詢**被以 400 擋下** |
| JSON 落地 | `items` 與 `clean_error_message` 皆為 `JSON_TYPE=array`；下游 `JSON_VALUE(...[0],'$.code')` 讀取正確 |
| 加法式載入路徑 | 顯式 schema + `ALLOW_FIELD_ADDITION` 不會破壞正常流程 |
| 一致性測試 | `test_schema_bq_consistency` 全綠 |

## 結論

四個表設計決策全部如宣告般生效。值得明說的有兩個：

**保險絲真的會擋。** `require_partition_filter=True` 只有在「不帶過濾的查詢會失敗而非昂貴地成功」時才有價值。它回 400——所以失效模式是**免費且大聲的**，而不是昂貴且靜默的（[ADR-0021](../adr/0021-require-partition-filter-fuse.md)）。

**JSON 是以 JSON 落地的，不是字串。** `items` 以 `JSON_TYPE=array` 抵達，才讓 `stg_` 與 `int_order_items` 不需要解析步驟就能讀進去。以序列化字串落地在載入時也會成功，代價是每一次下游讀取都要付一次 `PARSE_JSON`。

## 這推翻了什麼

沒有。這是一則啟用驗收記錄——它的價值在於那些設定是**被確認的而非被假設的**，而那很重要，因為四個之中有三個在出錯時是靜默的：

| 設定 | 若它靜默地沒有生效 |
|---|---|
| 分區 | 查詢掃全部；只有帳單會說 |
| 叢集 | 查詢變慢，不報錯 |
| location | 一直正常，直到第一次跨 location 查詢，然後以令人困惑的方式失敗 |
| **保險絲** | **唯一一個會自己出聲的** |

## 相關

- [ADR-0020](../adr/0020-partition-on-received-at.md) · [ADR-0021](../adr/0021-require-partition-filter-fuse.md) · [ADR-0026](../adr/0026-fields-single-source.md)
- [design/cloud-layer](../design/cloud-layer.md)
- [2026-08-partition-expiry-measurement](./2026-08-partition-expiry-measurement.md) — sandbox 後來對這張表做了什麼
