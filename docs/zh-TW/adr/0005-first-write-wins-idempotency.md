# ADR-0005：冪等性採 first-write-wins——預檢加 `IntegrityError` 後盾

[English](../../en/adr/0005-first-write-wins-idempotency.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-05 |
| **層** | 攝入 — ODS |

---

## 背景

ADR-0001 把業務去重下放給 ODS。因此 ODS 必須在併發之下、且 worker 之間沒有任何協調的前提下，讓每個 `order_id` 恰好持有一列。

預檢——`INSERT` 前先 `SELECT`——是顯而易見的實作，而且**單靠它並不足夠**。兩個 worker 可以都跑完 `SELECT`、都沒找到、都往下走。這個窗口很小，而且是真的：它被刻意重現過。

## 決策

兩條 `UNIQUE` 約束，做兩件不同的事：

| 約束 | 保證什麼 |
|---|---|
| `UNIQUE(ods.order_id)` | 每筆業務訂單一列 |
| `UNIQUE(ods.raw_id)` | 一列 Raw 最多產生一列 ODS——一條 1:1 的血緣邊 |

以及兩層執行：

1. **預檢**——commit 前 `SELECT ODS WHERE order_id = ?`。命中就把 Raw 標記為 `duplicate`，不寫入。
2. **`IntegrityError` 後盾**——在 commit 時捕捉，**不重試**。重讀一次找出贏家，把這筆標記為 `duplicate`，返回。

第一個 commit 的寫入者獲勝。第二個不是錯誤情況，它是一筆重複（ADR-0003）。

## 後果

**約束才是保證；預檢是最佳化。** 這個區別值得寫明，因為它決定了兩者不一致時該怎麼做：**預檢的存在是為了在常見情況下避免做白工，而資料庫才是讓結果在每一種情況下都正確的東西。** 拿掉預檢損失的是效能，拿掉約束損失的是正確性。

**`IntegrityError` 刻意不重試。** 重試會重新執行一個現在必然失敗的寫入——與「把確定性錯誤當成暫時性錯誤重試」屬於同一類錯誤，而那正是 ADR-0006 那顆毒藥丸的成因。

**兩種順序都驗證過：**

| 場景 | 結果 |
|---|---|
| 循序——同一個 `order_id` 送兩次 | 第一筆寫入 ODS；第二筆在預檢命中，標記 `duplicate` |
| TOCTOU 競賽——兩個 worker 都通過預檢 | 第一個 commit 成功；第二個拿到 `IntegrityError`，標記 `duplicate` |

兩種情況下，ODS 最終都是每個 `order_id` 恰好一列。

**代價是白做的工。** 輸掉的那個寫入者在約束拒絕它之前，已經解析、攤平、清洗過整包 payload 了。這是接受的：替代方案是協調，而協調在「根本沒有重複」這個常見情況下更貴。

## 考慮過的替代方案

**`INSERT ... ON CONFLICT DO NOTHING`。** 能把兩層壓成一句，但它是靜默的——輸掉的寫入者無法得知自己是否獲勝，所以 `raw.status` 沒辦法被正確設定。`processed` 與 `duplicate` 的區別（ADR-0003）比省下的一次來回更值錢。

**Last-write-wins。** 會讓 ODS 變成可變的，牴觸它作為不可變錨點的角色（ADR-0002），並打破「`quality_events` 是唯一記錄狀態隨時間變化的東西」這個前提。

**以 `order_id` 為鍵的分散式鎖。** 外部依賴，而且它**仍然需要約束作為後盾**——見 ADR-0004。

## 相關

- [ADR-0001](./0001-raw-no-business-dedup.md) — 這個責任從哪裡下放而來
- [ADR-0003](./0003-duplicate-terminal-status.md) — 輸家去哪裡
- [ADR-0004](./0004-cas-claim-rowcount.md) — 同一個「讓資料庫仲裁」模式，用在認領上
- [ADR-0006](./0006-nul-byte-fast-fail.md) — 這個決策避開的那個重試分類錯誤
