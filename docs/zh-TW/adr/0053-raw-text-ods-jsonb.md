# ADR-0053：Raw 以 `TEXT` 保存 payload；ODS 以 `JSONB` 保存結構化欄位

[English](../../en/adr/0053-raw-text-ods-jsonb.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-05 |
| **層** | 攝入 — 儲存 |

---

## 背景

同一份 JSON 會經過兩層，而 PostgreSQL 提供三種存法：

| 型別 | 存什麼 | 可查詢 | 保留原始位元組 |
|---|---|---|---|
| `TEXT` | 原封不動的字串 | 只能當文字 | ✅ |
| `JSON` | 驗證過的文字 | 有運算子，每次查詢都要解析 | ✅ |
| `JSONB` | 分解後的二進位形式 | 有運算子、可建索引 | ❌ |

直覺是全部用 `JSONB`——那是 PostgreSQL 文件會引導你去用的型別，而且查詢更快。

**那個直覺對其中一層是錯的**，原因在於 `JSONB` 在寫入途中做了什麼：

> `JSONB` 會**正規化**。它重排 key、剝掉不重要的空白，並且**摺疊重複的 key，只保留最後一個**。

## 決策

| 欄位 | 型別 | 因為 |
|---|---|---|
| `raw.raw_payload` | **`TEXT`** | Raw 逐位元組記錄「到達了什麼」 |
| `ods.items` | **`JSONB`** | 下游會查詢與 join 它 |
| `ods.clean_error_message`、`schema_drift_message`、`unmapped_fields` | **`JSONB`** | 品質層會查詢它們 |

**這個不對稱本身就是決策。** 同一份資料、兩層、相反的需求。

## 為何 Raw 必須是 `TEXT`

**① 否則「Raw 逐字記錄每一個入站請求」就是假的。** ADR-0001 建立在「Raw 是忠實記錄」之上。一份被正規化的 payload 不是抵達的那份 payload——**它是一份「除了被正規化掉的東西以外都與抵達那份相同」的 payload。**

**② Proposal C 的承諾倚賴它。** 「Raw 逐字保留使重建成為可能」（[ADR-0032](./0032-bounded-writeback.md)）是整條修正路徑的背書。從一份被正規化的 payload 重建，重現的是那次正規化，不是原件。

**③ 重複的 key 是證據，而 `JSONB` 會摧毀它。** `{"age": 30, "age": 31}` 是合法 JSON，而上游的 bug 有可能送出它。`TEXT` 兩個都保留；`JSONB` 靜默地只留下 `31`，**而「上游自相矛盾」這個事實就消失了——沒有錯誤，事後也無從發現。**

**④ 漂移偵測讀的是逐字的 payload。** `detect_schema_drift` 刻意繞過 Pydantic、直接檢視原始字串，才能記下**強制轉型之前的真實型別**（[ADR-0054](./0054-type-declaration-governance.md)）。key 的順序與重複，都可能是它需要看到的東西的一部分。

## 為何 ODS 必須是 `JSONB`

`items` 每次執行都被 `int_order_items` 讀取並攤平到 item 粒度。用 `TEXT` 的話，每一次讀取都要付一次解析；用 `JSONB` 則結構已經被分解，將來若有需要還能建 GIN 索引。

**ODS 是可查詢的那一層。Raw 不是**——`GET /raw/{id}` 回傳 payload 給人看，而沒有任何東西 join 它。

## 後果

**Raw 佔更多磁碟**——一個未壓縮、保留原始空白的字串。可以接受：Raw 是這個系統裡最便宜的浪費地點，而替代方案放棄的正是它存在的理由。

**Raw 無法在不 cast 的情況下用 JSON 運算子查詢。** 那不是損失。任何想查詢 payload 的東西，問的都是 ODS 該回答的問題，**而為此伸手進 Raw 會製造出通往同一份資料的第二條路徑。**

**ODS 失去原始的位元組形式**——沒關係，因為 Raw 有，而 `ods.raw_id` 是通回它的 1:1 邊。

**NUL byte 在寫入 Raw 之前仍然必須被剝掉。** 不論這個決策如何，PostgreSQL 的 `TEXT` 都無法儲存 `0x00`——見 [ADR-0006](./0006-nul-byte-fast-fail.md)。**`TEXT` 保留的是「PostgreSQL 存得下的一切」，而那不完全等於「一切」。**

## 考慮過的替代方案

**全部用 `JSONB`。** 讓沒有人會做的 Raw 查詢變快，換走逐字性質、重複 key 的證據，以及 Proposal C 的背書。

**全部用 `TEXT`。** 讓 Raw 保持正確，並讓每一次下游讀取 `items` 都付一次解析——而那是每次 `int_` 重建都會讀的欄位。

**Raw 用 `JSON`（非二進位型別）。** 保留文字**並且**驗證它是 JSON。否決：**在儲存層做驗證，正是 landing 層絕不該做的事。** 一份格式錯誤的 JSON 仍然是「上游送了什麼」的記錄，Raw 必須收下它，`process.py` 才能把那次失敗歸類到一個終端狀態，而不是讓寫入本身失敗。

## 相關

- [ADR-0001](./0001-raw-no-business-dedup.md) — 這條所保護的「記錄到達了什麼」性質
- [ADR-0032](./0032-bounded-writeback.md) — Proposal C 的承諾，它建立在這條之上
- [ADR-0054](./0054-type-declaration-governance.md) — 漂移偵測讀的就是那份逐字 payload
- [ADR-0006](./0006-nul-byte-fast-fail.md) — `TEXT` 仍然裝不下的東西
