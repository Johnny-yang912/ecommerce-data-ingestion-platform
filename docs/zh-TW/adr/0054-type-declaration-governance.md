# ADR-0054：型別強轉由「宣告」治理，而非由「強轉行為」治理

[English](../../en/adr/0054-type-declaration-governance.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-07 |
| **層** | 資料品質 — 入口 |

---

## 背景

上游改了某個欄位的型別。入口層怎麼處理，取決於 Pydantic 的 lax 模式能不能把那個值強轉成宣告的型別——**而這件事在兩個方向上是不對稱的**：

| 方向 | 例子 | Pydantic 行為 | 結果 |
|---|---|---|---|
| 該是字串，上游送數字 | `customer_name: 123` | **不會**把 int 轉成 str | `ValidationError` → 422 + `ingress_rejected`（不落地） |
| 該是數字，上游送可轉換的字串 | `age: "00501"` | **靜默強轉** `"00501"` → `501` | 通過、落地、下游計算正確 |

第一列是硬型別錯誤，在邊界被乾淨地拒絕。**第二列才是真正的盲點**：那個值符合 schema、下游也算得對，**但「上游這次把一個整數欄位送成字串了」這件事，在 Pydantic 層被靜默吞掉。**

## 決策

**`TYPE_DRIFT` 存在，而且它刻意不經過 Pydantic。**

`detect_schema_drift` 跑在**逐字保留的原始 payload** 上（[ADR-0053](./0053-raw-text-ods-jsonb.md)）——landing 層不會透過 `OrderIN` 重新序列化——它比對 JSON 原生型別與契約，並把**強轉之前的真實型別**記為 `has_schema_drift` + `TYPE_DRIFT`。非阻斷（[ADR-0002](./0002-has-clean-error-non-blocking.md)）。

### 強轉是有邊界的，不是「任何字串都靜默通過」

只有**乾淨、可解析為整數**的字串會通過：

| 輸入 | 結果 |
|---|---|
| `"501"`、`" 501 "` | 強轉 → 落地 + `TYPE_DRIFT` |
| `"12.0"` | 截斷為 `12` → 落地 + `TYPE_DRIFT` |
| `"12.5"`、`"abc"` | **422**，不落地 |

所以異常對照表裡「改型別」那一列精確的意思是：**可強轉** → 落地、被標記、被觀測；**硬型別錯誤** → 422 + `ingress_rejected`。

## 升級：是「宣告」決定了什麼會被靜默改寫

**強轉是「朝著宣告對齊」。** 而那把問題往上推了一層——從**值**推到**宣告本身**。

識別碼類的欄位被宣告為 `str`，**正是為了保住開頭的零**：

```python
customer_id: Optional[str]     postal_code: Optional[str]     product_id: Optional[str]
age: Optional[int]             delivery_days: Optional[int]   tax_pct: Optional[float]
```

把 `postal_code` 誤宣告為 `int`，`"00501"` 就會被靜默截斷成 `501`——**語意遺失，而且很難察覺。** 反過來，只有「概念上可計算」的量才被宣告為數值型別。

> **設定一個型別不是格式問題。它決定了哪些偏差會被靜默吞掉、哪些會被 `TYPE_DRIFT` 看見。**

## 極限：一個宣告無法自我驗證

`TYPE_DRIFT` 抓得到*「上游送的型別 ≠ 宣告」*。它**判斷不了那個宣告本身對不對**——因為它的比較基準**就是**那個宣告。

> **如果基準是錯的，`TYPE_DRIFT` 會用一把錯的尺，精確地量測。**

因此宣告需要它自己的保護。三層——前兩層可自動化，**第三層必然是人**：

| 層 | 機制 | 守得住 | 守不住 |
|---|---|---|---|
| **1** 跨層一致性 | `tests/test_schema_db_consistency.py` —— `ODSOrder`（Pydantic）↔ `ODS`（SQLAlchemy），逐欄位比對 `python_type` | 改了 `schema.py` 卻忘記 `models.py`（或反之）；漏掉的對映 | 兩層一起被宣告錯 |
| **2** 契約快照 | `tests/test_schema_snapshot.py` —— `model_json_schema()` 對照一份進版控的 golden file（`tests/snapshots/`） | 任何型別宣告的變更都會變成一個紅掉的測試**與一份會被看見的 diff** | 一次「刻意但錯誤」的變更（快照會跟著更新） |
| **3** 人的治理 | `schema.py` / `models.py` / `tests/snapshots/` 上的 CODEOWNERS，加上一份上游資料契約 | *「這個型別到底對不對」* | —— 這一層就是最終仲裁者 |

第 1、2 層把「純靠紀律」壓縮成「一個測試變紅、一份 diff 被看見」。但它們只回答**一致／沒有被靜默改動**。

**正確性的問題——「`age` 一開始到底該不該是 `int`」——沒有任何測試能自我驗證**，因為「正確」的定義是「符合與上游議定的契約」，**而那需要一個在宣告之外的真相來源。**

所以最後一層逃不掉人的判斷：

- **CODEOWNERS** 強迫一位指定的資料負責人審查 schema 變更，好讓那份快照 diff 真的**被看**——機制 2 提供鉤子，人提供判斷。
- **資料契約**把每個欄位議定的型別與理由寫下來，讓審查有一個可比對的基準。
- **`TYPE_DRIFT` 的 drift rate 可以反過來用**：某個欄位的 drift rate 長期偏高，是合理的理由去懷疑——**不是上游一直錯，而是你自己的宣告錯了。**

## 現況

| 層 | 狀態 |
|---|---|
| 1 — 跨層一致性測試 | ✅ 已就位、綠燈 |
| 2 — 契約快照 | ✅ 已就位、綠燈（`tests/snapshots/ods_order.schema.json`、`order_in.schema.json`） |
| 3 — CODEOWNERS + 資料契約 | ⏸ **尚未就位**——沒有團隊的團隊治理項目。見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) |

## 考慮過的替代方案

**倚賴 Pydantic 的驗證作為治理點。** 那是顯而易見的答案，**而它正是有盲點的那個東西**——強轉會靜默成功，所以「本該回報漂移的那一層」正是「把漂移抹掉的那一層」。

**關掉 lax 模式（改用嚴格型別）。** 每一份可強轉但漂移了的 payload 都會變成 422 而不落地。**那把一個監控訊號轉換成了資料遺失**，牴觸 ADR-0002——一個算得出正確結果的值，不是拒絕一筆訂單的理由。

**讓 `TYPE_DRIFT` 具阻斷權。** 那會給漂移訊號對 Gold 的權限，而雙訊號的邊界明確否決了它：**一筆只是帶著字串化數字抵達的乾淨訂單，不是一筆壞訂單。**

## 相關

- [ADR-0053](./0053-raw-text-ods-jsonb.md) — 漂移偵測所讀的那份逐字 payload
- [ADR-0002](./0002-has-clean-error-non-blocking.md) — 雙訊號的權限邊界
- [design/data-quality](../design/data-quality.md) — 這一列所展開的 15 項異常對照表
