# ADR-0009：Alembic 是 schema 的單一真相；移除 `create_all`

[English](../../en/adr/0009-alembic-single-source-of-truth.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 橫切 — schema |

---

## 背景

專案一開始在啟動時呼叫 `Base.metadata.create_all()`。它只在一種情況下有效：一個空的資料庫。

**`create_all` 只會建立，它從不變更。** 在 `models.py` 加一個欄位，它不會出現在既有的資料庫裡——`create_all` 看到表已經存在就跳過，而且是靜默的。schema 與 model 就此漂移，過程中沒有任何一個環節報錯，失效會在很久之後以「某個『應該』存在的欄位」的執行期錯誤浮現。

這讓 `create_all` 在結構上無法承載 schema 演進——而對一個整個主題就是「資料隨時間變化」的系統而言，那不是一個可選的性質。

## 決策

**Alembic 是 schema 的單一真相。** `Base.metadata.create_all` 被完全移除——不保留為便利路徑，因為便利路徑正是兩份真相再次分歧的方式。

配套選擇：

- **`env.py` 從 `settings.db_url` 取連線**，並 `import models` 讓 `Base.metadata` 被填充以供 autogenerate 使用。
- **`alembic.ini` 的 `sqlalchemy.url` 留空。** 填它會製造出資料庫 URL 的第二個所在地；`DB_URL` 維持單一真相（ADR-0008）。
- **`Base.metadata` 帶著一份命名慣例**（`ix_`、`uq_`、`ck_`、`fk_`、`pk_`）。初始 migration 以慣例原生的名稱產生。

命名慣例是這三者中最不明顯、也最有價值的一個。沒有它，約束名稱由資料庫指派、且各環境不同——於是一個「刪除或改名某約束」的未來 migration 會在一台機器上成功、在另一台上失敗，而失效浮現的時間遠晚於造成它的那個決定。

工作流程是：`alembic revision --autogenerate` → **審查產生的腳本** → `alembic upgrade head`。

## 後果

**schema 變更成為可審查的產出物。** 一個 migration 是 pull request 裡的一個檔案，不是某個行程啟動時的副作用。

**漂移可用一道指令偵測。** `check_migration_drift.py` 跑 `alembic upgrade head` 再對 `models.py` 做 `compare_metadata`，不一致就以非零碼結束。它是確定性、無並發、低 flake 的——所以它**可以**進 CI。目前保持手動，見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

**約束名稱跨環境穩定且可預測。**

**代價是「加個欄位」現在變成兩個步驟**，而且自動產生的腳本必須真的被讀過。autogenerate 不會偵測所有東西——欄位型別變更與表格改名尤其需要人為注意。

## 考慮過的替代方案

**本機開發用 `create_all`，部署環境用 Alembic。** 這是常見的折衷，而它是個陷阱：**開發環境正是 schema 變更被做出來的地方**，所以這個做法把變更放進了那個不追蹤變更的環境。第一個假設了「`create_all` 從未產生過的狀態」的 migration，會在第一次部署時失敗。

**手寫 SQL migration、不用 Alembic。** 沒有 autogenerate、沒有版本圖、沒有 `compare_metadata`——所以除了把兩邊都讀一遍之外，沒有辦法回答「資料庫是不是 `models.py` 所說的樣子」。

## 相關

- [ADR-0008](./0008-config-boundary.md) — 為何 `sqlalchemy.url` 留空
- [測試策略](../design/testing.md) — `check_migration_drift.py` 的定位，以及 CI 涵蓋不到什麼
- [STATUS](../STATUS.md)
