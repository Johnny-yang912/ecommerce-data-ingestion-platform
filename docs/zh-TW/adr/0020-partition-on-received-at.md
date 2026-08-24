# ADR-0020：以 `received_at` 分區——而它在 Raw 與 ODS 指的是兩個不同時刻

[English](../../en/adr/0020-partition-on-received-at.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-06 |
| **層** | 雲端抽取 |

---

## 背景

`orders` staging 表以 `received_at` 做日分區，並以 `order_id` + `has_clean_error` 叢集。對一張 append-only 的 staging 表而言，用落地時間分區是標準選擇：增量載入只碰一個分區，按時間過濾的查詢也只掃一個分區。

需要被記錄下來的不是這個選擇本身，而是：**同一個欄位名稱在兩張表上指的是兩個不同時刻**，而且至少有四個下游機制建立在它上面。

| 欄位 | 何時蓋上 | 意義 |
|---|---|---|
| `raw.received_at` | API 在請求路徑中同步寫入 Raw 時 | **訂單接收時間** |
| `ods.received_at` | worker 寫入 ODS 時（`process.py` 不把 Raw 的值帶過來） | **ODS 落地時間** |

## 決策

`orders` staging 以 `ods.received_at` 做日分區，並以 `order_id` + `has_clean_error` 叢集。

**這個語意是正確的，不是妥協。** extract 搬動的**就是** ODS。用 ODS 自己的時鐘同時作為分區欄位與增量游標，恰好回答了「extract 有沒有把 ODS 往前推」這個問題。改用 `raw.received_at` 會把 Raw→ODS 這一跳的延遲摺進 extract 的檢查裡，**讓一個訊號代表兩段管線**。

叢集則跟隨存取模式：`order_id` 是下游的 join 鍵，`has_clean_error` 是 Row Filter 的述詞。

## 後果

**每條時間線恰好覆蓋一跳，沒有一條在兼差：**

| 時間線 | 回答什麼 |
|---|---|
| `ods.received_at` | extract 有沒有把 ODS 往前推？ |
| `raw.status='pending'` 的最舊年齡 | 派工這一跳還活著嗎？（`raw_pending_watch`，ADR-0039） |
| 經 OTel 的 `raw.received_at` 連續性 | 上游還在送嗎？ |

**⚠️ 一個必須知道的範圍邊界。** 當恢復掃描把積壓排空時，那些列拿到的 `ods.received_at` 是**補寫當下**的時間。攝入的那段空窗因此**在 ODS 時間線上根本不存在**。任何建立在 `ods.received_at` 上的東西——分區、source freshness、`rpt_quality_events_daily` 的日界——都只看得見**取樣當下仍在進行中**的中斷，永遠看不見已經復原的那些。

**⚠️ 一個很容易搞錯的判準，明講：** 「有 Raw 卻沒有對應的 ODS」**不能**作為故障的定義。Raw 的終端狀態是 `processed` / `duplicate` / `error`，而後兩者**按正確行為就不會產生 ODS 列**。那個定義會對每一筆重複訂單發出告警。`pending` 才是乾淨的訊號。

**⚠️ 這個名字讀起來像接收時間，而我們不打算改名。** 改名是一次 migration，會波及 `FIELDS` 宣告（ADR-0026）與每一處 dbt 引用。**下一個讀到 `ods.received_at` 的人，應該以這份記錄為準，而不是從名字推論。**

## 考慮過的替代方案

**把 `raw.received_at` 帶進 ODS。** 主要的反對理由是語意上的，不是實務上的：以這個欄位的用途而言，現在的意義已經是對的，**而改動它才會讓它變錯**。代價——重建與回填表、以及連帶移動 Hard Gate 的「最新 UTC 日分區」口徑——只是次要理由。

**再加一個帶接收時間的時間欄位。** 能同時回答兩個問題，代價是兩個時間欄位、而它們的差值只對走過積壓的列有意義。不採用；上面那三條時間線的拆分已經覆蓋了這些問題，各自在擁有它的那一層。

## 相關

- [ADR-0023](./0023-watermark-approach-a.md) — 讀取這個分區的 watermark
- [ADR-0026](./0026-fields-single-source.md) — 為何改名會波及
- [ADR-0039](./0039-observation-signals-own-dag.md) — 覆蓋這條時間線看不到的那一跳
- [雲端層設計](../design/cloud-layer.md) — Gold 層分區，另行決定
