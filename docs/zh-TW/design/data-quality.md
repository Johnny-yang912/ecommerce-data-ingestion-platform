# 資料品質架構

[English](../../en/design/data-quality.md) | **繁體中文**

品質如何被判定、在哪裡阻斷，以及一個判定日後如何在不改寫歷史的前提下改變。

---

## 1. 設計目標

把不可信的入站資料轉為可信的分析資料，同時讓**每一次品質判斷保持可稽核**——包含那些日後被修訂的判斷。

有三個性質讓它成為可能，而每一個都依賴前一個：

1. **ODS 不可變且完整**——每一筆被接受的訂單恰好存在一次，不論髒或乾淨。
2. **品質判定是事件，不是狀態**——一份 append-only 的日誌記錄轉移。
3. **消費者自己合成有效狀態**，而不是讀一個被儲存起來的旗標。

---

## 2. 攝入時的兩個訊號

`clean_order()` 與 `detect_schema_drift()` 產生兩個**獨立、平行、非阻斷**的訊號。它們從不混用。

### 權限分界

| 面向 | `has_clean_error` | `has_schema_drift` |
|---|---|---|
| 意義 | 這筆記錄的**值**有業務問題 | 上游送來的**結構／契約**改變了 |
| 典型來源 | `quantity <= 0`、rating 超出範圍、NaN/Inf、未來日期、過長文字、數值 sentinel | 非預期欄位、改名欄位、型別漂移、非物件的巢狀群組 |
| 訊息欄位 | `clean_error_message` | `schema_drift_message` + `unmapped_fields` |
| **對 Gold 的權限** | **可以阻斷**——`int_*` 會把它隔離 | **不能阻斷**——一筆乾淨訂單即使有漂移仍然流進 Gold |
| 屬於 `quality_events` 狀態機？ | ✅ 是 | ❌ 否——它是維運訊號，不是品質狀態的演進 |
| 與規則版本綁定？ | ✅ 隨 `DQ_RULE_VERSION` 演進 | ❌ 無關；它關乎程式碼如何對映 schema |
| 補救路徑 | Proposal B（再評估）／`force=true`（重跑） | **一個工程動作**——對齊契約、加欄位對映、更新模型。**不是**規則再評估 |
| 可觀測性 | `quality_metric` log、`rpt_quality_*` | `schema_drift` log、`ingress_rejected` log |

> 一句話：**`has_clean_error` 有把資料擋在 Gold 之外的權限；`has_schema_drift` 沒有。** 它只能告警，並請一個人去把契約對齊。

### 四象限：訊號組合 → 處置 → 結果

| `has_clean_error` | `has_schema_drift` | 情況 | 處置 | 結果 |
|:---:|:---:|---|---|---|
| FALSE | FALSE | 完全乾淨 | 正常流動 | 抵達 Gold；`quality_events` → `clean` |
| TRUE | FALSE | 值有業務問題 | `int_*` Row Filter 擋下 | 進 `int_orders_quarantine`；→ `quarantined`；具 Proposal B 資格 |
| FALSE | TRUE | 契約變了但**值是乾淨的** | **仍然流進 Gold** + 漂移告警 | 抵達 Gold、不被擋；通知工程端 |
| TRUE | TRUE | 值是壞的**而且**契約變了 | 被 `has_clean_error` 擋下 + 漂移告警 | 進隔離區（值的問題 → Proposal B）；漂移另外處理。**兩條路徑互相獨立** |

**第三象限是關鍵設計點**：一筆其他方面都好、只是多帶了一個 `loyalty_points` 欄位的訂單，**不會被踢出 Gold**。那正是「為何選擇獨立訊號，而不是把它塞進 `has_clean_error`」的答案。

### 什麼不是訊號

格式正規化是第三件事，而且它不設定這兩個旗標：它做的是**強制轉換**（去空白、大小寫、型別對齊）。只有業務規則違反會設定 `has_clean_error`。

上游來的未知欄位被保留在 `unmapped_fields` 而非被靜默丟棄——所以「上游送了新東西」是可救回的，不是遺失的。

### 非阻斷的邊界

非阻斷適用於**業務規則違反**，不適用於儲存層面的不可能。欄位溢位（`DataError`）或字串含 NUL（`ValueError`）**根本寫不進去**——那些會快速失敗到終端 `error` 狀態，永遠不會到達 ODS。[ADR-0006](../adr/0006-nul-byte-fast-fail.md)

上游異常的完整清單與各自的處理方式見**[附錄 A](#附錄-a上游異常對照15-項)**。

---

## 3. 阻斷：兩個機制、兩種粒度

### Hard Gate —— run 層級，掛在 `stg_`

問的是**「source 是不是整個壞了？」**——突變偵測，不是清潔度檢查。失敗會中止整個 dbt run，讓 `int_`／`dim_`／`fct_` 保留上一次的乾淨狀態。

| | 口徑 | 閾值 | 嚴重度 | 角色 |
|---|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | 最新 `received_at` 分區 | 15% | `error` | **閘門** |
| `monitor_dataset_error_rate` | 全表 | 10% | `warn` | **儀表** |

閘門是逐批的，所以它的靈敏度不會隨歷史累積而衰減，而且上游修好後它能自己清除。全表數字保留作為能見度，並**刻意不給它阻斷權**。[ADR-0028](../adr/0028-hard-gate-per-batch-scope.md)

以自訂 generic test `macros/error_rate_below.sql` 實作——比率斷言必須在聚合層級以 `HAVING` 表達，因為 BigQuery 拒絕 `WHERE` 子句裡的 `COUNTIF`。

### Row Filter —— 逐筆層級，在 `int_` 內

問的是**「這一列能用嗎？」**——髒資料進 `int_orders_quarantine`，而不是被丟掉。

判準是**有效品質狀態**，不是字面的旗標：

```sql
COALESCE(
    s.has_clean_error = FALSE      -- 攝入時就乾淨
    OR e.to_state = 'promoted',    -- 或被後來的再評估 promote
    FALSE
) AS is_effectively_clean
```

ODS 是不可變的，所以一筆被 promote 的記錄**永遠**讀到 `has_clean_error = TRUE`。字面讀取那個旗標會讓它永久卡在隔離區。

**⚠️ 兩個不可以動的東西**：`LEFT JOIN`（inner join 會丟掉每一筆沒有事件的記錄——那幾乎是全部），以及 `COALESCE`（`FALSE OR NULL = NULL`，而 `WHERE NOT NULL` 也是 NULL，所以該列會**同時從兩張表消失**）。[ADR-0029](../adr/0029-effective-quality-state.md)

### 情境專用模型 —— 已設計，未實作

情境模型可以接受與它的問題無關的錯誤、施加補值，並僅為那個情境放行。**未實作**，因為要決定哪些錯誤無關，前提是知道分析問題是什麼——**先建等於把猜測包裝成設計**。[ADR-0027](../adr/0027-blocking-at-int-layer.md)

---

## 4. 品質事件日誌

`quality_events` 是 append-only 的，記錄的是**轉移**而非狀態：

```
initial_evaluation
  ├── 通過所有規則              → clean
  └── has_clean_error = TRUE    → quarantined

quarantined / re_quarantined
  ├── 再評估通過                → promoted               (promotion)
  ├── 再評估未通過              → 不寫任何事件
  └── 人工註銷                  → permanently_rejected   (rejection)

promoted
  ├── 更嚴的規則下不通過        → re_quarantined         (re_quarantination)
  └── 仍然通過                  → 不寫任何事件

permanently_rejected            ← 終端；沒有出邊
```

有三個性質本身就是決策：

- **「不寫事件」是刻意的**——只在確實改變時 append，讓這份日誌成為它自己的冪等閘門。
- **`permanently_rejected` 只能來自人**——在 PostgreSQL 的寫入目標上強制，不是靠下游過濾。
- **`re_quarantination` 是後來才加的，而它沒弄壞任何東西**——因為消費端是按 `to_state` 而非 `event_type` 計數的。

搭配 `DQ_RULE_VERSION`（目前 `v4`），逐列存在 `ods.dq_rule_version` 並**永不再觸碰**。[ADR-0031](../adr/0031-rule-versioning-quality-events.md)

### `DQ_RULE_VERSION` 什麼時候該 bump？

`DQ_RULE_VERSION` **只為業務值評估規則**（`business_clean`）標版本，不為 schema 對映標版本。兩者是正交的軸。

> **上游契約的改變本身不會 bump 版本。** 只有當你為了回應它而修改了 `business_clean` 時才 bump。

其間存在一條間接的鏈——schema 漂移經常**逼出**一次規則變更，而那時才 bump——但觸發條件精確地是**「你改了 `business_clean`」**，不是「上游改了」。

**判準**：*如果拿同一份原始 payload 重跑一次，`has_clean_error` ／ `clean_error_message` 會不會不一樣？*

| 變更 | 會改變評估結果嗎？ | Bump？ |
|---|---|---|
| 新增或修改一條 `business_clean` 規則（新檢查、改門檻） | ✅ | **要** |
| 一次**會影響後續評估**的 `format_clean` 變更（新的 sentinel→NULL 改變了哪些值會被標記） | ✅ | **要** |
| 新增欄位對映（`from_nested` 多接一個欄位） | ❌ | 不用 |
| 改名欄位的重新對映 | ❌ | 不用 |
| 改動 `detect_schema_drift` 的邏輯 | ❌（那是另一個訊號；從不碰 `has_clean_error`） | 不用 |
| 讓一條與時間相關的規則接受注入的 `as_of` | ❌（攝入路徑預設 `as_of=None` = `now()`，所以一份 payload 的首次評估不變） | 不用 |

**最後那一列存在的理由，是它修的正是「重跑會不會得到同一個答案」**——它改變的是**可重現性**，不是判定，**而那恰恰是它不該 bump 的原因。**

### 什麼時候才會寫事件？

**有 ODS 列，才有事件。** 那三列「不寫」的情況，正是人們預期會看到、而實際上不會有的：

| 情況 | 寫事件？ | `to_state` |
|---|---|---|
| ODS 寫入成功，無品質問題 | ✅ | `clean` |
| ODS 寫入成功，有品質問題 | ✅ | `quarantined` |
| 預檢攔截到重複（ODS 未寫入） | ❌ | — |
| TOCTOU `IntegrityError`（ODS 未寫入） | ❌ | — |
| 管線失敗 → `raw.status = error`（ODS 未寫入） | ❌ | — |

> **`quality_events` 是一份品質判斷的日誌，不是攝入嘗試的日誌。** 一筆從未抵達 ODS 的記錄從未被判斷過——它的下場記在 `raw.status`（[ADR-0011](../adr/0011-no-result-backend.md)）。

---

## 5. 補救：三條路徑

| | 路徑 | 觸及 |
|---|---|---|
| **A** | `POST /process_raw/{id}?force=true` | 卡在 `error`／`duplicate` 的記錄——從 Raw 重放 |
| **B** | 依新規則再評估 | 被後來已放寬的規則所隔離的記錄 |
| **C** | 從 Raw 做批次修正 | **值產生的缺陷**——已設計，未實作（[runbook](../runbooks/proposal-c-correction.md)） |

### Proposal B：事件驅動的再評估

`reevaluate_quality.py` 讀取候選、重跑當前規則，並**只在狀態確實改變時** append。

- **候選來自 BigQuery 的 `int_` 層**——與 Row Filter 使用同一個有效狀態定義，所以產生者與消費者不可能有不同意見。
- **狀態的判定對 PostgreSQL 做**——冪等性不可以建立在一個會過期的鏡像上（sandbox 的 60 天限制）。
- **dry-run 是預設值**；commit 是一個明確的旗標。

兩道可重現性守衛：

| 守衛 | 防止 |
|---|---|
| `business_clean(as_of=...)` | 與時間相關的規則純粹因為時鐘往前走而給出不同答案 |
| `NON_REPRODUCIBLE_CODES` | 因為**證據消失了**而非因為它通過了而 promote 一筆記錄 |

> **一次看不見記錄為何失敗的再評估，不可以下結論說它沒有失敗。**

[ADR-0030](../adr/0030-proposal-b-event-driven-reevaluation.md)

### Proposal C：A 與 B 都觸及不到的

一個清洗**bug** 汙染了已經 `processed` 的記錄裡的值——例如 sentinel 清單把 `"na"`（North America）當成 null，把某個欄位在數千列上洗成 NULL。

B 幫不上忙：**它的輸入就是那些被汙染的值。** 就算它看得出來，bounded writeback 也禁止它寫值。A 對 `processed` 回 400。

**若這條路徑不是被設計出來的，「Raw 逐字保留使重建成為可能」這個承諾就沒有東西背書。**

> **這裡預先定義修復路徑的形狀，不預先做出選擇。** 真的發生時，依災情規模、受影響欄位數、下游消費狀況、對主表操作的風險偏好、修復時程與運維能量，在兩種形態之間現場裁量。兩者不互斥——同一個團隊可以在不同事故選不同形態，甚至先用補丁止血、後擇期以遷移收斂回單一真相。
>
> 無論選哪種，Proposal C 都是**離線、runbook 驅動的批次操作**——刻意**不做成** HTTP endpoint，讓 `force=true` 維持唯一的 runtime 修復面，語意邊界不被稀釋。
>
> **執行順序見 [runbooks/proposal-c-correction](../runbooks/proposal-c-correction.md)。這一節回答「該選哪條路」，那一份回答「選好之後怎麼走」。**

#### C-1 兩種形態

兩者的**重跑端完全相同**（從 Raw 用修正後的邏輯重產值），分歧只在「修正值落在哪、如何生效」：

| | 遷移形：範圍化重建 | 補丁形：修正覆蓋 |
|---|---|---|
| 核心語意 | 災區列在主 ODS **原位替換**（同交易退役舊列 + 寫回新列，帶 `rebuild_batch_id`） | 主 ODS **完全不動**；修正列寫入獨立的 `ods_corrections` |
| 修復後的真相 | 主表即單一真相；舊值在 `ods_retired_<batch>` 留檔稽核 | 真相分裂兩表；正確值 = 主表 ⊕ overlay |
| ODS 不可變契約 | 需重新詮釋：被禁止的是「單筆、無版本、下游不知情」的改寫；批次、有版本、留退役副本、強制連動下游者是合法逃生口 | 字面上完全遵守，契約零重新詮釋 |
| BQ 落地 | 修正列 append 進**同一張 staging**（原 `received_at` 分區），`stg_` 去重以 `rebuild_batch_id` 決勝 | corrections 另成一張 BQ 表，`stg_` 以 JOIN／`COALESCE` 覆蓋 |
| `stg_` 複雜度 | 既有去重邏輯 + 一個決勝鍵，不新增 JOIN 縫 | 新增一條**常駐** JOIN 縫；每個讀 `stg_` 的人都必須知道 overlay 存在 |
| 雲端成本 | ≈ 0（複用既有通道） | 金錢上同樣可忽略（BQ 按掃描量計費，小表 JOIN 趨近 0）——**成本疑慮是假議題，真正的代價在語意面** |
| 事故疊加 | 每次多一批 append + 一個 batch id，讀取邏輯不變 | 每次多一批 correction，需管理批次優先序，讀取複雜度隨次數成長 |
| 未來全量重抽 | 安全——主表已正確 | 主表錯值會被原樣重抽，**必須記得補推 corrections**（P3） |
| PG 端其他消費者 | 自動拿到正確值 | 拿到錯值，除非自行實作 overlay |
| 回滾 | 反向再做一個批次（從 retired 蓋回），同一機制自我支撐 | 作廢該批 correction 即可，主表從未被碰過 |
| 不可逆點 | PG commit 那一刻（前置 dry-run diff + 人工閘門） | **無**——天然低風險 |
| 成本結構 | 操作較重，但**一次付清**，修復後架構回到原狀 | 操作極輕、見效快，但複雜度**常駐**於讀取路徑與未來運維 |
| 傾向因素 | 災情大而系統性（多筆多欄位）、預期主表還會被全量重抽、在意長期單一真相、有足夠運維窗口 | 災情小而定點、不容許碰主表的風險偏好、需要快速止損、暫無人力或窗口執行遷移 |

#### C-2 兩種形態都必須面對的注意事項

| # | 注意事項 | 內容 |
|---|---|---|
| 1 | 部署順序 | **先部署修法**止血——右邊界唯有如此才會被凍結。**先修資料再部署，等於永遠追著一個移動的靶。** |
| 2 | 影響窗口的界定 | 用 **`ods.received_at`**（值被產生的時刻）界定，不是 `raw.received_at`——被恢復掃描較晚重新處理的記錄，其 Raw 時戳落在窗口之外，會被漏掉 |
| 3 | 重新推導的路徑 | 只重用純函式 `from_nested → clean_order`。**絕不走 `process_raw_event`**——它的 first-write-wins 預檢會看到正在被替換的那一列，並把整批標記為 `duplicate` |
| 4 | 主動推送 | 修正列帶著舊的 `received_at`，所以只往前看的 watermark 永遠看不到它們。推上雲端是一個**明確的 runbook 步驟** |
| 5 | 批次版本軸 | `DQ_RULE_VERSION` 只為評估語意標版本——一個 `format_clean` 的值 bug 可以**在不改變 `has_clean_error` 的情況下**改變值，完全逃過 bump 判準。值的產生需要它自己的 batch id，而它同時也是 `stg_` 去重的決勝鍵——**不只是一個稽核欄位** |
| 6 | quality_events | 重產值時的 `clean_order` 已同時給出新值與新判定——**只在狀態真的改變時 append**（沿用攝入層的同一條規則），事件類型沿用 `promotion` / `re_quarantination`，batch id 記進 `reason`。值缺陷不是規則變動，多數列狀態不變、不寫事件才是對的；逐列的痕跡由 `rebuild_batch_id` 承載 |
| 7 | Late-arriving | 修正後的值落在舊分區；依 `received_at` 增量的 `stg_` 執行看不到它們。runbook 的最後一步必須是一次定向刷新 |
| 8 | 分歧窗口 | 在 PG commit 與 BQ 刷新之間，PG 持有新值而 Gold 仍持有舊值——與 Proposal B 的回流延遲同構。在 T+1 之下可接受，**但推送 + 刷新必須是綁定的 runbook 步驟，絕不可做到一半** |

#### 額外注意事項——遷移形

| # | 注意事項 | 內容 |
|---|---|---|
| M1 | 原子性 | 退役副本、主表的 delete+insert、以及那些事件要在**同一個交易**裡——拆開會留下一道「值換掉了但狀態機沒記錄」的裂縫 |
| M2 | `statement_timeout` | 全域的 30 秒逾時是為短交易設計的，它會殺掉一次上萬列的批次。重建用的連線必須覆寫它 |
| M3 | 併發安全 | 在 MVCC 下中間狀態是不可見的：併發的重複預檢讀到舊列；TOCTOU 的 INSERT 會阻塞到 commit 然後撞上 `IntegrityError`，與正常行為完全相同。**不需要額外處理——但必須被理解並記錄下來** |

#### 額外注意事項——補丁形

| # | 注意事項 | 內容 |
|---|---|---|
| P1 | 覆蓋優先序 | 同一個 `raw_id` 被修正兩次時，overlay 必須自己實作「最新批次勝出」——遷移形從主表免費得到這件事 |
| P2 | 第二條抽取路徑 | 修正表需要它自己的 `FIELDS` 宣告、抽取邏輯，以及與 `test_schema_bq_consistency` 同等級的一致性守衛 |
| P3 | 全量重抽的 runbook | 任何 staging 重建都必須重推修正——**明確寫進重建步驟，否則錯誤的值會復活** |
| P4 | 消費者契約 | *「讀 ODS 必須套用 overlay」*成為一條新的隱含契約，需要文件與「防止未來直接讀主表」的守衛 |

#### `raw_id` FK，以及它為何不是 C 的對立面

`ods.raw_id → raw.id`（`ON DELETE NO ACTION`，NOT NULL + UNIQUE = 1:1）**是對 C 本來就倚賴的一份契約的強制執行**：C 的核心前提是「從 Raw 重新推導值」，那本來就要求父列存在。**FK 只是把「我們假設 raw 在」變成「資料庫保證 raw 在」。**

| 形態 | 影響 |
|---|---|
| 遷移形 | **在建構上就 FK 安全**——重建的列重用既有的 `raw_id`，而 raw 從不被刪除。額外好處：若注意事項 3 的手動 INSERT 路徑掉了或捏造了 `raw_id`，它會從一個**靜默的孤兒**升級成一次立即的 FK 違反 |
| 補丁形 | 主 ODS 的 FK 完全不碰那張獨立的表。建議：`ods_corrections.raw_id` 建出來時給它同一條 FK |

| # | Runbook 增補 | 為何 |
|---|---|---|
| C-6.1 | 退役／封存表**不可繼承 FK**（`LIKE ... EXCLUDING CONSTRAINTS`） | 否則退役副本會釘住 raw 列，或封存失敗 |
| C-6.2 | Dry-run gate 斷言影響窗口內每一列的 `raw_id` 在 `raw` 裡仍解析得到 | FK 本來就會擋——在 dry-run 抓到可避免一個做到一半的 runbook |
| C-6.3 | FK 查找成本併入 M2 的 `statement_timeout` 覆寫 | 每次 INSERT 多一次 PK 索引查找；在批次規模下可忽略 |
| C-6.4 | 批次 INSERT 會對 raw 取 `FOR KEY SHARE` 列鎖——**與正常管線無衝突**（`try_claim_raw` 改的是非鍵欄位，取 `FOR NO KEY UPDATE`，兩者相容）；影響窗口內的列全是終端狀態，所以真實爭用 ≈ 0 | 依 M3 的精神，理解並記錄 |

> **FK 順帶形式化的一個相鄰前提**：Raw 必須活得比它的 ODS 列久。若哪天引入 Raw 的清除或 TTL，就必須尊重那個順序——`NO ACTION` 的 FK 會主動擋下「刪除一列仍被 ODS 引用的 raw」。**行為正確，但它改變了清除的語意。**

#### C-5 裁量時的考量面向

事故當下建議至少過一遍以下面向，再決定形態：

- **災情規模與形狀**：幾筆？幾個欄位？集中在一個窗口還是散落？
- **下游消費狀況**：錯值已被哪些報表／模型消費？修復急迫性多高？
- **風險偏好**：能否接受對主表的批次操作？有沒有窗口與人力做 dry-run 審核？
- **長期維護成本**：這條 overlay 縫由誰維護？團隊一年後還記得它嗎？
- **疊加可能性**：同類事故是一次性還是會再發生？（會再發生 → 常駐縫的複利成本要算進去）
- **混合路徑**：兩者不互斥——可先補丁止損，待窗口充裕時以遷移收斂回單一真相（補丁批次即遷移的有效輸入，機制相容）

---

## 6. 一致性

**Bounded writeback**：任何來自倉庫的回流，目標**只有 `quality_events`**。ODS 永不被修改。

```
❌ 倉庫 → UPDATE ODS 的欄位
✅ 倉庫 → INSERT 進 quality_events
```

因此 ODS 與倉庫之間的分歧是預期之內的，而且恰好有兩個可解釋的來源：

| 來源 | 由什麼解釋 |
|---|---|
| 規則版本演進 | `dq_rule_version` + `quality_events`——可查詢、帶時戳 |
| 情境模型接受了無關的錯誤 | 讀那個模型的 SQL 與 dbt description——靜態，沒有執行期追蹤表 |

[ADR-0032](../adr/0032-bounded-writeback.md)

---

## 7. 指標

兩層，邊界明確——**高基數切片按定義屬於倉庫**：

| | Tier 1 — 營運 | Tier 2 — 分析 |
|---|---|---|
| 延遲 | 分鐘級 | 日／週 |
| 在哪 | OTel 指標 + structlog `quality_metric` | `rpt_quality_*` |
| 倉庫中斷時存活 | 是 | 否 |

**歷史指標永不回溯改寫。** 若 v2 promote 了 15 筆 v1 隔離的記錄，v1 的隔離率仍然是當時那個數字——promotion 被計為它自己的指標，在它自己的軸上。**一條會自己改寫的趨勢線無法支撐任何結論。** [ADR-0033](../adr/0033-historical-metrics-never-rewritten.md) · [ADR-0034](../adr/0034-tier-1-tier-2-metrics.md)

那正是品質報告拆成兩張表的原因——見 [transformation §5](./transformation.md)。

---

## 附錄 A：上游異常對照（15 項）

上游可能出錯的每一種方式，以及由哪個機制吸收它。**這是 §2 雙訊號治理的具體實例化**——每一列都會落到四個象限之一。

| # | 異常 | 訊號／機制 | 結果 |
|---|---|---|---|
| 1 | 非預期的新欄位 | `has_schema_drift`（`UNEXPECTED_FIELD`） | 落地；新欄位存進 `unmapped_fields`，既有欄位不受影響 |
| 2 | 缺少預期欄位 | 入口放寬；偵測延後 | 以 NULL 落地；由 null-rate 監控偵測，不在入口攔 |
| 3 | 欄位改名 | 分解成第 1、2 列——新名 =「非預期」、舊名 =「缺少」 | 新名進 `unmapped_fields`；舊名以 NULL 落地 |
| 4 | **改型別** | 可強轉 → `TYPE_DRIFT`；硬錯誤 → 422。見 [ADR-0054](../adr/0054-type-declaration-governance.md) | 可強轉者落地 + 被標記；硬型別錯誤 → 422 + `ingress_rejected` |
| 5 | 日期格式／時區改變 | 格式錯誤 → 422；時區 → 一份書面契約 | 格式錯誤 422 + log；時區是議定的，不是偵測的 |
| 6 | 沒見過的 enum 值 | 落地；長度由過長路徑處理 | 新值落地；由下游的 `accepted_values`（warn）抓 |
| 7 | 語意漂移 | — | **規則抓不到這個。** 延後給分布監控 |
| 8 | 完全沒有資料 | — | OTel 管線已上線，但 **absent 告警未寫**——見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) |
| 9 | 同一個 `order_id` 重送 | 既有的冪等性 | first-write-wins；重複標記為 `duplicate`（[ADR-0005](../adr/0005-first-write-wins-idempotency.md)） |
| 10 | 非物件的巢狀群組 | `has_schema_drift`（`NON_OBJECT_GROUP`）+ 防禦性守衛 | 不會 crash；被標記，該群組以 NULL 落地 |
| 11 | sentinel／假的 null | `format_clean` 正規化（字串）；範圍檢查（數字） | 字串 sentinel → NULL；數值 sentinel 標記 `has_clean_error` |
| 12 | 過長字串 vs 欄位上限 | `has_clean_error`（`FIELD_TOO_LONG`）+ 寬鬆的 DB 牆 + 快速失敗 | 中等長度 → 標記後落地；誇張長度 → 終端 `error`，不再有毒藥丸 |
| 13 | NUL byte | 寫入前剝除 + 警告 | 剝除後落地；已解碼的 `\u0000` 案例見 [ADR-0006](../adr/0006-nul-byte-fast-fail.md) |
| 14 | NaN／Infinity | `has_clean_error`（`NON_FINITE_NUMBER`） | 標記後落地；下游隔離——**不會毒害聚合** |
| 15 | 未來日期／時鐘偏移 | `has_clean_error`（`ORDER_DATE_IN_FUTURE`）；抽取用 `>=` | 未來日期被標記；時鐘倒退由增量抽取的 `>=` 緩解 |

**三列值得放在一起讀**——它們是規則抓不到的那幾個：

- **#7 語意漂移**——每個值單獨看都合法，合起來是錯的。**只有分布看得見它。**
- **#8 完全沒有資料**——什麼都沒來，所以什麼都沒被評估，所以沒有訊號會觸發。這與[靜默排程停擺](../incidents/2026-08-silent-scheduling-stalls.md)是同一個結構性盲點：**缺席不產生記錄。**
- **#2 缺少欄位**——在入口刻意放寬。拒絕它等於用一個可監控的 NULL 換一筆遺失的訂單。

---

## 8. 相關

- [ADR-0002](../adr/0002-has-clean-error-non-blocking.md) — 這裡的一切所倚賴的那個決策
- [ingestion](./ingestion.md) — 訊號在哪裡被產生
- [transformation](./transformation.md) — 過濾器在哪裡執行
- [orchestration](./orchestration.md) — Proposal B 的部署 SOP
