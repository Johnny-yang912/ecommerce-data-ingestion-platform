# 資料品質控管架構

## 設計目標

確保進入分析層（Star Schema）的資料是最乾淨的。  
ODS 作為不可變的錨點，保留完整資料狀況；品質管控的責任隨資料往下游流動而逐步收緊。

---

## 各層品質合約（Q0）

```
Raw (PostgreSQL)                               ← Landing
  職責：逐字保留原始 request body；僅抽取 order_id 作為關鍵追溯欄位
  品質要求：無
  可修改：否

ODS (PostgreSQL)                               ← Bronze / 錨點
  職責：基本清洗與業務規則驗證，保留完整資料狀況
  品質要求：基本格式標準化；業務問題只標記，不攔截
  可修改：否（攝入當下的快照，永遠不被下游修改）

BQ staging（Airflow 抽取）
  職責：搬運 ODS，增量上傳
  品質要求：與 ODS 相同，純鏡像

dbt stg_*                                      ← Silver 入口
  職責：1:1 對應來源，型別對齊、欄位命名標準化
  品質要求：與 ODS 相同，仍保留所有資料含髒的
  附加：source freshness test、schema 基本測試

────────────── 攔截發生在這裡 ──────────────

dbt int_*                                      ← Gold 入口
  職責：跨表 join、衍生欄位、業務邏輯
  品質要求：只讓乾淨資料通過（has_clean_error = FALSE）
  髒資料去向：int_orders_quarantine

dbt dim_*/fct_*                                ← Gold
  職責：Star Schema，供下游彈性查詢
  品質要求：最乾淨，不含任何 has_clean_error=TRUE 的記錄

dbt rpt_*
  職責：固定粒度預聚合，BI Dashboard 直接使用
  品質要求：同 dim_*/fct_*
```

---

## 攝入層：上游異常與雙訊號治理

ODS 攝入邊界產生兩個**互不混用、權限不同**的品質訊號。核心原則：**「資料的值對不對」與「上游契約變了沒」是兩件事，分開標記、分開處置。**

### 兩個訊號的權限分界

| 面向 | `has_clean_error` | `has_schema_drift` |
|---|---|---|
| 語意 | 這筆資料的**值**有業務問題 | 上游送來的**結構/契約**變了 |
| 典型來源 | 數量≤0、評分超範圍、NaN/Inf、未來日期、超長文字、數值 sentinel | 多欄位、改名新欄、型別漂移、巢狀群組非物件 |
| 訊息欄位 | `clean_error_message` | `schema_drift_message` + `unmapped_fields` |
| **對 Gold 的權限** | **可攔截**：`int_*` 以 `WHERE has_clean_error=FALSE` 把它擋進 quarantine | **不可攔截**：值乾淨的訂單即使有 drift 仍流入 Gold；drift 純為監控訊號 |
| 進 `quality_events` 狀態機？ | ✅ 是（`initial_evaluation` → `clean`/`quarantined` → `promoted`…） | ❌ 否（非品質狀態演進，屬 ops 訊號） |
| 與規則版本相關？ | ✅ 隨 `DQ_RULE_VERSION` 演進 | ❌ 與規則版本無關，與「程式對 schema 的對映」有關 |
| Remediation 路徑 | **Proposal B**（規則升版重評估）／`force=True`（pipeline 失敗重跑） | **工程行動**：與上游對齊契約／新增欄位對映／更新 Pydantic model——**不是**規則重評估 |
| 觀測 | `quality_metric` log、`rpt_quality_*` | `schema_drift` log、`ingress_rejected` log、（Phase 4）drift 率監控 |

一句話：`has_clean_error` 有「把資料擋出 Gold」的權限；`has_schema_drift` **沒有**——它只能告警，要求人去對齊契約。

### 四象限：兩訊號組合 → 處置 → 結果

| `has_clean_error` | `has_schema_drift` | 情況 | 處置 | 結果 |
|:---:|:---:|---|---|---|
| FALSE | FALSE | 完全乾淨 | 正常流入 | 進 Gold（`dim_*`/`fct_*`）；`quality_events` → `clean` |
| TRUE | FALSE | 值有業務問題 | `int_*` Row Filter 攔截 | 進 `int_orders_quarantine`；`quality_events` → `quarantined`；走 Proposal B |
| FALSE | TRUE | 契約變了但**值乾淨** | **仍流入 Gold** + drift 告警 | 進 Gold（不被擋）；同時通知工程端對齊契約／補欄位對映 |
| TRUE | TRUE | 值有問題**且**契約也變 | 因 `has_clean_error` 被攔 + drift 告警 | 進 quarantine（值問題走 Proposal B）；drift 另由工程端處理。**兩條路各自獨立** |

關鍵：第三象限是設計重點——**一筆只是多了 `loyalty_points` 的好訂單，不會因此被踢出 Gold**；這正是當初選擇「獨立訊號」而非共用 `has_clean_error` 的理由。

### 上游可能的異常與應對（15 項對照）

| 異常 | 訊號 / 機制 | 結果 |
|---|---|---|
| 多一個沒見過的欄位 | `has_schema_drift`（`UNEXPECTED_FIELD`） | 落地；新欄存入 `unmapped_fields`，原欄正常 |
| 少一個預期欄位 | ingress 放寬（落地 NULL）；偵測留觀測層 | 落地為 NULL；少欄位偵測由 Phase 4 null-rate 監控 |
| 改名 | 拆成上兩列：新名＝「多一個沒見過的欄位」；舊名＝「少一個預期欄位」 | 同上兩列：新名收進 `unmapped_fields`；舊名落地 NULL |
| 改型別 | 可強轉→`TYPE_DRIFT`；硬錯→422（詳見下方〈改型別：從強轉行為到宣告治理〉） | 可強轉落地+標記；硬型別錯 422 + `ingress_rejected` |
| 改日期格式 / 時區 | 格式錯→422；時區→契約約定 | 格式錯 422+log；時區屬明文契約（見設計邊界） |
| 沒見過的 enum | 落地；長度由超長處理；偵測留 dbt | 新值落地；超長不卡死；Phase 4 `accepted_values`（warn） |
| 語意漂移 | — | 留 Phase 4–5 分佈監控（規則抓不到） |
| 沒有資料 | — | 留 Phase 5 OTel volume/freshness 告警 |
| 同 order_id 重送 | 既有 idempotency | first-write-wins，重複標 `duplicate` |
| 巢狀結構非物件 | `has_schema_drift`（`NON_OBJECT_GROUP`）+ 防禦守衛 | 不崩潰；標記，該群組落地為 NULL |
| sentinel / 假空值 | `format_clean` 正規化（字串）；range check（數值） | 字串 sentinel→NULL；數值 sentinel 標 `has_clean_error` |
| 超長字串爆長度 | `has_clean_error`（`FIELD_TOO_LONG`）+ DB 硬牆 fast-fail | 偏長→標記落地；離譜→終態 `error`（不再卡死） |
| NUL byte | 寫入前 strip + warning | 移除後落地，不再 500 掉單 |
| NaN / Infinity | `has_clean_error`（`NON_FINITE_NUMBER`） | 標記落地、下游 quarantine、不毒化聚合 |
| 未來日期 / 時鐘回撥 | `has_clean_error`（`ORDER_DATE_IN_FUTURE`）；抽取 `>=` | 未來日期標記；回撥由增量抽取 `>=` 緩解 |

### 改型別：從強轉行為到宣告治理（第 4 列展開）

攝入層對「上游改型別」的處置，取決於 Pydantic lax 模式能不能把值轉成宣告型別——而這是**雙向不對稱**的：

| 方向 | 例子 | Pydantic 行為 | 結果 |
|---|---|---|---|
| 該是字串、上游送數字 | `customer_name: 123` | 不接受 int→str 強轉 | `ValidationError` → 422 + `ingress_rejected`（不落地） |
| 該是數字、上游送可轉字串 | `age: "00501"` | 靜默強轉 `"00501"→501` | 通過、落地，值在下游運算正確 |

第一列是「硬型別錯」，在邊界就被擋掉，乾淨。**真正的盲區是第二列**：`"00501"→501` 一切符合 schema、下游也算得對，但「上游這次把整數欄位送成字串」這個契約偏離的事實，會在 Pydantic 層被無聲吃掉。這正是 `TYPE_DRIFT` 存在的理由——`detect_schema_drift` 不經過 Pydantic，而是跑在**逐字保留的原始 payload** 上（landing 層刻意不以 `OrderIN` 序列化回存，見〈設計邊界〉），用 JSON 原生型別比對契約，把強轉前的真實型別記成 `has_schema_drift` + `TYPE_DRIFT`（非阻斷）。

強轉本身也有邊界，不是「送字串就一定無聲通過」：只有**乾淨、可整數化的字串**會過（`"501"`、`" 501 "`；`"12.0"→12` 會截斷），`"12.5"`、`"abc"` 仍被擋成 422。所以第 4 列精確說是：**可強轉**（值能落到宣告型別）→ 落地 + `TYPE_DRIFT` 標記（被觀測）；**硬型別錯**（值無法轉）→ 422 + `ingress_rejected`（不落地）。

而既然強轉是「向宣告對齊」，**宣告本身就決定了什麼會被無聲改寫**——這把問題從「值」往上推到「宣告」。識別碼類欄位（`postal_code`、`customer_id`、`product_id`）一律宣告為 `str`，正是為了保住前導零：若誤宣告成 `int`，`"00501"` 會被靜默截成 `501`、語意遺失且難以察覺；反過來，只有「概念上可被運算」的量（`age`、`delivery_days`、`tax_pct`）才宣告 `int/float`。所以設定型別時的紀律不是格式問題，而是**在決定「哪些偏離會被無聲吃掉、哪些會被 `TYPE_DRIFT` 看見」**。

這也暴露了 `TYPE_DRIFT` 的極限：它能抓「上游送的值型別 ≠ 宣告」，卻**無法判斷「宣告本身對不對」**——因為它的比對基準就是那份宣告，無法拿宣告自證。基準錯了，`TYPE_DRIFT` 只會拿錯的尺去量。因此「宣告」需要另一套守護，分三層，前兩層可自動化、第三層必須靠人：

| 層 | 機制 | 守什麼 | 守不到 |
|---|---|---|---|
| 1 跨層一致性 | `tests/test_schema_db_consistency.py`：`ODSOrder`（Pydantic）↔ `ODS`（SQLAlchemy）逐欄位比對 `python_type` | 改了 schema.py 忘了改 models.py（或反之）、漏對映 | 兩層一起宣告錯 |
| 2 契約快照 | `tests/test_schema_snapshot.py`：`model_json_schema()` 對 committed golden 檔 | 任何型別宣告改動都變成會紅的測試 + 可審查 diff | 有意但錯誤的改動（快照隨之更新） |
| 3 人治理 | CODEOWNERS（`schema.py` / `models.py` / `tests/snapshots/`）+ 上游 data contract | 「這個型別到底對不對」 | —（這層即最終裁判） |

前兩層把「純靠紀律」收斂成「會紅的測試 + 會被看到的 diff」，但它們只回答**一致 / 沒被偷改**；**「`age` 本來該是 int 嗎」這種正確性問題沒有任何測試能自證**，因為「正確」的定義是「符合與上游約定的契約」，需要一個宣告以外的事實來源。所以最後一層逃不掉人的判斷：**CODEOWNERS** 讓指定的資料負責人必審 schema 改動，使快照 diff 真的有人看（機制 2 給鉤子、人給判斷）；**data contract** 把每欄的約定型別與理由寫成白紙黑字，讓 review 有可比基準；而既有的 `TYPE_DRIFT` **drift 率**也能反向利用——某欄位 drift 率長期居高，合理懷疑不是上游一直錯，而是自己的宣告錯了（見〈觀測與告警〉）。

**目前狀態**：機制 1、2 已落地（測試綠）；機制 3 的 CODEOWNERS 與 data contract 屬團隊治理項。

### 與 `DQ_RULE_VERSION` 的關係

`DQ_RULE_VERSION` 只版本化**業務值評估規則**（`business_clean`），不版本化 schema 對映。兩者是正交的軸：

> **上游契約變動本身不 bump `DQ_RULE_VERSION`**；只有當你因應而修改了 `business_clean`（值評估規則）時才 bump。

但兩者之間存在一條**間接因果鏈**：schema drift（上游變動）常常**逼你修改一條業務規則**——例如新增的 enum 值要納入驗證、語意漂移要收緊 range、新對映的欄位要加範圍檢查——那一刻才 bump。所以「schema drift 間接導致 bump」在實務上常見，但要說精準：bump 的觸發點是「**你動了 `business_clean`**」，不是「上游動了」。

bump 判準：**同一筆 raw payload 重跑一次，`has_clean_error` / `clean_error_message` 會不會得到不同結果？**

| 改動 | 會改變值評估結果嗎 | bump？ |
|---|---|---|
| 新增/修改 `business_clean` 規則（新檢查、改閾值） | ✅ | **要** |
| `format_clean` 改動且**影響後續判定**（如新 sentinel→NULL 改變了會被標記的值） | ✅ | **要** |
| 新增欄位對映（`from_nested` 多撈一欄） | ❌ | 不用（走 code review／migration） |
| 改名重新對映 | ❌ | 不用 |
| `detect_schema_drift` 偵測邏輯改動 | ❌（屬另一個訊號，不碰 `has_clean_error`） | 不用 |

**本次 v1 → v2 的 bump**：攝入層強化新增了 `FIELD_TOO_LONG`、`NON_FINITE_NUMBER`、`ORDER_DATE_IN_FUTURE` 三條 `business_clean` 規則，並以 sentinel 正規化影響值評估——同一筆 raw 重跑會得到不同的 `has_clean_error`，故 bump。此次規則是**變嚴**（標記更多），只往後生效即可、**不需回溯重評估**；回溯只在規則放寬要 promote 舊 quarantine 時才做（對應狀態機的 `re_quarantined` 邊緣情況）。

### 觀測與告警

- **逐筆**：`quality_metric`（含 `has_clean_error`）、`schema_drift`（drift 詳情）、`ingress_rejected`（被硬閘門擋下、不落地者）。
- **批次**（Phase 4）：`rpt_quality_*` 之外，可加 **drift 率門檻告警**（類比 Hard Gate，但 drift 率超標只**告警不中止** run——因 drift 不具攔截權限）。

### 設計邊界

- **order_id 為唯一硬閘門**：缺 order_id → 422（不落地，記 `ingress_rejected`）；其餘欄位缺失／型別／結構問題一律「落地 + 標記」，把判斷下放。
- **時區語意屬契約，非演算法**：裸 `order_date` 無法偵測時區漂移，以明文契約約定（UTC）解決；附帶的未來日期防線僅為健全性檢查。
- **`quality_events` 不記 schema drift**：維持其作為「業務品質狀態機」的語意純淨（與〈版本號與 quality_events 表〉的語意邊界一致）。

---

## 攔截機制（Q1）

兩個機制並用，覆蓋不同層次的問題。

### 機制一：Hard Gate（run-level）

在 `dbt stg_*` 掛載測試。測試失敗 → 整個 dbt run 中止，`int_*/dim_*/fct_*` 不更新，保留上一次的乾淨狀態。

```yaml
# stg_orders.yml
models:
  - name: stg_orders
    tests:
      # 嚴重：關鍵欄位全空 → 整批資料無意義，直接擋
      - not_null:
          column_name: order_id
          severity: error

      # 嚴重：整批錯誤率過高 → 可能是 source 系統異常
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.1"
          severity: error     # > 10% 擋住

      # 警告：錯誤率偏高但尚可接受 → 繼續跑但發告警
      - expression_is_true:
          expression: "countif(has_clean_error) / count(*) < 0.05"
          severity: warn      # > 5% 告警
```

### 機制二：Row Filter（record-level）

在 `dbt int_*` 的 SQL 中過濾，逐筆隔離。

```sql
-- int_orders.sql（乾淨資料流）
SELECT * FROM {{ ref('stg_orders') }}
WHERE has_clean_error = FALSE

-- int_orders_quarantine.sql（品質問題資料）
SELECT
    *,
    CURRENT_TIMESTAMP() AS quarantined_at
FROM {{ ref('stg_orders') }}
WHERE has_clean_error = TRUE
```

### 機制三：場景專用分析模型（int_* 層）

特定分析場景可在 `int_*` 層建立場景專用模型。`clean_error_message` 是 JSONB 物件陣列（`{"code", "field", "value", ...}`），場景以穩定的 `code` 比對（而非人類可讀的措辭），接受全局 quarantine 中與該場景無關的錯誤，並在模型內對問題欄位補值：

```sql
-- int_orders_shipping_analysis.sql
SELECT
    *,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM UNNEST(clean_error_message) AS e
            WHERE JSON_VALUE(e, '$.code') = 'customer_rating_out_of_range'
        )
        THEN NULL
        ELSE customer_rating
    END AS customer_rating_cleaned
FROM {{ ref('stg_orders') }}
WHERE
    has_clean_error = FALSE
    OR (
        -- 唯一問題是 rating，對出貨分析無關
        ARRAY_LENGTH(clean_error_message) = 1
        AND JSON_VALUE(clean_error_message[0], '$.code') = 'customer_rating_out_of_range'
    )
```

**審計軌跡**：補值邏輯與轉換邏輯共存於 SQL 檔案，dbt model description 記錄該場景接受哪些錯誤及原因。SQL 本身即審計軌跡，進 version control。

---

## 被攔截資料的處理（Q2）

### Quarantine 的對象

Row Filter 過濾出的記錄：`Raw.status = "processed"` 且 `has_clean_error = TRUE`。  
這些記錄**已在 ODS 中**，只是在 `int_*` 層被隔離，不流入 `dim_*/fct_*`。

### Remediation：A + B 並用

A 和 B 修的是**不同類型**的問題，需明確知道用哪個：

| | 修什麼 | 路徑 |
|---|---|---|
| **A：force=True** | `Raw.status = "error"` 或 `"duplicate"` 的記錄（從未成功寫入 ODS） | `POST /process_raw/{raw_id}?force=true` → 重跑 pipeline → 自然流入下游 |
| **B：Airflow 重評估** | `Raw.status = "processed"` + `has_clean_error = TRUE` 的記錄（在 ODS，但被 quarantine） | 用新版規則重評估 → 寫 `quality_events` → 下次 dbt run 自然流入 `int_*` |

> **注意**：`force=True` 對 quarantine 記錄無效（status="processed" → 回 400）。  
> Quarantine 記錄的問題是**規則評估**，不是 pipeline 失敗，不需要重跑 pipeline。

### 狀態機

```
initial_evaluation
  ├── 通過所有規則           → to_state: "clean"
  └── has_clean_error=TRUE  → to_state: "quarantined"

quarantined
  ├── B 重評估，新規則通過   → to_state: "promoted"
  └── 人工決定放棄           → to_state: "permanently_rejected"

promoted
  └── 規則變嚴後重評估失敗   → to_state: "re_quarantined"（邊緣情況）
```

---

## 版本號與 quality_events 表（Q2 延伸）

### 規則版本號

```python
# clean.py
DQ_RULE_VERSION = "v2"    # 每次規則改動時 bump，搭配 git tag 記錄變更內容
```

### ODS 新增欄位

```
ODS（新增）
├── dq_rule_version: String    ← 攝入時使用的規則版本，之後永遠不動
```

### quality_events 表（PostgreSQL，append-only）

記錄每筆資料的品質生命週期。只增不改，是狀態機的事件日誌。

```
quality_events
├── id:           Integer (PK)
├── raw_id:       Integer
├── order_id:     String
├── event_type:   String     "initial_evaluation" | "promotion" | "rejection"
├── from_state:   String?    null | "quarantined" | "promoted"
├── to_state:     String     "clean" | "quarantined" | "promoted" | "permanently_rejected"
├── rule_version: String     "v1" | "v2" | ...
├── event_at:     DateTime
└── reason:       JSONB?     list[dict] {code, field, value, ...}，與 ODS.clean_error_message 相同格式
```

**寫入時機：**
- `process.py` 成功寫 ODS 後 → 寫一筆 `initial_evaluation` 事件
- Airflow 重評估促進記錄後 → 寫一筆 `promotion` 或 `rejection` 事件

**語意邊界：**
此表嚴格限定為全局狀態機，只記錄攝入層事件與跨層（PG → BQ）的 Proposal B 評估事件。BQ 內場景專用模型的補值決策**不寫回**此表——場景補值屬於分析層內部的業務邏輯，不是資料品質狀態的演進。

---

## 資料一致性處理

### ODS 與 BQ 的品質狀態分歧

分歧**會存在**，來源有兩種，處理方式不同：

**情境一：規則版本演進造成的分歧**
透過 `dq_rule_version` + `quality_events` 可追溯：

```
無版本號時：
  ODS has_clean_error=TRUE，BQ dim_* 是乾淨的 → 「為什麼不一樣？」無從解釋

有版本號與 quality_events 時：
  ODS has_clean_error=TRUE, dq_rule_version="v1"   ← 攝入當下的真實
  quality_events: promoted under "v2" at 2026-03-01 ← 後續演進的真實
  → 分歧有解釋，可追溯
```

**情境二：場景專用模型造成的分歧**
全局 quarantined 的記錄可能出現在特定場景的 `dim_*/fct_*` 中（因場景模型接受了與該場景無關的錯誤）。此分歧透過閱讀對應場景模型的 SQL 與 dbt description 說明，不建立獨立追蹤表。

此為有意識的設計邊界：場景補值的可解釋性需求屬靜態審計（讀 code），非運行時稽核需求，SQL 文件就夠。

### Bounded Writeback 原則

BQ（或 Airflow 讀 BQ 後）的寫回對象**只有 `quality_events`**，不修改 ODS 本身。

```
❌ 禁止：BQ → 修改 ODS 欄位（破壞錨點語意）
✅ 允許：BQ → 寫 quality_events（品質演進的 audit log，為此設計）
```

---

## 歷史品質指標（Q3）

### 層次一：即時運維指標（分鐘級）

structlog 已有基礎，延伸以下 log 事件：

```python
# process.py，寫 ODS 後
logger.info("quality_metric",
    rule_version=DQ_RULE_VERSION,
    has_clean_error=has_clean_error,
    order_id=ods_order.order_id,
    error_fields=clean_error_message,
)
```

接 Grafana（Phase 4 OTel/Loki）→ 即時 error rate、Hard Gate 觸發告警。  
不需要新元件，structlog 基礎已在。

### 層次二：批次分析指標（日/週）

`quality_events` 由 Airflow 與 ODS 一起抽取進 BQ，dbt 建立 `rpt_quality_*` 模型：

```
rpt_quality_daily
├── date, rule_version
├── total_count, clean_count, quarantine_count, promoted_count
├── quarantine_rate, promotion_rate
└── 可按 rule_version 切片，比較版本間的品質差異

rpt_quality_field_breakdown
├── 哪些欄位最常觸發 has_clean_error
├── 每個欄位的 error rate 趨勢
└── 來源：clean_error_message（JSONB 物件陣列），直接 UNNEST 讀 e.field / e.code，不需 parse 文字

rpt_quality_version_comparison
├── v1 攔截了多少 → v2 促進了多少 → 目前仍在 quarantine 多少
└── 規則改動的實際效果量化
```

接 Looker Studio，供長期趨勢分析。

### 歷史指標為何不會被追溯性改寫

`quality_events` 是 append-only，歷史事件永遠存在：

```sql
-- v1 下的初始 quarantine rate（永遠不變）
SELECT countif(to_state = 'quarantined') / count(*) AS quarantine_rate
FROM quality_events
WHERE event_type = 'initial_evaluation' AND rule_version = 'v1'

-- v2 促進了多少（另一個獨立指標，不覆蓋 v1 數字）
SELECT count(*) AS promoted_by_v2
FROM quality_events
WHERE event_type = 'promotion' AND rule_version = 'v2'
```

---

## 實作範圍對應

### ODS / PostgreSQL 層

| 元件 | 所在層 | 狀態 |
|---|---|---|
| `format_clean()` + `business_clean()` | ODS 寫入前 | ✅ 已完成 |
| `has_clean_error` + `clean_error_message` | ODS | ✅ 已完成 |
| `DQ_RULE_VERSION` 常數（`clean.py`） | ODS | ✅ 已完成 |
| `dq_rule_version` 欄位（ODS model） | ODS | ✅ 已完成 |
| `QualityEvent` model + `quality_events` 表 | ODS | ✅ 已完成 |
| `quality_events` 寫入邏輯（`process.py` success path） | ODS | ✅ 已完成 |
| structlog `quality_metric` 事件（`process.py`） | ODS | ✅ 已完成 |

**quality_events 寫入語意**

| 情況 | 寫入？ | to_state |
|---|---|---|
| ODS 成功寫入，無品質問題 | ✅ | `clean` |
| ODS 成功寫入，有品質問題 | ✅ | `quarantined` |
| pre-check 攔截 duplicate（ODS 未寫） | ❌ | — |
| TOCTOU IntegrityError（ODS 未寫） | ❌ | — |
| pipeline 失敗 → Raw status=error（ODS 未寫） | ❌ | — |

### BQ Analytics 層

| 元件 | 所在層 | 狀態 |
|---|---|---|
| dbt `stg_*` Hard Gate tests | BQ Analytics | ⬜ Phase 4 |
| `int_orders` Row Filter（`WHERE has_clean_error = FALSE`） | BQ Analytics | ⬜ Phase 4 |
| `int_orders_quarantine` dbt model | BQ Analytics | ⬜ Phase 4 |
| 場景專用 `int_orders_*` 模型（JSONB 過濾 + 補值） | BQ Analytics | ⬜ Phase 4 |
| Airflow 重評估 task（Proposal B） | BQ Analytics | ⬜ Phase 4 |
| `rpt_quality_*` dbt 模型 | BQ Analytics | ⬜ Phase 4 |

---

## 已知邊界與設計決策

**A/B Remediation 的邊界必須明確**  
`force=True`（A）只能用於 `Raw.status = "error"` 或 `"duplicate"` 的記錄。  
Quarantine 記錄（`has_clean_error=TRUE`，`status="processed"`）只能走 Proposal B 重評估路徑。  
混用會導致 `force=True` 回 400 且無法診斷原因，需在操作文件中明確說明。

**ODS 與 BQ 品質狀態永久分歧**  
ODS 永遠反映攝入當下的品質評估（以 `dq_rule_version` 記錄使用的規則版本）。  
BQ `dim_*/fct_*` 反映目前最新評估下的乾淨狀態。  
這個分歧是有意的設計決策，透過 `dq_rule_version` + `quality_events` 提供追溯能力。

**Hard Gate 閾值為業務判斷**  
目前建議值：error rate > 10% 擋住，> 5% 告警。  
實際閾值應在有真實流量資料後調整，初始值為保守估計。

**`quality_events` 目前不覆蓋 BQ 層的促進事件**  
Proposal B（Airflow 重評估）尚未實作。目前 `quality_events` 只有攝入時的 `initial_evaluation` 事件。  
Phase 4 Airflow 建立後，促進（`promotion`）與永久拒絕（`rejection`）的事件寫入邏輯需一併補上。

**場景補值審計軌跡為 SQL 文件**  
場景專用 `int_*` 模型的補值邏輯記錄在 dbt 模型 SQL 與 model description，不建立獨立追蹤表。前提是無跨系統運行時稽核的實務需求；若未來出現此需求，再評估是否引入 BQ 層生命週期表。

**`stg_*` boolean 欄位延後**  
目前場景模型直接比對 `clean_error_message` JSONB 陣列內的穩定 `code`（`code` 與人類可讀措辭解耦，措辭異動不再使查詢失效）。當相同的 `code` 條件需跨多個場景模型維護時，再於 `stg_orders` 拆解為結構化 boolean 欄位（如 `has_rating_error`），將耦合集中到單一地方。
