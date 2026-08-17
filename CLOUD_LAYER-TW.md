# 雲端層架構：ODS → BigQuery 抽取與 staging

## 範圍與職責邊界

本文件記錄「雲端層」的設計決策——即資料離開 PostgreSQL（ODS）、進入 BigQuery 之後的抽取（E/L）與落地（staging）。轉換層（dbt `stg_`→`int_`→`dim_/fct_`→`rpt_`）的品質契約見 [DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md)。

```
ODS (PostgreSQL) ──[ E/L：Python ]──► BigQuery staging ──[ T：dbt ]──► stg_/int_/dim_/fct_/rpt_
```

**為什麼 E/L 與 T 切兩段**：兩者「失敗語意」不同。E/L 失敗要從 watermark 續傳、要冪等；T 失敗只要重跑 SQL。混在一起會讓兩種錯誤糾纏。

---

## 1. Staging 表設計

### 1.1 批次載入，不用串流

staging 由抽取腳本以 batch load job 灌入。BQ 的 streaming insert 按量計費，而 batch load job 免費；本專案是 T+1／小時級批次，沒有即時性需求，故一律走 batch load。串流只有在下游接即時預測模型時才有動機。staging 因此是一張由批次累積而成的實體表（ODS 的 append 鏡射）。

### 1.2 分區：`received_at`（DAY）

分區欄位選法的原則是「選那張表最常、最貴的查詢拿來過濾的欄位」。staging 的 access pattern 是**管線增量**（抽取 watermark、dbt incremental 都過濾 `received_at`），所以分區用 `received_at`，讓每次跑批只掃新增的分區（partition pruning）。

> **`received_at` vs `order_date`**：staging 服務管線，故用 ODS 的落地時間 `received_at` 分區（⚠️ 這個欄位不是收單時刻，見 §1.2.2）；下游 Gold（`dim_/fct_`）服務分析師，月/週平均按業務時間 `order_date` 過濾，那一層才改用 `order_date` 分區。分區欄位是「每張表依自己的 access pattern 各自選」。**Gold 側的完整決策見下方 §1.2.1**——那不是把本節照抄一遍，四個決定各有各的理由，且其中兩個與 staging 相反。

粒度選 DAY 不選 HOUR：批次是 T+1／小時級；且單表上限 4000 分區，DAY 可撐約 11 年、HOUR 僅 166 天。

### 1.2.2 ⚠️ `received_at` 在 Raw 與 ODS 是兩個不同的時刻 ⭐

這個欄位名在兩張表上意思不同，而下游有三個機制建立在它上面，所以必須寫清楚：

| 欄位 | 何時蓋章 | 語意 |
|---|---|---|
| `raw.received_at` | API 在請求路徑中同步寫 Raw 時（`models.py` `server_default=func.now()`） | **收單時刻** |
| `ods.received_at` | worker 寫 ODS 時（同樣吃 `server_default`；`process.py` 建 ODS 未帶入 Raw 的值） | **ODS 落地時刻** |

**語意是對的，不是妥協。** staging 是 `extract_ods_to_bq.py` 的鏡射，而 extract 搬的就是 ODS——用 ODS 自己的時鐘當分區欄位與 `loaded_at_field`，回答的正是「extract 有沒有把 ODS 往前搬」這個問題。若改用 `raw.received_at`，反而會把「Raw→ODS 那一段的延遲」混進 extract 的檢查裡，一個訊號同時代表兩段管線。

**但它有一個必須知道的範圍邊界**：積壓被恢復掃描沖出去時，那批列的 `ods.received_at` 是**回補當下**的寫入時刻，於是攝入中斷的斷層在 ODS 的時間軸上**不存在**。因此任何建立在 `ods.received_at` 上的觀測（分區、freshness、`rpt_quality_events_daily` 的日界）都只看得見「取樣當下仍在進行中」的中斷，看不見已經恢復的。

派工那一段的健康由別的東西回答：`raw.status='pending'` 的最舊年齡（由後續新增的 `raw_pending_watch` DAG 負責），以及 `raw.received_at` 的連續性（OTel 管線已於 2026-08-17 上線，但 **absent 告警尚未寫**，見 ORCHESTRATION-TW §4）。**三個時間軸各管一段，不要讓其中一個兼差。**

⚠️ 順帶釐清一個容易寫錯的判準：**不能用「Raw 有列但沒有對應的 ODS 列」當故障定義**。Raw 的終態是 `processed` / `duplicate` / `error`，後兩者不產生 ODS 列而且那是正確行為，照那個定義做檢查會讓每筆重複訂單都變成告警。`pending` 才是乾淨的訊號——它表示還沒有任何 worker 取走這一筆。

**為什麼不改成帶入 `raw.received_at`**：第一理由是上面那句——語意本來就是對的，改了才會錯。次要理由才是代價：分區欄位語意一變就要重建表與回填，且 Hard Gate 的「最新 UTC 日分區」口徑會跟著改。

⚠️ **欄位名容易被照字面讀成收單時刻。** 不改名（那是一次 migration，且會波及 ODS→BQ 的 `FIELDS` 宣告與 dbt 的所有引用），但下一個讀到 `ods.received_at` 的人請以本節為準，不要從欄位名推論。

### 1.2.1 Gold（`dim_/fct_`）的分區決策 ⭐

四個決定，逐條與 staging 對照：

| 決策 | staging | Gold `fct_*` | Gold `dim_*` | 為什麼不同 |
|---|---|---|---|---|
| 分區欄位 | `received_at` | **`order_date`** | **不分區** | 事實表服務分析師的業務時間過濾；維度是**按鍵 join** 進來的，分區對 join 沒有裁切作用，只換來一堆小分區與 metadata 開銷 |
| 粒度 | DAY | DAY | — | 同 §1.2 |
| 保留政策 | 60 天（sandbox 強制） | **5 年**（`var` gated） | — | Gold 要留全歷史，但 DAY 粒度受 4000 分區上限約束（約 11 年）→ 必須有明確保留政策，否則第 11 年撞頂 |
| `require_partition_filter` | ✅ 開 | ❌ **關** | — | 見下 |

#### 為什麼 Gold 不上保險絲

staging 開它零副作用（§1.4：存取一律帶 `received_at`）。但 Gold 服務分析師 ad-hoc 查詢與 Looker Studio 的**探索式查詢**，那些查詢常常不帶日期過濾——開了就是一律 400。這與 dbt README §4.6 的「保險絲傳染」是同一個張力，只是這次發生在最下游、面對的是人而不是管線。

代價是放棄了「防單次誤觸全表掃」。替代防線是 BQ 的 **custom quota**（per-user/per-project 每日掃描上限）。這兩者防的**不是同一件事**：保險絲防單次失誤（每次查詢都問「你帶過濾了嗎」），quota 防系統性濫用（累計超標才擋）。對一個開放給分析師探索的層，後者才是對的形狀。

#### 分區到底省多少：實測（2026-08，540 筆）

同資料建兩張表，跑「近 30 天切片」這個分析師典型查詢：

| | totalBytesProcessed | 相對全表 |
|---|---|---|
| 全表 | 68,856 B | 100% |
| 只 `cluster by order_date` | 12,474 B | **18%** |
| `partition by order_date` + cluster | 6,490 B | 9% |

**clustering 單獨就裁掉了 82%，分區再往下拿 9 個百分點。** 所以「分區大幅省成本」這個常見說法需要修正——分區的價值不在裁切量，而在 clustering 給不了的三件事：

1. **成本可預測**：分區裁切在查詢前由 metadata 決定，`dry run` 的 bytes 是準的；clustering 是 block 級、視資料排列而定，`dry run` 會高估。要做成本控管靠的是前者。
2. **`require_partition_filter` 的前提**：只有分區表能上（雖然 Gold 選擇不上）。
3. **分區級操作**：`insert_overwrite` 整分區原子替換、單分區 targeted refresh——`stg_` 的 runbook 全靠這個。

> 附帶：BQ 對每查詢每表有 **10 MB 最低計費**，故在本專案目前的資料量上兩版帳單完全相同。分區的效益只在「假設數千萬～數億筆」的前提下成立——這個前提是刻意宣告的（本專案是實務模擬），不是實測結論。外推見 dbt README §5.4 的每列成本表。

### 1.3 叢集：`order_id` + `has_clean_error`

分區內再依叢集欄位排序聚集，過濾這些欄位時跳過不相關區塊。選 `order_id`（下游 JOIN／去重，高基數）優先、`has_clean_error`（`int_` 的 Row Filter 每次必走）次之。

### 1.4 保險絲：`require_partition_filter=True`

任何查 staging、沒帶 `received_at` 過濾的查詢直接報錯，擋掉「不小心全表掃」的燒錢意外。staging 的存取一律帶 `received_at`，故對它幾乎零副作用。

> **連帶效應**：保險絲會擋掉 `SELECT MAX(received_at) FROM staging` 這種無過濾查詢——直接影響 watermark 讀法（見 §2）。

### 1.5 location 一致：`US`

BQ 每個 dataset 建立當下綁定 location 不可改；跨 location 查詢會直接報錯。所有 dataset（staging、dbt_dev、未來 dim/fct）統一建在 `US`，建 dataset 時明確指定、不靠預設。

### 1.6 第二張 staging 表：`quality_events`（與 orders 的刻意差異）⭐

`orders` 之外，抽取腳本同時把 `quality_events`（append-only 品質事件日誌）抽上 staging。**為什麼要抽**：下游 `int_*` 合成「有效品質狀態」時，要把 ODS 快照與 `quality_events` 最新事件 JOIN 起來（Proposal B promote 的記錄在 ODS 仍是 `has_clean_error=TRUE`，靠事件才流得回 Gold）——沒有這張表，回流機制的右表就不存在（見 [DQ_ARCHITECTURE-TW〈機制二：Row Filter〉](./DQ_ARCHITECTURE-TW.md)）。

它的表設計**不照抄 orders**，因為 access pattern 相反。§1.2–1.4 的每個決定都要重問一次：

| 決策 | orders | `quality_events` | 為什麼不同 |
|---|---|---|---|
| 分區 | `received_at`（DAY） | `event_at`（DAY） | 各表用自己的落地時間軸（⚠️ `received_at` 是 ODS 的落地時刻而非收單時刻，見 §1.2.2）；`event_at` 同時餵 watermark（方案 A 讀最新分區）|
| 叢集 | `order_id` + `has_clean_error` | `raw_id` + `to_state` | 下游以 **`raw_id`** 為 grain 取「每筆記錄的最新狀態」（與 dbt `stg_` 去重同鍵）；`to_state` 供狀態過濾 |
| 保險絲 | ✅ 開 | ❌ **關** | **關鍵差異**：orders 的查詢永遠帶 `received_at` 過濾；但 `quality_events` 的主消費者是「跨全歷史按 `raw_id` 取最新」，本質是**非分區過濾的全掃描**，開保險絲會直接擋掉這個必然查詢 |

> **回流比 orders 乾淨**：Proposal B 補的 promotion 事件 `event_at = now()`，落**當天**分區，例行增量 `event_at >= watermark` 自然撈得到；不像 orders 修正列落回**舊**分區、需要修復 runbook 主動補推（見 §7.1）。append-only 的時間語意讓 `quality_events` 的 E/L 反而更單純。

> **跨表一致性見 §3.2**：兩張表獨立抽取、獨立 watermark、獨立 load job；「orders 上了但 `quality_events` 沒上」怎麼防，見〈跨表一致性〉。

> **60 天過期上限（sandbox 限制）**：因本專案為練習用途、不啟用帳單，跑在 BQ sandbox，dataset 被強制套 60 天分區＋表過期，`quality_events` 也繼承此設定；故「跨全歷史取最新」的假設在 sandbox 下實際上限為 60 天——這是帳號層限制（腳本設 `expiration=None` 也會被 sandbox 忽略），啟用帳單後才解除。**完整實測見 §1.7**；該限制對 Gold 的業務時間軸分區後果嚴重得多。

### 1.7 sandbox 分區過期：實測記錄（2026-08）⭐

§1.6 只記了「`expiration=None` 會被忽略」。但 Gold 改用**業務時間軸**分區之後，這條限制的後果完全不同，故補一份完整實測。**下面每一項都是量出來的，不是推論。**

#### 1.7.1 過期按「分區的日期值」算，不是建表時間

四個 dataset（`staging`、`dbt_dev`）的 `default_partition_expiration_ms` 全部是 `5184000000`（60 天），既有分區表也全部繼承。實測：建一張 `partition by order_date` 的表，一次 CTAS 灌入橫跨 60 天界線的五個日期——

| 分區 | 結果 |
|---|---|
| 2024-01-01 | **rows=0，已消失** |
| 2026-05-01（94 天前） | **rows=0，已消失** |
| 2026-06-04（界線上） | rows=1 ✅ |
| 2026-07-01（33 天前） | rows=1 ✅ |
| 2026-08-03（今天） | rows=1 ✅ |

三個關鍵行為：**① 建表不失敗**（`CREATE OR REPLACE` 回傳成功）；**② 一個「2024-01-01」的分區在誕生的瞬間就已超過 60 天**；**③ 刪除同步且即時**——CTAS 回傳後立刻查，兩個舊分區已不在 `INFORMATION_SCHEMA.PARTITIONS`，連 `num_rows` metadata 都直接讀到 3 而不是 5。沒有 warning。

> `stg_orders` 沒踩到這個，純粹是因為它按 `received_at` 分區、攝入時間永遠是近期。**換成業務時間軸，這層保護就消失了。**

#### 1.7.2 過期上限鎖死在 60 天，四條路全封

| 做法 | 結果 |
|---|---|
| DDL `options(partition_expiration_days = 3650)` | ❌ job 失敗 |
| DDL `options(partition_expiration_days = NULL)` | ⚠️ **不報錯，靜默改寫成 60 天** |
| API 改 `table.time_partitioning.expiration_ms` | ❌ 403 |
| API 改 dataset `default_partition_expiration_ms` | ❌ 403 |

錯誤原文：

```
reason: billingNotEnabled
Partition expiration time must be less than 60 days while in sandbox mode.
```

**這是 `gold_partition_expiration_days` 必須用 `var` gate 住、預設不輸出的原因**——寫死 1825 會讓每次 `dbt run` 失敗、`dbt build` 的下游全部 skip。

> **一個要知道但不能用的漏洞**：`partition_expiration_days = 3650` 的 DDL 其實是**半成功**的——job 標記為失敗（`state=DONE`、`error_result.reason=billingNotEnabled`），但表確實被建出來、`expiration_ms` 真的是 3650 天、舊列存活可查詢、等 60 秒也沒被回收。管制擋在 **job 驗證層**，DDL 的副作用漏了過去。不能用：job 失敗等於 dbt 失敗；且這是管制漏洞而非支援路徑，補上之後表會開始無聲被回收。

#### 1.7.3 超出合法區間的日期：靜默落 `__UNPARTITIONED__`，**不會炸表**

**這一項推翻了本文件 §8 的舊記載**（原文：「離譜的未來日期會超出 BQ 分區可接受範圍、讓整張表建立失敗」）。實測：

```
partition_id=20260803           rows=1
partition_id=21591231           rows=1
partition_id=__UNPARTITIONED__  rows=3   ← 1959-12-31 / 2160-01-01 / 9999-12-31
建表成功，5 列全部存活可查詢
```

超出 `1960-01-01 ~ 2159-12-31` 的值不報錯，靜默進 `__UNPARTITIONED__`。連帶：這些列同樣**逃過 60 天回收**，且永遠無法被 partition pruning 裁掉。

因此 `dim_/fct_` 採 `order_date` 分區**不需要**合法區間守衛。（`int_orders_quarantine` 刻意不分區的決定仍成立，但理由只剩「`int_` 只被 DAG 內部消費、分區收益 ≈ 0」那一條。）

#### 1.7.4 `__NULL__` 分區逃過回收

`order_date` 在 ODS 是 nullable。NULL 落 `__NULL__` 分區，該分區沒有日期、算不出過期時間，故**不被回收**——實測中它與 2024-01-01 同批寫入，後者當場消失、它存活。後果：`fct_orders` 裡沒有 `order_date` 的訂單會比有日期的活得久。目前資料 0 個 NULL，但 schema 允許。

#### 1.7.5 兩個 60 天時鐘掛在不同軸上 ⭐

這是上述現象裡對**測試設計**影響最大的一條：

```
int_orders ← stg_orders ← staging.orders    按 received_at 過期
fct_orders / fct_order_items                按 order_date  過期
```

所以「`fct_orders` 的內容 == `int_orders` 的內容」這個不變式**不能寫成 `count(*) = count(*)`**：兩表保留期本來就不同，即使綠也是「兩個 reaper 剛好同步」而非「SQL 對」。更糟的是回收時機不同步——`fct_orders` 是 `CREATE OR REPLACE`、回收同步發生；`stg_orders` 是 incremental、舊分區靠 BQ 背景 reaper 非同步回收——邊界日必然對不上，測試會變成每天固定紅一陣子的 flaky test。

解法是**把不變式錨在 `order_date` 窗上**（反向 join，見 `tests/assert_fct_orders_complete_projection.sql`），並讓 `load_test.py` 產生的 `order_date ≈ received_at`（見該檔 `ORDER_DATE_LOOKBACK_DAYS`）使兩軸對齊。**舊版產生器把 `order_date` 寫死在 2024 年、與 `received_at` 平均相差 410 天，那會讓新灌的資料一進 Gold 就被回收、測試永久紅。**

#### 1.7.6 BQ 側是滾動 60 天的「攝入視窗」，ODS 無法回填 ⭐

前五項談的是分區怎麼被回收。這一項談那件事對**整條管線的形狀**做了什麼——**這是最容易被遺忘、卻最會誤導未來決策的一條。**

抽取腳本把 ODS 的 `received_at` **原值**寫成分區欄位（`extract_ods_to_bq.py` 的 `FIELDS` 與 `partition_field`）。所以：

> **任何 `received_at` 超過 60 天的 ODS 列，無論重抽幾次，都會在落地當下被回收。**

ODS 在 PostgreSQL 仍是永久且完整的錨點（`raw`/`ods`/`quality_events` 一筆不少）。但**在 BQ 這一側，管線結構上就是一個滾動的 60 天攝入視窗**——不是「保留最近 60 天的資料」，而是「只裝得下最近 60 天*被攝入*的資料」。歷史資料要回到 BQ，唯一的路是**重新攝入**（產生新的 `received_at`），不是重抽。

三個直接推論：

1. **「只清 BQ staging 再重抽」是無效的捷徑**。`get_watermark()` 會因查不到分區而回 `None`、觸發全量抽取（§2.1），但抽上去的舊列一樣當場蒸發，結果與清除前相同。要讓 BQ 有資料，只能灌新的。

2. **staging 清空後，抽取會靜默降級成「每次全掃 ODS」，而且回報成功**。watermark 恆為 `None` → 每輪都是全量；load job 回報 `output_rows = <ODS 全部列數>`，而 **E/L gate 只檢查 load job 有沒有拋例外，沒有事後 `SELECT COUNT(*)` 驗證**（§3.2）——於是會出現「每次跑批都成功載入 N 筆、表卻永遠是空的」這種**綠燈說謊**的狀態。要防它，gate 需補一個載入後的實際列數檢查（目前未實作）。

3. **`dbt source freshness` 會先於一切變紅，而且它量的不是你以為的東西。**

   `loaded_at_field` 指向 `received_at`，那是 **ODS 的落地時刻**（不是收單時刻，見 §1.2.2）；`ORDERS_FIELDS` 裡**沒有任何抽取時間欄位**（整份是 ODS 的 1:1 鏡射）。所以這支檢查回答的是「最新一筆訂單多久以前**進到 ODS**」，**不是**「抽取工作多久沒跑」。

   後果是兩種完全不同的失敗長得一模一樣：

   | 失敗 | 症狀 |
   |---|---|
   | (a) 上游停止送訂單（業務流斷了）| `max(received_at)` 不前進 → ERROR STALE |
   | (b) 抽取工作掛掉（管線斷了）| `max(received_at)` 不前進 → ERROR STALE |

   **這也意味著：Phase 5 把抽取排進 Airflow 並不會讓它變綠。** 排程的是搬運工不是產生器——ODS 沒有新訂單，就沒有新的 `received_at`。要它綠只有持續攝入一途（≤26h 一批）。想分辨 (a)/(b) 則需要另一個訊號（例如加一個 `_extracted_at` 欄位，走 §5.2 的 `ALLOW_FIELD_ADDITION`），屬 Phase 5 之後的事——目前抽取是手動的，(b) 根本不存在。

   **NULL 時優雅降級，不是硬錯誤**：`filter: received_at > now - 30 天` 這個窗在停止攝入 30 天後會變空，`max()` 回 NULL。dbt 對此有守衛（`dbt/adapters/base/impl.py` 的 `_create_freshness_response`：`if last_modified is None → datetime(1,1,1)`，註解為 "Interpret missing value as infinitely long ago"），`loaded_at_field` 路徑共用同一段，**不會 crash**，只是 `max_loaded_at` 顯示成 `0001-01-01`、結果仍是 ERROR STALE。

> 對比 §7 的 Proposal C：那裡談的是「修正列落在舊分區、watermark 看不到，所以要主動推」。這裡更強一階——在 sandbox 上，**舊分區根本不存在可推的目標**。§7.1「上雲是主動步驟」的結論不變，但 sandbox 下它的可行範圍被壓縮到 60 天內。

#### 1.7.7 本專案的立場：freshness 紅是預期狀態 ⭐

本專案的攝入是**手動的**（用 API 灌，`load_test.py` 只作壓測），不是持續流量。所以在兩次灌資料之間，`dbt source freshness` **必然是 ERROR STALE**——實測 2026-08-03 時已 stale 625 小時，對 `error_after: 50h` 超標 12.5 倍。

**這是接受的狀態，不是待修的缺陷**，理由有二：

1. **閾值描述的是被模擬系統的服務等級，不是模擬者的操作習慣。** 26h/50h 對一個真實持續收單的電商是合理的 SLA；把它放寬到 30 天只會讓這份設定對「這個系統該多快被餵」說謊。**寧可讓訊號誠實地紅，也不要為了好看而調鬆閾值。**
2. **它不阻礙任何運作。** `dbt build` 不含 freshness（`build` = run + test + snapshot + seed，freshness 是獨立指令），watermark 讀的是分區而非 freshness 結果，所以「灌資料 → 抽取 → `dbt build`」這條路徑完全不受影響。

⚠️ **但第 2 點是一個前提，不是一個保證。** 這個立場能成立，完全因為 freshness **沒有被接成阻斷式 gate**。所以：

| 規則 | 為什麼 |
|---|---|
| **Phase 5 的 Airflow DAG 不得把 `dbt source freshness` 放在抽取／`dbt build` 之前當前置檢查** | 同一個紅會立刻從「可接受的告警」變成「DAG 永久卡死」，而它反映的只是「你這幾天沒手動灌資料」 |
| ~~要納入 DAG 的話，只能是**旁路的觀測 task**（失敗不影響下游），或先降 `severity`~~ → **實作時再收緊一階：獨立成 `source_freshness_watch` DAG** | 旁路 task 還不夠——見下方〈實作結果〉 |
| ~~若日後改成**持續攝入**，本立場即失效，freshness 應恢復為有意義的 gate~~ → **2026-08-11 條件成立**（`seed_demo_daily` 每天四批），freshness 已由「預期恆紅」轉為「預期常綠」 | 紅現在確實代表壞了。**但仍不接成 gate**，理由已換一個——見下方 |

> **2026-08-05 實測補充**：灌完一批資料 15 分鐘後，由 `source_freshness_watch` DAG 跑
> `dbt source freshness`，兩個 source 皆 **PASS**。所以本節這個立場不是「我們調鬆了標準」，
> 而是「餵了就綠、沒餵就紅」——**訊號本身一直是誠實的**，紅的時候確實在說一件真的事，
> 只是那件事是「你最近沒餵它」而不是「管線壞了」。完整驗證見
> [ORCHESTRATION-TW §5.3](./ORCHESTRATION-TW.md)。

> 這與 DQ 架構對 `has_schema_drift` 的處理同構：**訊號的價值不等於它該有的權限**。drift 只能告警不能攔截；freshness 在「手動攝入」這個前提下同樣只能告警。權限來自「紅的時候是不是真的壞了」，而不是來自「這個指標重不重要」。

附帶：持續攝入同時也是 §1.7.6 那個「滾動 60 天攝入視窗」問題的解方——兩者**根因相同**（沒有持續攝入），所以未來若決定加 seeding，一次解決兩件事。

**⚠️ 條件成立了，但結論只變了一半（2026-08-11）**

上表原本預期「持續攝入 → freshness 恢復為 gate」。持續攝入實現了（`seed_demo_daily`），
前半成立：紅不再只代表「你沒餵它」。但**後半刻意不執行**，因為理由已經換了一個：

> **seeding 是這個系統唯一的資料來源。所以 seeding 掛掉的那天，就是「沒有新資料」的
> 那天——分析管線在舊資料上跑一次是無害且正確的，擋住它一點好處都沒有。**

freshness 因此從「一個因為前提不成立而不該有權限的訊號」變成「一個前提成立、但**阻斷
本身沒有價值**的訊號」。**結論相同，論證不同**——這個區別要寫清楚，否則下一個人會以為
條件成立後就該把它接成 gate。

另外「持續攝入」在此有限定：**一天四批，不是 24 小時連續**。26h/50h 的閾值在這個節奏下
餘裕很大（最壞只隔 13 小時，即 21:00 那批到隔日 10:00），偵測得到「整天沒進 staging」，
偵測不到「峰期停了三小時」。

⚠️ **上一版這裡寫「真實連續攝入下閾值要重訂」，那句話是錯的，已刪。** 閾值的來源是
【**載入**節奏】而非攝入節奏——staging 一天只被 extract 推一次，資料設計上就有最多 24 小時
的年齡，所以 `26 = 24 + 2`、`50 = 48 + 2`。攝入變成 24 小時連續**不會改變這個算式**，
只要倉儲仍是夜間批次載入。會改變它的是 extract 改成小時批或串流。
（完整推導見 [ORCHESTRATION-TW §2.7](./ORCHESTRATION-TW.md)。）

而「偵測不到峰期停三小時」也不是閾值的問題，是**範圍**的問題：freshness 量的是
`ods.received_at`＝extract 那一跳（見 §1.2.2），攝入中斷本來就不歸它管——那由
`raw_pending_watch` 與 OTel 的 absent 告警回答（後者尚未寫，見 ORCHESTRATION-TW §4）。

**實作結果（Phase 5）：旁路 task 不夠，必須獨立成一條 DAG** ⭐

上表原本寫「旁路的觀測 task（失敗不影響下游）」。真的要落地時發現這還不夠：
**Airflow 的 DAG run 狀態是所有 task 的彙總**，一個預期會紅的 leaf task 會讓
`orders_analytics_daily` 恆為 failed —— 於是「主管線成功率」這個訊號的價值歸零，
真正的管線故障被淹沒在每天都紅的噪音裡。

所以本節那句話要再往前推一步：**freshness 不只沒有「阻斷下游」的權限，也沒有
「污染別人成功率」的權限。** 落地形態是獨立的 `source_freshness_watch` DAG
（`orchestration/dags/`），兩條 DAG 的成功率各自代表一件事：

| DAG | 紅代表 |
|---|---|
| `orders_analytics_daily` | 管線壞了 |
| `source_freshness_watch` | staging 不新鮮＝extract 沒把 ODS 搬過去（2026-08-11 起由「預期恆紅」轉為「預期常綠」，見上方 §1.7.7 表格的更新）|

`tests/test_dags.py::TestFreshnessIsolation` 把這條隔離釘住：任何有實際產出的 DAG
混進 `dbt source freshness`，測試就紅。

#### 1.7.8 sandbox 的 60 天分區過期是【無條件】的，不是只鎖顯式設定 ⭐

2026-08-04 實測 `dbt_dev` 的 `INFORMATION_SCHEMA.TABLE_OPTIONS`：

| 表 | 有沒有設 `partition_expiration_days` | 實際值 |
|---|---|---|
| `stg_orders` / `stg_quality_events` | ❌ 模型內從未設定 | **60** |
| `fct_orders` / `fct_order_items` | ❌ `gold_partition_expiration_days=null` → 該選項根本不輸出 | **60** |
| `rpt_sales_daily_by_category` / `rpt_quality_events_daily` | ❌ 同上 | **60** |

**結論：`none` 不等於「不過期」——sandbox 會替每一張分區表補上 60 天。**
§1.7.2 記的「sandbox 硬鎖 < 60 天」只描述了顯式設定被拒的那一半；另一半是**未設定也照樣被套**。

**這已經造成實際後果**（同日實測）：

```
int_orders   540 筆（order_date 2024-01-01 ~ 2026-07-01，不分區故全留）
     ↓ 掉 333 筆
fct_orders   207 筆（order_date 2026-06-05 ~ 2026-07-01，剛好 60 天窗）
```

落差全是 `order_date` 落在 60 天窗外的列——其中絕大多數是舊版 `load_test.py` 寫死
`date(2024,1,1)` 產生的資料，一進 Gold 就被回收（該檔 `ORDER_DATE_LOOKBACK_DAYS`
的註解已預告這個災難，修正已落地但**尚未重新灌資料**）。

**這也解釋了 `assert_fct_orders_complete_projection` 為什麼必須帶 `order_date` 窗**：
那不是防禦性設計，是**唯一能讓它成立的寫法**。`int_` 不分區故不受回收影響、`fct_` 受影響，
無窗的 `count = count` 在 sandbox 下必然永遠紅。

啟用帳單後這個強制值解除，`gold_partition_expiration_days` 才真正生效（屆時
`gold_projection_window_days` 應一併調整為與保留政策一致）。

#### 1.7.9 staging 的保留期不是自由參數——Proposal B 對它有硬性下限 ⭐

前面幾節把 60 天當成「sandbox 加諸的限制」。但實作 Proposal B（`reevaluate_quality.py`）時
浮現一件事：**就算啟用帳單、限制解除，staging 的保留期也不能隨便設短。**

理由是重評估的候選來自 BQ 的 `int_` 層，而 `int_` ← `stg_` ← `staging.orders`。
Proposal B 的典型觸發是「規則放寬 → 撈回**跨全歷史**的舊 quarantine」——保留期一旦短於
「最舊的、還可能被撈回的 quarantine」，那批記錄在 BQ 這一側就不存在，重評估掃不到它，
而且**不會報錯**：查詢正常回傳、任務正常成功、只是少了一批。這與 §1.7.6 的「綠燈說謊」同型。

所以本專案的立場是：**staging 是有保留期的鏡射，但那個保留期是被 Proposal B 的
回溯範圍決定的，不是儲存成本說了算。** 決定值時的判準是一句話——
「我們願意回溯重評估多久以前的資料？」保留期至少要等於那個答案。

> 對照：ODS（PostgreSQL）永久且完整，所以 `permanently_rejected` 這類**永不回頭**的決定
> 即使超出 staging 保留期也不會出問題；會出問題的只有「還想撈回來」的那些。
> 這也是為什麼重評估的**狀態判定**讀 PG 而非 BQ（見 DQ_ARCHITECTURE-TW 的 Proposal B 段）——
> 冪等的保證不能建立在一個有保留期的鏡射上。

---

## 2. Watermark 策略

### 2.1 方案 A：從 `INFORMATION_SCHEMA.PARTITIONS` 推導

```sql
SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id))
FROM `<project>.staging.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders' AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
```

特性：**免費**（metadata，不掃資料）、**不受保險絲限制**（查的是 metadata view 非表本體）、**無狀態**（watermark 由 staging 自身推導，load 完下次讀即反映新資料，故沒有 `advance_watermark()` 步驟）。邊界用 `>=`，配 dbt `stg_` 去重 → 寧可重抓不漏抓。

### 2.2 `get_watermark()` 抽象＝換方案 B 的唯一接縫

方案 A 的精度被分區粒度卡死（DAY → 每次重抽「最新分區整天」）。批次頻率拉高時：

| 批次間隔 | 方案 A | 對策 |
|---|---|---|
| 日批 / T+1 | ✅ 重抽量微不足道 | A |
| 小時批 | ⚠️ 每跑重抽當天至今 | 改 HOUR 分區（受 4000 上限約束，需設過期）|
| 分鐘級微批 | ❌ 同日重抽數百次 | **方案 B**（獨立 watermark 表，精確到 timestamp）|

判準：**批次間隔 ≈ 分區粒度 → A；批次間隔 ≪ 分區粒度 → B。** 換 B 時只改 `get_watermark()`、並在 load 成功後新增 `advance_watermark()`，`main()` 不動。方案 B 的代價是狀態管理（存放位置、load-then-advance 順序不變式、bootstrap、失敗面、併發），金錢成本仍 ≈ 0。另有更硬的天花板：batch load job 每表每天 1500 個，逼近每分鐘批次。

---

## 3. 載入策略

### 3.1 單表落地語意

- **WRITE_APPEND**：冪等靠 append + dbt `stg_` 去重，不在 E/L 做 MERGE（保持原樣落地）。
- **JSON 欄位傳原生物件，非 `json.dumps`**（實機驗證結論）：psycopg2 解 JSONB 本就是 list/dict，直接傳，client 寫 NDJSON 時嵌成原生 JSON，BQ 存成 `JSON_TYPE=array/object`。若用 `json.dumps` → BQ 存成 JSON 字串純量，下游 `[0]` 索引失效。
- **`ALLOW_FIELD_ADDITION`**：支援 additive evolution，見 §5。

### 3.2 跨表一致性：per-table load job + gate（無跨表交易）⭐

多表抽取（orders + `quality_events`）帶出一個地端沒有的問題：**BQ 的 load job 只保證「單表」原子，跨表沒有交易**——無法像地端 Postgres 把兩張表的寫入包進同一個 commit。於是要防「orders 上去了、`quality_events` 沒上」導致 dbt 建在半套資料上。

不靠原子落地，靠**兩層防線**：

1. **各表獨立 watermark、失敗不推進**：每張表的 watermark 由自己的 staging 分區推導（方案 A，§2）。一張 load 失敗，它的 watermark 就沒前進，下輪 `event_at/received_at >= watermark` 自動把那批補抽（append-only + `>=` + dbt `stg_` 去重）。orders 成不成功，完全不影響 `quality_events` 的 watermark——這個獨立性正是自癒的來源。
2. **`main()` 的 gate**：逐表盡力抽取（一張失敗不擋另一張各自推進），最後彙整——**任一張失敗即整體 `raise`（非零 exit）**，下游 dbt(T) 不得開跑。現在手動階段是「全成功才接著跑 dbt」；Phase 5 Airflow 則落成「dbt task 的上游依賴＝兩個 extract task 都 success」，同一條 gate 語意。

**一致性模型是「最終一致」，非交易**：skew（一張到、一張沒到）只會造成**延遲**（某筆該回流的髒單晚一個 dbt run），不會造成髒資料——前提是下游 `int_*` 的 JOIN 寫成保守合成（事件缺席 → fall back 到 ODS 快照：乾淨照流、髒的續留 quarantine）。這也是維持「兩張獨立 load job」而非硬湊跨表原子的理由：獨立才好各自自癒、各自重試。

---

## 4. 設定與安全

- **`bq_project` 由 `Settings` 注入**，不寫死在模組：project ID 會隨部署環境而異（dev/prod 不同專案），屬 `config.py` 既定的「環境設定」邊界。它**不是機密**（安全靠 IAM 不靠隱密），但作為公開 repo 的基礎設施座標，注入可讓真實 ID 不入版控。
- **認證走 ADC**（`bq.py`）：本機讀金鑰路徑橋接環境變數，正式環境由平台注入，同程式碼零修改換環境（prod-parity）。
- **陷阱**：BQ client 要的是 project **ID**（GCP 常自動補數字後綴，如 `-498602`），不是顯示名稱。

---

## 5. ODS Schema 演進策略 ⭐

### 5.1 上游漂移 ≠ ODS 改動

攝入層**刻意容忍**上游 schema 漂移（見 DQ 兩訊號治理）：多送的欄位進 `unmapped_fields` + `has_schema_drift`；少送的落 NULL；型別漂移標 `TYPE_DRIFT`。**ODS 的欄位結構不會自己跟著變**——drift 只是訊號。ODS 真正演進，是工程師**刻意**經 Alembic migration 加/改欄。

### 5.2 BQ 能就地 migrate 什麼

| ODS 變更 | BQ 能就地? | 雲端層做法 |
|---|---|---|
| 加 nullable 欄 | ✅ `ALLOW_FIELD_ADDITION` | staging 自動接 |
| REQUIRED→NULLABLE | ✅ relaxation | staging 放寬 |
| 刪欄 | ✅ DROP（但丟歷史）| **不刪**，留著，dbt `stg_` 忽略 |
| 改名 | ✅ RENAME | **不改**，加新欄，dbt `stg_` rename |
| 型別不相容 | ❌ | 加新欄 + dbt cast |
| 改分區/叢集 | ❌ | 重建表（CTAS）|

### 5.3 刻意的紀律：staging 只做加法，改名/轉型丟給 dbt

即使 BQ 做得到 DROP/RENAME，staging 仍**刻意**選「只加不動」：① 保留歷史；② BQ DDL 無版控、不像 Alembic，把改名/轉型放進 **dbt `stg_`** SQL 才有 git 版控與 review；③ 解耦實體演進（罕見、只加）與邏輯演進（頻繁、在 SQL）。

> **不對稱**：ODS 有 Alembic 這個正式 migration 框架；staging **沒有對應框架**（dbt 從 `stg_` 才接手，不管 staging 本身）。實務上以 `ALLOW_FIELD_ADDITION` 補加欄、其餘交 dbt 作為替代。唯一「必須重建」的情況（改分區）在方案 A 下成本很低：`drop + recreate + 重抽`，watermark 自動歸零。

### 5.4 治理：每張表一份 `FIELDS`（schema 的第三份宣告）

每張 staging 表的 `FIELDS` 是該表 schema 繼 `schema.py`、`models.py` 之後的第三份手維護宣告，且漂移時最糟是「靜默漏抽資料」。抽取腳本用一個 `TableSpec` 把每張表的抽取契約收成一份物件——`table` / `model` / `time_col` / `fields` / 分區 / 叢集 / 保險絲——目前有兩份：`ORDERS_SPEC`（鏡射 `ODS`）與 `QUALITY_EVENTS_SPEC`（鏡射 `QualityEvent`，見 §1.6）。

每份 `fields` 同時驅動三處（單一真相來源）：BQ schema（`ensure_staging_table`）、列序列化（`_to_bq_dict`）、與一致性測試。`tests/test_schema_bq_consistency.py` 以 `SPECS` 逐表參數化，把「改了 `models.py` 卻忘了改 `fields`」變成會紅的測試（欄位齊備、型別、可空性四類）——延伸 DQ 文件機制 1 的精神到抽取層。**每加一張表只需在 `SPECS` 掛一份 spec，一致性守衛自動涵蓋**，不必另寫測試。

### 5.5 端到端範例：加欄 / 刪欄（含後續 NULL 處理）⭐

§5.2 的對照表是「哪種 ODS 變更、BQ 能不能就地」的靜態矩陣；本節是它的**逐步走查版**，把兩個最常見的變更從 ODS 一路追到 dbt `stg_`，並接上各自產生的 NULL 該怎麼處理。

前提：這裡的「加/刪欄」＝工程師**刻意**經 Alembic 改 ODS（§5.1 的刻意演進），**不是上游 drift**（drift 不動 ODS 結構）。`stg_orders` 已設 `on_schema_change='append_new_columns'`（見 [ecommerce_dbt/README.zh-TW §4.7](./ecommerce_dbt/README.zh-TW.md)）。

兩例產生的 NULL 在時間軸上是**鏡像**，處理哲學因此相反：

| | NULL 長在哪 | 語意 |
|---|---|---|
| 加欄 | 過去（歷史分區） | 這欄在那段歷史**根本不存在** |
| 刪欄 | 未來（停收後往後長） | 這欄之後**不再被填** |

**共同第一步永遠是先判斷 NULL 屬於哪一種**，再決定接受 / 回填 / 補值——判錯就會用錯工具。

#### 5.5.1 加欄：流程

| # | 關卡 | 動作 |
|---|---|---|
| 1 | ODS | Alembic 加一個 **nullable** 欄（NOT NULL 加欄無法走 `ALLOW_FIELD_ADDITION`，既有列會違反）|
| 2 | 一致性測試 | `test_no_ods_column_missing_from_fields` 變紅——「ODS 有、`FIELDS` 沒有」被擋下（否則靜默漏抽）|
| 3 | `FIELDS` | 補上該欄（型別/mode 對齊，否則型別/mode 測試也紅），測試轉綠＝三份宣告重新對齊 |
| 4 | 抽取＋載入 | `ALLOW_FIELD_ADDITION` 自動把新欄加進 staging 實體表；舊分區歷史列 NULL、新列有值 |
| 5 | `stg_orders`（未改清單）| `source` 的 `select *` 撈進來，但**最終顯式 SELECT 不列它 → 丟掉**；模型產出不變、下游看不到，只是「靜默搭車」躺在 staging |
| 6 | `stg_orders`（改清單顯現）| 把欄加進顯式 SELECT（進 git、被 review）→ 下次**一般增量跑批**即可：dbt 自動 `ALTER ADD COLUMN`（metadata、免費、舊分區 NULL）+ copy job 只覆寫回看窗分區。**免 `--full-refresh`、免全表重寫**，成本 ∝ 近期資料 |

#### 5.5.2 加欄：歷史大量 NULL 的後續處理

先分岔關鍵判斷：**這欄的歷史是「不存在」還是「漏抽」？**

| 處理 | 適用 | 做法 | Why |
|---|---|---|---|
| A. 接受 NULL（預設）| 值真的從現在才開始收集（新制上線）| 不填；下游按時間切或 `WHERE col IS NOT NULL` | NULL 誠實反映「過去沒有」，硬填＝製造假資料。成本 0 |
| B. Proposal C 回填 | 值其實一直在 Raw 裡，只是 ODS 之前沒對映（漏抽）| 從 Raw 用新對映批次重產 → push 修正列 → 災區分區 targeted refresh（見 §7、DQ Proposal C）| 「值缺漏」類，A/B remediation 管不到，正是 Proposal C 領域。重、但一次付清 |
| C. 下游補值 | 分析需要非 NULL（SUM/AVG 不想被稀釋、報表要顯示 0）| `int_/dim_` 層 `COALESCE(col, <default>)`，model description 記錄語意 | `stg_` 保持忠實（NULL），補值屬分析層業務決策（DQ 機制三：SQL 即審計）|
| D. 攝入時給 default | 業務上必然有值（如 `dq_rule_version`）| ODS migration 就設 default/NOT NULL，歷史列當下填滿 | 把「要不要 NULL」推到最上游最便宜的時點；代價是 NOT NULL 加欄需 migration 內填值，不走 `ALLOW_FIELD_ADDITION` |

⚠️ **`append_new_columns` 的盲區**：`ALTER ADD COLUMN` 把**所有**舊分區設 NULL，但一般增量只回填**回看窗**那幾天。若這欄在 staging 已存在一陣子（欄位引入時點 ≪ 加進 `stg_` SELECT 的時點），中間「staging 有真值、但在回看窗外」的分區，`stg_` 會**錯誤停在 NULL**。補救＝對那段區間一次性 targeted refresh、臨時放大 `stg_orders_lookback_days`、或該欄首次上線單獨 `--full-refresh` 一次。故「免 full-refresh」精確講是**免「未來每次」全表重寫**，首次若有歷史落差仍要一次性補。

#### 5.5.3 刪欄：流程

| # | 關卡 | 動作 |
|---|---|---|
| 1 | ODS | Alembic drop 欄，`models.py` 不再有它 |
| 2 | 一致性測試 | `test_no_stale_field_without_ods_column` 變紅——「`FIELDS` 有、ODS 沒有」的殘欄被擋下 |
| 3 | `FIELDS` | 移除該欄，測試轉綠；`_to_bq_dict` 不再吐它 |
| 4 | 抽取＋載入 | staging 實體欄**不刪、留著**（§5.2）；load schema 少該欄 → 新列 NULL、歷史列保留原值 |
| 5 | `stg_orders` | 顯式清單仍含該欄 → 照常查（staging 還在，新列讀 NULL、舊列讀原值）、**不 breaking**，變成 legacy 欄 |
| 6 | 要從模型移除 | **預設：留 legacy、不動**——`append_new_columns` 只加不刪，**刻意不介入 DROP**（對齊「staging 只做加法、刪欄留 legacy」§5.2/§5.3）。真要拿掉才 `--full-refresh` 重建（罕見、刻意的 escape hatch；若下游 `int_/dim_` 仍引用它，會在那次 `dbt run` 報錯，於 DAG 內被抓）|

#### 5.5.4 刪欄：未來大量 NULL（legacy 欄的 NULL 尾）的後續處理

這欄有真實歷史、未來 NULL 越長越長；問題從「怎麼填」變成「怎麼**不被誤用**」。

| 處理 | 適用 | 做法 | Why |
|---|---|---|---|
| A. 凍結留存（預設）| 大多數情況 | 讓它躺著：歷史可查、未來 NULL；要用就限歷史區間 | 對齊 §5.2/§5.3「不刪、留著保歷史」；BQ 儲存極廉，NULL 尾成本 ≈ 0 |
| B. 標記有效期，防誤用 | 有下游會碰它 | model description / 註記「X 日後停填」，或 `int_/dim_` 明確 `WHERE order_date < 停用日` 才引用 | 防止未來的人對半死欄做 `AVG` 被 NULL 尾稀釋（消費者契約問題，呼應 DQ Proposal C-4 P4）|
| C. 真的清掉 | 確定不需要、可接受丟歷史 | 從 `stg_` 顯式清單移除 + `--full-refresh` 重建（`append_new_columns` 不 DROP，故必須 full-refresh）| 唯一能讓欄「消失」的路。罕見、刻意 |
| D. 歸檔後移除 | 要主線乾淨又要留稽核 | 先把含該欄的歷史快照另存 archive 表，再從主線移除 | 兼顧「主線乾淨」與「歷史可稽核」，類比遷移式 `ods_retired_<batch>`。中成本、多一張表 |

#### 5.5.5 NULL 處理該落在哪一層（`int_` vs `dim_/fct_`）

先分兩種 NULL 處理：**(a) 消費者無關的正規化**（對所有下游客觀正確、答案唯一）與 **(b) 消費者相關的分析/呈現決定**（NULL→0 好聚合／保留 NULL 以計缺漏率／NULL→'unknown' 當維度桶——答案隨問題而變）。上面兩個結構性 NULL 幾乎都是 (b)。

核心語意原則：**NULL 帶資訊（「不存在 / 停止收集」），`COALESCE` 是有損且單向**——一旦在 `int_` 把 NULL 壓成 0，全下游再也分不出「沒收集」與「真的是 0」，想算涵蓋率的 `fct_` 就永遠算不出來。故：**保留 NULL 越久越好，只在「那個具體問題讓 collapse 變正確」的那層才 collapse**；填預設值是業務/呈現決定，屬 dim/fct/rpt 的高度，不是 int_ 管線（呼應「品質責任往下游收緊」）。

| 面向 | 放 `int_`（早、共享）| 放 `dim_/fct_`（晚、貼近消費者）|
|---|---|---|
| 可逆性 | 差：NULL 資訊在此消失、下游救不回 | 好：局部決定、爆炸半徑小 |
| 一致性 | 全下游同一解 → 只有 (a) 受惠 | 各取所需 → (b) 的天然歸屬 |
| 語意 | 對 (b) 是「替所有人做了不該替他做的決定」| 每個問題自己決定 |

文件既有樣板：DQ 機制三的場景補值**已放 int_**，但用**新欄**（`customer_rating_cleaned`，不覆寫原欄）＋**場景專用模型**（不污染正典 `int_orders`）＋**description 留痕**。照抄這套。

**建議**：這兩個結構性 NULL **預設不要在 `int_` collapse**，保留穿過 int_、在 `dim_/fct_/rpt_` 依問題處理（聚合本就忽略 NULL，常常不用填；刪欄的 NULL 尾用 `WHERE order_date < 停用日` 限有效期即可）。**例外**：某填值被證明 (a) 消費者無關且被多下游共用 → 才移進 int_，且**加新欄、不覆寫正典欄**（機制三那套）。**鐵律：永不在 `int_orders` 正典欄就地 `COALESCE` 掉 NULL**——那是在最共享的層、對最多消費者、做有損不可逆的決定。

#### 5.5.6 兩例都會踩的橫向陷阱

1. **別把結構性 NULL 當成品質錯誤**。DQ 的 `has_clean_error`/quarantine/Hard Gate 是給「值有業務問題」用的；欄位存在期外的 NULL 不是髒資料，不進 quarantine。Hard Gate 的 `error_rate_below` 看 `has_clean_error` 比率，結構 NULL 不會灌進去——**但**若在該欄掛了 `not_null` 測試，NULL 尾會讓測試爆。這類欄的測試要按「有效期」設計（只對有效區間斷言 not_null），或不掛 not_null。
2. **null-rate 監控會誤報**。Phase 4「少欄位由 null-rate 監控」會看到這兩例的 NULL 暴增。要**事先把它標為「預期的結構性 NULL」**（migration/上線 note、或監控 baseline 例外），否則每次假警報。

#### 5.5.7 判準

一句話收束：**先分辨 NULL 是「不存在 / 漏抽 / 停止收集」**——不存在→接受（5.5.2 A）、漏抽→Proposal C 回填（5.5.2 B）、停止收集→凍結留存+防誤用（5.5.4 A/B）；而**填值決定往 DAG 邊緣（dim/fct/rpt）推、正典欄永不覆寫**（5.5.5）。

---

## 6. 實機驗證記錄（2026-06）

| 驗證項 | 結果 |
|---|---|
| 分區/叢集/保險絲/location | `received_at(DAY)` / `[order_id, has_clean_error]` / `True` / `US` |
| 保險絲 | 無 `received_at` 過濾的查詢被 400 擋下 |
| JSON 落地 | `items`、`clean_error_message` 皆 `JSON_TYPE=array`；下游 `JSON_VALUE(...[0],'$.code')` 取值正確 |
| additive load 路徑 | explicit schema + `ALLOW_FIELD_ADDITION` 不破壞 happy path |
| 一致性測試 | `test_schema_bq_consistency` 全綠 |

---

## 7. 修正批次的回流路徑（Proposal C 的雲端側）

[DQ_ARCHITECTURE-TW](./DQ_ARCHITECTURE-TW.md) 的 Proposal C（歷史值缺陷的批次修復，方向性設計、尚未實作）在雲端層有四件事要知道：

### 7.1 watermark 永遠看不到修正列——上雲是主動步驟

修正列保留原本的 `received_at`（落回舊分區），而方案 A 的 watermark 是 `MAX(partition_id)`、只往前看；例行增量抽取的 `received_at >= 最新分區` 條件永遠撈不到舊分區裡的新列。所以「推上 staging」必須是修復 runbook 的主動步驟（按 batch id 圈出修正列、呼叫既有 `load_to_staging()` append），不是等例行排程。watermark 機制從頭到尾不參與、也不需要動。

### 7.2 遷移式形態：複用 append + dedup 通道，不需要 JOIN

staging 是 append-only：修正列 append 後，同一 `raw_id` 會永遠存在兩列（錯的舊列 + 對的新列），且兩列的 `received_at` / `raw_id` / `order_id` 完全相同——「取最新」沒有現成的排序依據，`stg_` 去重必須以 `rebuild_batch_id DESC NULLS LAST` 決勝。batch id 因此是回流機制的功能性零件，不只是稽核欄位。額外紅利：若災區右邊界落在當天分區，例行抽取會把修正列再抽一次——無害，重複的兩列連 batch id 都相同，去重取誰都一樣（「寧可重抓不漏抓」的既有設計直接吸收）。BQ load job 整批原子，不會出現半批可見。

### 7.3 補丁式形態：第二張表、另一份手維護宣告、一個重抽地雷

corrections 若另成一張 BQ 表：需要自己的 `FIELDS` 宣告、抽取邏輯、與 `test_schema_bq_consistency` 同級的一致性守衛（§5.4 的精神同樣適用）；且任何 staging 全量重建（如 §5.3 的改分區情境）會把主表錯值原樣重抽上去——**重建步驟必須明文包含補推 corrections**，否則錯值復活。

### 7.4 late-arriving：災區分區需 targeted refresh

修正值落在舊分區，按 `received_at` 增量的 `stg_` 例行跑批看不到。runbook 最後一步必須對災區分區做 targeted refresh（insert_overwrite 該批分區，或對 `stg_` 單一模型一次性 full-refresh）。在 push 完成前搶跑的例行 dbt run 只是「尚未生效」，不是錯誤狀態。

---

## 8. 待辦與未來

- 微批升級時：`get_watermark()` 換方案 B（+ `advance_watermark()`）。
- 進 dbt 分層：`stg_`（`stg_orders`、`stg_quality_events`）、`int_`（`int_orders`、`int_orders_quarantine`、`int_order_items`）、`dim_/fct_`（`dim_customer`、`dim_product`、`fct_orders`、`fct_order_items`）已落地，見 [ecommerce_dbt/README.zh-TW](./ecommerce_dbt/README.zh-TW.md)。§5.5.5「正典欄永不覆寫、填值往 DAG 邊緣推」的鐵律已在 `int_order_items` 落實為衍生金額的**嚴格 NULL 傳播**（不 `coalesce`），並在 `fct_orders` 以 `items_missing_amount` 顯性化 rollup 的不完整性（見 §1.2）。
- ~~`dim_/fct_` 若採 `order_date` 分區，需先做合法區間守衛~~ — **此條已於 2026-08 實測推翻，見 §1.7.3**。超出 BQ 分區合法區間的日期**不會**讓建表失敗，而是靜默落入 `__UNPARTITIONED__`。
