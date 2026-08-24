# 實作現況

[English](../en/STATUS.md) | **繁體中文**

最後檢視：2026-08-24

這份文件是「做了什麼、沒做什麼、為什麼」的唯一真相來源。設計文件描述系統如何運作，不承載現況；若設計文件與本檔衝突，以本檔為準。

---

## 狀態詞彙表

四種狀態，全 repo 一致使用。

**這個作品在既定範圍內已完成。** 下方所有非 ✅ 的項目，都是決策或限制，不是做到一半的工作。

| 標記 | 意義 | 隱含什麼 |
|---|---|---|
| ✅ **已實作** | 已建置並實際運行過 | — |
| ⛔ **決定不做** | 評估過，答案是否 | 即使有真實流量也還是不做。每一項都附重新評估的 trigger |
| ⏸ **暫緩** | 真實系統會做，但在這裡做不出意義 | 受作品性質限制——沒有真實流量，或需要付費帳號。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| ⬜ **待辦** | 該做、能做、還沒做 | 唯一真正屬於 backlog 的狀態 |

**關鍵區別：⛔ 是「不該做」，⏸ 是「該做但做不了」，⬜ 是「該做也會做」。** 把三者一律讀成「沒做」，正是這套詞彙表要防止的誤讀。

**⬜ 目前是空的**，因為這個作品在既定範圍內已完成。保留這個類別是為了日後擴充：若這個系統要繼續發展，真正的待辦事項會列在那裡。

---

## 分層矩陣

| 層 | 狀態 | 備註 |
|---|---|---|
| 攝入（API → Raw） | ✅ | 重試、速率限制、API Key 驗證、連線池防護 |
| 處理（Raw → ODS） | ✅ | CAS 認領、冪等性、品質標記、`quality_events` |
| 任務佇列 | ✅ | Celery + Redis、斷路器、有界恢復掃描 |
| 抽取（ODS → BQ） | ✅ | Watermark 增量載入、分區 + 叢集 staging |
| 轉換（dbt） | ✅ | `stg_` → `int_` → `dim_`/`fct_` → `rpt_`，93 個測試 |
| 編排（Airflow） | ✅ | 六個 DAG、兩個隔離 venv、失敗回呼 |
| 可觀測性（OTel） | ✅ | Traces + 營運指標；告警刻意未做 |
| BI（Looker Studio） | ✅ | 讀 `rpt_` 層的展示用儀表板。沒有真實觀眾，所以沒有東西驗證某張圖**有沒有用**——但連線與語意層確實被演練過了 |

---

## 攝入與處理

| 項目 | 狀態 | 說明 |
|---|---|---|
| 四點重試 + 指數退避 | ✅ | Raw 寫入、認領、處理、狀態提交；重試耗盡記 `CRITICAL` |
| 崩潰恢復掃描 | ✅ | Phase 5 移至 Celery Beat；含啟動時的補掃 |
| Timeout 與連線池防護 | ✅ | 30 秒 statement timeout、明確 pool 設定、池耗盡回 `503` |
| 冪等性 | ✅ | `UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`、first-write-wins、`IntegrityError` 後盾 |
| 每客戶速率限制 | ✅ | slowapi 以 `client_id` 為鍵，計數器共享於 Redis db 1 |
| API Key 驗證 | ✅ | `X-API-Key`、`secrets.compare_digest`、解析出的 `client_id` 落地為血緣 |
| 集中式設定管理 | ✅ | pydantic-settings `Settings` 單例；演算法常數刻意排除在外 |
| Alembic 為 schema 單一真相 | ✅ | 移除 `create_all`；`Base.metadata` 帶命名慣例 |
| Docker / compose | ✅ | db → migrate → api/worker/beat，由 healthcheck 把關 |
| NUL byte 毒藥丸 | ✅ | `ValueError` 快速失敗至終端 `error` 狀態 |
| 改為淨化 NUL 而非拒絕 | ⛔ | 拒絕的語意與 `DataError` 一致。**Trigger**：若決定這類訂單應標記後落地而非拒絕——需新增 clean-error code 並提升 `DQ_RULE_VERSION` |

## 任務佇列

| 項目 | 狀態 | 說明 |
|---|---|---|
| Celery + Redis 取代 `BackgroundTasks` | ✅ | `process.py` 保持 Celery-free，保留手動救援路徑 |
| 不啟用 result backend | ✅ | `raw.status` 是唯一真相 |
| `acks_late` + `reject_on_worker_lost` | ✅ | worker 遺失時重新投遞 |
| 以 `processing_started_at` 判定逾時 | ✅ | 修掉了積壓時同一 `raw_id` 在兩個 worker 上執行的缺陷 |
| 分派斷路器 | ✅ | broker 中斷時 p50：timeout → 5ms |
| 有界恢復掃描 | ✅ | 分頁 + id cursor + 每輪上限 + Redis 鎖 + 寬限期 |
| `raw.status` 索引 | ⏸ | 分頁限制住記憶體與派發量，但沒有限制查詢成本。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## 雲端抽取

| 項目 | 狀態 | 說明 |
|---|---|---|
| `extract_ods_to_bq.py` | ✅ | 批次載入、Watermark 方案 A、`ALLOW_FIELD_ADDITION` |
| 分區 + 叢集 staging 與成本保險絲 | ✅ | `received_at` DAY、叢集 `order_id` + `has_clean_error`、`require_partition_filter` |
| `FIELDS` 單一真相 | ✅ | 由 `tests/test_schema_bq_consistency.py` 把關 |
| `get_watermark()` 抽象層 | ✅ | 切換到 micro-batch watermark 的唯一接縫。批次是架構的選擇（見 ADR-0019）；這個接縫**記錄的是出口，不是未完成的功能**。這個專案為何不會走它——分區預算與報表口徑都以日為前提——寫在 ADR-0023 |
| Gold `order_date` 分區保留 | ⏸ | Sandbox 強制 60 天過期。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## 轉換（dbt）

| 項目 | 狀態 | 說明 |
|---|---|---|
| `stg_orders`、`stg_quality_events` | ✅ | 去重 + Hard Gate + freshness；incremental 搭配 `copy_partitions` |
| `int_orders`、`int_orders_quarantine`、`int_order_items` | ✅ | Row Filter 依有效品質狀態；分割不變式測試 |
| `dim_customer`、`dim_product` | ✅ | SCD1 + unknown member |
| `fct_orders`、`fct_order_items` | ✅ | 上捲一致性與無損投影皆有測試把關 |
| `rpt_quality_events_daily`、`rpt_quality_backlog`、`rpt_sales_daily_by_category` | ✅ | 品質報告採兩個時間軸 |
| 情境專用 `int_orders_*` | ⛔ | 已設計，刻意不建。要決定「哪些錯誤與此情境無關」「補什麼值」，前提是知道分析問題是什麼——**在場景出現前先建，等於把猜測包裝成設計，這一點在生產環境同樣成立**。**Trigger**：出現真實分析場景 |
| SCD2 `dim_customer` | ⏸ | 已設計；dbt snapshot 需要 sandbox 不給的寫入權限。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| `rpt_sales_*` 增量化 | ⏸ | 增量化的動機是成本與資料量；每天 800 筆模擬訂單兩者都沒有。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| 金額曝險度量 | ⏸ | 曝險是業務口徑；用產生的金額算出來不只是不精確，而是會誤導。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## 編排

| 項目 | 狀態 | 說明 |
|---|---|---|
| 六個 DAG，排程一律以 `Asia/Taipei` 明確宣告 | ✅ | 沒有任何 DAG 在 Airflow 層依賴另一個；排序契約只存在於時間間隔中 |
| 兩個隔離 venv | ✅ | 什麼都不裝進 Airflow 本身 |
| DAG 測試 + 專屬 CI job | ✅ | 52 個測試；DAG 檔不 import 任何專案模組 |
| Proposal B 事件產生器 | ✅ | `reevaluate_quality.py` + `dq_reevaluation` DAG |
| 失敗通知接線 | ✅ | task 層級 `on_failure_callback`；訊息寫「該做什麼」而非任務名 |
| 失敗通知的真實通道 | ⏸ | 預設為一行 log；每則訊息都帶 `channel=`，`channel=log` 即表示沒有人被通知 |
| 跨時區抽取 | ⏸ | 三個候選方案皆未選定——沒有跨日界的真實流量就無法驗證。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## 可觀測性

| 項目 | 狀態 | 說明 |
|---|---|---|
| 常駐 OTel Collector | ✅ | 雲端端點與憑證只存在於一個地方 |
| api → Celery → worker 的分散式追蹤 | ✅ | 已驗證同一 `trace_id` 出現在兩個行程 |
| 營運指標 | ✅ | 320 個活躍序列，佔免費額度 3.2% |
| structlog 注入 `trace_id` / `span_id` | ✅ | logs 刻意不走 OTLP |
| 業務 / DQ 指標 | ⏸ | 模擬上游的髒資料率在一天內恆定，分鐘級錯誤率說不出倉庫沒說過的事。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |
| Absent 告警與儀表板 | ⏸ | **論述本身即交付物**——見 [存活告警設計原則](./design/liveness-alerting.md) |
| Airflow OTel 整合 | ⏸ | 技術上已就緒——擋住它的是它的每一個消費端本身都處於暫緩。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

## 測試與 CI

| 項目 | 狀態 | 說明 |
|---|---|---|
| 單元 + 整合測試 | ✅ | 445 個測試、受管模組 100% 覆蓋、Python 3.10/3.12 矩陣 |
| DAG 解析測試 | ✅ | 52 個測試，獨立 workflow，使用官方 constraints |
| dbt 測試 | ✅ | 93 個測試，含自訂 generic 與 singular 不變式 |
| 對真實資料庫的 E2E 測試 | ⏸ | 容器啟動 flake 的維護成本高於目前風險 |
| `check_migration_drift.py` 進 CI | ⏸ | 它是確定性、無並發、低 flake 的，**技術上今天就能進 CI**；考量單人開發、schema 已趨穩而保留手動。見 [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) |

---

## 已知風險

| 風險 | 影響 | 目前的緩解 |
|---|---|---|
| `raw.status` 無索引 | 恢復掃描的查詢成本隨表成長 | 分頁限制住記憶體與派發量；索引形狀需要真實流量才能決定 |
| 跨時區抽取未解 | 業務的「一天」與分區的「一天」可能分歧 | 已記錄；目前沒有流量跨越日界 |
| Gold `order_date` 分區 60 天過期 | 較舊的列會從 Gold 靜默消失 | 已知並已量測；開通計費即可解除 |
| 「該跑沒跑」未被監控 | 排程器停擺不會產生紅燈——`on_failure_callback` 需要一個真的執行過的 run | **這不是獨立的缺口**：它是 absent 告警與 Airflow→OTel 整合兩項皆暫緩的後果。傳輸接縫已就緒（`_deliver()`，離真實通道只差一個環境變數），缺的是**偵測器**。這是 [2026 年 8 月靜默停擺事故](./incidents/2026-08-silent-scheduling-stalls.md)的殘留盲點 |
| CI 不驗證 DB 層契約 | 綠燈不代表 CAS／去重／遷移已被驗證 | 手動腳本：`load_test.py`、`restart_test.sh`、`check_migration_drift.py` |

---

## 相關文件

- [PORTFOLIO_SCOPE](./PORTFOLIO_SCOPE.md) — 每一個 ⏸ 項目，以及真實系統會怎麼做
- [CHANGELOG](../../CHANGELOG-TW.md) — 系統如何走到今天
- [架構決策記錄](./adr/README.md) — 每個決策為何如此
