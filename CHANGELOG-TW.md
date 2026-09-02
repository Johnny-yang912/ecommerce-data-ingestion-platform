# 變更紀錄

[English](./CHANGELOG.md) | **繁體中文**

這個系統如何走到今天。Phase 是發布單位；每個 Phase 之內，每一條是一行加上一個指向決策或量測的連結。

**收錄範圍**：行為、契約與架構的變更。重構與文件不列，除非它改變了一個決策。

---

## Phase 5 — 自動化與佇列升級 · 2026-08

### 新增

- **Celery + Redis** 取代 `BackgroundTasks`。`process.py` 保持 Celery-free，保留手動救援路徑。→ [ADR-0010](./docs/zh-TW/adr/0010-celery-replaces-backgroundtasks.md) · [ADR-0012](./docs/zh-TW/adr/0012-process-stays-celery-free.md)
- **Airflow 3.0.0**，六個 DAG，每個排程都以 `Asia/Taipei` 宣告。沒有任何 DAG 在 Airflow 層依賴另一個——排序契約住在時間間隔裡。→ [design/orchestration](./docs/zh-TW/design/orchestration.md)
- **分派斷路器。** broker 掛掉時的退化是*超線性*的——48 個併發請求中有 47 個在 120 秒內沒有完成，**而它們的 Raw 其實都已經寫進去了**。中斷期間 p50：逾時 → **5ms**。→ [ADR-0014](./docs/zh-TW/adr/0014-circuit-breaker-dispatch.md) · [實測](./docs/zh-TW/verification/2026-08-10-circuit-breaker-before-after.md)
- **有界恢復掃描**——分頁 + id 游標 + 每輪上限 + Redis 鎖 + 寬限期。對 120,000 積壓驗證：ODS 恰好成長 120,000，零重複。→ [ADR-0017](./docs/zh-TW/adr/0017-bounded-recovery-scan.md) · [實測](./docs/zh-TW/verification/2026-08-10-bounded-scan-120k.md)
- **Proposal B 事件產生器**（`reevaluate_quality.py`）——候選來自 BQ、狀態對 PG 判定、只在狀態確實改變時 append 事件。→ [ADR-0030](./docs/zh-TW/adr/0030-proposal-b-event-driven-reevaluation.md)
- **OpenTelemetry**——`api` → Celery → `worker` 的跨行程追蹤，加上營運指標。**320 個活躍序列，免費額度的 3.2%。** → [ADR-0050](./docs/zh-TW/adr/0050-resident-otel-collector.md) · [ADR-0052](./docs/zh-TW/adr/0052-sdk-views-series-budget.md)
- **失敗通知**，訊息陳述的是**該做什麼**而非任務名。掛在 task 層級，所以一條斷掉的七任務鏈恰好送出一則訊息。→ [ADR-0042](./docs/zh-TW/adr/0042-failure-notification-response-not-task.md)
- `raw.processing_started_at`，以及作為門檻零依賴歸屬的 `recovery_policy.py`。

### 變更

- **恢復掃描從 FastAPI 的 lifespan 搬到 Celery Beat。** 那正是允許多個 API 行程的原因。→ [ADR-0016](./docs/zh-TW/adr/0016-recovery-scan-in-beat.md)
- **限流計數器搬到 Redis db 1。** 跨 4 個 uvicorn worker，`60/minute` 已經靜默地變成 `60 × workers`——實測 100 個請求中 **91 個**通過而非 60，**而且沒有任何地方報錯**。→ [實測](./docs/zh-TW/verification/2026-08-10-rate-limit-multiprocess.md)
- **Hard Gate 口徑從全表改為逐批。** 全表分母會稀釋單批異常且無法自癒；實測連續四批錯誤率都在 10% 以上（其中一批 100%），而全表數字停在 9.122%，閘門**一次都沒有觸發**。→ [ADR-0028](./docs/zh-TW/adr/0028-hard-gate-per-batch-scope.md)
- **`source_freshness_watch` 從「預期會紅」翻轉為「預期會綠」**，在 seeding 成為系統真正的資料源之後。

### 修正

- **逾時改以 `processing_started_at` 判定，而非 `received_at`。** 積壓時舊基準會收回正在被處理的記錄，於是同一個 `raw_id` 在兩個 worker 上跑——重現出來，2,000 筆積壓中發生 2 次。**CAS 並沒有失效**：它擋不住第三方把狀態退回去。→ [ADR-0015](./docs/zh-TW/adr/0015-staleness-from-processing-started-at.md) · [實測](./docs/zh-TW/verification/2026-08-10-staleness-basis-self-collision.md)
- **NUL byte 毒藥丸。** 線上的一個 `\u0000` escape 是六個合法 ASCII 字元，所以入口防護什麼都沒剝掉；`json.loads` 把它解碼成真正的 NUL，而產生的 `ValueError` 被當成暫時性錯誤重試——永遠。→ [ADR-0006](./docs/zh-TW/adr/0006-nul-byte-fast-fail.md)
- **不得有 DB 交易跨越派工。** `db.refresh()` 曾經如此；60 併發下實測，32 個池槽位中有 23 個卡在 `idle in transaction`。
- **金額從 `FLOAT64` 移到 `NUMERIC`**，在一個上捲測試對 39 列差 **1 ULP** 變紅之後——浮點數的 `SUM()` 不具結合律。**修的是型別，不是容差。** → [design/transformation](./docs/zh-TW/design/transformation.md)
- **增量窗口的左邊界沒有對齊分區邊界——同一個缺陷存在於三支模型。** `insert_overwrite` 的原子單位是整個分區，而左界帶著跑批當下的時刻——一次比排程早兩小時的手動跑批，讓**半天的資料原子覆寫了整天**，`stg_orders` 與 `stg_quality_events` 的 `2026-08-26` 分區各從 **800 列被砍成 250 列**。DAG 綠、dbt test 綠、上游 staging 完好無損。三支模型（另含 `rpt_quality_events_daily`）全部對齊日界，各補定點回填 var，並加上兩支逐分區對帳測試。**修復分兩階段**：第一階段只涵蓋 `stg_orders` 便宣告結案，當晚才由 BI 落差發現另外兩支——範圍是照「哪張表壞了」劃的，而缺陷是「寫法」層級的。→ [ADR-0055](./docs/zh-TW/adr/0055-partition-aligned-incremental-window.md) · [事故](./docs/zh-TW/incidents/2026-08-30-stg-partition-truncation.md)

- **端點從 `async def` 改為 `def`：同步 DB 呼叫不再佔住 event loop。** 三個端點（`/orders`、`/process_raw`、`/raw`）的 handler 內是阻塞的 psycopg2 呼叫，而連線持有窗口內沒有任何 `await`——單一個卡住的查詢會凍結**整個 uvicorn 行程**，不只那一筆請求。**把 PostgreSQL 停住 8 秒實測：一個完全不碰資料庫的 `/health` 被卡了 8.2 秒（改動後 40ms）**——凍結時長等於資料庫卡住的時長，上限由 `statement_timeout`（30 秒）決定。一般負載下 `/health` p99 亦由 167ms 降至 34ms；吞吐 +42%，worker 數不再於 8 反轉（207 → 485 RPS）。⚠️ 代價：API 收得更快而 worker 在突發期間因 CPU 競爭而更慢，同一波 60,000 筆突發的積壓峰值由 5,453 升至 36,526——仍為零錯誤、119 秒完全回收。**修的是故障放大，效能是副作用。** → [實測](./docs/zh-TW/verification/2026-09-02-sync-handlers-before-after.md)

### 決定不做

- **Tier 1 的業務／DQ 指標**——模擬上游的髒資料率在一天內恆定，所以分鐘級錯誤率說不出倉庫沒說過的事。→ [PORTFOLIO_SCOPE](./docs/zh-TW/PORTFOLIO_SCOPE.md)
- **Absent 告警與儀表板**——今天就寫得出來，但它們的價值閾值與應對程序需要真實流量，而規則會住在一個無法版控的 UI 裡。**論述本身即交付物。** → [design/liveness-alerting](./docs/zh-TW/design/liveness-alerting.md)
- **Airflow → OTel 整合**——技術上一直就緒；它的每一個消費端本身都處於暫緩。

---

## Phase 4 — 分析管線 · 2026-06 → 2026-08

### 新增

- **`extract_ods_to_bq.py`**——只用批次載入、watermark 由 `INFORMATION_SCHEMA.PARTITIONS` 推導、分區 + 叢集的 staging 並以 `require_partition_filter` 作為成本保險絲。→ [ADR-0019](./docs/zh-TW/adr/0019-batch-load-not-streaming.md) · [ADR-0023](./docs/zh-TW/adr/0023-watermark-approach-a.md)
- **dbt `stg_` 層**——依 `raw_id` 去重、Hard Gate、source freshness；用 `copy_partitions` 的增量以繞開 sandbox 的 DML 禁令。→ [ADR-0044](./docs/zh-TW/adr/0044-copy-partitions-sandbox-dml.md)
- **dbt `int_` 層——阻斷發生的地方。** Row Filter 以**有效品質狀態**為鍵而非字面旗標，因為 ODS 不可變、被 promote 的記錄永遠讀到髒。→ [ADR-0029](./docs/zh-TW/adr/0029-effective-quality-state.md)
- **dbt `dim_`／`fct_`**——雙事實表、兩個 SCD1 維度，上捲一致性由 singular test 把關。→ [ADR-0047](./docs/zh-TW/adr/0047-measures-roll-up-to-header.md) · [ADR-0048](./docs/zh-TW/adr/0048-two-dimensions-scd1.md)
- **dbt `rpt_`**——三張表；品質報告拆成兩張，因為事件軸與快照的可變性是相反的。→ [ADR-0049](./docs/zh-TW/adr/0049-business-reports-read-gold.md)
- **`FIELDS` 一致性測試**——沒有它，加了 ODS 欄位卻忘記 `FIELDS` 會**靜默**失敗。→ [ADR-0026](./docs/zh-TW/adr/0026-fields-single-source.md)

### 變更

- **`int_` 的物化維持全量重建**，刻意不做增量：Proposal B 的 promotion 落在今天的分區，而它救的訂單坐在舊分區，所以增量窗口會**靜默地**切斷回流路徑。→ [ADR-0046](./docs/zh-TW/adr/0046-stg-incremental-int-full-rebuild.md)

### 撤回

- **採用 `order_date` 分區前那道計畫中的「合法區間 guard」。** 實測：超出範圍的日期**不會**讓 build 失敗——它們靜默落進 `__UNPARTITIONED__`，而且同樣逃過 60 天的回收。**原先假設的失效模式是錯的那一種。** → [實測](./docs/zh-TW/verification/2026-08-partition-expiry-measurement.md)

---

## Phase 3 — 可維運性 · 2026-06

- **服務間驗證**——static `X-API-Key`、`secrets.compare_digest`、一個 client 可對應多把 key 以供輪替。解析出的 `client_id` 落地為 `source_client_id`：**血緣隨驗證免費附帶**。不做面向使用者的 JWT——沒有人類使用者。→ [ADR-0007](./docs/zh-TW/adr/0007-static-api-key-not-jwt.md)
- **集中式設定管理**，帶一條明確的邊界：只放環境值。重試次數與逾時門檻留在各自模組開頭，因為**它們是程式行為，不是環境**。→ [ADR-0008](./docs/zh-TW/adr/0008-config-boundary.md)
- **Alembic 作為單一真相**；`create_all` 被完全移除——它只建立、從不變更，所以承載不了 schema 演進。`Base.metadata` 帶命名慣例，讓約束名稱跨環境穩定。→ [ADR-0009](./docs/zh-TW/adr/0009-alembic-single-source-of-truth.md)
- **Docker / docker-compose**——`db`（healthcheck）→ `migrate` → `api`，祕密在執行期注入、絕不烤進映像。

---

## Phase 2 — 可測試性 · 2026-05

- **Pytest 套件**——涵蓋四個重試點、CAS 認領、冪等性、崩潰恢復、清洗規則與驗證的單元與整合測試。
- **少數幾個釘住決策而非行為的測試**——劃分不變式、上捲不變式、「絕不拆 `dbt build`」、freshness 隔離、schema 一致性、探針依賴隔離。**每一個都把一份紀律轉成一個機制。** → [design/testing](./docs/zh-TW/design/testing.md)
- **資料品質架構**設計完成，ODS 層實作：`DQ_RULE_VERSION`、`dq_rule_version` 欄位，以及 append-only 的 `quality_events` 狀態機。→ [ADR-0031](./docs/zh-TW/adr/0031-rule-versioning-quality-events.md)

---

## Phase 1 — 可靠性 · 2026-04 → 2026-05

- **四點重試**搭配指數退避——Raw 寫入、認領、處理、狀態提交——並在重試耗盡時記 `CRITICAL`。
- **CAS 認領**以 `rowcount == 1` 判定，不用外部鎖服務：**那個本來就必須存在的狀態欄位完成了工作。** 以 100 個 worker 競爭同一個 `raw_id` 驗證 → ODS 計數 **1**。→ [ADR-0004](./docs/zh-TW/adr/0004-cas-claim-rowcount.md)
- **冪等性**——`UNIQUE(ods.raw_id)` + `UNIQUE(ods.order_id)`、first-write-wins、`IntegrityError` 作為 TOCTOU 後盾。→ [ADR-0005](./docs/zh-TW/adr/0005-first-write-wins-idempotency.md)
- **`duplicate` 作為終端狀態而非拒絕**，因為「上游送了兩次」與「這個系統失敗了」要求不同的回應。→ [ADR-0003](./docs/zh-TW/adr/0003-duplicate-terminal-status.md)
- **Raw 層不做業務去重**——重複提交可能帶著互補欄位，而提交頻率本身就是訊號。→ [ADR-0001](./docs/zh-TW/adr/0001-raw-no-business-dedup.md)
- **`has_clean_error` 非阻斷**——整個品質架構所倚賴的那個決策。→ [ADR-0002](./docs/zh-TW/adr/0002-has-clean-error-non-blocking.md)
- Timeout、連線池配置，以及逐客戶的限流。

---

## 慣例

- Phase 是發布單位。沒有語意化版本——這不是一個被散布的套件。
- **推翻了一個已寫下結論**的條目，會連向推翻它的那份驗證記錄。這樣的條目有六個。→ [verification/](./docs/zh-TW/verification/)
- 「決定不做」是一等公民類別。每一條都帶著重新檢視它的 trigger。→ [PORTFOLIO_SCOPE](./docs/zh-TW/PORTFOLIO_SCOPE.md)
