# 架構決策記錄（ADR）

[English](../../en/adr/README.md) | **繁體中文**

每一則記錄捕捉一個決策：當下的各方角力、選了什麼、代價是什麼、否決了什麼。記錄是 append-only 的——決策若改變，就新開一條 ADR 取代舊的；若只是機制移動，則寫進變更紀錄。

**什麼在這裡、什麼不在。** ADR 是給「有真實取捨、且可能走另一條路」的決策用的。機械性的實作細節留在[設計文件](../design/)。**如果讀者不會問「你為什麼那樣做？」，它就不是 ADR。**

狀態值：**Accepted** · **Superseded by ADR-NNNN** · **Proposed**。[STATUS](../STATUS.md) 使用的 ⛔／⏸ 標記適用於**功能**；ADR 記錄的是**決策**，決策只有被接受與否。

格式從 [ADR-0000](./0000-template.md) 開始看。

---

> **鋪設進行中。** 沒有連結的標題代表已規劃但尚未寫成。完整集合全部列出，讓範圍保持可複核。

---

## 攝入與資料架構

| # | 決策 | 狀態 |
|---|---|---|
| 0001 | [Raw 層不做業務去重](./0001-raw-no-business-dedup.md) | Accepted |
| 0002 | [`has_clean_error` 非阻斷](./0002-has-clean-error-non-blocking.md) | Accepted |
| 0003 | [`duplicate` 是 Raw 的終端狀態，不是拒絕](./0003-duplicate-terminal-status.md) | Accepted |
| 0004 | [CAS 認領以 `rowcount == 1` 判定，不外接佇列](./0004-cas-claim-rowcount.md) | Accepted |
| 0005 | [冪等性採 first-write-wins：預檢加 `IntegrityError` 後盾](./0005-first-write-wins-idempotency.md) | Accepted |
| 0006 | [NUL byte 快速失敗，而非淨化後落地](./0006-nul-byte-fast-fail.md) | Accepted |
| 0007 | [服務間驗證用 static API Key，不用 JWT](./0007-static-api-key-not-jwt.md) | Accepted |
| 0008 | [集中式設定只涵蓋環境值，不涵蓋演算法常數](./0008-config-boundary.md) | Accepted |
| 0009 | [Alembic 是 schema 的單一真相；移除 `create_all`](./0009-alembic-single-source-of-truth.md) | Accepted |
| 0053 | [Raw 以 `TEXT` 保存 payload；ODS 以 `JSONB` 保存結構化欄位](./0053-raw-text-ods-jsonb.md) | Accepted |

## 任務佇列

| # | 決策 | 狀態 |
|---|---|---|
| 0010 | [以 Celery + Redis 取代 `BackgroundTasks`](./0010-celery-replaces-backgroundtasks.md) | Accepted |
| 0011 | [不用 result backend——`raw.status` 是唯一真相](./0011-no-result-backend.md) | Accepted |
| 0012 | [`process.py` 保持 Celery-free 以保留手動救援路徑](./0012-process-stays-celery-free.md) | Accepted |
| 0013 | [broker 等待必須有界](./0013-bounded-broker-wait.md) | Accepted |
| 0014 | [分派加斷路器，且不得有交易跨越分派](./0014-circuit-breaker-dispatch.md) | Accepted |
| 0015 | [逾時以 `processing_started_at` 判定，非 `received_at`](./0015-staleness-from-processing-started-at.md) | Accepted |
| 0016 | [恢復掃描住在 Beat，不住在 API 行程](./0016-recovery-scan-in-beat.md) | Accepted |
| 0017 | [恢復掃描本身也必須有界](./0017-bounded-recovery-scan.md) | Accepted |
| 0018 | [現階段 `raw.status` 不建索引](./0018-raw-status-no-index.md) | Accepted |

## 雲端抽取與倉庫

| # | 決策 | 狀態 |
|---|---|---|
| 0019 | [批次載入，不做串流](./0019-batch-load-not-streaming.md) | Accepted |
| 0020 | [以 `received_at` 分區——而它在 Raw 與 ODS 指的是兩個不同時刻](./0020-partition-on-received-at.md) | Accepted |
| 0021 | [`require_partition_filter` 作為成本保險絲](./0021-require-partition-filter-fuse.md) | Accepted |
| 0022 | [`quality_events` staging 刻意與 `orders` 分歧](./0022-quality-events-staging-diverges.md) | Accepted |
| 0023 | [Watermark 採方案 A，`get_watermark()` 是唯一接縫](./0023-watermark-approach-a.md) | Accepted |
| 0024 | [每表一個 load job 加一道 gate；不做跨表交易](./0024-per-table-load-job-gate.md) | Accepted |
| 0025 | [Staging 只做加法；改名與轉型下推給 dbt](./0025-staging-additive-only.md) | Accepted |
| 0026 | [`FIELDS` 是第三份 schema 宣告，由一致性測試把關](./0026-fields-single-source.md) | Accepted |

## 資料品質

| # | 決策 | 狀態 |
|---|---|---|
| 0027 | [阻斷發生在 `int_`，不在 ODS](./0027-blocking-at-int-layer.md) | Accepted |
| 0028 | [Hard Gate 的口徑是逐批；全表是儀表](./0028-hard-gate-per-batch-scope.md) | Accepted |
| 0029 | [Row Filter 讀有效品質狀態，不讀字面的 `has_clean_error`](./0029-effective-quality-state.md) | Accepted |
| 0030 | [Proposal B：事件驅動的再評估，不重跑管線](./0030-proposal-b-event-driven-reevaluation.md) | Accepted |
| 0031 | [規則版本化，加上 append-only 的 `quality_events` 狀態機](./0031-rule-versioning-quality-events.md) | Accepted |
| 0032 | [Bounded writeback：倉庫的判斷不回流進 ODS](./0032-bounded-writeback.md) | Accepted |
| 0033 | [歷史品質指標永不回溯改寫](./0033-historical-metrics-never-rewritten.md) | Accepted |
| 0034 | [Tier-1 營運指標與 Tier-2 分析指標的邊界](./0034-tier-1-tier-2-metrics.md) | Accepted |
| 0054 | [型別強轉由「宣告」治理，而非由「強轉行為」治理](./0054-type-declaration-governance.md) | Accepted |

## 編排

| # | 決策 | 狀態 |
|---|---|---|
| 0035 | [依賴隔離：兩個 venv，什麼都不裝進 Airflow 本身](./0035-two-venvs-dependency-isolation.md) | Accepted |
| 0036 | [DAG 檔不得在 top-level import 專案模組](./0036-dag-no-toplevel-import.md) | Accepted |
| 0037 | [`catchup=False` 是結構性的，不是為了省事](./0037-catchup-false-structural.md) | Accepted |
| 0038 | [刻意不對稱的重試：extract = 2、dbt = 0](./0038-asymmetric-retries.md) | Accepted |
| 0039 | [觀察訊號各自獨立成一個 DAG](./0039-observation-signals-own-dag.md) | Accepted |
| 0040 | [dbt 分層執行，但結尾仍跑一次完整 `dbt test`](./0040-layered-dbt-execution.md) | Accepted |
| 0041 | [`profiles.yml`：結構進版控，值進環境](./0041-profiles-yml-structure-vs-values.md) | Accepted |
| 0042 | [失敗通知寫「該做什麼」而非任務名；通道刻意留空](./0042-failure-notification-response-not-task.md) | Accepted |

## 轉換（dbt）

| # | 決策 | 狀態 |
|---|---|---|
| 0043 | [`stg_` 建表，不建 view](./0043-stg-table-not-view.md) | Accepted |
| 0044 | [用 `incremental` + `insert_overwrite` + `copy_partitions` 繞開 sandbox 的 DML 禁令](./0044-copy-partitions-sandbox-dml.md) | Accepted |
| 0045 | [`int_` 層刻意重複實作有效狀態邏輯，而非抽共用模型](./0045-int-effective-state-duplication.md) | Accepted |
| 0046 | [`stg_` 走增量，`int_` 走全量重建](./0046-stg-incremental-int-full-rebuild.md) | Accepted |
| 0047 | [度量上捲到表頭，由不變式測試把關](./0047-measures-roll-up-to-header.md) | Accepted |
| 0048 | [只建兩個維度、採 SCD1；事實表帶當時的快照](./0048-two-dimensions-scd1.md) | Accepted |
| 0049 | [業務報表永遠讀 Gold；品質報告拆成兩張表](./0049-business-reports-read-gold.md) | Accepted |

## 可觀測性

| # | 決策 | 狀態 |
|---|---|---|
| 0050 | [常駐 Collector，以及 `.env` 為何避開 OTel 的標準端點名稱](./0050-resident-otel-collector.md) | Accepted |
| 0051 | [logs 不走 OTLP](./0051-logs-not-over-otlp.md) | Accepted |
| 0052 | [用 SDK Views 控制序列預算——貴的是自動指標](./0052-sdk-views-series-budget.md) | Accepted |

---

## 相關

- [STATUS](../STATUS.md) — 做了什麼，以及狀態詞彙表
- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — 什麼被暫緩，真實系統會怎麼做
- [設計文件](../design/) — 決策定案之後，系統如何運作
