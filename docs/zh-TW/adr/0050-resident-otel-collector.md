# ADR-0050：常駐 Collector，以及 `.env` 為何避開 OTel 的標準端點名稱

[English](../../en/adr/0050-resident-otel-collector.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08-17 |
| **層** | 可觀測性 |

---

## 背景

應用程式可以把遙測直接匯出到雲端後端。這麼做會把端點、憑證與匯出設定放進每一個會發出東西的行程——API、Celery worker、Beat，以及未來的 Airflow。

那是**四份祕密的副本**，以及後端搬家時要改的四個地方。

## 決策

**一個常駐的 OpenTelemetry Collector**（contrib 0.158.0，`otel/collector-config.yaml`）。應用程式一律以明文匯出到本機 Collector；**雲端端點與憑證只存在於一個地方。**

Exporter：Grafana Cloud（`ap-southeast-1`，Tempo + Prometheus）。

### ⚠️ `.env` 刻意避開 `OTEL_EXPORTER_OTLP_ENDPOINT`

這是值得記錄的部分，**因為它所防止的失效是靜默的**。

**任何看到 `OTEL_EXPORTER_OTLP_ENDPOINT` 的 SDK 都會直接匯出到它所指的地方**——完全繞過 Collector。**不會有任何錯誤，而且資料仍然會抵達**，所以看起來一切正常。Collector 只是不再在路徑上，而隨之消失的是憑證集中、處理管線，以及控制序列預算的那些 View（ADR-0052）。

因此那個標準名稱被**保留給「app → Collector」**，而雲端目的地用一個 SDK 不會誤撿的名稱設定。

這與 ADR-0008 拒絕在 `Settings` 裡重新宣告 OTel 設定是同一套推理：**不要把一個值放在另一層的慣例會靜默吃掉它的地方。**

## 後果

**輪替憑證或更換後端只有一個地方要改。**

**應用程式不帶任何雲端設定**，所以本機執行或測試除了 `otel_enabled=False`（預設值）之外什麼都不需要。

**埋點是 no-op-safe 的。** OTel 關閉時那些指標 instrument 是 proxy——呼叫它們不做事也不拋錯。**那正是 `process.py` 的埋點一律不加 `if otel_enabled` 判斷的原因。**

**Airflow 一直都可以加入**——明文送 `otel-collector:4318`，兩者本來就在同一個 compose 網路。**擋住那個整合的不是技術**；它的每一個消費端都被暫緩了。見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

**Trace context 跨行程被縫合起來。** `api` → Celery → `worker` 這條鏈共用同一個 `trace_id`，已在兩個行程上驗證。SDK 初始化掛在 `worker_process_init` 上，因為 **`BatchSpanProcessor` 是一個背景執行緒，而執行緒不會跨 `fork` 被繼承**——與 `_dispose_inherited_engine` 是同一個根因、同一個 hook。

**代價是多一個必須運行的容器**，否則遙測離不開這台主機。

## 考慮過的替代方案

**每個應用程式直接匯出。** 憑證四份副本、換後端要改四個地方，而且沒有一個地方可以統一套用處理或取樣。

**每個服務一個 sidecar Collector。** 隔離更好、容器更多，而憑證又回到 N 個地方。

**用標準的 `OTEL_EXPORTER_OTLP_ENDPOINT` 指向雲端目的地。** 就是上面那個靜默繞過的危害。**一個慣用名稱的方便，不值得換一個「會透過錯誤路徑產生看起來正確的資料」的失效模式。**

## 相關

- [ADR-0008](./0008-config-boundary.md) — 為何 `Settings` 只宣告 `otel_enabled`
- [ADR-0051](./0051-logs-not-over-otlp.md) — 刻意被排除在這條路徑外的那根支柱
- [ADR-0052](./0052-sdk-views-series-budget.md) — Collector 路徑讓什麼成為可能
