# ADR-0051：logs 不走 OTLP

[English](../../en/adr/0051-logs-not-over-otlp.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08-17 |
| **層** | 可觀測性 |

---

## 背景

OpenTelemetry 有三根支柱：traces、metrics、logs。既然 Collector 已經在位（ADR-0050），把 logs 也走它是顯而易見的收尾——一條管線、一個後端、關聯免費附送。

有兩件事反對現在做。

**logs 支柱在 Python 裡是最晚穩定的。** traces 與 metrics 的 API 已經定案；logging bridge 是三者中變動最大的，而採用它代表要追著那些變動跑，換取一個可以更便宜取得的好處。

**跨支柱關聯需要的恰好是兩個欄位。** 要從一行 log 跳到它的 trace，需要的是 `trace_id` 與 `span_id`——**不是一條傳輸通道。**

## 決策

**structlog 把 `trace_id` 與 `span_id` 注入每一筆 log 記錄**（W3C 格式：32 位與 16 位十六進位）。Logs 留在既有路徑——stdout，部署環境用 JSON、本機用 console（`log_format`）。

於是關聯是可用的——一行 log 指名它的 trace——**而 logs 支柱根本不在路徑上。**

## 後果

**關聯今天就可用**，代價是兩個欄位，而且不依賴一個不穩定的 API。

**Log 路由仍然是容器 runtime 的事**，那本來就是它所在的地方。`docker compose logs`、log driver，或任何與 collector 無關的 shipper 都繼續照常運作。

**代價是 logs 不在與 traces、metrics 相同的後端。** 從 Tempo 循著 `trace_id` 找到那行 log 是一個手動步驟，不是一次點擊。在這個規模可以接受；**對一個有值班的團隊就不行。**

**在可觀測性後端裡沒有結構化的 log 查詢。** 搜尋 log 意味著去容器 runtime 放它的地方搜尋。

**這不是對 logs 支柱的否決，只是對「現在採用」的否決。** 那兩個被注入的欄位，正是未來遷移所需要的東西，**所以這裡做的一切都不必被推翻。**

## 何時重新檢視

Python 的 logs 支柱穩定下來，**或者**真的有人在值班、而兩個後端之間的手動跳轉變成一個在時間壓力下付出的代價。

## 考慮過的替代方案

**現在就讓 logs 走 OTLP。** 一個後端、一個查詢介面，代價是追著一個不穩定的 API 跑，並新增一個失效模式：Collector 掛掉會遺失那些原本無論如何都會進 stdout 的 log 行。

**用獨立 agent 送 log（Promtail／Fluent Bit）。** 能在沒有 OTLP 依賴的前提下把 log 放進同一個後端——**這是一個真實的選項**，也是多一個要運行的元件，換一個目前沒有人在等的好處。

**乾脆不注入 `trace_id`。** 什麼都省不到，**而且放棄了讓「延後這根支柱」變得負擔得起的唯一那個性質。**

## 相關

- [ADR-0050](./0050-resident-otel-collector.md) — 這根支柱被排除在外的那條管線
- [ADR-0034](./0034-tier-1-tier-2-metrics.md) — 另一條「哪個訊號屬於哪裡」的邊界
