# ADR-0016：恢復掃描住在 Beat，不住在 API 行程

[English](../../en/adr/0016-recovery-scan-in-beat.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 任務佇列 — 恢復 |

---

## 背景

恢復掃描原本跑在 FastAPI `lifespan` 啟動的 asyncio 迴圈上。它能運作，而它是**行程內狀態**——這代表 API 被釘死在 `--workers 1`。第二個 uvicorn 行程會跑第二個掃描迴圈，讓每一筆卡住的記錄被派工兩次。

改用 Celery（ADR-0010）移除了兩份釘死 API 的狀態之一。這條 ADR 是另一份。

## 決策

掃描成為一個 Celery Beat 排程項目：

```python
beat_schedule = {
    "recovery-scan": {
        "task": "tasks.scan_and_dispatch",
        "schedule": float(settings.scan_interval_seconds),   # 300s
    },
}
```

**Beat 同時會在啟動時放一次補掃**，關掉單靠間隔會留下的一個缺口：沒有它的話，重啟代表第一次掃描要等一個完整間隔之後才發生，而重啟期間卡住的東西就要等那麼久。

**⚠️ Beat 絕不可被 `--scale`。** 兩個 beat 行程會把每一次排程掃描派出兩份。API 可以擴充、worker 可以擴充；**beat 是那個單例**。`docker-compose.yml` 讓三者共用同一個映像、只差在啟動命令——這讓這條限制很容易被不小心違反，因此值得大聲寫出來。

## 後果

**API 行程對背景工作而言變成無狀態的**，這才終於讓 `UVICORN_WORKERS > 1` 成為可能。

**而那立刻暴露了第二份行程內狀態**：slowapi 把限流計數器放在行程記憶體裡。跨 N 個行程，`60/minute` 會靜默地變成 `60 × N`——實測 4 個 worker 讓 100 個請求中的 **91 個**通過而不是 60，**而且沒有任何地方報錯**。計數器必須在同一次變更中搬到 Redis（db 1，與 broker 的 db 0 分開）。

> **移除一份行程內狀態不會讓行程變成無狀態的。它讓下一份狀態變得可見——而那一份是靜默的。**

**掃描的時序不再與 API 部署耦合。** 重啟 API 不再重設掃描時鐘。

**代價是第三個要運行與監控的長駐行程。**

## 考慮過的替代方案

**在 API 行程之間做 leader election。** 能讓掃描留在 API 且允許擴充，代價是一套協調機制——而那套協調依賴 Redis，正是掃描存在要恢復其故障的那個元件。

**應用程式之外的 cron job。** 應用內的活動零件變少，代價是在 Airflow 與 Beat 之外多一套排程機制，而且部署敘事跑到 compose 之外。

**在每個 worker 上都跑掃描。** 每個 worker 都重複派工——CAS 讓它安全（ADR-0004），但浪費的 worker 槽位正比於 worker 數量，而那正是 worker 稀缺的時候。

## 相關

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — 移除行程內狀態的另一半
- [ADR-0017](./0017-bounded-recovery-scan.md) — 這支掃描在負載下必須變成什麼樣子
- [ADR-0008](./0008-config-boundary.md) — 為何 `scan_interval_seconds` 是設定、而門檻不是
