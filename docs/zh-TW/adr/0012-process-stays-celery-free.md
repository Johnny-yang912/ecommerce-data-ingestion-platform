# ADR-0012：`process.py` 保持 Celery-free 以保留手動救援路徑

[English](../../en/adr/0012-process-stays-celery-free.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 任務佇列 |

---

## 背景

把 `process_raw_event` 變成 Celery task 最直接的方式，是在函式本身貼上 `@celery_app.task`。一行，不需要包裝模組。

它同時也把核心處理邏輯耦合到傳輸層——而這個耦合會在它最要命的那個情境咬人：**當傳輸層正是壞掉的那一個時。**

## 決策

`process.py` **零 Celery import**。裝飾器住在 `tasks.py` 的一層薄包裝裡：

```python
# tasks.py
@celery_app.task
def process_raw_event_task(raw_id): ...   # 呼叫 process.process_raw_event
```

這買到三件事：

**手動救援路徑保持暢通。** broker 掛掉時，一筆記錄仍然可以用 `python -c "from process import process_raw_event; ..."` 處理——不涉及佇列，不需要 broker。那條路徑存在的理由，正是為了「佇列就是問題本身」的情況。

**測試與腳本直接呼叫邏輯。** pytest 不需要 broker，`reevaluate_quality.py` 或任何臨時腳本也不需要。

**換掉佇列只動一個檔案。** 若哪天 Celery 被取代，改的是 `tasks.py`，處理邏輯不動。

## 這層包裝刻意很薄

`tasks.py` **不設 `autoretry_for`、不設 `max_retries`**，而這是一個決策而非疏漏。

`process.py` 內已經有四層重試：Raw 寫入、認領、處理、狀態提交。在上面再疊一層 Celery 重試會產生 **3 × 3 的重試放大**，更糟的是它會把 `error` 這個終端狀態的語意弄糊——一筆記錄可以同時處於 `error`、又同時被傳輸層安排了下一次嘗試。

`process_raw_event` 設計上**不對外拋例外**。它返回時，每一種失敗都已經被記進 `raw.status` 了。Celery 不需要、也不應該再判斷結果。

> **傳輸層的職責是投遞。一旦業務層記下了終端狀態，傳輸層就沒有任何東西還需要決定。**

## 後果

**多一個模組、多一層間接**，對追蹤呼叫路徑的讀者而言。這就是全部的代價。

**救援路徑必須真的能用**，這代表 `process.py` 不可以不小心長出一個 Celery import。今天沒有任何東西自動強制這件事——它是由這條 ADR 與模組 docstring 所持有的紀律。

這不是假設性的：同一類意外發生在 `check_raw_pending.py` 上，那裡一個共用常數把一支唯讀探針耦合到整條寫入路徑的依賴樹，並在一次不相干的部署中把它弄壞了（見 ADR-0039）。當時的修法是把常數抽進 `recovery_policy.py`，並用 `tests/test_script_deps.py` 釘住。

## 考慮過的替代方案

**直接裝飾 `process_raw_event`。** 省下一個模組；代價是救援路徑，以及讓每一個測試與腳本都得 import Celery。

**把包裝放進 `main.py`。** worker 行程就得 import 整個 FastAPI app——middleware、限流器、lifespan——而背景處理一個都不需要。`celery -A celery_app` 是 worker 與 beat 更乾淨的單一入口。

## 相關

- [ADR-0010](./0010-celery-replaces-backgroundtasks.md) — 被包裝的那個佇列
- [ADR-0013](./0013-bounded-broker-wait.md) — 這條路徑所服務的 broker-down 情境
- [ADR-0039](./0039-observation-signals-own-dag.md) — 同一個耦合意外，以及它如何被釘住
