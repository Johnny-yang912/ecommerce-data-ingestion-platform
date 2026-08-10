"""
Celery task 定義：傳輸層與業務邏輯之間的薄包裝。

為什麼不把 @celery_app.task 直接貼在 process.process_raw_event 上：
- process.py 得保持零 Celery 依賴，才能被 pytest、腳本、以及「broker 掛了手動補跑」
  這條救援路徑（`python -c "from process import process_raw_event; ..."`）直接呼叫。
- 未來若換掉佇列實作，只有這一層要改，核心處理邏輯不動。

也刻意不設 autoretry_for / max_retries：process.py 內已有四層 retry（Raw 寫入、
claim、processing、status 更新），再疊一層 Celery retry 會變成 3×3 的重試放大，
並讓 `error` 這個終態的語意變糊。process_raw_event 設計上不對外拋例外——所有失敗
都已經落成 raw.status，Celery 不需要、也不應該再介入判斷。
"""

import structlog

from celery_app import celery_app
from process import process_raw_event, scan_and_recover

logger = structlog.get_logger()


# 明確指定 name：task 名稱是 broker 上的線路契約，綁死在函式名上的話，
# 日後改名會讓佇列裡既有的訊息找不到對應 task（NotRegistered）。
@celery_app.task(name="tasks.process_raw_event")
def process_raw_event_task(raw_id: int) -> None:
    process_raw_event(raw_id)


@celery_app.task(name="tasks.scan_and_dispatch")
def scan_and_dispatch_task() -> int:
    """
    週期恢復掃描：重設 stale processing、收集所有 pending，逐一派工。回傳掃到的筆數。

    由 Celery Beat 觸發（見 celery_app.beat_schedule），取代原本掛在 FastAPI
    lifespan 裡的 `_periodic_scan` 迴圈。搬家的理由不是「Beat 比較好看」，而是
    那個迴圈是**行程內狀態**：API 一旦跑多個 uvicorn worker，每個行程都會各跑一份
    掃描迴圈。把它移出去，API 行程才真正無狀態、可水平擴展。

    **持久化佇列並沒有讓這個掃描變成冗餘**，反而讓它更關鍵——它負責的正是佇列
    自己救不回來的那一半，見 QUEUE-TW.md〈CAS claim 與重新投遞的交互作用〉：
      - worker 崩在 claim commit 之前 → status 仍 pending，訊息重投遞就能自行復原。
      - worker 崩在 claim commit 之後 → status 已是 processing，重投遞會 CAS 失敗、
        任務直接 return。**只有這裡的 stale 掃描能救它。**

    不捕捉 `.delay()` 的例外：broker 不可用時這個 task 根本不會被執行到（它自己也
    是從 broker 來的）。真的派到一半失敗就讓 task 整個失敗——所有記錄仍是 pending，
    下一輪掃描會原封不動再撈一次。`scan_and_recover` 本身冪等，重跑無害。
    """
    raw_ids = scan_and_recover()
    for raw_id in raw_ids:
        process_raw_event_task.delay(raw_id)
    logger.info("recovery scan 完成", count=len(raw_ids))
    return len(raw_ids)
