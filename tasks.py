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

from celery_app import celery_app
from process import process_raw_event


# 明確指定 name：task 名稱是 broker 上的線路契約，綁死在函式名上的話，
# 日後改名會讓佇列裡既有的訊息找不到對應 task（NotRegistered）。
@celery_app.task(name="tasks.process_raw_event")
def process_raw_event_task(raw_id: int) -> None:
    process_raw_event(raw_id)
