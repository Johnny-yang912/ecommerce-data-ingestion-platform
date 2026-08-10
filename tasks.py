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

import uuid
from contextlib import contextmanager

import redis
import structlog

from celery_app import celery_app
from config import settings
from process import SCAN_BATCH_SIZE, process_raw_event, scan_and_recover

logger = structlog.get_logger()

# 單次 task 最多走幾頁。上界 = SCAN_MAX_ROUNDS × SCAN_BATCH_SIZE，
# 用來擋住「一次 task 想把數十萬筆積壓全清完」——那會讓單一 worker 槽位被佔住
# 很久，也讓鎖的 TTL 難以估算。清不完就留給下一輪 tick，掃描本來就是週期性的。
SCAN_MAX_ROUNDS = 20

_SCAN_LOCK_KEY = "recovery-scan-lock"
# TTL 取一個掃描間隔：task 崩潰沒能釋放時，最多犧牲一輪 tick。
_SCAN_LOCK_TTL_SECONDS = 300

# 只刪自己那把鎖。先 get 再 delete 不是原子的——中間鎖可能已過期並被別人取得，
# 那樣會誤刪別人的鎖，讓兩輪掃描同時進行。
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


@contextmanager
def _scan_lock():
    """
    防止兩輪恢復掃描重疊。

    Beat 每 scan_interval_seconds 派一次，不管上一輪跑完沒有。積壓很深時單輪會
    跑滿 SCAN_MAX_ROUNDS，下一次 tick 可能在它還沒結束時就到——兩輪會撈到同一批
    記錄、送出兩份訊息。CAS 保得住正確性，但佇列量會平白翻倍。

    **這把鎖是最佳化，不是正確性要求**：所以取鎖本身失敗（Redis 抽風）時選擇
    照常執行，而不是跳過掃描——跳過的代價是記錄卡著沒人管，比重複派工嚴重得多。
    只有「明確被別人持有」才略過。

    直接用 redis 客戶端而非 kombu 的連線，是為了不依賴 transport 內部結構；
    每輪掃描才建一次連線，成本可忽略。
    """
    client = redis.Redis.from_url(
        settings.celery_broker_url, socket_connect_timeout=2, socket_timeout=2
    )
    token = str(uuid.uuid4())
    held = False
    try:
        held = bool(client.set(_SCAN_LOCK_KEY, token, nx=True, ex=_SCAN_LOCK_TTL_SECONDS))
        acquired = held
    except Exception:
        logger.warning("recovery scan 取鎖失敗，仍照常執行", exc_info=True)
        acquired = True

    try:
        yield acquired
    finally:
        if held:
            try:
                client.eval(_RELEASE_LOCK_LUA, 1, _SCAN_LOCK_KEY, token)
            except Exception:
                logger.warning("recovery scan 釋放鎖失敗，將由 TTL 過期", exc_info=True)


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

    ── 分頁、批次發佈、重疊保護 ─────────────────────────────────────────────

    以 id 為游標一頁一頁往前推（`scan_and_recover` 的 after_id），單次最多
    SCAN_MAX_ROUNDS 頁。記憶體佔用因此由 SCAN_BATCH_SIZE 決定，與積壓總量無關。

    整批共用同一個 producer：`.delay()` 每次都要向 producer pool 取用，數十萬次
    等於數十萬次取用；`apply_async(producer=...)` 讓整輪只走一條連線。

    單輪跑滿 SCAN_MAX_ROUNDS 代表積壓還沒清完，記一筆 warning——這是「恢復路徑
    跟不上」的直接訊號，比事後從佇列長度反推容易得多。
    """
    with _scan_lock() as acquired:
        if not acquired:
            logger.info("recovery scan 略過：上一輪仍在進行")
            return 0

        total = 0
        after_id = 0
        with celery_app.producer_pool.acquire(block=True) as producer:
            for _ in range(SCAN_MAX_ROUNDS):
                raw_ids = scan_and_recover(after_id=after_id)
                for raw_id in raw_ids:
                    process_raw_event_task.apply_async((raw_id,), producer=producer)
                total += len(raw_ids)
                if len(raw_ids) < SCAN_BATCH_SIZE:
                    break
                after_id = raw_ids[-1]
            else:
                logger.warning(
                    "recovery scan 達單輪上限，積壓未清完，等下一輪 tick 續傳",
                    dispatched=total,
                    max_rounds=SCAN_MAX_ROUNDS,
                )

        logger.info("recovery scan 完成", count=total)
        return total
