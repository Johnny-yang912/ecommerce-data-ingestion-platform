"""
Celery 應用實例：攝入路徑的任務佇列（取代 FastAPI BackgroundTasks）。

為什麼獨立成模組而不寫進 main.py：worker 行程只需要 import 這裡與 tasks.py，
不必拉起整個 FastAPI app（middleware、限流器、lifespan 都與背景處理無關）。
`celery -A celery_app` 因此成為 worker / beat 的單一入口。

與編排層的邊界：這個 broker 專屬攝入路徑（毫秒～秒級、單筆、由 HTTP 請求觸發），
與 Airflow（分鐘～小時級批次、由時鐘觸發）是正交的兩件事，**刻意不共用 Redis 實例**，
否則兩者的故障域會糾纏。詳見 ORCHESTRATION-TW.md〈範圍與職責邊界〉。
"""

from celery import Celery
from celery.signals import beat_init, setup_logging, worker_process_init

from config import settings
from logging_config import configure_logging

celery_app = Celery(
    "orders_ingestion",
    broker=settings.celery_broker_url,
    # task 模組在 worker 啟動時（finalize 階段）才 import，故 tasks.py 反向
    # import celery_app 不會構成循環 import。
    include=["tasks"],
)

celery_app.conf.update(
    # --- 序列化：只收 JSON ---
    # 不用 pickle：反序列化等同執行任意程式碼，broker 一旦被寫入即等同 RCE。
    task_serializer="json",
    accept_content=["json"],

    # --- 不開 result backend ---
    # 任務狀態的單一真相來源是 PostgreSQL 的 raw.status（pending / processing /
    # processed / error / duplicate），且已有 GET /raw/{raw_id} 可查。再開一份
    # Redis 結果狀態等於製造第二份會漂移的真相，還要額外處理過期策略。
    task_ignore_result=True,

    # --- ack 策略：late ack + worker 失聯即 requeue ---
    # 對照 worker 崩潰的兩個時點：
    #   崩在 claim commit 之前 → status 仍是 pending，訊息重投遞後 try_claim_raw
    #                            照樣搶得到，秒級恢復。
    #   崩在 claim commit 之後 → status 已是 processing，重投遞會 CAS 失敗、
    #                            task 直接 return；該筆改由 process.scan_and_recover
    #                            的 stale 掃描（STALE_PROCESSING_MINUTES）救回。
    # 也就是說 acks_late 在前者嚴格更好、在後者中性，代價是「同一則訊息可能被處理
    # 兩次」——而那正是既有冪等性（CAS claim + UNIQUE(ods.order_id)）本來就擋住的。
    task_acks_late=True,
    task_reject_on_worker_lost=True,

    # acks_late 的標配：不預抓。否則單一 worker 抓走一批訊息後崩潰，
    # 那整批都要等 visibility_timeout 到期才會重見天日。
    worker_prefetch_multiplier=1,

    broker_transport_options={
        # Redis 沒有真正的 ack，靠 visibility timeout 模擬重投遞。此值必須大於最長任務
        # 耗時，否則任務還在跑就被重投遞 → 重複執行風暴。process_raw_event 最壞情況是
        # 三層 exponential backoff（秒級），600s 留足餘裕。
        "visibility_timeout": 600,
        # broker 不可用時的等待上限。不設的話會退回 OS 層的 DNS / TCP 逾時——實測
        # `docker compose stop redis` 後，單次 POST /orders 卡了 19 秒才回應。
        # broker 對攝入路徑是「可選的」（派不出去就交給 recovery scan），
        # 它的故障絕不該讓請求端付出這種等待。
        "socket_connect_timeout": 2,
        "socket_timeout": 2,
        "retry_on_timeout": False,
    },
    broker_connection_timeout=2,

    # 送訊息失敗時只再試一次就放棄。瞬斷值得一次重試，真的掛了則要快速讓出控制權給
    # `_enqueue` 的降級路徑——重試越久，越只是把「已經有兜底的失敗」拖成請求延遲。
    task_publish_retry_policy={
        "max_retries": 1,
        "interval_start": 0,
        "interval_step": 0.1,
        "interval_max": 0.1,
    },

    # 不讓 Celery 劫持 stdout。它預設會把 sys.stdout 導進自己的 logger 並**一律以
    # WARNING 記錄**，導致 structlog 的每一行（含 info）在 worker 端都被重貼成
    # WARNING，log level 從此失去意義、告警規則也跟著失準。
    worker_redirect_stdouts=False,

    # 週期恢復掃描。原本是 FastAPI lifespan 裡的 asyncio 迴圈（行程內狀態，
    # 多 uvicorn worker 會各跑一份），搬到 Beat 之後 API 行程才真正無狀態。
    beat_schedule={
        "recovery-scan": {
            "task": "tasks.scan_and_dispatch",
            "schedule": float(settings.scan_interval_seconds),
        },
    },

    timezone="UTC",
    enable_utc=True,
)


@beat_init.connect
def _initial_recovery_scan(**_kwargs) -> None:
    """
    Beat 啟動時立刻補一次掃描。

    Beat 的第一次 tick 要等滿一個 schedule 間隔（預設 300s）才發生，中間如果有
    上一輪留下的 pending / stale processing，就會多躺 5 分鐘沒人管——原本掛在
    FastAPI lifespan 的 startup recovery 正是為了填這個洞。

    掛 beat_init 而非 API 的 lifespan，是因為「排程器重啟」才是該補掃的時機：
    API 重啟不代表有東西需要恢復，而 API 若跑多個行程，掛在 lifespan 會變成
    每個行程各補掃一次。
    """
    from tasks import scan_and_dispatch_task

    scan_and_dispatch_task.delay()


@setup_logging.connect
def _configure_worker_logging(**_kwargs) -> None:
    """
    worker / beat 啟動時套用專案自己的 structlog 設定。

    沒有這一步，`configure_logging()` 只會在 main.py（API 行程）被呼叫，worker 端
    等於沒設定過——`LOG_FORMAT=json` 對 worker 完全無效，正式環境會吐 console 格式。

    掛在 setup_logging 而非 worker_process_init 的理由：Celery 只要偵測到這個 signal
    有接收者，就會整段跳過自己的 logging 設定，不再搶 root logger。於是 Celery 自身的
    log（Task received / succeeded）也會流經同一個 ProcessorFormatter，與應用 log
    輸出同一種格式——這正是可觀測性想要的：一個 worker 只有一種 log 格式。
    """
    configure_logging()


@worker_process_init.connect
def _dispose_inherited_engine(**_kwargs) -> None:
    """
    prefork 子行程啟動時丟掉繼承來的 SQLAlchemy engine。

    database.py 在 **import 時** 就建好 engine，Celery 的 prefork 會在父行程
    import 完成後 fork——子行程因此繼承到同一批已開啟的 TCP socket。多個行程
    共用同一條連線會讓 psycopg2 的協定狀態互相踩踏（InterfaceError、回應串線、
    甚至讀到別的行程的結果）。dispose() 讓每個子行程按需重建自己的 pool。
    """
    from database import engine

    engine.dispose()
