"""模擬上游持續攝入：每天分四個時段把訂單打進 POST /orders。

    09:00 ─┐
    13:00 ─┤  每個時段一個 DAG run（各自獨立，失敗不影響其他時段）
    17:00 ─┤
    21:00 ─┘  → 23:00 orders_analytics_daily 抽取 + dbt

本專案沒有、也不會有真實上游——seeding **就是**那個上游。這條 DAG 因此不是
「示範用的輔助工具」，而是資料來源本身。ORCHESTRATION-TW §4 原本把它列為
「刻意先不做」，觸發點是「需要 BI 圖表持續有資料時」；那個條件成立了。

────────────────────────────────────────────────────────────────────────────
① 為什麼是獨立 DAG，而不是接在 orders_analytics_daily 前面 ⭐
   與 source_freshness_watch 當初被拆出去是【同一個論證】：**DAG 成功率必須
   是有意義的訊號**。seeding 掛掉與分析管線掛掉是兩件事，混在一條 DAG 裡的話
   「主 DAG 紅」就同時代表兩種完全不同的處置方式，訊號價值歸零。
   拆開之後：
     seed_demo_daily 紅   = 灌不進去（API/worker/redis 出事）
     orders_analytics_daily 紅 = 管線本身壞了
   量測佐證：主 DAG 全程 2.5 分鐘，單一 seeding slot 就要 3~5 分鐘（限流所致）。
   併進去會讓主 DAG 的執行時間主要由「等限流」構成，那不是它該負責的事。

② 時區用 Asia/Taipei 宣告，不自己換算 UTC ⭐
   假想的服務對象是台灣的營運團隊，早上 9 點看前一天的報表 → 報表必須在
   台北 09:00 前備妥 → 抽取與 dbt 排台北 23:00。把這個推理留在程式碼裡，
   比寫成 `schedule="0 15 * * *"` 然後在註解解釋「15 是台北 23 點」可靠得多。

③ ⚠️ 所有時段必須在台北 08:00 之後——這是正確性約束，不是偏好 ⭐
   Hard Gate 的口徑是【最新的一個 UTC 日分區】（macros/error_rate_below.sql），
   而 `received_at` 是 TIMESTAMP、`date()` 在 **UTC** 換日。
   台北 00:00–08:00 = 前一個 UTC 日 → 那批資料會落到**前一天的分區**，
   於是「今天灌的資料」被拆成兩個分區，gate 判的那批就不是你以為的那批，
   當天的髒率也不再是你設定的值。
   09/13/17/21 全部落在 UTC 同一日（01/05/09/13），這個約束才成立。

④ 髒率【全天一致】、payload 種子【逐時段不同】⭐
   兩者的種子刻意不同源，因為它們要對抗的問題相反：
     · 髒率同源於 ds_nodash → 全天四個 slot 相同。Hard Gate 判的是全天加權平均
       （見 ③），各 slot 不同的話「今天的髒率」根本不是一個有定義的東西。
     · payload 種子帶上時段 → 四批資料不同。若共用 ds_nodash，四個 slot 會產生
       **一模一樣的訂單**（同 seed = 同亂數序列），只有 order_id 因 wall-clock
       批次標記而不同——customer/product 分佈會被複製四份，dim_ 表嚴重失真。

⑤ retries=0，而且理由與 extract 相反 ⭐
   extract 重試是安全的（`>=` watermark 寧可重抓不漏抓，重複由 stg_ 去重）。
   **seeding 重試是真的多灌一批**——order_id 帶 wall-clock 批次標記，不會撞
   冪等，當天資料直接翻倍、髒率被稀釋、Hard Gate 的門檻線失真。
   這是 ORCHESTRATION-TW §2.6「retry 不對稱」原則的第三個案例：
   **重試的前提是該操作冪等，而不是「失敗了就再試一次」這個直覺。**

⑥ --require-landed-pct 是這條 DAG 唯一擋得住靜默失敗的東西 ⭐
   `POST /orders` 回 202 只代表 Raw 落地，ODS 由 Celery worker 非同步寫入，
   而 `main._enqueue` **刻意吞掉 broker 故障**。worker 或 redis 掛掉時，腳本會
   印滿版的 ok、exit 0、DAG 全綠，而 ODS 一筆都沒有。無人值守的排程正是最不
   可能有人察覺這件事的場景——它會一路綠到有人問「為什麼報表沒更新」為止。

⑦ catchup=False：`received_at` 是伺服器在攝入當下產生的，**回填在物理上不可能**。
   補跑 2026-08-01 的 run 只會在今天再灌一批。與主 DAG 的 catchup=False 結論
   相同但成因不同（那邊是 watermark 語意，這邊是時間戳的產生方式）。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"
PY_ANALYTICS = "/home/airflow/venvs/analytics/bin/python"

TZ = pendulum.timezone("Asia/Taipei")

# 各時段的筆數。刻意不等量且傍晚較重——等量會讓 received_at 的日內分佈成為一條
# 直線，而真實電商的下單量本來就有日間節奏。總量 800/天。
# ⚠️ 時段本身（09/13/17/21）不可改到台北 08:00 之前，理由見檔頭 ③。
SLOT_SIZES = {9: 150, 13: 200, 17: 200, 21: 250}

# 手動觸發時 data_interval_start 是「現在」，其台北小時幾乎不會落在 SLOT_SIZES 的鍵上。
# 用 .get(hour, DEFAULT) 而非 [hour]，否則手動跑必定以 Jinja KeyError 收場——
# 而手動觸發正是最需要它能動的時候（補灌、驗證改動、demo）。
DEFAULT_SLOT_SIZE = 200

# 候選髒率。由 ds_nodash 決定性地選一個，全天四個時段共用（檔頭 ④）。
#
# 上限刻意止於 12% 而非更靠近 15% 的 Hard Gate：n=800 時抽樣標準差約 1.2 個
# 百分點，設定 13% 會有 4.6%/天的機率【意外】超標（約每 22 天一次），12% 則降到
# 0.45%（約每 222 天）。而「主 DAG 紅了但不是管線壞了」正是本設計最想避免的事
# ——要展示 Hard Gate 攔截，走 seed_demo_gate_demo 那條手動劇本，不要靠碰運氣。
DIRTY_RATE_CHOICES = "0.03,0.05,0.08,0.10,0.12"

# 落地率門檻。不設 1.0：偶發的單筆 500（例如 non_finite_number 注入撞上
# starlette 的 allow_nan=False）是已知且無害的，不該讓整個 run 失敗。
# 0.9 能通過零星失敗，但擋得住「worker 沒起 → 落地 0%」這個真正要防的狀態。
REQUIRE_LANDED_PCT = 0.9

# 這個 DAG 的時間基準是【觸發時刻】，而不是 data_interval_start / logical_date ⭐
# ─────────────────────────────────────────────────────────────────────────────
# cron 排程的 data_interval 是「上一次觸發 → 這一次觸發」，所以 data_interval_start
# 指的是**上一個時段**。實測（timetable 直接推演）：
#
#     觸發於 08-12 13:00 → data_interval_start = 08-12 09:00  (hour=9,  ds=20260812)
#     觸發於 08-12 21:00 → data_interval_start = 08-12 17:00  (hour=17, ds=20260812)
#     觸發於 08-13 09:00 → data_interval_start = 08-12 21:00  (hour=21, ds=20260812) ⚠️
#     觸發於 08-13 13:00 → data_interval_start = 08-13 09:00  (hour=9,  ds=20260813)
#
# 第三列是致命的：08-13 早上那批會拿到 **20260812** 的 ds_nodash，於是當天四批裡
# 有一批的髒率來自前一天的種子——檔頭 ④「髒率全天一致」直接破功，而 Hard Gate
# 的整套推論就建立在那個一致性上。這個錯誤不會有任何症狀，只會讓當日髒率變成
# 一個沒人算得出來的混合值。
#
# 用 dag_run.run_after（＝這個 run 實際該執行的時刻）同時導出日期與時段，兩者
# 因此必然同源、必然落在同一個台北日曆日。它對手動觸發也有定義，而
# data_interval_start 在 Airflow 3 的手動 run 裡【根本不存在】（UndefinedError）。
#
# ⚠️ 時區轉換寫成 Python 函式而不是塞進模板 ⭐
#    `dag_run.run_after` 是 **stdlib datetime，不是 pendulum**，所以模板裡寫
#    `.in_timezone(...)` 會在 runtime 炸 UndefinedError。與其記住哪個 context 變數
#    是哪種型別，不如把轉換收進一個函式——它同時讓這段邏輯可以被單元測試直接呼叫，
#    而不必依賴「我對 Airflow 傳什麼型別的猜測」（那個猜測正是這裡踩過的坑）。
def to_taipei(dt):
    """把 context 給的時間換算成台北時區。stdlib datetime 與 pendulum 都適用。"""
    return dt.astimezone(TZ)


# 運算式刻意不含 {{ }}：要被嵌進別的運算式（SLOT_SIZES.get(...)），而 Jinja 的
# {{ }} 不能巢狀——`{{ f({{ x }}) }}` 是語法錯誤，且只在 task 執行時才炸。
_RUN_TPE = "to_taipei(dag_run.run_after)"
SLOT_HOUR = f"{_RUN_TPE}.hour"
SLOT_DATE = f'{_RUN_TPE}.strftime("%Y%m%d")'

with DAG(
    dag_id="seed_demo_daily",
    description="模擬上游持續攝入：每天 09/13/17/21（台北）各灌一批訂單",
    # 一個時段 = 一個 DAG run。四個 task 併在同一個 run 裡是做不到的——
    # Airflow 的 task 在相依滿足時就會跑，不會各自等到自己的時鐘時間。
    schedule="0 9,13,17,21 * * *",
    start_date=pendulum.datetime(2026, 8, 11, tz=TZ),
    catchup=False,          # 見檔頭 ⑦
    max_active_runs=1,      # 兩個 run 並行會互撞 60/min 限流，雙方一起退避
    default_args={
        "owner": "data-eng",
        "retries": 0,       # 見檔頭 ⑤
        "email_on_failure": False,
    },
    user_defined_macros={
        "SLOT_SIZES": SLOT_SIZES,
        "DEFAULT_SLOT_SIZE": DEFAULT_SLOT_SIZE,
        "to_taipei": to_taipei,
    },
    tags=["seeding", "ingestion", "simulated-upstream"],
    doc_md=__doc__,
) as dag:

    BashOperator(
        task_id="seed_orders",
        # $SEED_API_URL 由 compose 注入（見 docker-compose.airflow.yml）。
        # ⚠️ 不 cd 進 /opt/project：BashOperator 預設 cwd 是臨時目錄，讓
        #    seed_demo 的 load_dotenv(".env") 找不到檔案而純吃容器環境變數。
        #    cd 過去會讀到 bind mount 進來的主機 .env，那裡的 DB_URL 在容器內是錯的。
        bash_command=(
            f"{PY_ANALYTICS} {PROJECT_DIR}/seed_demo.py"
            ' --url "$SEED_API_URL"'
            # ⚠️ 這一段刻意【不用 f-string】：f-string 裡的 }} 會被跳脫成單一個 }，
            #    於是 Jinja 的結束標記 }} 被吃掉一半，模板變成語法錯誤——而且同樣
            #    只在 runtime 才炸。凡是要輸出 Jinja 標記的字串，一律用字串串接。
            " --n {{ SLOT_SIZES.get(" + SLOT_HOUR + ", " + str(DEFAULT_SLOT_SIZE) + ") }}"
            # 髒率同源於日期（全天一致），payload 種子＝日期＋時段（逐 slot 不同）。見檔頭 ④。
            # 兩者都由 run_after 導出，故必然是同一個台北日曆日——用 ds_nodash 會
            # 讓 09:00 那批取到前一天（見上方 _RUN_TPE 的說明）。
            f" --dirty-rate-choices {DIRTY_RATE_CHOICES}"
            " --dirty-rate-seed {{ " + SLOT_DATE + " }}"
            " --seed {{ " + SLOT_DATE + " }}{{ " + SLOT_HOUR + " }}" +
            f" --require-landed-pct {REQUIRE_LANDED_PCT}"
            " --verify-wait 60"
        ),
        # 250 筆 @ 0.8rps ≈ 5.2 分鐘，加 60 秒 verify 等待。給到 20 分鐘是為了
        # 吸收 429 退避；再長就不是退避而是真的卡住了，該讓它失敗。
        execution_timeout=pendulum.duration(minutes=20),
    )
