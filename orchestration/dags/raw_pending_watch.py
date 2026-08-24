"""派工路徑的存活探針：Raw 裡的列有沒有被 worker 取走處理。

    10:30 ─┐
    13:30 ─┤  每個 seeding 時段開始後 30 分鐘檢查一次
    17:30 ─┤
    21:30 ─┘

紅代表**第四種意義**。四支 DAG 的紅各自對應一種處置，這是它們被拆開的全部理由：

    seed_demo_daily         灌不進去（API 拒收 / seeding 腳本本身壞了）
    raw_pending_watch       Raw 進得來但沒人來取（redis / worker / beat）  ← 本 DAG
    orders_analytics_daily  管線壞了（extract 或 dbt）
    source_freshness_watch  staging 沒被推進（extract 空搬的 backstop）

────────────────────────────────────────────────────────────────────────────
① 它量的是【派工與取件】，不是【有沒有寫進 ODS】——這個區別是必要的 ⭐
   Raw 的終態有三種：`processed`、`duplicate`、`error`。**後兩者本來就不會產生
   ODS 列，而且那是正確行為**（duplicate 是刻意保留的監控訊號，見 CLAUDE.md 的
   架構約束）。所以「Raw 有列沒有對應的 ODS 列」不是故障的定義——照這個定義做
   探針，每一筆重複訂單都會讓它紅。

   本探針因此只看 `status='pending'`：**已經落到 Raw、但還沒有任何 worker 把它
   取走**。這個狀態沒有任何正當理由長期存在——它一定意味著派工路徑斷了。
   至於取走之後變成 processed 還是 duplicate/error，那是資料內容的事，
   由 DQ 機制負責，不歸這裡。

② 它補的是哪個洞 ⭐
   `main._enqueue` 刻意吞掉 broker 故障，所以 redis/worker 掛掉時 `POST /orders`
   照樣回 202、Raw 一直長、卻沒有任何行程來取件，而且沒有任何地方會報錯。
   下游的可見症狀是 ODS 不再成長，但**根因在派工**——本探針量的正是根因那一端，
   這也是它比「盯著 ODS 有沒有變多」更早、更明確的原因。

   `seed_demo.py --require-landed-pct` 擋的是同一件事，但它**活在資料產生器裡**
   ——真實系統的上游是別人的，不會有那個保護。本 DAG 是把它搬到平台側。
   量測與門檻推導見 `check_raw_pending.py` 檔頭。

③ ⚠️ 它是【過渡期的粗篩】，不是告警 ⭐
   實務上的存活監控是「秒級取樣 + 要求連續超標數分鐘」（Prometheus 的 `for:`）。
   **Airflow 表達不了後者**——每個 DAG run 都是獨立、無記憶的取樣，沒有「連續
   超標」這個概念。所以就算把它排成每分鐘一次，也只是「快速地瞎量」。
   另一個限制是故障域：它與被監控的系統住在同一組 compose，整台機器掛掉時
   兩者一起消失。真正的告警要等 OTel（見 README 的 OpenTelemetry 項）——
   而屆時第一條該寫的規則是 **absent**，不是那五個業務指標裡的任何一個，
   因為「什麼都沒發生」正是 metrics 最不擅長回答的問題。

④ 為什麼一天只檢查四次——這不是「因為是作品所以隨便一點」⭐

   量測頻率應由【被量的東西何時會變化】決定，而不是由「多久看一次比較安心」。
   本專案是固定時段攝入，時段之間**沒有東西進來，pending 在結構上不可能累積**。
   19:00 去量一個從 17:10 起就空的佇列，量到的不是「健康」，是**沒有資訊**。

   常見量級（**參考，不是準則**）：

     監控成熟度            量測頻率        觸發條件
     ─────────────────    ───────────    ─────────────────────────
     有 metrics stack      15–60 秒       超標持續 2–10 分鐘才告警
     只有排程器（過渡）     5–15 分鐘       連續 2 次超標
     本專案                一天 4 次       單次超標即紅（見下方為何可以）

   ⚠️ 實際頻率必須由這四項共同決定，照抄上表沒有意義：

     · **資料抵達頻率** —— 被量的東西多久變化一次
     · **檢查本身的成本** —— 每次是一次 DB 查詢或一次 scrape，高頻 × 大表在真實
       資料量下是實際負擔
     · **可容忍的偵測延遲** —— 由損失的【可逆性】決定：訂單沒收到不可逆（要分鐘級），
       報表晚了可逆（容得下日級）
     · **有沒有抑制機制** —— 有 `for:` 才敢高頻；沒有的話高頻只會製造抖動

   同一條原則在不同系統推出相反的數字：真實 24/7 平台推出 15 秒，這裡推出一天四次。
   **要改頻率，改 SEED_SLOT_HOURS / CHECK_OFFSET_MINUTES 即可**，dag-processor
   幾十秒內生效（刻意不用環境變數——那要重啟容器才生效，反而更不可調）。

⑤ 為什麼偏移 30 分鐘、而且單次超標就算紅 ⭐
   批次最長 6.2 分鐘送完（250 筆 @0.8rps + 60s verify），門檻約 20 分鐘（推導見
   `check_raw_pending.py` ④）。30 分鐘讓兩者都已完成，於是判定是**二元的**：

     正常                超過門檻的 pending = 0
     redis/worker 掛掉    超過門檻的 pending = 150～250（整批，年齡約 30 分鐘）

   沒有邊界模糊地帶，所以不需要「連續兩次」這種抑制——那是為了對付抖動，
   而這裡沒有抖動可對付。

⑥ SEED_SLOT_HOURS 與 seed_demo_daily 是【重複的常數】
   刻意不跨 DAG 檔 import：那會建立解析順序的耦合，而 dag-processor 是各檔獨立
   解析的。改成由測試釘住兩者相等（`tests/test_dags.py`），與 seeding→抽取
   那個時序契約用測試釘住是同一種做法。

⑦ retries=0
   查詢是唯讀且 deterministic 的，重試只會得到同一個答案。與 `dq_reevaluation`、
   `source_freshness_watch` 同理。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from _notify import build_failure_callback

PROJECT_DIR = "/opt/project"
PY_ANALYTICS = "/home/airflow/venvs/analytics/bin/python"

TZ = pendulum.timezone("Asia/Taipei")

# ⚠️ 必須與 seed_demo_daily.SLOT_SIZES 的鍵一致（見檔頭 ⑥，由測試釘住）。
SEED_SLOT_HOURS = (10, 13, 17, 21)

# 檢查點 = 時段開始後這麼多分鐘。見檔頭 ⑤。
CHECK_OFFSET_MINUTES = 30

SCHEDULE = f"{CHECK_OFFSET_MINUTES} {','.join(str(h) for h in SEED_SLOT_HOURS)} * * *"

with DAG(
    dag_id="raw_pending_watch",
    description="派工存活：Raw 是否有列超過恢復路徑的自癒時間仍未被 worker 取走",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 8, 11, tz=TZ),
    catchup=False,          # 補跑舊時段只會重問「現在」的狀態，無意義
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 0,       # 見檔頭 ⑦
        "email_on_failure": False,
        "on_failure_callback": build_failure_callback(
            "派工路徑在漏：有 Raw 列超過恢復路徑的自癒時間仍未被取走。"
            "先看 worker 與 broker 是否還活著，再看熔斷器狀態。"
        ),
    },
    tags=["observability", "ingestion"],
    doc_md=__doc__,
) as dag:

    # ⚠️ 不 cd 進 /opt/project：BashOperator 預設 cwd 是臨時目錄，讓 config 的
    #    load_dotenv 找不到主機的 .env 而純吃容器環境變數（DB_URL 由 compose 注入）。
    #    cd 過去會讀到 bind mount 進來的主機 .env，那裡的 DB_URL 在容器內是錯的。
    #    與 seed_demo_daily 的 seed_orders task 同一個理由。
    BashOperator(
        task_id="check_raw_pending",
        bash_command=f"{PY_ANALYTICS} {PROJECT_DIR}/check_raw_pending.py",
        # 唯讀單表計數，正常在毫秒級完成；跑到 5 分鐘代表 DB 本身有事，
        # 那時讓它失敗比掛著不動有用。
        execution_timeout=pendulum.duration(minutes=5),
    )
