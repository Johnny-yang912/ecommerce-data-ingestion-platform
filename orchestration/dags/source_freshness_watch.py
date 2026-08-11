"""`dbt source freshness` 的獨立觀測 DAG。

────────────────────────────────────────────────────────────────────────────
為什麼它不在主 DAG 裡 ⭐

CLOUD_LAYER-TW §1.7.7 已立了硬規則：**freshness 不得放在抽取／`dbt build` 之前
當前置檢查**——同一個紅會立刻從「可接受的告警」變成「DAG 永久卡死」，而它反映的
往往只是「這幾天沒手動灌資料」。

但實作時發現，該節建議的「旁路 task（失敗不影響下游）」還不夠。Airflow 的 DAG run
狀態是 task 的彙總：**一個預期會紅的 leaf task 會讓主管線的 DAG run 恆為 failed**，
於是「主 DAG 成功率」這個訊號的價值歸零——真正的失敗被淹沒在噪音裡。

這是 §1.7.7 自己那句話再往前一步：**訊號的價值不等於它該有的權限**。freshness
不只沒有「阻斷下游」的權限，也沒有「污染別人成功率」的權限。獨立成一條 DAG，
三邊的成功率就各自代表一件事：
  - seed_demo_daily 紅        = 灌不進去（API / worker / redis 出事）
  - orders_analytics_daily 紅 = 管線壞了
  - source_freshness_watch 紅 = 資料不新鮮（＝上面兩者之一已經紅了一陣子）

**2026-08-11 起：本 DAG 由「預期恆紅」轉為「預期常綠」** ⭐

上一版檔頭寫的是「本 DAG 在目前的手動攝入下預期恆紅」。`seed_demo_daily` 上線後
攝入變成連續的（每天 10/13/17/21 各一批），§1.7.7 規則表裡那句「若日後改成持續
攝入，本立場即失效」的條件成立了。**現在紅就真的代表壞了。**

**但它仍然不併回主 DAG 當前置 gate**，而且理由比先前更強。舊理由是「紅只反映
你這幾天沒手動灌資料」；現在的理由是：

> **seeding 是這個系統唯一的資料來源。所以 seeding 掛掉的那天，就是「沒有新資料」
> 的那天——分析管線在舊資料上跑一次是無害且正確的，擋住它一點好處都沒有。**

換句話說，freshness 從「一個因為前提不成立而不該有權限的訊號」變成「一個前提成立、
但**阻斷本身沒有價值**的訊號」。結論相同，論證不同——這個區別要寫清楚，否則下一個
人會以為條件成立後就該把它接成 gate。

⚠️ 但「持續攝入」在這裡是有限定的：一天四批，不是 24 小時連續。
所以本 DAG 證明的是「每日的**這一跳**還通著」，不是「攝入沒有中斷」——
它偵測得到「整天沒新資料進到 staging」，偵測不到「峰期停了三小時」。
後者要靠 Raw 那一側的量測，不是這裡（見下方三個時間軸的分工）。

閾值的來源是【載入節奏】而非攝入節奏——只要倉儲仍是每日批次載入，攝入變成
24 小時連續也不會改變 26h/50h。會改變它的是 extract 改成小時批或串流。
（完整推導見下方「閾值從哪來」。）

**它量的範圍剛好是一跳，而且那是刻意的** ⭐
`loaded_at_field` 指向 `received_at`＝**ODS 的落地時刻**（不是收單時刻，見
CLOUD_LAYER-TW §1.2.2）。而本 DAG 檢查的是 extract，extract 搬的正是 ODS——
**所以看 ODS 自己的時鐘是正確的時間軸，不是妥協。**

由此得到一個必須寫清楚的範圍邊界：**它看不見已經恢復的攝入中斷。** 積壓被恢復
掃描沖出去時，那批列的 `received_at` 是回補當下的寫入時刻，斷層在 ODS 的時間軸上
不存在。所以它偵測得到的只有「取樣當下仍在進行中」的中斷。

**這不是缺陷，是別人的職責。** 三個時間軸各管一段，混在一起的話一個紅會同時代表
兩段管線，訊號價值歸零——這正是三支 DAG 當初被拆開的同一條理由：

    Raw.received_at                    上游 + API：收得到單嗎        （OTel 之後）
    Raw.received_at → ODS.received_at  Redis/worker：搬得動嗎        raw_pending_watch
    BQ staging 上的 ODS.received_at    extract：搬進倉儲了嗎         ← 本 DAG

**閾值 26h/50h 從哪來，以及為什麼不動它** ⭐
`26 = 24 + 2`、`50 = 48 + 2`——**一個載入週期 + 2 小時寬限**。這個數字的來源是
【載入節奏】而不是攝入節奏：staging 一天只被 extract 推一次，資料設計上就會有
最多 24 小時的年齡，閾值必須大於 24h，否則每天抽取前它會自己紅。
真實 24/7 電商只要倉儲同樣是夜間批次載入，閾值就是同一個量級——**24/7 的即時性
活在營運層（分鐘級），不在倉儲的 freshness**。把它收緊等於把營運層的 SLA 套錯層。

取樣點與閾值互相決定，所以兩者要一起看：台北 08:00 取樣時，健康值約 13h
（前一日最後一批 21:05 起算）、一個週期沒進資料是 37h，26h 落在正中間，
**兩邊各有約 10 小時餘裕**。若把取樣點搬到抽取完成後，同一組數字會退化成
每個判準邊界都只差半小時——那不是門檻，是擲硬幣。

**它在這裡的職責只有一個：backstop** ⭐
extract 若「回報成功卻其實沒搬東西」（watermark 卡住、增量是空的），Hard Gate 判的
是最新分區＝昨天那批、會**通過**，`dbt test` 也全綠。那個狀況下本 DAG 是唯一會在
營運團隊 09:00 看報表之前叫的東西。排 08:00 正是為了留那 1 小時。
它不是主要偵測——extract 掛掉由 `orders_analytics_daily` 自己變紅來報。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/opt/project/ecommerce_dbt"
DBT = "/home/airflow/venvs/dbt/bin/dbt"

TZ = pendulum.timezone("Asia/Taipei")

with DAG(
    dag_id="source_freshness_watch",
    description="dbt source freshness（extract 的 backstop；不阻斷任何管線）",
    # 台北 08:00：早於營運團隊 09:00 看報表（留 1 小時處置），晚於前一晚 22:30 的抽取。
    #    原本寫 `@daily` + naive start_date，在預設 UTC 下解析
    #    出來就是 UTC 00:00 ＝台北 08:00，同一個瞬間。改寫的目的不是換時間，是讓
    #    另外兩支 DAG 檔頭明講的紀律（排程時間是業務決策，必須顯式宣告時區，不能
    #    寫 UTC 再靠註解解釋）在這一支也成立——原本它是三支裡唯一的例外。
    # ⚠️ 取樣點與閾值互相決定，改任一個都要重算另一個。見檔頭。
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        # 不重試：stale 是 deterministic 的，重跑必然得到同一個答案。
        "retries": 0,
    },
    tags=["observability", "dbt"],
    doc_md=__doc__,
) as dag:

    # 非零 exit（ERROR STALE）會讓 task 紅——那正是訊號本身，不是要被壓下去的雜訊。
    BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT} source freshness --no-use-colors",
    )
