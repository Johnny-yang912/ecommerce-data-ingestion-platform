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
攝入變成連續的（每天 09/13/17/21 各一批），§1.7.7 規則表裡那句「若日後改成持續
攝入，本立場即失效」的條件成立了。**現在紅就真的代表壞了。**

**但它仍然不併回主 DAG 當前置 gate**，而且理由比先前更強。舊理由是「紅只反映
你這幾天沒手動灌資料」；現在的理由是：

> **seeding 是這個系統唯一的資料來源。所以 seeding 掛掉的那天，就是「沒有新資料」
> 的那天——分析管線在舊資料上跑一次是無害且正確的，擋住它一點好處都沒有。**

換句話說，freshness 從「一個因為前提不成立而不該有權限的訊號」變成「一個前提成立、
但**阻斷本身沒有價值**的訊號」。結論相同，論證不同——這個區別要寫清楚，否則下一個
人會以為條件成立後就該把它接成 gate。

**它現在量的是整條鏈的存活** ⭐
`loaded_at_field` 指向 `received_at`＝ODS 的攝入時間。§1.7.6 推論 3 指出「上游停止
送單」與「抽取工作掛掉」會產生一模一樣的症狀，過去這是個待解的模糊性——但在
seeding 即上游的架構下，**兩者都是「我的管線壞了」**，處置方向一致。於是這個模糊性
不再是缺陷：它讓這條 DAG 成為唯一一個涵蓋 seeding → API → worker → ODS → extract
→ BQ 全鏈路的存活探針。要進一步分辨是哪一段斷了，看 `seed_demo_daily` 與
`orders_analytics_daily` 各自的成功率即可——那正是把它們拆成三條 DAG 的收益。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/opt/project/ecommerce_dbt"
DBT = "/home/airflow/venvs/dbt/bin/dbt"

with DAG(
    dag_id="source_freshness_watch",
    description="dbt source freshness（純觀測；不阻斷任何管線）",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
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
