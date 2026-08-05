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
兩邊的成功率就各自代表一件事：
  - orders_analytics_daily 紅 = 管線壞了
  - source_freshness_watch 紅 = 資料源不新鮮（在目前的攝入模式下，這是預期狀態）

⚠️ **本 DAG 在目前的手動攝入下預期恆紅。** 這是 §1.7.7 已經接受的狀態，不是待修的
缺陷——26h/50h 的閾值描述的是「被模擬的那個系統該有的服務等級」，不是「模擬者的
灌資料習慣」。寧可讓訊號誠實地紅，也不要為了好看而調鬆閾值。

**這個立場在什麼時候失效**：若日後加上持續攝入（seeding DAG 或接真實上游），紅就
真的代表「壞了」，屆時 freshness 應恢復為有意義的 gate，可以考慮併回主 DAG 的前置
檢查——但那需要同步修改 §1.7.7 的規則表，不能默默做。

**它量的不是「抽取工作有沒有跑」**：`loaded_at_field` 指向 `received_at`＝ODS 的攝入
時間，所以它回答的是「最新一筆訂單多久以前進到 ODS」。上游停止送單與抽取工作掛掉
會產生一模一樣的症狀（§1.7.6 推論 3）。要分辨兩者需要另一個訊號（例如加一個
`_extracted_at` 欄位），屬未來工作。
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
