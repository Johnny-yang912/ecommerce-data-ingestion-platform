"""主分析管線：ODS → BigQuery staging（E/L）→ dbt 分層轉換（T）。

    extract_orders ─────────┐
                            ├─► dbt_staging ─► dbt_intermediate ─► dbt_marts ─► dbt_reports ─► dbt_test_all
    extract_quality_events ─┘      (Hard Gate)

取代的是過去手動跑的 `python extract_ods_to_bq.py` + `dbt build`。
完整決策與理由見 ORCHESTRATION-TW.md；以下只記「讀這個檔時會想問的為什麼」。

────────────────────────────────────────────────────────────────────────────
① 這個檔【不得】import 專案模組 ⭐
   `config.py` 在 import 當下就實例化 Settings 且 `db_url` 必填。DAG 檔會被
   dag-processor 每隔幾十秒重新解析一次——top-level import 專案模組的話，解析行程
   只要缺 DB_URL，結果不是「task 失敗」而是 **DAG import error，整條 DAG 從 UI 消失**。
   用 BashOperator 把 import 推到 task 執行期，順帶讓這個檔進得了不連 DB 的 CI
   （tests/test_dags.py）。

② catchup=False 是結構性的，不是偷懶 ⭐
   本管線的 watermark 是 **destination-derived**（方案 A：從 staging 的
   MAX(partition_id) 推導），不是 execution-date-derived。補跑 2026-07-01 的
   backfill run 抽的仍然是「現在的增量」，與 logical date 無關 → N 個 backfill run
   做 N 次一模一樣的事，還多產 N 個 load job。**這不是 date-partitioned 的可回填 DAG。**
   要讓它真的可回填，得改成 `received_at >= data_interval_start AND < data_interval_end`
   的切片抽取，但那個右邊界會切掉遲到列，與既有「>= 寧可重抓不漏抓」直接衝突。刻意不做。

③ max_active_runs=1 是正確性而非禮貌
   兩個 run 並行時 get_watermark 會讀到同一個值（無害，dbt stg_ 去重吃掉），
   但 dbt 對同一批分區並行 insert_overwrite 會互相覆寫。

④ 日批而非小時批
   方案 A 的精度被 DAY 分區卡死，小時批每跑一次都重抽當天至今（CLOUD_LAYER §2.2
   的判準表）。要上小時批得先換 HOUR 分區或方案 B——那是另一個獨立決策。

⑤ retry 刻意不對稱：extract=2、dbt=0
   extract 的失敗多為暫時性（PG 連線、BQ 5xx / rateLimitExceeded）；dbt 的失敗多為
   deterministic（SQL 錯、測試紅、Hard Gate 擋下），retry 只是重跑一次注定失敗的東西。
   BQ 的暫時性錯誤由 profiles.yml 的 `job_retries: 1` 在 adapter 層更精準地處理。
   這個不對稱與 NUL byte poison-pill 的教訓同一條原則：**把 deterministic 錯誤當成
   暫時性而重試，就是在製造 poison-pill。**

⑥ 為什麼 extract 拆兩個 task
   各表 watermark 獨立、失敗不推進，是 CLOUD_LAYER §3.2 自癒模型的來源。合成單一
   task 會讓重試連帶重跑已經成功的那張，也看不出是哪張壞了。跨表 gate 從
   「腳本內彙整後 raise」搬到這裡的依賴邊（dbt 的上游＝兩個 extract 都 success），語意相同。

⑦ dbt 為什麼分層、又為什麼結尾還要跑一次 dbt test ⭐
   分層讓 Hard Gate 的攔截點在 UI 上看得見，也能從失敗層往下重跑。但逐層 `--select`
   會踩到 dbt 的 indirect selection 語意：**跨層的 singular test 可能被靜默跳過或在錯的
   時間點跑**。用 `--indirect-selection=buildable`（父節點必須「被選中或是被選中節點的祖先」）
   可以讓每支測試落在它所有輸入都新鮮的那一層——但這條規則細微，賭錯的下場是
   `assert_orders_split_is_partition` 這類「唯一的自動化安全網」被無聲跳過。
   故結尾補一個完整 `dbt test`：**測試被靜默跳過，比多跑一次測試糟糕得多。**
   兩者職責不同——逐層的測試是 **gate**（擋住下游建置），結尾那次是 **completeness**。

⑧ freshness 不在這條 DAG 裡
   CLOUD_LAYER §1.7.7 立了硬規則：不得當前置檢查。但「旁路 task」還不夠——一個
   預期會紅的 task 會讓主 DAG 恆為 failed，真正的失敗被噪音淹沒。故獨立成
   source_freshness_watch DAG（PR5）。

   ⚠️ 2026-08-11 起攝入已連續化（seed_demo_daily），freshness 因此**由恆紅轉為
   常綠**，§1.7.7 那句「若日後改成持續攝入，本立場即失效」的條件成立了。
   但它**仍然不該併回來當前置 gate**，理由換成了一個更強的：
   **seeding 是唯一的資料來源，所以 seeding 掛掉的那天就是「沒有新資料」——
   分析管線在舊資料上跑一次是無害且正確的，擋住它一點好處都沒有。**

⑨ seeding 不在這條 DAG 裡，理由同 ⑧ 那一類 ⭐
   資料產生器壞掉與分析管線壞掉需要完全不同的處置，混在一條 DAG 裡會讓
   「主 DAG 紅」失去單一意義。量測佐證：本管線全程約 2.5 分鐘，而單一 seeding
   時段就要 3~5 分鐘且大半在等 API 限流——那不是這條 DAG 該負責的等待。
   見 seed_demo_daily.py 檔頭 ①。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"
DBT_DIR = f"{PROJECT_DIR}/ecommerce_dbt"
PY_ANALYTICS = "/home/airflow/venvs/analytics/bin/python"
DBT = "/home/airflow/venvs/dbt/bin/dbt"

# 逐層執行的順序＝資料流方向。dbt build = run + test 綁在一起，刻意【不】拆成
# `dbt run` 一個 task、`dbt test` 另一個——那會讓 int_ 的上游變成「staging 的 run」
# 而非「staging 的 test」，Hard Gate（DQ 機制一）就此失效，髒資料照樣流進 Gold。
DBT_LAYERS = ["staging", "intermediate", "marts", "reports"]

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
}


def _dbt(command: str) -> str:
    """dbt 指令的共用前綴。--no-use-colors 讓 Airflow log 不含 ANSI 逃脫序列。"""
    return f"cd {DBT_DIR} && {DBT} {command} --no-use-colors"


with DAG(
    dag_id="orders_analytics_daily",
    description="ODS → BigQuery staging → dbt stg_/int_/dim_/fct_/rpt_",
    # 台北 23:00：假想的營運團隊早上 9 點看前一天的報表，報表必須在那之前備妥。
    # 用 Asia/Taipei 宣告而不是寫 UTC 15:00，是為了把這個推理留在程式碼裡——
    # 排程時間是【業務決策】（誰、什麼時候要看報表），不是技術參數。
    # ⚠️ 這也讓它排在 seed_demo_daily 最後一個時段（台北 21:00）之後，
    #    當天四批 seeding 都已落地才抽取。兩者的時序契約只存在於這個時間差裡。
    schedule="0 23 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=pendulum.timezone("Asia/Taipei")),
    catchup=False,          # 見檔頭 ②
    max_active_runs=1,      # 見檔頭 ③
    default_args=default_args,
    tags=["analytics", "dbt", "elt"],
    doc_md=__doc__,
) as dag:

    extracts = [
        BashOperator(
            task_id=f"extract_{table}",
            bash_command=f"{PY_ANALYTICS} {PROJECT_DIR}/extract_ods_to_bq.py --table {table}",
            retries=2,                                   # 見檔頭 ⑤
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
        )
        for table in ("orders", "quality_events")
    ]

    dbt_tasks = [
        BashOperator(
            task_id=f"dbt_{layer}",
            # --indirect-selection=buildable：讓跨層 singular test 落在「所有輸入都新鮮」
            # 的那一層執行，而不是在上游剛重建、下游還是舊表的中間狀態下誤紅。見檔頭 ⑦。
            bash_command=_dbt(
                f"build --select path:models/{layer} --indirect-selection=buildable"
            ),
            retries=0,                                   # 見檔頭 ⑤
        )
        for layer in DBT_LAYERS
    ]

    # completeness：確保沒有任何測試因為 selector 的細微語意被靜默跳過。見檔頭 ⑦。
    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=_dbt("test"),
        retries=0,
    )

    # gate：兩個 extract 都 success，dbt 才開跑（CLOUD_LAYER §3.2 的跨表一致性）。
    extracts >> dbt_tasks[0]
    for upstream, downstream in zip(dbt_tasks, dbt_tasks[1:]):
        upstream >> downstream
    dbt_tasks[-1] >> dbt_test_all
