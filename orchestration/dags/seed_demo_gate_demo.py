"""Hard Gate 攔截劇本：刻意灌一批超標的髒資料，看閘門把下游擋下來。（手動觸發）

    seed_dirty_batch ─► （接著手動跑 orders_analytics_daily，觀察 dbt_staging 變紅）

────────────────────────────────────────────────────────────────────────────
① `schedule=None`，而且這是**設計的核心**，不是「還沒設好」⭐
   為什麼不讓日常 seeding 偶爾超標就好：因為那會摧毀主 DAG 的成功率訊號。
   `source_freshness_watch` 當初獨立成 DAG，理由是「一個預期會紅的 task 會讓
   主管線恆為 failed，真正的失敗被噪音淹沒」。**「每隔幾天故意餵髒資料把主 DAG
   弄紅」是同一個錯誤的另一種寫法**——屆時「orders_analytics_daily 紅」會同時
   代表「管線壞了」與「閘門正常運作」，而這兩者需要完全相反的處置。

   把它做成手動劇本，兩個訊號就各自乾淨：
     日常排程 → 主 DAG 應該永遠綠
     這條劇本 → 主 DAG 應該紅，紅就是成功

   附帶好處：demo 時可以**當場演一次**，而不是說「你等三週就會看到」。

② 為什麼不能靠調高日常髒率來自然撞到門檻
   n=800/天、Hard Gate 15%（最新 UTC 日分區）時，抽樣標準差約 1.2 個百分點：
     設定 12% → 意外超標 0.45%/天（約 222 天一次）
     設定 13% → 意外超標 4.63%/天（約 22 天一次）
   也就是說，能「偶爾自然撞到」的設定值，必然也頻繁到會污染成功率訊號。
   **想要可控的展示，就不能靠機率。**

③ 預設 25% 是「明顯超標」而非「剛好超過」
   剛好壓線（例如 15.5%）會讓抽樣變異決定成敗，重跑兩次可能一次紅一次綠——
   一個結果不穩定的示範比沒有示範更糟。25% 遠離門檻，每次都紅。

④ ⚠️ 這條劇本會在 ODS 留下永久的髒資料
   它走的是真實攝入路徑（那正是它有價值的原因），所以灌進去的訂單會一直留在
   Raw/ODS 與 quality_events 裡，也會推高全表的 monitor_dataset_error_rate。
   這是刻意接受的：**要展示的正是「髒資料進得來、但被擋在 Gold 之外」**。
   若要復原，order_id 帶 SEED-<批次>- 前綴，可精準刪除。

⑤ 跑完之後要做什麼（劇本的後半段，刻意不自動化）
   手動觸發 orders_analytics_daily → dbt_staging 的
   hard_gate_latest_batch_error_rate 失敗 → 整個 dbt run 中止 → 下游 Gold
   保留上一次的乾淨狀態。**「下游沒有被污染」才是要展示的結論**，不是「有個
   測試紅了」。不自動接主 DAG 是因為那一步需要人看著，而不是看事後的 log。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"
PY_ANALYTICS = "/home/airflow/venvs/analytics/bin/python"

with DAG(
    dag_id="seed_demo_gate_demo",
    description="Hard Gate 展示：刻意灌超標髒資料，驗證閘門擋下 Gold（手動）",
    schedule=None,                                  # 見檔頭 ①
    start_date=pendulum.datetime(2026, 8, 11, tz=pendulum.timezone("Asia/Taipei")),
    catchup=False,
    max_active_runs=1,
    params={
        "dirty_rate": Param(
            0.25, type="number", minimum=0.0, maximum=1.0,
            title="髒資料比例",
            description="預設 0.25，明顯高於 Hard Gate 的 15%。刻意不設成剛好壓線——"
                        "壓線會讓抽樣變異決定成敗，重跑可能一次紅一次綠（見檔頭 ③）。",
        ),
        "n": Param(
            120, type="integer", minimum=1, maximum=1000,
            title="訂單筆數",
            description="預設 120（@0.8rps 約 2.5 分鐘）。要讓當日分區的整體比率被"
                        "推過門檻，這批的量必須相對當天已灌的量夠大——"
                        "當天已經灌了 800 筆的話，120 筆 25% 只能把全日拉到約 14%，"
                        "不一定夠。在當天 seeding 尚未跑、或願意加大 n 時最可靠。",
        ),
    },
    default_args={"owner": "data-eng", "retries": 0},
    tags=["data-quality", "hard-gate", "manual", "demo"],
    doc_md=__doc__,
) as dag:

    BashOperator(
        task_id="seed_dirty_batch",
        bash_command=(
            f"{PY_ANALYTICS} {PROJECT_DIR}/seed_demo.py"
            ' --url "$SEED_API_URL"'
            " --n {{ params.n }}"
            " --dirty-rate {{ params.dirty_rate }}"
            # 落地閘門照設：這批「該髒」的是資料內容，不是攝入路徑。
            # 攝入本身仍必須成功，否則等於什麼都沒展示到。
            " --require-landed-pct 0.9"
            " --verify-wait 60"
        ),
        execution_timeout=pendulum.duration(minutes=20),
    )
