"""DAG 結構測試（需要 Airflow，故跑在獨立的 CI job）。

**為什麼這支測試進得來**：DAG 檔刻意不 top-level import 任何專案模組
（`config.py` 一被 import 就要 DB_URL）。那條紀律原本是為了避免 dag-processor
每次解析都因缺環境變數而讓整條 DAG 從 UI 消失，附帶收益就是這裡——DagBag 可以在
不連 DB、不設任何專案環境變數的情況下解析成功。若哪天有人在 DAG 檔頂層寫了
`from config import settings`，這支測試會第一個紅。

**為什麼不跟主測試套件同一個 job**：Airflow 的安裝很重且 pin 了大量套件版本，
塞進那個「mock DB、數秒跑完」的 job 會毀掉它的速度優勢，也可能與專案既有的
pin 打架。見 .github/workflows/dags.yml。
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow.models", reason="DAG 測試需要 Airflow，見 .github/workflows/dags.yml")

from airflow.models.dagbag import DagBag  # noqa: E402

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent / "orchestration" / "dags")


@pytest.fixture(scope="module")
def dagbag():
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


class TestDagBag:

    def test_no_import_errors(self, dagbag):
        """import error 的後果不是「task 失敗」而是【整條 DAG 從 UI 消失】——
        比失敗更危險，因為沒有紅燈可看。"""
        assert dagbag.import_errors == {}

    def test_every_dag_file_produces_a_dag(self, dagbag):
        py_files = [p for p in Path(DAGS_FOLDER).glob("*.py") if not p.name.startswith("_")]
        assert len(dagbag.dags) >= len(py_files)

    def test_dags_are_acyclic_and_have_tasks(self, dagbag):
        for dag in dagbag.dags.values():
            assert dag.tasks, f"{dag.dag_id} 沒有任何 task"
            dag.topological_sort()   # 有環會拋例外


class TestOrdersAnalyticsDaily:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["orders_analytics_daily"]

    def test_catchup_disabled(self, dag):
        """watermark 是 destination-derived，不是 execution-date-derived：
        每個 backfill run 都會做一模一樣的事（見 DAG 檔頭 ②）。"""
        assert dag.catchup is False

    def test_single_active_run(self, dag):
        """並行 run 會讓 dbt 對同一批分區互相覆寫（見 DAG 檔頭 ③）。"""
        assert dag.max_active_runs == 1

    def test_dbt_waits_for_every_extract(self, dag):
        """跨表 gate：兩張表都上去了 dbt 才能開跑（CLOUD_LAYER §3.2）。
        少接一條邊 = dbt 在半套資料上建模。"""
        extracts = {t.task_id for t in dag.tasks if t.task_id.startswith("extract_")}
        assert extracts == {"extract_orders", "extract_quality_events"}
        assert dag.get_task("dbt_staging").upstream_task_ids == extracts

    def test_layers_run_in_data_flow_order(self, dag):
        """staging → intermediate → marts → reports。順序錯了 Hard Gate 就形同虛設。"""
        chain = ["dbt_staging", "dbt_intermediate", "dbt_marts", "dbt_reports"]
        for upstream, downstream in zip(chain, chain[1:]):
            assert dag.get_task(downstream).upstream_task_ids == {upstream}

    def test_full_test_suite_runs_last(self, dag):
        """completeness：確保沒有測試因為 selector 的細微語意被靜默跳過（檔頭 ⑦）。"""
        assert dag.get_task("dbt_test_all").upstream_task_ids == {"dbt_reports"}
        assert dag.get_task("dbt_test_all").downstream_task_ids == set()

    def test_dbt_never_splits_run_and_test(self, dag):
        """`dbt build` 把 run + test 綁在一起。若拆成 `dbt run` / `dbt test` 兩個 task，
        int_ 的上游會變成「staging 的 run」而非「staging 的 test」→ Hard Gate 失效。"""
        for layer in ("staging", "intermediate", "marts", "reports"):
            cmd = dag.get_task(f"dbt_{layer}").bash_command
            assert "dbt build --select" in cmd
            assert f"path:models/{layer}" in cmd

    def test_layer_builds_use_buildable_indirect_selection(self, dag):
        """逐層 `--select` 會踩到 dbt 的 indirect selection 語意：預設 eager 會讓
        跨層 singular test 在上游剛重建、下游還是舊表時誤紅；cautious 則讓它永遠不跑。
        buildable 才能讓每支測試落在「所有輸入都新鮮」的那一層（檔頭 ⑦）。"""
        for layer in ("staging", "intermediate", "marts", "reports"):
            assert "--indirect-selection=buildable" in dag.get_task(f"dbt_{layer}").bash_command

    def test_retry_asymmetry(self, dag):
        """extract 的失敗多為暫時性 → 重試；dbt 的失敗多為 deterministic → 重試只是
        重跑一次注定失敗的東西（NUL byte poison-pill 的同一條教訓，檔頭 ⑤）。"""
        for task_id in ("extract_orders", "extract_quality_events"):
            assert dag.get_task(task_id).retries == 2
        for task_id in ("dbt_staging", "dbt_intermediate", "dbt_marts", "dbt_reports", "dbt_test_all"):
            assert dag.get_task(task_id).retries == 0

    def test_freshness_is_not_in_this_dag(self, dag):
        """CLOUD_LAYER §1.7.7：freshness 不得當前置檢查。更進一步，連旁路 task 都不行——
        一個預期會紅的 task 會讓主 DAG 恆為 failed，真正的失敗就被噪音淹沒。
        它獨立成 source_freshness_watch DAG。"""
        for task in dag.tasks:
            assert "source freshness" not in task.bash_command
