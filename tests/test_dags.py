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
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.models", reason="DAG 測試需要 Airflow，見 .github/workflows/dags.yml")

from airflow.models.dagbag import DagBag  # noqa: E402

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent / "orchestration" / "dags")

# DAG 檔之間共用的模組（`_notify`）靠「dags 資料夾在 import 路徑上」被找到。
# Airflow 執行時與 DagBag 解析時都會自己加，但這裡有測試【不經過 DagBag】直接
# import _notify，故顯式加上，讓那些測試不依賴 fixture 的執行順序。
sys.path.insert(0, DAGS_FOLDER)


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


class TestFreshnessIsolation:

    def test_only_the_watch_dag_runs_freshness(self, dagbag):
        """訊號的價值不等於它該有的權限：freshness 既不能阻斷下游，也不該污染別人的
        成功率。任何一條有實際產出的 DAG 混進 freshness，這支測試就紅。"""
        owners = {
            dag_id for dag_id, dag in dagbag.dags.items()
            for t in dag.tasks if "source freshness" in getattr(t, "bash_command", "")
        }
        assert owners == {"source_freshness_watch"}

    def test_watch_dag_is_a_single_leaf(self, dagbag):
        dag = dagbag.dags["source_freshness_watch"]
        assert len(dag.tasks) == 1
        assert dag.tasks[0].retries == 0     # stale 是 deterministic，重跑答案一樣


class TestSeedDemoDaily:
    """seeding 已經不是輔助腳本，而是這個系統唯一的資料來源，故比照管線 DAG 測。"""

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["seed_demo_daily"]

    def test_no_retries(self, dag):
        """⭐ 與 extract 的 retries=2 方向相反，而且這個不對稱是重點。

        extract 重試安全（`>=` watermark 寧可重抓，重複由 stg_ 去重）；
        seeding 重試是【真的多灌一批】——order_id 帶 wall-clock 批次標記，不撞冪等，
        當天資料翻倍、髒率被稀釋、Hard Gate 的門檻線失真。
        重試的前提是操作冪等，不是「失敗了就再試一次」這個直覺。"""
        for task in dag.tasks:
            assert task.retries == 0

    def test_all_slots_land_in_one_utc_partition(self, dag):
        """⭐ 正確性約束：所有時段必須在台北 08:00 之後。

        Hard Gate 的口徑是【最新的一個 UTC 日分區】，而 received_at 的 date()
        在 UTC 換日。台北 00:00–08:00 屬前一個 UTC 日 → 當天資料被拆成兩個分區，
        gate 判的那批就不是設定的那批，當日髒率也不再是設定值。
        這條紅了代表有人改了排程時段而沒意識到分區邊界。"""
        hours = [int(h) for h in str(dag.schedule).split()[1].split(",")]
        assert hours, "沒解析到排程時段"
        for h in hours:
            assert 8 <= h <= 23, f"台北 {h}:00 會落到前一個 UTC 分區（見 DAG 檔頭 ③）"

    def test_schedule_is_taipei_time(self, dag):
        """排程時間是業務決策（誰、何時看報表），不是技術參數——必須顯式宣告時區，
        而不是寫成 UTC 再靠註解解釋。"""
        assert "Taipei" in str(dag.timezone)

    def test_runs_before_the_analytics_dag(self, dagbag, dag):
        """seeding 與主 DAG 的時序契約【只存在於時間差裡】（刻意不用 Trigger 相接，
        見 seed_demo_daily 檔頭 ①）。既然如此，這個時間差就必須被測試釘住——
        否則有人調整任一邊的排程時，那個隱性契約會無聲斷掉。"""
        last_seed = max(int(h) for h in str(dag.schedule).split()[1].split(","))
        analytics = dagbag.dags["orders_analytics_daily"]
        analytics_hour = int(str(analytics.schedule).split()[1])
        assert last_seed < analytics_hour, (
            f"最後一個 seeding 時段（{last_seed}:00）必須早於抽取（{analytics_hour}:00）")

    def test_landed_gate_is_enforced(self, dag):
        """⭐ 唯一擋得住靜默失敗的東西。

        POST 回 202 只代表 Raw 落地；_enqueue 刻意吞掉 broker 故障，所以 worker
        或 redis 掛掉時腳本會印滿版 ok、exit 0、DAG 全綠而 ODS 一筆都沒有。"""
        cmd = dag.get_task("seed_orders").bash_command
        assert "--require-landed-pct" in cmd

    def test_payload_seed_differs_per_slot(self, dag):
        """⭐ payload 種子必須逐時段不同，否則四個時段會灌出**一模一樣的訂單**
        （同 seed = 同亂數序列），只有 order_id 因 wall-clock 批次標記而不同——
        customer / product 分佈被複製四份，dim_ 表嚴重失真。

        與 test_all_slots_of_a_taipei_day_share_one_dirty_rate_seed 是一對：
        髒率要全天相同、payload 要逐時段不同，兩者的種子因此刻意不同源。"""
        from datetime import datetime, timedelta, timezone

        env = dag.get_template_env()
        tpl = env.from_string(dag.get_task("seed_orders").bash_command)
        seeds = set()
        tpe = timezone(timedelta(hours=8))
        for taipei_hour in (10, 13, 17, 21):
            run_after = datetime(2026, 8, 12, taipei_hour, tzinfo=tpe)
            out = tpl.render(dag_run=SimpleNamespace(run_after=run_after))
            seeds.add(out.split("--seed ")[1].split()[0])
        assert len(seeds) == 4, f"四個時段的 payload 種子必須互異，實得：{seeds}"

    def test_time_base_is_run_after_not_data_interval(self, dag):
        """⭐ cron 的 data_interval_start 是【上一個】觸發點；logical_date 在
        Airflow 3 的手動 run 裡可能不存在。兩者都不能當這條 DAG 的時間基準。
        這支測試釘住「用 run_after」這個決定本身，因為它的錯誤形式是無聲的。"""
        cmd = dag.get_task("seed_orders").bash_command
        assert "dag_run.run_after" in cmd
        assert "data_interval_start" not in cmd
        assert "ds_nodash" not in cmd

    def test_catchup_disabled(self, dag):
        """received_at 由伺服器在攝入當下產生，回填在物理上不可能——
        補跑舊日期只會在今天再灌一批。"""
        assert dag.catchup is False

    def test_single_active_run(self, dag):
        """兩個 run 並行會互撞 60/min 限流，雙方一起退避，實際更慢。"""
        assert dag.max_active_runs == 1

    def test_has_execution_timeout(self, dag):
        """429 退避可能無限拖著一個 task；沒有上限的話它會一直掛著不失敗。"""
        assert dag.get_task("seed_orders").execution_timeout is not None

    @pytest.mark.parametrize("utc_hour,taipei_hour,expected_n", [
        (2,  10, 150),   # 台北 10:00
        (5,  13, 200),   # 台北 13:00
        (9,  17, 200),   # 台北 17:00
        (13, 21, 250),   # 台北 21:00
        (4,  12, 200),   # 非排程時段（手動觸發）→ 落到 DEFAULT_SLOT_SIZE
    ])
    def test_bash_command_actually_renders(self, dag, utc_hour, taipei_hour, expected_n):
        """⭐ 真的把模板渲染一次，而不只是對字串做子字串比對。

        **為什麼需要這支測試**：Jinja 模板是 task 執行時才渲染的，錯誤在 DagBag
        解析階段【完全看不出來】——`dags list` 乾淨、import errors 為空、所有結構
        測試都綠，然後排程當天 task 在 0.16 秒內失敗。實作時連續踩到三種：
          ① `{{ f({{ x }}) }}` 巢狀 {{ }} 是語法錯誤
          ② f-string 裡的 `}}` 會被跳脫成單一個 `}`，把 Jinja 結束標記吃掉一半
          ③ 手動觸發時 data_interval_start 在 Airflow 3 裡根本不存在
        三種都只有真的渲染一次才抓得到。
        """
        # ⚠️ 刻意用 stdlib datetime 而非 pendulum：Airflow 傳給模板的
        #    dag_run.run_after 就是 stdlib datetime。這裡曾經用 pendulum 假物件，
        #    於是測試綠、runtime 卻炸 "'datetime.datetime' has no attribute
        #    'in_timezone'"——**假物件的型別錯了，測試就只是在測假物件**。
        from datetime import datetime, timezone

        env = dag.get_template_env()
        run_after = datetime(2026, 8, 12, utc_hour, tzinfo=timezone.utc)
        rendered = env.from_string(dag.get_task("seed_orders").bash_command).render(
            dag_run=SimpleNamespace(run_after=run_after))

        assert f"--n {expected_n}" in rendered, rendered
        # payload 種子＝日期＋台北時段（逐 slot 不同）
        assert f"--seed 20260812{taipei_hour}" in rendered, rendered
        # 髒率種子＝純日期（全天一致）
        assert "--dirty-rate-seed 20260812 " in rendered, rendered
        assert "{{" not in rendered and "}}" not in rendered, f"仍有未渲染的模板：{rendered}"

    def test_all_slots_of_a_taipei_day_share_one_dirty_rate_seed(self, dag):
        """⭐ cron 的 data_interval_start 是【上一個】觸發點，用它會讓每天 10:00 那批
        取到**前一天**的日期種子——當天四批的髒率就不再一致，而 Hard Gate 的整套
        推論（它判的是全天加權平均）正是建立在那個一致性上。

        這個錯誤不會有任何症狀：資料照樣灌得出來，只是當日髒率變成一個沒人算得
        出來的混合值。故必須由測試釘住。
        """
        from datetime import datetime, timedelta, timezone

        env = dag.get_template_env()
        tpl = env.from_string(dag.get_task("seed_orders").bash_command)
        seeds = set()
        tpe = timezone(timedelta(hours=8))
        for taipei_hour in (10, 13, 17, 21):
            run_after = datetime(2026, 8, 12, taipei_hour, tzinfo=tpe)
            out = tpl.render(dag_run=SimpleNamespace(run_after=run_after))
            seeds.add(out.split("--dirty-rate-seed ")[1].split()[0])
        assert seeds == {"20260812"}, f"同一個台北日的四個時段種子不一致：{seeds}"

    def test_slot_size_lookup_tolerates_manual_trigger(self, dag):
        """⭐ 手動觸發時 data_interval_start 是「現在」，其台北小時幾乎不會落在
        SLOT_SIZES 的鍵上。用 `[hour]` 下標會讓手動觸發必定以 Jinja KeyError 收場
        ——而手動觸發正是最需要它能動的時候（補灌、驗證改動、demo）。"""
        cmd = dag.get_task("seed_orders").bash_command
        assert "SLOT_SIZES.get(" in cmd, "必須用 .get(hour, 預設) 而非 [hour] 下標"
        assert "SLOT_SIZES[" not in cmd


class TestRawPendingWatch:
    """派工路徑的存活探針。它補的是 `_enqueue` 吞掉 broker 故障造成的靜默失敗
    ——那個狀態下 API 照回 202、Raw 一直長、卻沒有任何 worker 來取件、無人報錯。

    ⚠️ 它量的是「有沒有人來取」而非「有沒有寫進 ODS」：`duplicate` / `error`
    本來就不會產生 ODS 列，拿後者當判準的話每筆重複訂單都會誤報。"""

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["raw_pending_watch"]

    def test_slot_hours_match_the_seeding_dag(self, dagbag, dag):
        """⭐ `SEED_SLOT_HOURS` 是 seed_demo_daily 排程的**重複宣告**（刻意不跨 DAG
        檔 import，那會建立解析順序的耦合）。重複就必須被釘住，否則改了 seeding
        時段而忘了這邊時，探針會在「什麼都不會變化」的時刻檢查——它不會紅，
        只會安靜地失去意義，而那正是最難發現的壞法。"""
        seeding = dagbag.dags["seed_demo_daily"]
        seed_hours = sorted(int(h) for h in str(seeding.schedule).split()[1].split(","))
        watch_hours = sorted(int(h) for h in str(dag.schedule).split()[1].split(","))
        assert seed_hours == watch_hours, (
            f"探針時段 {watch_hours} 與 seeding 時段 {seed_hours} 不一致")

    def test_checks_after_the_batch_has_landed(self, dag):
        """偏移必須大於「批次送完 + 門檻」，否則正常批次會被當成卡住。
        250 筆 @0.8rps ≈ 5.2 分 + 60s verify ≈ 6.2 分；門檻約 20 分。"""
        offset = int(str(dag.schedule).split()[0])
        assert offset >= 27, f"偏移 {offset} 分鐘不足以涵蓋批次時間 + 門檻"

    def test_no_retries(self, dag):
        """唯讀且 deterministic，重試只會得到同一個答案。"""
        for task in dag.tasks:
            assert task.retries == 0

    def test_threshold_is_not_hardcoded_in_the_dag(self, dag):
        """⭐ 門檻必須在執行時從 `scan_interval_seconds` / `STALE_PROCESSING_MINUTES`
        / `PENDING_GRACE_SECONDS` 推導，不能寫死。寫死等於把推導結果凍成魔術數字，
        而下一個調 SCAN_INTERVAL_SECONDS 的人不會知道要回來改這裡。"""
        cmd = dag.get_task("check_raw_pending").bash_command
        assert "--max-age-seconds" not in cmd, "排程執行不該覆寫推導值（那是除錯用的旗標）"

    def test_is_not_upstream_of_anything(self, dag):
        """與 source_freshness_watch 同一條紀律：觀測訊號沒有阻斷下游的權限，
        也沒有污染別人成功率的權限。"""
        assert len(dag.tasks) == 1
        assert dag.tasks[0].downstream_task_ids == set()


class TestSeedDemoGateDemo:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["seed_demo_gate_demo"]

    def test_never_scheduled(self, dag):
        """⭐ 這是設計的核心，不是「還沒設好」。

        讓日常 seeding 偶爾超標會摧毀主 DAG 的成功率訊號——「主 DAG 紅」會同時
        代表「管線壞了」與「閘門正常運作」，而兩者需要完全相反的處置。
        這與 source_freshness_watch 被拆出去是同一個論證。"""
        assert dag.schedule is None

    def test_default_rate_clears_the_gate_decisively(self, dag):
        """Hard Gate 是 15%。預設值必須明顯超過而非剛好壓線——壓線會讓抽樣變異
        決定成敗，重跑一次紅一次綠，比沒有示範更糟。"""
        assert dag.params["dirty_rate"] >= 0.20

    def test_ingestion_itself_must_still_succeed(self, dag):
        """該髒的是【資料內容】，不是攝入路徑。攝入若失敗就什麼都沒展示到。"""
        assert "--require-landed-pct" in dag.get_task("seed_dirty_batch").bash_command


class TestSeedingIsIsolatedFromTheAnalyticsPipeline:

    def test_analytics_dag_does_not_seed(self, dagbag):
        """⭐ 資料產生器壞掉與分析管線壞掉需要不同處置，混在一條 DAG 裡會讓
        「主 DAG 紅」失去單一意義。與 freshness 被拆出去是同一條原則。"""
        for task in dagbag.dags["orders_analytics_daily"].tasks:
            assert "seed_demo.py" not in getattr(task, "bash_command", "")

    def test_only_seeding_dags_post_orders(self, dagbag):
        owners = {
            dag_id for dag_id, dag in dagbag.dags.items()
            for t in dag.tasks if "seed_demo.py" in getattr(t, "bash_command", "")
        }
        assert owners == {"seed_demo_daily", "seed_demo_gate_demo"}


class TestDqReevaluation:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["dq_reevaluation"]

    def test_never_scheduled(self, dag):
        """Proposal B 的觸發條件是「規則放寬了」——那是人為的部署事件，不是週期。
        規則沒變時重評估必然無事件產出，排成日批＝364 天的全歷史白工。"""
        assert dag.schedule is None

    def test_dry_run_is_the_default(self, dag):
        """append-only 表寫錯刪不掉；手動觸發的 UI 很容易一路點下去。"""
        assert dag.params["commit"] is False   # DAG.params 已解析成預設值，非 Param 物件

    def test_flags_are_omitted_unless_asked(self, dag):
        """指令模板必須是「留空就不加旗標」，讓預設值只存在於腳本裡——
        DAG 與腳本各留一份預設值就是漂移的來源。"""
        cmd = dag.get_task("reevaluate").bash_command
        assert "reevaluate_quality.py" in cmd
        for flag in ("--commit", "--limit", "--expect-rule-version"):
            assert f"' {flag}" in cmd or f"' {flag} '" in cmd
        # 不得寫死成無條件帶上 --commit
        assert not cmd.rstrip().endswith("--commit")

    def test_downstream_refresh_is_gated_on_commit(self, dag):
        """dry-run 沒有寫入，就沒有東西需要傳播；無條件觸發主 DAG 會製造假動作。"""
        assert dag.get_task("reevaluate").downstream_task_ids == {"should_refresh"}
        assert dag.get_task("should_refresh").downstream_task_ids == {"refresh_gold"}

    def test_refresh_targets_the_main_dag(self, dagbag, dag):
        """重評估只寫 PG；要回流 Gold 還需要 extract → int_ 重建。少了這一步，
        使用者會看到「我跑了 Proposal B 但什麼都沒發生」。"""
        target = dag.get_task("refresh_gold").trigger_dag_id
        assert target == "orders_analytics_daily"
        assert target in dagbag.dags          # 觸發目標必須真的存在

    def test_no_retries(self, dag):
        """人工觸發、有人盯著；失敗當下「先看清楚發生什麼」比自動再寫一次
        append-only 表更重要。"""
        for task in dag.tasks:
            assert task.retries == 0


class TestFailureNotification:
    """失敗通知的接線。

    這組測試守的是【漏掛】——一支排程 DAG 沒掛 callback 的後果不是報錯，
    而是它安靜地不通知任何人，跟其他三支看起來一模一樣。日後新增排程 DAG 時，
    SCHEDULED_DAG_IDS 這份清單就是提醒你補上的地方。
    """

    # 排程執行的四支：沒人盯著，失敗必須主動說話。
    SCHEDULED_DAG_IDS = [
        "orders_analytics_daily",
        "seed_demo_daily",
        "raw_pending_watch",
        "source_freshness_watch",
    ]

    # 人工觸發的兩支：觸發的人就在旁邊看著 UI，通知是噪音。
    MANUAL_DAG_IDS = ["seed_demo_gate_demo", "dq_reevaluation"]

    def test_scheduled_dags_notify_on_failure(self, dagbag):
        for dag_id in self.SCHEDULED_DAG_IDS:
            for task in dagbag.dags[dag_id].tasks:
                assert task.on_failure_callback, (
                    f"{dag_id}.{task.task_id} 沒有 on_failure_callback："
                    f"它失敗時不會通知任何人"
                )

    def test_manual_dags_do_not_notify(self, dagbag):
        """反向斷言：避免日後有人「順手全部加上」而讓手動 DAG 也開始吵。"""
        for dag_id in self.MANUAL_DAG_IDS:
            for task in dagbag.dags[dag_id].tasks:
                assert not task.on_failure_callback, (
                    f"{dag_id}.{task.task_id} 掛了 on_failure_callback，"
                    f"但它是人工觸發的"
                )

    def test_every_scheduled_dag_is_covered_by_this_list(self, dagbag):
        """清單不得與實際的排程 DAG 漂移——否則新增一支排程 DAG 時，
        上面那條斷言會因為「不在清單裡」而靜默地放它過去。"""
        scheduled = {
            dag_id for dag_id, dag in dagbag.dags.items() if dag.schedule is not None
        }
        assert scheduled == set(self.SCHEDULED_DAG_IDS)

    def test_default_channel_is_log(self):
        """預設不得指向任何外部端點：clone 下來就能跑，且不會出現一個
        指向不存在端點、實際上誰也通知不到的 callback。見 _notify.py 檔頭 ①。"""
        import _notify

        assert _notify.CHANNEL == _notify.CHANNEL_LOG

    def test_notification_never_raises(self):
        """通知在組裝或送出時拋例外 = 通知消失，而且 Airflow 記下的會是
        「通知系統壞了」而不是「你的 DAG 壞了」。故意餵一個空 context。"""
        import _notify

        callback = _notify.build_failure_callback("測試用語意")
        callback({})          # 沒有 task_instance / dag_run / exception
