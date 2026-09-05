"""DAG 呼叫的腳本，在【它們實際執行的環境】裡 import 得起來嗎。

這支測試的存在理由是一次真實故障：2026-08-17 的 OTel 上線在 `process.py` 加了
一行 `from telemetry import ...`，而 `check_raw_pending.py` 為了推導告警門檻
會 `from process import PENDING_GRACE_SECONDS, ...`——於是那支唯讀探針在 Airflow
裡以 `ModuleNotFoundError: No module named 'opentelemetry'` 全紅，四個小時後才
被排程的紅燈揭露。主測試套件當時全綠，因為**它跑在裝好依賴的環境裡**。

那次故障有兩個獨立的成因，本檔各用一層測試釘住：

  ① 過寬的 import 鏈    唯讀探針被綁到寫入路徑的依賴樹上
                        → TestAnalyticsImportClosure（純靜態，任何環境都跑）

  ② 環境與宣告的漂移    `requirements-analytics.txt` 已經是 `-r requirements.txt`
                        （宣告裡有 opentelemetry），但 Airflow 映像沒重 build，
                        venv 裡沒有。**原始碼走 bind mount 即時生效、依賴不會**
                        ——這個落差是 Dockerfile 註解裡那句「原始碼不烤進映像」的
                        代價，不是 bug，但需要有東西看著。
                        → TestAnalyticsInterpreter（要有那個直譯器才跑）

⚠️ 為什麼 ① 不能用「比對 requirements 宣告」來做：那次宣告是**對的**，靜態比對
   會是綠的。宣告與現實的差距只有實跑得出來，所以 ② 無法被靜態測試取代；
   而 ② 需要環境、① 不需要——兩層都保留，各自補對方的盲區。

⚠️ 為什麼不放在 tests/test_dags.py：那支檔頭就 `pytest.importorskip("airflow")`，
   只在獨立的 dags CI job 跑。①  的價值在於**主套件**就會紅（改 process.py 的人
   跑的是主套件），所以它必須住在一個不需要 Airflow 的檔案裡。
"""

from __future__ import annotations

import ast
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DAGS_FOLDER = ROOT / "orchestration" / "dags"

# Airflow 容器內的路徑（見 orchestration/Dockerfile 與 docker-compose.airflow.yml）。
VENV_PYTHON = "/home/airflow/venvs/analytics/bin/python"
CONTAINER_PROJECT_DIR = "/opt/project"

# 走 analytics venv 的腳本一律長這個樣子：`{PY_ANALYTICS} {PROJECT_DIR}/xxx.py ...`。
# 用文字比對而不是 import DAG 模組：後者要 Airflow，而本檔刻意不依賴它（見檔頭）。
_ANALYTICS_CALL = r"\{PY_ANALYTICS\}\s+\{PROJECT_DIR\}/([\w.\-/]+\.py)"

# ⚠️ 這是 analytics 環境的**禁區**，不是「不好的模組」：telemetry 會拉進整套 OTel
#    SDK 與 instrumentation。走 analytics venv 的腳本都是短命的批次行程，遙測對它們
#    的價值遠低於「多一組依賴就多一個開不起來的理由」——而它們開不起來的後果是
#    監控與管線一起靜默停擺。真要為批次行程做遙測，該走 Airflow 自己的 OTel 輸出。
FORBIDDEN_MODULES = {"telemetry"}


def _analytics_scripts() -> list[str]:
    """DAG 裡所有由 analytics 直譯器執行的腳本檔名（去重、排序）。"""
    import re

    found: set[str] = set()
    for dag_file in sorted(DAGS_FOLDER.glob("*.py")):
        if dag_file.name.startswith("_"):
            continue
        found.update(re.findall(_ANALYTICS_CALL, dag_file.read_text(encoding="utf-8")))
    return sorted(found)


ANALYTICS_SCRIPTS = _analytics_scripts()


def _project_module_path(name: str) -> Path | None:
    """回傳專案內同名模組的檔案路徑；不是專案模組（= 第三方或 stdlib）則 None。"""
    candidate = ROOT / f"{name}.py"
    return candidate if candidate.is_file() else None


def _direct_imports(path: Path) -> set[str]:
    """該檔案 import 的 top-level 模組名。

    ⚠️ 連函式內的 lazy import 也算（`ast.walk` 走整棵樹）：判準是「這支腳本跑起來
    會不會踩到」，而不是「import 期會不會炸」。lazy import 只是把爆炸延後到執行中途，
    對一支批次腳本來說那更糟——它已經做了一半的事。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # level > 0 是相對 import；本專案是平坦的模組佈局，不會出現。
            if node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names


def import_closure(script: str) -> dict[str, list[str]]:
    """腳本能到達的所有專案內模組 → 把它拉進來的路徑（供錯誤訊息指認責任）。

    只遞迴進專案內模組：第三方套件的內部 import 鏈不是本專案能管的，
    而「第三方套件裝了沒有」由 TestAnalyticsInterpreter 實跑回答。
    """
    start = ROOT / script
    reached: dict[str, list[str]] = {}
    queue: list[tuple[str, list[str]]] = [(start.stem, [script])]
    seen = {start.stem}
    while queue:
        module, chain = queue.pop(0)
        path = _project_module_path(module) if module != start.stem else start
        if path is None:
            continue
        for name in sorted(_direct_imports(path)):
            if _project_module_path(name) is None or name in seen:
                continue
            seen.add(name)
            trail = chain + [f"{name}.py"]
            reached[name] = trail
            queue.append((name, trail))
    return reached


def _analytics_interpreter() -> list[str]:
    """能執行 analytics venv 的命令；取不到就 skip（不是 fail——見檔頭 ②）。

    三種來源，依序：
      1. `ANALYTICS_PYTHON`（完整命令，可含參數）——CI 或非標準佈局用
      2. 本機就有那個路徑 = 正在 Airflow 容器內跑 pytest
      3. host 上借用執行中的容器：`docker exec -w /opt/project <container> <venv python>`
    """
    override = os.environ.get("ANALYTICS_PYTHON")
    if override:
        return shlex.split(override)

    if Path(VENV_PYTHON).is_file():
        return [VENV_PYTHON]

    container = os.environ.get("AIRFLOW_CONTAINER", "api-airflow-scheduler-1")
    if shutil.which("docker"):
        try:
            probe = subprocess.run(
                ["docker", "ps", "--filter", f"name=^{container}$", "--format", "{{.Names}}"],
                capture_output=True, text=True, timeout=20,
            )
        except (OSError, subprocess.SubprocessError):
            probe = None
        # returncode != 0 常見於「docker 在但這個 shell 沒有 docker group」——
        # 那是環境權限問題，不是被測程式的問題，所以一樣走 skip。
        if probe and probe.returncode == 0 and container in probe.stdout:
            return ["docker", "exec", "-w", CONTAINER_PROJECT_DIR, container, VENV_PYTHON]

    pytest.skip(
        "找不到 analytics 直譯器。在 Airflow 容器內跑 pytest、讓 "
        f"{container} 處於執行中，或設 ANALYTICS_PYTHON=<完整命令> 皆可啟用。"
    )


class TestAnalyticsScriptInventory:
    """先確定「要測哪些腳本」這件事本身是對的——清單錯了，下面兩層都在測空氣。"""

    def test_scripts_were_discovered(self):
        assert ANALYTICS_SCRIPTS, (
            f"在 {DAGS_FOLDER} 裡找不到任何 analytics 腳本呼叫。"
            "DAG 的 bash_command 寫法若改了，_ANALYTICS_CALL 這條 regex 要跟著改，"
            "否則本檔會靜默地什麼都不測。"
        )

    @pytest.mark.parametrize("script", ANALYTICS_SCRIPTS)
    def test_script_exists(self, script):
        """DAG 指到不存在的檔案，只會在排程跑到時才變成紅燈。"""
        assert (ROOT / script).is_file(), f"DAG 指向的腳本不存在：{script}"


class TestAnalyticsImportClosure:
    """① 靜態層：不需要任何環境，改 process.py 的人跑主套件就會看到紅燈。"""

    @pytest.mark.parametrize("script", ANALYTICS_SCRIPTS)
    def test_no_forbidden_module_in_closure(self, script):
        closure = import_closure(script)
        hits = FORBIDDEN_MODULES & closure.keys()
        assert not hits, (
            f"{script} 的 import 閉包碰到了 analytics 環境的禁區 {sorted(hits)}：\n  "
            + "\n  ".join(" → ".join(closure[m]) for m in sorted(hits))
            + "\n這條鏈會把 OTel SDK 拉進 analytics venv。需要的是常數就把常數"
            "搬到獨立模組（recovery_policy 就是這樣來的），不要 import 整個寫入路徑。"
        )

    def test_probe_does_not_depend_on_write_path(self):
        """⭐ 專門釘住 check_raw_pending：探針的故障域必須小於被監控對象。

        它與上面那條參數化測試不同——上面問「有沒有碰到禁區」，這條問的是
        「有沒有依賴寫入路徑」。`process` 本身沒有錯，錯的是一支唯讀探針掛在它上面：
        那讓「派工路徑壞了」與「探針自己壞了」共用同一個紅燈，而 exit code 1 與 2
        分開的整個設計目的就是要區分這兩件事（見 check_raw_pending.py ⑥）。
        """
        closure = import_closure("check_raw_pending.py")
        write_path = {"process", "tasks", "celery_app", "clean"} & closure.keys()
        assert not write_path, (
            f"check_raw_pending 依賴了寫入路徑模組 {sorted(write_path)}：\n  "
            + "\n  ".join(" → ".join(closure[m]) for m in sorted(write_path))
        )


class TestAnalyticsInterpreter:
    """② 實跑層：宣告與映像的漂移只有真的 import 一次才看得出來。"""

    @pytest.fixture(scope="class")
    def interpreter(self):
        return _analytics_interpreter()

    @pytest.mark.parametrize("script", ANALYTICS_SCRIPTS)
    def test_script_imports_in_analytics_venv(self, interpreter, script):
        module = Path(script).stem
        env = dict(os.environ)
        # config.Settings 在 import 期就實例化，缺 DB_URL 會 fail-fast（刻意的，
        # 見 config.py）。這裡給一個語法合法的假值：create_engine 不連線，
        # 所以測 import 不需要真的有 DB——也不該有，那會讓這層變成整合測試。
        env.setdefault("DB_URL", "postgresql://smoke:smoke@localhost:5432/smoke")
        # 腳本可能住在子目錄（scripts/）。Python 執行 `python <dir>/x.py` 時放進
        # sys.path[0] 的是【腳本自己的目錄】，不是專案根——用相對路徑重現同一件事。
        # 兩種直譯器來源的工作目錄都是專案根（本機 cwd=ROOT、容器 -w /opt/project），
        # 所以相對路徑在兩邊都成立。
        parent = Path(script).parent.as_posix()
        result = subprocess.run(
            [*interpreter, "-c", f"import sys; sys.path.insert(0, {parent!r}); import {module}"],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0, (
            f"analytics venv 無法 import {module}：\n{result.stderr.strip()}\n"
            "若是 ModuleNotFoundError，通常代表 requirements 改了但 Airflow 映像沒重 build："
            "  docker compose -f docker-compose.airflow.yml build --no-cache"
        )
