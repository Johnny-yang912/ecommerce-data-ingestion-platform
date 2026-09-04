"""
OpenAPI 契約快照：把 API 契約的任何改動變成可審查的 PR diff。

機制：app.openapi() 與 committed 的 openapi.json 比對。端點增刪、回應形狀改動、
狀態碼說明改寫都會讓測試紅，逼一次有意識的 review。

這與 test_schema_snapshot.py 是同一個手法，但守的東西不同：那支守的是型別宣告，
這支守的是【對外承諾】。少了它，改完 API 忘記重新匯出 openapi.json 會靜默發生，
而那份 JSON 正是 GitHub Pages 上公開給串接方看的規格——它會安靜地過期。

確認改動是有意的之後，更新快照：
    UPDATE_OPENAPI=1 pytest tests/test_openapi_snapshot.py
或直接執行匯出腳本（兩者產出相同）：
    python scripts/export_openapi.py
"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

SPEC_PATH = ROOT / "openapi.json"


def _current() -> str:
    from export_openapi import render

    return render()


def test_openapi_matches_committed_spec():
    current = _current()

    if os.environ.get("UPDATE_OPENAPI") == "1":
        SPEC_PATH.write_text(current, encoding="utf-8")
        pytest.skip(f"已更新快照：{SPEC_PATH.name}")

    assert SPEC_PATH.exists(), (
        f"找不到 {SPEC_PATH.name}。首次建立請執行：python scripts/export_openapi.py"
    )
    expected = SPEC_PATH.read_text(encoding="utf-8")
    assert current == expected, (
        "openapi.json 與程式碼產生的規格不符。若 API 契約的改動是有意的，執行 "
        "`python scripts/export_openapi.py` 更新並在 PR 說明改了什麼；"
        "否則代表對外承諾被意外更動了。"
    )


def test_spec_declares_the_retry_guidance_for_422():
    """422 的描述必須說出「不要重試」。

    這不是在測文案，是在釘一個契約：500 的語意是「我的錯，你再試」，而 422 的
    語意是「你的錯，重試永遠不會成功」。本專案曾經讓 422 在 render 階段炸成 500，
    上游因此重送了永遠不可能成功的 payload（見 CHANGELOG〈缺陷與修正〉）。
    修法補在實作上，這一條把它補在對外規格上。
    """
    from main import app

    spec = app.openapi()
    for path in ("/orders", "/process_raw/{raw_id}", "/raw/{raw_id}"):
        method = "post" if path != "/raw/{raw_id}" else "get"
        description = spec["paths"][path][method]["responses"]["422"]["description"]
        assert "Do not retry" in description, (
            f"{method.upper()} {path} 的 422 說明沒有告訴客戶端不要重試："
            f"{description!r}"
        )
