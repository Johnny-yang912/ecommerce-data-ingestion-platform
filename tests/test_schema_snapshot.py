"""
Schema 契約快照：把型別宣告的任何改動變成可審查的 PR diff。

機制：OrderIN / ODSOrder 的 model_json_schema() 與 committed 的 golden 檔比對。
任何欄位增刪、型別改動都會讓測試紅，逼一次有意識的 review。

確認改動是有意的之後，更新快照：
    UPDATE_SCHEMA_SNAPSHOT=1 pytest tests/test_schema_snapshot.py
"""
import json
import os
from pathlib import Path

import pytest

from schema import OrderIN, ODSOrder

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"
MODELS = {"order_in": OrderIN, "ods_order": ODSOrder}


def _current(model):
    return json.dumps(
        model.model_json_schema(), indent=2, sort_keys=True, ensure_ascii=False
    )


@pytest.mark.parametrize("slug, model", MODELS.items())
def test_schema_matches_snapshot(slug, model):
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    path = SNAPSHOT_DIR / f"{slug}.schema.json"
    current = _current(model)

    if os.environ.get("UPDATE_SCHEMA_SNAPSHOT") == "1":
        path.write_text(current, encoding="utf-8")
        pytest.skip(f"已更新快照：{path.name}")

    assert path.exists(), (
        f"找不到快照 {path.name}。首次建立請執行："
        f"UPDATE_SCHEMA_SNAPSHOT=1 pytest {Path(__file__).name}"
    )
    expected = path.read_text(encoding="utf-8")
    assert current == expected, (
        f"{slug} 的 schema 與快照不符。若改動有意，執行 "
        f"UPDATE_SCHEMA_SNAPSHOT=1 pytest 更新快照並在 PR 說明；"
        f"否則代表型別宣告被意外更動。"
    )
