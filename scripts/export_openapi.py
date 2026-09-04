#!/usr/bin/env python3
"""
把 FastAPI 產生的 OpenAPI 規格匯出成 repo 根目錄的 openapi.json。

為什麼要有這個檔案：`/docs` 只是渲染器，規格本身是那份 JSON。留在服務裡的話它
只存在於 localhost；匯出成檔案之後它才可攜——能進版控、能在 PR 裡 diff、能被
GitHub Pages 渲染成一個公開網址（見 .github/workflows/pages.yml）。

用法：
    python scripts/export_openapi.py

⚠️ 不需要資料庫。app.openapi() 只讀路由與型別宣告，不開任何連線；CI 那個假的
   DB_URL 只是為了讓 config 的 fail-fast 通過。

正規化方式（indent=2, sort_keys=True, ensure_ascii=False）與
tests/test_schema_snapshot.py 一致，讓不同 Python 版本產出逐位元組相同的結果。
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

OUTPUT = ROOT / "openapi.json"


def render() -> str:
    from main import app

    return json.dumps(app.openapi(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> int:
    spec = render()
    previous = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else None
    OUTPUT.write_text(spec, encoding="utf-8")

    if previous is None:
        print(f"已建立 {OUTPUT.relative_to(ROOT)}（{len(spec)} 位元組）")
    elif previous == spec:
        print(f"{OUTPUT.relative_to(ROOT)} 無變更")
    else:
        print(f"已更新 {OUTPUT.relative_to(ROOT)}——契約有變，記得在 PR 說明改了什麼")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
