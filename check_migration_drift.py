"""
Migration 漂移檢查（手動觸發，刻意不進 CI）

用途：把「改了 models.py 卻沒寫對應 migration」「migration 手改後與 models 分岔」
這類靜默漂移，變成一個可一鍵驗證、會明確紅/綠的結果。

為何不進 CI：本專案個人練習階段不把真實 DB 接進 CI（見 README〈持續整合〉）。
此腳本確定性、無併發、低 flake，且已預留 exit code 介面——將來要升級進 CI 或
pre-commit hook 時，直接複用本檔即可，無需改寫。

前提：執行時本機需有一顆可連的 Postgres；DB_URL 走 config.settings（與 alembic
env.py 同一真相來源），故請在專案根目錄執行（讓 alembic.ini 被正確讀到）。

流程：
  1. alembic upgrade head：把 DB_URL 指向的 DB 帶到最新（已在 head 則為 no-op）。
     先升級，後續比對才是純粹的「migration ↔ models」，不會把「DB 忘了升級」
     誤判成漂移。
  2. compare_metadata：反射真實 DB schema，對比 Base.metadata（models 宣告）。
  3. diff 為空 → exit 0（綠）；偵測到漂移 → 逐條印出 → exit 1（紅）；
     連線/環境錯誤 → exit 2。

用法：
  python check_migration_drift.py
"""

import sys

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from config import settings
from database import Base
import models  # noqa: F401
# 副作用 import：把 Raw / ODS / QualityEvent 註冊到 Base.metadata。
# 不 import，Base.metadata 為空，compare_metadata 會誤判「整庫該刪」，整份假漂移。
# 同 alembic/env.py 的處理。


def _upgrade_to_head() -> None:
    """以程式呼叫 alembic upgrade head，複用既有 alembic.ini + env.py。

    env.py 會自行從 settings.db_url 設定 sqlalchemy.url，故此處不重複指定 URL。
    """
    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")


def _detect_drift() -> list:
    """反射真實 DB schema，回傳與 Base.metadata 的差異清單（空 list = 無漂移）。"""
    engine = create_engine(settings.db_url)
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            return compare_metadata(ctx, Base.metadata)
    finally:
        engine.dispose()


def main() -> int:
    try:
        print("▶ alembic upgrade head ...", flush=True)
        _upgrade_to_head()
        print("▶ 比對真實 DB schema 與 models（compare_metadata）...", flush=True)
        diffs = _detect_drift()
    except OperationalError as e:
        print(
            "✗ 無法連線資料庫（請確認本機 Postgres 已啟動、DB_URL 正確）：\n"
            f"  {e}",
            file=sys.stderr,
        )
        return 2

    if not diffs:
        print("✅ 無漂移：migration 產生的 schema 與 models.py 一致。")
        return 0

    print(f"❌ 偵測到 {len(diffs)} 處漂移（models 與 migration 不一致）：", file=sys.stderr)
    for d in diffs:
        print(f"  - {d}", file=sys.stderr)
    print(
        "\n提示：若這是預期的 schema 變更，請執行 "
        '`alembic revision --autogenerate -m "..."` 補上 migration；'
        "若判定為假漂移（server default / 型別細節等），再評估是否於本腳本的 "
        "MigrationContext.configure 加上 compare_type / compare_server_default 等選項過濾。",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
