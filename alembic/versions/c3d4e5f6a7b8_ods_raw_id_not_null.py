"""tighten ods.raw_id to NOT NULL

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-06-14 00:00:00.000000

把 ODS.raw_id 從 nullable 收成 NOT NULL。raw_id 既無路徑會寫入 NULL（唯一寫入點
process.py:process_raw_event 永遠帶 Raw.id），又已是 UNIQUE——而 UNIQUE + nullable
在 PostgreSQL 允許多筆 NULL 並存，正是 stg_ 去重以 raw_id 為 grain 時的隱性破口。
此處把「本來就非空」的隱性事實升級為 schema 強制契約（並讓 BQ staging 鏡射為 REQUIRED）。

注意：SET NOT NULL 會掃全表，若已存在 raw_id 為 NULL 的列會直接失敗（PostgreSQL
預期行為）。upgrade 前先顯式檢查並以清楚訊息中止，避免曖昧的約束錯誤。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 前置守衛：有任何 NULL raw_id 就帶清楚訊息中止，而非讓 SET NOT NULL 噴模糊錯誤。
    null_count = op.get_bind().execute(
        sa.text("SELECT count(*) FROM ods WHERE raw_id IS NULL")
    ).scalar()
    if null_count:
        raise RuntimeError(
            f"無法收緊 ods.raw_id 為 NOT NULL：仍有 {null_count} 筆 raw_id IS NULL 的列。"
            "請先回填這些列的 raw_id（或釐清其來源）後再執行此 migration。"
        )
    op.alter_column("ods", "raw_id", existing_type=sa.Integer(), nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column("ods", "raw_id", existing_type=sa.Integer(), nullable=True)
