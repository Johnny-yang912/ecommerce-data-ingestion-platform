"""add ods schema drift columns

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-07 00:10:00.000000

新增與 has_clean_error 平行的 schema drift 訊號欄位（has_schema_drift /
schema_drift_message / unmapped_fields），並將 customer_id 改為可空——
order_id 為唯一硬閘門，缺 customer_id 改為可落地 + 由 drift 訊號標記。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "ods",
        sa.Column("has_schema_drift", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("ods", sa.Column("schema_drift_message", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("ods", sa.Column("unmapped_fields", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.alter_column("ods", "customer_id", existing_type=sa.String(length=50), nullable=True)


def downgrade() -> None:
    """Downgrade schema."""
    # 注意：縮回 NOT NULL 時，若已存在 customer_id 為 NULL 的列會失敗（PostgreSQL 預期行為）。
    op.alter_column("ods", "customer_id", existing_type=sa.String(length=50), nullable=False)
    op.drop_column("ods", "unmapped_fields")
    op.drop_column("ods", "schema_drift_message")
    op.drop_column("ods", "has_schema_drift")
