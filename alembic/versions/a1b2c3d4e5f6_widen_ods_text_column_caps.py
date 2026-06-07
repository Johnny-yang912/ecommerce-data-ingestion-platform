"""widen ods text column caps

Revision ID: a1b2c3d4e5f6
Revises: 285a2502b3e7
Create Date: 2026-06-07 00:00:00.000000

放寬 ODS 自由文字 / enum 類欄位的長度上限，作為「塞爆」的寬鬆硬牆（anti-garbage），
業務鍵（order_id / customer_id）與內部欄位（dq_rule_version / source_client_id）不動。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '285a2502b3e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (column, old_len, new_len)
_WIDENED = [
    ("ship_mode", 50, 64),
    ("order_status", 20, 64),
    ("customer_name", 100, 255),
    ("gender", 20, 32),
    ("membership_tier", 20, 64),
    ("acquisition_channel", 50, 64),
    ("preferred_payment_method", 50, 64),
    ("preferred_device", 50, 64),
    ("country", 50, 64),
    ("region", 50, 64),
    ("state", 50, 64),
    ("city", 50, 128),
    ("postal_code", 20, 32),
    ("payment_method", 50, 64),
    ("device_used", 50, 64),
]


def upgrade() -> None:
    """Upgrade schema."""
    for col, _old, new in _WIDENED:
        op.alter_column("ods", col, type_=sa.String(length=new), existing_nullable=True)


def downgrade() -> None:
    """Downgrade schema."""
    # 注意：縮回舊長度時，若既有資料已超過舊上限會失敗（PostgreSQL 預期行為）。
    for col, old, _new in _WIDENED:
        op.alter_column("ods", col, type_=sa.String(length=old), existing_nullable=True)
