"""add FK ods.raw_id -> raw.id (single-ingress invariant)

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-06-14 00:30:00.000000

把「每筆 ODS 列都對應一筆真實 Raw」從政策升級為 DB 保證——配合 raw_id 已 NOT NULL，
等於強制 single-ingress：資料不得繞過 Raw 直落 ODS（捏造 raw_id 會被 FK 擋下）。

ON DELETE NO ACTION：有 ODS 子列就不准刪對應 raw，保護 Proposal C 的「從 Raw 重建」承諾
（raw 必須活得比它的 ods 列久）。刻意不用 CASCADE——刪 raw 不該靜默連坐下游 ODS。

注意：建 FK 會掃全表並短暫上 ACCESS EXCLUSIVE 鎖。本專案規模小，直接建即可；
若未來 ODS 巨大，可改兩段式（先 ADD CONSTRAINT ... NOT VALID，再 VALIDATE CONSTRAINT）
以縮短鎖時間。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

FK_NAME = "fk_ods_raw_id_raw"  # 依 Base.metadata naming_convention 渲染而得


def upgrade() -> None:
    """Upgrade schema."""
    # 前置守衛：任何 raw_id 在 raw 找不到對應（孤兒列）會讓 FK 建立失敗，
    # 先以清楚訊息中止，而非讓 ADD CONSTRAINT 噴模糊錯誤。
    orphan = op.get_bind().execute(sa.text(
        "SELECT count(*) FROM ods o "
        "LEFT JOIN raw r ON o.raw_id = r.id "
        "WHERE r.id IS NULL"
    )).scalar()
    if orphan:
        raise RuntimeError(
            f"無法建立 FK ods.raw_id→raw.id：有 {orphan} 筆 ODS 列的 raw_id 在 raw 無對應（孤兒列）。"
            "這代表存在繞過 Raw 的直落資料，請先釐清來源並補上對應 Raw 後再執行。"
        )
    op.create_foreign_key(
        FK_NAME, "ods", "raw",
        ["raw_id"], ["id"],
        ondelete="NO ACTION",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(FK_NAME, "ods", type_="foreignkey")
