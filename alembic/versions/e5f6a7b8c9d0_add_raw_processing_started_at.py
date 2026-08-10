"""add raw.processing_started_at

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-08-10 14:20:00.000000

新增 raw.processing_started_at，作為 stale processing 判定的時間基準。

原本 scan_and_recover 以 received_at（攝入時刻）判定逾時，但它回答的是「這筆資料
躺了多久」，而不是「這次處理跑了多久」。平時兩者幾乎相等，積壓時則相差極大——
於是正在被處理的記錄會被誤判為逾時、收回改回 pending 並重新派工，造成同一個
raw_id 被兩個 worker 並行處理（CAS 擋不住，因為狀態已被第三方倒退回 pending）。
詳見 process.scan_and_recover 的時間軸說明與 QUEUE-TW.md §3.1。

Backfill：升級當下已在 processing 的記錄補上 received_at 作為近似值。不補的話
它們的 processing_started_at 為 NULL，永遠不符合 stale 條件 → 永久卡死。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "raw",
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    # 升級瞬間正在處理中的記錄：以 received_at 近似，維持與升級前一致的逾時行為。
    # 這是唯一一批可能 status='processing' 卻沒有 processing_started_at 的資料，
    # 補完之後「processing ⇒ processing_started_at 非空」即成為不變式
    # （此後所有進入 processing 的路徑只有 try_claim_raw，它一定會蓋上時刻）。
    op.execute(
        "UPDATE raw SET processing_started_at = received_at WHERE status = 'processing'"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("raw", "processing_started_at")
