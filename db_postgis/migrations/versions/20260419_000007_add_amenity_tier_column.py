from __future__ import annotations

from alembic import op


revision = "20260419_000007"
down_revision = "20260419_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE amenities
            ADD COLUMN IF NOT EXISTS tier TEXT NULL
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE amenities
            DROP COLUMN IF EXISTS tier
        """
    )
