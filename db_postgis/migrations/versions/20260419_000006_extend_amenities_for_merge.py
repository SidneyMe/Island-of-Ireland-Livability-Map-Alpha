from __future__ import annotations

from alembic import op


revision = "20260419_000006"
down_revision = "20260418_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE amenities
            ADD COLUMN IF NOT EXISTS name TEXT NULL
        """
    )
    op.execute(
        """
        ALTER TABLE amenities
            ADD COLUMN IF NOT EXISTS conflict_class TEXT NOT NULL DEFAULT 'osm_only'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE amenities
            DROP COLUMN IF EXISTS conflict_class
        """
    )
    op.execute(
        """
        ALTER TABLE amenities
            DROP COLUMN IF EXISTS name
        """
    )
