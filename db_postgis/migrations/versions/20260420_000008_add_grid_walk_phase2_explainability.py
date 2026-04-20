from __future__ import annotations

from alembic import op


revision = "20260420_000008"
down_revision = "20260419_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE grid_walk
            ADD COLUMN IF NOT EXISTS cluster_counts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            ADD COLUMN IF NOT EXISTS effective_units_json JSONB NOT NULL DEFAULT '{}'::jsonb
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE grid_walk
            DROP COLUMN IF EXISTS effective_units_json,
            DROP COLUMN IF EXISTS cluster_counts_json
        """
    )
