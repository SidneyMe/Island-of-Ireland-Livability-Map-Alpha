from __future__ import annotations

from alembic import op


revision = "20260427_000017"
down_revision = "20260427_000016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Informational: records which noise pipeline mode produced each build.
    # 'legacy' or 'artifact'. NULL for builds before this migration.
    op.execute(
        """
        ALTER TABLE build_manifest
            ADD COLUMN IF NOT EXISTS noise_mode TEXT NULL
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE build_manifest DROP COLUMN IF EXISTS noise_mode")
