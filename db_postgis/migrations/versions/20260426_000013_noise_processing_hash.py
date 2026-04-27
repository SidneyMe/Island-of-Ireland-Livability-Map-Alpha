from __future__ import annotations

from alembic import op


revision = "20260426_000013"
down_revision = "20260424_000012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE build_manifest
            ADD COLUMN IF NOT EXISTS noise_processing_hash TEXT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS build_manifest_noise_processing_hash_idx
            ON build_manifest (noise_processing_hash)
            WHERE noise_processing_hash IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS build_manifest_noise_processing_hash_idx")
    op.execute("ALTER TABLE build_manifest DROP COLUMN IF EXISTS noise_processing_hash")
