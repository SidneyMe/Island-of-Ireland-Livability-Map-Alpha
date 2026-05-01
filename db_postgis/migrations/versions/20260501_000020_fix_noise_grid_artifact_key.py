from __future__ import annotations

from alembic import op


revision = "20260501_000020"
down_revision = "20260501_000019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS noise_grid_artifact_key_idx")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS noise_grid_artifact_key_idx
        ON noise_grid_artifact (
            artifact_hash,
            jurisdiction,
            source_type,
            metric,
            grid_size_m,
            cell_x,
            cell_y
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS noise_grid_artifact_key_idx")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS noise_grid_artifact_key_idx
        ON noise_grid_artifact (
            artifact_hash,
            source_type,
            metric,
            grid_size_m,
            cell_x,
            cell_y
        )
        """
    )
