from __future__ import annotations

from alembic import op


revision = "20260501_000019"
down_revision = "20260428_000018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_grid_artifact (
            artifact_hash      TEXT             NOT NULL,
            noise_source_hash  TEXT             NOT NULL,
            jurisdiction       TEXT             NOT NULL,
            source_type        TEXT             NOT NULL,
            metric             TEXT             NOT NULL,
            grid_size_m        INTEGER          NOT NULL,
            cell_x             INTEGER          NOT NULL,
            cell_y             INTEGER          NOT NULL,
            round_number       INTEGER          NOT NULL,
            report_period      TEXT             NULL,
            db_low             DOUBLE PRECISION NULL,
            db_high            DOUBLE PRECISION NULL,
            db_value           TEXT             NOT NULL,
            geom               GEOMETRY(MultiPolygon, 2157) NOT NULL,
            CHECK (jurisdiction IN ('roi', 'ni')),
            CHECK (metric IN ('Lden', 'Lnight')),
            CHECK (source_type IN ('road', 'rail')),
            CHECK (grid_size_m > 0)
        )
        """
    )
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
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_grid_artifact_geom_gist
        ON noise_grid_artifact USING GIST (geom)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS noise_grid_artifact_geom_gist")
    op.execute("DROP INDEX IF EXISTS noise_grid_artifact_key_idx")
    op.execute("DROP TABLE IF EXISTS noise_grid_artifact")
