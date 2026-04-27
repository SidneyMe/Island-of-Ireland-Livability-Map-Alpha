from __future__ import annotations

from alembic import op


revision = "20260424_000012"
down_revision = "20260424_000011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_polygons (
            build_key TEXT NOT NULL,
            config_hash TEXT NOT NULL,
            import_fingerprint TEXT NOT NULL,
            jurisdiction TEXT NOT NULL,
            source_type TEXT NOT NULL,
            metric TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            report_period TEXT NULL,
            db_low DOUBLE PRECISION NULL,
            db_high DOUBLE PRECISION NULL,
            db_value TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_layer TEXT NOT NULL,
            source_ref TEXT NOT NULL,
            geom GEOMETRY(Geometry, 4326) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_polygons_build_metric_idx
            ON noise_polygons (build_key, metric)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_polygons_source_metric_idx
            ON noise_polygons (build_key, source_type, metric, db_value)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_polygons_geom_gist
            ON noise_polygons USING GIST (geom)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS noise_polygons_geom_gist")
    op.execute("DROP INDEX IF EXISTS noise_polygons_source_metric_idx")
    op.execute("DROP INDEX IF EXISTS noise_polygons_build_metric_idx")
    op.execute("DROP TABLE IF EXISTS noise_polygons")
