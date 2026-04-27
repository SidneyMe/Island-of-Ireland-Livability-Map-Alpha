from __future__ import annotations

from alembic import op


revision = "20260427_000015"
down_revision = "20260427_000014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Raw normalised candidates: one geometry per source feature, EPSG:2157
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_normalized (
            noise_source_hash  TEXT             NOT NULL,
            jurisdiction       TEXT             NOT NULL,
            source_type        TEXT             NOT NULL,
            metric             TEXT             NOT NULL,
            round_number       INTEGER          NOT NULL,
            report_period      TEXT             NULL,
            db_low             DOUBLE PRECISION NULL,
            db_high            DOUBLE PRECISION NULL,
            db_value           TEXT             NOT NULL,
            source_dataset     TEXT             NOT NULL,
            source_layer       TEXT             NOT NULL,
            source_ref         TEXT             NULL,
            geom               GEOMETRY(MultiPolygon, 2157) NOT NULL,
            CHECK (jurisdiction IN ('roi', 'ni')),
            CHECK (metric IN ('Lden', 'Lnight')),
            CHECK (source_type IN ('airport', 'industry', 'rail', 'road', 'consolidated')),
            CHECK (db_value IN ('45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75+')),
            CHECK (db_low IS NULL OR db_high IS NULL OR db_high >= db_low)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_normalized_geom_gist
            ON noise_normalized USING GIST (geom)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_normalized_group_idx
            ON noise_normalized
            (noise_source_hash, jurisdiction, source_type, metric, round_number, db_value)
        """
    )

    # Final resolved polygons after dissolve + round-priority, EPSG:2157
    # source_ref removed from this table; source identity lives in noise_resolved_provenance
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_resolved_display (
            noise_resolved_hash TEXT             NOT NULL,
            noise_feature_id    BIGSERIAL        PRIMARY KEY,
            jurisdiction        TEXT             NOT NULL,
            source_type         TEXT             NOT NULL,
            metric              TEXT             NOT NULL,
            round_number        INTEGER          NOT NULL,
            report_period       TEXT             NULL,
            db_low              DOUBLE PRECISION NULL,
            db_high             DOUBLE PRECISION NULL,
            db_value            TEXT             NOT NULL,
            geom                GEOMETRY(MultiPolygon, 2157) NOT NULL,
            CHECK (jurisdiction IN ('roi', 'ni')),
            CHECK (metric IN ('Lden', 'Lnight')),
            CHECK (source_type IN ('airport', 'industry', 'rail', 'road', 'consolidated')),
            CHECK (db_value IN ('45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75+')),
            CHECK (db_low IS NULL OR db_high IS NULL OR db_high >= db_low)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_resolved_display_geom_gist
            ON noise_resolved_display USING GIST (geom)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS noise_resolved_display_filter_idx
            ON noise_resolved_display
            (noise_resolved_hash, jurisdiction, source_type, metric, db_value)
        """
    )

    # Provenance: group-level aggregate (Milestone A).
    # Avoids expensive per-polygon spatial joins; captures source identity at dissolve time.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS noise_resolved_provenance (
            noise_resolved_hash TEXT    NOT NULL,
            jurisdiction        TEXT    NOT NULL,
            source_type         TEXT    NOT NULL,
            metric              TEXT    NOT NULL,
            round_number        INTEGER NOT NULL,
            source_dataset      TEXT    NOT NULL,
            source_layer        TEXT    NOT NULL,
            source_ref_count    INTEGER NOT NULL,
            source_refs_hash    TEXT    NOT NULL,
            PRIMARY KEY (
                noise_resolved_hash, jurisdiction, source_type, metric,
                round_number, source_dataset, source_layer
            )
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS noise_resolved_provenance")
    op.execute("DROP INDEX IF EXISTS noise_resolved_display_filter_idx")
    op.execute("DROP INDEX IF EXISTS noise_resolved_display_geom_gist")
    op.execute("DROP TABLE IF EXISTS noise_resolved_display")
    op.execute("DROP INDEX IF EXISTS noise_normalized_group_idx")
    op.execute("DROP INDEX IF EXISTS noise_normalized_geom_gist")
    op.execute("DROP TABLE IF EXISTS noise_normalized")
