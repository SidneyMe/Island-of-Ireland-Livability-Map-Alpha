from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260417_000004"
down_revision = "20260415_000003"
branch_labels = None
depends_on = None


def _qualified_table(schema: str | None, table: str) -> str:
    return f"{schema}.{table}" if schema else table


def _rename_column_if_exists(schema: str | None, table: str, old: str, new: str) -> None:
    schema_name = schema or "public"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                  AND table_name = '{table}'
                  AND column_name = '{old}'
            ) THEN
                ALTER TABLE {_qualified_table(schema, table)} RENAME COLUMN {old} TO {new};
            END IF;
        END
        $$;
        """
    )


def _drop_column_if_exists(schema: str | None, table: str, column: str) -> None:
    schema_name = schema or "public"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                  AND table_name = '{table}'
                  AND column_name = '{column}'
            ) THEN
                ALTER TABLE {_qualified_table(schema, table)} DROP COLUMN {column};
            END IF;
        END
        $$;
        """
    )


def _rename_table_if_exists(schema: str | None, old: str, new: str) -> None:
    schema_name = schema or "public"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                  AND table_name = '{old}'
            ) THEN
                ALTER TABLE {_qualified_table(schema, old)} RENAME TO {new};
            END IF;
        END
        $$;
        """
    )


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS transit_derived.stop_matches CASCADE")

    _drop_column_if_exists(None, "transport_reality", "selected_public_departures_30d")
    _drop_column_if_exists("transit_derived", "osm_stop_reality", "selected_public_departures_30d")

    _rename_table_if_exists("transit_derived", "osm_stop_reality", "gtfs_stop_reality")

    for schema, table in (
        (None, "transport_reality"),
        ("transit_derived", "gtfs_stop_reality"),
    ):
        _rename_column_if_exists(schema, table, "osm_source_ref", "source_ref")
        _rename_column_if_exists(schema, table, "osm_name", "stop_name")
        _rename_column_if_exists(schema, table, "matched_feed_id", "feed_id")
        _rename_column_if_exists(schema, table, "matched_stop_id", "stop_id")
        _rename_column_if_exists(schema, table, "match_status", "source_status")
        _rename_column_if_exists(schema, table, "match_reason_codes_json", "source_reason_codes_json")

    _drop_column_if_exists(None, "transport_reality", "match_confidence")
    _drop_column_if_exists("transit_derived", "gtfs_stop_reality", "match_confidence")
    _drop_column_if_exists("transit_derived", "gtfs_stop_reality", "osm_category")

    _rename_column_if_exists(None, "service_deserts", "nominal_reachable_stop_count", "baseline_reachable_stop_count")
    _rename_column_if_exists("transit_derived", "service_desert_cells", "nominal_reachable_stop_count", "baseline_reachable_stop_count")

    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_osm_stop_reality_reality_osm_idx")
    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_osm_stop_reality_status_idx")
    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_osm_stop_reality_geom_gist")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_gtfs_stop_reality_reality_source_idx
        ON transit_derived.gtfs_stop_reality (reality_fingerprint, source_ref)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_gtfs_stop_reality_status_idx
        ON transit_derived.gtfs_stop_reality (reality_fingerprint, reality_status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_gtfs_stop_reality_geom_gist
        ON transit_derived.gtfs_stop_reality
        USING gist (geom)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_gtfs_stop_reality_reality_source_idx")
    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_gtfs_stop_reality_status_idx")
    op.execute("DROP INDEX IF EXISTS transit_derived.transit_derived_gtfs_stop_reality_geom_gist")

    _rename_column_if_exists(None, "service_deserts", "baseline_reachable_stop_count", "nominal_reachable_stop_count")
    _rename_column_if_exists("transit_derived", "service_desert_cells", "baseline_reachable_stop_count", "nominal_reachable_stop_count")

    op.execute(
        """
        ALTER TABLE transit_derived.gtfs_stop_reality
        ADD COLUMN IF NOT EXISTS osm_category TEXT NOT NULL DEFAULT 'transport'
        """
    )
    op.execute(
        """
        ALTER TABLE transit_derived.gtfs_stop_reality
        ADD COLUMN IF NOT EXISTS match_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0
        """
    )
    op.execute(
        """
        ALTER TABLE transport_reality
        ADD COLUMN IF NOT EXISTS match_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0
        """
    )
    op.execute(
        """
        ALTER TABLE transit_derived.gtfs_stop_reality
        ADD COLUMN IF NOT EXISTS selected_public_departures_30d INTEGER
        """
    )
    op.execute(
        """
        ALTER TABLE transport_reality
        ADD COLUMN IF NOT EXISTS selected_public_departures_30d INTEGER
        """
    )

    for schema, table in (
        (None, "transport_reality"),
        ("transit_derived", "gtfs_stop_reality"),
    ):
        _rename_column_if_exists(schema, table, "source_ref", "osm_source_ref")
        _rename_column_if_exists(schema, table, "stop_name", "osm_name")
        _rename_column_if_exists(schema, table, "feed_id", "matched_feed_id")
        _rename_column_if_exists(schema, table, "stop_id", "matched_stop_id")
        _rename_column_if_exists(schema, table, "source_status", "match_status")
        _rename_column_if_exists(schema, table, "source_reason_codes_json", "match_reason_codes_json")

    _rename_table_if_exists("transit_derived", "gtfs_stop_reality", "osm_stop_reality")

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_reality_osm_idx
        ON transit_derived.osm_stop_reality (reality_fingerprint, osm_source_ref)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_status_idx
        ON transit_derived.osm_stop_reality (reality_fingerprint, reality_status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_geom_gist
        ON transit_derived.osm_stop_reality
        USING gist (geom)
        """
    )

    op.create_table(
        "stop_matches",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("osm_source_ref", sa.Text(), nullable=False),
        sa.Column("gtfs_feed_id", sa.Text(), nullable=False),
        sa.Column("gtfs_stop_id", sa.Text(), nullable=False),
        sa.Column("candidate_rank", sa.Integer(), nullable=False),
        sa.Column("distance_m", sa.Float(), nullable=False),
        sa.Column("name_similarity", sa.Float(), nullable=False),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("match_status", sa.Text(), nullable=False),
        sa.Column("match_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_stop_matches_reality_osm_rank_idx",
        "stop_matches",
        ["reality_fingerprint", "osm_source_ref", "candidate_rank"],
        unique=False,
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_stop_matches_reality_gtfs_idx",
        "stop_matches",
        ["reality_fingerprint", "gtfs_feed_id", "gtfs_stop_id"],
        unique=False,
        schema="transit_derived",
    )
