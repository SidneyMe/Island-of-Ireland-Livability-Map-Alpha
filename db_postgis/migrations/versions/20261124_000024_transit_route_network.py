from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision = "20261124_000024"
down_revision = "20260708_000023"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.add_column(
        "transport_reality",
        sa.Column("route_keys_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
    )

    op.create_table(
        "route_network_manifest",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("route_network_hash", sa.Text(), nullable=False),
        sa.Column("algorithm_version", sa.Integer(), nullable=False),
        sa.Column("fallback_backend", sa.Text(), nullable=False),
        sa.Column("route_count", sa.Integer(), nullable=False),
        sa.Column("pattern_count", sa.Integer(), nullable=False),
        sa.Column("route_stop_count", sa.Integer(), nullable=False),
        sa.Column("transfer_hub_count", sa.Integer(), nullable=False),
        sa.Column("warnings_json", _jsonb(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("reality_fingerprint"),
        schema="transit_derived",
    )
    op.create_table(
        "route_patterns",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("route_network_hash", sa.Text(), nullable=False),
        sa.Column("route_key", sa.Text(), nullable=False),
        sa.Column("pattern_id", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("route_short_name", sa.Text(), nullable=True),
        sa.Column("route_long_name", sa.Text(), nullable=True),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("route_color", sa.Text(), nullable=False),
        sa.Column("route_text_color", sa.Text(), nullable=False),
        sa.Column("direction_id", sa.Integer(), nullable=True),
        sa.Column("headsign", sa.Text(), nullable=True),
        sa.Column("origin_name", sa.Text(), nullable=True),
        sa.Column("destination_name", sa.Text(), nullable=True),
        sa.Column("geometry_source", sa.Text(), nullable=False),
        sa.Column("is_approximate", sa.Boolean(), nullable=False),
        sa.Column("length_m", sa.Float(), nullable=False),
        sa.Column("station_connection_count", sa.Integer(), nullable=False),
        sa.Column("visibility_class", sa.Text(), nullable=False),
        sa.Column("min_zoom", sa.Integer(), nullable=False),
        sa.Column("stop_ids_json", _jsonb(), nullable=False),
        sa.Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("reality_fingerprint", "pattern_id"),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_route_patterns_reality_route_idx",
        "route_patterns",
        ["reality_fingerprint", "route_key"],
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_route_patterns_geom_gist",
        "route_patterns",
        ["geom"],
        postgresql_using="gist",
        schema="transit_derived",
    )
    op.create_table(
        "route_pattern_stops",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("route_key", sa.Text(), nullable=False),
        sa.Column("pattern_id", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False),
        sa.Column("stop_name", sa.Text(), nullable=True),
        sa.Column("parent_station", sa.Text(), nullable=True),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("reality_fingerprint", "pattern_id", "stop_sequence"),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_route_pattern_stops_reality_stop_idx",
        "route_pattern_stops",
        ["reality_fingerprint", "feed_id", "stop_id"],
        schema="transit_derived",
    )
    op.create_table(
        "transfer_hubs",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("hub_id", sa.Text(), nullable=False),
        sa.Column("hub_name", sa.Text(), nullable=True),
        sa.Column("route_keys_json", _jsonb(), nullable=False),
        sa.Column("stop_refs_json", _jsonb(), nullable=False),
        sa.Column("modes_json", _jsonb(), nullable=False),
        sa.Column("route_count", sa.Integer(), nullable=False),
        sa.Column("backbone_route_count", sa.Integer(), nullable=False),
        sa.Column("regional_route_count", sa.Integer(), nullable=False),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("reality_fingerprint", "hub_id"),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_transfer_hubs_reality_idx",
        "transfer_hubs",
        ["reality_fingerprint"],
        schema="transit_derived",
    )

    op.create_table(
        "transport_route_patterns",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("route_key", sa.Text(), nullable=False),
        sa.Column("pattern_id", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("route_short_name", sa.Text(), nullable=True),
        sa.Column("route_long_name", sa.Text(), nullable=True),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("route_color", sa.Text(), nullable=False),
        sa.Column("route_text_color", sa.Text(), nullable=False),
        sa.Column("direction_id", sa.Integer(), nullable=True),
        sa.Column("headsign", sa.Text(), nullable=True),
        sa.Column("origin_name", sa.Text(), nullable=True),
        sa.Column("destination_name", sa.Text(), nullable=True),
        sa.Column("geometry_source", sa.Text(), nullable=False),
        sa.Column("is_approximate", sa.Boolean(), nullable=False),
        sa.Column("length_m", sa.Float(), nullable=False),
        sa.Column("station_connection_count", sa.Integer(), nullable=False),
        sa.Column("visibility_class", sa.Text(), nullable=False),
        sa.Column("min_zoom", sa.Integer(), nullable=False),
        sa.Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("build_key", "pattern_id"),
    )
    op.create_index("transport_route_patterns_build_route_idx", "transport_route_patterns", ["build_key", "route_key"])
    op.create_index("transport_route_patterns_geom_gist", "transport_route_patterns", ["geom"], postgresql_using="gist")
    op.create_table(
        "transport_transfer_hubs",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("hub_id", sa.Text(), nullable=False),
        sa.Column("hub_name", sa.Text(), nullable=True),
        sa.Column("route_keys_json", _jsonb(), nullable=False),
        sa.Column("modes_json", _jsonb(), nullable=False),
        sa.Column("route_count", sa.Integer(), nullable=False),
        sa.Column("backbone_route_count", sa.Integer(), nullable=False),
        sa.Column("regional_route_count", sa.Integer(), nullable=False),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("build_key", "hub_id"),
    )
    op.create_index("transport_transfer_hubs_build_idx", "transport_transfer_hubs", ["build_key"])
    op.create_index("transport_transfer_hubs_geom_gist", "transport_transfer_hubs", ["geom"], postgresql_using="gist")


def downgrade() -> None:
    op.drop_index("transport_transfer_hubs_geom_gist", table_name="transport_transfer_hubs")
    op.drop_index("transport_transfer_hubs_build_idx", table_name="transport_transfer_hubs")
    op.drop_table("transport_transfer_hubs")
    op.drop_index("transport_route_patterns_geom_gist", table_name="transport_route_patterns")
    op.drop_index("transport_route_patterns_build_route_idx", table_name="transport_route_patterns")
    op.drop_table("transport_route_patterns")
    op.drop_index("transit_derived_transfer_hubs_reality_idx", table_name="transfer_hubs", schema="transit_derived")
    op.drop_table("transfer_hubs", schema="transit_derived")
    op.drop_index("transit_derived_route_pattern_stops_reality_stop_idx", table_name="route_pattern_stops", schema="transit_derived")
    op.drop_table("route_pattern_stops", schema="transit_derived")
    op.drop_index("transit_derived_route_patterns_geom_gist", table_name="route_patterns", schema="transit_derived")
    op.drop_index("transit_derived_route_patterns_reality_route_idx", table_name="route_patterns", schema="transit_derived")
    op.drop_table("route_patterns", schema="transit_derived")
    op.drop_table("route_network_manifest", schema="transit_derived")
    op.drop_column("transport_reality", "route_keys_json")
