from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision = "20260414_000002"
down_revision = "20260411_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS transit_raw")
    op.execute("CREATE SCHEMA IF NOT EXISTS transit_derived")

    op.create_table(
        "transport_reality",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("osm_source_ref", sa.Text(), nullable=False),
        sa.Column("osm_name", sa.Text(), nullable=True),
        sa.Column("reality_status", sa.Text(), nullable=False),
        sa.Column("match_status", sa.Text(), nullable=False),
        sa.Column("school_only_state", sa.Text(), nullable=False),
        sa.Column("matched_feed_id", sa.Text(), nullable=True),
        sa.Column("matched_stop_id", sa.Text(), nullable=True),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("reality_confidence", sa.Float(), nullable=False),
        sa.Column("public_departures_7d", sa.Integer(), nullable=False),
        sa.Column("public_departures_30d", sa.Integer(), nullable=False),
        sa.Column("school_only_departures_30d", sa.Integer(), nullable=False),
        sa.Column("last_public_service_date", sa.Date(), nullable=True),
        sa.Column("last_any_service_date", sa.Date(), nullable=True),
        sa.Column("route_modes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("match_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reality_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "transport_reality_build_source_idx",
        "transport_reality",
        ["build_key", "osm_source_ref"],
        unique=True,
    )
    op.create_index(
        "transport_reality_status_idx",
        "transport_reality",
        ["build_key", "reality_status"],
        unique=False,
    )
    op.create_index(
        "transport_reality_geom_gist",
        "transport_reality",
        ["geom"],
        unique=False,
        postgresql_using="gist",
    )

    op.create_table(
        "service_deserts",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("resolution_m", sa.Integer(), nullable=False),
        sa.Column("cell_id", sa.Text(), nullable=False),
        sa.Column("analysis_date", sa.Date(), nullable=False),
        sa.Column("nominal_reachable_stop_count", sa.Integer(), nullable=False),
        sa.Column("reachable_public_departures_7d", sa.Integer(), nullable=False),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "service_deserts_build_resolution_cell_idx",
        "service_deserts",
        ["build_key", "resolution_m", "cell_id"],
        unique=True,
    )
    op.create_index(
        "service_deserts_geom_gist",
        "service_deserts",
        ["cell_geom"],
        unique=False,
        postgresql_using="gist",
    )

    op.create_table(
        "feed_manifest",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("analysis_date", sa.Date(), nullable=False),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("feed_fingerprint"),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_feed_manifest_feed_idx",
        "feed_manifest",
        ["feed_id", "analysis_date"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "stops",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("stop_code", sa.Text(), nullable=True),
        sa.Column("stop_name", sa.Text(), nullable=False),
        sa.Column("stop_desc", sa.Text(), nullable=True),
        sa.Column("stop_lat", sa.Float(), nullable=False),
        sa.Column("stop_lon", sa.Float(), nullable=False),
        sa.Column("parent_station", sa.Text(), nullable=True),
        sa.Column("zone_id", sa.Text(), nullable=True),
        sa.Column("location_type", sa.Integer(), nullable=True),
        sa.Column("wheelchair_boarding", sa.Integer(), nullable=True),
        sa.Column("platform_code", sa.Text(), nullable=True),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_stops_feed_stop_idx",
        "stops",
        ["feed_id", "stop_id"],
        unique=False,
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_stops_geom_gist",
        "stops",
        ["geom"],
        unique=False,
        postgresql_using="gist",
        schema="transit_raw",
    )

    op.create_table(
        "routes",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("agency_id", sa.Text(), nullable=True),
        sa.Column("route_short_name", sa.Text(), nullable=True),
        sa.Column("route_long_name", sa.Text(), nullable=True),
        sa.Column("route_desc", sa.Text(), nullable=True),
        sa.Column("route_type", sa.Integer(), nullable=True),
        sa.Column("route_url", sa.Text(), nullable=True),
        sa.Column("route_color", sa.Text(), nullable=True),
        sa.Column("route_text_color", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_routes_feed_route_idx",
        "routes",
        ["feed_id", "route_id"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "trips",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("service_id", sa.Text(), nullable=False),
        sa.Column("trip_id", sa.Text(), nullable=False),
        sa.Column("trip_headsign", sa.Text(), nullable=True),
        sa.Column("trip_short_name", sa.Text(), nullable=True),
        sa.Column("direction_id", sa.Integer(), nullable=True),
        sa.Column("block_id", sa.Text(), nullable=True),
        sa.Column("shape_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_trips_feed_trip_idx",
        "trips",
        ["feed_id", "trip_id"],
        unique=False,
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_trips_feed_service_idx",
        "trips",
        ["feed_id", "service_id"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "stop_times",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("trip_id", sa.Text(), nullable=False),
        sa.Column("arrival_seconds", sa.Integer(), nullable=True),
        sa.Column("departure_seconds", sa.Integer(), nullable=True),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False),
        sa.Column("pickup_type", sa.Integer(), nullable=True),
        sa.Column("drop_off_type", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_stop_times_feed_trip_seq_idx",
        "stop_times",
        ["feed_id", "trip_id", "stop_sequence"],
        unique=False,
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_stop_times_feed_stop_idx",
        "stop_times",
        ["feed_id", "stop_id"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "calendar_services",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("service_id", sa.Text(), nullable=False),
        sa.Column("monday", sa.Integer(), nullable=False),
        sa.Column("tuesday", sa.Integer(), nullable=False),
        sa.Column("wednesday", sa.Integer(), nullable=False),
        sa.Column("thursday", sa.Integer(), nullable=False),
        sa.Column("friday", sa.Integer(), nullable=False),
        sa.Column("saturday", sa.Integer(), nullable=False),
        sa.Column("sunday", sa.Integer(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_calendar_services_feed_service_idx",
        "calendar_services",
        ["feed_id", "service_id"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "calendar_dates",
        sa.Column("feed_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("service_id", sa.Text(), nullable=False),
        sa.Column("service_date", sa.Date(), nullable=False),
        sa.Column("exception_type", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_raw",
    )
    op.create_index(
        "transit_raw_calendar_dates_feed_service_date_idx",
        "calendar_dates",
        ["feed_id", "service_id", "service_date"],
        unique=False,
        schema="transit_raw",
    )

    op.create_table(
        "reality_manifest",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("analysis_date", sa.Date(), nullable=False),
        sa.Column("transit_config_hash", sa.Text(), nullable=False),
        sa.Column("feed_fingerprints_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("reality_fingerprint"),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_reality_manifest_import_idx",
        "reality_manifest",
        ["import_fingerprint", "analysis_date"],
        unique=False,
        schema="transit_derived",
    )

    op.create_table(
        "service_classification",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("service_id", sa.Text(), nullable=False),
        sa.Column("school_only_state", sa.Text(), nullable=False),
        sa.Column("route_ids_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("route_modes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("time_bucket_counts_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_service_classification_reality_service_idx",
        "service_classification",
        ["reality_fingerprint", "feed_id", "service_id"],
        unique=False,
        schema="transit_derived",
    )

    op.create_table(
        "gtfs_stop_service_summary",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("public_departures_7d", sa.Integer(), nullable=False),
        sa.Column("public_departures_30d", sa.Integer(), nullable=False),
        sa.Column("school_only_departures_30d", sa.Integer(), nullable=False),
        sa.Column("last_public_service_date", sa.Date(), nullable=True),
        sa.Column("last_any_service_date", sa.Date(), nullable=True),
        sa.Column("route_modes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("route_ids_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_gtfs_stop_service_summary_reality_stop_idx",
        "gtfs_stop_service_summary",
        ["reality_fingerprint", "feed_id", "stop_id"],
        unique=False,
        schema="transit_derived",
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

    op.create_table(
        "osm_stop_reality",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("osm_source_ref", sa.Text(), nullable=False),
        sa.Column("osm_name", sa.Text(), nullable=True),
        sa.Column("osm_category", sa.Text(), nullable=False),
        sa.Column("matched_feed_id", sa.Text(), nullable=True),
        sa.Column("matched_stop_id", sa.Text(), nullable=True),
        sa.Column("match_status", sa.Text(), nullable=False),
        sa.Column("reality_status", sa.Text(), nullable=False),
        sa.Column("school_only_state", sa.Text(), nullable=False),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("reality_confidence", sa.Float(), nullable=False),
        sa.Column("public_departures_7d", sa.Integer(), nullable=False),
        sa.Column("public_departures_30d", sa.Integer(), nullable=False),
        sa.Column("school_only_departures_30d", sa.Integer(), nullable=False),
        sa.Column("last_public_service_date", sa.Date(), nullable=True),
        sa.Column("last_any_service_date", sa.Date(), nullable=True),
        sa.Column("route_modes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("match_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reality_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_osm_stop_reality_reality_osm_idx",
        "osm_stop_reality",
        ["reality_fingerprint", "osm_source_ref"],
        unique=False,
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_osm_stop_reality_status_idx",
        "osm_stop_reality",
        ["reality_fingerprint", "reality_status"],
        unique=False,
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_osm_stop_reality_geom_gist",
        "osm_stop_reality",
        ["geom"],
        unique=False,
        postgresql_using="gist",
        schema="transit_derived",
    )

    op.create_table(
        "service_desert_cells",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("resolution_m", sa.Integer(), nullable=False),
        sa.Column("cell_id", sa.Text(), nullable=False),
        sa.Column("analysis_date", sa.Date(), nullable=False),
        sa.Column("nominal_reachable_stop_count", sa.Integer(), nullable=False),
        sa.Column("reachable_public_departures_7d", sa.Integer(), nullable=False),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_service_desert_cells_build_resolution_cell_idx",
        "service_desert_cells",
        ["build_key", "resolution_m", "cell_id"],
        unique=False,
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_service_desert_cells_geom_gist",
        "service_desert_cells",
        ["cell_geom"],
        unique=False,
        postgresql_using="gist",
        schema="transit_derived",
    )


def downgrade() -> None:
    op.drop_index(
        "transit_derived_service_desert_cells_geom_gist",
        table_name="service_desert_cells",
        schema="transit_derived",
    )
    op.drop_index(
        "transit_derived_service_desert_cells_build_resolution_cell_idx",
        table_name="service_desert_cells",
        schema="transit_derived",
    )
    op.drop_table("service_desert_cells", schema="transit_derived")

    op.drop_index(
        "transit_derived_osm_stop_reality_geom_gist",
        table_name="osm_stop_reality",
        schema="transit_derived",
    )
    op.drop_index(
        "transit_derived_osm_stop_reality_status_idx",
        table_name="osm_stop_reality",
        schema="transit_derived",
    )
    op.drop_index(
        "transit_derived_osm_stop_reality_reality_osm_idx",
        table_name="osm_stop_reality",
        schema="transit_derived",
    )
    op.drop_table("osm_stop_reality", schema="transit_derived")

    op.drop_index(
        "transit_derived_stop_matches_reality_gtfs_idx",
        table_name="stop_matches",
        schema="transit_derived",
    )
    op.drop_index(
        "transit_derived_stop_matches_reality_osm_rank_idx",
        table_name="stop_matches",
        schema="transit_derived",
    )
    op.drop_table("stop_matches", schema="transit_derived")

    op.drop_index(
        "transit_derived_gtfs_stop_service_summary_reality_stop_idx",
        table_name="gtfs_stop_service_summary",
        schema="transit_derived",
    )
    op.drop_table("gtfs_stop_service_summary", schema="transit_derived")

    op.drop_index(
        "transit_derived_service_classification_reality_service_idx",
        table_name="service_classification",
        schema="transit_derived",
    )
    op.drop_table("service_classification", schema="transit_derived")

    op.drop_index(
        "transit_derived_reality_manifest_import_idx",
        table_name="reality_manifest",
        schema="transit_derived",
    )
    op.drop_table("reality_manifest", schema="transit_derived")

    op.drop_index(
        "transit_raw_calendar_dates_feed_service_date_idx",
        table_name="calendar_dates",
        schema="transit_raw",
    )
    op.drop_table("calendar_dates", schema="transit_raw")

    op.drop_index(
        "transit_raw_calendar_services_feed_service_idx",
        table_name="calendar_services",
        schema="transit_raw",
    )
    op.drop_table("calendar_services", schema="transit_raw")

    op.drop_index(
        "transit_raw_stop_times_feed_stop_idx",
        table_name="stop_times",
        schema="transit_raw",
    )
    op.drop_index(
        "transit_raw_stop_times_feed_trip_seq_idx",
        table_name="stop_times",
        schema="transit_raw",
    )
    op.drop_table("stop_times", schema="transit_raw")

    op.drop_index(
        "transit_raw_trips_feed_service_idx",
        table_name="trips",
        schema="transit_raw",
    )
    op.drop_index(
        "transit_raw_trips_feed_trip_idx",
        table_name="trips",
        schema="transit_raw",
    )
    op.drop_table("trips", schema="transit_raw")

    op.drop_index(
        "transit_raw_routes_feed_route_idx",
        table_name="routes",
        schema="transit_raw",
    )
    op.drop_table("routes", schema="transit_raw")

    op.drop_index(
        "transit_raw_stops_geom_gist",
        table_name="stops",
        schema="transit_raw",
    )
    op.drop_index(
        "transit_raw_stops_feed_stop_idx",
        table_name="stops",
        schema="transit_raw",
    )
    op.drop_table("stops", schema="transit_raw")

    op.drop_index(
        "transit_raw_feed_manifest_feed_idx",
        table_name="feed_manifest",
        schema="transit_raw",
    )
    op.drop_table("feed_manifest", schema="transit_raw")

    op.drop_index("service_deserts_geom_gist", table_name="service_deserts")
    op.drop_index("service_deserts_build_resolution_cell_idx", table_name="service_deserts")
    op.drop_table("service_deserts")

    op.drop_index("transport_reality_geom_gist", table_name="transport_reality")
    op.drop_index("transport_reality_status_idx", table_name="transport_reality")
    op.drop_index("transport_reality_build_source_idx", table_name="transport_reality")
    op.drop_table("transport_reality")

    op.execute("DROP SCHEMA IF EXISTS transit_derived")
    op.execute("DROP SCHEMA IF EXISTS transit_raw")
