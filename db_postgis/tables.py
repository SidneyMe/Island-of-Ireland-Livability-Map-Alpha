from __future__ import annotations

from config import OSM_IMPORT_SCHEMA, TRANSIT_DERIVED_SCHEMA, TRANSIT_RAW_SCHEMA

from ._dependencies import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Geometry,
    Integer,
    JSONB,
    MetaData,
    Table,
    Text,
)
from .common import _table_key


metadata = MetaData()


grid_walk = Table(
    "grid_walk",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("resolution_m", Integer, nullable=False),
    Column("cell_id", Text, nullable=False),
    Column("centre_geom", Geometry("POINT", srid=4326), nullable=False),
    Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("effective_area_m2", Float, nullable=False),
    Column("effective_area_ratio", Float, nullable=False),
    Column("counts_json", JSONB, nullable=False),
    Column("cluster_counts_json", JSONB, nullable=False),
    Column("effective_units_json", JSONB, nullable=False),
    Column("scores_json", JSONB, nullable=False),
    Column("total_score", Float, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

amenities = Table(
    "amenities",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("tier", Text, nullable=True),
    Column("geom", Geometry("POINT", srid=4326), nullable=False),
    Column("source", Text, nullable=False),
    Column("source_ref", Text, nullable=True),
    Column("name", Text, nullable=True),
    Column("conflict_class", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

transport_reality = Table(
    "transport_reality",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("source_ref", Text, nullable=False),
    Column("stop_name", Text, nullable=True),
    Column("reality_status", Text, nullable=False),
    Column("source_status", Text, nullable=False),
    Column("school_only_state", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("stop_id", Text, nullable=False),
    Column("public_departures_7d", Integer, nullable=False),
    Column("public_departures_30d", Integer, nullable=False),
    Column("school_only_departures_30d", Integer, nullable=False),
    Column("weekday_morning_peak_deps", Float, nullable=False),
    Column("weekday_evening_peak_deps", Float, nullable=False),
    Column("weekday_offpeak_deps", Float, nullable=False),
    Column("saturday_deps", Float, nullable=False),
    Column("sunday_deps", Float, nullable=False),
    Column("friday_evening_deps", Float, nullable=False),
    Column("transport_score_units", Integer, nullable=False),
    Column("bus_daytime_deps", Float, nullable=False),
    Column("bus_daytime_headway_min", Float, nullable=True),
    Column("bus_frequency_tier", Text, nullable=True),
    Column("bus_frequency_score_units", Integer, nullable=False),
    Column("last_public_service_date", Date, nullable=True),
    Column("last_any_service_date", Date, nullable=True),
    Column("bus_active_days_mask_7d", Text, nullable=True),
    Column("bus_service_subtier", Text, nullable=True),
    Column("is_unscheduled_stop", Boolean, nullable=False),
    Column("has_exception_only_service", Boolean, nullable=False),
    Column("has_any_bus_service", Boolean, nullable=False),
    Column("has_daily_bus_service", Boolean, nullable=False),
    Column("route_modes_json", JSONB, nullable=False),
    Column("source_reason_codes_json", JSONB, nullable=False),
    Column("reality_reason_codes_json", JSONB, nullable=False),
    Column("geom", Geometry("POINT", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

service_deserts = Table(
    "service_deserts",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("resolution_m", Integer, nullable=False),
    Column("cell_id", Text, nullable=False),
    Column("analysis_date", Date, nullable=False),
    Column("baseline_reachable_stop_count", Integer, nullable=False),
    Column("reachable_public_departures_7d", Integer, nullable=False),
    Column("reason_codes_json", JSONB, nullable=False),
    Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

noise_polygons = Table(
    "noise_polygons",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("jurisdiction", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("round_number", Integer, nullable=False),
    Column("report_period", Text, nullable=True),
    Column("db_low", Float, nullable=True),
    Column("db_high", Float, nullable=True),
    Column("db_value", Text, nullable=False),
    Column("source_dataset", Text, nullable=False),
    Column("source_layer", Text, nullable=False),
    Column("source_ref", Text, nullable=False),
    Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

build_manifest = Table(
    "build_manifest",
    metadata,
    Column("build_key", Text, primary_key=True),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("extract_path", Text, nullable=False),
    Column("geo_hash", Text, nullable=False),
    Column("reach_hash", Text, nullable=False),
    Column("score_hash", Text, nullable=False),
    Column("render_hash", Text, nullable=False),
    Column("noise_processing_hash", Text, nullable=True),
    Column("noise_artifact_hash", Text, nullable=True),
    Column("noise_mode", Text, nullable=True),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("python_version", Text, nullable=False),
    Column("packages_json", JSONB, nullable=False),
    Column("summary_json", JSONB, nullable=False),
)

import_manifest = Table(
    "import_manifest",
    metadata,
    Column("import_fingerprint", Text, primary_key=True),
    Column("extract_path", Text, nullable=False),
    Column("extract_fingerprint", Text, nullable=False),
    Column("importer_version", Text, nullable=False),
    Column("importer_config_hash", Text, nullable=False),
    Column("normalization_scope_hash", Text, nullable=False, server_default=""),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    schema=OSM_IMPORT_SCHEMA,
)

features = Table(
    "features",
    metadata,
    Column("import_fingerprint", Text, nullable=False),
    Column("osm_type", Text, nullable=False),
    Column("osm_id", BigInteger, nullable=False),
    Column("category", Text, nullable=False),
    Column("name", Text, nullable=True),
    Column("tags_json", JSONB, nullable=False),
    Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=OSM_IMPORT_SCHEMA,
)

transit_feed_manifest = Table(
    "feed_manifest",
    metadata,
    Column("feed_fingerprint", Text, primary_key=True),
    Column("feed_id", Text, nullable=False),
    Column("analysis_date", Date, nullable=False),
    Column("source_path", Text, nullable=False),
    Column("source_url", Text, nullable=True),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_stops = Table(
    "stops",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("stop_id", Text, nullable=False),
    Column("stop_code", Text, nullable=True),
    Column("stop_name", Text, nullable=False),
    Column("stop_desc", Text, nullable=True),
    Column("stop_lat", Float, nullable=False),
    Column("stop_lon", Float, nullable=False),
    Column("parent_station", Text, nullable=True),
    Column("zone_id", Text, nullable=True),
    Column("location_type", Integer, nullable=True),
    Column("wheelchair_boarding", Integer, nullable=True),
    Column("platform_code", Text, nullable=True),
    Column("geom", Geometry("POINT", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_routes = Table(
    "routes",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("route_id", Text, nullable=False),
    Column("agency_id", Text, nullable=True),
    Column("route_short_name", Text, nullable=True),
    Column("route_long_name", Text, nullable=True),
    Column("route_desc", Text, nullable=True),
    Column("route_type", Integer, nullable=True),
    Column("route_url", Text, nullable=True),
    Column("route_color", Text, nullable=True),
    Column("route_text_color", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_trips = Table(
    "trips",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("route_id", Text, nullable=False),
    Column("service_id", Text, nullable=False),
    Column("trip_id", Text, nullable=False),
    Column("trip_headsign", Text, nullable=True),
    Column("trip_short_name", Text, nullable=True),
    Column("direction_id", Integer, nullable=True),
    Column("block_id", Text, nullable=True),
    Column("shape_id", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_stop_times = Table(
    "stop_times",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("trip_id", Text, nullable=False),
    Column("arrival_seconds", Integer, nullable=True),
    Column("departure_seconds", Integer, nullable=True),
    Column("stop_id", Text, nullable=False),
    Column("stop_sequence", Integer, nullable=False),
    Column("pickup_type", Integer, nullable=True),
    Column("drop_off_type", Integer, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_calendar_services = Table(
    "calendar_services",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("service_id", Text, nullable=False),
    Column("monday", Integer, nullable=False),
    Column("tuesday", Integer, nullable=False),
    Column("wednesday", Integer, nullable=False),
    Column("thursday", Integer, nullable=False),
    Column("friday", Integer, nullable=False),
    Column("saturday", Integer, nullable=False),
    Column("sunday", Integer, nullable=False),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_calendar_dates = Table(
    "calendar_dates",
    metadata,
    Column("feed_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("service_id", Text, nullable=False),
    Column("service_date", Date, nullable=False),
    Column("exception_type", Integer, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_RAW_SCHEMA,
)

transit_reality_manifest = Table(
    "reality_manifest",
    metadata,
    Column("reality_fingerprint", Text, primary_key=True),
    Column("import_fingerprint", Text, nullable=False),
    Column("analysis_date", Date, nullable=False),
    Column("transit_config_hash", Text, nullable=False),
    Column("feed_fingerprints_json", JSONB, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    schema=TRANSIT_DERIVED_SCHEMA,
)

transit_service_classification = Table(
    "service_classification",
    metadata,
    Column("reality_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("service_id", Text, nullable=False),
    Column("school_only_state", Text, nullable=False),
    Column("route_ids_json", JSONB, nullable=False),
    Column("route_modes_json", JSONB, nullable=False),
    Column("reason_codes_json", JSONB, nullable=False),
    Column("time_bucket_counts_json", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_DERIVED_SCHEMA,
)

transit_gtfs_stop_service_summary = Table(
    "gtfs_stop_service_summary",
    metadata,
    Column("reality_fingerprint", Text, nullable=False),
    Column("feed_id", Text, nullable=False),
    Column("stop_id", Text, nullable=False),
    Column("public_departures_7d", Integer, nullable=False),
    Column("public_departures_30d", Integer, nullable=False),
    Column("school_only_departures_30d", Integer, nullable=False),
    Column("weekday_morning_peak_deps", Float, nullable=False),
    Column("weekday_evening_peak_deps", Float, nullable=False),
    Column("weekday_offpeak_deps", Float, nullable=False),
    Column("saturday_deps", Float, nullable=False),
    Column("sunday_deps", Float, nullable=False),
    Column("friday_evening_deps", Float, nullable=False),
    Column("transport_score_units", Integer, nullable=False),
    Column("bus_daytime_deps", Float, nullable=False),
    Column("bus_daytime_headway_min", Float, nullable=True),
    Column("bus_frequency_tier", Text, nullable=True),
    Column("bus_frequency_score_units", Integer, nullable=False),
    Column("last_public_service_date", Date, nullable=True),
    Column("last_any_service_date", Date, nullable=True),
    Column("bus_active_days_mask_7d", Text, nullable=True),
    Column("bus_service_subtier", Text, nullable=True),
    Column("is_unscheduled_stop", Boolean, nullable=False),
    Column("has_exception_only_service", Boolean, nullable=False),
    Column("has_any_bus_service", Boolean, nullable=False),
    Column("has_daily_bus_service", Boolean, nullable=False),
    Column("route_modes_json", JSONB, nullable=False),
    Column("route_ids_json", JSONB, nullable=False),
    Column("reason_codes_json", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_DERIVED_SCHEMA,
)

transit_gtfs_stop_reality = Table(
    "gtfs_stop_reality",
    metadata,
    Column("reality_fingerprint", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("source_ref", Text, nullable=False),
    Column("stop_name", Text, nullable=True),
    Column("feed_id", Text, nullable=False),
    Column("stop_id", Text, nullable=False),
    Column("source_status", Text, nullable=False),
    Column("reality_status", Text, nullable=False),
    Column("school_only_state", Text, nullable=False),
    Column("public_departures_7d", Integer, nullable=False),
    Column("public_departures_30d", Integer, nullable=False),
    Column("school_only_departures_30d", Integer, nullable=False),
    Column("weekday_morning_peak_deps", Float, nullable=False),
    Column("weekday_evening_peak_deps", Float, nullable=False),
    Column("weekday_offpeak_deps", Float, nullable=False),
    Column("saturday_deps", Float, nullable=False),
    Column("sunday_deps", Float, nullable=False),
    Column("friday_evening_deps", Float, nullable=False),
    Column("transport_score_units", Integer, nullable=False),
    Column("bus_daytime_deps", Float, nullable=False),
    Column("bus_daytime_headway_min", Float, nullable=True),
    Column("bus_frequency_tier", Text, nullable=True),
    Column("bus_frequency_score_units", Integer, nullable=False),
    Column("last_public_service_date", Date, nullable=True),
    Column("last_any_service_date", Date, nullable=True),
    Column("bus_active_days_mask_7d", Text, nullable=True),
    Column("bus_service_subtier", Text, nullable=True),
    Column("is_unscheduled_stop", Boolean, nullable=False),
    Column("has_exception_only_service", Boolean, nullable=False),
    Column("has_any_bus_service", Boolean, nullable=False),
    Column("has_daily_bus_service", Boolean, nullable=False),
    Column("route_modes_json", JSONB, nullable=False),
    Column("source_reason_codes_json", JSONB, nullable=False),
    Column("reality_reason_codes_json", JSONB, nullable=False),
    Column("geom", Geometry("POINT", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_DERIVED_SCHEMA,
)

transit_service_desert_cells = Table(
    "service_desert_cells",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("reality_fingerprint", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("resolution_m", Integer, nullable=False),
    Column("cell_id", Text, nullable=False),
    Column("analysis_date", Date, nullable=False),
    Column("baseline_reachable_stop_count", Integer, nullable=False),
    Column("reachable_public_departures_7d", Integer, nullable=False),
    Column("reason_codes_json", JSONB, nullable=False),
    Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=TRANSIT_DERIVED_SCHEMA,
)


noise_artifact_manifest = Table(
    "noise_artifact_manifest",
    metadata,
    Column("artifact_hash", Text, primary_key=True),
    Column("artifact_type", Text, nullable=False),
    Column("manifest_json", JSONB, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
)

noise_artifact_lineage = Table(
    "noise_artifact_lineage",
    metadata,
    Column("artifact_hash", Text, nullable=False),
    Column("parent_hash", Text, nullable=False),
)

noise_active_artifact = Table(
    "noise_active_artifact",
    metadata,
    Column("artifact_type", Text, primary_key=True),
    Column("artifact_hash", Text, nullable=False),
)

# Canonical processing tables — EPSG:2157 geometry (metre-based)
noise_normalized = Table(
    "noise_normalized",
    metadata,
    Column("noise_source_hash", Text, nullable=False),
    Column("jurisdiction", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("round_number", Integer, nullable=False),
    Column("report_period", Text, nullable=True),
    Column("db_low", Float, nullable=True),
    Column("db_high", Float, nullable=True),
    Column("db_value", Text, nullable=False),
    Column("source_dataset", Text, nullable=False),
    Column("source_layer", Text, nullable=False),
    Column("source_ref", Text, nullable=True),
    Column("geom", Geometry("MULTIPOLYGON", srid=2157), nullable=False),
)

noise_resolved_display = Table(
    "noise_resolved_display",
    metadata,
    Column("noise_resolved_hash", Text, nullable=False),
    Column("noise_feature_id", BigInteger, primary_key=True),
    Column("jurisdiction", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("round_number", Integer, nullable=False),
    Column("report_period", Text, nullable=True),
    Column("db_low", Float, nullable=True),
    Column("db_high", Float, nullable=True),
    Column("db_value", Text, nullable=False),
    Column("geom", Geometry("MULTIPOLYGON", srid=2157), nullable=False),
)

noise_resolved_provenance = Table(
    "noise_resolved_provenance",
    metadata,
    Column("noise_resolved_hash", Text, nullable=False),
    Column("jurisdiction", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("round_number", Integer, nullable=False),
    Column("source_dataset", Text, nullable=False),
    Column("source_layer", Text, nullable=False),
    Column("source_ref_count", Integer, nullable=False),
    Column("source_refs_hash", Text, nullable=False),
)

noise_grid_artifact = Table(
    "noise_grid_artifact",
    metadata,
    Column("artifact_hash", Text, nullable=False),
    Column("noise_source_hash", Text, nullable=False),
    Column("jurisdiction", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("grid_size_m", Integer, nullable=False),
    Column("cell_x", Integer, nullable=False),
    Column("cell_y", Integer, nullable=False),
    Column("round_number", Integer, nullable=False),
    Column("report_period", Text, nullable=True),
    Column("db_low", Float, nullable=True),
    Column("db_high", Float, nullable=True),
    Column("db_value", Text, nullable=False),
    Column("geom", Geometry("MULTIPOLYGON", srid=2157), nullable=False),
)


REQUIRED_PUBLIC_TABLES = {
    "grid_walk",
    "amenities",
    "transport_reality",
    "service_deserts",
    "noise_polygons",
    "build_manifest",
    "noise_artifact_manifest",
    "noise_artifact_lineage",
    "noise_active_artifact",
    "noise_normalized",
    "noise_resolved_display",
    "noise_resolved_provenance",
    "noise_grid_artifact",
}

REQUIRED_RAW_TABLES = {
    "import_manifest",
}

REQUIRED_TRANSIT_RAW_TABLES = {
    "feed_manifest",
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar_services",
    "calendar_dates",
}

REQUIRED_TRANSIT_DERIVED_TABLES = {
    "reality_manifest",
    "service_classification",
    "gtfs_stop_service_summary",
    "gtfs_stop_reality",
    "service_desert_cells",
}

REQUIRED_MANAGED_SCHEMA_TABLES = (
    ("public", REQUIRED_PUBLIC_TABLES),
    (OSM_IMPORT_SCHEMA, REQUIRED_RAW_TABLES),
    (TRANSIT_RAW_SCHEMA, REQUIRED_TRANSIT_RAW_TABLES),
    (TRANSIT_DERIVED_SCHEMA, REQUIRED_TRANSIT_DERIVED_TABLES),
)

OPTIONAL_IMPORTED_TABLES: set[str] = {"features"}

MANAGED_RAW_SUPPORT_TABLES = (
    import_manifest,
    transit_feed_manifest,
    transit_stops,
    transit_routes,
    transit_trips,
    transit_stop_times,
    transit_calendar_services,
    transit_calendar_dates,
    transit_reality_manifest,
    transit_service_classification,
    transit_gtfs_stop_service_summary,
    transit_gtfs_stop_reality,
    transit_service_desert_cells,
)

IMPORTER_OWNED_RAW_TABLES = ("features",)

GEOMETRY_FIELDS = {
    _table_key(grid_walk): ("centre_geom", "cell_geom"),
    _table_key(amenities): ("geom",),
    _table_key(transport_reality): ("geom",),
    _table_key(service_deserts): ("cell_geom",),
    _table_key(noise_polygons): ("geom",),
    _table_key(features): ("geom",),
    _table_key(transit_stops): ("geom",),
    _table_key(transit_gtfs_stop_reality): ("geom",),
    _table_key(transit_service_desert_cells): ("cell_geom",),
    _table_key(noise_normalized): ("geom",),
    _table_key(noise_resolved_display): ("geom",),
    _table_key(noise_grid_artifact): ("geom",),
}
