-- Convenience bootstrap snapshot only.
-- Alembic migrations under db_postgis/migrations are the canonical managed-schema source of truth.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS osm_raw;
CREATE SCHEMA IF NOT EXISTS transit_raw;
CREATE SCHEMA IF NOT EXISTS transit_derived;

CREATE TABLE IF NOT EXISTS grid_walk (
    build_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    resolution_m INTEGER NOT NULL,
    cell_id TEXT NOT NULL,
    centre_geom GEOMETRY(Point, 4326) NOT NULL,
    cell_geom GEOMETRY(Geometry, 4326) NOT NULL,
    effective_area_m2 DOUBLE PRECISION NOT NULL,
    effective_area_ratio DOUBLE PRECISION NOT NULL,
    counts_json JSONB NOT NULL,
    cluster_counts_json JSONB NOT NULL,
    effective_units_json JSONB NOT NULL,
    scores_json JSONB NOT NULL,
    total_score DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS amenities (
    build_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    category TEXT NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    source TEXT NOT NULL,
    source_ref TEXT NULL,
    name TEXT NULL,
    conflict_class TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transport_reality (
    build_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    osm_source_ref TEXT NOT NULL,
    osm_name TEXT NULL,
    reality_status TEXT NOT NULL,
    match_status TEXT NOT NULL,
    school_only_state TEXT NOT NULL,
    matched_feed_id TEXT NULL,
    matched_stop_id TEXT NULL,
    match_confidence DOUBLE PRECISION NOT NULL,
    reality_confidence DOUBLE PRECISION NOT NULL,
    public_departures_7d INTEGER NOT NULL,
    public_departures_30d INTEGER NOT NULL,
    selected_public_departures_30d INTEGER NULL,
    school_only_departures_30d INTEGER NOT NULL,
    last_public_service_date DATE NULL,
    last_any_service_date DATE NULL,
    route_modes_json JSONB NOT NULL,
    match_reason_codes_json JSONB NOT NULL,
    reality_reason_codes_json JSONB NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS service_deserts (
    build_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    resolution_m INTEGER NOT NULL,
    cell_id TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    nominal_reachable_stop_count INTEGER NOT NULL,
    reachable_public_departures_7d INTEGER NOT NULL,
    reason_codes_json JSONB NOT NULL,
    cell_geom GEOMETRY(Geometry, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

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
);

CREATE TABLE IF NOT EXISTS build_manifest (
    build_key TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    extract_path TEXT NOT NULL,
    geo_hash TEXT NOT NULL,
    reach_hash TEXT NOT NULL,
    score_hash TEXT NOT NULL,
    render_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    python_version TEXT NOT NULL,
    packages_json JSONB NOT NULL,
    summary_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS osm_raw.import_manifest (
    import_fingerprint TEXT PRIMARY KEY,
    extract_path TEXT NOT NULL,
    extract_fingerprint TEXT NOT NULL,
    importer_version TEXT NOT NULL,
    importer_config_hash TEXT NOT NULL,
    normalization_scope_hash TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS osm_raw.features (
    import_fingerprint TEXT NOT NULL,
    osm_type TEXT NOT NULL,
    osm_id BIGINT NOT NULL,
    category TEXT NOT NULL,
    name TEXT NULL,
    tags_json JSONB NOT NULL,
    geom GEOMETRY(Geometry, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.feed_manifest (
    feed_fingerprint TEXT PRIMARY KEY,
    feed_id TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    source_path TEXT NOT NULL,
    source_url TEXT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.stops (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_code TEXT NULL,
    stop_name TEXT NOT NULL,
    stop_desc TEXT NULL,
    stop_lat DOUBLE PRECISION NOT NULL,
    stop_lon DOUBLE PRECISION NOT NULL,
    parent_station TEXT NULL,
    zone_id TEXT NULL,
    location_type INTEGER NULL,
    wheelchair_boarding INTEGER NULL,
    platform_code TEXT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.routes (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    agency_id TEXT NULL,
    route_short_name TEXT NULL,
    route_long_name TEXT NULL,
    route_desc TEXT NULL,
    route_type INTEGER NULL,
    route_url TEXT NULL,
    route_color TEXT NULL,
    route_text_color TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.trips (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    trip_headsign TEXT NULL,
    trip_short_name TEXT NULL,
    direction_id INTEGER NULL,
    block_id TEXT NULL,
    shape_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.stop_times (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    arrival_seconds INTEGER NULL,
    departure_seconds INTEGER NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    pickup_type INTEGER NULL,
    drop_off_type INTEGER NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.calendar_services (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_raw.calendar_dates (
    feed_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    service_date DATE NOT NULL,
    exception_type INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.reality_manifest (
    reality_fingerprint TEXT PRIMARY KEY,
    import_fingerprint TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    transit_config_hash TEXT NOT NULL,
    feed_fingerprints_json JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.service_classification (
    reality_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    school_only_state TEXT NOT NULL,
    route_ids_json JSONB NOT NULL,
    route_modes_json JSONB NOT NULL,
    reason_codes_json JSONB NOT NULL,
    time_bucket_counts_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.gtfs_stop_service_summary (
    reality_fingerprint TEXT NOT NULL,
    feed_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    public_departures_7d INTEGER NOT NULL,
    public_departures_30d INTEGER NOT NULL,
    school_only_departures_30d INTEGER NOT NULL,
    last_public_service_date DATE NULL,
    last_any_service_date DATE NULL,
    route_modes_json JSONB NOT NULL,
    route_ids_json JSONB NOT NULL,
    reason_codes_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.stop_matches (
    reality_fingerprint TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    osm_source_ref TEXT NOT NULL,
    gtfs_feed_id TEXT NOT NULL,
    gtfs_stop_id TEXT NOT NULL,
    candidate_rank INTEGER NOT NULL,
    distance_m DOUBLE PRECISION NOT NULL,
    name_similarity DOUBLE PRECISION NOT NULL,
    match_confidence DOUBLE PRECISION NOT NULL,
    match_status TEXT NOT NULL,
    match_reason_codes_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.osm_stop_reality (
    reality_fingerprint TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    osm_source_ref TEXT NOT NULL,
    osm_name TEXT NULL,
    osm_category TEXT NOT NULL,
    matched_feed_id TEXT NULL,
    matched_stop_id TEXT NULL,
    match_status TEXT NOT NULL,
    reality_status TEXT NOT NULL,
    school_only_state TEXT NOT NULL,
    match_confidence DOUBLE PRECISION NOT NULL,
    reality_confidence DOUBLE PRECISION NOT NULL,
    public_departures_7d INTEGER NOT NULL,
    public_departures_30d INTEGER NOT NULL,
    selected_public_departures_30d INTEGER NULL,
    school_only_departures_30d INTEGER NOT NULL,
    last_public_service_date DATE NULL,
    last_any_service_date DATE NULL,
    route_modes_json JSONB NOT NULL,
    match_reason_codes_json JSONB NOT NULL,
    reality_reason_codes_json JSONB NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transit_derived.service_desert_cells (
    build_key TEXT NOT NULL,
    reality_fingerprint TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    resolution_m INTEGER NOT NULL,
    cell_id TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    nominal_reachable_stop_count INTEGER NOT NULL,
    reachable_public_departures_7d INTEGER NOT NULL,
    reason_codes_json JSONB NOT NULL,
    cell_geom GEOMETRY(Geometry, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS grid_walk_build_resolution_cell_idx
    ON grid_walk (build_key, resolution_m, cell_id);

CREATE INDEX IF NOT EXISTS grid_walk_build_resolution_idx
    ON grid_walk (build_key, resolution_m);

CREATE INDEX IF NOT EXISTS grid_walk_config_resolution_idx
    ON grid_walk (config_hash, resolution_m);

CREATE INDEX IF NOT EXISTS amenities_build_category_idx
    ON amenities (build_key, category);

CREATE INDEX IF NOT EXISTS amenities_config_category_idx
    ON amenities (config_hash, category);

CREATE INDEX IF NOT EXISTS build_manifest_status_idx
    ON build_manifest (status);

CREATE INDEX IF NOT EXISTS build_manifest_extract_config_idx
    ON build_manifest (extract_path, config_hash, completed_at DESC);

CREATE INDEX IF NOT EXISTS build_manifest_import_idx
    ON build_manifest (import_fingerprint);

CREATE INDEX IF NOT EXISTS grid_walk_centre_geom_gist
    ON grid_walk USING GIST (centre_geom);

CREATE INDEX IF NOT EXISTS grid_walk_cell_geom_gist
    ON grid_walk USING GIST (cell_geom);

CREATE INDEX IF NOT EXISTS amenities_geom_gist
    ON amenities USING GIST (geom);

CREATE UNIQUE INDEX IF NOT EXISTS transport_reality_build_source_idx
    ON transport_reality (build_key, osm_source_ref);

CREATE INDEX IF NOT EXISTS transport_reality_status_idx
    ON transport_reality (build_key, reality_status);

CREATE INDEX IF NOT EXISTS transport_reality_geom_gist
    ON transport_reality USING GIST (geom);

CREATE UNIQUE INDEX IF NOT EXISTS service_deserts_build_resolution_cell_idx
    ON service_deserts (build_key, resolution_m, cell_id);

CREATE INDEX IF NOT EXISTS service_deserts_geom_gist
    ON service_deserts USING GIST (cell_geom);

CREATE INDEX IF NOT EXISTS noise_polygons_build_metric_idx
    ON noise_polygons (build_key, metric);

CREATE INDEX IF NOT EXISTS noise_polygons_source_metric_idx
    ON noise_polygons (build_key, source_type, metric, db_value);

CREATE INDEX IF NOT EXISTS noise_polygons_geom_gist
    ON noise_polygons USING GIST (geom);

CREATE INDEX IF NOT EXISTS osm_raw_import_manifest_path_idx
    ON osm_raw.import_manifest (extract_path, completed_at DESC);

CREATE INDEX IF NOT EXISTS osm_raw_features_import_category_idx
    ON osm_raw.features (import_fingerprint, category);

CREATE INDEX IF NOT EXISTS osm_raw_features_geom_gist
    ON osm_raw.features USING GIST (geom);

CREATE INDEX IF NOT EXISTS transit_raw_feed_manifest_feed_idx
    ON transit_raw.feed_manifest (feed_id, analysis_date);

CREATE INDEX IF NOT EXISTS transit_raw_stops_feed_stop_idx
    ON transit_raw.stops (feed_id, stop_id);

CREATE INDEX IF NOT EXISTS transit_raw_stops_geom_gist
    ON transit_raw.stops USING GIST (geom);

CREATE INDEX IF NOT EXISTS transit_raw_routes_feed_route_idx
    ON transit_raw.routes (feed_id, route_id);

CREATE INDEX IF NOT EXISTS transit_raw_trips_feed_trip_idx
    ON transit_raw.trips (feed_id, trip_id);

CREATE INDEX IF NOT EXISTS transit_raw_trips_feed_service_idx
    ON transit_raw.trips (feed_id, service_id);

CREATE INDEX IF NOT EXISTS transit_raw_stop_times_feed_trip_seq_idx
    ON transit_raw.stop_times (feed_id, trip_id, stop_sequence);

CREATE INDEX IF NOT EXISTS transit_raw_stop_times_feed_stop_idx
    ON transit_raw.stop_times (feed_id, stop_id);

CREATE INDEX IF NOT EXISTS transit_raw_calendar_services_feed_service_idx
    ON transit_raw.calendar_services (feed_id, service_id);

CREATE INDEX IF NOT EXISTS transit_raw_calendar_dates_feed_service_date_idx
    ON transit_raw.calendar_dates (feed_id, service_id, service_date);

CREATE INDEX IF NOT EXISTS transit_derived_reality_manifest_import_idx
    ON transit_derived.reality_manifest (import_fingerprint, analysis_date);

CREATE INDEX IF NOT EXISTS transit_derived_service_classification_reality_service_idx
    ON transit_derived.service_classification (reality_fingerprint, feed_id, service_id);

CREATE INDEX IF NOT EXISTS transit_derived_gtfs_stop_service_summary_reality_stop_idx
    ON transit_derived.gtfs_stop_service_summary (reality_fingerprint, feed_id, stop_id);

CREATE INDEX IF NOT EXISTS transit_derived_stop_matches_reality_osm_rank_idx
    ON transit_derived.stop_matches (reality_fingerprint, osm_source_ref, candidate_rank);

CREATE INDEX IF NOT EXISTS transit_derived_stop_matches_reality_gtfs_idx
    ON transit_derived.stop_matches (reality_fingerprint, gtfs_feed_id, gtfs_stop_id);

CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_reality_osm_idx
    ON transit_derived.osm_stop_reality (reality_fingerprint, osm_source_ref);

CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_status_idx
    ON transit_derived.osm_stop_reality (reality_fingerprint, reality_status);

CREATE INDEX IF NOT EXISTS transit_derived_osm_stop_reality_geom_gist
    ON transit_derived.osm_stop_reality USING GIST (geom);

CREATE INDEX IF NOT EXISTS transit_derived_service_desert_cells_build_resolution_cell_idx
    ON transit_derived.service_desert_cells (build_key, resolution_m, cell_id);

CREATE INDEX IF NOT EXISTS transit_derived_service_desert_cells_geom_gist
    ON transit_derived.service_desert_cells USING GIST (cell_geom);
