CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS osm_raw;

CREATE TABLE IF NOT EXISTS grid_walk (
    build_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    import_fingerprint TEXT NOT NULL,
    resolution_m INTEGER NOT NULL,
    cell_id TEXT NOT NULL,
    centre_geom GEOMETRY(Point, 4326) NOT NULL,
    cell_geom GEOMETRY(Geometry, 4326) NOT NULL,
    counts_json JSONB NOT NULL,
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

CREATE INDEX IF NOT EXISTS osm_raw_import_manifest_path_idx
    ON osm_raw.import_manifest (extract_path, completed_at DESC);

CREATE INDEX IF NOT EXISTS osm_raw_features_import_category_idx
    ON osm_raw.features (import_fingerprint, category);

CREATE INDEX IF NOT EXISTS osm_raw_features_geom_gist
    ON osm_raw.features USING GIST (geom);
