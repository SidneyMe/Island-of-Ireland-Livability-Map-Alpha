# Repo Map

> Refreshed: 2026-04-28. Evidence grades: **Confirmed** = read directly from code; **Inference** = strongly suggested but not explicitly proven; **Unclear** = cannot be determined from repo alone.

---

## 1. Project Purpose

- Offline-first pipeline that scores grid cells across the island of Ireland for livability using walk access to `shops`, `transport`, `healthcare`, and `parks`. (Confirmed)
- Shops, healthcare, and parks now use tiered score units instead of flat presence counts. Examples: corner shop vs supermarket, clinic vs emergency hospital, pocket park vs regional park. (Confirmed)
- Ingests local OSM PBF via `osm2pgsql`, GTFS feeds (NTA, Translink), and optionally an Overture Places geoparquet dataset. (Confirmed)
- Runs heavy work ahead of time: geometry prep -> amenity load/merge -> Rust walkgraph build -> igraph reachability -> grid scoring -> PMTiles bake. (Confirmed)
- Publishes results to PostGIS plus a PMTiles archive so the frontend can run without live tile SQL queries. (Confirmed)
- Builds a GTFS-first transit reality layer, bus daytime frequency tiers, frequency-weighted transport scoring, and a service-desert overlay from scheduled departures, not from OSM stop tags alone. (Confirmed)
- Adds a display-only official environmental-noise overlay from ROI EPA and NI OpenDataNI rounds using Lden / Lnight contours, with newest-round polygons masking older fallback geometry. This does not feed livability scoring yet. (Confirmed)
- Uses layered content hashes so changes to geometry, scoring params, GTFS feeds, Overture data, or importer config only invalidate the affected cache tiers. (Confirmed)
- Alpha-stage: amenity tiering, Overture merge, service deserts, and the new fine vector grid / inspect-backed surface path are still moving. (Inference from recent migrations, tests, and docs)

---

## 2. High-Level Architecture

- **CLI dispatcher**: `main.py`
- **Config and hash spine**: `config.py`
- **Schema and DB IO**: `db_postgis/`
- **OSM ingest**: `local_osm_import/` + `osm2pgsql_livability.lua`
- **GTFS ingest and transit reality**: `transit/`
- **Overture integration and dedupe**: `overture/loader.py`, `overture/merge.py`, `db_postgis/amenity_merge.py`
- **Noise overlay ingestion**: `noise/loader.py`
- **Noise artifact source ingest modes**: `noise_artifacts/ingest.py`, `noise_artifacts/ogr_ingest.py`
- **Amenity tier classifier**: `precompute/amenity_tiers.py`
- **Amenity clustering for Phase 2 variety scoring**: `precompute/amenity_clusters.py`
- **Precompute pipeline orchestration**: `precompute/__init__.py`, `precompute/workflow.py`, `precompute/phases.py`
- **Lightweight GTFS refresh CLI path**: `transit_refresh_runner.py`
- **Pipeline ETA and timing history**: `progress_tracker.py`
- **Rust walkgraph binary**: `walkgraph/`
- **PMTiles bake**: `precompute/bake_pmtiles.py`, `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py`
- **Runtime HTTP server**: `serve_from_db.py`
- **Frontend source**: `frontend/src/`
- **Frontend grid diagnostics helper**: `frontend/src/grid_debug.js`
- **Served frontend bundle**: `static/dist/`
- **Human docs / design notes**: `README.md`, `docs/*.md`

---

## 3. Entry Points

### `main.py`

- Primary CLI entry point. (Confirmed, LOC: 153)
- Dispatches:
  - `--refresh-import` -> `precompute.refresh_local_import()`
  - `--refresh-transit` -> `transit_refresh_runner.refresh_transit()`
  - `--force-transit-refresh` -> same path, but only valid with `--refresh-transit`
  - `--precompute` / `--precompute-dev` / `--precompute-test` -> `precompute.run_precompute(profile="full"|"dev"|"test")`
  - `--force-precompute` -> only valid with `--precompute` / `--precompute-dev` / `--precompute-test`
  - `--refresh-noise-artifact` -> refresh artifact during precompute when missing/stale
  - `--force-noise-artifact` -> force resolved artifact rebuild while reusing existing source rows
  - `--reimport-noise-source` -> force raw source re-import into `noise_normalized`
  - `--force-noise-all` -> force both source re-import and resolved rebuild
  - `--auto-refresh-import` -> only valid with `--precompute` / `--precompute-dev` / `--precompute-test`
  - `--serve` / `--render` / `--serve-dev` / `--render-dev` / `--serve-test` / `--render-test` -> `render_from_db.run_render_from_db(...)`
- `--render` is a legacy alias for `--serve`. `--render-dev` is a legacy alias for `--serve-dev`. `--render-test` is a legacy alias for `--serve-test`. (Confirmed)
- If no action flag is supplied, serving is the default path. (Confirmed)
- `--refresh-transit` now goes through a lightweight transit-only runner instead of importing the full `precompute` package first, emits tracker lines before DB/schema and source-state preflight, and reuses a cached OSM extract fingerprint when the local `.osm.pbf` path/size/mtime are unchanged. It still prints the explicit completion line plus the transit-phase `completed` tracker line. (Confirmed)

### `.github/workflows/scheduled_refresh.yml`

- Weekly self-hosted workflow. (Confirmed)
- Runs:
  - `python scripts/refresh_osm.py`
  - `python main.py --refresh-transit`
  - `python main.py --precompute --auto-refresh-import`
  - `python scripts/sanity_check.py --profile full`

### `.github/workflows/ci.yml`

- Push / PR validation workflow. (Confirmed)
- Runs:
  - `python -m unittest discover -s tests -t . -p "test_*.py"`
  - `cargo test --manifest-path walkgraph/Cargo.toml`
  - `python scripts/sanity_check.py --validate-only`

### `scripts/sanity_check.py`

- Standalone validation CLI used by CI and scheduled refresh. (Confirmed, LOC: 349)
- `--validate-only` checks fixture structure only.
- Normal mode resolves the active completed build, then reads scores through fine-surface runtime lookups when enabled, otherwise through `grid_walk` point lookups. (Confirmed from file and tests)

---

## 4. End-to-End Flow

```text
1. Local inputs
   osm/ireland-and-northern-ireland-latest.osm.pbf
   gtfs/*.zip
   overture/ireland_places.geoparquet (optional)
   noise_datasets/*.zip (optional display overlay)
   boundaries/*.geojson

2. Raw import
   python main.py --refresh-import
   -> local_osm_import/
   -> osm_raw.features + osm_raw.import_manifest

3. Transit reality
   python main.py --refresh-transit
   -> transit/workflow.py
   -> walkgraph gtfs-refresh
   -> transit_raw.* + transit_derived.*

4. Precompute
   python main.py --precompute
   -> precompute/__init__.run_precompute()
   -> precompute/workflow.run_precompute_impl()
   -> geometry
   -> amenities (+ Overture merge + amenity tier annotation)
   -> networks
   -> reachability
   -> scoring / grids

5. Publish
   -> grid_walk
   -> amenities
   -> transport_reality
   -> service_deserts
   -> noise_polygons
   -> build_manifest

6. PMTiles bake
   -> precompute/bake_pmtiles.py
   -> pmtiles_bake_worker.py + fine_vector_pmtiles_worker.py subprocesses
   -> coarse SQL MVT through z11, sparse fine vector grid at z12-z15
   -> noise MVT layer from noise_polygons, with noise-derived high-zoom tile coords included even when no point layer is present
   -> PMTiles archive source max zoom capped at 15
   -> bounded in-flight worker queue, fine-grid worker cap of 4, retry-smaller-on-pool-failure, temp output staging that preserves the previous archive on failure
   -> .livability_cache/livability[(-dev|-test)].pmtiles

7. Runtime serve
   python main.py --serve
   -> render_from_db.run_render_from_db()
   -> serve_from_db.serve_livability_app()
   Endpoints:
     GET /api/runtime
     GET /api/inspect
     GET /tiles/livability.pmtiles
     GET /tiles/livability-test.pmtiles
     GET /tiles/surface/{resolution}/{z}/{x}/{y}.png
     GET /exports/transport-reality.zip

8. Frontend
   static/dist/app.js
  -> reads PMTiles through pmtiles://
  -> reads runtime JSON from /api/runtime
  -> renders one active vector grid fill+outline pair, recreates those layers when the zoom band changes, and overzooms z15 source tiles to z19
  -> exposes a default-off Noise panel with Lden / Lnight, source-type, and dB-band filters backed by the `noise` PMTiles source-layer
  -> the fixed control panel now scrolls internally when its contents exceed the viewport height, so stacked debug + amenity controls stay reachable
  -> the transport panel now presents public transport tiers: base `calendar.txt` weekly bus-pattern filters (`Whole week`, `Mon-Sat`, `Tue-Sun`, `Weekdays only`, `Weekends only`, `Single-day only`, `Partial week`, `Unscheduled`), bus frequency tier filters (`Frequent`, `Moderate`, `Low frequency`, `Very low frequency`, `Token / skeletal`), GTFS mode filters (`Tram`, `Rail`), and a strict `calendar_dates`-only intersection filter; tram/rail-only popups show their mode tier instead of a missing bus tier; popups also expose bus headway, commute, Friday-evening, and score-unit frequency fields
  -> `/?debug-grid=1` now opt-in reveals a persistent control-panel `Grid debug` card with live source-vs-rendered counts, layer/source state, a diagnosis line, and a copyable plain-text snapshot; the status pill is reserved for actual runtime errors
  -> clicking transport rows now renders all colocated stop rows in one popup instead of taking only the first rendered feature, while clicking the active grid still opens the exact score breakdown popup and uses `/api/inspect` for fine-surface values when available
```

Notes:

- Service deserts are computed from reachable baseline GTFS stops and public departures in the configured desert window. (Confirmed)
- Fine surface exists for `full` and `test`. `dev` is coarse-only. (Confirmed)
- `test` uses the same full-resolution ladder as `full` but clips phase-1 study area loading to a compact Cork city bbox `(-8.55, 51.87, -8.41, 51.93)` in WGS84, so caches, imports, manifests, and PMTiles stay isolated from the island-wide build. (Confirmed)

---

## 5. Source-of-Truth Map

| Domain | Canonical | Runtime mirror / consumer | Snapshot / wrapper |
|---|---|---|---|
| Config and hash chain | `config.py` | Imported almost everywhere | None |
| Schema history | `db_postgis/migrations/versions/` | `db_postgis/tables.py` | `schema.sql` |
| OSM ingest rules | `osm2pgsql_livability.lua`, `local_osm_import/` | `config.IMPORTER_CONFIG_VERSION` | None |
| Transit reality | `transit/workflow.py`, `transit/rust_gtfs.py` | `config.transit_config_hash()` | None |
| Amenity tiering | `config.py` tier constants, `precompute/amenity_tiers.py` | `precompute/phases.py`, `precompute/publish.py` | None |
| Overture category mapping | `overture/loader.py::OVERTURE_CATEGORY_MAP` | `precompute/phases.py` | None |
| Overture merge logic | `overture/merge.py`, `db_postgis/amenity_merge.py` | `precompute/phases.py` | None |
| Noise overlay source handling | `noise/loader.py` | `noise_polygons`, `noise` PMTiles layer, `/api/runtime` noise counts | `noise_datasets/*.zip` local inputs |
| Precompute orchestration | `precompute/workflow.py` | `precompute/__init__.py` | None |
| PMTiles layer metadata | `precompute/bake_pmtiles.py` | `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py` | None |
| Runtime API contract | `serve_from_db.RuntimeState` | `frontend/src/runtime_contract.js`, `frontend/src/main.js` | `render_from_db.py` |
| Frontend source | `frontend/src/` | `static/dist/` after build | `static/dist/*` |
| Progress / ETA behavior | `progress_tracker.py` | `precompute/workflow.py`, `precompute/__init__.py` | `.livability_cache/precompute_timing_stats.json` |
| Product / methodology docs | `README.md`, `docs/*.md` | Humans only | None |

---

## 6. Key Modules

### `config.py`

- Purpose: master config, scoring params, path defaults, build profiles, and the entire invalidation spine. (Confirmed)
- Why it matters: `HASHES = build_config_hashes()` runs at import time. Missing files can silently change hashes by contributing zeroed metadata instead of raising. (Confirmed)
- Important constants:
  - `CAPS = {"shops": 6, "transport": 5, "healthcare": 5, "parks": 5}`
  - `SHOP_TIER_UNITS = {"corner": 1, "regular": 2, "supermarket": 3, "mall": 5}`
  - `HEALTHCARE_TIER_UNITS = {"local": 1, "clinic": 2, "hospital": 3, "emergency_hospital": 4}`
  - `PARK_TIER_UNITS = {"pocket": 1, "neighbourhood": 2, "district": 3, "regional": 4}`
  - `VARIETY_CLUSTER_RADIUS_M = 25.0`
  - `DISTANCE_DECAY_HALF_DISTANCE_M = {"shops": 150.0, "transport": 250.0, "healthcare": 300.0, "parks": 350.0}`
  - `TRANSIT_REALITY_ALGO_VERSION = 8`
  - `AMENITY_MERGE_ALGO_VERSION = 4`
  - `PMTILES_SCHEMA_VERSION = 9`
  - `GRID_GEOMETRY_SCHEMA_VERSION = 4`
  - `CACHE_SCHEMA_VERSION = 12` (unchanged by display-only noise)
  - `WALKGRAPH_FORMAT_VERSION = 3`
  - `IMPORTER_CONFIG_VERSION = "2026-04-08"`
- Build profiles:
  - `full`: vector grid `20000/10000/5000/2500/1000/500/250/100/50` baked into PMTiles, with archive source zoom capped at `15` and frontend overzoom to `19`
  - `dev`: coarse vector only
  - `test`: same resolution ladder as `full`, but with `study_area_kind="bbox"` and `study_area_bbox_wgs84=(-8.55, 51.87, -8.41, 51.93)` so the pipeline runs against compact Cork city only and writes `livability-test.pmtiles`
- Render hash includes a noise dataset signature and file-size/mtime metadata from `noise_datasets/*.zip`; score/cache hashes stay independent because noise is display-only. (Confirmed)
- LOC: 1227

### `noise/loader.py`

- Purpose: official ROI / NI environmental-noise loader for display-only polygons. (Confirmed)
- Reads ROI `NOISE_Round4/3/2` and NI `end_noisedata_round3/2/1`; ROI Round 4 road is FileGDB and requires `pyogrio`/GDAL support so the newest road layer is used instead of silently falling back. (Confirmed)
- Normalizes ROI `Time` to `Lden` / `Lnight`, ROI dB fields across `Db_Low` / `dB_Low`, `Db_High` / `dB_High`, `DbValue` / `dB_Value`, and NI `gridcode` bands while excluding `1000` no-data. NI mapping is now round-aware: Round 1 class-coded `GRIDCODE` values map via explicit verified lookup (not `+1/+5` arithmetic), Round 2/3 threshold-style codes map through an explicit verified table, and unknown codes raise clear errors instead of silently producing synthetic bands. Candidate rows now carry `raw_gridcode` for downstream diagnostics. (Confirmed)
- Candidate cache path is now chunked and streaming (`.livability_cache/noise_candidates/<key>/manifest.json + part-*.pkl.gz`) instead of single giant list materialization; cache-hit and cache-miss paths both yield rows incrementally so ingest can validate/insert early. (Confirmed)
- Legacy single-file candidate cache reads are now opt-in via `NOISE_ALLOW_LEGACY_CANDIDATE_CACHE=1`; default behavior deletes legacy cache and rebuilds chunked cache. (Confirmed)
- Includes `python -m noise.loader --dump-ni-round1-classes [--data-dir ...]` diagnostics for Round 1 class-label verification snapshots. (Confirmed)
- Materializes effective polygons by `jurisdiction + source_type + metric`, processing rounds newest-to-oldest and subtracting already-covered newer geometry before older fallback pieces are published. (Confirmed)
- LOC: ~1800 (fast-growing module with ingest/cache + diagnostics helpers). (Confirmed)

### `precompute/__init__.py`

- Purpose: public precompute API surface plus `_BuildState` lifecycle and service-desert computation. (Confirmed)
- Why it matters: `_STATE = _BuildState.bootstrap()` runs at import time with placeholder fingerprints. Real hashes are not valid until `_STATE.activate(...)`. (Confirmed)
- Exposes: `run_precompute()`, `refresh_local_import()`, `refresh_transit()`
- Owns: `_compute_service_deserts()`
- LOC: 1052

### `transit_refresh_runner.py`

- Purpose: lightweight CLI-only GTFS refresh path used by `main.py --refresh-transit`. (Confirmed)
- Why it matters: avoids importing the whole `precompute` package before the first transit progress line, now starts the tracker before DB/schema checks and source-state resolution, and passes transit progress callbacks into OSM source-state fingerprinting so users can see `osm2pgsql --version` probes plus cached-vs-rehashed `.osm.pbf` resolution immediately in the console. (Confirmed)
- Main functions: `refresh_transit()`, `_preflight_transit_rebuild()`

### `precompute/workflow.py`

- Purpose: injected orchestration for import refresh and full precompute. (Confirmed)
- Why it matters: contains the short-circuit logic for reusing completed builds, rebaking PMTiles only, or rebuilding fine surface only. Also owns tracker injection and timing persistence. (Confirmed)
- Main functions: `run_import_refresh_impl()`, `run_precompute_impl()`
- LOC: 454

### `progress_tracker.py`

- Purpose: phase progress, ETA, and persisted timing history for long-running pipelines. (Confirmed)
- Main types: `PhaseState`, `PrecomputeProgressTracker`
- Writes: `.livability_cache/precompute_timing_stats.json`
- Important behavior: progress-tracking failures disable tracking instead of failing the build. Historical timings drive ETA quality. (Confirmed)
- LOC: 651

### `precompute/phases.py`

- Purpose: the concrete geometry, amenities, networks, reachability, and grid-scoring phase implementations. (Confirmed)
- Why it matters: this is the core scoring logic. It decides how amenity rows are annotated, how scoring-only amenity clusters are formed, and how walk reachability is converted into raw counts, cluster counts, and distance-decayed effective units. (Confirmed)
- Important details:
  - transport rows are kept separate from Overture merge
  - amenity rows are annotated with `tier` and `score_units`
  - Phase 2 builds scoring-only amenity clusters per category and chooses a deterministic representative row per cluster
  - reachability cache now stores `walk_counts_by_origin_node`, `walk_cluster_counts_by_origin_node`, and `walk_effective_units_by_origin_node`
  - large routing checkpoints may exist as a legacy full blob plus appended `.chunks` overlay frames; loaders merge both so interrupted runs can salvage and resume reachability work without dropping previously cached nodes
  - origin-node normalization/union now uses low-memory sorted lists rather than Python `set(sorted(...))` dedupe, so coarse-grid and fine-surface reachability inputs can be combined without materializing another huge Python-object set
- LOC: 917

### `precompute/amenity_clusters.py`

- Purpose: scoring-only clustering helper for Phase 2 variety handling. (Confirmed)
- Why it matters: one physical cluster contributes one category-specific scoring item while the original amenity rows remain intact for publishing and raw-count explainability. (Confirmed)
- Main function: `build_amenity_clusters()`
- Representative selection order: highest `score_units`, then named rows before unnamed rows, then stable source identity ordering. (Confirmed)

### `precompute/amenity_tiers.py`

- Purpose: canonical tier classifier for `shops`, `healthcare`, `parks`, and GTFS-frequency transport score units. (Confirmed)
- Main functions: `classify_amenity_row()`, `annotate_amenity_row()`, `uses_weighted_units()`
- Why it matters: this file decides the tier names stored on amenities and the score units consumed by reachability / scoring; transport now consumes GTFS-derived `transport_score_units` instead of flat stop presence. (Confirmed)
- LOC: 230

### `precompute/bake_pmtiles.py`

- Purpose: PMTiles bake orchestrator and layer metadata owner. (Confirmed)
- Why it matters: owns `GRID_AMENITY_CATEGORIES`, `_pmtiles_metadata()`, the z15 source-zoom cap, and the sparse fine-grid / noise tile-spec planner that stitches coarse SQL tiles together with fine vector grid tiles and noise-only high-zoom tiles. It now also bounds parallel in-flight work, clamps fine-grid bakes to 4 workers, retries once at half workers after `BrokenProcessPool`, and only replaces the final PMTiles archive after a successful temp-file finalize. Tests import these directly. (Confirmed)
- The amenities layer metadata declares `category`, `tier`, `name`, and `conflict_class`. (Confirmed from tests and code)
- The transport layer metadata now declares weekly bus subtier / mask fields, bus daytime frequency fields, comma-separated `route_modes`, numeric `0/1` transport flags, commute/off-peak/weekend/Friday-evening departure averages, and `transport_score_units`; the frontend uses `route_modes` for exact rail/tram filtering and `bus_frequency_tier` for bus-frequency filtering, and the worker SQL emits one feature per published `transport_reality` row instead of grouping same-name same-coordinate stops. (Confirmed from code and tests)
- The `grid` source-layer now carries both coarse and fine features, with fine rows padded with zero-valued popup numerics so metadata and popup consumers stay schema-stable. (Confirmed from code and tests)
- The `noise` source-layer declares jurisdiction, source type, metric, round/report period, dB band/value, source dataset/layer/ref fields, and starts at z8. (Confirmed from code and tests)
- LOC: 856

### `fine_vector_pmtiles_worker.py`

- Purpose: top-level Windows-safe worker for fine-grid vector tile generation from surface shard manifests. (Confirmed)
- Why it matters: keeps Windows `spawn` subprocesses out of `precompute/__init__.py`, reads shell/score shard `.npz` files directly, aggregates canonical `50m` scores into `2500/1000/500/250/100/50` cells, clips polygons with a tile-edge buffer so neighboring z15 overzoom tiles overlap cleanly, and encodes the `grid` source-layer MVT bytes. Candidate cell windows are expanded by one cell on each side before clipping, raw shell/score shards plus aggregated surfaces live in bounded LRU caches that are cleared after each worker chunk so parallel bake RAM does not grow without bound, and clipped rings are now dropped if they collapse below valid polygon shape after normalization or buffered clipping. (Confirmed)
- z12 emits `2500m`, z13 emits `1000m`, z14 emits `500m`, and z15 emits mixed `250m` + `100m` + `50m` features. (Confirmed from code and tests)
- LOC: 615

### `pmtiles_bake_worker.py`

- Purpose: minimal subprocess worker that runs PMTiles tile SQL inside spawned processes. (Confirmed)
- Why it matters: import isolation is a hard constraint on Windows spawn. Heavy imports here can blow memory for parallel bake workers. It now remains the coarse SQL worker while fine-grid geometry encoding lives in `fine_vector_pmtiles_worker.py`. (Confirmed from module docstring and code)
- Main functions: `_bake_chunk_worker()`, `_tile_mvt_bytes_by_flags()`, `_resolution_for_zoom()`
- `_resolution_for_zoom()` only handles coarse vector tiers: `5000`, `10000`, `20000`. It is not the same as `config.resolution_for_zoom()`. (Confirmed)
- Also emits the SQL-backed `noise` MVT source-layer; keep GIS readers out of this file so subprocess imports stay light. (Confirmed)
- LOC: 400

### `serve_from_db.py`

- Purpose: runtime HTTP server for static assets, PMTiles range reads, fine-surface PNG tiles, runtime JSON, inspect API, and transport-reality export download. (Confirmed)
- Why it matters: `RuntimeState` is the server-side truth for `/api/runtime`. (Confirmed)
- `RuntimeState` includes:
  - `build_key`, `build_profile`
  - `coarse_vector_resolutions`, `fine_resolutions`, `surface_zoom_breaks`
  - `amenity_counts`, `amenity_tier_counts`
  - `transport_subtier_counts`, `transport_bus_frequency_counts`, `transport_flag_counts`, `transport_mode_counts`
  - `noise_enabled`, `noise_counts`, `noise_source_counts`, `noise_metric_counts`, `noise_band_counts`
  - `fine_surface_enabled`
  - `surface_shell_dir`, `surface_score_dir`, `surface_tile_dir`
  - `transport_reality_enabled`, `service_deserts_enabled`
  - `transport_reality_download_url`
  - `transit_analysis_date`, `transit_analysis_window_days`, `transit_service_desert_window_days`
  - `overture_dataset`
- `/api/runtime` still reports `surface_zoom_breaks`, `fine_resolutions_m`, `fine_surface_enabled`, `inspect_url`, and `max_zoom=19`, but it no longer advertises `surface_tile_url_template`; the main render path is now vector-only. Expected client aborts on `/api/inspect` are suppressed from server logs the same way PMTiles range disconnects are, while `/` and `/static/*` now ship with `Cache-Control: no-store` so rebuilt local frontend assets are not silently cached between reloads. (Confirmed from code and tests)
- `/api/runtime` includes noise overlay availability and filter counts for jurisdiction, source type, metric, and dB band. (Confirmed from code and tests)
- LOC: 758

### `db_postgis/tables.py`

- Purpose: SQLAlchemy table definitions used by reads / writes at runtime. (Confirmed)
- Why it matters: must match Alembic head. (Confirmed)
- Current schema facts:
  - `amenities` has `category`, `tier`, `geom`, `source`, `source_ref`, `name`, `conflict_class`
  - `grid_walk` has `counts_json`, `cluster_counts_json`, `effective_units_json`, `scores_json`, `total_score`, clipped-area fields
  - `transit_derived.gtfs_stop_service_summary`, `transit_derived.gtfs_stop_reality`, and public `transport_reality` now also carry `bus_active_days_mask_7d` (legacy export name for the base weekly bus mask), `bus_service_subtier`, `bus_daytime_deps`, `bus_daytime_headway_min`, `bus_frequency_tier`, `bus_frequency_score_units`, `is_unscheduled_stop`, `has_exception_only_service`, `has_any_bus_service`, `has_daily_bus_service`, `route_modes_json`, commute/off-peak/weekend/Friday-evening departure averages, and `transport_score_units`
  - public output tables are `grid_walk`, `amenities`, `transport_reality`, `service_deserts`, `build_manifest`
  - public `noise_polygons` stores build-scoped official noise geometry with jurisdiction, source type, metric, round, report period, dB band/value, source metadata, and 4326 geometry
- LOC: 497

### `db_postgis/migrations/versions/`

- Purpose: canonical schema history. (Confirmed)
- Latest migration: `20260424_000012_noise_polygons.py`
- Previous notable migration: `20260423_000010_transport_frequency_scoring.py`

### `overture/loader.py`

- Purpose: Overture Places loader, category mapper, and dataset signature provider. (Confirmed)
- Why it matters: Overture dataset signature feeds the config hash chain. (Confirmed)
- Current dedicated test: `tests/test_overture_loader.py` confirms gardens are excluded while real park-like categories survive mapping. (Confirmed)
- LOC: 256

### `overture/merge.py`

- Purpose: OSM/Overture dedupe and OSM self-dedupe using distance and normalized names / aliases. (Confirmed)
- Main functions: `resolve_merge_categories()`, `prepare_rows_for_merge()`, `deduplicate_osm_source_rows()`, `merge_source_amenity_rows()`
- Important constants:
  - `AUTO_MATCH_RADIUS_M = 35.0`
  - `NAME_MATCH_RADIUS_M = 75.0`
  - `OSM_SELF_DEDUPE_RADIUS_M = 10.0`
- Why it matters: changes here alter counts, conflict classes, and downstream scores. Bump `AMENITY_MERGE_ALGO_VERSION` when logic changes. (Confirmed)
- LOC: 750

### `db_postgis/amenity_merge.py`

- Purpose: DB-backed staging and streaming wrapper around the canonical OSM/Overture merge logic. (Confirmed)
- Main function: `load_merged_source_amenity_rows()`
- Why it matters: this is the active scale path used by the precompute amenity phase, so Overture merge behavior is split between pure merge rules in `overture/merge.py` and durable staging/query behavior here. (Confirmed)

---

## 7. Update-Together Relationships

### Hash invalidation chain

```text
config.py
  -> geo_hash
  -> reach_hash
  -> surface_shell_hash
  -> score_hash
  -> render_hash
  -> config_hash
  -> build_key
```

Things that clearly feed this chain:

- geometry / coastal-cleanup params
- scoring caps
- amenity tier-unit tables
- Overture dataset signature
- GTFS transit config hash
- importer config version
- schema / algorithm version constants

### Schema change

When schema changes touch a published table, update these together:

```text
db_postgis/migrations/versions/
db_postgis/tables.py
db_postgis/reads.py and/or db_postgis/writes.py
pmtiles_bake_worker.py          (if the field goes into a tile layer)
fine_vector_pmtiles_worker.py   (if the field must exist on fine vector grid features)
precompute/bake_pmtiles.py      (metadata)
serve_from_db.py                (if the field is surfaced at runtime)
frontend/src/*                  (if the field is consumed in the UI)
```

### Amenity tiering contract

```text
config.py tier-unit constants
precompute/amenity_tiers.py
precompute/phases.py
db_postgis/migrations/versions/20260419_000007_add_amenity_tier_column.py
db_postgis/tables.py
precompute/publish.py
precompute/bake_pmtiles.py
pmtiles_bake_worker.py
tests/test_amenity_tiers.py
tests/test_pmtiles_bake.py
```

### Runtime JSON contract

```text
serve_from_db.RuntimeState
frontend/src/runtime_contract.js
frontend/src/runtime_contract.test.js
frontend/src/main.js
```

### PMTiles layer contract

```text
precompute/bake_pmtiles.py::_pmtiles_metadata()
pmtiles_bake_worker.py SQL templates
fine_vector_pmtiles_worker.py feature properties / geometry encoding
frontend layer expectations
tests/test_pmtiles_bake.py
```

### Noise overlay contract

```text
noise/loader.py
db_postgis/migrations/versions/20260424_000012_noise_polygons.py
db_postgis/tables.py
precompute/publish.py
precompute/bake_pmtiles.py
pmtiles_bake_worker.py
serve_from_db.py
frontend/src/noise_filters.js
frontend/src/main.js
tests/test_noise_loader.py
tests/test_pmtiles_bake.py
frontend/src/noise_filters.test.js
```

### Reachability explainability contract

```text
precompute/amenity_clusters.py
precompute/network.py
precompute/phases.py
precompute/grid.py
precompute/surface.py
db_postgis/tables.py
db_postgis/reads.py
precompute/publish.py
precompute/bake_pmtiles.py
pmtiles_bake_worker.py
serve_from_db.py
frontend/src/main.js
tests/test_precompute_behavior.py
tests/test_surface_runtime.py
tests/test_pmtiles_bake.py
tests/test_server_behavior.py
```

---

## 8. Configuration Surface

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Full SQLAlchemy / PostGIS connection string; default `connect_timeout=15` is appended when absent | None |
| `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT` | DB fallback parts; rendered through the same default `connect_timeout=15` SQLAlchemy URL | None |
| `GTFS_NTA_ZIP_PATH` | NTA GTFS zip path | `gtfs/nta_gtfs.zip` |
| `GTFS_TRANSLINK_ZIP_PATH` | Translink GTFS zip path | `gtfs/translink_gtfs.zip` |
| `GTFS_NTA_URL`, `GTFS_TRANSLINK_URL` | Optional GTFS download URLs | Mostly empty by default |
| `GTFS_ANALYSIS_WINDOW_DAYS` | Transit analysis window | `30` |
| `GTFS_SERVICE_DESERT_WINDOW_DAYS` | Service-desert window | `7` |
| `GTFS_LOOKAHEAD_DAYS` | Transit lookahead window | `14` |
| `GTFS_AS_OF_DATE` | Override analysis date | unset -> today in Europe/Dublin |
| `WALKGRAPH_BIN` | Explicit walkgraph binary path | auto-detected |
| `LIVABILITY_SURFACE_THREADS` | Fine-surface worker thread count | unset -> all CPUs |
| `LIVABILITY_BAKE_WORKERS` | PMTiles bake worker count | `min(12, cpu_count())` |
| `LIVABILITY_FINE_RASTER_SURFACE` | Enable inspect-backed fine surface caches and legacy PNG endpoint; main map rendering now uses vector PMTiles | `"1"` |
| `COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2` | Skip opening step for very large coastal components | `1_000_000_000.0` |
| `OSM2PGSQL_BIN` | osm2pgsql binary path | `"osm2pgsql"` |

### Important config files

| File | Role |
|---|---|
| `config.py` | Canonical project config |
| `alembic.ini` | Alembic runner config |
| `osm2pgsql_livability.lua` | OSM tag filter / import rules |
| `.env.example` | Local env template |
| `pytest.ini` | Pytest collection scope and generated-directory exclusions |
| `frontend/package.json` | Frontend dependency and build scripts |

### Runtime assumptions

| Operation | Prerequisites |
|---|---|
| Any pipeline command | reachable PostGIS config |
| `--refresh-import` | local OSM PBF + `osm2pgsql` available |
| `--refresh-transit` | GTFS zips or URLs + compiled `walkgraph` with `gtfs-refresh` support |
| `--precompute` | managed schema ready, raw import ready or `--auto-refresh-import`, boundaries present, compiled `walkgraph` |
| `--serve` | completed precompute build and PMTiles archive for the chosen profile |
| Overture merge | `overture/ireland_places.geoparquet` present; otherwise it degrades gracefully |
| Noise overlay | `noise_datasets/*.zip` present for published contours; ROI Round 4 road requires `pyogrio`/GDAL FileGDB support |

---

## 9. Risky / Misleading Areas

- `pmtiles_bake_worker.py` import isolation is real, and `fine_vector_pmtiles_worker.py` exists specifically so Windows bake workers do not import `precompute/__init__.py`. Do not collapse them back together casually.
- `static/dist/` is a checked-in build artifact. Editing `frontend/src/*` without rebuilding leaves runtime stale.
- `schema.sql` is a snapshot, not schema truth. Treat Alembic as canonical.
- `render_from_db.py` is only a wrapper. The real server is `serve_from_db.py`.
- `config.resolution_for_zoom()` is not `pmtiles_bake_worker._resolution_for_zoom()`.
- `HASHES = build_config_hashes()` runs at import time and silently tolerates missing files by hashing zero-like metadata.
- `_STATE = _BuildState.bootstrap()` also runs at import time and is invalid until activation.
- `extract_fingerprint()` now caches the exact `.osm.pbf` content hash in `.livability_cache/osm_extract_fingerprint_cache.json`, keyed by resolved path + file size + `mtime_ns`. Deleting or corrupting that cache only affects startup time; the code falls back to a full re-hash.
- `COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2` has a non-zero live default even though the nearby comment still talks about "default 0 = disabled". Trust the constant, not the stale comment.
- `overture/ireland_places.geoparquet` and `boundaries/*.geojson` are external inputs, not committed repo assets.
- Transit reality is GTFS-first now. Do not assume an OSM-stop-to-GTFS matching workflow still drives scoring.
- PMTiles bake writes to a sibling temp archive and only replaces the final `.pmtiles` after finalize succeeds; failed bakes clean the temp file and preserve the previous archive.
- OSM import reuse is manifest/scope-aware: raw rows are only considered ready with a complete import manifest whose `normalization_scope_hash` matches the active profile. Raw rows without a matching manifest are dropped and rebuilt.
- Both Python and Rust GTFS parsers accept `calendar.txt`-only and `calendar_dates.txt`-only feeds, but still require at least one service calendar file.
- Root `pytest -q` is constrained by `pytest.ini` to `tests/` and excludes generated/local cache directories.
- Frontend click priority is now transport -> amenity -> service desert -> noise -> fine inspect -> coarse grid, with `frontend/src/click_priority.js` carrying the testable resolver.
- Noise datasets are local large inputs ignored by git; `noise_datasets/*.zip` should remain untracked. The loader intentionally fails loudly if the newer ROI Round 4 road FileGDB cannot be read.
- NI Round 1 shapefiles are class-coded (`GRIDCODE` 1..7 with `Noise_Cl` labels), not threshold-coded. Reusing threshold arithmetic on Round 1 creates invalid synthetic labels like `2-6`.
- Noise artifact ingest now stages rows in a temp table (`noise_ingest_stage_*`) and then runs SQL geometry normalization (`ST_GeomFromWKB` -> `ST_Transform` -> `ST_MakeValid`) instead of building giant 500-row inline `VALUES` statements with huge WKB hex params.
- Noise force semantics are split: resolved rebuild (`--force-noise-artifact`) is separate from source re-import (`--reimport-noise-source`), and `--force-noise-all` does both.
- `progress_tracker.py` is intentionally defensive. If tracking breaks, the build keeps going, so ETA regressions can hide without breaking tests.
- Reachability large-cache recovery is mixed-format now: `{key}.pkl(.gz)` is the base snapshot and `{key}.chunks.pkl(.gz)` is an overlay journal. If you touch cache loaders, preserve that merge order and fallback behavior.
- Reachability origin-node helpers now assume a split contract: `normalize_origin_node_ids(...)` produces sorted unique lists, and `merge_normalized_origin_node_ids(...)` unions already-normalized lists. Do not fall back to `sorted(set(...))` on multi-million origin sequences.
- The legacy `/tiles/surface/{resolution}/{z}/{x}/{y}.png` endpoint still exists for compatibility, but `/api/runtime` no longer advertises it and the frontend no longer uses it.
- Fine-grid rendering correctness now depends on three layers of protection: buffered geometry in `fine_vector_pmtiles_worker.py`, degenerate-ring filtering before local MVT encoding, and active-layer recreation in `frontend/src/main.js` / `frontend/src/runtime_contract.js`. If high-zoom seams or missing resolutions reappear, inspect all three before assuming the PMTiles archive is wrong.

---

## 10. Tests and Validation Signals

Representative tests confirmed present:

| Test file | What it covers |
|---|---|
| `tests/test_config.py` | config hash stability, env parsing, schema-version invalidation |
| `tests/test_amenity_tiers.py` | shop / healthcare / park tier classification |
| `tests/test_overture_loader.py` | Overture category filtering and park handling |
| `tests/test_osm_import_handling.py` | osm2pgsql wrapper and import manifest behavior |
| `tests/test_precompute_behavior.py` | phase sequencing, cache / hash behavior, service-desert publish summaries |
| `tests/test_precompute_cache.py` | tier cache read / write / invalidation helpers |
| `tests/test_pmtiles_bake.py` | tile field lists, layer metadata, amenity `tier` exposure, bounded parallel scheduling, retry behavior, temp-output cleanup, and old-archive preservation on failure |
| `tests/test_noise_loader.py` | ROI dB field normalization plus round-aware NI mapping (Round 1 class-code handling, Round 2/3 threshold mapping, unknown-code errors), and newest-round fallback geometry |
| `tests/test_noise_artifacts.py` | manifest SQL safety checks (`CAST(:error_detail AS text)`), ingest pre-validation diagnostics, and streaming ingest behavior (no full candidate-list materialization) |
| `tests/test_fine_vector_pmtiles_worker.py` | fine-grid shard aggregation, degenerate buffered-ring rejection, encoded per-zoom resolutions, mixed z15 resolutions, invalid-land skipping, buffered border-cell continuity, worker cache bounds/reset behavior |
| `tests/test_progress_tracker.py` | timing-history sanitization and persistence |
| `tests/test_server_behavior.py` | runtime API shape, transport subtier/mode count exposure, noise count exposure, and range serving |
| `tests/test_transit_phase1.py` | GTFS-first transit reality rows, weekly bus subtiers, bus daytime headway buckets, frequency departure windows, transport score units, exact local GTFS snapshot stop regressions for each bus-tier bucket plus strict exception-only / unscheduled examples, exports, school-only classification, `gtfs-refresh` artifact loading |
| `tests/test_surface_runtime.py` | fine-surface runtime behavior |
| `tests/test_sanity_check.py` | sanity fixture structure and runtime lookup mode selection |
| `frontend/src/runtime_contract.test.js` | frontend runtime contract parsing, active fill/outline grid layer definitions, lifecycle rebuild decisions, explicit visibility plans, the transport rail/tram styling priority, and the single active debug-grid filter path |
| `frontend/src/grid_debug.test.js` | persistent grid debug card rendering, diagnosis states, resolution display updates, and copyable snapshot formatting |
| `frontend/src/transport_filters.test.js` | public transport filter logic including weekly bus tiers, exact rail/tram mode matching, and exception-only intersection logic |
| `frontend/src/noise_filters.test.js` | noise metric/source/band options and MapLibre filter expression construction |
| `frontend/src/transport_reality_popup.test.js` | multi-row transport popup rendering, bus frequency labels, rail/tram mode tiers, and snapshot wording |
| `frontend/src/click_priority.test.js` | popup/action priority for transport, amenities, service deserts, noise, fine inspect, and coarse grid fallback |

Areas still relatively fragile:

- `overture/merge.py` has no dedicated single-purpose test file despite being large and high-impact.
- `_compute_service_deserts()` still lives inline in `precompute/__init__.py`, which makes it easier to miss.
- Fine-grid render correctness now spans both PMTiles bake tests and the dedicated fine-vector worker test, while exact explainability at high zoom still depends on `/api/inspect`.
- CI runs Python, Rust, and sanity fixture validation but does not currently run the frontend Node test suite from `frontend/package.json`.

---

## 11. Legacy / Snapshot Areas

| Path | Status | How to treat it |
|---|---|---|
| `render_from_db.py` | Compatibility wrapper | Edit `serve_from_db.py` instead |
| `schema.sql` | Bootstrap snapshot | Read only for orientation |
| `static/dist/*` | Built frontend artifacts | Rebuild from `frontend/src/` |
| `legacy/` | Local-only historical reference area | Do not import into active runtime |
| `.livability_cache/` | Generated outputs and timing history | Never edit manually |
| `walkgraph/target/` | Rust build output | Rebuild with Cargo |
| `docs/*.md` | Human context, design, methodology, validation notes | Useful for intent, not runtime truth |

---

## 12. Glossary

| Term | Meaning here |
|---|---|
| Grid cell | Scored polygon unit at a given resolution |
| Coarse vector resolution | `20000`, `10000`, or `5000` meter grid published to PMTiles |
| Fine vector resolution | `2500`, `1000`, `500`, `250`, `100`, `50` meter grid features baked into PMTiles and overzoomed at runtime |
| PMTiles | Single-file vector tile archive served over HTTP range requests |
| MVT | Mapbox Vector Tile bytes emitted from PostGIS `ST_AsMVT` |
| CAPS | Per-category score ceilings: `{"shops": 6, "transport": 5, "healthcare": 5, "parks": 5}` |
| Amenity tier | Stored subtype such as `corner`, `supermarket`, `clinic`, `regional` |
| Score units | Integer weight attached to a tier and consumed by scoring |
| Cluster count | Count of reachable scoring clusters after collapsing near-duplicate amenities within a category |
| Effective units | Distance-decayed float scoring input derived from reachable cluster representatives |
| Transit reality | GTFS-first stop-reality layer with status, departure counts, public frequency windows, weekday daytime bus tiers, and transport score units |
| Service desert | Cell with at least one reachable baseline GTFS stop but zero public departures in the desert window |
| Noise contour | Official display-only Lden / Lnight polygon from ROI EPA or NI OpenDataNI data, normalized into dB bands and source types |
| Conflict class | Amenity merge status: `osm_only`, `overture_only`, `source_agreement`, `source_conflict`; GTFS-direct transport rows use `gtfs_direct` |
| Geo hash | Geometry-level cache hash including geometry / coastal-cleanup inputs |
| Reach hash | Reachability-level hash including import, transit, tags, merge version, and Overture signature |
| Score hash | Scoring-level hash including caps, tier-unit tables, and resolution tiers |
| Build key | Build-scoped identifier used on published PostGIS rows |
| Build profile | `full`, `dev`, or `test` |

---

## 13. Recommended Reading Order

1. `config.py`
2. `main.py`
3. `db_postgis/tables.py`
4. `precompute/workflow.py`
5. `precompute/__init__.py`
6. `progress_tracker.py`
7. `precompute/phases.py`
8. `precompute/amenity_tiers.py`
9. `pmtiles_bake_worker.py`
10. `precompute/bake_pmtiles.py`
11. `noise/loader.py`
12. `serve_from_db.py`
13. `frontend/src/runtime_contract.js`
14. `frontend/src/main.js`
15. `frontend/src/noise_filters.js`
16. `overture/loader.py`
17. `overture/merge.py`
18. `scripts/sanity_check.py`
19. `tests/test_precompute_behavior.py`
20. `tests/test_pmtiles_bake.py`
21. `tests/test_noise_loader.py`
22. `tests/test_amenity_tiers.py`
23. `README.md` and `docs/*.md` if you need product / methodology context
### `noise_artifacts/ingest.py`

- Purpose: canonical noise source ingest entrypoint for artifact pipeline with mode dispatch.
- Supports `NOISE_INGEST_MODE=auto|ogr2ogr|python`; `auto` prefers `ogr2ogr`, otherwise falls back to Python COPY staging.
- Python path now uses one run-scoped temp staging table (`noise_ingest_stage`) with `geom_wkb BYTEA`, COPY-to-stage, configurable copy window (`NOISE_INGEST_COPY_BATCH_ROWS`) and flush window (`NOISE_INGEST_FLUSH_ROWS`), then `INSERT ... SELECT` normalization into `noise_normalized`.
- Normalization keeps strict band validation (`NN-NN` / `NN+`), includes NI gridcode context in errors, and avoids giant inline SQLAlchemy `VALUES` statements.

### `noise_artifacts/ogr_ingest.py`

- Purpose: high-throughput raw source import path using GDAL `ogr2ogr` direct to PostGIS staging tables.
- Includes `ogr2ogr_available()`, command builder, source ZIP extraction cache (`.livability_cache/noise_gdal`), per-layer import timing, field discovery (`pyogrio` with `fiona` fallback), case-insensitive ROI/NI allowlist field selection, geometry-metadata denylisting (`shape_*` variants), `-lco PRECISION=NO`, and SQL normalization into `noise_normalized`.
- NI normalization in this path still calls verified round-aware NI gridcode mapping logic; unknown class/threshold codes raise explicit errors with source context.
