# Repo Map

> Refreshed: 2026-07-08. Evidence grades: **Confirmed** = read directly from code; **Inference** = strongly suggested but not explicitly proven; **Unclear** = cannot be determined from repo alone.

---

## 1. Project Purpose

- Offline-first pipeline that scores grid cells across the island of Ireland for livability using walk access to `shops`, `transport`, `healthcare`, and `parks`. (Confirmed)
- Shops, healthcare, and parks now use tiered score units instead of flat presence counts. Examples: corner shop vs supermarket, clinic vs emergency hospital, pocket park vs regional park. (Confirmed)
- Ingests local OSM PBF via `osm2pgsql`, cache-managed public static GTFS ZIP feeds (default active transit inputs: `nta` and `translink`), and optionally an Overture Places geoparquet dataset. (Confirmed)
- Runs heavy work ahead of time: geometry prep -> amenity load/merge -> Rust walkgraph build -> igraph reachability -> grid scoring -> PMTiles bake. (Confirmed)
- Publishes results to PostGIS plus a main livability PMTiles archive and a separate noise PMTiles overlay so the frontend can run without live tile SQL queries. (Confirmed)
- Builds a GTFS-first transit reality layer, bus daytime frequency tiers, explicit rail/tram mode tiers, frequency-weighted transport scoring, a hidden railway-track proximity modifier derived from GTFS shapes, and a service-desert overlay from scheduled departures, not from OSM stop tags alone. (Confirmed)
- Current live transit config in `config.py` now wires the active `nta` and `translink` GTFS feeds, matching the README/tests and restoring Northern Ireland transport coverage in the published transport layer. (Confirmed)
- Adds a display-only transport/industry noise overlay (Phase E: roads + rail + airport + industry, Lden/Lnight) calibrated from official-derived strategic noise data; road/rail use grid proxy rows while airport/industry use resolved official-derived polygons, and runtime does not present measured point noise. This does not feed livability scoring yet. (Confirmed)
- Adds a display-only land-use context overlay from OSM `landuse` polygons (`residential`, `commercial`, `industrial`, `retail`, `farmland`, `forest`) as a lower-zoom contextual layer for the frontend, now baked and rendered from z5 through z11; it is not used in livability scoring yet. (Confirmed)
- Uses layered content hashes so changes to geometry, scoring params, GTFS feeds, Overture data, or importer config only invalidate the affected cache tiers. (Confirmed)
- Alpha-stage: amenity tiering, Overture merge, service deserts, and the new fine vector grid / inspect-backed surface path are still moving. (Inference from recent migrations, tests, and docs)

---

## 2. High-Level Architecture

- **CLI dispatcher**: `main.py`
- **Config and hash spine**: `config.py`
- **Schema and DB IO**: `db_postgis/`
- **OSM ingest**: `local_osm_import/` + `osm2pgsql_livability.lua`
- **GTFS ingest and transit reality**: `transit/`
- **GTFS rail-corridor proxy**: `transit/railway_corridors.py`
- **Overture integration and dedupe**: `overture/loader.py`, `overture/merge.py`, `db_postgis/amenity_merge.py`
- **Amenity merge observability**: `db_postgis/amenity_merge.py`, `precompute/phases.py`, `precompute/_rows.py`, `precompute/publish.py`
- **Noise overlay ingestion**: `noise/loader.py`
- **Noise artifact source ingest modes**: `noise_artifacts/ingest.py`, `noise_artifacts/ogr_ingest.py`
- **Amenity tier classifier**: `precompute/amenity_tiers.py`
- **Amenity clustering for Phase 2 variety scoring**: `precompute/amenity_clusters.py`
- **Precompute cache helpers**: `precompute/cache.py`, `precompute/_cache_wrappers.py`
- **Array-native reachability cache helpers**: `precompute/reachability_arrays.py`
- **Precompute tier helpers**: `precompute/tiers.py`, `precompute/_tier_wrappers.py`
- **Precompute planning layer**: `precompute/_planning.py`
- **Study-area geometry / coast mask**: `study_area.py`
- **Precompute pipeline orchestration**: `precompute/__init__.py`, `precompute/_planning.py`, `precompute/workflow.py`, `precompute/phases.py`
- **Lightweight GTFS refresh CLI path**: `transit_refresh_runner.py`
- **Pipeline ETA and timing history**: `progress_tracker.py`
- **Rust walkgraph binary**: `walkgraph/`
- **PMTiles bake**: `precompute/bake_pmtiles.py`, `noise_artifacts/bake.py`, `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py`
- **Runtime HTTP server**: `serve_from_db.py`
- **Runtime route helper**: `serve_routes.py`
- **Runtime request logging**: `serve_from_db.py` emits compact route-aware GET/HEAD logs with status, duration, response size, and disconnect hints.
- **Land-use context overlay**: `db_postgis/reads.py`, `precompute/_rows.py`, `precompute/bake_pmtiles.py`, `pmtiles_bake_worker.py`, `frontend/src/landuse_filters.js`; frontend initial style construction omits the `filter` property when all land-use classes are selected because MapLibre rejects `filter: null` in layer JSON.
- **Alembic schema-hardening migration**: `db_postgis/migrations/versions/20260617_000021_add_candidate_key_constraints.py`
- **Frontend source**: `frontend/src/` (including the land-use context controls with palette-matched swatches/checkbox accents, note text, layer filter helpers, runtime land-use zoom bounds, a `map_source_guard.js` helper that avoids asking MapLibre about `livability` load state before the source exists and delays source feature queries until the vector source has actually loaded, and cache-busted asset loading via `static/index.html`)
- **Frontend grid diagnostics helper**: `frontend/src/grid_debug.js`
- **Served frontend bundle**: `static/dist/`
- **Human docs / design notes**: `README.md`, `docs/*.md`
- **Read-only DB integrity preflight**: `scripts/db_integrity_check.py` and `tests/test_db_integrity_check.py`; checks logical duplicate groups and NULL key values before future PK/unique/FK constraints and deliberately skips `amenities` as ambiguous.

---

## 3. Entry Points

### `main.py`

- Primary CLI entry point. (Confirmed, LOC: 326)
- Dispatches:
  - `import` -> `precompute.refresh_local_import()`
  - `gtfs status` -> `transit_refresh_runner.gtfs_status()` diagnostics only (no download)
  - `gtfs refresh` -> `transit_refresh_runner.refresh_gtfs()`
  - `transit` -> `transit_refresh_runner.refresh_transit()`
  - `precompute --profile {full,dev,test}` -> `precompute.run_precompute(profile="full"|"dev"|"test")`
    - `--explain` with a precompute profile -> prints the planner decision tree without entering the expensive execution phases
    - `--force-precompute` -> rebuilds and replaces the current PostGIS build even if a complete manifest already exists
    - `--refresh-noise-artifact` -> refresh artifact during precompute when missing/stale
    - `--force-noise-artifact` -> force resolved artifact rebuild while reusing existing source rows
    - `--reimport-noise-source` -> force raw source re-import into `noise_normalized`
    - `--force-noise-all` -> force both source re-import and resolved rebuild
    - `--auto-refresh-import` -> allow precompute to refresh raw OSM import state when missing
  - `serve --profile {full,dev,test}` -> `render_from_db.run_render_from_db(...)`
- If no subcommand is supplied, serving is the default path. (Confirmed)
- `transit` now goes through a lightweight transit-only runner instead of importing the full `precompute` package first, emits tracker lines before DB/schema and source-state preflight, reuses a cached OSM extract fingerprint when the local `.osm.pbf` path/size/mtime are unchanged, and can optionally auto-refresh GTFS cache first via `--auto-refresh-gtfs`. It still prints the explicit completion line plus the transit-phase `completed` tracker line. (Confirmed)

### `.github/workflows/scheduled_refresh.yml`

- Weekly self-hosted workflow. (Confirmed)
- Uses workflow-level concurrency so scheduled and manual refresh runs do not overlap, and a 240-minute job timeout to keep long refresh/precompute runs bounded. The OSM force flag is expanded without bash-only syntax so the workflow stays portable across an undocumented self-hosted runner OS. (Confirmed)
- Runs:
  - `python scripts/refresh_osm.py`
  - `python main.py transit`
  - `python main.py precompute --auto-refresh-import`
  - `python scripts/sanity_check.py --profile full`

### `.github/workflows/ci.yml`

- Push / PR validation workflow. (Confirmed)
- Uses an explicit `pwsh` default shell on the Windows job so the PowerShell bundle freshness check is unambiguous. (Confirmed)
- Runs:
  - `npm ci` in `frontend/` using `frontend/package-lock.json`
  - `npm test` and `npm run build` in `frontend/`
  - `git diff --exit-code -- static/dist` to catch stale checked-in frontend bundles
  - `python -m unittest discover -s tests -t . -p "test_*.py"`
  - `cargo test --manifest-path walkgraph/Cargo.toml`
  - `python scripts/sanity_check.py --validate-only`

### `scripts/ci_local.ps1`

- Local Windows PowerShell CI runner for pre-push checks. (Confirmed)
- Runs the practical developer-facing sequence: command availability checks for `python`, `npm.cmd`, and `git`; `python -m pytest -q`; `python -m alembic current`; `python -m alembic upgrade head`; `python scripts/db_integrity_check.py`; `python main.py precompute --profile dev --explain`; `npm.cmd test --prefix frontend`; `npm.cmd run build --prefix frontend`; `git diff --exit-code -- static/dist`; and `git diff --check`. (Confirmed)
- Stops on the first failure, prints section headers, and reports elapsed time per step. (Confirmed)

### `scripts/sanity_check.py`

- Standalone validation CLI used by CI and scheduled refresh. (Confirmed, LOC: 349)
- `--validate-only` checks fixture structure only.
- Normal mode resolves the active completed build, then reads scores through fine-surface runtime lookups when enabled, otherwise through `grid_walk` point lookups. (Confirmed from file and tests)

### `scripts/win/run_noise_precompute_watchdog.ps1`

- Windows wrapper for noise-focused dev precompute with enforced wall-clock timeout. (Confirmed)
- Runs `scripts/win/geo_env.cmd` with project `.venv` Python and explicit mode selection: `DevReuse` / `AccurateReuse` (reuse-only fast paths, never rebuild), `DevPrepare` / `AccuratePrepare` (cache-aware refresh paths), and `DevForce` / `AccurateForce` (source reimport + resolved rebuild paths). (Confirmed)
- Supports overriding the Python executable via `NOISE_PYTHON_EXE` (for example `python` in the active conda env) and falls back to conda `python` when `.venv\Scripts\python.exe` is missing. (Confirmed)
- On timeout, kills the spawned process tree (and attempts cleanup of lingering `ogr2ogr` processes) and returns exit code `124`. (Confirmed)

---

## 4. End-to-End Flow

```text
1. Local inputs
   osm/ireland-and-northern-ireland-latest.osm.pbf
   .livability_cache/gtfs/*/current.zip (or override zip paths via env)
   overture/ireland_places.geoparquet (optional)
   noise_datasets/*.zip (optional display overlay)
   ireland_main_island_shp/ireland_main_island.shp (preferred exact main-island coast mask when present)
   boundaries/*.geojson

2. Raw import
   python main.py import
   -> local_osm_import/
   -> osm_raw.features + osm_raw.import_manifest

3. Transit reality
   python main.py transit
   -> transit/workflow.py
   -> walkgraph gtfs-refresh
   -> transit_raw.* + transit_derived.*

4. Precompute
   python main.py precompute
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
   -> main PMTiles excludes noise by default
   -> noise_artifacts/bake.py writes a separate noise-only archive from noise_polygons with source-layer `noise_proxy`
   -> PMTiles archive source max zoom capped at 15; profile noise caps default to z10 for dev and z13 for full/test
   -> bounded in-flight worker queue, fine-grid worker cap of 4, retry-smaller-on-pool-failure, temp output staging that preserves the previous archive on failure
   -> .livability_cache/livability[(-dev|-test)].pmtiles
   -> .livability_cache/noise[(-dev|-test)].pmtiles

7. Runtime serve
   python main.py serve
   -> render_from_db.run_render_from_db()
   -> serve_from_db.serve_livability_app()
   Endpoints:
     GET /api/runtime
     GET /api/inspect
     GET /tiles/livability.pmtiles
     GET /tiles/livability-test.pmtiles
     GET /tiles/noise.pmtiles
     GET /tiles/noise-test.pmtiles
     GET /tiles/surface/{resolution}/{z}/{x}/{y}.png
     GET /exports/transport-reality.zip

8. Frontend
   static/dist/app.js
  -> reads PMTiles through pmtiles://
  -> reads runtime JSON from /api/runtime with a longer cold-start timeout and one retry after abort so first-load DB/runtime warmup does not leave the app permanently stuck
  -> renders one active vector grid fill+outline pair with a client-side Combined/Shops/Transport/Healthcare/Parks color toggle, recreates those layers when the zoom band changes, and overzooms z15 source tiles to z19
  -> exposes a default-off Noise proxy panel backed by the separate `noise` vector source and its `noise_proxy` source-layer, with metric + kind filtering (`road`, `rail`, `airport`, `industry`), proxy score coloring, opacity control that now updates both fill and outline (`line-opacity = min(0.55, fillOpacity * 0.8)`), and an explicit caveat that road/rail are grid proxy while airport/industry are resolved official-derived polygons (not measured point noise)
  -> the fixed control panel now scrolls internally when its contents exceed the viewport height, so stacked debug + amenity controls stay reachable
  -> public transport tiers are rendered inside the `Amenities` -> `Transport` card: `Sub-tiers` now contains a `Bus` subgroup (with nested `Schedule` weekly bus-pattern + unscheduled/exception-only filters and nested `Frequency` headway tiers) plus a separate `Transport` mode group (`Bus` / `Rail` / `Tram` toggles), while the default all-mode state intentionally applies no vector filter so older PMTiles transport layers without the newer mode/subtier fields still show stops; narrowed filters require the newer transport fields so selectors visibly narrow when the matching profile PMTiles is served, tram/rail-only popups still show their mode tier, and popups expose bus headway, commute, Friday-evening, and score-unit frequency fields
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
| Transit reality | `transit/workflow.py`, `transit/rust_gtfs.py`, `transit/railway_corridors.py` | `config.transit_config_hash()` | None |
| Amenity tiering | `config.py` tier constants, `precompute/amenity_tiers.py` | `precompute/phases.py`, `precompute/publish.py` | None |
| Overture category mapping | `overture/loader.py::OVERTURE_CATEGORY_MAP` | `precompute/phases.py` | None |
| Overture merge logic | `overture/merge.py`, `db_postgis/amenity_merge.py` | `precompute/phases.py` | None |
| Amenity merge diagnostics | `db_postgis/amenity_merge.py`, `precompute/phases.py`, `precompute/_rows.py`, `precompute/publish.py` | `phase_amenities_impl()` -> `_summary_json()` -> `build_manifest.summary_json` | `summary_json["amenity_merge"]` persists stage timings, key row counts, candidate-path counts, and compact warnings without storing geometries or SQL text. |
| Noise overlay source handling | `noise/loader.py`, `noise_artifacts/*` | `noise_polygons`, separate `noise` PMTiles archive, `/api/runtime` noise counts + `noise_pmtiles_url` | `noise_datasets/*.zip` local inputs |
| Noise artifact ogr2ogr ingest safety/perf | `noise_artifacts/ogr_ingest.py` | Road GDB canonical pipeline (FileGDB -> local GPKG cache -> one PG stage import -> batch normalize), SQL timeouts, disk preflight (cache + PostgreSQL `data_directory`) | `NOISE_ROAD_GDB_CANONICAL_CACHE`, `NOISE_REBUILD_ROAD_GDB_CACHE`, `NOISE_ROAD_NORMALIZE_BATCH_SIZE`, `NOISE_SQL_*`, `NOISE_MIN_FREE_DISK_GB` |
| Precompute orchestration | `precompute/_planning.py`, `precompute/workflow.py` | `precompute/__init__.py` | `precompute/workflow.py` now also has an explain-mode short-circuit that prints the pure planner output without running geometry, reachability, publish, or schema-migration setup phases. `precompute._STATE.activate()` clears per-build amenity merge diagnostics so stale summaries do not leak across runs. |
| Managed schema validation | `db_postgis/schema.py` | `db_postgis.ensure_database_ready()` and startup validation | PK-equivalent coverage for `grid_walk_build_resolution_cell_idx` and `service_deserts_build_resolution_cell_idx` is accepted after the candidate-key migration, so startup no longer demands those exact index names when the primary key is present. |
| Coastal cleanup observability | `study_area.py`, `precompute/_rows.py`, `precompute/publish.py` | `clean_coastal_artifacts()` -> `_summary_json()` -> `build_manifest.summary_json` | `summary_json["coastal_cleanup"]` persists counts, fallback modes, thresholds, and sample warning metadata without storing geometries. |
| PMTiles layer metadata | `precompute/bake_pmtiles.py`, `noise_artifacts/bake.py` | `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py` | None |
| Runtime API contract | `serve_from_db.RuntimeState` | `frontend/src/runtime_contract.js`, `frontend/src/main.js` | `render_from_db.py` |
| Runtime request logging | `serve_from_db.py`, `serve_routes.py` | `LivabilityRequestHandler.do_GET()` / `do_HEAD()` | Route-aware log lines include method, path, route name, status, duration, bytes, and disconnect hints without changing HTTP responses. |
| Frontend source | `frontend/src/` | `static/dist/` after build | `static/dist/*` |
| Progress / ETA behavior | `progress_tracker.py` | `precompute/workflow.py`, `precompute/__init__.py` | `.livability_cache/precompute_timing_stats.json` |
| Product / methodology docs | `README.md`, `docs/PHASES.md`, `docs/*.md` | Humans only; README roadmap and phase notes distinguish display-only noise overlay from future scoring penalties | None |

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
  - `TRANSIT_REALITY_ALGO_VERSION = 10`
  - `AMENITY_MERGE_ALGO_VERSION = 4`
  - `FINE_SURFACE_SCHEMA_VERSION = 2`
  - `PMTILES_SCHEMA_VERSION = 13`
  - `GRID_GEOMETRY_SCHEMA_VERSION = 4`
  - `CACHE_SCHEMA_VERSION = 14`
  - `WALKGRAPH_FORMAT_VERSION = 3`
  - `IMPORTER_CONFIG_VERSION = "2026-04-08"`
  - `RAILWAY_PROXIMITY_ACTIVE_MODES = ("rail", "tram")`
  - `RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M = 50.0`
  - `RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M = 200.0`
  - `RAILWAY_PROXIMITY_MAX_PENALTY = 4.0`
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

- Purpose: lightweight CLI-only GTFS refresh path used by `main.py gtfs status`, `main.py gtfs refresh`, and `main.py transit`. (Confirmed)
- Why it matters: avoids importing the whole `precompute` package before the first transit progress line, now starts the tracker before DB/schema checks and source-state resolution, materializes the transit-derived rail corridor cache when transit reality is current, and passes transit progress callbacks into OSM source-state fingerprinting so users can see `osm2pgsql --version` probes plus cached-vs-rehashed `.osm.pbf` resolution immediately in the console. (Confirmed)
- Main functions: `refresh_gtfs()`, `gtfs_status()`, `refresh_transit()`, `_preflight_transit_rebuild()`

### `transit/gtfs_download.py`

- Purpose: cache-aware public static GTFS downloader/validator used by transit source resolution. (Confirmed)
- Why it matters: centralizes polite HTTP behavior (timeout, retry/backoff, conditional requests), atomic ZIP replacement, SHA256 manifests, and required-GTFS-file validation before any `current.zip` update is accepted. (Confirmed)
- Main functions: `refresh_gtfs_feed()`, `refresh_gtfs_feeds()`, `ensure_transit_feed_available()`

### `transit/railway_corridors.py`

- Purpose: shapes-backed rail/tram corridor materialization and hidden railway proximity penalty helper. (Confirmed)
- Why it matters: GTFS shapes are the current service-backed geometry source for rail/tram proximity, so this module hashes, dissolves, caches, and scores the corridor layer without changing the public overlay surface. (Confirmed)
- Main functions: `build_railway_corridor_materialization()`, `compute_railway_proximity_penalties()`, `railway_proximity_penalty_for_distance()`

### `precompute/workflow.py`

- Purpose: injected orchestration for import refresh and full precompute. (Confirmed)
- Why it matters: contains the short-circuit logic for reusing completed builds, refreshing noise-only rows when the selected artifact hash changes or noise refresh flags are used, rebaking PMTiles only, or rebuilding fine surface only. Also owns tracker injection and timing persistence. (Confirmed)
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
  - reachability now persists `walk_counts_by_origin_node`, `walk_cluster_counts_by_origin_node`, and `walk_effective_units_by_origin_node` through versioned array-native caches under `reachability_arrays/*`
  - routing checkpoints are committed `.npz` matrix chunks with `uint64` origin IDs, manifest-defined category order, and `uint32`/`float32` matrices; successful completion compacts chunks into `base.npz`
  - legacy pickle/gzip dict reachability caches are ignored by default and only converted when `LIVABILITY_MIGRATE_LEGACY_REACH_CACHE=1`
  - origin-node normalization/union now uses low-memory sorted lists rather than Python `set(sorted(...))` dedupe, so coarse-grid and fine-surface reachability inputs can be combined without materializing another huge Python-object set
  - railway proximity penalties are now computed from the transit-derived corridor layer and threaded through grid scoring, node score arrays, inspect payloads, and PMTiles export
- LOC: 1093

### `precompute/reachability_arrays.py`

- Purpose: versioned array-native cache format for origin-node reachability matrices. (Confirmed)
- Why it matters: preserves resumable reachability builds without expanding cached origins into dict-of-dicts, while keeping coarse-grid sparse JSON output via a one-row lookup adapter. (Confirmed)
- Format: `manifest.json` plus `base.npz` and optional `chunks/chunk_*.npz`; categories live in the manifest, `origin_ids` are `<u8`, count matrices are `<u4`, and effective-unit matrices are `<f4`. (Confirmed)
- Recovery behavior: orphan `.tmp` files are ignored, corrupt chunks are quarantined and recomputed, corrupt base matrices invalidate the v1 cache, and legacy pickle/gzip migration is opt-in only. (Confirmed)
- LOC: 554

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
- Why it matters: owns `GRID_AMENITY_CATEGORIES`, `_pmtiles_metadata()`, the z15 source-zoom cap, and the sparse fine-grid tile-spec planner that stitches coarse SQL tiles together with fine vector grid tiles. Noise is no longer declared or baked into the main livability archive. It also bounds parallel in-flight work, clamps fine-grid bakes to 4 workers, retries once at half workers after `BrokenProcessPool`, and only replaces the final PMTiles archive after a successful temp-file finalize. Tests import these directly. (Confirmed)
- Windows can still fail here with `could not load library "postgis-3.dll": The paging file is too small for this operation to complete` when too many parallel bake workers hit PostGIS at once; that is an OS memory/pagefile limit, not a missing DLL. (Confirmed)
- The amenities layer metadata declares `category`, `tier`, `name`, and `conflict_class`. (Confirmed from tests and code)
- The transport layer metadata now declares weekly bus subtier / mask fields, bus daytime frequency fields, `transport_mode_tier`, comma-separated `route_modes`, numeric `0/1` transport flags, commute/off-peak/weekend/Friday-evening departure averages, and `transport_score_units`; the frontend uses `route_modes` for exact rail/tram filtering and `bus_frequency_tier` for bus-frequency filtering, and the worker SQL emits one feature per published `transport_reality` row instead of grouping same-name same-coordinate stops. (Confirmed from code and tests)
- The `grid` source-layer now carries both coarse and fine features, with fine rows padded with zero-valued popup numerics so metadata and popup consumers stay schema-stable. (Confirmed from code and tests)
- LOC: 818

### `study_area.py`

- Purpose: loads and normalizes the metric study-area geometry used by grid generation, reachability, and PMTiles/fine-vector clipping. (Confirmed)
- Why it matters: `load_island_geometry_metric()` now prefers `ireland_main_island_shp/ireland_main_island.shp` when present, reading it as the exact main-island coast boundary and skipping the older ROI+NI merge/coastal-cleanup path. If the shapefile is missing, it falls back to the ROI/NI boundary merge and `clean_coastal_artifacts()` cleanup. (Confirmed from code and tests)
- Coastal cleanup fallback diagnostics are captured in a compact summary and persisted into `build_manifest.summary_json["coastal_cleanup"]` after precompute, so fallback modes, counts, and representative locations are inspectable after the build. (Confirmed from code and tests)
- The geo hash includes the main-island shapefile sidecar metadata (`.shp`, `.shx`, `.dbf`, `.prj`, `.cpg`) so PMTiles/precompute caches invalidate when the coastline input changes. (Confirmed from code and tests)
- LOC: 880

### `noise_artifacts/bake.py`

- Purpose: standalone noise PMTiles bake from published `noise_polygons` (proxy rows in artifact mode). (Confirmed)
- Why it matters: writes `noise[(-dev|-test)].pmtiles` with only the `noise` vector layer, removes stale noise archives when no rows exist, and reuses the lightweight `pmtiles_bake_worker.py` noise tile SQL so main PMTiles bakes stay noise-free. (Confirmed from code and tests)
- The `noise_proxy` source-layer declares proxy fields (`kind=road`, `class=unclassified`, `metric=Lden`, `calibrated_band_min`, `proxy_score`, `buffer_m=100`, `method=official_derived_grid_proxy`, `calibration_source=noise_grid_artifact`, `calibration_layer=grid_1000m`, `confidence=proxy_not_measured`, `actual_road_geometry=0`) and starts at z8. (Confirmed from code and tests)
- LOC: 344

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
- Also exposes the SQL-backed `noise_proxy` MVT helper used by the standalone noise PMTiles bake; keep GIS readers out of this file so subprocess imports stay light. (Confirmed)
- LOC: 425

### `serve_from_db.py`

- Purpose: runtime HTTP server for static assets, PMTiles range reads, fine-surface PNG tiles, runtime JSON, inspect API, and transport-reality export download. (Confirmed)
- Route matching and dispatch live in `serve_routes.py`; `serve_from_db.py` keeps the HTTP mechanics, response writing, and client-disconnect handling. (Confirmed)
- Why it matters: `RuntimeState` is the server-side truth for `/api/runtime`. (Confirmed)
- `RuntimeState` includes:
  - `build_key`, `build_profile`
  - `coarse_vector_resolutions`, `fine_resolutions`, `surface_zoom_breaks`
  - `amenity_counts`, `amenity_tier_counts`
  - `transport_subtier_counts`, `transport_bus_frequency_counts`, `transport_flag_counts`, `transport_mode_counts`
  - `noise_enabled`, `noise_counts`, `noise_source_counts`, `noise_metric_counts`, `noise_band_counts`, `noise_pmtiles_url`
  - `fine_surface_enabled`
  - `surface_shell_dir`, `surface_score_dir`, `surface_tile_dir`
  - `transport_reality_enabled`, `service_deserts_enabled`
  - `transport_reality_download_url`
  - `transit_analysis_date`, `transit_analysis_window_days`, `transit_service_desert_window_days`
  - `overture_dataset`
- `/api/runtime` still reports `surface_zoom_breaks`, `fine_resolutions_m`, `fine_surface_enabled`, `inspect_url`, and `max_zoom=19`, but it no longer advertises `surface_tile_url_template`; the main render path is now vector-only. Expected client aborts on `/api/inspect` are suppressed from server logs the same way PMTiles range disconnects are, while `/` and `/static/*` now ship with `Cache-Control: no-store` so rebuilt local frontend assets are not silently cached between reloads. Static and export responses are streamed in bounded chunks instead of being loaded with `read_bytes()`. (Confirmed from code and tests)
- Route matching now lives in `serve_routes.py`, which keeps path dispatch priority testable without changing the HTTP response code paths. (Confirmed)
- `/api/inspect` and `/tiles/surface/{resolution}/{z}/{x}/{y}.png` now reject non-finite or out-of-range inputs before fine-surface rendering starts; surface tiles validate supported resolution, integer zoom, and tile bounds against `2^z - 1`. (Confirmed from code and tests)
- `/api/runtime` includes noise overlay availability, `noise_pmtiles_url` for the separate overlay archive, and filter counts used by the proxy UI (`noise_source_counts`, `noise_metric_counts`); when no noise rows/archive are available it reports `noise_enabled=false` and `noise_pmtiles_url=null`. (Confirmed from code and tests)
- Strict mode still requires a manifest matching current `config_hash` + `extract_path`. An explicit local fallback can be enabled with `LIVABILITY_RUNTIME_ALLOW_STALE_DEV_RUNTIME=1`; it first considers the latest completed manifest for the same extract path, but if that manifest has transport reality enabled without usable transport summary/quality signals, it prefers the latest transport-ready completed manifest and flags runtime payload with `runtime_mode=stale_manifest_fallback` + `runtime_warning`. Runtime `pmtiles_url` follows the selected manifest `build_profile`, and the local server serves existing full/dev/test PMTiles routes so stale fallback does not pair dev runtime filters with a full-profile archive. (Confirmed from code and tests)
- LOC: 791

### `db_postgis/tables.py`

- Purpose: SQLAlchemy table definitions used by reads / writes at runtime. (Confirmed)
- Why it matters: must match Alembic head. (Confirmed)
- Current schema facts:
  - `amenities` has `category`, `tier`, `geom`, `source`, `source_ref`, `name`, `conflict_class`
  - `grid_walk` has `counts_json`, `cluster_counts_json`, `effective_units_json`, `scores_json`, `total_score`, clipped-area fields
  - `transit_derived.gtfs_stop_service_summary`, `transit_derived.gtfs_stop_reality`, `transit_derived.railway_corridor_manifest`, `transit_derived.railway_corridors`, and public `transport_reality` now also carry `bus_active_days_mask_7d` (legacy export name for the base weekly bus mask), `bus_service_subtier`, `bus_daytime_deps`, `bus_daytime_headway_min`, `bus_frequency_tier`, `bus_frequency_score_units`, `is_unscheduled_stop`, `has_exception_only_service`, `has_any_bus_service`, `has_daily_bus_service`, `route_modes_json`, commute/off-peak/weekend/Friday-evening departure averages, and `transport_score_units`
  - public output tables are `grid_walk`, `amenities`, `transport_reality`, `service_deserts`, `build_manifest`
  - public `noise_polygons` stores build-scoped noise overlay geometry; in artifact mode this is currently Phase E transport/industry output (`source_type IN ('road','rail','airport','industry')`, `metric IN ('Lden','Lnight')`, unclassified class) encoded via compatibility columns and exported as `noise_proxy` layer properties
- LOC: 497

### `db_postgis/_dependencies.py`

- Purpose: central import shim for SQLAlchemy, GeoAlchemy, and geometry helper symbols used by DB modules. (Confirmed)
- Why it matters: on Windows, it installs a small `platform` shim before importing SQLAlchemy/GeoAlchemy so a broken WMI-backed `platform.system()`/`platform.uname()` call cannot freeze CLI startup before any precompute banner prints. (Confirmed from code and tests)

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
     -> surface_shell_hash (geometry-only fine shell)
     -> geometry-only grid/snap caches
  -> reach_hash
     -> score_hash
        -> render_hash
  -> config_hash
  -> build_key
```

Things that clearly feed this chain:

- geometry / coastal-cleanup params, including the preferred main-island shapefile sidecars when present
- scoring caps
- amenity tier-unit tables
- Overture dataset signature
- GTFS transit config hash
- importer config version
- schema / algorithm version constants

Important split: fine surface shell shards, grid cell shells, walk cell node snaps, and walk origin-node union caches are geo-derived geometry artifacts. Transit reality / GTFS-only changes still invalidate reach, score, render/build layers as needed, but do not move those geometry-only shell/snap cache paths. (Confirmed)

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
| `GTFS_NTA_ZIP_PATH` | Default zip path for `nta` static feed | `gtfs/nta_gtfs.zip` |
| `GTFS_TRANSLINK_ZIP_PATH` | Default zip path for `translink` static feed | `gtfs/translink_gtfs.zip` |
| `GTFS_NTA_URL`, `GTFS_TRANSLINK_URL` | Public GTFS ZIP URLs (override-able) | unset by default; set explicitly when refreshing from network |
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
| `NOISE_ROAD_GDB_CANONICAL_CACHE` | Enable ROI Round 4 Road canonical FileGDB -> local GPKG extraction cache path | `"1"` |
| `NOISE_REBUILD_ROAD_GDB_CACHE` | Force rebuild of canonical Road GDB GPKG cache before PG import | `"0"` |
| `NOISE_REBUILD_DEV_FAST_GRID` | Force rebuild of the deterministic dev-fast road/rail grid cache | `"0"` |
| `NOISE_DEV_FAST_ARROW_BATCH_SIZE` | Batch size for Arrow streaming in dev-fast road/rail reads; lowering reduces peak memory | `16384` |
| `NOISE_DEV_FAST_PARALLEL_SPECS` | Enable parallel dev-fast road/rail source processing | `"1"` |
| `NOISE_DEV_FAST_MAX_WORKERS` | Cap worker threads when `NOISE_DEV_FAST_PARALLEL_SPECS=1` | `2` |
| `NOISE_ROAD_NORMALIZE_BATCH_SIZE` | Batch size for Road raw-stage normalization by `source_fid` | `5000` |
| `NOISE_OGR2OGR_TIMEOUT_SECONDS` | Hard timeout for every `ogr2ogr` subprocess import (non-road defaults to 300s, small SHP can fail fast) | `300` |
| `NOISE_SQL_STATEMENT_TIMEOUT_SECONDS` | Per-normalize statement timeout for heavy SQL | `900` |
| `NOISE_SQL_LOCK_TIMEOUT_SECONDS` | Per-normalize lock timeout for heavy SQL | `30` |
| `NOISE_TERMINATE_STALE_IMPORT_BACKENDS` | Optional stale `pg_stat_activity` cleanup for prior crashed noise ingest sessions | `0` |
| `NOISE_TERMINATE_STALE_STAGE_LOCKS` | Optional pre-import termination of stale table lock holders on `_noise_raw_*` stage targets | `0` |

### Important config files

| File | Role |
|---|---|
| `config.py` | Canonical project config |
| `alembic.ini` | Alembic runner config |
| `osm2pgsql_livability.lua` | OSM tag filter / import rules |
| `environment.yml` | Conda-forge Windows GDAL/PostGIS runtime spec (`livability-gdal`) |
| `.env.example` | Local env template |
| `scripts/win/geo_env.cmd` | Windows CMD launcher that activates Miniforge env, pins GDAL/PROJ/noise env vars, and hydrates `NOISE_OGR2OGR_*` settings from project `.env` before applying script defaults |
| `scripts/win/bootstrap_geo_env.cmd` | Windows first-time setup wrapper: activates conda base, creates `%GEO_CONDA_ENV%` from `environment.yml` via mamba when missing, then runs env checks |
| `scripts/win/check_geo_env.cmd` | Windows GDAL driver sanity check wrapper (including PostgreSQL/PostGIS driver visibility) |
| `scripts/win/selftest_geo_env.cmd` | Windows post-check smoke script for `ogr2ogr` path, GDAL/PROJ env vars, PostgreSQL GDAL driver, and core Python imports |
| `scripts/win/precompute_dev.cmd` | Windows dev precompute wrapper that routes `python main.py precompute --profile dev` through `geo_env.cmd` and the conda-backed `livability-gdal` Python |
| `scripts/win/precompute_noise_dev.cmd` | Windows fast DevReuse wrapper via `run_noise_precompute_watchdog.ps1 -Mode DevReuse`; requires an existing mode-matched resolved artifact and never passes `--force-precompute` |
| `scripts/win/prepare_noise_artifact_dev.cmd` | Windows dev-fast cache-aware artifact refresh wrapper via `run_noise_precompute_watchdog.ps1 -Mode DevPrepare`; builds when missing/stale without forcing source reimport or amenities/grids |
| `scripts/win/prepare_noise_artifact_accurate.cmd` | Windows accurate cache-aware artifact refresh wrapper via `run_noise_precompute_watchdog.ps1 -Mode AccuratePrepare`; builds when missing/stale without forcing source reimport or amenities/grids |
| `scripts/win/force_noise_artifact_dev.cmd` | Windows dev-fast full artifact rebuild wrapper via `run_noise_precompute_watchdog.ps1 -Mode DevForce`; forces source reimport + resolved rebuild without forcing amenities/grids |
| `scripts/win/force_noise_artifact_accurate.cmd` | Windows accurate full artifact rebuild wrapper via `run_noise_precompute_watchdog.ps1 -Mode AccurateForce`; forces source reimport + resolved rebuild without forcing amenities/grids |
| `scripts/win/precompute_noise_accurate.cmd` | Windows fast AccurateReuse wrapper via `run_noise_precompute_watchdog.ps1 -Mode AccurateReuse`; requires an existing accurate resolved artifact |
| `scripts/win/test_noise.cmd` | Windows targeted noise test wrapper via `geo_env.cmd` |
| `pytest.ini` | Pytest collection scope and generated-directory exclusions |
| `frontend/package.json` | Frontend dependency and build scripts |

### Runtime assumptions

| Operation | Prerequisites |
|---|---|
| Any pipeline command | reachable PostGIS config |
| `import` | local OSM PBF + `osm2pgsql` available |
| `gtfs status` / `gtfs refresh` | network access to configured public static GTFS ZIP URLs (unless custom local-only URL/path config is used) |
| `transit` | cached GTFS ZIP(s) by default; add `--auto-refresh-gtfs` or `--force-gtfs-refresh` to download/update feeds + compiled `walkgraph` with `gtfs-refresh` support |
| `precompute` | managed schema ready, raw import ready or `--auto-refresh-import`, preferred `ireland_main_island_shp` coastline or fallback boundaries present, compiled `walkgraph` |
| `serve` | completed precompute build and main PMTiles archive for the chosen profile; noise PMTiles is optional and advertised only when available |
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
- `transit_derived.service_desert_cells` has a foreign key to `build_manifest`, and the precompute flow now writes those rows after `publish_precomputed_artifacts()` inserts the `build_manifest` row, so the FK is satisfied for the same `build_key`. (Confirmed)
- `extract_fingerprint()` now caches the exact `.osm.pbf` content hash in `.livability_cache/osm_extract_fingerprint_cache.json`, keyed by resolved path + file size + `mtime_ns`. Deleting or corrupting that cache only affects startup time; the code falls back to a full re-hash.
- `COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2` has a non-zero live default even though the nearby comment still talks about "default 0 = disabled". Trust the constant, not the stale comment.
- `overture/ireland_places.geoparquet`, `ireland_main_island_shp/*`, and `boundaries/*.geojson` are external inputs, not committed repo assets.
- Transit reality is GTFS-first now. Do not assume an OSM-stop-to-GTFS matching workflow still drives scoring.
- PMTiles bake writes to a sibling temp archive and only replaces the final `.pmtiles` after finalize succeeds; failed bakes clean the temp file and preserve the previous archive.
- The public noise overlay is intentionally not clipped to the walking/study-area land mask; noise can extend over water, while the walking grid and score PMTiles still use the exact coastline.
- `db_postgis._dependencies` patches Windows `platform` calls before SQLAlchemy import. If startup hangs again before `Preparing livability precompute (...)`, check WMI/platform calls before assuming database or PMTiles work has started.
- OSM import reuse is manifest/scope-aware: raw rows are only considered ready with a complete import manifest whose `normalization_scope_hash` matches the active profile. Raw rows without a matching manifest are dropped and rebuilt.
- Both Python and Rust GTFS parsers accept `calendar.txt`-only and `calendar_dates.txt`-only feeds, but still require at least one service calendar file.
- Root `pytest -q` is constrained by `pytest.ini` to `tests/` and excludes generated/local cache directories.
- Frontend click priority is now transport -> amenity -> service desert -> noise -> fine inspect -> coarse grid, with `frontend/src/click_priority.js` carrying the testable resolver.
- Noise datasets are local large inputs ignored by git; `noise_datasets/*.zip` should remain untracked. The loader intentionally fails loudly if the newer ROI Round 4 road FileGDB cannot be read.
- NI Round 1 shapefiles are class-coded (`GRIDCODE` 1..7 with `Noise_Cl` labels), not threshold-coded. Reusing threshold arithmetic on Round 1 creates invalid synthetic labels like `2-6`.
- Transport reality is intentionally sparse at low zoom in some NI tiles: for example, the Belfast z9 tile only carries a handful of hub-like features in the baked PMTiles archive, so a sparse screenshot there is not by itself proof that transport data is missing from the build.
- Noise artifact ingest now stages rows in a temp table (`noise_ingest_stage_*`) and then runs SQL geometry normalization (`ST_GeomFromWKB` -> `ST_Transform` -> `ST_MakeValid`) instead of building giant 500-row inline `VALUES` statements with huge WKB hex params.
- Noise force semantics are split: resolved rebuild (`--force-noise-artifact`) is separate from source re-import (`--reimport-noise-source`), and `--force-noise-all` does both.
- Accurate noise mode reads all available rounds and applies road/rail simplification only inside the dissolve CTE; canonical `noise_normalized` rows must not be updated in place.
- Dev-fast road/rail grid rows are cached under a deterministic grid artifact hash derived from source hash, grid size, latest-round metadata, and grid algorithm version; `NOISE_REBUILD_DEV_FAST_GRID=1` is the escape hatch.
- Artifact-mode proxy publish now enforces Phase E mixed-source behavior: road/rail emit from `noise_grid_artifact` with per-cell metric/band mapping, while airport/industry emit from active `noise_resolved_display` rows with snapped display bands. Missing grid artifact hash is still a hard `noise_proxy_blocked` state.
- `scripts/win/precompute_noise_dev.cmd` and `scripts/win/precompute_noise_accurate.cmd` are strict reuse wrappers: they require a prebuilt mode-matched artifact and fail fast when missing. `scripts/win/prepare_noise_artifact_dev.cmd` and `scripts/win/prepare_noise_artifact_accurate.cmd` are cache-aware refresh wrappers; use `scripts/win/force_noise_artifact_dev.cmd` or `scripts/win/force_noise_artifact_accurate.cmd` for full source reimport + resolved rebuild workflows.
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
| `tests/test_precompute_planner.py` | pure precompute import / noise-artifact / build planning decisions |
| `tests/test_precompute_cache.py` | tier cache read / write / invalidation helpers |
| `tests/test_pmtiles_bake.py` | tile field lists, layer metadata, amenity `tier` exposure, bounded parallel scheduling, retry behavior, temp-output cleanup, and old-archive preservation on failure |
| `tests/test_noise_loader.py` | ROI dB field normalization plus round-aware NI mapping (Round 1 class-code handling, Round 2/3 threshold mapping, unknown-code errors), and newest-round fallback geometry |
| `tests/test_noise_artifacts.py` | manifest SQL safety checks (`CAST(:error_detail AS text)`), ingest pre-validation diagnostics, streaming ingest behavior, deterministic dev-fast grid cache identity, and non-mutating accurate simplification |
| `tests/test_fine_vector_pmtiles_worker.py` | fine-grid shard aggregation, degenerate buffered-ring rejection, encoded per-zoom resolutions, mixed z15 resolutions, invalid-land skipping, buffered border-cell continuity, bounded worker LRU cache behavior across chunks |
| `tests/test_progress_tracker.py` | timing-history sanitization and persistence |
| `tests/test_serve_routes.py` | pure route matching and dispatch priority for runtime, inspect, surface, static, export, and PMTiles paths |
| `tests/test_server_behavior.py` | runtime API shape, transport subtier/mode count exposure, noise count/PMTiles URL exposure, surface/inspect input validation, streaming file responses, and range serving for main + noise PMTiles |
| `tests/test_transit_phase1.py` | GTFS-first transit reality rows, weekly bus subtiers, bus daytime headway buckets, frequency departure windows, transport score units, exact local GTFS snapshot stop regressions for each bus-tier bucket plus strict exception-only / unscheduled examples, exports, school-only classification, `gtfs-refresh` artifact loading |
| `tests/test_surface_runtime.py` | fine-surface runtime behavior |
| `tests/test_sanity_check.py` | sanity fixture structure and runtime lookup mode selection |
| `tests/test_db_constraint_migration.py` | read-only preflight, primary-key / foreign-key application, missing-table handling, and downgrade drop safety for the new constraint migration |
| `tests/test_db_integrity_check.py` | duplicate detection, NULL key validation, missing-table handling, and ambiguous-key skipping for future constraint preflight |
| `frontend/src/runtime_contract.test.js` | frontend runtime contract parsing, score-layer label/expression mapping, separate noise vector source wiring, active fill/outline grid layer definitions, lifecycle rebuild decisions, explicit visibility plans, the transport rail/tram styling priority, and the single active debug-grid filter path |
| `frontend/src/grid_debug.test.js` | persistent grid debug card rendering, diagnosis states, resolution display updates, and copyable snapshot formatting |
| `frontend/src/map_source_guard.test.js` | MapLibre source guard behavior so missing `livability` sources are treated as not loaded without calling `isSourceLoaded()` and emitting the "no source with ID" error |
| `frontend/src/transport_filters.test.js` | public transport filter logic including weekly bus tiers, exact rail/tram mode matching, and exception-only intersection logic |
| `frontend/src/noise_filters.test.js` | noise metric/source/band options and MapLibre filter expression construction |
| `frontend/src/noise_proxy_controls.test.js` | noise overlay control wiring, opacity slider range, synced fill/outline opacity paint updates, and default/reset opacity handling |
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

Local disk hotspots to remember:

- `osm/flat-nodes.bin` is the dominant single file when the OSM import cache is present, and `.livability_cache/` can also grow large from rebuildable PMTiles and noise/geo caches. Both are generated state, not source.

---

## 12. Glossary

| Term | Meaning here |
|---|---|
| Grid cell | Scored polygon unit at a given resolution |
| Coarse vector resolution | `20000`, `10000`, or `5000` meter grid published to PMTiles |
| Fine vector resolution | `2500`, `1000`, `500`, `250`, `100`, `50` meter grid features baked into PMTiles and overzoomed at runtime |
| PMTiles | Single-file vector tile archive served over HTTP range requests; this app now writes separate main livability and optional noise archives |
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
| Geo hash | Geometry/import-level cache hash used for study area, walkgraph, fine surface shell, and geometry-only grid/snap caches |
| Surface shell hash | Fine-surface shell identity derived from `geo_hash` plus shell/grid schema and geometry parameters |
| Reach hash | Reachability-level hash including geo hash, transit reality, tags, merge version, and Overture signature |
| Score hash | Scoring-level hash including reach hash, caps, tier-unit tables, and resolution tiers; owns scored walk cells and fine surface score arrays |
| Build key | Build-scoped identifier used on published PostGIS rows |
| Build profile | `full`, `dev`, or `test` |

---

## 13. Recommended Reading Order

1. `config.py`
2. `main.py`
3. `db_postgis/tables.py`
4. `precompute/workflow.py`
5. `precompute/_planning.py`
6. `precompute/__init__.py`
7. `progress_tracker.py`
8. `precompute/phases.py`
9. `precompute/amenity_tiers.py`
10. `pmtiles_bake_worker.py`
11. `precompute/bake_pmtiles.py`
12. `noise/loader.py`
13. `serve_from_db.py`
14. `frontend/src/runtime_contract.js`
15. `frontend/src/main.js`
16. `frontend/src/noise_filters.js`
17. `overture/loader.py`
18. `overture/merge.py`
19. `scripts/sanity_check.py`
20. `tests/test_precompute_behavior.py`
21. `tests/test_precompute_planner.py`
22. `tests/test_pmtiles_bake.py`
23. `tests/test_noise_loader.py`
24. `tests/test_amenity_tiers.py`
23. `README.md` and `docs/*.md` if you need product / methodology context
### `noise_artifacts/ingest.py`

- Purpose: canonical noise source ingest entrypoint for artifact pipeline with mode dispatch.
- Supports `NOISE_INGEST_MODE=auto|ogr2ogr|python`; `auto` prefers `ogr2ogr`, otherwise falls back to Python COPY staging.
- Python path now uses one run-scoped temp staging table (`noise_ingest_stage`) with `geom_wkb BYTEA`, COPY-to-stage, configurable copy window (`NOISE_INGEST_COPY_BATCH_ROWS`) and flush window (`NOISE_INGEST_FLUSH_ROWS`), then `INSERT ... SELECT` normalization into `noise_normalized`.
- Normalization keeps strict band validation (`NN-NN` / `NN+`), includes NI gridcode context in errors, and avoids giant inline SQLAlchemy `VALUES` statements.

### `noise_artifacts/ogr_ingest.py`

- Purpose: high-throughput raw source import path using GDAL `ogr2ogr` direct to PostGIS staging tables.
- Includes `ogr2ogr_available()`, command builder, source ZIP extraction cache (`.livability_cache/noise_gdal`), per-layer import timing, field discovery (`pyogrio` with `fiona` fallback), case-insensitive ROI/NI allowlist field selection, geometry-metadata denylisting (`shape_*` variants), `--config PG_USE_COPY YES`, `-preserve_fid`, `-lco PRECISION=NO`, selective `-makevalid` use (kept on normal import/fallback paths, removed from ROI Road FileGDB -> canonical GPKG extract), and SQL normalization into `noise_normalized`.
- Road FileGDB canonical extraction now requires explicit non-empty `selected_fields`; full-field canonical extracts are rejected to avoid runaway FileGDB reads.
- ROI Round 4 Road FileGDB now uses a canonical three-phase path: extract once to local GPKG cache (`road_raw`), import GPKG to one PostgreSQL stage table with `PG_USE_COPY`, then normalize from that stage (optional `source_fid` batches) without direct FileGDB `fid` chunk imports.
- Road chunk failure cleanup now rolls back the current transaction before best-effort chunk-table drops, preventing `InFailedSqlTransaction` cascades during cleanup.
- All `ogr2ogr` subprocesses now run with enforced non-`None` hard timeouts, enhanced heartbeat diagnostics (`pid`, elapsed, timeout, last output age, context), and stronger interrupt/timeout cleanup paths that terminate active processes and process trees on Windows; ROI Road canonical FileGDB extraction disables the no-progress watchdog and relies on hard-timeout only.
- Stage imports now preflight stale backend and lock diagnostics, commit stage `DROP TABLE` before external `ogr2ogr` runs, and can optionally terminate stale blocking sessions via env toggles.
- Command building now blocks `-append` + `-select` combinations with an internal ingest error guard.
- NI normalization in this path still calls verified round-aware NI gridcode mapping logic; unknown class/threshold codes raise explicit errors with source context.
