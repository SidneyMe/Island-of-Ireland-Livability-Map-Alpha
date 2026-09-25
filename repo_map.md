# Repo Map

> Refreshed: **2026-09-25** on branch `beta`. Evidence grades used below: **Confirmed** = read directly from code; **Inference** = strongly suggested but not explicitly proven; **Unclear** = cannot be determined from the repo alone.
> Line counts in §0.7 are non-blank line counts measured 2026-09-25. Line-number anchors elsewhere are dated hints, not contracts — prefer symbols + `rg`.

---

## 0. Agent quick start (read this first)

### 0.1 How to use this file

- Read §0 only, then jump to the one section you need. §6 = per-module reference, §7 = "change these together" contracts, §8 = env/config surface, §9 = traps, §10 = test map, §12 = glossary.
- Grep before opening. §0.4 lists the symbols that answer most "where is X implemented?" questions in one search.
- Section 0.3 answers most tasks outright: read the named files, skip everything else.
- If you change behaviour, update the rows you invalidated (§0.3 / §0.4 / §6 / §9 / §10) in the same commit. A stale map costs the next agent more than no map.
- Do not restate this file into new docs. Fix this file instead.

### 0.2 Repo shape (one screen)

```text
main.py                    CLI dispatcher: import | basemap | gtfs (status|refresh) | transit | precompute | serve
config.py                  every constant + the hash/invalidation spine; imported almost everywhere
local_osm_import/          osm2pgsql raw OSM import (orchestrator.py, osm2pgsql.py, rules.py)
osm2pgsql_livability.lua   OSM tag filter (parks / landuse / roads). Contains no transport tags at all.
transit/                   GTFS download+cache, matching/classification, walkgraph gtfs-refresh, rail corridors
walkgraph/                 Rust binary: build, reachability, stats, surface, gtfs-refresh
precompute/                planner + phases + scoring + caches + PMTiles bake orchestration
network/                   Python walkgraph graph-index loader. NOT precompute/network.py (reachability math).
db_postgis/                Alembic schema, SQLAlchemy tables, reads, writes, manifests, engine
noise/ + noise_artifacts/  display-only noise overlay: legacy loader + artifact pipeline (ingest/resolve/bake)
overture/                  optional Overture Places loader + OSM/Overture merge rules
serve_from_db.py           runtime HTTP server: PMTiles ranges, /api/*, PNG surface tiles, static, exports
serve_routes.py            pure route matching / dispatch priority, unit-tested without HTTP
frontend/src/              MapLibre app as small modules; static/dist/ is the built, checked-in bundle
scripts/                   sanity_check, db_integrity_check, refresh_osm, bench_valhalla, ci_local, win/*.cmd
tests/ (37 files)          Python unittest/pytest suite; frontend/src/*.test.js (12 files) is the Node suite
docs/                      PHASES.md (design), notes.md (authority/methodology), BASEMAP.md
AGENTS.md                  always-loaded agent instructions; repo_map.md (this file) is the deep map
```

### 0.3 Task router — read this much, skip the rest

| If the task is … | Read (in order) | Usually skip |
|---|---|---|
| Scoring math: caps, decay, units, total score | `config.py` (`CAPS`, `DISTANCE_DECAY_HALF_DISTANCE_M`, `*_TIER_UNITS`, `SCORING_MODEL_VERSION`) → `precompute/grid.py` (`score_cell`, `score_cells`, `_normalized_area_ratio`) → `precompute/amenity_tiers.py` → `tests/test_amenity_tiers.py`, `tests/test_mode_aware_scoring.py` | walkgraph, noise, frontend |
| Add / change an amenity category, tier, or OSM tag | `config.py` (`TAGS`, tier-unit tables) → `osm2pgsql_livability.lua` → `db_postgis/reads.py` (`load_source_amenity_rows`, `_AMENITY_CATEGORY_VALUES`) → `precompute/amenity_tiers.py` → new migration → `pmtiles_bake_worker.py` (if it reaches tiles) | transit, noise |
| Hidden penalties: railway proximity / road proximity | `config.py` (`RAILWAY_PROXIMITY_*`, `ROAD_PROXIMITY_CLASS_SETTINGS`) → `transit/railway_corridors.py` → `precompute/road_proximity.py` → `precompute/grid.py` (deduction) → `tests/test_road_proximity.py` | frontend, noise |
| Transport scoring / GTFS frequency tiers | `transit/service.py` (tier units) → `walkgraph/src/gtfs/mod.rs` (`bus_frequency_tier_from_headway`, `transport_score_units_from_frequency`, `classify_services`) → `precompute/phases.py` (`phase_amenities_impl` transport split) → `db_postgis/reads.py` (`load_transport_reality_rows_for_scoring`) → `tests/test_transit_phase1.py` | physics of frontend styling |
| "Build published zero transport" / publish gate | `precompute/workflow.py` (search `No GTFS transport rows`) → `main.py` (`--allow-missing-transport`) → `precompute/__init__.py` (`run_precompute`) → `tests/test_precompute_behavior.py` (gate tests) | scoring internals |
| GTFS download / cache freshness | `transit/gtfs_download.py` → `transit/sources.py` → `config.py` (`transit_feed_configs`, `GTFS_*`) → `README.md` GTFS section → `tests/test_gtfs_download.py` | precompute phases |
| Transit reality rebuild (stops, masks, deserts) | `transit/workflow.py` (`ensure_transit_reality`) → `transit/rust_gtfs.py` (`run_walkgraph_gtfs_refresh`) → `walkgraph/src/gtfs/mod.rs` → `transit_refresh_runner.py` → `tests/test_transit_phase1.py`, `tests/test_transit_refresh_runner.py` | noise, frontend |
| Overture merge / dedupe | `overture/loader.py` → `overture/merge.py` → `db_postgis/amenity_merge.py` → `config.py` (`AMENITY_MERGE_ALGO_VERSION`) → `tests/test_overture_loader.py` | everything else |
| Noise overlay (display only) | `noise/loader.py` (legacy path) or `noise_artifacts/{runner,builder,materialize,resolve,dissolve,bake}.py` → `db_postgis/write_noise.py` → `frontend/src/noise_filters.js` → `docs/notes.md` → `tests/test_noise_artifacts.py`, `tests/test_noise_loader.py` | scoring, transport |
| Noise ingest performance / ogr2ogr / FileGDB | `noise_artifacts/ogr_ingest.py` → `noise_artifacts/ingest.py` → `scripts/win/*noise*.cmd` → §8 env rows `NOISE_*` | frontend |
| Precompute planner: skip vs rebuild | `precompute/_planning.py` → `precompute/workflow.py` (`_plan_precompute_build`) → `python main.py precompute --profile dev --explain` → `tests/test_precompute_planner.py` | phase bodies |
| Cache tiers, hashes, invalidation | `config.py` (`build_config_hashes`, `*_hash`, `build_key`) → `precompute/tiers.py` → `precompute/cache.py` → `precompute/_cache_wrappers.py` → `tests/test_precompute_cache.py`, `tests/test_config.py` | scoring internals |
| Reachability math (Python side) | `precompute/network.py` → `precompute/reachability_arrays.py` → `precompute/phases.py` (`phase_reachability_impl`) | Rust internals unless perf |
| Reachability / graph perf (Rust) | `walkgraph/src/reachability.rs` → `walkgraph/src/graph.rs` → `walkgraph/src/pbf.rs` → `walkgraph/src/surface/mod.rs` | Python pipeline |
| Walk/bike graph build and filters | `walkgraph/src/pbf.rs` (`WALK_EXCLUDED`, `BIKE_EXCLUDED`) → `walkgraph/src/graph.rs` → `precompute/phases.py` (`_build_or_load_bike_graph`) | transit GTFS |
| Fine surface, `/api/inspect`, PNG tiles | `precompute/surface.py` (`FineSurfaceRuntime`) → `fine_vector_pmtiles_worker.py` → `serve_from_db.py` (`/api/inspect`) → `tests/test_surface_runtime.py`, `tests/test_fine_vector_pmtiles_worker.py` | noise |
| PMTiles bake: new field / layer / zoom | `precompute/bake_pmtiles.py` (`_pmtiles_metadata`) → `pmtiles_bake_worker.py` → `tests/test_pmtiles_bake.py` (+ `fine_vector_pmtiles_worker.py` if fine grid) | runtime server |
| Frontend layer, control, popup | `frontend/src/` (small modules; start `runtime_contract.js` or the matching `*.js`) → matching `*.test.js` → rebuild `static/dist/` | Python pipeline |
| Runtime HTTP API / routes / caching | `serve_from_db.py` (`RuntimeState`) → `serve_routes.py` → `frontend/src/runtime_contract.js` → `tests/test_server_behavior.py`, `tests/test_serve_routes.py` | precompute |
| Schema change | §7 "Schema change" list → `db_postgis/migrations/versions/` (head `000023`) → `tests/test_db_constraint_migration.py`, `scripts/db_integrity_check.py` | frontend unless surfaced |
| OSM raw import / importer rules | `local_osm_import/*` → `osm2pgsql_livability.lua` → `config.py` (`IMPORTER_CONFIG_VERSION`) → `db_postgis/{reads,writes}.py` → `tests/test_osm_import_handling.py`, `tests/test_local_osm_rules.py` | transit, frontend |
| Self-hosted basemap | `basemap.py` → `basemap/docker-compose.yml` → `docs/BASEMAP.md` → `config.py` (`PROTOMAPS_*`) → `tests/test_basemap.py` | pipeline internals |
| Windows GDAL / noise execution | `AGENTS.md` → `scripts/win/*.cmd`, `scripts/win/run_noise_precompute_watchdog.ps1` → §8 | raw `python main.py` |
| Server / deploy / Linux runbook | `README.md` → §0.5 commands → `serve_from_db.py` → `static/index.html` | migrations history |
| "How do I verify X?" | §10 test map, then §0.5 command cheatsheet | — |

### 0.4 Symbol index — grep these instead of browsing

| Symbol / search string | Lives in |
|---|---|
| `build_config_hashes`, `geo_hash`, `reach_hash`, `score_hash`, `render_hash`, `config_hash`, `build_key` | `config.py` |
| `importer_config_hash`, `transit_config_hash`, `current_normalization_scope_hash`, `package_snapshot` | `config.py` |
| `IMPORTER_CONFIG_VERSION` = `"2026-08-31-gtfs-only-transport-1"` (l.56), `SCORING_MODEL_VERSION` = `"v3-gtfs-only-transport-2026-08-31"` (l.453) | `config.py` |
| `TAGS`, `CAPS`, `WALK_RADIUS_M`, `TRANSIT_ACCESS_WALK_RADIUS_M`, `TRANSIT_EGRESS_WALK_RADIUS_M` | `config.py` |
| `TRANSIT_REALITY_ALGO_VERSION` (10), `AMENITY_MERGE_ALGO_VERSION` (4), `WALKGRAPH_FORMAT_VERSION` (3) | `config.py` |
| `FINE_SURFACE_SCHEMA_VERSION` (2), `PMTILES_SCHEMA_VERSION` (13), `GRID_GEOMETRY_SCHEMA_VERSION` (4), `CACHE_SCHEMA_VERSION` (14) | `config.py` |
| `PROTOMAPS_BASEMAPS_COMMIT`, `PROTOMAPS_ASSETS_COMMIT`, `PROTOMAPS_TILE_SCHEMA_MAJOR`, `BASEMAP_FONT_STACKS`, `BASEMAP_CACHE_DIR` | `config.py` |
| `OSM_EXTRACT_PATH`, `GTFS_DIR`, `BOUNDARIES_DIR`, `ROI_BOUNDARY_PATH`, `NI_BOUNDARY_PATH`, `MAIN_ISLAND_BOUNDARY_PATH`, `CACHE_DIR` | `config.py` |
| `resolution_for_zoom`, `profile_fine_surface_enabled`, `normalize_build_profile`, `pmtiles_output_path` | `config.py` |
| `run_precompute`, `refresh_local_import`, `refresh_transit`, `_compute_service_deserts`, `_preflight_transit_rebuild` | `precompute/__init__.py` |
| `run_precompute_impl`, `run_import_refresh_impl`, `_plan_precompute_build`, `_run_pmtiles_bake` | `precompute/workflow.py` |
| `No GTFS transport rows` (publish gate), `--allow-missing-transport` | `precompute/workflow.py`, `main.py` |
| `plan_precompute`, `plan_import`, `plan_build`, `plan_noise_artifact`, `format_precompute_plan`, `PrecomputeContext` | `precompute/_planning.py` |
| `_BuildState`, `_STATE.activate`, `_active_fine_surface_enabled` | `precompute/_state.py` |
| `phase_geometry_impl`, `phase_amenities_impl`, `phase_networks_impl`, `phase_reachability_impl`, `phase_grids_impl` | `precompute/phases.py` |
| `grid_cells_cache_key`, `walk_cell_nodes_cache_key`, `walk_origin_nodes_cache_key`, `_build_or_load_bike_graph` | `precompute/phases.py` |
| `score_cell`, `score_cells` | `precompute/grid.py` |
| `classify_amenity_row`, `annotate_amenity_row`, `uses_weighted_units` | `precompute/amenity_tiers.py` |
| `build_amenity_clusters` | `precompute/amenity_clusters.py` |
| `SCORE_MODES`, `mode_available`, mode-aware weight helpers | `precompute/mode_aware.py` |
| `FineSurfaceRuntime`, `inspect` | `precompute/surface.py` |
| `write_tier_manifest`, `mark_building`, `mark_complete` | `precompute/tiers.py` |
| `bake_pmtiles`, `_pmtiles_metadata`, `GRID_AMENITY_CATEGORIES` | `precompute/bake_pmtiles.py` |
| `bake_noise_pmtiles` | `noise_artifacts/bake.py` |
| `_resolution_for_zoom` (coarse worker only), `_bake_chunk_worker`, `_tile_mvt_bytes_by_flags` | `pmtiles_bake_worker.py` |
| `precompute_walk_decayed_units_matrix_by_origin_node`, `precompute_walk_counts_by_origin_node`, `precompute_counts_by_node`, `normalize_origin_node_ids`, `merge_normalized_origin_node_ids`, `snap_amenities`, `nearest_nodes` | `precompute/network.py` |
| `load_walk_graph_index`, `WalkGraphIndex` | `network/loader.py` (root `network/` package) |
| `run_walkgraph_gtfs_refresh`, `load_gtfs_stop_reality_models` | `transit/rust_gtfs.py` |
| `ensure_transit_reality`, `transit_reality_refresh_required` | `transit/workflow.py` |
| `refresh_gtfs_feed`, `refresh_gtfs_feeds`, `ensure_transit_feed_available` | `transit/gtfs_download.py` |
| `BUS_FREQUENCY_TIER_UNITS` | `transit/service.py` |
| `railway_proximity_hash_for_state`, `build_railway_corridor_materialization`, `compute_railway_proximity_penalties`, `railway_proximity_penalty_for_distance` | `transit/railway_corridors.py` |
| `parse_maxspeed_kmh`, `road_penalty_for_distance`, `compute_road_proximity_penalties` | `precompute/road_proximity.py` |
| `RuntimeState`, `LivabilityRequestHandler`, `serve_livability_app` | `serve_from_db.py` |
| route kind table (`ROUTE_*`/kind names: root, export, pmtiles, surface_tile, static, api_runtime, api_inspect) | `serve_routes.py` |
| `ensure_database_ready`, `import_payload_ready`, `raw_import_ready` | `db_postgis/schema.py` |
| `load_source_amenity_rows`, `load_transport_reality_rows_for_scoring`, `load_transport_reality_points`, `load_road_rows`, `load_landuse_context_rows` | `db_postgis/reads.py` |
| `_AMENITY_CATEGORY_VALUES` (excludes transport) | `db_postgis/reads.py` |
| `publish_precomputed_artifacts`, `refresh_noise_overlay_for_build` | `db_postgis/writes.py` |
| `replace_service_desert_rows` | `db_postgis/write_transit.py` |
| import/transit/build manifest read+completion helpers | `db_postgis/manifests.py` |
| `build_engine` | `db_postgis/engine.py` (pool sizing lives here) |
| `_dependencies` platform shim | `db_postgis/_dependencies.py` |
| `ensure_local_osm_import`, `resolve_source_state` | `local_osm_import/__init__.py` |
| `clean_coastal_artifacts`, `load_island_geometry_metric` | `study_area.py` |
| `build_basemap`, `build_if_stale`, `is_stale`, `docker_command`, `validate_vendored_assets` | `basemap.py` |
| `refresh_gtfs`, `gtfs_status`, `refresh_transit` | `transit_refresh_runner.py` |
| `load_walk_graph_index` callers / walkgraph subcommand availability | `walkgraph_support.py` |
| `_workflow_kwargs`, `_amenity_data_with_transport` (test helpers — reuse, don't rebuild fixtures) | `tests/test_precompute_behavior.py` |
| bus subtier / headway / score-unit derivation | `walkgraph/src/gtfs/mod.rs` |
| fine-grid resolution ladder + MVT encoding | `fine_vector_pmtiles_worker.py` |

### 0.5 Command cheatsheet

```bash
# --- tests and validation (narrowest first) ---
python -m pytest -q tests/test_config.py            # Linux/mac; pytest.ini scopes to tests/
python -m unittest tests.test_config -v             # unittest equivalent (what CI runs)
cargo test --manifest-path walkgraph/Cargo.toml     # Rust
npm test --prefix frontend                          # Node suite (12 files)
python scripts/sanity_check.py --validate-only      # fixture structure only
python scripts/sanity_check.py --profile full       # fixture vs live build
python scripts/db_integrity_check.py                # read-only duplicate/NULL preflight

# --- pipeline ---
python main.py precompute --profile dev --explain   # planner decision only, fast, no side effects
python main.py precompute --profile dev             # cheap iteration profile (coarse only)
python main.py precompute --profile full --auto-refresh-import
python main.py precompute --profile full --force-precompute
python main.py transit --auto-refresh-gtfs          # refresh feeds then transit reality
python main.py gtfs status                          # cache freshness diagnostics (no download)
python main.py gtfs refresh --force-gtfs-refresh
python main.py basemap --force                      # self-hosted Protomaps archive (needs Docker)
python main.py serve                                # default 127.0.0.1:8000
python main.py serve --deployment                   # fail early unless basemap archive+assets exist

# --- database ---
python -m alembic current && python -m alembic upgrade head

# --- frontend ---
npm run build --prefix frontend && git diff --exit-code -- static/dist   # bundle must be committed

# --- discovery ---
rg -n "symbol_name" --glob '*.py'                   # symbol search beats opening files
```

Windows-specific (GDAL/noise/conda — never raw `python main.py` for these):

```text
scripts\win\geo_env.cmd .\.venv\Scripts\python.exe -m unittest tests.test_config -v
scripts\win\precompute_dev.cmd                      # dev precompute through geo_env
scripts\win\precompute_noise_dev.cmd                # noise reuse-only (never builds)
scripts\win\prepare_noise_artifact_dev.cmd          # cache-aware artifact refresh
scripts\win\force_noise_artifact_accurate.cmd       # source reimport + resolved rebuild
scripts\win\test_noise.cmd                          # targeted noise tests
scripts\ci_local.ps1                                # full local pre-push sequence

# refresh the file-size table (§0.7) — non-blank line counts
Get-ChildItem -Recurse -File -Include *.py,*.js,*.rs,*.md | Where-Object { $_.FullName -notmatch '\\(\.git|\.venv|\.livability_cache|node_modules|target|static\\dist)\\' } | ForEach-Object { "{0,6}  {1}" -f (Get-Content -LiteralPath $_.FullName | Measure-Object -Line).Lines, $_.FullName.Replace((Get-Location).Path + '\','') } | Sort-Object -Descending
```

### 0.6 Inputs you must have (all gitignored, none committed)

| Input | Exact path | Used by | If missing |
|---|---|---|---|
| OSM extract | `osm/ireland-and-northern-ireland-latest.osm.pbf` | import, transit fingerprint, basemap | import fails / `--auto-refresh-import` needed |
| osm2pgsql flat nodes | `osm/flat-nodes.bin` (generated, ~37 GiB) | import cache | regenerated (slow) |
| GTFS feeds | `gtfs/nta_gtfs.zip`, `gtfs/translink_gtfs.zip` (or `GTFS_NTA_ZIP_PATH` / `GTFS_TRANSLINK_ZIP_PATH` / `GTFS_*_URL`) | transit reality, transport scoring | **zero transport → publish gate blocks the build** |
| ROI boundary | `boundaries/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson` | study area fallback | fallback path unavailable |
| NI boundary | `boundaries/osni_open_data_largescale_boundaries_ni_outline.geojson` | study area fallback | fallback path unavailable |
| Main-island coast (preferred) | `ireland_main_island_shp/ireland_main_island.shp` + `.shx/.dbf/.prj/.cpg` | exact study-area mask | falls back to ROI+NI merge + coastal cleanup |
| Overture Places (optional) | `overture/ireland_places.geoparquet` (+ `.state`) | amenity merge | degrades gracefully |
| Noise datasets (optional) | `noise_datasets/*.zip` (6 rounds) | display-only overlay | overlay absent, scoring unaffected |

Local trap: the repo root contains an **empty directory** named `ireland-and-northern-ireland-latest.osm.pbf/`. It is not the PBF. The real extract lives under `osm/`.

### 0.7 Expensive files — do not read whole (non-blank lines, 2026-09-25)

| File | Lines | How to read it instead |
|---|---|---|
| `tests/test_precompute_behavior.py` | 4061 | Reuse `_workflow_kwargs` (~l.227) and `_amenity_data_with_transport` (~l.180); read only the class around your change |
| `noise_artifacts/ogr_ingest.py` | 2897 | Grep `^def ` or `NOISE_` env names; never read top-to-bottom |
| `frontend/src/main.js` | 1700 | Bootstrap/map wiring only; UI logic lives in sibling modules |
| `precompute/phases.py` | 1394 | Jump straight to `phase_*_impl` |
| `config.py` | 1376 | Grep the symbol; constants only |
| `serve_from_db.py` | 1310 | Grep `RuntimeState` / route names |
| `precompute/surface.py` | 1238 | Grep `FineSurfaceRuntime` |
| `repo_map.md` | 1048 | This file — read §0 and the one section you need; never end-to-end |
| `precompute/workflow.py` | 927 | Read the plan-execution branch you need |
| `study_area.py` | 864 | Grep `clean_coastal_artifacts` / `load_island_geometry_metric` |
| `precompute/network.py` | 815 | Dense math; grep the `precompute_walk_*` entry point |
| `precompute/bake_pmtiles.py` | 763 | Grep `_pmtiles_metadata` / planner helpers |
| `fine_vector_pmtiles_worker.py` | 626 | Grep the resolution ladder |
| `db_postgis/tables.py` | 619 | Grep the table name |
| `noise/loader.py` | 600 | Grep the metric/jurisdiction branch |
| `progress_tracker.py` | 577 | Grep `PrecomputeProgressTracker` |
| `db_postgis/schema.py` | 573 | Grep `_ready` helpers |
| `db_postgis/reads.py` | 564 | Grep the loader function |
| `precompute/reachability_arrays.py` | 554 | Grep the format constants |
| `db_postgis/writes.py` | 534 | Grep `publish_precomputed_artifacts` |
| `noise_artifacts/ingest.py` | 533 | Grep `NOISE_INGEST_*` |
| `pmtiles_bake_worker.py` | 486 | Small enough, but keep it import-light |

Also large: `precompute/__init__.py` 492, `precompute/_rows.py` 477, `noise_artifacts/builder.py` 446, `precompute/publish.py` 443, `README.md` 410, `precompute/valhalla_reachability.py` 405, `precompute/grid.py` 397, `schema.sql` 389 (snapshot, do not treat as truth), `precompute/tiers.py` 364, `CHANGELOG.md` 358, `precompute/cache.py` 262, `precompute/_planning.py` 240, `basemap.py` 211, `precompute/amenity_tiers.py` 199, `mapbox_vector_tile.py` 173 (root module — grep its importers before assuming the PyPI package), `osm2pgsql_livability.lua` 153, `transit_refresh_runner.py` 152, `precompute/road_proximity.py` 146, `db_postgis/manifests.py` 144, `precompute/amenity_clusters.py` 127, `precompute/mode_aware.py` 122, `walkgraph_support.py` 104, `serve_routes.py` 60. Refresh all sizes with the command in §0.5.

### 0.8 Never read / never edit

| Path | Why |
|---|---|
| `static/dist/*` | Checked-in build output; rebuild from `frontend/src/`, never hand-edit |
| `.livability_cache/**` | Generated caches, manifests, PMTiles, timing history |
| `walkgraph/target/**` | Cargo output |
| `schema.sql` | Stale bootstrap snapshot; Alembic is canonical |
| `CHANGELOG.md` | History only; read the top entry at most |
| `db_postgis/migrations/versions/*` except head/relevant one | 23 migrations; read `000023` and the file you are changing |
| `osm/flat-nodes.bin`, `osm/*.pbf` | ~37 GiB generated/binary |
| `noise_datasets/*.zip`, `overture/*.geoparquet`, `gtfs/*.zip` | Large local inputs |
| `legacy/`, `data/`, `reports/`, `.agents/`, `.test_logs/`, `__pycache__`, `.mypy_cache`, `.uv-cache`, `.uv-python` | Local/generated/empty; never runtime truth |
| `render_from_db.py` | 27-line compatibility wrapper around `serve_from_db.py` |

### 0.9 Top traps that cost the most time (full list in §9)

1. Two different `network` modules: the root `network/` **package** (`load_walk_graph_index`) vs `precompute/network.py` (reachability math). `from network import …` = the package.
2. The empty root directory named like the OSM PBF (§0.6).
3. `python main.py import` also runs `basemap.build_if_stale()` → **requires Docker**. On a Docker-less server use `python main.py precompute --auto-refresh-import` instead.
4. `precompute` refuses to publish a build with **zero GTFS transport**; only `--allow-missing-transport` overrides it.
5. The noise overlay is **display-only** and never scored. Penalising noise is new work, not a bug fix.
6. Mode-aware transit is always unavailable: `phase_reachability_impl` passes `transit_effective_units_by_node=None`. Walk and bike are real; transit is a stub.
7. `HASHES = build_config_hashes()` and `_STATE = _BuildState.bootstrap()` run at **import time**; missing inputs hash as zero-like metadata instead of raising, which silently reuses caches.
8. `pmtiles_bake_worker._resolution_for_zoom()` is **not** `config.resolution_for_zoom()`.
9. `pmtiles_bake_worker.py` must stay import-light (Windows spawn); fine-grid work belongs in `fine_vector_pmtiles_worker.py`.
10. Transport scoring is strictly GTFS-only: `TAGS["transport"] = {"source": "gtfs_direct"}`, `_AMENITY_CATEGORY_VALUES` excludes transport, and OSM stop helpers were deleted (`load_osm_transport_features`, `clear_normalized_network_rows`, `transit/chained_reach.py` no longer exist — do not import them).

---

## 1. Project Purpose

- Offline-first pipeline that scores grid cells across the island of Ireland for livability using walk access to `shops`, `transport`, `healthcare`, and `parks`. (Confirmed)
- Shops, healthcare, and parks use tiered score units instead of flat presence counts: corner shop vs supermarket, clinic vs emergency hospital, pocket park vs regional park. (Confirmed)
- Ingests local OSM PBF via `osm2pgsql`, cache-managed public static GTFS ZIP feeds (default active transit inputs: `nta` and `translink`), and optionally an Overture Places geoparquet dataset. (Confirmed)
- Runs heavy work ahead of time: geometry prep → amenity load/merge → Rust walkgraph build → reachability → grid scoring → PMTiles bake. (Confirmed)
- Publishes results to PostGIS plus a main livability PMTiles archive and an optional separate noise PMTiles overlay, so the frontend runs without live tile SQL. (Confirmed)
- Builds a strictly GTFS-only transit reality layer: bus daytime frequency tiers, explicit rail/tram mode tiers, frequency-weighted transport scoring, a hidden railway-track proximity modifier derived from GTFS shapes, and a service-desert overlay from scheduled departures. OSM transport stops are neither imported nor used as a scoring fallback. (Confirmed)
- Zero-transport builds are gated: `precompute` raises unless transport rows exist or `--allow-missing-transport` is passed. GTFS feeds are therefore a mandatory input, not optional. (Confirmed)
- Adds a dual-score mode-aware scoring payload: walk-only `total_score` and top-level category scores remain the map default, while nested `scores_json["mode_aware"]` and fine-surface inspect payloads can expose walk/bike/transit weighted comparison data with model version and mode weights. (Confirmed)
- Applies a hidden, explainable road-proximity penalty from OSM `motorway`, `trunk`, and `primary` ways, with class-specific distance decay and safe maxspeed weighting; strongest nearby-road selection avoids junction double-counting. (Confirmed)
- Adds a display-only transport/industry noise overlay (roads + rail + airport + industry, Lden/Lnight) calibrated from official-derived strategic noise data; road/rail use grid proxy rows while airport/industry use resolved official-derived polygons. This does not feed livability scoring. (Confirmed)
- Adds a display-only land-use context overlay from OSM `landuse` polygons (`residential`, `commercial`, `industrial`, `retail`, `farmland`, `forest`) as a lower-zoom contextual layer, baked and rendered from z5 through z11; not used in livability scoring. (Confirmed)
- Ships a self-hosted Protomaps basemap so the app has no third-party tile dependency; source profile/style are pinned by upstream commit SHA and vendored glyph/sprite assets live under `static/basemap/`. (Confirmed)
- Uses layered content hashes so changes to geometry, scoring params, GTFS feeds, Overture data, or importer config only invalidate the affected cache tiers. (Confirmed)
- Alpha-stage: amenity tiering, Overture merge, service deserts, and the fine vector grid / inspect-backed surface path are still moving. (Inference from recent migrations, tests, and docs)

---

## 2. High-Level Architecture

- **CLI dispatcher**: `main.py`
- **Config and hash spine**: `config.py`
- **Schema and DB IO**: `db_postgis/`
- **OSM ingest**: `local_osm_import/` + `osm2pgsql_livability.lua`
- **GTFS ingest and transit reality**: `transit/`
- **GTFS rail-corridor proxy**: `transit/railway_corridors.py`
- **Major-road proximity penalty**: `precompute/road_proximity.py`
- **Overture integration and dedupe**: `overture/loader.py`, `overture/merge.py`, `db_postgis/amenity_merge.py`
- **Amenity merge observability**: `db_postgis/amenity_merge.py`, `precompute/phases.py`, `precompute/_rows.py`, `precompute/publish.py`
- **Noise overlay ingestion**: `noise/loader.py`
- **Noise artifact source ingest modes**: `noise_artifacts/ingest.py`, `noise_artifacts/ogr_ingest.py`
- **Amenity tier classifier**: `precompute/amenity_tiers.py`
- **Mode-aware scoring payload helper**: `precompute/mode_aware.py`
- **Amenity clustering for Phase 2 variety scoring**: `precompute/amenity_clusters.py`
- **Grid scoring**: `precompute/grid.py`
- **Fine surface runtime and inspect payloads**: `precompute/surface.py`
- **Publish row builders and summaries**: `precompute/publish.py`, `precompute/_rows.py`
- **Precompute cache helpers**: `precompute/cache.py`, `precompute/_cache_wrappers.py`
- **Array-native reachability cache helpers**: `precompute/reachability_arrays.py`
- **Precompute tier helpers**: `precompute/tiers.py`, `precompute/_tier_wrappers.py`
- **Precompute planning layer**: `precompute/_planning.py`
- **Precompute build state**: `precompute/_state.py`
- **Reachability math (Python)**: `precompute/network.py`
- **Walkgraph graph-index loader (Python)**: `network/loader.py`
- **Study-area geometry / coast mask**: `study_area.py`
- **Precompute pipeline orchestration**: `precompute/__init__.py`, `precompute/workflow.py`, `precompute/phases.py`
- **Lightweight GTFS refresh CLI path**: `transit_refresh_runner.py`
- **Pipeline ETA and timing history**: `progress_tracker.py`
- **Rust walkgraph binary**: `walkgraph/` (graph build, reachability, surface shards, GTFS refresh; build profile supports walk and bike filtering)
- **PMTiles bake**: `precompute/bake_pmtiles.py`, `noise_artifacts/bake.py`, `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py`
- **Self-hosted context basemap**: `basemap.py`, `basemap/docker-compose.yml`, Protomaps style export plus same-origin glyph/sprite assets under `static/basemap/`; the builder preflights Docker's Linux engine with an actionable message and CI runs only a Docker `--area=monaco` fixture.
- **Runtime HTTP server**: `serve_from_db.py`
- **Runtime route helper**: `serve_routes.py`
- **Optional Valhalla pedestrian reachability**: `precompute/valhalla_reachability.py` uses canonical walkgraph snapped nodes with EPSG:2157 STRtree prefiltering and `/sources_to_targets`; `valhalla/docker-compose.yml` is the local service sandbox.
- **Valhalla benchmark helper**: `scripts/bench_valhalla.py`, `scripts/win/bench_valhalla.ps1`
- **Runtime request logging**: `serve_from_db.py` emits compact route-aware GET/HEAD logs with status, duration, response size, and disconnect hints.
- **Land-use context overlay**: `db_postgis/reads.py`, `precompute/_rows.py`, `precompute/bake_pmtiles.py`, `pmtiles_bake_worker.py`, `frontend/src/landuse_filters.js`; frontend style construction omits the `filter` property when all land-use classes are selected because MapLibre rejects `filter: null`.
- **Alembic schema-hardening migration**: `db_postgis/migrations/versions/20260617_000021_add_candidate_key_constraints.py` (head: `20260708_000023_transport_mode_tiering.py`)
- **Frontend source**: `frontend/src/` — modules: `main.js`, `runtime_contract.js`, `api_client.js`, `amenity_filters.js`, `transport_filters.js`, `transport_reality_popup.js`, `noise_filters.js`, `landuse_filters.js`, `click_priority.js`, `grid_debug.js`, `map_source_guard.js`; served bundle in `static/dist/`
- **Human docs / design notes**: `README.md`, `docs/PHASES.md`, `docs/notes.md`, `docs/BASEMAP.md`
- **Read-only DB integrity preflight**: `scripts/db_integrity_check.py` + `tests/test_db_integrity_check.py`; checks logical duplicate groups and NULL key values before PK/unique/FK constraints and deliberately skips `amenities` as ambiguous.

---

## 3. Entry Points

### `main.py`

- Primary CLI entry point. (Confirmed, non-blank LOC: 321)
- Dispatches:
  - `import` → `precompute.refresh_local_import()`
    - after a successful raw import, refreshes the self-hosted basemap when its PBF/pin manifest is stale (**this is why `import` needs Docker**)
  - `basemap [--force]` → `basemap.build_basemap()` using Docker and the configured Ireland+NI PBF
  - `gtfs status [--force-gtfs-refresh]` → `transit_refresh_runner.gtfs_status()` diagnostics
  - `gtfs refresh [--force-gtfs-refresh]` → `transit_refresh_runner.refresh_gtfs()`
  - `transit [--force-transit-refresh] [--auto-refresh-gtfs] [--force-gtfs-refresh]` → `transit_refresh_runner.refresh_transit()`
  - `precompute --profile {full,dev,test}` → `precompute.run_precompute(profile=...)`
    - `--explain` prints the planner decision tree without entering expensive phases
    - `--force-precompute` rebuilds and replaces the current PostGIS build even if a complete manifest exists
    - `--auto-refresh-import` allows raw OSM import refresh when missing
    - `--allow-missing-transport` bypasses the zero-transport publish gate (transport scores become zero)
    - `--refresh-noise-artifact` / `--force-noise-artifact` / `--reimport-noise-source` / `--force-noise-all` control noise artifact work
  - `serve --profile {full,dev,test} [--host] [--port] [--deployment]` → `render_from_db.run_render_from_db(...)`; defaults are `127.0.0.1:8000`; `--deployment` requires the local basemap artifact and assets
- If no subcommand is supplied, serving is the default path. (Confirmed)
- `transit` goes through a lightweight transit-only runner instead of importing the full `precompute` package first, emits tracker lines before DB/schema and source-state preflight, reuses a cached OSM extract fingerprint when the local `.osm.pbf` path/size/mtime are unchanged, and can auto-refresh GTFS cache first. (Confirmed)

### `setup.sh`

- POSIX/Linux local bootstrap for a PostgreSQL/PostGIS development toolchain. (Confirmed)
- On Debian/Ubuntu it installs missing system dependencies (including Cargo, PostgreSQL/PostGIS, GDAL, osm2pgsql, Node, and Python tooling); other package managers receive an explicit prerequisite error. (Confirmed)
- If the distro Cargo cannot parse `walkgraph/Cargo.lock`, it installs and selects the current stable Rust toolchain through rustup before building. (Confirmed)
- Writes a new owner-only `.env` with its provisioned split `POSTGRES_*` settings and `WALKGRAPH_BIN`, while preserving a pre-existing `.env`. (Confirmed)
- Starts a local PostgreSQL service when necessary and creates the configured role/database/extensions idempotently. (Confirmed)
- Reuses local inputs or downloads the OSM extract plus configured required boundary/GTFS sources; can also retrieve configured optional Overture, main-island, and raw noise datasets. (Confirmed)
- A missing or unreachable optional Overture/noise/main-island download logs a skip and lets bootstrap continue; a skipped main-island archive is never passed to `unzip`. Required inputs still fail explicitly. (Confirmed)
- Defaults to the `SidneyMe/livability-data` release `v1` mirror, which carries its five remote-install core inputs. Optional asset URLs are not guessed from that base because v1 does not carry them; configure their individual URLs when available. `DATASET_MIRROR_BASE_URL` can replace the core mirror, while per-dataset URLs take precedence. (Confirmed)
- Its default OSM URL uses the Geofabrik `.de` host and its NI boundary default is a stable GitHub mirror of the NI coastline/outline because OSNI endpoints may reset or deny unattended downloads; ArcGIS Hub remains an overrideable fallback. It rejects an empty database password and sets the supplied password on the provisioned role, including on reruns. (Confirmed)
- Builds the Rust helper and frontend bundle, then installs Python requirements and applies Alembic migrations. (Confirmed)
- Takes database settings from `POSTGRES_*`; it prompts for `POSTGRES_PASSWORD` only in an interactive shell and never writes the secret to disk. (Confirmed)

### `.github/workflows/scheduled_refresh.yml`

- Weekly self-hosted workflow. (Confirmed)
- Uses workflow-level concurrency so scheduled and manual refresh runs do not overlap, plus a 240-minute job timeout. The OSM force flag is expanded without bash-only syntax so the workflow stays portable across an undocumented self-hosted runner OS. (Confirmed)
- Runs:
  - `python scripts/refresh_osm.py`
  - `python main.py transit --auto-refresh-gtfs`
  - `python main.py precompute --auto-refresh-import`
  - `python scripts/sanity_check.py --profile full`

### `.github/workflows/ci.yml`

- Push / PR validation workflow. (Confirmed)
- Uses an explicit `pwsh` default shell on the Windows job so the PowerShell bundle freshness check is unambiguous. (Confirmed)
- Runs:
  - `npm ci` in `frontend/` using `frontend/package-lock.json`
  - `npm test` and `npm run build` in `frontend/`
  - `git ls-files --error-unmatch static/dist/app.css static/dist/app.js` and `git status --porcelain -- static/dist` to catch missing, untracked, or stale bundles
  - `python -m unittest discover -s tests -t . -p "test_*.py"`
  - `cargo test --manifest-path walkgraph/Cargo.toml`
  - `python scripts/sanity_check.py --validate-only`

### `scripts/ci_local.ps1`

- Local Windows PowerShell CI runner for pre-push checks. (Confirmed)
- Sequence: command availability checks for `python`, `npm.cmd`, `git`; `python -m pytest -q`; `python -m alembic current`; `alembic upgrade head`; `python scripts/db_integrity_check.py`; `python main.py precompute --profile dev --explain`; `npm.cmd test --prefix frontend`; `npm.cmd run build --prefix frontend`; `git diff --exit-code -- static/dist`; `git diff --check`. (Confirmed)
- Stops on the first failure, prints section headers, and reports elapsed time per step. (Confirmed)

### `scripts/bench_valhalla.py`

- Standalone local benchmark for the Valhalla `sources_to_targets` endpoint. (Confirmed, LOC 309)
- Avoids dependence on `build_manifest` / `grid_walk`: prepares routeable points by snapping jittered samples around major Ireland / NI city centres through `/locate`, builds one-source / multi-target pedestrian matrix jobs, then measures steady-state request throughput, route throughput, and latency percentiles. (Confirmed)
- Intended Windows entrypoint is `scripts/win/bench_valhalla.ps1`, defaulting to 8 workers and project `.venv` Python when available. (Confirmed)

### `scripts/sanity_check.py`

- Standalone validation CLI used by CI and scheduled refresh. (Confirmed, LOC 303)
- `--validate-only` checks fixture structure only.
- Normal mode resolves the active completed build, then reads scores through fine-surface runtime lookups when enabled, otherwise through `grid_walk` point lookups. (Confirmed from file and tests)

### `scripts/win/run_noise_precompute_watchdog.ps1`

- Windows wrapper for noise-focused dev precompute with enforced wall-clock timeout. (Confirmed)
- Modes: `DevReuse` / `AccurateReuse` (reuse-only, never rebuild), `DevPrepare` / `AccuratePrepare` (cache-aware refresh), `DevForce` / `AccurateForce` (source reimport + resolved rebuild). (Confirmed)
- Supports overriding the Python executable via `NOISE_PYTHON_EXE` and falls back to conda `python` when `.venv\Scripts\python.exe` is missing. (Confirmed)
- On timeout, kills the spawned process tree (and attempts cleanup of lingering `ogr2ogr` processes) and returns exit code `124`. (Confirmed)

---

## 4. End-to-End Flow

```text
1. Local inputs
   osm/ireland-and-northern-ireland-latest.osm.pbf
   gtfs/nta_gtfs.zip + gtfs/translink_gtfs.zip (mandatory: zero-transport builds are gated)
   overture/ireland_places.geoparquet (optional)
   noise_datasets/*.zip (optional display overlay)
   ireland_main_island_shp/ireland_main_island.shp (preferred exact coast mask when present)
   boundaries/*.geojson

2. Raw import
   python main.py import            (also refreshes the basemap; needs Docker)
   -> local_osm_import/
   -> osm_raw.features + osm_raw.import_manifest

3. Transit reality
   python main.py transit --auto-refresh-gtfs
   -> transit/workflow.py
   -> refreshes/verifies gitignored GTFS feed ZIP freshness when auto-refresh is requested
   -> walkgraph gtfs-refresh
   -> transit_raw.* + transit_derived.*

4. Precompute
   python main.py precompute        (add --allow-missing-transport only if GTFS is genuinely unavailable)
   -> precompute/__init__.run_precompute()
   -> precompute/workflow.run_precompute_impl()
   -> geometry
   -> amenities (+ Overture merge + amenity tier annotation)
   -> networks
   -> reachability
   -> scoring / grids
   -> publish gate: raise if the transport category is empty and --allow-missing-transport is absent

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
   -> PMTiles source max zoom capped at 15; profile noise caps default to z10 (dev) and z13 (full/test)
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
     GET /tiles/basemap.pmtiles
     GET /tiles/livability-test.pmtiles
     GET /tiles/noise.pmtiles
     GET /tiles/noise-test.pmtiles
     GET /tiles/surface/{resolution}/{z}/{x}/{y}.png
     GET /exports/transport-reality.zip
   -> server startup fails immediately if static/index.html, static/dist/app.css, or static/dist/app.js is missing

8. Frontend
   static/dist/app.js
   -> reads PMTiles through pmtiles://
   -> reads runtime JSON from /api/runtime with a longer cold-start timeout and one retry after abort
   -> renders one active vector grid fill+outline pair with a client-side Combined/Shops/Transport/Healthcare/Parks color toggle, recreates layers when the zoom band changes, overzooms z15 source tiles to z19
   -> default-off noise proxy panel on the separate `noise` vector source / `noise_proxy` source-layer, with metric+kind filtering, opacity control, and an explicit grid-proxy caveat
   -> control panel scrolls internally when contents exceed viewport height
   -> transport tiers render inside Amenities -> Transport: Bus subgroup (Schedule weekly patterns, Frequency headway tiers) plus Bus/Rail/Tram mode toggles; default all-mode state applies no vector filter so older PMTiles still show stops
   -> /?debug-grid=1 reveals a persistent Grid debug card (live source-vs-rendered counts, diagnosis, copyable snapshot)
   -> clicking transport rows renders all colocated stop rows in one popup; clicking the active grid opens the score breakdown popup via /api/inspect when available
```

Notes:

- Service deserts are computed from reachable baseline GTFS stops and public departures in the configured desert window. (Confirmed)
- Fine surface exists for `full` and `test`. `dev` is coarse-only. (Confirmed)
- `test` uses the same full-resolution ladder as `full` but clips phase-1 study-area loading to a compact Cork city bbox `(-8.55, 51.87, -8.41, 51.93)` in WGS84, so caches, imports, manifests, and PMTiles stay isolated from the island-wide build. (Confirmed)

---

## 5. Source-of-Truth Map

| Domain | Canonical | Runtime mirror / consumer | Snapshot / wrapper |
|---|---|---|---|
| Config and hash chain | `config.py` | Imported almost everywhere | None |
| Schema history | `db_postgis/migrations/versions/` | `db_postgis/tables.py` | `schema.sql` (stale) |
| OSM ingest rules | `osm2pgsql_livability.lua`, `local_osm_import/` | `config.IMPORTER_CONFIG_VERSION` | None |
| Major-road proximity scoring | `osm2pgsql_livability.lua`, `db_postgis/reads.py`, `precompute/road_proximity.py` | `grid_walk.scores_json`, fine-surface score arrays, `/api/runtime` road diagnostics, grid popups | `osm_raw.roads` importer-owned table |
| Transit reality (GTFS-only) | `transit/workflow.py`, `transit/rust_gtfs.py`, `walkgraph/src/gtfs/mod.rs` | `config.transit_config_hash()` | None |
| Railway proximity | `transit/railway_corridors.py` | `config.RAILWAY_PROXIMITY_*`, `grid_walk.scores_json` | None |
| Amenity tiering | `config.py` tier constants, `precompute/amenity_tiers.py` | `precompute/phases.py`, `precompute/publish.py` | None |
| Mode-aware score payloads | `config.py`, `precompute/mode_aware.py`, `precompute/grid.py`, `precompute/surface.py` | `grid_walk.scores_json["mode_aware"]`, `/api/inspect`, grid popups | Walk-only totals stay the default styling input; mode-aware data is nested for validation |
| Overture category mapping | `overture/loader.py::OVERTURE_CATEGORY_MAP` | `precompute/phases.py` | None |
| Overture merge logic | `overture/merge.py`, `db_postgis/amenity_merge.py` | `precompute/phases.py` | None |
| Amenity merge diagnostics | `db_postgis/amenity_merge.py`, `precompute/phases.py`, `precompute/_rows.py`, `precompute/publish.py` | `phase_amenities_impl()` → `_summary_json()` → `build_manifest.summary_json` | `summary_json["amenity_merge"]` persists stage timings, key row counts, candidate-path counts, compact warnings |
| Noise overlay source handling | `noise/loader.py`, `noise_artifacts/*` | `noise_polygons`, separate `noise` PMTiles archive, `/api/runtime` noise counts + `noise_pmtiles_url` | `noise_datasets/*.zip` local inputs |
| Noise artifact ogr2ogr ingest safety/perf | `noise_artifacts/ogr_ingest.py` | Road GDB canonical pipeline (FileGDB → local GPKG cache → one PG stage import → batch normalize), SQL timeouts, disk preflight | `NOISE_ROAD_GDB_CANONICAL_CACHE`, `NOISE_REBUILD_ROAD_GDB_CACHE`, `NOISE_ROAD_NORMALIZE_BATCH_SIZE`, `NOISE_SQL_*`, `NOISE_MIN_FREE_DISK_GB` |
| Precompute orchestration | `precompute/_planning.py`, `precompute/workflow.py` | `precompute/__init__.py` | Explain-mode short-circuit prints planner output without running geometry/reachability/publish/schema setup; `precompute._STATE.activate()` clears per-build amenity merge diagnostics |
| Managed schema validation | `db_postgis/schema.py` | `ensure_database_ready()` and startup validation | PK-equivalent coverage is accepted after the candidate-key migration, so startup no longer demands exact index names |
| Coastal cleanup observability | `study_area.py`, `precompute/_rows.py`, `precompute/publish.py` | `clean_coastal_artifacts()` → `summary_json` | `summary_json["coastal_cleanup"]` persists counts, fallback modes, thresholds, sample warnings |
| PMTiles layer metadata | `precompute/bake_pmtiles.py`, `noise_artifacts/bake.py` | `pmtiles_bake_worker.py`, `fine_vector_pmtiles_worker.py` | None |
| Basemap | `basemap.py`, `basemap/docker-compose.yml`, `config.PROTOMAPS_*` | `serve_from_db.py` basemap route + `--deployment` check | `static/basemap/` vendored assets, `.livability_cache/basemap/` manifest |
| Runtime API contract | `serve_from_db.RuntimeState` | `frontend/src/runtime_contract.js`, `frontend/src/main.js` | `render_from_db.py` |
| Runtime request logging | `serve_from_db.py`, `serve_routes.py` | `do_GET()` / `do_HEAD()` | Route-aware log lines (method, path, route, status, duration, bytes, disconnect hints) |
| Frontend source | `frontend/src/` | `static/dist/` after build | `static/dist/*` (checked in) |
| Progress / ETA behavior | `progress_tracker.py` | `precompute/workflow.py`, `precompute/__init__.py` | `.livability_cache/precompute_timing_stats.json` |
| Product / methodology docs | `README.md`, `docs/PHASES.md`, `docs/notes.md`, `docs/BASEMAP.md` | Humans only | Roadmap distinguishes display-only noise overlay from future scoring penalties; rail proximity is a station-access plateau plus track-adjacency damping |

---

## 6. Key Modules

### 6.1 Config and hashing

#### `config.py`

- Purpose: master config, scoring params, path defaults, build profiles, and the entire invalidation spine. (Confirmed)
- Why it matters: `HASHES = build_config_hashes()` runs at import time. Missing files can silently change hashes by contributing zeroed metadata instead of raising. (Confirmed)
- Important constants:
  - `CAPS = {"shops": 6, "transport": 5, "healthcare": 5, "parks": 5}`
  - `SHOP_TIER_UNITS = {"corner": 1, "regular": 2, "supermarket": 3, "mall": 5}`
  - `HEALTHCARE_TIER_UNITS = {"local": 1, "clinic": 2, "hospital": 3, "emergency_hospital": 4}`
  - `PARK_TIER_UNITS = {"pocket": 1, "neighbourhood": 2, "district": 3, "regional": 4}`
  - `VARIETY_CLUSTER_RADIUS_M = 25.0`
  - `DISTANCE_DECAY_HALF_DISTANCE_M = {"shops": 150.0, "transport": 250.0, "healthcare": 300.0, "parks": 350.0}`
  - `TRANSIT_REALITY_ALGO_VERSION = 10`, `AMENITY_MERGE_ALGO_VERSION = 4`, `WALKGRAPH_FORMAT_VERSION = 3`
  - `FINE_SURFACE_SCHEMA_VERSION = 2`, `PMTILES_SCHEMA_VERSION = 13`, `GRID_GEOMETRY_SCHEMA_VERSION = 4`, `CACHE_SCHEMA_VERSION = 14`
  - `IMPORTER_CONFIG_VERSION = "2026-08-31-gtfs-only-transport-1"`, `SCORING_MODEL_VERSION = "v3-gtfs-only-transport-2026-08-31"`
  - `WALK_RADIUS_M = 500`, `TRANSIT_ACCESS_WALK_RADIUS_M = 800`, `TRANSIT_EGRESS_WALK_RADIUS_M = 800`
  - `RAILWAY_PROXIMITY_ACTIVE_MODES = ("rail", "tram")`, full-penalty at 50 m, zero at 200 m, `RAILWAY_PROXIMITY_MAX_PENALTY = 4.0`
  - `ROAD_PROXIMITY_CLASS_SETTINGS`: motorway/trunk/primary with max penalties 4.0 / 3.0 / 2.0
  - `TAGS["transport"] = {"source": "gtfs_direct"}`; `LANDUSE_CONTEXT_CLASSES` for the context overlay
  - Basemap pins: `PROTOMAPS_BASEMAPS_COMMIT`, `PROTOMAPS_ASSETS_COMMIT`, `PROTOMAPS_TILE_SCHEMA_MAJOR = 4`, `BASEMAP_FONT_STACKS`, `BASEMAP_CACHE_DIR`
- Build profiles:
  - `full`: vector grid `20000/10000/5000/2500/1000/500/250/100/50` baked into PMTiles, archive source zoom capped at 15, frontend overzoom to 19
  - `dev`: coarse vector only
  - `test`: same ladder as `full` with `study_area_kind="bbox"` and `study_area_bbox_wgs84=(-8.55, 51.87, -8.41, 51.93)`; writes `livability-test.pmtiles`
- Render hash includes a noise dataset signature and file-size/mtime metadata from `noise_datasets/*.zip`; score/cache hashes stay independent because noise is display-only. (Confirmed)

### 6.2 CLI and orchestration

#### `precompute/__init__.py`

- Purpose: public precompute API surface plus `_BuildState` lifecycle and service-desert computation. (Confirmed)
- Why it matters: `_STATE = _BuildState.bootstrap()` runs at import time with placeholder fingerprints; real hashes are valid only after `_STATE.activate(...)`. (Confirmed)
- Exposes: `run_precompute()`, `refresh_local_import()`, `refresh_transit()`, `phase_reachability()`, `phase_grids()`
- Owns: `_compute_service_deserts()`, `_preflight_transit_rebuild()`
- `run_precompute(...)` accepts `allow_missing_transport: bool = False` and forwards it to `workflow.run_precompute_impl`. (Confirmed)

#### `precompute/workflow.py`

- Purpose: injected orchestration for import refresh and full precompute. (Confirmed)
- Why it matters: owns reuse/short-circuit logic (skip, rebake PMTiles only, refresh noise overlay only, rebuild fine surface, rebuild everything), tracker injection, timing persistence, and the **zero-transport publish gate** immediately after `phase_amenities`. (Confirmed)
- Main functions: `run_import_refresh_impl()`, `run_precompute_impl()`, `_plan_precompute_build()`, `_run_pmtiles_bake()`, `_run_noise_pmtiles_bake()`

#### `precompute/_planning.py`

- Purpose: pure planning layer — no IO, fully unit-testable. (Confirmed)
- Plans: `ImportPlan` (`skip`/`refresh`/`fail`), `NoiseArtifactPlan` (`skip`/`reuse`/`build`/`require_active`/`legacy`), `BuildPlan` (`skip`/`refresh_noise_overlay_only`/`rebake_missing_assets`/`rebuild_fine_surface`/`rebuild_full_pipeline`). (Confirmed)
- Main functions: `plan_import`, `plan_noise_artifact`, `plan_build`, `plan_precompute`, `format_precompute_plan`
- Use `python main.py precompute --profile dev --explain` to print a plan without side effects. (Confirmed)

#### `precompute/_state.py`

- Purpose: `_BuildState` lifecycle (`bootstrap`, `activate`) and `_active_fine_surface_enabled`. (Confirmed)
- Why it matters: import-time object; invalid until activated by the workflow.

#### `transit_refresh_runner.py`

- Purpose: lightweight CLI-only GTFS refresh path used by `main.py gtfs status`, `gtfs refresh`, and `transit`. (Confirmed)
- Why it matters: avoids importing the whole `precompute` package before the first transit progress line, starts the tracker before DB/schema checks, materializes the transit-derived rail corridor cache when transit reality is current, and passes transit progress callbacks into OSM source-state fingerprinting. (Confirmed)
- Main functions: `refresh_gtfs()`, `gtfs_status()`, `refresh_transit()`

#### `progress_tracker.py`

- Purpose: phase progress, ETA, and persisted timing history. (Confirmed)
- Main types: `PhaseState`, `PrecomputeProgressTracker`; writes `.livability_cache/precompute_timing_stats.json`
- Important behavior: progress-tracking failures disable tracking instead of failing the build. Historical timings drive ETA quality. (Confirmed)

### 6.3 Scoring, surfaces, caches

#### `precompute/phases.py`

- Purpose: the concrete geometry, amenities, networks, reachability, and grid-scoring phase implementations. (Confirmed)
- Why it matters: core scoring logic — how amenity rows are annotated, how scoring-only clusters are formed, how walk reachability becomes counts/cluster counts/distance-decayed effective units. (Confirmed)
- Important details:
  - transport rows are kept separate from Overture merge
  - amenity rows are annotated with `tier` and `score_units`
  - Phase 2 builds scoring-only amenity clusters per category and picks a deterministic representative per cluster
  - reachability persists `walk_counts_by_origin_node`, `walk_cluster_counts_by_origin_node`, `walk_effective_units_by_origin_node` through versioned array-native caches under `reachability_arrays/*`
  - routing checkpoints are committed `.npz` matrix chunks with `uint64` origin IDs, manifest-defined category order, `uint32`/`float32` matrices; success compacts into `base.npz`
  - legacy pickle/gzip dict reachability caches are ignored unless `LIVABILITY_MIGRATE_LEGACY_REACH_CACHE=1`
  - origin-node normalization/union uses low-memory sorted lists rather than `set(sorted(...))`
  - railway proximity penalties come from the transit-derived corridor layer and thread through grid scoring, node score arrays, inspect payloads, and PMTiles export
  - bike graph build is wrapped in try/except: failure logs `"bike mode-aware reachability unavailable"` and does not fail the build
  - `phase_reachability_impl` passes `transit_effective_units_by_node=None`, so the transit mode-aware leg is always unavailable

#### `precompute/grid.py`

- Purpose: per-cell scoring: category totals, caps, distance decay, area-ratio normalization, hidden penalty deduction. (Confirmed)
- Main functions: `score_cell`, `score_cells`; `_MIN_DENSITY_AREA_RATIO = 0.25` guards sliver cells via `_normalized_area_ratio`.

#### `precompute/surface.py`

- Purpose: fine-surface runtime, score/shell shards, inspect payloads, PNG tiles. (Confirmed)
- Main type: `FineSurfaceRuntime` with `_shell_shard_cache`, `_score_shard_cache`, and `_aggregated_cache`. These are per-runtime and not evicted, so a long-lived serving process can accumulate shard RAM; `_load_node_scores` caches node scores once per runtime. (Confirmed)

#### `precompute/mode_aware.py`

- Purpose: mode-aware scoring payload helper. (Confirmed)
- `SCORE_MODES = ("walk", "bike", "transit")`; `mode_available(...)` reports availability. Transit is currently never available (see §9), so `scores_json["mode_aware"]` carries walk/bike comparisons in practice.

#### `precompute/tiers.py` + `precompute/cache.py`

- Purpose: tier cache directory layout and manifest marking (`write_tier_manifest`, `mark_building`, `mark_complete`), plus cache read/write helpers. (Confirmed)
- Cache dirs under `.livability_cache/`: `geo_<hash>`, `reach_<hash>`, `score_<hash>`, `surface_shell_*`, `surface_scores_*`, `surface_tiles_*`; manifests mark `building`/`complete`; corrupt pickles are quarantined. (Confirmed)

#### `precompute/reachability_arrays.py`

- Purpose: versioned array-native cache format for origin-node reachability matrices. (Confirmed)
- Format: `manifest.json` plus `base.npz` and optional `chunks/chunk_*.npz`; categories in the manifest, `origin_ids` `<u8`, count matrices `<u4`, effective-unit matrices `<f4`. (Confirmed)
- Recovery: orphan `.tmp` ignored, corrupt chunks quarantined and recomputed, corrupt base invalidates the v1 cache, legacy migration opt-in only. (Confirmed)

#### `precompute/network.py`

- Purpose: Python-side reachability math and walkgraph bridge. (Confirmed)
- Main functions: `precompute_walk_weighted_totals_matrix_by_origin_node`, `precompute_walk_counts_by_origin_node`, `precompute_walk_decayed_units_matrix_by_origin_node`, `precompute_counts_by_node`, `snap_amenities`, `nearest_nodes`, `normalize_origin_node_ids`, `merge_normalized_origin_node_ids`
- Not to be confused with the root `network/` package.

#### `network/loader.py`

- Purpose: loads a walkgraph graph index from disk (`load_walk_graph_index`, `WalkGraphIndex`). (Confirmed)
- Test: `tests/test_network_loader.py`.

#### `precompute/amenity_clusters.py`

- Purpose: scoring-only clustering for Phase 2 variety handling. (Confirmed)
- One physical cluster contributes one category-specific scoring item; original rows stay intact for publishing and raw-count explainability.
- Representative order: highest `score_units`, then named before unnamed, then stable source identity.

#### `precompute/amenity_tiers.py`

- Purpose: canonical tier classifier for shops/healthcare/parks plus GTFS-frequency transport score units. (Confirmed)
- Main functions: `classify_amenity_row()`, `annotate_amenity_row()`, `uses_weighted_units()`
- This file decides tier names stored on amenities and the units consumed by scoring.

#### `precompute/publish.py` + `precompute/_rows.py`

- Purpose: published row builders (walk, amenity, noise), summary JSON assembly, and row-shaping helpers. (Confirmed)
- Main functions: `iter_walk_rows_impl`, `walk_rows_impl`, `iter_amenity_rows_impl`, `amenity_rows_impl`, `iter_noise_rows_impl`, `summary_json_impl`, `_amenity_tier_counts`, `_transport_summary_counts`

#### `precompute/road_proximity.py`

- Purpose: parse OSM maxspeed values and compute the strongest class- and speed-weighted motorway/trunk/primary proximity penalty for walkgraph nodes. (Confirmed)
- Uses linear class-specific decay, capped speed multipliers, metric CRS geometry, STRtree candidate lookup.
- Main functions: `parse_maxspeed_kmh()`, `road_penalty_for_distance()`, `compute_road_proximity_penalties()`

#### `precompute/valhalla_reachability.py` (opt-in)

- Purpose: optional Valhalla pedestrian reachability backend using canonical walkgraph snapped nodes, EPSG:2157 STRtree prefiltering, `/sources_to_targets`. (Confirmed)
- Enabled via `WALK_ROUTING_BACKEND=valhalla`; the default remains `walkgraph`.

### 6.4 Transit (GTFS-only)

#### `transit/workflow.py`

- Purpose: transit reality refresh orchestration; drives the Rust `gtfs-refresh` step. (Confirmed)
- Main functions: `ensure_transit_reality`, `transit_reality_refresh_required`
- Transport scoring consumes only these rows — there is no OSM fallback.

#### `transit/gtfs_download.py`

- Purpose: cache-aware public static GTFS downloader/validator used by transit source resolution. (Confirmed)
- Why it matters: polite HTTP (timeout, retry/backoff, conditional requests), atomic ZIP replacement, SHA256 manifests, freshness/calendar logging, and required-file validation before any `current.zip` update; auto-refresh fails if post-refresh freshness cannot be proven. (Confirmed)
- Main functions: `refresh_gtfs_feed()`, `refresh_gtfs_feeds()`, `ensure_transit_feed_available()` (raises `"GTFS cache missing. Run --refresh-gtfs first."`)

#### `transit/rust_gtfs.py`

- Purpose: writes the GTFS refresh config, runs the compiled `walkgraph gtfs-refresh`, reads the CSV artifacts back. (Confirmed)
- Main functions: `run_walkgraph_gtfs_refresh`, `load_gtfs_stop_reality_models`

#### `transit/service.py`

- Purpose: service-level frequency semantics, including `BUS_FREQUENCY_TIER_UNITS` used for transport score units. (Confirmed)

#### `transit/railway_corridors.py`

- Purpose: shapes-backed rail/tram corridor materialization and hidden railway proximity penalty helper. (Confirmed)
- Why it matters: GTFS shapes are the service-backed geometry source; the module hashes, dissolves, caches, validates the corridor manifest against the railway proximity hash, and scores the corridor layer without changing the public overlay surface. (Confirmed)
- Main functions: `railway_proximity_hash_for_state()`, `build_railway_corridor_materialization()`, `compute_railway_proximity_penalties()`, `railway_proximity_penalty_for_distance()`

#### Other transit modules

- `transit/classification.py` — service classification (school-only handling etc.). (Confirmed)
- `transit/matching.py` — stop matching/assignment. (Confirmed)
- `transit/export.py` — transport-reality export archive. (Confirmed)
- `transit/gtfs_zip.py` — ZIP reading helpers. (Confirmed)
- `transit/sources.py` — feed source resolution from config/env. (Confirmed)
- `transit/models.py`, `transit/naming.py` — row models and naming helpers. (Confirmed)

#### Rust GTFS: `walkgraph/src/gtfs/mod.rs`

- Purpose: single-threaded GTFS parse, service calendars, stop reality derivation, frequency tiers and transport score units. (Confirmed)
- Key symbols: `classify_services`, `bus_frequency_tier_from_headway`, `bus_subtier_for_mask`, `transport_score_units_from_frequency`, `summarize_gtfs_stops`, `derive_gtfs_stop_reality`. (Confirmed)

### 6.5 OSM import

#### `local_osm_import/` and `osm2pgsql_livability.lua`

- Purpose: raw OSM import via `osm2pgsql` using the Lua tag filter; publishes `osm_raw.features` + `osm_raw.import_manifest`. (Confirmed)
- Main functions: `ensure_local_osm_import`, `resolve_source_state` (adds GTFS refresh, flat-node pruning, `docker` compose source download) | `local_osm_import/orchestrator.py`, `osm2pgsql.py`, `rules.py`. (Confirmed)
- The Lua file filters parks, landuse, and roads. It contains **no transport/`bus_stop`/`railway` handling** and no GTFS strings — adding transport tags there would be dead code. (Confirmed)
- `IMPORTER_CONFIG_VERSION` is part of `importer_config_hash()` (which hashes the Lua path + content), so Lua edits invalidate import-derived caches. (Confirmed)

### 6.6 Database layer

#### `db_postgis/tables.py`

- Purpose: SQLAlchemy table definitions used by reads/writes at runtime; must match Alembic head. (Confirmed)
- Current schema facts:
  - `amenities` has `category`, `tier`, `geom`, `source`, `source_ref`, `name`, `conflict_class`
  - `grid_walk` has `counts_json`, `cluster_counts_json`, `effective_units_json`, `scores_json`, `total_score`, clipped-area fields
  - `transit_derived.gtfs_stop_service_summary`, `transit_derived.gtfs_stop_reality`, `transit_derived.railway_corridor_manifest`, `transit_derived.railway_corridors`, and public `transport_reality` carry bus mask/subtier/daytime/headway/frequency/score-unit fields, unscheduled + exception-only flags, `route_modes_json`, commute/off-peak/weekend/Friday-evening averages
  - public output tables are `grid_walk`, `amenities`, `transport_reality`, `service_deserts`, `build_manifest`
  - public `noise_polygons` stores build-scoped overlay geometry; in artifact mode this is Phase E transport/industry output (`source_type IN ('road','rail','airport','industry')`, `metric IN ('Lden','Lnight')`) exported as `noise_proxy` layer properties

#### `db_postgis/schema.py`

- Purpose: `ensure_database_ready()` (Alembic), `import_payload_ready()`, `raw_import_ready()`. (Confirmed)
- `import_payload_ready` checks table existence + total feature count only — it does not validate per-category completeness.

#### `db_postgis/reads.py`

- Purpose: runtime + scoring read paths. (Confirmed)
- Key symbols: `load_source_amenity_rows` (category filter + GTFS append), `load_transport_reality_rows_for_scoring`, `load_transport_reality_points`, `load_road_rows` (highway filter), `load_landuse_context_rows`, `_AMENITY_CATEGORY_VALUES = {"shops","healthcare","parks"}` (transport is deliberately excluded).

#### `db_postgis/writes.py`, `write_transit.py`, `write_noise.py`, `write_common.py`, `common.py`

- Purpose: publish/bulk-copy helpers split by domain. (Confirmed)
- Key symbols: `publish_precomputed_artifacts`, `refresh_noise_overlay_for_build` (writes.py), `replace_service_desert_rows` (write_transit.py).

#### `db_postgis/manifests.py` and `db_postgis/engine.py`

- `manifests.py`: import/transit/build manifest read+write and completion checks. (Confirmed)
- `engine.py`: `build_engine` — where connection pool sizing lives (relevant when tuning bake concurrency). (Confirmed)

#### `db_postgis/_dependencies.py`

- Purpose: central import shim for SQLAlchemy/GeoAlchemy/geometry helpers. (Confirmed)
- Why it matters: on Windows it installs a small `platform` shim before importing SQLAlchemy/GeoAlchemy so a broken WMI-backed `platform.system()`/`platform.uname()` cannot freeze CLI startup before any banner prints. (Confirmed)

#### `db_postgis/migrations/versions/`

- Canonical schema history; 23 migrations. Latest: `20260708_000023_transport_mode_tiering.py`. (Confirmed)

#### `db_postgis/amenity_merge.py`

- Purpose: DB-backed staging and streaming wrapper around the canonical OSM/Overture merge logic. (Confirmed)
- Main function: `load_merged_source_amenity_rows()`; this is the active scale path used by the precompute amenity phase.

### 6.7 Overture

#### `overture/loader.py`

- Purpose: Overture Places loader, category mapper, dataset signature provider. (Confirmed)
- Why it matters: the dataset signature feeds the config hash chain. Every row gets `park_area_m2: 0.0` and `footprint_area_m2: 0.0` (no area fetch path). (Confirmed)
- Test: `tests/test_overture_loader.py` confirms gardens are excluded while real park-like categories survive mapping. (Confirmed)

#### `overture/merge.py`

- Purpose: OSM/Overture dedupe and OSM self-dedupe using distance and normalized names/aliases. (Confirmed)
- Main functions: `resolve_merge_categories()`, `prepare_rows_for_merge()`, `deduplicate_osm_source_rows()`, `merge_source_amenity_rows()`
- Constants: `AUTO_MATCH_RADIUS_M = 35.0`, `NAME_MATCH_RADIUS_M = 75.0`, `OSM_SELF_DEDUPE_RADIUS_M = 10.0`
- Changes here alter counts, conflict classes, and downstream scores. Bump `AMENITY_MERGE_ALGO_VERSION` when logic changes. (Confirmed)

### 6.8 Noise overlay (display-only)

#### `noise/loader.py`

- Purpose: official ROI/NI environmental-noise loader for display-only polygons. (Confirmed)
- Reads ROI `NOISE_Round4/3/2` and NI `end_noisedata_round3/2/1`; ROI Round 4 road is FileGDB and requires `pyogrio`/GDAL so the newest road layer is used instead of silently falling back. (Confirmed)
- Normalizes ROI `Time` to `Lden`/`Lnight` and ROI dB field aliases (`Db_Low`/`dB_Low`, `Db_High`/`dB_High`, `DbValue`/`dB_Value`), excludes NI gridcode `1000` no-data, and maps NI rounds explicitly: Round 1 class-coded `GRIDCODE` via verified lookup (never `+1/+5` arithmetic), Round 2/3 threshold codes via verified table, unknown codes raise. Candidate rows carry `raw_gridcode`. (Confirmed)
- Candidate cache is chunked/streaming (`.livability_cache/noise_candidates/<key>/manifest.json + part-*.pkl.gz`); legacy single-file cache reads are opt-in via `NOISE_ALLOW_LEGACY_CANDIDATE_CACHE=1`. (Confirmed)
- Diagnostics: `python -m noise.loader --dump-ni-round1-classes [--data-dir ...]`. (Confirmed)
- Materializes effective polygons by `jurisdiction + source_type + metric`, newest round first, subtracting newer coverage before older fallback pieces publish. (Confirmed)

#### `noise_artifacts/` (artifact pipeline)

- `ingest.py` — canonical source ingest entrypoint with mode dispatch. Supports `NOISE_INGEST_MODE=auto|ogr2ogr|python`; the Python path uses one run-scoped temp staging table with `geom_wkb BYTEA`, COPY-to-stage, `NOISE_INGEST_COPY_BATCH_ROWS` / `NOISE_INGEST_FLUSH_ROWS`, then `INSERT ... SELECT` normalization (strict band validation, NI gridcode context in errors). (Confirmed)
- `ogr_ingest.py` — high-throughput raw import via GDAL `ogr2ogr` to PostGIS staging tables: `ogr2ogr_available()`, command builder, ZIP extraction cache (`.livability_cache/noise_gdal`), per-layer timing, field discovery (`pyogrio` with `fiona` fallback), case-insensitive ROI/NI allowlist, geometry-metadata denylisting, `--config PG_USE_COPY YES`, `-preserve_fid`, `-lco PRECISION=NO`, selective `-makevalid`. (Confirmed)
  - ROI Round 4 Road FileGDB uses a canonical three-phase path: extract once to local GPKG cache (`road_raw`), import GPKG to one PG stage table with `PG_USE_COPY`, then normalize from that stage (optional `source_fid` batches) — no direct FileGDB `fid` chunk imports. Canonical extraction requires explicit non-empty `selected_fields`. (Confirmed)
  - All `ogr2ogr` subprocesses run with enforced non-`None` hard timeouts, heartbeat diagnostics (pid, elapsed, timeout, last output age, context), and Windows process-tree cleanup; the Road GDB extraction disables the no-progress watchdog and relies on hard timeout. (Confirmed)
  - Stage imports preflight stale backend/lock diagnostics, commit stage `DROP TABLE` before external `ogr2ogr` runs, and can terminate stale sessions via `NOISE_TERMINATE_STALE_IMPORT_BACKENDS` / `NOISE_TERMINATE_STALE_STAGE_LOCKS`. `-append` + `-select` is blocked by an internal guard. (Confirmed)
- `builder.py` — disk preflight (`NOISE_MIN_FREE_DISK_GB`) and advisory lock. (Confirmed)
- `dissolve.py` — two-pass chunked dissolve. (Confirmed)
- `resolve.py` — round priority resolution into `noise_resolved_display`. (Confirmed)
- `dev_fast.py` — deterministic dev-fast road/rail grid proxy; cache key derives from source hash, grid size, latest-round metadata, and grid algorithm version (`NOISE_REBUILD_DEV_FAST_GRID=1` escape hatch). (Confirmed)
- `manifest.py` — hash functions (`source→domain→resolved→tiles`), artifact CRUD over `noise_artifact_manifest` / `_lineage` / `_active_artifact`. (Confirmed)
- `bake.py` — standalone noise PMTiles bake from published `noise_polygons`. (Confirmed)
  - Writes `noise[(-dev|-test)].pmtiles` with only the `noise` vector layer, removes stale archives when no rows exist, streams tile specs, and reuses the lightweight `noise_proxy` SQL helper in `pmtiles_bake_worker.py`. (Confirmed)
  - The `noise_proxy` source-layer declares proxy fields (`kind=road`, `class=unclassified`, `metric=Lden`, `calibrated_band_min`, `proxy_score`, `buffer_m=100`, `method=official_derived_grid_proxy`, `calibration_source=noise_grid_artifact`, `calibration_layer=grid_1000m`, `confidence=proxy_not_measured`, `actual_road_geometry=0`) and starts at z8. (Confirmed)
- `compare.py`, `modes.py`, `runner.py`, `exceptions.py`, `geometry.py` — comparison tooling, mode resolution, artifact runner orchestration, error types, geometry helpers. (Confirmed)
- `__main__.py` — artifact CLI; it no longer exposes `--bake-pmtiles` / `--output-dir` (noise baking happens through precompute/`bake.py`). (Confirmed)

### 6.9 Bake

#### `precompute/bake_pmtiles.py`

- Purpose: PMTiles bake orchestrator and layer metadata owner. (Confirmed)
- Why it matters: owns `GRID_AMENITY_CATEGORIES`, `_pmtiles_metadata()`, the z15 source-zoom cap, and the sparse fine-grid tile-spec planner stitching coarse SQL tiles with fine vector tiles. Noise is not declared or baked into the main archive. Bounds parallel in-flight work, clamps fine-grid bakes to 4 workers, retries once at half workers after `BrokenProcessPool`, and only replaces the final archive after a successful temp-file finalize. (Confirmed)
- Windows failure mode: `could not load library "postgis-3.dll": The paging file is too small…` when too many parallel bake workers hit PostGIS — an OS memory/pagefile limit, not a missing DLL. (Confirmed)
- Amenities layer metadata declares `category`, `tier`, `name`, `conflict_class`. (Confirmed)
- Transport layer metadata declares weekly bus subtier/mask fields, bus daytime frequency fields, `transport_mode_tier`, comma-separated `route_modes`, numeric `0/1` transport flags, commute/off-peak/weekend/Friday-evening averages, and `transport_score_units`. (Confirmed)
- The `grid` source-layer carries both coarse and fine features, with fine rows padded with zero-valued popup numerics so metadata stays schema-stable. (Confirmed)

#### `pmtiles_bake_worker.py`

- Purpose: minimal subprocess worker running PMTiles tile SQL inside spawned processes. (Confirmed)
- Why it matters: import isolation is a hard constraint on Windows spawn; heavy imports here blow memory for parallel workers. (Confirmed)
- Main functions: `_bake_chunk_worker()`, `_tile_mvt_bytes_by_flags()`, `_resolution_for_zoom()` (coarse tiers `5000`/`10000`/`20000` only — **not** `config.resolution_for_zoom()`).
- Layer bitmask lives here (`_LAYER_GRID` … `_LAYER_LANDUSE_CONTEXT`, `_LAYER_NOISE = 1 << 5`); also exposes the SQL-backed `noise_proxy` MVT helper. Keep GIS readers out of this file. (Confirmed)

#### `fine_vector_pmtiles_worker.py`

- Purpose: top-level Windows-safe worker for fine-grid vector tiles from surface shard manifests. (Confirmed)
- Why it matters: keeps Windows `spawn` subprocesses out of `precompute/__init__.py`, reads shell/score shard `.npz` directly, aggregates canonical `50m` scores into `2500/1000/500/250/100/50` cells, buffers tile-edge clipping so neighbouring z15 tiles overlap cleanly, and encodes the `grid` source-layer MVT bytes. Raw shell/score shards plus aggregated surfaces use bounded LRU caches (8/8/16) cleared after each chunk. Degenerate rings are dropped after normalization/buffered clipping. (Confirmed)
- z12 emits `2500m`, z13 `1000m`, z14 `500m`, z15 mixed `250m` + `100m` + `50m`. (Confirmed)

### 6.10 Runtime server and frontend

#### `serve_from_db.py`

- Purpose: runtime HTTP server for static assets, PMTiles range reads, fine-surface PNG tiles, runtime JSON, inspect API, and transport-reality export download. (Confirmed)
- Route matching/dispatch lives in `serve_routes.py`; this file keeps HTTP mechanics, response writing, and client-disconnect handling. (Confirmed)
- `RuntimeState` is the server-side truth for `/api/runtime`, including: `build_key`, `build_profile`, `coarse_vector_resolutions`, `fine_resolutions`, `surface_zoom_breaks`, `amenity_counts`, `amenity_tier_counts`, `transport_subtier_counts`, `transport_bus_frequency_counts`, `transport_flag_counts`, `transport_mode_counts`, `noise_enabled`, `noise_counts`, `noise_source_counts`, `noise_metric_counts`, `noise_band_counts`, `noise_pmtiles_url`, `fine_surface_enabled`, `surface_shell_dir`, `surface_score_dir`, `surface_tile_dir`, `transport_reality_enabled`, `service_deserts_enabled`, `transport_reality_download_url`, `transit_analysis_date`, `transit_analysis_window_days`, `transit_service_desert_window_days`, `overture_dataset`. (Confirmed)
- `/api/runtime` still reports `surface_zoom_breaks`, `fine_resolutions_m`, `fine_surface_enabled`, `inspect_url`, and `max_zoom=19`, but no longer advertises `surface_tile_url_template`; the main render path is vector-only. Runtime state reloads when the latest completed `build_key` changes; PMTiles URLs carry `?v=<build_key>`; PMTiles/export responses emit strong ETags with `If-None-Match`. Expected client aborts on `/api/inspect` are suppressed from logs; `/` and `/static/*` ship `Cache-Control: no-store`; static/export responses stream in bounded chunks. (Confirmed)
- PMTiles range serving rejects malformed, suffix/negative, multiple, reversed, and out-of-bounds ranges with `416` + `Content-Range: bytes */<size>`, preserving valid closed/open-ended ranges. (Confirmed)
- `/api/inspect` and `/tiles/surface/{resolution}/{z}/{x}/{y}.png` reject non-finite or out-of-range inputs before rendering; surface tiles validate resolution, integer zoom, and tile bounds against `2^z - 1`. (Confirmed)
- Basemap validation + `--deployment` gating live here; runtime reports noise availability and filter counts, and sets `noise_enabled=false` / `noise_pmtiles_url=null` when no rows or archive exist. (Confirmed)
- Strict mode requires a manifest matching current `config_hash` + `extract_path`. Explicit local fallback: `LIVABILITY_RUNTIME_ALLOW_STALE_DEV_RUNTIME=1`, which prefers the latest completed manifest for the same extract path, but prefers a transport-ready manifest when the newest one has transport enabled without usable signals — flagging `runtime_mode=stale_manifest_fallback` + `runtime_warning`. (Confirmed)

#### `serve_routes.py`

- Purpose: pure route matching and dispatch priority (root, export, pmtiles, surface_tile, static, api_runtime, api_inspect), testable without HTTP. (Confirmed)

#### `frontend/src/` modules

- `main.js` — bootstrap and MapLibre orchestration (large; read in sections). (Confirmed)
- `runtime_contract.js` — parses `/api/runtime`, maps score-layer labels/expressions, defines active fill/outline grid layers, lifecycle rebuild decisions, visibility plans, transport styling priority. (Confirmed)
- `api_client.js` — fetch wrappers with cold-start timeout + retry. (Confirmed)
- `amenity_filters.js`, `transport_filters.js`, `transport_reality_popup.js`, `noise_filters.js`, `landuse_filters.js` — filter/option logic per layer, each with a matching `*.test.js`. (Confirmed)
- `click_priority.js` — popup/action priority resolver (transport → amenity → service desert → noise → fine inspect → coarse grid). (Confirmed)
- `grid_debug.js` — the `/?debug-grid=1` diagnostics card. (Confirmed)
- `map_source_guard.js` — avoids asking MapLibre about `livability` load state before the source exists and delays source feature queries until loaded. (Confirmed)
- `static/index.html` — cache-busted asset loading. (Confirmed)

### 6.11 Rust: `walkgraph/`

- `main.rs` — clap CLI: `build`, `reachability`, `stats`, `surface`, `gtfs-refresh`; `FORMAT_VERSION = 3`. (Confirmed)
- `pbf.rs` — single-threaded PBF scan with `PRIVATE_VALUES`, `WALK_EXCLUDED`, `BIKE_EXCLUDED` filters. (Confirmed)
- `graph.rs` — CSR graph + adjacency sidecars; haversine edge length. (Confirmed)
- `reachability.rs` — rayon-parallel Dijkstra; `counts_for_origin`, `decayed_units_for_origin`. (Confirmed)
- `surface/` — `mod.rs` (auto thread count, clamped 2–6), `shard.rs` (point-in-polygon sampling + KD-tree), `itm.rs` (pure-Rust EPSG:2157), `npz.rs` (NPY/NPZ writer), `kdtree.rs`. (Confirmed)
- `gtfs/mod.rs` — GTFS parse + stop reality + frequency tiers (see §6.4).
- Perf notes: raising the `surface/mod.rs` thread clamp and aligning PMTiles bake workers with the Postgres pool (`db_postgis/engine.py`) are the cheap CPU wins; a Dijkstra rewrite and Python grid vectorization were assessed as not worth it. (Inference from a measured 1326 s full-profile run)

### 6.12 Basemap and scripts

#### `basemap.py`

- Purpose: builds/validates the self-hosted Protomaps basemap archive. (Confirmed)
- Key symbols: `build_basemap`, `build_if_stale`, `is_stale`, `load_manifest`, `source_fingerprint`, `docker_command`, `_require_docker_engine`, `validate_vendored_assets`, `missing_basemap_message`.
- `_handle_import` calls `build_if_stale()` — the reason `python main.py import` needs Docker. (Confirmed)
- Test: `tests/test_basemap.py` covers pin consistency, glyph-range completeness per font stack, manifest staleness, Docker command construction, CLI dispatch. (Confirmed)

#### `mapbox_vector_tile.py` (root)

- Root-level module (173 non-blank lines). Grep its importers before assuming the PyPI package is in use. (Confirmed present; role Inference)

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

Things that feed this chain:

- geometry / coastal-cleanup params, including the preferred main-island shapefile sidecars when present
- scoring caps and amenity tier-unit tables
- Overture dataset signature
- GTFS transit config hash and transit reality algorithm version
- importer config version (Lua path + content)
- schema / algorithm version constants

Important split: fine surface shell shards, grid cell shells, walk cell node snaps, and walk origin-node union caches are geo-derived geometry artifacts. GTFS-only changes invalidate reach/score/render/build layers as needed but do not move those geometry-only shell/snap cache paths. (Confirmed)

### Schema change

```text
db_postgis/migrations/versions/
db_postgis/tables.py
db_postgis/reads.py and/or writes.py
pmtiles_bake_worker.py          (if the field goes into a tile layer)
fine_vector_pmtiles_worker.py   (if the field must exist on fine vector grid features)
precompute/bake_pmtiles.py      (metadata)
serve_from_db.py                (if surfaced at runtime)
frontend/src/*                  (if consumed in the UI)
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

### Transport / GTFS-only contract

```text
config.py (TAGS["transport"], TRANSIT_REALITY_ALGO_VERSION, GTFS_* config)
transit/gtfs_download.py -> transit/workflow.py -> transit/rust_gtfs.py
walkgraph/src/gtfs/mod.rs
transit_derived.* + public transport_reality (db_postgis/tables.py, new migration)
db_postgis/reads.py::load_transport_reality_rows_for_scoring
precompute/phases.py (transport split) -> precompute/grid.py (transport category scoring)
precompute/workflow.py (zero-transport publish gate)
precompute/bake_pmtiles.py + pmtiles_bake_worker.py (transport tile fields)
serve_from_db.py (transport counts) + frontend/src/transport_*.js
tests/test_transit_phase1.py, tests/test_precompute_behavior.py (gate), tests/test_pmtiles_bake.py
```

### Noise overlay contract

```text
noise/loader.py  (legacy path)
noise_artifacts/{ingest,ogr_ingest,builder,dissolve,resolve,dev_fast,manifest,bake}.py
db_postgis/migrations/versions/20260424_000012_noise_polygons.py
db_postgis/tables.py, db_postgis/write_noise.py
precompute/publish.py, precompute/workflow.py (noise refresh paths)
pmtiles_bake_worker.py (noise_proxy SQL)
serve_from_db.py (noise counts + noise_pmtiles_url)
frontend/src/noise_filters.js, frontend/src/main.js
tests/test_noise_loader.py, tests/test_noise_artifacts.py, tests/test_pmtiles_bake.py
frontend/src/noise_filters.test.js, frontend/src/noise_proxy_controls.test.js
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
pmtiles_bake_worker.py SQL templates (coarse)
fine_vector_pmtiles_worker.py feature properties / geometry encoding (fine)
frontend layer expectations
tests/test_pmtiles_bake.py (+ tests/test_fine_vector_pmtiles_worker.py for fine grid)
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

### Basemap contract

```text
config.py PROTOMAPS_* pins + BASEMAP_* paths
basemap.py (+ basemap/docker-compose.yml)
static/basemap/ vendored glyphs/sprites/style assets
serve_from_db.py (basemap route + --deployment validation)
docs/BASEMAP.md
tests/test_basemap.py
```

---

## 8. Configuration Surface

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Full SQLAlchemy / PostGIS connection string; `connect_timeout=15` appended when absent | None |
| `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT` | DB fallback parts rendered through the same URL | None |
| `GTFS_NTA_ZIP_PATH` | Default zip path for the `nta` static feed | `gtfs/nta_gtfs.zip` |
| `GTFS_TRANSLINK_ZIP_PATH` | Default zip path for the `translink` static feed | `gtfs/translink_gtfs.zip` |
| `GTFS_NTA_URL`, `GTFS_TRANSLINK_URL` | Public GTFS ZIP URLs (overrideable) | unset; set explicitly to refresh from network |
| `GTFS_ANALYSIS_WINDOW_DAYS` | Transit analysis window | `30` |
| `GTFS_SERVICE_DESERT_WINDOW_DAYS` | Service-desert window | `7` |
| `GTFS_LOOKAHEAD_DAYS` | Transit lookahead window | `14` |
| `GTFS_AS_OF_DATE` | Override analysis date | unset → today in Europe/Dublin |
| `WALKGRAPH_BIN` | Explicit walkgraph binary path | auto-detected |
| `WALK_ROUTING_BACKEND` | Pedestrian reachability backend | `"walkgraph"`; `"valhalla"` opt-in |
| `VALHALLA_URL` | Local Valhalla base URL | `"http://127.0.0.1:8002"` |
| `VALHALLA_EXPECTED_VERSION` | Exact `/status` version required before routing | `"3.8.3"` |
| `VALHALLA_WORKERS` | Bounded Valhalla matrix-request concurrency | `8` |
| `VALHALLA_TIMEOUT_S` | Per-request Valhalla timeout | `30.0` |
| `VALHALLA_MAX_TARGETS_PER_REQUEST` | Matrix target batch size | `100` |
| `LIVABILITY_SURFACE_THREADS` | Fine-surface worker thread count | unset → all CPUs |
| `LIVABILITY_BAKE_WORKERS` | PMTiles bake worker count | `min(12, cpu_count())` |
| `LIVABILITY_FINE_RASTER_SURFACE` | Enable inspect-backed fine surface caches + legacy PNG endpoint | `"1"` |
| `LIVABILITY_RUNTIME_ALLOW_STALE_DEV_RUNTIME` | Allow stale-manifest dev fallback in the runtime server | `0` |
| `LIVABILITY_MIGRATE_LEGACY_REACH_CACHE` | Convert legacy pickle/gzip reachability caches | `0` |
| `COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2` | Skip opening step for very large coastal components | `1_000_000_000.0` |
| `OSM2PGSQL_BIN` | osm2pgsql binary path | `"osm2pgsql"` |
| `NOISE_INGEST_MODE` | Noise source ingest mode | `auto` (prefers `ogr2ogr`) |
| `NOISE_INGEST_COPY_BATCH_ROWS`, `NOISE_INGEST_FLUSH_ROWS` | Python ingest COPY/flush windows | module defaults |
| `NOISE_ALLOW_LEGACY_CANDIDATE_CACHE` | Read legacy single-file candidate cache | `0` |
| `NOISE_ROAD_GDB_CANONICAL_CACHE` | ROI Round 4 Road FileGDB → local GPKG cache path | `"1"` |
| `NOISE_REBUILD_ROAD_GDB_CACHE` | Force canonical Road GDB GPKG cache rebuild | `"0"` |
| `NOISE_REBUILD_DEV_FAST_GRID` | Force dev-fast road/rail grid cache rebuild | `"0"` |
| `NOISE_DEV_FAST_ARROW_BATCH_SIZE` | Arrow streaming batch size for dev-fast reads | `16384` |
| `NOISE_DEV_FAST_PARALLEL_SPECS` | Parallel dev-fast road/rail processing | `"1"` |
| `NOISE_DEV_FAST_MAX_WORKERS` | Worker cap for dev-fast parallel specs | `2` |
| `NOISE_ROAD_NORMALIZE_BATCH_SIZE` | Road raw-stage normalization batch (`source_fid`) | `5000` |
| `NOISE_OGR2OGR_TIMEOUT_SECONDS` | Hard timeout per `ogr2ogr` subprocess | `300` |
| `NOISE_SQL_STATEMENT_TIMEOUT_SECONDS` | Per-normalize statement timeout | `900` |
| `NOISE_SQL_LOCK_TIMEOUT_SECONDS` | Per-normalize lock timeout | `30` |
| `NOISE_MIN_FREE_DISK_GB` | Disk preflight before artifact builds | module default |
| `NOISE_TERMINATE_STALE_IMPORT_BACKENDS` | Terminate stale `pg_stat_activity` sessions from crashed ingest | `0` |
| `NOISE_TERMINATE_STALE_STAGE_LOCKS` | Terminate stale lock holders on `_noise_raw_*` targets | `0` |
| `NOISE_PYTHON_EXE` | Python used by the Windows noise watchdog | `.venv` → conda fallback |
| `DATASET_MIRROR_BASE_URL` | Replace `setup.sh`'s core input mirror | GitHub release `v1` |
| `scripts/win/bench_valhalla.ps1 -Workers` | Client concurrency for the Valhalla benchmark | `8` |

### Important config files

| File | Role |
|---|---|
| `config.py` | Canonical project config |
| `alembic.ini` | Alembic runner config |
| `osm2pgsql_livability.lua` | OSM tag filter / import rules (no transport tags) |
| `environment.yml` | Conda-forge Windows GDAL/PostGIS runtime spec (`livability-gdal`) |
| `requirements.txt` | Python dependencies |
| `.env.example` | Local env template (GTFS block documents feeds + the publish gate) |
| `scripts/win/geo_env.cmd` | Windows CMD launcher: activates Miniforge env, pins GDAL/PROJ/noise env vars, hydrates `NOISE_OGR2OGR_*` from project `.env` |
| `scripts/win/bootstrap_geo_env.cmd` | First-time Windows setup: activate conda base, create `%GEO_CONDA_ENV%` from `environment.yml` via mamba when missing |
| `scripts/win/check_geo_env.cmd`, `selftest_geo_env.cmd` | GDAL/PostGIS driver sanity checks and post-check smoke test |
| `scripts/win/precompute_dev.cmd` | Dev precompute through `geo_env.cmd` |
| `scripts/win/precompute_noise_dev.cmd`, `precompute_noise_accurate.cmd` | Strict reuse-only noise wrappers (require a mode-matched artifact; never `--force-precompute`) |
| `scripts/win/prepare_noise_artifact_dev.cmd`, `prepare_noise_artifact_accurate.cmd` | Cache-aware artifact refresh wrappers |
| `scripts/win/force_noise_artifact_dev.cmd`, `force_noise_artifact_accurate.cmd` | Full source reimport + resolved rebuild wrappers |
| `scripts/win/test_noise.cmd` | Targeted noise test wrapper |
| `pytest.ini` | Pytest scope: `testpaths = tests`, excludes generated/cache dirs (`.tmp`, `.tmp-test`, `cache`, `walkgraph/target`, `frontend/node_modules`, …) |
| `frontend/package.json` | Frontend dependency and build scripts |

### Runtime assumptions

| Operation | Prerequisites |
|---|---|
| Any pipeline command | reachable PostGIS config |
| `import` | local OSM PBF + `osm2pgsql`; **Docker** because of the basemap refresh step |
| `gtfs status` / `gtfs refresh` | network access to configured GTFS ZIP URLs (unless local-only paths are configured) |
| `transit` | cached GTFS ZIP(s) by default; add `--auto-refresh-gtfs` / `--force-gtfs-refresh` to download/update feeds, and a compiled `walkgraph` with `gtfs-refresh` |
| `precompute` | managed schema, raw import ready or `--auto-refresh-import`, preferred `ireland_main_island_shp` coastline or fallback boundaries, compiled `walkgraph`, and GTFS transport rows (or `--allow-missing-transport`) |
| `serve` | completed precompute build + main PMTiles archive for the chosen profile; noise PMTiles optional; `--deployment` additionally requires the basemap archive and vendored assets |
| Overture merge | `overture/ireland_places.geoparquet` present; otherwise degrades gracefully |
| Noise overlay | `noise_datasets/*.zip` present; ROI Round 4 road requires `pyogrio`/GDAL FileGDB support |
| Valhalla backend | local Valhalla service at `VALHALLA_URL` matching `VALHALLA_EXPECTED_VERSION` |

---

## 9. Risky / Misleading Areas

- **Two `network` modules.** Root `network/` is a package holding `load_walk_graph_index`; `precompute/network.py` holds reachability math. `from network import …` resolves to the package.
- **Empty decoy directory** at the repo root named `ireland-and-northern-ireland-latest.osm.pbf/`. The real extract is `osm/ireland-and-northern-ireland-latest.osm.pbf`.
- **`import` needs Docker** because `_handle_import` calls `basemap.build_if_stale()`. On a Docker-less host, use `precompute --auto-refresh-import` and run the pipeline without `import`.
- **Zero-transport publish gate**: `precompute/workflow.py` raises when the transport category is empty unless `--allow-missing-transport` is passed (then it warns and transport scores are zero). Both behaviours have tests.
- **Noise is display-only.** The overlay never feeds scoring; road/rail rows are grid proxies, not measured point noise, and the overlay is intentionally not clipped to the land mask.
- **Mode-aware transit is a stub**: `phase_reachability_impl` passes `transit_effective_units_by_node=None`, so only walk (default) and bike comparisons are real.
- **Transport scoring is strictly GTFS-only.** `TAGS["transport"] = {"source": "gtfs_direct"}`, `_AMENITY_CATEGORY_VALUES` excludes transport, and OSM stop helpers were removed (`load_osm_transport_features`, `clear_normalized_network_rows`, `transit/chained_reach.py` do not exist — do not import or "restore" them without an explicit task).
- `pmtiles_bake_worker.py` import isolation is real, and `fine_vector_pmtiles_worker.py` exists specifically so Windows bake workers do not import `precompute/__init__.py`. Do not collapse them back together casually.
- `static/dist/` is a checked-in build artifact. Editing `frontend/src/*` without rebuilding leaves runtime stale; CI fails on a dirty bundle.
- `schema.sql` is a snapshot, not schema truth. Treat Alembic as canonical.
- `render_from_db.py` is only a wrapper. The real server is `serve_from_db.py`.
- `config.resolution_for_zoom()` is not `pmtiles_bake_worker._resolution_for_zoom()`.
- `HASHES = build_config_hashes()` runs at import time and silently tolerates missing files by hashing zero-like metadata (renaming an input can therefore reuse caches instead of failing).
- `_STATE = _BuildState.bootstrap()` also runs at import time and is invalid until activation.
- `transit_derived.service_desert_cells` has a foreign key to `build_manifest`; desert rows are written after `publish_precomputed_artifacts()` inserts the manifest row for the same `build_key`.
- `extract_fingerprint()` caches the `.osm.pbf` content hash in `.livability_cache/osm_extract_fingerprint_cache.json`, keyed by resolved path + size + `mtime_ns`. Deleting/corrupting it only costs startup time.
- `COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2` has a non-zero live default even though the nearby comment still says "default 0 = disabled". Trust the constant.
- `overture/ireland_places.geoparquet`, `ireland_main_island_shp/*`, `boundaries/*.geojson`, `gtfs/*.zip`, `noise_datasets/*` are external inputs, not committed assets.
- Overture rows carry zero park/footprint areas (`park_area_m2 = 0.0`) because no area fetch path exists; park scoring depends on OSM area fields.
- OSM import reuse is manifest/scope-aware: raw rows count as ready only with a complete import manifest whose `normalization_scope_hash` matches the active profile; otherwise rows are dropped and rebuilt.
- Both Python and Rust GTFS parsers accept `calendar.txt`-only and `calendar_dates.txt`-only feeds, but require at least one service calendar file.
- Frontend click priority is transport → amenity → service desert → noise → fine inspect → coarse grid (`frontend/src/click_priority.js`).
- MapLibre rejects `filter: null`, so the land-use style omits `filter` entirely when all classes are selected.
- NI Round 1 noise shapefiles are class-coded (`GRIDCODE` 1..7 with `Noise_Cl` labels), not threshold-coded; threshold arithmetic on Round 1 produces invalid labels like `2-6`.
- Transport reality is intentionally sparse at low zoom in some NI tiles: the Belfast z9 tile carries only a handful of hub-like features, so a sparse screenshot there is not proof of missing data.
- Noise artifact ingest stages rows in a temp table (`noise_ingest_stage_*`) then normalizes in SQL (`ST_GeomFromWKB` → `ST_Transform` → `ST_MakeValid`) instead of giant inline `VALUES` statements.
- Noise force semantics are split: resolved rebuild (`--force-noise-artifact`) vs source re-import (`--reimport-noise-source`); `--force-noise-all` does both.
- Accurate noise mode reads all rounds and simplifies road/rail only inside the dissolve CTE; canonical `noise_normalized` rows must not be updated in place.
- Artifact-mode proxy publish enforces Phase E mixed-source behavior: road/rail from `noise_grid_artifact` with per-cell metric/band mapping, airport/industry from active `noise_resolved_display` rows. A missing grid artifact hash is a hard `noise_proxy_blocked` state.
- `progress_tracker.py` is intentionally defensive: if tracking breaks, the build continues, so ETA regressions can hide without failing tests.
- Reachability large-cache recovery is mixed-format: `{key}.pkl(.gz)` base + `{key}.chunks.pkl(.gz)` overlay journal. Preserve that merge order.
- Reachability origin-node helpers assume a split contract: `normalize_origin_node_ids(...)` produces sorted unique lists and `merge_normalized_origin_node_ids(...)` unions already-normalized lists. Do not use `sorted(set(...))` on multi-million sequences.
- The legacy `/tiles/surface/{resolution}/{z}/{x}/{y}.png` endpoint still exists for compatibility, but `/api/runtime` no longer advertises it and the frontend does not use it.
- Fine-grid render correctness spans three layers: buffered geometry in `fine_vector_pmtiles_worker.py`, degenerate-ring filtering before MVT encoding, and active-layer recreation in `frontend/src/main.js` / `runtime_contract.js`. If seams reappear, inspect all three before blaming the archive.
- Test environment gotchas: `.venv` can be broken (e.g. a namespace-package `pyarrow` without `__version__`) — use the conda env through `scripts\win\geo_env.cmd`; and tests using `TemporaryDirectory` can fail on `chmod` inside a restricted sandbox (not a code failure).
- `tests/test_transit_phase1.py` snapshot regressions are skipped unless local GTFS zips are present, so a green run there may mean "skipped", not "verified".

---

## 10. Tests and Validation Signals

| Test file | What it covers |
|---|---|
| `tests/test_config.py` | config hash stability, env parsing, schema-version invalidation |
| `tests/test_main_cli.py` | CLI dispatch for import/basemap/gtfs/transit/precompute/serve, profile forwarding, `--allow-missing-transport` |
| `tests/test_precompute_behavior.py` | phase sequencing, cache/hash behavior, service-desert publish summaries, **zero-transport publish gate + allow flag** (4.0k lines; use its helpers) |
| `tests/test_precompute_planner.py` | pure import / noise-artifact / build planning decisions |
| `tests/test_precompute_cache.py` | tier cache read/write/invalidation helpers |
| `tests/test_mode_aware_scoring.py` | mode-aware weighted payloads and score-hash invalidation for mode weights |
| `tests/test_amenity_tiers.py` | shop / healthcare / park tier classification |
| `tests/test_overture_loader.py` | Overture category filtering and park handling |
| `tests/test_osm_import_handling.py` | osm2pgsql wrapper, import manifest behavior, Lua/SQL expectations |
| `tests/test_local_osm_rules.py` | Lua/importer rule mapping |
| `tests/test_road_proximity.py` | maxspeed parsing, class calibration, distance decay, strongest-road selection, empty/invalid geometry |
| `tests/test_transit_phase1.py` | GTFS reality rows, weekly bus subtiers, headway buckets, frequency windows, score units, snapshot stop regressions (skipped without zips), school-only classification, `gtfs-refresh` artifacts |
| `tests/test_transit_refresh_runner.py` | lightweight transit CLI path behavior |
| `tests/test_gtfs_download.py` | conditional requests, atomic replacement, manifest/freshness validation |
| `tests/test_network_loader.py` | walkgraph graph index loading |
| `tests/test_pmtiles_bake.py` | tile field lists, layer metadata, amenity `tier`, road/rail score fields, bounded parallel scheduling, retry, temp-output cleanup, archive preservation, streamed noise tile specs |
| `tests/test_fine_vector_pmtiles_worker.py` | fine-grid shard aggregation, degenerate-ring rejection, per-zoom resolutions, mixed z15, invalid-land skipping, buffered continuity, bounded LRU caches |
| `tests/test_surface_runtime.py` | fine-surface runtime behavior and atomic PNG cache writes |
| `tests/test_server_behavior.py` | runtime API shape/reload, transport + noise counts, PMTiles cache tokens/ETags, inspect/surface input validation, streaming, range serving |
| `tests/test_serve_routes.py` | pure route matching and dispatch priority |
| `tests/test_basemap.py` | Protomaps pin consistency, glyph-range assets per font stack, manifest staleness, Docker command construction, CLI dispatch |
| `tests/test_noise_loader.py` | ROI dB normalization, round-aware NI mapping, unknown-code errors, newest-round fallback |
| `tests/test_noise_artifacts.py` | manifest SQL safety, ingest diagnostics, streaming ingest, dev-fast grid identity, non-mutating accurate simplification, CLI surface |
| `tests/test_noise_ogr_ingest.py` | ogr2ogr command building, canonical GPKG path, field selection, staging |
| `tests/test_noise_accuracy_mode.py`, `test_noise_mode_artifact.py`, `test_noise_mode_guard.py` | noise accuracy/mode dispatch and guards |
| `tests/test_noise_qa_snapshots.py` | noise QA snapshot comparisons |
| `tests/test_progress_tracker.py` | timing-history sanitization and persistence |
| `tests/test_sanity_check.py` | sanity fixture structure and runtime lookup mode selection |
| `tests/test_db_constraint_migration.py` | read-only preflight, PK/FK application, missing tables, downgrade safety |
| `tests/test_db_integrity_check.py` | duplicate detection, NULL key validation, missing tables, ambiguous-key skipping |
| `tests/test_db_postgis_writes.py` | publish/bulk-copy write paths |
| `tests/test_coastal_cleanup.py` | coastal cleanup fallback behavior and summaries |
| `tests/test_valhalla_reachability.py` | opt-in Valhalla backend integration behavior |
| `tests/test_walkgraph_support.py` | walkgraph binary discovery/subcommand checks |
| `tests/test_refresh_osm.py` | OSM download/refresh helper behavior |
| `frontend/src/runtime_contract.test.js` | runtime parsing, score-layer mapping, noise source wiring, grid layer definitions, lifecycle rebuilds, transport styling priority |
| `frontend/src/grid_debug.test.js`, `grid_popup.test.js` | debug card + score popup rendering |
| `frontend/src/map_source_guard.test.js` | source guard behavior (no `isSourceLoaded()` errors) |
| `frontend/src/transport_filters.test.js`, `transport_reality_popup.test.js` | weekly bus tiers, exact rail/tram matching, exception-only logic, multi-row popups |
| `frontend/src/noise_filters.test.js`, `noise_proxy_controls.test.js` | noise options/filters, control wiring, synced opacity |
| `frontend/src/amenity_filters.test.js`, `landuse_filters.test.js`, `landuse_context_controls.test.js` | amenity/land-use filter and control logic |
| `frontend/src/click_priority.test.js` | popup/action priority resolution |

Areas still relatively fragile:

- `overture/merge.py` has no dedicated single-purpose test file despite being large and high-impact.
- `_compute_service_deserts()` still lives inline in `precompute/__init__.py`, which makes it easy to miss.
- Fine-grid correctness spans bake tests + the fine-vector worker test; exact high-zoom explainability still depends on `/api/inspect`.
- Sanity-fixture ranges are broad and structural, so `sanity_check` proves plumbing, not score correctness.
- CI runs Python, Rust, npm test/build, and sanity validation, but there is no real PostGIS-backed integration job.

---

## 11. Legacy / Snapshot Areas

| Path | Status | How to treat it |
|---|---|---|
| `render_from_db.py` | Compatibility wrapper | Edit `serve_from_db.py` instead |
| `schema.sql` | Bootstrap snapshot | Read only for orientation |
| `static/dist/*` | Built frontend artifacts | Rebuild from `frontend/src/` and commit |
| `legacy/` | Local-only historical reference | Do not import into active runtime |
| `.livability_cache/` | Generated outputs, caches, timing history | Never edit manually |
| `walkgraph/target/` | Rust build output | Rebuild with Cargo |
| `docs/*.md` | Human context, design, methodology | Useful for intent, not runtime truth |
| `reports/`, `.test_logs/`, `.agents/` | Empty/local scratch dirs | Ignore |

Local disk hotspots:

- `osm/flat-nodes.bin` (~37 GiB) dominates when the OSM import cache exists; `.livability_cache/` grows large from rebuildable PMTiles, surface shards, and geo/noise caches. Both are generated state, not source.

---

## 12. Glossary

| Term | Meaning here |
|---|---|
| Grid cell | Scored polygon unit at a given resolution |
| Coarse vector resolution | `20000`, `10000`, or `5000` meter grid published to PMTiles |
| Fine vector resolution | `2500`, `1000`, `500`, `250`, `100`, `50` meter grid features baked into PMTiles and overzoomed at runtime |
| PMTiles | Single-file vector tile archive served over HTTP range requests; separate main livability and optional noise archives |
| MVT | Mapbox Vector Tile bytes emitted from PostGIS `ST_AsMVT` |
| CAPS | Per-category score ceilings: `{"shops": 6, "transport": 5, "healthcare": 5, "parks": 5}` |
| Amenity tier | Stored subtype such as `corner`, `supermarket`, `clinic`, `regional` |
| Score units | Integer weight attached to a tier, consumed by scoring |
| Cluster count | Count of reachable scoring clusters after collapsing near-duplicate amenities within a category |
| Effective units | Distance-decayed float scoring input derived from reachable cluster representatives |
| Transit reality | GTFS-only stop-reality layer with status, departure counts, public frequency windows, weekday daytime bus tiers, and transport score units; unavailable GTFS means no transport signal |
| Publish gate | Precompute refusal to publish a build whose transport category is empty (bypass: `--allow-missing-transport`) |
| Bus frequency tier | Headway-derived bus tier (`BUS_FREQUENCY_TIER_UNITS`) feeding transport score units |
| Service desert | Cell with at least one reachable baseline GTFS stop but zero public departures in the desert window |
| Noise contour | Official display-only Lden/Lnight polygon from ROI EPA or NI OpenDataNI data, normalized into dB bands and source types |
| Noise artifact | Versioned source→domain→resolved→tiles pipeline (`noise_artifact_manifest` / `_lineage` / `_active_artifact`) feeding the display overlay |
| Conflict class | Amenity merge status: `osm_only`, `overture_only`, `source_agreement`, `source_conflict`; GTFS-direct transport rows use `gtfs_direct` |
| Geo hash | Geometry/import-level cache hash used for study area, walkgraph, fine surface shell, and geometry-only grid/snap caches |
| Surface shell hash | Fine-surface shell identity derived from `geo_hash` plus shell/grid schema and geometry parameters |
| Reach hash | Reachability-level hash including geo hash, transit reality, tags, merge version, Overture signature; opt-in Valhalla additionally hashes backend, integration algorithm version, and expected Valhalla version |
| Score hash | Scoring-level hash including reach hash, caps, tier-unit tables, resolution tiers; owns scored walk cells and fine surface score arrays |
| Build key | Build-scoped identifier used on published PostGIS rows |
| Build profile | `full`, `dev`, or `test` |

---

## 13. Recommended Reading Order

1. `config.py` — constants + hash spine (grep; do not read whole)
2. `main.py` — the CLI surface you will actually drive
3. `precompute/_planning.py` — what the pipeline decides before it runs
4. `precompute/workflow.py` — how those decisions execute (and the publish gate)
5. `precompute/phases.py` — the scoring pipeline itself
6. `precompute/grid.py` + `precompute/amenity_tiers.py` — the scoring math
7. `db_postgis/tables.py` + `db_postgis/reads.py` — persisted shape of the data
8. `precompute/bake_pmtiles.py` + `pmtiles_bake_worker.py` — how data becomes tiles
9. `serve_from_db.py` + `serve_routes.py` — how it is served
10. `frontend/src/runtime_contract.js` + `frontend/src/main.js` — how it is drawn
11. `transit/workflow.py` + `walkgraph/src/gtfs/mod.rs` — the GTFS-only transport spine
12. `noise/loader.py` + `noise_artifacts/bake.py` — the display-only overlay
13. `overture/loader.py` + `overture/merge.py` — optional amenity enrichment
14. `study_area.py` — the coast mask every geometry artifact depends on
15. `tests/test_precompute_planner.py` + `tests/test_precompute_behavior.py` — executable intent (use the helpers)
16. `README.md` + `docs/PHASES.md` + `docs/notes.md` — product/methodology context when behaviour is ambiguous
