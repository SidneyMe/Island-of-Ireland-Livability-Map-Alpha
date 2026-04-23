# Changelog

Format: date, version tag (where applicable), what changed, what scoring logic changed.

---

## 2026-04-22 - Transit bus subtiers, unscheduled stops, and exact transport PMTiles rows

### Added

- Migration `20260422_000009_add_transit_bus_subtiers.py`: adds `bus_active_days_mask_7d`, `bus_service_subtier`, `is_unscheduled_stop`, `has_exception_only_service`, `has_any_bus_service`, and `has_daily_bus_service` to `transit_derived.gtfs_stop_service_summary`, `transit_derived.gtfs_stop_reality`, and public `transport_reality`
- Weekly stop-level bus subtier derivation in the Python and Rust GTFS paths:
  - `mon_sun`
  - `mon_sat`
  - `tue_sun`
  - `weekdays_only`
  - `weekends_only`
  - `single_day_only`
  - `partial_week`
- Explicit unscheduled-stop emission for stops present in `stops.txt` but never referenced by `stop_times.txt`
- Runtime summary counts for transport subtiers and transport flags (`transport_subtier_counts`, `transport_flag_counts`)
- `frontend/src/transport_filters.js` + tests for the new weekly-tier / exception-only transport filter logic

### Changed

- `TRANSIT_REALITY_ALGO_VERSION = 6`
- `PMTILES_SCHEMA_VERSION = 6`
- Weekly bus tiering now uses the retrospective 7-day service-desert window only; the legacy departure counts and `reality_status` fields still keep the existing lookahead-aware snapshot behavior for compatibility
- PMTiles `transport_reality` tiles now emit one feature per published stop row instead of grouping same-name same-coordinate rows
- Transport popups now render all colocated transport features deterministically instead of taking the first rendered feature only
- The frontend transport overlay now uses weekly-service colors and filters instead of the old green/red active-vs-inactive framing

### Fixed

- Colocated transport rows are no longer re-aggregated back into summed departure counts inside the PMTiles SQL layer
- Truly unscheduled GTFS stops are no longer silently omitted from `gtfs_stop_reality`
- Snapshot-based transport semantics are now clearer in the popup wording and runtime metadata

### Scoring logic

- No scoring logic change. `reality_status` remains the compatibility field for transport scoring and service-desert weighting.

## 2026-04-22 - Fine-vector PMTiles grid, Cork test profile, and grid diagnostics

### Added

#### Fine vector bake path

- `fine_vector_pmtiles_worker.py`: Windows-safe fine-grid MVT worker that loads shell and score shards directly, aggregates canonical 50 m surfaces into 2500 / 1000 / 500 / 250 / 100 / 50 m polygons, clips them with a tile buffer, and encodes local `grid` layer bytes
- `mapbox_vector_tile.py`: lightweight local vector-tile encoder used by the fine-grid worker path
- `tests/test_fine_vector_pmtiles_worker.py`: dedicated coverage for encoded per-zoom resolutions, buffered border continuity, degenerate-ring rejection, and worker cache limits

#### Test profile and CLI

- `config.py`: new `test` build profile using the full fine-resolution ladder against a compact Cork city bbox `(-8.55, 51.87, -8.41, 51.93)` and writing `livability-test.pmtiles`
- `main.py`: `--precompute-test`, `--serve-test`, and `--render-test`
- `serve_from_db.py`: profile-specific missing-precompute guidance now points at the correct precompute flag for `full`, `dev`, or `test`

#### Frontend diagnostics

- `frontend/src/grid_debug.js`: persistent `/?debug-grid=1` control-panel card with live source-vs-rendered counts, layer/source state, diagnosis text, and a copyable plain-text snapshot
- `frontend/src/grid_debug.test.js`: regression coverage for the debug card, diagnosis states, and snapshot formatting

### Changed

- `precompute/bake_pmtiles.py`: PMTiles bake now stitches coarse SQL tiles through z11 together with sparse fine vector grid tiles at z12-z15, with the archive source max zoom capped at 15 and the browser expected to overzoom to 19
- `frontend/src/runtime_contract.js` and `frontend/src/main.js`: main grid rendering is now vector-only, rebuilding one active fill / outline / debug layer trio as the zoom band changes instead of swapping raster fine-surface layers
- `/api/runtime` no longer advertises `surface_tile_url_template`; exact click inspection continues through `/api/inspect`
- `static/index.html` and `static/app.css`: the fixed control panel now scrolls internally so stacked debug and amenity controls remain reachable

### Fixed

- Parallel PMTiles baking now bounds in-flight work, clamps fine-grid bake workers to 4, retries once at half worker count after `BrokenProcessPool`, and only replaces the final archive after temp-file finalize succeeds
- Failed bakes now clean up temp output instead of leaving behind partial archives
- Fine-grid polygon encoding now buffers neighboring border cells and drops degenerate clipped rings, closing high-zoom seam and sliver failure cases covered by the new worker tests
- `/` and `/static/*` now ship with `Cache-Control: no-store`, and expected client aborts on `/api/inspect` are suppressed from server logs like PMTiles range disconnects

### Scoring logic

- No scoring logic change. This patch changes how fine-resolution grid data is baked, served, and debugged, not how category scores are computed.

## 2026-04-20 - Phase 2 amenity tiers, sub-tier filters, and park-source cleanup

### Added

#### Tiered amenity scoring

- `precompute/amenity_tiers.py`: classifies scored amenities into sub-tiers before routing/scoring and annotates each row with `tier` and `score_units`
- Tier unit tables in `config.py` for shops, healthcare, and parks; OSM footprint-aware and area-aware thresholds added for shop size and park size classification
- Generic weighted reachability cache `walk_weighted_units_by_origin_node` replaces the old park-only weighted cache so shops, healthcare, and parks can all score by weighted units

#### Runtime and publish metadata

- Migration `000007`: adds nullable `tier` to the managed `amenities` table
- `precompute/publish.py`: summary payload now includes `amenity_tier_counts` alongside the existing top-level `amenity_counts`
- PMTiles amenities layer now publishes `tier` as a feature property; `PMTILES_SCHEMA_VERSION = 3`
- `serve_from_db.py`: `/api/runtime` now exposes `amenity_tier_counts`

#### Frontend amenity filtering

- `frontend/src/amenity_filters.js`: runtime-driven helpers for amenity sub-tier filtering
- `frontend/src/main.js`: nested multi-select dropdowns for `shops`, `healthcare`, and `parks`; transport remains a plain toggle
- Amenity popup now shows the tier label when present

#### Tests

- `tests/test_amenity_tiers.py`: coverage for tier boundaries and Overture fallback classification
- `tests/test_server_behavior.py`, `tests/test_surface_runtime.py`: runtime contract coverage for `amenity_tier_counts`
- `tests/test_pmtiles_bake.py`: verifies `tier` is exported in the amenities layer metadata and tile SQL
- `tests/test_overture_loader.py`: regression coverage for excluding Overture `garden` rows while keeping real park sources

### Changed

- `config.py`: scoring caps now use weighted-unit saturation - `shops=6`, `transport=5`, `healthcare=5`, `parks=5`
- `db_postgis/reads.py`: OSM source amenity rows now carry `footprint_area_m2` for polygon-based tiering
- `overture/loader.py`: retained raw primary category / brand / confidence metadata for conservative fallback classification
- `precompute/phases.py`, `precompute/grid.py`, `precompute/surface.py`: shops, healthcare, and parks now score from weighted units; transport remains count-based
- `CACHE_SCHEMA_VERSION = 10` to invalidate stale scoring caches after the tiered scoring change

### Fixed

- `garden` is no longer treated as a park source in `config.py`, `overture/loader.py`, or `osm2pgsql_livability.lua`
- Small residential / ornamental gardens no longer appear in the park overlay and no longer contribute to park counts or park scores after rebuild
- `AMENITY_MERGE_ALGO_VERSION = 4`; reach-tier invalidation now also includes a stable Overture category-map signature so category-map changes cannot reuse stale reach / score caches

### Scoring logic (Phase 2)

- Top-level category weights remain equal at 25 points each
- Transport is still raw-count based: cap `5`, one reachable stop = one scoring unit
- Shops now score by weighted units: `corner=1`, `regular=2`, `supermarket=3`, `mall=5`, cap `6`
- Healthcare now scores by weighted units: `local=1`, `clinic=2`, `hospital=3`, `emergency_hospital=4`, cap `5`
- Parks now score by weighted units: `pocket=1`, `neighbourhood=2`, `district=3`, `regional=4`, cap `5`
- Score formula stays `min(units, cap) / cap * 25`
- `garden` contributes `0` because it no longer enters the park amenity pipeline

---

## 2026-04-19 - OSM + Overture amenity deduplication

### Added

#### Deduplication pipeline

- `overture/merge.py`: spatial deduplication engine matching OSM and Overture amenities; two-pass strategy - proximity match within 35 m (auto), name-normalised alias match within 75 m; cross-category matching supported for ambiguous Overture categories
- `db_postgis/amenity_merge.py`: DB-side orchestration; writes resolved canonical amenity rows, suppresses Overture POIs that duplicate an OSM entry; OSM self-deduplication pass removes duplicate OSM nodes within 10 m
- Migration `000006`: extends the amenities table with merge-provenance columns (source, overture_id, merge_path)
- `overture/loader.py`: `dataset_info()` / `dataset_signature()` - stable hash of the GeoParquet file + state file, used to invalidate cached merge results when the Overture dataset is replaced

#### Config

- `AMENITY_MERGE_ALGO_VERSION = 3` - bumping this constant forces a full merge rebuild without requiring a dataset change
- Overture dataset signature and release tag are now folded into `reach_hash` and `build_hashes_for_import`, so any dataset update automatically invalidates the precompute cache

#### Category mapping

- Overture `ov_*` overlay categories (`ov_shops`, `ov_healthcare`, `ov_parks`) collapsed into the canonical scoring categories (`shops`, `healthcare`, `parks`); Overture POIs that survive deduplication now contribute directly to scoring rather than as a separate visualization layer
- Corresponding `ov_*` entries removed from `CATEGORY_COLORS` in `config.py`

#### Tests

- `tests/test_precompute_behavior.py`: 673-line suite covering merge category resolution, OSM self-dedupe, proximity and alias matching, cross-category suppression, and pipeline idempotency
- `tests/test_osm_import_handling.py`: additional cases for amenity provenance tracking through the import path
- `tests/test_config.py`: hash invalidation tests for `AMENITY_MERGE_ALGO_VERSION` and `overture_dataset_signature`
- `tests/test_db_postgis_writes.py`: 98 new cases for the DB-side merge orchestration layer

### Changed

- Overture POIs previously rendered as a separate visualization overlay are now merged into the main amenity dataset and participate in livability scoring
- `precompute/phases.py`: amenity merge phase wired into the precompute pipeline after OSM import and before surface computation

---

## 2026-04-22 - Remove standalone Local Link from active GTFS inputs

### Changed

- Active GTFS feed selection now uses NTA + Translink only.
- `config.py::transit_feed_configs()` no longer configures the standalone TFI Local Link feed, so `--refresh-transit` and `--precompute` ignore any leftover `locallink_gtfs.zip` artifact.
- The rationale is feed selection, not cross-feed dedupe: current NTA GTFS is treated as the Republic-side source of truth and already covers the Local Link service set we want in the active pipeline.
- Docs and examples now describe NTA + Translink as the active GTFS inputs.

---

## 2026-04-18 - Phase 1 Complete: GTFS ingestion and transit reality pipeline

### Added

#### GTFS ingestion

- `transit/` module (11 files): feed downloading, ZIP parsing, service window expansion, school-run classification, departure summarisation, stop reality derivation, GeoJSON export, Rust subprocess bridge, workflow orchestration
- Three feeds were initially configured and ingested: NTA (Republic of Ireland), Translink (Northern Ireland), TFI Local Link (rural/regional). This was later narrowed to NTA + Translink in the active pipeline on 2026-04-22.
- Rust GTFS pipeline (`walkgraph/src/gtfs/`): processes all feeds, expands calendar windows, counts departures per stop per service, writes CSV artifacts consumed by the Python loader
- `walkgraph/tests/gtfs_cli.rs`: integration test harness for the Rust GTFS CLI path
- `walkgraph_support.py`: Python bridge that locates and invokes the walkgraph binary

#### Transit reality

- Phantom stop detection: stops with zero public departures in the 30-day analysis window flagged `inactive_confirmed` and excluded from scoring
- School-run filtering: services detected as school-only (keyword match + time-bucket concentration + weekday-only pattern) excluded from public departure counts; stops serving only school runs flagged `school_only_confirmed`
- Service desert classification: grid cells with at least one nominal stop but zero real weekly departures flagged as service deserts
- `transit/classification.py`: multi-factor school-only heuristic (keyword list configurable via `GTFS_SCHOOL_KEYWORDS`, AM/PM hour buckets configurable via env)
- `transit/export.py`: standalone GeoJSON + manifest + ZIP bundle written to `cache/exports/` after each reality refresh; publishable independently of the livability map

#### Database schema

- `transit_raw` schema: `feed_manifest`, `stops` (with PostGIS geometry), `routes`, `trips`, `stop_times`, `calendar_services`, `calendar_dates`
- `transit_derived` schema: `reality_manifest`, `service_classification`, `gtfs_stop_service_summary`, `gtfs_stop_reality`, `service_desert_cells`
- `transport_reality` and `service_deserts` tables in public schema for scoring and UI consumption
- Migrations `000002 -> 000005`: initial schema, selected-departures column, drop of OSM stop-matching layer (went GTFS-first after OSM coverage gap discovered - ~12k OSM stops vs ~29k GTFS), drop of `reality_confidence` column

#### Fingerprint-based caching

- Feed fingerprint: hash of ZIP size + content; reality fingerprint: hash of analysis date + all GTFS config parameters + feed fingerprints
- Pipeline skips re-parsing and re-derivation when fingerprints match manifests already in the database; full rebuild triggered only on feed update or config change

#### Overture Places

- `overture/` module: Overture Places GeoParquet loaded as a POI rescue layer
- Confidence-aware conflict detection: Overture POIs merged with OSM amenities, resolving duplicates by source confidence

#### Frontend

- `transport_reality_popup.js` / `.test.js`: map popup component showing stop name, reality status (active / inactive / school-only), departure counts, route modes, and last-service date for any clicked transit stop

#### Tests

- `tests/test_transit_phase1.py`: unit tests for ZIP parsing, service window expansion, school-only classification, departure aggregation, stop reality derivation, export bundle generation
- `tests/test_db_postgis_writes.py`: DB write path tests covering feed and reality artifact loading
- `tests/test_walkgraph_support.py`: walkgraph binary detection and invocation tests

### Changed

- `config.py`: GTFS feed configuration (`TransitFeedConfig`, `TransitFeedState`, `TransitRealityState`), fingerprint helpers, analysis-date resolution; `TRANSIT_REALITY_ALGO_VERSION = 5`
- `CACHE_SCHEMA_VERSION` bumped to `9` (transport reality rows now part of scoring state)
- `precompute/__init__.py`: `refresh_transit()` entry point added; transport reality rows and service desert cells integrated into the precompute build graph
- `db_postgis/writes.py`, `reads.py`, `schema.py`, `tables.py`, `manifests.py`: extended with all transit raw and derived table operations
- `schema.sql` updated to reflect the full transit schema alongside existing grid and amenity tables
- `serve_from_db.py`: transport reality endpoint added for local dev inspection
- OSM -> GTFS cross-reference approach dropped: OSM Ireland coverage (~12k stops) is too sparse (~60% gap vs GTFS) to be a reliable join key; scoring now sources stops directly from GTFS feeds

### Weights (unchanged from Phase 0)

shops: cap=5 (25%) | transport: cap=5 (25%) | healthcare: cap=3 (25%) | parks: cap=2 (25%)

---

## 2026-04-08 - Phase 0 complete

### Added

- Coastal grid-cell clipping: cells now store `effective_area_m2` and `effective_area_ratio`
- Coastal amenity density normalization: shops, transport, healthcare normalized by clipped area (floor 0.25)
- Park scoring switched from raw feature count to reachable polygon area; `_PARK_AREA_UNIT_M2 = 50_000`
- Livability sanity fixture: hand-picked reference locations with expected score ranges in `fixtures/`
- `scripts/sanity_check.py`: fixture runner wired into CI
- `scripts/refresh_osm.py` / `sanity_check.py`: OSM refresh and validation scripts
- Automated OSM re-import scheduled refresh workflow (`.github/workflows/scheduled_refresh.yml`)
- Basic CI: Python 3.12 + Rust stable matrix on GitHub Actions
- Platform-aware walkgraph binary detection (`_default_walkgraph_bin()` in `config.py`)

### Schema and config changes

- `GRID_GEOMETRY_SCHEMA_VERSION` bumped to reflect new persisted grid metadata
- `OSM_EXTRACT_NAME` updated to `ireland-and-northern-ireland-latest.osm.pbf`
- `IMPORTER_CONFIG_VERSION` set to `2026-04-08`
- Walk graph format version: `WALKGRAPH_FORMAT_VERSION = 3`

### Weights (Phase 0)

shops: cap=5 (25%) | transport: cap=5 (25%) | healthcare: cap=3 (25%) | parks: cap=2 (25%)

---

## 2026-04-01 - Alpha baseline

### Added

- Initial pipeline: OSM import -> PostGIS -> walkgraph reachability -> grid scoring -> PMTiles output
- Study area: Island of Ireland (Republic + Northern Ireland), EPSG:2157
- Grid resolutions: 50 m (canonical) through 20 km (coarse vector), zoom-based rendering
- Scoring categories: shops, transport, healthcare, parks - capped presence count
- Rust `walkgraph` binary: pedestrian graph construction from OSM PBF, BFS reachability at 500 m
- MapLibre GL frontend with PMTiles tile source
- Local dev server (`serve_from_db.py`)
- Cache and schema versioning via `config.py` (`CACHE_SCHEMA_VERSION`, `GRID_GEOMETRY_SCHEMA_VERSION`, `PMTILES_SCHEMA_VERSION`)
- Multi-resolution build profiles: `full` (50 m -> 20 km) and `dev` (5 km -> 20 km)

### Weights (alpha baseline)

shops: cap=5 (25%) | transport: cap=5 (25%) | healthcare: cap=3 (25%) | parks: cap=2 (25%)
