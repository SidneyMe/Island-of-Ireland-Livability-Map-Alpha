# Island of Ireland Livability Map

Local-first livability mapping for the island of Ireland — built with PostGIS, a Rust walk-graph helper, PMTiles, and MapLibre. Scores places by what's nearby, how quiet it is, how green it is, and whether the transport actually runs — not raw amenity count.

It started as "haha what if I made a silly map of Ireland" and turned into a real local-first geospatial system. Not polished. An experiment that got serious.

**Vibecoded in pure flow state.**

## Why This Exists

I live in a big Irish town. A real one. My nearest corner shop is still a 15-minute walk away. This map exists so nobody else has to find that out the hard way — pick a spot, see what's walkable, decide accordingly.

## Contents

- [Status](#status)
- [Preview](#preview)
- [What It Does](#what-it-does)
- [Architecture Overview](#architecture-overview)
- [Project Layout](#project-layout)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Required Local Data](#required-local-data)
- [Setup Commands](#setup-commands)
- [Usage Commands](#usage-commands)
- [Testing](#testing)
- [Git And Data Hygiene](#git-and-data-hygiene)
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Attribution And Data Sources](#attribution-and-data-sources)
- [How This Was Built](#how-this-was-built)
- [Acknowledgements](#acknowledgements)
- [Notes And Caveats](#notes-and-caveats)

## Status

**Alpha** — It works, it’s fast, and the core experience is already enjoyable.  
Expect sharp edges, some jank, and missing polish. More features will come as the vibe demands.

## Preview

https://github.com/user-attachments/assets/e8f3717e-5948-49aa-a1cb-51490ca91f97

## What It Does

- Imports amenities and walkable network data from a local `.osm.pbf` extract.
- Uses PostGIS as the durable store for raw imports, manifests, grid scores, and runtime data.
- Builds a compact walk graph with the Rust `walkgraph` helper.
- Computes livability scores across multiple grid resolutions.
- Scores access to shops, public transport, healthcare, and parks using tiered units, amenity clustering, and distance decay.
- Publishes GTFS transport stops with base-calendar weekly bus tiers, weekday daytime bus frequency tiers, commute/off-peak/weekend departure metrics, stricter unscheduled-stop detection, and a service-desert overlay.
- Bakes grid, amenity, transport reality, and service-desert layers into `.livability_cache/livability.pmtiles`.
- Serves a local MapLibre app from `static/index.html`.

## Architecture Overview

The project is intentionally local-first. The heavy work happens ahead of time, then the browser reads a compact PMTiles archive through the local Python server.

```text
Local OSM PBF + boundary GeoJSON
        |
        v
osm2pgsql import
        |
        v
PostGIS raw + derived tables
        |
        v
Python precompute workflow
        |
        +--> Rust walkgraph helper for network/reachability work
        |
        v
Grid scores + amenity layers
        |
        v
.livability_cache/livability.pmtiles
        |
        v
Local Python server + MapLibre frontend
```

The split is simple:

- Python owns orchestration, configuration, database reads/writes, scoring, caching, and serving.
- PostGIS owns durable geospatial state and runtime query readiness.
- Rust owns fast walk-graph generation and reachability calculations.
- Vite/MapLibre owns the browser map.
- PMTiles is the handoff between precompute and rendering.

## Project Layout

- `main.py` is the main CLI entrypoint.
- `config.py` defines paths, scoring weights, environment variables, and hashing inputs.
- `precompute/` contains the geometry, network, reachability, scoring, publishing, and PMTiles workflow.
- `db_postgis/` contains SQLAlchemy table definitions, schema checks, reads, writes, and manifests.
- `local_osm_import/` wraps `osm2pgsql` import behavior.
- `network/` loads graph sidecars and calls the Rust helper.
- `walkgraph/` is the Rust CLI used for fast walk graph generation and reachability.
- `frontend/` is the Vite source project for the MapLibre UI.
- `static/` is served by the local Python server. `static/dist/` is the checked-in Vite production bundle.
- `legacy/` contains quarantined historical UI/server files that are retained for reference but are not part of the active runtime path.
- Alembic migrations under `db_postgis/migrations/` are the canonical managed-schema source of truth. `schema.sql` is kept as a bootstrap snapshot for convenience, and the SQLAlchemy metadata in `db_postgis/` mirrors the runtime query model.

## Prerequisites

Install these before running the full pipeline:

- Python 3.12 or newer
- PostgreSQL with PostGIS enabled
- `osm2pgsql`
- Rust and Cargo
- Node.js and npm

Python dependencies are listed in `requirements.txt`. Frontend dependencies are listed in `frontend/package.json`.

## Environment Setup

Copy `.env.example` to `.env`, then replace the placeholder values with your local database settings. You can also export equivalent environment variables instead of using a `.env` file.

Database configuration can use a single SQLAlchemy/PostGIS URL:

```env
DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/database_name
```

Or split PostgreSQL variables:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=database_name
POSTGRES_USER=user
POSTGRES_PASSWORD=password
```

Optional local tool overrides:

```env
OSM2PGSQL_BIN=osm2pgsql
# Windows:
WALKGRAPH_BIN=walkgraph/target/release/walkgraph.exe
# Linux/macOS:
WALKGRAPH_BIN=walkgraph/target/release/walkgraph
```

Set `WALKGRAPH_BIN` only if you need an explicit override, and use the variant that matches your platform.

## Required Local Data

The current config expects these files:

```text
osm/ireland-and-northern-ireland-latest.osm.pbf
boundaries/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson
boundaries/osni_open_data_largescale_boundaries_ni_outline.geojson
```

These inputs are intentionally not committed. They are large local data files, so the repo ignores them by default. If you want to version or distribute them, use Git LFS or another explicit data distribution path.

## Setup Commands

Create and activate a Python environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

If your POSIX environment does not map `python` to Python 3, use `python3` for the same commands below.

Install frontend dependencies and build the production bundle:

```text
cd frontend
npm install
npm run build
cd ..
```

Build the Rust graph helper:

```text
cd walkgraph
cargo build --release
cd ..
```

The app auto-applies pending managed-schema migrations on startup. If you still want a bootstrap snapshot for a fresh database, you can apply `schema.sql` manually:

```text
psql "postgresql://user:password@localhost:5432/database_name" -f schema.sql
```

If your app `DATABASE_URL` uses the SQLAlchemy driver form `postgresql+psycopg://`, remove `+psycopg` for the `psql` command. If you use split `POSTGRES_*` variables instead, run the schema command with the matching `psql` connection arguments for your database. Existing databases should be upgraded through Alembic on startup rather than by reapplying `schema.sql`.

## GTFS And Transport Scoring Setup

The transport reality and scoring pipeline uses local GTFS zip files first and only downloads feeds when you explicitly configure feed URLs.
The active pipeline ingests NTA and Translink only; the standalone TFI Local Link feed is intentionally not configured because current NTA GTFS is treated as the Republic-side source of truth.
The transport overlay is snapshot-based scheduled GTFS, not live GTFS-RT. Legacy `active_confirmed` / `inactive_confirmed` status still exists for compatibility. Bus-only stops now use weekday daytime bus frequency tiers for `transport_score_units`: `frequent` (<=15 min), `moderate` (16-30 min), `low_frequency` (31-60 min), `very_low_frequency` (61-120 min), and `token_skeletal` (>120 min). Rail/tram-only and mixed bus plus rail/tram stops keep the earlier commute/off-peak/weekend formula for now, but still expose bus frequency metadata when bus service is present. The UI keeps retrospective 7-day bus subtiers such as `Whole week`, `Mon-Sat`, `Weekdays only`, and `Unscheduled`, and popups expose bus headway, commute, Friday-evening, and score-unit fields.

Default local zip paths:

```text
gtfs/nta_gtfs.zip
gtfs/translink_gtfs.zip
```

Useful environment overrides:

```text
GTFS_NTA_ZIP_PATH=gtfs/nta_gtfs.zip
GTFS_TRANSLINK_ZIP_PATH=gtfs/translink_gtfs.zip
GTFS_AS_OF_DATE=2026-04-14
GTFS_ANALYSIS_WINDOW_DAYS=30
GTFS_SERVICE_DESERT_WINDOW_DAYS=7
```

Optional `GTFS_NTA_URL` and `GTFS_TRANSLINK_URL` values can be set when you want `--refresh-transit` to refresh those local zip files before parsing.

## Usage Commands

Each example shows `python` first and the `python3` POSIX alternative second. Use whichever form matches your shell.

Refresh the raw local OSM import:

```text
python main.py --refresh-import
python3 main.py --refresh-import
```

Refresh GTFS-derived transport reality and rebuild the standalone export bundle:

```text
python main.py --refresh-transit
python3 main.py --refresh-transit
```

`--refresh-transit` now reuses the existing transit reality when the GTFS inputs and current OSM import fingerprint still match. Use `--force-transit-refresh` when you want a full rebuild anyway.
The refreshed transport dataset now keeps one published stop row per GTFS stop, emits unscheduled boarding stops only when a `stops.txt` row has no `stop_times`, and carries weekly bus subtier plus frequency fields into the export bundle, PMTiles layer, and runtime JSON.

Run precompute using an existing raw import:

```text
python main.py --precompute
python3 main.py --precompute
```

`--precompute` now ensures GTFS transit reality exists before transport scoring runs. The publish step also writes:

- `transport_reality` PMTiles/runtime layer
- `service_deserts` PMTiles/runtime layer
- `.livability_cache/exports/transport-reality.zip`

Allow precompute to refresh the import if the raw import is missing:

```text
python main.py --precompute --auto-refresh-import
python3 main.py --precompute --auto-refresh-import
```

Force a rebuild of the current PostGIS build:

```text
python main.py --precompute --force-precompute
python3 main.py --precompute --force-precompute
```

### Windows Noise Workflow (Fast Reuse vs Slow Prepare)

Normal fast dev loop:

```text
scripts\win\precompute_noise_dev.cmd
```

This command requires an existing resolved noise artifact for the selected mode.
It never imports raw noise ZIP/SHP/GDB files, never runs `ogr2ogr`, and never builds noise artifacts.
If the required artifact is missing, it fails fast by design.

Build or refresh the dev-fast noise artifact (slow path):

```text
scripts\win\prepare_noise_artifact_dev.cmd
```

Build or refresh the accurate noise artifact (slow path):

```text
scripts\win\prepare_noise_artifact_accurate.cmd
```

Performance contract for `precompute_noise_dev.cmd`:
- Target runtime: under 10 minutes.
- Hard watchdog cap: 20 minutes.
- If DevReuse approaches the watchdog timeout, treat it as a performance regression bug.

Do not use artifact refresh/reimport/force flags in the normal dev loop.

Serve the local web app:

```text
python main.py --serve
python3 main.py --serve
```

The server defaults to:

```text
http://127.0.0.1:8000/
```

You can override the bind address:

```text
python main.py --serve --host 127.0.0.1 --port 8080
python3 main.py --serve --host 127.0.0.1 --port 8080
```

`--render` still exists as a legacy alias for `--serve`.

## Testing

Like the usage commands above, use the `python` or `python3` form that matches your environment.

Run Python tests:

```text
python -m unittest discover -s tests -t . -p "test_*.py"
python3 -m unittest discover -s tests -t . -p "test_*.py"
```

Run Rust tests:

```text
cd walkgraph
cargo test
cd ..
```

Validate the sanity fixture structure:

```text
python scripts/sanity_check.py --validate-only
python3 scripts/sanity_check.py --validate-only
```

Run the sanity fixture against your current completed local build:

```text
python scripts/sanity_check.py --profile full
python3 scripts/sanity_check.py --profile full
```

Build the frontend bundle used by `static/index.html`:

```text
cd frontend
npm run build
cd ..
```

## Git And Data Hygiene

The repository should keep source, configs, tests, small runtime assets, and the documented `static/dist/` runtime bundle. It should not commit local secrets, virtual environments, generated caches, dependency folders, local data, or build output.

Important ignored local/generated paths include:

```text
.env
.venv/
frontend/node_modules/
.livability_cache/
cache/
walkgraph/target/
*.pmtiles
osm/*.osm.pbf
boundaries/*.geojson
```

`static/dist/app.js` and `static/dist/app.css` are generated by `npm run build` and intentionally checked in as the runtime bundle. Rebuild them before committing frontend source changes.

## Known Limitations

- Scoring weights and caps are hand-picked, not calibrated against any ground-truth livability index.
- The 500 m walk radius is a single fixed value — no mode-aware scoring (cycling, transit-chained trips).
- Healthcare has type tiers, but not capacity, catchment, waiting-time, or quality weighting.
- Park scoring uses size tiers, but not park quality, entrance accessibility, lighting, or facility condition.
- Transport scoring uses scheduled GTFS frequency, not live reliability, cancellations, crowding, or fares.
- Grid cells straddling the coast can still look artificially sparse.
- Linux/macOS should work in principle now that the active runtime path no longer assumes Windows-only executable names, but those platforms are still not fully tested. Contributions welcome.
- CI currently validates Python tests, Rust tests, and sanity-fixture structure. End-to-end sanity score assertions still require a completed local build.

## Roadmap

This is a **livability** map, not a walkability map. High scores mean a place is good to live in — not just that amenities are nearby, but that the environment is pleasant, quiet, green, and well-served. A noisy city centre with every shop in walking distance may score lower than a quiet suburb with a GP, a park, and a supermarket. The scoring model should reflect how a place feels to live in, not just what you can reach on foot.

Implementation detail, phase ordering, and open design questions for every item below live in [docs/PHASES.md](docs/PHASES.md).

### Calibration and validation

- [x] Build a livability sanity fixture — a set of hand-picked reference locations across Ireland, used as a regression test for every scoring change.
- [ ] Compare against an independent dataset (e.g. CSO deprivation indices) as a secondary sanity check, once the scoring model is mature enough for the comparison to be meaningful.

### Sub-tier every category

The current model now tiers shops, healthcare, and parks by type or size, then counts distinct nearby clusters so co-located services do not inflate the score.

- [x] **Shops:** corner shop → regular shop → supermarket → mall / retail cluster.
- [x] **Healthcare:** pharmacy / GP → clinic / health centre → hospital → major hospital with A&E.
- [x] **Parks:** score by area, not count. Pocket park / playground (<0.5 ha) → neighbourhood park (0.5–5 ha) → district park (5–25 ha) → regional park / nature reserve (25+ ha).
- [x] **Variety signal** — count distinct clusters rather than distinct tags, so co-located services don't inflate the score.

### Service reality check

Implemented as the GTFS-first transport reality layer. Stops are sourced from scheduled NTA and Translink feeds, school-only service is excluded from public scoring, and bus-only transport scoring now consumes weekday daytime frequency tiers rather than flat stop presence.

- [x] GTFS-first transport reality: stops are sourced directly from NTA + Translink feeds, and stops with zero scheduled services in the last 30 days are flagged as inactive and excluded from scoring.
- [x] Separate public services from school-only routes so the latter don't count toward general transit access.
- [x] Flag service deserts — grid cells with reachable GTFS baseline stops but zero reachable public departures — and expose them as a dedicated overlay.
- [x] Replace the old binary map view with weekly bus subtiers (`Whole week`, `Mon-Sat`, `Tue-Sun`, `Weekdays only`, `Weekends only`, `Single-day only`, `Partial week`, `Unscheduled`) derived from base GTFS calendar patterns.
- [x] Classify bus-only stops by weekday daytime frequency: `frequent`, `moderate`, `low_frequency`, `very_low_frequency`, and `token_skeletal`.
- [ ] Publish a standalone "active vs inactive Irish transport" dataset derived from the above, licensed ODbL. Local export to `.livability_cache/exports/` is generated after each transit refresh; hosted publishing is pending Phase 8.

### Transport scoring overhaul

Builds on the service reality layer above. Bus-only transport scoring now uses scheduled public daytime frequency, with remaining work focused on rail/tram interpretation and nuisance tradeoffs.

- [x] Pull GTFS feeds (NTA + Translink) and compute commute, off-peak, weekend, and Friday-evening departures per stop.
- [x] Replace flat stop-count scoring with frequency-derived `transport_score_units`.
- [x] Cap low-frequency stops through the 1-5 `transport_score_units` scale so a single rare-service stop cannot match frequent urban transit access.
- [x] Use bus weekday daytime headway as the scoring driver for bus-only stops, with no rural/urban distinction.
- [ ] Design rail/tram-specific scoring without touching the bus frequency model.
- [ ] Rail proximity sweet spot: reward walking distance to a station, penalize immediate adjacency (noise, dust).

### Noise and nuisance penalty layer

- [ ] Railway track proximity (the tracks themselves, not stations).
- [ ] Motorway and major-road noise.
- [ ] Flight paths and airport proximity.
- [ ] Over-concentration of nightlife, fast food, and retail relative to residential context.
- [ ] Conditional pitch noise — only when lit or embedded in dense residential context.

### New scoring categories and modifiers

- [ ] Daily-convenience anchors: drinking water, parcel lockers, public shelters.
- [ ] Separate chemists from pharmacies — chemists score under daily convenience, not healthcare.
- [ ] Night-usability modifier for footpaths, crossings, and stops based on lighting data.
- [ ] Surface-quality modifier for walkable ways.
- [ ] Shade and pleasantness modifier from street trees and tree rows.
- [ ] Waterfront pleasantness bonus for paths and parks adjacent to water.
- [ ] Community and public-realm anchors (community centres, libraries, benches, picnic tables).
- [ ] Dual-role nightlife features (pubs, bars, fast food) that add value in moderation and penalize over-concentration.
- [ ] Land-use context layer (residential / commercial / industrial) driving concentration penalties and green-buffer bonuses.

### Scoring mechanics

- [x] Distance-decay scoring to replace the binary in-range / out-of-range cutoff.
- [ ] Mode-aware scoring: cycling, transit-chained trips, and variable walk radii instead of a single fixed 500 m.

### UI and features

- [x] **Per-cell score breakdowns on click** - click popups now show total score plus per-category raw counts, cluster counts, effective units, and component scores; fine-surface clicks use `/api/inspect` for exact values.
- [ ] **User-adjustable weight sliders** — let users score for their own priorities rather than the built-in defaults.
- [ ] **Layer toggles** — view the map by a single category instead of only the combined score.
- [ ] **Shortlist mode** — save multiple locations and view their breakdowns in one panel.
- [ ] **Compare mode** — pin two locations side by side with their score breakdowns.
- [ ] **Isochrone overlay** — click anywhere, draw the reachable polygon at 5 / 10 / 15 min on foot.
- [ ] **"Is this wrong?" feedback loop** — direct links into OpenStreetMap to edit the relevant feature, turning corrections into upstream contributions.
- [ ] **Shareable permalinks** — URL encodes viewport and weight vector.
- [ ] **Data freshness indicator** — surface the OSM extract date in the UI.

### Infrastructure

- [x] Clip grid cells to land so coastal cells aren't artificially sparse. Effective area stored per cell; amenity density and park scoring normalised by clipped land area.
- [x] Automated data refresh — scripted OSM re-import and precompute on a schedule.
- [x] Remove Windows-specific assumptions from the code path (hardcoded `.exe` suffix, PowerShell-only setup commands) so the project can run on Linux/macOS in principle. Testing and documentation for those platforms is out of scope — PRs from Linux/macOS users welcome.
- [x] Basic CI — run Python and Rust tests on push.

### Public launch

The hosted demo is the project's public moment, and it deliberately sits at the end. Until then the project lives on GitHub and updates land there continuously, but the public-facing site exists once there is something worth landing on — the local pipeline working end-to-end, the scoring model calibrated, the UI polished, and a real story to tell.

- [ ] **Hosted static demo** — public site serving the precomputed PMTiles plus a stripped-down frontend. No backend, no database.
- [ ] **Privacy promise** — no tracking of individual users, no search or query logging, no behavioural profiling. Only anonymous aggregate visit counts may be collected. Score queries run in the user's browser.

## Attribution And Data Sources

- **OpenStreetMap** — amenity, network, and POI data. © OpenStreetMap contributors, available under the [Open Database License (ODbL)](https://www.openstreetmap.org/copyright). Any derived tiles or screenshots must preserve this attribution.
- **Overture Maps Foundation** — optional Places data used to rescue missing shop, healthcare, and park POIs before canonical amenity scoring. See [Overture Maps](https://overturemaps.org/) and preserve any release-specific attribution required by the dataset.
- **Republic of Ireland county boundaries** — Tailte Éireann (formerly Ordnance Survey Ireland), National Statutory Boundaries 2024.
- **Northern Ireland outline** — OSNI Open Data, Largescale Boundaries.
- **Basemap rendering** — [MapLibre GL](https://maplibre.org/) on the frontend; tiles are served locally from the generated PMTiles archive.

This project is a derivative database under ODbL terms. The generated `livability.pmtiles` archive is a database derived from OSM and inherits ODbL — it must be redistributed under the same license, with attribution preserved. The source code in this repository is a separate work and is not covered by ODbL.

## How This Was Built

Architecture and system design are mine — deciding what the pieces should be, how they should fit together, and where the boundaries belonged. The implementation itself was largely AI-generated under that direction.

## Acknowledgements

This project stands on a stack of excellent open-source tools:

- [OpenStreetMap](https://www.openstreetmap.org/) ❤️ — the data that makes any of this possible.
- [Overture Maps Foundation](https://overturemaps.org/) — supplemental Places data that helps fill open-POI gaps while preserving provenance.
- [osm2pgsql](https://osm2pgsql.org/) — OSM → PostGIS import.
- [PostGIS](https://postgis.net/) — durable geospatial storage and query.
- [MapLibre GL](https://maplibre.org/) — the browser map renderer.
- [PMTiles / Protomaps](https://protomaps.com/) — single-file tile archive format that makes local-first serving practical.
- [pyproj](https://pyproj4.github.io/pyproj/), [Shapely](https://shapely.readthedocs.io/), [GeoPandas](https://geopandas.org/) — Python geospatial glue.
- [python-igraph](https://python.igraph.org/) — graph operations on the Python side.
- [Vite](https://vitejs.dev/) — frontend build tooling.

## Notes And Caveats

- This is alpha software. Expect rough edges, local assumptions, and setup steps that still require some patience.
- `.livability_cache/livability.pmtiles` is generated by precompute and served through `/tiles/livability.pmtiles`.
- `static/index.html` references `static/app.css` plus the generated `static/dist` files at runtime.
- `render_from_db.py` remains as a compatibility wrapper around `serve_from_db.py`; new serving code should use the local server path.
- `legacy/` contains quarantined historical files. They are not active unless a future change explicitly restores them.
