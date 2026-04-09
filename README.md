# Island of Ireland Livability Map

**Vibecoded in pure flow state**, with heavy AI assistance.

My focus was on **architecture and system design** — deciding what the pieces should be, how they should fit together, and where the boundaries belonged. The implementation itself was largely AI-generated under that direction.

It started as "haha what if I made a silly map of Ireland" and turned into a local-first geospatial system with PostGIS, a Rust walk-graph helper, and PMTiles. Not polished — an experiment that got serious.

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
- Scores access to shops, public transport, healthcare, and parks.
- Bakes grid and amenity layers into `.livability_cache/livability.pmtiles`.
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
- `schema.sql` is the canonical provisioning DDL. The SQLAlchemy table metadata in `db_postgis/` mirrors that schema for runtime queries and readiness checks.

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
WALKGRAPH_BIN=walkgraph/target/release/walkgraph.exe
```

## Required Local Data

The current config expects these files:

```text
osm/ireland-and-northern-ireland-260405.osm.pbf
boundaries/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson
boundaries/osni_open_data_largescale_boundaries_ni_outline.geojson
```

These inputs are intentionally not committed. They are large local data files, so the repo ignores them by default. If you want to version or distribute them, use Git LFS or another explicit data distribution path.

## Setup Commands

Create and activate a Python environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Install frontend dependencies and build the production bundle:

```powershell
cd frontend
npm install
npm run build
cd ..
```

Build the Rust graph helper:

```powershell
cd walkgraph
cargo build --release
cd ..
```

Prepare the database schema:

```powershell
psql "postgresql://user:password@localhost:5432/database_name" -f schema.sql
```

If your app `DATABASE_URL` uses the SQLAlchemy driver form `postgresql+psycopg://`, remove `+psycopg` for the `psql` command. If you use split `POSTGRES_*` variables instead, run the schema command with the matching `psql` connection arguments for your database.

## Usage Commands

Refresh the raw local OSM import:

```powershell
python main.py --refresh-import
```

Run precompute using an existing raw import:

```powershell
python main.py --precompute
```

Allow precompute to refresh the import if the raw import is missing:

```powershell
python main.py --precompute --auto-refresh-import
```

Force a rebuild of the current PostGIS build:

```powershell
python main.py --precompute --force-precompute
```

Serve the local web app:

```powershell
python main.py --serve
```

The server defaults to:

```text
http://127.0.0.1:8000/
```

You can override the bind address:

```powershell
python main.py --serve --host 127.0.0.1 --port 8080
```

`--render` still exists as a legacy alias for `--serve`.

## Testing

Run Python tests:

```powershell
python -m unittest discover -s tests -t . -p "test_*.py"
```

Run Rust tests:

```powershell
cd walkgraph
cargo test
cd ..
```

Build the frontend bundle used by `static/index.html`:

```powershell
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
- Healthcare scoring treats a rural GP the same as a major hospital; there's no capacity or quality weighting.
- Parks are counted by presence, not area — a pocket playground scores the same as a regional park.
- Grid cells straddling the coast aren't clipped to land, so coastal cells can look artificially sparse.
- Windows-first setup. Paths, shell snippets, and the default `walkgraph.exe` lookup assume Windows; Linux/macOS should work but aren't documented yet.
- No automated CI — tests are run locally.

## Roadmap

### Resolve current limitations

- [ ] Calibrate scoring weights and caps against a ground-truth livability index (or at least a sanity dataset).
- [ ] Mode-aware scoring: cycling, transit-chained trips, and variable walk radii instead of a single fixed 500 m.
- [ ] Weight healthcare by capacity and type — a regional hospital should not equal a single rural GP.
- [ ] Score parks by area (and quality tags), not just presence.
- [ ] Clip grid cells to land so coastal cells aren't artificially sparse.
- [ ] Cross-platform setup: document Linux/macOS, drop the Windows-only `walkgraph.exe` assumption.
- [ ] Basic CI — run Python + Rust tests on push.

### Richer scoring system

The current four-category model (shops / transport / healthcare / parks) is too coarse.

- [ ] Add drinking water, parcel lockers, and public shelters as convenience anchors.
- [ ] Separate chemists from pharmacies — chemists score under daily convenience, not healthcare.
- [ ] Night-usability modifier for footpaths, crossings, and stops based on lighting data.
- [ ] Surface-quality modifier for walkable ways.
- [ ] Shade and pleasantness modifier from street trees and tree rows.
- [ ] Waterfront pleasantness bonus for paths and parks adjacent to water.
- [ ] Community and public-realm anchors (community centres, picnic tables, shelters).
- [ ] Dual-role nightlife features (pubs, bars, fast food) that add value in moderation and penalize over-concentration.
- [ ] Conditional pitch-noise penalty — only when lit or in dense residential context.
- [ ] Land-use context layer (residential / commercial / industrial) driving concentration penalties and green-buffer bonuses.
- [ ] Distance-decay scoring to replace the binary in-range / out-of-range cutoff.
- [ ] Sub-weights within categories (supermarket > corner shop > petrol station shop).
- [ ] User-adjustable weight sliders in the UI.
- [ ] Per-cell score breakdowns on click — see *why* a cell scored what it did.

Detailed OSM tag choices and reasoning for each of these will live in a separate design note when that work starts.

### Features

- [ ] **Isochrone overlay** — click anywhere, draw the actual reachable polygon at 5 / 10 / 15 min on foot. Makes the walk graph visible.
- [ ] **Compare mode** — pin two locations side by side with their score breakdowns.
- [ ] **Time-of-day awareness** — a bus stop with 2 buses/day isn't the same as one with 50. Pull GTFS where available.
- [ ] **Shareable permalinks** — URL encodes viewport + weights.
- [ ] **Hosted demo** — static PMTiles + a stripped-down frontend, so people can try it without the full setup.
- [ ] **Data freshness indicator** — show the OSM extract date in the UI.

## Attribution And Data Sources

- **OpenStreetMap** — amenity, network, and POI data. © OpenStreetMap contributors, available under the [Open Database License (ODbL)](https://www.openstreetmap.org/copyright). Any derived tiles or screenshots must preserve this attribution.
- **Republic of Ireland county boundaries** — Tailte Éireann (formerly Ordnance Survey Ireland), National Statutory Boundaries 2024.
- **Northern Ireland outline** — OSNI Open Data, Largescale Boundaries.
- **Basemap rendering** — [MapLibre GL](https://maplibre.org/) on the frontend; tiles are served locally from the generated PMTiles archive.

This project is a derivative database under ODbL terms. The generated `livability.pmtiles` archive is a database derived from OSM and inherits ODbL — it must be redistributed under the same license, with attribution preserved. The source code in this repository is a separate work and is not covered by ODbL.

## Acknowledgements

This project stands on a stack of excellent open-source tools:

- [OpenStreetMap](https://www.openstreetmap.org/) ❤️ — the data that makes any of this possible.
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
