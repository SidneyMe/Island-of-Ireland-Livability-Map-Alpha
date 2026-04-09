# Island of Ireland Livability Map

A local-first livability mapping experiment for the island of Ireland. It imports local OpenStreetMap data, stores the working state in PostGIS, uses a Rust helper for fast walk-network work, bakes the output into PMTiles, and serves it through a small MapLibre web app.

This is not a SaaS dashboard or a polished civic-tech product. It is a hands-on geospatial system for exploring how reachable everyday amenities are across Ireland and Northern Ireland without depending on a hosted backend.

## Status

Alpha. Built in pure flow state in under a week. It started as "haha, what if I made a silly map of Ireland?" and then somehow turned into a local-first geospatial system with PostGIS, a Rust walk-graph helper, PMTiles, and actual architecture.

It works, it has sharp edges, and the whole point was to make something that did not feel awful to use.

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

## Notes And Caveats

- This is alpha software. Expect rough edges, local assumptions, and setup steps that still require some patience.
- `.livability_cache/livability.pmtiles` is generated by precompute and served through `/tiles/livability.pmtiles`.
- `static/index.html` references `static/app.css` plus the generated `static/dist` files at runtime.
- `render_from_db.py` remains as a compatibility wrapper around `serve_from_db.py`; new serving code should use the local server path.
- `legacy/` contains quarantined historical files. They are not active unless a future change explicitly restores them.
