# Agent Instructions

## Start here (token discipline)
- `repo_map.md` is the deep map. Read its **§0 Agent quick start** first and stop there unless you need more: §0.3 task → files to read, §0.4 symbol → file, §0.5 commands, §0.6 required inputs, §0.7 files never to read whole, §0.9 top traps. Never read `repo_map.md` end-to-end.
- Behaviour facts that change what you do:
  - Transport scoring is **GTFS-only**. There is no OSM transport-stop fallback, and `precompute` refuses to publish a zero-transport build unless `--allow-missing-transport` is passed.
  - The noise overlay is **display-only** and never scored.
  - `python main.py import` also refreshes the basemap and therefore **needs Docker**; on a Docker-less host use `python main.py precompute --auto-refresh-import` instead.
  - The root `network/` package (walkgraph index loader) is not `precompute/network.py` (reachability math).
- Keep `repo_map.md` honest: when you change behaviour, update the §0 / §6 / §9 / §10 rows you invalidated in the same commit.

## Scope discipline
- Work narrowly. Edit only files directly relevant to the task.
- Use `rg` (ripgrep) or symbol search before opening large files.
- Do not scan the full repository unless the task explicitly requires it.

## Files to skip by default
Do not open these unless directly required by the task:
- `CHANGELOG.md`
- Old migrations (`db_postgis/migrations/versions/*` — read the head or the one you are changing)
- `schema.sql` (stale snapshot; Alembic is canonical)
- Generated bundles (`static/dist/`, `frontend/dist/`)
- Local artifacts (`*.db`, `*.pmtiles`, `data/`, `.livability_cache/`, `osm/*.pbf`, `osm/flat-nodes.bin`, `gtfs/*.zip`, `noise_datasets/*.zip`, `overture/*.geoparquet`)
- `legacy/`, `reports/`, `.test_logs/`, `.agents/`, `walkgraph/target/`
- Unrelated test files

## Large files — read targeted sections only
`repo_map.md` §0.7 lists every file that is too large to read whole (with non-blank line counts and which symbol to grep instead). In short: `frontend/src/main.js` (UI logic lives in smaller modules), `precompute/phases.py` (jump to `phase_*_impl`), `config.py`, `serve_from_db.py`, `precompute/surface.py`, `noise_artifacts/ogr_ingest.py`, and `tests/test_precompute_behavior.py` (4k lines — reuse its `_workflow_kwargs` and `_amenity_data_with_transport` helpers instead of building fixtures).

## Testing strategy
- Run the narrowest test first: `npm test --prefix frontend` for frontend changes.
- Prefer targeted tests over full test suites.
- Do not run backend or Rust tests for frontend-only changes.
- Backend (Windows, conda env): `scripts\win\geo_env.cmd .\.venv\Scripts\python.exe -m unittest tests.test_<name> -v`. On Linux/mac: `python -m pytest -q tests/test_<name>.py`. CI parity: `python -m unittest discover -s tests -t . -p "test_*.py"`.
- If `.venv` is broken (for example a namespace-package `pyarrow` with no `__version__`), use the conda env through `scripts\win\geo_env.cmd`; tests that use `TemporaryDirectory` can also fail on `chmod` inside a restricted sandbox — an environment limit, not a code failure.
- Rust: `cargo test --manifest-path walkgraph/Cargo.toml`. Frontend bundle check: `npm run build --prefix frontend` then `git diff --exit-code -- static/dist`.
- Planner/behaviour questions are cheapest to answer with `python main.py precompute --profile dev --explain` (no side effects).
- `tests/test_transit_phase1.py` snapshot regressions are skipped without local GTFS zips — a green run there may mean "skipped".

## Edit discipline
- Make small focused edits.
- Preserve public/runtime behaviour unless the task explicitly asks for a behaviour change.
- Do not reformat unrelated files.
- Do not add dependencies.

## Windows GDAL/Noise execution
- First-time setup:
  - `scripts\win\bootstrap_geo_env.cmd`
- For Windows GDAL/noise work, run through:
  - `scripts\win\check_geo_env.cmd`
  - `scripts\win\selftest_geo_env.cmd`
  - `scripts\win\geo_env.cmd .\.venv\Scripts\python.exe ...`
  - `scripts\win\precompute_noise_dev.cmd`
  - `scripts\win\prepare_noise_artifact_dev.cmd`
  - `scripts\win\prepare_noise_artifact_accurate.cmd`
  - `scripts\win\precompute_noise_accurate.cmd`
  - `scripts\win\test_noise.cmd`
- Noise workflow split:
  - `precompute_noise_dev.cmd` is reuse-only and never builds/reimports artifacts.
  - `prepare_noise_artifact_*.cmd` commands are the slow artifact build/refresh path.
- To reuse an existing conda base env:
  - `set GEO_CONDA_ENV=base`
  - `scripts\win\check_geo_env.cmd`
- Do not run raw `python main.py ...` for noise/GDAL tasks on Windows.
- Do not rely on random shell env state; use the launcher wrappers.
- Do not commit Miniforge installs, conda env directories, `.venv`, `.env`, local dataset zips, `.livability_cache`, generated PMTiles, or generated local DB/cache files.
