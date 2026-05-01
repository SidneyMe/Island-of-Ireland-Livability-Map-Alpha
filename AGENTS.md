# Agent Instructions

## Scope discipline
- Work narrowly. Edit only files directly relevant to the task.
- Use `rg` (ripgrep) or symbol search before opening large files.
- Do not scan the full repository unless the task explicitly requires it.

## Files to skip by default
Do not open these unless directly required by the task:
- `CHANGELOG.md`
- Old migrations (`migrations/`)
- `schema.sql` (full file)
- Generated bundles (`static/dist/`, `frontend/dist/`)
- Local artifacts (`*.db`, `*.pmtiles`, `data/`)
- Unrelated test files

## Large files — read targeted sections only
- `frontend/src/main.js` — bootstrap + orchestration. Most UI logic now lives in smaller modules. Use symbol search to find the section you need; do not read the whole file unless the task spans multiple unrelated sections.

## Testing strategy
- Run the narrowest test first: `npm test --prefix frontend` for frontend changes.
- Prefer targeted tests over full test suites.
- Do not run backend or Rust tests for frontend-only changes.

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
  - `scripts\win\precompute_noise_accurate.cmd`
  - `scripts\win\test_noise.cmd`
- To reuse an existing conda base env:
  - `set GEO_CONDA_ENV=base`
  - `scripts\win\check_geo_env.cmd`
- Do not run raw `python main.py ...` for noise/GDAL tasks on Windows.
- Do not rely on random shell env state; use the launcher wrappers.
- Do not commit Miniforge installs, conda env directories, `.venv`, `.env`, local dataset zips, `.livability_cache`, generated PMTiles, or generated local DB/cache files.
