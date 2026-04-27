from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    NOISE_BACKGROUND_DISPATCH,
    NOISE_MODE,
    OSM_EXTRACT_PATH,
    OUTPUT_HTML,
)
from db_postgis import (
    load_service_desert_rows,
    load_transport_reality_points,
)
import noise.loader as _noise_loader
import overture.loader as _overture

from . import grid as _grid
from . import publish as _publish
from ._state import _STATE


# ---------------------------------------------------------------------------
# Artifact mode sentinel and state
# ---------------------------------------------------------------------------

# Set by _noise_rows_from_artifact so _noise_processing_hash() can read it
# without needing an engine argument (workflow calls it with no args).
_CURRENT_ARTIFACT_HASH: str | None = None


class _ArtifactNoiseReference:
    """
    Sentinel returned by _noise_rows() in NOISE_MODE=artifact.

    The publish pipeline (_publish_noise_polygons) detects this object and
    calls copy_noise_artifact_to_noise_polygons() instead of the legacy
    candidate-staging pipeline.  No raw noise files are opened.
    """
    def __init__(self, noise_resolved_hash: str, manifest):
        self.noise_resolved_hash = noise_resolved_hash
        self.manifest = manifest

    def __len__(self) -> int:
        return 0  # prevents noise_row_count from iterating


# ---------------------------------------------------------------------------
# Background noise loading (legacy path only)
# ---------------------------------------------------------------------------

_BACKGROUND_NOISE_LOCK = threading.Lock()
_BACKGROUND_NOISE_THREAD: threading.Thread | None = None
_BACKGROUND_NOISE_DONE = threading.Event()
_BACKGROUND_NOISE_RESULT: list[dict[str, Any]] | None = None
_BACKGROUND_NOISE_ERROR: BaseException | None = None


def _background_noise_target(study_area_wgs84, cache_dir: Path) -> None:
    global _BACKGROUND_NOISE_RESULT, _BACKGROUND_NOISE_ERROR
    from concurrent.futures.process import BrokenProcessPool

    def _load(workers: int | None) -> list[dict[str, Any]]:
        return list(
            _noise_loader.iter_noise_candidate_rows_cached(
                study_area_wgs84=study_area_wgs84,
                cache_dir=cache_dir,
                progress_cb=None,
                workers=workers,
            )
        )

    try:
        try:
            result = _load(workers=None)
        except BrokenProcessPool as exc:
            print(
                f"[noise] background loader pool died ({type(exc).__name__}: {exc}); "
                "retrying serially in-thread",
                flush=True,
            )
            result = _load(workers=1)
        with _BACKGROUND_NOISE_LOCK:
            _BACKGROUND_NOISE_RESULT = result
    except BaseException as exc:  # noqa: BLE001 - re-raised on join
        with _BACKGROUND_NOISE_LOCK:
            _BACKGROUND_NOISE_ERROR = exc
    finally:
        _BACKGROUND_NOISE_DONE.set()


def _dispatch_noise_in_background() -> None:
    global _BACKGROUND_NOISE_THREAD, _BACKGROUND_NOISE_RESULT, _BACKGROUND_NOISE_ERROR
    if NOISE_MODE == "artifact":
        return  # artifact mode: no background thread; _noise_rows returns a sentinel
    if not NOISE_BACKGROUND_DISPATCH:
        return
    if _STATE.study_area_wgs84 is None:
        return
    with _BACKGROUND_NOISE_LOCK:
        if _BACKGROUND_NOISE_THREAD is not None and _BACKGROUND_NOISE_THREAD.is_alive():
            return
        _BACKGROUND_NOISE_RESULT = None
        _BACKGROUND_NOISE_ERROR = None
        _BACKGROUND_NOISE_DONE.clear()
        thread = threading.Thread(
            target=_background_noise_target,
            args=(_STATE.study_area_wgs84, CACHE_DIR),
            name="precompute-noise-loader",
            daemon=True,
        )
        _BACKGROUND_NOISE_THREAD = thread
    thread.start()


def _await_background_noise() -> list[dict[str, Any]] | None:
    global _BACKGROUND_NOISE_THREAD, _BACKGROUND_NOISE_RESULT, _BACKGROUND_NOISE_ERROR
    with _BACKGROUND_NOISE_LOCK:
        thread = _BACKGROUND_NOISE_THREAD
    if thread is None:
        return None
    _BACKGROUND_NOISE_DONE.wait()
    with _BACKGROUND_NOISE_LOCK:
        result = _BACKGROUND_NOISE_RESULT
        error = _BACKGROUND_NOISE_ERROR
        _BACKGROUND_NOISE_THREAD = None
        _BACKGROUND_NOISE_RESULT = None
        _BACKGROUND_NOISE_ERROR = None
    if error is not None:
        raise error
    return result


# ---------------------------------------------------------------------------
# Row generators
# ---------------------------------------------------------------------------

def _walk_rows(
    walk_grids: dict[int, list[dict[str, Any]]],
    created_at,
    *,
    progress_cb=None,
):
    return _publish.iter_walk_rows_impl(
        walk_grids,
        created_at,
        hashes=_STATE.hashes,
        study_area_metric=_STATE.study_area_metric,
        materialize_cell_geometry=_grid.materialize_cell_geometry,
        progress_cb=progress_cb,
    )


def _amenity_rows(
    amenity_source_rows: list[dict[str, Any]],
    created_at,
    *,
    progress_cb=None,
):
    return _publish.iter_amenity_rows_impl(
        amenity_source_rows,
        created_at,
        hashes=_STATE.hashes,
        progress_cb=progress_cb,
    )


def _transport_reality_rows(engine, created_at, *, progress_cb=None):
    if _STATE.transit_reality_state is None:
        return []
    rows = load_transport_reality_points(
        engine,
        _STATE.transit_reality_state.reality_fingerprint,
    )
    result = [
        {
            "build_key": _STATE.hashes.build_key,
            "config_hash": _STATE.hashes.config_hash,
            "import_fingerprint": _STATE.hashes.import_fingerprint,
            "source_ref": row["source_ref"],
            "stop_name": row["stop_name"],
            "reality_status": row["reality_status"],
            "source_status": row["source_status"],
            "school_only_state": row["school_only_state"],
            "feed_id": row["feed_id"],
            "stop_id": row["stop_id"],
            "public_departures_7d": row["public_departures_7d"],
            "public_departures_30d": row["public_departures_30d"],
            "school_only_departures_30d": row["school_only_departures_30d"],
            "weekday_morning_peak_deps": row.get("weekday_morning_peak_deps", 0.0),
            "weekday_evening_peak_deps": row.get("weekday_evening_peak_deps", 0.0),
            "weekday_offpeak_deps": row.get("weekday_offpeak_deps", 0.0),
            "saturday_deps": row.get("saturday_deps", 0.0),
            "sunday_deps": row.get("sunday_deps", 0.0),
            "friday_evening_deps": row.get("friday_evening_deps", 0.0),
            "transport_score_units": row.get("transport_score_units", 0),
            "bus_daytime_deps": row.get("bus_daytime_deps", 0.0),
            "bus_daytime_headway_min": row.get("bus_daytime_headway_min"),
            "bus_frequency_tier": row.get("bus_frequency_tier"),
            "bus_frequency_score_units": row.get("bus_frequency_score_units", 0),
            "last_public_service_date": row["last_public_service_date"],
            "last_any_service_date": row["last_any_service_date"],
            "bus_active_days_mask_7d": row.get("bus_active_days_mask_7d"),
            "bus_service_subtier": row.get("bus_service_subtier"),
            "is_unscheduled_stop": bool(row.get("is_unscheduled_stop", False)),
            "has_exception_only_service": bool(row.get("has_exception_only_service", False)),
            "has_any_bus_service": bool(row.get("has_any_bus_service", False)),
            "has_daily_bus_service": bool(row.get("has_daily_bus_service", False)),
            "route_modes_json": row["route_modes_json"],
            "source_reason_codes_json": row["source_reason_codes_json"],
            "reality_reason_codes_json": row["reality_reason_codes_json"],
            "geom": row["geom"],
            "created_at": created_at,
        }
        for row in rows
    ]
    if progress_cb is not None and result:
        progress_cb("detail", detail=f"loaded {len(result):,} transport_reality rows", force_log=True)
    return result


def _service_desert_rows(engine, created_at, *, progress_cb=None):
    rows = load_service_desert_rows(engine, _STATE.hashes.build_key)
    result = [
        {
            "build_key": _STATE.hashes.build_key,
            "config_hash": _STATE.hashes.config_hash,
            "import_fingerprint": _STATE.hashes.import_fingerprint,
            "resolution_m": row["resolution_m"],
            "cell_id": row["cell_id"],
            "analysis_date": row["analysis_date"],
            "baseline_reachable_stop_count": row["baseline_reachable_stop_count"],
            "reachable_public_departures_7d": row["reachable_public_departures_7d"],
            "reason_codes_json": row["reason_codes_json"],
            "cell_geom": row["cell_geom"],
            "created_at": created_at,
        }
        for row in rows
    ]
    if progress_cb is not None and result:
        progress_cb("detail", detail=f"loaded {len(result):,} service_desert rows", force_log=True)
    return result


def _noise_rows(engine, created_at, *, progress_cb=None):
    if NOISE_MODE == "artifact":
        return _noise_rows_from_artifact(engine, progress_cb=progress_cb)

    # Legacy path — unchanged
    background = _await_background_noise()
    if background is not None:
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=f"using background noise candidates ({len(background):,} rows)",
                force_log=True,
            )
        rows: Any = iter(background)
    else:
        rows = _noise_loader.iter_noise_candidate_rows_cached(
            study_area_wgs84=_STATE.study_area_wgs84,
            progress_cb=progress_cb,
            cache_dir=CACHE_DIR,
        )
    return _publish.iter_noise_rows_impl(
        rows,
        created_at,
        hashes=_STATE.hashes,
        progress_cb=progress_cb,
    )


def _noise_rows_from_artifact(engine, *, progress_cb=None) -> "_ArtifactNoiseReference":
    """
    Artifact mode: return a sentinel that causes the publish pipeline to
    direct-copy from noise_resolved_display instead of running candidate staging.

    This function does NOT:
    - Read raw noise files
    - Compute source hashes from data_dir
    - Call noise.loader
    - Run candidate materialisation or ST_Difference

    Side-effect: stores artifact_hash in _CURRENT_ARTIFACT_HASH so that the
    no-argument _noise_processing_hash() can return it when called later.
    """
    global _CURRENT_ARTIFACT_HASH
    from noise_artifacts.manifest import get_active_artifact

    artifact = get_active_artifact(engine, "resolved")
    if artifact is None:
        raise RuntimeError(
            "NOISE_MODE=artifact but no active complete resolved artifact found. "
            "Run first: python -m noise_artifacts\n"
            "Or switch back to legacy mode: NOISE_MODE=legacy"
        )
    _CURRENT_ARTIFACT_HASH = artifact.artifact_hash
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"artifact mode: using noise_resolved_hash={artifact.artifact_hash}",
            force_log=True,
        )
    return _ArtifactNoiseReference(
        noise_resolved_hash=artifact.artifact_hash,
        manifest=artifact,
    )



def _noise_processing_hash() -> str | None:
    """Stable hash over noise-specific inputs only (not walk/transit config).

    In artifact mode: returns _CURRENT_ARTIFACT_HASH, which is set by
    _noise_rows_from_artifact() before this function is called in the workflow.
    No file I/O, no data_dir access.

    In legacy mode: hashes file inventory + study area + loader config.
    Used as a DB-level clone key so unchanged noise inputs skip re-processing.
    """
    if NOISE_MODE == "artifact":
        return _CURRENT_ARTIFACT_HASH

    # Legacy path — compute from raw file inventory
    if _STATE.study_area_wgs84 is None:
        return None
    try:
        import hashlib
        import json
        from noise.loader import (
            NOISE_CANDIDATES_CACHE_VERSION,
            NOISE_SIMPLIFY_TOLERANCE_M,
            NOISE_DATA_DIR,
            _study_area_signature,
            dataset_signature,
        )
        parts = {
            "noise_dataset_signature": dataset_signature(NOISE_DATA_DIR),
            "study_area_hash": _study_area_signature(_STATE.study_area_wgs84),
            "noise_loader_version": NOISE_CANDIDATES_CACHE_VERSION,
            "simplify_tolerance_m": NOISE_SIMPLIFY_TOLERANCE_M,
            "target_crs": "EPSG:4326",
            "noise_schema_version": 1,
        }
        return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]
    except Exception:
        return None


def _summary_json(
    study_area_wgs84,
    walk_grids: dict[int, list[dict[str, Any]]],
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
    *,
    transport_reality_rows: list[dict[str, Any]] | None = None,
    noise_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return _publish.summary_json_impl(
        study_area_wgs84,
        walk_grids,
        amenity_data,
        amenity_source_rows,
        transport_reality_rows=transport_reality_rows,
        noise_rows=noise_rows,
        hashes=_STATE.hashes,
        build_profile=_STATE.profile,
        source_state=_STATE.source_state,
        osm_extract_path=OSM_EXTRACT_PATH,
        grid_sizes_m=list(_STATE.settings.grid_sizes_m),
        fine_resolutions_m=list(_STATE.settings.fine_resolutions_m),
        output_html=OUTPUT_HTML,
        zoom_breaks=list(_STATE.settings.surface_zoom_breaks),
        transit_reality_state=_STATE.transit_reality_state,
        transit_analysis_window_days=GTFS_ANALYSIS_WINDOW_DAYS,
        transit_service_desert_window_days=GTFS_SERVICE_DESERT_WINDOW_DAYS,
        transport_reality_download_url="/exports/transport-reality.zip",
        service_deserts_enabled=True,
        overture_dataset=_overture.dataset_info(),
    )
