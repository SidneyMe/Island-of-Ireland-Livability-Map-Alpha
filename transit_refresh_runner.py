from __future__ import annotations

from typing import Any

from config import (
    CACHE_DIR,
    WALKGRAPH_BIN,
    current_normalization_scope_hash,
    normalize_build_profile,
)
from db_postgis import build_engine, ensure_database_ready, import_payload_ready
from local_osm_import import ensure_local_osm_import, resolve_source_state
from progress_tracker import PrecomputeProgressTracker
from transit import ensure_transit_reality, transit_reality_refresh_required
from transit.sources import describe_gtfs_feeds, refresh_gtfs_feeds
from walkgraph_support import ensure_walkgraph_subcommand_available


def _set_transit_detail(
    tracker: PrecomputeProgressTracker,
    detail: str,
    *,
    force_log: bool = True,
) -> None:
    tracker.set_phase_detail("transit", detail, force_log=force_log)


def _preflight_transit_rebuild(
    engine,
    *,
    import_fingerprint: str | None = None,
    force_refresh: bool = False,
    auto_refresh_gtfs: bool = False,
    force_gtfs_refresh: bool = False,
    progress_cb=None,
) -> tuple[Any, bool]:
    reality_state, refresh_required = transit_reality_refresh_required(
        engine,
        import_fingerprint=import_fingerprint,
        auto_refresh_gtfs=auto_refresh_gtfs,
        force_gtfs_refresh=force_gtfs_refresh,
        force_refresh=force_refresh,
        progress_cb=progress_cb,
    )
    if refresh_required:
        ensure_walkgraph_subcommand_available(WALKGRAPH_BIN, "gtfs-refresh")
    return reality_state, refresh_required


def _load_study_area_wgs84(profile: str, tracker: PrecomputeProgressTracker):
    tracker.start_phase("geometry", detail="loading study area geometry")
    from study_area import load_study_area_geometries, study_area_wgs84_envelope_from_metric

    study_area_metric = load_study_area_geometries(
        profile=profile,
        progress_cb=tracker.phase_callback("geometry"),
    )
    study_area_wgs84 = study_area_wgs84_envelope_from_metric(study_area_metric)
    tracker.finish_phase("geometry", "completed", detail="computed study area geometry")
    return study_area_wgs84


def refresh_gtfs(
    *,
    force_refresh: bool = False,
    profile: str | None = None,
) -> list[Any]:
    normalized_profile = normalize_build_profile(profile)
    del normalized_profile
    tracker = PrecomputeProgressTracker(CACHE_DIR / "precompute_timing_stats.json")
    tracker.start_phase("transit", detail="checking GTFS feed availability and cache state")
    transit_progress_cb = tracker.phase_callback("transit")
    results = refresh_gtfs_feeds(force=force_refresh, progress_cb=transit_progress_cb)
    tracker.finish_phase("transit", "completed", detail="GTFS static feed cache updated")
    return results


def gtfs_status(
    *,
    force_refresh: bool = False,
    profile: str | None = None,
) -> list[Any]:
    normalized_profile = normalize_build_profile(profile)
    del normalized_profile
    tracker = PrecomputeProgressTracker(CACHE_DIR / "precompute_timing_stats.json")
    tracker.start_phase("transit", detail="checking GTFS cache status")
    statuses = describe_gtfs_feeds(force=force_refresh)
    tracker.finish_phase("transit", "completed", detail="GTFS cache status collected")
    return statuses


def refresh_transit(
    force_refresh: bool = False,
    *,
    auto_refresh_gtfs: bool = False,
    force_gtfs_refresh: bool = False,
    profile: str | None = None,
) -> str:
    normalized_profile = normalize_build_profile(profile)
    tracker = PrecomputeProgressTracker(CACHE_DIR / "precompute_timing_stats.json")
    tracker.start_phase(
        "transit",
        detail="initializing transit refresh",
    )
    transit_progress_cb = tracker.phase_callback("transit")

    _set_transit_detail(tracker, "connecting to PostgreSQL / checking managed schema")
    engine = build_engine()
    ensure_database_ready(engine)
    _set_transit_detail(tracker, "resolving OSM source state")
    source_state = resolve_source_state(progress_cb=transit_progress_cb)

    _set_transit_detail(tracker, "starting GTFS feed availability checks")

    reality_state, refresh_required = _preflight_transit_rebuild(
        engine,
        import_fingerprint=source_state.import_fingerprint,
        force_refresh=force_refresh,
        auto_refresh_gtfs=auto_refresh_gtfs,
        force_gtfs_refresh=force_gtfs_refresh,
        progress_cb=transit_progress_cb,
    )
    if not refresh_required:
        transit_state = ensure_transit_reality(
            engine,
            import_fingerprint=source_state.import_fingerprint,
            force_refresh=False,
            auto_refresh_gtfs=False,
            force_gtfs_refresh=False,
            progress_cb=transit_progress_cb,
            reality_state=reality_state,
        )
        tracker.finish_phase(
            "transit",
            "cached",
            detail=f"transit reality ready ({transit_state.reality_fingerprint})",
        )
        return transit_state.reality_fingerprint

    normalization_scope_hash = current_normalization_scope_hash(normalized_profile)
    study_area_wgs84 = None
    if not import_payload_ready(
        engine,
        source_state.import_fingerprint,
        normalization_scope_hash,
    ):
        _set_transit_detail(tracker, "GTFS feeds ready; loading geometry prerequisites")
        study_area_wgs84 = _load_study_area_wgs84(normalized_profile, tracker)
        ensure_local_osm_import(
            engine,
            source_state,
            study_area_wgs84=study_area_wgs84,
            normalization_scope_hash=normalization_scope_hash,
            force_refresh=False,
            progress_cb=tracker.phase_callback("import"),
        )

    transit_state = ensure_transit_reality(
        engine,
        import_fingerprint=source_state.import_fingerprint,
        auto_refresh_gtfs=False,
        force_gtfs_refresh=False,
        force_refresh=force_refresh,
        progress_cb=transit_progress_cb,
        reality_state=reality_state,
    )
    tracker.finish_phase(
        "transit",
        "completed",
        detail=f"transit reality ready ({transit_state.reality_fingerprint})",
    )
    return transit_state.reality_fingerprint
