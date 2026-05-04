from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    CAPS,
    LIVABILITY_SURFACE_THREADS,
    TAGS,
    WALK_RADIUS_M,
    WALKGRAPH_BIN,
    current_normalization_scope_hash,
    normalize_build_profile,
    noise_pmtiles_output_path,
    package_snapshot,
    pmtiles_output_path,
    profile_fine_surface_enabled,
    python_version,
    resolve_noise_max_zoom,
)
from db_postgis import (
    build_engine,
    ensure_database_ready,
    has_complete_build,
    import_payload_ready,
    load_transport_reality_points,
    publish_precomputed_artifacts,
    refresh_noise_overlay_for_build,
    replace_service_desert_rows,
)
from local_osm_import import ensure_local_osm_import, resolve_source_state
from network import load_walk_graph_index
from progress_tracker import PrecomputeProgressTracker
from transit import transit_reality_refresh_required as _transit_reality_refresh_required
from walkgraph_support import ensure_walkgraph_subcommand_available

from . import amenity_clusters as _amenity_clusters
from . import cache as _cache
from . import grid as _grid
from . import network as _network
from . import phases as _phases
from . import publish as _publish
from . import surface as _surface
from . import tiers as _tiers
from . import workflow as _workflow
from .bake_pmtiles import bake_pmtiles as _bake_pmtiles
from noise_artifacts.bake import bake_noise_pmtiles as _bake_noise_pmtiles
from ._state import _STATE, _active_fine_surface_enabled, _elapsed
from ._cache_wrappers import (
    cache_exists,
    cache_load,
    cache_save,
    cache_exists_large,
    cache_load_large,
    _cache_load_for_finalize,
    _cache_load_large_for_finalize,
    cache_save_large,
    cache_save_large_append_frame,
    cache_reset_large_frames,
)
from ._tier_wrappers import (
    _write_tier_manifest,
    _mark_building,
    _mark_complete,
    _can_finalize_geo_tier,
    _can_finalize_reach_tier,
    _surface_analysis_ready,
    validate_all_tiers,
    print_cache_status,
)
from ._phase_wrappers import (
    _count_unique_source_nodes,
    _ordered_categories,
    _geometry_is_2d,
    _grid_cells_are_2d,
    _ensure_grid_geometries_2d,
    _clone_grid_shells,
    haversine_m,
    build_cell_id,
    build_scoring_grid,
    build_grid,
    materialize_grid_geometry,
    materialize_cell_geometry,
    clone_scoring_grid_shells,
    nearest_nodes,
    snap_amenities,
    normalize_origin_node_ids,
    merge_normalized_origin_node_ids,
    precompute_counts_by_node,
    precompute_walk_counts_by_origin_node,
    precompute_walk_weighted_totals_by_origin_node,
    precompute_walk_decayed_units_by_origin_node,
    score_cell,
    score_cells,
    _score_grid_fast_path_candidate,
    snap_cells_to_nodes,
    phase_geometry,
    phase_amenities,
    phase_networks,
)
from ._rows import (
    _dispatch_noise_in_background,
    _await_background_noise,
    _walk_rows,
    _amenity_rows,
    _transport_reality_rows,
    _service_desert_rows,
    _noise_rows,
    _noise_processing_hash,
    _summary_json,
)
from ._transit_wrappers import _ensure_transit_reality


def phase_reachability(
    walk_graph,
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
    tracker: PrecomputeProgressTracker,
    *,
    walk_origin_node_ids,
):
    return _phases.phase_reachability_impl(
        walk_graph,
        amenity_data,
        amenity_source_rows,
        tracker,
        walk_origin_node_ids=walk_origin_node_ids,
        cache_dir=_STATE.reach_cache_dir,
        reach_hash=_STATE.hashes.reach_hash,
        tiers_building=_STATE.tiers_building,
        walk_radius_m=WALK_RADIUS_M,
        cache_load=cache_load,
        cache_save=cache_save,
        cache_load_large=cache_load_large,
        cache_save_large=cache_save_large,
        cache_save_large_append_frame=cache_save_large_append_frame,
        cache_reset_large_frames=cache_reset_large_frames,
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        snap_amenities=snap_amenities,
        normalize_origin_node_ids=normalize_origin_node_ids,
        precompute_walk_counts_by_origin_node=precompute_walk_counts_by_origin_node,
        precompute_walk_decayed_units_by_origin_node=precompute_walk_decayed_units_by_origin_node,
    )


def phase_grids(
    engine,
    study_area_metric,
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
    tracker: PrecomputeProgressTracker,
):
    return _phases.phase_grids_impl(
        engine,
        study_area_metric,
        amenity_data,
        amenity_source_rows,
        tracker,
        grid_sizes_m=list(_STATE.settings.grid_sizes_m),
        cache_dir=_STATE.score_cache_dir,
        score_hash=_STATE.hashes.score_hash,
        tiers_building=_STATE.tiers_building,
        cache_exists=cache_exists,
        cache_load=cache_load,
        cache_save=cache_save,
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        grid_cells_are_2d=_grid_cells_are_2d,
        phase_networks=phase_networks,
        phase_reachability=phase_reachability,
        normalize_origin_node_ids=normalize_origin_node_ids,
        merge_normalized_origin_node_ids=merge_normalized_origin_node_ids,
        build_grid=build_grid,
        elapsed=_elapsed,
        clone_grid_shells=_clone_grid_shells,
        snap_cells_to_nodes=snap_cells_to_nodes,
        score_cells=score_cells,
        fine_surface_enabled=_active_fine_surface_enabled(),
        reach_hash=_STATE.hashes.reach_hash,
        surface_shell_hash=_STATE.hashes.surface_shell_hash,
        surface_shell_dir=_STATE.surface_shell_dir,
        surface_score_dir=_STATE.surface_score_dir,
        ensure_surface_shell_cache=_surface.ensure_surface_shell_cache,
        ensure_surface_score_cache=_surface.ensure_surface_score_cache,
        collect_surface_origin_nodes=_surface.collect_surface_origin_nodes,
        surface_analysis_ready=_surface.surface_analysis_ready,
        graph_dir=_STATE.geo_cache_dir / "walk_graph",
        walkgraph_bin=WALKGRAPH_BIN,
        surface_threads=LIVABILITY_SURFACE_THREADS,
    )


def _compute_service_deserts(engine, walk_grids: dict[int, list[dict[str, Any]]]) -> None:
    if _STATE.transit_reality_state is None or _STATE.study_area_metric is None:
        return
    walk_graph = load_walk_graph_index(_STATE.geo_cache_dir / "walk_graph")
    reality_rows = load_transport_reality_points(
        engine,
        _STATE.transit_reality_state.reality_fingerprint,
    )
    if not reality_rows:
        replace_service_desert_rows(engine, build_key=_STATE.hashes.build_key, desert_rows=[])
        return

    baseline_rows = [
        row
        for row in reality_rows
        if str(row.get("source_status") or "gtfs_direct") == "gtfs_direct"
    ]
    if not baseline_rows:
        replace_service_desert_rows(engine, build_key=_STATE.hashes.build_key, desert_rows=[])
        return

    baseline_points = {
        "transport": [
            (float(row["geom"].y), float(row["geom"].x))
            for row in baseline_rows
        ]
    }
    baseline_nodes_by_category = snap_amenities(walk_graph, baseline_points)
    public_weight_rows: dict[str, list] = {"public_departures_7d": []}
    if baseline_rows:
        public_nodes = nearest_nodes(
            walk_graph,
            [float(row["geom"].x) for row in baseline_rows],
            [float(row["geom"].y) for row in baseline_rows],
        )
        for row, node in zip(baseline_rows, public_nodes):
            departures = int(row["public_departures_7d"] or 0)
            if departures <= 0:
                continue
            public_weight_rows["public_departures_7d"].append((int(node), departures))

    walk_cell_nodes_by_size: dict[int, list[int]] = {}
    for resolution_m, cells in walk_grids.items():
        cached_nodes = cache_load(f"walk_cell_nodes_{resolution_m}", _STATE.score_cache_dir)
        if cached_nodes is None:
            cached_nodes = snap_cells_to_nodes(
                walk_graph,
                cells,
                f"walk_cell_nodes_{resolution_m}",
                _STATE.score_cache_dir,
            )
        walk_cell_nodes_by_size[int(resolution_m)] = [int(node) for node in cached_nodes]

    origin_nodes = normalize_origin_node_ids(
        node
        for nodes in walk_cell_nodes_by_size.values()
        for node in nodes
    )
    baseline_counts_by_node = precompute_walk_counts_by_origin_node(
        walk_graph,
        baseline_nodes_by_category,
        origin_nodes,
        cutoff=WALK_RADIUS_M,
        weight="length_m",
    )
    public_totals_by_node = precompute_walk_weighted_totals_by_origin_node(
        walk_graph,
        public_weight_rows,
        origin_nodes,
        cutoff=WALK_RADIUS_M,
        weight="length_m",
    )

    created_at = datetime.now(timezone.utc)
    desert_rows: list[dict[str, Any]] = []
    for resolution_m, cells in walk_grids.items():
        cell_nodes = walk_cell_nodes_by_size[int(resolution_m)]
        for cell, origin_node in zip(cells, cell_nodes):
            baseline_stop_count = int(
                baseline_counts_by_node.get(int(origin_node), {}).get("transport", 0)
            )
            reachable_public_departures = int(
                public_totals_by_node.get(int(origin_node), {}).get("public_departures_7d", 0)
            )
            if baseline_stop_count <= 0 or reachable_public_departures > 0:
                continue
            cell_geom = cell.get("geometry")
            if cell_geom is None:
                cell_geom = materialize_cell_geometry(cell, _STATE.study_area_metric)
            desert_rows.append(
                {
                    "build_key": _STATE.hashes.build_key,
                    "reality_fingerprint": _STATE.transit_reality_state.reality_fingerprint,
                    "import_fingerprint": _STATE.hashes.import_fingerprint,
                    "resolution_m": int(resolution_m),
                    "cell_id": cell["cell_id"],
                    "analysis_date": _STATE.transit_reality_state.analysis_date,
                    "baseline_reachable_stop_count": baseline_stop_count,
                    "reachable_public_departures_7d": reachable_public_departures,
                    "reason_codes_json": ["baseline_reachable_without_public_departures_7d"],
                    "cell_geom": cell_geom,
                    "created_at": created_at,
                }
            )
    replace_service_desert_rows(
        engine,
        build_key=_STATE.hashes.build_key,
        desert_rows=desert_rows,
    )


def _preflight_transit_rebuild(
    engine,
    *,
    force_refresh: bool = False,
    refresh_download: bool = False,
    progress_cb=None,
) -> tuple[Any, bool]:
    import_fingerprint = None
    if _STATE.source_state is not None:
        import_fingerprint = _STATE.source_state.import_fingerprint
    reality_state, refresh_required = _transit_reality_refresh_required(
        engine,
        import_fingerprint=import_fingerprint,
        refresh_download=refresh_download,
        force_refresh=force_refresh,
        progress_cb=progress_cb,
    )
    if refresh_required:
        ensure_walkgraph_subcommand_available(WALKGRAPH_BIN, "gtfs-refresh")
    return reality_state, refresh_required


def run_precompute(
    force_precompute: bool = False,
    *,
    auto_refresh_import: bool = False,
    profile: str = "full",
    force_noise_artifact: bool = False,
    reimport_noise_source: bool = False,
    force_noise_all: bool = False,
    noise_accurate: bool = False,
    require_active_noise_artifact: bool = False,
    refresh_noise_artifact: bool = False,
) -> str:
    normalized_profile = normalize_build_profile(profile)
    return _workflow.run_precompute_impl(
        force_precompute=force_precompute,
        auto_refresh_import=auto_refresh_import,
        force_noise_artifact=force_noise_artifact,
        reimport_noise_source=reimport_noise_source,
        force_noise_all=force_noise_all,
        noise_accurate=noise_accurate,
        require_active_noise_artifact=require_active_noise_artifact,
        refresh_noise_artifact=refresh_noise_artifact,
        cache_dir=CACHE_DIR,
        build_profile=normalized_profile,
        current_normalization_scope_hash=lambda: current_normalization_scope_hash(normalized_profile),
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=lambda import_fingerprint: _STATE.activate(
            import_fingerprint,
            profile=normalized_profile,
        ),
        print_cache_status=print_cache_status,
        validate_all_tiers=validate_all_tiers,
        phase_geometry=phase_geometry,
        phase_amenities=phase_amenities,
        phase_grids=phase_grids,
        score_grid_fast_path_candidate=_score_grid_fast_path_candidate,
        has_complete_build=has_complete_build,
        import_payload_ready=import_payload_ready,
        ensure_local_osm_import=ensure_local_osm_import,
        ensure_transit_reality=_ensure_transit_reality,
        transit_preflight=_preflight_transit_rebuild,
        tracker_factory=PrecomputeProgressTracker,
        walk_rows=_walk_rows,
        amenity_rows=_amenity_rows,
        transport_reality_rows=_transport_reality_rows,
        service_desert_rows=_service_desert_rows,
        noise_rows=_noise_rows,
        dispatch_noise_loader=_dispatch_noise_in_background,
        compute_service_deserts=_compute_service_deserts,
        noise_processing_hash=_noise_processing_hash,
        publish_precomputed_artifacts=publish_precomputed_artifacts,
        summary_json=_summary_json,
        package_snapshot=package_snapshot,
        python_version=python_version,
        get_hashes=lambda: _STATE.hashes,
        set_source_state=lambda source_state: setattr(_STATE, "source_state", source_state),
        fine_surface_ready=_surface_analysis_ready if profile_fine_surface_enabled(normalized_profile) else None,
        bake_pmtiles=_bake_pmtiles,
        pmtiles_output_path=pmtiles_output_path(normalized_profile),
        bake_noise_pmtiles=_bake_noise_pmtiles,
        noise_pmtiles_output_path=noise_pmtiles_output_path(normalized_profile),
        noise_max_zoom=resolve_noise_max_zoom(normalized_profile),
        refresh_noise_overlay_for_build=refresh_noise_overlay_for_build,
    )


def refresh_local_import(force_refresh: bool = True) -> str:
    normalized_profile = normalize_build_profile()
    return _workflow.run_import_refresh_impl(
        force_refresh=force_refresh,
        cache_dir=CACHE_DIR,
        current_normalization_scope_hash=current_normalization_scope_hash,
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=lambda import_fingerprint: _STATE.activate(
            import_fingerprint,
            profile=normalized_profile,
        ),
        phase_geometry=phase_geometry,
        import_payload_ready=import_payload_ready,
        ensure_local_osm_import=ensure_local_osm_import,
        tracker_factory=PrecomputeProgressTracker,
        get_hashes=lambda: _STATE.hashes,
        set_source_state=lambda source_state: setattr(_STATE, "source_state", source_state),
    )


def refresh_transit(
    force_refresh: bool = False,
    *,
    refresh_download: bool = True,
) -> str:
    normalized_profile = normalize_build_profile()
    engine = build_engine()
    ensure_database_ready(engine)
    source_state = resolve_source_state()
    _STATE.source_state = source_state
    _STATE.activate(source_state.import_fingerprint, profile=normalized_profile)
    tracker = PrecomputeProgressTracker(CACHE_DIR / "precompute_timing_stats.json")
    tracker.start_phase(
        "transit",
        detail="checking GTFS feed availability and cache state",
    )
    transit_progress_cb = tracker.phase_callback("transit")

    reality_state, refresh_required = _preflight_transit_rebuild(
        engine,
        force_refresh=force_refresh,
        refresh_download=refresh_download,
        progress_cb=transit_progress_cb,
    )
    if not refresh_required:
        transit_state = _ensure_transit_reality(
            engine,
            import_fingerprint=source_state.import_fingerprint,
            study_area_wgs84=None,
            force_refresh=False,
            refresh_download=False,
            progress_cb=transit_progress_cb,
            reality_state=reality_state,
        )
        tracker.finish_phase(
            "transit",
            "cached",
            detail=f"transit reality ready ({transit_state.reality_fingerprint})",
        )
        return transit_state.reality_fingerprint

    tracker.set_phase_detail("transit", "GTFS feeds ready; loading geometry prerequisites")
    study_area_metric, study_area_wgs84 = phase_geometry(tracker)
    _STATE.study_area_metric = study_area_metric
    _STATE.study_area_wgs84 = study_area_wgs84

    normalization_scope_hash = current_normalization_scope_hash(normalized_profile)
    if not import_payload_ready(
        engine,
        source_state.import_fingerprint,
        normalization_scope_hash,
    ):
        ensure_local_osm_import(
            engine,
            source_state,
            study_area_wgs84=study_area_wgs84,
            normalization_scope_hash=normalization_scope_hash,
            force_refresh=False,
            progress_cb=tracker.phase_callback("import"),
        )

    transit_state = _ensure_transit_reality(
        engine,
        import_fingerprint=source_state.import_fingerprint,
        study_area_wgs84=study_area_wgs84,
        refresh_download=False,
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
