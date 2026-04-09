from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    PMTILES_OUTPUT_PATH,
    CACHE_SCHEMA_VERSION,
    CAPS,
    FORCE_RECOMPUTE,
    GRID_SIZES_M,
    MANIFEST_NAME,
    OSM_EXTRACT_PATH,
    OUTPUT_HTML,
    TAGS,
    USE_COMPRESSED_CACHE,
    WALKGRAPH_BIN,
    WALKGRAPH_BBOX_PADDING_M,
    WALK_RADIUS_M,
    ZOOM_BREAKS,
    build_hashes_for_import,
    current_normalization_scope_hash,
    package_snapshot,
    python_version,
)
from db_postgis import (
    build_engine,
    ensure_database_ready,
    has_complete_build,
    import_payload_ready,
    load_source_amenity_rows,
    publish_precomputed_artifacts,
)
from local_osm_import import ensure_local_osm_import, resolve_source_state
from network import (
    graph_meta_matches,
    load_walk_graph,
    load_walk_graph_index,
    run_walkgraph_build,
)
from progress_tracker import PrecomputeProgressTracker
from study_area import load_study_area_geometries, study_area_wgs84_envelope_from_metric

from . import cache as _cache
from . import grid as _grid
from . import network as _network
from . import phases as _phases
from . import publish as _publish
from . import tiers as _tiers
from . import workflow as _workflow
from .bake_pmtiles import bake_pmtiles as _bake_pmtiles


@dataclass
class _BuildState:
    hashes: Any
    geo_cache_dir: Path
    reach_cache_dir: Path
    score_cache_dir: Path
    tier_valid: dict[Path, bool] = field(default_factory=dict)
    tiers_building: set[Path] = field(default_factory=set)
    source_state: Any = None
    study_area_metric: Any = None
    study_area_wgs84: Any = None

    @classmethod
    def bootstrap(cls) -> _BuildState:
        hashes = build_hashes_for_import("bootstrap")
        return cls(
            hashes=hashes,
            geo_cache_dir=CACHE_DIR / f"geo_{hashes.geo_hash}",
            reach_cache_dir=CACHE_DIR / f"reach_{hashes.reach_hash}",
            score_cache_dir=CACHE_DIR / f"score_{hashes.score_hash}",
        )

    def activate(self, import_fingerprint: str) -> None:
        self.hashes = build_hashes_for_import(import_fingerprint)
        self.geo_cache_dir = CACHE_DIR / f"geo_{self.hashes.geo_hash}"
        self.reach_cache_dir = CACHE_DIR / f"reach_{self.hashes.reach_hash}"
        self.score_cache_dir = CACHE_DIR / f"score_{self.hashes.score_hash}"


_STATE = _BuildState.bootstrap()


def cache_exists(key: str, cache_dir: Path) -> bool:
    return _cache.cache_exists(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
    )


def cache_load(key: str, cache_dir: Path):
    return _cache.cache_load(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
    )


def cache_save(key: str, data: Any, cache_dir: Path) -> None:
    _cache.cache_save(key, data, cache_dir)


def cache_exists_large(key: str, cache_dir: Path) -> bool:
    return _cache.cache_exists_large(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_load_large(key: str, cache_dir: Path):
    return _cache.cache_load_large(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def _cache_load_for_finalize(key: str, cache_dir: Path):
    return _cache.cache_load_for_finalize(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
    )


def _cache_load_large_for_finalize(key: str, cache_dir: Path):
    return _cache.cache_load_large_for_finalize(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_save_large(key: str, data: Any, cache_dir: Path) -> None:
    _cache.cache_save_large(
        key,
        data,
        cache_dir,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def _write_tier_manifest(
    tier_dir: Path,
    tier_name: str,
    tier_hash: str,
    status: str,
    last_phase: str = "",
) -> None:
    _tiers.write_tier_manifest(
        tier_dir,
        tier_name,
        tier_hash,
        status,
        last_phase,
        manifest_name=MANIFEST_NAME,
        cache_schema_version=CACHE_SCHEMA_VERSION,
        python_version=python_version,
        package_snapshot=package_snapshot,
        render_hash=_STATE.hashes.render_hash,
    )


def _mark_building(tier_dir: Path, tier_name: str, tier_hash: str, phase: str) -> None:
    _tiers.mark_building(
        tier_dir,
        tier_name,
        tier_hash,
        phase,
        tiers_building=_STATE.tiers_building,
        write_tier_manifest=_write_tier_manifest,
    )


def _mark_complete(tier_dir: Path, tier_name: str, tier_hash: str, phase: str) -> None:
    _tiers.mark_complete(
        tier_dir,
        tier_name,
        tier_hash,
        phase,
        tiers_building=_STATE.tiers_building,
        tier_valid=_STATE.tier_valid,
        write_tier_manifest=_write_tier_manifest,
    )


def _can_finalize_geo_tier(study_area_metric: Any, study_area_wgs84: Any) -> bool:
    return _tiers.can_finalize_geo_tier(
        study_area_metric,
        study_area_wgs84,
        geo_cache_dir=_STATE.geo_cache_dir,
        cache_load_for_finalize=_cache_load_for_finalize,
    )


def _can_finalize_reach_tier(
    amenity_data: dict[str, list[tuple[float, float]]] | None,
) -> bool:
    return _tiers.can_finalize_reach_tier(
        amenity_data,
        reach_cache_dir=_STATE.reach_cache_dir,
        cache_load_for_finalize=_cache_load_for_finalize,
        cache_load_large_for_finalize=_cache_load_large_for_finalize,
    )


def validate_all_tiers() -> None:
    _tiers.validate_all_tiers(
        geo_cache_dir=_STATE.geo_cache_dir,
        reach_cache_dir=_STATE.reach_cache_dir,
        score_cache_dir=_STATE.score_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        reach_hash=_STATE.hashes.reach_hash,
        score_hash=_STATE.hashes.score_hash,
        force_recompute=FORCE_RECOMPUTE,
        manifest_name=MANIFEST_NAME,
        cache_schema_version=CACHE_SCHEMA_VERSION,
        cache_load_for_finalize=_cache_load_for_finalize,
        cache_load_large_for_finalize=_cache_load_large_for_finalize,
        grid_sizes_m=list(GRID_SIZES_M),
        tier_valid=_STATE.tier_valid,
    )


def print_cache_status() -> None:
    _tiers.print_cache_status(
        cache_dir=CACHE_DIR,
        geo_cache_dir=_STATE.geo_cache_dir,
        reach_cache_dir=_STATE.reach_cache_dir,
        score_cache_dir=_STATE.score_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        reach_hash=_STATE.hashes.reach_hash,
        score_hash=_STATE.hashes.score_hash,
        render_hash=_STATE.hashes.render_hash,
        manifest_name=MANIFEST_NAME,
    )


def _elapsed(started_at: float) -> str:
    return f"[{time.perf_counter() - started_at:.1f}s]"


_count_unique_source_nodes = _network._count_unique_source_nodes
_ordered_categories = _network._ordered_categories
_geometry_is_2d = _grid._geometry_is_2d
_grid_cells_are_2d = _grid._grid_cells_are_2d
_ensure_grid_geometries_2d = _grid._ensure_grid_geometries_2d
haversine_m = _grid.haversine_m
build_cell_id = _grid.build_cell_id
build_scoring_grid = _grid.build_scoring_grid
build_grid = _grid.build_grid
materialize_grid_geometry = _grid.materialize_grid_geometry
materialize_cell_geometry = _grid.materialize_cell_geometry
clone_scoring_grid_shells = _grid.clone_scoring_grid_shells
_clone_grid_shells = _grid._clone_grid_shells
nearest_nodes = _network.nearest_nodes
snap_amenities = _network.snap_amenities
normalize_origin_node_ids = _network.normalize_origin_node_ids
precompute_counts_by_node = _network.precompute_counts_by_node
precompute_walk_counts_by_origin_node = _network.precompute_walk_counts_by_origin_node
score_cell = _grid.score_cell
score_cells = _grid.score_cells


def _score_grid_fast_path_candidate() -> bool:
    return all(cache_exists(f"walk_cells_{size}", _STATE.score_cache_dir) for size in GRID_SIZES_M)


def snap_cells_to_nodes(graph, cells: list[dict[str, Any]], key: str, cache_dir: Path) -> list[int]:
    cached = cache_load(key, cache_dir)
    if cached is not None:
        return cached
    if not cells:
        cache_save(key, [], cache_dir)
        return []
    lats, lons = zip(*(cell["centre"] for cell in cells))
    nodes = nearest_nodes(graph, list(lons), list(lats))
    cache_save(key, nodes, cache_dir)
    return nodes


def phase_geometry(tracker: PrecomputeProgressTracker):
    study_area_metric, study_area_wgs84 = _phases.phase_geometry_impl(
        tracker,
        cache_dir=_STATE.geo_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        cache_load=cache_load,
        cache_save=cache_save,
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        geometry_is_2d=_geometry_is_2d,
        can_finalize_geo_tier=_can_finalize_geo_tier,
        load_study_area_geometries=load_study_area_geometries,
        study_area_wgs84_from_metric=study_area_wgs84_envelope_from_metric,
    )
    _STATE.study_area_metric = study_area_metric
    _STATE.study_area_wgs84 = study_area_wgs84
    return study_area_metric, study_area_wgs84


def phase_amenities(
    engine,
    study_area_wgs84,
    tracker: PrecomputeProgressTracker,
) -> tuple[dict[str, list[tuple[float, float]]], list[dict[str, Any]]]:
    return _phases.phase_amenities_impl(
        engine,
        study_area_wgs84,
        tracker,
        tags=list(TAGS),
        cache_dir=_STATE.reach_cache_dir,
        reach_hash=_STATE.hashes.reach_hash,
        import_fingerprint=_STATE.hashes.import_fingerprint,
        cache_load=cache_load,
        cache_save=cache_save,
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        can_finalize_reach_tier=_can_finalize_reach_tier,
        load_source_amenity_rows=load_source_amenity_rows,
    )


def phase_networks(
    engine,
    tracker: PrecomputeProgressTracker,
):
    return _phases.phase_networks_impl(
        engine,
        tracker,
        source_state=_STATE.source_state,
        study_area_wgs84=_STATE.study_area_wgs84,
        cache_dir=_STATE.geo_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        tiers_building=_STATE.tiers_building,
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        graph_meta_matches=graph_meta_matches,
        load_walk_graph_index=load_walk_graph_index,
        run_walkgraph_build=run_walkgraph_build,
        walkgraph_bin=WALKGRAPH_BIN,
        bbox_padding_m=WALKGRAPH_BBOX_PADDING_M,
    )


def phase_reachability(
    walk_graph,
    amenity_data: dict[str, list[tuple[float, float]]],
    tracker: PrecomputeProgressTracker,
    *,
    walk_origin_node_ids,
):
    return _phases.phase_reachability_impl(
        walk_graph,
        amenity_data,
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
        mark_building=_mark_building,
        mark_complete=_mark_complete,
        snap_amenities=snap_amenities,
        normalize_origin_node_ids=normalize_origin_node_ids,
        precompute_walk_counts_by_origin_node=precompute_walk_counts_by_origin_node,
    )


def phase_grids(
    engine,
    study_area_metric,
    amenity_data: dict[str, list[tuple[float, float]]],
    tracker: PrecomputeProgressTracker,
):
    return _phases.phase_grids_impl(
        engine,
        study_area_metric,
        amenity_data,
        tracker,
        grid_sizes_m=list(GRID_SIZES_M),
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
        build_grid=build_grid,
        elapsed=_elapsed,
        clone_grid_shells=_clone_grid_shells,
        snap_cells_to_nodes=snap_cells_to_nodes,
        score_cells=score_cells,
    )


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
        materialize_cell_geometry=materialize_cell_geometry,
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


def _summary_json(
    study_area_wgs84,
    walk_grids: dict[int, list[dict[str, Any]]],
    amenity_data: dict[str, list[tuple[float, float]]],
) -> dict[str, Any]:
    return _publish.summary_json_impl(
        study_area_wgs84,
        walk_grids,
        amenity_data,
        hashes=_STATE.hashes,
        source_state=_STATE.source_state,
        osm_extract_path=OSM_EXTRACT_PATH,
        grid_sizes_m=list(GRID_SIZES_M),
        output_html=OUTPUT_HTML,
        zoom_breaks=ZOOM_BREAKS,
    )


def run_precompute(
    force_precompute: bool = False,
    *,
    auto_refresh_import: bool = False,
) -> str:
    return _workflow.run_precompute_impl(
        force_precompute=force_precompute,
        auto_refresh_import=auto_refresh_import,
        cache_dir=CACHE_DIR,
        current_normalization_scope_hash=current_normalization_scope_hash,
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=_STATE.activate,
        print_cache_status=print_cache_status,
        validate_all_tiers=validate_all_tiers,
        phase_geometry=phase_geometry,
        phase_amenities=phase_amenities,
        phase_grids=phase_grids,
        score_grid_fast_path_candidate=_score_grid_fast_path_candidate,
        has_complete_build=has_complete_build,
        import_payload_ready=import_payload_ready,
        ensure_local_osm_import=ensure_local_osm_import,
        tracker_factory=PrecomputeProgressTracker,
        walk_rows=_walk_rows,
        amenity_rows=_amenity_rows,
        publish_precomputed_artifacts=publish_precomputed_artifacts,
        summary_json=_summary_json,
        package_snapshot=package_snapshot,
        python_version=python_version,
        get_hashes=lambda: _STATE.hashes,
        set_source_state=lambda source_state: setattr(_STATE, "source_state", source_state),
        bake_pmtiles=_bake_pmtiles,
        pmtiles_output_path=PMTILES_OUTPUT_PATH,
    )


def refresh_local_import(force_refresh: bool = True) -> str:
    return _workflow.run_import_refresh_impl(
        force_refresh=force_refresh,
        cache_dir=CACHE_DIR,
        current_normalization_scope_hash=current_normalization_scope_hash,
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=_STATE.activate,
        phase_geometry=phase_geometry,
        import_payload_ready=import_payload_ready,
        ensure_local_osm_import=ensure_local_osm_import,
        tracker_factory=PrecomputeProgressTracker,
        get_hashes=lambda: _STATE.hashes,
        set_source_state=lambda source_state: setattr(_STATE, "source_state", source_state),
    )
