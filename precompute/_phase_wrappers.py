from __future__ import annotations

from pathlib import Path
from typing import Any

from config import (
    TAGS,
    WALKGRAPH_BIN,
    WALKGRAPH_BBOX_PADDING_M,
)
from db_postgis import (
    load_merged_source_amenity_rows,
    load_source_amenity_rows,
)
from network import graph_meta_matches, load_walk_graph_index, run_walkgraph_build
from progress_tracker import PrecomputeProgressTracker
from study_area import load_study_area_geometries, study_area_wgs84_envelope_from_metric
import overture.loader as _overture
import overture.merge as _overture_merge

from . import grid as _grid
from . import network as _network
from . import phases as _phases
from . import surface as _surface
from ._cache_wrappers import cache_exists, cache_load, cache_save
from ._state import _STATE, _active_fine_surface_enabled
from ._tier_wrappers import (
    _can_finalize_geo_tier,
    _can_finalize_reach_tier,
    _mark_building,
    _mark_complete,
    _surface_analysis_ready,
)

# Re-exports from _network used throughout the codebase and tests
_count_unique_source_nodes = _network._count_unique_source_nodes
_ordered_categories = _network._ordered_categories
nearest_nodes = _network.nearest_nodes
snap_amenities = _network.snap_amenities
normalize_origin_node_ids = _network.normalize_origin_node_ids
merge_normalized_origin_node_ids = _network.merge_normalized_origin_node_ids
precompute_counts_by_node = _network.precompute_counts_by_node
precompute_walk_counts_by_origin_node = _network.precompute_walk_counts_by_origin_node
precompute_walk_weighted_totals_by_origin_node = _network.precompute_walk_weighted_totals_by_origin_node
precompute_walk_decayed_units_by_origin_node = _network.precompute_walk_decayed_units_by_origin_node

# Re-exports from _grid used throughout the codebase and tests
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
score_cell = _grid.score_cell
score_cells = _grid.score_cells


def _score_grid_fast_path_candidate() -> bool:
    coarse_ready = all(
        cache_exists(f"walk_cells_{size}", _STATE.score_cache_dir)
        for size in _STATE.settings.grid_sizes_m
    )
    if not coarse_ready:
        return False
    if not _active_fine_surface_enabled():
        return True
    return _surface_analysis_ready()


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
        load_study_area_geometries=lambda *, progress_cb=None: load_study_area_geometries(
            profile=_STATE.profile,
            progress_cb=progress_cb,
        ),
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
        load_overture_amenity_rows=_overture.load_overture_amenity_rows,
        merge_source_amenity_rows=_overture_merge.merge_source_amenity_rows,
        load_merged_source_amenity_rows=load_merged_source_amenity_rows,
        transit_reality_fingerprint=_STATE.hashes.transit_reality_fingerprint,
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


