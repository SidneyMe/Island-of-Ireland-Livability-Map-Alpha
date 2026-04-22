from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    CACHE_SCHEMA_VERSION,
    CAPS,
    FORCE_RECOMPUTE,
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    LIVABILITY_SURFACE_THREADS,
    MANIFEST_NAME,
    OSM_EXTRACT_PATH,
    OUTPUT_HTML,
    TAGS,
    USE_COMPRESSED_CACHE,
    WALKGRAPH_BIN,
    WALKGRAPH_BBOX_PADDING_M,
    WALK_RADIUS_M,
    build_hashes_for_import,
    build_profile_settings,
    current_normalization_scope_hash,
    normalize_build_profile,
    package_snapshot,
    pmtiles_output_path,
    profile_fine_surface_enabled,
    python_version,
)
from db_postgis import (
    build_engine,
    ensure_database_ready,
    has_complete_build,
    import_payload_ready,
    load_merged_source_amenity_rows,
    load_source_amenity_rows,
    load_service_desert_rows,
    load_transport_reality_points,
    publish_precomputed_artifacts,
    replace_service_desert_rows,
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
from transit import (
    ensure_transit_reality as _ensure_transit_reality_impl,
    transit_reality_refresh_required as _transit_reality_refresh_required,
)
from walkgraph_support import ensure_walkgraph_subcommand_available

from . import cache as _cache
from . import amenity_clusters as _amenity_clusters
from . import grid as _grid
from . import network as _network
from . import phases as _phases
from . import publish as _publish
from . import surface as _surface
from . import tiers as _tiers
from . import workflow as _workflow
from .bake_pmtiles import bake_pmtiles as _bake_pmtiles
import overture.loader as _overture
import overture.merge as _overture_merge


@dataclass
class _BuildState:
    profile: str
    settings: Any
    hashes: Any
    geo_cache_dir: Path
    reach_cache_dir: Path
    score_cache_dir: Path
    surface_shell_dir: Path
    surface_score_dir: Path
    surface_tile_dir: Path
    tier_valid: dict[Path, bool] = field(default_factory=dict)
    tiers_building: set[Path] = field(default_factory=set)
    source_state: Any = None
    transit_reality_state: Any = None
    study_area_metric: Any = None
    study_area_wgs84: Any = None

    @classmethod
    def bootstrap(cls) -> _BuildState:
        profile = normalize_build_profile()
        settings = build_profile_settings(profile)
        hashes = build_hashes_for_import(
            "bootstrap",
            transit_reality_fingerprint="bootstrap",
            profile=profile,
        )
        return cls(
            profile=profile,
            settings=settings,
            hashes=hashes,
            geo_cache_dir=CACHE_DIR / f"geo_{hashes.geo_hash}",
            reach_cache_dir=CACHE_DIR / f"reach_{hashes.reach_hash}",
            score_cache_dir=CACHE_DIR / f"score_{hashes.score_hash}",
            surface_shell_dir=_surface.surface_shell_dir(
                CACHE_DIR,
                surface_shell_hash=hashes.surface_shell_hash,
            ),
            surface_score_dir=_surface.surface_score_dir(
                CACHE_DIR,
                score_hash=hashes.score_hash,
            ),
            surface_tile_dir=_surface.surface_tile_dir(
                CACHE_DIR,
                score_hash=hashes.score_hash,
                render_hash=hashes.render_hash,
            ),
        )

    def activate(
        self,
        import_fingerprint: str,
        *,
        transit_reality_fingerprint: str = "transit-unavailable",
        profile: str,
    ) -> None:
        self.profile = normalize_build_profile(profile)
        self.settings = build_profile_settings(self.profile)
        self.hashes = build_hashes_for_import(
            import_fingerprint,
            transit_reality_fingerprint=transit_reality_fingerprint,
            profile=self.profile,
        )
        self.geo_cache_dir = CACHE_DIR / f"geo_{self.hashes.geo_hash}"
        self.reach_cache_dir = CACHE_DIR / f"reach_{self.hashes.reach_hash}"
        self.score_cache_dir = CACHE_DIR / f"score_{self.hashes.score_hash}"
        self.surface_shell_dir = _surface.surface_shell_dir(
            CACHE_DIR,
            surface_shell_hash=self.hashes.surface_shell_hash,
        )
        self.surface_score_dir = _surface.surface_score_dir(
            CACHE_DIR,
            score_hash=self.hashes.score_hash,
        )
        self.surface_tile_dir = _surface.surface_tile_dir(
            CACHE_DIR,
            score_hash=self.hashes.score_hash,
            render_hash=self.hashes.render_hash,
        )


_STATE = _BuildState.bootstrap()


def _active_fine_surface_enabled() -> bool:
    return profile_fine_surface_enabled(_STATE.profile)


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


def cache_save_large_append_frame(key: str, frame: Any, cache_dir: Path) -> None:
    _cache.cache_save_large_append_frame(
        key,
        frame,
        cache_dir,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_reset_large_frames(key: str, cache_dir: Path) -> None:
    _cache.cache_reset_large_frames(
        key,
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


def _surface_analysis_ready() -> bool:
    return _surface.surface_analysis_ready(
        _STATE.surface_shell_dir,
        _STATE.surface_score_dir,
        expected_surface_shell_hash=_STATE.hashes.surface_shell_hash,
        expected_score_hash=_STATE.hashes.score_hash,
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
        grid_sizes_m=list(_STATE.settings.grid_sizes_m),
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
    for label, directory in (
        ("surface_shell", _STATE.surface_shell_dir),
        ("surface_scores", _STATE.surface_score_dir),
        ("surface_tiles", _STATE.surface_tile_dir),
    ):
        manifest = _surface.load_surface_manifest(directory)
        if manifest is None:
            print(f"  {label}  {directory.name}  (no manifest)")
            continue
        shard_inventory = manifest.get("shard_inventory", [])
        completed_shards = manifest.get("completed_shards")
        total_shards = manifest.get("total_shards")
        shard_progress = None
        if isinstance(total_shards, int) and total_shards >= 0:
            completed_value = (
                int(completed_shards)
                if isinstance(completed_shards, int) and completed_shards >= 0
                else 0
            )
            shard_progress = f"{completed_value}/{int(total_shards)}"
        elif isinstance(shard_inventory, list):
            shard_progress = str(len(shard_inventory))
            if label == "surface_shell" and manifest.get("status") == "building":
                shard_dir = directory / "shards"
                existing_files = len(list(shard_dir.glob("*.npz"))) if shard_dir.exists() else 0
                if existing_files > 0:
                    shard_progress = str(existing_files)
        print(
            f"  {label}  {directory.name}  "
            f"status={manifest.get('status', '?')}  "
            f"shards={shard_progress if shard_progress is not None else 0}"
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
merge_normalized_origin_node_ids = _network.merge_normalized_origin_node_ids
precompute_counts_by_node = _network.precompute_counts_by_node
precompute_walk_counts_by_origin_node = _network.precompute_walk_counts_by_origin_node
precompute_walk_weighted_totals_by_origin_node = _network.precompute_walk_weighted_totals_by_origin_node
precompute_walk_decayed_units_by_origin_node = _network.precompute_walk_decayed_units_by_origin_node
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
    result = _phases.phase_amenities_impl(
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
    # Load Overture places for visualization — never fed to walkgraph scoring
    return result


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


def _transport_reality_rows(engine, created_at, *, progress_cb=None):
    del progress_cb
    if _STATE.transit_reality_state is None:
        return []
    rows = load_transport_reality_points(
        engine,
        _STATE.transit_reality_state.reality_fingerprint,
    )
    return [
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
            "last_public_service_date": row["last_public_service_date"],
            "last_any_service_date": row["last_any_service_date"],
            "route_modes_json": row["route_modes_json"],
            "source_reason_codes_json": row["source_reason_codes_json"],
            "reality_reason_codes_json": row["reality_reason_codes_json"],
            "geom": row["geom"],
            "created_at": created_at,
        }
        for row in rows
    ]


def _service_desert_rows(engine, created_at, *, progress_cb=None):
    del progress_cb
    rows = load_service_desert_rows(engine, _STATE.hashes.build_key)
    return [
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
    public_weight_rows = {
        "public_departures_7d": []
    }
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


def _summary_json(
    study_area_wgs84,
    walk_grids: dict[int, list[dict[str, Any]]],
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return _publish.summary_json_impl(
        study_area_wgs84,
        walk_grids,
        amenity_data,
        amenity_source_rows,
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


def _ensure_transit_reality(
    engine,
    *,
    import_fingerprint: str,
    study_area_wgs84=None,
    force_refresh: bool = False,
    refresh_download: bool = False,
    progress_cb=None,
    reality_state: Any = None,
) -> Any:
    del study_area_wgs84
    transit_state = _ensure_transit_reality_impl(
        engine,
        import_fingerprint=import_fingerprint,
        refresh_download=refresh_download,
        force_refresh=force_refresh,
        progress_cb=progress_cb,
        reality_state=reality_state,
    )
    _STATE.transit_reality_state = transit_state
    _STATE.activate(
        import_fingerprint,
        transit_reality_fingerprint=transit_state.reality_fingerprint,
        profile=_STATE.profile,
    )
    return transit_state


def _preflight_transit_rebuild(
    engine,
    *,
    force_refresh: bool = False,
    refresh_download: bool = False,
) -> tuple[Any, bool]:
    import_fingerprint = None
    if _STATE.source_state is not None:
        import_fingerprint = _STATE.source_state.import_fingerprint
    reality_state, refresh_required = _transit_reality_refresh_required(
        engine,
        import_fingerprint=import_fingerprint,
        refresh_download=refresh_download,
        force_refresh=force_refresh,
    )
    if refresh_required:
        ensure_walkgraph_subcommand_available(WALKGRAPH_BIN, "gtfs-refresh")
    return reality_state, refresh_required


def run_precompute(
    force_precompute: bool = False,
    *,
    auto_refresh_import: bool = False,
    profile: str = "full",
) -> str:
    normalized_profile = normalize_build_profile(profile)
    return _workflow.run_precompute_impl(
        force_precompute=force_precompute,
        auto_refresh_import=auto_refresh_import,
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
        compute_service_deserts=_compute_service_deserts,
        publish_precomputed_artifacts=publish_precomputed_artifacts,
        summary_json=_summary_json,
        package_snapshot=package_snapshot,
        python_version=python_version,
        get_hashes=lambda: _STATE.hashes,
        set_source_state=lambda source_state: setattr(_STATE, "source_state", source_state),
        fine_surface_ready=_surface_analysis_ready if profile_fine_surface_enabled(normalized_profile) else None,
        bake_pmtiles=_bake_pmtiles,
        pmtiles_output_path=pmtiles_output_path(normalized_profile),
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

    reality_state, refresh_required = _preflight_transit_rebuild(
        engine,
        force_refresh=force_refresh,
        refresh_download=refresh_download,
    )
    if not refresh_required:
        transit_state = _ensure_transit_reality(
            engine,
            import_fingerprint=source_state.import_fingerprint,
            study_area_wgs84=None,
            force_refresh=False,
            refresh_download=False,
            reality_state=reality_state,
        )
        return transit_state.reality_fingerprint

    tracker = PrecomputeProgressTracker(CACHE_DIR / "precompute_timing_stats.json")
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
        progress_cb=tracker.phase_callback("transit"),
        reality_state=reality_state,
    )
    return transit_state.reality_fingerprint
