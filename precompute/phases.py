from __future__ import annotations

import time
from pathlib import Path
from typing import Any


_PARK_CATEGORY = "parks"
_PARK_AREA_UNIT_M2 = 50_000.0


def _grid_size_signature(grid_sizes_m: list[int]) -> str:
    sizes = sorted({int(size) for size in grid_sizes_m})
    if not sizes:
        return "none"
    return "_".join(str(size) for size in sizes)


def _walk_origin_nodes_cache_key(grid_sizes_m: list[int]) -> str:
    return f"walk_origin_nodes__sizes_{_grid_size_signature(grid_sizes_m)}"


def _merge_counts_lookup(
    existing_counts: dict[int, dict[str, int]],
    new_counts: dict[int, dict[str, int]],
) -> dict[int, dict[str, int]]:
    merged_counts = dict(existing_counts)
    merged_counts.update(new_counts)
    return {node: merged_counts[node] for node in sorted(merged_counts)}


def _merge_numeric_lookup(
    existing_values: dict[int, float],
    new_values: dict[int, float],
) -> dict[int, float]:
    merged_values = {int(node): float(value) for node, value in existing_values.items()}
    merged_values.update({int(node): float(value) for node, value in new_values.items()})
    return {node: merged_values[node] for node in sorted(merged_values)}


def _park_area_m2_from_row(row: dict[str, Any]) -> float:
    try:
        area_m2 = float(row.get("park_area_m2", 0.0))
    except (TypeError, ValueError):
        return 0.0
    if area_m2 < 0.0:
        return 0.0
    return area_m2


def _park_node_weight_rows(
    amenity_source_rows: list[dict[str, Any]],
    park_nodes: list[int],
) -> dict[str, list[tuple[int, int]]]:
    park_rows = [row for row in amenity_source_rows if row.get("category") == _PARK_CATEGORY]
    if len(park_rows) != len(park_nodes):
        raise ValueError(
            "Cached park amenity rows and snapped park nodes are out of sync: "
            f"{len(park_rows)} park rows vs {len(park_nodes)} snapped park nodes."
        )

    weight_rows: list[tuple[int, int]] = []
    for row, node in zip(park_rows, park_nodes):
        integer_area_m2 = int(round(_park_area_m2_from_row(row)))
        if integer_area_m2 <= 0:
            continue
        weight_rows.append((int(node), integer_area_m2))
    return {_PARK_CATEGORY: weight_rows}


def _park_area_units_lookup(
    park_area_m2_by_node: dict[int, dict[str, int]],
) -> dict[int, float]:
    return {
        int(node): float(category_totals.get(_PARK_CATEGORY, 0)) / _PARK_AREA_UNIT_M2
        for node, category_totals in park_area_m2_by_node.items()
    }


def _record_substep(
    tracker,
    phase_name: str,
    substep_name: str,
    started_at: float,
    *,
    force_log: bool = False,
) -> float:
    seconds = max(time.perf_counter() - started_at, 0.0)
    tracker.record_substep(
        phase_name,
        substep_name,
        seconds,
        force_log=force_log,
    )
    return seconds


def _load_or_build_grid_cells(
    size: int,
    study_area_metric,
    *,
    tracker,
    cache_dir,
    cache_load,
    cache_save,
    grid_cells_are_2d,
    build_grid,
    elapsed,
) -> tuple[list[dict[str, Any]], bool]:
    grid_key = f"grid_cells_{size}"
    cached_grid_cells = cache_load(grid_key, cache_dir)
    if cached_grid_cells is not None:
        if grid_cells_are_2d(cached_grid_cells):
            return cached_grid_cells, False
        print(f"  [score] cached {size}m grid shells contain non-2D geometry - rebuilding")

    started_at = time.perf_counter()
    print(f"Phase 5 - grid {size:>5}m   building ...", end=" ", flush=True)
    grid_cells = build_grid(size, study_area_metric)
    print(f"{len(grid_cells)} cells {elapsed(started_at)}")
    _record_substep(tracker, "grids", "grid_shell_build", started_at, force_log=True)
    cache_save(grid_key, grid_cells, cache_dir)
    return grid_cells, True


def _load_or_build_walk_origin_nodes(
    grid_sizes_m: list[int],
    walk_cell_nodes_by_size: dict[int, list[int]],
    *,
    cache_dir,
    cache_load,
    cache_save,
    normalize_origin_node_ids,
) -> tuple[list[int], bool]:
    origin_key = _walk_origin_nodes_cache_key(grid_sizes_m)
    cached_origin_nodes = cache_load(origin_key, cache_dir)
    if cached_origin_nodes is not None:
        return list(cached_origin_nodes), False

    walk_origin_nodes = list(
        normalize_origin_node_ids(
            node
            for size in sorted(walk_cell_nodes_by_size)
            for node in walk_cell_nodes_by_size[size]
        )
    )
    cache_save(origin_key, walk_origin_nodes, cache_dir)
    return walk_origin_nodes, True


def _score_summary(scores: list[float]) -> str:
    if not scores:
        return "empty"
    return (
        f"min={min(scores):.0f} avg={sum(scores) / len(scores):.1f} "
        f"max={max(scores):.0f}"
    )


def _graph_bbox(study_area_wgs84) -> tuple[float, float, float, float]:
    min_lon, min_lat, max_lon, max_lat = study_area_wgs84.bounds
    return (float(min_lat), float(min_lon), float(max_lat), float(max_lon))


def phase_geometry_impl(
    tracker,
    *,
    cache_dir,
    geo_hash: str,
    cache_load,
    cache_save,
    mark_building,
    mark_complete,
    geometry_is_2d,
    can_finalize_geo_tier,
    load_study_area_geometries,
    study_area_wgs84_from_metric,
):
    tracker.start_phase("geometry", detail="loading study area geometry")
    study_area_metric = cache_load("study_area_metric", cache_dir)
    study_area_wgs84 = cache_load("study_area_wgs84", cache_dir)
    if study_area_metric is not None and not geometry_is_2d(study_area_metric):
        print("  [geo] cached study area geometry has a Z dimension - rebuilding geometry cache")
        study_area_metric = None
    if study_area_wgs84 is not None and not geometry_is_2d(study_area_wgs84):
        print("  [geo] cached WGS84 study area geometry has a Z dimension - rebuilding geometry cache")
        study_area_wgs84 = None
    if study_area_metric is not None and study_area_wgs84 is None:
        print("  [geo] cached WGS84 study area geometry missing - rebuilding geometry cache")

    if study_area_metric is not None and study_area_wgs84 is None:
        mark_building(cache_dir, "geo", geo_hash, "geometry")
        study_area_wgs84 = study_area_wgs84_from_metric(study_area_metric)
        cache_save("study_area_wgs84", study_area_wgs84, cache_dir)
        if can_finalize_geo_tier(study_area_metric, study_area_wgs84):
            mark_complete(cache_dir, "geo", geo_hash, "geometry")
        tracker.finish_phase("geometry", "completed", detail="repaired WGS84 study area geometry")
    elif study_area_metric is None or study_area_wgs84 is None:
        mark_building(cache_dir, "geo", geo_hash, "geometry")
        study_area_metric, study_area_wgs84 = load_study_area_geometries(
            progress_cb=tracker.phase_callback("geometry"),
        )
        cache_save("study_area_metric", study_area_metric, cache_dir)
        cache_save("study_area_wgs84", study_area_wgs84, cache_dir)
        if can_finalize_geo_tier(study_area_metric, study_area_wgs84):
            mark_complete(cache_dir, "geo", geo_hash, "geometry")
        tracker.finish_phase("geometry", "completed", detail="computed study area geometry")
    else:
        tracker.finish_phase("geometry", "cached", detail="geometry cache hit")
    return study_area_metric, study_area_wgs84


def phase_amenities_impl(
    engine,
    study_area_wgs84,
    tracker,
    *,
    tags: list[str],
    cache_dir,
    reach_hash: str,
    import_fingerprint: str,
    cache_load,
    cache_save,
    mark_building,
    mark_complete,
    can_finalize_reach_tier,
    load_source_amenity_rows,
) -> tuple[dict[str, list[tuple[float, float]]], list[dict[str, Any]]]:
    tracker.start_phase(
        "amenities",
        total_units=len(tags),
        rebuild_total_units=len(tags),
        unit_label="categories",
        detail="checking amenity cache",
    )
    amenity_source_rows = cache_load("amenities", cache_dir)
    if amenity_source_rows is not None:
        amenity_data = {category: [] for category in tags}
        for row in amenity_source_rows:
            amenity_data.setdefault(row["category"], []).append((row["lat"], row["lon"]))
        total = len(amenity_source_rows)
        tracker.credit_phase(
            "amenities",
            len(tags),
            detail=f"{total:,} cached features",
            force_log=True,
        )
        tracker.finish_phase("amenities", "cached", detail=f"{total:,} features")
        return amenity_data, amenity_source_rows

    mark_building(cache_dir, "reach", reach_hash, "amenities")
    amenity_rows = load_source_amenity_rows(engine, import_fingerprint, study_area_wgs84)
    amenity_source_rows = []
    amenity_data = {category: [] for category in tags}
    counts_by_category = {category: 0 for category in tags}
    for row in amenity_rows:
        lat = float(row["geom"].y)
        lon = float(row["geom"].x)
        amenity_data.setdefault(row["category"], []).append((lat, lon))
        counts_by_category[row["category"]] = counts_by_category.get(row["category"], 0) + 1
        amenity_source_rows.append(
            {
                "category": row["category"],
                "lat": lat,
                "lon": lon,
                "source_ref": row["source_ref"],
                "park_area_m2": _park_area_m2_from_row(row),
            }
        )
    for category in tags:
        tracker.advance_phase(
            "amenities",
            units=1,
            rebuild_units=1,
            detail=f"{category} ({counts_by_category.get(category, 0):,} features)",
        )
    cache_save("amenities", amenity_source_rows, cache_dir)
    if can_finalize_reach_tier(amenity_data):
        mark_complete(cache_dir, "reach", reach_hash, "amenities")
    total = len(amenity_source_rows)
    tracker.finish_phase("amenities", "completed", detail=f"{total:,} features")
    return amenity_data, amenity_source_rows


def phase_networks_impl(
    engine,
    tracker,
    *,
    source_state,
    study_area_wgs84,
    cache_dir: Path,
    geo_hash: str,
    tiers_building: set,
    mark_building,
    mark_complete,
    graph_meta_matches,
    load_walk_graph_index,
    run_walkgraph_build,
    walkgraph_bin: str,
    bbox_padding_m: float,
):
    del engine
    tracker.start_phase(
        "networks",
        total_units=1,
        rebuild_total_units=0,
        unit_label="networks",
        detail="checking walk graph cache",
    )

    graph_dir = cache_dir / "walk_graph"
    bbox = _graph_bbox(study_area_wgs84)
    cache_hit = graph_meta_matches(
        graph_dir,
        extract_fingerprint=source_state.extract_fingerprint,
        bbox=bbox,
        bbox_padding_m=bbox_padding_m,
    )

    if not cache_hit:
        if cache_dir not in tiers_building:
            mark_building(cache_dir, "geo", geo_hash, "walk_graph")
        tracker.set_live_work("networks", detail="building walk graph")
        started_at = time.perf_counter()
        run_walkgraph_build(
            source_state.extract_path,
            graph_dir,
            walkgraph_bin=walkgraph_bin,
            bbox=bbox,
            bbox_padding_m=bbox_padding_m,
            extract_fingerprint=source_state.extract_fingerprint,
            progress_cb=tracker.phase_callback("networks"),
        )
        _record_substep(tracker, "networks", "walkgraph_build", started_at, force_log=True)

    graph = load_walk_graph_index(graph_dir)
    tracker.credit_phase(
        "networks",
        1,
        detail=f"walk graph ({graph.vcount():,} nodes, {graph.ecount():,} edges)",
        force_log=True,
    )
    mark_complete(cache_dir, "geo", geo_hash, "walk_graph")
    tracker.finish_phase(
        "networks",
        "cached" if cache_hit else "completed",
        detail=f"walk graph ready ({graph.vcount():,} nodes)",
    )
    return graph


def phase_reachability_impl(
    walk_graph,
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
    tracker,
    *,
    walk_origin_node_ids,
    cache_dir,
    reach_hash: str,
    tiers_building: set,
    walk_radius_m: float,
    cache_load,
    cache_save,
    cache_load_large,
    cache_save_large,
    mark_building,
    mark_complete,
    snap_amenities,
    normalize_origin_node_ids,
    precompute_walk_counts_by_origin_node,
    precompute_walk_weighted_totals_by_origin_node,
):
    if walk_origin_node_ids is None:
        raise ValueError("walk_origin_node_ids is required for walk reachability")

    requested_walk_origin_nodes = normalize_origin_node_ids(walk_origin_node_ids)
    tracker.start_phase(
        "reachability",
        total_units=walk_graph.vcount(),
        rebuild_total_units=0,
        unit_label="walk origins",
        detail="checking walk reachability cache",
    )

    built_any = False

    walk_nodes_by_category = cache_load("walk_nodes_by_cat", cache_dir)
    if walk_nodes_by_category is None:
        built_any = True
        if cache_dir not in tiers_building:
            mark_building(cache_dir, "reach", reach_hash, "walk_nodes")
        started_at = time.perf_counter()
        walk_nodes_by_category = snap_amenities(walk_graph, amenity_data)
        _record_substep(tracker, "reachability", "walk_amenity_snap", started_at, force_log=True)
        cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)

    walk_counts_by_node = cache_load_large("walk_counts_by_origin_node", cache_dir)
    if walk_counts_by_node is None:
        walk_counts_by_node = {}
    walk_park_area_units_by_node = cache_load_large("walk_park_area_units_by_origin_node", cache_dir)
    if walk_park_area_units_by_node is None:
        walk_park_area_units_by_node = {}

    missing_count_nodes = tuple(
        node for node in requested_walk_origin_nodes if node not in walk_counts_by_node
    )
    missing_park_area_nodes = tuple(
        node for node in requested_walk_origin_nodes if node not in walk_park_area_units_by_node
    )
    missing_origin_nodes = tuple(
        node for node in requested_walk_origin_nodes if node not in walk_counts_by_node
        or node not in walk_park_area_units_by_node
    )

    if missing_origin_nodes:
        built_any = True
        if cache_dir not in tiers_building:
            mark_building(cache_dir, "reach", reach_hash, "walk_reachability")

    tracker.set_phase_totals(
        "reachability",
        total_units=len(requested_walk_origin_nodes),
        rebuild_total_units=len(missing_origin_nodes),
        unit_label="walk origins",
        detail="walk reachability",
        force_log=True,
    )

    cached_origin_count = len(requested_walk_origin_nodes) - len(missing_origin_nodes)
    if cached_origin_count > 0:
        tracker.credit_phase(
            "reachability",
            cached_origin_count,
            detail=f"walk cached ({cached_origin_count:,} origins)",
            force_log=True,
        )

    if missing_origin_nodes:
        counts_cache_save_seconds = 0.0
        park_cache_save_seconds = 0.0

        if missing_count_nodes:
            routing_started_at = time.perf_counter()

            def _checkpoint_save_counts(chunk_counts: dict[int, dict[str, int]]) -> None:
                nonlocal walk_counts_by_node, counts_cache_save_seconds
                walk_counts_by_node = _merge_counts_lookup(walk_counts_by_node, chunk_counts)
                save_started_at = time.perf_counter()
                cache_save_large("walk_counts_by_origin_node", walk_counts_by_node, cache_dir)
                counts_cache_save_seconds += max(time.perf_counter() - save_started_at, 0.0)

            new_walk_counts = precompute_walk_counts_by_origin_node(
                walk_graph,
                walk_nodes_by_category,
                missing_origin_nodes,
                cutoff=walk_radius_m,
                weight="length_m",
                progress_cb=tracker.phase_callback("reachability"),
                detail="walk origins",
                save_chunk_cb=_checkpoint_save_counts,
            )
            _record_substep(
                tracker,
                "reachability",
                "walk_routing",
                routing_started_at,
                force_log=True,
            )
            walk_counts_by_node = _merge_counts_lookup(walk_counts_by_node, new_walk_counts)
            if counts_cache_save_seconds <= 0.0:
                save_started_at = time.perf_counter()
                cache_save_large("walk_counts_by_origin_node", walk_counts_by_node, cache_dir)
                counts_cache_save_seconds = max(time.perf_counter() - save_started_at, 0.0)
            tracker.record_substep(
                "reachability",
                "walk_cache_save",
                counts_cache_save_seconds,
                force_log=True,
            )

        if missing_park_area_nodes:
            park_weight_rows = _park_node_weight_rows(
                amenity_source_rows,
                walk_nodes_by_category.get(_PARK_CATEGORY, []),
            )
            if park_weight_rows.get(_PARK_CATEGORY):
                park_routing_started_at = time.perf_counter()

                def _checkpoint_save_park(chunk_totals: dict[int, dict[str, int]]) -> None:
                    nonlocal walk_park_area_units_by_node, park_cache_save_seconds
                    chunk_units = _park_area_units_lookup(chunk_totals)
                    walk_park_area_units_by_node = _merge_numeric_lookup(
                        walk_park_area_units_by_node,
                        chunk_units,
                    )
                    save_started_at = time.perf_counter()
                    cache_save_large(
                        "walk_park_area_units_by_origin_node",
                        walk_park_area_units_by_node,
                        cache_dir,
                    )
                    park_cache_save_seconds += max(time.perf_counter() - save_started_at, 0.0)

                park_progress_cb = None
                if not missing_count_nodes:
                    park_progress_cb = tracker.phase_callback("reachability")

                park_totals_by_node = precompute_walk_weighted_totals_by_origin_node(
                    walk_graph,
                    park_weight_rows,
                    missing_origin_nodes,
                    cutoff=walk_radius_m,
                    weight="length_m",
                    progress_cb=park_progress_cb,
                    detail="walk origins",
                    save_chunk_cb=_checkpoint_save_park,
                )
                _record_substep(
                    tracker,
                    "reachability",
                    "walk_park_area_routing",
                    park_routing_started_at,
                    force_log=True,
                )
                walk_park_area_units_by_node = _merge_numeric_lookup(
                    walk_park_area_units_by_node,
                    _park_area_units_lookup(park_totals_by_node),
                )
            else:
                walk_park_area_units_by_node = _merge_numeric_lookup(
                    walk_park_area_units_by_node,
                    {node: 0.0 for node in missing_origin_nodes},
                )

            if park_cache_save_seconds <= 0.0:
                save_started_at = time.perf_counter()
                cache_save_large(
                    "walk_park_area_units_by_origin_node",
                    walk_park_area_units_by_node,
                    cache_dir,
                )
                park_cache_save_seconds = max(time.perf_counter() - save_started_at, 0.0)
            tracker.record_substep(
                "reachability",
                "walk_park_area_cache_save",
                park_cache_save_seconds,
                force_log=True,
            )

    if built_any:
        mark_complete(cache_dir, "reach", reach_hash, "reachability")
    tracker.finish_phase(
        "reachability",
        "completed" if built_any else "cached",
        detail=f"{len(requested_walk_origin_nodes):,} walk origins",
    )

    return walk_nodes_by_category, walk_counts_by_node, walk_park_area_units_by_node


def phase_grids_impl(
    engine,
    study_area_metric,
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]],
    tracker,
    *,
    grid_sizes_m: list[int],
    cache_dir,
    score_hash: str,
    tiers_building: set,
    cache_exists,
    cache_load,
    cache_save,
    mark_building,
    mark_complete,
    grid_cells_are_2d,
    phase_networks,
    phase_reachability,
    normalize_origin_node_ids,
    build_grid,
    elapsed,
    clone_grid_shells,
    snap_cells_to_nodes,
    score_cells,
    fine_surface_enabled: bool,
    reach_hash: str,
    surface_shell_hash: str,
    surface_shell_dir: Path,
    surface_score_dir: Path,
    ensure_surface_shell_cache,
    ensure_surface_score_cache,
    collect_surface_origin_nodes,
    surface_analysis_ready,
    graph_dir: Path,
    walkgraph_bin: str,
    surface_threads: int | None,
):
    total_steps = len(grid_sizes_m)
    if fine_surface_enabled:
        tracker.set_phase_expected("node_scores", True)
        tracker.set_phase_expected("fine_surface", True)
    else:
        tracker.skip_phase("node_scores", detail="fine raster surface disabled")
        tracker.skip_phase("fine_surface", detail="fine raster surface disabled")
    tracker.start_phase(
        "grids",
        total_units=total_steps,
        rebuild_total_units=0,
        unit_label="scoring steps",
        detail="checking walk grid caches",
    )

    cached_grids: dict[int, list[dict[str, Any]]] = {}
    sizes_to_rebuild: list[int] = []
    for size in grid_sizes_m:
        walk_key = f"walk_cells_{size}"
        if cache_exists(walk_key, cache_dir):
            walk_cached = cache_load(walk_key, cache_dir)
            if walk_cached is not None and grid_cells_are_2d(walk_cached):
                cached_grids[size] = walk_cached
                continue
            print(f"  [score] cached {size}m walk grid contains non-2D geometry - rebuilding")
        sizes_to_rebuild.append(size)

    surface_ready = False
    if fine_surface_enabled:
        surface_ready = surface_analysis_ready(
            surface_shell_dir,
            surface_score_dir,
            expected_surface_shell_hash=surface_shell_hash,
            expected_score_hash=score_hash,
        )

    if not sizes_to_rebuild and (not fine_surface_enabled or surface_ready):
        tracker.skip_phase("networks", detail="walk score grids already cached")
        tracker.skip_phase("reachability", detail="walk reachability already cached")
        if fine_surface_enabled:
            tracker.start_phase(
                "node_scores",
                total_units=0,
                rebuild_total_units=0,
                unit_label="artifacts",
                detail="checking node score cache",
            )
            tracker.finish_phase("node_scores", "cached", detail="fine surface score cache hit")
            tracker.start_phase(
                "fine_surface",
                total_units=0,
                rebuild_total_units=0,
                unit_label="shards",
                detail="checking fine surface shard cache",
            )
            tracker.finish_phase("fine_surface", "cached", detail="fine surface shell cache hit")
        tracker.credit_phase("grids", total_steps, detail="all walk grids cached", force_log=True)
        tracker.finish_phase("grids", "cached", detail="walk grid cache hit")
        return {size: cached_grids[size] for size in grid_sizes_m}

    tracker.set_phase_expected("networks", True)
    tracker.set_phase_expected("reachability", True)
    tracker.set_phase_totals(
        "grids",
        rebuild_total_units=len(sizes_to_rebuild),
        unit_label="walk grids",
        force_log=True,
    )

    walk_graph = phase_networks(engine, tracker)

    did_build_score = False

    def ensure_score_building(phase_name: str) -> None:
        nonlocal did_build_score
        if did_build_score:
            return
        mark_building(cache_dir, "score", score_hash, phase_name)
        did_build_score = True

    grid_cells_by_size: dict[int, list[dict[str, Any]]] = dict(cached_grids)
    walk_cell_nodes_by_size: dict[int, list[int]] = {}
    walk_origin_nodes: list[int] = []
    surface_origin_nodes: list[int] = []

    needs_walk_origin_nodes = bool(sizes_to_rebuild) or fine_surface_enabled
    if needs_walk_origin_nodes:
        for size in grid_sizes_m:
            if size not in grid_cells_by_size:
                grid_cells, built_grid_cells = _load_or_build_grid_cells(
                    size,
                    study_area_metric,
                    tracker=tracker,
                    cache_dir=cache_dir,
                    cache_load=cache_load,
                    cache_save=cache_save,
                    grid_cells_are_2d=grid_cells_are_2d,
                    build_grid=build_grid,
                    elapsed=elapsed,
                )
                if built_grid_cells:
                    ensure_score_building(f"grid_shell_{size}")
                grid_cells_by_size[size] = grid_cells
            tracker.set_live_work("grids", detail=f"{size}m walk node snap")
            started_at = time.perf_counter()
            walk_cell_nodes_by_size[size] = snap_cells_to_nodes(
                walk_graph,
                grid_cells_by_size[size],
                f"walk_cell_nodes_{size}",
                cache_dir,
            )
            _record_substep(tracker, "grids", "walk_snapping", started_at, force_log=True)

        walk_origin_nodes, built_walk_origin_nodes = _load_or_build_walk_origin_nodes(
            grid_sizes_m,
            walk_cell_nodes_by_size,
            cache_dir=cache_dir,
            cache_load=cache_load,
            cache_save=cache_save,
            normalize_origin_node_ids=normalize_origin_node_ids,
        )
        if built_walk_origin_nodes:
            ensure_score_building(f"walk_origins_{_grid_size_signature(grid_sizes_m)}")

    if fine_surface_enabled:
        ensure_surface_shell_cache(
            shell_dir=surface_shell_dir,
            surface_shell_hash=surface_shell_hash,
            reach_hash=reach_hash,
            study_area_metric=study_area_metric,
            graph_dir=graph_dir,
            walkgraph_bin=walkgraph_bin,
            node_count=walk_graph.vcount(),
            threads=surface_threads,
            tracker=tracker,
        )
        surface_origin_nodes = collect_surface_origin_nodes(surface_shell_dir)

    reachability_origin_nodes = list(
        normalize_origin_node_ids(list(walk_origin_nodes) + list(surface_origin_nodes))
    )

    _, walk_counts_by_node, walk_park_area_units_by_node = phase_reachability(
        walk_graph,
        amenity_data,
        amenity_source_rows,
        tracker,
        walk_origin_node_ids=reachability_origin_nodes,
    )

    if fine_surface_enabled:
        ensure_surface_score_cache(
            shell_dir=surface_shell_dir,
            score_dir=surface_score_dir,
            surface_shell_hash=surface_shell_hash,
            score_hash=score_hash,
            walk_graph=walk_graph,
            walk_counts_by_node=walk_counts_by_node,
            walk_park_area_units_by_node=walk_park_area_units_by_node,
            tracker=tracker,
        )

    if not sizes_to_rebuild:
        tracker.credit_phase("grids", total_steps, detail="all walk grids cached", force_log=True)
        tracker.finish_phase("grids", "cached", detail="walk grid cache hit")
        return {size: cached_grids[size] for size in grid_sizes_m}

    walk_grids: dict[int, list[dict[str, Any]]] = {}
    for size in grid_sizes_m:
        if size in cached_grids:
            walk_grids[size] = cached_grids[size]
            tracker.credit_phase(
                "grids",
                1,
                detail=f"{size}m cached ({len(cached_grids[size]):,} cells)",
                force_log=True,
            )
            continue

        grid_cells = grid_cells_by_size[size]
        walk_cells = clone_grid_shells(grid_cells)
        walk_key = f"walk_cells_{size}"

        tracker.set_live_work("grids", detail=f"{size}m walk scoring")
        started_at = time.perf_counter()
        print("           walk scoring  ...", end=" ", flush=True)
        score_cells(
            walk_cells,
            walk_counts_by_node,
            walk_cell_nodes_by_size[size],
            walk_park_area_units_by_node,
        )
        walk_scores = [cell["total"] for cell in walk_cells]
        print(f"{_score_summary(walk_scores)} {elapsed(started_at)}")
        _record_substep(tracker, "grids", "walk_scoring", started_at, force_log=True)
        ensure_score_building(f"grid_{size}")
        cache_save(walk_key, walk_cells, cache_dir)
        walk_grids[size] = walk_cells
        tracker.advance_phase(
            "grids",
            units=1,
            rebuild_units=1,
            detail=f"{size}m walk scored",
            force_log=True,
        )

    if did_build_score:
        mark_complete(cache_dir, "score", score_hash, "grids")

    tracker.finish_phase("grids", "completed", detail="walk grid scoring ready")
    return walk_grids
