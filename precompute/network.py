from __future__ import annotations

from array import array
from collections.abc import Callable, Iterable, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
import heapq
import shutil
import uuid
import math

try:
    import igraph as ig
except ImportError:  # pragma: no cover - depends on local environment
    ig = None

import numpy as np
from sklearn.neighbors import BallTree

from config import PROJECT_TEMP_DIR, TAGS, WALKGRAPH_BIN
from network.loader import WalkGraphIndex, run_walkgraph_reachability


RUST_REACHABILITY_CHUNK_SIZE = 250_000
U32_DTYPE = np.dtype("<u4")


def _require_igraph():
    if ig is None:  # pragma: no cover - exercised when dependency is missing
        raise RuntimeError(
            "python-igraph is required for walk reachability fallback. "
            "Install requirements.txt before running --precompute."
        )
    return ig


def _count_unique_source_nodes(nodes_by_category: dict[str, list[int]]) -> int:
    return len({node for nodes in nodes_by_category.values() for node in nodes})


def _ordered_categories(
    categories: set[str] | list[str] | tuple[str, ...] | dict[str, Any],
) -> list[str]:
    category_set = {str(category) for category in categories}
    tag_order = [category for category in TAGS if category in category_set]
    extras = sorted(category for category in category_set if category not in TAGS)
    return tag_order + extras


def _graph_attr_names(graph) -> set[str]:
    attributes = getattr(graph, "attributes", None)
    if not callable(attributes):
        return set()
    try:
        return {str(name) for name in attributes()}
    except Exception:  # pragma: no cover - defensive for mocked graph objects
        return set()


def _vertex_coordinate_arrays(graph) -> tuple[np.ndarray, np.ndarray]:
    if "_vertex_coord_cache" in _graph_attr_names(graph):
        return graph["_vertex_coord_cache"]

    attrs = _graph_attr_names(graph)
    if "_node_latitudes" in attrs and "_node_longitudes" in attrs:
        latitudes = np.asarray(graph["_node_latitudes"], dtype=np.float64)
        longitudes = np.asarray(graph["_node_longitudes"], dtype=np.float64)
    else:
        latitudes = np.asarray(graph.vs["lat"], dtype=np.float64)
        longitudes = np.asarray(graph.vs["lon"], dtype=np.float64)
    cache = (latitudes, longitudes)
    graph["_vertex_coord_cache"] = cache
    return cache


def _nearest_node_index(graph) -> BallTree:
    if "_nearest_node_index" in _graph_attr_names(graph):
        return graph["_nearest_node_index"]

    latitudes, longitudes = _vertex_coordinate_arrays(graph)
    if latitudes.size == 0:
        raise RuntimeError("Cannot snap to an empty walk graph.")
    coords = np.radians(np.column_stack([latitudes, longitudes]))
    tree = BallTree(coords, metric="haversine")
    graph["_nearest_node_index"] = tree
    return tree


def nearest_nodes(graph, lons: list[float], lats: list[float]) -> list[int]:
    tree = _nearest_node_index(graph)
    query = np.radians(np.column_stack([lats, lons]))
    _, indexes = tree.query(query, k=1)
    return indexes.astype(int).ravel().tolist()


def snap_amenities(
    graph,
    amenity_data: dict[str, list[tuple[float, float]]],
) -> dict[str, list[int]]:
    nodes_by_category: dict[str, list[int]] = {}
    for category, points in amenity_data.items():
        if not points:
            nodes_by_category[category] = []
            continue
        lats, lons = zip(*points)
        nodes_by_category[category] = nearest_nodes(graph, list(lons), list(lats))
    return nodes_by_category


def _fast_path_normalized_origin_node_ids(
    origin_node_ids: Iterable[int],
) -> list[int] | None:
    if not isinstance(origin_node_ids, list):
        return None
    previous: int | None = None
    for index, node in enumerate(origin_node_ids):
        if type(node) is not int:
            return None
        if index and node <= previous:
            return None
        previous = node
    return origin_node_ids


def normalize_origin_node_ids(origin_node_ids: Iterable[int]) -> list[int]:
    fast_path = _fast_path_normalized_origin_node_ids(origin_node_ids)
    if fast_path is not None:
        return fast_path

    node_buffer = array("q")
    for node in origin_node_ids:
        if node is None:
            continue
        node_buffer.append(int(node))
    if not node_buffer:
        return []
    node_array = np.frombuffer(node_buffer, dtype=np.int64, count=len(node_buffer))
    return np.unique(node_array).tolist()


def merge_normalized_origin_node_ids(*origin_node_sequences: Sequence[int]) -> list[int]:
    merged_nodes: list[int] = []
    previous: int | None = None
    for node in heapq.merge(*(sequence for sequence in origin_node_sequences if sequence)):
        value = int(node)
        if previous is None or value != previous:
            merged_nodes.append(value)
            previous = value
    return merged_nodes


def _normalize_category_counts(category_counts: dict[str, int]) -> dict[str, int]:
    return {
        category: int(category_counts.get(category, 0))
        for category in _ordered_categories(category_counts.keys())
        if int(category_counts.get(category, 0)) > 0
    }


def _counts_from_vector(
    values: Sequence[int] | np.ndarray,
    categories: Sequence[str],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for index, category in enumerate(categories):
        value = int(values[index])
        if value > 0:
            counts[category] = value
    return counts


def _category_weight_rows_from_nodes(
    nodes_by_category: dict[str, list[int]],
) -> dict[str, list[tuple[int, int]]]:
    return {
        category: [(int(node), 1) for node in nodes]
        for category, nodes in nodes_by_category.items()
    }


def _node_weight_matrix(
    node_weights_by_category: dict[str, list[tuple[int, int]]],
) -> tuple[list[int], list[str], np.ndarray]:
    unique_amenity_nodes = normalize_origin_node_ids(
        node
        for node_weights in node_weights_by_category.values()
        for node, weight in node_weights
        if int(weight) > 0
    )
    categories = _ordered_categories(
        {
            category
            for category, node_weights in node_weights_by_category.items()
            if any(int(weight) > 0 for _, weight in node_weights)
        }
    )
    node_to_row = {node: index for index, node in enumerate(unique_amenity_nodes)}
    matrix = np.zeros((len(unique_amenity_nodes), len(categories)), dtype=U32_DTYPE)
    category_index = {category: index for index, category in enumerate(categories)}
    for category in categories:
        for node, weight in node_weights_by_category.get(category, []):
            integer_weight = int(weight)
            if integer_weight <= 0:
                continue
            matrix[node_to_row[int(node)], category_index[category]] += integer_weight
    return unique_amenity_nodes, categories, matrix


def _amenity_node_weights(
    nodes_by_category: dict[str, list[int]],
) -> tuple[list[int], list[str], np.ndarray]:
    return _node_weight_matrix(_category_weight_rows_from_nodes(nodes_by_category))


def _edge_weights(graph, weight: str | Sequence[float]) -> Sequence[float]:
    if isinstance(weight, str):
        if weight == "length_m" and "_edge_length_m" in _graph_attr_names(graph):
            return graph["_edge_length_m"]
        return graph.es[weight]
    return weight


def _routing_batch_size(node_count: int, request_count: int, target_count: int) -> int:
    if node_count <= 0 or request_count <= 0:
        return 1
    target_bytes = 64 * 1024 * 1024
    bytes_per_row = max(target_count, 1) * 8
    batch_size = max(target_bytes // bytes_per_row, 1)
    return max(1, min(request_count, int(batch_size), 256))


def _normalize_counts_by_node(
    counts_by_node: dict[int, dict[str, int]],
) -> dict[int, dict[str, int]]:
    for node, category_counts in counts_by_node.items():
        counts_by_node[node] = _normalize_category_counts(category_counts)
    return counts_by_node


def _normalize_units_by_node(
    units_by_node: dict[int, dict[str, float]],
) -> dict[int, dict[str, float]]:
    for node, category_units in units_by_node.items():
        units_by_node[node] = {
            str(category): float(value)
            for category, value in category_units.items()
            if float(value) > 0.0
        }
    return units_by_node


def _is_walkgraph_index(graph) -> bool:
    return isinstance(graph, WalkGraphIndex)


def _iter_origin_chunks(origin_nodes: Sequence[int], *, chunk_size: int) -> Iterable[list[int]]:
    for start in range(0, len(origin_nodes), chunk_size):
        yield [int(node) for node in origin_nodes[start : start + chunk_size]]


def _write_u32_array(path: Path, values: Sequence[int]) -> None:
    np.asarray(values, dtype=U32_DTYPE).tofile(path)


@contextmanager
def _temporary_directory():
    PROJECT_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    temp_dir = PROJECT_TEMP_DIR / f"walkreach_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _write_amenity_weight_records(
    path: Path,
    amenity_nodes: Sequence[int],
    amenity_weights: np.ndarray,
) -> None:
    category_count = int(amenity_weights.shape[1])
    payload = np.zeros((len(amenity_nodes), category_count + 1), dtype=U32_DTYPE)
    if amenity_nodes:
        payload[:, 0] = np.asarray(amenity_nodes, dtype=U32_DTYPE)
    if amenity_weights.size:
        payload[:, 1:] = np.asarray(amenity_weights, dtype=U32_DTYPE)
    payload.tofile(path)


def _read_reachability_output(
    path: Path,
    *,
    origin_count: int,
    category_count: int,
) -> np.ndarray:
    flat = np.fromfile(path, dtype=U32_DTYPE)
    expected_size = int(origin_count) * int(category_count)
    if flat.size != expected_size:
        raise RuntimeError(
            f"walkgraph reachability returned {flat.size} values, expected {expected_size}."
        )
    if origin_count == 0 or category_count == 0:
        return np.zeros((origin_count, category_count), dtype=np.uint32)
    return flat.reshape((origin_count, category_count))


def _read_reachability_output_f32(
    path: Path,
    *,
    origin_count: int,
    category_count: int,
) -> np.ndarray:
    flat = np.fromfile(path, dtype=np.dtype("<f4"))
    expected_size = int(origin_count) * int(category_count)
    if flat.size != expected_size:
        raise RuntimeError(
            f"walkgraph reachability returned {flat.size} float values, expected {expected_size}."
        )
    if origin_count == 0 or category_count == 0:
        return np.zeros((origin_count, category_count), dtype=np.float32)
    return flat.reshape((origin_count, category_count))


def _chunk_counts_from_matrix(
    origin_nodes: Sequence[int],
    matrix: np.ndarray,
    categories: Sequence[str],
) -> dict[int, dict[str, int]]:
    counts_by_node: dict[int, dict[str, int]] = {}
    for row_index, node in enumerate(origin_nodes):
        counts_by_node[int(node)] = _counts_from_vector(matrix[row_index], categories)
    return counts_by_node


def _units_from_vector(
    values: Sequence[float] | np.ndarray,
    categories: Sequence[str],
) -> dict[str, float]:
    units: dict[str, float] = {}
    for index, category in enumerate(categories):
        value = float(values[index])
        if value > 0.0:
            units[str(category)] = value
    return units


def _chunk_units_from_matrix(
    origin_nodes: Sequence[int],
    matrix: np.ndarray,
    categories: Sequence[str],
) -> dict[int, dict[str, float]]:
    units_by_node: dict[int, dict[str, float]] = {}
    for row_index, node in enumerate(origin_nodes):
        units_by_node[int(node)] = _units_from_vector(matrix[row_index], categories)
    return units_by_node


def _rust_reachability_chunk(
    graph: WalkGraphIndex,
    origin_nodes: Sequence[int],
    amenity_nodes: Sequence[int],
    amenity_weights: np.ndarray,
    categories: Sequence[str],
    *,
    cutoff: float,
    progress_cb=None,
) -> dict[int, dict[str, int]]:
    with _temporary_directory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        origins_path = temp_dir / "origins.bin"
        weights_path = temp_dir / "amenity_weights.bin"
        output_path = temp_dir / "counts.bin"
        _write_u32_array(origins_path, origin_nodes)
        _write_amenity_weight_records(weights_path, amenity_nodes, amenity_weights)
        run_walkgraph_reachability(
            graph.graph_dir,
            origins_path,
            weights_path,
            output_path,
            category_count=len(categories),
            cutoff_m=cutoff,
            walkgraph_bin=WALKGRAPH_BIN,
            progress_cb=progress_cb,
        )
        count_matrix = _read_reachability_output(
            output_path,
            origin_count=len(origin_nodes),
            category_count=len(categories),
        )
    return _chunk_counts_from_matrix(origin_nodes, count_matrix, categories)


def _rust_decayed_units_chunk(
    graph: WalkGraphIndex,
    origin_nodes: Sequence[int],
    amenity_nodes: Sequence[int],
    amenity_weights: np.ndarray,
    categories: Sequence[str],
    *,
    cutoff: float,
    half_distances_m: dict[str, float],
    progress_cb=None,
) -> dict[int, dict[str, float]]:
    with _temporary_directory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        origins_path = temp_dir / "origins.bin"
        weights_path = temp_dir / "amenity_weights.bin"
        output_path = temp_dir / "effective_units.bin"
        _write_u32_array(origins_path, origin_nodes)
        _write_amenity_weight_records(weights_path, amenity_nodes, amenity_weights)
        half_distances = [
            float(half_distances_m.get(str(category), 0.0))
            for category in categories
        ]
        run_walkgraph_reachability(
            graph.graph_dir,
            origins_path,
            weights_path,
            output_path,
            category_count=len(categories),
            cutoff_m=cutoff,
            walkgraph_bin=WALKGRAPH_BIN,
            output_mode="decayed-units",
            half_distances_m=half_distances,
            progress_cb=progress_cb,
        )
        effective_matrix = _read_reachability_output_f32(
            output_path,
            origin_count=len(origin_nodes),
            category_count=len(categories),
        )
    return _chunk_units_from_matrix(origin_nodes, effective_matrix, categories)


def _python_reachability_chunk(
    graph,
    origin_nodes: Sequence[int],
    amenity_nodes: Sequence[int],
    amenity_weights: np.ndarray,
    categories: Sequence[str],
    *,
    cutoff: float,
    weight: str | Sequence[float],
) -> dict[int, dict[str, int]]:
    edge_weights = _edge_weights(graph, weight)
    distances = np.asarray(
        graph.distances(
            source=list(origin_nodes),
            target=amenity_nodes,
            weights=edge_weights,
            mode="out",
        ),
        dtype=np.float64,
    )
    if distances.ndim == 1:
        distances = np.asarray([distances], dtype=np.float64)
    reachable = np.isfinite(distances) & (distances <= float(cutoff))
    count_matrix = reachable.astype(np.int8, copy=False) @ amenity_weights
    return _chunk_counts_from_matrix(origin_nodes, count_matrix, categories)


def _python_decayed_units_chunk(
    graph,
    origin_nodes: Sequence[int],
    amenity_nodes: Sequence[int],
    amenity_weights: np.ndarray,
    categories: Sequence[str],
    *,
    cutoff: float,
    weight: str | Sequence[float],
    half_distances_m: dict[str, float],
) -> dict[int, dict[str, float]]:
    edge_weights = _edge_weights(graph, weight)
    distances = np.asarray(
        graph.distances(
            source=list(origin_nodes),
            target=amenity_nodes,
            weights=edge_weights,
            mode="out",
        ),
        dtype=np.float64,
    )
    if distances.ndim == 1:
        distances = np.asarray([distances], dtype=np.float64)
    reachable = np.isfinite(distances) & (distances <= float(cutoff))
    totals = np.zeros((len(origin_nodes), len(categories)), dtype=np.float32)
    for category_index, category in enumerate(categories):
        base_units = amenity_weights[:, category_index].astype(np.float64, copy=False)
        if not np.any(base_units):
            continue
        half_distance = float(half_distances_m.get(str(category), 0.0))
        if not math.isfinite(half_distance) or half_distance <= 0.0:
            raise ValueError(f"Invalid half-distance for category={category!r}: {half_distance!r}")
        decay = np.where(
            reachable,
            np.power(0.5, distances / half_distance),
            0.0,
        )
        totals[:, category_index] = np.asarray(
            decay * base_units.reshape(1, -1),
            dtype=np.float64,
        ).sum(axis=1, dtype=np.float64).astype(np.float32)
    return _chunk_units_from_matrix(origin_nodes, totals, categories)


def _walkgraph_chunk_size(origin_count: int) -> int:
    return max(1, min(int(origin_count), RUST_REACHABILITY_CHUNK_SIZE))


def precompute_walk_weighted_totals_by_origin_node(
    graph,
    node_weights_by_category: dict[str, list[tuple[int, int]]],
    origin_node_ids: Iterable[int],
    cutoff: float,
    weight: str | Sequence[float] = "length_m",
    progress_cb=None,
    detail: str | None = None,
    save_chunk_cb: Callable[[dict[int, dict[str, int]]], None] | None = None,
) -> dict[int, dict[str, int]]:
    origin_nodes = normalize_origin_node_ids(origin_node_ids)
    if progress_cb is not None:
        progress_cb("live_start", total_units=len(origin_nodes), detail=detail)
    if not origin_nodes:
        return {}

    amenity_nodes, categories, amenity_weights = _node_weight_matrix(node_weights_by_category)
    if not amenity_nodes:
        if progress_cb is not None:
            progress_cb("advance", units=len(origin_nodes), detail=detail)
        if save_chunk_cb is not None:
            empty = {node: {} for node in origin_nodes}
            if empty:
                save_chunk_cb(empty)
            return {}
        return {node: {} for node in origin_nodes}

    accumulate = save_chunk_cb is None
    counts_by_node: dict[int, dict[str, int]] = {}

    if _is_walkgraph_index(graph):
        chunk_size = _walkgraph_chunk_size(len(origin_nodes))
        for origin_chunk in _iter_origin_chunks(origin_nodes, chunk_size=chunk_size):
            chunk_counts = _rust_reachability_chunk(
                graph,
                origin_chunk,
                amenity_nodes,
                amenity_weights,
                categories,
                cutoff=cutoff,
                progress_cb=progress_cb,
            )
            if accumulate:
                counts_by_node.update(chunk_counts)
            elif chunk_counts:
                save_chunk_cb(chunk_counts)
            if progress_cb is not None:
                progress_cb("advance", units=len(origin_chunk), detail=detail)
        return _normalize_counts_by_node(counts_by_node)

    _require_igraph()
    chunk_size = _routing_batch_size(graph.vcount(), len(origin_nodes), len(amenity_nodes))
    for origin_chunk in _iter_origin_chunks(origin_nodes, chunk_size=chunk_size):
        chunk_counts = _python_reachability_chunk(
            graph,
            origin_chunk,
            amenity_nodes,
            amenity_weights,
            categories,
            cutoff=cutoff,
            weight=weight,
        )
        if accumulate:
            counts_by_node.update(chunk_counts)
        elif chunk_counts:
            save_chunk_cb(chunk_counts)
        if progress_cb is not None:
            progress_cb("advance", units=len(origin_chunk), detail=detail)

    return _normalize_counts_by_node(counts_by_node)


def precompute_walk_counts_by_origin_node(
    graph,
    nodes_by_category: dict[str, list[int]],
    origin_node_ids: Iterable[int],
    cutoff: float,
    weight: str | Sequence[float] = "length_m",
    progress_cb=None,
    detail: str | None = None,
    save_chunk_cb: Callable[[dict[int, dict[str, int]]], None] | None = None,
) -> dict[int, dict[str, int]]:
    return precompute_walk_weighted_totals_by_origin_node(
        graph,
        _category_weight_rows_from_nodes(nodes_by_category),
        origin_node_ids,
        cutoff,
        weight=weight,
        progress_cb=progress_cb,
        detail=detail,
        save_chunk_cb=save_chunk_cb,
    )


def precompute_walk_decayed_units_by_origin_node(
    graph,
    node_weights_by_category: dict[str, list[tuple[int, int]]],
    origin_node_ids: Iterable[int],
    cutoff: float,
    half_distance_m_by_category: dict[str, float],
    weight: str | Sequence[float] = "length_m",
    progress_cb=None,
    detail: str | None = None,
    save_chunk_cb: Callable[[dict[int, dict[str, float]]], None] | None = None,
) -> dict[int, dict[str, float]]:
    origin_nodes = normalize_origin_node_ids(origin_node_ids)
    if progress_cb is not None:
        progress_cb("live_start", total_units=len(origin_nodes), detail=detail)
    if not origin_nodes:
        return {}

    amenity_nodes, categories, amenity_weights = _node_weight_matrix(node_weights_by_category)
    if not amenity_nodes:
        if progress_cb is not None:
            progress_cb("advance", units=len(origin_nodes), detail=detail)
        if save_chunk_cb is not None:
            empty = {node: {} for node in origin_nodes}
            if empty:
                save_chunk_cb(empty)
            return {}
        return {node: {} for node in origin_nodes}

    accumulate = save_chunk_cb is None
    decayed_units_by_node: dict[int, dict[str, float]] = {}

    if _is_walkgraph_index(graph):
        chunk_size = _walkgraph_chunk_size(len(origin_nodes))
        for origin_chunk in _iter_origin_chunks(origin_nodes, chunk_size=chunk_size):
            chunk_units = _rust_decayed_units_chunk(
                graph,
                origin_chunk,
                amenity_nodes,
                amenity_weights,
                categories,
                cutoff=cutoff,
                half_distances_m=half_distance_m_by_category,
                progress_cb=progress_cb,
            )
            if accumulate:
                decayed_units_by_node.update(chunk_units)
            elif chunk_units:
                save_chunk_cb(chunk_units)
            if progress_cb is not None:
                progress_cb("advance", units=len(origin_chunk), detail=detail)
        return _normalize_units_by_node(decayed_units_by_node)

    _require_igraph()
    chunk_size = _routing_batch_size(graph.vcount(), len(origin_nodes), len(amenity_nodes))
    for origin_chunk in _iter_origin_chunks(origin_nodes, chunk_size=chunk_size):
        chunk_units = _python_decayed_units_chunk(
            graph,
            origin_chunk,
            amenity_nodes,
            amenity_weights,
            categories,
            cutoff=cutoff,
            weight=weight,
            half_distances_m=half_distance_m_by_category,
        )
        if accumulate:
            decayed_units_by_node.update(chunk_units)
        elif chunk_units:
            save_chunk_cb(chunk_units)
        if progress_cb is not None:
            progress_cb("advance", units=len(origin_chunk), detail=detail)
    return _normalize_units_by_node(decayed_units_by_node)


def precompute_counts_by_node(
    graph,
    nodes_by_category: dict[str, list[int]],
    cutoff: float,
    weight: str | Sequence[float] = "length_m",
    progress_cb=None,
    detail: str | None = None,
    *,
    origin_node_ids: Iterable[int] | None = None,
) -> dict[int, dict[str, int]]:
    if origin_node_ids is None:
        origins = range(graph.vcount()) if _is_walkgraph_index(graph) else graph.vs.indices
    else:
        origins = origin_node_ids
    return precompute_walk_counts_by_origin_node(
        graph,
        nodes_by_category,
        origins,
        cutoff,
        weight=weight,
        progress_cb=progress_cb,
        detail=detail,
    )
