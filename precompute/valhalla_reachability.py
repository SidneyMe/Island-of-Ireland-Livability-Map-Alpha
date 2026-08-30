"""Valhalla-backed pedestrian reachability with walkgraph node identities.

The walkgraph remains the canonical source of snapped origin and target node
coordinates.  Valhalla supplies only the pedestrian network distance between
those already-snapped nodes.
"""

from __future__ import annotations

import concurrent.futures
import json
import math
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import numpy as np
from pyproj import Transformer
from shapely.geometry import box
from shapely.strtree import STRtree

from .network import _vertex_coordinate_arrays
from .reachability_arrays import ReachabilityMatrix, merge_reachability_matrices


CHECKPOINT_ORIGIN_COUNT = 256


class ValhallaReachabilityError(RuntimeError):
    """A configuration or infrastructure error from the Valhalla backend."""


@dataclass(frozen=True)
class ValhallaSettings:
    url: str
    expected_version: str
    workers: int
    timeout_s: float
    max_targets_per_request: int


@dataclass(frozen=True)
class TargetContributions:
    node_id: int
    raw_counts: dict[str, int]
    cluster_counts: dict[str, int]
    base_units: dict[str, int]


@dataclass(frozen=True)
class UnifiedTargetIndex:
    targets: tuple[TargetContributions, ...]
    latitudes: np.ndarray
    longitudes: np.ndarray
    metric_x: np.ndarray
    metric_y: np.ndarray
    tree: STRtree
    graph_latitudes: np.ndarray
    graph_longitudes: np.ndarray

    def candidates_for_origin(self, origin_node: int, radius_m: float) -> np.ndarray:
        node = int(origin_node)
        if node < 0 or node >= self.graph_latitudes.size:
            raise ValhallaReachabilityError(f"Walkgraph origin node is out of range: {node}")
        transformer = _metric_transformer()
        x, y = transformer.transform(
            float(self.graph_longitudes[node]), float(self.graph_latitudes[node])
        )
        candidate_indexes = np.asarray(
            self.tree.query(box(x - radius_m, y - radius_m, x + radius_m, y + radius_m)),
            dtype=np.intp,
        )
        if candidate_indexes.size == 0:
            return candidate_indexes
        dx = self.metric_x[candidate_indexes] - float(x)
        dy = self.metric_y[candidate_indexes] - float(y)
        return candidate_indexes[(dx * dx + dy * dy) <= float(radius_m) ** 2]


_TRANSFORMER: Transformer | None = None


def _metric_transformer() -> Transformer:
    global _TRANSFORMER
    if _TRANSFORMER is None:
        _TRANSFORMER = Transformer.from_crs("EPSG:4326", "EPSG:2157", always_xy=True)
    return _TRANSFORMER


def _positive_int_weights(rows: dict[str, list[tuple[int, int]]]) -> dict[int, dict[str, int]]:
    result: dict[int, dict[str, int]] = {}
    for category, weighted_nodes in rows.items():
        for node, weight in weighted_nodes:
            value = int(weight)
            if value > 0:
                result.setdefault(int(node), {}).setdefault(str(category), 0)
                result[int(node)][str(category)] += value
    return result


def _unit_node_weights(nodes_by_category: dict[str, list[int]]) -> dict[int, dict[str, int]]:
    result: dict[int, dict[str, int]] = {}
    for category, nodes in nodes_by_category.items():
        for node in nodes:
            result.setdefault(int(node), {}).setdefault(str(category), 0)
            result[int(node)][str(category)] += 1
    return result


def build_unified_target_index(
    graph,
    raw_nodes_by_category: dict[str, list[int]],
    cluster_nodes_by_category: dict[str, list[int]],
    base_unit_rows: dict[str, list[tuple[int, int]]],
) -> UnifiedTargetIndex:
    """Aggregate all three metrics by snapped target node and spatially index it."""
    raw_by_node = _unit_node_weights(raw_nodes_by_category)
    clusters_by_node = _unit_node_weights(cluster_nodes_by_category)
    units_by_node = _positive_int_weights(base_unit_rows)
    node_ids = sorted({*raw_by_node, *clusters_by_node, *units_by_node})
    graph_latitudes, graph_longitudes = _vertex_coordinate_arrays(graph)
    for node in node_ids:
        if node < 0 or node >= graph_latitudes.size:
            raise ValhallaReachabilityError(f"Walkgraph target node is out of range: {node}")

    latitudes = np.asarray([graph_latitudes[node] for node in node_ids], dtype=np.float64)
    longitudes = np.asarray([graph_longitudes[node] for node in node_ids], dtype=np.float64)
    metric_x, metric_y = _metric_transformer().transform(longitudes, latitudes)
    targets = tuple(
        TargetContributions(
            node_id=node,
            raw_counts=dict(raw_by_node.get(node, {})),
            cluster_counts=dict(clusters_by_node.get(node, {})),
            base_units=dict(units_by_node.get(node, {})),
        )
        for node in node_ids
    )
    points = np.empty(len(node_ids), dtype=object)
    if node_ids:
        from shapely import points as shapely_points

        points[:] = shapely_points(metric_x, metric_y)
    return UnifiedTargetIndex(
        targets=targets,
        latitudes=latitudes,
        longitudes=longitudes,
        metric_x=np.asarray(metric_x, dtype=np.float64),
        metric_y=np.asarray(metric_y, dtype=np.float64),
        tree=STRtree(points),
        graph_latitudes=np.asarray(graph_latitudes, dtype=np.float64),
        graph_longitudes=np.asarray(graph_longitudes, dtype=np.float64),
    )


def _endpoint_url(base_url: str, suffix: str) -> str:
    return f"{str(base_url).rstrip('/')}/{suffix.lstrip('/')}"


class ValhallaClient:
    def __init__(self, settings: ValhallaSettings) -> None:
        self.settings = settings
        self._ready = False
        self._ready_lock = threading.Lock()

    def _read_json(self, request: urllib.request.Request | str) -> dict:
        try:
            with urllib.request.urlopen(request, timeout=self.settings.timeout_s) as response:
                raw_body = response.read()
        except urllib.error.HTTPError as exc:
            raise ValhallaReachabilityError(
                f"Valhalla HTTP {exc.code} for {getattr(request, 'full_url', request)!s}."
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ValhallaReachabilityError(
                f"Valhalla request failed for {getattr(request, 'full_url', request)!s}: {exc}"
            ) from exc
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValhallaReachabilityError("Valhalla returned malformed JSON.") from exc
        if not isinstance(payload, dict):
            raise ValhallaReachabilityError("Valhalla returned a JSON value other than an object.")
        return payload

    def ensure_ready(self) -> None:
        if self._ready:
            return
        with self._ready_lock:
            if self._ready:
                return
            status = self._read_json(_endpoint_url(self.settings.url, "status"))
            version = status.get("version")
            if not isinstance(version, str) or not version.strip():
                raise ValhallaReachabilityError("Valhalla /status response has no valid version.")
            if version.strip() != self.settings.expected_version:
                raise ValhallaReachabilityError(
                    "Valhalla version mismatch: "
                    f"expected {self.settings.expected_version!r}, got {version.strip()!r}."
                )
            self._ready = True

    def route_distances_m(
        self,
        source_lat: float,
        source_lon: float,
        target_latitudes: Sequence[float],
        target_longitudes: Sequence[float],
    ) -> list[float | None]:
        if len(target_latitudes) != len(target_longitudes):
            raise ValueError("Valhalla target latitude/longitude lengths differ.")
        if not target_latitudes:
            return []
        self.ensure_ready()
        request_body = {
            "sources": [{"lat": float(source_lat), "lon": float(source_lon)}],
            "targets": [
                {"lat": float(lat), "lon": float(lon)}
                for lat, lon in zip(target_latitudes, target_longitudes)
            ],
            "costing": "pedestrian",
            "verbose": False,
            "units": "kilometers",
        }
        request = urllib.request.Request(
            _endpoint_url(self.settings.url, "sources_to_targets"),
            data=json.dumps(request_body, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        response = self._read_json(request)
        if response.get("units") != "kilometers":
            raise ValhallaReachabilityError(
                "Valhalla matrix response must declare units='kilometers'."
            )
        matrix = response.get("sources_to_targets")
        if not isinstance(matrix, dict):
            raise ValhallaReachabilityError("Valhalla matrix response lacks sources_to_targets.")
        distances = matrix.get("distances")
        if not isinstance(distances, list) or len(distances) != 1:
            raise ValhallaReachabilityError("Valhalla matrix response has an invalid distance row count.")
        row = distances[0]
        if not isinstance(row, list) or len(row) != len(target_latitudes):
            raise ValhallaReachabilityError("Valhalla matrix response has an invalid distance row.")
        parsed: list[float | None] = []
        for value in row:
            if value is None:
                parsed.append(None)
                continue
            if isinstance(value, bool):
                raise ValhallaReachabilityError("Valhalla matrix distance is not numeric.")
            try:
                distance_km = float(value)
            except (TypeError, ValueError) as exc:
                raise ValhallaReachabilityError("Valhalla matrix distance is not numeric.") from exc
            if not math.isfinite(distance_km) or distance_km < 0.0:
                raise ValhallaReachabilityError("Valhalla matrix distance is invalid.")
            parsed.append(distance_km * 1000.0)
        return parsed


def _matrix_from_rows(
    rows: dict[int, dict[str, int | float]],
    categories: Sequence[str],
    value_kind: str,
) -> ReachabilityMatrix:
    return ReachabilityMatrix.from_sparse_dict(rows, categories, value_kind=value_kind)


def _add_target_contributions(
    target: TargetContributions,
    distance_m: float,
    *,
    raw_counts: dict[str, int] | None,
    cluster_counts: dict[str, int] | None,
    effective_units: dict[str, float] | None,
    half_distances_m: dict[str, float],
) -> None:
    if raw_counts is not None:
        for category, count in target.raw_counts.items():
            raw_counts[category] = raw_counts.get(category, 0) + int(count)
    if cluster_counts is not None:
        for category, count in target.cluster_counts.items():
            cluster_counts[category] = cluster_counts.get(category, 0) + int(count)
    if effective_units is not None:
        for category, base_units in target.base_units.items():
            half_distance = float(half_distances_m.get(category, 0.0))
            if not math.isfinite(half_distance) or half_distance <= 0.0:
                raise ValhallaReachabilityError(
                    f"Invalid half-distance for category={category!r}: {half_distance!r}"
                )
            effective_units[category] = effective_units.get(category, 0.0) + (
                int(base_units) * math.pow(0.5, distance_m / half_distance)
            )


def _route_origin(
    origin_node: int,
    target_index: UnifiedTargetIndex,
    client: ValhallaClient,
    *,
    radius_m: float,
    half_distances_m: dict[str, float],
    need_raw: bool,
    need_clusters: bool,
    need_effective: bool,
) -> tuple[int, dict[str, int] | None, dict[str, int] | None, dict[str, float] | None]:
    raw_counts: dict[str, int] | None = {} if need_raw else None
    cluster_counts: dict[str, int] | None = {} if need_clusters else None
    effective_units: dict[str, float] | None = {} if need_effective else None
    candidate_indexes = target_index.candidates_for_origin(origin_node, radius_m)
    if candidate_indexes.size == 0:
        return int(origin_node), raw_counts, cluster_counts, effective_units
    source_lat = float(target_index.graph_latitudes[int(origin_node)])
    source_lon = float(target_index.graph_longitudes[int(origin_node)])
    max_targets = client.settings.max_targets_per_request
    for start in range(0, int(candidate_indexes.size), max_targets):
        indexes = candidate_indexes[start : start + max_targets]
        distances = client.route_distances_m(
            source_lat,
            source_lon,
            target_index.latitudes[indexes],
            target_index.longitudes[indexes],
        )
        for index, distance_m in zip(indexes.tolist(), distances):
            if distance_m is None or distance_m > radius_m:
                continue
            _add_target_contributions(
                target_index.targets[int(index)],
                distance_m,
                raw_counts=raw_counts,
                cluster_counts=cluster_counts,
                effective_units=effective_units,
                half_distances_m=half_distances_m,
            )
    return int(origin_node), raw_counts, cluster_counts, effective_units


def route_valhalla_reachability(
    graph,
    raw_nodes_by_category: dict[str, list[int]],
    cluster_nodes_by_category: dict[str, list[int]],
    base_unit_rows: dict[str, list[tuple[int, int]]],
    *,
    missing_count_nodes: Iterable[int],
    missing_cluster_count_nodes: Iterable[int],
    missing_effective_nodes: Iterable[int],
    raw_categories: Sequence[str],
    cluster_categories: Sequence[str],
    effective_categories: Sequence[str],
    radius_m: float,
    half_distances_m: dict[str, float],
    settings: ValhallaSettings,
    progress_cb: Callable[..., object] | None = None,
    save_chunk_cb: Callable[[ReachabilityMatrix, ReachabilityMatrix, ReachabilityMatrix], None] | None = None,
) -> tuple[ReachabilityMatrix, ReachabilityMatrix, ReachabilityMatrix]:
    """Route missing origins once and fan each distance out to its needed metrics."""
    missing_raw = {int(node) for node in missing_count_nodes}
    missing_clusters = {int(node) for node in missing_cluster_count_nodes}
    missing_effective = {int(node) for node in missing_effective_nodes}
    origins = sorted(missing_raw | missing_clusters | missing_effective)
    empty_result = (
        ReachabilityMatrix.empty(raw_categories, value_kind="counts"),
        ReachabilityMatrix.empty(cluster_categories, value_kind="counts"),
        ReachabilityMatrix.empty(effective_categories, value_kind="effective_units"),
    )
    if not origins:
        return empty_result
    target_index = build_unified_target_index(
        graph, raw_nodes_by_category, cluster_nodes_by_category, base_unit_rows
    )
    client = ValhallaClient(settings)
    if progress_cb is not None:
        progress_cb("live_start", total_units=len(origins), detail="Valhalla walk origins")

    accumulated = [[], [], []]
    raw_rows: dict[int, dict[str, int]] = {}
    cluster_rows: dict[int, dict[str, int]] = {}
    effective_rows: dict[int, dict[str, float]] = {}

    def flush() -> None:
        if not raw_rows and not cluster_rows and not effective_rows:
            return
        matrices = (
            _matrix_from_rows(raw_rows, raw_categories, "counts"),
            _matrix_from_rows(cluster_rows, cluster_categories, "counts"),
            _matrix_from_rows(effective_rows, effective_categories, "effective_units"),
        )
        raw_rows.clear()
        cluster_rows.clear()
        effective_rows.clear()
        if save_chunk_cb is None:
            for index, matrix in enumerate(matrices):
                if len(matrix):
                    accumulated[index].append(matrix)
        else:
            save_chunk_cb(*matrices)

    max_in_flight = max(1, int(settings.workers) * 2)
    origin_iterator = iter(origins)
    with concurrent.futures.ThreadPoolExecutor(max_workers=settings.workers) as executor:
        pending: dict[concurrent.futures.Future, int] = {}

        def submit_next() -> bool:
            try:
                origin = next(origin_iterator)
            except StopIteration:
                return False
            future = executor.submit(
                _route_origin,
                origin,
                target_index,
                client,
                radius_m=radius_m,
                half_distances_m=half_distances_m,
                need_raw=origin in missing_raw,
                need_clusters=origin in missing_clusters,
                need_effective=origin in missing_effective,
            )
            pending[future] = origin
            return True

        while len(pending) < max_in_flight and submit_next():
            pass
        while pending:
            done, _ = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                pending.pop(future)
                origin, raw, clusters, effective = future.result()
                if raw is not None:
                    raw_rows[origin] = raw
                if clusters is not None:
                    cluster_rows[origin] = clusters
                if effective is not None:
                    effective_rows[origin] = effective
                if progress_cb is not None:
                    progress_cb("advance", units=1, detail="Valhalla walk origins")
                if len(raw_rows) + len(cluster_rows) + len(effective_rows) >= CHECKPOINT_ORIGIN_COUNT:
                    flush()
                submit_next()
    flush()

    if save_chunk_cb is not None:
        return empty_result
    return (
        merge_reachability_matrices(accumulated[0], categories=raw_categories, value_kind="counts"),
        merge_reachability_matrices(accumulated[1], categories=cluster_categories, value_kind="counts"),
        merge_reachability_matrices(
            accumulated[2], categories=effective_categories, value_kind="effective_units"
        ),
    )
