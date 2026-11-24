from __future__ import annotations

import math
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np
from shapely import distance as shapely_distance
from shapely import points as shapely_points
from shapely.ops import transform
from shapely.strtree import STRtree

from config import (
    ROAD_PROXIMITY_ALGORITHM_VERSION,
    ROAD_PROXIMITY_CLASS_SETTINGS,
    ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER,
    ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER,
    TO_TARGET,
)


_SPEED_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(km/?h|kph|mph)?\s*$", re.IGNORECASE)
_ROAD_PENALTY_CHUNK_POINTS = 250_000


def _proximity_workers(chunk_count: int) -> int:
    return max(1, min(chunk_count, os.cpu_count() or 1))


def parse_maxspeed_kmh(value: Any, fallback_speed_kmh: float) -> float:
    """Parse one unambiguous OSM maxspeed value, otherwise use the class fallback."""
    match = _SPEED_PATTERN.match(str(value or ""))
    if match is None:
        return float(fallback_speed_kmh)
    try:
        speed = float(match.group(1))
    except (TypeError, ValueError):
        return float(fallback_speed_kmh)
    if not math.isfinite(speed) or speed <= 0.0:
        return float(fallback_speed_kmh)
    unit = str(match.group(2) or "km/h").lower().replace("/", "")
    if unit == "mph":
        speed *= 1.609344
    return speed


def road_penalty_for_distance(
    distance_m: float,
    *,
    highway: str,
    maxspeed: Any = None,
) -> float:
    settings = ROAD_PROXIMITY_CLASS_SETTINGS.get(str(highway))
    if settings is None:
        return 0.0
    try:
        distance = float(distance_m)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(distance) or distance < 0.0:
        return 0.0

    full_distance = float(settings["full_penalty_distance_m"])
    zero_distance = float(settings["zero_penalty_distance_m"])
    if distance >= zero_distance:
        return 0.0
    fallback_speed = float(settings["fallback_speed_kmh"])
    speed = parse_maxspeed_kmh(maxspeed, fallback_speed)
    speed_multiplier = min(
        max(speed / fallback_speed, ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER),
        ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER,
    )
    max_penalty = float(settings["max_penalty"]) * speed_multiplier
    if distance <= full_distance or zero_distance <= full_distance:
        return max_penalty
    fraction = (distance - full_distance) / (zero_distance - full_distance)
    return max(0.0, max_penalty * (1.0 - fraction))


def _node_coordinate_arrays(graph) -> tuple[np.ndarray, np.ndarray]:
    attributes = getattr(graph, "attributes", None)
    try:
        names = {str(name) for name in attributes()} if callable(attributes) else set()
    except Exception:
        names = set()
    if "_node_latitudes" in names and "_node_longitudes" in names:
        latitudes = np.asarray(graph["_node_latitudes"], dtype=np.float64)
        longitudes = np.asarray(graph["_node_longitudes"], dtype=np.float64)
    else:
        latitudes = np.asarray(graph.vs["lat"], dtype=np.float64)
        longitudes = np.asarray(graph.vs["lon"], dtype=np.float64)
    return latitudes, longitudes


class RoadPenaltyField:
    """Road proximity penalty at arbitrary EPSG:2157 points (index built once)."""

    def __init__(self, road_rows: list[dict[str, Any]]) -> None:
        geometries = []
        self.highway_values: list[str] = []
        self.maxspeeds: list[Any] = []
        for row in road_rows or ():
            geometry = row.get("geom")
            highway = str(row.get("highway") or "")
            if highway not in ROAD_PROXIMITY_CLASS_SETTINGS or geometry is None:
                continue
            if getattr(geometry, "is_empty", False):
                continue
            metric_geometry = transform(TO_TARGET, geometry)
            if metric_geometry.is_empty:
                continue
            geometries.append(metric_geometry)
            self.highway_values.append(highway)
            self.maxspeeds.append(row.get("maxspeed"))
        self.geometries = np.asarray(geometries, dtype=object)
        self._tree = STRtree(geometries) if geometries else None
        self.max_distance = max(
            float(settings["zero_penalty_distance_m"])
            for settings in ROAD_PROXIMITY_CLASS_SETTINGS.values()
        )
        # Per-geometry constants of road_penalty_for_distance, so the per-pair
        # penalty can be evaluated with the same float64 arithmetic in numpy.
        full_distances: list[float] = []
        zero_distances: list[float] = []
        max_penalties: list[float] = []
        for highway, maxspeed in zip(self.highway_values, self.maxspeeds):
            settings = ROAD_PROXIMITY_CLASS_SETTINGS[highway]
            fallback_speed = float(settings["fallback_speed_kmh"])
            speed = parse_maxspeed_kmh(maxspeed, fallback_speed)
            speed_multiplier = min(
                max(speed / fallback_speed, ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER),
                ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER,
            )
            full_distances.append(float(settings["full_penalty_distance_m"]))
            zero_distances.append(float(settings["zero_penalty_distance_m"]))
            max_penalties.append(float(settings["max_penalty"]) * speed_multiplier)
        self._full_distances = np.asarray(full_distances, dtype=np.float64)
        self._zero_distances = np.asarray(zero_distances, dtype=np.float64)
        self._max_penalties = np.asarray(max_penalties, dtype=np.float64)

    def _pair_penalties(self, distances: np.ndarray, geometry_indices: np.ndarray) -> np.ndarray:
        """Vectorized road_penalty_for_distance for (distance, geometry) pairs."""
        full = self._full_distances[geometry_indices]
        zero = self._zero_distances[geometry_indices]
        max_penalty = self._max_penalties[geometry_indices]
        with np.errstate(divide="ignore", invalid="ignore"):
            fraction = (distances - full) / (zero - full)
            tapered = np.maximum(0.0, max_penalty * (1.0 - fraction))
        penalty = np.where((distances <= full) | (zero <= full), max_penalty, tapered)
        valid = np.isfinite(distances) & (distances >= 0.0) & (distances < zero)
        return np.where(valid, penalty, 0.0)

    def _chunk_max_penalties(self, metric_x: np.ndarray, metric_y: np.ndarray) -> np.ndarray:
        best = np.zeros(len(metric_x), dtype=np.float64)
        points = shapely_points(metric_x, metric_y)
        candidate_pairs = self._tree.query(points, predicate="dwithin", distance=self.max_distance)
        if len(candidate_pairs) != 2 or len(candidate_pairs[0]) == 0:
            return best
        point_indices = np.asarray(candidate_pairs[0], dtype=np.int64)
        geometry_indices = np.asarray(candidate_pairs[1], dtype=np.int64)
        distances = np.asarray(
            shapely_distance(points[point_indices], self.geometries[geometry_indices]),
            dtype=np.float64,
        )
        np.maximum.at(best, point_indices, self._pair_penalties(distances, geometry_indices))
        return best

    def at(self, metric_x: np.ndarray, metric_y: np.ndarray) -> np.ndarray:
        """Largest road penalty per point (``float32``)."""
        metric_x = np.asarray(metric_x, dtype=np.float64)
        metric_y = np.asarray(metric_y, dtype=np.float64)
        if self._tree is None or len(metric_x) == 0:
            return np.zeros(len(metric_x), dtype=np.float32)
        # Shapely releases the GIL in query/distance, so point chunks run in
        # parallel; the max over float64 pair penalties is order-independent.
        best = np.zeros(len(metric_x), dtype=np.float64)
        bounds = range(0, len(metric_x), _ROAD_PENALTY_CHUNK_POINTS)
        with ThreadPoolExecutor(max_workers=_proximity_workers(len(bounds))) as executor:
            futures = {
                start: executor.submit(
                    self._chunk_max_penalties,
                    metric_x[start : start + _ROAD_PENALTY_CHUNK_POINTS],
                    metric_y[start : start + _ROAD_PENALTY_CHUNK_POINTS],
                )
                for start in bounds
            }
            for start, future in futures.items():
                chunk = future.result()
                best[start : start + len(chunk)] = chunk
        # float32(max) equals the serial "keep float32 of any larger penalty" loop;
        # adding 0.0 folds a -0.0 taper result into the serial loop's +0.0.
        return (best + 0.0).astype(np.float32)


def compute_road_proximity_penalties(
    walk_graph,
    road_rows: list[dict[str, Any]],
) -> np.ndarray:
    node_count = int(walk_graph.vcount())
    penalties = np.zeros(node_count, dtype=np.float32)
    if node_count <= 0 or not road_rows:
        return penalties
    field = RoadPenaltyField(road_rows)
    if field._tree is None:
        return penalties
    latitudes, longitudes = _node_coordinate_arrays(walk_graph)
    if latitudes.size == 0 or longitudes.size == 0:
        return penalties
    metric_x, metric_y = TO_TARGET(longitudes, latitudes)
    return field.at(metric_x, metric_y)


def road_proximity_summary(road_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "enabled": True,
        "data_present": bool(road_rows),
        "row_count": len(road_rows),
        "algorithm_version": ROAD_PROXIMITY_ALGORITHM_VERSION,
        "class_settings": {
            str(highway): dict(settings)
            for highway, settings in ROAD_PROXIMITY_CLASS_SETTINGS.items()
        },
        "speed_multiplier_min": float(ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER),
        "speed_multiplier_max": float(ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER),
        "overlap_policy": "strongest_nearby_road",
    }
