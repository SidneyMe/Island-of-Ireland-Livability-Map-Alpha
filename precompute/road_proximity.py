from __future__ import annotations

import math
import re
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


def compute_road_proximity_penalties(
    walk_graph,
    road_rows: list[dict[str, Any]],
) -> np.ndarray:
    node_count = int(walk_graph.vcount())
    penalties = np.zeros(node_count, dtype=np.float32)
    if node_count <= 0 or not road_rows:
        return penalties

    geometries = []
    highway_values = []
    maxspeeds = []
    for row in road_rows:
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
        highway_values.append(highway)
        maxspeeds.append(row.get("maxspeed"))
    if not geometries:
        return penalties

    latitudes, longitudes = _node_coordinate_arrays(walk_graph)
    if latitudes.size == 0 or longitudes.size == 0:
        return penalties
    metric_x, metric_y = TO_TARGET(longitudes, latitudes)
    node_points = shapely_points(
        np.asarray(metric_x, dtype=np.float64),
        np.asarray(metric_y, dtype=np.float64),
    )
    tree = STRtree(geometries)
    max_distance = max(
        float(settings["zero_penalty_distance_m"])
        for settings in ROAD_PROXIMITY_CLASS_SETTINGS.values()
    )
    candidate_pairs = tree.query(node_points, predicate="dwithin", distance=max_distance)
    if len(candidate_pairs) != 2:
        return penalties

    node_indices = np.asarray(candidate_pairs[0], dtype=np.int64)
    geometry_indices = np.asarray(candidate_pairs[1], dtype=np.int64)
    distances = np.asarray(
        shapely_distance(node_points[node_indices], [geometries[index] for index in geometry_indices]),
        dtype=np.float64,
    )
    for node_index, geometry_index, distance_m in zip(node_indices, geometry_indices, distances):
        penalty = road_penalty_for_distance(
            float(distance_m),
            highway=highway_values[int(geometry_index)],
            maxspeed=maxspeeds[int(geometry_index)],
        )
        if penalty > float(penalties[int(node_index)]):
            penalties[int(node_index)] = np.float32(penalty)
    return penalties


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
