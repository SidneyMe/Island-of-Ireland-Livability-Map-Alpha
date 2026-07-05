from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import numpy as np
from shapely import distance as shapely_distance
from shapely import points as shapely_points
from shapely.geometry import LineString
from shapely.ops import transform, unary_union

from config import (
    RAILWAY_PROXIMITY_ACTIVE_MODES,
    RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
    RAILWAY_PROXIMITY_MAX_PENALTY,
    RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
    TRANSIT_REALITY_ALGO_VERSION,
    TO_TARGET,
    TO_WGS84,
    hash_dict,
)
from db_postgis._dependencies import select
from db_postgis.tables import (
    transit_routes,
    transit_service_classification,
    transit_trips,
)
from sqlalchemy import and_
from transit.gtfs_zip import route_mode


SOURCE_KIND = "gtfs_shapes"


@dataclass(frozen=True, slots=True)
class RailwayCorridorMaterialization:
    railway_proximity_hash: str
    manifest: dict[str, Any]
    rows: list[dict[str, Any]]


def railway_proximity_penalty_for_distance(distance_m: float | int | None) -> float:
    try:
        distance = float(distance_m)
    except (TypeError, ValueError):
        return 0.0
    if not distance == distance or distance < 0.0:  # NaN guard without importing math
        return 0.0
    if distance <= RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M:
        return float(RAILWAY_PROXIMITY_MAX_PENALTY)
    if distance >= RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M:
        return 0.0
    span = RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M - RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M
    if span <= 0.0:
        return 0.0
    ratio = 1.0 - (
        (distance - RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M) / span
    )
    return max(0.0, float(RAILWAY_PROXIMITY_MAX_PENALTY) * ratio)


def _shape_points_by_id(zip_path: Path) -> dict[str, list[tuple[int, float, float]]]:
    with ZipFile(zip_path) as archive:
        try:
            with archive.open("shapes.txt") as handle:
                reader = csv.DictReader(
                    line.decode("utf-8") for line in handle
                )
                rows_by_shape_id: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
                for row in reader:
                    shape_id = str(row.get("shape_id") or "").strip()
                    if not shape_id:
                        continue
                    try:
                        sequence = int(str(row.get("shape_pt_sequence") or "").strip())
                        lat = float(str(row.get("shape_pt_lat") or "").strip())
                        lon = float(str(row.get("shape_pt_lon") or "").strip())
                    except ValueError as exc:
                        raise RuntimeError(
                            f"GTFS shapes.txt in '{zip_path}' contains invalid shape point data."
                        ) from exc
                    rows_by_shape_id[shape_id].append((sequence, lat, lon))
        except KeyError as exc:
            raise FileNotFoundError("shapes.txt") from exc

    return {
        shape_id: sorted(points, key=lambda item: item[0])
        for shape_id, points in rows_by_shape_id.items()
    }


def _line_components(geometry) -> list[Any]:
    if geometry is None or getattr(geometry, "is_empty", False):
        return []
    geom_type = getattr(geometry, "geom_type", "")
    if geom_type == "LineString":
        return [geometry]
    if geom_type == "MultiLineString":
        return [geom for geom in geometry.geoms if getattr(geom, "geom_type", "") == "LineString"]
    if geom_type == "GeometryCollection":
        parts: list[Any] = []
        for geom in geometry.geoms:
            parts.extend(_line_components(geom))
        return parts
    return []


def _dissolve_lines(lines_wgs84: list[LineString]) -> tuple[Any | None, int]:
    if not lines_wgs84:
        return None, 0
    metric_lines = [transform(TO_TARGET, line) for line in lines_wgs84 if not line.is_empty]
    if not metric_lines:
        return None, 0
    dissolved_metric = unary_union(metric_lines)
    dissolved_components = _line_components(dissolved_metric)
    if not dissolved_components:
        return None, 0
    return transform(TO_WGS84, dissolved_metric), len(dissolved_components)


def _node_coordinate_arrays(graph) -> tuple[np.ndarray, np.ndarray]:
    attrs = set()
    attributes = getattr(graph, "attributes", None)
    if callable(attributes):
        try:
            attrs = {str(name) for name in attributes()}
        except Exception:
            attrs = set()
    if "_node_latitudes" in attrs and "_node_longitudes" in attrs:
        latitudes = np.asarray(graph["_node_latitudes"], dtype=np.float64)
        longitudes = np.asarray(graph["_node_longitudes"], dtype=np.float64)
    else:
        latitudes = np.asarray(graph.vs["lat"], dtype=np.float64)
    longitudes = np.asarray(graph.vs["lon"], dtype=np.float64)
    return latitudes, longitudes


def _is_school_only_service(service_id: str, school_only_state: Any) -> bool:
    state = str(school_only_state or "").strip().lower()
    if state in {"yes", "true", "1", "school_only"}:
        return True
    if not state and "school" in service_id.lower():
        return True
    return False


def compute_railway_proximity_penalties(
    walk_graph,
    corridor_rows: list[dict[str, Any]],
) -> np.ndarray:
    node_count = int(walk_graph.vcount())
    penalties = np.zeros(node_count, dtype=np.float32)
    if node_count <= 0 or not corridor_rows:
        return penalties

    corridor_metric_geometries = [
        transform(TO_TARGET, row["geom"])
        for row in corridor_rows
        if row.get("geom") is not None and not getattr(row["geom"], "is_empty", False)
    ]
    if not corridor_metric_geometries:
        return penalties

    corridor_union = unary_union(corridor_metric_geometries)
    if corridor_union is None or getattr(corridor_union, "is_empty", False):
        return penalties

    latitudes, longitudes = _node_coordinate_arrays(walk_graph)
    if latitudes.size == 0 or longitudes.size == 0:
        return penalties
    metric_x, metric_y = TO_TARGET(longitudes, latitudes)
    node_points = shapely_points(
        np.asarray(metric_x, dtype=np.float64),
        np.asarray(metric_y, dtype=np.float64),
    )
    distances_m = np.asarray(shapely_distance(node_points, corridor_union), dtype=np.float32)
    span = RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M - RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M
    if span <= 0.0:
        penalties.fill(float(RAILWAY_PROXIMITY_MAX_PENALTY))
        return penalties
    penalties = np.where(
        distances_m <= RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
        float(RAILWAY_PROXIMITY_MAX_PENALTY),
        np.where(
            distances_m >= RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
            0.0,
            float(RAILWAY_PROXIMITY_MAX_PENALTY)
            * (
                1.0
                - (
                    (distances_m - RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M)
                    / span
                )
            ),
        ),
    ).astype(np.float32, copy=False)
    return penalties


def build_railway_corridor_materialization(
    engine,
    *,
    reality_fingerprint: str,
    import_fingerprint: str,
    transit_config_hash: str,
    feed_states,
    progress_cb=None,
) -> RailwayCorridorMaterialization:
    active_feed_rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    with engine.connect() as connection:
        for feed_state in feed_states:
            feed_id = str(feed_state.feed_id)
            service_rows = connection.execute(
                select(
                    transit_service_classification.c.service_id,
                    transit_service_classification.c.school_only_state,
                )
                .where(transit_service_classification.c.reality_fingerprint == reality_fingerprint)
                .where(transit_service_classification.c.feed_id == feed_id)
            ).mappings().all()
            active_service_ids: set[str] = set()
            for row in service_rows:
                service_id = str(row.get("service_id") or "").strip()
                if not service_id:
                    continue
                if _is_school_only_service(service_id, row.get("school_only_state")):
                    continue
                active_service_ids.add(service_id)
            if not active_service_ids:
                if progress_cb is not None:
                    progress_cb(
                        "detail",
                        detail=f"rail corridor build: no active rail/tram services for {feed_id}",
                        force_log=True,
                    )
                continue

            trip_rows = connection.execute(
                select(
                    transit_trips.c.trip_id,
                    transit_trips.c.service_id,
                    transit_trips.c.shape_id,
                    transit_routes.c.route_type,
                )
                .select_from(
                    transit_trips.join(
                        transit_routes,
                        and_(
                            transit_trips.c.feed_id == transit_routes.c.feed_id,
                            transit_trips.c.route_id == transit_routes.c.route_id,
                        ),
                    )
                )
                .where(transit_trips.c.feed_id == feed_id)
                .where(transit_trips.c.service_id.in_(sorted(active_service_ids)))
            ).mappings().all()

            active_trip_rows = []
            active_shape_ids: set[str] = set()
            route_modes: set[str] = set()
            for row in trip_rows:
                service_id = str(row.get("service_id") or "").strip()
                if service_id not in active_service_ids:
                    continue
                route_type = row.get("route_type")
                mode = str(route_mode(route_type))
                if mode not in RAILWAY_PROXIMITY_ACTIVE_MODES:
                    continue
                route_modes.add(mode)
                active_trip_rows.append(row)
                shape_id = str(row.get("shape_id") or "").strip()
                if shape_id:
                    active_shape_ids.add(shape_id)

            if not active_trip_rows or not active_shape_ids:
                if progress_cb is not None:
                    progress_cb(
                        "detail",
                        detail=f"rail corridor build: no active rail/tram shapes for {feed_id}",
                        force_log=True,
                    )
                continue

            try:
                shape_points = _shape_points_by_id(feed_state.zip_path)
            except FileNotFoundError:
                warnings.append(
                    {
                        "code": "missing_shapes_txt",
                        "feed_id": feed_id,
                        "source_path": str(feed_state.zip_path),
                        "message": (
                            f"GTFS feed {feed_id} does not include shapes.txt; "
                            "railway track proximity will skip this feed."
                        ),
                    }
                )
                if progress_cb is not None:
                    progress_cb(
                        "detail",
                        detail=f"rail corridor build: missing shapes.txt for {feed_id} -> skipped",
                        force_log=True,
                    )
                continue

            lines: list[LineString] = []
            for shape_id in sorted(active_shape_ids):
                points = shape_points.get(shape_id)
                if not points or len(points) < 2:
                    continue
                coords = [(lon, lat) for _sequence, lat, lon in points]
                if len({coord for coord in coords}) < 2:
                    continue
                line = LineString(coords)
                if line.is_empty or line.length <= 0.0:
                    continue
                lines.append(line)

            if not lines:
                if progress_cb is not None:
                    progress_cb(
                        "detail",
                        detail=f"rail corridor build: no usable shapes for {feed_id}",
                        force_log=True,
                    )
                continue

            dissolved_geom, dissolved_shape_count = _dissolve_lines(lines)
            if dissolved_geom is None:
                continue

            active_service_count = len({str(row.get("service_id") or "") for row in active_trip_rows if row.get("service_id")})
            active_trip_count = len(active_trip_rows)
            active_shape_count = len(lines)
            active_feed_rows.append(
                {
                    "reality_fingerprint": reality_fingerprint,
                    "import_fingerprint": import_fingerprint,
                    "railway_proximity_hash": "",
                    "feed_id": feed_id,
                    "source_kind": SOURCE_KIND,
                    "active_service_count": active_service_count,
                    "active_trip_count": active_trip_count,
                    "active_shape_count": active_shape_count,
                    "dissolved_shape_count": dissolved_shape_count,
                    "route_modes_json": sorted(route_modes),
                    "geom": dissolved_geom,
                }
            )
            if progress_cb is not None:
                progress_cb(
                    "detail",
                    detail=(
                        f"rail corridor build: {feed_id} -> "
                        f"{active_shape_count:,} shapes, {dissolved_shape_count:,} dissolved parts"
                    ),
                    force_log=True,
                )

    railway_proximity_hash = hash_dict(
        {
            "transit_reality_algo_version": TRANSIT_REALITY_ALGO_VERSION,
            "reality_fingerprint": reality_fingerprint,
            "import_fingerprint": import_fingerprint,
            "transit_config_hash": transit_config_hash,
            "source_kind": SOURCE_KIND,
            "full_penalty_distance_m": RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
            "zero_penalty_distance_m": RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
            "max_penalty": RAILWAY_PROXIMITY_MAX_PENALTY,
            "active_modes": list(RAILWAY_PROXIMITY_ACTIVE_MODES),
            "feed_rows": [
                {
                    "feed_id": row["feed_id"],
                    "active_service_count": row["active_service_count"],
                    "active_trip_count": row["active_trip_count"],
                    "active_shape_count": row["active_shape_count"],
                    "dissolved_shape_count": row["dissolved_shape_count"],
                    "route_modes": row["route_modes_json"],
                }
                for row in active_feed_rows
            ],
            "warnings": warnings,
        }
    )

    rows = [
        {
            **row,
            "railway_proximity_hash": railway_proximity_hash,
        }
        for row in active_feed_rows
    ]

    manifest = {
        "reality_fingerprint": reality_fingerprint,
        "import_fingerprint": import_fingerprint,
        "transit_config_hash": transit_config_hash,
        "railway_proximity_hash": railway_proximity_hash,
        "source_kind": SOURCE_KIND,
        "active_feed_count": len(active_feed_rows),
        "active_service_count": sum(int(row["active_service_count"]) for row in active_feed_rows),
        "active_trip_count": sum(int(row["active_trip_count"]) for row in active_feed_rows),
        "active_shape_count": sum(int(row["active_shape_count"]) for row in active_feed_rows),
        "warning_count": len(warnings),
        "warnings_json": warnings,
        "railway_proximity_full_penalty_distance_m": RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
        "railway_proximity_zero_penalty_distance_m": RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
        "railway_proximity_max_penalty": RAILWAY_PROXIMITY_MAX_PENALTY,
        "railway_proximity_active_modes": list(RAILWAY_PROXIMITY_ACTIVE_MODES),
        "status": "complete",
    }
    return RailwayCorridorMaterialization(
        railway_proximity_hash=railway_proximity_hash,
        manifest=manifest,
        rows=rows,
    )
