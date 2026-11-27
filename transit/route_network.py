from __future__ import annotations

import colorsys
import hashlib
import json
import math
import re
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Iterable

from shapely.geometry import LineString, Point
from shapely.ops import transform
from shapely.strtree import STRtree
from sqlalchemy import and_, select

from config import (
    TRANSIT_ROUTE_NETWORK_ALGO_VERSION,
    TO_TARGET,
    VALHALLA_EXPECTED_VERSION,
    VALHALLA_TIMEOUT_S,
    VALHALLA_URL,
    hash_dict,
)
from db_postgis._dependencies import to_shape
from db_postgis.tables import (
    transit_routes,
    transit_service_classification,
    transit_stop_times,
    transit_stops,
    transit_trips,
)

from .railway_corridors import _shape_points_by_id
from .translink_geometry import (
    TranslinkGeometrySourceState,
    load_translink_geometry_sources,
    resolve_translink_geometry_source_state,
)


BACKBONE_MIN_ZOOM = 6
REGIONAL_MIN_ZOOM = 8
LOCAL_MIN_ZOOM = 10
BACKBONE_BUS_LENGTH_M = 40_000.0
REGIONAL_BUS_LENGTH_M = 15_000.0
RAIL_CONNECTION_DISTANCE_M = 250.0
TRANSFER_CLUSTER_DISTANCE_M = 120.0
VALHALLA_MAX_WAYPOINTS = 40
SUPPORTED_MODES = frozenset({"bus", "rail", "tram"})
_HEX_COLOR_RE = re.compile(r"^[0-9a-fA-F]{6}$")
_NAME_TOKEN_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class RoutePattern:
    route_key: str
    pattern_id: str
    feed_id: str
    route_id: str
    route_short_name: str | None
    route_long_name: str | None
    mode: str
    route_color: str
    route_text_color: str
    direction_id: int | None
    headsign: str | None
    origin_name: str | None
    destination_name: str | None
    geometry_source: str
    is_approximate: bool
    length_m: float
    station_connection_count: int
    visibility_class: str
    min_zoom: int
    stop_ids: tuple[str, ...]
    geometry: Any


@dataclass(frozen=True, slots=True)
class RouteStop:
    route_key: str
    pattern_id: str
    feed_id: str
    stop_id: str
    stop_sequence: int
    stop_name: str | None
    parent_station: str | None
    geometry: Point


@dataclass(frozen=True, slots=True)
class RouteNetworkMaterialization:
    route_network_hash: str
    manifest: dict[str, Any]
    patterns: list[dict[str, Any]]
    stops: list[dict[str, Any]]
    transfer_hubs: list[dict[str, Any]]


@dataclass(slots=True)
class _RailNodeIndex:
    points: list[Point]
    keys: list[str]
    tree: STRtree
    object_indexes: dict[int, int]
    wkb_indexes: dict[bytes, list[int]]


def route_network_hash_for_state(
    reality_fingerprint: str,
    *,
    translink_source_state: TranslinkGeometrySourceState | None = None,
) -> str:
    source_state = translink_source_state or resolve_translink_geometry_source_state()
    return hash_dict(
        {
            "algorithm_version": TRANSIT_ROUTE_NETWORK_ALGO_VERSION,
            "reality_fingerprint": reality_fingerprint,
            "fallback_policy": "gtfs-shape-valhalla-auto-stop-sequence",
            "valhalla_expected_version": VALHALLA_EXPECTED_VERSION,
            "backbone_bus_length_m": BACKBONE_BUS_LENGTH_M,
            "regional_bus_length_m": REGIONAL_BUS_LENGTH_M,
            "rail_connection_distance_m": RAIL_CONNECTION_DISTANCE_M,
            "transfer_cluster_distance_m": TRANSFER_CLUSTER_DISTANCE_M,
            "translink_geometry_sources": source_state.hash_payload(),
        }
    )


def route_key(feed_id: str, route_id: str) -> str:
    return f"{str(feed_id).strip()}:{str(route_id).strip()}"


def _digest(*parts: object, length: int = 20) -> str:
    payload = "\0".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:length]


def display_route_mode(route_type: int | None) -> str:
    if route_type in {0, 900, 901, 902, 903, 904, 905, 906}:
        return "tram"
    if route_type in {1, 2, 100, 101, 102, 103, 105, 106, 107, 108, 109, 110, 111, 112, 113}:
        return "rail"
    if route_type in {3, 200, 201, 202, 204, 700, 701, 702, 704, 705, 711, 712, 713}:
        return "bus"
    return "other"


def _contrast_text_color(background: str) -> str:
    value = background.lstrip("#")
    red, green, blue = (int(value[index:index + 2], 16) for index in (0, 2, 4))
    luminance = (0.2126 * red + 0.7152 * green + 0.0722 * blue) / 255.0
    return "#172126" if luminance >= 0.58 else "#ffffff"


def route_colors(route_key_value: str, raw_color: str | None, raw_text_color: str | None) -> tuple[str, str]:
    normalized = str(raw_color or "").strip().lstrip("#")
    if _HEX_COLOR_RE.fullmatch(normalized):
        color = "#" + normalized.lower()
    else:
        hue = int(hashlib.sha256(route_key_value.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
        red, green, blue = colorsys.hls_to_rgb(hue, 0.43, 0.66)
        color = "#{:02x}{:02x}{:02x}".format(round(red * 255), round(green * 255), round(blue * 255))
    text = str(raw_text_color or "").strip().lstrip("#")
    return color, ("#" + text.lower()) if _HEX_COLOR_RE.fullmatch(text) else _contrast_text_color(color)


def _school_only(value: object) -> bool:
    return str(value or "").strip().lower() in {"yes", "true", "1", "school_only"}


def _service_is_active(row: dict[str, Any]) -> bool:
    if _school_only(row.get("school_only_state")):
        return False
    reasons = {str(value) for value in (row.get("reason_codes_json") or [])}
    return bool(reasons.intersection({"weekday_service_present", "weekend_service_present"}))


def _coords_from_valhalla_shape(value: object) -> list[tuple[float, float]]:
    if isinstance(value, dict):
        value = value.get("coordinates")
    if not isinstance(value, list):
        return []
    coords: list[tuple[float, float]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            return []
        try:
            lon, lat = float(item[0]), float(item[1])
        except (TypeError, ValueError):
            return []
        if not math.isfinite(lon) or not math.isfinite(lat):
            return []
        coords.append((lon, lat))
    return coords


class ValhallaAutoRouter:
    def __init__(self, url: str = VALHALLA_URL, *, timeout_s: float = VALHALLA_TIMEOUT_S) -> None:
        self.url = str(url).rstrip("/")
        self.timeout_s = min(float(timeout_s), 5.0)
        self.available = False

    def _json(self, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.url}/{endpoint.lstrip('/')}"
        request: urllib.request.Request | str = url
        if payload is not None:
            request = urllib.request.Request(
                url,
                data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
        with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not isinstance(result, dict):
            raise ValueError("Valhalla returned a non-object payload")
        return result

    def preflight(self) -> bool:
        try:
            status = self._json("status")
            self.available = str(status.get("version") or "").strip() == VALHALLA_EXPECTED_VERSION
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError, urllib.error.URLError):
            self.available = False
        return self.available

    def route(self, stops: list[Point]) -> LineString | None:
        if not self.available or len(stops) < 2:
            return None
        merged: list[tuple[float, float]] = []
        for start in range(0, len(stops) - 1, VALHALLA_MAX_WAYPOINTS - 1):
            chunk = stops[start:start + VALHALLA_MAX_WAYPOINTS]
            if len(chunk) < 2:
                break
            try:
                payload = self._json(
                    "route",
                    {
                        "locations": [{"lat": point.y, "lon": point.x} for point in chunk],
                        "costing": "auto",
                        "shape_format": "geojson",
                        "units": "kilometers",
                    },
                )
            except (OSError, TimeoutError, ValueError, json.JSONDecodeError, urllib.error.URLError):
                return None
            legs = ((payload.get("trip") or {}).get("legs") or [])
            chunk_coords: list[tuple[float, float]] = []
            for leg in legs:
                if not isinstance(leg, dict):
                    return None
                coords = _coords_from_valhalla_shape(leg.get("shape"))
                if len(coords) < 2:
                    return None
                if chunk_coords and chunk_coords[-1] == coords[0]:
                    coords = coords[1:]
                chunk_coords.extend(coords)
            if len(chunk_coords) < 2:
                return None
            if merged and merged[-1] == chunk_coords[0]:
                chunk_coords = chunk_coords[1:]
            merged.extend(chunk_coords)
        return LineString(merged) if len(set(merged)) >= 2 else None


def _normalized_name(value: object) -> str:
    return _NAME_TOKEN_RE.sub(" ", str(value or "").lower()).strip()


def _visibility_class(mode: str, length_m: float, station_connections: int) -> tuple[str, int]:
    if mode in {"rail", "tram"} or length_m >= BACKBONE_BUS_LENGTH_M or station_connections >= 2:
        return "backbone", BACKBONE_MIN_ZOOM
    if mode == "bus" and length_m >= REGIONAL_BUS_LENGTH_M:
        return "regional", REGIONAL_MIN_ZOOM
    return "local", LOCAL_MIN_ZOOM


def _build_rail_node_index(rail_stops: list[RouteStop]) -> _RailNodeIndex | None:
    seen: set[tuple[str, str, str]] = set()
    points: list[Point] = []
    keys: list[str] = []
    for stop in rail_stops:
        node_key = (
            f"{stop.feed_id}:{stop.parent_station}"
            if stop.parent_station
            else f"{stop.feed_id}:{stop.stop_id}"
        )
        identity = (stop.feed_id, stop.stop_id, node_key)
        if identity in seen:
            continue
        seen.add(identity)
        metric = transform(TO_TARGET, stop.geometry)
        points.append(metric)
        keys.append(node_key)
    if not points:
        return None
    wkb_indexes: dict[bytes, list[int]] = defaultdict(list)
    for index, point in enumerate(points):
        wkb_indexes[point.wkb].append(index)
    return _RailNodeIndex(
        points=points,
        keys=keys,
        tree=STRtree(points),
        object_indexes={id(point): index for index, point in enumerate(points)},
        wkb_indexes=dict(wkb_indexes),
    )


def _station_connection_count(
    pattern: RoutePattern,
    stops: list[RouteStop],
    rail_stops: list[RouteStop] | _RailNodeIndex | None,
    proximity_cache: dict[tuple[str, str], frozenset[str]] | None = None,
) -> int:
    if pattern.mode != "bus" or not rail_stops:
        return 0
    index = rail_stops if isinstance(rail_stops, _RailNodeIndex) else _build_rail_node_index(rail_stops)
    if index is None:
        return 0
    connected: set[str] = set()
    for stop in stops:
        stop_key = (stop.feed_id, stop.stop_id)
        if proximity_cache is not None and stop_key in proximity_cache:
            connected.update(proximity_cache[stop_key])
            continue
        metric_stop = transform(TO_TARGET, stop.geometry)
        nearby: set[str] = set()
        for candidate in index.tree.query(metric_stop.buffer(RAIL_CONNECTION_DISTANCE_M)):
            if isinstance(candidate, int) or hasattr(candidate, "__index__"):
                candidate_indexes = [int(candidate)]
            else:
                candidate_index = index.object_indexes.get(id(candidate), -1)
                if candidate_index < 0:
                    candidate_indexes = index.wkb_indexes.get(candidate.wkb, [])
                else:
                    candidate_indexes = [candidate_index]
            for candidate_index in candidate_indexes:
                if metric_stop.distance(index.points[candidate_index]) <= RAIL_CONNECTION_DISTANCE_M:
                    nearby.add(index.keys[candidate_index])
        if proximity_cache is not None:
            proximity_cache[stop_key] = frozenset(nearby)
        connected.update(nearby)
    return len(connected)


def _build_transfer_hubs(
    stops: list[RouteStop],
    route_classes: dict[str, str],
    route_modes: dict[str, str],
) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for stop in stops:
        key = (stop.feed_id, stop.stop_id)
        payload = unique.setdefault(
            key,
            {
                "feed_id": stop.feed_id,
                "stop_id": stop.stop_id,
                "name": stop.stop_name,
                "parent_station": stop.parent_station,
                "geometry": stop.geometry,
                "route_keys": set(),
            },
        )
        payload["route_keys"].add(stop.route_key)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unnamed: list[dict[str, Any]] = []
    for payload in unique.values():
        if payload["parent_station"]:
            grouped[f"parent:{payload['feed_id']}:{payload['parent_station']}"].append(payload)
        else:
            name = _normalized_name(payload["name"])
            (grouped[f"name:{name}"] if name else unnamed).append(payload)

    clusters: list[list[dict[str, Any]]] = []
    for key, candidates in grouped.items():
        if key.startswith("parent:"):
            clusters.append(candidates)
            continue
        parents = list(range(len(candidates)))

        def find(index: int) -> int:
            while parents[index] != index:
                parents[index] = parents[parents[index]]
                index = parents[index]
            return index

        def union(left: int, right: int) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parents[right_root] = left_root

        metric_points = [transform(TO_TARGET, candidate["geometry"]) for candidate in candidates]
        for left in range(len(candidates)):
            for right in range(left + 1, len(candidates)):
                if metric_points[left].distance(metric_points[right]) <= TRANSFER_CLUSTER_DISTANCE_M:
                    union(left, right)
        components: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for index, candidate in enumerate(candidates):
            components[find(index)].append(candidate)
        clusters.extend(components.values())
    clusters.extend([[row] for row in unnamed])

    hubs: list[dict[str, Any]] = []
    for cluster in clusters:
        keys = sorted({key for row in cluster for key in row["route_keys"]})
        if len(keys) < 2:
            continue
        point = Point(
            sum(row["geometry"].x for row in cluster) / len(cluster),
            sum(row["geometry"].y for row in cluster) / len(cluster),
        )
        names = [str(row["name"]).strip() for row in cluster if str(row.get("name") or "").strip()]
        hub_name = Counter(names).most_common(1)[0][0] if names else None
        stop_refs = sorted({f"{row['feed_id']}:{row['stop_id']}" for row in cluster})
        hubs.append(
            {
                "hub_id": _digest("hub", *stop_refs),
                "hub_name": hub_name,
                "route_keys_json": keys,
                "stop_refs_json": stop_refs,
                "modes_json": sorted({route_modes[key] for key in keys if key in route_modes}),
                "route_count": len(keys),
                "backbone_route_count": sum(route_classes.get(key) == "backbone" for key in keys),
                "regional_route_count": sum(route_classes.get(key) in {"backbone", "regional"} for key in keys),
                "geom": point,
            }
        )
    return hubs


def build_route_network_materialization(
    engine,
    *,
    reality_state,
    valhalla_router: ValhallaAutoRouter | None = None,
    translink_source_state: TranslinkGeometrySourceState | None = None,
    progress_cb=None,
) -> RouteNetworkMaterialization:
    reality_fingerprint = reality_state.reality_fingerprint
    source_state = translink_source_state or resolve_translink_geometry_source_state()
    network_hash = route_network_hash_for_state(
        reality_fingerprint,
        translink_source_state=source_state,
    )
    feed_states = {state.feed_id: state for state in reality_state.feed_states}
    warnings: list[str] = []

    with engine.connect() as connection:
        route_rows = [dict(row) for row in connection.execute(select(transit_routes)).mappings()]
        service_rows = [
            dict(row)
            for row in connection.execute(
                select(transit_service_classification).where(
                    transit_service_classification.c.reality_fingerprint == reality_fingerprint
                )
            ).mappings()
        ]
        active_services = {
            (str(row["feed_id"]), str(row["service_id"]))
            for row in service_rows
            if _service_is_active(row)
        }
        trip_rows = [dict(row) for row in connection.execute(select(transit_trips)).mappings()]
        active_trips = {
            (str(row["feed_id"]), str(row["trip_id"])): row
            for row in trip_rows
            if (str(row["feed_id"]), str(row["service_id"])) in active_services
        }
        stop_time_rows = [
            dict(row)
            for row in connection.execute(
                select(transit_stop_times).order_by(
                    transit_stop_times.c.feed_id,
                    transit_stop_times.c.trip_id,
                    transit_stop_times.c.stop_sequence,
                )
            ).mappings()
            if (str(row["feed_id"]), str(row["trip_id"])) in active_trips
        ]
        stop_rows = [dict(row) for row in connection.execute(select(transit_stops)).mappings()]

    routes = {(str(row["feed_id"]), str(row["route_id"])): row for row in route_rows}
    stops_by_key = {(str(row["feed_id"]), str(row["stop_id"])): row for row in stop_rows}
    times_by_trip: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in stop_time_rows:
        times_by_trip[(str(row["feed_id"]), str(row["trip_id"]))].append(row)

    shapes_by_feed: dict[str, dict[str, list[tuple[int, float, float]]]] = {}
    for feed_id, feed_state in feed_states.items():
        try:
            shapes_by_feed[feed_id] = _shape_points_by_id(feed_state.zip_path)
        except FileNotFoundError:
            shapes_by_feed[feed_id] = {}
            warnings.append(f"GTFS feed {feed_id} has no shapes.txt; route fallbacks used")
        except RuntimeError as exc:
            shapes_by_feed[feed_id] = {}
            warnings.append(str(exc))

    router = valhalla_router or ValhallaAutoRouter()
    fallback_backend = "valhalla" if router.preflight() else "stop_sequence"
    if fallback_backend == "stop_sequence":
        warnings.append("Valhalla unavailable; missing GTFS shapes use approximate stop sequences")

    translink_geometry = None
    if "translink" in feed_states:
        translink_geometry = load_translink_geometry_sources(source_state)
        warnings.extend(translink_geometry.warnings)

    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for (feed_id, trip_id), trip in active_trips.items():
        route_id_value = str(trip["route_id"])
        route = routes.get((feed_id, route_id_value))
        if route is None:
            continue
        mode = display_route_mode(route.get("route_type"))
        if mode not in SUPPORTED_MODES:
            continue
        stop_times = times_by_trip.get((feed_id, trip_id), [])
        stop_ids = tuple(str(row["stop_id"]) for row in stop_times if (feed_id, str(row["stop_id"])) in stops_by_key)
        if len(stop_ids) < 2:
            continue
        group_key = (
            feed_id,
            route_id_value,
            trip.get("direction_id"),
            str(trip.get("shape_id") or ""),
            stop_ids,
        )
        grouped[group_key].append(trip)

    patterns: list[RoutePattern] = []
    pattern_stops: list[RouteStop] = []
    shape_geometry_cache: dict[tuple[str, str], tuple[LineString, float] | None] = {}
    official_geometry_cache: dict[
        tuple[str, str, str, tuple[str, ...]],
        tuple[LineString, str, float] | None,
    ] = {}
    fallback_geometry_cache: dict[tuple[str, str, tuple[str, ...]], tuple[LineString, str, float]] = {}
    geometry_source_counts: Counter[str] = Counter()
    grouped_items = sorted(grouped.items(), key=lambda item: tuple(str(v) for v in item[0]))
    for group_index, (group_key, trips) in enumerate(grouped_items, start=1):
        feed_id, route_id_value, direction_id, shape_id, stop_ids = group_key
        route = routes[(feed_id, route_id_value)]
        route_key_value = route_key(feed_id, route_id_value)
        mode = display_route_mode(route.get("route_type"))
        color, text_color = route_colors(route_key_value, route.get("route_color"), route.get("route_text_color"))
        pattern_id = _digest(route_key_value, direction_id, shape_id, *stop_ids)
        stop_objects: list[RouteStop] = []
        for sequence, stop_id in enumerate(stop_ids):
            row = stops_by_key[(feed_id, stop_id)]
            geometry = to_shape(row["geom"])
            stop_objects.append(
                RouteStop(
                    route_key=route_key_value,
                    pattern_id=pattern_id,
                    feed_id=feed_id,
                    stop_id=stop_id,
                    stop_sequence=sequence,
                    stop_name=row.get("stop_name"),
                    parent_station=row.get("parent_station"),
                    geometry=geometry,
                )
            )
        points = [stop.geometry for stop in stop_objects]
        geometry = None
        length_m = None
        geometry_source = "stop_sequence_approximate"
        shape_points = shapes_by_feed.get(feed_id, {}).get(shape_id) if shape_id else None
        if shape_points:
            shape_cache_key = (feed_id, shape_id)
            if shape_cache_key not in shape_geometry_cache:
                coords = [(lon, lat) for _sequence, lat, lon in shape_points]
                if len(set(coords)) >= 2:
                    shape_geometry = LineString(coords)
                    shape_geometry_cache[shape_cache_key] = (
                        shape_geometry,
                        float(transform(TO_TARGET, shape_geometry).length),
                    )
                else:
                    shape_geometry_cache[shape_cache_key] = None
            cached_shape = shape_geometry_cache[shape_cache_key]
            if cached_shape is not None:
                geometry, length_m = cached_shape
                geometry_source = "gtfs_shape"
        if geometry is None and feed_id == "translink" and translink_geometry is not None:
            official_cache_key = (
                mode,
                str(route.get("agency_id") or ""),
                str(route.get("route_short_name") or ""),
                stop_ids,
            )
            if official_cache_key not in official_geometry_cache:
                official_match = None
                if mode == "bus":
                    official_match = translink_geometry.match_bus_pattern(
                        agency_id=route.get("agency_id"),
                        route_short_name=route.get("route_short_name"),
                        stops=points,
                    )
                elif mode == "rail":
                    official_match = translink_geometry.match_rail_pattern(points)
                official_geometry_cache[official_cache_key] = (
                    None
                    if official_match is None
                    else (
                        official_match.geometry,
                        official_match.source,
                        float(transform(TO_TARGET, official_match.geometry).length),
                    )
                )
            official_geometry = official_geometry_cache[official_cache_key]
            if official_geometry is not None:
                geometry, geometry_source, length_m = official_geometry
        if geometry is None:
            fallback_cache_key = (feed_id, mode, stop_ids)
            cached_fallback = fallback_geometry_cache.get(fallback_cache_key)
            if cached_fallback is None:
                fallback_geometry = router.route(points) if mode == "bus" and router.available else None
                fallback_source = "valhalla_auto" if fallback_geometry is not None else "stop_sequence_approximate"
                if fallback_geometry is None:
                    coords = [(point.x, point.y) for point in points]
                    if len(set(coords)) < 2:
                        continue
                    fallback_geometry = LineString(coords)
                cached_fallback = (
                    fallback_geometry,
                    fallback_source,
                    float(transform(TO_TARGET, fallback_geometry).length),
                )
                fallback_geometry_cache[fallback_cache_key] = cached_fallback
            geometry, geometry_source, length_m = cached_fallback
        assert length_m is not None
        geometry_source_counts[geometry_source] += 1
        headsigns = [str(trip.get("trip_headsign") or "").strip() for trip in trips]
        headsign = Counter(value for value in headsigns if value).most_common(1)
        patterns.append(
            RoutePattern(
                route_key=route_key_value,
                pattern_id=pattern_id,
                feed_id=feed_id,
                route_id=route_id_value,
                route_short_name=(str(route.get("route_short_name") or "").strip() or None),
                route_long_name=(str(route.get("route_long_name") or "").strip() or None),
                mode=mode,
                route_color=color,
                route_text_color=text_color,
                direction_id=None if direction_id is None else int(direction_id),
                headsign=headsign[0][0] if headsign else None,
                origin_name=stop_objects[0].stop_name,
                destination_name=stop_objects[-1].stop_name,
                geometry_source=geometry_source,
                is_approximate=geometry_source == "stop_sequence_approximate",
                length_m=length_m,
                station_connection_count=0,
                visibility_class="local",
                min_zoom=LOCAL_MIN_ZOOM,
                stop_ids=stop_ids,
                geometry=geometry,
            )
        )
        pattern_stops.extend(stop_objects)
        if progress_cb is not None and group_index % 1000 == 0:
            progress_cb(
                "detail",
                detail=f"route network geometry: {group_index:,}/{len(grouped_items):,} patterns",
                force_log=True,
            )

    stops_by_pattern: dict[str, list[RouteStop]] = defaultdict(list)
    for stop in pattern_stops:
        stops_by_pattern[stop.pattern_id].append(stop)
    pattern_modes = {pattern.pattern_id: pattern.mode for pattern in patterns}
    rail_stops = [
        stop for stop in pattern_stops
        if pattern_modes.get(stop.pattern_id) in {"rail", "tram"}
    ]
    rail_node_index = _build_rail_node_index(rail_stops)
    rail_proximity_cache: dict[tuple[str, str], frozenset[str]] = {}
    classified_patterns: list[RoutePattern] = []
    for pattern in patterns:
        connections = _station_connection_count(
            pattern,
            stops_by_pattern[pattern.pattern_id],
            rail_node_index,
            rail_proximity_cache,
        )
        visibility_class, min_zoom = _visibility_class(pattern.mode, pattern.length_m, connections)
        classified_patterns.append(
            replace(
                pattern,
                station_connection_count=connections,
                visibility_class=visibility_class,
                min_zoom=min_zoom,
            )
        )
    route_classes: dict[str, str] = {}
    route_modes: dict[str, str] = {}
    class_rank = {"local": 0, "regional": 1, "backbone": 2}
    for pattern in classified_patterns:
        route_modes[pattern.route_key] = pattern.mode
        current = route_classes.get(pattern.route_key, "local")
        if class_rank[pattern.visibility_class] > class_rank[current]:
            route_classes[pattern.route_key] = pattern.visibility_class
        else:
            route_classes.setdefault(pattern.route_key, current)
    class_min_zoom = {"backbone": BACKBONE_MIN_ZOOM, "regional": REGIONAL_MIN_ZOOM, "local": LOCAL_MIN_ZOOM}
    patterns = [
        replace(
            pattern,
            visibility_class=route_classes[pattern.route_key],
            min_zoom=class_min_zoom[route_classes[pattern.route_key]],
        )
        for pattern in classified_patterns
    ]
    hubs = _build_transfer_hubs(pattern_stops, route_classes, route_modes)

    created_at = datetime.now(timezone.utc)
    pattern_rows = [
        {
            "reality_fingerprint": reality_fingerprint,
            "route_network_hash": network_hash,
            "route_key": pattern.route_key,
            "pattern_id": pattern.pattern_id,
            "feed_id": pattern.feed_id,
            "route_id": pattern.route_id,
            "route_short_name": pattern.route_short_name,
            "route_long_name": pattern.route_long_name,
            "mode": pattern.mode,
            "route_color": pattern.route_color,
            "route_text_color": pattern.route_text_color,
            "direction_id": pattern.direction_id,
            "headsign": pattern.headsign,
            "origin_name": pattern.origin_name,
            "destination_name": pattern.destination_name,
            "geometry_source": pattern.geometry_source,
            "is_approximate": pattern.is_approximate,
            "length_m": pattern.length_m,
            "station_connection_count": pattern.station_connection_count,
            "visibility_class": pattern.visibility_class,
            "min_zoom": pattern.min_zoom,
            "stop_ids_json": list(pattern.stop_ids),
            "geom": pattern.geometry,
            "created_at": created_at,
        }
        for pattern in patterns
    ]
    stop_rows_out = [
        {
            "reality_fingerprint": reality_fingerprint,
            "route_key": stop.route_key,
            "pattern_id": stop.pattern_id,
            "feed_id": stop.feed_id,
            "stop_id": stop.stop_id,
            "stop_sequence": stop.stop_sequence,
            "stop_name": stop.stop_name,
            "parent_station": stop.parent_station,
            "geom": stop.geometry,
            "created_at": created_at,
        }
        for stop in pattern_stops
    ]
    hub_rows = [
        {"reality_fingerprint": reality_fingerprint, **hub, "created_at": created_at}
        for hub in hubs
    ]
    route_count = len({pattern.route_key for pattern in patterns})
    manifest = {
        "reality_fingerprint": reality_fingerprint,
        "route_network_hash": network_hash,
        "algorithm_version": TRANSIT_ROUTE_NETWORK_ALGO_VERSION,
        "fallback_backend": fallback_backend,
        "route_count": route_count,
        "pattern_count": len(pattern_rows),
        "route_stop_count": len(stop_rows_out),
        "transfer_hub_count": len(hub_rows),
        "warnings_json": warnings,
        "status": "complete",
        "created_at": created_at,
        "completed_at": created_at,
    }
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=(
                "route geometry sources: "
                + ", ".join(
                    f"{source}={count}"
                    for source, count in sorted(geometry_source_counts.items())
                )
            ),
        )
        progress_cb(
            "detail",
            detail=(
                f"route network: {route_count} routes, {len(pattern_rows)} patterns, "
                f"{len(hub_rows)} transfer hubs"
            ),
            force_log=True,
        )
    return RouteNetworkMaterialization(network_hash, manifest, pattern_rows, stop_rows_out, hub_rows)


__all__ = [
    "BACKBONE_MIN_ZOOM",
    "LOCAL_MIN_ZOOM",
    "REGIONAL_MIN_ZOOM",
    "RouteNetworkMaterialization",
    "ValhallaAutoRouter",
    "build_route_network_materialization",
    "display_route_mode",
    "route_colors",
    "route_key",
    "route_network_hash_for_state",
]
