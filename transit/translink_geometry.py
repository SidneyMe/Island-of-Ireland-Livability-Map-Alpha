from __future__ import annotations

import hashlib
import heapq
import json
import math
import numbers
import re
import statistics
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from zipfile import ZipFile

from pyproj import Transformer
from shapely.geometry import LineString, Point, shape
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree

from config import (
    GTFS_TRANSLINK_METRO_ROUTES_PATH,
    GTFS_TRANSLINK_RAIL_NETWORK_PATH,
    GTFS_TRANSLINK_ULSTERBUS_ROUTES_PATH,
    TO_TARGET,
)


BUS_STOP_SNAP_DISTANCE_M = 250.0
BUS_MIN_STOP_COVERAGE = 0.80
RAIL_STOP_SNAP_DISTANCE_M = 250.0
RAIL_MIN_STOP_COVERAGE = 0.90
_ALNUM_RE = re.compile(r"[^0-9A-Z]+")
_LEADING_ZERO_RE = re.compile(r"^0+(?=[0-9A-Z])")
_METRO_AGENCIES = frozenset({"51-49-MET", "51-49-GDR"})
_ULSTERBUS_AGENCIES = frozenset(
    {"51-49-ULB", "51-49-UTS", "51-49-FY", "51-49-GLE", "51-49-BE"}
)


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        return "missing"
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class TranslinkGeometrySourceState:
    ulsterbus_path: Path
    metro_path: Path
    rail_path: Path
    ulsterbus_fingerprint: str
    metro_fingerprint: str
    rail_fingerprint: str

    def hash_payload(self) -> dict[str, str]:
        return {
            "ulsterbus": self.ulsterbus_fingerprint,
            "metro_glider": self.metro_fingerprint,
            "rail": self.rail_fingerprint,
        }


def resolve_translink_geometry_source_state() -> TranslinkGeometrySourceState:
    return TranslinkGeometrySourceState(
        ulsterbus_path=GTFS_TRANSLINK_ULSTERBUS_ROUTES_PATH,
        metro_path=GTFS_TRANSLINK_METRO_ROUTES_PATH,
        rail_path=GTFS_TRANSLINK_RAIL_NETWORK_PATH,
        ulsterbus_fingerprint=_sha256_file(GTFS_TRANSLINK_ULSTERBUS_ROUTES_PATH),
        metro_fingerprint=_sha256_file(GTFS_TRANSLINK_METRO_ROUTES_PATH),
        rail_fingerprint=_sha256_file(GTFS_TRANSLINK_RAIL_NETWORK_PATH),
    )


def normalize_service_key(value: object) -> str:
    normalized = _ALNUM_RE.sub("", str(value or "").upper())
    return _LEADING_ZERO_RE.sub("", normalized)


def line_diva_service_key(value: object) -> str:
    normalized = _ALNUM_RE.sub("", str(value or "").upper())
    return normalize_service_key(normalized[2:] if len(normalized) > 2 else normalized)


def _node_key(value: object) -> str:
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, numbers.Real) and math.isfinite(float(value)):
        numeric = float(value)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)
    return str(value or "").strip()


@dataclass(frozen=True, slots=True)
class MatchedGeometry:
    geometry: LineString
    source: str
    coverage: float
    max_snap_distance_m: float


@dataclass(frozen=True, slots=True)
class _GraphEdge:
    from_node: str
    to_node: str
    geometry: LineString
    length_m: float


@dataclass(slots=True)
class _RouteGraph:
    key: str
    adjacency: dict[str, list[tuple[str, int, bool, float]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    edges: list[_GraphEdge] = field(default_factory=list)
    node_points: dict[str, Point] = field(default_factory=dict)
    _node_keys: list[str] = field(default_factory=list)
    _node_geometries: list[Point] = field(default_factory=list)
    _tree: STRtree | None = None
    _object_indexes: dict[int, int] = field(default_factory=dict)
    _wkb_indexes: dict[bytes, list[int]] = field(default_factory=dict)

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        geometry: LineString,
        *,
        length_m: float | None = None,
    ) -> None:
        if not from_node or not to_node or geometry.is_empty:
            return
        coords = list(geometry.coords)
        if len(coords) < 2:
            return
        resolved_length = (
            float(length_m)
            if length_m is not None
            else float(transform(TO_TARGET, geometry).length)
        )
        if not math.isfinite(resolved_length) or resolved_length <= 0:
            return
        edge_index = len(self.edges)
        self.edges.append(_GraphEdge(from_node, to_node, geometry, resolved_length))
        self.adjacency[from_node].append((to_node, edge_index, True, resolved_length))
        self.adjacency[to_node].append((from_node, edge_index, False, resolved_length))
        start_xy = TO_TARGET(*coords[0])
        end_xy = TO_TARGET(*coords[-1])
        self.node_points.setdefault(from_node, Point(*start_xy))
        self.node_points.setdefault(to_node, Point(*end_xy))

    def _ensure_index(self) -> None:
        if self._tree is not None:
            return
        self._node_keys = sorted(self.node_points)
        self._node_geometries = [self.node_points[key] for key in self._node_keys]
        if not self._node_geometries:
            return
        self._tree = STRtree(self._node_geometries)
        self._object_indexes = {id(item): index for index, item in enumerate(self._node_geometries)}
        for index, item in enumerate(self._node_geometries):
            self._wkb_indexes.setdefault(item.wkb, []).append(index)

    def nearest_node(self, point_wgs84: Point) -> tuple[str | None, float]:
        self._ensure_index()
        if self._tree is None:
            return None, math.inf
        point_target = Point(*TO_TARGET(point_wgs84.x, point_wgs84.y))
        nearest = self._tree.nearest(point_target)
        if isinstance(nearest, numbers.Integral):
            index = int(nearest)
        else:
            index = self._object_indexes.get(id(nearest), -1)
            if index < 0:
                indexes = self._wkb_indexes.get(nearest.wkb, [])
                index = indexes[0] if indexes else -1
        if index < 0:
            return None, math.inf
        return self._node_keys[index], float(point_target.distance(self._node_geometries[index]))

    def shortest_path(self, start: str, end: str) -> list[tuple[_GraphEdge, bool]] | None:
        if start == end:
            return []
        queue: list[tuple[float, str]] = [(0.0, start)]
        distances = {start: 0.0}
        previous: dict[str, tuple[str, int, bool]] = {}
        while queue:
            distance, node = heapq.heappop(queue)
            if distance != distances.get(node):
                continue
            if node == end:
                break
            for neighbor, edge_index, forward, weight in self.adjacency.get(node, ()):  # pragma: no branch
                candidate = distance + weight
                if candidate >= distances.get(neighbor, math.inf):
                    continue
                distances[neighbor] = candidate
                previous[neighbor] = (node, edge_index, forward)
                heapq.heappush(queue, (candidate, neighbor))
        if end not in previous:
            return None
        result: list[tuple[_GraphEdge, bool]] = []
        node = end
        while node != start:
            prior, edge_index, forward = previous[node]
            result.append((self.edges[edge_index], forward))
            node = prior
        result.reverse()
        return result


def _snap_pattern(
    graph: _RouteGraph,
    stops: list[Point],
    *,
    max_distance_m: float,
) -> tuple[list[str | None], list[float], float]:
    snapped: list[str | None] = []
    distances: list[float] = []
    for point in stops:
        node, distance = graph.nearest_node(point)
        distances.append(distance)
        snapped.append(node if distance <= max_distance_m else None)
    coverage = sum(node is not None for node in snapped) / len(snapped) if snapped else 0.0
    return snapped, distances, coverage


def _assemble_pattern(graph: _RouteGraph, snapped: list[str | None]) -> LineString | None:
    matched_nodes = [node for node in snapped if node is not None]
    if len(matched_nodes) < 2:
        return None
    coordinates: list[tuple[float, float]] = []
    for start, end in zip(matched_nodes, matched_nodes[1:]):
        if start == end:
            continue
        path = graph.shortest_path(start, end)
        if path is None:
            return None
        for edge, forward in path:
            edge_coords = list(edge.geometry.coords)
            if not forward:
                edge_coords.reverse()
            if coordinates and coordinates[-1] == edge_coords[0]:
                edge_coords = edge_coords[1:]
            coordinates.extend(edge_coords)
    return LineString(coordinates) if len(set(coordinates)) >= 2 else None


def _candidate_match(
    graph: _RouteGraph,
    stops: list[Point],
    *,
    max_distance_m: float,
    min_coverage: float,
) -> MatchedGeometry | None:
    if len(stops) < 2:
        return None
    snapped, distances, coverage = _snap_pattern(
        graph,
        stops,
        max_distance_m=max_distance_m,
    )
    if snapped[0] is None or snapped[-1] is None or coverage < min_coverage:
        return None
    geometry = _assemble_pattern(graph, snapped)
    if geometry is None:
        return None
    accepted_distances = [distance for node, distance in zip(snapped, distances) if node is not None]
    return MatchedGeometry(
        geometry=geometry,
        source="",
        coverage=coverage,
        max_snap_distance_m=max(accepted_distances, default=0.0),
    )


def _provider_for_agency(agency_id: object) -> str | None:
    normalized = str(agency_id or "").strip()
    if normalized in _METRO_AGENCIES:
        return "metro"
    if normalized in _ULSTERBUS_AGENCIES:
        return "ulsterbus"
    return None


@dataclass(slots=True)
class TranslinkGeometrySources:
    bus_graphs: dict[str, dict[str, list[_RouteGraph]]]
    rail_graph: _RouteGraph | None
    warnings: list[str]

    def match_bus_pattern(
        self,
        *,
        agency_id: object,
        route_short_name: object,
        stops: list[Point],
    ) -> MatchedGeometry | None:
        provider = _provider_for_agency(agency_id)
        service_key = normalize_service_key(route_short_name)
        if provider is None or not service_key:
            return None
        candidates = self.bus_graphs.get(provider, {}).get(service_key, [])
        ranked: list[tuple[float, float, str, MatchedGeometry]] = []
        for graph in candidates:
            snapped, distances, coverage = _snap_pattern(
                graph,
                stops,
                max_distance_m=BUS_STOP_SNAP_DISTANCE_M,
            )
            if not snapped or snapped[0] is None or snapped[-1] is None or coverage < BUS_MIN_STOP_COVERAGE:
                continue
            geometry = _assemble_pattern(graph, snapped)
            if geometry is None:
                continue
            accepted = [distance for node, distance in zip(snapped, distances) if node is not None]
            match = MatchedGeometry(
                geometry=geometry,
                source="translink_route_gis",
                coverage=coverage,
                max_snap_distance_m=max(accepted, default=0.0),
            )
            ranked.append((-coverage, statistics.median(accepted), graph.key, match))
        return min(ranked, default=None, key=lambda item: item[:3])[3] if ranked else None

    def match_rail_pattern(self, stops: list[Point]) -> MatchedGeometry | None:
        if self.rail_graph is None:
            return None
        match = _candidate_match(
            self.rail_graph,
            stops,
            max_distance_m=RAIL_STOP_SNAP_DISTANCE_M,
            min_coverage=RAIL_MIN_STOP_COVERAGE,
        )
        if match is None:
            return None
        return MatchedGeometry(
            geometry=match.geometry,
            source="translink_rail_network",
            coverage=match.coverage,
            max_snap_distance_m=match.max_snap_distance_m,
        )


def _zip_mif_path(zip_path: Path, prefix: str) -> str:
    with ZipFile(zip_path) as archive:
        names = [
            name
            for name in archive.namelist()
            if Path(name).name.lower().startswith(prefix.lower()) and name.lower().endswith(".mif")
        ]
    if len(names) != 1:
        raise RuntimeError(
            f"Expected one {prefix} MIF layer in '{zip_path}', found {len(names)}."
        )
    return f"/vsizip/{zip_path.resolve().as_posix()}/{names[0]}"


def _load_bus_graphs(zip_path: Path) -> dict[str, list[_RouteGraph]]:
    if not zip_path.is_file():
        raise FileNotFoundError(zip_path)
    try:
        import pyogrio
    except ImportError as exc:  # pragma: no cover - depends on host GIS environment
        raise RuntimeError("pyogrio is required to read Translink MIF route data") from exc
    layer_path = _zip_mif_path(zip_path, "PtLinks")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"Value .* parsed incompletely.*",
            category=RuntimeWarning,
        )
        frame = pyogrio.read_dataframe(
            layer_path,
            columns=["LineDIVA", "FromStopID", "ToStopID"],
        )
    graphs_by_diva: dict[str, _RouteGraph] = {}
    for row in frame.itertuples(index=False):
        line_diva = str(row.LineDIVA or "").strip().upper()
        if not line_diva or "Z" in line_diva:
            continue
        geometry = row.geometry
        if geometry is None or geometry.geom_type != "LineString" or geometry.is_empty:
            continue
        graph = graphs_by_diva.setdefault(line_diva, _RouteGraph(line_diva))
        graph.add_edge(_node_key(row.FromStopID), _node_key(row.ToStopID), geometry)
    result: dict[str, list[_RouteGraph]] = defaultdict(list)
    for line_diva, graph in sorted(graphs_by_diva.items()):
        if graph.edges:
            result[line_diva_service_key(line_diva)].append(graph)
    return dict(result)


def _iter_lines(geometry: Any) -> Iterable[LineString]:
    if geometry.geom_type == "LineString":
        yield geometry
    elif geometry.geom_type == "MultiLineString":
        yield from geometry.geoms


def _load_rail_graph(path: Path) -> _RouteGraph:
    if not path.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    crs_name = str(((payload.get("crs") or {}).get("properties") or {}).get("name") or "")
    if crs_name.upper() != "EPSG:29902":
        raise RuntimeError(
            f"Translink rail network must declare EPSG:29902; got {crs_name or 'missing CRS'}."
        )
    to_wgs84 = Transformer.from_crs("EPSG:29902", "EPSG:4326", always_xy=True)
    graph = _RouteGraph("translink_rail_network")
    source_lines: list[LineString] = []
    for feature in payload.get("features") or []:
        raw_geometry = feature.get("geometry")
        if not raw_geometry:
            continue
        source_lines.extend(_iter_lines(shape(raw_geometry)))
    # The published sections overlap or meet mid-segment at junctions. Noding the
    # complete linework connects those intersections while preserving the one
    # genuinely isolated historical branch as a separate component.
    noded_linework = unary_union(source_lines)
    for line in _iter_lines(noded_linework):
        coords = list(line.coords)
        for start, end in zip(coords, coords[1:]):
            from_node = f"{round(start[0], 1):.1f},{round(start[1], 1):.1f}"
            to_node = f"{round(end[0], 1):.1f},{round(end[1], 1):.1f}"
            start_wgs84 = to_wgs84.transform(*start)
            end_wgs84 = to_wgs84.transform(*end)
            graph.add_edge(
                from_node,
                to_node,
                LineString([start_wgs84, end_wgs84]),
                length_m=math.dist(start, end),
            )
    if not graph.edges:
        raise RuntimeError(f"Translink rail network '{path}' contains no usable linework.")
    return graph


def load_translink_geometry_sources(
    source_state: TranslinkGeometrySourceState | None = None,
) -> TranslinkGeometrySources:
    state = source_state or resolve_translink_geometry_source_state()
    bus_graphs: dict[str, dict[str, list[_RouteGraph]]] = {}
    messages: list[str] = []
    for provider, path in (
        ("ulsterbus", state.ulsterbus_path),
        ("metro", state.metro_path),
    ):
        try:
            bus_graphs[provider] = _load_bus_graphs(path)
        except Exception as exc:  # source data/GDAL failures must preserve the old fallback
            bus_graphs[provider] = {}
            messages.append(f"Translink {provider} route geometry unavailable: {exc}")
    rail_graph = None
    try:
        rail_graph = _load_rail_graph(state.rail_path)
    except Exception as exc:  # source data failures must preserve the old fallback
        messages.append(f"Translink rail geometry unavailable: {exc}")
    return TranslinkGeometrySources(
        bus_graphs=bus_graphs,
        rail_graph=rail_graph,
        warnings=messages,
    )


__all__ = [
    "BUS_MIN_STOP_COVERAGE",
    "BUS_STOP_SNAP_DISTANCE_M",
    "MatchedGeometry",
    "RAIL_MIN_STOP_COVERAGE",
    "RAIL_STOP_SNAP_DISTANCE_M",
    "TranslinkGeometrySourceState",
    "TranslinkGeometrySources",
    "line_diva_service_key",
    "load_translink_geometry_sources",
    "normalize_service_key",
    "resolve_translink_geometry_source_state",
]
