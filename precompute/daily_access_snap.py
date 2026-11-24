"""Daily access score (v4): land mask, cell origins and edge entries.

Reference implementation. A point (cell origin, object, GTFS stop) enters
the walk network anywhere on an edge within ``DAILY_ACCESS_SNAP_RADIUS_M``
whose straight connector stays on land (``daily_access_geometry``). Every
admissible entry is kept: there is no "nearest node per component" and no
choice of a single component. Graph components are diagnostics only.

Points deeper than ``DAILY_ACCESS_LAND_BAND_M`` inside the mask skip the
boundary test (their whole disc is land). The land mask is the full
computational mask (main island), never clipped to a test/dev bbox; it
represents the sea and estuaries, not inland lakes or rivers.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import shapely
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components as _scipy_components
from shapely.geometry import Point, box
from shapely.geometry.base import BaseGeometry

from config import DAILY_ACCESS_LAND_BAND_M, DAILY_ACCESS_SNAP_RADIUS_M

from .daily_access_geometry import (
    BoundarySegments,
    EdgeFrame,
    Piece,
    edge_frame,
    endpoint_costs,
    footprint_pieces,
    footprint_rings,
    point_pieces,
)


REASON_OUTSIDE_LAND_MASK = "outside_land_mask"
REASON_NO_EDGE_WITHIN_RADIUS = "no_edge_within_radius"
REASON_NO_LAND_CONNECTOR = "no_land_connector"


def graph_components(offsets: np.ndarray, targets: np.ndarray, node_count: int) -> np.ndarray:
    """Undirected component id per node, numbered by each component's lowest node index."""
    offsets = np.asarray(offsets, dtype=np.int64)
    targets = np.asarray(targets, dtype=np.int64)
    adjacency = csr_matrix(
        (np.ones(len(targets), dtype=np.int8), targets, offsets),
        shape=(int(node_count), int(node_count)),
    )
    _count, labels = _scipy_components(adjacency, directed=False)
    first_node = np.full(int(labels.max(initial=-1)) + 1, np.iinfo(np.int64).max, dtype=np.int64)
    np.minimum.at(first_node, labels, np.arange(int(node_count), dtype=np.int64))
    rank = np.empty_like(first_node)
    rank[np.argsort(first_node, kind="stable")] = np.arange(len(first_node))
    return rank[labels].astype(np.int32)


class LandMask:
    """Polygon parts of the land mask, the interior beyond the band, boundary segments."""

    def __init__(self, geometry_metric: BaseGeometry, *, band_m: float = DAILY_ACCESS_LAND_BAND_M) -> None:
        parts = [part for part in getattr(geometry_metric, "geoms", [geometry_metric]) if not part.is_empty]
        if not parts:
            raise ValueError("land mask is empty")
        self.parts = parts
        self.band_m = float(band_m)
        self._tree = shapely.STRtree(parts)
        for part in parts:
            shapely.prepare(part)
        self.interior = shapely.buffer(geometry_metric, -self.band_m)
        shapely.prepare(self.interior)
        self.geometry = geometry_metric
        shapely.prepare(self.geometry)
        self._boundary: BoundarySegments | None = None

    @property
    def boundary(self) -> BoundarySegments:
        if self._boundary is None:
            self._boundary = BoundarySegments(self.geometry)
        return self._boundary

    def part_index(self, x: float, y: float) -> int | None:
        point = Point(float(x), float(y))
        for index in sorted(int(i) for i in self._tree.query(point)):
            if self.parts[index].covers(point):
                return index
        return None

    def in_interior(self, x: np.ndarray, y: np.ndarray) -> np.ndarray:
        return shapely.contains_xy(self.interior, np.asarray(x, dtype=np.float64), np.asarray(y, dtype=np.float64))

    def covers_xy(self, x: np.ndarray, y: np.ndarray) -> np.ndarray:
        return shapely.covers(self.geometry, shapely.points(np.asarray(x, dtype=np.float64), np.asarray(y, dtype=np.float64)))

    def boundary_near(self, min_x: float, min_y: float, max_x: float, max_y: float):
        return self.boundary.near(min_x, min_y, max_x, max_y)


def undirected_edges(offsets: np.ndarray, targets: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """``(slots, a, b)`` of every CSR slot with ``a < b``: edge id = position here."""
    offsets = np.asarray(offsets, dtype=np.int64)
    counts = np.diff(offsets)
    src = np.repeat(np.arange(len(counts), dtype=np.int64), counts)
    dst = np.asarray(targets, dtype=np.int64)
    slots = np.flatnonzero(src < dst)
    return slots, src[slots], dst[slots]


class EdgeIndex:
    """Undirected walk edges as straight segments, with a spatial index."""

    def __init__(
        self,
        node_x: np.ndarray,
        node_y: np.ndarray,
        offsets: np.ndarray,
        targets: np.ndarray,
        lengths: np.ndarray,
        *,
        radius_m: float = DAILY_ACCESS_SNAP_RADIUS_M,
    ) -> None:
        self.node_x = np.asarray(node_x, dtype=np.float64)
        self.node_y = np.asarray(node_y, dtype=np.float64)
        self.slots, self.node_a, self.node_b = undirected_edges(offsets, targets)
        self.lengths = np.asarray(lengths, dtype=np.float32)[self.slots].astype(np.float64)
        self.radius_m = float(radius_m)
        lines = shapely.linestrings(
            np.stack(
                [
                    np.column_stack([self.node_x[self.node_a], self.node_y[self.node_a]]),
                    np.column_stack([self.node_x[self.node_b], self.node_y[self.node_b]]),
                ],
                axis=1,
            )
        ) if len(self.slots) else np.zeros(0, dtype=object)
        self._tree = shapely.STRtree(lines)
        self._frames: dict[int, EdgeFrame] = {}

    def __len__(self) -> int:
        return len(self.slots)

    def frame(self, edge: int) -> EdgeFrame:
        frame = self._frames.get(edge)
        if frame is None:
            a, b = int(self.node_a[edge]), int(self.node_b[edge])
            frame = edge_frame(
                edge, a, b, self.node_x[a], self.node_y[a], self.node_x[b], self.node_y[b], self.lengths[edge]
            )
            self._frames[edge] = frame
        return frame

    def edges_near_box(self, min_x: float, min_y: float, max_x: float, max_y: float) -> list[int]:
        """Edges whose bbox meets the box (superset; sorted)."""
        if not len(self.slots):
            return []
        return sorted(int(edge) for edge in self._tree.query(box(min_x, min_y, max_x, max_y)))

    def edge_for_slot(self, slot: int) -> int:
        position = int(np.searchsorted(self.slots, slot))
        if position >= len(self.slots) or int(self.slots[position]) != int(slot):
            raise KeyError(slot)
        return position


@dataclass
class Entries:
    """Admissible pieces per edge and the derived endpoint costs."""

    pieces: dict[int, list[Piece]] = field(default_factory=dict)
    node_costs: dict[int, float] = field(default_factory=dict)
    reason: str | None = None

    @property
    def has_network(self) -> bool:
        return bool(self.pieces)

    def add(self, frame: EdgeFrame, pieces: list[Piece], offset_m: float = 0.0) -> None:
        if not pieces:
            return
        self.pieces.setdefault(frame.edge, []).extend(pieces)
        to_a, to_b = endpoint_costs(frame, pieces, offset_m)
        for node, cost in ((frame.node_a, to_a), (frame.node_b, to_b)):
            if cost < self.node_costs.get(node, np.inf):
                self.node_costs[node] = cost


def point_entries(
    index: EdgeIndex,
    land_mask: LandMask,
    x: float,
    y: float,
    *,
    interior: bool | None = None,
) -> Entries:
    """Every admissible entry of a point (cell origin, point object, stop)."""
    entries = Entries()
    radius = index.radius_m
    if land_mask.part_index(x, y) is None:
        entries.reason = REASON_OUTSIDE_LAND_MASK
        return entries
    if interior is None:
        interior = bool(land_mask.in_interior(np.asarray([x]), np.asarray([y]))[0])
    box_ = (x - radius, y - radius, x + radius, y + radius)
    boundary = None if interior else land_mask.boundary_near(*box_)
    candidates = index.edges_near_box(*box_)
    for edge in candidates:
        frame = index.frame(edge)
        entries.add(frame, point_pieces(frame, x, y, radius_m=radius, boundary=boundary))
    if not entries.has_network:
        in_radius = any(
            point_pieces(index.frame(edge), x, y, radius_m=radius, boundary=None) for edge in candidates
        )
        entries.reason = REASON_NO_LAND_CONNECTOR if in_radius else REASON_NO_EDGE_WITHIN_RADIUS
    return entries


def footprint_entries(
    index: EdgeIndex,
    land_mask: LandMask,
    footprint_metric: BaseGeometry,
    *,
    interior: bool | None = None,
) -> Entries:
    """Entries of a polygonal object: connectors from its outline, 0 inside."""
    if footprint_metric.geom_type not in {"Polygon", "MultiPolygon"}:
        centroid = footprint_metric.centroid
        return point_entries(index, land_mask, float(centroid.x), float(centroid.y))
    entries = Entries()
    anchor = footprint_metric.representative_point()
    if land_mask.part_index(float(anchor.x), float(anchor.y)) is None:
        entries.reason = REASON_OUTSIDE_LAND_MASK
        return entries
    if interior is None:
        interior = bool(land_mask.interior.covers(footprint_metric))
    radius = index.radius_m
    min_x, min_y, max_x, max_y = footprint_metric.bounds
    box_ = (min_x - radius, min_y - radius, max_x + radius, max_y + radius)
    boundary = None if interior else land_mask.boundary_near(*box_)
    rings = footprint_rings(footprint_metric)
    candidates = index.edges_near_box(*box_)
    for edge in candidates:
        frame = index.frame(edge)
        entries.add(frame, footprint_pieces(frame, rings, radius_m=radius, boundary=boundary))
    if not entries.has_network:
        in_radius = any(
            footprint_pieces(index.frame(edge), rings, radius_m=radius, boundary=None) for edge in candidates
        )
        entries.reason = REASON_NO_LAND_CONNECTOR if in_radius else REASON_NO_EDGE_WITHIN_RADIUS
    return entries


def cell_origin(
    x_min: float,
    y_min: float,
    size_m: float,
    land_mask: LandMask,
    *,
    geometry: BaseGeometry | None = None,
) -> tuple[float, float] | None:
    """Cell centre if it is on land, else a point on the cell's land part.

    Both are strictly inside the mask: a cell that only touches land along an
    edge or a corner has no land area and gets no origin.
    """
    mask = land_mask.geometry if geometry is None else geometry
    centre_x = x_min + size_m / 2.0
    centre_y = y_min + size_m / 2.0
    if bool(shapely.contains_xy(mask, centre_x, centre_y)):
        return centre_x, centre_y
    land = shapely.intersection(box(x_min, y_min, x_min + size_m, y_min + size_m), mask)
    polygons = [part for part in shapely.get_parts(land) if part.area > 0.0]
    if not polygons:
        return None
    point = shapely.multipolygons(polygons).point_on_surface()
    return float(point.x), float(point.y)


__all__ = [
    "EdgeIndex",
    "Entries",
    "LandMask",
    "REASON_NO_EDGE_WITHIN_RADIUS",
    "REASON_NO_LAND_CONNECTOR",
    "REASON_OUTSIDE_LAND_MASK",
    "cell_origin",
    "footprint_entries",
    "graph_components",
    "point_entries",
    "undirected_edges",
]
