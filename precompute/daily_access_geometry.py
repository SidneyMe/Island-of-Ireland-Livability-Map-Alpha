"""Daily access score (v4): exact network entries on walk-graph edges.

Reference implementation; ``walkgraph/src/daily_access/geometry.rs`` mirrors
it operation for operation.

An edge is the straight segment between its two graph nodes in EPSG:2157:
``a`` and ``b`` (``a`` = lower node index), geometric length ``G`` and graph
length ``L`` (haversine, float32). A position ``x`` in ``[0, G]`` costs
``c * x`` to reach ``a`` and ``c * (G - x)`` to reach ``b`` with ``c = L / G``.
Edges are never joined where they merely cross on the map: only shared nodes
connect them.

A point reaches position ``x`` through a straight connector of length
``sqrt((x - p)^2 + h^2)`` (``p`` = projection on the edge line, ``h`` =
perpendicular distance). The connector is admissible when it is at most
``DAILY_ACCESS_SNAP_RADIUS_M`` long and does not cross the land-mask boundary
(points deeper than the land band skip that test: their whole disc is land).
The admissible ``x`` form a union of closed intervals: the radius interval
minus, for every nearby boundary segment, the exact interval of ``x`` whose
connector meets it (the connector "shadow" of a segment is convex, so it cuts
one interval out of the edge line). Nothing is sampled.

Costs over an admissible interval are minimised exactly:

* towards ``a``: ``sqrt((x-p)^2+h^2) + c x`` has its minimum at
  ``p - c h / sqrt(1 - c^2)`` when ``c < 1`` (clamped to the interval) and at
  the lower bound when ``c >= 1``;
* towards ``b``: symmetric, ``p + c h / sqrt(1 - c^2)`` or the upper bound.

Every admissible position of an object is described by a *piece*:

* ``POINT(q, k)``: connector from a fixed point (point objects, footprint
  vertices, cell origins), cost ``sqrt((y-q)^2+k^2)``;
* ``LINE(alpha, beta)``: perpendicular connector from a footprint side,
  cost ``|alpha y + beta|``;
* ``ZERO``: the edge runs inside the footprint (or a park), cost 0.

All three are convex on their interval, so a cell and an object on the same
edge (direct walk ``c |x - y|`` between the two entries, never through an
endpoint) are minimised exactly by a convex 1-D search over the cell entry.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import shapely
from shapely.geometry.base import BaseGeometry

from config import (
    DAILY_ACCESS_INTERVAL_MERGE_M,
    DAILY_ACCESS_SAME_EDGE_ITERATIONS,
    DAILY_ACCESS_SNAP_RADIUS_M,
)


EPS_LENGTH_M = 1e-6
PIECE_POINT = 0
PIECE_LINE = 1
PIECE_ZERO = 2
INF = math.inf
_GOLDEN = (math.sqrt(5.0) - 1.0) / 2.0


@dataclass(frozen=True)
class EdgeFrame:
    edge: int
    node_a: int
    node_b: int
    ax: float
    ay: float
    ux: float
    uy: float
    length_geom_m: float
    length_graph_m: float

    @property
    def c(self) -> float:
        if self.length_geom_m <= EPS_LENGTH_M:
            return 1.0
        return self.length_graph_m / self.length_geom_m

    def project(self, x: float, y: float) -> tuple[float, float]:
        """``(p, h)``: position of the projection along the edge line, perpendicular distance."""
        dx = float(x) - self.ax
        dy = float(y) - self.ay
        return dx * self.ux + dy * self.uy, abs(dx * self.uy - dy * self.ux)

    def point(self, position: float) -> tuple[float, float]:
        return self.ax + position * self.ux, self.ay + position * self.uy


@dataclass(frozen=True)
class Piece:
    kind: int
    lo: float
    hi: float
    a: float = 0.0  # POINT: q ; LINE: alpha
    b: float = 0.0  # POINT: k ; LINE: beta

    def value(self, position: float) -> float:
        if self.kind == PIECE_POINT:
            return math.hypot(position - self.a, self.b)
        if self.kind == PIECE_LINE:
            return abs(self.a * position + self.b)
        return 0.0


def edge_frame(edge: int, node_a: int, node_b: int, ax: float, ay: float, bx: float, by: float, length_graph_m: float) -> EdgeFrame:
    dx = float(bx) - float(ax)
    dy = float(by) - float(ay)
    length = math.hypot(dx, dy)
    if length <= EPS_LENGTH_M:
        return EdgeFrame(edge, node_a, node_b, float(ax), float(ay), 1.0, 0.0, 0.0, float(length_graph_m))
    return EdgeFrame(edge, node_a, node_b, float(ax), float(ay), dx / length, dy / length, length, float(length_graph_m))


# --- intervals ----------------------------------------------------------------


def clip_linear(interval: tuple[float, float] | None, c0: float, c1: float) -> tuple[float, float] | None:
    """Keep the positions ``x`` of ``interval`` where ``c0 + c1 x >= 0``."""
    if interval is None:
        return None
    lo, hi = interval
    if c1 > 0.0:
        lo = max(lo, -c0 / c1)
    elif c1 < 0.0:
        hi = min(hi, -c0 / c1)
    elif c0 < 0.0:
        return None
    return (lo, hi) if lo <= hi else None


def radius_interval(p: float, h: float, length_geom_m: float, radius_m: float) -> tuple[float, float] | None:
    if h > radius_m:
        return None
    half = math.sqrt(max(radius_m * radius_m - h * h, 0.0))
    lo = max(0.0, p - half)
    hi = min(length_geom_m, p + half)
    return (lo, hi) if lo <= hi else None


def admissible_intervals(lo: float, hi: float, blocks: Iterable[tuple[float, float]]) -> list[tuple[float, float]]:
    """Closure of ``[lo, hi]`` minus the closed ``blocks``.

    Zero-length blocks only touch the connector and do not remove anything.
    Once something is blocked, gaps no wider than
    ``DAILY_ACCESS_INTERVAL_MERGE_M`` are dropped: float noise at a shared
    boundary vertex must not open a sliver across the water.
    """
    merge = DAILY_ACCESS_INTERVAL_MERGE_M
    if lo > hi:
        return []
    blocks = list(blocks)
    if hi - lo <= 0.0:
        return [] if any(b0 - merge <= lo <= b1 + merge for b0, b1 in blocks) else [(lo, hi)]
    clipped = sorted(
        (max(b0, lo), min(b1, hi)) for b0, b1 in blocks if min(b1, hi) - max(b0, lo) > 0.0
    )
    if not clipped:
        return [(lo, hi)]
    out: list[tuple[float, float]] = []
    cursor = lo
    for start, end in clipped:
        if start - cursor > merge:
            out.append((cursor, start))
        cursor = max(cursor, end)
    if hi - cursor > merge:
        out.append((cursor, hi))
    return out


def _cross(ax: float, ay: float, bx: float, by: float) -> float:
    return ax * by - ay * bx


def point_shadow(
    frame: EdgeFrame,
    px: float,
    py: float,
    segment: Sequence[float],
    interval: tuple[float, float],
) -> tuple[float, float] | None:
    """Positions whose connector from ``(px, py)`` meets the boundary ``segment``.

    The set ``{P + t (s - P) : s in S, t >= 1}`` is convex: with
    ``Q - P = alpha d1 + beta d2`` it is ``alpha >= 0, beta >= 0,
    alpha + beta >= 1``, three half-planes along the edge line.
    """
    d1x, d1y = segment[0] - px, segment[1] - py
    d2x, d2y = segment[2] - px, segment[3] - py
    cr = _cross(d1x, d1y, d2x, d2y)
    if cr == 0.0:
        return None  # P is collinear with the segment: measure zero on the edge
    sign = 1.0 if cr > 0.0 else -1.0
    wx, wy = frame.ax - px, frame.ay - py
    ux, uy = frame.ux, frame.uy
    out = clip_linear(interval, sign * _cross(wx, wy, d2x, d2y), sign * _cross(ux, uy, d2x, d2y))
    out = clip_linear(out, sign * _cross(d1x, d1y, wx, wy), sign * _cross(d1x, d1y, ux, uy))
    ex, ey = d2x - d1x, d2y - d1y
    return clip_linear(out, sign * _cross(wx, wy, ex, ey) - abs(cr), sign * _cross(ux, uy, ex, ey))


def parallel_shadow(
    frame: EdgeFrame,
    v1x: float,
    v1y: float,
    dx: float,
    dy: float,
    side: float,
    segment: Sequence[float],
    interval: tuple[float, float],
) -> tuple[float, float] | None:
    """Positions whose perpendicular connector from a footprint side meets ``segment``.

    Local frame of the side: ``tau`` along ``d`` from ``v1``, ``t`` along the
    normal ``n = (-dy, dx)``; the connector is the vertical segment from
    ``(tau, 0)`` to ``(tau, t)`` on side ``side`` (+1 / -1). It meets the
    boundary segment where its ``tau`` lies in the segment's range and the
    segment's ``t`` there lies between 0 and the connector's ``t``: four
    half-planes along the edge line (no division by the segment's span).
    """
    nx, ny = -dy, dx

    def local(x: float, y: float) -> tuple[float, float]:
        rx, ry = x - v1x, y - v1y
        return rx * dx + ry * dy, rx * nx + ry * ny

    tau1, t1 = local(segment[0], segment[1])
    tau2, t2 = local(segment[2], segment[3])
    if tau1 == tau2:
        return None  # parallel to the connectors: measure zero
    if tau1 > tau2:
        tau1, t1, tau2, t2 = tau2, t2, tau1, t1
    span = tau2 - tau1
    tau0, t0 = local(frame.ax, frame.ay)
    u_tau = frame.ux * dx + frame.uy * dy
    u_t = frame.ux * nx + frame.uy * ny
    out = clip_linear(interval, tau0 - tau1, u_tau)
    out = clip_linear(out, tau2 - tau0, -u_tau)
    dt = t2 - t1
    out = clip_linear(out, side * (t1 * span + dt * (tau0 - tau1)), side * dt * u_tau)
    return clip_linear(out, side * (t0 * span - t1 * span - dt * (tau0 - tau1)), side * (u_t * span - dt * u_tau))


# --- boundary -------------------------------------------------------------------


class BoundarySegments:
    """Land-mask boundary segments in a uniform grid (cell = snap radius)."""

    def __init__(self, geometry_metric: BaseGeometry, *, cell_m: float = DAILY_ACCESS_SNAP_RADIUS_M) -> None:
        rings = []
        for polygon in getattr(geometry_metric, "geoms", [geometry_metric]):
            if polygon.is_empty:
                continue
            rings.append(np.asarray(polygon.exterior.coords, dtype=np.float64))
            rings.extend(np.asarray(ring.coords, dtype=np.float64) for ring in polygon.interiors)
        if rings:
            self.segments = np.concatenate(
                [np.column_stack([ring[:-1], ring[1:]]) for ring in rings if len(ring) > 1]
            )
        else:
            self.segments = np.zeros((0, 4), dtype=np.float64)
        self.cell_m = float(cell_m)
        buckets: dict[tuple[int, int], list[int]] = {}
        for index, (x0, y0, x1, y1) in enumerate(self.segments.tolist()):
            for bx in range(self._bucket(min(x0, x1)), self._bucket(max(x0, x1)) + 1):
                for by in range(self._bucket(min(y0, y1)), self._bucket(max(y0, y1)) + 1):
                    buckets.setdefault((bx, by), []).append(index)
        self._buckets = buckets

    def _bucket(self, value: float) -> int:
        return int(math.floor(value / self.cell_m))

    def near(self, min_x: float, min_y: float, max_x: float, max_y: float) -> list[tuple[float, float, float, float]]:
        """Segments whose bbox may meet the box (sorted by index: deterministic)."""
        found: set[int] = set()
        for bx in range(self._bucket(min_x), self._bucket(max_x) + 1):
            for by in range(self._bucket(min_y), self._bucket(max_y) + 1):
                found.update(self._buckets.get((bx, by), ()))
        return [tuple(self.segments[index]) for index in sorted(found)]


# --- pieces ---------------------------------------------------------------------


def point_pieces(
    frame: EdgeFrame,
    px: float,
    py: float,
    *,
    radius_m: float,
    boundary: Sequence[Sequence[float]] | None,
    domain: tuple[float, float] | None = None,
) -> list[Piece]:
    """Admissible ``POINT`` pieces of a connector from ``(px, py)``.

    ``boundary=None`` means the point is deeper than the land band.
    ``domain`` restricts the positions (a footprint vertex's region).
    """
    p, h = frame.project(px, py)
    interval = radius_interval(p, h, frame.length_geom_m, radius_m)
    if interval is not None and domain is not None:
        lo, hi = max(interval[0], domain[0]), min(interval[1], domain[1])
        interval = (lo, hi) if lo <= hi else None
    if interval is None:
        return []
    blocks = []
    for segment in boundary or ():
        blocked = point_shadow(frame, px, py, segment, interval)
        if blocked is not None:
            blocks.append(blocked)
    return [Piece(PIECE_POINT, lo, hi, p, h) for lo, hi in admissible_intervals(*interval, blocks)]


def _polygon_rings(footprint: BaseGeometry) -> list[np.ndarray]:
    rings = []
    for polygon in getattr(footprint, "geoms", [footprint]):
        if polygon.is_empty:
            continue
        rings.append(np.asarray(polygon.exterior.coords, dtype=np.float64))
        rings.extend(np.asarray(ring.coords, dtype=np.float64) for ring in polygon.interiors)
    return rings


def footprint_rings(footprint: BaseGeometry) -> list[np.ndarray]:
    """Closed rings (``(n, 2)``, first == last) of a polygonal footprint."""
    return _polygon_rings(footprint)


def _inside_intervals(frame: EdgeFrame, rings: Sequence[np.ndarray]) -> list[tuple[float, float]]:
    """Positions on ``[0, G]`` inside the footprint (even-odd over its rings)."""
    if frame.length_geom_m <= 0.0:
        return []
    crossings: list[float] = []
    ux, uy = frame.ux, frame.uy
    for ring in rings:
        for index in range(len(ring) - 1):
            x0, y0 = float(ring[index][0]) - frame.ax, float(ring[index][1]) - frame.ay
            x1, y1 = float(ring[index + 1][0]) - frame.ax, float(ring[index + 1][1]) - frame.ay
            s0 = _cross(ux, uy, x0, y0)
            s1 = _cross(ux, uy, x1, y1)
            # Vertices on the line count as "below": a touch is 0 or 2 crossings.
            if (s0 > 0.0) == (s1 > 0.0):
                continue
            fraction = s0 / (s0 - s1)
            crossings.append((x0 + fraction * (x1 - x0)) * ux + (y0 + fraction * (y1 - y0)) * uy)
    crossings.sort()
    out = []
    for start, end in zip(crossings[0::2], crossings[1::2]):
        lo, hi = max(start, 0.0), min(end, frame.length_geom_m)
        if lo < hi:
            out.append((lo, hi))
    return out


def footprint_pieces(
    frame: EdgeFrame,
    rings: Sequence[np.ndarray],
    *,
    radius_m: float,
    boundary: Sequence[Sequence[float]] | None,
) -> list[Piece]:
    """Admissible pieces of connectors from a polygon footprint (0 inside).

    Every footprint side contributes the connector from its own nearest
    point: the vertex regions are ``POINT`` pieces, the slab in between is a
    ``LINE`` piece per side of the side's line. The minimum over sides is the
    distance to the footprint; a side whose nearest connector is blocked can
    still be reached through another side.
    """
    pieces: list[Piece] = []
    length = frame.length_geom_m
    for lo, hi in _inside_intervals(frame, rings):
        pieces.append(Piece(PIECE_ZERO, lo, hi))
    for ring in rings:
        for index in range(len(ring) - 1):
            v1x, v1y = float(ring[index][0]), float(ring[index][1])
            v2x, v2y = float(ring[index + 1][0]), float(ring[index + 1][1])
            side_length = math.hypot(v2x - v1x, v2y - v1y)
            if side_length <= EPS_LENGTH_M:
                continue
            dx, dy = (v2x - v1x) / side_length, (v2y - v1y) / side_length
            tau0 = (frame.ax - v1x) * dx + (frame.ay - v1y) * dy
            u_tau = frame.ux * dx + frame.uy * dy
            full = (0.0, length)
            before = clip_linear(full, -tau0, -u_tau)  # tau <= 0: vertex v1
            after = clip_linear(full, tau0 - side_length, u_tau)  # tau >= len: vertex v2
            slab = clip_linear(clip_linear(full, tau0, u_tau), side_length - tau0, -u_tau)
            if before is not None:
                pieces.extend(point_pieces(frame, v1x, v1y, radius_m=radius_m, boundary=boundary, domain=before))
            if after is not None:
                pieces.extend(point_pieces(frame, v2x, v2y, radius_m=radius_m, boundary=boundary, domain=after))
            if slab is None:
                continue
            nx, ny = -dy, dx
            t0 = (frame.ax - v1x) * nx + (frame.ay - v1y) * ny
            u_t = frame.ux * nx + frame.uy * ny
            for side in (1.0, -1.0):
                interval = clip_linear(slab, side * t0, side * u_t)  # side * t >= 0
                interval = clip_linear(interval, radius_m - side * t0, -side * u_t)  # side * t <= R
                if interval is None:
                    continue
                blocks = []
                for segment in boundary or ():
                    blocked = parallel_shadow(frame, v1x, v1y, dx, dy, side, segment, interval)
                    if blocked is not None:
                        blocks.append(blocked)
                for lo, hi in admissible_intervals(*interval, blocks):
                    pieces.append(Piece(PIECE_LINE, lo, hi, side * u_t, side * t0))
    return pieces


# --- costs ----------------------------------------------------------------------


def min_with_slope(piece: Piece, lo: float, hi: float, slope: float) -> float:
    """Exact ``min over [lo, hi]`` of ``piece(y) + slope * y`` (inf if empty)."""
    if lo > hi:
        return INF
    if piece.kind == PIECE_POINT:
        if abs(slope) < 1.0:
            stationary = piece.a - slope * piece.b / math.sqrt(1.0 - slope * slope)
            position = min(max(stationary, lo), hi)
        else:
            position = lo if slope > 0.0 else hi
        return piece.value(position) + slope * position
    if piece.kind == PIECE_LINE:
        best = min(piece.value(lo) + slope * lo, piece.value(hi) + slope * hi)
        if piece.a != 0.0:
            kink = -piece.b / piece.a
            if lo < kink < hi:
                best = min(best, piece.value(kink) + slope * kink)
        return best
    return slope * (lo if slope > 0.0 else hi)


def endpoint_costs(frame: EdgeFrame, pieces: Iterable[Piece], offset_m: float = 0.0) -> tuple[float, float]:
    """``(to a, to b)``: best connector + along-edge cost over all pieces."""
    c = frame.c
    to_a = INF
    to_b = INF
    for piece in pieces:
        to_a = min(to_a, min_with_slope(piece, piece.lo, piece.hi, c))
        to_b = min(to_b, c * frame.length_geom_m + min_with_slope(piece, piece.lo, piece.hi, -c))
    return to_a + offset_m, to_b + offset_m


def _potential(piece: Piece, c: float, position: float) -> float:
    """``min over y in the piece`` of ``c |x - y| + piece(y)``."""
    best = INF
    if piece.lo <= position:
        best = c * position + min_with_slope(piece, piece.lo, min(position, piece.hi), -c)
    if position <= piece.hi:
        best = min(best, -c * position + min_with_slope(piece, max(position, piece.lo), piece.hi, c))
    return best


def same_edge_distance(frame: EdgeFrame, cell: Piece, target: Piece, offset_m: float = 0.0) -> float:
    """Exact direct distance when cell and object enter the same edge.

    ``f(x) + min_y (c |x - y| + g(y))`` is convex in ``x`` (both terms are),
    so a golden-section search over the cell piece finds its minimum.
    """
    c = frame.c

    def total(position: float) -> float:
        return cell.value(position) + _potential(target, c, position)

    lo, hi = cell.lo, cell.hi
    best = min(total(lo), total(hi))
    left, right = lo, hi
    inner_left = right - _GOLDEN * (right - left)
    inner_right = left + _GOLDEN * (right - left)
    value_left = total(inner_left)
    value_right = total(inner_right)
    for _ in range(DAILY_ACCESS_SAME_EDGE_ITERATIONS):
        if value_left <= value_right:
            right = inner_right
            inner_right, value_right = inner_left, value_left
            inner_left = right - _GOLDEN * (right - left)
            value_left = total(inner_left)
        else:
            left = inner_left
            inner_left, value_left = inner_right, value_right
            inner_right = left + _GOLDEN * (right - left)
            value_right = total(inner_right)
    best = min(best, value_left, value_right)
    return best + offset_m


__all__ = [
    "BoundarySegments",
    "EdgeFrame",
    "EPS_LENGTH_M",
    "PIECE_LINE",
    "PIECE_POINT",
    "PIECE_ZERO",
    "Piece",
    "admissible_intervals",
    "clip_linear",
    "edge_frame",
    "endpoint_costs",
    "footprint_pieces",
    "footprint_rings",
    "min_with_slope",
    "parallel_shadow",
    "point_pieces",
    "point_shadow",
    "radius_interval",
    "same_edge_distance",
]
