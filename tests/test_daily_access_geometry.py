"""Daily access v4 edge geometry: exact minima against independent brute force.

The brute force never reuses the kernel's formulas: it samples positions
densely along the edge, measures connectors with shapely and checks them
against the land polygon with ``covers``.
"""

from __future__ import annotations

import math
import random
from unittest import TestCase

import numpy as np
import shapely
from shapely.geometry import LineString, Point, Polygon, box

from precompute.daily_access_geometry import (
    BoundarySegments,
    PIECE_LINE,
    PIECE_POINT,
    PIECE_ZERO,
    Piece,
    admissible_intervals,
    edge_frame,
    endpoint_costs,
    footprint_pieces,
    footprint_rings,
    min_with_slope,
    point_pieces,
    same_edge_distance,
)

RADIUS = 250.0


def brute_point_costs(frame, px, py, land, *, step=0.05):
    """Min connector + along-edge cost towards a and b by dense sampling."""
    positions = np.append(np.arange(0.0, frame.length_geom_m, step), frame.length_geom_m)
    qx = frame.ax + positions * frame.ux
    qy = frame.ay + positions * frame.uy
    connector = np.hypot(qx - px, qy - py)
    ok = connector <= RADIUS
    if land is not None:
        lines = shapely.linestrings(np.stack([np.column_stack([np.full_like(qx, px), np.full_like(qy, py)]), np.column_stack([qx, qy])], axis=1))
        ok &= shapely.covers(land, lines)
    if not np.any(ok):
        return math.inf, math.inf
    c = frame.c
    to_a = float(np.min((connector + c * positions)[ok]))
    to_b = float(np.min((connector + c * (frame.length_geom_m - positions))[ok]))
    return to_a, to_b


class FixedCounterexampleTests(TestCase):
    def test_interior_stationary_point_beats_projection_and_bounds(self) -> None:
        # G = 500, L = 498.7, p = 249, h = 2, radius 250, route through a.
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 500.0, 0.0, 498.7)
        pieces = point_pieces(frame, 249.0, 2.0, radius_m=RADIUS, boundary=None)
        self.assertEqual(len(pieces), 1)
        c = frame.c
        naive = min(math.hypot(x - 249.0, 2.0) + c * x for x in (pieces[0].lo, pieces[0].hi, 249.0))
        self.assertAlmostEqual(naive, 249.008032, places=5)
        to_a, _ = endpoint_costs(frame, pieces)
        self.assertAlmostEqual(to_a, 248.496728, places=5)
        x_star = 249.0 - c * 2.0 / math.sqrt(1.0 - c * c)
        self.assertAlmostEqual(x_star, 221.319103, places=5)
        self.assertAlmostEqual(math.hypot(x_star - 249.0, 2.0) + c * x_star, to_a, places=9)

    def test_graph_length_above_geometry_uses_the_bounds(self) -> None:
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 500.0, 0.0, 501.0)  # c > 1: monotone branches
        pieces = point_pieces(frame, 249.0, 2.0, radius_m=RADIUS, boundary=None)
        to_a, to_b = endpoint_costs(frame, pieces)
        self.assertAlmostEqual(to_a, math.hypot(249.0, 2.0), places=9)  # x = lo = 0
        hi = pieces[0].hi  # radius bound short of b
        self.assertAlmostEqual(to_b, math.hypot(hi - 249.0, 2.0) + frame.c * (500.0 - hi), places=9)

    def test_on_the_line_and_degenerate_edges(self) -> None:
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 100.0, 0.0, 99.0)
        pieces = point_pieces(frame, 40.0, 0.0, radius_m=RADIUS, boundary=None)  # h = 0
        to_a, to_b = endpoint_costs(frame, pieces)
        self.assertAlmostEqual(to_a, 0.99 * 40.0, places=9)
        self.assertAlmostEqual(to_b, 0.99 * 60.0, places=9)
        point = edge_frame(1, 2, 3, 10.0, 10.0, 10.0, 10.0, 0.0)
        to_a, to_b = endpoint_costs(point, point_pieces(point, 13.0, 14.0, radius_m=RADIUS, boundary=None))
        self.assertAlmostEqual(to_a, 5.0)
        self.assertAlmostEqual(to_b, 5.0)
        self.assertEqual(point_pieces(point, 1000.0, 10.0, radius_m=RADIUS, boundary=None), [])


class IntervalTests(TestCase):
    def test_closure_merge_and_single_points(self) -> None:
        self.assertEqual(admissible_intervals(0.0, 10.0, [(2.0, 5.0)]), [(0.0, 2.0), (5.0, 10.0)])
        self.assertEqual(admissible_intervals(0.0, 10.0, [(0.0, 5.0), (5.0 + 1e-9, 10.0)]), [])
        self.assertEqual(admissible_intervals(0.0, 10.0, [(3.0, 3.0)]), [(0.0, 10.0)])  # touch only
        self.assertEqual(admissible_intervals(4.0, 4.0, [(3.0, 5.0)]), [])
        self.assertEqual(admissible_intervals(4.0, 4.0, [(5.0, 6.0)]), [(4.0, 4.0)])


class LandShadowBruteForceTests(TestCase):
    def test_random_coasts_match_dense_sampling(self) -> None:
        rng = random.Random(5)
        checked = 0
        for _ in range(60):
            # A jagged coastline: land below a polyline with random teeth and a bay.
            xs = np.linspace(-400.0, 900.0, 40)
            ys = np.asarray([rng.uniform(-60.0, 60.0) for _ in xs])
            ys[15:22] -= rng.uniform(80.0, 200.0)  # a bay cutting into the land
            land = Polygon([(-400.0, -900.0), *zip(xs.tolist(), ys.tolist()), (900.0, -900.0)])
            if not land.is_valid:
                continue
            boundary = BoundarySegments(land)
            px, py = rng.uniform(-100.0, 600.0), rng.uniform(-300.0, -80.0)
            if not land.contains(Point(px, py)):
                continue
            ax, ay = rng.uniform(-200.0, 700.0), rng.uniform(-400.0, 50.0)
            angle = rng.uniform(0.0, math.pi)
            length = rng.uniform(50.0, 600.0)
            frame = edge_frame(0, 0, 1, ax, ay, ax + length * math.cos(angle), ay + length * math.sin(angle), length * rng.uniform(0.985, 1.01))
            near = boundary.near(px - RADIUS, py - RADIUS, px + RADIUS, py + RADIUS)
            pieces = point_pieces(frame, px, py, radius_m=RADIUS, boundary=near)
            got = endpoint_costs(frame, pieces)
            expected = brute_point_costs(frame, px, py, land)
            for value, reference in zip(got, expected):
                if math.isinf(reference):
                    # Sampling can miss a sliver narrower than the step, never the converse.
                    self.assertTrue(math.isinf(value) or value >= 0.0)
                    continue
                self.assertLessEqual(value, reference + 1e-6)
                self.assertGreaterEqual(value, reference - 0.12)
            checked += 1
        self.assertGreater(checked, 20)

    def test_two_admissible_intervals_through_one_headland(self) -> None:
        # Land is a bar y <= 0 plus a thin tower (x 195..205) reaching y = 300.
        land = shapely.union(box(-1000, -1000, 1000, 0), box(195, -1000, 205, 300))
        boundary = BoundarySegments(land)
        # The cell stands in the tower; the edge runs on land along y = -50.
        frame = edge_frame(0, 0, 1, -100.0, -50.0, 500.0, -50.0, 600.0)
        px, py = 200.0, 150.0  # in the tower, 200 m above the bar
        near = boundary.near(px - RADIUS, py - RADIUS, px + RADIUS, py + RADIUS)
        pieces = point_pieces(frame, px, py, radius_m=RADIUS, boundary=near)
        self.assertEqual(len(pieces), 1)  # only the tower base is visible
        lo, hi = pieces[0].lo, pieces[0].hi
        self.assertTrue(290.0 < lo < 300.0 and 300.0 < hi < 310.0, (lo, hi))
        # Bay shape: land is a U around a 40 m channel; the edge crosses both
        # arms, so the admissible set splits into two intervals.
        u_land = Polygon([(0, 0), (1000, 0), (1000, 600), (520, 600), (520, 200), (480, 200), (480, 600), (0, 600)])
        boundary = BoundarySegments(u_land)
        frame = edge_frame(0, 0, 1, 200.0, 300.0, 800.0, 300.0, 600.0)
        near = boundary.near(500 - RADIUS, 100 - RADIUS, 500 + RADIUS, 100 + RADIUS)
        pieces = point_pieces(frame, 500.0, 100.0, radius_m=RADIUS, boundary=near)
        self.assertEqual(len(pieces), 2, pieces)
        self.assertAlmostEqual(pieces[0].lo, 150.0, places=6)  # radius limit
        self.assertAlmostEqual(pieces[0].hi, 260.0, places=6)  # through the arm corner (480, 200)
        self.assertAlmostEqual(pieces[1].lo, 340.0, places=6)
        self.assertAlmostEqual(pieces[1].hi, 450.0, places=6)
        got = endpoint_costs(frame, pieces)
        expected = brute_point_costs(frame, 500.0, 100.0, u_land, step=0.02)
        self.assertAlmostEqual(got[0], expected[0], delta=0.05)
        self.assertAlmostEqual(got[1], expected[1], delta=0.05)


def brute_same_edge(frame, cell_pieces, target_fn, target_ok, *, step=0.25):
    xs = np.concatenate([np.arange(p.lo, p.hi + step / 2, step) for p in cell_pieces])
    xs = np.unique(np.clip(xs, 0, frame.length_geom_m))
    ys = np.append(np.arange(0.0, frame.length_geom_m, step), frame.length_geom_m)
    f = np.asarray([min(p.value(x) for p in cell_pieces if p.lo - 1e-9 <= x <= p.hi + 1e-9) for x in xs])
    g = np.asarray([target_fn(y) if target_ok(y) else np.inf for y in ys])
    grid = f[:, None] + frame.c * np.abs(xs[:, None] - ys[None, :]) + g[None, :]
    return float(grid.min())


class SameEdgeTests(TestCase):
    def test_direct_walk_between_two_entries_matches_2d_brute_force(self) -> None:
        rng = random.Random(9)
        for _ in range(40):
            length = rng.uniform(40.0, 400.0)
            frame = edge_frame(0, 0, 1, 0.0, 0.0, length, 0.0, length * rng.uniform(0.98, 1.02))
            cx, cy = rng.uniform(0, length), rng.uniform(-80, 80)
            ox, oy = rng.uniform(0, length), rng.uniform(-80, 80)
            cell = point_pieces(frame, cx, cy, radius_m=RADIUS, boundary=None)
            target = point_pieces(frame, ox, oy, radius_m=RADIUS, boundary=None)
            got = min(same_edge_distance(frame, a, b) for a in cell for b in target)
            expected = brute_same_edge(frame, cell, lambda y: math.hypot(y - ox, oy), lambda y: True)
            self.assertLessEqual(got, expected + 1e-6)
            self.assertGreaterEqual(got, expected - 0.3)
            # Never worse than leaving through an endpoint and coming back.
            to_a_cell, to_b_cell = endpoint_costs(frame, cell)
            to_a_obj, to_b_obj = endpoint_costs(frame, target)
            self.assertLessEqual(got, min(to_a_cell + to_a_obj, to_b_cell + to_b_obj) + 1e-9)

    def test_same_spot_on_a_long_edge_needs_no_endpoint(self) -> None:
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 2000.0, 0.0, 2000.0)
        cell = point_pieces(frame, 1000.0, 20.0, radius_m=RADIUS, boundary=None)[0]
        shop = point_pieces(frame, 1010.0, -10.0, radius_m=RADIUS, boundary=None)[0]
        # Reflection: straight line across the edge = sqrt(10^2 + 30^2).
        self.assertAlmostEqual(same_edge_distance(frame, cell, shop), math.hypot(10.0, 30.0), places=6)
        to_a_cell, to_b_cell = endpoint_costs(frame, [cell])
        self.assertGreater(to_a_cell, 990.0)  # both endpoints are ~1 km away

    def test_zero_and_line_pieces(self) -> None:
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 300.0, 0.0, 300.0)
        cell = point_pieces(frame, 50.0, 30.0, radius_m=RADIUS, boundary=None)[0]
        park = Piece(PIECE_ZERO, 100.0, 200.0)
        self.assertAlmostEqual(same_edge_distance(frame, cell, park), math.hypot(50.0, 30.0), delta=1e-6)
        line = Piece(PIECE_LINE, 0.0, 300.0, 0.0, 12.0)  # parallel side 12 m away
        self.assertAlmostEqual(same_edge_distance(frame, cell, line), 30.0 + 12.0, delta=1e-6)
        self.assertAlmostEqual(min_with_slope(Piece(PIECE_POINT, 0.0, 300.0, 150.0, 0.0), 0.0, 300.0, 1.0), 150.0)


class FootprintTests(TestCase):
    def test_footprint_distance_matches_shapely_sampling(self) -> None:
        rng = random.Random(13)
        for _ in range(30):
            cx, cy = rng.uniform(-50, 50), rng.uniform(20, 120)
            sides = rng.randint(4, 8)
            radii = [rng.uniform(8, 40) for _ in range(sides)]
            angles = sorted(rng.uniform(0, 2 * math.pi) for _ in range(sides))
            footprint = Polygon([(cx + r * math.cos(t), cy + r * math.sin(t)) for r, t in zip(radii, angles)])
            if not footprint.is_valid or footprint.area < 50:
                continue
            length = rng.uniform(100, 500)
            frame = edge_frame(0, 0, 1, -length / 2, 0.0, length / 2, 0.0, length * rng.uniform(0.98, 1.02))
            pieces = footprint_pieces(frame, footprint_rings(footprint), radius_m=RADIUS, boundary=None)
            step = 0.05
            positions = np.append(np.arange(0.0, length, step), length)
            points = shapely.points(frame.ax + positions * frame.ux, frame.ay + positions * frame.uy)
            distance = shapely.distance(footprint, points)
            ok = distance <= RADIUS
            expected_a = float(np.min((distance + frame.c * positions)[ok]))
            expected_b = float(np.min((distance + frame.c * (length - positions))[ok]))
            got_a, got_b = endpoint_costs(frame, pieces)
            self.assertAlmostEqual(got_a, expected_a, delta=0.1)
            self.assertLessEqual(got_a, expected_a + 1e-6)
            self.assertAlmostEqual(got_b, expected_b, delta=0.1)

    def test_edge_through_the_footprint_costs_nothing_to_enter(self) -> None:
        footprint = box(90, -10, 110, 10)
        frame = edge_frame(0, 0, 1, 0.0, 0.0, 300.0, 0.0, 300.0)
        pieces = footprint_pieces(frame, footprint_rings(footprint), radius_m=RADIUS, boundary=None)
        self.assertIn(PIECE_ZERO, {piece.kind for piece in pieces})
        to_a, to_b = endpoint_costs(frame, pieces)
        self.assertAlmostEqual(to_a, 90.0, places=6)
        self.assertAlmostEqual(to_b, 190.0, places=6)

    def test_point_and_polygon_are_not_interchangeable(self) -> None:
        footprint = box(-100, 30, 100, 60)  # a long building parallel to the edge
        frame = edge_frame(0, 0, 1, -400.0, 0.0, 400.0, 0.0, 800.0)
        polygon_cost = endpoint_costs(frame, footprint_pieces(frame, footprint_rings(footprint), radius_m=RADIUS, boundary=None))
        centroid_cost = endpoint_costs(frame, point_pieces(frame, 0.0, 45.0, radius_m=RADIUS, boundary=None))
        # From the corner (-100, 30) out to the radius limit along the edge.
        self.assertAlmostEqual(polygon_cost[0], RADIUS + 300.0 - math.sqrt(RADIUS**2 - 30.0**2), delta=1e-6)
        self.assertAlmostEqual(centroid_cost[0], RADIUS + 400.0 - math.sqrt(RADIUS**2 - 45.0**2), delta=1e-6)
        self.assertGreater(centroid_cost[0], polygon_cost[0] + 50.0)


if __name__ == "__main__":
    import unittest

    unittest.main()
