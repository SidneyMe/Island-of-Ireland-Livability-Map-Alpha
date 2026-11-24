from __future__ import annotations

from unittest import TestCase

import numpy as np
from shapely.geometry import LineString

from precompute.road_proximity import (
    compute_road_proximity_penalties,
    parse_maxspeed_kmh,
    road_penalty_for_distance,
)


class _Graph:
    def __init__(self, latitudes, longitudes) -> None:
        self.vs = {"lat": list(latitudes), "lon": list(longitudes)}

    def vcount(self) -> int:
        return len(self.vs["lat"])

    def attributes(self):
        return []


class RailwayProximityTests(TestCase):
    def test_penalties_work_with_production_walk_graph_index(self) -> None:
        # Production graphs are WalkGraphIndex (no igraph `.vs`); coordinates
        # come from _node_latitudes/_node_longitudes. A regression here used to
        # raise and silently zero every railway penalty.
        from pathlib import Path

        from network.loader import WalkGraphIndex
        from transit.railway_corridors import compute_railway_proximity_penalties

        graph = WalkGraphIndex(
            graph_dir=Path("."),
            meta={"node_count": 2},
            node_latitudes=np.array([53.35, 53.36], dtype=np.float32),
            node_longitudes=np.array([-6.255, -6.255], dtype=np.float32),
        )
        corridor = {"geom": LineString([(-6.26, 53.35), (-6.25, 53.35)])}

        penalties = compute_railway_proximity_penalties(graph, [corridor])

        self.assertEqual(penalties.shape, (2,))
        self.assertAlmostEqual(float(penalties[0]), 4.0)
        self.assertEqual(float(penalties[1]), 0.0)


class RoadProximityTests(TestCase):
    def test_maxspeed_parser_accepts_kmh_and_mph(self) -> None:
        self.assertEqual(parse_maxspeed_kmh("100 km/h", 80), 100.0)
        self.assertAlmostEqual(parse_maxspeed_kmh("50 mph", 80), 80.4672)

    def test_maxspeed_parser_falls_back_for_ranges_and_text(self) -> None:
        self.assertEqual(parse_maxspeed_kmh("80;100", 80), 80.0)
        self.assertEqual(parse_maxspeed_kmh("signals", 80), 80.0)

    def test_class_distance_decay_and_speed_cap(self) -> None:
        self.assertEqual(road_penalty_for_distance(50, highway="motorway"), 4.0)
        self.assertEqual(road_penalty_for_distance(300, highway="motorway"), 0.0)
        self.assertAlmostEqual(
            road_penalty_for_distance(187.5, highway="motorway"),
            2.0,
        )
        self.assertEqual(
            road_penalty_for_distance(20, highway="primary", maxspeed="200 km/h"),
            2.5,
        )

    def test_unknown_class_has_no_penalty(self) -> None:
        self.assertEqual(road_penalty_for_distance(0, highway="secondary"), 0.0)

    def test_compute_uses_strongest_nearby_road(self) -> None:
        graph = _Graph([53.35], [-6.26])
        rows = [
            {
                "highway": "primary",
                "maxspeed": None,
                "geom": LineString([(-6.2605, 53.35), (-6.2595, 53.35)]),
            },
            {
                "highway": "motorway",
                "maxspeed": None,
                "geom": LineString([(-6.261, 53.351), (-6.260, 53.351)]),
            },
        ]
        penalties = compute_road_proximity_penalties(graph, rows)
        self.assertEqual(penalties.shape, (1,))
        self.assertGreater(float(penalties[0]), 0.0)
        self.assertLessEqual(float(penalties[0]), 4.0)

    def test_empty_and_invalid_rows_return_zero_array(self) -> None:
        graph = _Graph([53.35], [-6.26])
        penalties = compute_road_proximity_penalties(
            graph,
            [{"highway": "motorway", "geom": None}],
        )
        np.testing.assert_array_equal(penalties, np.zeros(1, dtype=np.float32))

    def test_vectorized_field_matches_per_pair_reference(self) -> None:
        from shapely import distance as shapely_distance
        from shapely import points as shapely_points

        from config import ROAD_PROXIMITY_CLASS_SETTINGS
        from precompute import road_proximity

        rng = np.random.default_rng(7)
        highways = sorted(ROAD_PROXIMITY_CLASS_SETTINGS)
        speeds = [None, "30", "50 mph", "120 km/h", "signals", "80;100", "200 km/h"]
        rows = []
        for index in range(60):
            lon = -6.30 + rng.uniform(0.0, 0.08)
            lat = 53.33 + rng.uniform(0.0, 0.05)
            rows.append(
                {
                    "highway": highways[index % len(highways)],
                    "maxspeed": speeds[index % len(speeds)],
                    "geom": LineString([(lon, lat), (lon + rng.uniform(-0.01, 0.01), lat + 0.004)]),
                }
            )
        field = road_proximity.RoadPenaltyField(rows)
        bounds = np.array([geom.bounds for geom in field.geometries])
        metric_x = rng.uniform(bounds[:, 0].min() - 400, bounds[:, 2].max() + 400, 3000)
        metric_y = rng.uniform(bounds[:, 1].min() - 400, bounds[:, 3].max() + 400, 3000)

        reference = np.zeros(len(metric_x), dtype=np.float32)
        points = shapely_points(metric_x, metric_y)
        point_idx, geom_idx = field._tree.query(points, predicate="dwithin", distance=field.max_distance)
        for p, g, d in zip(point_idx, geom_idx, shapely_distance(points[point_idx], field.geometries[geom_idx])):
            penalty = road_penalty_for_distance(
                float(d), highway=field.highway_values[int(g)], maxspeed=field.maxspeeds[int(g)]
            )
            if penalty > float(reference[int(p)]):
                reference[int(p)] = np.float32(penalty)

        original_chunk = road_proximity._ROAD_PENALTY_CHUNK_POINTS
        try:
            road_proximity._ROAD_PENALTY_CHUNK_POINTS = 256
            chunked = field.at(metric_x, metric_y)
        finally:
            road_proximity._ROAD_PENALTY_CHUNK_POINTS = original_chunk

        self.assertGreater(int(np.count_nonzero(reference)), 100)
        np.testing.assert_array_equal(chunked, reference)
        np.testing.assert_array_equal(field.at(metric_x, metric_y), reference)
