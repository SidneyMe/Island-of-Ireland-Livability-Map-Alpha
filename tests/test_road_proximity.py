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
