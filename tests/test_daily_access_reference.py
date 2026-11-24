from __future__ import annotations

import itertools
import math
import os
import random
import unittest
import uuid
from unittest import TestCase

import numpy as np
import shapely
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from shapely.geometry import Point, Polygon, box

from config import DAILY_ACCESS_CATEGORY_MAX, DAILY_ACCESS_SEARCH_CUTOFF_M
from db_postgis.daily_access_green import GreenSite, candidate_edge_slots
from precompute.daily_access_classify import LogicalPlace, build_logical_places
from precompute.daily_access_reach import Seed, k_nearest_labels, reach_for_kind
from precompute.daily_access_reference import (
    ReferenceGraph,
    TransportStop,
    compute_reference,
)
from precompute.daily_access_score import (
    access_status,
    category_components,
    decay,
    need_access,
    quantize_access,
    quantize_distance,
    quantize_score,
    top3_access,
    total_score,
)
from precompute.daily_access_snap import LandMask, cell_origin, graph_components, point_entries


def build_graph(coords: dict[int, tuple[float, float]], edges: list[tuple[int, int]]) -> ReferenceGraph:
    """Symmetric CSR graph with Euclidean float32 lengths (like walkgraph)."""
    node_count = max(coords) + 1
    adjacency: dict[int, list[tuple[int, float]]] = {node: [] for node in range(node_count)}
    for a, b in edges:
        length = float(np.float32(math.dist(coords[a], coords[b])))
        adjacency[a].append((b, length))
        adjacency[b].append((a, length))
    offsets = [0]
    targets: list[int] = []
    lengths: list[float] = []
    for node in range(node_count):
        for target, length in adjacency[node]:
            targets.append(target)
            lengths.append(length)
        offsets.append(len(targets))
    return ReferenceGraph.from_arrays(
        node_x=np.asarray([coords.get(node, (1e9, 1e9))[0] for node in range(node_count)]),
        node_y=np.asarray([coords.get(node, (1e9, 1e9))[1] for node in range(node_count)]),
        offsets=np.asarray(offsets),
        targets=np.asarray(targets),
        lengths=np.asarray(lengths, dtype=np.float32),
    )


def line_graph(xs: list[float], y: float = 0.0, start: int = 0) -> tuple[dict, list]:
    coords = {start + index: (x, y) for index, x in enumerate(xs)}
    edges = [(start + index, start + index + 1) for index in range(len(xs) - 1)]
    return coords, edges


LAND = LandMask(box(-5_000.0, -5_000.0, 10_000.0, 10_000.0))


def place(kind: str, need: str, quality: float, ref: str, x: float, y: float, **extra) -> LogicalPlace:
    return LogicalPlace(
        place_id=f"{kind}|test:{ref}",
        service_kind=kind,
        need=need,
        quality=quality,
        members=[{"source": "test", "source_ref": ref, "x": x, "y": y, **extra}],
    )


def score(graph, cells, *, land=LAND, **kwargs):
    return compute_reference(graph=graph, land_mask=land, cells=cells, cell_size_m=50.0, **kwargs)


class CurveAndAggregationTests(TestCase):
    def test_curve_is_monotone_with_fixed_thresholds(self) -> None:
        for need, (full_m, zero_m) in {
            "food": (400.0, 1600.0),
            "green": (400.0, 2000.0),
            "transport": (240.0, 800.0),
        }.items():
            self.assertEqual(decay(full_m, need), 1.0)
            self.assertEqual(decay(zero_m, need), 0.0)
            self.assertAlmostEqual(decay((full_m + zero_m) / 2, need), 0.5)
            values = [decay(distance, need) for distance in np.linspace(0, zero_m + 100, 400)]
            self.assertTrue(all(a >= b for a, b in zip(values, values[1:])), need)
        self.assertEqual(DAILY_ACCESS_SEARCH_CUTOFF_M, {
            "food": 1600.0, "other_retail": 1600.0, "primary_care": 1600.0,
            "pharmacy": 1600.0, "green": 2000.0, "transport": 800.0,
        })

    def test_top3_saturation_80_15_5(self) -> None:
        self.assertAlmostEqual(top3_access([1.0]), 0.80)
        self.assertAlmostEqual(top3_access([1.0, 1.0]), 0.95)
        self.assertAlmostEqual(top3_access([1.0, 1.0, 1.0, 1.0]), 1.0)
        self.assertAlmostEqual(top3_access([0.2, 1.0, 0.5]), 0.8 + 0.15 * 0.5 + 0.05 * 0.2)

    def test_same_label_counts_once_and_transport_takes_best(self) -> None:
        self.assertAlmostEqual(need_access("food", [("a", 1.0, 0.0), ("a", 1.0, 10.0)]), 0.8)
        self.assertAlmostEqual(need_access("transport", [("s1", 0.5, 0.0), ("s2", 1.0, 0.0)]), 1.0)

    def test_category_maxima_and_total(self) -> None:
        full = {need: 1.0 for need in ("food", "other_retail", "primary_care", "pharmacy", "green", "transport")}
        components = category_components(full)
        self.assertEqual(components, DAILY_ACCESS_CATEGORY_MAX)
        self.assertEqual(total_score(components), 100.0)
        self.assertEqual(total_score(components, railway_penalty=4.0, road_penalty=3.0), 93.0)
        self.assertEqual(total_score(category_components({"food": 0.01}), road_penalty=4.0), 0.0)

    def test_quantisation_half_up_and_statuses(self) -> None:
        self.assertEqual(quantize_score(12.345), 1235)
        self.assertEqual(quantize_score(100.0), 10000)
        self.assertEqual(quantize_access(0.12345), 1235)
        self.assertEqual(quantize_distance(123.45), 1235)
        self.assertEqual(quantize_distance(10_000.0), 65535)
        self.assertEqual([access_status(v) for v in (0.0, 0.2, 0.5)], ["no_options", "limited", "options_found"])


class KNearestLabelsTests(TestCase):
    def test_matches_brute_force_on_random_graphs(self) -> None:
        rng = random.Random(7)
        for _ in range(20):
            count = 30
            coords = {node: (rng.uniform(0, 1000), rng.uniform(0, 1000)) for node in range(count)}
            edges = {tuple(sorted(rng.sample(range(count), 2))) for _ in range(70)}
            graph = build_graph(coords, sorted(edges))
            seeds = [Seed(rng.randrange(count), rng.uniform(0, 50), rng.randrange(6)) for _ in range(10)]
            settled = k_nearest_labels(graph.offsets, graph.targets, graph.lengths, seeds, k=3, cutoff_m=900.0)

            # Brute force: all-pairs shortest paths, then best distance per label.
            distance = np.full((count, count), np.inf)
            for node in range(count):
                distance[node, node] = 0.0
                for slot in range(graph.offsets[node], graph.offsets[node + 1]):
                    distance[node, graph.targets[slot]] = min(distance[node, graph.targets[slot]], float(graph.lengths[slot]))
            for mid, a, b in itertools.product(range(count), repeat=3):
                if distance[a, mid] + distance[mid, b] < distance[a, b]:
                    distance[a, b] = distance[a, mid] + distance[mid, b]
            for node in range(count):
                best: dict[int, float] = {}
                for seed in seeds:
                    value = seed.offset_m + distance[seed.node, node]
                    if value <= 900.0 and value < best.get(seed.label, np.inf):
                        best[seed.label] = value
                expected = sorted(best.values())[:3]
                got = sorted(d for d, _ in settled.get(node, []))
                np.testing.assert_allclose(got, expected, atol=1e-6)

    def test_object_connector_counts_toward_cutoff(self) -> None:
        graph = build_graph(*line_graph([0.0, 700.0]))
        reach = reach_for_kind(
            "transport_tier_5", graph.offsets, graph.targets, graph.lengths,
            [Seed(1, 120.0, 0)], np.asarray([0, 1]), k=1, cutoff_m=800.0,
        )
        self.assertEqual(reach.distance_codes[1, 0], 1200)
        self.assertEqual(reach.distance_codes[0, 0], 65535)  # 820 m > 800 m


class EntryTests(TestCase):
    def test_component_ids_are_diagnostics_only(self) -> None:
        coords = {0: (0, 0), 1: (10, 0), 2: (500, 0), 3: (510, 0), 4: (20, 0)}
        graph = build_graph(coords, [(2, 3), (0, 1), (1, 4)])
        self.assertEqual(graph.component_id.tolist(), [0, 0, 1, 1, 0])
        self.assertEqual(graph_components(graph.offsets, graph.targets, 5).tolist(), [0, 0, 1, 1, 0])

    def test_every_admissible_edge_is_an_entry(self) -> None:
        # Two edges of one component and one of another, all within 250 m.
        coords = {0: (0.0, 0.0), 1: (0.0, 100.0), 2: (60.0, 0.0), 3: (60.0, 10.0), 4: (0.0, 260.0), 5: (0.0, 300.0), 6: (300.0, 0.0)}
        graph = build_graph(coords, [(0, 1), (2, 3), (4, 5), (2, 6)])
        entries = point_entries(graph.edge_index(), LAND, 0.0, -10.0)
        self.assertEqual(sorted(entries.node_costs), [0, 1, 2, 3, 6])  # 4-5 is beyond 250 m
        self.assertAlmostEqual(entries.node_costs[0], 10.0)
        # Straight to node 3 beats node 2 + the 10 m edge.
        self.assertAlmostEqual(entries.node_costs[3], math.hypot(60.0, 20.0), places=6)

    def test_sea_connector_is_rejected_and_interior_skips_check(self) -> None:
        land = LandMask(Polygon([(0, 0), (2000, 0), (2000, 2000), (1100, 2000), (1100, 500), (900, 500), (900, 2000), (0, 2000)]))
        coords = {0: (890.0, 1000.0), 1: (890.0, 1100.0), 2: (1110.0, 1000.0), 3: (1110.0, 1100.0)}
        graph = build_graph(coords, [(0, 1), (2, 3)])
        # The edge across the bay is ~220 m away in a straight line over water.
        self.assertEqual(sorted(point_entries(graph.edge_index(), land, 1110.0, 950.0).node_costs), [2, 3])
        inner = LandMask(box(-5000, -5000, 5000, 5000))
        self.assertEqual(sorted(point_entries(graph.edge_index(), inner, 1000.0, 1000.0).node_costs), [0, 1, 2, 3])
        self.assertFalse(point_entries(graph.edge_index(), inner, 1000.0, 1400.0).has_network)  # 250 m is the limit

    def test_partial_coastal_cell_uses_point_on_land(self) -> None:
        land = LandMask(box(0, 0, 1000, 1000))
        self.assertEqual(cell_origin(100, 100, 50, land), (125.0, 125.0))
        x, y = cell_origin(980, 100, 50, land)
        self.assertTrue(980 <= x <= 1000 and 100 <= y <= 150)
        self.assertIsNone(cell_origin(2000, 2000, 50, land))


class CounterexampleTests(TestCase):
    """The three component-variant failures, plus node-count invariance."""

    def test_a_connecting_edge_never_lowers_access(self) -> None:
        # Main line with a supermarket; a short side path with a pharmacy.
        coords, edges = line_graph([0.0, 300.0, 600.0])
        coords.update({10: (0.0, 30.0), 11: (150.0, 270.0)})
        edges.append((10, 11))
        places = [
            place("food_full", "food", 1.0, "super", 600.0, 0.0),
            place("pharmacy", "pharmacy", 1.0, "chem", 150.0, 280.0),
        ]
        before = score(build_graph(coords, edges), [(-25.0, 0.0)], places=places).cells[0]
        after = score(build_graph(coords, edges + [(11, 1)]), [(-25.0, 0.0)], places=places).cells[0]
        for need in ("food", "pharmacy"):
            self.assertGreater(before.score.service_access[need], 0.0, need)  # both, not one variant
            self.assertGreaterEqual(after.score.service_access[need], before.score.service_access[need] - 1e-12, need)
        self.assertGreaterEqual(after.score.total, before.score.total)

    def test_b_cell_beside_the_middle_of_a_long_edge_has_a_network(self) -> None:
        graph = build_graph(*line_graph([0.0, 2000.0]))  # endpoints 1 km from the cell
        places = [place("food_full", "food", 1.0, "s", 1300.0, 20.0)]
        cell = score(graph, [(975.0, 15.0)], places=places).cells[0]
        self.assertEqual(cell.network_status, "ok")
        (label, distance), = cell.best_by_kind["food_full"]
        # Origin (1000, 40), shop (1300, 20), both on this edge: the direct
        # walk is the straight line across it, sqrt(300^2 + 60^2).
        self.assertAlmostEqual(distance, math.hypot(300.0, 60.0), delta=1e-3)
        # An extra node on the same straight edge changes nothing.
        split = build_graph({0: (0.0, 0.0), 1: (2000.0, 0.0), 2: (1000.0, 0.0)}, [(0, 2), (2, 1)])
        cell_split = score(split, [(975.0, 15.0)], places=places).cells[0]
        self.assertAlmostEqual(cell_split.best_by_kind["food_full"][0][1], distance, delta=0.1)
        self.assertAlmostEqual(cell_split.score.total, cell.score.total, delta=0.02)

    def test_c_adding_a_stop_keeps_the_shop(self) -> None:
        # The shop and the stop are reachable through different components.
        coords = {0: (0.0, 40.0), 1: (400.0, 40.0), 10: (0.0, -40.0), 11: (-400.0, -40.0)}
        graph = build_graph(coords, [(0, 1), (10, 11)])
        places = [place("food_full", "food", 1.0, "s", 400.0, 60.0)]
        without = score(graph, [(-25.0, -25.0)], places=places).cells[0]
        with_stop = score(graph, [(-25.0, -25.0)], places=places, stops=[TransportStop("gtfs/nta/1", "nta", 5, -400.0, -60.0)]).cells[0]
        self.assertGreater(with_stop.score.service_access["transport"], 0.0)
        self.assertAlmostEqual(with_stop.score.service_access["food"], without.score.service_access["food"], places=12)
        self.assertAlmostEqual(with_stop.score.components["shops"], without.score.components["shops"], places=12)

    def test_split_edge_keeps_distances_and_scores(self) -> None:
        rng = random.Random(21)
        base = {0: (0.0, 0.0), 1: (900.0, 0.0), 2: (900.0, 700.0), 3: (0.0, 700.0)}
        edges = [(0, 1), (1, 2), (2, 3), (3, 0)]
        places = [place("food_full", "food", 1.0, f"s{i}", rng.uniform(0, 900), rng.uniform(-40, 740)) for i in range(4)]
        stops = [TransportStop(f"gtfs/nta/{i}", "nta", 4, rng.uniform(0, 900), rng.uniform(-40, 740)) for i in range(3)]
        cells = [(rng.uniform(-100, 950), rng.uniform(-100, 750)) for _ in range(30)]
        one = score(build_graph(base, edges), cells, places=places, stops=stops)
        coords = dict(base)
        coords.update({4: (300.0, 0.0), 5: (650.0, 0.0), 6: (900.0, 333.0)})
        split_edges = [(0, 4), (4, 5), (5, 1), (1, 6), (6, 2), (2, 3), (3, 0)]
        two = score(build_graph(coords, split_edges), cells, places=places, stops=stops)
        for a, b in zip(one.cells, two.cells):
            self.assertEqual(a.network_status, b.network_status)
            self.assertAlmostEqual(a.score.total, b.score.total, delta=0.03)
            for need, value in a.score.service_access.items():
                self.assertAlmostEqual(value, b.score.service_access[need], delta=0.0005)

    def test_penalties_come_from_the_cell_origin(self) -> None:
        def penalty_fn(x, y):
            return np.where(np.asarray(x) < 0.0, 3.0, 0.0), np.full(len(x), 1.5)

        coarse = build_graph(*line_graph([0.0, 2000.0], y=30.0))
        fine = build_graph(*line_graph([0.0, 500.0, 1000.0, 1500.0, 2000.0], y=30.0))
        cells = [(-60.0, -25.0), (975.0, -25.0)]
        for graph in (coarse, fine):
            result = score(graph, cells, penalty_fn=penalty_fn)
            self.assertEqual([cell.score.penalties["railway_proximity"] for cell in result.cells], [3.0, 0.0])
            self.assertEqual([cell.score.penalties["road_proximity"] for cell in result.cells], [1.5, 1.5])


class SameEdgeAndIdentityTests(TestCase):
    def test_cell_and_object_on_one_edge_walk_directly(self) -> None:
        graph = build_graph(*line_graph([0.0, 3000.0]))
        places = [place("pharmacy", "pharmacy", 1.0, "p", 1560.0, -30.0)]
        cell = score(graph, [(1475.0, 15.0)], places=places).cells[0]
        (_, distance), = cell.best_by_kind["pharmacy"]
        # Connector 40 m + 60 m along + 30 m, nowhere near an endpoint.
        self.assertAlmostEqual(distance, math.hypot(60.0, 70.0), delta=0.05)  # straight-line reflection bound
        self.assertLess(distance, 140.0)

    def test_one_object_through_many_entries_counts_once(self) -> None:
        # A grid: the supermarket is reachable through every edge around it.
        coords = {}
        edges = []
        for row in range(4):
            for col in range(4):
                coords[row * 4 + col] = (col * 100.0, row * 100.0)
                if col < 3:
                    edges.append((row * 4 + col, row * 4 + col + 1))
                if row < 3:
                    edges.append((row * 4 + col, (row + 1) * 4 + col))
        graph = build_graph(coords, edges)
        two_members = LogicalPlace("food_full|a,b", "food_full", "food", 1.0, [
            {"source": "osm_local_pbf", "source_ref": "N/1", "x": 150.0, "y": 150.0},
            {"source": "overture_places", "source_ref": "x", "x": 160.0, "y": 155.0},
        ])
        cell = score(graph, [(125.0, 125.0)], places=[two_members]).cells[0]
        self.assertAlmostEqual(cell.score.service_access["food"], 0.8)  # one place, not 0.95/1.0
        self.assertEqual(len(cell.best_by_kind["food_full"]), 1)

    def test_two_shops_and_one_duplicated_shop(self) -> None:
        graph = build_graph(*line_graph([0.0, 400.0]))
        rows = [
            {"service_kind": "other_retail", "need": "other_retail", "source": "osm_local_pbf", "source_ref": "N/1",
             "x": 100.0, "y": 10.0, "name": "Books", "class_reason": "other_retail"},
            {"service_kind": "other_retail", "need": "other_retail", "source": "osm_local_pbf", "source_ref": "N/2",
             "x": 105.0, "y": 10.0, "name": "Shoes", "class_reason": "other_retail"},
            {"service_kind": "other_retail", "need": "other_retail", "source": "overture_places", "source_ref": "o1",
             "x": 110.0, "y": 12.0, "name": "Shoes", "class_reason": "overture_other_retail", "identity_refs": ("osm:N/2",)},
        ]
        places = build_logical_places(rows)
        self.assertEqual(len(places), 2)
        cell = score(graph, [(75.0, -25.0)], places=places).cells[0]
        self.assertAlmostEqual(cell.score.service_access["other_retail"], 0.95)  # two places: 0.80 + 0.15
        keys = {cell_key for cell_key in (score(graph, [(75.0, -25.0)], places=places).labels.as_json()["other_retail"])
                for cell_key in [cell_key["key"]]}
        self.assertEqual(keys, {"other_retail|osm:N/1", "other_retail|osm:N/2,overture:o1"})

    def test_labels_are_logical_ids_for_every_kind(self) -> None:
        graph = build_graph(*line_graph([0.0, 400.0]))
        stops = [TransportStop("gtfs/nta/B", "nta", 3, 300.0, 5.0), TransportStop("gtfs/translink/A", "translink", 3, 50.0, 5.0)]
        result = score(graph, [(25.0, -25.0)], stops=stops)
        table = result.labels.as_json()["transport_tier_3"]
        self.assertEqual([(entry["label"], entry["key"]) for entry in table], [(0, "gtfs:nta:B"), (1, "gtfs:translink:A")])
        (label, _), = result.cells[0].best_by_kind["transport_tier_3"]
        self.assertEqual(result.labels.key_of("transport_tier_3", label), "gtfs:translink:A")


class NetworkStatusAndAreaTests(TestCase):
    def test_no_network_cell_is_valid_zero_with_its_own_penalties(self) -> None:
        graph = build_graph(*line_graph([0.0, 100.0]))
        cell = score(graph, [(2000.0, 2000.0)], penalty_fn=lambda x, y: (np.full(len(x), 2.0), np.zeros(len(x)))).cells[0]
        self.assertEqual(cell.network_status, "no_walking_network")
        self.assertEqual(cell.quantized["total"], 0)
        self.assertEqual(set(cell.quantized["service_access"].values()), {0})
        self.assertEqual(set(cell.quantized["components"].values()), {0})
        self.assertEqual(cell.quantized["penalties"]["railway_proximity"], 200)

    def test_network_without_services_is_ok_with_zero_access(self) -> None:
        cell = score(build_graph(*line_graph([0.0, 100.0])), [(-25.0, -25.0)]).cells[0]
        self.assertEqual(cell.network_status, "ok")
        self.assertEqual(cell.score.total, 0.0)

    def test_coastal_cell_reaches_the_far_arm_only_through_land(self) -> None:
        # U-shaped land around a 40 m channel; a shop on each arm.
        land = LandMask(Polygon([(0, 0), (1000, 0), (1000, 600), (520, 600), (520, 200), (480, 200), (480, 600), (0, 600)]))
        graph = build_graph({0: (200.0, 300.0), 1: (470.0, 300.0), 2: (530.0, 300.0), 3: (800.0, 300.0)}, [(0, 1), (2, 3)])
        places = [place("food_full", "food", 1.0, "w", 300.0, 320.0), place("food_full", "food", 1.0, "e", 700.0, 320.0)]
        cell = score(graph, [(475.0, 75.0)], land=land, places=places).cells[0]  # origin (500, 100)
        self.assertEqual(cell.network_status, "ok")
        self.assertEqual(len(cell.best_by_kind["food_full"]), 2)
        # Water cell: no origin; partial coastal cell: point on its land part.
        self.assertEqual(score(graph, [(475.0, 650.0)], land=land).cells[0].network_status, "outside_mask")
        partial = score(graph, [(510.0, 400.0)], land=land).cells[0]
        self.assertEqual(partial.network_status, "ok")
        self.assertGreaterEqual(partial.origin[0], 520.0)

    def test_far_bbox_changes_leave_the_control_cell_alone(self) -> None:
        coords, edges = line_graph([0.0, 400.0, 800.0])
        places = [place("food_full", "food", 1.0, "s", 600.0, 20.0)]
        stops = [TransportStop("gtfs/nta/1", "nta", 5, 300.0, -10.0)]
        control = score(build_graph(coords, edges), [(75.0, -25.0)], places=places, stops=stops).cells[0]
        # 5 km away: graph, a shop and a stop beyond every cutoff + both connectors.
        coords.update({20: (5800.0, 0.0), 21: (6200.0, 0.0)})
        edges.append((20, 21))
        far_places = places + [place("food_full", "food", 1.0, "far", 6000.0, 10.0)]
        far_stops = stops + [TransportStop("gtfs/nta/2", "nta", 5, 6000.0, -10.0)]
        wider = score(build_graph(coords, edges), [(75.0, -25.0)], places=far_places, stops=far_stops).cells[0]
        self.assertEqual(control.quantized, wider.quantized)

    def test_green_roots_merge_by_max_across_kinds(self) -> None:
        graph = build_graph(*line_graph([0.0, 100.0, 200.0]))

        def site(key, kind, quality, root):
            return GreenSite(key, 0, f"root-{root}", root, "park", "x", quality, kind, True, False, 1.0, (key,), None, None)

        sites = [site("big", "green_large", 1.0, 0), site("play", "green_playground", 0.4, 0), site("other", "green_pocket", 0.6, 1)]
        approaches = {"big": [(2, 0.0)], "play": [(1, 0.0)], "other": [(1, 0.0)]}
        cell = score(graph, [(-25.0, -25.0)], green_sites=sites, park_approaches=approaches).cells[0]
        # Root 0 -> max(1.0, 0.4) = 1.0; root 1 -> 0.6.
        self.assertAlmostEqual(cell.score.service_access["green"], 0.8 * 1.0 + 0.15 * 0.6)

    def test_park_interior_edge_is_entered_directly(self) -> None:
        graph = build_graph(*line_graph([0.0, 3000.0]))
        sites = [GreenSite("p", 0, "p", 0, "park", "x", 1.0, "green_large", True, False, 1.0, ("p",), None, None)]
        # The park covers 1400..1600 m of the long edge; its approach seeds sit at the far endpoints.
        approaches = {"p": [(0, 1400.0), (1, 1400.0)]}
        intervals = {"p": [(0, 1400.0, 1600.0)]}
        cell = score(graph, [(1475.0, 5.0)], green_sites=sites, park_approaches=approaches, park_intervals=intervals).cells[0]
        (_, distance), = cell.best_by_kind["green_large"]
        self.assertAlmostEqual(distance, 30.0, places=6)  # straight onto the in-park path

    def test_transport_tiers_and_feed_losses(self) -> None:
        graph = build_graph(*line_graph([0.0, 100.0]))
        stops = [
            TransportStop("gtfs/nta/a", "nta", 2, 100.0, 0.0),
            TransportStop("gtfs/nta/b", "nta", 5, 9000.0, 9000.0),  # no edge within 250 m
        ]
        result = score(graph, [(-25.0, -25.0)], stops=stops)
        self.assertAlmostEqual(result.cells[0].score.service_access["transport"], 0.30)
        self.assertEqual(result.losses["transport_feed:nta"]["reasons"], {"no_edge_within_radius": 1})
        self.assertTrue(any("transport_feed:nta" in warning for warning in result.warnings))

    def test_reach_rounding_error_stays_below_one_hundredth_of_a_point(self) -> None:
        rng = random.Random(3)
        coords = {node: (rng.uniform(0, 1500), rng.uniform(0, 1500)) for node in range(60)}
        edges = sorted({tuple(sorted(rng.sample(range(60), 2))) for _ in range(160)})
        graph = build_graph(coords, edges)
        places = [
            place(kind, need, quality, f"{kind}{index}", rng.uniform(0, 1500), rng.uniform(0, 1500))
            for index in range(5)
            for kind, need, quality in (("food_full", "food", 1.0), ("pharmacy", "pharmacy", 1.0), ("other_retail", "other_retail", 1.0))
        ]
        cells = [(x, y) for x in range(0, 1500, 150) for y in range(0, 1500, 150)]
        quantized = score(graph, cells, places=places)
        raw = score(graph, cells, places=places, use_raw_distances=True)
        for a, b in zip(quantized.cells, raw.cells):
            self.assertLessEqual(abs(a.score.total - b.score.total), 0.01)


def brute_force_distances(graph: ReferenceGraph, land: Polygon, cell_xy, objects_xy, *, step=0.5, radius=250.0):
    """Cell -> object network distances on an edge-subdivided graph.

    Independent of the kernel: every edge is cut into ``step`` pieces, the
    cell and each object connect to every sample point whose straight
    connector is within ``radius`` and covered by ``land`` (shapely), and a
    plain Dijkstra runs from the object's virtual node. Virtual nodes only
    start or end a path.
    """
    counts = np.diff(graph.offsets)
    src = np.repeat(np.arange(len(counts)), counts)
    node_count = len(graph.node_x)
    xs, ys, rows, cols, weights = [], [], [], [], []
    for slot in np.flatnonzero(src < graph.targets):
        a, b = int(src[slot]), int(graph.targets[slot])
        ax, ay, bx, by = graph.node_x[a], graph.node_y[a], graph.node_x[b], graph.node_y[b]
        geom = math.hypot(bx - ax, by - ay)
        c = float(graph.lengths[slot]) / geom
        pieces = max(1, int(math.ceil(geom / step)))
        base = node_count + len(xs)
        for i in range(pieces + 1):
            t = i / pieces
            xs.append(ax + t * (bx - ax))
            ys.append(ay + t * (by - ay))
            if i:
                rows.append(base + i - 1)
                cols.append(base + i)
                weights.append(c * geom / pieces)
        # The first and last samples are the real nodes (zero-length links).
        rows.extend([base, base + pieces])
        cols.extend([a, b])
        weights.extend([0.0, 0.0])
    source = node_count + len(xs)
    pts = np.column_stack([xs, ys])

    def connectors(x, y):
        dist = np.hypot(pts[:, 0] - x, pts[:, 1] - y)
        near = np.flatnonzero(dist <= radius)
        if not len(near):
            return near, dist[near]
        lines = shapely.linestrings([[(x, y), (pts[i, 0], pts[i, 1])] for i in near])
        ok = shapely.covers(land, lines)
        return near[ok], dist[near[ok]]

    cell_samples, cell_dist = connectors(*cell_xy)
    out = []
    for ox, oy in objects_xy:
        obj_samples, obj_dist = connectors(ox, oy)
        if not len(obj_samples) or not len(cell_samples):
            out.append(math.inf)
            continue
        # Network edges both ways; the object source only leaves.
        r = rows + cols + [source] * len(obj_samples)
        c_ = cols + rows + [node_count + int(i) for i in obj_samples]
        w = weights + weights + obj_dist.tolist()
        matrix = coo_matrix((np.asarray(w) + 1e-12, (r, c_)), shape=(source + 1, source + 1)).tocsr()
        dist_from_object = dijkstra(matrix, directed=True, indices=source)
        out.append(float(np.min(dist_from_object[node_count + cell_samples] + cell_dist)))
    return out


class BruteForceHarnessTests(TestCase):
    def test_reference_matches_subdivided_graph_with_on_edge_cases(self) -> None:
        rng = random.Random(17)
        land_poly = Polygon([(-300, -300), (1300, -300), (1300, 1300), (560, 1300), (560, 500), (440, 500), (440, 1300), (-300, 1300)])
        land = LandMask(land_poly)
        compared = 0
        for trial in range(10):
            coords = {node: (rng.uniform(0, 1000), rng.uniform(0, 1000)) for node in range(9)}
            edges = sorted({tuple(sorted(rng.sample(range(9), 2))) for _ in range(14)})
            graph = build_graph(coords, edges)
            objects = []
            for index in range(4):
                a, b = edges[rng.randrange(len(edges))]
                t = rng.uniform(0.2, 0.8)
                x = coords[a][0] + t * (coords[b][0] - coords[a][0]) + rng.uniform(-40, 40)
                y = coords[a][1] + t * (coords[b][1] - coords[a][1]) + rng.uniform(-40, 40)
                if land_poly.contains(Point(x, y)):
                    objects.append((f"s{index}", x, y))
            a, b = edges[0]
            cell_xy = (coords[a][0] + 0.5 * (coords[b][0] - coords[a][0]) + 15.0,
                       coords[a][1] + 0.5 * (coords[b][1] - coords[a][1]) - 10.0)
            if not land_poly.contains(Point(cell_xy)) or not objects:
                continue
            places = [place("food_full", "food", 1.0, ref, x, y) for ref, x, y in objects]
            result = score(graph, [(cell_xy[0] - 25.0, cell_xy[1] - 25.0)], land=land, places=places, use_raw_distances=True)
            got = {result.labels.key_of("food_full", label): distance for label, distance in result.cells[0].best_by_kind.get("food_full", [])}
            expected = brute_force_distances(graph, land_poly, cell_xy, [(x, y) for _, x, y in objects])
            ranked = sorted(zip(expected, (f"food_full|test:{ref}" for ref, _, _ in objects)))
            for distance, key in ranked[:3]:
                if distance > 1600.0:
                    continue
                self.assertIn(key, got, (trial, ranked, got))
                self.assertLessEqual(got[key], distance + 1e-6, (trial, key))
                self.assertGreaterEqual(got[key], distance - 0.6, (trial, key))
                compared += 1
        self.assertGreaterEqual(compared, 8)


class EdgeFilterTests(TestCase):
    def test_grid_filter_keeps_every_edge_touching_an_envelope(self) -> None:
        rng = random.Random(11)
        coords = {node: (rng.uniform(0, 5000), rng.uniform(0, 5000)) for node in range(300)}
        edges = sorted({tuple(sorted(rng.sample(range(300), 2))) for _ in range(600)})
        graph = build_graph(coords, edges)
        envelopes = [(1000.0, 1000.0, 1300.0, 1200.0), (3000.0, 4000.0, 3050.0, 4020.0)]
        kept = set(candidate_edge_slots(graph.node_x, graph.node_y, graph.offsets, graph.targets, envelopes).tolist())
        counts = np.diff(graph.offsets)
        src = np.repeat(np.arange(len(counts)), counts)
        for slot in range(len(graph.targets)):
            a, b = int(src[slot]), int(graph.targets[slot])
            if a >= b:
                continue
            segment = shapely.LineString([coords[a], coords[b]])
            if any(segment.intersects(box(*env)) for env in envelopes):
                self.assertIn(slot, kept)


@unittest.skipUnless(
    os.getenv("DAILY_ACCESS_DB_TESTS") == "1",
    "set DAILY_ACCESS_DB_TESTS=1 to run against the local PostGIS (rolled back)",
)
class ParkApproachPostgisTests(TestCase):
    def test_entries_touching_nested_and_national_park_rules(self) -> None:
        import json

        from config import OSM_IMPORT_SCHEMA, TO_TARGET, TO_WGS84
        from db_postgis._dependencies import text
        from db_postgis.daily_access_green import stage_green_sites, stage_park_approaches
        from db_postgis.engine import build_engine

        fingerprint = f"daily-access-test-{uuid.uuid4().hex}"
        x0, y0 = TO_TARGET(-6.26, 53.35)

        def wkt(minx, miny, maxx, maxy):
            corners = [TO_WGS84(x0 + x, y0 + y) for x, y in ((minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny))]
            return "POLYGON((" + ",".join(f"{lon} {lat}" for lon, lat in corners) + "))"

        features = [
            ("W", 1, {"leisure": "park"}, wkt(0, 0, 100, 100)),
            ("W", 2, {"boundary": "national_park"}, wkt(1000, 0, 1400, 400)),
            ("W", 3, {"leisure": "park"}, wkt(3000, 0, 3100, 100)),  # only touched
        ]
        local = {
            0: (-50.0, 50.0), 1: (50.0, 50.0), 2: (50.0, -30.0), 3: (50.0, 130.0),
            4: (950.0, 200.0), 5: (1100.0, 200.0), 6: (1200.0, 100.0), 7: (1300.0, 100.0),
            8: (3000.0, -50.0), 9: (3000.0, 150.0),
        }
        coords = {node: (x0 + x, y0 + y) for node, (x, y) in local.items()}
        graph = build_graph(coords, [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9)])
        flags = np.zeros(len(graph.targets), dtype=np.uint8)
        counts = np.diff(graph.offsets)
        src = np.repeat(np.arange(len(counts)), counts)
        for slot in range(len(graph.targets)):
            if {int(src[slot]), int(graph.targets[slot])} == {4, 5}:
                flags[slot] = 1

        engine = build_engine()
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                for osm_type, osm_id, tags, polygon in features:
                    connection.execute(
                        text(
                            f"""
                            INSERT INTO {OSM_IMPORT_SCHEMA}.features
                                (osm_type, osm_id, import_fingerprint, category, name, tags_json, geom, created_at)
                            VALUES (:t, :i, :fp, 'parks', NULL, CAST(:tags AS jsonb), ST_GeomFromText(:wkt, 4326), now())
                            """
                        ),
                        {"t": osm_type, "i": osm_id, "fp": fingerprint, "tags": json.dumps(tags), "wkt": polygon},
                    )
                study = box(*TO_WGS84(x0 - 200, y0 - 200), *TO_WGS84(x0 + 3200, y0 + 500))
                sites, _ = stage_green_sites(connection, import_fingerprint=fingerprint, study_area_wgs84=study)
                approaches, intervals, stats = stage_park_approaches(
                    connection, sites, node_x=graph.node_x, node_y=graph.node_y,
                    offsets=graph.offsets, targets=graph.targets, lengths=graph.lengths, edge_flags=flags,
                )
            finally:
                transaction.rollback()

        by_type = {site.semantic_type + ("3" if "W/3" in site.site_key else ""): site.site_key for site in sites}
        park = dict(approaches[by_type["park"]])
        self.assertEqual(park[1], 0.0)
        self.assertAlmostEqual(park[0], 50.0, delta=0.5)  # entry at the boundary
        self.assertAlmostEqual(park[2], 30.0, delta=0.5)  # pass-through edge
        self.assertAlmostEqual(park[3], 30.0, delta=0.5)
        national = dict(approaches[by_type["national_park"]])
        self.assertEqual(national[5], 0.0)
        self.assertAlmostEqual(national[4], 50.0, delta=0.5)
        self.assertNotIn(6, national)  # ordinary road inside gets no zero distance
        self.assertNotIn(by_type["park3"], approaches)  # boundary touch only
        self.assertEqual(stats["unknown_access_sites"], 1)
        # In-park parts of edges are zero-cost same-edge intervals (edge id, lo, hi).
        park_parts = intervals[by_type["park"]]
        self.assertTrue(any(abs(hi - lo - 100.0) < 0.5 for _, lo, hi in park_parts), park_parts)


if __name__ == "__main__":
    unittest.main()
