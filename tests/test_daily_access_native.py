from __future__ import annotations

import json
import os
import random
import tempfile
import unittest
from pathlib import Path
from unittest import TestCase

import numpy as np
from shapely.geometry import Polygon, box

from config import TO_WGS84
from db_postgis.daily_access_green import GreenSite
from precompute.daily_access_classify import LogicalPlace
from precompute.daily_access_native import compare_with_reference, compute_native
from precompute.daily_access_reference import TransportStop, compute_reference, load_reference_graph
from precompute.daily_access_snap import LandMask, graph_components
from config import WALKGRAPH_BIN
from walkgraph_support import ensure_walkgraph_subcommand_available

# Somewhere near Cork, in ITM metres.
X0, Y0 = 567_000.0, 571_000.0


def _native_available() -> bool:
    try:
        ensure_walkgraph_subcommand_available(WALKGRAPH_BIN, "daily-access-score")
    except Exception:
        return False
    return True


def write_walkgraph(graph_dir: Path, coords: list[tuple[float, float]], edges: list[tuple[int, int]]) -> None:
    """A format-4 walk graph: f32 lat/lon nodes, symmetric CSR, components."""
    graph_dir.mkdir(parents=True, exist_ok=True)
    lonlat = [TO_WGS84(X0 + x, Y0 + y) for x, y in coords]
    nodes = np.zeros(len(coords), dtype=[("lat", "<f4"), ("lon", "<f4")])
    nodes["lat"] = [lat for _, lat in lonlat]
    nodes["lon"] = [lon for lon, _ in lonlat]
    nodes.tofile(graph_dir / "walk_graph.nodes.bin")
    # Lengths like walkgraph: from the stored (f32) coordinates.
    from config import TO_TARGET

    metric = [TO_TARGET(float(lon), float(lat)) for lat, lon in zip(nodes["lat"], nodes["lon"])]
    adjacency: list[list[tuple[int, float]]] = [[] for _ in coords]
    for a, b in edges:
        length = float(np.float32(np.hypot(metric[a][0] - metric[b][0], metric[a][1] - metric[b][1])))
        adjacency[a].append((b, length))
        adjacency[b].append((a, length))
    offsets, targets, lengths = [0], [], []
    for neighbours in adjacency:
        for target, length in neighbours:
            targets.append(target)
            lengths.append(length)
        offsets.append(len(targets))
    np.asarray(offsets, dtype="<u8").tofile(graph_dir / "walk_graph.adjacency_offsets.bin")
    np.asarray(targets, dtype="<u4").tofile(graph_dir / "walk_graph.adjacency_targets.bin")
    np.asarray(lengths, dtype="<f4").tofile(graph_dir / "walk_graph.adjacency_lengths.bin")
    np.zeros(len(targets), dtype="u1").tofile(graph_dir / "walk_graph.edge_flags.bin")
    components = graph_components(np.asarray(offsets), np.asarray(targets), len(coords))
    components.astype("<u4").tofile(graph_dir / "walk_graph.components.bin")
    (graph_dir / "walk_graph.meta.json").write_text(
        json.dumps(
            {
                "format_version": 4,
                "graph_profile": "walk",
                "extract_fingerprint": None,
                "pbf_path": "synthetic",
                "pbf_size": 0,
                "pbf_mtime_ns": 0,
                "bbox": None,
                "bbox_padding_m": 0.0,
                "node_count": len(coords),
                "edge_count": len(targets),
                "component_count": int(components.max()) + 1,
                "created_utc": "2026-10-10T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )


@unittest.skipUnless(_native_available(), "walkgraph binary without daily-access subcommands")
class NativeParityTests(TestCase):
    def test_native_matches_reference(self) -> None:
        rng = random.Random(42)
        # A main grid network, long straight edges (entries mid-edge), short
        # fragments; land is a box with a 200 m wide bay cut into it.
        coords: list[tuple[float, float]] = []
        edges: list[tuple[int, int]] = []
        size, step = 10, 150.0
        for row in range(size):
            for col in range(size):
                coords.append((col * step + rng.uniform(-20, 20), row * step + rng.uniform(-20, 20)))
        for row in range(size):
            for col in range(size):
                node = row * size + col
                if col + 1 < size and rng.random() < 0.85:
                    edges.append((node, node + 1))
                if row + 1 < size and rng.random() < 0.85:
                    edges.append((node, node + size))
        for y in (-150.0, 1600.0):
            start = len(coords)
            coords.extend([(-300.0, y), (1800.0, y)])  # 2.1 km edges
            edges.append((start, start + 1))
        for _ in range(15):
            x, y = rng.uniform(0, size * step), rng.uniform(0, size * step)
            start = len(coords)
            coords.extend([(x, y), (x + rng.uniform(10, 60), y + rng.uniform(10, 60))])
            edges.append((start, start + 1))
        land = LandMask(
            Polygon(
                [
                    (X0 - 500, Y0 - 500), (X0 + 2300, Y0 - 500), (X0 + 2300, Y0 + 2300),
                    (X0 + 900, Y0 + 2300), (X0 + 900, Y0 + 800), (X0 + 700, Y0 + 800),
                    (X0 + 700, Y0 + 2300), (X0 - 500, Y0 + 2300),
                ]
            )
        )

        with tempfile.TemporaryDirectory() as temp:
            graph_dir = Path(temp) / "graph"
            write_walkgraph(graph_dir, coords, edges)
            graph = load_reference_graph(graph_dir)

            def point(index: int) -> dict:
                return {"x": float(graph.node_x[index]) + rng.uniform(-30, 30), "y": float(graph.node_y[index]) + rng.uniform(-30, 30)}

            kinds = [
                ("food_full", "food", 1.0), ("food_convenience", "food", 0.65), ("food_specialty", "food", 0.35),
                ("other_retail", "other_retail", 1.0), ("primary_care_gp", "primary_care", 1.0),
                ("primary_care_clinic", "primary_care", 0.6), ("pharmacy", "pharmacy", 1.0),
            ]
            places = []
            for index in range(40):
                kind, need, quality = kinds[index % len(kinds)]
                members = [dict(point(rng.randrange(len(coords))), source="test", source_ref=f"m{index}-{m}") for m in range(rng.choice((1, 1, 2)))]
                if index % 5 == 0:  # polygonal footprint
                    cx, cy = members[0]["x"], members[0]["y"]
                    members[0]["footprint_wkb"] = box(cx - 15, cy - 10, cx + 25, cy + 12).wkb
                places.append(LogicalPlace(f"{kind}|p{index:02d}", kind, need, quality, members))
            # Objects beside the middle of the long edges.
            places.append(LogicalPlace("food_full|long", "food_full", "food", 1.0, [{"x": X0 + 750.0, "y": Y0 - 120.0, "source": "test", "source_ref": "long"}]))
            stops = [
                TransportStop(f"gtfs/nta/s{index}", "nta", rng.randint(0, 5), **{k: v for k, v in point(rng.randrange(len(coords))).items()})
                for index in range(30)
            ]
            green_kinds = [("green_playground", 0.4), ("green_pocket", 0.6), ("green_neighbourhood", 0.85), ("green_large", 1.0)]
            green_sites, approaches, intervals = [], {}, {}
            for index in range(12):
                kind, quality = green_kinds[index % 4]
                key = f"site{index}"
                green_sites.append(
                    GreenSite(key, index, f"root{index % 5}", index % 5, "park", "x", quality, kind, True, False, 1.0, (key,), None, None)
                )
                approaches[key] = [(rng.randrange(len(coords)), rng.uniform(0, 40)) for _ in range(rng.randint(0, 3))]
            # A park around the middle of the southern long edge (edge id of that pair).
            long_a = size * size
            from precompute.daily_access_snap import undirected_edges

            slots, edge_a, edge_b = undirected_edges(graph.offsets, graph.targets)
            long_edge = int(np.flatnonzero((edge_a == long_a) & (edge_b == long_a + 1))[0])
            green_sites.append(GreenSite("park-long", 99, "root-long", 99, "park", "x", 1.0, "green_large", True, False, 1.0, ("park-long",), None, None))
            approaches["park-long"] = [(long_a, 1000.0), (long_a + 1, 1000.0)]
            intervals["park-long"] = [(long_edge, 1000.0, 1100.0)]

            def penalty_fn(x, y):
                x = np.asarray(x)
                return np.where(x < X0 + 300.0, 2.5, 0.0), np.where(np.asarray(y) > Y0 + 1000.0, 1.25, 0.0)

            cells = [(X0 - 400 + x, Y0 - 400 + y) for x in range(0, 2400, 50) for y in range(0, 2400, 50)]
            kwargs = dict(
                land_mask=land, cells=cells, cell_size_m=50.0, places=places, green_sites=green_sites,
                park_approaches=approaches, park_intervals=intervals, stops=stops, penalty_fn=penalty_fn,
            )
            reference = compute_reference(graph=graph, **kwargs)
            native = compute_native(graph_dir=graph_dir, **kwargs)

        report = compare_with_reference(reference, native)
        if os.getenv("DAILY_ACCESS_PARITY_REPORT"):
            print(json.dumps(report, indent=1))
        self.assertTrue(report["within_limits"], report)
        self.assertEqual(report["status_mismatches"], 0, report)
        self.assertLessEqual(report["max_reach_step_diff"], 1, report)
        self.assertEqual(report["reach_label_mismatches"], 0, report)
        self.assertFalse(report["origin_mismatch"], report)
        self.assertEqual(reference.losses, native.losses)
        statuses = set(native.network_status.tolist())
        self.assertTrue({0, 1, 2} <= statuses, statuses)  # outside, ok and no-network cells all exercised


if __name__ == "__main__":
    unittest.main()
