from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import TestCase, mock

import numpy as np
from shapely.geometry import Polygon, box

import main
from precompute import daily_access_build as build
from precompute.surface import ShardEntry


def _entry(x0: int = 0, y0: int = 0, size: int = 200) -> ShardEntry:
    cells = size // 50
    return ShardEntry(
        shard_id="s", x_min_m=x0, y_min_m=y0, x_max_m=x0 + size, y_max_m=y0 + size,
        rows=cells, cols=cells, path="unused",
    )


class DailyAccessBuildTests(TestCase):
    def test_valhalla_is_rejected_before_heavy_phases(self) -> None:
        with self.assertRaisesRegex(build.DailyAccessBuildError, "not supported"):
            build.ensure_supported_routing_backend("valhalla")
        build.ensure_supported_routing_backend("walkgraph")
        with (
            mock.patch.object(build, "WALK_ROUTING_BACKEND", "valhalla"),
            mock.patch("db_postgis.build_engine") as engine,
        ):
            with self.assertRaises(build.DailyAccessBuildError):
                build.run_daily_access_build("test")
        engine.assert_not_called()

    def test_tier_keys_chain_and_weights_only_rebuild_score(self) -> None:
        base = dict(
            extract_fingerprint="x", graph_bbox=(51.0, -9.0, 52.0, -8.0), graph_padding_m=2250.0,
            geo_hash="g", mask_identity="m", reach_inputs="r",
        )
        keys = build.tier_keys(**base)
        self.assertEqual(keys, build.tier_keys(**base))
        self.assertEqual(keys, build.tier_keys(**base, mode="walk"))
        bike = build.tier_keys(**base, mode="bike")
        self.assertNotEqual(bike.graph, keys.graph)
        self.assertNotEqual(bike.shell, keys.shell)
        self.assertNotEqual(bike.reach, keys.reach)
        self.assertNotEqual(bike.score, keys.score)
        with mock.patch.dict(
            build.DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE["bike"],
            {"food": 4100.0},
        ):
            changed_bike_reach = build.tier_keys(**base, mode="bike")
            unchanged_walk = build.tier_keys(**base)
        self.assertNotEqual(changed_bike_reach.reach, bike.reach)
        self.assertEqual(unchanged_walk, keys)
        weights = build.tier_keys(**base, score_inputs="other-weights")
        self.assertEqual((weights.graph, weights.shell, weights.reach), (keys.graph, keys.shell, keys.reach))
        self.assertNotEqual(weights.score, keys.score)
        with mock.patch("precompute.daily_access_build.daily_access_score_params", return_value={"category_max": {"shops": 40}}):
            reweighted = build.tier_keys(**base)
        self.assertEqual(reweighted.reach, keys.reach)
        self.assertNotEqual(reweighted.score, keys.score)
        inputs = build.tier_keys(**{**base, "reach_inputs": "r2"})  # new objects / green grouping
        self.assertEqual(inputs.shell, keys.shell)
        self.assertNotEqual(inputs.reach, keys.reach)
        self.assertNotEqual(inputs.score, keys.score)
        with mock.patch("precompute.daily_access_build.daily_access_geometry_params", return_value={"version": 99}):
            geometry = build.tier_keys(**base)
        self.assertNotEqual(geometry.shell, keys.shell)
        self.assertNotEqual(build.tier_keys(**{**base, "mask_identity": "m2"}).shell, keys.shell)

    def test_land_area_is_exact_and_row_major(self) -> None:
        entry = _entry()
        corners = build.shard_cell_corners(entry)
        self.assertEqual(corners[1].tolist(), [50.0, 0.0])
        self.assertEqual(corners[4].tolist(), [0.0, 50.0])
        study = Polygon([(0, 0), (200, 0), (0, 200)])
        area = build.shard_land_area(entry, corners, study).reshape(4, 4)
        self.assertEqual(area[0, 0], 2500.0)
        self.assertEqual(area[0, 3], 1250.0)  # diagonal cell: exactly half
        self.assertEqual(area[3, 3], 0.0)
        self.assertAlmostEqual(float(area.sum()), 20_000.0, places=3)
        inland = build.shard_land_area(entry, corners, box(-10, -10, 300, 300))
        self.assertTrue(np.all(inland == 2500.0))

    def test_reach_cache_round_trip_keeps_real_ids_for_every_kind(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            kinds = []
            for kind, need, k in (("food_full", "food", 3), ("green_pocket", "green", 3), ("transport_tier_5", "transport", 1)):
                distances = np.asarray([[10, 20, 65535], [5, 65535, 65535]], dtype="<u2")[:, :k]
                labels = np.asarray([[7, 9, 2**32 - 1], [4, 2**32 - 1, 2**32 - 1]], dtype="<u4")[:, :k]
                distances.tofile(temp_dir / f"{kind}.d")
                labels.tofile(temp_dir / f"{kind}.l")
                kinds.append({"kind": kind, "kind_index": 0, "need": need, "quality": 1.0, "k": k, "cutoff_m": 1600.0,
                              "out_distances": str(temp_dir / f"{kind}.d"), "out_labels": str(temp_dir / f"{kind}.l")})
            reach_dir = temp_dir / "reach"
            reach_dir.mkdir()
            table = {"food_full": [{"label": 7, "key": "food_full|osm:N/1", "members": ["osm:N/1"]}]}
            build.save_reach_cache(reach_dir, np.asarray([3, 8]), kinds, table)
            origins, restored, rows = build.load_reach_cache(reach_dir, mmap=False)
            self.assertEqual(origins.tolist(), [3, 8])
            self.assertEqual(rows["food_full"][1][0].tolist(), [7, 9, 2**32 - 1])  # not column numbers
            self.assertEqual(rows["transport_tier_5"][1][:, 0].tolist(), [7, 4])
            self.assertEqual(build.load_label_table(reach_dir), table)

    def test_cli_dispatches_daily_access(self) -> None:
        with mock.patch("precompute.daily_access_build.run_daily_access_build", return_value={"ok": 1}) as run:
            self.assertEqual(main.main(["daily-access", "--profile", "test", "--allow-missing-transport"]), 0)
        run.assert_called_once_with("test", force=False, allow_missing_transport=True, publish=False)
        with mock.patch("precompute.daily_access_build.run_daily_access_build", return_value={"ok": 1}) as run:
            self.assertEqual(main.main(["daily-access", "--profile", "dev", "--publish"]), 0)
        run.assert_called_once_with("dev", force=False, allow_missing_transport=False, publish=True)

    def test_zero_usable_gtfs_blocks_without_override(self) -> None:
        losses = {"transport": {"eligible": 3, "unbound": 3}}
        with self.assertRaisesRegex(build.DailyAccessBuildError, "allow-missing-transport"):
            build.transport_gate(losses, allow_missing_transport=False)
        self.assertTrue(build.transport_gate(losses, allow_missing_transport=True))
        with self.assertRaises(build.DailyAccessBuildError):
            build.transport_gate({}, allow_missing_transport=False)
        self.assertEqual(build.transport_gate({"transport": {"eligible": 3, "unbound": 1}}, allow_missing_transport=False), [])

    def test_incomplete_or_corrupt_shards_are_never_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            tier = Path(temp) / "score"
            build.begin_tier(tier, "score", "k", {})
            (tier / "shards").mkdir()
            np.savez_compressed(tier / "shards" / "a.npz", total=np.zeros((2, 2), dtype=np.uint16))
            self.assertIn("status 'building'", build.tier_problems(tier, key="k", shard_ids=["a"]))
            build.complete_tier(tier, {"stats": {}})
            self.assertEqual(build.tier_problems(tier, key="k", shard_ids=["a"], fields=["total"]), [])
            self.assertTrue(build.tier_problems(tier, key="k", shard_ids=["a", "b"]))  # missing shard
            self.assertTrue(build.tier_problems(tier, key="k", shard_ids=["a"], fields=["components"]))
            (tier / "shards" / "a.npz").write_bytes(b"truncated")
            problems = build.tier_problems(tier, key="k", shard_ids=["a"])
            self.assertTrue(any("checksum" in problem for problem in problems), problems)
            self.assertTrue(build.tier_problems(tier, key="other"))

    def test_reach_format_bump_does_not_invalidate_the_geometry_shell(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            shell_dir = Path(temp) / "shell"
            build.begin_tier(shell_dir, "shell", "shell-key", {})
            build.complete_tier(shell_dir, {}, shard_dir=None)
            manifest_path = shell_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["reach_cache_version"] = 2
            manifest["score_model_version"] = "old-score"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            self.assertEqual(build.tier_problems(shell_dir, key="shell-key"), [])


from tests.test_daily_access_native import X0, Y0, _native_available, write_walkgraph  # noqa: E402
from tests.test_daily_access_runtime import write_empty_reach_tier  # noqa: E402


class ReachInventoryTests(TestCase):
    def test_legacy_reach_manifest_without_inventory_is_not_reused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            reach_dir = write_empty_reach_tier(
                Path(temp), losses={"transport": {"eligible": 1, "unbound": 0}},
            )
            manifest_path = reach_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest.pop("artifact_files")
            manifest["reach_cache_version"] = 2
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            problems = build.reach_tier_problems(reach_dir, key="r")
            self.assertTrue(any("reach_cache_version" in item for item in problems))
            self.assertTrue(any("no valid reach artifact inventory" in item for item in problems))

    def test_missing_corrupt_and_incompatible_reach_files_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            reach_dir = write_empty_reach_tier(
                Path(temp), losses={"transport": {"eligible": 1, "unbound": 0}},
            )
            self.assertEqual(build.reach_tier_problems(reach_dir, key="r"), [])

            labels_path = reach_dir / "food_full.labels.npy"
            original_labels = labels_path.read_bytes()
            labels_path.write_bytes(original_labels[:-1] + bytes([original_labels[-1] ^ 0xFF]))
            self.assertTrue(any("checksum mismatch food_full.labels.npy" in item
                                for item in build.reach_tier_problems(reach_dir, key="r")))

            labels_path.write_bytes(original_labels)
            intervals_path = reach_dir / "objects" / "park_intervals.bin"
            intervals_path.unlink()
            self.assertTrue(any("missing reach artifacts" in item
                                for item in build.reach_tier_problems(reach_dir, key="r")))

    def test_checks_reach_array_dimensions_even_with_current_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            reach_dir = write_empty_reach_tier(
                Path(temp), losses={"transport": {"eligible": 1, "unbound": 0}},
            )
            np.save(reach_dir / "food_full.distances.npy", np.zeros((0, 99), dtype="<u2"))
            build.complete_tier(
                reach_dir, {}, shard_dir=None, inventory_files=True,
            )
            self.assertTrue(any("food_full.distances.npy has shape/dtype" in item
                                for item in build.reach_tier_problems(reach_dir, key="r")))


class ShardPipelineTests(TestCase):
    def test_shells_reach_and_scores_match_reference_cold_and_from_cache(self) -> None:
        if not _native_available():
            self.skipTest("walkgraph binary without daily-access subcommands")
        from precompute.daily_access_classify import LogicalPlace
        from precompute.daily_access_native import merge_area_seeds, native_objects, native_reach, write_mask_geojson, write_objects
        from precompute.daily_access_reference import TransportStop, build_sources, compute_reference, load_reference_graph
        from precompute.daily_access_snap import LandMask

        coords = [(x * 120.0, y * 120.0) for y in range(6) for x in range(6)]
        edges = [(i, i + 1) for i in range(36) if (i + 1) % 6] + [(i, i + 6) for i in range(30)]
        entries = [
            ShardEntry("t0", int(X0), int(Y0), int(X0) + 400, int(Y0) + 800, 16, 8, "unused"),
            ShardEntry("t1", int(X0) + 400, int(Y0), int(X0) + 800, int(Y0) + 800, 16, 8, "unused"),
        ]
        land = LandMask(box(X0 - 100, Y0 - 100, X0 + 900, Y0 + 900))
        study = box(X0, Y0, X0 + 760, Y0 + 640)  # partial cells at the top and right
        places = [
            LogicalPlace("food_full|a", "food_full", "food", 1.0, [{"x": X0 + 600.0, "y": Y0 + 610.0, "source": "t", "source_ref": "a"}]),
            LogicalPlace("pharmacy|b", "pharmacy", "pharmacy", 1.0, [{"x": X0 + 10.0, "y": Y0 + 300.0, "source": "t", "source_ref": "b"}]),
            LogicalPlace("food_full|c", "food_full", "food", 1.0, [{"x": X0 + 130.0, "y": Y0 + 50.0, "source": "t", "source_ref": "c",
                                                                     "footprint_wkb": box(X0 + 110, Y0 + 30, X0 + 150, Y0 + 70).wkb}]),
        ]
        stops = [TransportStop("gtfs/nta/s", "nta", 4, X0 + 240.0, Y0 + 250.0)]

        def penalty_fn(x, y):
            return np.where(np.asarray(x) < X0 + 100.0, 2.5, 0.0), np.full(len(x), 0.75)

        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            graph_dir = temp_dir / "graph"
            write_walkgraph(graph_dir, coords, edges)
            graph = load_reference_graph(graph_dir)
            mask_path = temp_dir / "mask.geojson"
            write_mask_geojson(land, mask_path)
            shell_dir, reach_dir = temp_dir / "shell", temp_dir / "reach"
            build.begin_tier(shell_dir, "shell", "s", {})
            build.build_shells(graph_dir=graph_dir, land_mask=land, study_area_metric=study, entries=entries,
                               shell_dir=shell_dir, work_dir=temp_dir, progress=lambda _: None)
            build.complete_tier(shell_dir, {"stats": {}})
            build.begin_tier(reach_dir, "reach", "r", {})
            sources = build_sources(places=places, green_sites=(), park_approaches={}, park_intervals=None, stops=stops)
            objects = write_objects(sources, land, reach_dir / "objects")
            seeds, _ = native_objects(graph_dir, mask_path, objects, temp_dir)
            seeds = merge_area_seeds(seeds, sources.area_seeds)
            origins = build.collect_origin_nodes(shell_dir, entries)
            _, kinds = native_reach(graph_dir, seeds, origins, temp_dir, load_results=False)
            build.save_reach_cache(reach_dir, origins, kinds, sources.labels.as_json())
            build.complete_tier(
                reach_dir, {"origins": len(origins), "objects": objects.count},
                shard_dir=None, inventory_files=True,
            )
            self.assertEqual(build.reach_tier_problems(reach_dir, key="r"), [])

            scored = []
            for run in range(2):  # the second run reads everything back from disk only
                score_dir = temp_dir / f"score{run}"
                work = temp_dir / f"work{run}"
                work.mkdir()
                build.build_scores(graph_dir=graph_dir, mask_path=mask_path, shell_dir=shell_dir, reach_dir=reach_dir,
                                   score_dir=score_dir, entries=entries, penalty_fn=penalty_fn, work_dir=work,
                                   progress=lambda _: None, block_cells=128)  # one shard per block
                scored.append({entry.shard_id: build.load_score_shard(score_dir, entry.shard_id) for entry in entries})
            corners = np.concatenate([build.shard_cell_corners(entry) for entry in entries])
            reference = compute_reference(
                graph=graph, land_mask=land, cells=[tuple(c) for c in corners], cell_size_m=50.0,
                places=places, stops=stops, penalty_fn=penalty_fn,
            )
            shells = {entry.shard_id: build.load_shell_shard(shell_dir, entry.shard_id) for entry in entries}
            table = build.load_label_table(reach_dir)
            _, _, rows = build.load_reach_cache(reach_dir, mmap=False)
            cache_origins = np.load(reach_dir / "origins.npy")

        for shard_id in ("t0", "t1"):
            for field in ("total", "components", "penalties", "service_access", "network_status"):
                np.testing.assert_array_equal(scored[0][shard_id][field], scored[1][shard_id][field])
        self.assertEqual(shells["t0"]["land_area_m2"][15, 0], 0.0)  # above the study area
        self.assertAlmostEqual(float(shells["t1"]["land_area_m2"][12, 7]), 10.0 * 40.0, places=3)  # partial corner
        position = 0
        needs = ("food", "other_retail", "primary_care", "pharmacy", "green", "transport")
        for entry in entries:
            score = scored[1][entry.shard_id]
            valid = shells[entry.shard_id]["valid_land_mask"].ravel()
            for flat in range(entry.rows * entry.cols):
                cell = reference.cells[position]
                position += 1
                row, col = divmod(flat, entry.cols)
                if not valid[flat]:
                    self.assertEqual(score["total"][row, col], 65535)
                    continue
                self.assertEqual(score["total"][row, col], cell.quantized["total"], (entry.shard_id, row, col))
                self.assertEqual([int(score["service_access"][i, row, col]) for i in range(6)],
                                 [cell.quantized["service_access"][need] for need in needs])
                self.assertEqual(int(score["penalties"][0, row, col]), cell.quantized["penalties"]["railway_proximity"])
                self.assertEqual(int(score["penalties"][1, row, col]), cell.quantized["penalties"]["road_proximity"])
        # Identity survives the disk: the cache's ids name the same logical places.
        self.assertEqual(table, reference.labels.as_json())
        key_of = {entry["label"]: entry["key"] for entry in table["food_full"]}
        food_labels = {key_of[int(label)] for label in np.asarray(rows["food_full"][1]).ravel() if label != 2**32 - 1}
        self.assertEqual(food_labels, {"food_full|a", "food_full|c"})
        ref_food = {key for cell in reference.cells for label, _ in cell.best_by_kind.get("food_full", [])
                    for key in [reference.labels.key_of("food_full", label)]}
        self.assertEqual(ref_food, food_labels)
        self.assertTrue(len(cache_origins) > 0)
