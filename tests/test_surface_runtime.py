from __future__ import annotations

import json
import struct
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import numpy as np
from shapely.geometry import box

import config
import precompute.surface as surface
from network.loader import run_surface_shell_build


def _write_shell_manifest(shell_dir: Path, *, shard_inventory: list[dict[str, object]]) -> None:
    surface.write_surface_manifest(
        shell_dir,
        {
            "status": "complete",
            "schema_version": 1,
            "surface_shell_hash": "shell-hash-123",
            "reach_hash": "reach-hash-123",
            "base_resolution_m": 50,
            "shard_size_m": 20000,
            "tile_size_px": 256,
            "shard_inventory": shard_inventory,
        },
    )


def _write_score_manifest(score_dir: Path, *, shard_inventory: list[dict[str, object]]) -> None:
    surface.write_surface_manifest(
        score_dir,
        {
            "status": "complete",
            "schema_version": 1,
            "score_hash": "score-hash-123",
            "surface_shell_hash": "shell-hash-123",
            "base_resolution_m": 50,
            "node_scores_file": "node_scores.npz",
            "shard_inventory": shard_inventory,
        },
    )


class SurfaceShardTests(TestCase):
    def test_shards_align_to_20km_and_divide_into_50m_cells(self) -> None:
        entries = surface.iter_shard_entries(box(500.0, 750.0, 40500.0, 40750.0))

        self.assertTrue(entries)
        self.assertEqual(entries[0].rows, 400)
        self.assertEqual(entries[0].cols, 400)
        for entry in entries:
            with self.subTest(shard_id=entry.shard_id):
                self.assertEqual(entry.x_min_m % 20000, 0)
                self.assertEqual(entry.y_min_m % 20000, 0)
                self.assertEqual(entry.x_max_m - entry.x_min_m, 20000)
                self.assertEqual(entry.y_max_m - entry.y_min_m, 20000)

    def test_ensure_surface_shell_cache_forwards_explicit_threads_to_loader(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            graph_dir = tmp / "graph"
            graph_dir.mkdir()

            with mock.patch("network.loader.run_surface_shell_build") as build_mock:
                surface.ensure_surface_shell_cache(
                    shell_dir=shell_dir,
                    surface_shell_hash="shell-hash-123",
                    reach_hash="reach-hash-123",
                    study_area_metric=box(0.0, 0.0, 100.0, 100.0),
                    graph_dir=graph_dir,
                    walkgraph_bin="walkgraph.exe",
                    node_count=5,
                    threads=5,
                    tracker=None,
                )

        self.assertEqual(build_mock.call_args.kwargs["threads"], 5)


class AggregationTests(TestCase):
    def test_weighted_mean_block_reduce_uses_effective_land_area(self) -> None:
        values = np.array(
            [
                [10.0, 30.0],
                [70.0, 90.0],
            ],
            dtype=np.float32,
        )
        weights = np.array(
            [
                [1.0, 1.0],
                [0.5, 0.5],
            ],
            dtype=np.float32,
        )

        aggregated, valid_mask = surface.weighted_mean_block_reduce(values, weights, 2)

        expected = ((10.0 * 1.0) + (30.0 * 1.0) + (70.0 * 0.5) + (90.0 * 0.5)) / 3.0
        self.assertTrue(bool(valid_mask[0, 0]))
        self.assertAlmostEqual(float(aggregated[0, 0]), expected)

    def test_weighted_mean_block_reduce_marks_water_only_blocks_nodata(self) -> None:
        aggregated, valid_mask = surface.weighted_mean_block_reduce(
            np.zeros((2, 2), dtype=np.float32),
            np.zeros((2, 2), dtype=np.float32),
            2,
        )

        self.assertFalse(bool(valid_mask[0, 0]))
        self.assertTrue(np.isnan(aggregated[0, 0]))


class FineSurfaceRuntimeTests(TestCase):
    def test_render_tile_caches_png_and_reuses_cached_result(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            score_dir = tmp / "scores"
            tile_dir = tmp / "tiles"
            _write_shell_manifest(shell_dir, shard_inventory=[])
            surface.save_node_score_arrays(
                score_dir,
                {
                    "categories": ["shops", "transport", "healthcare", "parks"],
                    "counts_matrix": np.zeros((1, 4), dtype=np.uint32),
                    "weighted_units_matrix": np.zeros((1, 4), dtype=np.float32),
                    "reference_scores": np.zeros((1, 4), dtype=np.float32),
                    "reference_total": np.zeros(1, dtype=np.float32),
                },
            )
            _write_score_manifest(score_dir, shard_inventory=[])
            runtime = surface.FineSurfaceRuntime(shell_dir, score_dir, tile_dir)

            with mock.patch.object(runtime, "_iter_intersecting_shards", return_value=iter(())) as shard_iter_mock:
                first = runtime.render_tile(resolution_m=250, z=15, x=3, y=4)
                second = runtime.render_tile(resolution_m=250, z=15, x=3, y=4)

        self.assertEqual(first[:8], b"\x89PNG\r\n\x1a\n")
        self.assertEqual(first, second)
        shard_iter_mock.assert_called_once()

    def test_aggregated_shard_surface_uses_persisted_canonical_totals(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            score_dir = tmp / "scores"
            tile_dir = tmp / "tiles"
            shard_id = "0_0"
            shell_path = surface.surface_shell_shard_path(shell_dir, shard_id)
            shell_path.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                shell_path,
                origin_node_idx=np.array([[0, 1], [2, 3]], dtype=np.int32),
                effective_area_ratio=np.array([[1.0, 0.5], [1.0, 0.0]], dtype=np.float32),
                valid_land_mask=np.array([[True, True], [True, False]], dtype=bool),
                x_min_m=np.array([0], dtype=np.int32),
                y_min_m=np.array([0], dtype=np.int32),
            )
            score_path = surface.surface_score_shard_path(score_dir, shard_id)
            score_path.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                score_path,
                total_score_50=np.array([[20.0, 40.0], [80.0, np.nan]], dtype=np.float32),
            )
            surface.save_node_score_arrays(
                score_dir,
                {
                    "categories": ["shops", "transport", "healthcare", "parks"],
                    "counts_matrix": np.zeros((4, 4), dtype=np.uint32),
                    "weighted_units_matrix": np.zeros((4, 4), dtype=np.float32),
                    "reference_scores": np.zeros((4, 4), dtype=np.float32),
                    "reference_total": np.zeros(4, dtype=np.float32),
                },
            )
            _write_shell_manifest(
                shell_dir,
                shard_inventory=[
                    {
                        "shard_id": shard_id,
                        "x_min_m": 0,
                        "y_min_m": 0,
                        "x_max_m": 20000,
                        "y_max_m": 20000,
                        "rows": 2,
                        "cols": 2,
                        "path": "shards/0_0.npz",
                    }
                ],
            )
            _write_score_manifest(
                score_dir,
                shard_inventory=[
                    {
                        "shard_id": shard_id,
                        "path": "shards/0_0.npz",
                    }
                ],
            )

            runtime = surface.FineSurfaceRuntime(shell_dir, score_dir, tile_dir)
            aggregated, valid_mask = runtime.aggregated_shard_surface(shard_id, 100)

        self.assertEqual(aggregated.shape, (1, 1))
        self.assertTrue(bool(valid_mask[0, 0]))
        expected = ((20.0 * 1.0) + (40.0 * 0.5) + (80.0 * 1.0)) / 2.5
        self.assertAlmostEqual(float(aggregated[0, 0]), expected)

    def test_inspect_returns_canonical_50m_breakdown(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            score_dir = tmp / "scores"
            tile_dir = tmp / "tiles"
            shard_id = "0_0"
            shard_path = surface.surface_shell_shard_path(shell_dir, shard_id)
            shard_path.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                shard_path,
                origin_node_idx=np.array([[0]], dtype=np.int32),
                effective_area_ratio=np.array([[1.0]], dtype=np.float32),
                valid_land_mask=np.array([[True]], dtype=bool),
                x_min_m=np.array([0], dtype=np.int32),
                y_min_m=np.array([0], dtype=np.int32),
            )
            score_path = surface.surface_score_shard_path(score_dir, shard_id)
            score_path.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                score_path,
                total_score_50=np.array([[25.0]], dtype=np.float32),
            )
            surface.save_node_score_arrays(
                score_dir,
                {
                    "categories": ["shops", "transport", "healthcare", "parks"],
                    "counts_matrix": np.array([[1, 0, 0, 0]], dtype=np.uint32),
                    "weighted_units_matrix": np.array([[6.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                    "reference_scores": np.array([[25.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                    "reference_total": np.array([25.0], dtype=np.float32),
                },
            )
            _write_shell_manifest(
                shell_dir,
                shard_inventory=[
                    {
                        "shard_id": shard_id,
                        "x_min_m": 0,
                        "y_min_m": 0,
                        "x_max_m": 20000,
                        "y_max_m": 20000,
                        "rows": 1,
                        "cols": 1,
                        "path": "shards/0_0.npz",
                    }
                ],
            )
            _write_score_manifest(
                score_dir,
                shard_inventory=[
                    {
                        "shard_id": shard_id,
                        "path": "shards/0_0.npz",
                    }
                ],
            )

            runtime = surface.FineSurfaceRuntime(shell_dir, score_dir, tile_dir)
            with mock.patch.object(surface, "TO_TARGET", side_effect=lambda lon, lat: (25.0, 25.0)):
                payload = runtime.inspect(lat=53.4, lon=-6.2, zoom=15)

        self.assertTrue(payload["valid_land"])
        self.assertEqual(payload["resolution_m"], 50)
        self.assertEqual(payload["visible_resolution_m"], 250)
        self.assertEqual(payload["counts"], {"shops": 1})
        self.assertEqual(payload["component_scores"]["shops"], 25.0)
        self.assertEqual(payload["total_score"], 25.0)

    @unittest.skipUnless(Path(config.WALKGRAPH_BIN).is_file(), "walkgraph binary is required for the integration fixture")
    def test_walkgraph_surface_subcommand_builds_expected_shell_payload(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            nodes_bin = tmp / "nodes.bin"
            geojson_path = tmp / "study_area.geojson"
            config_json_path = tmp / "surface-config.json"
            shell_dir = tmp / "shell"

            node_rows = [
                (53.3498, -6.2603),
                (53.3500, -6.2590),
            ]
            nodes_bin.write_bytes(b"".join(struct.pack("<ff", lat, lon) for lat, lon in node_rows))

            metric_x, metric_y = config.TO_TARGET(-6.2603, 53.3498)
            shard_x = surface.aligned_floor(metric_x, 100)
            shard_y = surface.aligned_floor(metric_y, 100)
            geojson_path.write_text(
                json.dumps(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[
                                [shard_x + 5, shard_y + 5],
                                [shard_x + 95, shard_y + 5],
                                [shard_x + 95, shard_y + 95],
                                [shard_x + 5, shard_y + 95],
                                [shard_x + 5, shard_y + 5],
                            ]],
                        },
                        "properties": {},
                    }
                ),
                encoding="utf-8",
            )
            config_json_path.write_text(json.dumps({"tile_size_px": 256}), encoding="utf-8")

            run_surface_shell_build(
                nodes_bin=nodes_bin,
                study_area_geojson_path=geojson_path,
                shell_dir=shell_dir,
                walkgraph_bin=config.WALKGRAPH_BIN,
                surface_shell_hash="shell-hash-123",
                reach_hash="reach-hash-123",
                node_count=len(node_rows),
                config_json_path=config_json_path,
                shard_size_m=100,
                base_resolution_m=50,
                threads=1,
            )

            manifest = surface.load_surface_manifest(shell_dir)
            self.assertIsNotNone(manifest)
            assert manifest is not None
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["completed_shards"], 1)
            self.assertEqual(manifest["total_shards"], 1)
            self.assertEqual(len(manifest["shard_inventory"]), 1)

            shard_id = manifest["shard_inventory"][0]["shard_id"]
            payload = surface.load_shell_shard_payload(shell_dir, shard_id)
            self.assertEqual(payload["origin_node_idx"].shape, (2, 2))
            self.assertEqual(payload["effective_area_ratio"].shape, (2, 2))
            self.assertEqual(payload["valid_land_mask"].shape, (2, 2))
