from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

import numpy as np
from shapely.geometry import box

import serve_from_db
from config import DAILY_ACCESS_SEARCH_CUTOFF_M, SURFACE_SHARD_SIZE_M, TO_WGS84
from precompute import daily_access_build as build
from precompute import daily_access_publish as publish
from precompute.daily_access_native import ALL_KINDS, INTERVAL_DTYPE, OBJECT_DTYPE
from precompute.daily_access_reference import service_kind_k, service_kind_need, service_kind_quality
from precompute.daily_access_runtime import ByteLRU, DailyAccessSurfaceRuntime

SIZE = SURFACE_SHARD_SIZE_M // 50  # 400 cells per side
X0, Y0 = 560_000, 560_000  # shard origin (multiple of 20 km)
SHARD = f"{X0}_{Y0}"


def write_tiers(
    root: Path,
    *,
    corrupt: bool = False,
    profile: str = "dev",
    import_fingerprint: str = "imp",
    reality_fingerprint: str = "gtfs",
) -> tuple[Path, Path]:
    """One shard: left half land with network (total 60), a no-network strip, water on the right."""
    shell_dir, score_dir = root / "shell", root / "score"
    common = {
        "shards": [SHARD], "profile": profile, "resolution_m": 50, "shard_size_m": SURFACE_SHARD_SIZE_M,
        "quantization": build.QUANTIZATION, "mask_identity": "mask", "study_area_bounds_metric": [1, 2, 3, 4],
        "graph_bbox_wgs84": [1, 2, 3, 4], "graph_padding_m": 2250.0, "reach_buffer_m": 2000.0,
        "import_fingerprint": import_fingerprint, "transit_reality_fingerprint": reality_fingerprint,
    }
    build.begin_tier(shell_dir, "shell", "s", common)
    build.begin_tier(score_dir, "score", "c", {**common, "shell_key": "s", "reach_key": "r"})
    (shell_dir / "shards").mkdir()
    (score_dir / "shards").mkdir()
    land = np.zeros((SIZE, SIZE), dtype=np.float32)
    land[:, :200] = 2500.0
    land[:, 200] = 1250.0  # half-land coastal column
    status = np.zeros((SIZE, SIZE), dtype=np.uint8)
    status[:, :180] = 1
    status[:, 180:201] = 2  # land without a walking network
    total = np.full((SIZE, SIZE), 65535, dtype=np.uint16)
    total[:, :201] = 0
    total[:, :180] = 6000
    components = np.full((4, SIZE, SIZE), 65535, dtype=np.uint16)
    components[:, :, :201] = 0
    components[0, :, :180] = 3000
    components[1, :, :180] = 3000
    penalties = np.full((2, SIZE, SIZE), 65535, dtype=np.uint16)
    penalties[:, :, :201] = 0
    penalties[1, :, :201] = 150
    access = np.full((6, SIZE, SIZE), 65535, dtype=np.uint16)
    access[:, :, :201] = 0
    access[0, :, :180] = 9000
    np.savez_compressed(
        shell_dir / "shards" / f"{SHARD}.npz",
        land_area_m2=land, valid_land_mask=land > 0, origin_x=np.zeros_like(land, dtype=np.float64),
        origin_y=np.zeros_like(land, dtype=np.float64), origin_flags=(land > 0).astype(np.uint8),
        entry_flags=(status == 1).astype(np.uint8), used_nodes=np.zeros(0, dtype=np.uint32),
        x_min_m=np.asarray([X0], dtype=np.int32), y_min_m=np.asarray([Y0], dtype=np.int32),
    )
    np.savez_compressed(
        score_dir / "shards" / f"{SHARD}.npz",
        total=total, components=components, penalties=penalties, service_access=access, network_status=status,
        x_min_m=np.asarray([X0], dtype=np.int32), y_min_m=np.asarray([Y0], dtype=np.int32),
    )
    build.complete_tier(shell_dir, {"stats": {}})
    build.complete_tier(score_dir, {"stats": {}})
    if corrupt:
        (score_dir / "shards" / f"{SHARD}.npz").write_bytes(b"broken")
    return shell_dir, score_dir


def write_empty_reach_tier(
    root: Path,
    *,
    losses: dict,
    profile: str = "dev",
    import_fingerprint: str = "imp",
    reality_fingerprint: str = "gtfs",
) -> Path:
    reach_dir = root / "reach"
    build.begin_tier(reach_dir, "reach", "r", {
        "profile": profile, "resolution_m": 50, "shard_size_m": SURFACE_SHARD_SIZE_M,
        "quantization": build.QUANTIZATION, "mask_identity": "mask", "study_area_bounds_metric": [1, 2, 3, 4],
        "graph_bbox_wgs84": [1, 2, 3, 4], "graph_padding_m": 2250.0, "reach_buffer_m": 2000.0,
        "import_fingerprint": import_fingerprint, "transit_reality_fingerprint": reality_fingerprint,
    })
    objects_dir = reach_dir / "objects"
    objects_dir.mkdir()
    np.zeros(0, dtype=OBJECT_DTYPE).tofile(objects_dir / "objects.bin")
    np.asarray([0], dtype="<u8").tofile(objects_dir / "ring_offsets.bin")
    np.zeros((0, 2), dtype="<f8").tofile(objects_dir / "ring_vertices.bin")
    np.zeros(0, dtype=INTERVAL_DTYPE).tofile(objects_dir / "park_intervals.bin")
    raw_dir = root / "reach-raw"
    raw_dir.mkdir()
    kinds = []
    for index, kind in enumerate(ALL_KINDS):
        k = service_kind_k(kind)
        distances = raw_dir / f"{kind}.distances.bin"
        labels = raw_dir / f"{kind}.labels.bin"
        np.zeros((0, k), dtype="<u2").tofile(distances)
        np.zeros((0, k), dtype="<u4").tofile(labels)
        need = service_kind_need(kind)
        kinds.append({
            "kind": kind, "kind_index": index, "need": need, "quality": service_kind_quality(kind),
            "k": k, "cutoff_m": float(DAILY_ACCESS_SEARCH_CUTOFF_M[need]),
            "out_distances": str(distances), "out_labels": str(labels),
        })
    build.save_reach_cache(reach_dir, np.zeros(0, dtype=np.uint32), kinds, {})
    build.complete_tier(
        reach_dir, {"origins": 0, "objects": 0, "losses": losses},
        shard_dir=None, inventory_files=True,
    )
    return reach_dir


def lat_lon(x: float, y: float) -> tuple[float, float]:
    lon, lat = TO_WGS84(x, y)
    return lat, lon


class RuntimeTests(TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.shell_dir, self.score_dir = write_tiers(self.root)
        self.runtime = DailyAccessSurfaceRuntime(self.shell_dir, self.score_dir, self.root / "tiles", profile="dev")

    def tearDown(self) -> None:
        self._temp.cleanup()

    def test_inspect_reads_the_score_shard_and_marks_legacy_fields(self) -> None:
        lat, lon = lat_lon(X0 + 25.0, Y0 + 25.0)
        payload = self.runtime.inspect(lat=lat, lon=lon)
        self.assertEqual(payload["scoring_model"], "daily-access-v4")
        self.assertEqual(payload["network_status"], "ok")
        self.assertAlmostEqual(payload["total_score"], 60.0)
        self.assertAlmostEqual(payload["component_scores"]["shops"], 30.0)
        self.assertAlmostEqual(payload["penalties"]["road_proximity"], 1.5)
        self.assertAlmostEqual(payload["service_access"]["food"], 0.9)
        self.assertIsNone(payload["visible"])  # no zoom -> no visible cell
        self.assertIsNone(payload["counts"])
        self.assertEqual(payload["legacy_fields_unavailable"], ["counts", "cluster_counts", "effective_units"])
        lat, lon = lat_lon(X0 + 9025.0, Y0 + 25.0)  # no-network land
        payload = self.runtime.inspect(lat=lat, lon=lon)
        self.assertEqual(payload["network_status"], "no_walking_network")
        self.assertEqual(payload["total_score"], 0.0)
        self.assertTrue(payload["valid_land"])

    def test_visible_cell_is_a_land_weighted_mean_of_stored_values(self) -> None:
        lat, lon = lat_lon(X0 + 100.0, Y0 + 100.0)
        payload = self.runtime.inspect(lat=lat, lon=lon, zoom=5)
        visible = payload["visible"]
        resolution = payload["visible_resolution_m"]
        self.assertEqual(resolution, 20_000)  # dev ladder at z5
        land = 180 * 2500.0 + 20 * 2500.0 + 1250.0
        self.assertAlmostEqual(visible["total_score"], 60.0 * 180 * 2500.0 / land, places=6)
        self.assertAlmostEqual(visible["service_access"]["food"], 0.9 * 180 * 2500.0 / land, places=6)
        self.assertAlmostEqual(visible["penalties"]["road_proximity"], 1.5, places=6)  # averaged the same way
        self.assertEqual(visible["network_status"], "partial")
        self.assertAlmostEqual(visible["network_coverage_share"], 180 * 2500.0 / land, places=6)
        self.assertAlmostEqual(visible["land_area_m2"], land * SIZE, places=0)

    def test_png_aggregates_only_the_requested_channel_and_caches_are_bounded(self) -> None:
        png = self.runtime.render_tile(resolution_m=1000, z=10, x=495, y=339, channel="access_food")
        self.assertTrue(png.startswith(b"\x89PNG"))
        channels = {key[2] for key in self.runtime._aggregates._items}
        self.assertLessEqual(channels, {"access_food"})
        with self.assertRaises(ValueError):
            self.runtime.render_tile(resolution_m=1000, z=10, x=495, y=339, channel="counts")
        cache = ByteLRU(1000)
        cache.put("a", np.zeros(100, dtype=np.float64))  # 800 bytes
        cache.put("b", np.zeros(100, dtype=np.float64))
        self.assertIsNone(cache.get("a"))
        self.assertLessEqual(cache.bytes, 1000)
        cache.put("huge", np.zeros(1000))
        self.assertIsNone(cache.get("huge"))
        self.assertLessEqual(sum(self.runtime.cache_bytes().values()), (64 + 128 + 64) * 2**20)

    def test_coarse_rows_are_area_weighted_with_real_components(self) -> None:
        study = box(X0, Y0, X0 + 10_050.0 - 25.0, Y0 + SURFACE_SHARD_SIZE_M)
        cells = publish.coarse_cells(self.runtime, [SHARD], 10_000, study)
        self.assertEqual(len(cells), 4)  # two columns of land x two rows; water excluded
        west = [cell for cell in cells if cell["cell_id"].split(":")[1] == str(X0 * 1000)][0]
        # 180 network columns at 60 points + 20 no-network land columns at 0.
        self.assertAlmostEqual(west["total"], 54.0)
        self.assertAlmostEqual(west["scores"]["shops"], 27.0)
        self.assertAlmostEqual(west["scores"]["road_proximity"], -1.5)  # v3 tile sign convention
        self.assertAlmostEqual(west["scores"]["network_coverage_share"], 0.9)
        east = [cell for cell in cells if cell is not west and cell["cell_id"].split(":")[2] == west["cell_id"].split(":")[2]][0]
        self.assertAlmostEqual(east["scores"]["network_coverage_share"], 0.0)
        self.assertAlmostEqual(east["total"], 0.0)


class RuntimeServiceTests(TestCase):
    def test_dev_inspect_works_without_fine_tiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            shell_dir, score_dir = write_tiers(Path(temp))
            summary = {"scoring_model": "daily-access-v4",
                       "daily_access": {"shell_dir": str(shell_dir), "score_dir": str(score_dir), "model": "daily-access-v4",
                                        "fine_png_resolutions_m": [2500, 1000]}}
            model, info, warning = serve_from_db._daily_access_state(summary, "dev")
            self.assertEqual(model, "daily-access-v4")
            self.assertTrue(info["inspect_ready"])
            self.assertFalse(info["fine_png_enabled"])  # dev publishes 20/10/5 km only
            self.assertIsNone(warning)
            service = serve_from_db.RuntimeService(mock.sentinel.engine, profile="dev")
            state = SimpleNamespace(daily_access=info, surface_tile_dir=None, build_profile="dev", fine_resolutions=[],
                                    scoring_model=model)
            service.state = mock.Mock(return_value=state)
            lat, lon = lat_lon(X0 + 25.0, Y0 + 25.0)
            payload = service.inspect(lat=lat, lon=lon, zoom=8)
            self.assertEqual(payload["scoring_model"], "daily-access-v4")
            self.assertAlmostEqual(payload["total_score"], 60.0)
            self.assertIsNotNone(payload["visible"])
            with self.assertRaises(ValueError):
                service.get_surface_tile(resolution_m=1000, z=10, x=1, y=1)  # no fine tiles in dev
            del service, state

    def test_corrupt_shard_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            shell_dir, score_dir = write_tiers(Path(temp), corrupt=True)
            summary = {"scoring_model": "daily-access-v4", "daily_access": {"shell_dir": str(shell_dir), "score_dir": str(score_dir)}}
            # The manifest checksums catch it once shard ids are verified.
            self.assertTrue(build.tier_problems(score_dir, shard_ids=[SHARD]))
            _model, info, _warning = serve_from_db._daily_access_state(summary, "dev")
            self.assertFalse(info["inspect_ready"])


def publish_layers():
    from precompute.display_layers import DisplayLayers
    return DisplayLayers(summary={"map_center": {"lat": 51.9, "lon": -8.5}})


class PublishTests(TestCase):
    def _context(self, root: Path):
        shell_dir, score_dir = write_tiers(root)
        reach_dir = write_empty_reach_tier(root, losses={
            "transport": {"eligible": 4, "unbound": 1, "unbound_share": 0.25},
            "transport_feed:nta": {"eligible": 4, "unbound": 1, "unbound_share": 0.25},
        })
        keys = build.TierKeys(graph="g", shell="s", reach="r", score="c")
        import dataclasses

        @dataclasses.dataclass
        class Hashes:
            build_key: str = "v3-key"
            config_hash: str = "cfg"
            import_fingerprint: str = "imp"

        return SimpleNamespace(
            engine=mock.sentinel.engine, profile="dev", keys=keys, hashes=Hashes(), shell_dir=shell_dir,
            reach_dir=reach_dir, score_dir=score_dir, study_area_metric=box(X0, Y0, X0 + 20_000, Y0 + 20_000),
            study_area_wgs84=box(-8.6, 51.8, -8.3, 52.0),
            source_state=SimpleNamespace(extract_path="x.pbf", import_fingerprint="imp"),
            graph_dir=root / "graph", reality_fingerprint="gtfs", overture_dataset_signature="overture-test",
        ), None

    def test_checks_tiers_and_warns_on_feed_losses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            context, _ = self._context(Path(temp))
            checked = publish.check_publishable(context, allow_missing_transport=False)
            self.assertEqual(checked["shard_ids"], [SHARD])
            self.assertTrue(any("transport_feed:nta" in warning for warning in checked["warnings"]))
            (context.score_dir / "shards" / f"{SHARD}.npz").write_bytes(b"broken")
            with self.assertRaisesRegex(build.DailyAccessBuildError, "not publishable"):
                publish.check_publishable(context, allow_missing_transport=False)

    def test_zero_gtfs_blocks_publish_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            context, _ = self._context(Path(temp))
            build.complete_tier(
                context.reach_dir,
                {"losses": {"transport": {"eligible": 2, "unbound": 2, "unbound_share": 1.0}}},
                shard_dir=None,
                inventory_files=True,
            )
            with self.assertRaisesRegex(build.DailyAccessBuildError, "allow-missing-transport"):
                publish.check_publishable(context, allow_missing_transport=False)
            publish.check_publishable(context, allow_missing_transport=True)

    def test_corrupt_reach_artifact_blocks_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            context, _ = self._context(Path(temp))
            (context.reach_dir / "labels.json").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(build.DailyAccessBuildError, "checksum mismatch labels.json"):
                publish.check_publishable(context, allow_missing_transport=False)

    def test_failed_bake_keeps_the_active_build(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            context, _ = self._context(Path(temp))
            with (
                mock.patch.object(publish, "_write_staged_build") as staged_write,
                mock.patch.object(publish, "select_noise_artifact", return_value=(None, [])),
                mock.patch.object(publish, "prepare_display_layers", return_value=publish_layers()),
                mock.patch.object(publish, "_delete_build") as delete_build,
                mock.patch.object(publish, "switch_active_build") as switch,
                mock.patch.object(publish, "pmtiles_output_path", return_value=Path(temp) / "live.pmtiles"),
            ):
                with self.assertRaisesRegex(RuntimeError, "bake failed"):
                    publish.publish_daily_access(
                        context, progress=lambda _: None,
                        bake_pmtiles=mock.Mock(side_effect=RuntimeError("bake failed")),
                        archive_build_key=lambda path: None,
                    )
            staged = staged_write.call_args.kwargs["hashes"]
            self.assertTrue(staged.config_hash.endswith(publish.STAGING_SUFFIX))  # never visible to runtimes
            self.assertEqual(staged_write.call_args.kwargs["summary_json"]["scoring_model"], "daily-access-v4")
            delete_build.assert_called_once()
            switch.assert_not_called()

    def test_successful_publish_switches_after_the_archive_is_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            context, _ = self._context(Path(temp))
            order = []

            def bake(engine, build_key, path, **kwargs):
                order.append(("bake", kwargs.get("fine_grid_config")))
                Path(path).write_bytes(b"pmtiles")

            with (
                mock.patch.object(publish, "_write_staged_build"),
                mock.patch.object(publish, "select_noise_artifact", return_value=(None, [])),
                mock.patch.object(publish, "prepare_display_layers", return_value=publish_layers()),
                mock.patch.object(publish, "switch_active_build", side_effect=lambda *a, **k: order.append(("switch", k["live_config_hash"]))),
                mock.patch.object(publish, "pmtiles_output_path", return_value=Path(temp) / "live.pmtiles"),
            ):
                captured = {}

                def stamp(path):
                    return captured["key"]

                original = publish.hash_dict

                def remember(payload):
                    captured["key"] = original(payload)
                    return captured["key"]

                with mock.patch.object(publish, "hash_dict", side_effect=remember):
                    result = publish.publish_daily_access(context, progress=lambda _: None, bake_pmtiles=bake, archive_build_key=stamp)
            self.assertEqual(order, [("bake", {}), ("switch", "cfg")])  # dev: coarse only, never v3 node grids
            self.assertEqual((Path(temp) / "live.pmtiles").read_bytes(), b"pmtiles")
            self.assertEqual(result["config_hash"], "cfg")


class FineVectorGridTests(TestCase):
    def test_v4_fine_cells_carry_real_components(self) -> None:
        from fine_vector_pmtiles_worker import FineGridTileContext

        with tempfile.TemporaryDirectory() as temp:
            shell_dir, score_dir = write_tiers(Path(temp))
            context = FineGridTileContext(shell_dir=shell_dir, score_dir=score_dir)
            self.assertTrue(context.daily_access)
            aggregates, valid = context.daily_access_aggregates(SHARD, 1000)
            self.assertTrue(valid[0, 0])
            self.assertAlmostEqual(float(aggregates["total_score"][0, 0]), 60.0, places=4)
            self.assertAlmostEqual(float(aggregates["score_shops"][0, 0]), 30.0, places=4)
            self.assertAlmostEqual(float(aggregates["score_road_proximity"][0, 0]), -1.5, places=4)
            self.assertFalse(valid[0, -1])  # water
            del context
