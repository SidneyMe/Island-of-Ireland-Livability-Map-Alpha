from __future__ import annotations

import importlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock


bake_pmtiles = importlib.import_module("precompute.bake_pmtiles")


class PmtilesBakeContractTests(TestCase):
    def test_grid_tile_sql_exports_popup_score_and_count_fields(self) -> None:
        sql = str(bake_pmtiles._GRID_TILE_SQL)

        for category in bake_pmtiles.GRID_AMENITY_CATEGORIES:
            with self.subTest(category=category):
                self.assertIn(
                    f"COALESCE((g.counts_json ->> '{category}')::integer, 0) "
                    f"AS count_{category}",
                    sql,
                )
                self.assertIn(
                    f"COALESCE((g.cluster_counts_json ->> '{category}')::integer, 0) "
                    f"AS cluster_{category}",
                    sql,
                )
                self.assertIn(
                    f"COALESCE((g.effective_units_json ->> '{category}')::double precision, 0.0) "
                    f"AS effective_units_{category}",
                    sql,
                )
                self.assertIn(
                    f"COALESCE((g.scores_json ->> '{category}')::double precision, 0.0) "
                    f"AS score_{category}",
                    sql,
                )

    def test_grid_layer_metadata_declares_popup_score_and_count_fields(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            service_desert_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
        )
        grid_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "grid"
        )

        for category in bake_pmtiles.GRID_AMENITY_CATEGORIES:
            with self.subTest(category=category):
                self.assertEqual(grid_layer["fields"][f"count_{category}"], "Number")
                self.assertEqual(grid_layer["fields"][f"cluster_{category}"], "Number")
                self.assertEqual(grid_layer["fields"][f"effective_units_{category}"], "Number")
                self.assertEqual(grid_layer["fields"][f"score_{category}"], "Number")

    def test_pmtiles_metadata_declares_transit_reality_and_service_desert_layers(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            service_desert_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
        )

        transport_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "transport_reality"
        )
        desert_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "service_deserts"
        )

        self.assertEqual(transport_layer["fields"]["reality_status"], "String")
        self.assertEqual(transport_layer["fields"]["public_departures_30d"], "Number")
        self.assertEqual(transport_layer["fields"]["source_status"], "String")
        self.assertEqual(transport_layer["fields"]["school_only_departures_30d"], "Number")
        self.assertEqual(transport_layer["fields"]["weekday_morning_peak_deps"], "Number")
        self.assertEqual(transport_layer["fields"]["weekday_evening_peak_deps"], "Number")
        self.assertEqual(transport_layer["fields"]["friday_evening_deps"], "Number")
        self.assertEqual(transport_layer["fields"]["transport_score_units"], "Number")
        self.assertEqual(transport_layer["fields"]["bus_daytime_deps"], "Number")
        self.assertEqual(transport_layer["fields"]["bus_daytime_headway_min"], "Number")
        self.assertEqual(transport_layer["fields"]["bus_frequency_tier"], "String")
        self.assertEqual(transport_layer["fields"]["bus_frequency_score_units"], "Number")
        self.assertEqual(transport_layer["fields"]["bus_active_days_mask_7d"], "String")
        self.assertEqual(transport_layer["fields"]["bus_service_subtier"], "String")
        self.assertEqual(transport_layer["fields"]["route_modes"], "String")
        self.assertEqual(transport_layer["fields"]["is_unscheduled_stop"], "Number")
        self.assertEqual(transport_layer["fields"]["has_exception_only_service"], "Number")
        self.assertEqual(desert_layer["fields"]["baseline_reachable_stop_count"], "Number")
        self.assertEqual(desert_layer["fields"]["reachable_public_departures_7d"], "Number")

    def test_pmtiles_metadata_declares_noise_layer(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            service_desert_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
            noise_max_zoom=12,
        )

        noise_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "noise"
        )

        self.assertEqual(noise_layer["minzoom"], bake_pmtiles.NOISE_MIN_ZOOM)
        self.assertEqual(noise_layer["maxzoom"], 12)
        self.assertEqual(noise_layer["fields"]["source_type"], "String")
        self.assertEqual(noise_layer["fields"]["metric"], "String")
        self.assertEqual(noise_layer["fields"]["round"], "Number")
        self.assertEqual(noise_layer["fields"]["db_low"], "Number")
        self.assertEqual(noise_layer["fields"]["db_value"], "String")

    def test_amenity_layer_metadata_declares_tier_name_and_conflict_class(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            service_desert_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
        )

        amenity_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "amenities"
        )

        self.assertEqual(amenity_layer["fields"]["category"], "String")
        self.assertEqual(amenity_layer["fields"]["tier"], "String")
        self.assertEqual(amenity_layer["fields"]["name"], "String")
        self.assertEqual(amenity_layer["fields"]["source"], "String")
        self.assertEqual(amenity_layer["fields"]["source_ref"], "String")
        self.assertEqual(amenity_layer["fields"]["conflict_class"], "String")

    def test_transport_reality_tile_sql_exports_gtfs_direct_fields(self) -> None:
        sql = str(bake_pmtiles._TRANSPORT_REALITY_TILE_SQL)

        self.assertIn("t.source_ref", sql)
        self.assertIn("t.weekday_morning_peak_deps", sql)
        self.assertIn("t.friday_evening_deps", sql)
        self.assertIn("t.transport_score_units", sql)
        self.assertIn("t.bus_daytime_deps", sql)
        self.assertIn("COALESCE(t.bus_daytime_headway_min, 0.0)", sql)
        self.assertIn("COALESCE(t.bus_frequency_tier, '')", sql)
        self.assertIn("t.bus_frequency_score_units", sql)
        self.assertIn("t.bus_active_days_mask_7d", sql)
        self.assertIn("t.bus_service_subtier", sql)
        self.assertIn("jsonb_array_elements_text(t.route_modes_json)", sql)
        self.assertIn("CASE WHEN t.is_unscheduled_stop THEN 1 ELSE 0 END", sql)
        self.assertNotIn("STRING_AGG", sql)
        self.assertNotIn("SUM(t.school_only_departures_30d)", sql)

    def test_noise_tile_sql_exports_display_fields(self) -> None:
        sql = str(bake_pmtiles._NOISE_TILE_SQL)

        self.assertIn("n.source_type", sql)
        self.assertIn("n.metric", sql)
        self.assertIn("n.round_number AS round", sql)
        self.assertIn("n.db_value", sql)
        self.assertNotIn("ST_Subdivide", sql)
        self.assertIn("'noise'", sql)

    def test_noise_tile_sql_reads_from_noise_polygons(self) -> None:
        """FIX 13: noise PMTiles layer must read from noise_polygons (compatibility table)."""
        sql = str(bake_pmtiles._NOISE_TILE_SQL)
        self.assertIn("noise_polygons", sql)

    def test_noise_source_layer_id_is_noise(self) -> None:
        """FIX 13: frontend source-layer name must remain 'noise'."""
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            service_desert_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
            noise_max_zoom=12,
        )
        layer_ids = [layer["id"] for layer in metadata["vector_layers"]]
        self.assertIn("noise", layer_ids, "noise layer must be declared in PMTiles metadata")

    def test_amenity_tile_sql_exports_tier_name_and_conflict_class(self) -> None:
        sql = str(bake_pmtiles._AMENITY_TILE_SQL)

        self.assertIn("a.category", sql)
        self.assertIn("a.tier", sql)
        self.assertIn("a.name", sql)
        self.assertIn("a.source", sql)
        self.assertIn("a.source_ref", sql)
        self.assertIn("a.conflict_class", sql)

    def test_bake_pmtiles_bbox_scans_only_coarse_zooms_and_uses_amenity_tiles_above_that(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

            def close(self) -> None:
                pass

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[(-6.2, 53.4)]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[(-6.1, 53.5)]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(bake_pmtiles, "_tile_range_for_bbox", return_value=(0, 0, 0, 0)) as bbox_mock,
                mock.patch.object(bake_pmtiles, "_point_tile_coordinates", return_value=[(1, 2)]) as point_tiles_mock,
                mock.patch.object(bake_pmtiles, "_vector_tile_mvt_bytes_by_flags", return_value=b"mvt"),
            ):
                bake_pmtiles.bake_pmtiles(
                    _FakeEngine(),
                    "build-key-123",
                    output_path,
                    min_zoom=11,
                    max_zoom=12,
                    amenity_min_zoom=9,
                    workers=1,
                    fine_grid_config={},
                )

        self.assertTrue(bbox_mock.call_args_list)
        self.assertTrue(point_tiles_mock.call_args_list)
        self.assertEqual(sorted({call.args[0] for call in bbox_mock.call_args_list}), [11])
        self.assertEqual(
            sorted({call.kwargs["zoom"] for call in point_tiles_mock.call_args_list}),
            [12],
        )

    def test_bake_pmtiles_caps_archive_header_max_zoom_at_15(self) -> None:
        captured = {}

        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                captured["header"] = header
                captured["metadata"] = metadata

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

            def close(self) -> None:
                pass

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(bake_pmtiles, "_tile_range_for_bbox", return_value=(0, 0, 0, 0)),
                mock.patch.object(bake_pmtiles, "_vector_tile_mvt_bytes_by_flags", return_value=b"mvt"),
            ):
                bake_pmtiles.bake_pmtiles(
                    _FakeEngine(),
                    "build-key-123",
                    output_path,
                    min_zoom=11,
                    max_zoom=19,
                    workers=1,
                    fine_grid_config={},
                )

        self.assertEqual(captured["header"]["max_zoom"], 15)


class PmtilesTileSpecIteratorTests(TestCase):
    def test_layer_flags_match_zoom_tiers(self) -> None:
        # Tiny 1-tile bbox at z11/z12 so we can enumerate every spec.
        specs = list(
            bake_pmtiles._iter_tile_specs(
                min_zoom=11,
                max_zoom=12,
                bbox=(-6.2, 53.4, -6.2, 53.4),
                amenity_points=[(-6.2, 53.4)],
                transport_reality_points=[(-6.2, 53.4)],
                coarse_grid_max_zoom=11,
                amenity_min_zoom=9,
                transport_reality_min_zoom=9,
                fine_grid_tile_coords_by_zoom={12: [(1, 2)]},
            )
        )

        # z11 = coarse grid zoom → grid + amenities + transport + service_deserts.
        z11_specs = [spec for spec in specs if spec[0] == 11]
        self.assertTrue(z11_specs)
        for z, _, _, layers in z11_specs:
            self.assertEqual(z, 11)
            self.assertTrue(layers & bake_pmtiles._LAYER_GRID)
            self.assertTrue(layers & bake_pmtiles._LAYER_AMENITIES)
            self.assertTrue(layers & bake_pmtiles._LAYER_TRANSPORT_REALITY)
            self.assertTrue(layers & bake_pmtiles._LAYER_SERVICE_DESERTS)
            self.assertFalse(layers & bake_pmtiles._LAYER_NOISE)

        # z12 > coarse_grid_max_zoom → only amenities + transport_reality.
        z12_specs = [spec for spec in specs if spec[0] == 12]
        self.assertTrue(z12_specs)
        for z, _, _, layers in z12_specs:
            self.assertEqual(z, 12)
            self.assertFalse(layers & bake_pmtiles._LAYER_GRID)
            self.assertFalse(layers & bake_pmtiles._LAYER_SERVICE_DESERTS)
            self.assertFalse(layers & bake_pmtiles._LAYER_NOISE)
        self.assertTrue(any(layers & bake_pmtiles._LAYER_FINE_GRID for _, _, _, layers in z12_specs))
        self.assertTrue(any(layers & bake_pmtiles._LAYER_AMENITIES for _, _, _, layers in z12_specs))
        self.assertTrue(
            any(layers & bake_pmtiles._LAYER_TRANSPORT_REALITY for _, _, _, layers in z12_specs)
        )

    def test_noise_tile_specs_are_included_above_coarse_zoom(self) -> None:
        specs = list(
            bake_pmtiles._iter_tile_specs(
                min_zoom=12,
                max_zoom=12,
                bbox=(-6.2, 53.4, -6.2, 53.4),
                amenity_points=[],
                transport_reality_points=[],
                coarse_grid_max_zoom=11,
                amenity_min_zoom=9,
                transport_reality_min_zoom=9,
                noise_tile_coords_by_zoom={12: [(10, 11)]},
            )
        )

        self.assertEqual(specs, [(12, 10, 11, bake_pmtiles._LAYER_NOISE)])

    def test_below_amenity_min_zoom_skips_amenity_layers(self) -> None:
        # At z5 with amenity_min_zoom=9, only grid + service_deserts should be included.
        specs = list(
            bake_pmtiles._iter_tile_specs(
                min_zoom=5,
                max_zoom=5,
                bbox=(-6.2, 53.4, -6.2, 53.4),
                amenity_points=[],
                transport_reality_points=[],
                coarse_grid_max_zoom=11,
                amenity_min_zoom=9,
                transport_reality_min_zoom=9,
            )
        )

        self.assertTrue(specs)
        for z, _, _, layers in specs:
            self.assertEqual(z, 5)
            self.assertTrue(layers & bake_pmtiles._LAYER_GRID)
            self.assertFalse(layers & bake_pmtiles._LAYER_AMENITIES)
            self.assertFalse(layers & bake_pmtiles._LAYER_TRANSPORT_REALITY)
            self.assertTrue(layers & bake_pmtiles._LAYER_SERVICE_DESERTS)

    def test_fine_grid_specs_exist_only_for_z12_to_z15(self) -> None:
        specs = list(
            bake_pmtiles._iter_tile_specs(
                min_zoom=12,
                max_zoom=15,
                bbox=(-6.2, 53.4, -6.2, 53.4),
                amenity_points=[],
                transport_reality_points=[],
                coarse_grid_max_zoom=11,
                amenity_min_zoom=9,
                transport_reality_min_zoom=9,
                fine_grid_tile_coords_by_zoom={
                    12: [(0, 0)],
                    13: [(0, 0)],
                    14: [(0, 0)],
                    15: [(0, 0)],
                },
            )
        )

        self.assertEqual(sorted({z for z, _, _, _ in specs}), [12, 13, 14, 15])
        for z, _, _, layers in specs:
            self.assertTrue(layers & bake_pmtiles._LAYER_FINE_GRID)
            self.assertFalse(layers & bake_pmtiles._LAYER_GRID)

    def test_chunked_splits_into_fixed_chunks(self) -> None:
        chunks = list(bake_pmtiles._chunked(range(10), 4))
        self.assertEqual(chunks, [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])


class PmtilesBakeReliabilityTests(TestCase):
    def test_bake_parallel_keeps_bounded_in_flight_futures(self) -> None:
        class _FakeWriter:
            def __init__(self) -> None:
                self.tiles: list[tuple[int, bytes]] = []

            def write_tile(self, tile_id, payload) -> None:
                self.tiles.append((tile_id, payload))

        class _FakeFuture:
            def __init__(self, chunk: list[tuple[int, int, int, int]]) -> None:
                self.chunk = list(chunk)

            def result(self) -> list[tuple[int, bytes]]:
                return [
                    (
                        bake_pmtiles.zxy_to_tileid(z, x, y),
                        f"tile-{z}-{x}-{y}".encode("utf-8"),
                    )
                    for z, x, y, _ in self.chunk
                ]

        class _FakePool:
            def __init__(self, *, max_workers: int) -> None:
                self.max_workers = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

            def submit(self, fn, chunk, build_key, db_url, fine_grid_config):
                del fn, build_key, db_url, fine_grid_config
                return _FakeFuture(chunk)

        pending_sizes: list[int] = []

        def _fake_wait(futures, *, return_when):
            del return_when
            pending_sizes.append(len(futures))
            done = next(iter(futures))
            return {done}, set(futures) - {done}

        tile_specs = [
            (12, idx, 0, bake_pmtiles._LAYER_FINE_GRID)
            for idx in range(10)
        ]
        writer = _FakeWriter()
        with (
            mock.patch.object(bake_pmtiles, "ProcessPoolExecutor", _FakePool),
            mock.patch.object(bake_pmtiles, "wait", side_effect=_fake_wait),
            mock.patch.object(bake_pmtiles, "_CHUNK_SIZE", 1),
        ):
            tiles_written, tiles_empty, per_zoom = bake_pmtiles._bake_parallel(
                writer=writer,
                build_key="build-key-123",
                db_url="postgresql://example",
                tile_specs=tile_specs,
                workers=2,
                total_tile_count=len(tile_specs),
                fine_grid_config={"shell_dir": "shell", "score_dir": "scores"},
            )

        self.assertEqual(tiles_written, len(tile_specs))
        self.assertEqual(tiles_empty, 0)
        self.assertEqual(per_zoom, {12: len(tile_specs)})
        self.assertEqual(len(writer.tiles), len(tile_specs))
        self.assertTrue(pending_sizes)
        self.assertLessEqual(max(pending_sizes), 4)

    def test_bake_pmtiles_retries_broken_pool_with_half_worker_count(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata
                self.handle.write(b"pmtiles")

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            parallel_error = bake_pmtiles.ParallelBakeWorkerFailure(
                workers=4,
                completed_specs=1,
                total_tile_count=1,
                has_fine_grid=True,
                reason="worker crashed",
            )
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(
                    bake_pmtiles,
                    "fine_grid_tile_coordinates_by_zoom",
                    return_value={12: [(0, 0)]},
                ),
                mock.patch.object(bake_pmtiles, "_fine_grid_shard_count", return_value=278),
                mock.patch.object(
                    bake_pmtiles,
                    "_bake_parallel",
                    side_effect=[parallel_error, (1, 0, {12: 1})],
                ) as parallel_mock,
            ):
                result = bake_pmtiles.bake_pmtiles(
                    _FakeEngine(),
                    "build-key-123",
                    output_path,
                    min_zoom=12,
                    max_zoom=12,
                    workers=8,
                    fine_grid_config={"shell_dir": "shell", "score_dir": "scores"},
                )
                self.assertTrue(output_path.exists())

        self.assertEqual(result, output_path)
        self.assertEqual(
            [call.kwargs["workers"] for call in parallel_mock.call_args_list],
            [4, 2],
        )

    def test_bake_pmtiles_retry_exhaustion_preserves_existing_archive(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata
                self.handle.write(b"new-archive")

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            output_path.write_bytes(b"old-archive")
            temp_output_path = output_path.with_name(output_path.name + ".tmp")
            first_error = bake_pmtiles.ParallelBakeWorkerFailure(
                workers=4,
                completed_specs=1,
                total_tile_count=1,
                has_fine_grid=True,
                reason="worker crashed",
            )
            second_error = bake_pmtiles.ParallelBakeWorkerFailure(
                workers=2,
                completed_specs=1,
                total_tile_count=1,
                has_fine_grid=True,
                reason="worker crashed again",
            )
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(
                    bake_pmtiles,
                    "fine_grid_tile_coordinates_by_zoom",
                    return_value={12: [(0, 0)]},
                ),
                mock.patch.object(bake_pmtiles, "_fine_grid_shard_count", return_value=278),
                mock.patch.object(
                    bake_pmtiles,
                    "_bake_parallel",
                    side_effect=[first_error, second_error],
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "PMTiles bake failed after retry"):
                    bake_pmtiles.bake_pmtiles(
                        _FakeEngine(),
                        "build-key-123",
                        output_path,
                        min_zoom=12,
                        max_zoom=12,
                        workers=8,
                        fine_grid_config={"shell_dir": "shell", "score_dir": "scores"},
                    )

            self.assertEqual(output_path.read_bytes(), b"old-archive")
            self.assertFalse(temp_output_path.exists())

    def test_bake_pmtiles_failure_cleans_temp_output_and_preserves_final_archive(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            output_path.write_bytes(b"old-archive")
            temp_output_path = output_path.with_name(output_path.name + ".tmp")
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(bake_pmtiles, "_tile_range_for_bbox", return_value=(0, 0, 0, 0)),
                mock.patch.object(
                    bake_pmtiles,
                    "_bake_sequential",
                    side_effect=RuntimeError("boom"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    bake_pmtiles.bake_pmtiles(
                        _FakeEngine(),
                        "build-key-123",
                        output_path,
                        min_zoom=11,
                        max_zoom=11,
                        workers=1,
                        fine_grid_config={},
                    )

            self.assertEqual(output_path.read_bytes(), b"old-archive")
            self.assertFalse(temp_output_path.exists())

    def test_bake_pmtiles_success_replaces_temp_output_atomically(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata
                self.handle.write(b"new-archive")

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            output_path.write_bytes(b"old-archive")
            temp_output_path = output_path.with_name(output_path.name + ".tmp")
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[]),
                mock.patch.object(bake_pmtiles, "_load_noise_bounds", return_value=[]),
                mock.patch.object(bake_pmtiles, "_tile_range_for_bbox", return_value=(0, 0, 0, 0)),
                mock.patch.object(bake_pmtiles, "_bake_sequential", return_value=(0, 0, {})),
            ):
                result = bake_pmtiles.bake_pmtiles(
                    _FakeEngine(),
                    "build-key-123",
                    output_path,
                    min_zoom=11,
                    max_zoom=11,
                    workers=1,
                    fine_grid_config={},
                )

            self.assertEqual(result, output_path)
            self.assertEqual(output_path.read_bytes(), b"new-archive")
            self.assertFalse(temp_output_path.exists())
