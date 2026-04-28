from __future__ import annotations

import os
import sys
import tempfile
from itertools import islice
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest import mock

import geopandas as gpd
from shapely.geometry import box

import noise.loader as noise_loader
from noise.loader import (
    candidates_cache_key,
    iter_noise_candidate_rows_cached,
    materialize_effective_noise_rows,
    ni_round1_class_snapshot,
    normalize_ni_gridcode_band,
    normalize_noise_band,
)


class NoiseLoaderNormalizationTests(TestCase):
    def test_filegdb_chunk_windows_split_into_four_coarse_reads(self) -> None:
        self.assertEqual(
            noise_loader._filegdb_chunk_windows(462, 4),
            [(0, 116), (116, 116), (232, 115), (347, 115)],
        )
        self.assertEqual(
            noise_loader._filegdb_chunk_windows(8, 4),
            [(0, 2), (2, 2), (4, 2), (6, 2)],
        )

    def test_filegdb_chunk_count_env_allows_only_two_or_four(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(noise_loader._noise_filegdb_chunk_count(), 4)
        with mock.patch.dict(os.environ, {"NOISE_FILEGDB_CHUNKS": "2"}):
            self.assertEqual(noise_loader._noise_filegdb_chunk_count(), 2)
        with mock.patch.dict(os.environ, {"NOISE_FILEGDB_CHUNKS": "3"}):
            with self.assertRaisesRegex(ValueError, "either 2 or 4"):
                noise_loader._noise_filegdb_chunk_count()

    def test_filegdb_reader_uses_coarse_skip_and_limit_windows(self) -> None:
        read_calls = []

        def _read_dataframe(path, **kwargs):
            read_calls.append((path, kwargs))
            offset = int(kwargs["skip_features"])
            limit = int(kwargs["max_features"])
            return gpd.GeoDataFrame(
                {
                    "Time": ["Lden"] * limit,
                    "Db_Low": [55.0] * limit,
                    "Db_High": [59.0] * limit,
                    "DbValue": ["55-59"] * limit,
                    "ReportPeriod": ["Round 4"] * limit,
                    "geometry": [box(0, 0, 1, 1)] * limit,
                },
                geometry="geometry",
                crs=4326,
                index=list(range(offset + 1, offset + limit + 1)),
            )

        fake_pyogrio = SimpleNamespace(
            list_layers=mock.Mock(return_value=[("Noise_R4_Road", "MultiPolygon")]),
            read_info=mock.Mock(
                return_value={
                    "features": 8,
                    "fields": [
                        "Time",
                        "Db_Low",
                        "Db_High",
                        "DbValue",
                        "ReportPeriod",
                    ],
                    "crs": "EPSG:4326",
                }
            ),
            read_dataframe=mock.Mock(side_effect=_read_dataframe),
        )
        progress = mock.Mock()

        with (
            mock.patch.dict(sys.modules, {"pyogrio": fake_pyogrio}),
            mock.patch.object(
                noise_loader.tempfile,
                "TemporaryDirectory",
                side_effect=AssertionError("FileGDB reader must not split to temp files"),
            ),
            mock.patch.dict(os.environ, {}, clear=True),
        ):
            frames = list(
                noise_loader._read_gdb_member_chunks(
                    noise_loader.NOISE_DATA_DIR / "NOISE_Round4.zip",
                    "Noise R4 DataDownload/Noise_R4_Road.gdb",
                    progress_cb=progress,
                )
            )

        self.assertEqual(len(frames), 4)
        self.assertEqual(
            [
                (
                    call_kwargs["skip_features"],
                    call_kwargs["max_features"],
                    call_kwargs["fid_as_index"],
                )
                for _, call_kwargs in read_calls
            ],
            [(0, 2, True), (2, 2, True), (4, 2, True), (6, 2, True)],
        )
        self.assertTrue(all("DbValue" in call_kwargs["columns"] for _, call_kwargs in read_calls))
        progress_details = [call.kwargs["detail"] for call in progress.call_args_list]
        self.assertTrue(any("8 features, 4 chunks" in detail for detail in progress_details))
        self.assertTrue(any("offset=0 limit=2" in detail for detail in progress_details))

    def test_roi_noise_band_normalization_handles_open_ended_values(self) -> None:
        low, high, label = normalize_noise_band("75-99", db_low=75, db_high=99)

        self.assertEqual(low, 75)
        self.assertEqual(high, 99)
        self.assertEqual(label, "75+")

        low, high, label = normalize_noise_band("75+")

        self.assertEqual(low, 75)
        self.assertEqual(high, 99)
        self.assertEqual(label, "75+")

    def test_noise_band_allows_70_plus(self) -> None:
        low, high, label = normalize_noise_band("70+", db_low=70, db_high=99)

        self.assertEqual(low, 70)
        self.assertEqual(high, 99)
        self.assertEqual(label, "70+")

    def test_noise_band_does_not_relabel_70_plus_to_75_plus(self) -> None:
        # Explicit string label "70+"
        _, _, label = normalize_noise_band("70+")
        self.assertEqual(label, "70+")
        self.assertNotEqual(label, "75+")

        # Numeric bounds low=70, high=99 also produce "70+", not "75+"
        _, _, label = normalize_noise_band(db_low=70, db_high=99)
        self.assertEqual(label, "70+")
        self.assertNotEqual(label, "75+")

        # Range string "70-99" normalises to "70+", not "75+"
        _, _, label = normalize_noise_band("70-99")
        self.assertEqual(label, "70+")
        self.assertNotEqual(label, "75+")

    def test_roi_noise_band_normalization_handles_numeric_bounds(self) -> None:
        low, high, label = normalize_noise_band(db_low=55, db_high=59)

        self.assertEqual(low, 55)
        self.assertEqual(high, 59)
        self.assertEqual(label, "55-59")

    def test_ni_round1_class_mapping_for_lden_is_round_aware(self) -> None:
        self.assertEqual(
            normalize_ni_gridcode_band(
                1,
                round_number=1,
                source_type="airport",
                metric="Lden",
            ),
            (45.0, 49.0, "45-49"),
        )
        self.assertEqual(
            normalize_ni_gridcode_band(
                3,
                round_number=1,
                source_type="airport",
                metric="Lden",
            ),
            (55.0, 59.0, "55-59"),
        )

    def test_ni_round1_small_class_code_does_not_produce_2_6(self) -> None:
        _, _, label = normalize_ni_gridcode_band(
            1,
            round_number=1,
            source_type="airport",
            metric="Lden",
        )
        self.assertNotEqual(label, "2-6")

    def test_ni_round3_threshold_style_mapping_still_works(self) -> None:
        self.assertEqual(
            normalize_ni_gridcode_band(
                49,
                round_number=3,
                source_type="road",
                metric="Lnight",
            ),
            (50.0, 54.0, "50-54"),
        )
        self.assertEqual(
            normalize_ni_gridcode_band(
                54,
                round_number=3,
                source_type="road",
                metric="Lden",
            ),
            (55.0, 59.0, "55-59"),
        )
        self.assertEqual(
            normalize_ni_gridcode_band(
                1000,
                round_number=3,
                source_type="road",
                metric="Lden",
            ),
            (None, None, None),
        )

    def test_ni_round2_small_unmapped_gridcode_raises_clear_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "NI Round 2 gridcode 1 .* no verified threshold mapping",
        ):
            normalize_ni_gridcode_band(
                1,
                round_number=2,
                source_type="airport",
                metric="Lden",
            )

    def test_ni_round1_snapshot_has_known_bia_pairs_when_dataset_available(self) -> None:
        zip_path = noise_loader.NOISE_DATA_DIR / noise_loader.NI_ZIP_BY_ROUND[1]
        if not zip_path.exists():
            self.skipTest(f"dataset not available: {zip_path}")
        try:
            rows = ni_round1_class_snapshot()
        except RuntimeError as exc:
            self.skipTest(str(exc))

        bia_lden = {
            (row["raw_gridcode"], row["noise_class_label"])
            for row in rows
            if "major_airports/bia/r1_bia_lden.shp" in row["source_layer"].lower()
        }
        bia_lnight = {
            (row["raw_gridcode"], row["noise_class_label"])
            for row in rows
            if "major_airports/bia/r1_bia_lngt.shp" in row["source_layer"].lower()
        }
        self.assertIn((1, "< 50"), bia_lden)
        self.assertIn((2, "50 - 54"), bia_lden)
        self.assertIn((7, ">= 75"), bia_lden)
        self.assertIn((1, "< 45"), bia_lnight)
        self.assertIn((2, "45 - 49"), bia_lnight)
        self.assertIn((7, ">= 70"), bia_lnight)

    def test_ni_unknown_class_error_includes_source_layer_ref_and_gridcode(self) -> None:
        fake_gdf = gpd.GeoDataFrame(
            {
                "GRIDCODE": [99],
                "geometry": [box(0, 0, 1, 1)],
            },
            geometry="geometry",
            crs=4326,
            index=[123],
        )
        with mock.patch.object(
            noise_loader,
            "_preferred_ni_entries",
            return_value=["END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp"],
        ):
            with mock.patch.object(noise_loader, "_read_vector_member", return_value=fake_gdf):
                with mock.patch.object(Path, "exists", return_value=True):
                    with mock.patch("noise.loader.zipfile.ZipFile") as _zipfile_mock:
                        _zipfile_mock.return_value.__enter__.return_value.infolist.return_value = [
                            mock.Mock(is_dir=lambda: False, filename="END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp")
                        ]
                        with self.assertRaises(ValueError) as ctx:
                            list(
                                noise_loader._ni_round_candidate_rows(
                                    data_dir=Path("."),
                                    round_number=1,
                                    zip_name="end_noisedata_round1.zip",
                                )
                            )
        msg = str(ctx.exception)
        self.assertIn("source_layer=END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp", msg)
        self.assertIn("source_ref=end_noisedata_round1.zip:END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp:123", msg)
        self.assertIn("raw_gridcode=99", msg)


class NoiseFallbackTests(TestCase):
    def test_newer_round_geometry_masks_older_round_geometry(self) -> None:
        candidate_rows = [
            {
                "jurisdiction": "roi",
                "source_type": "road",
                "metric": "Lden",
                "round_number": 4,
                "db_value": "60-64",
                "source_dataset": "new.zip",
                "source_layer": "new",
                "source_ref": "new-1",
                "geom": box(0, 0, 1, 1),
            },
            {
                "jurisdiction": "roi",
                "source_type": "road",
                "metric": "Lden",
                "round_number": 3,
                "db_value": "55-59",
                "source_dataset": "old.zip",
                "source_layer": "old",
                "source_ref": "old-1",
                "geom": box(0.5, 0, 1.5, 1),
            },
        ]

        rows = materialize_effective_noise_rows(candidate_rows, box(-1, -1, 2, 2))

        self.assertEqual(len(rows), 2)
        by_round = {row["round_number"]: row["geom"].area for row in rows}
        self.assertAlmostEqual(by_round[4], 1.0)
        self.assertAlmostEqual(by_round[3], 0.5)

    def test_same_group_only_masks_within_jurisdiction_source_and_metric(self) -> None:
        candidate_rows = [
            {
                "jurisdiction": "roi",
                "source_type": "road",
                "metric": "Lden",
                "round_number": 4,
                "db_value": "60-64",
                "source_dataset": "road.zip",
                "source_layer": "road",
                "source_ref": "road-1",
                "geom": box(0, 0, 1, 1),
            },
            {
                "jurisdiction": "roi",
                "source_type": "rail",
                "metric": "Lden",
                "round_number": 3,
                "db_value": "55-59",
                "source_dataset": "rail.zip",
                "source_layer": "rail",
                "source_ref": "rail-1",
                "geom": box(0, 0, 1, 1),
            },
        ]

        rows = materialize_effective_noise_rows(candidate_rows, box(-1, -1, 2, 2))

        self.assertEqual(len(rows), 2)
        self.assertEqual(sorted(row["source_type"] for row in rows), ["rail", "road"])
        self.assertTrue(all(row["geom"].area == 1.0 for row in rows))


class NoiseCandidateCacheTests(TestCase):
    def _fake_candidate_row(self, *, source_type: str, round_number: int, geom):
        return {
            "jurisdiction": "roi",
            "source_type": source_type,
            "metric": "Lden",
            "round_number": round_number,
            "report_period": f"Round {round_number}",
            "db_low": 55.0,
            "db_high": 59.0,
            "db_value": "55-59",
            "source_dataset": f"{source_type}.zip",
            "source_layer": source_type,
            "source_ref": f"{source_type}-1",
            "geom": geom,
        }

    def test_cache_warms_on_miss_and_returns_identical_rows_on_hit(self) -> None:
        rows = [
            self._fake_candidate_row(source_type="road", round_number=4, geom=box(0, 0, 1, 1)),
            self._fake_candidate_row(source_type="rail", round_number=3, geom=box(1, 1, 2, 2)),
        ]
        call_count = {"value": 0}

        def fake_iter(**_kwargs):
            call_count["value"] += 1
            for row in rows:
                yield row

        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            study_area = box(-1, -1, 3, 3)
            key = candidates_cache_key(study_area)
            cache_entry_dir = noise_loader._candidates_cache_entry_dir(cache_dir, key)

            with mock.patch.object(
                noise_loader,
                "iter_noise_candidate_rows",
                side_effect=fake_iter,
            ):
                first = list(
                    iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                )
            self.assertEqual(call_count["value"], 1)
            self.assertEqual(len(first), 2)

            self.assertTrue(cache_entry_dir.exists())
            self.assertTrue((cache_entry_dir / "manifest.json").exists())
            self.assertTrue(any(cache_entry_dir.glob("part-*.pkl.gz")))

            with mock.patch.object(
                noise_loader,
                "iter_noise_candidate_rows",
                side_effect=AssertionError("loader must not run on cache hit"),
            ):
                second = list(
                    iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                )

        self.assertEqual(len(second), len(first))
        for original, hit in zip(first, second):
            self.assertEqual(original["source_type"], hit["source_type"])
            self.assertEqual(original["round_number"], hit["round_number"])
            self.assertEqual(original["geom"].wkb, hit["geom"].wkb)

    def test_cache_key_changes_when_dataset_signature_changes(self) -> None:
        study_area = box(0, 0, 1, 1)
        with tempfile.TemporaryDirectory() as tmp_name:
            data_dir = Path(tmp_name)
            with mock.patch.object(noise_loader, "dataset_signature", return_value="sigA"):
                key_a = candidates_cache_key(study_area, data_dir)
            with mock.patch.object(noise_loader, "dataset_signature", return_value="sigB"):
                key_b = candidates_cache_key(study_area, data_dir)
        self.assertNotEqual(key_a, key_b)

    def test_cache_key_changes_when_study_area_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            data_dir = Path(tmp_name)
            with mock.patch.object(noise_loader, "dataset_signature", return_value="sig"):
                key_small = candidates_cache_key(box(0, 0, 1, 1), data_dir)
                key_large = candidates_cache_key(box(0, 0, 10, 10), data_dir)
        self.assertNotEqual(key_small, key_large)

    def test_corrupt_cache_falls_back_to_loader(self) -> None:
        rows = [
            self._fake_candidate_row(source_type="road", round_number=4, geom=box(0, 0, 1, 1)),
        ]
        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            study_area = box(-1, -1, 2, 2)
            key = candidates_cache_key(study_area)
            cache_entry_dir = noise_loader._candidates_cache_entry_dir(cache_dir, key)
            cache_entry_dir.mkdir(parents=True, exist_ok=True)
            (cache_entry_dir / "manifest.json").write_text("{bad json", encoding="utf-8")

            with mock.patch.object(
                noise_loader,
                "iter_noise_candidate_rows",
                side_effect=lambda **_: iter(rows),
            ):
                rebuilt = list(
                    iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                )

            self.assertEqual(len(rebuilt), 1)
            self.assertTrue((cache_entry_dir / "manifest.json").exists())

    def test_cache_hit_streams_without_full_deserialize(self) -> None:
        rows = [
            self._fake_candidate_row(source_type="road", round_number=4, geom=box(0, 0, 1, 1)),
            self._fake_candidate_row(source_type="rail", round_number=3, geom=box(1, 1, 2, 2)),
            self._fake_candidate_row(source_type="airport", round_number=2, geom=box(2, 2, 3, 3)),
        ]
        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            study_area = box(-1, -1, 4, 4)
            with mock.patch.object(
                noise_loader,
                "iter_noise_candidate_rows",
                side_effect=lambda **_: iter(rows),
            ):
                # Warm cache.
                list(
                    iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                )

            call_count = {"count": 0}
            original_deserialize = noise_loader._deserialize_candidate

            def _counting_deserialize(payload):
                call_count["count"] += 1
                return original_deserialize(payload)

            with mock.patch.object(noise_loader, "_deserialize_candidate", side_effect=_counting_deserialize):
                it = iter(
                    iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                )
                first = next(it)

            self.assertEqual(call_count["count"], 1)
            self.assertEqual(first["source_type"], "road")

    def test_cache_miss_streams_before_source_exhausted(self) -> None:
        produced = {"count": 0}

        def fake_iter(**_kwargs):
            for idx in range(10_000):
                produced["count"] += 1
                yield {
                    "jurisdiction": "roi",
                    "source_type": "road",
                    "metric": "Lden",
                    "round_number": 4,
                    "report_period": "Round 4",
                    "db_low": 55.0,
                    "db_high": 59.0,
                    "db_value": "55-59",
                    "source_dataset": "road.zip",
                    "source_layer": "road",
                    "source_ref": f"road-{idx}",
                    "geom": box(0, 0, 1, 1),
                }

        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            study_area = box(-1, -1, 2, 2)
            with mock.patch.object(noise_loader, "_ogr2ogr_available", return_value=False):
                with mock.patch.object(noise_loader, "iter_noise_candidate_rows", side_effect=fake_iter):
                    it = iter_noise_candidate_rows_cached(
                        study_area_wgs84=study_area,
                        cache_dir=cache_dir,
                        workers=1,
                    )
                    first_500 = list(islice(it, 500))

        self.assertEqual(len(first_500), 500)
        self.assertLess(
            produced["count"],
            10_000,
            "cache miss must yield before fully exhausting source generator",
        )

    def test_cached_loader_source_has_no_full_list_materialization(self) -> None:
        import inspect

        src = inspect.getsource(noise_loader.iter_noise_candidate_rows_cached)
        self.assertNotIn("serialized = [", src)
        self.assertNotIn("deserialized = [", src)

    def test_legacy_single_file_cache_is_deleted_unless_explicitly_allowed(self) -> None:
        rows = [
            self._fake_candidate_row(source_type="road", round_number=4, geom=box(0, 0, 1, 1)),
        ]
        produced = {"count": 0}

        def fake_iter(**_kwargs):
            produced["count"] += 1
            yield rows[0]

        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            study_area = box(-1, -1, 2, 2)
            key = candidates_cache_key(study_area)
            legacy_cache_path = noise_loader._candidates_cache_path(cache_dir, key)
            legacy_cache_path.parent.mkdir(parents=True, exist_ok=True)
            noise_loader._save_cached_candidates(
                legacy_cache_path,
                [noise_loader._serialize_candidate(rows[0])],
            )
            self.assertTrue(legacy_cache_path.exists())

            with mock.patch.dict(os.environ, {"NOISE_ALLOW_LEGACY_CANDIDATE_CACHE": "0"}, clear=False):
                with mock.patch.object(noise_loader, "iter_noise_candidate_rows", side_effect=fake_iter):
                    out = list(
                        iter_noise_candidate_rows_cached(
                            study_area_wgs84=study_area,
                            cache_dir=cache_dir,
                            workers=1,
                        )
                    )

        self.assertEqual(len(out), 1)
        self.assertEqual(produced["count"], 1, "legacy cache should be ignored/deleted unless explicitly allowed")
