from __future__ import annotations

import os
import sys
import tempfile
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

    def test_roi_noise_band_normalization_handles_numeric_bounds(self) -> None:
        low, high, label = normalize_noise_band(db_low=55, db_high=59)

        self.assertEqual(low, 55)
        self.assertEqual(high, 59)
        self.assertEqual(label, "55-59")

    def test_ni_gridcode_maps_to_display_band_and_excludes_no_data(self) -> None:
        self.assertEqual(normalize_ni_gridcode_band(49), (50.0, 54.0, "50-54"))
        self.assertEqual(normalize_ni_gridcode_band(54), (55.0, 59.0, "55-59"))
        self.assertEqual(normalize_ni_gridcode_band(74), (75.0, 99.0, "75+"))
        self.assertEqual(normalize_ni_gridcode_band(1000), (None, None, None))


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

            cache_files = list(cache_dir.glob("noise_candidates_*.pkl.gz"))
            self.assertEqual(len(cache_files), 1)

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
            cache_path = noise_loader._candidates_cache_path(cache_dir, key)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(b"not a valid gzip pickle")

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
            self.assertTrue(cache_path.exists())
