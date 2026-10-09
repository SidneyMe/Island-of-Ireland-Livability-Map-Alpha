from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import scripts.fetch_datasets as fetch


def _zip_bytes(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        for name, payload in members.items():
            bundle.writestr(name, payload)
    return buffer.getvalue()


def _dataset(key: str) -> fetch.Dataset:
    return {dataset.key: dataset for dataset in fetch.DATASETS}[key]


class CandidateUrlTests(TestCase):
    def test_mirror_is_tried_before_official_by_default(self) -> None:
        with mock.patch.dict(os.environ, {"DATASET_MIRROR_BASE_URL": "https://mirror.example/v9/"}, clear=True):
            urls = fetch.candidate_urls(_dataset("gtfs_nta"))
        self.assertEqual(
            urls,
            [
                "https://mirror.example/v9/nta_gtfs.zip",
                "https://www.transportforireland.ie/transitData/Data/GTFS_All.zip",
            ],
        )

    def test_refresh_prefers_official_source_for_refreshable_datasets(self) -> None:
        with mock.patch.dict(os.environ, {"DATASET_MIRROR_BASE_URL": "https://mirror.example/v9"}, clear=True):
            urls = fetch.candidate_urls(_dataset("osm"), refresh=True)
        self.assertEqual(urls[0], _dataset("osm").official_urls[0])
        self.assertEqual(urls[-1], "https://mirror.example/v9/ireland-and-northern-ireland-latest.osm.pbf")

    def test_explicit_url_env_wins(self) -> None:
        with mock.patch.dict(os.environ, {"NOISE_ROUND4_URL": "https://example.test/r4.zip"}, clear=True):
            self.assertEqual(fetch.candidate_urls(_dataset("noise_roi_round4")), ["https://example.test/r4.zip"])

    def test_default_mirror_is_release_v2(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            urls = fetch.candidate_urls(_dataset("noise_ni_round1"))
        self.assertEqual(urls, [fetch.DEFAULT_MIRROR_BASE_URL + "/end_noisedata_round1.zip"])

    def test_every_pipeline_input_is_listed(self) -> None:
        keys = {dataset.key for dataset in fetch.DATASETS}
        for required in ("osm", "roi_boundary", "ni_boundary", "gtfs_nta", "gtfs_translink", "overture", "main_island"):
            self.assertIn(required, keys)
        self.assertEqual(sum(1 for key in keys if key.startswith("noise_")), 6)


class FetchDatasetTests(TestCase):
    def test_existing_file_is_reused_without_download(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "gtfs" / "nta_gtfs.zip"
            target.parent.mkdir(parents=True)
            target.write_bytes(_zip_bytes({"stops.txt": b"x"}))
            download = mock.Mock()
            with mock.patch.dict(os.environ, {}, clear=True):
                result = fetch.fetch_dataset(_dataset("gtfs_nta"), root=root, download=download)
        self.assertEqual(result, "reused")
        download.assert_not_called()

    def test_refresh_redownloads_refreshable_dataset(self) -> None:
        payload = _zip_bytes({"stops.txt": b"fresh"})
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "gtfs" / "nta_gtfs.zip"
            target.parent.mkdir(parents=True)
            target.write_bytes(_zip_bytes({"stops.txt": b"stale"}))
            with mock.patch.dict(os.environ, {}, clear=True):
                result = fetch.fetch_dataset(
                    _dataset("gtfs_nta"),
                    root=root,
                    refresh=True,
                    download=lambda url, dest: dest.write_bytes(payload),
                )
            self.assertEqual(result, "downloaded")
            self.assertEqual(target.read_bytes(), payload)

    def test_invalid_zip_falls_back_to_next_source(self) -> None:
        good = _zip_bytes({"stops.txt": b"ok"})
        calls: list[str] = []

        def download(url: str, dest: Path) -> None:
            calls.append(url)
            dest.write_bytes(b"<html>not a zip</html>" if "mirror" in url else good)

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with mock.patch.dict(os.environ, {"DATASET_MIRROR_BASE_URL": "https://mirror.example/v2"}, clear=True):
                result = fetch.fetch_dataset(_dataset("gtfs_nta"), root=root, download=download, sleep=lambda _: None)
            self.assertEqual(result, "downloaded")
            self.assertTrue(zipfile.is_zipfile(root / "gtfs" / "nta_gtfs.zip"))
            self.assertFalse((root / "gtfs" / "nta_gtfs.zip.part").exists())
        self.assertEqual(calls.count("https://mirror.example/v2/nta_gtfs.zip"), fetch._RETRIES)
        self.assertEqual(calls[-1], "https://www.transportforireland.ie/transitData/Data/GTFS_All.zip")

    def test_main_island_archive_is_extracted(self) -> None:
        archive = _zip_bytes({name: b"shape" for name in (
            "ireland_main_island.shp",
            "ireland_main_island.shx",
            "ireland_main_island.dbf",
            "ireland_main_island.prj",
            "ireland_main_island.cpg",
        )})
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with mock.patch.dict(os.environ, {}, clear=True):
                fetch.fetch_dataset(
                    _dataset("main_island"),
                    root=root,
                    download=lambda url, dest: dest.write_bytes(archive),
                )
            self.assertTrue((root / "ireland_main_island_shp" / "ireland_main_island.shp").is_file())
            self.assertTrue((root / "ireland_main_island_shp" / "ireland_main_island.cpg").is_file())

    def test_all_sources_failing_raises(self) -> None:
        def download(url: str, dest: Path) -> None:
            raise OSError("connection reset")

        with TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "Download failed for NI Round 1 noise archive"):
                    fetch.fetch_dataset(
                        _dataset("noise_ni_round1"),
                        root=Path(tmp),
                        download=download,
                        sleep=lambda _: None,
                    )

    def test_main_reports_missing_datasets_with_nonzero_exit(self) -> None:
        with mock.patch.object(fetch, "fetch_dataset", side_effect=RuntimeError("boom")):
            with mock.patch("builtins.print"):
                self.assertEqual(fetch.main(["--only", "osm"]), 1)
