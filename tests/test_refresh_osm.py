from __future__ import annotations

import hashlib
import io
import json
import urllib.error
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import scripts.refresh_osm as refresh_osm


_SAMPLE_CONTENT = b"fake osm pbf content for testing" * 64
_LAST_MODIFIED = "Mon, 14 Apr 2026 00:00:00 GMT"


class _MockHTTPHeaders:
    def __init__(self, mapping: dict[str, str]) -> None:
        self._mapping = mapping

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._mapping.get(key, default)


class _MockHTTPResponse:
    def __init__(
        self,
        content: bytes = _SAMPLE_CONTENT,
        last_modified: str | None = _LAST_MODIFIED,
    ) -> None:
        self._stream = io.BytesIO(content)
        header_map: dict[str, str] = {}
        if last_modified is not None:
            header_map["Last-Modified"] = last_modified
        self.headers = _MockHTTPHeaders(header_map)

    def read(self, n: int = -1) -> bytes:
        return self._stream.read(n)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _patch_paths(tmp_dir: Path):
    extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"
    return (
        mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
        mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
    )


class DownloadExtractTests(TestCase):
    def test_download_writes_file_and_manifest(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen", return_value=_MockHTTPResponse()),
                mock.patch.object(refresh_osm, "_fetch_md5", return_value=None),
            ):
                result = refresh_osm.download_extract()

            self.assertFalse(result["skipped"])
            self.assertEqual(result["reason"], "downloaded")
            self.assertTrue(extract_path.exists(), "extract file should exist after download")
            manifest_path = tmp_dir / refresh_osm.EXTRACT_MANIFEST_NAME
            self.assertTrue(manifest_path.exists(), "manifest file should exist after download")
            # no temp files left behind
            tmp_files = list(tmp_dir.glob("*.tmp"))
            self.assertEqual(tmp_files, [], "no .tmp files should remain")

    def test_304_returns_skipped_not_modified(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"
            seed_manifest = {"extract_date": _LAST_MODIFIED, "source_url": refresh_osm.GEOFABRIK_URL}
            (tmp_dir / refresh_osm.EXTRACT_MANIFEST_NAME).write_text(
                json.dumps(seed_manifest), encoding="utf-8"
            )

            not_modified = urllib.error.HTTPError(
                refresh_osm.GEOFABRIK_URL, 304, "Not Modified", {}, None
            )
            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen", side_effect=not_modified),
            ):
                result = refresh_osm.download_extract()

        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "not-modified")
        self.assertFalse(extract_path.exists(), "extract should not be created on 304")

    def test_manifest_fields_correct(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch(
                    "urllib.request.urlopen",
                    return_value=_MockHTTPResponse(content=_SAMPLE_CONTENT, last_modified=_LAST_MODIFIED),
                ),
                mock.patch.object(refresh_osm, "_fetch_md5", return_value=None),
            ):
                result = refresh_osm.download_extract()

            manifest = result["manifest"]
            self.assertIsNotNone(manifest)
            self.assertEqual(manifest["extract_date"], _LAST_MODIFIED)
            self.assertEqual(manifest["source_url"], refresh_osm.GEOFABRIK_URL)
            self.assertIsNotNone(manifest.get("downloaded_utc"))
            sha256 = manifest.get("sha256", "")
            self.assertEqual(len(sha256), 64, "sha256 should be 64 hex chars")
            self.assertEqual(sha256, _sha256(_SAMPLE_CONTENT))

            # manifest on disk should match
            manifest_path = tmp_dir / refresh_osm.EXTRACT_MANIFEST_NAME
            on_disk = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(on_disk["extract_date"], _LAST_MODIFIED)
            self.assertEqual(on_disk["sha256"], _sha256(_SAMPLE_CONTENT))

    def test_temp_file_cleaned_up_on_failure(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"

            broken_response = _MockHTTPResponse()
            broken_response._stream = mock.Mock()
            broken_response._stream.read = mock.Mock(side_effect=OSError("network died"))

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen", return_value=broken_response),
                self.assertRaises(OSError),
            ):
                refresh_osm.download_extract()

        tmp_files = list(tmp_dir.glob("*.tmp"))
        self.assertEqual(tmp_files, [], "no .tmp files should remain after failure")
        self.assertFalse(extract_path.exists(), "extract should not be created after failure")

    def test_dry_run_writes_nothing(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen") as urlopen_mock,
            ):
                result = refresh_osm.download_extract(dry_run=True)

            urlopen_mock.assert_not_called()
            self.assertTrue(result["skipped"])
            self.assertEqual(result["reason"], "dry-run")
            # temp dir should be completely empty
            all_files = list(tmp_dir.iterdir())
            self.assertEqual(all_files, [], "dry-run must not write any files")

    def test_force_omits_if_modified_since(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"
            # seed a manifest so a non-force run would include the header
            (tmp_dir / refresh_osm.EXTRACT_MANIFEST_NAME).write_text(
                json.dumps({"extract_date": _LAST_MODIFIED}), encoding="utf-8"
            )

            captured_requests: list[urllib.request.Request] = []

            def capture_urlopen(req, **kwargs):
                captured_requests.append(req)
                return _MockHTTPResponse()

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen", side_effect=capture_urlopen),
                mock.patch.object(refresh_osm, "_fetch_md5", return_value=None),
            ):
                refresh_osm.download_extract(force=True)

        self.assertEqual(len(captured_requests), 1)
        req = captured_requests[0]
        self.assertIsNone(
            req.get_header("If-modified-since"),
            "force=True must not send If-Modified-Since",
        )

    def test_checksum_mismatch_cleans_up_and_raises(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_dir = Path(tmp_name)
            extract_path = tmp_dir / "ireland-and-northern-ireland-latest.osm.pbf"

            wrong_md5 = "a" * 32  # deliberately wrong

            with (
                mock.patch.object(refresh_osm, "OSM_DIR", tmp_dir),
                mock.patch.object(refresh_osm, "OSM_EXTRACT_PATH", extract_path),
                mock.patch("urllib.request.urlopen", return_value=_MockHTTPResponse()),
                mock.patch.object(refresh_osm, "_fetch_md5", return_value=wrong_md5),
                self.assertRaises(RuntimeError) as cm,
            ):
                refresh_osm.download_extract()

        self.assertIn("Checksum mismatch", str(cm.exception))
        tmp_files = list(tmp_dir.glob("*.tmp"))
        self.assertEqual(tmp_files, [], "no .tmp files should remain after checksum failure")
        self.assertFalse(extract_path.exists(), "extract should not be created on checksum failure")
