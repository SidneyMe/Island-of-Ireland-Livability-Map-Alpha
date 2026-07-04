from __future__ import annotations

import io
import json
import os
import urllib.error
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock
from zipfile import ZipFile

import config
from transit import gtfs_download


def _gtfs_zip_bytes(
    *,
    include_agency: bool = True,
    marker: str = "v1",
    calendar_start: str = "20260401",
    calendar_end: str = "20261231",
    include_calendar: bool = True,
    include_calendar_dates: bool = False,
) -> bytes:
    payload = io.BytesIO()
    with ZipFile(payload, "w") as zip_file:
        if include_agency:
            zip_file.writestr(
                "agency.txt",
                "agency_id,agency_name,agency_url,agency_timezone\n"
                "A,Agency Name,https://example.com,Europe/Dublin\n",
            )
        zip_file.writestr(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            f"S1,Stop {marker},53.35,-6.26\n",
        )
        zip_file.writestr(
            "routes.txt",
            "route_id,route_short_name,route_type\n"
            "R1,1,3\n",
        )
        zip_file.writestr(
            "trips.txt",
            "route_id,service_id,trip_id\n"
            "R1,SV1,T1\n",
        )
        zip_file.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,08:00:00,08:00:00,S1,1\n",
        )
        if include_calendar:
            zip_file.writestr(
                "calendar.txt",
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                f"SV1,1,1,1,1,1,0,0,{calendar_start},{calendar_end}\n",
            )
        if include_calendar_dates:
            zip_file.writestr(
                "calendar_dates.txt",
                "service_id,date,exception_type\n"
                f"SV1,{calendar_end},1\n",
            )
        zip_file.writestr(
            "feed_info.txt",
            "feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n"
            "Publisher,https://example.com,en,2026.04\n",
        )
    return payload.getvalue()


class _FakeHttpResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str] | None = None, status: int = 200) -> None:
        self._buffer = io.BytesIO(body)
        self.headers = headers or {}
        self.status = status

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class GtfsDownloadTests(TestCase):
    def _feed(self, tmp_path: Path) -> config.TransitFeedConfig:
        return config.TransitFeedConfig(
            feed_id="nta",
            label="NTA",
            zip_path=tmp_path / "cache" / "gtfs" / "nta_gtfs" / "current.zip",
            url="https://example.test/GTFS_All.zip",
            enabled=True,
            priority=10,
            use_for_transit=True,
        )

    def test_first_download_writes_current_zip_and_manifest(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            body = _gtfs_zip_bytes(marker="first")

            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(
                    body,
                    headers={"ETag": '"etag-1"', "Last-Modified": "Tue, 26 May 2026 10:00:00 GMT"},
                ),
            ):
                result = gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertTrue(feed.zip_path.exists())
            self.assertTrue((feed.zip_path.parent / "current.sha256").exists())
            manifest = json.loads((feed.zip_path.parent / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["feed_id"], "nta")
            self.assertEqual(manifest["http_status"], 200)
            self.assertTrue(manifest["zip_valid"])
            self.assertIn("agency.txt", manifest["gtfs_files"])
            self.assertEqual(result.feed_id, "nta")
            self.assertTrue(result.changed)

    def test_not_modified_uses_conditional_headers_and_keeps_current_zip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            first_body = _gtfs_zip_bytes(marker="first")
            first_response = _FakeHttpResponse(
                first_body,
                headers={"ETag": '"etag-1"', "Last-Modified": "Tue, 26 May 2026 10:00:00 GMT"},
            )
            captured_headers: list[dict[str, str]] = []

            def _urlopen_side_effect(request, timeout=0):
                del timeout
                captured_headers.append(dict(request.header_items()))
                if len(captured_headers) == 1:
                    return first_response
                raise urllib.error.HTTPError(
                    request.full_url,
                    304,
                    "Not Modified",
                    {"ETag": '"etag-1"', "Last-Modified": "Tue, 26 May 2026 10:00:00 GMT"},
                    None,
                )

            with mock.patch.object(gtfs_download.urllib.request, "urlopen", side_effect=_urlopen_side_effect):
                gtfs_download.refresh_gtfs_feed(feed, force=False)
                manifest_path = feed.zip_path.parent / "manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["checked_at_utc"] = "2000-01-01T00:00:00Z"
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                result = gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertFalse(result.changed)
            second_headers = {key.lower(): value for key, value in captured_headers[1].items()}
            self.assertEqual(second_headers.get("if-none-match"), '"etag-1"')
            self.assertEqual(second_headers.get("if-modified-since"), "Tue, 26 May 2026 10:00:00 GMT")

    def test_invalid_zip_is_rejected_and_previous_current_zip_is_preserved(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            first_body = _gtfs_zip_bytes(marker="first")
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(first_body),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)

            original_sha = gtfs_download._sha256_file(feed.zip_path)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(b"not-a-zip"),
            ):
                with self.assertRaises(RuntimeError):
                    gtfs_download.refresh_gtfs_feed(feed, force=True)

            self.assertEqual(gtfs_download._sha256_file(feed.zip_path), original_sha)

    def test_missing_required_gtfs_files_is_rejected(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            first_body = _gtfs_zip_bytes(marker="first")
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(first_body),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)

            missing_agency_body = _gtfs_zip_bytes(include_agency=False, marker="second")
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(missing_agency_body),
            ):
                with self.assertRaises(RuntimeError):
                    gtfs_download.refresh_gtfs_feed(feed, force=True)

    def test_changed_feed_updates_sha_and_current_zip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="first")),
            ):
                first = gtfs_download.refresh_gtfs_feed(feed, force=True)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="second")),
            ):
                second = gtfs_download.refresh_gtfs_feed(feed, force=True)

            self.assertNotEqual(first.sha256, second.sha256)
            self.assertTrue(second.changed)

    def test_network_failure_keeps_previous_current_zip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="first")),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)

            original_sha = gtfs_download._sha256_file(feed.zip_path)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                side_effect=urllib.error.URLError("offline"),
            ):
                with self.assertRaises(RuntimeError):
                    gtfs_download.refresh_gtfs_feed(feed, force=True)

            self.assertEqual(gtfs_download._sha256_file(feed.zip_path), original_sha)

    def test_fresh_cache_skips_network_unless_forced(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="first")),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=False)

            with mock.patch.object(gtfs_download.urllib.request, "urlopen") as urlopen_mock:
                result = gtfs_download.refresh_gtfs_feed(feed, force=False)
            self.assertFalse(result.changed)
            urlopen_mock.assert_not_called()

            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="second")),
            ) as forced_mock:
                forced_result = gtfs_download.refresh_gtfs_feed(feed, force=True)
            self.assertTrue(forced_result.changed)
            forced_mock.assert_called_once()

    def test_recent_cache_but_expired_calendar_triggers_network_check(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            expired_end = (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y%m%d")
            fresh_end = (datetime.now(UTC).date() + timedelta(days=30)).strftime("%Y%m%d")

            responses = [
                _FakeHttpResponse(_gtfs_zip_bytes(marker="expired", calendar_end=expired_end)),
                _FakeHttpResponse(_gtfs_zip_bytes(marker="fresh", calendar_end=fresh_end)),
            ]

            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                side_effect=responses,
            ) as urlopen_mock:
                gtfs_download.refresh_gtfs_feed(feed, force=True)
                gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertEqual(urlopen_mock.call_count, 2)

    def test_recent_cache_but_near_expiry_calendar_triggers_network_check(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            near_end = (
                datetime.now(UTC).date() + timedelta(days=3)
            ).strftime("%Y%m%d")
            far_end = (datetime.now(UTC).date() + timedelta(days=45)).strftime("%Y%m%d")

            responses = [
                _FakeHttpResponse(_gtfs_zip_bytes(marker="near", calendar_end=near_end)),
                _FakeHttpResponse(_gtfs_zip_bytes(marker="far", calendar_end=far_end)),
            ]

            with (
                mock.patch.object(gtfs_download, "GTFS_STALE_IF_ENDS_WITHIN_DAYS", 7),
                mock.patch.object(
                    gtfs_download.urllib.request,
                    "urlopen",
                    side_effect=responses,
                ) as urlopen_mock,
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)
                gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertEqual(urlopen_mock.call_count, 2)

    def test_recent_cache_and_far_future_calendar_skips_network(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            far_end = (datetime.now(UTC).date() + timedelta(days=45)).strftime("%Y%m%d")

            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="far", calendar_end=far_end)),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)

            with mock.patch.object(gtfs_download.urllib.request, "urlopen") as urlopen_mock:
                result = gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertFalse(result.changed)
            urlopen_mock.assert_not_called()

    def test_not_modified_with_expired_cache_sets_warning(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            expired_end = (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y%m%d")

            with mock.patch.object(
                gtfs_download.urllib.request,
                "urlopen",
                return_value=_FakeHttpResponse(_gtfs_zip_bytes(marker="expired", calendar_end=expired_end)),
            ):
                gtfs_download.refresh_gtfs_feed(feed, force=True)

            def _not_modified(request, timeout=0):
                del timeout
                raise urllib.error.HTTPError(
                    request.full_url,
                    304,
                    "Not Modified",
                    {"ETag": '"etag-1"', "Last-Modified": "Tue, 26 May 2026 10:00:00 GMT"},
                    None,
                )

            with mock.patch.object(gtfs_download.urllib.request, "urlopen", side_effect=_not_modified):
                gtfs_download.refresh_gtfs_feed(feed, force=False)

            manifest = json.loads((feed.zip_path.parent / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest.get("http_status"), 304)
            self.assertIn("Server returned 304", str(manifest.get("warning") or ""))

    def test_old_manifest_without_calendar_fields_does_not_crash(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            feed = self._feed(tmp_path)
            feed.zip_path.parent.mkdir(parents=True, exist_ok=True)
            feed.zip_path.write_bytes(_gtfs_zip_bytes(marker="old-manifest"))
            manifest_path = feed.zip_path.parent / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "feed_id": feed.feed_id,
                        "sha256": gtfs_download._sha256_file(feed.zip_path),
                        "checked_at_utc": gtfs_download._utc_now_iso(),
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(gtfs_download.urllib.request, "urlopen") as urlopen_mock:
                result = gtfs_download.refresh_gtfs_feed(feed, force=False)

            self.assertFalse(result.changed)
            urlopen_mock.assert_not_called()
            updated = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(updated.get("calendar_unknown"))

    def test_combined_gtfs_fingerprint_changes_only_when_used_feed_changes(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            all_zip = tmp_path / "all.zip"
            realtime_zip = tmp_path / "realtime.zip"
            all_zip.write_bytes(_gtfs_zip_bytes(marker="all-v1"))
            realtime_zip.write_bytes(_gtfs_zip_bytes(marker="rt-v1"))

            with mock.patch.dict(
                os.environ,
                {
                    "GTFS_NTA_ZIP_PATH": str(all_zip),
                    "GTFS_TRANSLINK_ZIP_PATH": str(realtime_zip),
                },
                clear=False,
            ):
                state_one = config.build_transit_reality_state(analysis_date=date(2026, 5, 26))
                realtime_zip.write_bytes(_gtfs_zip_bytes(marker="rt-v2"))
                state_two = config.build_transit_reality_state(analysis_date=date(2026, 5, 26))
                all_zip.write_bytes(_gtfs_zip_bytes(marker="all-v2"))
                state_three = config.build_transit_reality_state(analysis_date=date(2026, 5, 26))

            self.assertNotEqual(state_one.reality_fingerprint, state_two.reality_fingerprint)
            self.assertNotEqual(state_two.reality_fingerprint, state_three.reality_fingerprint)
