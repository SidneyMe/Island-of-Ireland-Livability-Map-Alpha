from __future__ import annotations

import io
import json
from functools import lru_cache
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock
from zipfile import ZipFile

from config import (
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_LOOKAHEAD_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    TRANSIT_REALITY_ALGO_VERSION,
    TransitFeedState,
)
from transit import workflow as transit_workflow
from transit.classification import classify_services
from transit.export import export_transport_reality_bundle
from transit.gtfs_zip import parse_gtfs_zip
from transit.matching import derive_gtfs_stop_reality
from transit.models import (
    CalendarDateException,
    CalendarService,
    FeedDataset,
    GtfsStopReality,
    StopInfo,
    StopServiceSummary,
)
from transit.rust_gtfs import load_gtfs_stop_reality_models, run_walkgraph_gtfs_refresh
from transit.service import expand_service_windows, summarize_gtfs_stops


NTA_GTFS_SNAPSHOT_PATH = Path("gtfs/nta_gtfs.zip")
TRANSLINK_GTFS_SNAPSHOT_PATH = Path("gtfs/translink_gtfs.zip")
SNAPSHOT_ANALYSIS_DATE = date(2026, 4, 23)


def _write_gtfs_zip(path: Path, files: dict[str, str]) -> None:
    with ZipFile(path, "w") as archive:
        for file_name, content in files.items():
            archive.writestr(file_name, content)


def _minimal_gtfs_files(
    *,
    include_calendar: bool = True,
    include_calendar_dates: bool = True,
) -> dict[str, str]:
    files = {
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main Street,53.35,-6.26\n",
        "routes.txt": "route_id,route_type\nR1,3\n",
        "trips.txt": "route_id,service_id,trip_id\nR1,SVC1,T1\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\n",
    }
    if include_calendar:
        files["calendar.txt"] = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "SVC1,1,1,1,1,1,0,0,20260401,20260430\n"
        )
    if include_calendar_dates:
        files["calendar_dates.txt"] = "service_id,date,exception_type\nSVC1,20260414,1\n"
    return files


def _feed_state(zip_path: Path) -> TransitFeedState:
    return TransitFeedState(
        feed_id="nta",
        label="NTA",
        zip_path=zip_path,
        source_url=None,
        feed_fingerprint="feed-fingerprint-123",
        analysis_date=date(2026, 4, 14),
    )


def _single_stop_dataset(
    *,
    service_id: str = "SVC1",
    service_start: date,
    service_end: date,
    weekday_flags: tuple[int, int, int, int, int, int, int] = (1, 1, 1, 1, 1, 0, 0),
    calendar_dates: list[CalendarDateException] | None = None,
    school_keywords: set[str] | None = None,
    time_bucket_counts: dict[str, int] | None = None,
    location_type: int | None = None,
    event_seconds: int | None = None,
) -> FeedDataset:
    return FeedDataset(
        feed_id="nta",
        feed_fingerprint="feed-fingerprint",
        analysis_date=date(2026, 4, 14),
        created_at=datetime.now(timezone.utc),
        source_path="feed.zip",
        source_url=None,
        stops={
            "S1": StopInfo(
                feed_id="nta",
                stop_id="S1",
                stop_code="1001",
                stop_name="Main Street",
                stop_desc=None,
                stop_lat=53.35,
                stop_lon=-6.26,
                parent_station=None,
                zone_id=None,
                location_type=location_type,
                wheelchair_boarding=None,
                platform_code=None,
            )
        },
        calendar_services={
            service_id: CalendarService(
                feed_id="nta",
                service_id=service_id,
                monday=weekday_flags[0],
                tuesday=weekday_flags[1],
                wednesday=weekday_flags[2],
                thursday=weekday_flags[3],
                friday=weekday_flags[4],
                saturday=weekday_flags[5],
                sunday=weekday_flags[6],
                start_date=service_start,
                end_date=service_end,
            )
        },
        calendar_dates=list(calendar_dates or []),
        stop_service_occurrences={("S1", service_id, "R1", "bus"): 1},
        stop_service_time_occurrences={
            ("S1", service_id, "R1", "bus", event_seconds): 1
        } if event_seconds is not None else {},
        service_time_buckets={
            service_id: dict(time_bucket_counts or {"morning": 0, "afternoon": 0, "offpeak": 1})
        },
        service_route_ids={service_id: {"R1"}},
        service_route_modes={service_id: {"bus"}},
        service_keywords={service_id: set(school_keywords or set())} if school_keywords else {},
    )


def _summaries_for_dataset(
    dataset: FeedDataset,
    *,
    lookahead_days: int = GTFS_LOOKAHEAD_DAYS,
) -> list[StopServiceSummary]:
    service_windows = expand_service_windows(
        dataset,
        analysis_date=dataset.analysis_date,
        window_days=GTFS_ANALYSIS_WINDOW_DAYS,
        desert_window_days=GTFS_SERVICE_DESERT_WINDOW_DAYS,
        lookahead_days=lookahead_days,
    )
    classifications = classify_services(
        dataset,
        reality_fingerprint="reality-123",
        service_windows=service_windows,
    )
    return summarize_gtfs_stops(
        dataset,
        reality_fingerprint="reality-123",
        service_windows=service_windows,
        service_classifications=classifications,
    )


def _gtfs_reality_row(**overrides: object) -> GtfsStopReality:
    payload: dict[str, object] = {
        "reality_fingerprint": "reality-123",
        "import_fingerprint": "import-123",
        "source_ref": "gtfs/nta/S1",
        "stop_name": "Main Street",
        "feed_id": "nta",
        "stop_id": "S1",
        "source_status": "gtfs_direct",
        "reality_status": "active_confirmed",
        "school_only_state": "no",
        "public_departures_7d": 7,
        "public_departures_30d": 21,
        "school_only_departures_30d": 0,
        "last_public_service_date": date(2026, 4, 14),
        "last_any_service_date": date(2026, 4, 14),
        "route_modes": ("bus",),
        "source_reason_codes": ("gtfs_direct_source",),
        "reality_reason_codes": ("public_service_present",),
        "lat": 53.35,
        "lon": -6.26,
    }
    payload.update(overrides)
    return GtfsStopReality(**payload)


@lru_cache(maxsize=None)
def _snapshot_rows_by_stop(
    feed_id: str,
    zip_path_text: str,
    analysis_date_iso: str,
) -> dict[str, GtfsStopReality]:
    zip_path = Path(zip_path_text)
    analysis_date = date.fromisoformat(analysis_date_iso)
    feed_state = TransitFeedState(
        feed_id=feed_id,
        label=feed_id.upper(),
        zip_path=zip_path,
        source_url=None,
        feed_fingerprint=f"snapshot-{feed_id}",
        analysis_date=analysis_date,
    )
    dataset = parse_gtfs_zip(feed_state)
    service_windows = expand_service_windows(
        dataset,
        analysis_date=analysis_date,
        window_days=GTFS_ANALYSIS_WINDOW_DAYS,
        desert_window_days=GTFS_SERVICE_DESERT_WINDOW_DAYS,
        lookahead_days=GTFS_LOOKAHEAD_DAYS,
    )
    classifications = classify_services(
        dataset,
        reality_fingerprint=f"snapshot-{feed_id}",
        service_windows=service_windows,
    )
    summaries = summarize_gtfs_stops(
        dataset,
        reality_fingerprint=f"snapshot-{feed_id}",
        service_windows=service_windows,
        service_classifications=classifications,
    )
    rows = derive_gtfs_stop_reality(
        [dataset],
        summaries,
        reality_fingerprint=f"snapshot-{feed_id}",
        import_fingerprint="snapshot-import",
    )
    return {row.stop_id: row for row in rows}


class _FakeProcess:
    def __init__(self, *, stderr_text: str = "", returncode: int = 0) -> None:
        self.stderr = io.StringIO(stderr_text)
        self.returncode = returncode

    def wait(self) -> int:
        return self.returncode


class TransitGtfsParsingTests(TestCase):
    def test_parse_gtfs_zip_accepts_calendar_dates_only_feed(self) -> None:
        with TemporaryDirectory() as tmp_name:
            zip_path = Path(tmp_name) / "calendar-dates-only.zip"
            _write_gtfs_zip(
                zip_path,
                _minimal_gtfs_files(include_calendar=False, include_calendar_dates=True),
            )

            dataset = parse_gtfs_zip(_feed_state(zip_path))

        self.assertEqual(dataset.calendar_services, {})
        self.assertEqual(len(dataset.calendar_dates), 1)
        self.assertEqual(dataset.calendar_dates[0].service_id, "SVC1")

    def test_parse_gtfs_zip_accepts_calendar_only_feed(self) -> None:
        with TemporaryDirectory() as tmp_name:
            zip_path = Path(tmp_name) / "calendar-only.zip"
            _write_gtfs_zip(
                zip_path,
                _minimal_gtfs_files(include_calendar=True, include_calendar_dates=False),
            )

            dataset = parse_gtfs_zip(_feed_state(zip_path))

        self.assertIn("SVC1", dataset.calendar_services)
        self.assertEqual(dataset.calendar_dates, [])

    def test_parse_gtfs_zip_requires_at_least_one_calendar_file(self) -> None:
        with TemporaryDirectory() as tmp_name:
            zip_path = Path(tmp_name) / "missing-calendar.zip"
            _write_gtfs_zip(
                zip_path,
                _minimal_gtfs_files(include_calendar=False, include_calendar_dates=False),
            )

            with self.assertRaisesRegex(RuntimeError, "calendar.txt or calendar_dates.txt"):
                parse_gtfs_zip(_feed_state(zip_path))


class TransitRealityRewriteTests(TestCase):
    def test_derive_gtfs_stop_reality_marks_upcoming_service_active(self) -> None:
        dataset = _single_stop_dataset(
            service_id="NEW",
            service_start=date(2026, 4, 15),
            service_end=date(2026, 5, 31),
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.source_ref, "gtfs/nta/S1")
        self.assertEqual(row.stop_name, "Main Street")
        self.assertEqual(row.source_status, "gtfs_direct")
        self.assertEqual(row.reality_status, "active_confirmed")
        self.assertEqual(row.public_departures_7d, 10)
        self.assertEqual(row.public_departures_30d, 10)
        self.assertEqual(row.bus_active_days_mask_7d, "1111100")
        self.assertEqual(row.bus_service_subtier, "weekdays_only")
        self.assertTrue(row.has_any_bus_service)
        self.assertFalse(row.has_daily_bus_service)
        self.assertEqual(row.source_reason_codes, ("gtfs_direct_source",))

    def test_frequency_windows_drive_transport_score_units(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 5 * 3600): 16,
            ("S1", "SVC1", "R1", "bus", 17 * 3600): 16,
            ("S1", "SVC1", "R1", "bus", 12 * 3600): 80,
        }

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        row = rows[0]
        self.assertEqual(row.weekday_morning_peak_deps, 16)
        self.assertEqual(row.weekday_evening_peak_deps, 16)
        self.assertEqual(row.weekday_offpeak_deps, 80)
        self.assertEqual(row.transport_score_units, 5)

    def test_offpeak_volume_cannot_outscore_commute_service(self) -> None:
        commute_dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        commute_dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 5 * 3600): 16,
            ("S1", "SVC1", "R1", "bus", 17 * 3600): 16,
        }
        offpeak_dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        offpeak_dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 12 * 3600): 200,
        }

        commute_row = derive_gtfs_stop_reality(
            [commute_dataset],
            _summaries_for_dataset(commute_dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]
        offpeak_row = derive_gtfs_stop_reality(
            [offpeak_dataset],
            _summaries_for_dataset(offpeak_dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]

        self.assertGreater(commute_row.transport_score_units, offpeak_row.transport_score_units)
        self.assertEqual(offpeak_row.transport_score_units, 1)

    def test_friday_evening_counts_through_saturday_2am_without_dominating(self) -> None:
        friday_dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            weekday_flags=(0, 0, 0, 0, 1, 0, 0),
        )
        friday_dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 16 * 3600): 12,
            ("S1", "SVC1", "R1", "bus", 25 * 3600 + 30 * 60): 12,
        }
        commute_dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        commute_dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 5 * 3600): 16,
            ("S1", "SVC1", "R1", "bus", 17 * 3600): 16,
        }

        friday_row = derive_gtfs_stop_reality(
            [friday_dataset],
            _summaries_for_dataset(friday_dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]
        commute_row = derive_gtfs_stop_reality(
            [commute_dataset],
            _summaries_for_dataset(commute_dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]

        self.assertEqual(friday_row.friday_evening_deps, 24)
        self.assertLess(friday_row.transport_score_units, commute_row.transport_score_units)

    def test_weekend_day_type_departure_averages_are_exposed(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            weekday_flags=(0, 0, 0, 0, 0, 1, 1),
        )
        dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 11 * 3600): 3,
        }

        row = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]

        self.assertEqual(row.saturday_deps, 3)
        self.assertEqual(row.sunday_deps, 3)

    def test_school_only_departures_do_not_contribute_frequency_score(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            school_keywords={"school"},
            time_bucket_counts={"morning": 10, "afternoon": 10, "offpeak": 0},
        )
        dataset.stop_service_time_occurrences = {
            ("S1", "SVC1", "R1", "bus", 7 * 3600): 10,
            ("S1", "SVC1", "R1", "bus", 15 * 3600): 10,
        }

        row = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )[0]

        self.assertEqual(row.reality_status, "school_only_confirmed")
        self.assertEqual(row.weekday_morning_peak_deps, 0)
        self.assertEqual(row.weekday_evening_peak_deps, 0)
        self.assertEqual(row.transport_score_units, 0)

    def test_derive_gtfs_stop_reality_marks_beyond_lookahead_service_inactive(self) -> None:
        dataset = _single_stop_dataset(
            service_id="LATER",
            service_start=date(2026, 4, 29),
            service_end=date(2026, 5, 31),
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.reality_status, "inactive_confirmed")
        self.assertEqual(row.school_only_state, "no")
        self.assertEqual(row.public_departures_30d, 0)
        self.assertEqual(row.school_only_departures_30d, 0)
        self.assertEqual(row.bus_active_days_mask_7d, "1111100")
        self.assertEqual(row.bus_service_subtier, "weekdays_only")
        self.assertFalse(row.is_unscheduled_stop)
        self.assertTrue(row.has_any_bus_service)

    def test_derive_gtfs_stop_reality_emits_unscheduled_stop(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        dataset = FeedDataset(
            feed_id=dataset.feed_id,
            feed_fingerprint=dataset.feed_fingerprint,
            analysis_date=dataset.analysis_date,
            created_at=dataset.created_at,
            source_path=dataset.source_path,
            source_url=dataset.source_url,
            stops=dataset.stops,
            calendar_services=dataset.calendar_services,
            stop_service_occurrences={},
            service_time_buckets={},
            service_route_ids={},
            service_route_modes={},
            service_keywords={},
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.reality_status, "inactive_confirmed")
        self.assertTrue(row.is_unscheduled_stop)
        self.assertIsNone(row.bus_active_days_mask_7d)
        self.assertIsNone(row.bus_service_subtier)
        self.assertFalse(row.has_any_bus_service)
        self.assertIn("unscheduled_stop", row.reality_reason_codes)

    def test_derive_gtfs_stop_reality_does_not_emit_unscheduled_parent_stop(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            location_type=1,
        )
        dataset = FeedDataset(
            feed_id=dataset.feed_id,
            feed_fingerprint=dataset.feed_fingerprint,
            analysis_date=dataset.analysis_date,
            created_at=dataset.created_at,
            source_path=dataset.source_path,
            source_url=dataset.source_url,
            stops=dataset.stops,
            calendar_services=dataset.calendar_services,
            stop_service_occurrences={},
            service_time_buckets={},
            service_route_ids={},
            service_route_modes={},
            service_keywords={},
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(rows, [])

    def test_derive_gtfs_stop_reality_marks_school_only_service(self) -> None:
        dataset = _single_stop_dataset(
            service_id="SCHOOL",
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            school_keywords={"school"},
            time_bucket_counts={"morning": 9, "afternoon": 1, "offpeak": 0},
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.reality_status, "school_only_confirmed")
        self.assertEqual(row.school_only_state, "yes")
        self.assertEqual(row.public_departures_30d, 0)
        self.assertGreater(row.school_only_departures_30d, 0)
        self.assertEqual(row.bus_active_days_mask_7d, "1111100")
        self.assertEqual(row.bus_service_subtier, "weekdays_only")

    def test_derive_gtfs_stop_reality_keeps_tue_sun_tier_when_exception_adds_monday(self) -> None:
        dataset = _single_stop_dataset(
            service_id="TUESUN",
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
            weekday_flags=(0, 1, 1, 1, 1, 1, 1),
            calendar_dates=[
                CalendarDateException(
                    feed_id="nta",
                    service_id="TUESUN",
                    service_date=date(2026, 4, 13),
                    exception_type=1,
                )
            ],
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.bus_active_days_mask_7d, "0111111")
        self.assertEqual(row.bus_service_subtier, "tue_sun")
        self.assertNotEqual(row.bus_service_subtier, "mon_sun")
        self.assertFalse(row.has_exception_only_service)

    def test_derive_gtfs_stop_reality_marks_calendar_dates_only_bus_service(self) -> None:
        dataset = _single_stop_dataset(
            service_start=date(2026, 4, 1),
            service_end=date(2026, 4, 30),
        )
        dataset = FeedDataset(
            feed_id=dataset.feed_id,
            feed_fingerprint=dataset.feed_fingerprint,
            analysis_date=dataset.analysis_date,
            created_at=dataset.created_at,
            source_path=dataset.source_path,
            source_url=dataset.source_url,
            stops=dataset.stops,
            calendar_services={},
            calendar_dates=[
                CalendarDateException(
                    feed_id="nta",
                    service_id="EXC_ONLY",
                    service_date=date(2026, 4, 14),
                    exception_type=1,
                )
            ],
            stop_service_occurrences={("S1", "EXC_ONLY", "R1", "bus"): 1},
            service_time_buckets={"EXC_ONLY": {"morning": 0, "afternoon": 0, "offpeak": 1}},
            service_route_ids={"EXC_ONLY": {"R1"}},
            service_route_modes={"EXC_ONLY": {"bus"}},
            service_keywords={},
        )

        rows = derive_gtfs_stop_reality(
            [dataset],
            _summaries_for_dataset(dataset),
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.bus_active_days_mask_7d, "0000000")
        self.assertIsNone(row.bus_service_subtier)
        self.assertTrue(row.has_exception_only_service)
        self.assertTrue(row.has_any_bus_service)

    def test_derive_gtfs_stop_reality_prefers_child_platforms_over_parent_station(self) -> None:
        dataset = FeedDataset(
            feed_id="nta",
            feed_fingerprint="feed-fingerprint",
            analysis_date=date(2026, 4, 14),
            created_at=datetime.now(timezone.utc),
            source_path="feed.zip",
            source_url=None,
            stops={
                "PARENT": StopInfo(
                    feed_id="nta",
                    stop_id="PARENT",
                    stop_code=None,
                    stop_name="Central Station",
                    stop_desc=None,
                    stop_lat=53.35,
                    stop_lon=-6.26,
                    parent_station=None,
                    zone_id=None,
                    location_type=1,
                    wheelchair_boarding=None,
                    platform_code=None,
                ),
                "PLATFORM-1": StopInfo(
                    feed_id="nta",
                    stop_id="PLATFORM-1",
                    stop_code=None,
                    stop_name="Central Station Platform 1",
                    stop_desc=None,
                    stop_lat=53.3501,
                    stop_lon=-6.2601,
                    parent_station="PARENT",
                    zone_id=None,
                    location_type=0,
                    wheelchair_boarding=None,
                    platform_code="1",
                ),
            },
        )
        summaries = [
            StopServiceSummary(
                reality_fingerprint="reality-123",
                feed_id="nta",
                stop_id="PARENT",
                public_departures_7d=12,
                public_departures_30d=48,
                school_only_departures_30d=0,
                last_public_service_date=date(2026, 4, 14),
                last_any_service_date=date(2026, 4, 14),
                route_modes=("rail",),
                route_ids=("R1",),
                reason_codes=("public_service_present",),
            ),
            StopServiceSummary(
                reality_fingerprint="reality-123",
                feed_id="nta",
                stop_id="PLATFORM-1",
                public_departures_7d=12,
                public_departures_30d=48,
                school_only_departures_30d=0,
                last_public_service_date=date(2026, 4, 14),
                last_any_service_date=date(2026, 4, 14),
                route_modes=("rail",),
                route_ids=("R1",),
                reason_codes=("public_service_present",),
            ),
        ]

        rows = derive_gtfs_stop_reality(
            [dataset],
            summaries,
            reality_fingerprint="reality-123",
            import_fingerprint="import-123",
        )

        self.assertEqual([row.stop_id for row in rows], ["PLATFORM-1"])
        self.assertEqual(rows[0].stop_name, "Central Station Platform 1")


class TransitExportAndLoaderTests(TestCase):
    def test_export_transport_reality_bundle_uses_gtfs_direct_fields(self) -> None:
        with TemporaryDirectory() as tmp_name:
            export_paths = export_transport_reality_bundle(
                [_gtfs_reality_row()],
                analysis_date=date(2026, 4, 14),
                export_dir=Path(tmp_name),
            )

            geojson_payload = json.loads(export_paths["geojson"].read_text(encoding="utf-8"))
            readme_text = export_paths["readme"].read_text(encoding="utf-8")

        properties = geojson_payload["features"][0]["properties"]
        self.assertEqual(properties["source_ref"], "gtfs/nta/S1")
        self.assertEqual(properties["stop_name"], "Main Street")
        self.assertEqual(properties["source_status"], "gtfs_direct")
        self.assertIn("weekday_morning_peak_deps", properties)
        self.assertIn("friday_evening_deps", properties)
        self.assertIn("transport_score_units", properties)
        self.assertNotIn("osm_source_ref", properties)
        self.assertNotIn("match_status", properties)
        self.assertIn("bus_service_subtier", properties)
        self.assertIn("has_any_bus_service", properties)
        self.assertIn("GTFS", readme_text)
        self.assertIn("directly from configured GTFS feeds", readme_text)

    def test_load_gtfs_stop_reality_models_parses_csv_payload(self) -> None:
        with TemporaryDirectory() as tmp_name:
            csv_path = Path(tmp_name) / "gtfs_stop_reality.csv"
            csv_path.write_text(
                "\n".join(
                    (
                        "reality_fingerprint,import_fingerprint,source_ref,stop_name,feed_id,stop_id,source_status,reality_status,school_only_state,public_departures_7d,public_departures_30d,school_only_departures_30d,weekday_morning_peak_deps,weekday_evening_peak_deps,weekday_offpeak_deps,saturday_deps,sunday_deps,friday_evening_deps,transport_score_units,last_public_service_date,last_any_service_date,bus_active_days_mask_7d,bus_service_subtier,is_unscheduled_stop,has_exception_only_service,has_any_bus_service,has_daily_bus_service,route_modes_json,source_reason_codes_json,reality_reason_codes_json,lat,lon,created_at",
                        'reality-123,import-123,gtfs/nta/S1,Main Street,nta,S1,gtfs_direct,active_confirmed,no,7,21,0,4,5,6,7,8,9,3,2026-04-14,2026-04-14,1111111,mon_sun,false,false,true,true,"[""bus""]","[""gtfs_direct_source""]","[""public_service_present""]",53.35,-6.26,2026-04-14T00:00:00+00:00',
                    )
                ),
                encoding="utf-8",
            )

            rows = load_gtfs_stop_reality_models(csv_path)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source_ref, "gtfs/nta/S1")
        self.assertEqual(rows[0].stop_id, "S1")
        self.assertEqual(rows[0].source_status, "gtfs_direct")
        self.assertEqual(rows[0].route_modes, ("bus",))
        self.assertEqual(rows[0].bus_active_days_mask_7d, "1111111")
        self.assertEqual(rows[0].bus_service_subtier, "mon_sun")
        self.assertTrue(rows[0].has_any_bus_service)
        self.assertEqual(rows[0].weekday_morning_peak_deps, 4)
        self.assertEqual(rows[0].friday_evening_deps, 9)
        self.assertEqual(rows[0].transport_score_units, 3)


class TransitRustRefreshTests(TestCase):
    def test_run_walkgraph_gtfs_refresh_writes_config_without_osm_inputs(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            zip_path = tmp_path / "feed.zip"
            zip_path.write_text("placeholder", encoding="utf-8")

            with (
                mock.patch("transit.rust_gtfs.ensure_walkgraph_subcommand_available"),
                mock.patch("transit.rust_gtfs.subprocess.Popen", return_value=_FakeProcess()),
            ):
                output_dir = run_walkgraph_gtfs_refresh(
                    feed_states=(_feed_state(zip_path),),
                    import_fingerprint="import-123",
                    reality_fingerprint="reality-123",
                    output_dir=tmp_path / "gtfs_refresh",
                )

            config_payload = json.loads((output_dir.parent / "gtfs_refresh_config.json").read_text(encoding="utf-8"))

        self.assertEqual(config_payload["matcher_version"], TRANSIT_REALITY_ALGO_VERSION)
        self.assertEqual(config_payload["commute_am_start_hour"], 4)
        self.assertEqual(config_payload["friday_evening_end_hour"], 2)
        self.assertNotIn("osm_features_path", config_payload)
        self.assertNotIn("match_radius_m", config_payload)


class TransitWorkflowTests(TestCase):
    def test_transit_reality_refresh_required_ignores_import_fingerprint(self) -> None:
        prepared_state = SimpleNamespace(reality_fingerprint="reality-123")
        progress_cb = mock.Mock()

        with (
            mock.patch.object(
                transit_workflow,
                "prepare_transit_reality_state",
                return_value=prepared_state,
            ) as prepare_state_mock,
            mock.patch.object(transit_workflow, "has_complete_transit_reality_manifest", return_value=True) as manifest_mock,
        ):
            state, refresh_required = transit_workflow.transit_reality_refresh_required(
                mock.sentinel.engine,
                import_fingerprint="import-123",
                progress_cb=progress_cb,
            )

        self.assertIs(state, prepared_state)
        self.assertFalse(refresh_required)
        prepare_state_mock.assert_called_once_with(
            refresh_download=False,
            progress_cb=progress_cb,
        )
        manifest_mock.assert_called_once_with(mock.sentinel.engine, "reality-123")

    def test_ensure_transit_reality_uses_gtfs_stop_reality_artifact(self) -> None:
        feed_state = TransitFeedState(
            feed_id="nta",
            label="NTA",
            zip_path=Path("feed.zip"),
            source_url=None,
            feed_fingerprint="feed-fingerprint-123",
            analysis_date=date(2026, 4, 14),
        )
        reality_state = SimpleNamespace(
            analysis_date=date(2026, 4, 14),
            transit_config_hash="config-hash",
            feed_states=(feed_state,),
            feed_fingerprints={"nta": "feed-fingerprint-123"},
            reality_fingerprint="reality-123",
        )

        with TemporaryDirectory() as tmp_name:
            tmp_path = Path(tmp_name)
            artifacts_dir = tmp_path / "artifacts"
            (artifacts_dir / "derived").mkdir(parents=True)
            (artifacts_dir / "raw" / "nta").mkdir(parents=True)

            with (
                mock.patch.object(transit_workflow, "PROJECT_TEMP_DIR", tmp_path / "project-temp"),
                mock.patch.object(transit_workflow, "has_complete_transit_reality_manifest", return_value=False),
                mock.patch.object(transit_workflow, "has_complete_transit_feed_manifest", return_value=False),
                mock.patch.object(transit_workflow, "run_walkgraph_gtfs_refresh", return_value=artifacts_dir) as run_refresh_mock,
                mock.patch.object(transit_workflow, "replace_gtfs_feed_rows_from_artifacts"),
                mock.patch.object(transit_workflow, "replace_transit_reality_rows_from_artifacts"),
                mock.patch.object(transit_workflow, "load_gtfs_stop_reality_models", return_value=[_gtfs_reality_row()]) as load_models_mock,
                mock.patch.object(transit_workflow, "export_transport_reality_bundle"),
            ):
                transit_workflow.ensure_transit_reality(
                    mock.sentinel.engine,
                    import_fingerprint="import-123",
                    reality_state=reality_state,
                )

        self.assertNotIn("osm_features", run_refresh_mock.call_args.kwargs)
        load_models_mock.assert_called_once_with(artifacts_dir / "derived" / "gtfs_stop_reality.csv")


@unittest.skipUnless(
    NTA_GTFS_SNAPSHOT_PATH.is_file(),
    "Local NTA GTFS snapshot is required for exact stop regression checks.",
)
class TransitSnapshotNtaRegressionTests(TestCase):
    def _row(self, stop_id: str) -> GtfsStopReality:
        rows = _snapshot_rows_by_stop("nta", str(NTA_GTFS_SNAPSHOT_PATH), SNAPSHOT_ANALYSIS_DATE.isoformat())
        return rows[stop_id]

    def test_snapshot_examples_cover_weekly_bus_tiers(self) -> None:
        expected_rows = {
            "8530LLL10789": ("ATU Ballyraine", "1111110", "mon_sat"),
            "8220B106421": ("3Arena", "1111111", "mon_sun"),
            "8460PB003677": ("ATU Galway", "0000101", "partial_week"),
            "847000043": ("ATU Galway", "0000001", "single_day_only"),
            "8250DB007651": ("Amgen", "0111111", "tue_sun"),
            "847000037": ("ATU Galway", "1111100", "weekdays_only"),
            "8220DB000521": ("Annesley Bridge Road", "0000011", "weekends_only"),
        }

        for stop_id, (stop_name, expected_mask, expected_subtier) in expected_rows.items():
            with self.subTest(stop_id=stop_id, expected_subtier=expected_subtier):
                row = self._row(stop_id)
                self.assertEqual(row.stop_name, stop_name)
                self.assertEqual(row.bus_active_days_mask_7d, expected_mask)
                self.assertEqual(row.bus_service_subtier, expected_subtier)

    def test_snapshot_examples_keep_tue_sun_stops_as_tue_sun(self) -> None:
        expected_tue_sun_ids = (
            "8250DB007651",
            "8230DB001132",
            "8220DB000968",
            "8220DB000998",
            "8250DB003106",
        )

        for stop_id in expected_tue_sun_ids:
            with self.subTest(stop_id=stop_id):
                row = self._row(stop_id)
                self.assertEqual(row.bus_active_days_mask_7d, "0111111")
                self.assertEqual(row.bus_service_subtier, "tue_sun")
                self.assertNotEqual(row.bus_service_subtier, "mon_sun")


@unittest.skipUnless(
    TRANSLINK_GTFS_SNAPSHOT_PATH.is_file(),
    "Local Translink GTFS snapshot is required for exact stop regression checks.",
)
class TransitSnapshotTranslinkRegressionTests(TestCase):
    def _row(self, stop_id: str) -> GtfsStopReality:
        rows = _snapshot_rows_by_stop(
            "translink",
            str(TRANSLINK_GTFS_SNAPSHOT_PATH),
            SNAPSHOT_ANALYSIS_DATE.isoformat(),
        )
        return rows[stop_id]

    def test_snapshot_examples_mark_unscheduled_boarding_stops(self) -> None:
        expected_unscheduled_rows = {
            "51-49-700000010714": "Black Stone Pillars",
            "51-49-700000010717": "Bridge Road",
            "51-49-700000010719": "Bridge Road",
            "51-49-700000008701": "Dunloy Quarry",
            "51-49-700000015636": "Galdanagh Crossroads",
        }

        for stop_id, stop_name in expected_unscheduled_rows.items():
            with self.subTest(stop_id=stop_id):
                row = self._row(stop_id)
                self.assertEqual(row.stop_name, stop_name)
                self.assertTrue(row.is_unscheduled_stop)
                self.assertIsNone(row.bus_active_days_mask_7d)
                self.assertIsNone(row.bus_service_subtier)

    def test_snapshot_examples_mark_calendar_dates_only_service(self) -> None:
        expected_exception_only_rows = {
            "51-49-700000003613": "Abbey Green",
            "51-49-700000001455": "Abbey Park",
            "51-49-700000001340": "Abbey Retail Park",
            "51-49-700000001354": "Abbey Retail Park",
            "51-49-700000001717": "Abbey Road",
            "51-49-700000001738": "Abbey Road",
            "51-49-700000000780": "Abbeycentre",
            "51-49-700000000782": "Abbeycentre",
            "51-49-700000000784": "Abbeycentre",
            "51-49-700000000888": "Abbeydale Park",
        }

        for stop_id, stop_name in expected_exception_only_rows.items():
            with self.subTest(stop_id=stop_id):
                row = self._row(stop_id)
                self.assertEqual(row.stop_name, stop_name)
                self.assertTrue(row.has_exception_only_service)
                self.assertFalse(row.is_unscheduled_stop)
