"""Python reference vs walkgraph gtfs-refresh (the production path) on one synthetic feed.

transit/service.py and walkgraph/src/gtfs/mod.rs duplicate the tier tables, headway
thresholds and frequency weights; every published field must agree. The two known
gaps have their own expectedFailure tests: fixing one turns its test into an
"unexpected success", and its workaround in this file should then be removed.
"""

from __future__ import annotations

import dataclasses
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock
from zipfile import ZipFile

from config import (
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_LOOKAHEAD_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    WALKGRAPH_BIN,
    TransitFeedState,
)
from transit.classification import classify_services
from transit.gtfs_zip import parse_gtfs_zip, route_mode
from transit.matching import derive_gtfs_stop_reality
from transit.models import GtfsStopReality
from transit.rust_gtfs import load_gtfs_stop_reality_models, run_walkgraph_gtfs_refresh
from transit.service import expand_service_windows, summarize_gtfs_stops
from walkgraph_support import ensure_walkgraph_subcommand_available


ANALYSIS_DATE = date(2026, 4, 14)  # a Tuesday
REALITY_FINGERPRINT = "parity-reality"
IMPORT_FINGERPRINT = "parity-import"
ROUTE_TYPES = {"tram": 0, "rail": 2, "bus": 3, "ferry": 4}
MODE_BY_ROUTE_TYPE = {route_type: mode for mode, route_type in ROUTE_TYPES.items()}


def _native_available() -> bool:
    try:
        ensure_walkgraph_subcommand_available(WALKGRAPH_BIN, "gtfs-refresh")
    except Exception:
        return False
    return True


def _daytime(count: int) -> list[str]:
    """``count`` departures spread evenly over the 06:00-20:00 bus daytime window."""
    seconds = [6 * 3600 + (14 * 3600 * index) // count for index in range(count)]
    return [f"{value // 3600:02d}:{value % 3600 // 60:02d}:{value % 60:02d}" for value in seconds]


class _Feed:
    def __init__(self) -> None:
        self.stops: list[str] = []
        self.routes: list[str] = []
        self.trips: list[str] = []
        self.stop_times: list[str] = []
        self.calendar: list[str] = []
        self.calendar_dates: list[str] = []

    def service(self, service_id: str, days: str, *, start: str = "20260401", end: str = "20260530") -> str:
        self.calendar.append(",".join([service_id, *days, start, end]))
        return service_id

    def stop(
        self, stop_id: str, *, name: str | None = None, location_type: str = "0", parent: str = ""
    ) -> str:
        index = len(self.stops)
        lat, lon = 53.0 + index * 0.01, -6.0 - index * 0.01
        self.stops.append(f"{stop_id},{name or 'Stop ' + stop_id},{lat:.4f},{lon:.4f},{location_type},{parent}")
        return stop_id

    def calls(self, stop_id: str, mode: str, service_id: str, times: list[str], *, name: str = "Route") -> None:
        route_id = f"R{len(self.routes)}"
        self.routes.append(f"{route_id},{name},{ROUTE_TYPES[mode]}")
        for index, time in enumerate(times):
            trip_id = f"{route_id}-T{index}"
            self.trips.append(f"{route_id},{service_id},{trip_id}")
            self.stop_times.append(f"{trip_id},{time},{time},{stop_id},1")

    def write(self, path: Path) -> None:
        tables = {
            "stops.txt": ("stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station", self.stops),
            "routes.txt": ("route_id,route_long_name,route_type", self.routes),
            "trips.txt": ("route_id,service_id,trip_id", self.trips),
            "stop_times.txt": ("trip_id,arrival_time,departure_time,stop_id,stop_sequence", self.stop_times),
            "calendar.txt": (
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date",
                self.calendar,
            ),
            "calendar_dates.txt": ("service_id,date,exception_type", self.calendar_dates),
        }
        with ZipFile(path, "w") as archive:
            for file_name, (header, rows) in tables.items():
                archive.writestr(file_name, "\n".join([header, *rows]) + "\n")


def _parity_feed() -> _Feed:
    feed = _Feed()
    weekdays = feed.service("WEEKDAYS", "1111100")
    daily = feed.service("DAILY", "1111111")
    # Non-bus stops skip Fridays: see test_friday_evening_counts_every_mode.
    no_friday = feed.service("NO_FRIDAY", "1111011")
    feed.service("MON_SAT", "1111110")
    feed.service("TUE_SUN", "0111111")
    feed.service("WEEKENDS", "0000011")
    feed.service("SINGLE", "0010000")
    feed.service("PARTIAL", "1010100")
    ended = feed.service("ENDED", "1111100", start="20260101", end="20260201")

    # Both sides of every headway threshold (14 h window: 15/30/60/120 min).
    for count in (57, 56, 55, 29, 28, 27, 15, 14, 13, 8, 7, 6, 1):
        feed.calls(feed.stop(f"BUS-{count}"), "bus", weekdays, _daytime(count))
    # Weekly bus masks.
    for service_id in ("DAILY", "MON_SAT", "TUE_SUN", "WEEKENDS", "SINGLE", "PARTIAL"):
        feed.calls(feed.stop(f"MASK-{service_id}"), "bus", service_id, ["09:00:00", "12:00:00"])
    # Single modes; "unlisted" modes keep the frequency formula.
    for mode in ("rail", "tram", "ferry"):
        for count in (1, 12, 60):
            feed.calls(feed.stop(f"{mode.upper()}-{count}"), mode, no_friday, _daytime(count))
    # Mixed modes.
    for modes in (
        ("bus", "rail"), ("bus", "tram"), ("rail", "tram"), ("bus", "rail", "tram"),
        ("bus", "ferry"), ("rail", "ferry"), ("tram", "ferry"),
    ):
        for bus_count in (6, 56):
            stop_id = feed.stop("+".join(modes).upper() + f"-{bus_count}")
            for mode in modes:
                if mode == "bus":
                    feed.calls(stop_id, mode, weekdays, _daytime(bus_count))
                else:
                    feed.calls(stop_id, mode, no_friday, _daytime(2))
    # Peaks, Friday night past midnight, weekends (frequency formula weights).
    late = ["05:30:00", "07:59:59", "08:00:00", "16:00:00", "19:59:59", "20:00:00", "23:30:00", "25:30:00", "26:00:00"]
    feed.calls(feed.stop("PEAKS-FERRY"), "ferry", no_friday, late)
    feed.calls(feed.stop("PEAKS-BUS"), "bus", daily, late)
    for count in (3, 20, 40):
        feed.calls(feed.stop(f"WEEKEND-FERRY-{count}"), "ferry", "WEEKENDS", _daytime(count))
    # No departures in the analysis window.
    for mode in ("rail", "tram", "bus"):
        feed.calls(feed.stop(f"ENDED-{mode.upper()}"), mode, ended, _daytime(4))
    # School-only service, alone and beside public rail.
    school = ["08:00:00", "08:10:00", "08:20:00", "15:00:00"]
    feed.service("SCHOOL", "1111100")
    feed.calls(feed.stop("SCHOOL-ONLY"), "bus", "SCHOOL", school, name="School Service")
    feed.stop("SCHOOL-RAIL")
    feed.calls("SCHOOL-RAIL", "bus", "SCHOOL", school, name="School Service")
    feed.calls("SCHOOL-RAIL", "rail", no_friday, _daytime(3))
    feed.calls(feed.stop("SCHOOL-RAIL-ONLY"), "rail", "SCHOOL", school, name="School Service")
    # Service defined by calendar_dates alone, and an exception on a regular one.
    for day in ("20260415", "20260416", "20260418"):
        feed.calendar_dates.append(f"DATES_ONLY,{day},1")
    feed.calendar_dates.append("TUE_SUN,20260420,1")
    feed.calendar_dates.append("DAILY,20260417,2")
    feed.calls(feed.stop("FRIDAY-RAIL"), "rail", daily, late)
    feed.calls(feed.stop("DATES-BUS"), "bus", "DATES_ONLY", _daytime(8))
    feed.calls(feed.stop("DATES-RAIL"), "rail", "DATES_ONLY", _daytime(8))
    # Stations: a parent with a served platform, unscheduled stops.
    feed.stop("PARENT", location_type="1")
    feed.calls(feed.stop("PLATFORM", location_type="0", parent="PARENT"), "rail", no_friday, _daytime(5))
    feed.stop("UNSCHEDULED")
    feed.stop("UNSCHEDULED-PARENT", location_type="1")
    return feed


def _python_rows(feed_state: TransitFeedState) -> list[GtfsStopReality]:
    # See test_route_mode_reads_gtfs_rail_as_rail.
    with mock.patch("transit.gtfs_zip.route_mode", side_effect=MODE_BY_ROUTE_TYPE.__getitem__):
        dataset = parse_gtfs_zip(feed_state)
    windows = expand_service_windows(
        dataset,
        analysis_date=feed_state.analysis_date,
        window_days=GTFS_ANALYSIS_WINDOW_DAYS,
        desert_window_days=GTFS_SERVICE_DESERT_WINDOW_DAYS,
        lookahead_days=GTFS_LOOKAHEAD_DAYS,
    )
    summaries = summarize_gtfs_stops(
        dataset,
        reality_fingerprint=REALITY_FINGERPRINT,
        service_windows=windows,
        service_classifications=classify_services(
            dataset, reality_fingerprint=REALITY_FINGERPRINT, service_windows=windows
        ),
    )
    return derive_gtfs_stop_reality(
        [dataset],
        summaries,
        reality_fingerprint=REALITY_FINGERPRINT,
        import_fingerprint=IMPORT_FINGERPRINT,
    )


class RouteModeTests(TestCase):
    @unittest.expectedFailure
    def test_route_mode_reads_gtfs_rail_as_rail(self) -> None:
        # GTFS route_type 2 is rail. transit/railway_corridors.py uses this mapping in
        # production, so heavy rail is left out of the railway proximity corridors.
        self.assertEqual(route_mode(2), "rail")


@unittest.skipUnless(_native_available(), "walkgraph binary without gtfs-refresh")
class TransitNativeParityTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        temp = TemporaryDirectory()
        cls.addClassCleanup(temp.cleanup)
        root = Path(temp.name)
        zip_path = root / "parity_gtfs.zip"
        _parity_feed().write(zip_path)
        state = TransitFeedState(
            feed_id="nta",
            label="NTA",
            zip_path=zip_path,
            source_url=None,
            feed_fingerprint="parity-feed",
            analysis_date=ANALYSIS_DATE,
        )
        cls.python = {row.stop_id: row for row in _python_rows(state)}
        output = run_walkgraph_gtfs_refresh(
            feed_states=(state,),
            import_fingerprint=IMPORT_FINGERPRINT,
            reality_fingerprint=REALITY_FINGERPRINT,
            output_dir=root / "artifacts",
        )
        rows = load_gtfs_stop_reality_models(output / "derived" / "gtfs_stop_reality.csv")
        cls.native = {row.stop_id: row for row in rows}

    def test_every_published_field_matches(self) -> None:
        self.assertEqual(sorted(self.python), sorted(self.native))
        for stop_id, expected in self.python.items():
            for field in dataclasses.fields(GtfsStopReality):
                if (stop_id, field.name) == ("FRIDAY-RAIL", "friday_evening_deps"):
                    continue
                left, right = getattr(expected, field.name), getattr(self.native[stop_id], field.name)
                with self.subTest(stop_id=stop_id, field=field.name):
                    if isinstance(left, float) and isinstance(right, float):
                        self.assertAlmostEqual(left, right, places=6)
                    else:
                        self.assertEqual(left, right)

    def test_fixed_rail_and_tram_tiers(self) -> None:
        expected = {
            "RAIL-1": ("rail", 4), "RAIL-60": ("rail", 4),
            "TRAM-1": ("tram", 5), "TRAM-60": ("tram", 5),
            "BUS+RAIL-6": ("bus_rail", 4), "BUS+RAIL-56": ("bus_rail", 5),
            "BUS+TRAM-6": ("bus_tram", 5), "BUS+RAIL+TRAM-6": ("bus_rail_tram", 5),
            "RAIL+TRAM-6": ("rail_tram", 5), "RAIL+FERRY-6": ("rail", 4),
            "BUS-56": ("bus_only", 5), "BUS-55": ("bus_only", 4), "BUS-6": ("bus_only", 1),
        }
        for stop_id, pair in expected.items():
            with self.subTest(stop_id=stop_id):
                row = self.native[stop_id]
                self.assertEqual((row.transport_mode_tier, row.transport_score_units), pair)

    def test_no_public_departures_means_no_units(self) -> None:
        for stop_id in ("ENDED-RAIL", "ENDED-TRAM", "ENDED-BUS", "SCHOOL-RAIL-ONLY", "SCHOOL-ONLY", "UNSCHEDULED"):
            with self.subTest(stop_id=stop_id):
                self.assertEqual(self.native[stop_id].public_departures_30d, 0)
                self.assertEqual(self.native[stop_id].transport_score_units, 0)
                self.assertEqual(self.python[stop_id].transport_score_units, 0)
        self.assertEqual(self.native["ENDED-RAIL"].transport_mode_tier, "rail")
        self.assertEqual(self.native["SCHOOL-ONLY"].school_only_state, "yes")
        self.assertEqual(self.native["SCHOOL-RAIL"].transport_score_units, 4)

    @unittest.expectedFailure
    def test_friday_evening_counts_every_mode(self) -> None:
        # walkgraph fills friday_evening for buses only.
        self.assertGreater(self.python["FRIDAY-RAIL"].friday_evening_deps, 0)
        self.assertAlmostEqual(
            self.python["FRIDAY-RAIL"].friday_evening_deps,
            self.native["FRIDAY-RAIL"].friday_evening_deps,
            places=6,
        )

    def test_feed_covers_every_tier(self) -> None:
        rows = self.python.values()
        self.assertEqual(
            {row.bus_frequency_tier for row in rows} - {None},
            {"frequent", "moderate", "low_frequency", "very_low_frequency", "token_skeletal"},
        )
        self.assertLessEqual(
            {"mon_sun", "mon_sat", "tue_sun", "weekdays_only", "weekends_only", "single_day_only", "partial_week"},
            {row.bus_service_subtier for row in rows},
        )
        self.assertEqual({row.transport_score_units for row in rows}, {0, 1, 2, 3, 4, 5})


if __name__ == "__main__":
    unittest.main()
