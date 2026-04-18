from __future__ import annotations

import io
import json
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
    CalendarService,
    FeedDataset,
    GtfsStopReality,
    StopInfo,
    StopServiceSummary,
)
from transit.rust_gtfs import load_gtfs_stop_reality_models, run_walkgraph_gtfs_refresh
from transit.service import expand_service_windows, summarize_gtfs_stops


def _write_gtfs_zip(path: Path, files: dict[str, str]) -> None:
    with ZipFile(path, "w") as archive:
        for file_name, content in files.items():
            archive.writestr(file_name, content)


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
    school_keywords: set[str] | None = None,
    time_bucket_counts: dict[str, int] | None = None,
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
                location_type=None,
                wheelchair_boarding=None,
                platform_code=None,
            )
        },
        calendar_services={
            service_id: CalendarService(
                feed_id="nta",
                service_id=service_id,
                monday=1,
                tuesday=1,
                wednesday=1,
                thursday=1,
                friday=1,
                saturday=0,
                sunday=0,
                start_date=service_start,
                end_date=service_end,
            )
        },
        stop_service_occurrences={("S1", service_id, "R1", "bus"): 1},
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


class _FakeProcess:
    def __init__(self, *, stderr_text: str = "", returncode: int = 0) -> None:
        self.stderr = io.StringIO(stderr_text)
        self.returncode = returncode

    def wait(self) -> int:
        return self.returncode


class TransitGtfsParsingTests(TestCase):
    def test_parse_gtfs_zip_requires_calendar_file(self) -> None:
        with TemporaryDirectory() as tmp_name:
            zip_path = Path(tmp_name) / "missing-calendar.zip"
            _write_gtfs_zip(
                zip_path,
                {
                    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main Street,53.35,-6.26\n",
                    "routes.txt": "route_id,route_type\nR1,3\n",
                    "trips.txt": "route_id,service_id,trip_id\nR1,SVC1,T1\n",
                    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\n",
                    "calendar_dates.txt": "service_id,date,exception_type\nSVC1,20260414,1\n",
                },
            )

            with self.assertRaisesRegex(RuntimeError, "calendar.txt"):
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
        self.assertEqual(row.source_reason_codes, ("gtfs_direct_source",))

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
        self.assertNotIn("osm_source_ref", properties)
        self.assertNotIn("match_status", properties)
        self.assertIn("GTFS", readme_text)
        self.assertIn("directly from configured GTFS feeds", readme_text)

    def test_load_gtfs_stop_reality_models_parses_csv_payload(self) -> None:
        with TemporaryDirectory() as tmp_name:
            csv_path = Path(tmp_name) / "gtfs_stop_reality.csv"
            csv_path.write_text(
                "\n".join(
                    (
                        "reality_fingerprint,import_fingerprint,source_ref,stop_name,feed_id,stop_id,source_status,reality_status,school_only_state,public_departures_7d,public_departures_30d,school_only_departures_30d,last_public_service_date,last_any_service_date,route_modes_json,source_reason_codes_json,reality_reason_codes_json,lat,lon,created_at",
                        'reality-123,import-123,gtfs/nta/S1,Main Street,nta,S1,gtfs_direct,active_confirmed,no,7,21,0,2026-04-14,2026-04-14,"[""bus""]","[""gtfs_direct_source""]","[""public_service_present""]",53.35,-6.26,2026-04-14T00:00:00+00:00',
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
        self.assertNotIn("osm_features_path", config_payload)
        self.assertNotIn("match_radius_m", config_payload)


class TransitWorkflowTests(TestCase):
    def test_transit_reality_refresh_required_ignores_import_fingerprint(self) -> None:
        prepared_state = SimpleNamespace(reality_fingerprint="reality-123")

        with (
            mock.patch.object(transit_workflow, "prepare_transit_reality_state", return_value=prepared_state),
            mock.patch.object(transit_workflow, "has_complete_transit_reality_manifest", return_value=True) as manifest_mock,
        ):
            state, refresh_required = transit_workflow.transit_reality_refresh_required(
                mock.sentinel.engine,
                import_fingerprint="import-123",
            )

        self.assertIs(state, prepared_state)
        self.assertFalse(refresh_required)
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
