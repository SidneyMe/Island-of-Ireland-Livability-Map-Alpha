from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase, mock

import transit_refresh_runner


def _tracker_mock() -> mock.Mock:
    tracker = mock.Mock()
    tracker.phase_callback.return_value = mock.Mock(name="progress_cb")
    return tracker


class TransitRefreshRunnerTests(TestCase):
    def test_refresh_transit_reuses_cached_manifest_without_geometry(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")
        reality_state = SimpleNamespace(reality_fingerprint="reality-123")
        tracker = _tracker_mock()
        events: list[str] = []

        tracker.start_phase.side_effect = lambda *args, **kwargs: events.append("start_phase")

        with (
            mock.patch.object(
                transit_refresh_runner,
                "build_engine",
                return_value=mock.sentinel.engine,
            ),
            mock.patch.object(
                transit_refresh_runner,
                "ensure_database_ready",
                side_effect=lambda engine: events.append("ensure_database_ready"),
            ),
            mock.patch.object(
                transit_refresh_runner,
                "resolve_source_state",
                side_effect=lambda **kwargs: (
                    events.append("resolve_source_state"),
                    source_state,
                )[1],
            ) as resolve_source_state_mock,
            mock.patch.object(
                transit_refresh_runner,
                "_preflight_transit_rebuild",
                return_value=(reality_state, False),
            ) as preflight_mock,
            mock.patch.object(
                transit_refresh_runner,
                "ensure_transit_reality",
                return_value=reality_state,
            ) as ensure_transit_mock,
            mock.patch.object(
                transit_refresh_runner,
                "PrecomputeProgressTracker",
                return_value=tracker,
            ),
            mock.patch.object(transit_refresh_runner, "_load_study_area_wgs84") as load_geometry_mock,
        ):
            result = transit_refresh_runner.refresh_transit()

        self.assertEqual(result, "reality-123")
        self.assertEqual(
            events[:3],
            ["start_phase", "ensure_database_ready", "resolve_source_state"],
        )
        resolve_source_state_mock.assert_called_once_with(
            progress_cb=tracker.phase_callback.return_value,
        )
        preflight_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            force_refresh=False,
            refresh_download=True,
            progress_cb=tracker.phase_callback.return_value,
        )
        ensure_transit_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            force_refresh=False,
            refresh_download=False,
            progress_cb=tracker.phase_callback.return_value,
            reality_state=reality_state,
        )
        tracker.start_phase.assert_called_once_with(
            "transit",
            detail="initializing transit refresh",
        )
        self.assertEqual(
            tracker.set_phase_detail.call_args_list,
            [
                mock.call(
                    "transit",
                    "connecting to PostgreSQL / checking managed schema",
                    force_log=True,
                ),
                mock.call(
                    "transit",
                    "resolving OSM source state",
                    force_log=True,
                ),
                mock.call(
                    "transit",
                    "starting GTFS feed availability checks",
                    force_log=True,
                ),
            ],
        )
        tracker.finish_phase.assert_called_once_with(
            "transit",
            "cached",
            detail="transit reality ready (reality-123)",
        )
        load_geometry_mock.assert_not_called()

    def test_refresh_transit_skips_geometry_when_import_payload_is_ready(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")
        reality_state = SimpleNamespace(reality_fingerprint="reality-123")
        tracker = _tracker_mock()

        with (
            mock.patch.object(
                transit_refresh_runner,
                "build_engine",
                return_value=mock.sentinel.engine,
            ),
            mock.patch.object(transit_refresh_runner, "ensure_database_ready"),
            mock.patch.object(
                transit_refresh_runner,
                "resolve_source_state",
                return_value=source_state,
            ) as resolve_source_state_mock,
            mock.patch.object(
                transit_refresh_runner,
                "_preflight_transit_rebuild",
                return_value=(reality_state, True),
            ),
            mock.patch.object(
                transit_refresh_runner,
                "import_payload_ready",
                return_value=True,
            ) as import_ready_mock,
            mock.patch.object(
                transit_refresh_runner,
                "ensure_local_osm_import",
            ) as ensure_import_mock,
            mock.patch.object(
                transit_refresh_runner,
                "ensure_transit_reality",
                return_value=reality_state,
            ) as ensure_transit_mock,
            mock.patch.object(
                transit_refresh_runner,
                "PrecomputeProgressTracker",
                return_value=tracker,
            ),
            mock.patch.object(transit_refresh_runner, "_load_study_area_wgs84") as load_geometry_mock,
        ):
            result = transit_refresh_runner.refresh_transit()

        self.assertEqual(result, "reality-123")
        resolve_source_state_mock.assert_called_once_with(
            progress_cb=tracker.phase_callback.return_value,
        )
        import_ready_mock.assert_called_once()
        ensure_import_mock.assert_not_called()
        load_geometry_mock.assert_not_called()
        ensure_transit_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            refresh_download=False,
            force_refresh=False,
            progress_cb=tracker.phase_callback.return_value,
            reality_state=reality_state,
        )
        self.assertEqual(
            tracker.set_phase_detail.call_args_list,
            [
                mock.call(
                    "transit",
                    "connecting to PostgreSQL / checking managed schema",
                    force_log=True,
                ),
                mock.call(
                    "transit",
                    "resolving OSM source state",
                    force_log=True,
                ),
                mock.call(
                    "transit",
                    "starting GTFS feed availability checks",
                    force_log=True,
                ),
            ],
        )
        tracker.finish_phase.assert_called_once_with(
            "transit",
            "completed",
            detail="transit reality ready (reality-123)",
        )

    def test_refresh_transit_loads_geometry_only_for_import_rebuild(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")
        reality_state = SimpleNamespace(reality_fingerprint="reality-123")
        tracker = _tracker_mock()

        with (
            mock.patch.object(
                transit_refresh_runner,
                "build_engine",
                return_value=mock.sentinel.engine,
            ),
            mock.patch.object(transit_refresh_runner, "ensure_database_ready"),
            mock.patch.object(
                transit_refresh_runner,
                "resolve_source_state",
                return_value=source_state,
            ),
            mock.patch.object(
                transit_refresh_runner,
                "_preflight_transit_rebuild",
                return_value=(reality_state, True),
            ),
            mock.patch.object(
                transit_refresh_runner,
                "import_payload_ready",
                return_value=False,
            ) as import_ready_mock,
            mock.patch.object(
                transit_refresh_runner,
                "_load_study_area_wgs84",
                return_value=mock.sentinel.study_area_wgs84,
            ) as load_geometry_mock,
            mock.patch.object(
                transit_refresh_runner,
                "ensure_local_osm_import",
            ) as ensure_import_mock,
            mock.patch.object(
                transit_refresh_runner,
                "ensure_transit_reality",
                return_value=reality_state,
            ) as ensure_transit_mock,
            mock.patch.object(
                transit_refresh_runner,
                "PrecomputeProgressTracker",
                return_value=tracker,
            ),
        ):
            result = transit_refresh_runner.refresh_transit(profile="test")

        self.assertEqual(result, "reality-123")
        import_ready_mock.assert_called_once()
        load_geometry_mock.assert_called_once_with("test", tracker)
        ensure_import_mock.assert_called_once_with(
            mock.sentinel.engine,
            source_state,
            study_area_wgs84=mock.sentinel.study_area_wgs84,
            normalization_scope_hash=mock.ANY,
            force_refresh=False,
            progress_cb=tracker.phase_callback.return_value,
        )
        ensure_transit_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            refresh_download=False,
            force_refresh=False,
            progress_cb=tracker.phase_callback.return_value,
            reality_state=reality_state,
        )
