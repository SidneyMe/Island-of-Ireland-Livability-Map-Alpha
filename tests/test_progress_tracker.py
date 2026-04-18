from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import progress_tracker
from progress_tracker import PrecomputeProgressTracker


class ProgressTrackerTests(TestCase):
    def test_format_hms_formats_and_clamps_seconds(self) -> None:
        self.assertEqual(progress_tracker._format_hms(3661.9), "01:01:01")
        self.assertEqual(progress_tracker._format_hms(-10.0), "00:00:00")

    def test_invalid_stats_history_is_filtered(self) -> None:
        with TemporaryDirectory() as tmp_name:
            stats_path = Path(tmp_name) / "progress.json"
            stats_path.write_text(
                json.dumps(
                    {
                        "last_total_seconds": -1,
                        "phases": {
                            "geometry": 12.5,
                            "amenities": -4,
                            "not-a-phase": 99,
                        },
                        "substeps": {
                            "geometry": {
                                "union": 3.0,
                                "negative": -2,
                            },
                            "not-a-phase": {
                                "ignored": 1.0,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            tracker = PrecomputeProgressTracker(stats_path)

        self.assertIsNone(tracker._history["last_total_seconds"])
        self.assertEqual(tracker._history["phases"], {"geometry": 12.5})
        self.assertEqual(tracker._history["substeps"], {"geometry": {"union": 3.0}})

    def test_corrupt_stats_history_falls_back_to_empty_history(self) -> None:
        with TemporaryDirectory() as tmp_name:
            stats_path = Path(tmp_name) / "progress.json"
            stats_path.write_text("{bad json", encoding="utf-8")

            tracker = PrecomputeProgressTracker(stats_path)

        self.assertEqual(
            tracker._history,
            {"last_total_seconds": None, "phases": {}, "substeps": {}},
        )

    def test_phase_start_advance_and_finish_transitions(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tracker = PrecomputeProgressTracker(
                Path(tmp_name) / "progress.json",
                progress_interval_seconds=999.0,
            )

            with mock.patch("builtins.print"):
                tracker.start_phase(
                    "geometry",
                    total_units=4,
                    rebuild_total_units=2,
                    unit_label="cells",
                    detail="starting",
                )
                tracker.advance_phase(
                    "geometry",
                    units=2,
                    rebuild_units=1,
                    detail="halfway",
                    force_log=True,
                )
                tracker.finish_phase("geometry", "completed", detail="done")

        phase = tracker.phases["geometry"]
        self.assertIsNotNone(phase.started_at)
        self.assertIsNotNone(phase.finished_at)
        self.assertEqual(phase.status, "completed")
        self.assertEqual(phase.completed_units, 4)
        self.assertEqual(phase.rebuild_completed_units, 2)
        self.assertEqual(phase.unit_label, "cells")
        self.assertEqual(phase.detail, "done")

    def test_save_successful_timings_persists_stats_file(self) -> None:
        with TemporaryDirectory() as tmp_name:
            stats_path = Path(tmp_name) / "nested" / "progress.json"
            tracker = PrecomputeProgressTracker(stats_path)
            tracker.run_started_at = 100.0
            phase = tracker.phases["geometry"]
            phase.started_at = 101.0
            phase.finished_at = 106.5
            phase.status = "completed"
            tracker.substeps["geometry"] = {"union": 1.25}

            with mock.patch.object(progress_tracker.time, "perf_counter", return_value=110.0):
                tracker.save_successful_timings()

            payload = json.loads(stats_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["last_total_seconds"], 10.0)
        self.assertEqual(payload["phases"]["geometry"], 5.5)
        self.assertEqual(payload["substeps"]["geometry"], {"union": 1.25})

    def test_phase_callback_records_substep_events(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tracker = PrecomputeProgressTracker(Path(tmp_name) / "progress.json")
            callback = tracker.phase_callback("geometry")

            callback("substep", substep_name="roi_read", seconds=1.5)

        self.assertEqual(tracker.substeps["geometry"], {"roi_read": 1.5})
