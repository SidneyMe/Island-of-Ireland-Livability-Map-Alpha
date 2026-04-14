from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock

from scripts import sanity_check


def _valid_fixture_payload() -> dict[str, object]:
    locations = []
    for index in range(sanity_check.EXPECTED_LOCATION_COUNT):
        locations.append(
            {
                "id": f"loc_{index:02d}",
                "name": f"Location {index:02d}",
                "lat": 53.1 + (index * 0.01),
                "lon": -6.4 - (index * 0.01),
                "tags": ["control_town"] if index % 2 == 0 else ["suburb"],
                "expected_total_score": {"min": 10.0, "max": 20.0},
                "rationale": f"Fixture rationale {index:02d}",
            }
        )
    return {
        "fixture_version": sanity_check.EXPECTED_FIXTURE_VERSION,
        "score_scale": sanity_check.EXPECTED_SCORE_SCALE,
        "notes": [
            "Synthetic test fixture.",
            "Used to exercise the sanity check runner.",
        ],
        "locations": locations,
    }


def _write_fixture(tmp_dir: Path, payload: dict[str, object]) -> Path:
    fixture_path = tmp_dir / "fixture.json"
    fixture_path.write_text(json.dumps(payload), encoding="utf-8")
    return fixture_path


def _grid_walk_rows(payload: dict[str, object], *, score: float = 15.0) -> list[dict[str, object]]:
    rows = []
    for location in payload["locations"]:  # type: ignore[index]
        rows.append(
            {
                "point_id": location["id"],
                "lat": location["lat"],
                "lon": location["lon"],
                "resolution_m": 5000,
                "total_score": score,
                "scores_json": {"shops": 10.0},
                "counts_json": {"shops": 2},
            }
        )
    return rows


class FixtureStructureTests(TestCase):
    def test_checked_in_fixture_validates_cleanly(self) -> None:
        payload = sanity_check.load_fixture(sanity_check.DEFAULT_FIXTURE_PATH)

        self.assertEqual(sanity_check.validate_fixture_payload(payload), [])

    def test_checked_in_fixture_has_expected_size_and_unique_ids(self) -> None:
        payload = sanity_check.load_fixture(sanity_check.DEFAULT_FIXTURE_PATH)
        locations = payload["locations"]

        self.assertEqual(len(locations), sanity_check.EXPECTED_LOCATION_COUNT)
        self.assertEqual(
            len({location["id"] for location in locations}),
            sanity_check.EXPECTED_LOCATION_COUNT,
        )

    def test_checked_in_fixture_uses_known_tags_and_valid_ranges(self) -> None:
        payload = sanity_check.load_fixture(sanity_check.DEFAULT_FIXTURE_PATH)
        coastal_count = 0

        for location in payload["locations"]:
            with self.subTest(location_id=location["id"]):
                self.assertTrue(set(location["tags"]).issubset(sanity_check.VALID_TAGS))
                self.assertLessEqual(location["expected_total_score"]["min"], location["expected_total_score"]["max"])
                self.assertGreaterEqual(location["expected_total_score"]["min"], 0.0)
                self.assertLessEqual(location["expected_total_score"]["max"], 100.0)
                self.assertGreaterEqual(location["lat"], sanity_check.IRELAND_LAT_RANGE[0])
                self.assertLessEqual(location["lat"], sanity_check.IRELAND_LAT_RANGE[1])
                self.assertGreaterEqual(location["lon"], sanity_check.IRELAND_LON_RANGE[0])
                self.assertLessEqual(location["lon"], sanity_check.IRELAND_LON_RANGE[1])
                if "coastal" in location["tags"]:
                    coastal_count += 1

        self.assertGreaterEqual(coastal_count, 4)


class RunnerTests(TestCase):
    def test_validate_only_succeeds_on_valid_fixture(self) -> None:
        with TemporaryDirectory() as tmp_name:
            fixture_path = _write_fixture(Path(tmp_name), _valid_fixture_payload())
            stdout = io.StringIO()

            with redirect_stdout(stdout):
                exit_code = sanity_check.run_sanity_check(
                    fixture_path=fixture_path,
                    validate_only=True,
                )

        self.assertEqual(exit_code, 0)
        self.assertIn("Fixture valid:", stdout.getvalue())

    def test_validate_only_reports_invalid_fixture(self) -> None:
        payload = _valid_fixture_payload()
        payload["locations"][0]["tags"] = ["not-a-real-tag"]  # type: ignore[index]

        with TemporaryDirectory() as tmp_name:
            fixture_path = _write_fixture(Path(tmp_name), payload)
            stdout = io.StringIO()

            with redirect_stdout(stdout):
                exit_code = sanity_check.run_sanity_check(
                    fixture_path=fixture_path,
                    validate_only=True,
                )

        self.assertEqual(exit_code, 1)
        self.assertIn("Fixture validation failed", stdout.getvalue())
        self.assertIn("unknown tag", stdout.getvalue())

    def test_run_sanity_check_uses_runtime_service_fine_surface_path(self) -> None:
        payload = _valid_fixture_payload()
        service = mock.Mock()
        service.state.return_value = SimpleNamespace(
            fine_surface_enabled=True,
            build_key="build-key-123",
        )
        service.inspect.return_value = {
            "valid_land": True,
            "resolution_m": 50,
            "component_scores": {"shops": 10.0},
            "counts": {"shops": 2},
            "total_score": 15.0,
        }

        with TemporaryDirectory() as tmp_name:
            fixture_path = _write_fixture(Path(tmp_name), payload)
            stdout = io.StringIO()
            with (
                mock.patch.object(sanity_check, "build_engine", return_value=mock.sentinel.engine),
                mock.patch.object(sanity_check, "ensure_database_ready"),
                mock.patch.object(sanity_check, "RuntimeService", return_value=service) as runtime_cls,
                mock.patch.object(sanity_check, "load_point_scores_for_build") as point_scores_mock,
                redirect_stdout(stdout),
            ):
                exit_code = sanity_check.run_sanity_check(
                    fixture_path=fixture_path,
                    profile="dev",
                )

        self.assertEqual(exit_code, 0)
        runtime_cls.assert_called_once_with(mock.sentinel.engine, profile="dev")
        service.state.assert_called_once_with()
        self.assertEqual(service.inspect.call_count, sanity_check.EXPECTED_LOCATION_COUNT)
        point_scores_mock.assert_not_called()
        self.assertIn("using fine_surface lookups", stdout.getvalue())

    def test_run_sanity_check_falls_back_to_grid_walk(self) -> None:
        payload = _valid_fixture_payload()
        service = mock.Mock()
        service.state.return_value = SimpleNamespace(
            fine_surface_enabled=False,
            build_key="build-key-456",
        )

        with TemporaryDirectory() as tmp_name:
            fixture_path = _write_fixture(Path(tmp_name), payload)
            stdout = io.StringIO()
            with (
                mock.patch.object(sanity_check, "build_engine", return_value=mock.sentinel.engine),
                mock.patch.object(sanity_check, "ensure_database_ready"),
                mock.patch.object(sanity_check, "RuntimeService", return_value=service),
                mock.patch.object(
                    sanity_check,
                    "load_point_scores_for_build",
                    return_value=_grid_walk_rows(payload),
                ) as point_scores_mock,
                redirect_stdout(stdout),
            ):
                exit_code = sanity_check.run_sanity_check(fixture_path=fixture_path)

        self.assertEqual(exit_code, 0)
        service.inspect.assert_not_called()
        point_scores_mock.assert_called_once()
        self.assertEqual(point_scores_mock.call_args.kwargs["build_key"], "build-key-456")
        self.assertIn("using grid_walk lookups", stdout.getvalue())

    def test_run_sanity_check_reports_mismatch_with_readable_output(self) -> None:
        payload = _valid_fixture_payload()
        rows = _grid_walk_rows(payload)
        rows[0]["total_score"] = 35.0
        service = mock.Mock()
        service.state.return_value = SimpleNamespace(
            fine_surface_enabled=False,
            build_key="build-key-789",
        )

        with TemporaryDirectory() as tmp_name:
            fixture_path = _write_fixture(Path(tmp_name), payload)
            stdout = io.StringIO()
            with (
                mock.patch.object(sanity_check, "build_engine", return_value=mock.sentinel.engine),
                mock.patch.object(sanity_check, "ensure_database_ready"),
                mock.patch.object(sanity_check, "RuntimeService", return_value=service),
                mock.patch.object(sanity_check, "load_point_scores_for_build", return_value=rows),
                redirect_stdout(stdout),
            ):
                exit_code = sanity_check.run_sanity_check(fixture_path=fixture_path)

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn("Sanity fixture failed for build build-key-789", output)
        self.assertIn("MISMATCH loc_00", output)
        self.assertIn("expected 10.0-20.0, got 35.0", output)
