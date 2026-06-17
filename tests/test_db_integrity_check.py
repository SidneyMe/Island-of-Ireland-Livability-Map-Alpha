from __future__ import annotations

import io
from contextlib import redirect_stdout
from unittest import TestCase, mock

from scripts import db_integrity_check


class _FakeResult:
    def __init__(self, *, scalar_value=None, rows=None) -> None:
        self._scalar_value = scalar_value
        self._rows = rows or []

    def scalar_one(self):
        return self._scalar_value

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, results: list[_FakeResult]) -> None:
        self._results = results
        self.executed = []

    def execute(self, statement):
        self.executed.append(statement)
        if not self._results:
            raise AssertionError("unexpected execute call")
        return self._results.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def connect(self):
        return self._connection


def _spec(label: str) -> db_integrity_check.DuplicateCheckSpec:
    return next(spec for spec in db_integrity_check.build_duplicate_check_specs() if spec.label == label)


class DuplicateCheckSpecTests(TestCase):
    def test_default_specs_include_expected_keys_and_skip_ambiguous_amenities(self) -> None:
        specs = db_integrity_check.build_duplicate_check_specs()
        by_label = {spec.label: spec for spec in specs}

        self.assertEqual(by_label["grid_walk"].key_columns, ("build_key", "resolution_m", "cell_id"))
        self.assertEqual(
            by_label["transit_derived.service_classification"].key_columns,
            ("reality_fingerprint", "feed_id", "service_id"),
        )
        self.assertEqual(
            by_label["transit_derived.gtfs_stop_service_summary"].key_columns,
            ("reality_fingerprint", "feed_id", "stop_id"),
        )
        self.assertEqual(
            by_label["transit_derived.gtfs_stop_reality"].key_columns,
            ("reality_fingerprint", "source_ref"),
        )
        self.assertTrue(by_label["amenities"].ambiguous)
        self.assertIn("safe logical key", by_label["amenities"].ambiguity_reason or "")

    def test_duplicate_and_null_queries_include_expected_key_columns(self) -> None:
        spec = _spec("grid_walk")
        duplicate_query = db_integrity_check.build_duplicate_sample_query(spec, sample_limit=3)
        null_query = db_integrity_check.build_null_sample_query(spec, sample_limit=3)
        null_count_query = db_integrity_check.build_null_count_query(spec, "build_key")

        duplicate_sql = str(duplicate_query)
        for column_name in spec.key_columns:
            self.assertIn(column_name, duplicate_sql)
        self.assertIn("duplicate_rows", duplicate_sql)

        null_sql = str(null_query)
        self.assertIn("NULL", null_sql)
        self.assertIn("build_key", null_sql)
        self.assertIn("resolution_m", null_sql)
        self.assertIn("cell_id", null_sql)

        null_count_sql = str(null_count_query)
        self.assertIn("build_key", null_count_sql)
        self.assertIn("NULL", null_count_sql)


class RunnerTests(TestCase):
    def test_run_integrity_check_reports_ok_when_no_duplicates_and_no_nulls(self) -> None:
        spec = _spec("grid_walk")
        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
            ]
        )
        engine = _FakeEngine(connection)
        stdout = io.StringIO()

        with (
            mock.patch.object(db_integrity_check, "table_exists", return_value=True),
            mock.patch.object(db_integrity_check, "_build_engine", return_value=engine),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check(checks=(spec,))

        self.assertEqual(exit_code, 0)
        self.assertIn(
            "OK grid_walk: no duplicates and no NULL key values for (build_key, resolution_m, cell_id)",
            stdout.getvalue(),
        )

    def test_run_integrity_check_reports_duplicate_failures(self) -> None:
        spec = _spec("grid_walk")
        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=2),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(
                    rows=[
                        {
                            "build_key": "build-1",
                            "resolution_m": 5000,
                            "cell_id": "cell-1",
                            "duplicate_rows": 2,
                        }
                    ]
                ),
            ]
        )
        engine = _FakeEngine(connection)
        stdout = io.StringIO()

        with (
            mock.patch.object(db_integrity_check, "table_exists", return_value=True),
            mock.patch.object(db_integrity_check, "_build_engine", return_value=engine),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check(checks=(spec,), sample_limit=1)

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn("FAIL grid_walk: duplicate groups found for (build_key, resolution_m, cell_id)", output)
        self.assertIn("2 duplicate groups", output)
        self.assertIn("Sample duplicate keys:", output)
        self.assertIn("build_key='build-1'", output)

    def test_run_integrity_check_reports_null_failures(self) -> None:
        spec = _spec("grid_walk")
        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=1),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=2),
                _FakeResult(
                    rows=[
                        {
                            "build_key": None,
                            "resolution_m": 5000,
                            "cell_id": "cell-1",
                        },
                        {
                            "build_key": "build-2",
                            "resolution_m": 5000,
                            "cell_id": None,
                        },
                    ]
                ),
            ]
        )
        engine = _FakeEngine(connection)
        stdout = io.StringIO()

        with (
            mock.patch.object(db_integrity_check, "table_exists", return_value=True),
            mock.patch.object(db_integrity_check, "_build_engine", return_value=engine),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check(checks=(spec,), sample_limit=2)

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn("FAIL grid_walk: NULL key values found for (build_key, resolution_m, cell_id)", output)
        self.assertIn("build_key: 1 row with NULL", output)
        self.assertIn("cell_id: 2 rows with NULL", output)
        self.assertIn("Sample NULL rows:", output)
        self.assertIn("NULL columns: build_key", output)
        self.assertIn("NULL columns: cell_id", output)

    def test_run_integrity_check_reports_both_duplicate_and_null_failures(self) -> None:
        spec = _spec("grid_walk")
        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=1),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=1),
                _FakeResult(scalar_value=0),
                _FakeResult(
                    rows=[
                        {
                            "build_key": "build-1",
                            "resolution_m": 5000,
                            "cell_id": "cell-1",
                            "duplicate_rows": 2,
                        }
                    ]
                ),
                _FakeResult(
                    rows=[
                        {
                            "build_key": None,
                            "resolution_m": 5000,
                            "cell_id": "cell-2",
                        }
                    ]
                ),
            ]
        )
        engine = _FakeEngine(connection)
        stdout = io.StringIO()

        with (
            mock.patch.object(db_integrity_check, "table_exists", return_value=True),
            mock.patch.object(db_integrity_check, "_build_engine", return_value=engine),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check(checks=(spec,), sample_limit=1)

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn("FAIL grid_walk: duplicate groups found for (build_key, resolution_m, cell_id)", output)
        self.assertIn("FAIL grid_walk: NULL key values found for (build_key, resolution_m, cell_id)", output)
        self.assertIn("Sample duplicate keys:", output)
        self.assertIn("Sample NULL rows:", output)

    def test_run_integrity_check_reports_ok_fail_missing_and_skip(self) -> None:
        specs = {spec.label: spec for spec in db_integrity_check.build_duplicate_check_specs()}
        ok_spec = specs["grid_walk"]
        fail_spec = specs["transit_derived.service_classification"]
        missing_spec = specs["transit_derived.gtfs_stop_service_summary"]
        skip_spec = specs["amenities"]

        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=2),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(scalar_value=0),
                _FakeResult(
                    rows=[
                        {
                            "reality_fingerprint": "rf-1",
                            "feed_id": "feed-1",
                            "service_id": "svc-1",
                            "duplicate_rows": 2,
                        }
                    ]
                ),
            ]
        )
        engine = _FakeEngine(connection)
        stdout = io.StringIO()

        def fake_table_exists(_engine, table_name, schema=None):
            if table_name == "gtfs_stop_service_summary":
                return False
            return True

        with (
            mock.patch.object(db_integrity_check, "table_exists", side_effect=fake_table_exists),
            mock.patch.object(db_integrity_check, "_build_engine", return_value=engine),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check(
                checks=(ok_spec, fail_spec, missing_spec, skip_spec),
                sample_limit=2,
            )

        self.assertEqual(exit_code, 1)
        output = stdout.getvalue()
        self.assertIn("OK grid_walk: no duplicates and no NULL key values for (build_key, resolution_m, cell_id)", output)
        self.assertIn(
            "FAIL transit_derived.service_classification: duplicate groups found for (reality_fingerprint, feed_id, service_id)",
            output,
        )
        self.assertIn("Sample duplicate keys:", output)
        self.assertIn("reality_fingerprint='rf-1'", output)
        self.assertIn("MISSING transit_derived.gtfs_stop_service_summary: table not found", output)
        self.assertIn("SKIP amenities: no safe logical key could be derived", output)

    def test_run_integrity_check_reports_engine_errors_cleanly(self) -> None:
        stdout = io.StringIO()
        with (
            mock.patch.object(db_integrity_check, "_build_engine", side_effect=RuntimeError("boom")),
            redirect_stdout(stdout),
        ):
            exit_code = db_integrity_check.run_integrity_check()

        self.assertEqual(exit_code, 1)
        self.assertIn("Error: boom", stdout.getvalue())

    def test_run_integrity_check_rejects_empty_database_url(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = db_integrity_check.run_integrity_check(database_url="")

        self.assertEqual(exit_code, 1)
        self.assertIn("Error: --database-url was provided but empty", stdout.getvalue())

    def test_main_rejects_empty_database_url_argument(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = db_integrity_check.main(["--database-url", ""])

        self.assertEqual(exit_code, 1)
        self.assertIn("Error: --database-url was provided but empty", stdout.getvalue())
