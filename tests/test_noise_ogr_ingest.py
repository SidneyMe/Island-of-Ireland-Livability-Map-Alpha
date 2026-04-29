from __future__ import annotations

import os
import queue
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy.engine import make_url


class _FakeExecuteResult:
    def __init__(self, *, rows=None, rowcount: int = 0):
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]

    def scalar_one_or_none(self):
        row = self.fetchone()
        if row is None:
            return None
        if isinstance(row, (tuple, list)):
            return row[0] if row else None
        return row

    def scalar_one(self):
        value = self.scalar_one_or_none()
        if value is None:
            raise AssertionError("expected scalar_one value")
        return value


class _FakeNormalizeConn:
    def __init__(self):
        self.sql_texts: list[str] = []

    def execute(self, statement, params=None):  # noqa: ANN001 - SQLAlchemy text object in production.
        sql = str(statement)
        self.sql_texts.append(sql)
        if "SELECT DISTINCT" in sql:
            return _FakeExecuteResult(rows=[("45-49", 45.0, 49.0)])
        if "INSERT INTO noise_normalized" in sql:
            return _FakeExecuteResult(rowcount=0)
        return _FakeExecuteResult()


class _FakeUnionNormalizeConn:
    def __init__(self):
        self.sql_texts: list[str] = []

    def execute(self, statement, params=None):  # noqa: ANN001 - SQLAlchemy text object in production.
        del params
        sql = str(statement)
        self.sql_texts.append(sql)
        if "SELECT DISTINCT" in sql:
            return _FakeExecuteResult(rows=[("45-49", 45.0, 49.0)])
        if "INSERT INTO noise_normalized" in sql:
            return _FakeExecuteResult(rowcount=7)
        return _FakeExecuteResult()


class _FakeStream:
    def __init__(self, lines):
        self._lines = list(lines)
        self._index = 0

    def readline(self):
        if self._index >= len(self._lines):
            return ""
        value = self._lines[self._index]
        self._index += 1
        return value

    def close(self):
        return None


class _FakePopenProcess:
    def __init__(self, lines, returncode: int = 0):
        self.stdout = _FakeStream(lines)
        self._returncode = int(returncode)
        self._killed = False

    def poll(self):
        if self._killed:
            return -9
        if self.stdout._index >= len(self.stdout._lines):
            return self._returncode
        return None

    def wait(self):
        if self._killed:
            return -9
        return self._returncode

    def kill(self):
        self._killed = True

    def terminate(self):
        self._killed = True


class _ImmediateFuture:
    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value

    def cancel(self):
        return None


class _ControllableFuture:
    def __init__(self, *, value=None, exc=None):
        self._value = value
        self._exc = exc
        self.cancelled = False

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._value

    def cancel(self):
        self.cancelled = True


class _FakeIngestConn:
    def __init__(self):
        self.sql: list[str] = []
        self.pg_tables: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.lock_rows: list[tuple] = []
        self.stale_backend_rows: list[tuple] = []
        self.raise_on_lock_diag = False

    def execute(self, statement, params=None):  # noqa: ANN001
        sql = str(statement)
        self.sql.append(sql)
        if self.raise_on_lock_diag and "JOIN pg_locks" in sql:
            raise RuntimeError("lock diag failed")
        if "SHOW data_directory" in sql:
            return _FakeExecuteResult(rows=[("C:/tmp",)])
        if "FROM pg_stat_activity" in sql and "_noise_raw_" in sql and "COPY" in sql:
            return _FakeExecuteResult(rows=list(self.stale_backend_rows))
        if "JOIN pg_locks" in sql and "to_regclass" in sql:
            return _FakeExecuteResult(rows=list(self.lock_rows))
        if "FROM pg_tables" in sql:
            return _FakeExecuteResult(rows=[(name,) for name in self.pg_tables])
        return _FakeExecuteResult()

    def commit(self):
        self.commit_count += 1
        return None

    def rollback(self):
        self.rollback_count += 1
        return None


class _FakeConnectCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


class _FakeIngestEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return _FakeConnectCtx(self._conn)


class NoiseOgrIngestTests(TestCase):
    def _fake_engine(self):
        return SimpleNamespace(url=make_url("postgresql+psycopg://user:pass@localhost:5432/livability"))

    def test_ogr2ogr_available_false_when_binary_missing(self) -> None:
        from noise_artifacts.ogr_ingest import ogr2ogr_available

        with patch("noise_artifacts.ogr_ingest.shutil.which", return_value=None):
            self.assertFalse(ogr2ogr_available())

    def test_build_ogr2ogr_command_contains_expected_flags(self) -> None:
        from noise_artifacts.ogr_ingest import build_ogr2ogr_command

        cmd = build_ogr2ogr_command(
            engine=self._fake_engine(),
            source_path=Path("C:/tmp/layer.shp"),
            stage_table="_noise_raw_test",
        )
        self.assertIn("-f", cmd)
        self.assertIn("PostgreSQL", cmd)
        self.assertIn("-t_srs", cmd)
        self.assertIn("EPSG:2157", cmd)
        self.assertIn("-nlt", cmd)
        self.assertIn("MULTIPOLYGON", cmd)
        self.assertIn("-lco", cmd)
        self.assertIn("GEOMETRY_NAME=geom", cmd)
        self.assertIn("_noise_raw_test", cmd)
        self.assertIn("PRECISION=NO", cmd)
        self.assertIn("PG_USE_COPY", cmd)
        self.assertIn("YES", cmd)
        self.assertIn("-preserve_fid", cmd)
        self.assertIn("-progress", cmd)

    def test_build_ogr2ogr_command_adds_select_clause_when_fields_provided(self) -> None:
        from noise_artifacts.ogr_ingest import build_ogr2ogr_command

        selected_fields = ["DB_LOW", "DB_HIGH", "OBJECTID"]
        cmd = build_ogr2ogr_command(
            engine=self._fake_engine(),
            source_path=Path("C:/tmp/layer.shp"),
            stage_table="_noise_raw_test",
            selected_fields=selected_fields,
        )
        self.assertIn("-select", cmd)
        self.assertIn("DB_LOW,DB_HIGH,OBJECTID", cmd)

    def test_build_ogr2ogr_command_rejects_append_plus_select(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import build_ogr2ogr_command

        with self.assertRaises(NoiseIngestError):
            build_ogr2ogr_command(
                engine=self._fake_engine(),
                source_path=Path("C:/tmp/layer.shp"),
                stage_table="_noise_raw_test",
                selected_fields=["DB_LOW"],
                append=True,
            )

    def test_build_ogr2ogr_command_includes_progress(self) -> None:
        from noise_artifacts.ogr_ingest import build_ogr2ogr_command

        cmd = build_ogr2ogr_command(
            engine=self._fake_engine(),
            source_path=Path("C:/tmp/layer.shp"),
            stage_table="_noise_raw_test",
        )
        self.assertIn("-progress", cmd)

    def test_select_existing_noise_fields_excludes_shape_metadata_fields(self) -> None:
        from noise_artifacts.ogr_ingest import _select_existing_noise_fields

        available = ["OBJECTID", "DB_LOW", "DB_HIGH", "shape_star", "shape_leng"]
        candidates = ["DB_LOW", "DB_HIGH", "OBJECTID", "shape_star"]
        selected = _select_existing_noise_fields(
            available,
            candidates,
            source_path=Path("C:/tmp/noise.shp"),
            layer_name="Noise_R4_Airport",
        )
        self.assertEqual(selected, ["DB_LOW", "DB_HIGH", "OBJECTID"])
        self.assertNotIn("shape_star", selected)
        self.assertNotIn("shape_leng", selected)

    def test_select_existing_noise_fields_raises_when_no_usable_fields(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _select_existing_noise_fields

        available = ["shape_star", "shape_leng"]
        candidates = ["DB_LOW", "DB_HIGH", "GRIDCODE"]
        with self.assertRaises(NoiseIngestError) as ctx:
            _select_existing_noise_fields(
                available,
                candidates,
                source_path=Path("C:/tmp/noise.shp"),
                layer_name="Noise_R4_Airport",
            )

        message = str(ctx.exception)
        self.assertIn("No usable noise fields", message)
        self.assertIn("noise.shp", message)
        self.assertIn("Noise_R4_Airport", message)
        self.assertIn("shape_star", message)

    def test_roi_round4_airport_regression_never_selects_shape_fields(self) -> None:
        from noise_artifacts.ogr_ingest import (
            _noise_ogr_candidate_fields,
            _select_existing_noise_fields,
        )

        available = ["OBJECTID", "DB_LOW", "DB_HIGH", "DB_VALUE", "shape_star", "shape_leng"]
        candidates = _noise_ogr_candidate_fields(
            jurisdiction="roi",
            source_type="airport",
            round_number=4,
        )
        selected = _select_existing_noise_fields(
            available,
            candidates,
            source_path=Path("C:/tmp/noise.shp"),
            layer_name="Noise_R4_Airport",
        )
        self.assertEqual(selected, ["DB_LOW", "DB_HIGH", "DB_VALUE", "OBJECTID"])
        self.assertNotIn("shape_star", selected)
        self.assertNotIn("shape_leng", selected)

    def test_roi_candidate_fields_include_time_for_metric_mapping(self) -> None:
        from noise_artifacts.ogr_ingest import _noise_ogr_candidate_fields

        candidates = _noise_ogr_candidate_fields(
            jurisdiction="roi",
            source_type="road",
            round_number=4,
        )
        self.assertIn("Time", candidates)

    def test_roi_normalize_sql_filters_cleaned_geometry(self) -> None:
        from noise_artifacts.ogr_ingest import _build_roi_normalize_insert_sql

        sql = _build_roi_normalize_insert_sql(
            stage_table="_noise_raw_stage",
            map_table="noise_roi_band_map",
            metric_case_expr="CASE WHEN 1=1 THEN 'Lden' ELSE NULL END",
            report_expr="CAST(s.\"ReportPeriod\" AS text)",
            source_ref_expr=":source_ref",
            db_value_expr="CAST(s.\"DbValue\" AS text)",
            db_low_expr="CAST(s.\"Db_Low\" AS double precision)",
            db_high_expr="CAST(s.\"Db_High\" AS double precision)",
        )
        self.assertIn("g.clean_geom", sql)
        self.assertIn("g.clean_geom IS NOT NULL", sql)
        self.assertIn("NOT ST_IsEmpty(g.clean_geom)", sql)
        self.assertIn("ST_Area(g.clean_geom) > 0", sql)
        self.assertIn("CROSS JOIN LATERAL", sql)

        top_select = sql.split('FROM "_noise_raw_stage" s', 1)[0]
        self.assertIn("g.clean_geom", top_select)
        self.assertNotIn("ST_Multi(", top_select)

    def test_ni_normalize_sql_filters_cleaned_geometry(self) -> None:
        from noise_artifacts.ogr_ingest import _build_ni_normalize_insert_sql

        sql = _build_ni_normalize_insert_sql(
            stage_table="_noise_raw_stage",
            map_table="noise_ni_band_map",
            report_expr="CAST(s.\"ReportPeriod\" AS text)",
            source_ref_expr=":source_ref",
            grid_expr="CAST(s.\"GRIDCODE\" AS integer)",
        )
        self.assertIn("g.clean_geom", sql)
        self.assertIn("g.clean_geom IS NOT NULL", sql)
        self.assertIn("NOT ST_IsEmpty(g.clean_geom)", sql)
        self.assertIn("ST_Area(g.clean_geom) > 0", sql)

    def test_normalize_roi_stage_skips_rows_when_cleaned_geometry_is_not_usable(self) -> None:
        from noise_artifacts.ogr_ingest import _normalize_roi_stage

        conn = _FakeNormalizeConn()
        progress_events: list[str] = []

        with patch("noise_artifacts.ogr_ingest._table_columns", return_value=["Time", "DbValue", "Db_Low", "Db_High", "ReportPeriod", "source_fid"]):
            with patch("noise_artifacts.ogr_ingest._source_ref_expr", return_value=("CAST('x' AS text)", {})):
                with patch("noise_artifacts.ogr_ingest._stage_raw_geom_stats", return_value=(1, 0, 0)):
                    with patch("noise_artifacts.ogr_ingest._stage_clean_geom_ready_counts", return_value=(1, 0)):
                        with patch("noise.loader.normalize_noise_band", return_value=(45.0, 49.0, "45-49")):
                            inserted = _normalize_roi_stage(
                                conn,
                                stage_table="_noise_raw_stage",
                                noise_source_hash="h1",
                                round_number=4,
                                source_type="road",
                                source_dataset="Rd4-2022",
                                source_layer="Noise R4 DataDownload/Noise_R4_Road.gdb",
                                progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                            )

        self.assertEqual(inserted, 0)
        self.assertTrue(any("skipped_after_geometry_cleaning=1" in event for event in progress_events))
        insert_sql = next(sql for sql in conn.sql_texts if "INSERT INTO noise_normalized" in sql)
        self.assertIn("g.clean_geom IS NOT NULL", insert_sql)
        self.assertIn("NOT ST_IsEmpty(g.clean_geom)", insert_sql)
        self.assertIn("ST_Area(g.clean_geom) > 0", insert_sql)

    def test_normalize_roi_stage_union_sql_uses_cte_insert_without_create_table(self) -> None:
        from noise_artifacts.ogr_ingest import _normalize_roi_stage_union

        conn = _FakeUnionNormalizeConn()
        with patch("noise_artifacts.ogr_ingest._table_columns", return_value=["Time", "DbValue", "Db_Low", "Db_High", "ReportPeriod", "source_fid"]):
            with patch("noise_artifacts.ogr_ingest._source_ref_expr", return_value=("CAST('x' AS text)", {})):
                with patch("noise.loader.normalize_noise_band", return_value=(45.0, 49.0, "45-49")):
                    inserted = _normalize_roi_stage_union(
                        conn,
                        stage_tables=["_noise_raw_a_c001", "_noise_raw_a_c002"],
                        noise_source_hash="h1",
                        round_number=4,
                        source_type="road",
                        source_dataset="Rd4-2022.zip",
                        source_layer="Noise_R4_Road.gdb",
                    )
        self.assertEqual(inserted, 7)
        insert_sql = next(sql for sql in conn.sql_texts if "INSERT INTO noise_normalized" in sql)
        self.assertIn("WITH raw_union AS", insert_sql)
        self.assertIn("UNION ALL", insert_sql)
        self.assertIn("INSERT INTO noise_normalized", insert_sql)
        self.assertNotIn("CREATE TABLE", insert_sql)
        self.assertIn("clean_geom", insert_sql)
        self.assertIn("clean_geom IS NOT NULL", insert_sql)
        self.assertIn("NOT ST_IsEmpty", insert_sql)
        self.assertIn("ST_Area", insert_sql)

    def test_extract_source_archive_if_needed_supports_windows_style_member_paths(self) -> None:
        from noise_artifacts.ogr_ingest import extract_source_archive_if_needed

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            zip_path = tmp / "noise.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("Folder/Sub/layer.shp", b"dummy")
                zf.writestr("Folder/Sub/layer.dbf", b"dummy")
                zf.writestr("Folder/Sub/layer.shx", b"dummy")

            extracted = extract_source_archive_if_needed(zip_path)
            self.assertTrue((extracted / "Folder" / "Sub" / "layer.shp").exists())

    def test_run_ogr2ogr_import_streams_output(self) -> None:
        from noise_artifacts.ogr_ingest import _run_ogr2ogr_import

        progress_events: list[str] = []
        fake_proc = _FakePopenProcess(
            [
                "0...10...20\n",
                "ERROR simulated failure signal\n",
            ],
            returncode=0,
        )

        with patch("noise_artifacts.ogr_ingest.subprocess.Popen", return_value=fake_proc):
            _run_ogr2ogr_import(
                engine=self._fake_engine(),
                source_path=Path("C:/tmp/noise.shp"),
                stage_table="_noise_raw_stage",
                layer_name=None,
                selected_fields=["DB_LOW"],
                progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                timeout_seconds=None,
            )

        starting = next(msg for msg in progress_events if "starting ogr2ogr import" in msg)
        self.assertIn("password=***", starting)
        self.assertNotIn("password=pass", starting)
        self.assertTrue(any("ERROR simulated failure signal" in msg for msg in progress_events))

    def test_ogr2ogr_keyboard_interrupt_terminates_process(self) -> None:
        from noise_artifacts.ogr_ingest import _run_ogr2ogr_import

        class _Proc:
            def __init__(self):
                self.stdout = _FakeStream([])
                self.terminated = False
                self.killed = False

            def poll(self):
                return None

            def wait(self, timeout=None):
                del timeout
                return 0

            def terminate(self):
                self.terminated = True

            def kill(self):
                self.killed = True

        proc = _Proc()
        with patch("noise_artifacts.ogr_ingest.subprocess.Popen", return_value=proc):
            with patch("noise_artifacts.ogr_ingest.queue.Queue.get", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    _run_ogr2ogr_import(
                        engine=self._fake_engine(),
                        source_path=Path("C:/tmp/noise.shp"),
                        stage_table="_noise_raw_stage",
                        layer_name=None,
                        selected_fields=["DB_LOW"],
                        progress_cb=None,
                        timeout_seconds=None,
                    )
        self.assertTrue(proc.terminated)
        self.assertFalse(proc.killed)

    def test_ogr2ogr_keyboard_interrupt_kills_after_timeout(self) -> None:
        from noise_artifacts.ogr_ingest import _run_ogr2ogr_import

        class _Proc:
            def __init__(self):
                self.stdout = _FakeStream([])
                self.terminated = False
                self.killed = False

            def poll(self):
                return None

            def wait(self, timeout=None):
                if timeout is None:
                    return 0
                raise TimeoutError("simulate timeout")

            def terminate(self):
                self.terminated = True

            def kill(self):
                self.killed = True

        proc = _Proc()
        with patch("noise_artifacts.ogr_ingest.subprocess.Popen", return_value=proc):
            with patch("noise_artifacts.ogr_ingest.queue.Queue.get", side_effect=KeyboardInterrupt):
                with patch("noise_artifacts.ogr_ingest.subprocess.TimeoutExpired", TimeoutError):
                    with self.assertRaises(KeyboardInterrupt):
                        _run_ogr2ogr_import(
                            engine=self._fake_engine(),
                            source_path=Path("C:/tmp/noise.shp"),
                            stage_table="_noise_raw_stage",
                            layer_name=None,
                            selected_fields=["DB_LOW"],
                            progress_cb=None,
                            timeout_seconds=None,
                        )
        self.assertTrue(proc.terminated)
        self.assertTrue(proc.killed)

    def test_timeout_kills_proc_and_raises_noise_ingest_error(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _run_ogr2ogr_import

        class _Proc:
            def __init__(self):
                self.stdout = _FakeStream([])
                self.killed = False

            def poll(self):
                return None

            def wait(self, timeout=None):
                del timeout
                return 0

            def kill(self):
                self.killed = True

            def terminate(self):
                self.killed = True

        proc = _Proc()
        monotonic_values = iter([0.0, 2.0, 2.1, 2.2, 2.3, 2.4])

        with patch("noise_artifacts.ogr_ingest.subprocess.Popen", return_value=proc):
            with patch("noise_artifacts.ogr_ingest.time.monotonic", side_effect=lambda: next(monotonic_values)):
                with patch("noise_artifacts.ogr_ingest._terminate_active_ogr2ogr_processes", return_value=1) as mock_term:
                    with self.assertRaises(NoiseIngestError) as ctx:
                        _run_ogr2ogr_import(
                            engine=self._fake_engine(),
                            source_path=Path("C:/tmp/noise.shp"),
                            stage_table="_noise_raw_stage",
                            layer_name="Noise_R4_Road",
                            selected_fields=["DB_LOW"],
                            where_clause="fid >= 0 AND fid <= 24",
                            progress_cb=None,
                            timeout_seconds=1.0,
                            operation_context="road-chunk idx=1/3 fid=0-24",
                        )
        self.assertTrue(proc.killed)
        mock_term.assert_called_once()
        message = str(ctx.exception)
        self.assertIn("timed out", message)
        self.assertIn("source=noise.shp", message)
        self.assertIn("target=_noise_raw_stage", message)
        self.assertIn("context=road-chunk idx=1/3 fid=0-24", message)
        self.assertIn("cmd=ogr2ogr", message)

    def test_ogr2ogr_heartbeat_includes_timeout_pid_and_last_output_age(self) -> None:
        from noise_artifacts.ogr_ingest import _run_ogr2ogr_import

        class _Proc:
            def __init__(self):
                self.stdout = _FakeStream([])
                self.pid = 1234
                self._poll_calls = 0

            def poll(self):
                self._poll_calls += 1
                return None if self._poll_calls < 4 else 0

            def wait(self, timeout=None):
                del timeout
                return 0

            def terminate(self):
                return None

            def kill(self):
                return None

        proc = _Proc()
        progress_events: list[str] = []
        monotonic_values = iter([0.0, 31.0, 31.1, 31.2, 31.3, 31.4, 31.5, 31.6])
        with patch("noise_artifacts.ogr_ingest.subprocess.Popen", return_value=proc):
            with patch("noise_artifacts.ogr_ingest.time.monotonic", side_effect=lambda: next(monotonic_values)):
                with patch("noise_artifacts.ogr_ingest.queue.Queue.get", side_effect=queue.Empty):
                    _run_ogr2ogr_import(
                        engine=self._fake_engine(),
                        source_path=Path("C:/tmp/noise.shp"),
                        stage_table="_noise_raw_stage",
                        layer_name=None,
                        selected_fields=["DB_LOW"],
                        progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                        timeout_seconds=300.0,
                    )
        heartbeat = next(msg for msg in progress_events if "ogr2ogr still running:" in msg)
        self.assertIn("pid=1234", heartbeat)
        self.assertIn("timeout=", heartbeat)
        self.assertIn("last_output_age=", heartbeat)

    def test_default_ogr2ogr_timeout_is_non_none(self) -> None:
        from noise_artifacts.ogr_ingest import _ogr2ogr_timeout_seconds

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_ogr2ogr_timeout_seconds(), 300.0)

    def test_non_road_roi_import_passes_non_none_timeout(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Airport.shp",
            file_format="shp",
            source_type="airport",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).parent.mkdir(parents=True, exist_ok=True)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            engine = _FakeIngestEngine(_FakeIngestConn())
            import_calls: list[dict[str, object]] = []
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                    with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=lambda **kwargs: import_calls.append(dict(kwargs))):
                                        with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=43):
                                            with patch("noise_artifacts.ogr_ingest._normalize_roi_stage", return_value=2):
                                                with patch("noise_artifacts.ogr_ingest._prepare_stage_table", return_value=None):
                                                    ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
        self.assertEqual(len(import_calls), 1)
        self.assertIsNotNone(import_calls[0].get("timeout_seconds"))

    def test_stage_drop_committed_before_external_ogr_import(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Airport.shp",
            file_format="shp",
            source_type="airport",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            conn = _FakeIngestConn()
            engine = _FakeIngestEngine(conn)
            commit_counts_at_import: list[int] = []

            def _capture_import(**kwargs):  # noqa: ANN001
                del kwargs
                commit_counts_at_import.append(conn.commit_count)

            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                    with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=_capture_import):
                                        with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=43):
                                            with patch("noise_artifacts.ogr_ingest._normalize_roi_stage", return_value=2):
                                                with patch("noise_artifacts.ogr_ingest._prepare_stage_table", return_value=None):
                                                    ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
            self.assertTrue(commit_counts_at_import)
            self.assertGreaterEqual(commit_counts_at_import[0], 1)

    def test_stale_lock_diagnostic_query_runs_before_import(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Airport.shp",
            file_format="shp",
            source_type="airport",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            conn = _FakeIngestConn()
            engine = _FakeIngestEngine(conn)
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeri", "OBJECTID"]):
                                    with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                        with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=43):
                                            with patch("noise_artifacts.ogr_ingest._normalize_roi_stage", return_value=2):
                                                with patch("noise_artifacts.ogr_ingest._prepare_stage_table", return_value=None):
                                                    ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
            self.assertTrue(any("JOIN pg_locks" in sql for sql in conn.sql))

    def test_stage_lock_diagnostic_failure_rolls_back_and_does_not_block_drop(self) -> None:
        from noise_artifacts.ogr_ingest import _prepare_stage_table_for_external_import

        conn = _FakeIngestConn()
        conn.raise_on_lock_diag = True
        _prepare_stage_table_for_external_import(conn, stage_table="_noise_raw_abc123")
        self.assertGreaterEqual(conn.rollback_count, 1)
        self.assertTrue(any('DROP TABLE IF EXISTS "_noise_raw_abc123"' in sql for sql in conn.sql))

    def test_road_gdb_path_does_not_call_chunk_import(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")
            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                                with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=25):
                                                    with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=1):
                                                        with patch("noise_artifacts.ogr_ingest._import_one_road_chunk") as mock_chunk:
                                                            ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
        mock_chunk.assert_not_called()

    def test_road_gdb_canonical_path_does_not_use_thread_pool(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")
            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                                with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=25):
                                                    with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=1):
                                                        with patch("noise_artifacts.ogr_ingest.ThreadPoolExecutor") as mock_pool:
                                                            ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)

        mock_pool.assert_not_called()

    def test_windows_workers_gt_one_logs_warning(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )
        progress_events: list[str] = []
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")
            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                                with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=25):
                                                    with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=1):
                                                        with patch("noise_artifacts.ogr_ingest.os.name", "nt"):
                                                            with patch.dict(os.environ, {"NOISE_OGR2OGR_GDB_WORKERS": "2"}, clear=False):
                                                                ingest_noise_normalized_ogr2ogr(
                                                                    engine,
                                                                    "h1",
                                                                    tmp,
                                                                    None,
                                                                    progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                                                                )

        self.assertTrue(any("experimental" in msg for msg in progress_events))

    def test_road_gdb_executor_cancels_pending_futures_on_first_failure(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _run_road_gdb_chunks_fail_fast

        futures = [
            _ControllableFuture(exc=RuntimeError("chunk failed")),
            _ControllableFuture(value={"idx": 2, "chunk_stage_table": "_noise_raw_a_c002", "rows": 1}),
        ]
        submit_count = {"n": 0}

        class _FakeExecutor:
            def __init__(self, max_workers):
                del max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def submit(self, fn, **kwargs):  # noqa: ANN001
                del fn, kwargs
                idx = submit_count["n"]
                submit_count["n"] += 1
                return futures[idx]

            def shutdown(self, wait=True, cancel_futures=False):
                del wait, cancel_futures
                return None

        with patch("noise_artifacts.ogr_ingest.ThreadPoolExecutor", _FakeExecutor):
            with patch("noise_artifacts.ogr_ingest.wait", return_value=({futures[0]}, {futures[1]})):
                with patch("noise_artifacts.ogr_ingest._terminate_active_ogr2ogr_processes", return_value=0):
                    with patch("noise_artifacts.ogr_ingest._cleanup_road_chunk_tables_after_failure") as mock_cleanup:
                        with self.assertRaises(NoiseIngestError):
                            _run_road_gdb_chunks_fail_fast(
                                _FakeIngestConn(),
                                engine=_FakeIngestEngine(_FakeIngestConn()),
                                source_path=Path("C:/tmp/Noise_R4_Road.gdb"),
                                layer_name="Noise_R4_Road",
                                selected_fields=["Time"],
                                stage_table="_noise_raw_a",
                                id_column="fid",
                                chunk_ranges=[(0, 24), (25, 49), (50, 74)],
                                worker_count=2,
                                timeout_seconds=30,
                                noise_source_hash="h1",
                                round_number=4,
                                source_type="road",
                                source_dataset="Rd4-2022.zip",
                                source_layer="Noise_R4_Road.gdb",
                            )
        self.assertTrue(futures[1].cancelled)
        mock_cleanup.assert_called_once()

    def test_road_gdb_executor_does_not_start_more_chunks_after_failure(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _run_road_gdb_chunks_fail_fast

        futures = [
            _ControllableFuture(exc=RuntimeError("chunk failed")),
            _ControllableFuture(value={"idx": 2, "chunk_stage_table": "_noise_raw_a_c002", "rows": 1}),
            _ControllableFuture(value={"idx": 3, "chunk_stage_table": "_noise_raw_a_c003", "rows": 1}),
        ]
        submit_count = {"n": 0}

        class _FakeExecutor:
            def __init__(self, max_workers):
                del max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def submit(self, fn, **kwargs):  # noqa: ANN001
                del fn, kwargs
                idx = submit_count["n"]
                submit_count["n"] += 1
                return futures[idx]

            def shutdown(self, wait=True, cancel_futures=False):
                del wait, cancel_futures
                return None

        with patch("noise_artifacts.ogr_ingest.ThreadPoolExecutor", _FakeExecutor):
            with patch("noise_artifacts.ogr_ingest.wait", return_value=({futures[0]}, {futures[1]})):
                with patch("noise_artifacts.ogr_ingest._terminate_active_ogr2ogr_processes", return_value=0):
                    with patch("noise_artifacts.ogr_ingest._cleanup_road_chunk_tables_after_failure"):
                        with self.assertRaises(NoiseIngestError):
                            _run_road_gdb_chunks_fail_fast(
                                _FakeIngestConn(),
                                engine=_FakeIngestEngine(_FakeIngestConn()),
                                source_path=Path("C:/tmp/Noise_R4_Road.gdb"),
                                layer_name="Noise_R4_Road",
                                selected_fields=["Time"],
                                stage_table="_noise_raw_a",
                                id_column="fid",
                                chunk_ranges=[(0, 24), (25, 49), (50, 74)],
                                worker_count=2,
                                timeout_seconds=30,
                                noise_source_hash="h1",
                                round_number=4,
                                source_type="road",
                                source_dataset="Rd4-2022.zip",
                                source_layer="Noise_R4_Road.gdb",
                            )
        self.assertEqual(submit_count["n"], 2)

    def test_keyboard_interrupt_terminates_active_ogr2ogr_processes(self) -> None:
        from noise_artifacts.ogr_ingest import _run_road_gdb_chunks_fail_fast

        future = _ControllableFuture(exc=KeyboardInterrupt())

        class _FakeExecutor:
            def __init__(self, max_workers):
                del max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def submit(self, fn, **kwargs):  # noqa: ANN001
                del fn, kwargs
                return future

            def shutdown(self, wait=True, cancel_futures=False):
                del wait, cancel_futures
                return None

        with patch("noise_artifacts.ogr_ingest.ThreadPoolExecutor", _FakeExecutor):
            with patch("noise_artifacts.ogr_ingest.wait", return_value=({future}, set())):
                with patch("noise_artifacts.ogr_ingest._terminate_active_ogr2ogr_processes", return_value=1) as mock_term:
                    with patch("noise_artifacts.ogr_ingest._cleanup_road_chunk_tables_after_failure"):
                        with self.assertRaises(KeyboardInterrupt):
                            _run_road_gdb_chunks_fail_fast(
                                _FakeIngestConn(),
                                engine=_FakeIngestEngine(_FakeIngestConn()),
                                source_path=Path("C:/tmp/Noise_R4_Road.gdb"),
                                layer_name="Noise_R4_Road",
                                selected_fields=["Time"],
                                stage_table="_noise_raw_a",
                                id_column="fid",
                                chunk_ranges=[(0, 24)],
                                worker_count=1,
                                timeout_seconds=30,
                                noise_source_hash="h1",
                                round_number=4,
                                source_type="road",
                                source_dataset="Rd4-2022.zip",
                                source_layer="Noise_R4_Road.gdb",
                            )
        mock_term.assert_called_once()

    def test_discover_road_gdb_chunks_uses_feature_count_and_fid(self) -> None:
        from noise_artifacts.ogr_ingest import _discover_road_gdb_chunks

        calls = {"read_dataframe": 0}

        def _read_dataframe(*args, **kwargs):  # noqa: ANN001
            del args, kwargs
            calls["read_dataframe"] += 1
            raise AssertionError("read_dataframe must not be called for chunk discovery")

        fake_pyogrio = SimpleNamespace(
            read_info=lambda *args, **kwargs: {  # noqa: ANN001
                "features": 462,
                "fields": ["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"],
            },
            read_dataframe=_read_dataframe,
        )
        with patch.dict(
            os.environ,
            {"NOISE_OGR2OGR_GDB_CHUNK_SIZE": "25", "NOISE_OGR2OGR_FID_START": "0"},
            clear=False,
        ):
            with patch.dict("sys.modules", {"pyogrio": fake_pyogrio}):
                id_column, ranges, feature_count = _discover_road_gdb_chunks(
                    Path("C:/tmp/Noise_R4_Road.gdb"),
                    "Noise_R4_Road",
                )
        self.assertEqual(id_column, "fid")
        self.assertEqual(feature_count, 462)
        self.assertEqual(len(ranges), 19)
        self.assertEqual(ranges[0], (0, 24))
        self.assertEqual(ranges[-1], (450, 461))
        self.assertEqual(calls["read_dataframe"], 0)

    def test_discover_road_gdb_chunks_without_objectid_does_not_crash(self) -> None:
        from noise_artifacts.ogr_ingest import _discover_road_gdb_chunks

        fake_pyogrio = SimpleNamespace(
            read_info=lambda *args, **kwargs: {  # noqa: ANN001
                "features": 75,
                "fields": ["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"],
            }
        )
        with patch.dict(os.environ, {"NOISE_OGR2OGR_GDB_CHUNK_SIZE": "25"}, clear=False):
            with patch.dict("sys.modules", {"pyogrio": fake_pyogrio}):
                id_column, ranges, feature_count = _discover_road_gdb_chunks(
                    Path("C:/tmp/Noise_R4_Road.gdb"),
                    "Noise_R4_Road",
                )
        self.assertEqual(id_column, "fid")
        self.assertEqual(feature_count, 75)
        self.assertEqual(ranges, [(0, 24), (25, 49), (50, 74)])

    def test_worker_default_is_conservative(self) -> None:
        from noise_artifacts.ogr_ingest import _ogr2ogr_gdb_workers

        with patch.dict(os.environ, {}, clear=True):
            with patch("noise_artifacts.ogr_ingest.os.cpu_count", return_value=16):
                self.assertLessEqual(_ogr2ogr_gdb_workers(), 2)

    def test_windows_default_road_gdb_workers_is_one(self) -> None:
        from noise_artifacts.ogr_ingest import _ogr2ogr_gdb_workers

        with patch.dict(os.environ, {}, clear=True):
            with patch("noise_artifacts.ogr_ingest.os.name", "nt"):
                with patch("noise_artifacts.ogr_ingest.os.cpu_count", return_value=16):
                    self.assertEqual(_ogr2ogr_gdb_workers(), 1)

    def test_non_windows_default_road_gdb_workers_is_two(self) -> None:
        from noise_artifacts.ogr_ingest import _ogr2ogr_gdb_workers

        with patch.dict(os.environ, {}, clear=True):
            with patch("noise_artifacts.ogr_ingest.os.name", "posix"):
                with patch("noise_artifacts.ogr_ingest.os.cpu_count", return_value=16):
                    self.assertEqual(_ogr2ogr_gdb_workers(), 2)

    def test_disk_preflight_helper_passes_when_enough_free_space(self) -> None:
        from noise_artifacts.ogr_ingest import _assert_road_gdb_disk_preflight

        conn = _FakeIngestConn()
        with patch("noise_artifacts.ogr_ingest._free_disk_gb", return_value=50.0):
            with patch.dict(os.environ, {"NOISE_MIN_FREE_DISK_GB": "10"}, clear=False):
                _assert_road_gdb_disk_preflight(conn, progress_cb=None)

    def test_disk_preflight_checks_postgres_data_dir(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _assert_road_gdb_disk_preflight

        conn = _FakeIngestConn()

        def _fake_free(path):  # noqa: ANN001
            return 40.0 if "cache" in str(path).lower() else 5.0

        with patch("noise_artifacts.ogr_ingest._noise_gdal_cache_dir", return_value=Path("C:/cache")):
            with patch("noise_artifacts.ogr_ingest._pg_data_directory", return_value=Path("C:/pgdata")):
                with patch("noise_artifacts.ogr_ingest._nearest_existing_path", side_effect=lambda p: p):
                    with patch("noise_artifacts.ogr_ingest._free_disk_gb", side_effect=_fake_free):
                        with patch.dict(os.environ, {"NOISE_MIN_FREE_DISK_GB": "30"}, clear=False):
                            with self.assertRaises(NoiseIngestError) as ctx:
                                _assert_road_gdb_disk_preflight(conn, progress_cb=None)
        self.assertIn("PostgreSQL data directory", str(ctx.exception))

    def test_disk_preflight_checks_cache_dir(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import _assert_road_gdb_disk_preflight

        conn = _FakeIngestConn()

        def _fake_free(path):  # noqa: ANN001
            return 5.0 if "cache" in str(path).lower() else 40.0

        with patch("noise_artifacts.ogr_ingest._noise_gdal_cache_dir", return_value=Path("C:/cache")):
            with patch("noise_artifacts.ogr_ingest._pg_data_directory", return_value=Path("C:/pgdata")):
                with patch("noise_artifacts.ogr_ingest._nearest_existing_path", side_effect=lambda p: p):
                    with patch("noise_artifacts.ogr_ingest._free_disk_gb", side_effect=_fake_free):
                        with patch.dict(os.environ, {"NOISE_MIN_FREE_DISK_GB": "30"}, clear=False):
                            with self.assertRaises(NoiseIngestError) as ctx:
                                _assert_road_gdb_disk_preflight(conn, progress_cb=None)
        self.assertIn("cache path", str(ctx.exception))

    def test_import_one_road_chunk_never_uses_append(self) -> None:
        from noise_artifacts.ogr_ingest import _import_one_road_chunk

        engine = _FakeIngestEngine(_FakeIngestConn())
        with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import") as mock_import:
            with patch("noise_artifacts.ogr_ingest._stage_row_count", return_value=12):
                result = _import_one_road_chunk(
                    engine=engine,
                    source_path=Path("C:/tmp/Noise_R4_Road.gdb"),
                    layer_name="Noise_R4_Road",
                    selected_fields=["Time", "Db_Low"],
                    chunk_stage_table="_noise_raw_abcd_c001",
                    idx=1,
                    total_chunks=3,
                    chunk_low=0,
                    chunk_high=24,
                    where_clause="fid >= 0 AND fid <= 24",
                    timeout_seconds=None,
                    progress_cb=None,
                )

        self.assertEqual(int(result["rows"]), 12)
        kwargs = mock_import.call_args.kwargs
        self.assertFalse(kwargs["append"])
        self.assertEqual(kwargs["stage_table"], "_noise_raw_abcd_c001")
        self.assertIn("fid >=", kwargs["where_clause"])

    def test_road_gdb_uses_canonical_cache_and_single_pg_import(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")

            progress_events: list[str] = []
            pg_import_calls: list[dict[str, object]] = []
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=lambda **kwargs: pg_import_calls.append(dict(kwargs))):
                                                with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=75):
                                                    with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=12):
                                                        ingest_noise_normalized_ogr2ogr(
                                                            engine,
                                                            "h1",
                                                            tmp,
                                                            None,
                                                            progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                                                        )

        self.assertEqual(len(pg_import_calls), 1)
        call = pg_import_calls[0]
        self.assertEqual(call["source_path"], canonical_path)
        self.assertEqual(call["layer_name"], "road_raw")
        self.assertIsNone(call["selected_fields"])
        self.assertTrue(any("Road GDB importing GPKG -> PostgreSQL stage" in msg for msg in progress_events))

    def test_road_gdb_cache_miss_runs_single_extraction(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical-miss.gpkg"
            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path) as mock_cache:
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                                with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=12):
                                                    with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=4):
                                                        ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
        mock_cache.assert_called_once()

    def test_road_gdb_extraction_command_excludes_shape_fields(self) -> None:
        from noise_artifacts.ogr_ingest import _build_ogr2ogr_gdb_to_gpkg_command

        cmd = _build_ogr2ogr_gdb_to_gpkg_command(
            source_path=Path("C:/tmp/Noise_R4_Road.gdb"),
            layer_name="Noise_R4_Road",
            cache_gpkg_path=Path("C:/tmp/cache.gpkg"),
            selected_fields=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"],
        )
        cmd_text = " ".join(cmd)
        self.assertIn("-select Time,Db_Low,Db_High,DbValue,ReportPeriod", cmd_text)
        self.assertNotIn("Shape_Length", cmd_text)
        self.assertNotIn("Shape_Area", cmd_text)

    def test_no_direct_filegdb_fid_where_chunk_command(self) -> None:
        import inspect
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        src = inspect.getsource(ingest_noise_normalized_ogr2ogr)
        self.assertNotIn("Noise_R4_Road.gdb ... -where fid", src)

    def test_road_gdb_no_create_table_as_union_all(self) -> None:
        import inspect
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        src = inspect.getsource(ingest_noise_normalized_ogr2ogr)
        self.assertNotIn("CREATE TABLE", src)
        self.assertNotIn("_merge_road_chunk_tables", src)

    def test_road_gdb_cache_hit_skips_extraction(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")

            canonical_path = tmp / "canonical-hit.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path) as mock_cache:
                                            with patch("noise_artifacts.ogr_ingest._run_road_gdb_canonical_extraction") as mock_extract:
                                                with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import"):
                                                    with patch("noise_artifacts.ogr_ingest._imported_stage_row_count_or_fail", return_value=25):
                                                        with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_batched", return_value=2):
                                                            ingest_noise_normalized_ogr2ogr(
                                                                engine,
                                                                "h1",
                                                                tmp,
                                                                None,
                                                            )
        mock_cache.assert_called_once()
        mock_extract.assert_not_called()

    def test_road_gdb_cleanup_after_failure_by_default(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")

            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=RuntimeError("boom")):
                                                with self.assertRaises(RuntimeError):
                                                    ingest_noise_normalized_ogr2ogr(
                                                        engine,
                                                        "h1",
                                                        tmp,
                                                        None,
                                                    )
        self.assertTrue(any("DROP TABLE IF EXISTS" in sql for sql in engine._conn.sql))

    def test_road_gdb_keep_failed_tables_env(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")

            progress_events: list[str] = []
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=RuntimeError("boom")):
                                                with patch.dict(os.environ, {"NOISE_KEEP_FAILED_STAGE_TABLES": "1"}, clear=False):
                                                    with self.assertRaises(RuntimeError):
                                                        ingest_noise_normalized_ogr2ogr(
                                                            engine,
                                                            "h1",
                                                            tmp,
                                                            None,
                                                            progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
                                                        )
        self.assertTrue(any("keeping failed chunk staging tables" in msg for msg in progress_events))

    def test_failed_chunk_cleanup_drops_all_known_chunk_tables(self) -> None:
        from noise_artifacts.ogr_ingest import _cleanup_road_chunk_tables_after_failure

        conn = _FakeIngestConn()
        conn.pg_tables = ["_noise_raw_a_c003"]
        drop_calls: list[list[str]] = []

        def _fake_drop(_conn, table_names, progress_cb=None):  # noqa: ANN001
            del _conn, progress_cb
            drop_calls.append(list(table_names))
            return len(table_names)

        with patch("noise_artifacts.ogr_ingest._drop_tables_best_effort", side_effect=_fake_drop):
            with patch.dict(os.environ, {"NOISE_KEEP_FAILED_STAGE_TABLES": "0"}, clear=False):
                _cleanup_road_chunk_tables_after_failure(
                    conn,
                    stage_table="_noise_raw_a",
                    known_chunk_tables={"_noise_raw_a_c001", "_noise_raw_a_c002"},
                )
        flattened = [name for call in drop_calls for name in call]
        self.assertIn("_noise_raw_a_c001", flattened)
        self.assertIn("_noise_raw_a_c002", flattened)
        self.assertIn("_noise_raw_a_c003", flattened)

    def test_failed_chunk_cleanup_respects_noise_keep_failed_stage_tables(self) -> None:
        from noise_artifacts.ogr_ingest import _cleanup_road_chunk_tables_after_failure

        conn = _FakeIngestConn()
        with patch("noise_artifacts.ogr_ingest._drop_tables_best_effort") as mock_drop:
            with patch.dict(os.environ, {"NOISE_KEEP_FAILED_STAGE_TABLES": "1"}, clear=False):
                _cleanup_road_chunk_tables_after_failure(
                    conn,
                    stage_table="_noise_raw_a",
                    known_chunk_tables={"_noise_raw_a_c001"},
                )
        mock_drop.assert_not_called()

    def test_normalize_union_not_called_after_road_import_failure(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical.gpkg"
            canonical_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=RuntimeError("boom")):
                                                with patch("noise_artifacts.ogr_ingest._normalize_roi_stage_union", side_effect=AssertionError("must not call")) as mock_union:
                                                    with self.assertRaises(RuntimeError):
                                                        ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
        mock_union.assert_not_called()

    def test_failure_during_extraction_deletes_partial_cache(self) -> None:
        from noise_artifacts.ogr_ingest import _run_road_gdb_canonical_extraction

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source = tmp / "Noise_R4_Road.gdb"
            source.write_text("stub", encoding="utf-8")
            cache = tmp / "partial.gpkg"
            cache.write_text("partial", encoding="utf-8")
            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_command", side_effect=RuntimeError("fail")):
                with self.assertRaises(RuntimeError):
                    _run_road_gdb_canonical_extraction(
                        source_path=source,
                        layer_name="Noise_R4_Road",
                        selected_fields=["Time", "Db_Low"],
                        cache_gpkg_path=cache,
                    )
            self.assertFalse(cache.exists())

    def test_failure_during_pg_import_keeps_valid_cache(self) -> None:
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            (tmp / roi_spec.member).write_text("stub", encoding="utf-8")
            canonical_path = tmp / "canonical-valid.gpkg"
            canonical_path.write_text("ok", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._assert_road_gdb_disk_preflight", return_value=None):
                                        with patch("noise_artifacts.ogr_ingest._ensure_road_gdb_canonical_cache", return_value=canonical_path):
                                            with patch("noise_artifacts.ogr_ingest._run_ogr2ogr_import", side_effect=RuntimeError("pg fail")):
                                                with self.assertRaises(RuntimeError):
                                                    ingest_noise_normalized_ogr2ogr(engine, "h1", tmp, None)
            self.assertTrue(canonical_path.exists())

    def test_road_gdb_disk_preflight_blocks_before_import_when_low(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ogr_ingest import ingest_noise_normalized_ogr2ogr

        roi_spec = SimpleNamespace(
            zip_name="Rd4-2022.zip",
            member="Noise_R4_Road.gdb",
            file_format="gdb",
            source_type="road",
            round_number=4,
        )

        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            source_path = tmp / roi_spec.member
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("stub", encoding="utf-8")

            engine = _FakeIngestEngine(_FakeIngestConn())
            with patch("noise.loader.ROI_SOURCE_SPECS", [roi_spec]):
                with patch("noise.loader.NI_ZIP_BY_ROUND", {}):
                    with patch("noise_artifacts.ogr_ingest.extract_source_archive_if_needed", return_value=tmp):
                        with patch("noise_artifacts.ogr_ingest._available_ogr_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                            with patch("noise_artifacts.ogr_ingest._noise_ogr_candidate_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                with patch("noise_artifacts.ogr_ingest._select_existing_noise_fields", return_value=["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod"]):
                                    with patch("noise_artifacts.ogr_ingest._free_disk_gb", return_value=0.5):
                                        with patch("noise_artifacts.ogr_ingest._run_road_gdb_canonical_extraction") as mock_extract:
                                            with patch.dict(os.environ, {"NOISE_MIN_FREE_DISK_GB": "10"}, clear=False):
                                                with self.assertRaises(NoiseIngestError):
                                                    ingest_noise_normalized_ogr2ogr(
                                                        engine,
                                                        "h1",
                                                        tmp,
                                                        None,
                                                    )
        mock_extract.assert_not_called()

    def test_drop_tables_best_effort_attempts_all_tables(self) -> None:
        from noise_artifacts.ogr_ingest import _drop_tables_best_effort

        class _DropConn:
            def __init__(self):
                self.sql: list[str] = []

            def execute(self, statement, params=None):  # noqa: ANN001
                del params
                self.sql.append(str(statement))
                if "c002" in str(statement):
                    raise RuntimeError("drop fail")
                return _FakeExecuteResult()

        conn = _DropConn()
        dropped = _drop_tables_best_effort(
            conn,
            ["_noise_raw_stage_c001", "_noise_raw_stage_c002", "_noise_raw_stage_c003"],
            progress_cb=None,
        )
        self.assertEqual(dropped, 2)
        sql_blob = "\n".join(conn.sql)
        self.assertIn("DROP TABLE IF EXISTS \"_noise_raw_stage_c001\" CASCADE", sql_blob)
        self.assertIn("DROP TABLE IF EXISTS \"_noise_raw_stage_c002\" CASCADE", sql_blob)

    def test_stage_count_logged_after_import(self) -> None:
        from noise_artifacts.ogr_ingest import _imported_stage_row_count_or_fail

        progress_events: list[str] = []
        with patch("noise_artifacts.ogr_ingest._stage_row_count", return_value=462):
            rows = _imported_stage_row_count_or_fail(
                _FakeIngestConn(),
                stage_table="_noise_raw_stage",
                source_label="ROI layer Noise_R4_Road.gdb",
                elapsed_seconds=73.2,
                progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
            )
        self.assertEqual(rows, 462)
        self.assertTrue(any("rows=462" in msg for msg in progress_events))

    def test_statement_timeout_config_is_applied(self) -> None:
        from noise_artifacts.ogr_ingest import _apply_sql_timeouts

        class _Conn:
            def __init__(self):
                self.calls = []

            def execute(self, statement, params=None):  # noqa: ANN001
                self.calls.append((str(statement), dict(params or {})))
                return _FakeExecuteResult()

        conn = _Conn()
        with patch.dict(
            os.environ,
            {
                "NOISE_SQL_STATEMENT_TIMEOUT_SECONDS": "900",
                "NOISE_SQL_LOCK_TIMEOUT_SECONDS": "30",
            },
            clear=False,
        ):
            _apply_sql_timeouts(conn)
        sql_blob = "\n".join(call[0] for call in conn.calls)
        self.assertIn("SET LOCAL statement_timeout", sql_blob)
        self.assertIn("SET LOCAL lock_timeout", sql_blob)
        self.assertIn("'900s'", sql_blob)
        self.assertIn("'30s'", sql_blob)
        self.assertTrue(all(not call[1] for call in conn.calls))

    def test_road_normalization_batch_logs_progress(self) -> None:
        from noise_artifacts.ogr_ingest import _normalize_roi_stage_batched

        progress_events: list[str] = []

        class _Conn:
            def __init__(self):
                self.count_call = 0

            def execute(self, statement, params=None):  # noqa: ANN001
                sql = str(statement)
                if "SELECT MIN(source_fid), MAX(source_fid)" in sql:
                    return _FakeExecuteResult(rows=[(1, 10000)])
                if "SELECT COUNT(*)" in sql:
                    self.count_call += 1
                    return _FakeExecuteResult(rows=[(5000,)])
                return _FakeExecuteResult()

            def commit(self):
                return None

        conn = _Conn()
        with patch("noise_artifacts.ogr_ingest._normalize_roi_stage", return_value=7):
            inserted = _normalize_roi_stage_batched(
                conn,
                stage_table="_noise_raw_stage",
                noise_source_hash="h1",
                round_number=4,
                source_type="road",
                source_dataset="Rd4-2022.zip",
                source_layer="Noise_R4_Road.gdb",
                progress_cb=lambda _kind, detail, force_log: progress_events.append(str(detail)),
            )
        self.assertGreater(inserted, 0)
        self.assertTrue(any("Road normalize batch" in msg for msg in progress_events))

    def test_mode_ogr2ogr_raises_when_binary_missing(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ingest import ingest_noise_normalized

        engine = SimpleNamespace()

        with patch.dict(os.environ, {"NOISE_INGEST_MODE": "ogr2ogr"}, clear=False):
            with patch("noise_artifacts.ingest._existing_source_row_count", return_value=0):
                with patch("noise_artifacts.ogr_ingest.ogr2ogr_available", return_value=False):
                    with self.assertRaises(NoiseIngestError) as ctx:
                        ingest_noise_normalized(engine, "h1", Path("."), None)
        self.assertIn("NOISE_INGEST_MODE=ogr2ogr", str(ctx.exception))

    def test_mode_auto_falls_back_to_python_when_ogr2ogr_missing(self) -> None:
        from noise_artifacts.ingest import ingest_noise_normalized

        engine = SimpleNamespace()

        with patch.dict(os.environ, {"NOISE_INGEST_MODE": "auto"}, clear=False):
            with patch("noise_artifacts.ingest._existing_source_row_count", return_value=0):
                with patch("noise_artifacts.ogr_ingest.ogr2ogr_available", return_value=False):
                    with patch("noise_artifacts.ingest._ingest_noise_normalized_python_copy", return_value=7) as mock_py:
                        n = ingest_noise_normalized(engine, "h1", Path("."), None)
        self.assertEqual(n, 7)
        mock_py.assert_called_once()

    def test_mode_auto_prefers_ogr2ogr_when_available(self) -> None:
        from noise_artifacts.ingest import ingest_noise_normalized

        engine = SimpleNamespace()

        with patch.dict(os.environ, {"NOISE_INGEST_MODE": "auto"}, clear=False):
            with patch("noise_artifacts.ingest._existing_source_row_count", return_value=0):
                with patch("noise_artifacts.ogr_ingest.ogr2ogr_available", return_value=True):
                    with patch("noise_artifacts.ogr_ingest.ingest_noise_normalized_ogr2ogr", return_value=9) as mock_ogr:
                        n = ingest_noise_normalized(engine, "h1", Path("."), None)
        self.assertEqual(n, 9)
        mock_ogr.assert_called_once()
