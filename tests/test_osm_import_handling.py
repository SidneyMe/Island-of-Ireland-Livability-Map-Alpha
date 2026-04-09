from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest import TestCase, mock

from config import OSM_IMPORT_SCHEMA, SourceState
import db_postgis
import db_postgis.schema as db_schema
import local_osm_import


class FakeStdout:
    def __init__(
        self,
        lines: list[str],
        done_event: threading.Event,
        *,
        delays: list[float] | None = None,
    ) -> None:
        self._lines = list(lines)
        self._delays = list(delays or [0.0] * len(lines))
        self._done_event = done_event

    def __iter__(self):
        try:
            for index, line in enumerate(self._lines):
                delay = self._delays[index] if index < len(self._delays) else 0.0
                if delay:
                    time.sleep(delay)
                yield line
        finally:
            self._done_event.set()

    def close(self) -> None:
        return None


class FakeProcess:
    def __init__(
        self,
        lines: list[str],
        *,
        returncode: int = 0,
        delays: list[float] | None = None,
    ) -> None:
        self._done_event = threading.Event()
        self.returncode = returncode
        self.stdout = FakeStdout(lines, self._done_event, delays=delays)

    def poll(self) -> int | None:
        return self.returncode if self._done_event.is_set() else None

    def wait(self) -> int:
        self._done_event.wait(timeout=2.0)
        return self.returncode


class FakeManagedTable:
    def __init__(self, name: str, schema: str = OSM_IMPORT_SCHEMA) -> None:
        self.name = name
        self.schema = schema
        self.create = mock.Mock()


class MonotonicClock:
    def __init__(self, *values: float) -> None:
        self._values = list(values)
        self._last = values[-1] if values else 0.0

    def __call__(self) -> float:
        if self._values:
            self._last = self._values.pop(0)
        return self._last


def detail_messages(progress_cb: mock.Mock) -> list[str]:
    return [
        call.kwargs["detail"]
        for call in progress_cb.call_args_list
        if call.args and call.args[0] == "detail"
    ]


class LocalOsmImportTests(TestCase):
    def setUp(self) -> None:
        self.source_state = SourceState(
            extract_path=Path("osm/sample.osm.pbf"),
            extract_fingerprint="extract-fingerprint",
            importer_version="osm2pgsql 2.1.0",
            importer_config_hash="config-hash",
            import_fingerprint="import-fingerprint",
        )
        self.study_area_wgs84 = mock.sentinel.study_area_wgs84
        self.normalization_scope_hash = "scope-hash"

    def test_connection_arguments_preserve_relevant_query_settings(self) -> None:
        with mock.patch.object(
            local_osm_import,
            "database_url",
            return_value=(
                "postgresql+psycopg://user:secret@db:5432/gis"
                "?sslmode=require&sslrootcert=/ca.pem&sslcert=/client.crt&sslkey=/client.key"
            ),
        ):
            command, env = local_osm_import._connection_arguments()

        self.assertEqual(command, ["-H", "db", "-P", "5432", "-d", "gis", "-U", "user"])
        self.assertEqual(env["PGPASSWORD"], "secret")
        self.assertEqual(env["PGSSLMODE"], "require")
        self.assertEqual(env["PGSSLROOTCERT"], "/ca.pem")
        self.assertEqual(env["PGSSLCERT"], "/client.crt")
        self.assertEqual(env["PGSSLKEY"], "/client.key")
        self.assertEqual(env["PGCONNECT_TIMEOUT"], "15")

    def test_connection_arguments_requires_password_for_remote_host(self) -> None:
        with mock.patch.object(
            local_osm_import,
            "database_url",
            return_value="postgresql+psycopg://user@db:5432/gis",
        ):
            with self.assertRaisesRegex(RuntimeError, "requires a database password"):
                local_osm_import._connection_arguments()

    def test_run_osm2pgsql_import_uses_create_and_streams_output(self) -> None:
        progress = mock.Mock()
        process = FakeProcess(["reading input\n", "writing rows\n"])

        with (
            mock.patch.object(
                local_osm_import,
                "_connection_arguments",
                return_value=(
                    ["-H", "db", "-P", "5432", "-d", "gis", "-U", "user"],
                    {"PGPASSWORD": "secret", "PGSSLMODE": "require", "PGCONNECT_TIMEOUT": "15"},
                ),
            ),
            mock.patch.object(local_osm_import.subprocess, "Popen", return_value=process) as popen_mock,
        ):
            local_osm_import._run_osm2pgsql_import(self.source_state, progress_cb=progress)

        command = popen_mock.call_args.args[0]
        kwargs = popen_mock.call_args.kwargs
        env = kwargs["env"]

        self.assertIn("--create", command)
        self.assertNotIn("--append", command)
        self.assertEqual(command[command.index("--schema") + 1], OSM_IMPORT_SCHEMA)
        self.assertEqual(command[command.index("--middle-schema") + 1], OSM_IMPORT_SCHEMA)
        self.assertEqual(kwargs["stdin"], local_osm_import.subprocess.DEVNULL)
        self.assertEqual(kwargs["stdout"], local_osm_import.subprocess.PIPE)
        self.assertEqual(kwargs["stderr"], local_osm_import.subprocess.STDOUT)
        self.assertTrue(kwargs["text"])
        self.assertEqual(env["LIVABILITY_IMPORT_SCHEMA"], OSM_IMPORT_SCHEMA)
        self.assertEqual(env["LIVABILITY_IMPORT_FINGERPRINT"], self.source_state.import_fingerprint)
        self.assertEqual(env["PGSSLMODE"], "require")

        self.assertEqual(
            detail_messages(progress),
            [
                "running osm2pgsql --create",
                "osm2pgsql: reading input",
                "osm2pgsql: writing rows",
            ],
        )

    def test_run_osm2pgsql_import_raises_with_recent_output_on_failure(self) -> None:
        progress = mock.Mock()
        process = FakeProcess([f"line {index}\n" for index in range(1, 36)], returncode=12)

        with (
            mock.patch.object(
                local_osm_import,
                "_connection_arguments",
                return_value=(["-H", "db", "-P", "5432", "-d", "gis", "-U", "user"], {"PGPASSWORD": "secret"}),
            ),
            mock.patch.object(local_osm_import.subprocess, "Popen", return_value=process),
        ):
            with self.assertRaises(RuntimeError) as exc_info:
                local_osm_import._run_osm2pgsql_import(self.source_state, progress_cb=progress)

        message = str(exc_info.exception)
        self.assertIn("Exit status: 12", message)
        self.assertIn("line 35", message)
        self.assertIn("line 6", message)
        self.assertNotIn("line 5", message)

    def test_run_osm2pgsql_import_emits_heartbeat_while_waiting_for_output(self) -> None:
        progress = mock.Mock()
        process = FakeProcess(["first line\n", "second line\n"], delays=[0.0, 0.35])
        clock = MonotonicClock(0.0, 0.0, 16.0, 16.1)

        with (
            mock.patch.object(
                local_osm_import,
                "_connection_arguments",
                return_value=(["-H", "db", "-P", "5432", "-d", "gis", "-U", "user"], {"PGPASSWORD": "secret"}),
            ),
            mock.patch.object(local_osm_import.subprocess, "Popen", return_value=process),
            mock.patch.object(local_osm_import.time, "monotonic", side_effect=clock),
        ):
            local_osm_import._run_osm2pgsql_import(self.source_state, progress_cb=progress)

        self.assertEqual(
            detail_messages(progress).count("osm2pgsql still running; waiting for new output..."),
            1,
        )

    def test_ensure_local_osm_import_first_run_is_raw_only(self) -> None:
        tracker = mock.Mock()

        with (
            mock.patch.object(local_osm_import, "import_payload_ready", return_value=False),
            mock.patch.object(local_osm_import, "raw_import_ready", return_value=False),
            mock.patch.object(local_osm_import, "osm2pgsql_properties_exists", return_value=False),
            mock.patch.object(
                local_osm_import,
                "drop_importer_owned_raw_tables",
                side_effect=lambda engine: tracker.drop_tables(),
            ),
            mock.patch.object(
                local_osm_import,
                "_run_osm2pgsql_import",
                side_effect=lambda source_state, progress_cb=None: tracker.run_import(),
            ),
            mock.patch.object(
                local_osm_import,
                "ensure_managed_raw_support_tables",
                side_effect=lambda engine: tracker.ensure_support_tables(),
            ),
            mock.patch.object(
                local_osm_import,
                "begin_import_manifest",
                side_effect=lambda *args, **kwargs: tracker.begin_manifest(),
            ),
            mock.patch.object(
                local_osm_import,
                "complete_import_manifest",
                side_effect=lambda *args, **kwargs: tracker.complete_manifest(),
            ),
            mock.patch.object(local_osm_import, "clear_normalized_import_artifacts") as clear_mock,
        ):
            local_osm_import.ensure_local_osm_import(
                mock.sentinel.engine,
                self.source_state,
                study_area_wgs84=self.study_area_wgs84,
                normalization_scope_hash=self.normalization_scope_hash,
                progress_cb=tracker,
            )

        self.assertEqual(
            tracker.mock_calls[:5],
            [
                mock.call("detail", detail=mock.ANY, force_log=True),
                mock.call.drop_tables(),
                mock.call.run_import(),
                mock.call.ensure_support_tables(),
                mock.call.begin_manifest(),
            ],
        )
        self.assertEqual(tracker.mock_calls[5], mock.call.complete_manifest())
        clear_mock.assert_not_called()
        self.assertFalse(hasattr(local_osm_import, "normalize_imported_osm"))

    def test_ensure_local_osm_import_reuses_ready_payload(self) -> None:
        with (
            mock.patch.object(local_osm_import, "import_payload_ready", return_value=True),
            mock.patch.object(local_osm_import, "_run_osm2pgsql_import") as run_import_mock,
            mock.patch.object(local_osm_import, "begin_import_manifest") as begin_mock,
        ):
            local_osm_import.ensure_local_osm_import(
                mock.sentinel.engine,
                self.source_state,
                study_area_wgs84=self.study_area_wgs84,
                normalization_scope_hash=self.normalization_scope_hash,
            )

        run_import_mock.assert_not_called()
        begin_mock.assert_not_called()

    def test_ensure_local_osm_import_reuses_raw_payload_without_rerunning_osm2pgsql(self) -> None:
        tracker = mock.Mock()

        with (
            mock.patch.object(local_osm_import, "import_payload_ready", return_value=False),
            mock.patch.object(local_osm_import, "raw_import_ready", return_value=True),
            mock.patch.object(local_osm_import, "osm2pgsql_properties_exists", return_value=True),
            mock.patch.object(local_osm_import, "drop_importer_owned_raw_tables") as drop_mock,
            mock.patch.object(local_osm_import, "_run_osm2pgsql_import") as run_import_mock,
            mock.patch.object(
                local_osm_import,
                "ensure_managed_raw_support_tables",
                side_effect=lambda engine: tracker.ensure_support_tables(),
            ),
            mock.patch.object(
                local_osm_import,
                "begin_import_manifest",
                side_effect=lambda *args, **kwargs: tracker.begin_manifest(),
            ),
            mock.patch.object(
                local_osm_import,
                "complete_import_manifest",
                side_effect=lambda *args, **kwargs: tracker.complete_manifest(),
            ),
        ):
            local_osm_import.ensure_local_osm_import(
                mock.sentinel.engine,
                self.source_state,
                study_area_wgs84=self.study_area_wgs84,
                normalization_scope_hash=self.normalization_scope_hash,
                progress_cb=tracker,
            )

        drop_mock.assert_not_called()
        run_import_mock.assert_not_called()
        self.assertIn(mock.call.ensure_support_tables(), tracker.mock_calls)
        self.assertIn(mock.call.begin_manifest(), tracker.mock_calls)
        self.assertIn(mock.call.complete_manifest(), tracker.mock_calls)

    def test_ensure_local_osm_import_clears_rows_if_manifest_completion_fails(self) -> None:
        with (
            mock.patch.object(local_osm_import, "import_payload_ready", return_value=False),
            mock.patch.object(local_osm_import, "raw_import_ready", return_value=True),
            mock.patch.object(local_osm_import, "osm2pgsql_properties_exists", return_value=True),
            mock.patch.object(local_osm_import, "drop_importer_owned_raw_tables"),
            mock.patch.object(local_osm_import, "_run_osm2pgsql_import"),
            mock.patch.object(local_osm_import, "ensure_managed_raw_support_tables"),
            mock.patch.object(local_osm_import, "begin_import_manifest"),
            mock.patch.object(local_osm_import, "complete_import_manifest", side_effect=RuntimeError("boom")),
            mock.patch.object(local_osm_import, "clear_normalized_import_artifacts") as clear_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                local_osm_import.ensure_local_osm_import(
                    mock.sentinel.engine,
                    self.source_state,
                    study_area_wgs84=self.study_area_wgs84,
                    normalization_scope_hash=self.normalization_scope_hash,
                )

        clear_mock.assert_called_once_with(mock.sentinel.engine, self.source_state.import_fingerprint)


class DbPostgisImportStateTests(TestCase):
    def test_import_payload_ready_returns_false_without_osm2pgsql_metadata(self) -> None:
        with (
            mock.patch.object(db_postgis, "osm2pgsql_properties_exists", return_value=False),
            mock.patch.object(db_postgis, "table_exists") as table_exists_mock,
        ):
            ready = db_postgis.import_payload_ready(mock.sentinel.engine, "import-fingerprint", "scope-hash")

        self.assertFalse(ready)
        table_exists_mock.assert_not_called()

    def test_import_payload_ready_returns_true_with_features(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value

        with (
            mock.patch.object(db_postgis, "osm2pgsql_properties_exists", return_value=True),
            mock.patch.object(db_postgis, "table_exists", return_value=True),
            mock.patch.object(db_schema, "_count_import_rows", return_value=3),
        ):
            ready = db_postgis.import_payload_ready(engine, "import-fingerprint", "scope-hash")

        self.assertTrue(ready)
        connection.execute.assert_not_called()

    def test_clear_normalized_import_artifacts_deletes_features_and_manifest_rows_only(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        db_postgis.clear_normalized_import_artifacts(engine, "import-fingerprint")

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined = "\n".join(statements)
        self.assertIn("osm_raw.features", joined)
        self.assertIn("osm_raw.import_manifest", joined)
        self.assertNotIn("network_ways", joined)
        self.assertNotIn("walk_edges", joined)
        self.assertNotIn("drive_edges", joined)

    def test_drop_importer_owned_raw_tables_only_targets_features(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        db_postgis.drop_importer_owned_raw_tables(engine)

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        self.assertEqual(
            statements,
            [f'DROP TABLE IF EXISTS "{OSM_IMPORT_SCHEMA}"."features"'],
        )

    def test_ensure_managed_raw_support_tables_creates_import_manifest_support_only(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value
        managed_tables = (FakeManagedTable("import_manifest"),)

        with mock.patch.object(db_schema, "MANAGED_RAW_SUPPORT_TABLES", managed_tables):
            db_postgis.ensure_managed_raw_support_tables(engine)

        managed_tables[0].create.assert_called_once_with(connection, checkfirst=True)
        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        self.assertEqual(len(statements), 1)
        self.assertIn("ALTER TABLE osm_raw.import_manifest", statements[0])


class TextCleanupTests(TestCase):
    def test_lua_no_network_ways(self) -> None:
        text = Path("osm2pgsql_livability.lua").read_text(encoding="utf-8")
        self.assertNotIn("network_ways", text)
        self.assertNotIn("object.tags.highway then", text)

    def test_schema_no_drive_or_network_tables(self) -> None:
        text = Path("schema.sql").read_text(encoding="utf-8")
        for removed_name in (
            "grid_drive",
            "hotspots",
            "network_ways",
            "walk_edges",
            "drive_edges",
            "walk_nodes",
            "drive_nodes",
        ):
            self.assertNotIn(removed_name, text)
