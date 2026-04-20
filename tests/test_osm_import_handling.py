from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

from config import OSM_IMPORT_SCHEMA, SourceState, TRANSIT_DERIVED_SCHEMA, TRANSIT_RAW_SCHEMA
import db_postgis
import db_postgis.reads as db_reads
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


def _db_ready_connection(*, postgis_exists: bool = True):
    connection = mock.MagicMock()

    def _execute(statement, *args, **kwargs):
        del args, kwargs
        sql = str(statement)
        result = mock.Mock()
        if "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')" in sql:
            result.scalar_one.return_value = postgis_exists
        return result

    connection.execute.side_effect = _execute
    return connection


def _managed_schema_inspector(
    *,
    include_alembic_version: bool = False,
    missing_tables: set[tuple[str, str]] | None = None,
    missing_columns: dict[tuple[str, str], set[str]] | None = None,
    missing_indexes: set[str] | None = None,
):
    missing_tables = missing_tables or set()
    missing_columns = missing_columns or {}
    missing_indexes = missing_indexes or set()

    public_tables = []
    raw_tables = []
    transit_raw_tables = []
    transit_derived_tables = []
    for table in db_schema._MANAGED_TABLES:
        schema_name = str(table.schema or "public")
        table_name = str(table.name)
        if (schema_name, table_name) in missing_tables:
            continue
        if schema_name == "public":
            public_tables.append(table_name)
        elif schema_name == OSM_IMPORT_SCHEMA:
            raw_tables.append(table_name)
        elif schema_name == TRANSIT_RAW_SCHEMA:
            transit_raw_tables.append(table_name)
        elif schema_name == TRANSIT_DERIVED_SCHEMA:
            transit_derived_tables.append(table_name)
    if include_alembic_version:
        public_tables.append("alembic_version")

    inspector = mock.Mock()

    def _get_table_names(schema=None):
        if schema is None:
            return list(public_tables)
        if schema == OSM_IMPORT_SCHEMA:
            return list(raw_tables)
        if schema == TRANSIT_RAW_SCHEMA:
            return list(transit_raw_tables)
        if schema == TRANSIT_DERIVED_SCHEMA:
            return list(transit_derived_tables)
        return []

    def _get_columns(table_name, schema=None):
        schema_name = str(schema or "public")
        table = next(
            table
            for table in db_schema._MANAGED_TABLES
            if str(table.name) == str(table_name)
            and str(table.schema or "public") == schema_name
        )
        omitted = missing_columns.get((schema_name, str(table_name)), set())
        return [
            {"name": column.name}
            for column in table.columns
            if column.name not in omitted
        ]

    def _get_indexes(table_name, schema=None):
        schema_name = str(schema or "public")
        names = [
            index_spec.index_name
            for index_spec in db_schema._MANAGED_INDEX_SPECS
            if index_spec.schema == schema_name
            and index_spec.table_name == str(table_name)
            and index_spec.index_name not in missing_indexes
        ]
        return [{"name": name} for name in names]

    inspector.get_table_names.side_effect = _get_table_names
    inspector.get_columns.side_effect = _get_columns
    inspector.get_indexes.side_effect = _get_indexes
    return inspector


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
        inspector = _managed_schema_inspector(include_alembic_version=True)

        with mock.patch.object(db_schema, "inspect", return_value=inspector):
            db_postgis.ensure_managed_raw_support_tables(engine)

        engine.begin.assert_not_called()

    def test_ensure_database_ready_auto_upgrades_empty_managed_schema(self) -> None:
        engine = mock.MagicMock()
        begin_connection = _db_ready_connection()
        connect_connection = mock.MagicMock()
        engine.begin.return_value.__enter__.return_value = begin_connection
        engine.connect.return_value.__enter__.return_value = connect_connection
        empty_managed_tables = {
            (str(table.schema or "public"), str(table.name))
            for table in db_schema._MANAGED_TABLES
        }
        legacy_inspector = _managed_schema_inspector(
            missing_tables=empty_managed_tables,
        )
        final_inspector = _managed_schema_inspector(include_alembic_version=True)

        with (
            mock.patch.object(
                db_schema,
                "inspect",
                side_effect=[legacy_inspector, final_inspector],
            ),
            mock.patch.object(db_schema.command, "stamp") as stamp_mock,
            mock.patch.object(db_schema.command, "upgrade") as upgrade_mock,
        ):
            db_postgis.ensure_database_ready(engine)

        stamp_mock.assert_not_called()
        upgrade_mock.assert_called_once()

    def test_ensure_database_ready_stamps_supported_legacy_schema_before_upgrade(self) -> None:
        engine = mock.MagicMock()
        begin_connection = _db_ready_connection()
        connect_connection = mock.MagicMock()
        engine.begin.return_value.__enter__.return_value = begin_connection
        engine.connect.return_value.__enter__.return_value = connect_connection
        legacy_inspector = _managed_schema_inspector()
        final_inspector = _managed_schema_inspector(include_alembic_version=True)

        with (
            mock.patch.object(
                db_schema,
                "inspect",
                side_effect=[legacy_inspector, final_inspector],
            ),
            mock.patch.object(db_schema.command, "stamp") as stamp_mock,
            mock.patch.object(db_schema.command, "upgrade") as upgrade_mock,
        ):
            db_postgis.ensure_database_ready(engine)

        stamp_mock.assert_called_once()
        self.assertEqual(
            stamp_mock.call_args.args[1],
            db_schema.ALEMBIC_INITIAL_REVISION,
        )
        upgrade_mock.assert_called_once()

    def test_ensure_database_ready_rejects_legacy_schema_drift_without_alembic_version(self) -> None:
        engine = mock.MagicMock()
        begin_connection = _db_ready_connection()
        connect_connection = mock.MagicMock()
        engine.begin.return_value.__enter__.return_value = begin_connection
        engine.connect.return_value.__enter__.return_value = connect_connection
        drifted_inspector = _managed_schema_inspector(
            missing_columns={("public", "grid_walk"): {"effective_area_ratio"}}
        )

        with (
            mock.patch.object(db_schema, "inspect", return_value=drifted_inspector),
            mock.patch.object(db_schema.command, "stamp") as stamp_mock,
            mock.patch.object(db_schema.command, "upgrade") as upgrade_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "Unsupported drift/manual intervention required"):
                db_postgis.ensure_database_ready(engine)

        stamp_mock.assert_not_called()
        upgrade_mock.assert_not_called()


class DbReadTests(TestCase):
    def test_load_source_amenity_rows_preserves_polygon_park_area(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execution_options.return_value = connection
        connection.execute.return_value.yield_per.return_value.mappings.return_value = [
            {
                "category": "parks",
                "name": "Town Park",
                "tags_json": {"name": "Town Park", "operator": "Council"},
                "osm_type": "way",
                "osm_id": 123,
                "point_geom": "park-point",
                "park_area_m2": 125_000.0,
                "footprint_area_m2": 125_000.0,
            },
            {
                "category": "parks",
                "name": "Playground",
                "tags_json": {"name": "Playground"},
                "osm_type": "node",
                "osm_id": 456,
                "point_geom": "playground-point",
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            },
        ]
        root = SimpleNamespace(
            from_shape=mock.Mock(return_value="study-area"),
            to_shape=lambda geom: geom,
        )

        with mock.patch.object(db_reads, "root_module", return_value=root):
            rows = db_postgis.load_source_amenity_rows(
                engine,
                "import-fingerprint",
                mock.sentinel.study_area_wgs84,
            )

        self.assertEqual(rows[0]["source_ref"], "way/123")
        self.assertEqual(rows[0]["source"], "osm_local_pbf")
        self.assertEqual(rows[0]["name"], "Town Park")
        self.assertEqual(rows[0]["tags_json"], {"name": "Town Park", "operator": "Council"})
        self.assertEqual(rows[0]["geom"], "park-point")
        self.assertEqual(rows[0]["park_area_m2"], 125_000.0)
        self.assertEqual(rows[0]["footprint_area_m2"], 125_000.0)
        self.assertEqual(rows[1]["source_ref"], "node/456")
        self.assertEqual(rows[1]["source"], "osm_local_pbf")
        self.assertEqual(rows[1]["name"], "Playground")
        self.assertEqual(rows[1]["tags_json"], {"name": "Playground"})
        self.assertEqual(rows[1]["park_area_m2"], 0.0)
        self.assertEqual(rows[1]["footprint_area_m2"], 0.0)

    def test_load_source_amenity_rows_excludes_non_operational_pois_and_reports_count(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execution_options.return_value = connection
        connection.execute.return_value.yield_per.return_value.mappings.return_value = [
            {
                "category": "shops",
                "name": None,
                "tags_json": {"shop": "vacant", "addr:street": "West Street"},
                "osm_type": "node",
                "osm_id": 101,
                "point_geom": "vacant-point",
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            },
            {
                "category": "shops",
                "name": "Penneys",
                "tags_json": {"name": "Penneys", "shop": "clothes", "brand": "Primark"},
                "osm_type": "node",
                "osm_id": 102,
                "point_geom": "penneys-point",
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            },
            {
                "category": "shops",
                "name": "Old Shop",
                "tags_json": {"name": "Old Shop", "disused:shop": "clothes"},
                "osm_type": "node",
                "osm_id": 103,
                "point_geom": "old-shop-point",
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            },
        ]
        root = SimpleNamespace(
            from_shape=mock.Mock(return_value="study-area"),
            to_shape=lambda geom: geom,
        )
        stats_out: dict[str, object] = {}

        with mock.patch.object(db_reads, "root_module", return_value=root):
            rows = db_postgis.load_source_amenity_rows(
                engine,
                "import-fingerprint",
                mock.sentinel.study_area_wgs84,
                stats_out=stats_out,
            )

        self.assertEqual(
            rows,
            [
                {
                    "category": "shops",
                    "source": "osm_local_pbf",
                    "source_ref": "node/102",
                    "name": "Penneys",
                    "tags_json": {"name": "Penneys", "shop": "clothes", "brand": "Primark"},
                    "geom": "penneys-point",
                    "park_area_m2": 0.0,
                    "footprint_area_m2": 0.0,
                }
            ],
        )
        self.assertEqual(stats_out["excluded_non_operational_osm_rows"], 2)

    def test_load_transport_reality_rows_for_scoring_keeps_gtfs_direct_active_rows(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.all.return_value = [
            {
                "source_ref": "gtfs/nta/S1",
                "geom": "gtfs-point",
            }
        ]
        root = SimpleNamespace(
            from_shape=mock.Mock(return_value="study-area"),
            to_shape=lambda geom: geom,
        )

        with mock.patch.object(db_reads, "root_module", return_value=root):
            rows = db_postgis.load_transport_reality_rows_for_scoring(
                engine,
                "reality-123",
                mock.sentinel.study_area_wgs84,
            )

        self.assertEqual(
            rows,
            [
                {
                    "category": "transport",
                    "source": "gtfs_direct",
                    "source_ref": "gtfs/nta/S1",
                    "name": None,
                    "conflict_class": "gtfs_direct",
                    "geom": "gtfs-point",
                    "park_area_m2": 0.0,
                }
            ],
        )

    def test_load_walk_rows_includes_effective_area_fields(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.all.return_value = [
            {
                "resolution_m": 5000,
                "cell_id": "cell-1",
                "centre_geom": "centre-geom",
                "cell_geom": "cell-geom",
                "effective_area_m2": 12_500_000.0,
                "effective_area_ratio": 0.5,
                "counts_json": {"shops": 2},
                "cluster_counts_json": {"shops": 1},
                "effective_units_json": {"shops": 1.5},
                "scores_json": {"shops": 10.0},
                "total_score": 10.0,
            }
        ]

        with mock.patch.object(db_postgis, "to_shape", side_effect=lambda geom: geom):
            rows = db_postgis.load_walk_rows(engine, "build-key")

        self.assertEqual(rows[0]["effective_area_m2"], 12_500_000.0)
        self.assertEqual(rows[0]["effective_area_ratio"], 0.5)
        self.assertEqual(rows[0]["cell_geom"], "cell-geom")
        self.assertEqual(rows[0]["cluster_counts_json"], {"shops": 1})
        self.assertEqual(rows[0]["effective_units_json"], {"shops": 1.5})

    def test_load_walk_rows_for_resolutions_includes_effective_area_fields(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.all.return_value = [
            {
                "resolution_m": 10000,
                "cell_id": "cell-2",
                "centre_geom": "centre-geom-2",
                "cell_geom": "cell-geom-2",
                "effective_area_m2": 80_000_000.0,
                "effective_area_ratio": 0.8,
                "counts_json": {"parks": 1},
                "cluster_counts_json": {"parks": 1},
                "effective_units_json": {"parks": 0.75},
                "scores_json": {"parks": 12.5},
                "total_score": 12.5,
            }
        ]

        with mock.patch.object(db_postgis, "to_shape", side_effect=lambda geom: geom):
            rows = db_postgis.load_walk_rows_for_resolutions(engine, "build-key", [10000])

        self.assertEqual(rows[0]["effective_area_m2"], 80_000_000.0)
        self.assertEqual(rows[0]["effective_area_ratio"], 0.8)
        self.assertEqual(rows[0]["centre_geom"], "centre-geom-2")
        self.assertEqual(rows[0]["cluster_counts_json"], {"parks": 1})
        self.assertEqual(rows[0]["effective_units_json"], {"parks": 0.75})

    def test_load_point_scores_for_build_short_circuits_empty_points(self) -> None:
        engine = mock.MagicMock()

        rows = db_postgis.load_point_scores_for_build(engine, "build-key", [])

        self.assertEqual(rows, [])
        engine.connect.assert_not_called()

    def test_load_point_scores_for_build_uses_st_covers_and_smallest_resolution(self) -> None:
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.all.return_value = [
            {
                "point_id": "pt-1",
                "lat": 53.4,
                "lon": -6.2,
                "resolution_m": 5000,
                "total_score": 61.5,
                "scores_json": {"shops": 10.0},
                "counts_json": {"shops": 2},
            }
        ]

        rows = db_postgis.load_point_scores_for_build(
            engine,
            "build-key",
            [
                {"id": "pt-1", "lat": 53.4, "lon": -6.2},
                {"id": "pt-2", "lat": 53.5, "lon": -6.3},
            ],
        )

        statement = str(connection.execute.call_args.args[0])
        params = connection.execute.call_args.args[1]

        self.assertIn("ST_Covers", statement)
        self.assertIn("ORDER BY g.resolution_m ASC", statement)
        self.assertEqual(params["build_key"], "build-key")
        self.assertEqual(params["point_id_0"], "pt-1")
        self.assertEqual(params["lat_0"], 53.4)
        self.assertEqual(params["lon_0"], -6.2)
        self.assertEqual(rows[0]["resolution_m"], 5000)
        self.assertEqual(rows[0]["total_score"], 61.5)
        self.assertEqual(rows[0]["scores_json"], {"shops": 10.0})
        self.assertEqual(rows[0]["counts_json"], {"shops": 2})


class TextCleanupTests(TestCase):
    def test_lua_no_network_ways(self) -> None:
        text = Path("osm2pgsql_livability.lua").read_text(encoding="utf-8")
        self.assertNotIn("network_ways", text)
        self.assertNotIn("object.tags.highway then", text)

    def test_lua_does_not_treat_garden_as_park(self) -> None:
        text = Path("osm2pgsql_livability.lua").read_text(encoding="utf-8")
        self.assertIn("local park_values = {", text)
        self.assertNotIn("garden = true", text)

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
