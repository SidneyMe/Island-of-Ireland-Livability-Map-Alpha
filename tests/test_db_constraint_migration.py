from __future__ import annotations

import importlib.util
import io
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import sys
from unittest import TestCase, mock


def _load_migration_module(module_name: str = "candidate_key_constraints_under_test"):
    module_path = (
        Path(__file__).resolve().parents[1]
        / "db_postgis"
        / "migrations"
        / "versions"
        / "20260617_000021_add_candidate_key_constraints.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_migration_module()


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
        self.executed.append(str(statement))
        if not self._results:
            raise AssertionError("unexpected execute call")
        return self._results.pop(0)


class _FakeInspector:
    def __init__(
        self,
        *,
        table_names_by_schema: dict[str, list[str]],
        pk_constraints: dict[tuple[str, str], dict[str, object]] | None = None,
        unique_constraints: dict[tuple[str, str], list[dict[str, object]]] | None = None,
        foreign_keys: dict[tuple[str, str], list[dict[str, object]]] | None = None,
        indexes: dict[tuple[str, str], list[dict[str, object]]] | None = None,
    ) -> None:
        self.table_names_by_schema = table_names_by_schema
        self.pk_constraints = pk_constraints or {}
        self.unique_constraints = unique_constraints or {}
        self.foreign_keys = foreign_keys or {}
        self.indexes = indexes or {}

    def get_table_names(self, schema=None):
        return list(self.table_names_by_schema.get(schema or "public", []))

    def get_pk_constraint(self, table_name, schema=None):
        return dict(self.pk_constraints.get((schema or "public", table_name), {"constrained_columns": [], "name": None}))

    def get_unique_constraints(self, table_name, schema=None):
        return list(self.unique_constraints.get((schema or "public", table_name), []))

    def get_foreign_keys(self, table_name, schema=None):
        return list(self.foreign_keys.get((schema or "public", table_name), []))

    def get_indexes(self, table_name, schema=None):
        return list(self.indexes.get((schema or "public", table_name), []))


class PreflightHelperTests(TestCase):
    def test_migration_module_loads_via_file_loader(self) -> None:
        loaded_module = _load_migration_module("candidate_key_constraints_loader_regression")
        self.assertEqual(loaded_module.revision, module.revision)

    def test_check_integrity_reports_duplicate_and_null_issues(self) -> None:
        spec = module.PRIMARY_KEY_SPECS[0]
        connection = _FakeConnection(
            [
                _FakeResult(scalar_value=2),
                _FakeResult(
                    rows=[
                        {
                            "build_key_null_count": 0,
                            "resolution_m_null_count": 1,
                            "cell_id_null_count": 2,
                        }
                    ]
                ),
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

        result = module._check_integrity(connection, spec, sample_limit=1)
        message = module._format_integrity_failure(spec, result)

        self.assertEqual(result.duplicate_group_count, 2)
        self.assertEqual(
            [(issue[0], issue[1]) for issue in result.null_issues],
            [("resolution_m", 1), ("cell_id", 2)],
        )
        self.assertIn("duplicate groups found: 2", message)
        self.assertIn("NULL key values found", message)
        self.assertIn("sample duplicate keys", message)
        self.assertIn("sample NULL rows", message)


class UpgradeTests(TestCase):
    def test_upgrade_applies_expected_constraints_for_clean_schema(self) -> None:
        inspector = _FakeInspector(
            table_names_by_schema={
                "public": ["grid_walk", "service_deserts", "build_manifest"],
                "transit_derived": [
                    "service_classification",
                    "gtfs_stop_service_summary",
                    "gtfs_stop_reality",
                    "service_desert_cells",
                    "reality_manifest",
                ],
            },
            pk_constraints={
                ("public", "build_manifest"): {
                    "constrained_columns": ["build_key"],
                    "name": "build_manifest_pkey",
                }
            },
            indexes={
                ("public", "grid_walk"): [{"name": "grid_walk_build_resolution_cell_idx"}],
                ("public", "service_deserts"): [{"name": "service_deserts_build_resolution_cell_idx"}],
            },
        )

        stdout = io.StringIO()
        with (
            mock.patch.object(module, "_ensure_clean_candidate_keys"),
            mock.patch.object(module.op, "get_bind", return_value=mock.sentinel.bind),
            mock.patch.object(module.sa, "inspect", return_value=inspector),
            mock.patch.object(module.op, "execute") as execute_mock,
            mock.patch.object(module.op, "create_primary_key") as create_pk_mock,
            mock.patch.object(module.op, "create_foreign_key") as create_fk_mock,
            redirect_stdout(stdout),
        ):
            module.upgrade()

        execute_sql = "\n".join(str(call.args[0]) for call in execute_mock.call_args_list)
        self.assertIn(
            'ALTER TABLE "public"."grid_walk" ADD CONSTRAINT "grid_walk_build_resolution_cell_pkey" PRIMARY KEY USING INDEX "grid_walk_build_resolution_cell_idx"',
            execute_sql,
        )
        self.assertIn(
            'ALTER TABLE "public"."service_deserts" ADD CONSTRAINT "service_deserts_build_resolution_cell_pkey" PRIMARY KEY USING INDEX "service_deserts_build_resolution_cell_idx"',
            execute_sql,
        )
        create_pk_mock.assert_has_calls(
            [
                mock.call(
                    "transit_derived_service_classification_pkey",
                    "service_classification",
                    ["reality_fingerprint", "feed_id", "service_id"],
                    schema="transit_derived",
                ),
                mock.call(
                    "transit_derived_gtfs_stop_service_summary_pkey",
                    "gtfs_stop_service_summary",
                    ["reality_fingerprint", "feed_id", "stop_id"],
                    schema="transit_derived",
                ),
                mock.call(
                    "transit_derived_gtfs_stop_reality_pkey",
                    "gtfs_stop_reality",
                    ["reality_fingerprint", "source_ref"],
                    schema="transit_derived",
                ),
                mock.call(
                    "transit_derived_service_desert_cells_pkey",
                    "service_desert_cells",
                    ["build_key", "resolution_m", "cell_id"],
                    schema="transit_derived",
                ),
            ]
        )
        create_fk_mock.assert_has_calls(
            [
                mock.call(
                    "grid_walk_build_key_fkey",
                    source_table="grid_walk",
                    referent_table="build_manifest",
                    local_cols=["build_key"],
                    remote_cols=["build_key"],
                    source_schema="public",
                    referent_schema="public",
                    ondelete="CASCADE",
                ),
                mock.call(
                    "service_deserts_build_key_fkey",
                    source_table="service_deserts",
                    referent_table="build_manifest",
                    local_cols=["build_key"],
                    remote_cols=["build_key"],
                    source_schema="public",
                    referent_schema="public",
                    ondelete="CASCADE",
                ),
                mock.call(
                    "transit_derived_service_desert_cells_build_key_fkey",
                    source_table="service_desert_cells",
                    referent_table="build_manifest",
                    local_cols=["build_key"],
                    remote_cols=["build_key"],
                    source_schema="transit_derived",
                    referent_schema="public",
                    ondelete="CASCADE",
                ),
            ]
        )

    def test_upgrade_skips_missing_transit_tables(self) -> None:
        inspector = _FakeInspector(
            table_names_by_schema={
                "public": ["grid_walk", "service_deserts", "build_manifest"],
                "transit_derived": [],
            },
            pk_constraints={
                ("public", "build_manifest"): {
                    "constrained_columns": ["build_key"],
                    "name": "build_manifest_pkey",
                }
            },
            indexes={
                ("public", "grid_walk"): [{"name": "grid_walk_build_resolution_cell_idx"}],
                ("public", "service_deserts"): [{"name": "service_deserts_build_resolution_cell_idx"}],
            },
        )

        with (
            mock.patch.object(module, "_ensure_clean_candidate_keys"),
            mock.patch.object(module.op, "get_bind", return_value=mock.sentinel.bind),
            mock.patch.object(module.sa, "inspect", return_value=inspector),
            mock.patch.object(module.op, "execute") as execute_mock,
            mock.patch.object(module.op, "create_primary_key") as create_pk_mock,
            mock.patch.object(module.op, "create_foreign_key") as create_fk_mock,
        ):
            module.upgrade()

        self.assertEqual(create_pk_mock.call_count, 0)
        self.assertEqual(create_fk_mock.call_count, 2)
        self.assertEqual(execute_mock.call_count, 2)

    def test_upgrade_fails_before_making_changes_when_preflight_finds_dirty_data(self) -> None:
        dirty_error = RuntimeError("dirty data found")
        inspector = _FakeInspector(table_names_by_schema={"public": [], "transit_derived": []})

        with (
            mock.patch.object(module, "_ensure_clean_candidate_keys", side_effect=dirty_error),
            mock.patch.object(module.op, "get_bind", return_value=mock.sentinel.bind),
            mock.patch.object(module.sa, "inspect", return_value=inspector),
            mock.patch.object(module.op, "execute") as execute_mock,
            mock.patch.object(module.op, "create_primary_key") as create_pk_mock,
            mock.patch.object(module.op, "create_foreign_key") as create_fk_mock,
        ):
            with self.assertRaises(RuntimeError):
                module.upgrade()

        execute_mock.assert_not_called()
        create_pk_mock.assert_not_called()
        create_fk_mock.assert_not_called()

    def test_upgrade_rejects_missing_referenced_primary_key_before_changes(self) -> None:
        inspector = _FakeInspector(
            table_names_by_schema={
                "public": ["grid_walk", "service_deserts", "build_manifest"],
                "transit_derived": [
                    "service_classification",
                    "gtfs_stop_service_summary",
                    "gtfs_stop_reality",
                    "service_desert_cells",
                    "reality_manifest",
                ],
            },
            indexes={
                ("public", "grid_walk"): [{"name": "grid_walk_build_resolution_cell_idx"}],
                ("public", "service_deserts"): [{"name": "service_deserts_build_resolution_cell_idx"}],
            },
        )

        with (
            mock.patch.object(module, "_ensure_clean_candidate_keys"),
            mock.patch.object(module.op, "get_bind", return_value=mock.sentinel.bind),
            mock.patch.object(module.sa, "inspect", return_value=inspector),
            mock.patch.object(module.op, "execute") as execute_mock,
            mock.patch.object(module.op, "create_primary_key") as create_pk_mock,
            mock.patch.object(module.op, "create_foreign_key") as create_fk_mock,
        ):
            with self.assertRaises(RuntimeError):
                module.upgrade()

        execute_mock.assert_not_called()
        create_pk_mock.assert_not_called()
        create_fk_mock.assert_not_called()


class DowngradeTests(TestCase):
    def test_downgrade_drops_named_constraints(self) -> None:
        inspector = _FakeInspector(
            table_names_by_schema={
                "public": ["grid_walk", "service_deserts", "build_manifest"],
                "transit_derived": [
                    "service_classification",
                    "gtfs_stop_service_summary",
                    "gtfs_stop_reality",
                    "service_desert_cells",
                    "reality_manifest",
                ],
            },
            pk_constraints={
                ("public", "grid_walk"): {
                    "constrained_columns": ["build_key", "resolution_m", "cell_id"],
                    "name": "grid_walk_build_resolution_cell_pkey",
                },
                ("public", "service_deserts"): {
                    "constrained_columns": ["build_key", "resolution_m", "cell_id"],
                    "name": "service_deserts_build_resolution_cell_pkey",
                },
                ("transit_derived", "service_classification"): {
                    "constrained_columns": ["reality_fingerprint", "feed_id", "service_id"],
                    "name": "transit_derived_service_classification_pkey",
                },
                ("transit_derived", "gtfs_stop_service_summary"): {
                    "constrained_columns": ["reality_fingerprint", "feed_id", "stop_id"],
                    "name": "transit_derived_gtfs_stop_service_summary_pkey",
                },
                ("transit_derived", "gtfs_stop_reality"): {
                    "constrained_columns": ["reality_fingerprint", "source_ref"],
                    "name": "transit_derived_gtfs_stop_reality_pkey",
                },
                ("transit_derived", "service_desert_cells"): {
                    "constrained_columns": ["build_key", "resolution_m", "cell_id"],
                    "name": "transit_derived_service_desert_cells_pkey",
                },
            },
            foreign_keys={
                ("public", "grid_walk"): [
                    {
                        "name": "grid_walk_build_key_fkey",
                        "constrained_columns": ["build_key"],
                        "referred_schema": "public",
                        "referred_table": "build_manifest",
                        "referred_columns": ["build_key"],
                    }
                ],
                ("public", "service_deserts"): [
                    {
                        "name": "service_deserts_build_key_fkey",
                        "constrained_columns": ["build_key"],
                        "referred_schema": "public",
                        "referred_table": "build_manifest",
                        "referred_columns": ["build_key"],
                    }
                ],
                ("transit_derived", "service_desert_cells"): [
                    {
                        "name": "transit_derived_service_desert_cells_build_key_fkey",
                        "constrained_columns": ["build_key"],
                        "referred_schema": "public",
                        "referred_table": "build_manifest",
                        "referred_columns": ["build_key"],
                    }
                ],
            },
        )

        with (
            mock.patch.object(module.op, "get_bind", return_value=mock.sentinel.bind),
            mock.patch.object(module.sa, "inspect", return_value=inspector),
            mock.patch.object(module.op, "execute") as execute_mock,
        ):
            module.downgrade()

        executed_sql = "\n".join(str(call.args[0]) for call in execute_mock.call_args_list)
        self.assertIn('DROP CONSTRAINT IF EXISTS "transit_derived_service_desert_cells_build_key_fkey"', executed_sql)
        self.assertIn('DROP CONSTRAINT IF EXISTS "grid_walk_build_resolution_cell_pkey"', executed_sql)
        self.assertIn('DROP CONSTRAINT IF EXISTS "service_deserts_build_resolution_cell_pkey"', executed_sql)
