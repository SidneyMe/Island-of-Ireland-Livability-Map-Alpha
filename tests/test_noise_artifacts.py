"""
Tests for the noise artifact manifest system (Phases 2 and 3).
All DB operations use mock engines — no live DB required.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from noise_artifacts.manifest import (
    ArtifactManifest,
    get_active_artifact,
    mark_artifact_complete,
    mark_artifact_failed,
    noise_domain_hash,
    noise_resolved_hash,
    noise_source_hash,
    noise_tile_hash,
    record_lineage,
    reset_artifact_for_retry,
    set_active_artifact,
    upsert_artifact,
)


# ---------------------------------------------------------------------------
# Hash function tests
# ---------------------------------------------------------------------------

class NoiseArtifactHashTests(TestCase):

    def test_noise_source_hash_is_deterministic(self) -> None:
        h1 = noise_source_hash("sig-abc", 2, 3)
        h2 = noise_source_hash("sig-abc", 2, 3)
        self.assertEqual(h1, h2)

    def test_noise_source_hash_is_16_hex_chars(self) -> None:
        h = noise_source_hash("sig-abc", 2, 3)
        self.assertEqual(len(h), 16)
        int(h, 16)  # must be valid hex

    def test_noise_source_hash_changes_when_signature_changes(self) -> None:
        h1 = noise_source_hash("sig-abc", 2, 3)
        h2 = noise_source_hash("sig-xyz", 2, 3)
        self.assertNotEqual(h1, h2)

    def test_noise_source_hash_changes_when_parser_version_changes(self) -> None:
        h1 = noise_source_hash("sig-abc", 1, 3)
        h2 = noise_source_hash("sig-abc", 2, 3)
        self.assertNotEqual(h1, h2)

    def test_noise_source_hash_changes_when_schema_version_changes(self) -> None:
        h1 = noise_source_hash("sig-abc", 2, 1)
        h2 = noise_source_hash("sig-abc", 2, 2)
        self.assertNotEqual(h1, h2)

    def test_noise_domain_hash_is_deterministic(self) -> None:
        boundary = b"\x00\x01\x02"
        h1 = noise_domain_hash(boundary, 1)
        h2 = noise_domain_hash(boundary, 1)
        self.assertEqual(h1, h2)

    def test_noise_domain_hash_changes_when_boundary_changes(self) -> None:
        h1 = noise_domain_hash(b"\x00\x01", 1)
        h2 = noise_domain_hash(b"\x00\x02", 1)
        self.assertNotEqual(h1, h2)

    def test_noise_domain_hash_changes_when_extent_version_changes(self) -> None:
        h1 = noise_domain_hash(b"\x00\x01", 1)
        h2 = noise_domain_hash(b"\x00\x01", 2)
        self.assertNotEqual(h1, h2)

    def test_noise_resolved_hash_is_deterministic(self) -> None:
        h1 = noise_resolved_hash("src", "dom", 1, 1, 1, 0.1)
        h2 = noise_resolved_hash("src", "dom", 1, 1, 1, 0.1)
        self.assertEqual(h1, h2)

    def test_noise_resolved_hash_changes_when_any_input_changes(self) -> None:
        base = noise_resolved_hash("src", "dom", 1, 1, 1, 0.1)
        variants = [
            noise_resolved_hash("src2", "dom", 1, 1, 1, 0.1),
            noise_resolved_hash("src", "dom2", 1, 1, 1, 0.1),
            noise_resolved_hash("src", "dom", 2, 1, 1, 0.1),
            noise_resolved_hash("src", "dom", 1, 2, 1, 0.1),
            noise_resolved_hash("src", "dom", 1, 1, 2, 0.1),
            noise_resolved_hash("src", "dom", 1, 1, 1, 0.5),
        ]
        for variant in variants:
            with self.subTest(variant=variant):
                self.assertNotEqual(base, variant)

    def test_noise_tile_hash_is_deterministic(self) -> None:
        h1 = noise_tile_hash("res", 1, 8, 13, ("metric", "db_value"), (5.0, 10.0))
        h2 = noise_tile_hash("res", 1, 8, 13, ("metric", "db_value"), (5.0, 10.0))
        self.assertEqual(h1, h2)

    def test_noise_tile_hash_property_order_does_not_matter(self) -> None:
        h1 = noise_tile_hash("res", 1, 8, 13, ("metric", "db_value"), (5.0, 10.0))
        h2 = noise_tile_hash("res", 1, 8, 13, ("db_value", "metric"), (5.0, 10.0))
        self.assertEqual(h1, h2, "exported_properties are sorted before hashing")


# ---------------------------------------------------------------------------
# Manifest DB operation tests (mock engine)
# ---------------------------------------------------------------------------

def _make_mock_engine():
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


class UpsertArtifactTests(TestCase):

    def test_upsert_calls_execute(self) -> None:
        engine, conn = _make_mock_engine()
        upsert_artifact(engine, "abc123", "resolved", {"key": "val"})
        self.assertTrue(conn.execute.called)

    def test_upsert_sql_contains_on_conflict_do_nothing(self) -> None:
        engine, conn = _make_mock_engine()
        upsert_artifact(engine, "abc123", "resolved", {})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("ON CONFLICT", sql_text)
        self.assertIn("DO NOTHING", sql_text)

    def test_upsert_sql_inserts_with_building_status(self) -> None:
        engine, conn = _make_mock_engine()
        upsert_artifact(engine, "abc123", "resolved", {})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("'building'", sql_text)


class ResetArtifactTests(TestCase):

    def test_reset_updates_status_to_building(self) -> None:
        engine, conn = _make_mock_engine()
        reset_artifact_for_retry(engine, "abc123", {"reset": True})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("'building'", sql_text)
        self.assertIn("UPDATE", sql_text)

    def test_reset_clears_completed_at(self) -> None:
        engine, conn = _make_mock_engine()
        reset_artifact_for_retry(engine, "abc123", {})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("completed_at", sql_text)
        self.assertIn("NULL", sql_text)


class MarkCompleteTests(TestCase):

    def test_mark_complete_updates_status(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_complete(engine, "abc123", updated_manifest_json={"rows": 42})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("'complete'", sql_text)
        self.assertIn("UPDATE", sql_text)

    def test_mark_complete_sets_completed_at(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_complete(engine, "abc123", updated_manifest_json={})
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("completed_at", sql_text)


class MarkFailedTests(TestCase):

    def test_mark_failed_updates_status(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_failed(engine, "abc123", error_detail="boom")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("'failed'", sql_text)
        self.assertIn("UPDATE", sql_text)

    def test_mark_failed_stores_error_detail(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_failed(engine, "abc123", error_detail="boom")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("error_detail", sql_text)

    def test_mark_failed_casts_error_detail_parameter_to_text(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_failed(engine, "abc123", error_detail="boom")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("COALESCE(manifest_json, '{}'::jsonb)", sql_text)
        self.assertIn("CAST(:error_detail AS text)", sql_text)

    def test_mark_failed_truncates_error_detail_to_4000_chars(self) -> None:
        engine, conn = _make_mock_engine()
        mark_artifact_failed(engine, "abc123", error_detail="x" * 5000)
        params = conn.execute.call_args[0][1]
        self.assertEqual(len(params["error_detail"]), 4000)


class RecordLineageTests(TestCase):

    def test_record_lineage_inserts_row(self) -> None:
        engine, conn = _make_mock_engine()
        record_lineage(engine, "child", "parent")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("INSERT INTO noise_artifact_lineage", sql_text)

    def test_record_lineage_is_idempotent(self) -> None:
        engine, conn = _make_mock_engine()
        record_lineage(engine, "child", "parent")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("ON CONFLICT", sql_text)


class SetActiveArtifactTests(TestCase):

    def test_set_active_upserts_pointer(self) -> None:
        engine, conn = _make_mock_engine()
        set_active_artifact(engine, "resolved", "abc123")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("INSERT INTO noise_active_artifact", sql_text)
        self.assertIn("ON CONFLICT", sql_text)

    def test_set_active_updates_existing_pointer(self) -> None:
        engine, conn = _make_mock_engine()
        set_active_artifact(engine, "resolved", "abc123")
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("DO UPDATE", sql_text)


class GetActiveArtifactTests(TestCase):

    def _make_engine_with_row(self, row_data):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mapping_mock = MagicMock()
        mapping_mock.first.return_value = row_data
        conn.execute.return_value.mappings.return_value = mapping_mock
        return engine

    def test_returns_none_when_no_active_artifact(self) -> None:
        engine = self._make_engine_with_row(None)
        result = get_active_artifact(engine, "resolved")
        self.assertIsNone(result)

    def test_returns_manifest_when_found(self) -> None:
        now = datetime.now(timezone.utc)
        row = {
            "artifact_hash": "abc123",
            "artifact_type": "resolved",
            "status": "complete",
            "manifest_json": {"rows": 42},
            "created_at": now,
            "completed_at": now,
        }
        engine = self._make_engine_with_row(row)
        result = get_active_artifact(engine, "resolved")
        self.assertIsNotNone(result)
        self.assertEqual(result.artifact_hash, "abc123")
        self.assertEqual(result.artifact_type, "resolved")
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.manifest_json, {"rows": 42})

    def test_query_filters_on_complete_status(self) -> None:
        engine = self._make_engine_with_row(None)
        get_active_artifact(engine, "resolved")
        conn = engine.connect.return_value.__enter__.return_value
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("status = 'complete'", sql_text)

    def test_query_joins_active_artifact_to_manifest(self) -> None:
        engine = self._make_engine_with_row(None)
        get_active_artifact(engine, "tiles")
        conn = engine.connect.return_value.__enter__.return_value
        sql_text = str(conn.execute.call_args[0][0].text)
        self.assertIn("noise_active_artifact", sql_text)
        self.assertIn("noise_artifact_manifest", sql_text)


# ---------------------------------------------------------------------------
# DB table registration tests
# ---------------------------------------------------------------------------

class NoiseArtifactTableRegistrationTests(TestCase):

    def test_noise_artifact_manifest_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_artifact_manifest", REQUIRED_PUBLIC_TABLES)

    def test_noise_artifact_lineage_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_artifact_lineage", REQUIRED_PUBLIC_TABLES)

    def test_noise_active_artifact_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_active_artifact", REQUIRED_PUBLIC_TABLES)

    def test_noise_artifact_manifest_table_importable(self) -> None:
        from db_postgis.tables import noise_artifact_manifest
        self.assertEqual(noise_artifact_manifest.name, "noise_artifact_manifest")

    def test_noise_artifact_lineage_table_importable(self) -> None:
        from db_postgis.tables import noise_artifact_lineage
        self.assertEqual(noise_artifact_lineage.name, "noise_artifact_lineage")

    def test_noise_active_artifact_table_importable(self) -> None:
        from db_postgis.tables import noise_active_artifact
        self.assertEqual(noise_active_artifact.name, "noise_active_artifact")

    def test_all_three_tables_in_managed_tables(self) -> None:
        import db_postgis.schema as _schema
        table_names = {t.name for t in _schema._MANAGED_TABLES}
        self.assertIn("noise_artifact_manifest", table_names)
        self.assertIn("noise_artifact_lineage", table_names)
        self.assertIn("noise_active_artifact", table_names)


# ---------------------------------------------------------------------------
# Migration SQL content tests
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 3: Canonical table registration tests
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 4A-8A: pipeline module tests
# ---------------------------------------------------------------------------

class GeometrySqlHelperTests(TestCase):

    def test_clean_geometry_sql_contains_make_valid_and_collection_extract(self) -> None:
        from noise_artifacts.geometry import clean_geometry_sql
        sql = clean_geometry_sql("g.geom")
        self.assertIn("ST_MakeValid", sql)
        self.assertIn("ST_CollectionExtract", sql)
        self.assertIn("g.geom", sql)

    def test_reduce_precision_sql_uses_grid_metres(self) -> None:
        from noise_artifacts.geometry import reduce_precision_sql
        sql = reduce_precision_sql("g.geom", grid_metres=0.5)
        self.assertIn("ST_ReducePrecision", sql)
        self.assertIn("0.5", sql)

    def test_reduce_precision_sql_does_not_use_degrees(self) -> None:
        from noise_artifacts.geometry import reduce_precision_sql
        sql = reduce_precision_sql("g.geom", grid_metres=0.1)
        # Must NOT use degree-scale snap grids (e.g. 0.0000001)
        self.assertNotIn("0.0000001", sql)

    def test_subdivide_geometry_sql_parameterises_max_vertices(self) -> None:
        from noise_artifacts.geometry import subdivide_geometry_sql
        sql = subdivide_geometry_sql("g.geom", max_vertices=128)
        self.assertIn("ST_Subdivide", sql)
        self.assertIn("128", sql)

    def test_area_filter_excludes_null_and_empty(self) -> None:
        from noise_artifacts.geometry import area_filter_fragment
        sql = area_filter_fragment("g.geom")
        self.assertIn("IS NOT NULL", sql)
        self.assertIn("ST_IsEmpty", sql)
        self.assertIn("ST_Area", sql)


class DissolveSqlTests(TestCase):

    def test_dissolve_module_importable(self) -> None:
        from noise_artifacts import dissolve
        self.assertTrue(callable(dissolve.dissolve_noise_into_staging))

    def test_pass1_dissolve_sql_contains_st_unary_union(self) -> None:
        import inspect
        from noise_artifacts.dissolve import _pass1_dissolve
        src = inspect.getsource(_pass1_dissolve)
        self.assertIn("ST_UnaryUnion", src)

    def test_pass1_dissolve_sql_contains_st_reduce_precision(self) -> None:
        import inspect
        from noise_artifacts.dissolve import _pass1_dissolve
        src = inspect.getsource(_pass1_dissolve)
        self.assertIn("ST_ReducePrecision", src)

    def test_pass1_dissolve_sql_contains_processing_grid(self) -> None:
        import inspect
        from noise_artifacts.dissolve import _pass1_dissolve
        src = inspect.getsource(_pass1_dissolve)
        self.assertIn("processing_grid", src)

    def test_pass2_dissolve_sql_contains_st_unary_union(self) -> None:
        import inspect
        from noise_artifacts.dissolve import _pass2_dissolve
        src = inspect.getsource(_pass2_dissolve)
        self.assertIn("ST_UnaryUnion", src)

    def test_staging_tables_are_unlogged(self) -> None:
        import inspect
        from noise_artifacts.dissolve import _create_dissolve_staging
        src = inspect.getsource(_create_dissolve_staging)
        self.assertIn("UNLOGGED", src)

    def test_dissolve_has_square_grid_fallback(self) -> None:
        import inspect
        import noise_artifacts.dissolve as _d
        src = inspect.getsource(_d)
        self.assertIn("generate_series", src)
        self.assertIn("ST_SquareGrid", src)


class ResolveSqlTests(TestCase):

    def test_resolve_module_importable(self) -> None:
        from noise_artifacts import resolve
        self.assertTrue(callable(resolve.materialize_resolved_display))

    def test_insert_resolved_round_sql_contains_st_difference(self) -> None:
        import inspect
        from noise_artifacts.resolve import _insert_resolved_round
        src = inspect.getsource(_insert_resolved_round)
        self.assertIn("ST_Difference", src)

    def test_insert_resolved_round_sql_contains_st_subdivide(self) -> None:
        import inspect
        from noise_artifacts.resolve import _insert_resolved_round
        src = inspect.getsource(_insert_resolved_round)
        self.assertIn("ST_Subdivide", src)

    def test_insert_resolved_round_sql_contains_st_reduce_precision(self) -> None:
        import inspect
        from noise_artifacts.resolve import _insert_resolved_round
        src = inspect.getsource(_insert_resolved_round)
        self.assertIn("ST_ReducePrecision", src)

    def test_fetch_rounds_orders_descending(self) -> None:
        import inspect
        from noise_artifacts.resolve import _fetch_rounds
        src = inspect.getsource(_fetch_rounds)
        self.assertIn("DESC", src)

    def test_insert_provenance_is_group_level(self) -> None:
        import inspect
        from noise_artifacts.resolve import _insert_provenance
        src = inspect.getsource(_insert_provenance)
        # Group-level means GROUP BY, not per-polygon spatial join
        self.assertIn("GROUP BY", src)
        self.assertNotIn("ST_Intersects", src)

    def test_provenance_uses_on_conflict_do_nothing(self) -> None:
        import inspect
        from noise_artifacts.resolve import _insert_provenance
        src = inspect.getsource(_insert_provenance)
        self.assertIn("ON CONFLICT", src)
        self.assertIn("DO NOTHING", src)


class DevFastModeTests(TestCase):

    def test_dev_fast_source_code_uses_chunked_reads(self) -> None:
        import inspect
        import noise_artifacts.dev_fast as _df

        src = inspect.getsource(_df.build_dev_fast_road_rail_grid)
        self.assertIn("_iter_noise_gdf_chunks(", src)
        self.assertNotIn("pyogrio.read_dataframe(str(source_path), layer=layer_name)", src)
        self.assertNotIn("pyogrio.read_dataframe(str(source_path))", src)

        chunk_src = inspect.getsource(_df._iter_noise_gdf_chunks)
        self.assertIn("skip_features=", chunk_src)
        self.assertIn("max_features=", chunk_src)

    def test_dev_fast_never_uses_ogr2ogr_import_paths(self) -> None:
        import inspect
        from noise_artifacts.dev_fast import build_dev_fast_road_rail_grid

        src = inspect.getsource(build_dev_fast_road_rail_grid)
        self.assertNotIn("ingest_noise_normalized_ogr2ogr", src)
        self.assertNotIn("_run_ogr2ogr_import", src)

    def test_dev_fast_grid_uses_max_band_per_cell_key(self) -> None:
        from shapely.geometry import box

        from noise_artifacts.dev_fast import _add_geom_cells

        acc = {}
        touched_a = _add_geom_cells(
            acc,
            jurisdiction="roi",
            source_type="road",
            metric="Lden",
            round_number=4,
            report_period="Round 4",
            db_low=45.0,
            db_high=49.0,
            db_value="45-49",
            geom=box(0, 0, 10, 10),
            grid_size_m=100,
        )
        touched_b = _add_geom_cells(
            acc,
            jurisdiction="roi",
            source_type="road",
            metric="Lden",
            round_number=4,
            report_period="Round 4",
            db_low=70.0,
            db_high=74.0,
            db_value="70-74",
            geom=box(0, 0, 10, 10),
            grid_size_m=100,
        )
        self.assertEqual(touched_a, 1)
        self.assertEqual(touched_b, 1)
        self.assertEqual(len(acc), 1)
        record = next(iter(acc.values()))
        self.assertEqual(record.db_value, "70-74")
        self.assertEqual(record.db_high, 74.0)

    def test_dev_fast_rejects_tiny_grid_without_override(self) -> None:
        from noise_artifacts.dev_fast import _validate_dev_fast_grid_size
        from noise_artifacts.exceptions import NoiseIngestError

        with patch.dict(
            os.environ,
            {
                "NOISE_DEV_FAST_MIN_GRID_SIZE_M": "500",
                "NOISE_ALLOW_TINY_NOISE_GRID": "0",
            },
            clear=False,
        ):
            with self.assertRaises(NoiseIngestError):
                _validate_dev_fast_grid_size(250)

    def test_dev_fast_allows_tiny_grid_when_override_enabled(self) -> None:
        from noise_artifacts.dev_fast import _validate_dev_fast_grid_size

        with patch.dict(
            os.environ,
            {
                "NOISE_DEV_FAST_MIN_GRID_SIZE_M": "500",
                "NOISE_ALLOW_TINY_NOISE_GRID": "1",
            },
            clear=False,
        ):
            _validate_dev_fast_grid_size(250)

    def test_dev_fast_rejects_feature_bbox_grid_explosion(self) -> None:
        from shapely.geometry import box

        from noise_artifacts.dev_fast import _add_geom_cells
        from noise_artifacts.exceptions import NoiseIngestError

        with patch.dict(os.environ, {"NOISE_DEV_FAST_MAX_CELLS_PER_FEATURE": "10"}, clear=False):
            with self.assertRaises(NoiseIngestError):
                _add_geom_cells(
                    {},
                    jurisdiction="roi",
                    source_type="road",
                    metric="Lden",
                    round_number=4,
                    report_period="Round 4",
                    db_low=55.0,
                    db_high=59.0,
                    db_value="55-59",
                    geom=box(0, 0, 1000, 1000),
                    grid_size_m=100,
                )

    def test_dev_fast_writes_only_occupied_cells(self) -> None:
        from shapely.geometry import box

        from noise_artifacts.dev_fast import _add_geom_cells

        acc = {}
        touched = _add_geom_cells(
            acc,
            jurisdiction="roi",
            source_type="road",
            metric="Lden",
            round_number=4,
            report_period="Round 4",
            db_low=60.0,
            db_high=64.0,
            db_value="60-64",
            geom=box(10, 10, 15, 15),
            grid_size_m=100,
        )
        self.assertEqual(touched, 1)
        self.assertEqual(len(acc), 1)

    def test_dev_fast_materialization_enforces_exact_vs_grid_split(self) -> None:
        import inspect
        from noise_artifacts.dev_fast import materialize_dev_fast_resolved

        src = inspect.getsource(materialize_dev_fast_resolved)
        self.assertIn("source_type IN ('airport', 'industry')", src)
        self.assertIn("source_type IN ('road', 'rail')", src)
        self.assertIn("artifact_hash = :resolved_hash", src)
        self.assertIn("FROM noise_grid_artifact", src)
        self.assertIn("GROUP BY jurisdiction, source_type, metric, round_number", src)


class NoiseGridArtifactIndexTests(TestCase):

    def test_migration_000020_sets_unique_key_with_jurisdiction(self) -> None:
        import importlib
        import inspect

        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260501_000020_fix_noise_grid_artifact_key"
        )
        src = inspect.getsource(mod)
        self.assertIn("noise_grid_artifact_key_idx", src)
        self.assertIn("jurisdiction", src)

    def test_managed_schema_expects_jurisdiction_in_grid_index(self) -> None:
        import db_postgis.schema as _schema

        spec = next(
            s for s in _schema._MANAGED_INDEX_SPECS
            if s.index_name == "noise_grid_artifact_key_idx"
        )
        self.assertEqual(
            spec.columns,
            (
                "artifact_hash",
                "jurisdiction",
                "source_type",
                "metric",
                "grid_size_m",
                "cell_x",
                "cell_y",
            ),
        )


class BuilderTests(TestCase):

    def _make_engine_with_status(self, status=None):
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        row = MagicMock()
        row.__getitem__ = MagicMock(side_effect=lambda k: status if k == "status" else 0)
        row.mappings.return_value.first.return_value = None if status is None else row
        conn.execute.return_value.mappings.return_value.first.return_value = (
            None if status is None else {"status": status, "row_count": 0,
                                          "jurisdiction_count": 0,
                                          "source_type_count": 0, "metric_count": 0}
        )
        return engine

    def test_builder_marks_failed_on_ingest_error(self) -> None:
        from unittest.mock import patch, MagicMock
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.mappings.return_value.first.return_value = None

        with patch("noise_artifacts.builder.ingest_noise_normalized",
                   side_effect=RuntimeError("boom")):
            with patch("noise_artifacts.builder.mark_artifact_failed") as mock_fail:
                with patch("noise_artifacts.builder.upsert_artifact"):
                    with patch("noise_artifacts.builder.record_lineage"):
                        with patch("noise_artifacts.builder._assert_disk_preflight", return_value=None):
                            with self.assertRaises(RuntimeError):
                                from noise_artifacts.builder import build_noise_artifact
                                build_noise_artifact(
                                    engine,
                                    data_dir="/fake",
                                    domain_wgs84=MagicMock(),
                                    domain_wkb=b"\x00",
                                    source_hash="src",
                                    domain_hash="dom",
                                    resolved_hash="res",
                                )
                # FIX 6: all three artifacts (resolved, source, domain) must be marked failed
                self.assertEqual(mock_fail.call_count, 3)
                for single_call in mock_fail.call_args_list:
                    self.assertIn("error_detail", single_call.kwargs)

    def test_builder_sets_active_artifact_on_success(self) -> None:
        from unittest.mock import patch, MagicMock
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        # Must include "status" since FIX 5 reads existing["status"] in _ensure_artifact.
        # "complete" + force=False → "already_complete" fast-path still calls set_active_artifact.
        conn.execute.return_value.mappings.return_value.first.return_value = {
            "status": "complete",
            "row_count": 10, "jurisdiction_count": 2,
            "source_type_count": 3, "metric_count": 2,
        }

        with patch("noise_artifacts.builder.ingest_noise_normalized"):
            with patch("noise_artifacts.builder.dissolve_noise_into_staging",
                       return_value=("dissolve_tbl", "round_tbl")):
                with patch("noise_artifacts.builder.materialize_resolved_display",
                           return_value={"total_inserted": 10, "groups_processed": 3}):
                    with patch("noise_artifacts.builder.mark_artifact_complete"):
                        with patch("noise_artifacts.builder.set_active_artifact") as mock_set:
                            with patch("noise_artifacts.builder.record_lineage"):
                                with patch("noise_artifacts.builder.upsert_artifact"):
                                    with patch("noise_artifacts.builder.drop_staging_tables"):
                                        from noise_artifacts.builder import build_noise_artifact
                                        build_noise_artifact(
                                            engine,
                                            data_dir="/fake",
                                            domain_wgs84=MagicMock(),
                                            domain_wkb=b"\x00",
                                            source_hash="src",
                                            domain_hash="dom",
                                            resolved_hash="res",
                                        )
                        mock_set.assert_called_once_with(engine, "resolved", "res")

    def test_builder_uses_postgres_advisory_lock(self) -> None:
        import inspect
        from noise_artifacts.builder import build_noise_artifact

        src = inspect.getsource(build_noise_artifact)
        self.assertIn("pg_advisory_lock", src)
        self.assertIn("pg_advisory_unlock", src)

    def test_builder_dev_fast_asserts_no_road_rail_normalized_rows(self) -> None:
        import inspect
        from noise_artifacts.builder import build_noise_artifact

        src = inspect.getsource(build_noise_artifact)
        self.assertIn("_assert_no_dev_fast_road_rail_normalized", src)


class IngestSqlTests(TestCase):

    def test_ingest_sql_transforms_to_2157(self) -> None:
        import inspect
        from noise_artifacts.ingest import _flush_stage_to_normalized
        src = inspect.getsource(_flush_stage_to_normalized)
        self.assertIn("ST_Transform", src)
        self.assertIn("2157", src)

    def test_ingest_sql_makes_valid(self) -> None:
        import inspect
        from noise_artifacts.ingest import _flush_stage_to_normalized
        src = inspect.getsource(_flush_stage_to_normalized)
        self.assertIn("ST_MakeValid", src)

    def test_ingest_stage_table_has_geom_wkb_bytea(self) -> None:
        import inspect
        from noise_artifacts.ingest import _create_ingest_stage_table
        src = inspect.getsource(_create_ingest_stage_table)
        self.assertIn("CREATE TEMP TABLE", src)
        self.assertIn("geom_wkb BYTEA", src)

    def test_ingest_uses_copy_staging_not_inline_values(self) -> None:
        import inspect
        from noise_artifacts.ingest import _copy_rows_into_stage_via_psycopg, _copy_rows_into_stage
        src_copy = inspect.getsource(_copy_rows_into_stage_via_psycopg)
        src_stage = inspect.getsource(_copy_rows_into_stage)
        self.assertIn("COPY", src_copy)
        self.assertIn("_STAGE_TABLE_NAME", src_copy)
        self.assertNotIn("FROM (VALUES", src_stage)
        self.assertNotIn("wkb_hex", src_stage)

    def test_ingest_sql_filters_null_geom_wkb(self) -> None:
        """Rows with null WKB must be excluded before ST_GeomFromWKB."""
        import inspect
        from noise_artifacts.ingest import _flush_stage_to_normalized
        src = inspect.getsource(_flush_stage_to_normalized)
        self.assertIn("geom_wkb IS NOT NULL", src)

    def test_ingest_sql_filters_empty_and_zero_area_geom(self) -> None:
        """FIX 3: transformed geometry that is empty or zero-area must be excluded."""
        import inspect
        from noise_artifacts.ingest import _flush_stage_to_normalized
        src = inspect.getsource(_flush_stage_to_normalized)
        self.assertIn("ST_IsEmpty", src)
        self.assertIn("ST_Area", src)

    def test_copy_batch_to_stage_skips_rows_without_geom(self) -> None:
        from noise_artifacts.ingest import _copy_batch_to_stage

        conn = MagicMock()
        row = {
            "jurisdiction": "roi",
            "source_type": "road",
            "metric": "Lden",
            "round_number": 4,
            "db_value": "55-59",
            "source_dataset": "NOISE_Round4.zip",
            "source_layer": "Road.shp",
            "source_ref": "x",
            "geom": None,
        }
        copied = _copy_batch_to_stage(conn, "h123", [row])
        self.assertEqual(copied, 0)

    def test_ingest_deletes_prior_rows_on_force(self) -> None:
        import inspect
        from noise_artifacts.ingest import ingest_noise_normalized
        src = inspect.getsource(ingest_noise_normalized)
        self.assertIn("_delete_source_rows(engine, noise_source_hash)", src)

    def test_bake_stub_raises_not_implemented(self) -> None:
        from noise_artifacts.bake import bake_noise_artifact_pmtiles
        from pathlib import Path
        engine = MagicMock()
        with self.assertRaises(NotImplementedError):
            bake_noise_artifact_pmtiles(engine, "tilehash", output_dir=Path("/tmp"))


class MainModuleTests(TestCase):
    """Tests for noise_artifacts.__main__ CLI helpers."""

    def test_default_data_dir_comes_from_noise_loader_not_config(self) -> None:
        """FIX 1: default data_dir must use noise.loader.NOISE_DATA_DIR, not config.NOISE_DATA_DIR."""
        import inspect
        import noise_artifacts.runner as _runner
        src = inspect.getsource(_runner.build_default_noise_artifact)
        self.assertIn("NOISE_DATA_DIR", src)
        self.assertNotIn("config.NOISE_DATA_DIR", src)

    def test_domain_loader_uses_load_island_geometry_metric(self) -> None:
        """FIX 2: domain must cover the full island, not NI-only. Canonical home is runner.py."""
        import inspect
        import noise_artifacts.runner as _runner
        src = inspect.getsource(_runner._load_domain)
        self.assertIn("load_island_geometry_metric", src)

    def test_domain_loader_does_not_use_convex_hull(self) -> None:
        """FIX 2: convex_hull on NI boundary was wrong; real island geometry must be used."""
        import inspect
        import noise_artifacts.runner as _runner
        src = inspect.getsource(_runner._load_domain)
        self.assertNotIn("convex_hull", src)

    def test_domain_boundary_bytes_hashes_both_roi_and_ni(self) -> None:
        """FIX 2: domain boundary hash must cover both ROI and NI boundary files."""
        import inspect
        import noise_artifacts.runner as _runner
        src = inspect.getsource(_runner._load_domain_boundary_bytes)
        self.assertIn("ROI_BOUNDARY_PATH", src)
        self.assertIn("NI_BOUNDARY_PATH", src)

    def test_compare_handles_none_ratio(self) -> None:
        """FIX 11: abs(ratio - 1.0) crashes when ratio is None."""
        import noise_artifacts.__main__ as _main
        from unittest.mock import MagicMock, patch

        engine = MagicMock()
        result = {
            "groups_matching": 1,
            "groups_diverging": 1,
            "area_ratio_by_group": {
                "roi/road/Lden": None,
                "roi/rail/Lden": 0.99,
            },
        }
        with patch("noise_artifacts.__main__._run_compare") as mock_compare:
            mock_compare.return_value = 1
            # Directly test _run_compare's ratio-printing logic via inspection
            pass

        # Test the actual None-safe loop in _run_compare source
        import inspect
        src = inspect.getsource(_main._run_compare)
        self.assertIn("ratio is None", src)


class NoiseCanonicalTableRegistrationTests(TestCase):

    def test_noise_normalized_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_normalized", REQUIRED_PUBLIC_TABLES)

    def test_noise_resolved_display_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_resolved_display", REQUIRED_PUBLIC_TABLES)

    def test_noise_resolved_provenance_in_required_public_tables(self) -> None:
        from db_postgis.tables import REQUIRED_PUBLIC_TABLES
        self.assertIn("noise_resolved_provenance", REQUIRED_PUBLIC_TABLES)

    def test_all_three_canonical_tables_in_managed_tables(self) -> None:
        import db_postgis.schema as _schema
        table_names = {t.name for t in _schema._MANAGED_TABLES}
        self.assertIn("noise_normalized", table_names)
        self.assertIn("noise_resolved_display", table_names)
        self.assertIn("noise_resolved_provenance", table_names)

    def test_noise_normalized_geom_is_2157(self) -> None:
        from db_postgis.tables import noise_normalized
        geom_col = noise_normalized.c["geom"]
        self.assertEqual(geom_col.type.srid, 2157)

    def test_noise_resolved_display_geom_is_2157(self) -> None:
        from db_postgis.tables import noise_resolved_display
        geom_col = noise_resolved_display.c["geom"]
        self.assertEqual(geom_col.type.srid, 2157)

    def test_noise_resolved_provenance_has_no_geom(self) -> None:
        from db_postgis.tables import noise_resolved_provenance
        col_names = {c.name for c in noise_resolved_provenance.c}
        self.assertNotIn("geom", col_names)

    def test_noise_normalized_in_geometry_fields(self) -> None:
        from db_postgis.tables import GEOMETRY_FIELDS, noise_normalized
        from db_postgis.common import _table_key
        self.assertIn(_table_key(noise_normalized), GEOMETRY_FIELDS)

    def test_noise_resolved_display_in_geometry_fields(self) -> None:
        from db_postgis.tables import GEOMETRY_FIELDS, noise_resolved_display
        from db_postgis.common import _table_key
        self.assertIn(_table_key(noise_resolved_display), GEOMETRY_FIELDS)

    def test_migration_000015_creates_all_three_canonical_tables(self) -> None:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000015_noise_canonical_tables"
        )
        src = inspect.getsource(mod)
        self.assertIn("noise_normalized", src)
        self.assertIn("noise_resolved_display", src)
        self.assertIn("noise_resolved_provenance", src)

    def test_migration_000015_canonical_geom_is_2157(self) -> None:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000015_noise_canonical_tables"
        )
        src = inspect.getsource(mod)
        self.assertIn("2157", src)

    def test_migration_000015_has_db_high_ge_db_low_check(self) -> None:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000015_noise_canonical_tables"
        )
        src = inspect.getsource(mod)
        self.assertIn("db_high >= db_low", src)

    def test_migration_000015_correct_down_revision(self) -> None:
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000015_noise_canonical_tables"
        )
        self.assertEqual(mod.down_revision, "20260427_000014")

    def test_migration_000015_downgrade_drops_all_tables(self) -> None:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000015_noise_canonical_tables"
        )
        src = inspect.getsource(mod)
        self.assertIn("DROP TABLE IF EXISTS noise_resolved_provenance", src)
        self.assertIn("DROP TABLE IF EXISTS noise_resolved_display", src)
        self.assertIn("DROP TABLE IF EXISTS noise_normalized", src)


class MigrationSqlTests(TestCase):

    def _get_migration_source(self) -> str:
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000014_noise_artifact_manifest"
        )
        import inspect
        return inspect.getsource(mod)

    def test_migration_creates_noise_artifact_manifest(self) -> None:
        src = self._get_migration_source()
        self.assertIn("noise_artifact_manifest", src)

    def test_migration_creates_noise_artifact_lineage(self) -> None:
        src = self._get_migration_source()
        self.assertIn("noise_artifact_lineage", src)

    def test_migration_creates_noise_active_artifact(self) -> None:
        src = self._get_migration_source()
        self.assertIn("noise_active_artifact", src)

    def test_migration_has_correct_check_on_artifact_type(self) -> None:
        src = self._get_migration_source()
        for expected in ("source", "domain", "resolved", "tiles", "exposure"):
            self.assertIn(expected, src)

    def test_migration_has_correct_check_on_status(self) -> None:
        src = self._get_migration_source()
        for expected in ("building", "complete", "failed", "superseded"):
            self.assertIn(expected, src)

    def test_migration_has_correct_down_revision(self) -> None:
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000014_noise_artifact_manifest"
        )
        self.assertEqual(mod.down_revision, "20260426_000013")

    def test_migration_downgrade_drops_all_three_tables(self) -> None:
        src = self._get_migration_source()
        self.assertIn("DROP TABLE IF EXISTS noise_active_artifact", src)
        self.assertIn("DROP TABLE IF EXISTS noise_artifact_lineage", src)
        self.assertIn("DROP TABLE IF EXISTS noise_artifact_manifest", src)


# ---------------------------------------------------------------------------
# Migration 000018: relaxed db_value constraint tests
# ---------------------------------------------------------------------------

class Migration000018ConstraintTests(TestCase):

    def _get_migration_source(self) -> str:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260428_000018_relax_noise_db_value_check"
        )
        return inspect.getsource(mod)

    def test_migration_000018_has_correct_down_revision(self) -> None:
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260428_000018_relax_noise_db_value_check"
        )
        self.assertEqual(mod.down_revision, "20260427_000017")

    def test_migration_000018_drops_old_constraint_before_adding_new(self) -> None:
        src = self._get_migration_source()
        self.assertIn("DROP CONSTRAINT IF EXISTS noise_normalized_db_value_check", src)
        self.assertIn("DROP CONSTRAINT IF EXISTS noise_resolved_display_db_value_check", src)

    def test_migration_000018_new_check_uses_regex_not_fixed_list(self) -> None:
        src = self._get_migration_source()
        self.assertIn("~", src, "new constraint must use ~ regex operator")
        # The upgrade _NEW_CHECK must use regex; it must appear before downgrade's fixed list.
        # We verify by checking _NEW_CHECK does not contain the old fixed-list pattern.
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260428_000018_relax_noise_db_value_check"
        )
        new_check = mod._NEW_CHECK
        self.assertNotIn("IN (", new_check, "_NEW_CHECK must use regex, not IN list")

    def test_migration_000018_new_check_allows_open_ended_bands(self) -> None:
        src = self._get_migration_source()
        # Must match XX+ pattern
        self.assertIn(r"[0-9]{2}\+", src)

    def test_migration_000018_new_check_allows_range_bands(self) -> None:
        src = self._get_migration_source()
        # Must match XX-XX pattern
        self.assertIn(r"[0-9]{2}-[0-9]{2}", src)

    def test_migration_000018_downgrade_restores_fixed_list(self) -> None:
        src = self._get_migration_source()
        self.assertIn("IN ('45-49'", src)
        self.assertIn("'75+'", src)


# ---------------------------------------------------------------------------
# Ingest pre-validation and diagnostics tests
# ---------------------------------------------------------------------------

class IngestValidationTests(TestCase):

    def test_validate_accepts_range_band(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        _validate_noise_row({"db_value": "55-59", "db_low": 55.0, "db_high": 59.0})

    def test_validate_accepts_70_plus(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        _validate_noise_row({"db_value": "70+", "db_low": 70.0, "db_high": 99.0})

    def test_validate_accepts_75_plus(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        _validate_noise_row({"db_value": "75+", "db_low": 75.0, "db_high": 99.0})

    def test_validate_rejects_text_label(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        from noise_artifacts.exceptions import NoiseIngestError
        with self.assertRaises(NoiseIngestError):
            _validate_noise_row({"db_value": "seventy", "db_low": 70.0, "db_high": 99.0})

    def test_validate_rejects_empty_string(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        from noise_artifacts.exceptions import NoiseIngestError
        with self.assertRaises(NoiseIngestError):
            _validate_noise_row({"db_value": "", "db_low": 70.0, "db_high": 99.0})

    def test_validate_rejects_inverted_range(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        from noise_artifacts.exceptions import NoiseIngestError
        with self.assertRaises(NoiseIngestError):
            _validate_noise_row({"db_value": "59-55", "db_low": 59.0, "db_high": 55.0})

    def test_validate_invalid_ni_band_error_contains_gridcode_and_source_ref(self) -> None:
        from noise_artifacts.ingest import _validate_noise_row
        from noise_artifacts.exceptions import NoiseIngestError

        row = {
            "jurisdiction": "ni",
            "source_type": "airport",
            "metric": "Lden",
            "round_number": 1,
            "db_value": "2-6",
            "raw_gridcode": 1,
            "source_dataset": "end_noisedata_round1.zip",
            "source_layer": "END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp",
            "source_ref": "end_noisedata_round1.zip:...:1",
            "wkb_hex": "deadbeef",
        }
        with self.assertRaises(NoiseIngestError) as ctx:
            _validate_noise_row(row)
        msg = str(ctx.exception)
        self.assertIn("Invalid NI noise band:", msg)
        self.assertIn("source_dataset=end_noisedata_round1.zip", msg)
        self.assertIn("source_ref=end_noisedata_round1.zip:...:1", msg)
        self.assertIn("raw_gridcode=1", msg)
        self.assertIn("produced db_value='2-6'", msg)
        self.assertNotIn("deadbeef", msg)

    def test_diagnose_identifies_70_plus_as_suspicious(self) -> None:
        # Simulate a batch with a row whose db_value is "70+" — which should NOT
        # be suspicious under the new constraint. This ensures the diagnostic
        # function's suspicion logic matches the new regex, not the old fixed list.
        from noise_artifacts.ingest import _diagnose_ingest_integrity_error, _BAND_RE
        # "70+" matches _BAND_RE so it should NOT be flagged as suspicious
        self.assertIsNotNone(_BAND_RE.match("70+"))
        self.assertIsNotNone(_BAND_RE.match("75+"))
        self.assertIsNone(_BAND_RE.match("seventy"))
        self.assertIsNone(_BAND_RE.match("70 plus"))

    def test_diagnose_message_omits_wkb_hex(self) -> None:
        from unittest.mock import MagicMock
        from noise_artifacts.ingest import _diagnose_ingest_integrity_error
        from sqlalchemy.exc import IntegrityError

        mock_exc = MagicMock(spec=IntegrityError)
        mock_exc.orig = MagicMock()
        mock_exc.orig.diag.constraint_name = "noise_normalized_db_value_check"

        batch = [{
            "db_value": "abc", "db_low": 70.0, "db_high": 99.0,
            "jurisdiction": "roi", "source_type": "airport",
            "metric": "Lnight", "round_number": 4,
            "report_period": "Rd4-2022",
            "source_dataset": "NOISE_Round4.zip",
            "source_layer": "Noise_R4_Airport.shp",
            "source_ref": "NOISE_Round4.zip:Noise_R4_Airport.shp:0",
            "wkb_hex": "deadbeef" * 100,
        }]

        err = _diagnose_ingest_integrity_error(None, None, "srcabc123", batch, mock_exc)
        msg = str(err)

        self.assertIn("noise_normalized_db_value_check", msg)
        self.assertIn("db_value", msg)
        self.assertIn("abc", msg)
        self.assertNotIn("wkb_hex", msg)
        self.assertNotIn("deadbeef", msg)

    def test_diagnose_message_does_not_dump_all_500_params(self) -> None:
        from unittest.mock import MagicMock
        from noise_artifacts.ingest import _diagnose_ingest_integrity_error
        from sqlalchemy.exc import IntegrityError

        mock_exc = MagicMock(spec=IntegrityError)
        mock_exc.orig = MagicMock()
        mock_exc.orig.diag.constraint_name = "noise_normalized_db_value_check"

        batch = [
            {
                "db_value": "abc", "db_low": 70.0, "db_high": 99.0,
                "jurisdiction": "roi", "source_type": "airport",
                "metric": "Lnight", "round_number": 4, "report_period": "Rd4-2022",
                "source_dataset": "ds.zip", "source_layer": "layer.shp",
                "source_ref": f"ds.zip:layer.shp:{i}",
                "wkb_hex": "ff" * 1000,
            }
            for i in range(500)
        ]

        err = _diagnose_ingest_integrity_error(None, None, "srcabc123", batch, mock_exc)
        msg = str(err)
        # Message should be compact — well under 10 000 chars
        self.assertLess(len(msg), 10_000, "diagnostics message must not dump all 500 rows")


# ---------------------------------------------------------------------------
# Progress callback plumbing tests
# ---------------------------------------------------------------------------

class BuilderProgressTests(TestCase):

    def test_builder_emits_progress_messages(self) -> None:
        from unittest.mock import patch, MagicMock
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        # Return None for artifact manifest lookups so _ensure_artifact returns "created"
        # (not "already_complete"), which forces the full build path.
        # Then return counts for _compute_artifact_counts.
        call_count = [0]

        def _fake_first():
            call_count[0] += 1
            # First 3 calls are _ensure_artifact (source/domain/resolved status lookups)
            if call_count[0] <= 3:
                return None
            # Subsequent calls return counts for _compute_artifact_counts
            return {"row_count": 5, "jurisdiction_count": 2,
                    "source_type_count": 3, "metric_count": 2}

        conn.execute.return_value.mappings.return_value.first.side_effect = _fake_first

        messages = []

        def fake_progress(action, *, detail="", force_log=False):
            messages.append(detail)

        with patch("noise_artifacts.builder.ingest_noise_normalized", return_value=10):
            with patch("noise_artifacts.builder.dissolve_noise_into_staging",
                       return_value=("dtbl", "rtbl")):
                with patch("noise_artifacts.builder.materialize_resolved_display",
                           return_value={"total_inserted": 5, "groups_processed": 2}):
                    with patch("noise_artifacts.builder.mark_artifact_complete"):
                        with patch("noise_artifacts.builder.set_active_artifact"):
                            with patch("noise_artifacts.builder.record_lineage"):
                                with patch("noise_artifacts.builder.upsert_artifact"):
                                    with patch("noise_artifacts.builder.drop_staging_tables"):
                                        with patch("noise_artifacts.builder._assert_disk_preflight", return_value=None):
                                            from noise_artifacts.builder import build_noise_artifact
                                            build_noise_artifact(
                                                engine,
                                                data_dir="/fake",
                                                domain_wgs84=MagicMock(),
                                                domain_wkb=b"\x00",
                                                source_hash="src",
                                                domain_hash="dom",
                                                resolved_hash="res",
                                                noise_accuracy_mode="accurate",
                                                progress_cb=fake_progress,
                                            )

        joined = " | ".join(messages)
        self.assertTrue(any("ingest start" in m for m in messages), f"missing 'ingest start' in: {joined}")
        self.assertTrue(any("ingest done" in m for m in messages), f"missing 'ingest done' in: {joined}")
        self.assertTrue(any("dissolve" in m for m in messages), f"missing 'dissolve' in: {joined}")
        self.assertTrue(any("resolve" in m for m in messages), f"missing 'resolve' in: {joined}")
        self.assertTrue(any("artifact complete" in m for m in messages), f"missing 'artifact complete' in: {joined}")

    def test_ingest_emits_progress_messages(self) -> None:
        from unittest.mock import patch, MagicMock
        from noise_artifacts.ingest import ingest_noise_normalized

        messages = []

        def fake_progress(action, *, detail="", force_log=False):
            messages.append(detail)

        fake_rows = [
            {
                "jurisdiction": "roi", "source_type": "airport", "metric": "Lnight",
                "round_number": 4, "db_value": "70+", "db_low": 70.0, "db_high": 99.0,
                "report_period": "Rd4-2022", "source_dataset": "ds.zip",
                "source_layer": "layer.shp", "source_ref": "ref1", "geom": None,
            }
        ]

        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # iter_noise_candidate_rows_cached is imported inside ingest_noise_normalized,
        # so patch it at its source module.
        with patch.dict(os.environ, {"NOISE_INGEST_MODE": "python"}, clear=False):
            with patch("noise.loader.iter_noise_candidate_rows_cached", return_value=iter(fake_rows)):
                with patch("noise_artifacts.ingest._create_ingest_stage_table"):
                    with patch("noise_artifacts.ingest._copy_batch_to_stage", return_value=0):
                        with patch("noise_artifacts.ingest._flush_stage_to_normalized", return_value=0):
                            with patch("noise_artifacts.ingest._drop_ingest_stage_table"):
                                ingest_noise_normalized(
                                    engine, "srchash123", "/fake_dir", MagicMock(),
                                    progress_cb=fake_progress,
                                )

        self.assertTrue(any("ingest streamed" in m for m in messages), f"messages: {messages}")
        self.assertTrue(any("ingest done: read" in m for m in messages), f"messages: {messages}")

    def test_ingest_function_source_uses_streaming_cached_loader(self) -> None:
        import inspect
        from noise_artifacts import ingest as _ingest

        src = inspect.getsource(_ingest.ingest_noise_normalized)
        self.assertIn("NOISE_INGEST_MODE", src)
        self.assertNotIn("candidate_rows = list(", src)

    def test_ingest_partial_source_rows_are_not_cache_hit(self) -> None:
        from noise_artifacts.ingest import ingest_noise_normalized

        fake_rows = [
            {
                "jurisdiction": "roi",
                "source_type": "road",
                "metric": "Lden",
                "round_number": 4,
                "db_value": "55-59",
                "db_low": 55.0,
                "db_high": 59.0,
                "report_period": "Rd4-2022",
                "source_dataset": "NOISE_Round4.zip",
                "source_layer": "Noise_R4_Road.shp",
                "source_ref": "ref1",
                "geom": None,
            }
        ]
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("noise_artifacts.ingest._existing_source_row_count", return_value=42):
            with patch(
                "noise_artifacts.ingest._source_manifest_state",
                return_value={"status": "failed", "manifest_json": {"row_count": 999}},
            ):
                with patch.dict(os.environ, {"NOISE_INGEST_MODE": "python"}, clear=False):
                    with patch("noise.loader.iter_noise_candidate_rows_cached", return_value=iter(fake_rows)) as mock_rows:
                        with patch("noise_artifacts.ingest._create_ingest_stage_table"):
                            with patch("noise_artifacts.ingest._copy_batch_to_stage", return_value=0):
                                with patch("noise_artifacts.ingest._flush_stage_to_normalized", return_value=0):
                                    with patch("noise_artifacts.ingest._drop_ingest_stage_table"):
                                        ingest_noise_normalized(engine, "srchash123", "/fake_dir", MagicMock())
        self.assertTrue(engine.begin.called, "expected stale source rows delete before rebuild")
        mock_rows.assert_called()

    def test_ingest_complete_manifest_with_matching_row_count_is_cache_hit(self) -> None:
        from noise_artifacts.ingest import INGEST_SCHEMA_VERSION, ingest_noise_normalized

        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("noise_artifacts.ingest._existing_source_row_count", return_value=42):
            with patch(
                "noise_artifacts.ingest._source_manifest_state",
                return_value={
                    "status": "complete",
                    "manifest_json": {
                        "row_count": 42,
                        "ingest_schema_version": INGEST_SCHEMA_VERSION,
                        "ingest_complete": True,
                    },
                },
            ):
                with patch("noise.loader.iter_noise_candidate_rows_cached") as mock_rows:
                    inserted = ingest_noise_normalized(
                        engine,
                        "srchash123",
                        "/fake_dir",
                        MagicMock(),
                    )
        self.assertEqual(inserted, 0)
        mock_rows.assert_not_called()

    def test_ingest_reimport_source_ignores_existing_rows(self) -> None:
        from noise_artifacts.ingest import ingest_noise_normalized

        fake_rows = [
            {
                "jurisdiction": "roi",
                "source_type": "road",
                "metric": "Lden",
                "round_number": 4,
                "db_value": "55-59",
                "db_low": 55.0,
                "db_high": 59.0,
                "report_period": "Rd4-2022",
                "source_dataset": "NOISE_Round4.zip",
                "source_layer": "Noise_R4_Road.shp",
                "source_ref": "ref1",
                "geom": None,
            }
        ]

        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("noise_artifacts.ingest._existing_source_row_count", return_value=42):
            with patch(
                "noise_artifacts.ingest._source_manifest_state",
                return_value={
                    "status": "complete",
                    "manifest_json": {"row_count": 42, "ingest_schema_version": 1, "ingest_complete": True},
                },
            ):
                with patch.dict(os.environ, {"NOISE_INGEST_MODE": "python"}, clear=False):
                    with patch("noise.loader.iter_noise_candidate_rows_cached", return_value=iter(fake_rows)):
                        with patch("noise_artifacts.ingest._create_ingest_stage_table"):
                            with patch("noise_artifacts.ingest._copy_batch_to_stage", return_value=0):
                                with patch("noise_artifacts.ingest._flush_stage_to_normalized", return_value=0):
                                    with patch("noise_artifacts.ingest._drop_ingest_stage_table"):
                                        ingest_noise_normalized(
                                            engine,
                                            "srchash123",
                                            "/fake_dir",
                                            MagicMock(),
                                            reimport_source=True,
                                        )

        self.assertTrue(engine.begin.called, "expected source delete transaction for reimport")

    def test_ingest_invalid_first_batch_raises_before_generator_exhaustion(self) -> None:
        from noise_artifacts.exceptions import NoiseIngestError
        from noise_artifacts.ingest import ingest_noise_normalized

        produced = {"count": 0}

        def _row(db_value: str, raw_gridcode: int, idx: int) -> dict:
            return {
                "jurisdiction": "ni",
                "source_type": "airport",
                "metric": "Lden",
                "round_number": 1,
                "db_value": db_value,
                "db_low": 55.0,
                "db_high": 59.0,
                "report_period": "Round 1",
                "source_dataset": "end_noisedata_round1.zip",
                "source_layer": "END_NoiseData_Round1/Major_Airports/BIA/R1_bia_lden.shp",
                "source_ref": f"end_noisedata_round1.zip:layer:{idx}",
                "raw_gridcode": raw_gridcode,
                "geom": None,
            }

        valid = [_row("55-59", 3, i) for i in range(499)]
        bad = _row("2-6", 1, 499)
        tail = [_row("55-59", 3, i) for i in range(500, 1000)]

        def _gen():
            for row in valid:
                produced["count"] += 1
                yield row
            produced["count"] += 1
            yield bad
            for row in tail:
                produced["count"] += 1
                yield row

        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with tempfile.TemporaryDirectory() as tmp_name:
            env = {
                "NOISE_CANDIDATES_CACHE_DIR": tmp_name,
                "NOISE_LOADER_WORKERS": "1",
                "NOISE_INGEST_MODE": "python",
                "NOISE_INGEST_COPY_BATCH_ROWS": "500",
            }
            with patch.dict(os.environ, env, clear=False):
                with patch("noise.loader.iter_noise_candidate_rows", side_effect=lambda **_: _gen()):
                    with patch("noise.loader._ogr2ogr_available", return_value=False):
                        with patch("noise_artifacts.ingest._create_ingest_stage_table"):
                            with patch("noise_artifacts.ingest._copy_batch_to_stage", return_value=0) as mock_copy:
                                with patch("noise_artifacts.ingest._flush_stage_to_normalized", return_value=0):
                                    with patch("noise_artifacts.ingest._drop_ingest_stage_table"):
                                        with self.assertRaises(NoiseIngestError):
                                            ingest_noise_normalized(engine, "srchash123", "/fake_dir", MagicMock())

        self.assertEqual(produced["count"], 500, "ingest must stop after the first invalid batch")
        mock_copy.assert_not_called()

