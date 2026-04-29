from __future__ import annotations

import os
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
