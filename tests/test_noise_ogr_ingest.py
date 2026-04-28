from __future__ import annotations

import os
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy.engine import make_url


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
