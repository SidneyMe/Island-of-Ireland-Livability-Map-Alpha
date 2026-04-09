from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import config


class DatabaseUrlTests(TestCase):
    def test_database_url_converts_postgres_scheme(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"DATABASE_URL": "postgres://user:secret@localhost:5432/gis"},
            clear=True,
        ):
            self.assertEqual(
                config.database_url(),
                "postgresql+psycopg://user:secret@localhost:5432/gis",
            )

    def test_database_url_converts_postgresql_scheme(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:secret@localhost:5432/gis"},
            clear=True,
        ):
            self.assertEqual(
                config.database_url(),
                "postgresql+psycopg://user:secret@localhost:5432/gis",
            )

    def test_database_url_builds_from_split_postgres_env(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "6543",
                "POSTGRES_DB": "gis database",
                "POSTGRES_USER": "map user",
                "POSTGRES_PASSWORD": "pa ss/word",
            },
            clear=True,
        ):
            self.assertEqual(
                config.database_url(),
                "postgresql+psycopg://map+user:pa+ss%2Fword@db.local:6543/gis+database",
            )

    def test_database_url_reports_missing_split_env(self) -> None:
        with mock.patch.dict(os.environ, {"POSTGRES_HOST": "db.local"}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                config.database_url()

        message = str(ctx.exception)
        self.assertIn("DATABASE_URL", message)
        self.assertIn("POSTGRES_DB", message)
        self.assertIn("POSTGRES_USER", message)
        self.assertIn("POSTGRES_PASSWORD", message)


class ConfigHashTests(TestCase):
    def test_pmtiles_schema_version_changes_render_hash_only(self) -> None:
        with mock.patch.object(config, "PMTILES_SCHEMA_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "PMTILES_SCHEMA_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.render_hash, current_hashes.render_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)

    def test_grid_geometry_schema_version_changes_score_hash(self) -> None:
        with mock.patch.object(config, "GRID_GEOMETRY_SCHEMA_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "GRID_GEOMETRY_SCHEMA_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.geo_hash, current_hashes.geo_hash)
        self.assertEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)


class LocalOsmExtractValidationTests(TestCase):
    def test_validate_local_osm_extract_rejects_pdf_typo(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not a PDF"):
            config.validate_local_osm_extract(Path("extract.osm.pdf"))

    def test_validate_local_osm_extract_rejects_wrong_suffix(self) -> None:
        with self.assertRaisesRegex(RuntimeError, r"local '.osm.pbf' file"):
            config.validate_local_osm_extract(Path("extract.pbf"))

    def test_validate_local_osm_extract_rejects_missing_pbf(self) -> None:
        with TemporaryDirectory() as tmp_name:
            missing = Path(tmp_name) / "missing.osm.pbf"
            with self.assertRaisesRegex(RuntimeError, "was not found"):
                config.validate_local_osm_extract(missing)

    def test_validate_local_osm_extract_accepts_existing_osm_pbf(self) -> None:
        with TemporaryDirectory() as tmp_name:
            extract = Path(tmp_name) / "sample.osm.pbf"
            extract.write_bytes(b"pbf")

            self.assertEqual(config.validate_local_osm_extract(extract), extract)
