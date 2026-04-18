from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from db_postgis import writes as db_writes


class DbPostgisTransitArtifactWriteTests(TestCase):
    def test_replace_gtfs_feed_rows_from_artifacts_executes_stop_staging_sql(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        with TemporaryDirectory() as tmp_name, mock.patch.object(
            db_writes,
            "_copy_from_csv_file",
        ) as copy_mock:
            artifacts_dir = Path(tmp_name)
            db_writes.replace_gtfs_feed_rows_from_artifacts(
                engine,
                feed_fingerprint="feed-fingerprint-123",
                feed_id="nta",
                analysis_date=date(2026, 4, 14),
                source_path="gtfs/nta_gtfs.zip",
                source_url=None,
                artifacts_dir=artifacts_dir,
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined_statements = "\n".join(statements)
        self.assertIn("DROP TABLE IF EXISTS", joined_statements)
        self.assertIn("CREATE TEMP TABLE", joined_statements)
        self.assertIn('ALTER COLUMN "geom" DROP NOT NULL', joined_statements)
        self.assertIn("ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)", joined_statements)
        self.assertTrue(
            any(
                call.kwargs.get("csv_path") == artifacts_dir / "stops.csv"
                and call.kwargs.get("qualified_table") == '"_tmp_transit_stops_stage"'
                for call in copy_mock.call_args_list
            )
        )

    def test_replace_transit_reality_rows_from_artifacts_executes_stop_reality_staging_sql(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        with TemporaryDirectory() as tmp_name, mock.patch.object(
            db_writes,
            "_copy_from_csv_file",
        ) as copy_mock:
            artifacts_dir = Path(tmp_name)
            db_writes.replace_transit_reality_rows_from_artifacts(
                engine,
                reality_fingerprint="reality-123",
                import_fingerprint="import-123",
                analysis_date=date(2026, 4, 14),
                transit_config_hash="config-hash",
                feed_fingerprints_json={"nta": "feed-fingerprint-123"},
                artifacts_dir=artifacts_dir,
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined_statements = "\n".join(statements)
        self.assertIn("DROP TABLE IF EXISTS", joined_statements)
        self.assertIn("CREATE TEMP TABLE", joined_statements)
        self.assertIn("ALTER TABLE", joined_statements)
        self.assertIn('ALTER COLUMN "geom" DROP NOT NULL', joined_statements)
        self.assertIn('ADD COLUMN "lat" DOUBLE PRECISION', joined_statements)
        self.assertIn('ADD COLUMN "lon" DOUBLE PRECISION', joined_statements)
        self.assertIn("ST_SetSRID(ST_MakePoint(lon, lat), 4326)", joined_statements)
        self.assertTrue(
            any(
                call.kwargs.get("csv_path") == artifacts_dir / "gtfs_stop_reality.csv"
                and call.kwargs.get("qualified_table") == '"_tmp_transit_gtfs_stop_reality_stage"'
                for call in copy_mock.call_args_list
            )
        )
