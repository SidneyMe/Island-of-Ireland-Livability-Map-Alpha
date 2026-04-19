from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from shapely.geometry import Point

from db_postgis import amenity_merge as db_amenity_merge
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


class DbPostgisAmenityMergeTests(TestCase):
    def test_load_merged_source_amenity_rows_stages_temp_tables_and_indexes(self) -> None:
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        stats_payload = {
            "merge_categories_resolved": ["healthcare", "parks", "shops"],
            "osm_rows": 1,
            "overture_rows": 1,
            "osm_alias_rows": 1,
            "overture_alias_rows": 1,
            "osm_self_candidate_count": 0,
            "osm_duplicate_cluster_count": 0,
            "osm_duplicate_rows_removed": 0,
            "osm_duplicates_by_category": {},
            "candidate_pair_count": 1,
            "same_category_candidate_count": 1,
            "cross_category_candidate_count": 0,
            "candidate_pairs_by_path": {
                "same_category_near": 1,
                "same_category_alias": 0,
                "cross_category_near": 0,
                "cross_category_alias": 0,
            },
            "candidate_pairs_by_osm_category": {"shops": 1},
            "stage_ms": {
                "filter_osm_rows": 0.0,
                "generate_osm_self_candidates": 0.0,
                "collapse_osm_duplicates": 0.0,
                "generate_overture_candidates": 0.0,
                "greedy_assignment": 0.0,
            },
        }

        with (
            mock.patch.object(db_amenity_merge, "_collect_candidate_stats", return_value=stats_payload),
            mock.patch.object(
                db_amenity_merge,
                "_load_osm_self_candidate_rows",
                return_value=[],
            ),
            mock.patch.object(
                db_amenity_merge,
                "_load_collapsed_candidate_rows",
                return_value=[
                    {
                        "osm_row_id": 1,
                        "overture_row_id": 1,
                        "same_category": True,
                        "aliases_agree": True,
                        "distance_m": 5.0,
                    }
                ],
            ),
        ):
            merged_rows, merge_stats = db_amenity_merge.load_merged_source_amenity_rows(
                engine,
                [
                    {
                        "category": "shops",
                        "source": "osm_local_pbf",
                        "source_ref": "node/1",
                        "name": "Corner Shop",
                        "tags_json": {"name": "Corner Shop"},
                        "geom": Point(-6.26, 53.35),
                        "park_area_m2": 0.0,
                    }
                ],
                [
                    {
                        "category": "shops",
                        "source": "overture_places",
                        "source_ref": "ovt-1",
                        "name": "Corner Shop",
                        "brand": None,
                        "geom": Point(-6.26005, 53.35005),
                        "park_area_m2": 0.0,
                    }
                ],
                scoring_categories=["shops", "healthcare", "parks"],
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined_statements = "\n".join(statements)
        self.assertIn("CREATE TEMP TABLE", joined_statements)
        self.assertIn("ON COMMIT DROP", joined_statements)
        self.assertIn("ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 2157)", joined_statements)
        self.assertIn("CREATE INDEX ON", joined_statements)
        self.assertIn("left_osm_row_id", joined_statements)
        self.assertIn("osm_self_dedupe_radius_m", joined_statements)
        self.assertIn("same_category_near", joined_statements)
        self.assertEqual(merged_rows[0]["conflict_class"], "source_agreement")
        self.assertEqual(merge_stats["candidate_pair_count"], 1)
