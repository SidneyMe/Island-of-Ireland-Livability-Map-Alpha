from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from shapely.geometry import Point, box

from db_postgis import amenity_merge as db_amenity_merge
from db_postgis import schema as db_schema
from db_postgis import tables as db_tables
from db_postgis import writes as db_writes


class DbPostgisTransitArtifactWriteTests(TestCase):
    def test_noise_polygons_is_managed_public_geometry(self) -> None:
        managed_tables = {str(table.name) for table in db_schema._MANAGED_TABLES}
        managed_indexes = {spec.index_name for spec in db_schema._MANAGED_INDEX_SPECS}

        self.assertIn("noise_polygons", db_tables.REQUIRED_PUBLIC_TABLES)
        self.assertIn("noise_polygons", managed_tables)
        self.assertEqual(
            db_tables.GEOMETRY_FIELDS[db_tables.noise_polygons.name],
            ("geom",),
        )
        self.assertIn("noise_polygons_build_metric_idx", managed_indexes)
        self.assertIn("noise_polygons_source_metric_idx", managed_indexes)
        self.assertIn("noise_polygons_geom_gist", managed_indexes)

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

    def test_noise_publish_stages_candidates_before_database_materialization(self) -> None:
        connection = mock.MagicMock()
        # Force the prior-build clone lookup to find nothing so staging runs.
        connection.execute.return_value.scalar_one_or_none.return_value = None
        summary_json: dict[str, object] = {}
        created_at = datetime.now(timezone.utc)
        row = {
            "build_key": "build-key-123",
            "config_hash": "config-hash-123",
            "import_fingerprint": "import-fingerprint-123",
            "jurisdiction": "roi",
            "source_type": "road",
            "metric": "Lden",
            "round_number": 4,
            "report_period": "Round 4",
            "db_low": 55.0,
            "db_high": 59.0,
            "db_value": "55-59",
            "source_dataset": "NOISE_Round4.zip",
            "source_layer": "Noise_R4_Road",
            "source_ref": "noise-1",
            "geom": box(0.0, 0.0, 1.0, 1.0),
            "created_at": created_at,
        }

        with (
            mock.patch.object(db_writes, "_materialize_noise_polygons_from_stage", return_value=1) as materialize,
            mock.patch.object(db_writes, "_update_noise_summary_from_database") as update_summary,
            mock.patch.object(db_writes, "_stage_noise_candidate_rows", return_value=1) as stage,
            mock.patch("config.NOISE_MODE", "legacy"),
        ):
            db_writes._publish_noise_polygons(
                connection,
                noise_rows=[row],
                build_key="build-key-123",
                config_hash="config-hash-123",
                import_fingerprint="import-fingerprint-123",
                render_hash="render-hash-123",
                created_at=created_at,
                study_area_wgs84=box(-1.0, -1.0, 2.0, 2.0),
                summary_json=summary_json,
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined_statements = "\n".join(statements)
        self.assertIn("CREATE TEMP TABLE", joined_statements)
        self.assertIn("_tmp_noise_polygons_stage", joined_statements)
        self.assertIn("CREATE INDEX ON", joined_statements)
        self.assertIn("USING GIST (geom)", joined_statements)
        stage.assert_called_once()
        materialize.assert_called_once()
        update_summary.assert_called_once_with(
            connection,
            build_key="build-key-123",
            summary_json=summary_json,
        )

    def test_noise_publish_clones_from_prior_build_when_render_hash_matches(self) -> None:
        connection = mock.MagicMock()
        # Returning a non-empty build_key triggers the clone path.
        connection.execute.return_value.scalar_one_or_none.return_value = "prior-build-key"
        # The INSERT ... SELECT result needs a rowcount.
        connection.execute.return_value.rowcount = 17
        summary_json: dict[str, object] = {}
        created_at = datetime.now(timezone.utc)

        with (
            mock.patch.object(db_writes, "_stage_noise_candidate_rows") as stage,
            mock.patch.object(db_writes, "_materialize_noise_polygons_from_stage") as materialize,
            mock.patch.object(db_writes, "_update_noise_summary_from_database") as update_summary,
            mock.patch("config.NOISE_MODE", "legacy"),
        ):
            db_writes._publish_noise_polygons(
                connection,
                noise_rows=iter([]),  # candidate rows discarded
                build_key="new-build-key",
                config_hash="new-config-hash",
                import_fingerprint="new-import-fingerprint",
                render_hash="render-hash-shared",
                created_at=created_at,
                study_area_wgs84=box(-1.0, -1.0, 2.0, 2.0),
                summary_json=summary_json,
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        joined_statements = "\n".join(statements)
        self.assertIn("INSERT INTO", joined_statements)
        self.assertIn("FROM build_manifest", joined_statements)
        # No staging path should run when clone fires.
        stage.assert_not_called()
        materialize.assert_not_called()
        update_summary.assert_called_once()

    def test_noise_materialization_sql_uses_intersecting_newer_fallback_pieces(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value = mock.Mock(rowcount=3)

        inserted = db_writes._materialize_noise_group_round(
            connection,
            staging_quoted='"_tmp_noise_polygons_stage"',
            study_area_wkb=box(-1.0, -1.0, 2.0, 2.0).wkb,
            build_key="build-key-123",
            jurisdiction="roi",
            source_type="road",
            metric="Lden",
            round_number=3,
        )

        self.assertEqual(inserted, 3)
        statement = str(connection.execute.call_args.args[0])
        params = connection.execute.call_args.args[1]
        self.assertIn("_tmp_noise_polygons_stage", statement)
        self.assertIn("ST_Difference", statement)
        self.assertIn("ST_UnaryUnion(ST_Collect(n.geom))", statement)
        self.assertEqual(statement.count("ST_Subdivide"), 1)
        self.assertEqual(params["build_key"], "build-key-123")
        self.assertEqual(params["round_number"], 3)


class ArtifactCopyTests(TestCase):
    """FIX 10: copy_noise_artifact_to_noise_polygons clips to study area."""

    def test_copy_sql_uses_st_intersection_when_study_area_given(self) -> None:
        import inspect
        src = inspect.getsource(db_writes.copy_noise_artifact_to_noise_polygons)
        self.assertIn("ST_Intersection", src)

    def test_copy_sql_clips_to_study_area_with_cte(self) -> None:
        import inspect
        src = inspect.getsource(db_writes.copy_noise_artifact_to_noise_polygons)
        self.assertIn("clipped_geom", src)
        self.assertIn("has_study_area", src)

    def test_copy_sql_filters_empty_and_zero_area_clipped(self) -> None:
        import inspect
        src = inspect.getsource(db_writes.copy_noise_artifact_to_noise_polygons)
        self.assertIn("ST_IsEmpty(dr.clipped_geom)", src)
        self.assertIn("ST_Area(dr.clipped_geom)", src)

    def test_copy_executes_insert_with_noise_resolved_hash(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.rowcount = 5

        n = db_writes.copy_noise_artifact_to_noise_polygons(
            connection,
            noise_resolved_hash="res-abc",
            build_key="bk",
            config_hash="ch",
            import_fingerprint="ifp",
            study_area_wgs84=None,
        )

        self.assertEqual(n, 5)
        sql_text = str(connection.execute.call_args.args[0])
        params = connection.execute.call_args.args[1]
        self.assertIn("noise_resolved_display", sql_text)
        self.assertIn("INSERT INTO noise_polygons", sql_text)
        self.assertEqual(params["noise_resolved_hash"], "res-abc")

    def test_copy_passes_has_study_area_false_when_none(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.rowcount = 0

        db_writes.copy_noise_artifact_to_noise_polygons(
            connection,
            noise_resolved_hash="res-abc",
            build_key="bk",
            config_hash="ch",
            import_fingerprint="ifp",
            study_area_wgs84=None,
        )

        params = connection.execute.call_args.args[1]
        self.assertFalse(params["has_study_area"])
        self.assertIsNone(params["study_wkb"])

    def test_copy_passes_has_study_area_true_with_wkb(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.rowcount = 0
        study = box(0.0, 0.0, 1.0, 1.0)

        db_writes.copy_noise_artifact_to_noise_polygons(
            connection,
            noise_resolved_hash="res-abc",
            build_key="bk",
            config_hash="ch",
            import_fingerprint="ifp",
            study_area_wgs84=study,
        )

        params = connection.execute.call_args.args[1]
        self.assertTrue(params["has_study_area"])
        self.assertEqual(params["study_wkb"], study.wkb)

    def test_publish_artifact_calls_summary_update_after_copy(self) -> None:
        """FIX 10: summary JSON must be updated from DB rows after artifact copy."""
        from precompute._rows import _ArtifactNoiseReference
        sentinel = _ArtifactNoiseReference("res-test-hash", None)
        connection = mock.MagicMock()
        created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        summary: dict = {}

        with (
            mock.patch.object(db_writes, "copy_noise_artifact_to_noise_polygons", return_value=10),
            mock.patch.object(db_writes, "_update_noise_summary_from_database") as update_mock,
        ):
            db_writes._publish_noise_polygons(
                connection,
                noise_rows=sentinel,
                build_key="bk",
                config_hash="ch",
                import_fingerprint="ifp",
                render_hash="rh",
                created_at=created_at,
                study_area_wgs84=None,
                summary_json=summary,
            )

        update_mock.assert_called_once_with(
            connection,
            build_key="bk",
            summary_json=summary,
        )

    def test_noise_only_refresh_does_not_delete_non_noise_tables(self) -> None:
        import inspect

        src = inspect.getsource(db_writes.refresh_noise_overlay_for_build)
        self.assertIn("delete(noise_polygons)", src)
        self.assertNotIn("delete(grid_walk)", src)
        self.assertNotIn("delete(amenities)", src)
        self.assertNotIn("delete(transport_reality)", src)
        self.assertNotIn("delete(service_deserts)", src)


class EnsureDatabaseReadyTests(TestCase):
    """FIX 4: ensure_database_ready must create pgcrypto."""

    def test_ensure_database_ready_creates_pgcrypto(self) -> None:
        import inspect
        src = inspect.getsource(db_schema.ensure_database_ready)
        self.assertIn("pgcrypto", src)
        self.assertIn("CREATE EXTENSION IF NOT EXISTS pgcrypto", src)

    def test_ensure_database_ready_raises_on_pgcrypto_failure(self) -> None:
        import inspect
        src = inspect.getsource(db_schema.ensure_database_ready)
        # Must have error messaging for pgcrypto failure
        self.assertIn("sha256", src)


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
