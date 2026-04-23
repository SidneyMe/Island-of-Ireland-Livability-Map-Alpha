from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import time
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
                "postgresql+psycopg://user:secret@localhost:5432/gis?connect_timeout=15",
            )

    def test_database_url_converts_postgresql_scheme(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:secret@localhost:5432/gis"},
            clear=True,
        ):
            self.assertEqual(
                config.database_url(),
                "postgresql+psycopg://user:secret@localhost:5432/gis?connect_timeout=15",
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
                "postgresql+psycopg://map+user:pa+ss%2Fword@db.local:6543/gis+database?connect_timeout=15",
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

    def test_database_url_preserves_explicit_connect_timeout(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "DATABASE_URL": (
                    "postgresql://user:secret@localhost:5432/gis"
                    "?sslmode=require&connect_timeout=9"
                )
            },
            clear=True,
        ):
            self.assertEqual(
                config.database_url(),
                (
                    "postgresql+psycopg://user:secret@localhost:5432/gis"
                    "?sslmode=require&connect_timeout=9"
                ),
            )


class ConfigHashTests(TestCase):
    def test_transit_feed_configs_only_include_nta_and_translink(self) -> None:
        self.assertEqual(
            [feed.feed_id for feed in config.transit_feed_configs()],
            ["nta", "translink"],
        )

    def test_transit_config_hash_inputs_exclude_locallink(self) -> None:
        with mock.patch.object(config, "hash_dict", return_value="transit-hash-123") as hash_mock:
            transit_hash = config.transit_config_hash()

        self.assertEqual(transit_hash, "transit-hash-123")
        payload = hash_mock.call_args.args[0]
        self.assertEqual(
            [feed["feed_id"] for feed in payload["feeds"]],
            ["nta", "translink"],
        )

    def test_build_transit_reality_state_excludes_locallink_feed(self) -> None:
        def _fingerprint(path: Path) -> str:
            return f"fp-{path.name}"

        with mock.patch.object(config, "transit_feed_fingerprint", side_effect=_fingerprint):
            state = config.build_transit_reality_state(analysis_date=date(2026, 4, 22))

        self.assertEqual(
            [feed.feed_id for feed in state.feed_states],
            ["nta", "translink"],
        )
        self.assertEqual(
            state.feed_fingerprints,
            {
                "nta": "fp-nta_gtfs.zip",
                "translink": "fp-translink_gtfs.zip",
            },
        )

    def test_transit_reality_algorithm_version_changes_transit_config_hash(self) -> None:
        with mock.patch.object(config, "TRANSIT_REALITY_ALGO_VERSION", 1):
            previous_hash = config.transit_config_hash()
        with mock.patch.object(config, "TRANSIT_REALITY_ALGO_VERSION", 2):
            current_hash = config.transit_config_hash()

        self.assertNotEqual(previous_hash, current_hash)

    def test_pmtiles_schema_version_changes_render_hash_only(self) -> None:
        with mock.patch.object(config, "PMTILES_SCHEMA_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "PMTILES_SCHEMA_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.surface_shell_hash, current_hashes.surface_shell_hash)
        self.assertEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.render_hash, current_hashes.render_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)

    def test_caps_changes_invalidate_score_hash_but_reuse_surface_shell_hash(self) -> None:
        with mock.patch.object(config, "CAPS", {"shops": 5, "transport": 5, "healthcare": 3, "parks": 2}):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "CAPS", {"shops": 7, "transport": 5, "healthcare": 3, "parks": 2}):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.surface_shell_hash, current_hashes.surface_shell_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)

    def test_build_profiles_produce_distinct_config_hashes_and_build_keys(self) -> None:
        full_hashes = config.build_config_hashes(profile="full")
        dev_hashes = config.build_config_hashes(profile="dev")
        test_hashes = config.build_config_hashes(profile="test")
        full_build = config.build_hashes_for_import("import-fingerprint-123", profile="full")
        dev_build = config.build_hashes_for_import("import-fingerprint-123", profile="dev")
        test_build = config.build_hashes_for_import("import-fingerprint-123", profile="test")

        self.assertEqual(full_hashes.geo_hash, dev_hashes.geo_hash)
        self.assertEqual(full_hashes.reach_hash, dev_hashes.reach_hash)
        self.assertEqual(full_hashes.score_hash, dev_hashes.score_hash)
        self.assertNotEqual(full_hashes.config_hash, dev_hashes.config_hash)
        self.assertNotEqual(full_hashes.render_hash, dev_hashes.render_hash)
        self.assertNotEqual(full_hashes.geo_hash, test_hashes.geo_hash)
        self.assertNotEqual(full_hashes.reach_hash, test_hashes.reach_hash)
        self.assertNotEqual(full_hashes.score_hash, test_hashes.score_hash)
        self.assertNotEqual(full_hashes.config_hash, test_hashes.config_hash)
        self.assertEqual(full_build.geo_hash, dev_build.geo_hash)
        self.assertEqual(full_build.reach_hash, dev_build.reach_hash)
        self.assertEqual(full_build.score_hash, dev_build.score_hash)
        self.assertNotEqual(full_build.build_key, dev_build.build_key)
        self.assertNotEqual(full_build.build_key, test_build.build_key)
        self.assertEqual(full_build.build_profile, "full")
        self.assertEqual(dev_build.build_profile, "dev")
        self.assertEqual(test_build.build_profile, "test")

    def test_overture_dataset_signature_changes_invalidate_reach_hash(self) -> None:
        with mock.patch.object(config, "overture_dataset_signature", return_value="sig-a"), mock.patch.object(
            config,
            "overture_dataset_info",
            return_value={"last_release": "2026-04-15.0"},
        ):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "overture_dataset_signature", return_value="sig-b"), mock.patch.object(
            config,
            "overture_dataset_info",
            return_value={"last_release": "2026-04-15.0"},
        ):
            current_hashes = config.build_config_hashes()

        self.assertNotEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)

    def test_overture_category_map_signature_changes_invalidate_reach_hash(self) -> None:
        with (
            mock.patch.object(config, "overture_category_map_signature", return_value="map-a"),
            mock.patch.object(config, "overture_dataset_signature", return_value="sig-fixed"),
            mock.patch.object(
                config,
                "overture_dataset_info",
                return_value={"last_release": "2026-04-15.0"},
            ),
        ):
            previous_hashes = config.build_config_hashes()
            previous_build_hashes = config.build_hashes_for_import("import-fingerprint-123")
        with (
            mock.patch.object(config, "overture_category_map_signature", return_value="map-b"),
            mock.patch.object(config, "overture_dataset_signature", return_value="sig-fixed"),
            mock.patch.object(
                config,
                "overture_dataset_info",
                return_value={"last_release": "2026-04-15.0"},
            ),
        ):
            current_hashes = config.build_config_hashes()
            current_build_hashes = config.build_hashes_for_import("import-fingerprint-123")

        self.assertNotEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_build_hashes.reach_hash, current_build_hashes.reach_hash)
        self.assertNotEqual(previous_build_hashes.score_hash, current_build_hashes.score_hash)

    def test_amenity_merge_algorithm_version_changes_reach_hash(self) -> None:
        with mock.patch.object(config, "AMENITY_MERGE_ALGO_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "AMENITY_MERGE_ALGO_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertNotEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)

    def test_shop_tier_threshold_changes_invalidate_score_hash(self) -> None:
        with mock.patch.object(config, "SHOP_MALL_MIN_FOOTPRINT_M2", 8_000.0):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "SHOP_MALL_MIN_FOOTPRINT_M2", 9_000.0):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.surface_shell_hash, current_hashes.surface_shell_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)

    def test_garden_is_not_configured_as_park_source(self) -> None:
        self.assertNotIn("garden", config.TAGS["parks"]["leisure"])
        self.assertNotIn("garden", config.OVERTURE_PARK_NEIGHBOURHOOD_VALUES)


class SurfaceResolutionTests(TestCase):
    def test_surface_resolution_ladder_matches_architecture(self) -> None:
        self.assertEqual(config.COARSE_VECTOR_RESOLUTIONS_M, [20000, 10000, 5000])
        self.assertEqual(config.FINE_RESOLUTIONS_M, [2500, 1000, 500, 250, 100, 50])
        self.assertEqual(config.CANONICAL_BASE_RESOLUTION_M, 50)

    def test_dev_profile_has_coarse_only_surface_settings(self) -> None:
        dev_settings = config.build_profile_settings("dev")

        self.assertEqual(list(dev_settings.coarse_vector_resolutions_m), [20000, 10000, 5000])
        self.assertEqual(list(dev_settings.fine_resolutions_m), [])
        self.assertEqual(
            list(dev_settings.surface_zoom_breaks),
            [(10, 5000), (8, 10000), (0, 20000)],
        )
        self.assertFalse(dev_settings.fine_surface_enabled)

    def test_test_profile_matches_full_surface_settings_and_uses_cork_city_bbox(self) -> None:
        full_settings = config.build_profile_settings("full")
        test_settings = config.build_profile_settings("test")

        self.assertEqual(config.normalize_build_profile("test"), "test")
        self.assertEqual(list(test_settings.coarse_vector_resolutions_m), list(full_settings.coarse_vector_resolutions_m))
        self.assertEqual(list(test_settings.fine_resolutions_m), list(full_settings.fine_resolutions_m))
        self.assertEqual(list(test_settings.surface_zoom_breaks), list(full_settings.surface_zoom_breaks))
        self.assertTrue(test_settings.fine_surface_enabled)
        self.assertEqual(test_settings.study_area_kind, "bbox")
        self.assertIsNone(test_settings.study_area_county_name)
        self.assertEqual(test_settings.study_area_bbox_wgs84, (-8.55, 51.87, -8.41, 51.93))
        self.assertEqual(config.pmtiles_filename("test"), "livability-test.pmtiles")
        self.assertEqual(config.precompute_flag_for_profile("test"), "--precompute-test")

    def test_test_profile_bbox_changes_geo_hash_from_previous_county_setting(self) -> None:
        bbox_hashes = config.build_config_hashes(profile="test")
        legacy_test_settings = config.BuildProfileSettings(
            name="test",
            coarse_vector_resolutions_m=(20_000, 10_000, 5_000),
            fine_resolutions_m=(2_500, 1_000, 500, 250, 100, 50),
            surface_zoom_breaks=(
                (18, 50),
                (16, 100),
                (15, 250),
                (14, 500),
                (13, 1_000),
                (12, 2_500),
                (10, 5_000),
                (8, 10_000),
                (0, 20_000),
            ),
            fine_surface_enabled=True,
            study_area_kind="county",
            study_area_county_name="CORK",
        )

        with mock.patch.dict(config._BUILD_PROFILE_SETTINGS, {"test": legacy_test_settings}):
            legacy_hashes = config.build_config_hashes(profile="test")

        self.assertNotEqual(bbox_hashes.geo_hash, legacy_hashes.geo_hash)
        self.assertNotEqual(bbox_hashes.config_hash, legacy_hashes.config_hash)

    def test_resolution_for_zoom_uses_fixed_breaks(self) -> None:
        expectations = {
            0: 20000,
            7: 20000,
            8: 10000,
            9: 10000,
            10: 5000,
            11: 5000,
            12: 2500,
            13: 1000,
            14: 500,
            15: 250,
            16: 100,
            17: 100,
            18: 50,
            19: 50,
        }
        for zoom, resolution in expectations.items():
            with self.subTest(zoom=zoom):
                self.assertEqual(config.resolution_for_zoom(zoom), resolution)

    def test_zoom_bounds_for_resolution_match_runtime_contract(self) -> None:
        self.assertEqual(config.zoom_bounds_for_resolution(20000), (0, 7))
        self.assertEqual(config.zoom_bounds_for_resolution(10000), (8, 9))
        self.assertEqual(config.zoom_bounds_for_resolution(5000), (10, 11))
        self.assertEqual(config.zoom_bounds_for_resolution(2500), (12, 12))
        self.assertEqual(config.zoom_bounds_for_resolution(1000), (13, 13))
        self.assertEqual(config.zoom_bounds_for_resolution(500), (14, 14))
        self.assertEqual(config.zoom_bounds_for_resolution(250), (15, 15))
        self.assertEqual(config.zoom_bounds_for_resolution(100), (16, 17))
        self.assertEqual(config.zoom_bounds_for_resolution(50), (18, 19))

    def test_dev_resolution_for_zoom_uses_coarse_only_breaks(self) -> None:
        expectations = {
            0: 20000,
            7: 20000,
            8: 10000,
            9: 10000,
            10: 5000,
            19: 5000,
        }
        for zoom, resolution in expectations.items():
            with self.subTest(zoom=zoom):
                self.assertEqual(config.resolution_for_zoom(zoom, profile="dev"), resolution)

    def test_grid_geometry_schema_version_changes_score_hash(self) -> None:
        with mock.patch.object(config, "GRID_GEOMETRY_SCHEMA_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "GRID_GEOMETRY_SCHEMA_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertEqual(previous_hashes.geo_hash, current_hashes.geo_hash)
        self.assertEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)

    def test_coastal_cleanup_algorithm_version_changes_geo_hash_and_downstream_hashes(self) -> None:
        with mock.patch.object(config, "COASTAL_CLEANUP_ALGORITHM_VERSION", 1):
            previous_hashes = config.build_config_hashes()
        with mock.patch.object(config, "COASTAL_CLEANUP_ALGORITHM_VERSION", 2):
            current_hashes = config.build_config_hashes()

        self.assertNotEqual(previous_hashes.geo_hash, current_hashes.geo_hash)
        self.assertNotEqual(previous_hashes.reach_hash, current_hashes.reach_hash)
        self.assertNotEqual(previous_hashes.score_hash, current_hashes.score_hash)
        self.assertNotEqual(previous_hashes.render_hash, current_hashes.render_hash)
        self.assertNotEqual(previous_hashes.config_hash, current_hashes.config_hash)


class SurfaceThreadEnvTests(TestCase):
    def test_surface_thread_env_accepts_positive_integer(self) -> None:
        with mock.patch.dict(os.environ, {"LIVABILITY_SURFACE_THREADS": "5"}, clear=False):
            self.assertEqual(config._optional_positive_int_env("LIVABILITY_SURFACE_THREADS"), 5)

    def test_surface_thread_env_treats_blank_as_unset(self) -> None:
        with mock.patch.dict(os.environ, {"LIVABILITY_SURFACE_THREADS": "   "}, clear=False):
            self.assertIsNone(config._optional_positive_int_env("LIVABILITY_SURFACE_THREADS"))

    def test_surface_thread_env_rejects_invalid_values(self) -> None:
        for raw_value in ("0", "-1", "nope"):
            with self.subTest(raw_value=raw_value):
                with mock.patch.dict(os.environ, {"LIVABILITY_SURFACE_THREADS": raw_value}, clear=False):
                    with self.assertRaisesRegex(RuntimeError, "positive integer"):
                        config._optional_positive_int_env("LIVABILITY_SURFACE_THREADS")


class WalkgraphBinResolutionTests(TestCase):
    def _write_walkgraph_bin(self, base_dir: Path, relative_path: str) -> str:
        candidate = base_dir / "walkgraph" / "target" / Path(relative_path)
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_text("walkgraph")
        return str(candidate)

    def test_default_walkgraph_bin_prefers_windows_release_exe(self) -> None:
        with TemporaryDirectory() as tmp_name:
            base_dir = Path(tmp_name)
            expected = self._write_walkgraph_bin(base_dir, "release/walkgraph.exe")
            self._write_walkgraph_bin(base_dir, "release/walkgraph")

            with (
                mock.patch.object(config, "BASE_DIR", base_dir),
                mock.patch.object(config.os, "name", "nt"),
            ):
                self.assertEqual(config._default_walkgraph_bin(), expected)

    def test_default_walkgraph_bin_prefers_posix_release_binary(self) -> None:
        with TemporaryDirectory() as tmp_name:
            base_dir = Path(tmp_name)
            expected = self._write_walkgraph_bin(base_dir, "release/walkgraph")
            self._write_walkgraph_bin(base_dir, "release/walkgraph.exe")

            with (
                mock.patch.object(config, "BASE_DIR", base_dir),
                mock.patch.object(config.os, "name", "posix"),
            ):
                self.assertEqual(config._default_walkgraph_bin(), expected)

    def test_default_walkgraph_bin_returns_debug_binary_when_release_missing(self) -> None:
        with TemporaryDirectory() as tmp_name:
            base_dir = Path(tmp_name)
            expected = self._write_walkgraph_bin(base_dir, "debug/walkgraph")

            with (
                mock.patch.object(config, "BASE_DIR", base_dir),
                mock.patch.object(config.os, "name", "posix"),
            ):
                self.assertEqual(config._default_walkgraph_bin(), expected)

    def test_default_walkgraph_bin_finds_alternate_suffix_when_only_one_exists(self) -> None:
        with TemporaryDirectory() as tmp_name:
            base_dir = Path(tmp_name)
            expected = self._write_walkgraph_bin(base_dir, "release/walkgraph.exe")

            with (
                mock.patch.object(config, "BASE_DIR", base_dir),
                mock.patch.object(config.os, "name", "posix"),
            ):
                self.assertEqual(config._default_walkgraph_bin(), expected)

    def test_default_walkgraph_bin_falls_back_to_path_lookup(self) -> None:
        with TemporaryDirectory() as tmp_name:
            base_dir = Path(tmp_name)

            with (
                mock.patch.object(config, "BASE_DIR", base_dir),
                mock.patch.object(config.os, "name", "nt"),
            ):
                self.assertEqual(config._default_walkgraph_bin(), "walkgraph")


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


class ExtractFingerprintTests(TestCase):
    def test_extract_fingerprint_ignores_path_and_mtime_for_identical_content(self) -> None:
        with TemporaryDirectory() as tmp_name:
            temp_dir = Path(tmp_name)
            first = temp_dir / "first.osm.pbf"
            second = temp_dir / "second.osm.pbf"

            first.write_bytes(b"same extract bytes")
            time.sleep(0.01)
            second.write_bytes(b"same extract bytes")

            first_fingerprint = config.extract_fingerprint(first)
            second_fingerprint = config.extract_fingerprint(second)

            self.assertEqual(first_fingerprint, second_fingerprint)

    def test_extract_fingerprint_changes_when_content_changes(self) -> None:
        with TemporaryDirectory() as tmp_name:
            temp_dir = Path(tmp_name)
            first = temp_dir / "first.osm.pbf"
            second = temp_dir / "second.osm.pbf"

            first.write_bytes(b"extract version one")
            second.write_bytes(b"extract version two")

            self.assertNotEqual(
                config.extract_fingerprint(first),
                config.extract_fingerprint(second),
            )

    def test_extract_fingerprint_reuses_cached_hash_when_file_is_unchanged(self) -> None:
        with TemporaryDirectory() as tmp_name:
            temp_dir = Path(tmp_name)
            cache_path = temp_dir / "osm_extract_fingerprint_cache.json"
            extract = temp_dir / "sample.osm.pbf"
            extract.write_bytes(b"stable extract bytes")

            with mock.patch.object(
                config,
                "OSM_EXTRACT_FINGERPRINT_CACHE_PATH",
                cache_path,
            ):
                first = config.extract_fingerprint(extract)
                with mock.patch.object(
                    config,
                    "_content_hash",
                    side_effect=AssertionError("cache should avoid rehashing"),
                ):
                    second = config.extract_fingerprint(extract)

            self.assertEqual(first, second)
            self.assertTrue(cache_path.exists())

    def test_extract_fingerprint_recomputes_when_file_changes(self) -> None:
        with TemporaryDirectory() as tmp_name:
            temp_dir = Path(tmp_name)
            cache_path = temp_dir / "osm_extract_fingerprint_cache.json"
            extract = temp_dir / "sample.osm.pbf"
            extract.write_bytes(b"extract version one")

            with mock.patch.object(
                config,
                "OSM_EXTRACT_FINGERPRINT_CACHE_PATH",
                cache_path,
            ):
                first = config.extract_fingerprint(extract)
                time.sleep(0.01)
                extract.write_bytes(b"extract version two with more bytes")
                with mock.patch.object(
                    config,
                    "_content_hash",
                    wraps=config._content_hash,
                ) as content_hash_mock:
                    second = config.extract_fingerprint(extract)

            self.assertNotEqual(first, second)
            content_hash_mock.assert_called_once_with(extract)

    def test_extract_fingerprint_recomputes_when_cache_is_corrupt(self) -> None:
        with TemporaryDirectory() as tmp_name:
            temp_dir = Path(tmp_name)
            cache_path = temp_dir / "osm_extract_fingerprint_cache.json"
            extract = temp_dir / "sample.osm.pbf"
            extract.write_bytes(b"stable extract bytes")

            with mock.patch.object(
                config,
                "OSM_EXTRACT_FINGERPRINT_CACHE_PATH",
                cache_path,
            ):
                first = config.extract_fingerprint(extract)
                cache_path.write_text("{not-json", encoding="utf-8")
                with mock.patch.object(
                    config,
                    "_content_hash",
                    wraps=config._content_hash,
                ) as content_hash_mock:
                    second = config.extract_fingerprint(extract)

            self.assertEqual(first, second)
            content_hash_mock.assert_called_once_with(extract)
