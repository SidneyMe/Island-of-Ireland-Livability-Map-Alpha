"""
Phase 9A tests: NOISE_MODE=artifact sentinel pattern and direct-copy pipeline.
No live DB — all DB operations use mock objects.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch


def _make_fake_manifest(artifact_hash="res123"):
    from noise_artifacts.manifest import ArtifactManifest
    return ArtifactManifest(
        artifact_hash=artifact_hash,
        artifact_type="resolved",
        status="complete",
        manifest_json={},
        created_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
    )


class NoiseModeConfigTests(TestCase):

    def test_noise_mode_defaults_to_legacy(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            import importlib
            import config
            importlib.reload(config)
            self.assertEqual(config.NOISE_MODE, "legacy")

    def test_noise_mode_accepts_artifact(self) -> None:
        with patch.dict(os.environ, {"NOISE_MODE": "artifact"}):
            from config import _noise_mode
            self.assertEqual(_noise_mode(), "artifact")

    def test_noise_mode_accepts_legacy(self) -> None:
        with patch.dict(os.environ, {"NOISE_MODE": "legacy"}):
            from config import _noise_mode
            self.assertEqual(_noise_mode(), "legacy")

    def test_noise_mode_rejects_invalid_value(self) -> None:
        with patch.dict(os.environ, {"NOISE_MODE": "banana"}):
            from config import _noise_mode
            with self.assertRaises(ValueError):
                _noise_mode()

    def test_noise_topology_grid_metres_has_default(self) -> None:
        import config
        self.assertIsInstance(config.NOISE_TOPOLOGY_GRID_METRES, float)
        self.assertGreater(config.NOISE_TOPOLOGY_GRID_METRES, 0.0)

    def test_noise_dissolve_tile_size_metres_has_default(self) -> None:
        import config
        self.assertIsInstance(config.NOISE_DISSOLVE_TILE_SIZE_METRES, float)
        self.assertGreater(config.NOISE_DISSOLVE_TILE_SIZE_METRES, 0.0)


class ArtifactSentinelTests(TestCase):

    def test_artifact_noise_reference_is_importable(self) -> None:
        from precompute._rows import _ArtifactNoiseReference
        ref = _ArtifactNoiseReference("res123", None)
        self.assertEqual(ref.noise_resolved_hash, "res123")

    def test_artifact_noise_reference_len_is_zero(self) -> None:
        from precompute._rows import _ArtifactNoiseReference
        ref = _ArtifactNoiseReference("res123", None)
        self.assertEqual(len(ref), 0)

    def test_noise_rows_returns_sentinel_in_artifact_mode(self) -> None:
        from precompute._rows import _ArtifactNoiseReference, _noise_rows

        fake_manifest = _make_fake_manifest("res123")
        engine = MagicMock()

        with patch.dict(os.environ, {"NOISE_MODE": "artifact"}):
            with patch("precompute._rows.NOISE_MODE", "artifact"):
                with patch(
                    "noise_artifacts.manifest.get_active_artifact",
                    return_value=fake_manifest,
                ):
                    result = _noise_rows(engine, datetime.now(timezone.utc))

        self.assertIsInstance(result, _ArtifactNoiseReference)
        self.assertEqual(result.noise_resolved_hash, "res123")

    def test_noise_rows_raises_when_no_active_artifact(self) -> None:
        from precompute._rows import _noise_rows

        engine = MagicMock()
        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch(
                "noise_artifacts.manifest.get_active_artifact",
                return_value=None,
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    _noise_rows(engine, datetime.now(timezone.utc))
        self.assertIn("python -m noise_artifacts", str(ctx.exception))

    def test_noise_rows_uses_legacy_path_when_mode_is_legacy(self) -> None:
        """In legacy mode, _noise_rows must NOT call get_active_artifact."""
        from precompute._rows import _noise_rows

        engine = MagicMock()
        with patch("precompute._rows.NOISE_MODE", "legacy"):
            with patch(
                "noise_artifacts.manifest.get_active_artifact",
                side_effect=AssertionError("must not call in legacy mode"),
            ) as mock_ga:
                with patch("precompute._rows._await_background_noise", return_value=None):
                    with patch(
                        "precompute._rows._noise_loader.iter_noise_candidate_rows_cached",
                        return_value=iter([]),
                    ):
                        with patch("precompute._rows._publish") as mock_pub:
                            mock_pub.iter_noise_rows_impl.return_value = MagicMock(row_count=0)
                            _noise_rows(engine, datetime.now(timezone.utc))
            mock_ga.assert_not_called()

    def test_noise_processing_hash_returns_artifact_hash_after_noise_rows(self) -> None:
        from precompute._rows import _noise_rows, _noise_processing_hash

        fake_manifest = _make_fake_manifest("res-hash-abc")
        engine = MagicMock()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch(
                "noise_artifacts.manifest.get_active_artifact",
                return_value=fake_manifest,
            ):
                _noise_rows(engine, datetime.now(timezone.utc))

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            result = _noise_processing_hash()

        self.assertEqual(result, "res-hash-abc")

    def test_dispatch_background_is_noop_in_artifact_mode(self) -> None:
        import threading
        from precompute._rows import _dispatch_noise_in_background

        threads_before = set(t.ident for t in threading.enumerate())
        with patch("precompute._rows.NOISE_MODE", "artifact"):
            _dispatch_noise_in_background()
        threads_after = set(t.ident for t in threading.enumerate())
        # No new threads should have been started
        new_threads = threads_after - threads_before
        noise_threads = [
            t for t in threading.enumerate()
            if t.ident in new_threads and "noise" in (t.name or "").lower()
        ]
        self.assertEqual(noise_threads, [])


class DirectCopyFunctionTests(TestCase):

    def test_copy_function_importable(self) -> None:
        from db_postgis.writes import copy_noise_artifact_to_noise_polygons
        self.assertTrue(callable(copy_noise_artifact_to_noise_polygons))

    def test_copy_sql_uses_st_transform_to_4326(self) -> None:
        import inspect
        from db_postgis.writes import copy_noise_artifact_to_noise_polygons
        src = inspect.getsource(copy_noise_artifact_to_noise_polygons)
        self.assertIn("ST_Transform", src)
        self.assertIn("4326", src)

    def test_copy_sql_reads_from_noise_resolved_display(self) -> None:
        import inspect
        from db_postgis.writes import copy_noise_artifact_to_noise_polygons
        src = inspect.getsource(copy_noise_artifact_to_noise_polygons)
        self.assertIn("noise_resolved_display", src)

    def test_copy_sql_joins_provenance(self) -> None:
        import inspect
        from db_postgis.writes import copy_noise_artifact_to_noise_polygons
        src = inspect.getsource(copy_noise_artifact_to_noise_polygons)
        self.assertIn("noise_resolved_provenance", src)

    def test_copy_sql_inserts_into_noise_polygons(self) -> None:
        import inspect
        from db_postgis.writes import copy_noise_artifact_to_noise_polygons
        src = inspect.getsource(copy_noise_artifact_to_noise_polygons)
        self.assertIn("INSERT INTO noise_polygons", src)

    def test_publish_calls_direct_copy_for_sentinel(self) -> None:
        import inspect
        from db_postgis.writes import _publish_noise_polygons
        src = inspect.getsource(_publish_noise_polygons)
        self.assertIn("_ArtifactNoiseReference", src)
        self.assertIn("copy_noise_artifact_to_noise_polygons", src)

    def test_publish_does_not_stage_for_sentinel(self) -> None:
        """Sentinel path must return before calling _stage_noise_candidate_rows."""
        import inspect
        from db_postgis.writes import _publish_noise_polygons
        src = inspect.getsource(_publish_noise_polygons)
        # The sentinel path must 'return' before _stage_noise_candidate_rows is called
        sentinel_path = src[src.index("_ArtifactNoiseReference"):]
        first_return = sentinel_path.index("return")
        stage_pos = sentinel_path.find("_stage_noise_candidate_rows")
        # Stage must either not appear before the return, or not at all in sentinel block
        self.assertLess(first_return, stage_pos if stage_pos != -1 else len(sentinel_path))


class ConfigArtifactModeTests(TestCase):
    """FIX 7: build_config_hashes must not call raw noise functions in artifact mode."""

    def test_artifact_mode_does_not_call_noise_dataset_signature(self) -> None:
        import config
        import noise.loader as noise_loader
        from unittest.mock import patch

        with patch.dict(os.environ, {"NOISE_MODE": "artifact"}):
            with patch.object(
                noise_loader,
                "dataset_signature",
                side_effect=AssertionError("must not call dataset_signature in artifact mode"),
            ) as guarded_sig:
                with patch("config.NOISE_MODE", "artifact"):
                    config.build_config_hashes()

        guarded_sig.assert_not_called()

    def test_artifact_mode_does_not_call_noise_dataset_info(self) -> None:
        import config
        import noise.loader as noise_loader
        from unittest.mock import patch

        with patch.dict(os.environ, {"NOISE_MODE": "artifact"}):
            with patch.object(
                noise_loader,
                "dataset_info",
                side_effect=AssertionError("must not call dataset_info in artifact mode"),
            ) as guarded_info:
                with patch("config.NOISE_MODE", "artifact"):
                    config.build_config_hashes()

        guarded_info.assert_not_called()

    def test_artifact_mode_render_params_contains_noise_mode_key(self) -> None:
        """render_params must include noise_mode so the render_hash encodes the mode."""
        import inspect
        import config
        src = inspect.getsource(config.build_config_hashes)
        self.assertIn("noise_mode", src)

    def test_artifact_mode_render_params_does_not_include_raw_dataset_fields(self) -> None:
        """In artifact mode, noise_dataset_signature and noise_dataset_files are not in render_params."""
        import inspect
        import config
        src = inspect.getsource(config.build_config_hashes)
        # The conditional must gate these on legacy mode
        self.assertIn("NOISE_MODE == \"legacy\"", src)


class WorkflowArtifactHashTests(TestCase):
    """FIX 8: workflow must pass noise_artifact_hash to publish_precomputed_artifacts."""

    def test_workflow_passes_noise_artifact_hash(self) -> None:
        import inspect
        from precompute import workflow
        src = inspect.getsource(workflow.run_precompute_impl)
        self.assertIn("noise_artifact_hash", src)
        self.assertIn("resolved_artifact_hash", src)

    def test_workflow_detects_artifact_noise_reference(self) -> None:
        import inspect
        from precompute import workflow
        src = inspect.getsource(workflow.run_precompute_impl)
        self.assertIn("_ArtifactNoiseReference", src)

    def test_workflow_resolves_noise_processing_hash_once(self) -> None:
        """resolved_noise_hash must be computed once before the publish call."""
        import inspect
        from precompute import workflow
        src = inspect.getsource(workflow.run_precompute_impl)
        self.assertIn("resolved_noise_hash", src)


class MigrationArtifactRefTests(TestCase):

    def test_migration_000016_correct_down_revision(self) -> None:
        import importlib
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000016_noise_artifact_ref"
        )
        self.assertEqual(mod.down_revision, "20260427_000015")

    def test_migration_000016_adds_noise_artifact_hash_column(self) -> None:
        import importlib, inspect
        mod = importlib.import_module(
            "db_postgis.migrations.versions.20260427_000016_noise_artifact_ref"
        )
        src = inspect.getsource(mod)
        self.assertIn("noise_artifact_hash", src)

    def test_build_manifest_table_has_noise_artifact_hash_column(self) -> None:
        from db_postgis.tables import build_manifest
        col_names = {c.name for c in build_manifest.c}
        self.assertIn("noise_artifact_hash", col_names)
