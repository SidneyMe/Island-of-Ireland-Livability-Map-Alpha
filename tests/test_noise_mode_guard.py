"""
Phase 10: Regression guard tests.

Prove that NOISE_MODE=artifact causes the livability build to:
  - never call noise.loader.iter_noise_candidate_rows (or any variant)
  - never open raw noise dataset paths
  - succeed even when noise_datasets/ does not exist

These tests are the acceptance criterion for Milestone A.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch


def _make_fake_manifest(artifact_hash="guard-test-hash"):
    from noise_artifacts.manifest import ArtifactManifest
    return ArtifactManifest(
        artifact_hash=artifact_hash,
        artifact_type="resolved",
        status="complete",
        manifest_json={},
        created_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
    )


class NoiseModeArtifactGuardTests(TestCase):
    """
    Core regression guard: _noise_rows must not call the legacy loader in artifact mode.
    """

    def test_artifact_mode_never_calls_iter_noise_candidate_rows(self) -> None:
        """iter_noise_candidate_rows must never be called when NOISE_MODE=artifact."""
        import noise.loader as noise_loader
        from precompute._rows import _ArtifactNoiseReference, _noise_rows

        engine = MagicMock()
        fake_manifest = _make_fake_manifest()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch.object(
                noise_loader,
                "iter_noise_candidate_rows",
                side_effect=AssertionError(
                    "iter_noise_candidate_rows must not be called in artifact mode"
                ),
            ) as guarded_loader:
                with patch(
                    "noise_artifacts.manifest.get_active_artifact",
                    return_value=fake_manifest,
                ):
                    result = _noise_rows(engine, datetime.now(timezone.utc))

        # Verify: loader was not called, result is the sentinel
        guarded_loader.assert_not_called()
        self.assertIsInstance(result, _ArtifactNoiseReference)

    def test_artifact_mode_never_calls_iter_noise_candidate_rows_cached(self) -> None:
        """The cached variant must also not be called."""
        import noise.loader as noise_loader
        from precompute._rows import _noise_rows

        engine = MagicMock()
        fake_manifest = _make_fake_manifest()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch.object(
                noise_loader,
                "iter_noise_candidate_rows_cached",
                side_effect=AssertionError("cached loader must not be called in artifact mode"),
            ) as guarded_cached:
                with patch(
                    "noise_artifacts.manifest.get_active_artifact",
                    return_value=fake_manifest,
                ):
                    _noise_rows(engine, datetime.now(timezone.utc))

        guarded_cached.assert_not_called()

    def test_artifact_mode_never_calls_load_noise_rows(self) -> None:
        """The top-level load_noise_rows must also not be called."""
        import noise.loader as noise_loader
        from precompute._rows import _noise_rows

        engine = MagicMock()
        fake_manifest = _make_fake_manifest()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch.object(
                noise_loader,
                "load_noise_rows",
                side_effect=AssertionError("load_noise_rows must not be called in artifact mode"),
            ) as guarded_load:
                with patch(
                    "noise_artifacts.manifest.get_active_artifact",
                    return_value=fake_manifest,
                ):
                    _noise_rows(engine, datetime.now(timezone.utc))

        guarded_load.assert_not_called()

    def test_artifact_mode_succeeds_without_noise_datasets_dir(self) -> None:
        """
        _noise_rows must succeed even when the noise_datasets/ directory is absent.
        In artifact mode the function must return a sentinel without touching any file.
        """
        from precompute._rows import _ArtifactNoiseReference, _noise_rows

        engine = MagicMock()
        fake_manifest = _make_fake_manifest()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch(
                "noise_artifacts.manifest.get_active_artifact",
                return_value=fake_manifest,
            ):
                # Must NOT raise FileNotFoundError or similar even without raw files
                result = _noise_rows(engine, datetime.now(timezone.utc))

        self.assertIsInstance(result, _ArtifactNoiseReference)

    def test_artifact_mode_never_opens_zipfile(self) -> None:
        """zipfile.ZipFile must not be opened for noise datasets in artifact mode."""
        import zipfile
        from precompute._rows import _noise_rows

        engine = MagicMock()
        fake_manifest = _make_fake_manifest()

        def _assert_no_noise_zip(path, *args, **kwargs):
            path_str = str(path or "")
            if any(kw in path_str.lower() for kw in ("noise", "noise_round", "noisedata")):
                raise AssertionError(
                    f"zipfile.ZipFile opened with noise path in artifact mode: {path}"
                )
            return MagicMock()

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch.object(zipfile.ZipFile, "__init__", _assert_no_noise_zip):
                with patch(
                    "noise_artifacts.manifest.get_active_artifact",
                    return_value=fake_manifest,
                ):
                    _noise_rows(engine, datetime.now(timezone.utc))

    def test_dispatch_noise_never_starts_thread_in_artifact_mode(self) -> None:
        """Background noise thread must never be started in artifact mode."""
        import threading
        from precompute._rows import _dispatch_noise_in_background

        new_threads = []
        original_start = threading.Thread.start

        def _capture_start(self):
            if "noise" in (self.name or "").lower():
                new_threads.append(self)
            original_start(self)

        with patch("precompute._rows.NOISE_MODE", "artifact"):
            with patch.object(threading.Thread, "start", _capture_start):
                _dispatch_noise_in_background()

        self.assertEqual(
            new_threads, [],
            "No noise background thread should start in artifact mode"
        )

    def test_publish_pipeline_does_not_call_stage_for_sentinel(self) -> None:
        """
        When _publish_noise_polygons receives a sentinel, it must not call
        _stage_noise_candidate_rows (which would load raw data).
        """
        from precompute._rows import _ArtifactNoiseReference
        from db_postgis import writes as _writes

        sentinel = _ArtifactNoiseReference("res-guard-hash", None)
        conn = MagicMock()
        conn.execute.return_value.rowcount = 0

        with patch.object(_writes, "_stage_noise_candidate_rows",
                          side_effect=AssertionError("must not stage in artifact mode")) as mock_stage:
            with patch.object(_writes, "copy_noise_artifact_to_noise_polygons", return_value=0):
                with patch.object(_writes, "_update_noise_summary_from_database"):
                    _writes._publish_noise_polygons(
                        conn,
                        noise_rows=sentinel,
                        build_key="bk",
                        config_hash="ch",
                        import_fingerprint="if",
                        render_hash="rh",
                        created_at=datetime.now(timezone.utc),
                        study_area_wgs84=None,
                        summary_json={},
                    )

        mock_stage.assert_not_called()

    def test_publish_pipeline_calls_direct_copy_for_sentinel(self) -> None:
        """When the sentinel is detected, copy_noise_artifact_to_noise_polygons is called."""
        from precompute._rows import _ArtifactNoiseReference
        from db_postgis import writes as _writes

        sentinel = _ArtifactNoiseReference("res-guard-hash", None)
        conn = MagicMock()

        with patch.object(_writes, "copy_noise_artifact_to_noise_polygons",
                          return_value=42) as mock_copy:
            with patch.object(_writes, "_update_noise_summary_from_database"):
                _writes._publish_noise_polygons(
                    conn,
                    noise_rows=sentinel,
                    build_key="bk",
                    config_hash="ch",
                    import_fingerprint="if",
                    render_hash="rh",
                    created_at=datetime.now(timezone.utc),
                    study_area_wgs84=None,
                    summary_json={},
                )

        mock_copy.assert_called_once()
        kwargs = mock_copy.call_args.kwargs
        self.assertEqual(kwargs["noise_resolved_hash"], "res-guard-hash")
        self.assertEqual(kwargs["build_key"], "bk")
