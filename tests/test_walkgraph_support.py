from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import walkgraph_support


def _completed_help(*, stdout: str = "", stderr: str = "", returncode: int = 0):
    return mock.Mock(stdout=stdout, stderr=stderr, returncode=returncode)


class WalkgraphSupportTests(TestCase):
    def setUp(self) -> None:
        walkgraph_support.clear_walkgraph_probe_caches()

    def tearDown(self) -> None:
        walkgraph_support.clear_walkgraph_probe_caches()

    def test_supported_subcommand_passes(self) -> None:
        with TemporaryDirectory() as tmp_name:
            binary_path = Path(tmp_name) / "walkgraph.exe"
            binary_path.write_text("walkgraph", encoding="utf-8")

            with mock.patch.object(
                walkgraph_support.subprocess,
                "run",
                return_value=_completed_help(stdout="Commands:\n  gtfs-refresh  Refresh GTFS\n"),
            ):
                resolved_path = walkgraph_support.ensure_walkgraph_subcommand_available(
                    str(binary_path),
                    "gtfs-refresh",
                )

        self.assertEqual(resolved_path, binary_path.resolve())

    def test_missing_subcommand_raises_actionable_error(self) -> None:
        with TemporaryDirectory() as tmp_name:
            binary_path = Path(tmp_name) / "walkgraph.exe"
            binary_path.write_text("walkgraph", encoding="utf-8")

            with mock.patch.object(
                walkgraph_support.subprocess,
                "run",
                return_value=_completed_help(stdout="Commands:\n  build\n  reachability\n"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "required subcommand 'gtfs-refresh'",
                ) as exc:
                    walkgraph_support.ensure_walkgraph_subcommand_available(
                        str(binary_path),
                        "gtfs-refresh",
                    )

        self.assertIn("cargo build --release", str(exc.exception))

    def test_repo_local_binary_older_than_source_raises_rebuild_error(self) -> None:
        with TemporaryDirectory() as tmp_name:
            project_dir = Path(tmp_name) / "walkgraph"
            target_dir = project_dir / "target"
            binary_path = target_dir / "release" / "walkgraph.exe"

            binary_path.parent.mkdir(parents=True, exist_ok=True)
            binary_path.write_text("walkgraph", encoding="utf-8")

            with (
                mock.patch.object(walkgraph_support, "WALKGRAPH_TARGET_DIR", target_dir),
                mock.patch.object(
                    walkgraph_support,
                    "_repo_local_binary_is_stale",
                    return_value=True,
                ) as stale_mock,
                mock.patch.object(walkgraph_support.subprocess, "run") as run_mock,
            ):
                with self.assertRaisesRegex(RuntimeError, "older than the current Rust source"):
                    walkgraph_support.ensure_walkgraph_subcommand_available(
                        str(binary_path),
                        "gtfs-refresh",
                    )

        stale_mock.assert_called_once()
        self.assertFalse(run_mock.called)

    def test_repo_local_binary_is_stale_when_source_is_newer(self) -> None:
        with TemporaryDirectory() as tmp_name:
            target_dir = Path(tmp_name) / "target"
            binary_path = target_dir / "release" / "walkgraph.exe"

            binary_path.parent.mkdir(parents=True, exist_ok=True)
            binary_path.write_text("walkgraph", encoding="utf-8")
            binary_mtime_ns = binary_path.stat().st_mtime_ns

            with mock.patch.object(
                walkgraph_support,
                "_latest_repo_source_mtime_ns",
                return_value=binary_mtime_ns + 1,
            ):
                is_stale = walkgraph_support._repo_local_binary_is_stale(binary_path)

        self.assertTrue(is_stale)

    def test_external_path_binary_skips_repo_stale_check(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            target_dir = tmp / "walkgraph" / "target"
            external_binary = tmp / "tools" / "walkgraph.exe"

            external_binary.parent.mkdir(parents=True, exist_ok=True)
            external_binary.write_text("walkgraph", encoding="utf-8")

            with (
                mock.patch.object(walkgraph_support, "WALKGRAPH_TARGET_DIR", target_dir),
                mock.patch.object(
                    walkgraph_support,
                    "_repo_local_binary_is_stale",
                ) as stale_mock,
                mock.patch.object(walkgraph_support.shutil, "which", return_value=str(external_binary)),
                mock.patch.object(
                    walkgraph_support.subprocess,
                    "run",
                    return_value=_completed_help(stdout="Commands:\n  gtfs-refresh\n"),
                ),
            ):
                resolved_path = walkgraph_support.ensure_walkgraph_subcommand_available(
                    "walkgraph",
                    "gtfs-refresh",
                )

        self.assertEqual(resolved_path, external_binary.resolve())
        stale_mock.assert_not_called()
