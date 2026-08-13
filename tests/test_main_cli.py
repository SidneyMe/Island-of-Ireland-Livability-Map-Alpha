from __future__ import annotations

import io
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

import main


class MainCliTests(TestCase):
    def test_default_invocation_starts_local_server(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}):
            exit_code = main.main([])

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="full",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )

    def test_parse_args_defaults_to_serve(self) -> None:
        args = main.parse_args([])

        self.assertEqual(args.command, "serve")
        self.assertEqual(args.profile, "full")
        self.assertEqual(args.host, main.DEFAULT_SERVER_HOST)
        self.assertEqual(args.port, main.DEFAULT_SERVER_PORT)

    def test_old_serve_alias_is_rejected(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            main.parse_args(["--render"])

        self.assertEqual(ctx.exception.code, 2)

    def test_import_subcommand_dispatches_refresh_local_import(self) -> None:
        refresh_mock = mock.Mock()
        fake_precompute_module = SimpleNamespace(refresh_local_import=refresh_mock)

        with mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}):
            exit_code = main.main(["import"])

        self.assertEqual(exit_code, 0)
        refresh_mock.assert_called_once_with()

    def test_gtfs_status_subcommand_dispatches_status_runner(self) -> None:
        status_row = SimpleNamespace(
            feed_id="nta",
            path="/tmp/current.zip",
            sha256="abc",
            downloaded_at_utc="2026-05-26T00:00:00Z",
            checked_at_utc="2026-05-26T01:00:00Z",
            etag="etag",
            last_modified="Tue, 26 May 2026 10:00:00 GMT",
            calendar_min_date="20260501",
            calendar_max_date="20261231",
            days_until_calendar_end=200,
            cache_age_hours=1.5,
            freshness_decision="fresh",
            network_request=False,
        )
        gtfs_status_mock = mock.Mock(return_value=[status_row])
        fake_runner_module = SimpleNamespace(gtfs_status=gtfs_status_mock)

        with (
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main(["gtfs", "status"])

        self.assertEqual(exit_code, 0)
        gtfs_status_mock.assert_called_once_with(force_refresh=False)
        print_mock.assert_any_call("GTFS static feed status", flush=True)

    def test_gtfs_refresh_subcommand_forwards_force_flag(self) -> None:
        refresh_gtfs_mock = mock.Mock(return_value=[])
        fake_runner_module = SimpleNamespace(refresh_gtfs=refresh_gtfs_mock)

        with (
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main(["gtfs", "refresh", "--force-gtfs-refresh"])

        self.assertEqual(exit_code, 0)
        refresh_gtfs_mock.assert_called_once_with(force_refresh=True)
        print_mock.assert_any_call("Starting GTFS static feed refresh...", flush=True)
        print_mock.assert_any_call("GTFS static feed refresh complete", flush=True)

    def test_transit_subcommand_forwards_flags(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_runner_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main(
                [
                    "transit",
                    "--force-transit-refresh",
                    "--auto-refresh-gtfs",
                    "--force-gtfs-refresh",
                ]
            )

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=True,
            auto_refresh_gtfs=True,
            force_gtfs_refresh=True,
        )
        print_mock.assert_any_call("Starting GTFS transit refresh...", flush=True)
        print_mock.assert_any_call("GTFS transit refresh complete -> transit-reality-123", flush=True)

    def test_scheduled_refresh_runs_transit_with_gtfs_auto_refresh(self) -> None:
        workflow = Path(".github/workflows/scheduled_refresh.yml").read_text(encoding="utf-8")

        self.assertIn("python main.py transit --auto-refresh-gtfs", workflow)

    def test_ci_frontend_bundle_check_detects_untracked_dist_files(self) -> None:
        workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

        self.assertIn("git ls-files --error-unmatch static/dist/app.css static/dist/app.js", workflow)
        self.assertIn("git status --porcelain -- static/dist", workflow)

    def test_precompute_subcommand_forwards_profile_and_noise_flags(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-test")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}):
            exit_code = main.main(
                [
                    "precompute",
                    "--profile",
                    "test",
                    "--force-precompute",
                    "--explain",
                    "--auto-refresh-import",
                    "--refresh-noise-artifact",
                    "--force-noise-artifact",
                    "--reimport-noise-source",
                    "--force-noise-all",
                    "--noise-accurate",
                ]
            )

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="test",
            force_precompute=True,
            auto_refresh_import=True,
            force_noise_artifact=True,
            reimport_noise_source=True,
            force_noise_all=True,
            noise_accurate=True,
            require_active_noise_artifact=False,
            refresh_noise_artifact=True,
            explain=True,
        )

    def test_precompute_requires_active_noise_artifact_conflicts_with_build_flags(self) -> None:
        for flag in (
            "--refresh-noise-artifact",
            "--reimport-noise-source",
            "--force-noise-artifact",
            "--force-noise-all",
        ):
            with self.subTest(flag=flag):
                with (
                    mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
                    self.assertRaises(SystemExit) as ctx,
                ):
                    main.main(["precompute", "--require-active-noise-artifact", flag])

                self.assertEqual(ctx.exception.code, 2)
                self.assertIn(
                    "--require-active-noise-artifact cannot be combined with",
                    stderr.getvalue(),
                )

    def test_serve_subcommand_overrides_profile_host_and_port(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:9000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}):
            exit_code = main.main(
                ["serve", "--profile", "dev", "--host", "0.0.0.0", "--port", "9000"]
            )

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="dev",
            host="0.0.0.0",
            port=9000,
        )
