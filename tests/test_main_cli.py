from __future__ import annotations

import io
import sys
from types import SimpleNamespace
from unittest import TestCase, mock

import main


class MainCliTests(TestCase):
    def test_force_transit_refresh_requires_refresh_transit(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--force-transit-refresh"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--force-transit-refresh requires --refresh-transit",
            stderr.getvalue(),
        )

    def test_auto_refresh_gtfs_requires_refresh_transit(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--auto-refresh-gtfs"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--auto-refresh-gtfs requires --refresh-transit",
            stderr.getvalue(),
        )

    def test_force_gtfs_refresh_requires_gtfs_or_transit_refresh(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--force-gtfs-refresh"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--force-gtfs-refresh requires --refresh-gtfs or --refresh-transit",
            stderr.getvalue(),
        )

    def test_status_requires_refresh_gtfs(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--status"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--status requires --refresh-gtfs",
            stderr.getvalue(),
        )

    def test_force_precompute_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--force-precompute"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--force-precompute requires --precompute, --precompute-dev, or --precompute-test",
            stderr.getvalue(),
        )

    def test_explain_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--explain"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--explain requires --precompute, --precompute-dev, or --precompute-test",
            stderr.getvalue(),
        )

    def test_auto_refresh_import_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--auto-refresh-import"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--auto-refresh-import requires --precompute, --precompute-dev, or --precompute-test",
            stderr.getvalue(),
        )

    def test_precompute_flags_are_mutually_exclusive(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--precompute", "--precompute-dev"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--precompute, --precompute-dev, and --precompute-test are mutually exclusive",
            stderr.getvalue(),
        )

    def test_serve_profiles_are_mutually_exclusive(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve", "--serve-dev"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--serve/--render, --serve-dev/--render-dev, and --serve-test/--render-test are mutually exclusive",
            stderr.getvalue(),
        )

    def test_default_invocation_starts_local_server(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py"]),
            mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="full",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )

    def test_serve_never_triggers_gtfs_refresh(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)
        refresh_transit_mock = mock.Mock(side_effect=AssertionError("should not be called"))
        refresh_gtfs_mock = mock.Mock(side_effect=AssertionError("should not be called"))
        fake_runner_module = SimpleNamespace(
            refresh_transit=refresh_transit_mock,
            refresh_gtfs=refresh_gtfs_mock,
        )

        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve"]),
            mock.patch.dict(
                sys.modules,
                {
                    "render_from_db": fake_render_module,
                    "transit_refresh_runner": fake_runner_module,
                },
            ),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_not_called()
        refresh_gtfs_mock.assert_not_called()

    def test_precompute_dev_dispatches_dev_profile(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-dev")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--precompute-dev"]),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="dev",
            force_precompute=False,
            auto_refresh_import=False,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=False,
            require_active_noise_artifact=False,
            refresh_noise_artifact=False,
        )

    def test_precompute_dev_explain_dispatches_explain_flag(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-dev")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--precompute-dev", "--explain"]),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="dev",
            force_precompute=False,
            auto_refresh_import=False,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=False,
            require_active_noise_artifact=False,
            refresh_noise_artifact=False,
            explain=True,
        )

    def test_precompute_dev_passes_force_and_auto_refresh_flags(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-dev")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--force-precompute", "--auto-refresh-import"],
            ),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="dev",
            force_precompute=True,
            auto_refresh_import=True,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=False,
            require_active_noise_artifact=False,
            refresh_noise_artifact=False,
        )

    def test_precompute_test_dispatches_test_profile(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-test")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-test", "--force-precompute", "--auto-refresh-import"],
            ),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="test",
            force_precompute=True,
            auto_refresh_import=True,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=False,
            require_active_noise_artifact=False,
            refresh_noise_artifact=False,
        )

    def test_noise_accurate_flag_dispatches_accurate_mode(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-dev")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--noise-accurate"],
            ),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="dev",
            force_precompute=False,
            auto_refresh_import=False,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=True,
            require_active_noise_artifact=False,
            refresh_noise_artifact=False,
        )

    def test_require_active_noise_artifact_flag_dispatches(self) -> None:
        precompute_mock = mock.Mock(return_value="build-key-dev")
        fake_precompute_module = SimpleNamespace(run_precompute=precompute_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--require-active-noise-artifact"],
            ),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        precompute_mock.assert_called_once_with(
            profile="dev",
            force_precompute=False,
            auto_refresh_import=False,
            force_noise_artifact=False,
            reimport_noise_source=False,
            force_noise_all=False,
            noise_accurate=False,
            require_active_noise_artifact=True,
            refresh_noise_artifact=False,
        )

    def test_require_active_noise_artifact_conflicts_with_refresh(self) -> None:
        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--require-active-noise-artifact", "--refresh-noise-artifact"],
            ),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--require-active-noise-artifact cannot be combined with --refresh-noise-artifact",
            stderr.getvalue(),
        )

    def test_require_active_noise_artifact_conflicts_with_reimport(self) -> None:
        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--require-active-noise-artifact", "--reimport-noise-source"],
            ),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--require-active-noise-artifact cannot be combined with --reimport-noise-source",
            stderr.getvalue(),
        )

    def test_require_active_noise_artifact_conflicts_with_force_noise_artifact(self) -> None:
        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--require-active-noise-artifact", "--force-noise-artifact"],
            ),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--require-active-noise-artifact cannot be combined with --force-noise-artifact",
            stderr.getvalue(),
        )

    def test_require_active_noise_artifact_conflicts_with_force_noise_all(self) -> None:
        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--precompute-dev", "--require-active-noise-artifact", "--force-noise-all"],
            ),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--require-active-noise-artifact cannot be combined with --force-noise-all",
            stderr.getvalue(),
        )

    def test_serve_dev_dispatches_dev_profile(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve-dev"]),
            mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="dev",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )

    def test_serve_test_dispatches_test_profile(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve-test"]),
            mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="test",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )

    def test_refresh_transit_dispatches_precompute_helper(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_runner_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--refresh-transit"]),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=False,
            auto_refresh_gtfs=False,
            force_gtfs_refresh=False,
        )
        self.assertEqual(
            print_mock.call_args_list,
            [
                mock.call("Starting GTFS transit refresh...", flush=True),
                mock.call("GTFS transit refresh complete -> transit-reality-123", flush=True),
            ],
        )

    def test_refresh_transit_passes_force_flag(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_runner_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--refresh-transit", "--force-transit-refresh"],
            ),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=True,
            auto_refresh_gtfs=False,
            force_gtfs_refresh=False,
        )
        self.assertEqual(
            print_mock.call_args_list,
            [
                mock.call("Starting GTFS transit refresh...", flush=True),
                mock.call("GTFS transit refresh complete -> transit-reality-123", flush=True),
            ],
        )

    def test_refresh_gtfs_dispatches_gtfs_refresh_runner(self) -> None:
        refresh_gtfs_mock = mock.Mock(return_value=[])
        fake_runner_module = SimpleNamespace(refresh_gtfs=refresh_gtfs_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--refresh-gtfs"]),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_gtfs_mock.assert_called_once_with(force_refresh=False)
        self.assertEqual(
            print_mock.call_args_list,
            [
                mock.call("Starting GTFS static feed refresh...", flush=True),
                mock.call("GTFS static feed refresh complete", flush=True),
            ],
        )

    def test_refresh_gtfs_status_dispatches_status_runner(self) -> None:
        status_row = SimpleNamespace(
            feed_id="tfi_gtfs_all",
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
            mock.patch.object(sys, "argv", ["main.py", "--refresh-gtfs", "--status"]),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print") as print_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        gtfs_status_mock.assert_called_once_with(force_refresh=False)
        self.assertEqual(
            print_mock.call_args_list[0],
            mock.call("GTFS static feed status", flush=True),
        )

    def test_refresh_transit_auto_refresh_gtfs_passes_flags(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_runner_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--refresh-transit", "--auto-refresh-gtfs"]),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print"),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=False,
            auto_refresh_gtfs=True,
            force_gtfs_refresh=False,
        )

    def test_refresh_transit_force_gtfs_refresh_passes_flags(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_runner_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--refresh-transit", "--auto-refresh-gtfs", "--force-gtfs-refresh"],
            ),
            mock.patch.dict(sys.modules, {"transit_refresh_runner": fake_runner_module}),
            mock.patch("builtins.print"),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=False,
            auto_refresh_gtfs=True,
            force_gtfs_refresh=True,
        )

    def test_render_dev_alias_dispatches_dev_profile(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--render-dev"]),
            mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="dev",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )

    def test_render_test_alias_dispatches_test_profile(self) -> None:
        render_mock = mock.Mock(return_value="http://127.0.0.1:8000/")
        fake_render_module = SimpleNamespace(run_render_from_db=render_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--render-test"]),
            mock.patch.dict(sys.modules, {"render_from_db": fake_render_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="test",
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )
