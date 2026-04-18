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

    def test_force_precompute_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--force-precompute"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--force-precompute requires --precompute or --precompute-dev",
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
            "--auto-refresh-import requires --precompute or --precompute-dev",
            stderr.getvalue(),
        )

    def test_precompute_and_precompute_dev_are_mutually_exclusive(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--precompute", "--precompute-dev"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--precompute and --precompute-dev are mutually exclusive", stderr.getvalue())

    def test_serve_and_serve_dev_are_mutually_exclusive(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve", "--serve-dev"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--serve/--render and --serve-dev/--render-dev are mutually exclusive",
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

    def test_refresh_transit_dispatches_precompute_helper(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_precompute_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(sys, "argv", ["main.py", "--refresh-transit"]),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=False,
            refresh_download=True,
        )

    def test_refresh_transit_passes_force_flag(self) -> None:
        refresh_transit_mock = mock.Mock(return_value="transit-reality-123")
        fake_precompute_module = SimpleNamespace(refresh_transit=refresh_transit_mock)

        with (
            mock.patch.object(
                sys,
                "argv",
                ["main.py", "--refresh-transit", "--force-transit-refresh"],
            ),
            mock.patch.dict(sys.modules, {"precompute": fake_precompute_module}),
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        refresh_transit_mock.assert_called_once_with(
            force_refresh=True,
            refresh_download=True,
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
