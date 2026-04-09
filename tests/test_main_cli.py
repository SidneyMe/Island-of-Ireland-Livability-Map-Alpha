from __future__ import annotations

import io
import sys
from types import SimpleNamespace
from unittest import TestCase, mock

import main


class MainCliTests(TestCase):
    def test_force_precompute_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--force-precompute"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--force-precompute requires --precompute", stderr.getvalue())

    def test_auto_refresh_import_requires_precompute(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--auto-refresh-import"]),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
            self.assertRaises(SystemExit) as ctx,
        ):
            main.main()

        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--auto-refresh-import requires --precompute", stderr.getvalue())

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
            host=main.DEFAULT_SERVER_HOST,
            port=main.DEFAULT_SERVER_PORT,
        )
