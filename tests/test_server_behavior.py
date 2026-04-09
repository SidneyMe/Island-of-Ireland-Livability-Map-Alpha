from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest import TestCase, mock

import config
import main
import render_from_db
import serve_from_db


class _FakeService:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def get_runtime(self) -> dict[str, object]:
        self.calls.append(("runtime",))
        return {
            "build_key": "build-123",
            "map_center": {"lat": 53.4, "lon": -6.2},
            "grid_sizes_m": [20000, 10000, 5000],
            "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
            "category_colors": config.CATEGORY_COLORS,
            "default_zoom": 6,
            "pmtiles_url": "/tiles/livability.pmtiles",
        }


class _ServerHarness:
    def __init__(self, service: _FakeService, *, pmtiles_path: Path, static_dir: Path) -> None:
        self.httpd = serve_from_db.create_http_server(
            service=service,
            host="127.0.0.1",
            port=0,
            static_dir=static_dir,
            pmtiles_path=pmtiles_path,
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self) -> str:
        self.thread.start()
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.httpd.shutdown()
        self.thread.join(timeout=5)
        self.httpd.server_close()


def _make_fixture(tmp: Path, pmtiles_bytes: bytes = b"PMTILESFAKE" * 32) -> tuple[Path, Path]:
    static_dir = tmp / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_bytes(b"<!doctype html><title>livability</title>")
    pmtiles_path = tmp / "livability.pmtiles"
    pmtiles_path.write_bytes(pmtiles_bytes)
    return static_dir, pmtiles_path


class LocalServerEndpointTests(TestCase):
    def test_runtime_endpoint_returns_payload(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/api/runtime") as response:
                    payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload["build_key"], "build-123")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability.pmtiles")
        self.assertEqual(service.calls, [("runtime",)])

    def test_root_serves_static_index_html(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/") as response:
                    body = response.read()
                    content_type = response.headers.get_content_type()

        self.assertEqual(content_type, "text/html")
        self.assertIn(b"livability", body.lower())

    def test_pmtiles_full_get(self) -> None:
        body = b"PMTILES_BODY_BYTES" * 100
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name), pmtiles_bytes=body)
            with _ServerHarness(_FakeService(), pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/tiles/livability.pmtiles") as response:
                    payload = response.read()
                    accept_ranges = response.headers.get("Accept-Ranges")

        self.assertEqual(payload, body)
        self.assertEqual(accept_ranges, "bytes")

    def test_pmtiles_range_request(self) -> None:
        body = bytes(range(256)) * 4  # 1024 bytes
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name), pmtiles_bytes=body)
            with _ServerHarness(_FakeService(), pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                request = Request(base_url + "/tiles/livability.pmtiles", headers={"Range": "bytes=10-19"})
                with urlopen(request) as response:
                    payload = response.read()
                    status = response.status
                    content_range = response.headers.get("Content-Range")

        self.assertEqual(status, 206)
        self.assertEqual(payload, body[10:20])
        self.assertEqual(content_range, f"bytes 10-19/{len(body)}")

    def test_pmtiles_open_ended_range(self) -> None:
        body = b"x" * 200
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name), pmtiles_bytes=body)
            with _ServerHarness(_FakeService(), pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                request = Request(base_url + "/tiles/livability.pmtiles", headers={"Range": "bytes=50-"})
                with urlopen(request) as response:
                    payload = response.read()
                    self.assertEqual(response.status, 206)

        self.assertEqual(payload, body[50:])

    def test_unknown_route_returns_404_json(self) -> None:
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(_FakeService(), pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with self.assertRaises(HTTPError) as ctx:
                    urlopen(base_url + "/api/walk-grid?resolution_m=5000&bbox=-6,53,-5,54")

        self.assertEqual(ctx.exception.code, 404)

    def test_create_http_server_raises_when_pmtiles_missing(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            static_dir = tmp / "static"
            static_dir.mkdir()
            (static_dir / "index.html").write_bytes(b"<!doctype html><title>livability</title>")
            missing_pmtiles_path = tmp / "livability.pmtiles"
            self.assertFalse(missing_pmtiles_path.exists())

            with self.assertRaisesRegex(RuntimeError, "PMTiles archive not found"):
                serve_from_db.create_http_server(
                    service=_FakeService(),
                    host="127.0.0.1",
                    port=0,
                    static_dir=static_dir,
                    pmtiles_path=missing_pmtiles_path,
                )


class RenderAndCliTests(TestCase):
    def test_render_wrapper_invokes_serve(self) -> None:
        with mock.patch.object(
            render_from_db, "serve_livability_app", return_value="http://127.0.0.1:8000/"
        ) as serve_mock:
            url = render_from_db.run_render_from_db()

        self.assertEqual(url, "http://127.0.0.1:8000/")
        serve_mock.assert_called_once()

    def test_runtime_service_get_runtime_payload(self) -> None:
        manifest = {
            "build_key": "build-123",
            "summary_json": {
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
            },
        }
        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertEqual(payload["grid_sizes_m"], [20000, 10000, 5000])
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability.pmtiles")
        self.assertEqual(payload["default_zoom"], 6)

    def test_main_serve_flag_starts_local_app(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve"]),
            mock.patch("render_from_db.run_render_from_db", return_value="http://127.0.0.1:8000/") as render_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            host=config.DEFAULT_SERVER_HOST,
            port=config.DEFAULT_SERVER_PORT,
        )
