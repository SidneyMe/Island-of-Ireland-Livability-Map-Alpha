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
            "coarse_vector_resolutions_m": [20000, 10000, 5000],
            "fine_surface_enabled": True,
            "fine_resolutions_m": [2500, 1000, 500, 250, 100, 50],
            "surface_zoom_breaks": [
                {"min_zoom": 18, "resolution_m": 50},
                {"min_zoom": 16, "resolution_m": 100},
                {"min_zoom": 15, "resolution_m": 250},
                {"min_zoom": 14, "resolution_m": 500},
                {"min_zoom": 13, "resolution_m": 1000},
                {"min_zoom": 12, "resolution_m": 2500},
                {"min_zoom": 10, "resolution_m": 5000},
                {"min_zoom": 8, "resolution_m": 10000},
                {"min_zoom": 0, "resolution_m": 20000},
            ],
            "surface_tile_url_template": "/tiles/surface/{resolution_m}/{z}/{x}/{y}.png",
            "inspect_url": "/api/inspect",
            "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
            "category_colors": config.CATEGORY_COLORS,
            "default_zoom": 6,
            "max_zoom": 19,
            "pmtiles_url": "/tiles/livability.pmtiles",
            "transport_reality_enabled": True,
            "service_deserts_enabled": True,
            "transport_reality_download_url": "/exports/transport-reality.zip",
            "transit_analysis_date": "2026-04-14",
            "transit_analysis_window_days": 30,
            "transit_service_desert_window_days": 7,
        }

    def get_surface_tile(self, *, resolution_m: int, z: int, x: int, y: int) -> bytes:
        self.calls.append(("surface", resolution_m, z, x, y))
        return b"\x89PNG\r\n\x1a\nfake"

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, object]:
        self.calls.append(("inspect", lat, lon, zoom))
        return {
            "resolution_m": 50,
            "visible_resolution_m": 250,
            "valid_land": True,
            "effective_area_ratio": 1.0,
            "counts": {"shops": 2},
            "component_scores": {"shops": 10.0, "transport": 0.0, "healthcare": 0.0, "parks": 0.0},
            "total_score": 10.0,
        }


class _DisabledFineService(_FakeService):
    def get_runtime(self) -> dict[str, object]:
        payload = super().get_runtime()
        payload["fine_surface_enabled"] = False
        payload["fine_resolutions_m"] = []
        payload.pop("surface_tile_url_template", None)
        payload.pop("inspect_url", None)
        return payload

    def get_surface_tile(self, *, resolution_m: int, z: int, x: int, y: int) -> bytes:
        del resolution_m, z, x, y
        raise RuntimeError("Fine surface runtime is unavailable for this build.")

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, object]:
        del lat, lon, zoom
        raise RuntimeError("Fine surface runtime is unavailable for this build.")


class _ServerHarness:
    def __init__(
        self,
        service: _FakeService,
        *,
        pmtiles_path: Path,
        static_dir: Path,
        profile: str = "full",
    ) -> None:
        self.httpd = serve_from_db.create_http_server(
            service=service,
            profile=profile,
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

    def test_export_endpoint_serves_transport_reality_zip(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            export_dir = Path(tmp_name) / "exports"
            export_dir.mkdir()
            export_path = export_dir / "transport-reality.zip"
            export_path.write_bytes(b"zip-bytes")
            with mock.patch.object(serve_from_db, "EXPORTS_DIR", export_dir):
                with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                    with urlopen(base_url + "/exports/transport-reality.zip") as response:
                        payload = response.read()
                        content_type = response.headers.get_content_type()

        self.assertEqual(content_type, "application/zip")
        self.assertEqual(payload, b"zip-bytes")

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

    def test_dev_profile_pmtiles_route_is_profile_specific(self) -> None:
        body = b"PMTILES_DEV_BYTES" * 16
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name), pmtiles_bytes=body)
            with _ServerHarness(
                _FakeService(),
                pmtiles_path=pmtiles_path,
                static_dir=static_dir,
                profile="dev",
            ) as base_url:
                with urlopen(base_url + "/tiles/livability-dev.pmtiles") as response:
                    payload = response.read()

        self.assertEqual(payload, body)

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

    def test_surface_tile_endpoint_returns_png(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/tiles/surface/250/15/3/4.png") as response:
                    payload = response.read()
                    content_type = response.headers.get_content_type()

        self.assertEqual(content_type, "image/png")
        self.assertEqual(payload, b"\x89PNG\r\n\x1a\nfake")
        self.assertIn(("surface", 250, 15, 3, 4), service.calls)

    def test_inspect_endpoint_returns_payload(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/api/inspect?lat=53.4&lon=-6.2&zoom=15") as response:
                    payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload["resolution_m"], 50)
        self.assertEqual(payload["visible_resolution_m"], 250)
        self.assertIn(("inspect", 53.4, -6.2, 15.0), service.calls)

    def test_surface_tile_endpoint_returns_404_when_fine_surface_disabled(self) -> None:
        service = _DisabledFineService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with self.assertRaises(HTTPError) as ctx:
                    urlopen(base_url + "/tiles/surface/250/15/3/4.png")

        self.assertEqual(ctx.exception.code, 404)

    def test_inspect_endpoint_returns_404_when_fine_surface_disabled(self) -> None:
        service = _DisabledFineService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with self.assertRaises(HTTPError) as ctx:
                    urlopen(base_url + "/api/inspect?lat=53.4&lon=-6.2&zoom=15")

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
        serve_mock.assert_called_once_with(
            host=config.DEFAULT_SERVER_HOST,
            port=config.DEFAULT_SERVER_PORT,
            profile="full",
        )

    def test_runtime_service_get_runtime_payload(self) -> None:
        manifest = {
            "build_key": "build-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "transport_reality_download_url": "/exports/transport-reality.zip",
                "transit_analysis_date": "2026-04-14",
                "transit_analysis_window_days": 30,
                "transit_service_desert_window_days": 7,
                "fine_resolutions_m": [2500, 1000, 500, 250, 100, 50],
                "surface_zoom_breaks": [
                    [18, 50],
                    [16, 100],
                    [15, 250],
                    [14, 500],
                    [13, 1000],
                    [12, 2500],
                    [10, 5000],
                    [8, 10000],
                    [0, 20000],
                ],
            },
        }
        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=True),
            mock.patch.object(serve_from_db._surface, "build_surface_shell_hash", return_value="shell-hash-123"),
            mock.patch.object(serve_from_db._surface, "surface_shell_dir", return_value=Path("surface-shell")),
            mock.patch.object(serve_from_db._surface, "surface_score_dir", return_value=Path("surface-scores")),
            mock.patch.object(serve_from_db._surface, "surface_tile_dir", return_value=Path("surface-tiles")),
            mock.patch.object(serve_from_db._surface, "ensure_surface_tile_cache_manifest", return_value={}),
            mock.patch.object(serve_from_db._surface, "surface_analysis_ready", return_value=True),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertEqual(payload["grid_sizes_m"], [20000, 10000, 5000])
        self.assertEqual(payload["build_profile"], "full")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability.pmtiles")
        self.assertEqual(payload["default_zoom"], 6)
        self.assertEqual(payload["max_zoom"], 19)
        self.assertTrue(payload["fine_surface_enabled"])
        self.assertEqual(payload["fine_resolutions_m"], [2500, 1000, 500, 250, 100, 50])
        self.assertEqual(payload["inspect_url"], "/api/inspect")
        self.assertTrue(payload["transport_reality_enabled"])
        self.assertTrue(payload["service_deserts_enabled"])
        self.assertEqual(payload["transport_reality_download_url"], "/exports/transport-reality.zip")
        self.assertEqual(payload["transit_analysis_date"], "2026-04-14")

    def test_runtime_service_omits_fine_surface_fields_when_unavailable(self) -> None:
        manifest = {
            "build_key": "build-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "transport_reality_enabled": False,
                "service_deserts_enabled": False,
            },
        }
        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=True),
            mock.patch.object(serve_from_db._surface, "build_surface_shell_hash", return_value="shell-hash-123"),
            mock.patch.object(serve_from_db._surface, "surface_shell_dir", return_value=Path("surface-shell")),
            mock.patch.object(serve_from_db._surface, "surface_score_dir", return_value=Path("surface-scores")),
            mock.patch.object(serve_from_db._surface, "surface_tile_dir", return_value=Path("surface-tiles")),
            mock.patch.object(serve_from_db._surface, "ensure_surface_tile_cache_manifest", return_value={}),
            mock.patch.object(serve_from_db._surface, "surface_analysis_ready", return_value=False),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertFalse(payload["fine_surface_enabled"])
        self.assertEqual(payload["fine_resolutions_m"], [])
        self.assertNotIn("surface_tile_url_template", payload)
        self.assertNotIn("inspect_url", payload)
        self.assertFalse(payload["transport_reality_enabled"])
        self.assertFalse(payload["service_deserts_enabled"])

    def test_runtime_service_uses_dev_config_hash_and_coarse_only_payload(self) -> None:
        manifest = {
            "build_key": "build-dev-123",
            "reach_hash": "reach-hash-dev",
            "score_hash": "score-hash-dev",
            "render_hash": "render-hash-dev",
            "summary_json": {
                "build_profile": "dev",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "fine_resolutions_m": [],
                "surface_zoom_breaks": [
                    [10, 5000],
                    [8, 10000],
                    [0, 20000],
                ],
            },
        }
        expected_config_hash = config.build_config_hashes(profile="dev").config_hash
        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest) as runtime_mock,
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine, profile="dev").get_runtime()

        self.assertEqual(runtime_mock.call_args.kwargs["config_hash"], expected_config_hash)
        self.assertEqual(payload["build_profile"], "dev")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability-dev.pmtiles")
        self.assertEqual(payload["fine_resolutions_m"], [])
        self.assertEqual(
            payload["surface_zoom_breaks"],
            [
                {"min_zoom": 10, "resolution_m": 5000},
                {"min_zoom": 8, "resolution_m": 10000},
                {"min_zoom": 0, "resolution_m": 20000},
            ],
        )
        self.assertFalse(payload["fine_surface_enabled"])
        self.assertNotIn("surface_tile_url_template", payload)
        self.assertNotIn("inspect_url", payload)

    def test_main_serve_flag_starts_local_app(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve"]),
            mock.patch("render_from_db.run_render_from_db", return_value="http://127.0.0.1:8000/") as render_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="full",
            host=config.DEFAULT_SERVER_HOST,
            port=config.DEFAULT_SERVER_PORT,
        )

    def test_main_serve_dev_flag_starts_dev_local_app(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve-dev"]),
            mock.patch("render_from_db.run_render_from_db", return_value="http://127.0.0.1:8000/") as render_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="dev",
            host=config.DEFAULT_SERVER_HOST,
            port=config.DEFAULT_SERVER_PORT,
        )
