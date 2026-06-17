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
            "inspect_url": "/api/inspect",
            "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
            "amenity_tier_counts": {
                "shops": {"corner": 3, "regular": 9},
                "transport": {},
                "healthcare": {"local": 1},
                "parks": {"district": 3},
            },
            "transport_subtier_counts": {"mon_sun": 2, "weekdays_only": 1},
            "transport_bus_frequency_counts": {"frequent": 2, "moderate": 1},
            "transport_flag_counts": {
                "is_unscheduled_stop": 1,
                "has_exception_only_service": 1,
                "has_any_bus_service": 3,
                "has_daily_bus_service": 2,
            },
            "transport_mode_counts": {"tram": 1, "rail": 1},
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
            "cluster_counts": {"shops": 1},
            "effective_units": {"shops": 1.5},
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
        noise_pmtiles_path: Path | None = None,
        profile: str = "full",
    ) -> None:
        self.httpd = serve_from_db.create_http_server(
            service=service,
            profile=profile,
            host="127.0.0.1",
            port=0,
            static_dir=static_dir,
            pmtiles_path=pmtiles_path,
            noise_pmtiles_path=noise_pmtiles_path,
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
    dist_dir = static_dir / "dist"
    dist_dir.mkdir()
    (dist_dir / "app.js").write_text("console.log('livability');", encoding="utf-8")
    pmtiles_path = tmp / "livability.pmtiles"
    pmtiles_path.write_bytes(pmtiles_bytes)
    return static_dir, pmtiles_path


def _make_validating_runtime_service() -> tuple[serve_from_db.RuntimeService, mock.Mock, mock.Mock]:
    service = serve_from_db.RuntimeService(mock.sentinel.engine)
    runtime = mock.Mock()
    state = mock.Mock()
    state.fine_resolutions = [2500, 1000, 500, 250, 100, 50]
    service.state = mock.Mock(return_value=state)
    service.surface_runtime = mock.Mock(return_value=runtime)
    return service, runtime, state


class LocalServerEndpointTests(TestCase):
    def test_client_disconnect_logging_suppresses_inspect_abort_noise(self) -> None:
        handler = serve_from_db.LivabilityRequestHandler.__new__(serve_from_db.LivabilityRequestHandler)
        with mock.patch("builtins.print") as print_mock:
            handler._log_client_disconnect("/api/inspect", ConnectionAbortedError("aborted"))
            handler._log_client_disconnect("/tiles/livability.pmtiles", ConnectionAbortedError("aborted"))
            handler._log_client_disconnect("/api/runtime", ConnectionAbortedError("aborted"))

        print_mock.assert_called_once_with("Client disconnected during /api/runtime: aborted")

    def test_runtime_endpoint_returns_payload(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/api/runtime") as response:
                    payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload["build_key"], "build-123")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability.pmtiles")
        self.assertNotIn("surface_tile_url_template", payload)
        self.assertEqual(service.calls, [("runtime",)])

    def test_missing_precompute_message_uses_test_flag(self) -> None:
        message = serve_from_db._missing_precompute_message(profile="test")

        self.assertIn("profile=test", message)
        self.assertIn("Run --precompute-test first.", message)

    def test_root_serves_static_index_html(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/") as response:
                    body = response.read()
                    content_type = response.headers.get_content_type()
                    cache_control = response.headers.get("Cache-Control")

        self.assertEqual(content_type, "text/html")
        self.assertIn(b"livability", body.lower())
        self.assertEqual(cache_control, "no-store")

    def test_static_assets_are_served_no_store(self) -> None:
        service = _FakeService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with urlopen(base_url + "/static/dist/app.js") as response:
                    body = response.read()
                    content_type = response.headers.get_content_type()
                    cache_control = response.headers.get("Cache-Control")

        self.assertIn("javascript", content_type)
        self.assertIn(b"livability", body)
        self.assertEqual(cache_control, "no-store")

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
                    cache_control = response.headers.get("Cache-Control")

        self.assertEqual(payload, body)
        self.assertEqual(accept_ranges, "bytes")
        self.assertEqual(cache_control, "public, max-age=3600")

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

    def test_test_profile_pmtiles_route_is_profile_specific(self) -> None:
        body = b"PMTILES_TEST_BYTES" * 16
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name), pmtiles_bytes=body)
            with _ServerHarness(
                _FakeService(),
                pmtiles_path=pmtiles_path,
                static_dir=static_dir,
                profile="test",
            ) as base_url:
                with urlopen(base_url + "/tiles/livability-test.pmtiles") as response:
                    payload = response.read()

        self.assertEqual(payload, body)

    def test_server_serves_existing_non_primary_profile_pmtiles_routes(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            static_dir, full_pmtiles_path = _make_fixture(tmp, pmtiles_bytes=b"FULL_BYTES")
            dev_pmtiles_path = tmp / "livability-dev.pmtiles"
            dev_pmtiles_path.write_bytes(b"DEV_BYTES")

            def fake_pmtiles_output_path(profile: str | None = None) -> Path:
                if profile == "dev":
                    return dev_pmtiles_path
                return full_pmtiles_path

            with (
                mock.patch.object(serve_from_db, "pmtiles_output_path", side_effect=fake_pmtiles_output_path),
                _ServerHarness(
                    _FakeService(),
                    pmtiles_path=full_pmtiles_path,
                    static_dir=static_dir,
                    profile="full",
                ) as base_url,
            ):
                with urlopen(base_url + "/tiles/livability-dev.pmtiles") as response:
                    payload = response.read()

        self.assertEqual(payload, b"DEV_BYTES")

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

    def test_noise_pmtiles_range_request(self) -> None:
        main_body = b"MAIN" * 64
        noise_body = b"NOISE_ARCHIVE" * 32
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            static_dir, pmtiles_path = _make_fixture(tmp, pmtiles_bytes=main_body)
            noise_pmtiles_path = tmp / "noise.pmtiles"
            noise_pmtiles_path.write_bytes(noise_body)
            with _ServerHarness(
                _FakeService(),
                pmtiles_path=pmtiles_path,
                noise_pmtiles_path=noise_pmtiles_path,
                static_dir=static_dir,
            ) as base_url:
                request = Request(base_url + "/tiles/noise.pmtiles", headers={"Range": "bytes=3-8"})
                with urlopen(request) as response:
                    payload = response.read()
                    status = response.status
                    content_range = response.headers.get("Content-Range")

        self.assertEqual(status, 206)
        self.assertEqual(payload, noise_body[3:9])
        self.assertEqual(content_range, f"bytes 3-8/{len(noise_body)}")

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
        self.assertEqual(payload["cluster_counts"], {"shops": 1})
        self.assertEqual(payload["effective_units"], {"shops": 1.5})
        self.assertIn(("inspect", 53.4, -6.2, 15.0), service.calls)

    def test_inspect_endpoint_rejects_invalid_coordinates(self) -> None:
        service, runtime, _state = _make_validating_runtime_service()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                bad_queries = [
                    "lat=nan&lon=-6.2",
                    "lat=inf&lon=-6.2",
                    "lat=999&lon=-6.2",
                    "lat=53.4&lon=-999",
                    "lat=53.4&lon=-6.2&zoom=-1",
                    "lat=53.4&lon=-6.2&zoom=999",
                ]
                for query in bad_queries:
                    with self.subTest(query=query):
                        with self.assertRaises(HTTPError) as ctx:
                            urlopen(base_url + "/api/inspect?" + query)
                        self.assertEqual(ctx.exception.code, 400)

        runtime.inspect.assert_not_called()

    def test_surface_tile_endpoint_returns_404_when_fine_surface_disabled(self) -> None:
        service = _DisabledFineService()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                with self.assertRaises(HTTPError) as ctx:
                    urlopen(base_url + "/tiles/surface/250/15/3/4.png")

        self.assertEqual(ctx.exception.code, 404)

    def test_surface_tile_endpoint_rejects_invalid_coordinates(self) -> None:
        service, runtime, _state = _make_validating_runtime_service()
        with TemporaryDirectory() as tmp_name:
            static_dir, pmtiles_path = _make_fixture(Path(tmp_name))
            with _ServerHarness(service, pmtiles_path=pmtiles_path, static_dir=static_dir) as base_url:
                bad_paths = [
                    "/tiles/surface/123/15/3/4.png",
                    "/tiles/surface/250/-1/3/4.png",
                    "/tiles/surface/250/20/0/0.png",
                    "/tiles/surface/250/15/3.5/4.png",
                    "/tiles/surface/250/15/32768/4.png",
                    "/tiles/surface/250/15/3/-1.png",
                    "/tiles/surface/250/15/3/32768.png",
                ]
                for path in bad_paths:
                    with self.subTest(path=path):
                        with self.assertRaises(HTTPError) as ctx:
                            urlopen(base_url + path)
                        self.assertEqual(ctx.exception.code, 400)

        runtime.render_tile.assert_not_called()

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


class RuntimeValidationTests(TestCase):
    def test_runtime_service_rejects_invalid_inspect_values_before_rendering(self) -> None:
        service, runtime, _state = _make_validating_runtime_service()
        bad_cases = [
            {"lat": float("nan"), "lon": -6.2},
            {"lat": float("inf"), "lon": -6.2},
            {"lat": 999, "lon": -6.2},
            {"lat": 53.4, "lon": -999},
            {"lat": 53.4, "lon": -6.2, "zoom": -1},
            {"lat": 53.4, "lon": -6.2, "zoom": 999},
        ]
        for kwargs in bad_cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    service.inspect(**kwargs)

        runtime.inspect.assert_not_called()

    def test_runtime_service_rejects_invalid_surface_tile_values_before_rendering(self) -> None:
        service, runtime, _state = _make_validating_runtime_service()
        bad_cases = [
            {"resolution_m": 123, "z": 15, "x": 3, "y": 4},
            {"resolution_m": 250, "z": -1, "x": 0, "y": 0},
            {"resolution_m": 250, "z": 20, "x": 0, "y": 0},
            {"resolution_m": 250, "z": 15.5, "x": 0, "y": 0},
            {"resolution_m": 250, "z": 15, "x": -1, "y": 0},
            {"resolution_m": 250, "z": 15, "x": 32768, "y": 0},
            {"resolution_m": 250, "z": 15, "x": 0, "y": -1},
            {"resolution_m": 250, "z": 15, "x": 0, "y": 32768},
            {"resolution_m": 250, "z": 15, "x": 3.5, "y": 4},
            {"resolution_m": 250, "z": 15, "x": 3, "y": 4.5},
        ]
        for kwargs in bad_cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    service.get_surface_tile(**kwargs)

        runtime.render_tile.assert_not_called()

    def test_runtime_service_allows_valid_surface_tile_and_inspect_requests(self) -> None:
        service, runtime, _state = _make_validating_runtime_service()
        runtime.render_tile.return_value = b"png"
        runtime.inspect.return_value = {"ok": True}

        tile_payload = service.get_surface_tile(resolution_m=250, z=15, x=3, y=4)
        inspect_payload = service.inspect(lat=53.4, lon=-6.2, zoom=15)

        self.assertEqual(tile_payload, b"png")
        self.assertEqual(inspect_payload, {"ok": True})
        runtime.render_tile.assert_called_once_with(resolution_m=250, z=15, x=3, y=4)
        runtime.inspect.assert_called_once_with(lat=53.4, lon=-6.2, zoom=15.0)


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
            "geo_hash": "geo-hash-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3, "regular": 9},
                    "transport": {},
                    "healthcare": {"local": 1},
                    "parks": {"district": 3},
                },
                "transport_subtier_counts": {"mon_sun": 2, "weekdays_only": 1},
                "transport_bus_frequency_counts": {"frequent": 2, "moderate": 1},
                "transport_flag_counts": {
                    "is_unscheduled_stop": 1,
                    "has_exception_only_service": 1,
                    "has_any_bus_service": 3,
                    "has_daily_bus_service": 2,
                },
                "transport_mode_counts": {"tram": 4, "rail": 6},
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "transport_reality_download_url": "/exports/transport-reality.zip",
                "noise_enabled": True,
                "noise_counts": {"roi": 5, "ni": 3},
                "noise_source_counts": {"road": 4, "rail": 2, "airport": 2, "industry": 1},
                "noise_metric_counts": {"Lden": 5, "Lnight": 3},
                "noise_band_counts": {"55-59": 6, "75+": 2},
                "transit_analysis_date": "2026-04-14",
                "transit_analysis_window_days": 30,
                "transit_service_desert_window_days": 7,
                "overture_dataset": {"last_release": "2026-04-15.0"},
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
        with TemporaryDirectory() as tmp_name:
            noise_pmtiles_path = Path(tmp_name) / "noise.pmtiles"
            noise_pmtiles_path.write_bytes(b"noise")
            with (
                mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
                mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
                mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=True),
                mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=noise_pmtiles_path),
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
        self.assertNotIn("surface_tile_url_template", payload)
        self.assertEqual(payload["inspect_url"], "/api/inspect")
        self.assertTrue(payload["transport_reality_enabled"])
        self.assertTrue(payload["service_deserts_enabled"])
        self.assertEqual(payload["transport_reality_download_url"], "/exports/transport-reality.zip")
        self.assertEqual(payload["transit_analysis_date"], "2026-04-14")
        self.assertEqual(payload["overture_dataset"], {"last_release": "2026-04-15.0"})
        self.assertEqual(payload["amenity_tier_counts"]["shops"]["corner"], 3)
        self.assertEqual(payload["transport_subtier_counts"]["mon_sun"], 2)
        self.assertEqual(payload["transport_bus_frequency_counts"], {"frequent": 2, "moderate": 1})
        self.assertEqual(payload["transport_flag_counts"]["is_unscheduled_stop"], 1)
        self.assertEqual(payload["transport_mode_counts"], {"tram": 4, "rail": 6})
        self.assertTrue(payload["noise_enabled"])
        self.assertEqual(payload["noise_pmtiles_url"], "/tiles/noise.pmtiles")
        self.assertEqual(payload["noise_counts"], {"roi": 5, "ni": 3})
        self.assertEqual(payload["noise_source_counts"]["road"], 4)
        self.assertEqual(payload["noise_source_counts"]["rail"], 2)
        self.assertEqual(payload["noise_source_counts"]["airport"], 2)
        self.assertEqual(payload["noise_source_counts"]["industry"], 1)
        self.assertEqual(payload["noise_metric_counts"], {"Lden": 5, "Lnight": 3})
        self.assertEqual(payload["noise_band_counts"]["75+"], 2)
        self.assertTrue(payload["noise_proxy_metadata"]["enabled"])
        self.assertEqual(payload["noise_proxy_metadata"]["source_id"], "noise")
        self.assertEqual(payload["noise_proxy_metadata"]["source_layer"], "noise_proxy")
        self.assertEqual(payload["noise_proxy_metadata"]["kind"], ["road", "rail", "airport", "industry"])
        self.assertEqual(payload["noise_proxy_metadata"]["kinds"], ["road", "rail", "airport", "industry"])
        self.assertEqual(payload["noise_proxy_metadata"]["metrics"], ["Lden", "Lnight"])
        self.assertEqual(payload["noise_proxy_metadata"]["default_metric"], "Lden")
        self.assertEqual(payload["noise_proxy_metadata"]["method"], "mixed")
        self.assertEqual(
            payload["noise_proxy_metadata"]["methods"],
            ["official_derived_grid_proxy", "official_resolved_contour"],
        )
        self.assertEqual(payload["noise_proxy_metadata"]["confidence"], "proxy_or_modelled_not_measured")
        self.assertEqual(
            payload["noise_proxy_metadata"]["confidences"],
            ["proxy_not_measured", "official_modelled_not_measured"],
        )
        self.assertFalse(payload["noise_proxy_metadata"]["actual_road_geometry"])
        self.assertFalse(payload["noise_proxy_metadata"]["actual_rail_geometry"])
        self.assertFalse(payload["noise_proxy_metadata"]["actual_airport_geometry"])
        self.assertFalse(payload["noise_proxy_metadata"]["actual_industry_geometry"])
        self.assertFalse(payload["noise_proxy_metadata"]["actual_runway_geometry"])
        self.assertFalse(payload["noise_proxy_metadata"]["actual_geometry"])
        self.assertEqual(payload["runtime_mode"], "strict_manifest")
        self.assertIsNone(payload["runtime_warning"])
        self.assertNotIn("ov_shops", payload["category_colors"])

    def test_runtime_service_omits_fine_surface_fields_when_unavailable(self) -> None:
        manifest = {
            "build_key": "build-123",
            "geo_hash": "geo-hash-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {},
                    "transport": {},
                    "healthcare": {},
                    "parks": {},
                },
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
        self.assertEqual(payload["transport_mode_counts"], {})
        self.assertEqual(payload["transport_bus_frequency_counts"], {})
        self.assertFalse(payload["noise_enabled"])
        self.assertIsNone(payload["noise_pmtiles_url"])
        self.assertEqual(payload["noise_counts"], {})

    def test_runtime_service_noise_proxy_metadata_stays_road_only_without_rail_rows(self) -> None:
        manifest = {
            "build_key": "build-123",
            "geo_hash": "geo-hash-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 0, "transport": 0, "healthcare": 0, "parks": 0},
                "amenity_tier_counts": {"shops": {}, "transport": {}, "healthcare": {}, "parks": {}},
                "noise_enabled": True,
                "noise_counts": {"proxy": 4},
                "noise_source_counts": {"road": 4},
                "noise_metric_counts": {"Lden": 2, "Lnight": 2},
            },
        }
        with TemporaryDirectory() as tmp_name:
            noise_pmtiles_path = Path(tmp_name) / "noise.pmtiles"
            noise_pmtiles_path.write_bytes(b"noise")
            with (
                mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
                mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
                mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
                mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=noise_pmtiles_path),
            ):
                payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertEqual(payload["noise_proxy_metadata"]["kinds"], ["road"])

    def test_runtime_service_noise_proxy_metadata_stays_road_rail_without_airport_rows(self) -> None:
        manifest = {
            "build_key": "build-123",
            "geo_hash": "geo-hash-123",
            "reach_hash": "reach-hash-123",
            "score_hash": "score-hash-123",
            "render_hash": "render-hash-123",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 0, "transport": 0, "healthcare": 0, "parks": 0},
                "amenity_tier_counts": {"shops": {}, "transport": {}, "healthcare": {}, "parks": {}},
                "noise_enabled": True,
                "noise_counts": {"proxy": 6},
                "noise_source_counts": {"road": 4, "rail": 2},
                "noise_metric_counts": {"Lden": 3, "Lnight": 3},
            },
        }
        with TemporaryDirectory() as tmp_name:
            noise_pmtiles_path = Path(tmp_name) / "noise.pmtiles"
            noise_pmtiles_path.write_bytes(b"noise")
            with (
                mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest),
                mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
                mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
                mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=noise_pmtiles_path),
            ):
                payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertEqual(payload["noise_proxy_metadata"]["kinds"], ["road", "rail"])

    def test_runtime_service_uses_dev_config_hash_and_coarse_only_payload(self) -> None:
        manifest = {
            "build_key": "build-dev-123",
            "geo_hash": "geo-hash-dev",
            "reach_hash": "reach-hash-dev",
            "score_hash": "score-hash-dev",
            "render_hash": "render-hash-dev",
            "summary_json": {
                "build_profile": "dev",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3, "regular": 9},
                    "transport": {},
                    "healthcare": {"local": 1},
                    "parks": {"district": 3},
                },
                "transport_subtier_counts": {"mon_sun": 2},
                "transport_bus_frequency_counts": {"frequent": 2},
                "transport_flag_counts": {
                    "is_unscheduled_stop": 1,
                    "has_exception_only_service": 0,
                    "has_any_bus_service": 2,
                    "has_daily_bus_service": 2,
                },
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

    def test_runtime_service_uses_test_config_hash_and_profile_specific_pmtiles(self) -> None:
        manifest = {
            "build_key": "build-test-123",
            "geo_hash": "geo-hash-test",
            "reach_hash": "reach-hash-test",
            "score_hash": "score-hash-test",
            "render_hash": "render-hash-test",
            "summary_json": {
                "build_profile": "test",
                "map_center": {"lat": 51.9, "lon": -8.47},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3, "regular": 9},
                    "transport": {},
                    "healthcare": {"local": 1},
                    "parks": {"district": 3},
                },
                "transport_subtier_counts": {"mon_sun": 2},
                "transport_bus_frequency_counts": {"frequent": 2},
                "transport_flag_counts": {
                    "is_unscheduled_stop": 1,
                    "has_exception_only_service": 0,
                    "has_any_bus_service": 2,
                    "has_daily_bus_service": 2,
                },
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
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
        expected_config_hash = config.build_config_hashes(profile="test").config_hash
        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=manifest) as runtime_mock,
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=True),
            mock.patch.object(serve_from_db._surface, "build_surface_shell_hash", return_value="shell-hash-test"),
            mock.patch.object(serve_from_db._surface, "surface_shell_dir", return_value=Path("surface-shell")),
            mock.patch.object(serve_from_db._surface, "surface_score_dir", return_value=Path("surface-scores")),
            mock.patch.object(serve_from_db._surface, "surface_tile_dir", return_value=Path("surface-tiles")),
            mock.patch.object(serve_from_db._surface, "ensure_surface_tile_cache_manifest", return_value={}),
            mock.patch.object(serve_from_db._surface, "surface_analysis_ready", return_value=True),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine, profile="test").get_runtime()

        self.assertEqual(runtime_mock.call_args.kwargs["config_hash"], expected_config_hash)
        self.assertEqual(payload["build_profile"], "test")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability-test.pmtiles")
        self.assertEqual(payload["fine_resolutions_m"], [2500, 1000, 500, 250, 100, 50])
        self.assertEqual(payload["inspect_url"], "/api/inspect")

    def test_runtime_service_stale_manifest_fallback_is_env_gated(self) -> None:
        manifest = {
            "build_key": "build-fallback-123",
            "geo_hash": "geo-hash-fallback",
            "reach_hash": "reach-hash-fallback",
            "score_hash": "score-hash-fallback",
            "render_hash": "render-hash-fallback",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3},
                    "transport": {},
                    "healthcare": {},
                    "parks": {},
                },
                "noise_enabled": True,
                "noise_source_counts": {"road": 1},
                "noise_metric_counts": {"Lden": 1},
                "noise_proxy_calibration_table": [{"calibration_stat": "p75"}],
            },
        }

        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=None),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_load_latest_completed_manifest_for_extract",
                return_value=manifest,
            ) as fallback_mock,
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
            mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=Path(__file__)),
        ):
            with mock.patch.dict("os.environ", {}, clear=False):
                with self.assertRaises(RuntimeError):
                    serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()
            fallback_mock.assert_not_called()

        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=None),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_load_latest_completed_manifest_for_extract",
                return_value=manifest,
            ) as fallback_mock,
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
            mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=Path(__file__)),
            mock.patch.dict("os.environ", {serve_from_db.RUNTIME_STALE_FALLBACK_ENV: "1"}, clear=False),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        fallback_mock.assert_called_once()
        self.assertEqual(payload["runtime_mode"], "stale_manifest_fallback")
        self.assertIsInstance(payload["runtime_warning"], str)
        self.assertEqual(payload["noise_metric_counts"], {"Lden": 1})
        self.assertEqual(payload["noise_proxy_metadata"]["metrics"], ["Lden"])
        self.assertEqual(payload["noise_proxy_metadata"]["default_metric"], "Lden")

    def test_runtime_service_stale_manifest_fallback_prefers_transport_ready_manifest(self) -> None:
        poor_manifest = {
            "build_key": "build-fallback-poor",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3},
                    "transport": {},
                    "healthcare": {},
                    "parks": {},
                },
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "transport_reality_download_url": "/exports/transport-reality.zip",
                "transit_analysis_date": "2026-04-22",
            },
        }
        ready_manifest = {
            "build_key": "build-fallback-ready",
            "summary_json": {
                "build_profile": "full",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3},
                    "transport": {},
                    "healthcare": {},
                    "parks": {},
                },
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "transport_subtier_counts": {"mon_sat": 8},
                "transport_bus_frequency_counts": {"moderate": 3},
                "transport_flag_counts": {"has_any_bus_service": 7},
                "transport_mode_counts": {"rail": 2},
                "transport_reality_download_url": "/exports/transport-reality.zip",
                "transit_analysis_date": "2026-04-22",
            },
        }

        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=None),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_load_latest_completed_manifest_for_extract",
                return_value=poor_manifest,
            ),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_transport_quality_row_count",
                return_value=0,
            ),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_load_latest_transport_ready_manifest_for_extract",
                return_value=ready_manifest,
            ) as ready_mock,
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
            mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=Path(__file__)),
            mock.patch.dict("os.environ", {serve_from_db.RUNTIME_STALE_FALLBACK_ENV: "1"}, clear=False),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        ready_mock.assert_called_once()
        self.assertEqual(payload["build_key"], "build-fallback-ready")
        self.assertEqual(payload["runtime_mode"], "stale_manifest_fallback")
        self.assertIn("transport-ready", payload["runtime_warning"])
        self.assertEqual(payload["transport_subtier_counts"], {"mon_sat": 8})

    def test_runtime_service_uses_manifest_profile_pmtiles_url_for_stale_fallback(self) -> None:
        manifest = {
            "build_key": "build-fallback-dev",
            "summary_json": {
                "build_profile": "dev",
                "map_center": {"lat": 53.4, "lon": -7.7},
                "amenity_counts": {"shops": 12, "transport": 4, "healthcare": 1, "parks": 3},
                "amenity_tier_counts": {
                    "shops": {"corner": 3},
                    "transport": {},
                    "healthcare": {},
                    "parks": {},
                },
                "transport_reality_enabled": True,
                "service_deserts_enabled": True,
                "transport_subtier_counts": {"mon_sat": 8},
                "transport_bus_frequency_counts": {"moderate": 3},
                "transport_flag_counts": {"has_any_bus_service": 7},
                "transport_mode_counts": {"rail": 2},
                "transport_reality_download_url": "/exports/transport-reality.zip",
                "transit_analysis_date": "2026-04-22",
            },
        }

        with (
            mock.patch.object(serve_from_db, "load_runtime_manifest", return_value=None),
            mock.patch.object(
                serve_from_db.RuntimeService,
                "_load_latest_completed_manifest_for_extract",
                return_value=manifest,
            ),
            mock.patch.object(serve_from_db, "load_available_resolutions", return_value=[20000, 10000, 5000]),
            mock.patch.object(serve_from_db, "profile_fine_surface_enabled", return_value=False),
            mock.patch.object(serve_from_db, "noise_pmtiles_output_path", return_value=Path(__file__)),
            mock.patch.dict("os.environ", {serve_from_db.RUNTIME_STALE_FALLBACK_ENV: "1"}, clear=False),
        ):
            payload = serve_from_db.RuntimeService(mock.sentinel.engine).get_runtime()

        self.assertEqual(payload["build_profile"], "dev")
        self.assertEqual(payload["pmtiles_url"], "/tiles/livability-dev.pmtiles")

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

    def test_main_serve_test_flag_starts_test_local_app(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["main.py", "--serve-test"]),
            mock.patch("render_from_db.run_render_from_db", return_value="http://127.0.0.1:8000/") as render_mock,
        ):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        render_mock.assert_called_once_with(
            profile="test",
            host=config.DEFAULT_SERVER_HOST,
            port=config.DEFAULT_SERVER_PORT,
        )
