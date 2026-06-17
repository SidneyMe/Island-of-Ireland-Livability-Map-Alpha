from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import serve_routes


class ServeRoutesTests(TestCase):
    def test_resolve_get_route_matches_core_endpoints(self) -> None:
        with TemporaryDirectory() as tmp_name:
            static_dir = Path(tmp_name) / "static"
            static_dir.mkdir()

            def pmtiles_path_for_url(path: str) -> Path | None:
                if path == "/tiles/livability.pmtiles":
                    return Path(tmp_name) / "livability.pmtiles"
                return None

            cases = {
                "/": ("root", static_dir / "index.html"),
                "/exports/transport-reality.zip": (
                    "export",
                    Path(tmp_name) / "exports" / "transport-reality.zip",
                ),
                "/tiles/livability.pmtiles": ("pmtiles", Path(tmp_name) / "livability.pmtiles"),
                "/tiles/surface/250/15/3/4.png": ("surface_tile", None),
                "/static/dist/app.js": ("static", static_dir / "dist" / "app.js"),
                "/api/runtime": ("api_runtime", None),
                "/api/inspect": ("api_inspect", None),
            }

            for path, (kind, target_path) in cases.items():
                with self.subTest(path=path):
                    route = serve_routes.resolve_get_route(
                        path,
                        static_dir=static_dir,
                        export_path=Path(tmp_name) / "exports" / "transport-reality.zip",
                        pmtiles_path_for_url=pmtiles_path_for_url,
                        noise_pmtiles_url_path=None,
                        noise_pmtiles_path=None,
                    )
                    self.assertIsNotNone(route)
                    assert route is not None
                    self.assertEqual(route.kind, kind)
                    if target_path is not None:
                        self.assertEqual(route.target_path, target_path)

                    if path == "/tiles/surface/250/15/3/4.png":
                        self.assertEqual(route.groups, ("250", "15", "3", "4"))

    def test_resolve_get_route_prioritizes_pmtiles_before_static_and_api(self) -> None:
        with TemporaryDirectory() as tmp_name:
            static_dir = Path(tmp_name) / "static"
            static_dir.mkdir()

            def pmtiles_path_for_url(path: str) -> Path | None:
                if path == "/static/api/runtime":
                    return Path(tmp_name) / "overlapping.pmtiles"
                return None

            route = serve_routes.resolve_get_route(
                "/static/api/runtime",
                static_dir=static_dir,
                export_path=Path(tmp_name) / "exports" / "transport-reality.zip",
                pmtiles_path_for_url=pmtiles_path_for_url,
                noise_pmtiles_url_path=None,
                noise_pmtiles_path=None,
            )

            self.assertIsNotNone(route)
            assert route is not None
            self.assertEqual(route.kind, "pmtiles")
            self.assertEqual(route.target_path, Path(tmp_name) / "overlapping.pmtiles")

    def test_resolve_get_route_unknown_returns_none(self) -> None:
        with TemporaryDirectory() as tmp_name:
            static_dir = Path(tmp_name) / "static"
            static_dir.mkdir()

            route = serve_routes.resolve_get_route(
                "/api/walk-grid",
                static_dir=static_dir,
                export_path=Path(tmp_name) / "exports" / "transport-reality.zip",
                pmtiles_path_for_url=lambda path: None,
                noise_pmtiles_url_path=None,
                noise_pmtiles_path=None,
            )

            self.assertIsNone(route)

    def test_resolve_head_route_matches_pmtiles_and_noise(self) -> None:
        with TemporaryDirectory() as tmp_name:
            pmtiles_path = Path(tmp_name) / "livability.pmtiles"
            noise_pmtiles_path = Path(tmp_name) / "noise.pmtiles"

            def pmtiles_path_for_url(path: str) -> Path | None:
                if path == "/tiles/livability.pmtiles":
                    return pmtiles_path
                return None

            self.assertEqual(
                serve_routes.resolve_head_route(
                    "/tiles/livability.pmtiles",
                    pmtiles_path_for_url=pmtiles_path_for_url,
                    noise_pmtiles_url_path="/tiles/noise.pmtiles",
                    noise_pmtiles_path=noise_pmtiles_path,
                ),
                pmtiles_path,
            )
            self.assertEqual(
                serve_routes.resolve_head_route(
                    "/tiles/noise.pmtiles",
                    pmtiles_path_for_url=lambda path: None,
                    noise_pmtiles_url_path="/tiles/noise.pmtiles",
                    noise_pmtiles_path=noise_pmtiles_path,
                ),
                noise_pmtiles_path,
            )
            self.assertIsNone(
                serve_routes.resolve_head_route(
                    "/api/runtime",
                    pmtiles_path_for_url=lambda path: None,
                    noise_pmtiles_url_path="/tiles/noise.pmtiles",
                    noise_pmtiles_path=noise_pmtiles_path,
                )
            )
