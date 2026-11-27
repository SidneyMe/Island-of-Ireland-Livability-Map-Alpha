from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal
import re
from urllib.parse import unquote


SURFACE_TILE_RE = re.compile(r"^/tiles/surface/([^/]+)/([^/]+)/([^/]+)/([^/]+)\.png$")
TRANSIT_ROUTE_ASSET_RE = re.compile(
    r"^/transit/routes/([A-Za-z0-9][A-Za-z0-9._-]*)/(index\.json|[0-9a-f]{24}\.geojson)$"
)


@dataclass(frozen=True)
class RouteMatch:
    kind: Literal[
        "root",
        "export",
        "pmtiles",
        "surface_tile",
        "static",
        "route_asset",
        "api_runtime",
        "api_inspect",
    ]
    target_path: Path | None = None
    groups: tuple[str, ...] = ()


def resolve_get_route(
    path: str,
    *,
    static_dir: Path,
    export_path: Path,
    route_assets_dir: Path | None = None,
    pmtiles_path_for_url: Callable[[str], Path | None],
    noise_pmtiles_url_path: str | None,
    noise_pmtiles_path: Path | None,
) -> RouteMatch | None:
    if path == "/":
        return RouteMatch("root", target_path=static_dir / "index.html")
    if path == "/exports/transport-reality.zip":
        return RouteMatch("export", target_path=export_path)
    route_asset_match = TRANSIT_ROUTE_ASSET_RE.match(path)
    if route_asset_match and route_assets_dir is not None:
        build_key, asset_name = route_asset_match.groups()
        return RouteMatch(
            "route_asset",
            target_path=route_assets_dir / build_key / asset_name,
        )

    pmtiles_path = pmtiles_path_for_url(path)
    if pmtiles_path is not None:
        return RouteMatch("pmtiles", target_path=pmtiles_path)
    if path == noise_pmtiles_url_path:
        return RouteMatch("pmtiles", target_path=noise_pmtiles_path)

    surface_match = SURFACE_TILE_RE.match(path)
    if surface_match:
        return RouteMatch("surface_tile", groups=surface_match.groups())

    if path.startswith("/static/"):
        # Request paths arrive percent-encoded; basemap glyph folders contain
        # spaces ("Noto Sans Regular"), so an undecoded path 404s every label.
        # _serve_static still rejects anything resolving outside static_dir.
        return RouteMatch("static", target_path=static_dir / unquote(path.removeprefix("/static/")))
    if path == "/api/runtime":
        return RouteMatch("api_runtime")
    if path == "/api/inspect":
        return RouteMatch("api_inspect")
    return None


def resolve_head_route(
    path: str,
    *,
    pmtiles_path_for_url: Callable[[str], Path | None],
    noise_pmtiles_url_path: str | None,
    noise_pmtiles_path: Path | None,
) -> Path | None:
    pmtiles_path = pmtiles_path_for_url(path)
    if pmtiles_path is not None:
        return pmtiles_path
    if path == noise_pmtiles_url_path:
        return noise_pmtiles_path
    return None
