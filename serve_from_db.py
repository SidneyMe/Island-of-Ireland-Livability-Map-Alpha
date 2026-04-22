from __future__ import annotations

import gzip
import json
import mimetypes
import re
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from config import (
    CACHE_DIR,
    CATEGORY_COLORS,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    OSM_EXTRACT_PATH,
    SURFACE_DEFAULT_ZOOM,
    SURFACE_MAX_ZOOM,
    build_config_hashes,
    build_profile_settings,
    normalize_build_profile,
    pmtiles_output_path,
    pmtiles_url_path,
    precompute_flag_for_profile,
    profile_fine_surface_enabled,
)
from db_postgis import (
    build_engine,
    ensure_database_ready,
    load_available_resolutions,
    load_runtime_manifest,
)
from precompute import surface as _surface
from transit.export import EXPORTS_DIR, ZIP_FILENAME


STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"
MISSING_PRECOMPUTE_MESSAGE = "No PostGIS precompute found for current config"
CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")
SURFACE_TILE_RE = re.compile(r"^/tiles/surface/(\d+)/(\d+)/(\d+)/(\d+)\.png$")


def _missing_precompute_message(
    reason: str | None = None,
    *,
    profile: str = "full",
    config_hash: str | None = None,
) -> str:
    normalized_profile = normalize_build_profile(profile)
    resolved_hash = config_hash or build_config_hashes(normalized_profile).config_hash
    precompute_flag = precompute_flag_for_profile(normalized_profile)
    message = (
        f"{MISSING_PRECOMPUTE_MESSAGE} "
        f"(profile={normalized_profile}, config_hash={resolved_hash}, extract_path={OSM_EXTRACT_PATH}). "
        f"Run {precompute_flag} first."
    )
    if reason:
        return f"{message} Reason: {reason}."
    return message


@dataclass(frozen=True)
class RuntimeState:
    build_key: str
    build_profile: str
    map_center: dict[str, float]
    coarse_vector_resolutions: list[int]
    fine_resolutions: list[int]
    surface_zoom_breaks: list[tuple[int, int]]
    amenity_counts: dict[str, int]
    amenity_tier_counts: dict[str, dict[str, int]]
    fine_surface_enabled: bool
    surface_shell_dir: Path | None
    surface_score_dir: Path | None
    surface_tile_dir: Path | None
    transport_reality_enabled: bool
    service_deserts_enabled: bool
    transport_reality_download_url: str | None
    transit_analysis_date: str | None
    transit_analysis_window_days: int | None
    transit_service_desert_window_days: int | None
    overture_dataset: dict[str, Any] | None


class RuntimeService:
    def __init__(self, engine, *, profile: str = "full") -> None:
        self._engine = engine
        self._profile = normalize_build_profile(profile)
        self._profile_settings = build_profile_settings(self._profile)
        self._hashes = build_config_hashes(self._profile)
        self._pmtiles_url = pmtiles_url_path(self._profile)
        self._state: RuntimeState | None = None
        self._surface_runtime: _surface.FineSurfaceRuntime | None = None

    @staticmethod
    def _resolution_list(values: Any, fallback: list[int]) -> list[int]:
        if not isinstance(values, list):
            return [int(value) for value in fallback]
        return [int(value) for value in values]

    @staticmethod
    def _zoom_breaks(values: Any, fallback: list[tuple[int, int]]) -> list[tuple[int, int]]:
        if not isinstance(values, list):
            return [(int(min_zoom), int(resolution_m)) for min_zoom, resolution_m in fallback]
        normalized: list[tuple[int, int]] = []
        for entry in values:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            normalized.append((int(entry[0]), int(entry[1])))
        if normalized:
            return normalized
        return [(int(min_zoom), int(resolution_m)) for min_zoom, resolution_m in fallback]

    def _load_state(self) -> RuntimeState:
        manifest = load_runtime_manifest(
            self._engine,
            extract_path=str(OSM_EXTRACT_PATH),
            config_hash=self._hashes.config_hash,
        )
        if manifest is None:
            raise RuntimeError(
                _missing_precompute_message(
                    profile=self._profile,
                    config_hash=self._hashes.config_hash,
                )
            )

        summary_json = manifest.get("summary_json", {}) or {}
        resolutions = load_available_resolutions(self._engine, manifest["build_key"])
        if not resolutions:
            raise RuntimeError(
                _missing_precompute_message(
                    "incomplete build: no walk rows found",
                    profile=self._profile,
                    config_hash=self._hashes.config_hash,
                )
            )

        map_center = summary_json.get("map_center")
        if not map_center:
            raise RuntimeError(
                _missing_precompute_message(
                    "missing map center in manifest",
                    profile=self._profile,
                    config_hash=self._hashes.config_hash,
                )
            )

        amenity_counts = {
            category: int((summary_json.get("amenity_counts", {}) or {}).get(category, 0))
            for category in sorted(CATEGORY_COLORS)
        }
        raw_tier_counts = summary_json.get("amenity_tier_counts", {}) or {}
        amenity_tier_counts = {
            category: {
                str(tier): int(value)
                for tier, value in (
                    (raw_tier_counts.get(category, {}) or {})
                    if isinstance(raw_tier_counts.get(category, {}) or {}, dict)
                    else {}
                ).items()
            }
            for category in sorted(CATEGORY_COLORS)
        }
        profile_name = str(summary_json.get("build_profile") or self._profile)
        fine_resolutions = self._resolution_list(
            summary_json.get("fine_resolutions_m"),
            list(self._profile_settings.fine_resolutions_m),
        )
        surface_zoom_breaks = self._zoom_breaks(
            summary_json.get("surface_zoom_breaks"),
            list(self._profile_settings.surface_zoom_breaks),
        )
        fine_surface_enabled = False
        surface_shell_dir: Path | None = None
        surface_score_dir: Path | None = None
        surface_tile_dir: Path | None = None
        if profile_fine_surface_enabled(self._profile):
            surface_shell_hash = _surface.build_surface_shell_hash(str(manifest["reach_hash"]))
            surface_shell_dir = _surface.surface_shell_dir(
                CACHE_DIR,
                surface_shell_hash=surface_shell_hash,
            )
            surface_score_dir = _surface.surface_score_dir(
                CACHE_DIR,
                score_hash=str(manifest["score_hash"]),
            )
            surface_tile_dir = _surface.surface_tile_dir(
                CACHE_DIR,
                score_hash=str(manifest["score_hash"]),
                render_hash=str(manifest["render_hash"]),
            )
            _surface.ensure_surface_tile_cache_manifest(
                surface_tile_dir,
                score_hash=str(manifest["score_hash"]),
                render_hash=str(manifest["render_hash"]),
            )
            fine_surface_enabled = _surface.surface_analysis_ready(
                surface_shell_dir,
                surface_score_dir,
                expected_surface_shell_hash=surface_shell_hash,
                expected_score_hash=str(manifest["score_hash"]),
            )

        return RuntimeState(
            build_key=str(manifest["build_key"]),
            build_profile=profile_name,
            map_center={"lat": float(map_center["lat"]), "lon": float(map_center["lon"])},
            coarse_vector_resolutions=[int(value) for value in resolutions],
            fine_resolutions=fine_resolutions,
            surface_zoom_breaks=surface_zoom_breaks,
            amenity_counts=amenity_counts,
            amenity_tier_counts=amenity_tier_counts,
            fine_surface_enabled=fine_surface_enabled,
            surface_shell_dir=surface_shell_dir,
            surface_score_dir=surface_score_dir,
            surface_tile_dir=surface_tile_dir,
            transport_reality_enabled=bool(summary_json.get("transport_reality_enabled")),
            service_deserts_enabled=bool(summary_json.get("service_deserts_enabled")),
            transport_reality_download_url=(
                str(summary_json["transport_reality_download_url"])
                if summary_json.get("transport_reality_download_url")
                else None
            ),
            transit_analysis_date=(
                str(summary_json["transit_analysis_date"])
                if summary_json.get("transit_analysis_date")
                else None
            ),
            transit_analysis_window_days=(
                int(summary_json["transit_analysis_window_days"])
                if summary_json.get("transit_analysis_window_days") is not None
                else None
            ),
            transit_service_desert_window_days=(
                int(summary_json["transit_service_desert_window_days"])
                if summary_json.get("transit_service_desert_window_days") is not None
                else None
            ),
            overture_dataset=(
                dict(summary_json.get("overture_dataset") or {})
                if summary_json.get("overture_dataset")
                else None
            ),
        )

    def state(self) -> RuntimeState:
        if self._state is None:
            self._state = self._load_state()
        return self._state

    def get_runtime(self) -> dict[str, Any]:
        payload = self._get_runtime_base()
        state = self.state()
        if state.fine_surface_enabled:
            payload["inspect_url"] = "/api/inspect"
        return payload

    def _get_runtime_base(self) -> dict[str, Any]:
        state = self.state()
        return {
            "build_key": state.build_key,
            "build_profile": state.build_profile,
            "map_center": state.map_center,
            "grid_sizes_m": state.coarse_vector_resolutions,
            "coarse_vector_resolutions_m": state.coarse_vector_resolutions,
            "fine_resolutions_m": list(state.fine_resolutions) if state.fine_surface_enabled else [],
            "surface_zoom_breaks": [
                {"min_zoom": int(min_zoom), "resolution_m": int(resolution_m)}
                for min_zoom, resolution_m in state.surface_zoom_breaks
            ],
            "amenity_counts": state.amenity_counts,
            "amenity_tier_counts": state.amenity_tier_counts,
            "category_colors": CATEGORY_COLORS,
            "default_zoom": SURFACE_DEFAULT_ZOOM,
            "max_zoom": SURFACE_MAX_ZOOM,
            "fine_surface_enabled": state.fine_surface_enabled,
            "pmtiles_url": self._pmtiles_url,
            "transport_reality_enabled": state.transport_reality_enabled,
            "service_deserts_enabled": state.service_deserts_enabled,
            "transport_reality_download_url": state.transport_reality_download_url,
            "transit_analysis_date": state.transit_analysis_date,
            "transit_analysis_window_days": state.transit_analysis_window_days,
            "transit_service_desert_window_days": state.transit_service_desert_window_days,
            "overture_dataset": state.overture_dataset,
        }

    def surface_runtime(self) -> _surface.FineSurfaceRuntime:
        state = self.state()
        if (
            not state.fine_surface_enabled
            or state.surface_shell_dir is None
            or state.surface_score_dir is None
            or state.surface_tile_dir is None
        ):
            raise RuntimeError("Fine surface runtime is unavailable for this build.")
        if self._surface_runtime is None:
            self._surface_runtime = _surface.FineSurfaceRuntime(
                state.surface_shell_dir,
                state.surface_score_dir,
                state.surface_tile_dir,
            )
        return self._surface_runtime

    def get_surface_tile(self, *, resolution_m: int, z: int, x: int, y: int) -> bytes:
        normalized_resolution = int(resolution_m)
        if normalized_resolution not in self.state().fine_resolutions:
            raise ValueError(f"Unsupported fine surface resolution: {resolution_m}")
        return self.surface_runtime().render_tile(
            resolution_m=normalized_resolution,
            z=int(z),
            x=int(x),
            y=int(y),
        )

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, Any]:
        return self.surface_runtime().inspect(lat=float(lat), lon=float(lon), zoom=zoom)


class LivabilityHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class,
        *,
        service: RuntimeService,
        static_dir: Path,
        index_html: bytes,
        pmtiles_path: Path,
        pmtiles_url_path: str,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.service = service
        self.static_dir = static_dir.resolve()
        self.index_html = index_html
        self.pmtiles_path = pmtiles_path
        self.pmtiles_url_path = str(pmtiles_url_path)


class LivabilityRequestHandler(BaseHTTPRequestHandler):
    server_version = "LivabilityLocal/2.0"

    @property
    def livability_server(self) -> LivabilityHTTPServer:
        return self.server  # type: ignore[return-value]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        try:
            self._dispatch_request(parsed)
        except CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(parsed.path, exc)
        except ValueError as exc:
            self._try_write_json(parsed.path, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except RuntimeError as exc:
            self._try_write_json(parsed.path, HTTPStatus.NOT_FOUND, {"error": str(exc)})
        except FileNotFoundError:
            self._try_write_json(parsed.path, HTTPStatus.NOT_FOUND, {"error": "not found"})
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Request failed for {parsed.path}: {exc}")
            self._try_write_json(parsed.path, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal server error"})

    def do_HEAD(self) -> None:  # noqa: N802
        # MapLibre/PMTiles will send HEAD for the archive on some flows.
        parsed = urlsplit(self.path)
        if parsed.path == self.livability_server.pmtiles_url_path:
            try:
                self._serve_pmtiles(head_only=True)
            except CLIENT_DISCONNECT_ERRORS as exc:
                self._log_client_disconnect(parsed.path, exc)
            return
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        del format, args

    def _dispatch_request(self, parsed) -> None:
        if parsed.path == "/":
            self._write_bytes(
                HTTPStatus.OK,
                self.livability_server.index_html,
                "text/html; charset=utf-8",
                extra_headers={"Cache-Control": "no-store"},
            )
            return
        if parsed.path == "/exports/transport-reality.zip":
            self._serve_export(EXPORTS_DIR / ZIP_FILENAME)
            return
        if parsed.path == self.livability_server.pmtiles_url_path:
            self._serve_pmtiles()
            return
        surface_match = SURFACE_TILE_RE.match(parsed.path)
        if surface_match:
            resolution_m, z, x, y = (int(value) for value in surface_match.groups())
            self._serve_surface_tile(
                resolution_m=resolution_m,
                z=z,
                x=x,
                y=y,
            )
            return
        if parsed.path.startswith("/static/"):
            self._serve_static(self.livability_server.static_dir / parsed.path.removeprefix("/static/"))
            return
        if parsed.path == "/api/runtime":
            self._write_json(HTTPStatus.OK, self.livability_server.service.get_runtime())
            return
        if parsed.path == "/api/inspect":
            self._serve_inspect(parsed.query)
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _serve_pmtiles(self, *, head_only: bool = False) -> None:
        pmtiles_path = self.livability_server.pmtiles_path
        if not pmtiles_path.exists():
            raise FileNotFoundError(str(pmtiles_path))
        file_size = pmtiles_path.stat().st_size
        range_header = self.headers.get("Range")

        if range_header:
            match = RANGE_RE.match(range_header.strip())
            if not match:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return
            start = int(match.group(1))
            end_text = match.group(2)
            end = int(end_text) if end_text else file_size - 1
            if start >= file_size:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return
            end = min(end, file_size - 1)
            length = end - start + 1
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            if head_only:
                return
            with pmtiles_path.open("rb") as handle:
                handle.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = handle.read(min(65536, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()
        if head_only:
            return
        with pmtiles_path.open("rb") as handle:
            while True:
                chunk = handle.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def _serve_static(self, target_path: Path) -> None:
        safe_root = self.livability_server.static_dir
        resolved = target_path.resolve()
        if safe_root not in resolved.parents and resolved != safe_root:
            raise FileNotFoundError
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        content = resolved.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_export(self, export_path: Path) -> None:
        resolved = export_path.resolve()
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError(str(resolved))
        content = resolved.read_bytes()
        self._write_bytes(
            HTTPStatus.OK,
            content,
            "application/zip",
            extra_headers={
                "Content-Disposition": f'attachment; filename="{resolved.name}"',
                "Cache-Control": "public, max-age=3600",
            },
        )

    def _serve_surface_tile(self, *, resolution_m: int, z: int, x: int, y: int) -> None:
        payload = self.livability_server.service.get_surface_tile(
            resolution_m=resolution_m,
            z=z,
            x=x,
            y=y,
        )
        self._write_bytes(
            HTTPStatus.OK,
            payload,
            "image/png",
            extra_headers={"Cache-Control": "public, max-age=3600"},
        )

    def _serve_inspect(self, raw_query: str) -> None:
        query = parse_qs(raw_query, keep_blank_values=False)
        try:
            lat = float(query["lat"][0])
            lon = float(query["lon"][0])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ValueError("inspect requires numeric lat and lon query parameters") from exc

        zoom_value: float | None = None
        if "zoom" in query and query["zoom"]:
            try:
                zoom_value = float(query["zoom"][0])
            except (TypeError, ValueError) as exc:
                raise ValueError("inspect zoom must be numeric when provided") from exc

        payload = self.livability_server.service.inspect(
            lat=lat,
            lon=lon,
            zoom=zoom_value,
        )
        self._write_json(HTTPStatus.OK, payload)

    def _try_write_json(self, path: str, status: HTTPStatus, payload: dict[str, Any]) -> bool:
        try:
            self._write_json(status, payload)
            return True
        except CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(path, exc)
            return False

    def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode("utf-8")
        use_gzip = "gzip" in (self.headers.get("Accept-Encoding", "") or "").lower()
        if use_gzip:
            body = gzip.compress(body)
        self._write_bytes(
            status,
            body,
            "application/json; charset=utf-8",
            extra_headers={
                "Vary": "Accept-Encoding",
                **({"Content-Encoding": "gzip"} if use_gzip else {}),
            },
        )

    def _write_bytes(
        self,
        status: HTTPStatus,
        body: bytes,
        content_type: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        for header_name, header_value in (extra_headers or {}).items():
            self.send_header(header_name, header_value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _log_client_disconnect(self, path: str, exc: BaseException) -> None:
        if path.startswith("/tiles/") or path.startswith("/api/inspect"):
            return
        print(f"Client disconnected during {path}: {exc}")


def build_runtime_service(*, profile: str = "full") -> RuntimeService:
    engine = build_engine()
    ensure_database_ready(engine)
    return RuntimeService(engine, profile=profile)


def create_http_server(
    *,
    service: RuntimeService | None = None,
    profile: str = "full",
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
    static_dir: Path = STATIC_DIR,
    pmtiles_path: Path | None = None,
) -> LivabilityHTTPServer:
    normalized_profile = normalize_build_profile(profile)
    resolved_pmtiles_path = pmtiles_path or pmtiles_output_path(normalized_profile)
    resolved_pmtiles_url_path = pmtiles_url_path(normalized_profile)
    runtime_service = service or build_runtime_service(profile=normalized_profile)
    index_html_path = static_dir / "index.html"
    if not index_html_path.exists():
        raise RuntimeError(f"static index.html not found at {index_html_path}")
    index_html = index_html_path.read_bytes()
    if not resolved_pmtiles_path.exists():
        precompute_flag = precompute_flag_for_profile(normalized_profile)
        raise RuntimeError(
            f"PMTiles archive not found at {resolved_pmtiles_path}. "
            f"Run {precompute_flag} to bake it before serving; the map cannot load without it."
        )
    return LivabilityHTTPServer(
        (host, int(port)),
        LivabilityRequestHandler,
        service=runtime_service,
        static_dir=static_dir,
        index_html=index_html,
        pmtiles_path=resolved_pmtiles_path,
        pmtiles_url_path=resolved_pmtiles_url_path,
    )


def serve_livability_app(
    *,
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
    profile: str = "full",
) -> str:
    normalized_profile = normalize_build_profile(profile)
    httpd = create_http_server(host=host, port=port, profile=normalized_profile)
    bound_host, bound_port = httpd.server_address[:2]
    url = f"http://{bound_host}:{bound_port}/"
    print("Phase R1 - serving      ... done")
    print(f"Serving livability MapLibre app ({normalized_profile}) -> {url}")
    print("Press Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping local server...")
    finally:
        httpd.server_close()
    return url
