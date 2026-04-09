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
from urllib.parse import urlsplit

from config import (
    CATEGORY_COLORS,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    HASHES,
    OSM_EXTRACT_PATH,
    PMTILES_OUTPUT_PATH,
)
from db_postgis import (
    build_engine,
    ensure_database_ready,
    load_available_resolutions,
    load_runtime_manifest,
)


STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"
MISSING_PRECOMPUTE_MESSAGE = "No PostGIS precompute found for current config"
CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")


def _missing_precompute_message(reason: str | None = None) -> str:
    message = (
        f"{MISSING_PRECOMPUTE_MESSAGE} "
        f"(config_hash={HASHES.config_hash}, extract_path={OSM_EXTRACT_PATH}). "
        "Run --precompute first."
    )
    if reason:
        return f"{message} Reason: {reason}."
    return message


@dataclass(frozen=True)
class RuntimeState:
    build_key: str
    map_center: dict[str, float]
    resolutions: list[int]
    amenity_counts: dict[str, int]


class RuntimeService:
    def __init__(self, engine) -> None:
        self._engine = engine
        self._state: RuntimeState | None = None

    def _load_state(self) -> RuntimeState:
        manifest = load_runtime_manifest(
            self._engine,
            extract_path=str(OSM_EXTRACT_PATH),
            config_hash=HASHES.config_hash,
        )
        if manifest is None:
            raise RuntimeError(_missing_precompute_message())

        summary_json = manifest.get("summary_json", {}) or {}
        resolutions = load_available_resolutions(self._engine, manifest["build_key"])
        if not resolutions:
            raise RuntimeError(_missing_precompute_message("incomplete build: no walk rows found"))

        map_center = summary_json.get("map_center")
        if not map_center:
            raise RuntimeError(_missing_precompute_message("missing map center in manifest"))

        amenity_counts = {
            category: int((summary_json.get("amenity_counts", {}) or {}).get(category, 0))
            for category in sorted(CATEGORY_COLORS)
        }
        return RuntimeState(
            build_key=str(manifest["build_key"]),
            map_center={"lat": float(map_center["lat"]), "lon": float(map_center["lon"])},
            resolutions=[int(value) for value in resolutions],
            amenity_counts=amenity_counts,
        )

    def state(self) -> RuntimeState:
        if self._state is None:
            self._state = self._load_state()
        return self._state

    def get_runtime(self) -> dict[str, Any]:
        state = self.state()
        return {
            "build_key": state.build_key,
            "map_center": state.map_center,
            "grid_sizes_m": state.resolutions,
            "amenity_counts": state.amenity_counts,
            "category_colors": CATEGORY_COLORS,
            "default_zoom": 6,
            "pmtiles_url": "/tiles/livability.pmtiles",
        }


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
    ) -> None:
        super().__init__(server_address, handler_class)
        self.service = service
        self.static_dir = static_dir.resolve()
        self.index_html = index_html
        self.pmtiles_path = pmtiles_path


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
        if parsed.path == "/tiles/livability.pmtiles":
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
            )
            return
        if parsed.path == "/tiles/livability.pmtiles":
            self._serve_pmtiles()
            return
        if parsed.path.startswith("/static/"):
            self._serve_static(self.livability_server.static_dir / parsed.path.removeprefix("/static/"))
            return
        if parsed.path == "/api/runtime":
            self._write_json(HTTPStatus.OK, self.livability_server.service.get_runtime())
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
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _try_write_json(self, path: str, status: HTTPStatus, payload: dict[str, Any]) -> bool:
        try:
            self._write_json(status, payload)
            return True
        except CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(path, exc)
            return False

    def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
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
        if path.startswith("/tiles/"):
            return
        print(f"Client disconnected during {path}: {exc}")


def build_runtime_service() -> RuntimeService:
    engine = build_engine()
    ensure_database_ready(engine)
    return RuntimeService(engine)


def create_http_server(
    *,
    service: RuntimeService | None = None,
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
    static_dir: Path = STATIC_DIR,
    pmtiles_path: Path = PMTILES_OUTPUT_PATH,
) -> LivabilityHTTPServer:
    runtime_service = service or build_runtime_service()
    index_html_path = static_dir / "index.html"
    if not index_html_path.exists():
        raise RuntimeError(f"static index.html not found at {index_html_path}")
    index_html = index_html_path.read_bytes()
    if not pmtiles_path.exists():
        raise RuntimeError(
            f"PMTiles archive not found at {pmtiles_path}. "
            "Run --precompute to bake it before serving; the map cannot load without it."
        )
    return LivabilityHTTPServer(
        (host, int(port)),
        LivabilityRequestHandler,
        service=runtime_service,
        static_dir=static_dir,
        index_html=index_html,
        pmtiles_path=pmtiles_path,
    )


def serve_livability_app(
    *,
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
) -> str:
    httpd = create_http_server(host=host, port=port)
    bound_host, bound_port = httpd.server_address[:2]
    url = f"http://{bound_host}:{bound_port}/"
    print("Phase R1 - serving      ... done")
    print(f"Serving livability MapLibre app -> {url}")
    print("Press Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping local server...")
    finally:
        httpd.server_close()
    return url
