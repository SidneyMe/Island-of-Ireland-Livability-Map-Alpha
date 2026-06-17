from __future__ import annotations

import gzip
import json
import math
import mimetypes
import os
import re
import time
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
    noise_pmtiles_output_path,
    noise_pmtiles_url_path,
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
from serve_routes import resolve_get_route, resolve_head_route
from transit.export import EXPORTS_DIR, ZIP_FILENAME


STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"
MISSING_PRECOMPUTE_MESSAGE = "No PostGIS precompute found for current config"
CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")
RUNTIME_STALE_FALLBACK_ENV = "LIVABILITY_RUNTIME_ALLOW_STALE_DEV_RUNTIME"
FILE_STREAM_CHUNK_SIZE = 64 * 1024


def _require_finite_float(raw_value: Any, *, field_name: str) -> float:
    if isinstance(raw_value, bool):
        raise ValueError(f"{field_name} must be numeric")
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    return value


def _require_finite_float_in_range(
    raw_value: Any,
    *,
    field_name: str,
    minimum: float,
    maximum: float,
) -> float:
    value = _require_finite_float(raw_value, field_name=field_name)
    if value < minimum or value > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def _require_exact_int(raw_value: Any, *, field_name: str) -> int:
    value = _require_finite_float(raw_value, field_name=field_name)
    integer_value = int(value)
    if float(integer_value) != value:
        raise ValueError(f"{field_name} must be an integer")
    return integer_value


def _require_tile_coordinates(z: Any, x: Any, y: Any) -> tuple[int, int, int]:
    tile_z = _require_exact_int(z, field_name="tile z")
    if tile_z < 0 or tile_z > SURFACE_MAX_ZOOM:
        raise ValueError(f"tile z must be between 0 and {SURFACE_MAX_ZOOM}")

    max_tile_coord = (1 << tile_z) - 1
    tile_x = _require_exact_int(x, field_name="tile x")
    tile_y = _require_exact_int(y, field_name="tile y")
    if tile_x < 0 or tile_x > max_tile_coord:
        raise ValueError(f"tile x must be between 0 and {max_tile_coord} for zoom {tile_z}")
    if tile_y < 0 or tile_y > max_tile_coord:
        raise ValueError(f"tile y must be between 0 and {max_tile_coord} for zoom {tile_z}")
    return tile_z, tile_x, tile_y


def _stream_file_response(
    handler: BaseHTTPRequestHandler,
    file_path: Path,
    *,
    content_type: str,
    status: HTTPStatus = HTTPStatus.OK,
    extra_headers: dict[str, str] | None = None,
    head_only: bool = False,
) -> None:
    resolved = file_path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(str(resolved))

    file_size = resolved.stat().st_size
    record_response = getattr(handler, "_record_response", None)
    if callable(record_response):
        record_response(status, 0 if head_only else file_size)
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    for header_name, header_value in (extra_headers or {}).items():
        handler.send_header(header_name, header_value)
    handler.send_header("Content-Length", str(file_size))
    handler.end_headers()
    if head_only:
        return

    with resolved.open("rb") as handle:
        while True:
            chunk = handle.read(FILE_STREAM_CHUNK_SIZE)
            if not chunk:
                break
            handler.wfile.write(chunk)


def _env_truthy(name: str) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


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
    transport_subtier_counts: dict[str, int]
    transport_bus_frequency_counts: dict[str, int]
    transport_flag_counts: dict[str, int]
    transport_mode_counts: dict[str, int]
    noise_enabled: bool
    noise_counts: dict[str, int]
    noise_source_counts: dict[str, int]
    noise_metric_counts: dict[str, int]
    noise_band_counts: dict[str, int]
    noise_band_counts_by_metric: dict[str, dict[str, int]]
    noise_pmtiles_url: str | None
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
    runtime_mode: str
    runtime_warning: str | None
    noise_proxy_metadata: dict[str, Any] | None


class RuntimeService:
    def __init__(self, engine, *, profile: str = "full") -> None:
        self._engine = engine
        self._profile = normalize_build_profile(profile)
        self._profile_settings = build_profile_settings(self._profile)
        self._hashes = build_config_hashes(self._profile)
        self._pmtiles_url = pmtiles_url_path(self._profile)
        self._noise_pmtiles_url = noise_pmtiles_url_path(self._profile)
        self._noise_pmtiles_path = noise_pmtiles_output_path(self._profile)
        self._allow_stale_runtime_fallback = _env_truthy(RUNTIME_STALE_FALLBACK_ENV)
        self._state: RuntimeState | None = None
        self._surface_runtime: _surface.FineSurfaceRuntime | None = None

    def _load_latest_completed_manifest_for_extract(self) -> dict[str, Any] | None:
        with self._engine.connect() as connection:
            row = connection.exec_driver_sql(
                """
                SELECT *
                FROM build_manifest
                WHERE extract_path = %(extract_path)s
                  AND status = 'complete'
                ORDER BY completed_at DESC NULLS LAST, created_at DESC
                LIMIT 1
                """,
                {"extract_path": str(OSM_EXTRACT_PATH)},
            ).mappings().first()
        if row is None:
            return None
        return dict(row)

    def _load_latest_transport_ready_manifest_for_extract(self) -> dict[str, Any] | None:
        if not hasattr(self._engine, "connect"):
            return None
        try:
            with self._engine.connect() as connection:
                row = connection.exec_driver_sql(
                    """
                    SELECT bm.*
                    FROM build_manifest AS bm
                    WHERE bm.extract_path = %(extract_path)s
                      AND bm.status = 'complete'
                      AND COALESCE((bm.summary_json ->> 'transport_reality_enabled')::boolean, false)
                      AND (
                        COALESCE((bm.summary_json -> 'transport_subtier_counts')::text, '{}') NOT IN ('{}', 'null')
                        OR COALESCE((bm.summary_json -> 'transport_bus_frequency_counts')::text, '{}') NOT IN ('{}', 'null')
                        OR COALESCE((bm.summary_json -> 'transport_flag_counts')::text, '{}') NOT IN ('{}', 'null')
                        OR COALESCE((bm.summary_json -> 'transport_mode_counts')::text, '{}') NOT IN ('{}', 'null')
                        OR EXISTS (
                          SELECT 1
                          FROM transport_reality AS t
                          WHERE t.build_key = bm.build_key
                            AND (
                              COALESCE(t.bus_service_subtier, '') <> ''
                              OR COALESCE(t.bus_frequency_tier, '') <> ''
                              OR t.has_any_bus_service
                              OR t.has_daily_bus_service
                            )
                        )
                      )
                    ORDER BY bm.completed_at DESC NULLS LAST, bm.created_at DESC
                    LIMIT 1
                    """,
                    {"extract_path": str(OSM_EXTRACT_PATH)},
                ).mappings().first()
        except Exception:
            return None
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _summary_has_transport_counts(summary_json: Any) -> bool:
        if not isinstance(summary_json, dict):
            return False
        for key in (
            "transport_subtier_counts",
            "transport_bus_frequency_counts",
            "transport_flag_counts",
            "transport_mode_counts",
        ):
            value = summary_json.get(key)
            if isinstance(value, dict) and len(value) > 0:
                return True
        return False

    def _transport_quality_row_count(self, build_key: str) -> int:
        if not hasattr(self._engine, "connect"):
            return 0
        try:
            with self._engine.connect() as connection:
                value = connection.exec_driver_sql(
                    """
                    SELECT COUNT(*)::bigint AS transport_quality_rows
                    FROM transport_reality AS t
                    WHERE t.build_key = %(build_key)s
                      AND (
                        COALESCE(t.bus_service_subtier, '') <> ''
                        OR COALESCE(t.bus_frequency_tier, '') <> ''
                        OR t.has_any_bus_service
                        OR t.has_daily_bus_service
                      )
                    """,
                    {"build_key": str(build_key)},
                ).scalar_one()
            return int(value or 0)
        except Exception:
            return 0

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
        runtime_mode = "strict_manifest"
        runtime_warning: str | None = None
        if manifest is None and self._allow_stale_runtime_fallback:
            manifest = self._load_latest_completed_manifest_for_extract()
            if manifest is not None:
                runtime_mode = "stale_manifest_fallback"
                runtime_warning = (
                    "Using latest completed build_manifest for this extract path because "
                    f"{RUNTIME_STALE_FALLBACK_ENV}=1; config hash does not match current runtime config."
                )
                summary = manifest.get("summary_json", {}) or {}
                transport_ready = self._summary_has_transport_counts(summary)
                if not transport_ready:
                    transport_ready = self._transport_quality_row_count(
                        str(manifest.get("build_key") or "")
                    ) > 0
                if not transport_ready:
                    replacement = self._load_latest_transport_ready_manifest_for_extract()
                    if replacement is not None and replacement.get("build_key") != manifest.get("build_key"):
                        manifest = replacement
                        runtime_warning = (
                            "Using latest transport-ready completed build_manifest for this extract path "
                            f"because {RUNTIME_STALE_FALLBACK_ENV}=1 and the newest completed manifest "
                            "lacks GTFS transport-reality summary coverage."
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
        raw_transport_subtier_counts = summary_json.get("transport_subtier_counts", {}) or {}
        transport_subtier_counts = {
            str(subtier): int(value)
            for subtier, value in (
                raw_transport_subtier_counts.items()
                if isinstance(raw_transport_subtier_counts, dict)
                else ()
            )
            if str(subtier)
        }
        raw_transport_bus_frequency_counts = summary_json.get("transport_bus_frequency_counts", {}) or {}
        transport_bus_frequency_counts = {
            str(tier): int(value)
            for tier, value in (
                raw_transport_bus_frequency_counts.items()
                if isinstance(raw_transport_bus_frequency_counts, dict)
                else ()
            )
            if str(tier)
        }
        raw_transport_flag_counts = summary_json.get("transport_flag_counts", {}) or {}
        transport_flag_counts = {
            str(flag_name): int(value)
            for flag_name, value in (
                raw_transport_flag_counts.items()
                if isinstance(raw_transport_flag_counts, dict)
                else ()
            )
            if str(flag_name)
        }
        raw_transport_mode_counts = summary_json.get("transport_mode_counts", {}) or {}
        transport_mode_counts = {
            str(mode): int(value)
            for mode, value in (
                raw_transport_mode_counts.items()
                if isinstance(raw_transport_mode_counts, dict)
                else ()
            )
            if str(mode)
        }
        raw_noise_counts = summary_json.get("noise_counts", {}) or {}
        noise_counts = {
            str(key): int(value)
            for key, value in (
                raw_noise_counts.items()
                if isinstance(raw_noise_counts, dict)
                else ()
            )
            if str(key)
        }
        raw_noise_source_counts = summary_json.get("noise_source_counts", {}) or {}
        noise_source_counts = {
            str(key): int(value)
            for key, value in (
                raw_noise_source_counts.items()
                if isinstance(raw_noise_source_counts, dict)
                else ()
            )
            if str(key)
        }
        raw_noise_metric_counts = summary_json.get("noise_metric_counts", {}) or {}
        noise_metric_counts = {
            str(key): int(value)
            for key, value in (
                raw_noise_metric_counts.items()
                if isinstance(raw_noise_metric_counts, dict)
                else ()
            )
            if str(key)
        }
        raw_noise_band_counts = summary_json.get("noise_band_counts", {}) or {}
        noise_band_counts = {
            str(key): int(value)
            for key, value in (
                raw_noise_band_counts.items()
                if isinstance(raw_noise_band_counts, dict)
                else ()
            )
            if str(key)
        }
        raw_noise_band_counts_by_metric = summary_json.get("noise_band_counts_by_metric", {}) or {}
        noise_band_counts_by_metric: dict[str, dict[str, int]] = {}
        if isinstance(raw_noise_band_counts_by_metric, dict):
            for metric, counts in raw_noise_band_counts_by_metric.items():
                metric_key = str(metric).strip()
                if not metric_key or not isinstance(counts, dict):
                    continue
                normalized_counts = {
                    str(key): int(value)
                    for key, value in counts.items()
                    if str(key)
                }
                if normalized_counts:
                    noise_band_counts_by_metric[metric_key] = normalized_counts
        noise_rows_available = bool(summary_json.get("noise_enabled")) or bool(noise_source_counts)
        noise_pmtiles_available = noise_rows_available and self._noise_pmtiles_path.exists()
        ordered_metrics = [
            metric
            for metric in ("Lden", "Lnight")
            if int(noise_metric_counts.get(metric, 0)) > 0
        ]
        if not ordered_metrics:
            ordered_metrics = sorted(
                [
                    str(metric)
                    for metric, count in noise_metric_counts.items()
                    if str(metric) and int(count) > 0
                ]
            )
        ordered_kinds = [
            source_kind
            for source_kind in ("road", "rail", "airport", "industry")
            if int(noise_source_counts.get(source_kind, 0)) > 0
        ]
        if not ordered_kinds:
            ordered_kinds = sorted(
                [
                    str(source_kind)
                    for source_kind, count in noise_source_counts.items()
                    if str(source_kind) and int(count) > 0
                ]
            )
        default_metric = "Lden" if "Lden" in ordered_metrics else (ordered_metrics[0] if ordered_metrics else "Lden")
        has_resolved_exact = any(source_kind in ordered_kinds for source_kind in ("airport", "industry"))
        methods = (
            ["official_derived_grid_proxy", "official_resolved_contour"]
            if has_resolved_exact
            else ["official_derived_grid_proxy"]
        )
        confidences = (
            ["proxy_not_measured", "official_modelled_not_measured"]
            if has_resolved_exact
            else ["proxy_not_measured"]
        )
        noise_proxy_metadata = {
            "enabled": bool(noise_pmtiles_available),
            "source_id": "noise",
            "source_layer": "noise_proxy",
            "kind": list(ordered_kinds),
            "kinds": list(ordered_kinds),
            "metrics": ordered_metrics,
            "default_metric": default_metric,
            "method": methods[0] if len(methods) == 1 else "mixed",
            "methods": methods,
            "confidence": (
                "proxy_or_modelled_not_measured"
                if has_resolved_exact
                else confidences[0]
            ),
            "confidences": confidences,
            "actual_road_geometry": False,
            "actual_rail_geometry": False,
            "actual_airport_geometry": False,
            "actual_industry_geometry": False,
            "actual_runway_geometry": False,
            "actual_geometry": False,
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
            surface_shell_hash = _surface.build_surface_shell_hash(str(manifest["geo_hash"]))
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
            transport_subtier_counts=transport_subtier_counts,
            transport_bus_frequency_counts=transport_bus_frequency_counts,
            transport_flag_counts=transport_flag_counts,
            transport_mode_counts=transport_mode_counts,
            noise_enabled=noise_pmtiles_available,
            noise_counts=noise_counts,
            noise_source_counts=noise_source_counts,
            noise_metric_counts=noise_metric_counts,
            noise_band_counts=noise_band_counts,
            noise_band_counts_by_metric=noise_band_counts_by_metric,
            noise_pmtiles_url=self._noise_pmtiles_url if noise_pmtiles_available else None,
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
            runtime_mode=runtime_mode,
            runtime_warning=runtime_warning,
            noise_proxy_metadata=noise_proxy_metadata,
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
            "transport_subtier_counts": state.transport_subtier_counts,
            "transport_bus_frequency_counts": state.transport_bus_frequency_counts,
            "transport_flag_counts": state.transport_flag_counts,
            "transport_mode_counts": state.transport_mode_counts,
            "noise_enabled": state.noise_enabled,
            "noise_counts": state.noise_counts,
            "noise_source_counts": state.noise_source_counts,
            "noise_metric_counts": state.noise_metric_counts,
            "noise_band_counts": state.noise_band_counts,
            "noise_band_counts_by_metric": state.noise_band_counts_by_metric,
            "noise_pmtiles_url": state.noise_pmtiles_url,
            "category_colors": CATEGORY_COLORS,
            "default_zoom": SURFACE_DEFAULT_ZOOM,
            "max_zoom": SURFACE_MAX_ZOOM,
            "fine_surface_enabled": state.fine_surface_enabled,
            "pmtiles_url": pmtiles_url_path(state.build_profile),
            "transport_reality_enabled": state.transport_reality_enabled,
            "service_deserts_enabled": state.service_deserts_enabled,
            "transport_reality_download_url": state.transport_reality_download_url,
            "transit_analysis_date": state.transit_analysis_date,
            "transit_analysis_window_days": state.transit_analysis_window_days,
            "transit_service_desert_window_days": state.transit_service_desert_window_days,
            "overture_dataset": state.overture_dataset,
            "runtime_mode": state.runtime_mode,
            "runtime_warning": state.runtime_warning,
            "noise_proxy_metadata": state.noise_proxy_metadata,
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
        normalized_resolution = _require_exact_int(resolution_m, field_name="resolution_m")
        if normalized_resolution not in self.state().fine_resolutions:
            raise ValueError(f"Unsupported fine surface resolution: {resolution_m}")
        tile_z, tile_x, tile_y = _require_tile_coordinates(z, x, y)
        return self.surface_runtime().render_tile(
            resolution_m=normalized_resolution,
            z=tile_z,
            x=tile_x,
            y=tile_y,
        )

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, Any]:
        normalized_lat = _require_finite_float_in_range(
            lat,
            field_name="inspect lat",
            minimum=-90.0,
            maximum=90.0,
        )
        normalized_lon = _require_finite_float_in_range(
            lon,
            field_name="inspect lon",
            minimum=-180.0,
            maximum=180.0,
        )
        normalized_zoom = None
        if zoom is not None:
            normalized_zoom = _require_finite_float_in_range(
                zoom,
                field_name="inspect zoom",
                minimum=0.0,
                maximum=float(SURFACE_MAX_ZOOM),
            )
        return self.surface_runtime().inspect(
            lat=normalized_lat,
            lon=normalized_lon,
            zoom=normalized_zoom,
        )


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
        pmtiles_paths_by_url_path: dict[str, Path] | None = None,
        noise_pmtiles_path: Path | None = None,
        noise_pmtiles_url_path: str | None = None,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.service = service
        self.static_dir = static_dir.resolve()
        self.index_html = index_html
        self.pmtiles_path = pmtiles_path
        self.pmtiles_url_path = str(pmtiles_url_path)
        self.pmtiles_paths_by_url_path = {
            str(pmtiles_url_path): pmtiles_path,
            **{
                str(url_path): path
                for url_path, path in (pmtiles_paths_by_url_path or {}).items()
            },
        }
        self.noise_pmtiles_path = noise_pmtiles_path
        self.noise_pmtiles_url_path = (
            str(noise_pmtiles_url_path) if noise_pmtiles_url_path else None
        )

    def pmtiles_path_for_url(self, url_path: str) -> Path | None:
        return self.pmtiles_paths_by_url_path.get(str(url_path))


class LivabilityRequestHandler(BaseHTTPRequestHandler):
    server_version = "LivabilityLocal/2.0"

    @property
    def livability_server(self) -> LivabilityHTTPServer:
        return self.server  # type: ignore[return-value]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        route = resolve_get_route(
            parsed.path,
            static_dir=self.livability_server.static_dir,
            export_path=EXPORTS_DIR / ZIP_FILENAME,
            pmtiles_path_for_url=self.livability_server.pmtiles_path_for_url,
            noise_pmtiles_url_path=self.livability_server.noise_pmtiles_url_path,
            noise_pmtiles_path=self.livability_server.noise_pmtiles_path,
        )
        self._begin_request_log(
            method="GET",
            path=parsed.path,
            route_name=(route.kind if route is not None else "unknown"),
        )
        try:
            self._dispatch_request(parsed, route=route)
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
        finally:
            self._log_request_summary()

    def do_HEAD(self) -> None:  # noqa: N802
        # MapLibre/PMTiles will send HEAD for the archive on some flows.
        parsed = urlsplit(self.path)
        pmtiles_path = resolve_head_route(
            parsed.path,
            pmtiles_path_for_url=self.livability_server.pmtiles_path_for_url,
            noise_pmtiles_url_path=self.livability_server.noise_pmtiles_url_path,
            noise_pmtiles_path=self.livability_server.noise_pmtiles_path,
        )
        self._begin_request_log(
            method="HEAD",
            path=parsed.path,
            route_name="pmtiles" if pmtiles_path is not None else "unknown",
        )
        if pmtiles_path is not None:
            try:
                self._serve_pmtiles(pmtiles_path, head_only=True)
            except CLIENT_DISCONNECT_ERRORS as exc:
                self._log_client_disconnect(parsed.path, exc)
        else:
            self._record_response(HTTPStatus.METHOD_NOT_ALLOWED, 0)
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.end_headers()
        self._log_request_summary()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        del format, args

    def _dispatch_request(self, parsed, *, route) -> None:
        if route is None:
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return

        if route.kind == "root":
            self._write_bytes(
                HTTPStatus.OK,
                self.livability_server.index_html,
                "text/html; charset=utf-8",
                extra_headers={"Cache-Control": "no-store"},
            )
            return
        if route.kind == "export":
            self._serve_export(route.target_path)
            return
        if route.kind == "pmtiles":
            self._serve_pmtiles(route.target_path)
            return
        if route.kind == "surface_tile":
            resolution_m, z, x, y = (int(value) for value in route.groups)
            self._serve_surface_tile(
                resolution_m=resolution_m,
                z=z,
                x=x,
                y=y,
            )
            return
        if route.kind == "static":
            self._serve_static(route.target_path)
            return
        if route.kind == "api_runtime":
            self._write_json(HTTPStatus.OK, self.livability_server.service.get_runtime())
            return
        if route.kind == "api_inspect":
            self._serve_inspect(parsed.query)
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _begin_request_log(self, *, method: str, path: str, route_name: str) -> None:
        self._request_method = method
        self._request_path = path
        self._request_route_name = route_name
        self._request_started_at = time.perf_counter()
        self._request_status = None
        self._request_response_bytes = None
        self._request_client_disconnected = False

    def _record_response(self, status: HTTPStatus, response_bytes: int | None = None) -> None:
        self._request_status = status
        if response_bytes is not None:
            self._request_response_bytes = int(response_bytes)

    def _log_request_summary(self) -> None:
        started_at = getattr(self, "_request_started_at", None)
        duration_ms = 0.0
        if started_at is not None:
            duration_ms = max(time.perf_counter() - float(started_at), 0.0) * 1000.0
        status = getattr(self, "_request_status", None)
        status_value = int(status) if status is not None else HTTPStatus.INTERNAL_SERVER_ERROR
        route_name = getattr(self, "_request_route_name", "unknown")
        response_bytes = getattr(self, "_request_response_bytes", None)
        parts = [
            "[server]",
            f"{getattr(self, '_request_method', 'GET')}",
            f"{getattr(self, '_request_path', self.path)}",
            f"route={route_name}",
            f"status={status_value}",
            f"duration_ms={duration_ms:.1f}",
        ]
        if response_bytes is not None:
            parts.append(f"bytes={int(response_bytes)}")
        if getattr(self, "_request_client_disconnected", False):
            parts.append("disconnected=true")
        print(" ".join(parts))

    def _serve_pmtiles(self, pmtiles_path: Path | None, *, head_only: bool = False) -> None:
        if pmtiles_path is None or not pmtiles_path.exists():
            raise FileNotFoundError(str(pmtiles_path))
        file_size = pmtiles_path.stat().st_size
        range_header = self.headers.get("Range")

        if range_header:
            match = RANGE_RE.match(range_header.strip())
            if not match:
                self._record_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, 0)
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return
            start = int(match.group(1))
            end_text = match.group(2)
            end = int(end_text) if end_text else file_size - 1
            if start >= file_size:
                self._record_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, 0)
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return
            end = min(end, file_size - 1)
            length = end - start + 1
            self._record_response(HTTPStatus.PARTIAL_CONTENT, 0 if head_only else length)
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

        self._record_response(HTTPStatus.OK, 0 if head_only else file_size)
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
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        _stream_file_response(
            self,
            resolved,
            content_type=content_type,
            extra_headers={"Cache-Control": "no-store"},
        )

    def _serve_export(self, export_path: Path) -> None:
        _stream_file_response(
            self,
            export_path,
            content_type="application/zip",
            extra_headers={
                "Content-Disposition": f'attachment; filename="{export_path.name}"',
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
        self._record_response(HTTPStatus.OK, len(payload))
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
        self._record_response(status, len(body))
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
        self._record_response(status, len(body))
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        for header_name, header_value in (extra_headers or {}).items():
            self.send_header(header_name, header_value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _log_client_disconnect(self, path: str, exc: BaseException) -> None:
        self._request_client_disconnected = True
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
    noise_pmtiles_path: Path | None = None,
) -> LivabilityHTTPServer:
    normalized_profile = normalize_build_profile(profile)
    resolved_pmtiles_path = pmtiles_path or pmtiles_output_path(normalized_profile)
    resolved_pmtiles_url_path = pmtiles_url_path(normalized_profile)
    resolved_pmtiles_paths_by_url_path = {
        pmtiles_url_path(candidate_profile): pmtiles_output_path(candidate_profile)
        for candidate_profile in ("full", "dev", "test")
        if pmtiles_output_path(candidate_profile).exists()
    }
    resolved_pmtiles_paths_by_url_path[resolved_pmtiles_url_path] = resolved_pmtiles_path
    resolved_noise_pmtiles_path = (
        noise_pmtiles_path or noise_pmtiles_output_path(normalized_profile)
    )
    resolved_noise_pmtiles_url_path = noise_pmtiles_url_path(normalized_profile)
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
        pmtiles_paths_by_url_path=resolved_pmtiles_paths_by_url_path,
        noise_pmtiles_path=resolved_noise_pmtiles_path,
        noise_pmtiles_url_path=resolved_noise_pmtiles_url_path,
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
