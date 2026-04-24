from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any, Callable, Iterable, Iterator

from shapely.geometry import Point


PREP_PROGRESS_EVERY = 1_000


def _centre_point(cell: dict[str, Any]) -> Point:
    lat, lon = cell["centre"]
    return Point(lon, lat)


def _emit_progress(progress_cb, detail: str) -> None:
    if progress_cb is None:
        return
    progress_cb("detail", detail=detail, force_log=True)


def _ensure_row_geometry_2d(geometry, label: str, cell_id: str | None) -> None:
    if geometry is None:
        raise ValueError(f"{label} is missing geometry for cell_id={cell_id!r}.")
    if not bool(getattr(geometry, "has_z", False)):
        return
    raise ValueError(f"{label} contains a non-2D geometry for cell_id={cell_id!r}.")


@dataclass
class WalkRowPreparationStats:
    geometry_materialize_seconds: float = 0.0
    row_assembly_seconds: float = 0.0
    prepared_rows: int = 0


@dataclass
class AmenityRowPreparationStats:
    row_assembly_seconds: float = 0.0
    prepared_rows: int = 0


class PreparedRowStream:
    def __init__(self, row_count: int, iterator_factory: Callable[[], Iterator[dict[str, Any]]], stats):
        self.row_count = int(row_count)
        self._iterator_factory = iterator_factory
        self.stats = stats

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self._iterator_factory()

    def __len__(self) -> int:
        return self.row_count


def walk_row_count(walk_grids: dict[int, list[dict[str, Any]]]) -> int:
    return sum(len(cells) for cells in walk_grids.values())


def amenity_row_count(amenity_source_rows: list[dict[str, Any]]) -> int:
    return len(amenity_source_rows)


def _amenity_tier_counts(
    amenity_source_rows: list[dict[str, Any]] | None,
    *,
    categories: Iterable[str],
) -> dict[str, dict[str, int]]:
    counts = {str(category): {} for category in categories}
    for row in amenity_source_rows or []:
        category = str(row.get("category") or "")
        tier = row.get("tier")
        if not category or not isinstance(tier, str) or not tier:
            continue
        bucket = counts.setdefault(category, {})
        bucket[tier] = int(bucket.get(tier, 0)) + 1
    return counts


def _transport_subtier_counts(
    transport_reality_rows: list[dict[str, Any]] | None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in transport_reality_rows or []:
        subtier = str(row.get("bus_service_subtier") or "").strip()
        if not subtier:
            continue
        counts[subtier] = int(counts.get(subtier, 0)) + 1
    return counts


def _transport_bus_frequency_counts(
    transport_reality_rows: list[dict[str, Any]] | None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in transport_reality_rows or []:
        tier = str(row.get("bus_frequency_tier") or "").strip()
        if not tier:
            continue
        counts[tier] = int(counts.get(tier, 0)) + 1
    return counts


def _transport_flag_counts(
    transport_reality_rows: list[dict[str, Any]] | None,
) -> dict[str, int]:
    flag_names = (
        "is_unscheduled_stop",
        "has_exception_only_service",
        "has_any_bus_service",
        "has_daily_bus_service",
    )
    counts = {name: 0 for name in flag_names}
    for row in transport_reality_rows or []:
        for flag_name in flag_names:
            if bool(row.get(flag_name, False)):
                counts[flag_name] = int(counts.get(flag_name, 0)) + 1
    return counts


def _transport_mode_counts(
    transport_reality_rows: list[dict[str, Any]] | None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in transport_reality_rows or []:
        modes = row.get("route_modes_json") or row.get("route_modes") or []
        if isinstance(modes, str):
            modes = [value.strip() for value in modes.split(",")]
        seen = {
            str(mode).strip()
            for mode in modes
            if str(mode).strip()
        }
        for mode in seen:
            counts[mode] = int(counts.get(mode, 0)) + 1
    return counts


def iter_walk_rows_impl(
    walk_grids: dict[int, list[dict[str, Any]]],
    created_at: datetime,
    *,
    hashes,
    study_area_metric,
    materialize_cell_geometry,
    progress_cb=None,
    progress_every: int = PREP_PROGRESS_EVERY,
) -> PreparedRowStream:
    total_rows = walk_row_count(walk_grids)
    stats = WalkRowPreparationStats()

    def _iter_rows() -> Iterator[dict[str, Any]]:
        prepared_rows = 0
        for resolution_m, cells in walk_grids.items():
            for cell in cells:
                row_started_at = time.perf_counter()
                geometry_elapsed = 0.0
                geometry = cell.get("geometry")
                if geometry is None:
                    if study_area_metric is None:
                        raise ValueError(
                            "study_area_metric is required to materialize walk grid geometry"
                        )
                    geometry_started_at = time.perf_counter()
                    try:
                        geometry = materialize_cell_geometry(cell, study_area_metric)
                    except Exception as exc:
                        raise RuntimeError(
                            "Failed to materialize walk grid geometry "
                            f"for resolution_m={resolution_m!r}, "
                            f"cell_id={cell.get('cell_id')!r}, "
                            f"metric_bounds={cell.get('metric_bounds')!r}, "
                            f"clip_required={cell.get('clip_required')!r}."
                        ) from exc
                    geometry_elapsed = max(time.perf_counter() - geometry_started_at, 0.0)
                    stats.geometry_materialize_seconds += geometry_elapsed

                _ensure_row_geometry_2d(geometry, f"walk grid {resolution_m}m", cell.get("cell_id"))
                row = {
                    "build_key": hashes.build_key,
                    "config_hash": hashes.config_hash,
                    "import_fingerprint": hashes.import_fingerprint,
                    "resolution_m": resolution_m,
                    "cell_id": cell["cell_id"],
                    "centre_geom": _centre_point(cell),
                    "cell_geom": geometry,
                    "effective_area_m2": float(cell["effective_area_m2"]),
                    "effective_area_ratio": float(cell["effective_area_ratio"]),
                    "counts_json": cell["counts"],
                    "cluster_counts_json": cell["cluster_counts"],
                    "effective_units_json": cell["effective_units"],
                    "scores_json": cell["scores"],
                    "total_score": cell["total"],
                    "created_at": created_at,
                }
                stats.row_assembly_seconds += max(
                    time.perf_counter() - row_started_at - geometry_elapsed,
                    0.0,
                )
                prepared_rows += 1
                stats.prepared_rows = prepared_rows
                if prepared_rows % max(int(progress_every), 1) == 0 or prepared_rows == total_rows:
                    _emit_progress(
                        progress_cb,
                        f"preparing grid_walk rows {prepared_rows:,}/{total_rows:,}",
                    )
                yield row

    return PreparedRowStream(total_rows, _iter_rows, stats)


def walk_rows_impl(
    walk_grids: dict[int, list[dict[str, Any]]],
    created_at: datetime,
    *,
    hashes,
    study_area_metric,
    materialize_cell_geometry,
    progress_cb=None,
) -> list[dict[str, Any]]:
    return list(
        iter_walk_rows_impl(
            walk_grids,
            created_at,
            hashes=hashes,
            study_area_metric=study_area_metric,
            materialize_cell_geometry=materialize_cell_geometry,
            progress_cb=progress_cb,
        )
    )


def iter_amenity_rows_impl(
    amenity_source_rows: list[dict[str, Any]],
    created_at: datetime,
    *,
    hashes,
    progress_cb=None,
    progress_every: int = PREP_PROGRESS_EVERY,
) -> PreparedRowStream:
    total_rows = amenity_row_count(amenity_source_rows)
    stats = AmenityRowPreparationStats()

    def _iter_rows() -> Iterator[dict[str, Any]]:
        prepared_rows = 0
        for row in amenity_source_rows:
            row_started_at = time.perf_counter()
            payload = {
                "build_key": hashes.build_key,
                "config_hash": hashes.config_hash,
                "import_fingerprint": hashes.import_fingerprint,
                "category": row["category"],
                "tier": row.get("tier"),
                "geom": Point(row["lon"], row["lat"]),
                "source": str(row.get("source") or "osm_local_pbf"),
                "source_ref": row["source_ref"],
                "name": row.get("name"),
                "conflict_class": str(row.get("conflict_class") or "osm_only"),
                "created_at": created_at,
            }
            stats.row_assembly_seconds += max(time.perf_counter() - row_started_at, 0.0)
            prepared_rows += 1
            stats.prepared_rows = prepared_rows
            if prepared_rows % max(int(progress_every), 1) == 0 or prepared_rows == total_rows:
                _emit_progress(
                    progress_cb,
                    f"preparing amenities rows {prepared_rows:,}/{total_rows:,}",
                )
            yield payload

    return PreparedRowStream(total_rows, _iter_rows, stats)


def amenity_rows_impl(
    amenity_source_rows: list[dict[str, Any]],
    created_at: datetime,
    *,
    hashes,
    progress_cb=None,
) -> list[dict[str, Any]]:
    return list(
        iter_amenity_rows_impl(
            amenity_source_rows,
            created_at,
            hashes=hashes,
            progress_cb=progress_cb,
        )
    )


def summary_json_impl(
    study_area_wgs84,
    walk_grids: dict[int, list[dict[str, Any]]],
    amenity_data: dict[str, list[tuple[float, float]]],
    amenity_source_rows: list[dict[str, Any]] | None = None,
    transport_reality_rows: list[dict[str, Any]] | None = None,
    *,
    hashes,
    build_profile: str,
    source_state,
    osm_extract_path,
    grid_sizes_m: list[int],
    fine_resolutions_m: list[int],
    output_html,
    zoom_breaks,
    transit_reality_state=None,
    transit_analysis_window_days: int | None = None,
    transit_service_desert_window_days: int | None = None,
    transport_reality_download_url: str | None = None,
    service_deserts_enabled: bool = False,
    overture_dataset: dict[str, Any] | None = None,
) -> dict[str, Any]:
    centre = study_area_wgs84.centroid
    payload = {
        "build_key": hashes.build_key,
        "config_hash": hashes.config_hash,
        "build_profile": str(build_profile),
        "import_fingerprint": hashes.import_fingerprint,
        "extract_path": str(source_state.extract_path) if source_state is not None else str(osm_extract_path),
        "grid_sizes_m": grid_sizes_m,
        "coarse_vector_resolutions_m": grid_sizes_m,
        "fine_resolutions_m": fine_resolutions_m,
        "map_center": {"lat": centre.y, "lon": centre.x},
        "walk_cell_counts": {str(size): len(cells) for size, cells in walk_grids.items()},
        "amenity_counts": {category: len(points) for category, points in amenity_data.items()},
        "amenity_tier_counts": _amenity_tier_counts(
            amenity_source_rows,
            categories=amenity_data.keys(),
        ),
        "output_html": output_html,
        "zoom_breaks": zoom_breaks,
        "surface_zoom_breaks": zoom_breaks,
    }
    if transit_reality_state is not None:
        payload.update(
            {
                "transit_analysis_date": transit_reality_state.analysis_date.isoformat(),
                "transit_analysis_window_days": int(transit_analysis_window_days or 0),
                "transit_service_desert_window_days": int(
                    transit_service_desert_window_days or 0
                ),
                "transit_reality_fingerprint": transit_reality_state.reality_fingerprint,
                "transport_reality_enabled": True,
                "service_deserts_enabled": bool(service_deserts_enabled),
                "transport_reality_download_url": transport_reality_download_url,
                "transport_subtier_counts": _transport_subtier_counts(transport_reality_rows),
                "transport_bus_frequency_counts": _transport_bus_frequency_counts(
                    transport_reality_rows
                ),
                "transport_flag_counts": _transport_flag_counts(transport_reality_rows),
                "transport_mode_counts": _transport_mode_counts(transport_reality_rows),
            }
        )
    if overture_dataset:
        payload["overture_dataset"] = overture_dataset
    return payload
