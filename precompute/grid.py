from __future__ import annotations

import math
from typing import Any

from shapely import force_2d
from shapely.errors import GEOSException
from shapely.geometry import box as shapely_box
from shapely.ops import clip_by_rect, transform
from shapely.prepared import prep

from config import CAPS, TO_WGS84


def _as_2d(geometry):
    if geometry is None or geometry.is_empty:
        return geometry
    return force_2d(geometry)


def _geometry_is_2d(geometry: Any) -> bool:
    return geometry is not None and not bool(getattr(geometry, "has_z", False))


def _has_effective_area_metadata(cell: dict[str, Any]) -> bool:
    area_value = cell.get("effective_area_m2")
    ratio_value = cell.get("effective_area_ratio")

    if not isinstance(area_value, (int, float)):
        return False
    if not isinstance(ratio_value, (int, float)):
        return False

    area_value = float(area_value)
    ratio_value = float(ratio_value)
    return (
        math.isfinite(area_value)
        and math.isfinite(ratio_value)
        and area_value >= 0.0
        and 0.0 <= ratio_value <= 1.0 + 1e-9
    )


def _grid_cells_are_2d(cells: list[dict[str, Any]] | None) -> bool:
    if cells is None:
        return False
    return all(
        isinstance(cell.get("clip_required"), bool)
        and _has_effective_area_metadata(cell)
        and (cell.get("geometry") is None or _geometry_is_2d(cell.get("geometry")))
        for cell in cells
    )


def _ensure_grid_geometries_2d(cells: list[dict[str, Any]], label: str) -> None:
    for cell in cells:
        geometry = cell.get("geometry")
        if geometry is None:
            raise ValueError(
                f"{label} is missing geometry for cell_id={cell.get('cell_id')!r}."
            )
        if _geometry_is_2d(geometry):
            continue
        raise ValueError(
            f"{label} contains a non-2D geometry for cell_id={cell.get('cell_id')!r}."
        )


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_cell_id(resolution_m: float, raw_minx: float, raw_miny: float) -> str:
    resolution_value = int(round(resolution_m))
    raw_minx_mm = int(round(raw_minx * 1000))
    raw_miny_mm = int(round(raw_miny * 1000))
    return f"{resolution_value}:{raw_minx_mm}:{raw_miny_mm}"


def _empty_scoring_fields() -> dict[str, Any]:
    return {
        "counts": {},
        "scores": {},
        "total": 0.0,
    }


def _metric_bounds_from_cell(cell: dict[str, Any]) -> tuple[float, float, float, float]:
    metric_bounds = cell.get("metric_bounds")
    if metric_bounds is not None:
        return tuple(float(value) for value in metric_bounds)

    resolution_text, minx_text, miny_text = str(cell["cell_id"]).split(":", 2)
    resolution_m = float(int(resolution_text))
    minx = int(minx_text) / 1000.0
    miny = int(miny_text) / 1000.0
    return (minx, miny, minx + resolution_m, miny + resolution_m)


def _clean_metric_geometry(geometry):
    geometry = _as_2d(geometry)
    if geometry is None or geometry.is_empty:
        return geometry
    if not geometry.is_valid:
        geometry = _as_2d(geometry.buffer(0))
    return geometry


def _clip_metric_geometry_to_bounds(
    study_geom_metric,
    bounds: tuple[float, float, float, float],
):
    minx, miny, maxx, maxy = bounds
    try:
        clipped = clip_by_rect(study_geom_metric, minx, miny, maxx, maxy)
    except GEOSException:
        # clip_by_rect can produce degenerate rings on complex coastal geometry;
        # intersection is slower but handles these edge cases correctly.
        clipped = study_geom_metric.intersection(shapely_box(minx, miny, maxx, maxy))
    return _clean_metric_geometry(clipped)


def _effective_area_metadata(geom_metric, raw_cell_area_m2: float) -> dict[str, float]:
    clipped_area_m2 = 0.0 if geom_metric is None else float(geom_metric.area)
    if raw_cell_area_m2 <= 0.0:
        return {
            "effective_area_m2": 0.0,
            "effective_area_ratio": 0.0,
        }
    clipped_area_m2 = min(max(clipped_area_m2, 0.0), float(raw_cell_area_m2))
    return {
        "effective_area_m2": clipped_area_m2,
        "effective_area_ratio": clipped_area_m2 / float(raw_cell_area_m2),
    }


def build_scoring_grid(
    spacing_m: float,
    study_geom_metric,
    keep_mode: str = "intersects",
    clip: bool = True,
) -> list[dict[str, Any]]:
    minx, miny, maxx, maxy = study_geom_metric.bounds
    prepared = prep(study_geom_metric)
    raw_cell_area_m2 = float(spacing_m) * float(spacing_m)

    cells: list[dict[str, Any]] = []
    y = miny
    while y < maxy:
        x = minx
        while x < maxx:
            raw_cell = shapely_box(x, y, x + spacing_m, y + spacing_m)

            if keep_mode == "within":
                keep = prepared.contains(raw_cell)
                needs_clip = clip and not keep
            elif keep_mode == "intersects":
                fully_inside = prepared.contains(raw_cell)
                keep = fully_inside or prepared.intersects(raw_cell)
                needs_clip = clip and keep and not fully_inside
            else:
                raise ValueError("keep_mode must be 'intersects' or 'within'")

            if keep:
                metric_bounds = (x, y, x + spacing_m, y + spacing_m)
                geom_metric = (
                    _clip_metric_geometry_to_bounds(study_geom_metric, metric_bounds)
                    if needs_clip
                    else raw_cell
                )
                if not geom_metric.is_empty:
                    anchor_metric = geom_metric.representative_point()
                    anchor_wgs84 = transform(TO_WGS84, anchor_metric)
                    geometry_wgs84 = transform(TO_WGS84, geom_metric)
                    area_metadata = _effective_area_metadata(geom_metric, raw_cell_area_m2)
                    cells.append(
                        {
                            "cell_id": build_cell_id(spacing_m, x, y),
                            "centre": (anchor_wgs84.y, anchor_wgs84.x),
                            "metric_bounds": metric_bounds,
                            "clip_required": bool(needs_clip),
                            **area_metadata,
                            "geometry": _as_2d(geometry_wgs84),
                            **_empty_scoring_fields(),
                        }
                    )
            x += spacing_m
        y += spacing_m

    return cells


def build_grid(
    spacing_m: float,
    study_geom_metric,
    keep_mode: str = "intersects",
    clip: bool = True,
) -> list[dict[str, Any]]:
    return build_scoring_grid(
        spacing_m,
        study_geom_metric,
        keep_mode=keep_mode,
        clip=clip,
    )


def _metric_geometry_for_cell(
    cell: dict[str, Any],
    study_geom_metric,
    *,
    clip: bool = True,
):
    minx, miny, maxx, maxy = _metric_bounds_from_cell(cell)
    raw_cell = shapely_box(minx, miny, maxx, maxy)
    clip_required = cell.get("clip_required")
    if clip_required is None:
        clip_required = bool(clip)
    if clip_required:
        return _clip_metric_geometry_to_bounds(study_geom_metric, (minx, miny, maxx, maxy))
    return raw_cell


def materialize_cell_geometry(
    cell: dict[str, Any],
    study_geom_metric,
    *,
    clip: bool = True,
):
    geometry = cell.get("geometry")
    if geometry is not None:
        return geometry
    return transform(
        TO_WGS84,
        _metric_geometry_for_cell(cell, study_geom_metric, clip=clip),
    )


def materialize_grid_geometry(
    cells: list[dict[str, Any]],
    study_geom_metric,
    *,
    clip: bool = True,
) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    for cell in cells:
        geometry = cell.get("geometry")
        if geometry is None:
            geometry = materialize_cell_geometry(cell, study_geom_metric, clip=clip)
            materialized.append({**cell, "geometry": geometry})
        else:
            materialized.append(cell)
    return materialized


def clone_scoring_grid_shells(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cloned: list[dict[str, Any]] = []
    for cell in cells:
        shell = {
            "cell_id": cell["cell_id"],
            "centre": cell["centre"],
            "metric_bounds": _metric_bounds_from_cell(cell),
            "clip_required": bool(cell.get("clip_required", True)),
            "effective_area_m2": float(cell["effective_area_m2"]),
            "effective_area_ratio": float(cell["effective_area_ratio"]),
            **_empty_scoring_fields(),
        }
        geometry = cell.get("geometry")
        if geometry is not None:
            shell["geometry"] = geometry
        cloned.append(shell)
    return cloned


def _clone_grid_shells(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return clone_scoring_grid_shells(cells)


_DENSITY_NORMALIZED_CATEGORIES = ("shops", "transport", "healthcare")
_MIN_DENSITY_AREA_RATIO = 0.25
_WEIGHTED_UNIT_CATEGORIES = frozenset({"shops", "healthcare", "parks"})


def _normalized_area_ratio(effective_area_ratio: Any) -> float:
    try:
        ratio = float(effective_area_ratio)
    except (TypeError, ValueError):
        ratio = 1.0
    if not math.isfinite(ratio):
        ratio = 1.0
    ratio = min(max(ratio, 0.0), 1.0)
    return max(ratio, _MIN_DENSITY_AREA_RATIO)


def score_cell(
    counts: dict[str, int],
    *,
    effective_area_ratio: float = 1.0,
    density_normalized_categories: tuple[str, ...] = _DENSITY_NORMALIZED_CATEGORIES,
    weighted_units: dict[str, float] | None = None,
) -> tuple[dict[str, float], float]:
    normalized_ratio = _normalized_area_ratio(effective_area_ratio)
    per_category: dict[str, float] = {}
    for category, cap in CAPS.items():
        raw_count = counts.get(category, 0)
        effective_count = float(raw_count)
        if weighted_units is not None and category in _WEIGHTED_UNIT_CATEGORIES:
            effective_count = float(weighted_units.get(category, 0.0))
        if category == "parks":
            effective_count = effective_count / normalized_ratio
        elif category in density_normalized_categories:
            effective_count = raw_count / normalized_ratio
            if weighted_units is not None and category in _WEIGHTED_UNIT_CATEGORIES:
                effective_count = float(weighted_units.get(category, 0.0)) / normalized_ratio
        per_category[category] = min(effective_count / cap, 1.0) * 25.0
    return per_category, sum(per_category.values())


def score_cells(
    cells: list[dict[str, Any]],
    counts_by_node: dict[Any, dict[str, int]],
    cell_nodes: list[Any],
    weighted_units_by_node: dict[Any, dict[str, int]] | None = None,
) -> None:
    if not cells:
        return

    for cell, node in zip(cells, cell_nodes):
        counts = dict(counts_by_node.get(node, {}))
        scores, total = score_cell(
            counts,
            effective_area_ratio=float(cell.get("effective_area_ratio", 1.0)),
            weighted_units=(
                None
                if weighted_units_by_node is None
                else {
                    str(category): float(value)
                    for category, value in dict(weighted_units_by_node.get(node, {})).items()
                }
            ),
        )
        cell["counts"] = counts
        cell["scores"] = scores
        cell["total"] = total
