from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import geopandas as gpd
from shapely import force_2d
from shapely.errors import GEOSException
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Polygon, box
from shapely.ops import transform, unary_union

from config import (
    COASTAL_ARTIFACT_WIDTH_M,
    COASTAL_COMPONENT_PRESERVE_AREA_M2,
    M1_CORRIDOR_ANCHORS_WGS84,
    M1_CORRIDOR_BUFFER_M,
    NI_BOUNDARY_LAYER,
    NI_BOUNDARY_PATH,
    ROI_BOUNDARY_LAYER,
    ROI_BOUNDARY_PATH,
    STUDY_AREA_KIND,
    TARGET_CRS,
    TO_TARGET,
    TO_WGS84,
)


def _emit_progress(progress_cb, detail: str) -> None:
    if progress_cb is None:
        return
    progress_cb("detail", detail=detail, force_log=True)


def _as_2d(geometry):
    if geometry is None or geometry.is_empty:
        return geometry
    return force_2d(geometry)


def clean_union(geometries) -> Any:
    cleaned = []
    for geometry in geometries:
        geometry = _as_2d(geometry)
        if geometry is None or geometry.is_empty:
            continue
        if not geometry.is_valid:
            geometry = geometry.buffer(0)
            geometry = _as_2d(geometry)
        if not geometry.is_empty:
            cleaned.append(geometry)
    if not cleaned:
        raise ValueError("No valid geometries found after cleaning.")
    unioned = unary_union(cleaned)
    unioned = _as_2d(unioned)
    if not unioned.is_valid:
        unioned = unioned.buffer(0)
        unioned = _as_2d(unioned)
    return unioned


def load_boundary_geometry(
    path: Path,
    *,
    layer=None,
    source_crs=None,
    row_filter: Callable[[gpd.GeoDataFrame], Any] | None = None,
    progress_cb=None,
):
    _emit_progress(progress_cb, f"reading boundary file {path.name}")
    gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)
    if gdf.empty:
        raise ValueError(f"No features loaded from {path}")
    if row_filter is not None:
        gdf = gdf.loc[row_filter(gdf)].copy()
    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        raise ValueError(f"No non-null geometries in {path}")
    if gdf.crs is None:
        if source_crs is None:
            raise ValueError(f"{path} has no CRS. Supply source_crs explicitly.")
        gdf = gdf.set_crs(source_crs, allow_override=True)
    _emit_progress(progress_cb, f"projecting {path.name} to {TARGET_CRS}")
    gdf = gdf.to_crs(TARGET_CRS)
    _emit_progress(progress_cb, f"unioning geometries from {path.name}")
    return clean_union(gdf.geometry)


def _geometry_components(geometry):
    """Yield individual Polygon components from a Polygon or MultiPolygon."""
    geometry = _as_2d(geometry)
    if geometry is None or geometry.is_empty:
        return
    if isinstance(geometry, Polygon):
        yield geometry
    elif isinstance(geometry, MultiPolygon):
        for part in geometry.geoms:
            yield part
    elif isinstance(geometry, GeometryCollection):
        for part in geometry.geoms:
            yield from _geometry_components(part)


def _normalize_coastal_component(component):
    component = _as_2d(component)
    if component is None or component.is_empty:
        return component
    if not component.is_valid:
        component = _as_2d(component.buffer(0))
    return component


def _open_coastal_component(component, *, artifact_width_m: float):
    component = _normalize_coastal_component(component)
    if component is None or component.is_empty:
        return component

    eroded = _as_2d(component.buffer(-float(artifact_width_m), join_style="mitre"))
    if eroded.is_empty:
        return eroded

    restored = _as_2d(eroded.buffer(float(artifact_width_m), join_style="mitre"))
    if not restored.is_valid:
        restored = _as_2d(restored.buffer(0))
    return restored


def _metric_bounds_to_wgs84_bounds(bounds: tuple[float, float, float, float]) -> dict[str, float]:
    minx, miny, maxx, maxy = bounds
    corners_wgs84 = [
        TO_WGS84(minx, miny),
        TO_WGS84(minx, maxy),
        TO_WGS84(maxx, miny),
        TO_WGS84(maxx, maxy),
    ]
    lons = [float(lon) for lon, lat in corners_wgs84]
    lats = [float(lat) for lon, lat in corners_wgs84]
    return {
        "min_lat": min(lats),
        "min_lon": min(lons),
        "max_lat": max(lats),
        "max_lon": max(lons),
    }


def _component_cleanup_diagnostic(component, *, component_index: int, cleanup_mode: str) -> dict[str, Any]:
    representative_point_wgs84 = _as_2d(transform(TO_WGS84, component.representative_point()))
    return {
        "component_index": int(component_index),
        "cleanup_mode": str(cleanup_mode),
        "area_m2": float(component.area),
        "representative_lat": float(representative_point_wgs84.y),
        "representative_lon": float(representative_point_wgs84.x),
        "bounds_wgs84": _metric_bounds_to_wgs84_bounds(component.bounds),
    }


def _coastal_artifact_regions(
    original_component,
    opened_component,
    *,
    artifact_width_m: float,
):
    original_component = _normalize_coastal_component(original_component)
    opened_component = _normalize_coastal_component(opened_component)
    if original_component is None or original_component.is_empty:
        return []
    if opened_component is None or opened_component.is_empty:
        return [original_component]

    artifact_candidates = _normalize_coastal_component(
        original_component.difference(opened_component)
    )
    if artifact_candidates is None or artifact_candidates.is_empty:
        return []

    distance_threshold_m = max(1.0, float(artifact_width_m) / 2.0)
    boundary_zone = _as_2d(opened_component.boundary.buffer(distance_threshold_m))
    far_core = _normalize_coastal_component(artifact_candidates.difference(boundary_zone))
    if far_core is None or far_core.is_empty:
        return []

    artifact_regions = _normalize_coastal_component(
        artifact_candidates.intersection(far_core.buffer(distance_threshold_m))
    )
    if artifact_regions is None or artifact_regions.is_empty:
        return []
    return list(_geometry_components(artifact_regions))


def _prune_coastal_artifacts(
    original_component,
    opened_component,
    *,
    artifact_width_m: float,
):
    artifact_regions = _coastal_artifact_regions(
        original_component,
        opened_component,
        artifact_width_m=artifact_width_m,
    )
    if not artifact_regions:
        return _normalize_coastal_component(original_component)

    pruned = _normalize_coastal_component(
        original_component.difference(unary_union(artifact_regions))
    )
    return pruned


def _cleanup_coastal_component(
    component,
    *,
    artifact_width_m: float,
    component_index: int = -1,
):
    original_component = _as_2d(component)
    if original_component is None or original_component.is_empty:
        return "skip", original_component, None

    try:
        opened = _open_coastal_component(
            original_component,
            artifact_width_m=artifact_width_m,
        )
        cleaned = _prune_coastal_artifacts(
            original_component,
            opened,
            artifact_width_m=artifact_width_m,
        )
        return "primary", cleaned, None
    except GEOSException:
        simplify_tolerance_m = max(1.0, float(artifact_width_m) / 3.0)
        try:
            simplified_component = _as_2d(
                original_component.simplify(
                    simplify_tolerance_m,
                    preserve_topology=True,
                )
            )
        except GEOSException:
            diagnostic = _component_cleanup_diagnostic(
                original_component,
                component_index=component_index,
                cleanup_mode="original",
            )
            return "original", original_component, diagnostic
        if simplified_component is None or simplified_component.is_empty:
            diagnostic = _component_cleanup_diagnostic(
                original_component,
                component_index=component_index,
                cleanup_mode="original",
            )
            return "original", original_component, diagnostic
        try:
            opened = _open_coastal_component(
                simplified_component,
                artifact_width_m=artifact_width_m,
            )
            cleaned = _prune_coastal_artifacts(
                original_component,
                opened,
                artifact_width_m=artifact_width_m,
            )
            diagnostic = _component_cleanup_diagnostic(
                original_component,
                component_index=component_index,
                cleanup_mode="degraded",
            )
            return "degraded", cleaned, diagnostic
        except GEOSException:
            diagnostic = _component_cleanup_diagnostic(
                original_component,
                component_index=component_index,
                cleanup_mode="original",
            )
            return "original", original_component, diagnostic


def _report_coastal_cleanup_fallbacks(fallback_diagnostics: list[dict[str, Any]]) -> None:
    if not fallback_diagnostics:
        return

    for diagnostic in fallback_diagnostics:
        bounds = diagnostic["bounds_wgs84"]
        print(
            "  [geo] coastal cleanup fallback used: "
            f"component={diagnostic['component_index']} "
            f"mode={diagnostic['cleanup_mode']} "
            f"rep={diagnostic['representative_lat']:.6f},{diagnostic['representative_lon']:.6f} "
            f"area_m2={diagnostic['area_m2']:.0f} "
            "bounds_wgs84="
            f"({bounds['min_lat']:.6f},{bounds['min_lon']:.6f})"
            "->"
            f"({bounds['max_lat']:.6f},{bounds['max_lon']:.6f})"
        )


def clean_coastal_artifacts(
    geometry,
    *,
    artifact_width_m: float = COASTAL_ARTIFACT_WIDTH_M,
    preserve_area_m2: float = COASTAL_COMPONENT_PRESERVE_AREA_M2,
):
    """Remove ultra-narrow coastal spurs via morphological opening in metric space.

    Each polygon component of *geometry* (already in a metric CRS) is eroded by
    *artifact_width_m* then dilated back only to detect coastal appendages that
    extend materially away from the surviving coastline. Those artifacts are then
    pruned from the original component so surviving shoreline vertices stay sharp.
    Components that would otherwise be erased entirely are kept if their original
    area is at least *preserve_area_m2* (protects real islands from being discarded).
    """
    cleaned_parts = []
    fallback_diagnostics: list[dict[str, Any]] = []
    for component_index, part in enumerate(_geometry_components(geometry)):
        part = _as_2d(part)
        if part is None or part.is_empty:
            continue

        cleanup_mode, cleaned, diagnostic = _cleanup_coastal_component(
            part,
            artifact_width_m=artifact_width_m,
            component_index=component_index,
        )
        if diagnostic is not None:
            fallback_diagnostics.append(diagnostic)

        if cleanup_mode == "original":
            cleaned_parts.append(cleaned)
            continue

        if cleaned is None or cleaned.is_empty:
            if part.area >= float(preserve_area_m2):
                cleaned_parts.append(part)
        else:
            cleaned_parts.append(cleaned)

    if not cleaned_parts:
        raise ValueError("clean_coastal_artifacts: all components were removed by cleanup.")

    _report_coastal_cleanup_fallbacks(fallback_diagnostics)

    result = unary_union(cleaned_parts)
    result = _as_2d(result)
    if not result.is_valid:
        result = _as_2d(result.buffer(0))
    return result


_BOUNDARY_SIMPLIFY_TOLERANCE_M = 5.0


def load_island_geometry_metric(*, progress_cb=None):
    roi_geom = load_boundary_geometry(
        ROI_BOUNDARY_PATH,
        layer=ROI_BOUNDARY_LAYER,
        progress_cb=progress_cb,
    )
    ni_geom = load_boundary_geometry(
        NI_BOUNDARY_PATH,
        layer=NI_BOUNDARY_LAYER,
        progress_cb=progress_cb,
    )
    _emit_progress(progress_cb, "merging ROI and NI boundaries")
    raw = clean_union([roi_geom, ni_geom])
    _emit_progress(progress_cb, "simplifying merged island boundary")
    raw = _as_2d(raw.simplify(_BOUNDARY_SIMPLIFY_TOLERANCE_M, preserve_topology=True))
    _emit_progress(progress_cb, "cleaning coastal artifacts")
    return clean_coastal_artifacts(raw)


def load_m1_corridor_metric(island_geom_metric):
    if len(M1_CORRIDOR_ANCHORS_WGS84) < 2:
        raise RuntimeError("M1 corridor configuration requires at least two anchor coordinates.")

    island_geom_metric = _as_2d(island_geom_metric)
    corridor_line_wgs84 = LineString(M1_CORRIDOR_ANCHORS_WGS84)
    corridor_line_metric = _as_2d(transform(TO_TARGET, corridor_line_wgs84))
    buffered = _as_2d(corridor_line_metric.buffer(float(M1_CORRIDOR_BUFFER_M)))
    clipped = _as_2d(buffered.intersection(island_geom_metric))
    if not clipped.is_valid:
        clipped = clipped.buffer(0)
        clipped = _as_2d(clipped)
    if clipped.is_empty:
        raise RuntimeError("Configured M1 corridor study area is empty after clipping to the island boundary.")
    return clipped


def load_study_area_metric(*, progress_cb=None):
    island_geom_metric = load_island_geometry_metric(progress_cb=progress_cb)
    if STUDY_AREA_KIND == "m1_corridor":
        _emit_progress(progress_cb, "clipping island geometry to M1 corridor")
        return load_m1_corridor_metric(island_geom_metric)
    if STUDY_AREA_KIND == "ireland":
        return island_geom_metric
    raise RuntimeError(f"Unsupported STUDY_AREA_KIND={STUDY_AREA_KIND!r}.")


def load_study_area_geometries(*, progress_cb=None):
    study_area_metric = _as_2d(load_study_area_metric(progress_cb=progress_cb))
    _emit_progress(progress_cb, "transforming study area to WGS84")
    study_area_wgs84 = _as_2d(transform(TO_WGS84, study_area_metric))
    return study_area_metric, study_area_wgs84


def study_area_wgs84_envelope_from_metric(study_area_metric):
    minx, miny, maxx, maxy = study_area_metric.bounds
    corners_wgs84 = [
        TO_WGS84(minx, miny),
        TO_WGS84(minx, maxy),
        TO_WGS84(maxx, miny),
        TO_WGS84(maxx, maxy),
    ]
    lons = [lon for lon, lat in corners_wgs84]
    lats = [lat for lon, lat in corners_wgs84]
    return _as_2d(box(min(lons), min(lats), max(lons), max(lats)))
