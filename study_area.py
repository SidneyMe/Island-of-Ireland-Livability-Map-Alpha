from __future__ import annotations

import hashlib
import math
import os
import pickle
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import geopandas as gpd
import shapely
from shapely import force_2d
from shapely.errors import GEOSException
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Polygon, box
from shapely.ops import transform, unary_union

from config import (
    CACHE_DIR,
    COASTAL_ARTIFACT_WIDTH_M,
    COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2,
    COASTAL_COMPONENT_PRESERVE_AREA_M2,
    COUNTY_BOUNDARY_LAYER,
    COUNTY_BOUNDARY_NAME_FIELD,
    COUNTY_BOUNDARY_PATH,
    M1_CORRIDOR_ANCHORS_WGS84,
    M1_CORRIDOR_BUFFER_M,
    NI_BOUNDARY_LAYER,
    NI_BOUNDARY_PATH,
    ROI_BOUNDARY_LAYER,
    ROI_BOUNDARY_PATH,
    TARGET_CRS,
    TO_TARGET,
    TO_WGS84,
    build_profile_settings,
)


_GEO_SHARED_CACHE_DIR = CACHE_DIR / "geo_shared"
_GEO_SHARED_SCHEMA_VERSION = 1
_GEO_SHARED_CACHE_ENABLED = (
    os.getenv("LIVABILITY_GEO_SHARED_CACHE", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)


def _file_fingerprint(path: Path) -> str:
    try:
        stat = path.stat()
        return f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"
    except FileNotFoundError:
        return f"{path.name}:missing"


def _geo_shared_key(*parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _geo_shared_load(prefix: str, key: str):
    if not _GEO_SHARED_CACHE_ENABLED:
        return None
    path = _GEO_SHARED_CACHE_DIR / f"{prefix}_{key}.pkl"
    if not path.exists():
        return None
    try:
        with path.open("rb") as handle:
            return pickle.load(handle)
    except (EOFError, pickle.UnpicklingError, OSError):
        return None


def _geo_shared_save(prefix: str, key: str, data: Any) -> None:
    if not _GEO_SHARED_CACHE_ENABLED:
        return
    _GEO_SHARED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    final = _GEO_SHARED_CACHE_DIR / f"{prefix}_{key}.pkl"
    tmp = final.with_suffix(".pkl.tmp")
    try:
        with tmp.open("wb") as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, final)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise


def _emit_progress(progress_cb, detail: str) -> None:
    if progress_cb is None:
        return
    progress_cb("detail", detail=detail, force_log=True)


def _emit_substep(progress_cb, substep_name: str, seconds: float) -> None:
    if progress_cb is None:
        return
    progress_cb(
        "substep",
        substep_name=substep_name,
        seconds=max(float(seconds), 0.0),
        force_log=True,
    )


def _as_2d(geometry):
    if geometry is None or geometry.is_empty:
        return geometry
    return force_2d(geometry)


_POLYGON_GEOMETRY_TYPES = {"Polygon", "MultiPolygon"}


def _read_boundary_file(path: Path, *, layer=None, geometry_only: bool) -> gpd.GeoDataFrame:
    read_kwargs = {"layer": layer} if layer else {}
    if not geometry_only:
        return gpd.read_file(path, **read_kwargs)

    try:
        return gpd.read_file(path, columns=[], **read_kwargs)
    except (TypeError, ValueError):
        return gpd.read_file(path, **read_kwargs)


def _coverage_union_fast_path(geometries) -> Any:
    coverage_union_all = getattr(shapely, "coverage_union_all", None)
    if coverage_union_all is None:
        raise RuntimeError("coverage_union_all is unavailable")
    return _as_2d(coverage_union_all(geometries))


def _union_cleaned_geometries(geometries) -> Any:
    geometry_list = list(geometries)
    if not geometry_list:
        raise ValueError("No geometries supplied for union.")

    if all(geometry.geom_type in _POLYGON_GEOMETRY_TYPES for geometry in geometry_list):
        try:
            unioned = _coverage_union_fast_path(geometry_list)
        except (GEOSException, RuntimeError, TypeError, ValueError):
            unioned = None
        if unioned is not None and not unioned.is_empty and unioned.is_valid:
            return unioned

    unioned = unary_union(geometry_list)
    unioned = _as_2d(unioned)
    if not unioned.is_valid:
        unioned = unioned.buffer(0)
        unioned = _as_2d(unioned)
    return unioned


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
    return _union_cleaned_geometries(cleaned)


def load_boundary_geometry(
    path: Path,
    *,
    layer=None,
    source_crs=None,
    row_filter: Callable[[gpd.GeoDataFrame], Any] | None = None,
    label: str | None = None,
    progress_cb=None,
):
    boundary_label = label or path.stem
    _emit_progress(progress_cb, f"reading boundary file {path.name}")
    read_started_at = time.perf_counter()
    gdf = _read_boundary_file(path, layer=layer, geometry_only=row_filter is None)
    _emit_substep(progress_cb, f"{boundary_label}_read", time.perf_counter() - read_started_at)
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
    project_started_at = time.perf_counter()
    gdf = gdf.to_crs(TARGET_CRS)
    _emit_substep(
        progress_cb,
        f"{boundary_label}_project",
        time.perf_counter() - project_started_at,
    )
    _emit_progress(progress_cb, f"unioning geometries from {path.name}")
    union_started_at = time.perf_counter()
    unioned = clean_union(gdf.geometry)
    _emit_substep(progress_cb, f"{boundary_label}_union", time.perf_counter() - union_started_at)
    return unioned


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
    skip_area_threshold_m2: float = 0.0,
):
    original_component = _as_2d(component)
    if original_component is None or original_component.is_empty:
        return "skip", original_component, None

    # Mainland short-circuit: components above the configured area threshold
    # skip the morphological opening (erode 50m / dilate 50m) entirely. The
    # cleanup only catches narrow coastal spurs; a mainland body has none, so
    # spending ~100s on it is wasted work. Default 0.0 disables this gate.
    if (
        skip_area_threshold_m2 > 0.0
        and float(original_component.area) > skip_area_threshold_m2
    ):
        return "skipped_large", original_component, None

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


def _clean_coastal_artifact_components(
    geometry,
    *,
    artifact_width_m: float,
    preserve_area_m2: float,
    skip_area_threshold_m2: float = 0.0,
    progress_cb=None,
):
    parts = [
        _as_2d(part)
        for part in _geometry_components(geometry)
        if part is not None and not part.is_empty
    ]
    if not parts:
        raise ValueError("clean_coastal_artifacts: all components were removed before cleanup.")

    total_components = len(parts)
    largest_component_index = max(range(total_components), key=lambda index: parts[index].area)
    cleaned_parts: list[Any] = []
    fallback_diagnostics: list[dict[str, Any]] = []
    if total_components > 1:
        _emit_progress(progress_cb, f"cleaning coastal artifacts across {total_components:,} components")

    components_started_at = time.perf_counter()

    def _process(component_index: int):
        part = parts[component_index]
        cleanup_mode, cleaned, diagnostic = _cleanup_coastal_component(
            part,
            artifact_width_m=artifact_width_m,
            component_index=component_index,
            skip_area_threshold_m2=skip_area_threshold_m2,
        )
        return component_index, cleanup_mode, cleaned, diagnostic

    if total_components > 1 and largest_component_index >= 0:
        _emit_progress(
            progress_cb,
            (
                "cleaning coastal artifacts: "
                f"largest component {largest_component_index + 1:,}/{total_components:,}"
            ),
        )

    # Shapely 2.x GEOS calls release the GIL; threads avoid pickling giant
    # geometries that a ProcessPool would require.
    worker_count = max(1, min(os.cpu_count() or 1, 8))
    results: list[tuple[int, str, Any, Any]] = [None] * total_components  # type: ignore[list-item]
    if total_components > 1 and worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for component_index, cleanup_mode, cleaned, diagnostic in executor.map(
                _process, range(total_components)
            ):
                results[component_index] = (component_index, cleanup_mode, cleaned, diagnostic)
    else:
        for component_index in range(total_components):
            results[component_index] = _process(component_index)

    for component_index, cleanup_mode, cleaned, diagnostic in results:
        part = parts[component_index]
        if diagnostic is not None:
            fallback_diagnostics.append(diagnostic)

        if cleanup_mode in ("original", "skipped_large"):
            cleaned_parts.append(cleaned)
        elif cleaned is None or cleaned.is_empty:
            if part.area >= float(preserve_area_m2):
                cleaned_parts.append(part)
        else:
            cleaned_parts.append(cleaned)

    if total_components > 1 and largest_component_index >= 0:
        _emit_progress(
            progress_cb,
            (
                "cleaning coastal artifacts: "
                f"largest component complete ({largest_component_index + 1:,}/{total_components:,})"
            ),
        )

    _emit_substep(
        progress_cb,
        "coastal_cleanup_components",
        time.perf_counter() - components_started_at,
    )
    return cleaned_parts, fallback_diagnostics


def clean_coastal_artifacts(
    geometry,
    *,
    artifact_width_m: float = COASTAL_ARTIFACT_WIDTH_M,
    preserve_area_m2: float = COASTAL_COMPONENT_PRESERVE_AREA_M2,
    skip_area_threshold_m2: float = COASTAL_CLEANUP_SKIP_MAINLAND_AREA_M2,
    progress_cb=None,
):
    """Remove ultra-narrow coastal spurs via morphological opening in metric space.

    Each polygon component of *geometry* (already in a metric CRS) is eroded by
    *artifact_width_m* then dilated back only to detect coastal appendages that
    extend materially away from the surviving coastline. Those artifacts are then
    pruned from the original component so surviving shoreline vertices stay sharp.
    Components that would otherwise be erased entirely are kept if their original
    area is at least *preserve_area_m2* (protects real islands from being discarded).
    Components whose area exceeds *skip_area_threshold_m2* skip the cleanup entirely
    (default 0.0 disables the gate; use it to avoid ~100s spent opening the mainland,
    which has no narrow coastal spurs that would be caught by erode/dilate).
    """
    cleaned_parts, fallback_diagnostics = _clean_coastal_artifact_components(
        geometry,
        artifact_width_m=artifact_width_m,
        preserve_area_m2=preserve_area_m2,
        skip_area_threshold_m2=skip_area_threshold_m2,
        progress_cb=progress_cb,
    )

    if not cleaned_parts:
        raise ValueError("clean_coastal_artifacts: all components were removed by cleanup.")

    _report_coastal_cleanup_fallbacks(fallback_diagnostics)
    if len(cleaned_parts) > 1:
        _emit_progress(progress_cb, "reassembling cleaned coastal components")
    union_started_at = time.perf_counter()
    result = _union_cleaned_geometries(cleaned_parts)
    _emit_substep(progress_cb, "coastal_cleanup_union", time.perf_counter() - union_started_at)
    return result


_BOUNDARY_SIMPLIFY_TOLERANCE_M = 5.0


def _load_simplified_boundary_cached(
    path: Path,
    *,
    layer,
    label: str,
    tolerance_m: float,
    progress_cb=None,
):
    """Read + project + union + simplify a boundary, cached by file fingerprint.

    Shared cache lives in CACHE_DIR/geo_shared/ and is keyed only by the boundary
    file's size + mtime + TARGET_CRS + simplify tolerance. Config version bumps
    (e.g. CACHE_SCHEMA_VERSION, COASTAL_CLEANUP_ALGORITHM_VERSION) do not
    invalidate this cache — the 215s ROI simplify survives downstream churn.
    """
    fp = _file_fingerprint(path)
    key = _geo_shared_key(
        "simplified_boundary",
        _GEO_SHARED_SCHEMA_VERSION,
        fp,
        TARGET_CRS,
        f"{tolerance_m:.6f}",
    )
    cached = _geo_shared_load(f"{label}_simplified", key)
    if cached is not None:
        _emit_progress(progress_cb, f"reusing cached simplified {label} boundary")
        _emit_substep(progress_cb, f"{label}_read", 0.0)
        _emit_substep(progress_cb, f"{label}_project", 0.0)
        _emit_substep(progress_cb, f"{label}_union", 0.0)
        _emit_substep(progress_cb, f"{label}_simplify", 0.0)
        return cached

    geom = load_boundary_geometry(
        path,
        layer=layer,
        label=label,
        progress_cb=progress_cb,
    )
    _emit_progress(progress_cb, f"simplifying {label.upper()} boundary before merge")
    simplify_started_at = time.perf_counter()
    geom = _as_2d(geom.simplify(tolerance_m, preserve_topology=True))
    _emit_substep(progress_cb, f"{label}_simplify", time.perf_counter() - simplify_started_at)

    try:
        _geo_shared_save(f"{label}_simplified", key, geom)
    except OSError:
        pass
    return geom


def _load_merged_island_cached(
    roi_geom,
    ni_geom,
    *,
    tolerance_m: float,
    roi_fingerprint: str,
    ni_fingerprint: str,
    progress_cb=None,
):
    """Merge ROI + NI + post-merge simplify, cached by boundary fingerprints."""
    key = _geo_shared_key(
        "merged_island",
        _GEO_SHARED_SCHEMA_VERSION,
        roi_fingerprint,
        ni_fingerprint,
        TARGET_CRS,
        f"{tolerance_m:.6f}",
    )
    cached = _geo_shared_load("island_merged", key)
    if cached is not None:
        _emit_progress(progress_cb, "reusing cached merged island boundary")
        _emit_substep(progress_cb, "island_merge", 0.0)
        _emit_substep(progress_cb, "island_simplify", 0.0)
        return cached

    _emit_progress(progress_cb, "merging ROI and NI boundaries")
    merge_started_at = time.perf_counter()
    merged = clean_union([roi_geom, ni_geom])
    _emit_substep(progress_cb, "island_merge", time.perf_counter() - merge_started_at)

    _emit_progress(progress_cb, "simplifying merged island boundary")
    simplify_started_at = time.perf_counter()
    merged = _as_2d(merged.simplify(tolerance_m, preserve_topology=True))
    _emit_substep(progress_cb, "island_simplify", time.perf_counter() - simplify_started_at)

    try:
        _geo_shared_save("island_merged", key, merged)
    except OSError:
        pass
    return merged


def load_island_geometry_metric(*, progress_cb=None):
    roi_fp = _file_fingerprint(ROI_BOUNDARY_PATH)
    ni_fp = _file_fingerprint(NI_BOUNDARY_PATH)
    roi_geom = _load_simplified_boundary_cached(
        ROI_BOUNDARY_PATH,
        layer=ROI_BOUNDARY_LAYER,
        label="roi",
        tolerance_m=_BOUNDARY_SIMPLIFY_TOLERANCE_M,
        progress_cb=progress_cb,
    )
    ni_geom = _load_simplified_boundary_cached(
        NI_BOUNDARY_PATH,
        layer=NI_BOUNDARY_LAYER,
        label="ni",
        tolerance_m=_BOUNDARY_SIMPLIFY_TOLERANCE_M,
        progress_cb=progress_cb,
    )
    raw = _load_merged_island_cached(
        roi_geom,
        ni_geom,
        tolerance_m=_BOUNDARY_SIMPLIFY_TOLERANCE_M,
        roi_fingerprint=roi_fp,
        ni_fingerprint=ni_fp,
        progress_cb=progress_cb,
    )
    _emit_progress(progress_cb, "cleaning coastal artifacts")
    return clean_coastal_artifacts(raw, progress_cb=progress_cb)


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


def load_county_geometry_metric(county_name: str, *, progress_cb=None):
    normalized_name = str(county_name or "").strip()
    if not normalized_name:
        raise RuntimeError("County study area configuration requires a non-empty county name.")

    boundary_label = f"county_{normalized_name.lower()}"
    _emit_progress(progress_cb, f"reading county boundary file {COUNTY_BOUNDARY_PATH.name}")
    read_started_at = time.perf_counter()
    gdf = _read_boundary_file(COUNTY_BOUNDARY_PATH, layer=COUNTY_BOUNDARY_LAYER, geometry_only=False)
    _emit_substep(progress_cb, f"{boundary_label}_read", time.perf_counter() - read_started_at)
    if gdf.empty:
        raise ValueError(f"No features loaded from {COUNTY_BOUNDARY_PATH}")
    if COUNTY_BOUNDARY_NAME_FIELD not in gdf.columns:
        raise RuntimeError(
            f"County boundary file {COUNTY_BOUNDARY_PATH} is missing {COUNTY_BOUNDARY_NAME_FIELD!r}."
        )

    county_series = gdf[COUNTY_BOUNDARY_NAME_FIELD].astype(str).str.strip().str.casefold()
    target_name = normalized_name.casefold()
    gdf = gdf.loc[county_series == target_name].copy()
    if gdf.empty:
        raise RuntimeError(
            f"Configured county study area {normalized_name!r} was not found in {COUNTY_BOUNDARY_PATH.name}."
        )
    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        raise RuntimeError(
            f"Configured county study area {normalized_name!r} has no usable geometry in {COUNTY_BOUNDARY_PATH.name}."
        )
    if gdf.crs is None:
        raise ValueError(f"{COUNTY_BOUNDARY_PATH} has no CRS. Supply source_crs explicitly.")

    _emit_progress(progress_cb, f"projecting county {normalized_name} to {TARGET_CRS}")
    project_started_at = time.perf_counter()
    gdf = gdf.to_crs(TARGET_CRS)
    _emit_substep(progress_cb, f"{boundary_label}_project", time.perf_counter() - project_started_at)

    _emit_progress(progress_cb, f"unioning county geometry for {normalized_name}")
    union_started_at = time.perf_counter()
    unioned = clean_union(gdf.geometry)
    _emit_substep(progress_cb, f"{boundary_label}_union", time.perf_counter() - union_started_at)
    return unioned


def load_bbox_geometry_metric(
    island_geom_metric,
    bbox_wgs84: tuple[float, float, float, float] | None,
    *,
    progress_cb=None,
):
    if bbox_wgs84 is None or len(bbox_wgs84) != 4:
        raise RuntimeError("BBox study area configuration requires four WGS84 coordinates.")

    min_lon, min_lat, max_lon, max_lat = (
        float(bbox_wgs84[0]),
        float(bbox_wgs84[1]),
        float(bbox_wgs84[2]),
        float(bbox_wgs84[3]),
    )
    if not all(math.isfinite(value) for value in (min_lon, min_lat, max_lon, max_lat)):
        raise RuntimeError("BBox study area configuration must use finite WGS84 coordinates.")
    if min_lon >= max_lon or min_lat >= max_lat:
        raise RuntimeError(
            "BBox study area configuration must satisfy min_lon < max_lon and min_lat < max_lat."
        )

    _emit_progress(progress_cb, "building bbox study area in WGS84")
    bbox_wgs84_geom = _as_2d(box(min_lon, min_lat, max_lon, max_lat))
    _emit_progress(progress_cb, "projecting bbox study area to metric CRS")
    bbox_metric = _as_2d(transform(TO_TARGET, bbox_wgs84_geom))
    if not bbox_metric.is_valid:
        bbox_metric = _as_2d(bbox_metric.buffer(0))

    _emit_progress(progress_cb, "clipping bbox study area to island geometry")
    clipped = _as_2d(bbox_metric.intersection(_as_2d(island_geom_metric)))
    if not clipped.is_valid:
        clipped = _as_2d(clipped.buffer(0))
    if clipped.is_empty:
        raise RuntimeError("Configured bbox study area is empty after clipping to the island boundary.")
    return clipped


def load_study_area_metric(*, profile: str | None = None, progress_cb=None):
    settings = build_profile_settings(profile)
    if settings.study_area_kind == "county":
        _emit_progress(
            progress_cb,
            f"loading county study area {settings.study_area_county_name}",
        )
        return load_county_geometry_metric(
            str(settings.study_area_county_name or ""),
            progress_cb=progress_cb,
        )

    island_geom_metric = load_island_geometry_metric(progress_cb=progress_cb)
    if settings.study_area_kind == "m1_corridor":
        _emit_progress(progress_cb, "clipping island geometry to M1 corridor")
        return load_m1_corridor_metric(island_geom_metric)
    if settings.study_area_kind == "bbox":
        _emit_progress(progress_cb, "clipping island geometry to configured bbox")
        return load_bbox_geometry_metric(
            island_geom_metric,
            settings.study_area_bbox_wgs84,
            progress_cb=progress_cb,
        )
    if settings.study_area_kind == "ireland":
        return island_geom_metric
    raise RuntimeError(f"Unsupported STUDY_AREA_KIND={settings.study_area_kind!r}.")


def load_study_area_geometries(*, profile: str | None = None, progress_cb=None):
    study_area_metric = _as_2d(load_study_area_metric(profile=profile, progress_cb=progress_cb))
    _emit_progress(progress_cb, "transforming study area to WGS84")
    transform_started_at = time.perf_counter()
    study_area_wgs84 = _as_2d(transform(TO_WGS84, study_area_metric))
    _emit_substep(
        progress_cb,
        "study_area_wgs84_transform",
        time.perf_counter() - transform_started_at,
    )
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
