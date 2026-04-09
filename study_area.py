from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import geopandas as gpd
from shapely import force_2d
from shapely.geometry import LineString, box
from shapely.ops import transform, unary_union

from config import (
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
):
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
    gdf = gdf.to_crs(TARGET_CRS)
    return clean_union(gdf.geometry)


def load_island_geometry_metric():
    roi_geom = load_boundary_geometry(ROI_BOUNDARY_PATH, layer=ROI_BOUNDARY_LAYER)
    ni_geom = load_boundary_geometry(NI_BOUNDARY_PATH, layer=NI_BOUNDARY_LAYER)
    return clean_union([roi_geom, ni_geom])


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


def load_study_area_metric():
    island_geom_metric = load_island_geometry_metric()
    if STUDY_AREA_KIND == "m1_corridor":
        return load_m1_corridor_metric(island_geom_metric)
    if STUDY_AREA_KIND == "ireland":
        return island_geom_metric
    raise RuntimeError(f"Unsupported STUDY_AREA_KIND={STUDY_AREA_KIND!r}.")


def load_study_area_geometries():
    study_area_metric = _as_2d(load_study_area_metric())
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
