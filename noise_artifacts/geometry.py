"""
PostGIS geometry SQL helpers for the noise artifact pipeline.

All helpers operate on EPSG:2157 (metre-based) geometry.
Do NOT use degree-based snap grids here; precision is always in metres.
"""
from __future__ import annotations


def clean_geometry_sql(geom_expr: str) -> str:
    """Return an SQL fragment that extracts only polygons and makes them valid."""
    return f"ST_CollectionExtract(ST_MakeValid({geom_expr}), 3)"


def reduce_precision_sql(geom_expr: str, grid_metres: float = 0.1) -> str:
    """
    Return an SQL fragment that reduces coordinate precision.
    grid_metres is in metres and is only meaningful for EPSG:2157 geometry.
    """
    return f"ST_ReducePrecision({geom_expr}, {grid_metres})"


def subdivide_geometry_sql(geom_expr: str, max_vertices: int = 256) -> str:
    """Return an SQL fragment that subdivides a geometry for index efficiency."""
    return f"ST_Subdivide({geom_expr}, {max_vertices})"


def area_filter_fragment(geom_expr: str) -> str:
    """Return a WHERE clause fragment that excludes null, empty, or zero-area geometries."""
    return (
        f"{geom_expr} IS NOT NULL "
        f"AND NOT ST_IsEmpty({geom_expr}) "
        f"AND ST_Area({geom_expr}) > 0"
    )
