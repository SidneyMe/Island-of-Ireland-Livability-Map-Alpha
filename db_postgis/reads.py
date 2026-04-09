from __future__ import annotations

from typing import Any

from ._dependencies import Engine, func, select
from .common import root_module
from .tables import amenities, build_manifest, features, grid_walk, import_manifest


def load_source_amenity_rows(engine: Engine, import_fingerprint: str, study_area_wgs84) -> list[dict[str, Any]]:
    root = root_module()
    study_area = root.from_shape(study_area_wgs84, srid=4326)
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                features.c.category,
                features.c.osm_type,
                features.c.osm_id,
                func.ST_PointOnSurface(features.c.geom).label("point_geom"),
            )
            .where(features.c.import_fingerprint == import_fingerprint)
            .where(func.ST_Intersects(features.c.geom, study_area))
            .order_by(features.c.category, features.c.osm_type, features.c.osm_id)
        ).mappings().all()

    return [
        {
            "category": row["category"],
            "source_ref": f"{row['osm_type']}/{row['osm_id']}",
            "geom": root.to_shape(row["point_geom"]),
        }
        for row in rows
    ]


def load_walk_rows(engine: Engine, build_key: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                grid_walk.c.resolution_m,
                grid_walk.c.cell_id,
                grid_walk.c.centre_geom,
                grid_walk.c.cell_geom,
                grid_walk.c.counts_json,
                grid_walk.c.scores_json,
                grid_walk.c.total_score,
            )
            .where(grid_walk.c.build_key == build_key)
            .order_by(grid_walk.c.resolution_m, grid_walk.c.cell_id)
        ).mappings().all()

    root = root_module()
    return [
        {
            "resolution_m": row["resolution_m"],
            "cell_id": row["cell_id"],
            "centre_geom": root.to_shape(row["centre_geom"]),
            "cell_geom": root.to_shape(row["cell_geom"]),
            "counts_json": row["counts_json"],
            "scores_json": row["scores_json"],
            "total_score": row["total_score"],
        }
        for row in rows
    ]


def load_walk_rows_for_resolutions(
    engine: Engine,
    build_key: str,
    resolutions: list[int],
) -> list[dict[str, Any]]:
    if not resolutions:
        return []

    normalized_resolutions = sorted({int(resolution) for resolution in resolutions}, reverse=True)
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                grid_walk.c.resolution_m,
                grid_walk.c.cell_id,
                grid_walk.c.centre_geom,
                grid_walk.c.cell_geom,
                grid_walk.c.counts_json,
                grid_walk.c.scores_json,
                grid_walk.c.total_score,
            )
            .where(grid_walk.c.build_key == build_key)
            .where(grid_walk.c.resolution_m.in_(normalized_resolutions))
            .order_by(grid_walk.c.resolution_m.desc(), grid_walk.c.cell_id)
        ).mappings().all()

    root = root_module()
    return [
        {
            "resolution_m": row["resolution_m"],
            "cell_id": row["cell_id"],
            "centre_geom": root.to_shape(row["centre_geom"]),
            "cell_geom": root.to_shape(row["cell_geom"]),
            "counts_json": row["counts_json"],
            "scores_json": row["scores_json"],
            "total_score": row["total_score"],
        }
        for row in rows
    ]


def load_amenity_rows(engine: Engine, build_key: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(amenities.c.category, amenities.c.geom, amenities.c.source, amenities.c.source_ref)
            .where(amenities.c.build_key == build_key)
            .order_by(amenities.c.category, amenities.c.source_ref)
        ).mappings().all()

    root = root_module()
    return [
        {
            "category": row["category"],
            "geom": root.to_shape(row["geom"]),
            "source": row["source"],
            "source_ref": row["source_ref"],
        }
        for row in rows
    ]


def load_available_resolutions(engine: Engine, build_key: str) -> list[int]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(grid_walk.c.resolution_m)
            .where(grid_walk.c.build_key == build_key)
            .distinct()
            .order_by(grid_walk.c.resolution_m)
        ).all()
    return sorted((int(row[0]) for row in rows), reverse=True)


def load_build_manifest(engine: Engine, build_key: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            select(build_manifest)
            .where(build_manifest.c.build_key == build_key)
        ).mappings().first()
    return dict(row) if row is not None else None


def load_import_manifest(engine: Engine, import_fingerprint: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            select(import_manifest)
            .where(import_manifest.c.import_fingerprint == import_fingerprint)
        ).mappings().first()
    return dict(row) if row is not None else None
