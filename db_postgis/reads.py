from __future__ import annotations

from typing import Any

from ._dependencies import Engine, case, func, select, text
from .common import root_module
from .tables import (
    amenities,
    build_manifest,
    features,
    grid_walk,
    import_manifest,
    transit_gtfs_stop_reality,
    transit_service_desert_cells,
)


def load_osm_transport_features(
    engine: Engine,
    import_fingerprint: str,
    study_area_wgs84,
) -> list[dict[str, Any]]:
    root = root_module()
    study_area = root.from_shape(study_area_wgs84, srid=4326)
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                features.c.name,
                features.c.osm_type,
                features.c.osm_id,
                func.ST_PointOnSurface(features.c.geom).label("point_geom"),
                features.c.tags_json,
            )
            .where(features.c.import_fingerprint == import_fingerprint)
            .where(features.c.category == "transport")
            .where(func.ST_Intersects(features.c.geom, study_area))
            .order_by(features.c.osm_type, features.c.osm_id)
        ).mappings().all()

    return [
        {
            "source_ref": f"{row['osm_type']}/{row['osm_id']}",
            "name": row["name"],
            "geom": root.to_shape(row["point_geom"]),
            "tags_json": dict(row.get("tags_json") or {}),
        }
        for row in rows
    ]


def load_transport_reality_rows_for_scoring(
    engine: Engine,
    reality_fingerprint: str,
    study_area_wgs84,
) -> list[dict[str, Any]]:
    root = root_module()
    study_area = root.from_shape(study_area_wgs84, srid=4326)
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                transit_gtfs_stop_reality.c.source_ref,
                transit_gtfs_stop_reality.c.geom,
            )
            .where(transit_gtfs_stop_reality.c.reality_fingerprint == reality_fingerprint)
            .where(
                transit_gtfs_stop_reality.c.reality_status.not_in(
                    ("inactive_confirmed", "school_only_confirmed")
                )
            )
            .where(func.ST_Intersects(transit_gtfs_stop_reality.c.geom, study_area))
            .order_by(transit_gtfs_stop_reality.c.source_ref)
        ).mappings().all()

    return [
        {
            "category": "transport",
            "source_ref": row["source_ref"],
            "geom": root.to_shape(row["geom"]),
            "park_area_m2": 0.0,
        }
        for row in rows
    ]


def load_transport_reality_points(
    engine: Engine,
    reality_fingerprint: str,
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(transit_gtfs_stop_reality)
            .where(transit_gtfs_stop_reality.c.reality_fingerprint == reality_fingerprint)
            .order_by(transit_gtfs_stop_reality.c.source_ref)
        ).mappings().all()

    root = root_module()
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "source_ref": row["source_ref"],
                "stop_name": row["stop_name"],
                "feed_id": row["feed_id"],
                "stop_id": row["stop_id"],
                "reality_status": row["reality_status"],
                "source_status": row["source_status"],
                "school_only_state": row["school_only_state"],
                "public_departures_7d": int(row["public_departures_7d"] or 0),
                "public_departures_30d": int(row["public_departures_30d"] or 0),
                "school_only_departures_30d": int(row["school_only_departures_30d"] or 0),
                "last_public_service_date": row["last_public_service_date"],
                "last_any_service_date": row["last_any_service_date"],
                "route_modes_json": list(row.get("route_modes_json") or []),
                "source_reason_codes_json": list(row.get("source_reason_codes_json") or []),
                "reality_reason_codes_json": list(row.get("reality_reason_codes_json") or []),
                "geom": root.to_shape(row["geom"]),
            }
        )
    return payload


def load_service_desert_rows(engine: Engine, build_key: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(transit_service_desert_cells)
            .where(transit_service_desert_cells.c.build_key == build_key)
            .order_by(
                transit_service_desert_cells.c.resolution_m,
                transit_service_desert_cells.c.cell_id,
            )
        ).mappings().all()

    root = root_module()
    return [
        {
            "build_key": row["build_key"],
            "resolution_m": int(row["resolution_m"]),
            "cell_id": row["cell_id"],
            "analysis_date": row["analysis_date"],
            "baseline_reachable_stop_count": int(row["baseline_reachable_stop_count"] or 0),
            "reachable_public_departures_7d": int(row["reachable_public_departures_7d"] or 0),
            "reason_codes_json": list(row.get("reason_codes_json") or []),
            "cell_geom": root.to_shape(row["cell_geom"]),
        }
        for row in rows
    ]


def load_source_amenity_rows(
    engine: Engine,
    import_fingerprint: str,
    study_area_wgs84,
    *,
    transit_reality_fingerprint: str | None = None,
) -> list[dict[str, Any]]:
    root = root_module()
    study_area = root.from_shape(study_area_wgs84, srid=4326)
    park_area_m2 = case(
        (
            (features.c.category == "parks") & (func.ST_Dimension(features.c.geom) == 2),
            func.COALESCE(func.ST_Area(func.ST_Transform(features.c.geom, 2157)), 0.0),
        ),
        else_=0.0,
    ).label("park_area_m2")
    query = (
        select(
            features.c.category,
            features.c.osm_type,
            features.c.osm_id,
            func.ST_PointOnSurface(features.c.geom).label("point_geom"),
            park_area_m2,
        )
        .where(features.c.import_fingerprint == import_fingerprint)
        .where(features.c.category != "transport")
        .where(func.ST_Intersects(features.c.geom, study_area))
        .order_by(features.c.category, features.c.osm_type, features.c.osm_id)
    )
    amenity_rows = []
    with engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(query)
        for row in result.yield_per(500).mappings():
            amenity_rows.append(
                {
                    "category": row["category"],
                    "source_ref": f"{row['osm_type']}/{row['osm_id']}",
                    "geom": root.to_shape(row["point_geom"]),
                    "park_area_m2": float(row.get("park_area_m2") or 0.0),
                }
            )
    if transit_reality_fingerprint is not None:
        amenity_rows.extend(
            load_transport_reality_rows_for_scoring(
                engine,
                transit_reality_fingerprint,
                study_area_wgs84,
            )
        )
    return amenity_rows


def load_walk_rows(engine: Engine, build_key: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                grid_walk.c.resolution_m,
                grid_walk.c.cell_id,
                grid_walk.c.centre_geom,
                grid_walk.c.cell_geom,
                grid_walk.c.effective_area_m2,
                grid_walk.c.effective_area_ratio,
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
            "effective_area_m2": float(row["effective_area_m2"]),
            "effective_area_ratio": float(row["effective_area_ratio"]),
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
                grid_walk.c.effective_area_m2,
                grid_walk.c.effective_area_ratio,
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
            "effective_area_m2": float(row["effective_area_m2"]),
            "effective_area_ratio": float(row["effective_area_ratio"]),
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


def load_point_scores_for_build(
    engine: Engine,
    build_key: str,
    points: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not points:
        return []

    values_sql: list[str] = []
    params: dict[str, Any] = {"build_key": str(build_key)}
    for index, point in enumerate(points):
        values_sql.append(f"(:point_id_{index}, :lat_{index}, :lon_{index})")
        params[f"point_id_{index}"] = str(point["id"])
        params[f"lat_{index}"] = float(point["lat"])
        params[f"lon_{index}"] = float(point["lon"])

    statement = text(
        f"""
        WITH fixture_points(point_id, lat, lon) AS (
            VALUES
                {", ".join(values_sql)}
        ),
        matched AS (
            SELECT
                p.point_id,
                p.lat,
                p.lon,
                g.resolution_m,
                g.total_score,
                g.scores_json,
                g.counts_json,
                ROW_NUMBER() OVER (
                    PARTITION BY p.point_id
                    ORDER BY g.resolution_m ASC, g.cell_id ASC
                ) AS rownum
            FROM fixture_points AS p
            JOIN grid_walk AS g
              ON g.build_key = :build_key
             AND ST_Covers(
                    g.cell_geom,
                    ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)
                )
        )
        SELECT
            point_id,
            lat,
            lon,
            resolution_m,
            total_score,
            scores_json,
            counts_json
        FROM matched
        WHERE rownum = 1
        ORDER BY point_id
        """
    )

    with engine.connect() as connection:
        rows = connection.execute(statement, params).mappings().all()

    return [
        {
            "point_id": str(row["point_id"]),
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "resolution_m": int(row["resolution_m"]),
            "total_score": float(row["total_score"]),
            "scores_json": dict(row.get("scores_json") or {}),
            "counts_json": dict(row.get("counts_json") or {}),
        }
        for row in rows
    ]


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
