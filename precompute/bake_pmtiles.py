"""Bake the precomputed grid + amenities into a single PMTiles archive.

Pure-Python pipeline: PostGIS ``ST_AsMVT`` is invoked once per (z, x, y) and
the resulting Mapbox Vector Tile bytes are written into a PMTiles archive
using the ``pmtiles`` Python writer. No external binaries required, so this
runs the same on Windows, macOS, and Linux.

The frontend serves this archive statically via HTTP range requests
(``/tiles/livability.pmtiles``) and renders it on the GPU through MapLibre,
removing PostGIS from the request hot path entirely.
"""

from __future__ import annotations

import gzip
import math
from pathlib import Path

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from sqlalchemy import text

from config import zoom_bounds_for_resolution


# Zoom + bbox window for the bake. The frontend min/max zoom mirrors these.
DEFAULT_MIN_ZOOM = 5
DEFAULT_MAX_ZOOM = 19
# Island of Ireland bounding box (lon_min, lat_min, lon_max, lat_max).
DEFAULT_BBOX = (-11.0, 51.3, -5.3, 55.5)
# Amenity points are only meaningful once you can actually see individual
# features; below this zoom they would be unreadable noise.
AMENITY_MIN_ZOOM = 9
TRANSPORT_REALITY_MIN_ZOOM = 9
GRID_AMENITY_CATEGORIES = ("shops", "transport", "healthcare", "parks")


def _grid_layer_fields() -> dict[str, str]:
    fields = {
        "cell_id": "String",
        "resolution_m": "Number",
        "total_score": "Number",
    }
    for category in GRID_AMENITY_CATEGORIES:
        fields[f"count_{category}"] = "Number"
        fields[f"score_{category}"] = "Number"
    return fields


def _pmtiles_metadata(
    *,
    min_zoom: int,
    max_zoom: int,
    grid_max_zoom: int,
    amenity_min_zoom: int,
    transport_reality_min_zoom: int,
) -> dict[str, object]:
    return {
        "name": "livability",
        "attribution": "© OpenStreetMap contributors",
        "vector_layers": [
            {
                "id": "grid",
                "minzoom": min_zoom,
                "maxzoom": grid_max_zoom,
                "fields": _grid_layer_fields(),
            },
            {
                "id": "amenities",
                "minzoom": amenity_min_zoom,
                "maxzoom": max_zoom,
                "fields": {
                    "category": "String",
                    "source": "String",
                    "source_ref": "String",
                },
            },
            {
                "id": "transport_reality",
                "minzoom": transport_reality_min_zoom,
                "maxzoom": max_zoom,
                "fields": {
                    "source_ref": "String",
                    "stop_name": "String",
                    "feed_id": "String",
                    "stop_id": "String",
                    "reality_status": "String",
                    "source_status": "String",
                    "school_only_state": "String",
                    "public_departures_7d": "Number",
                    "public_departures_30d": "Number",
                    "school_only_departures_30d": "Number",
                },
            },
            {
                "id": "service_deserts",
                "minzoom": min_zoom,
                "maxzoom": grid_max_zoom,
                "fields": {
                    "cell_id": "String",
                    "resolution_m": "Number",
                    "baseline_reachable_stop_count": "Number",
                    "reachable_public_departures_7d": "Number",
                },
            },
        ],
    }


def _resolution_for_zoom(zoom: int) -> int:
    if zoom >= 10:
        return 5000
    if zoom >= 8:
        return 10000
    return 20000


def _lon_to_tile_x(lon: float, zoom: int) -> int:
    return int(math.floor((lon + 180.0) / 360.0 * (1 << zoom)))


def _lat_to_tile_y(lat: float, zoom: int) -> int:
    rad = math.radians(lat)
    return int(
        math.floor(
            (1.0 - math.log(math.tan(rad) + 1.0 / math.cos(rad)) / math.pi)
            / 2.0
            * (1 << zoom)
        )
    )


def _tile_range_for_bbox(
    zoom: int, bbox: tuple[float, float, float, float]
) -> tuple[int, int, int, int]:
    """Return ``(x_min, x_max, y_min, y_max)`` (inclusive) for ``bbox`` at ``zoom``."""
    min_lon, min_lat, max_lon, max_lat = bbox
    max_index = (1 << zoom) - 1
    x_min = max(0, _lon_to_tile_x(min_lon, zoom))
    x_max = min(max_index, _lon_to_tile_x(max_lon, zoom))
    # In slippy-map tile space y=0 is north, so the *max* lat maps to the *min* y.
    y_min = max(0, _lat_to_tile_y(max_lat, zoom))
    y_max = min(max_index, _lat_to_tile_y(min_lat, zoom))
    return x_min, x_max, y_min, y_max


_GRID_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            g.cell_id,
            g.resolution_m,
            g.total_score,
            COALESCE((g.counts_json ->> 'shops')::integer, 0) AS count_shops,
            COALESCE((g.scores_json ->> 'shops')::double precision, 0.0) AS score_shops,
            COALESCE((g.counts_json ->> 'transport')::integer, 0) AS count_transport,
            COALESCE((g.scores_json ->> 'transport')::double precision, 0.0) AS score_transport,
            COALESCE((g.counts_json ->> 'healthcare')::integer, 0) AS count_healthcare,
            COALESCE((g.scores_json ->> 'healthcare')::double precision, 0.0) AS score_healthcare,
            COALESCE((g.counts_json ->> 'parks')::integer, 0) AS count_parks,
            COALESCE((g.scores_json ->> 'parks')::double precision, 0.0) AS score_parks,
            ST_AsMVTGeom(
                ST_Transform(g.cell_geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM grid_walk AS g, tile
        WHERE g.build_key = :build_key
          AND g.resolution_m = :resolution_m
          AND g.cell_geom && tile.env_4326
          AND ST_Intersects(g.cell_geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'grid', 4096, 'geom') FROM mvtgeom
    """
)


_AMENITY_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            a.category,
            a.source,
            a.source_ref,
            ST_AsMVTGeom(
                ST_Transform(a.geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM amenities AS a, tile
        WHERE a.build_key = :build_key
          AND a.geom && tile.env_4326
          AND ST_Intersects(a.geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'amenities', 4096, 'geom') FROM mvtgeom
    """
)


_TRANSPORT_REALITY_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            t.source_ref,
            t.stop_name,
            t.feed_id,
            t.stop_id,
            t.reality_status,
            t.source_status,
            t.school_only_state,
            t.public_departures_7d,
            t.public_departures_30d,
            t.school_only_departures_30d,
            ST_AsMVTGeom(
                ST_Transform(t.geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM transport_reality AS t, tile
        WHERE t.build_key = :build_key
          AND t.geom && tile.env_4326
          AND ST_Intersects(t.geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'transport_reality', 4096, 'geom') FROM mvtgeom
    """
)


_SERVICE_DESERT_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            s.cell_id,
            s.resolution_m,
            s.baseline_reachable_stop_count,
            s.reachable_public_departures_7d,
            ST_AsMVTGeom(
                ST_Transform(s.cell_geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM service_deserts AS s, tile
        WHERE s.build_key = :build_key
          AND s.resolution_m = :resolution_m
          AND s.cell_geom && tile.env_4326
          AND ST_Intersects(s.cell_geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'service_deserts', 4096, 'geom') FROM mvtgeom
    """
)


_AMENITY_POINT_SQL = text(
    """
    SELECT
        ST_X(a.geom) AS lon,
        ST_Y(a.geom) AS lat
    FROM amenities AS a
    WHERE a.build_key = :build_key
    """
)


_TRANSPORT_REALITY_POINT_SQL = text(
    """
    SELECT
        ST_X(t.geom) AS lon,
        ST_Y(t.geom) AS lat
    FROM transport_reality AS t
    WHERE t.build_key = :build_key
    """
)


def _tile_mvt_bytes(
    connection,
    *,
    build_key: str,
    z: int,
    x: int,
    y: int,
    include_grid: bool,
    include_amenities: bool,
    include_transport_reality: bool,
    include_service_deserts: bool,
) -> bytes:
    """Build a single MVT for ``(z, x, y)`` containing both grid and amenities.

    The MVT format is a top-level repeated ``Layer`` message, so two
    independent ``ST_AsMVT`` calls can simply be concatenated to produce a
    valid multi-layer tile without any reparsing.
    """
    grid_bytes = b""
    if include_grid:
        resolution_m = _resolution_for_zoom(z)
        grid_bytes = (
            connection.execute(
                _GRID_TILE_SQL,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "build_key": build_key,
                    "resolution_m": resolution_m,
                },
            ).scalar()
            or b""
        )
    amenity_bytes = b""
    if include_amenities:
        amenity_bytes = (
            connection.execute(
                _AMENITY_TILE_SQL,
                {"z": z, "x": x, "y": y, "build_key": build_key},
            ).scalar()
            or b""
        )
    transport_reality_bytes = b""
    if include_transport_reality:
        transport_reality_bytes = (
            connection.execute(
                _TRANSPORT_REALITY_TILE_SQL,
                {"z": z, "x": x, "y": y, "build_key": build_key},
            ).scalar()
            or b""
        )
    service_desert_bytes = b""
    if include_service_deserts:
        service_desert_bytes = (
            connection.execute(
                _SERVICE_DESERT_TILE_SQL,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "build_key": build_key,
                    "resolution_m": _resolution_for_zoom(z),
                },
            ).scalar()
            or b""
        )
    return (
        bytes(grid_bytes)
        + bytes(amenity_bytes)
        + bytes(transport_reality_bytes)
        + bytes(service_desert_bytes)
    )


def _load_amenity_points(connection, *, build_key: str) -> list[tuple[float, float]]:
    rows = connection.execute(
        _AMENITY_POINT_SQL,
        {"build_key": build_key},
    ).mappings().all()
    return [(float(row["lon"]), float(row["lat"])) for row in rows]


def _load_transport_reality_points(connection, *, build_key: str) -> list[tuple[float, float]]:
    rows = connection.execute(
        _TRANSPORT_REALITY_POINT_SQL,
        {"build_key": build_key},
    ).mappings().all()
    return [(float(row["lon"]), float(row["lat"])) for row in rows]


def _amenity_tile_coordinates(
    points: list[tuple[float, float]],
    *,
    zoom: int,
    bbox: tuple[float, float, float, float],
) -> list[tuple[int, int]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    max_index = (1 << zoom) - 1
    tile_coords: set[tuple[int, int]] = set()
    for lon, lat in points:
        if lon < min_lon or lon > max_lon or lat < min_lat or lat > max_lat:
            continue
        tile_x = min(max(_lon_to_tile_x(lon, zoom), 0), max_index)
        tile_y = min(max(_lat_to_tile_y(lat, zoom), 0), max_index)
        tile_coords.add((tile_x, tile_y))
    return sorted(tile_coords)


def _point_tile_coordinates(
    points: list[tuple[float, float]],
    *,
    zoom: int,
    bbox: tuple[float, float, float, float],
) -> list[tuple[int, int]]:
    return _amenity_tile_coordinates(points, zoom=zoom, bbox=bbox)


def bake_pmtiles(
    engine,
    build_key: str,
    output_path: Path,
    *,
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    min_zoom: int = DEFAULT_MIN_ZOOM,
    max_zoom: int = DEFAULT_MAX_ZOOM,
    amenity_min_zoom: int = AMENITY_MIN_ZOOM,
    transport_reality_min_zoom: int = TRANSPORT_REALITY_MIN_ZOOM,
    **_legacy_kwargs,
) -> Path:
    """Bake the build into a PMTiles archive at ``output_path``.

    Iterates every (z, x, y) tile that intersects ``bbox`` between
    ``min_zoom`` and ``max_zoom`` (inclusive), asks PostGIS for the MVT
    bytes, gzips them, and writes them to a PMTiles file. Empty tiles are
    skipped.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    min_lon, min_lat, max_lon, max_lat = bbox

    tiles_written = 0
    tiles_empty = 0

    with output_path.open("wb") as handle, engine.connect() as connection:
        writer = Writer(handle)
        amenity_points = _load_amenity_points(connection, build_key=build_key)
        transport_reality_points = _load_transport_reality_points(connection, build_key=build_key)
        _, coarse_grid_max_zoom = zoom_bounds_for_resolution(5000)
        for zoom in range(min_zoom, max_zoom + 1):
            include_amenities = zoom >= amenity_min_zoom
            include_transport_reality = zoom >= transport_reality_min_zoom
            include_grid = zoom <= coarse_grid_max_zoom
            include_service_deserts = zoom <= coarse_grid_max_zoom
            zoom_started = tiles_written
            if include_grid or include_service_deserts:
                x_min, x_max, y_min, y_max = _tile_range_for_bbox(zoom, bbox)
                for x in range(x_min, x_max + 1):
                    for y in range(y_min, y_max + 1):
                        payload = _tile_mvt_bytes(
                            connection,
                            build_key=build_key,
                            z=zoom,
                            x=x,
                            y=y,
                            include_grid=include_grid,
                            include_amenities=include_amenities,
                            include_transport_reality=include_transport_reality,
                            include_service_deserts=include_service_deserts,
                        )
                        if not payload:
                            tiles_empty += 1
                            continue
                        writer.write_tile(zxy_to_tileid(zoom, x, y), gzip.compress(payload))
                        tiles_written += 1
                print(
                    f"  z{zoom}: {tiles_written - zoom_started:,} non-empty tiles "
                    f"(bbox scan {x_min}..{x_max} x {y_min}..{y_max})"
                )
                continue

            tile_coords: set[tuple[int, int]] = set()
            if include_amenities:
                tile_coords.update(
                    _point_tile_coordinates(
                        amenity_points,
                        zoom=zoom,
                        bbox=bbox,
                    )
                )
            if include_transport_reality:
                tile_coords.update(
                    _point_tile_coordinates(
                        transport_reality_points,
                        zoom=zoom,
                        bbox=bbox,
                    )
                )
            for x, y in tile_coords:
                payload = _tile_mvt_bytes(
                    connection,
                    build_key=build_key,
                    z=zoom,
                    x=x,
                    y=y,
                    include_grid=False,
                    include_amenities=include_amenities,
                    include_transport_reality=include_transport_reality,
                    include_service_deserts=False,
                )
                if not payload:
                    tiles_empty += 1
                    continue
                writer.write_tile(zxy_to_tileid(zoom, x, y), gzip.compress(payload))
                tiles_written += 1
            print(
                f"  z{zoom}: {tiles_written - zoom_started:,} non-empty tiles "
                f"(amenity tiles={len(tile_coords):,})"
            )

        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "min_lon_e7": int(min_lon * 1e7),
            "min_lat_e7": int(min_lat * 1e7),
            "max_lon_e7": int(max_lon * 1e7),
            "max_lat_e7": int(max_lat * 1e7),
            "center_zoom": min_zoom,
            "center_lon_e7": int(center_lon * 1e7),
            "center_lat_e7": int(center_lat * 1e7),
        }
        metadata = _pmtiles_metadata(
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            grid_max_zoom=coarse_grid_max_zoom,
            amenity_min_zoom=amenity_min_zoom,
            transport_reality_min_zoom=transport_reality_min_zoom,
        )
        writer.finalize(header, metadata)

    print(
        f"PMTiles baked: {tiles_written:,} non-empty, {tiles_empty:,} empty -> {output_path}"
    )
    return output_path


__all__ = ["bake_pmtiles", "DEFAULT_BBOX", "DEFAULT_MIN_ZOOM", "DEFAULT_MAX_ZOOM"]
