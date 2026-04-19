"""Bake the precomputed grid + amenities into a single PMTiles archive.

Pure-Python pipeline: PostGIS ``ST_AsMVT`` is invoked once per (z, x, y) and
the resulting Mapbox Vector Tile bytes are written into a PMTiles archive
using the ``pmtiles`` Python writer. No external binaries required, so this
runs the same on Windows, macOS, and Linux.

The frontend serves this archive statically via HTTP range requests
(``/tiles/livability.pmtiles``) and renders it on the GPU through MapLibre,
removing PostGIS from the request hot path entirely.

Parallelism: when ``workers > 1`` the per-tile work (PostGIS round trips +
``gzip.compress``) is dispatched to a ``ProcessPoolExecutor``. Each worker
process opens its own SQLAlchemy engine on first use and reuses it across
chunks. The PMTiles writer accepts out-of-order writes (it sorts entries at
``finalize()``) so the main thread simply writes results as they arrive.
"""

from __future__ import annotations

import gzip
import math
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable, Iterator

from pmtiles.tile import Compression, TileType, tileid_to_zxy, zxy_to_tileid
from pmtiles.writer import Writer
from sqlalchemy import text

from config import BAKE_PMTILES_WORKERS, database_url, zoom_bounds_for_resolution

# The worker lives in a top-level module (not under ``precompute``) so that
# ``ProcessPoolExecutor`` spawn subprocesses on Windows only re-import the
# minimal dependency graph for it. Importing ``precompute._bake_worker``
# would trigger ``precompute/__init__.py`` in each subprocess, which pulls
# scipy/sklearn/geopandas/pandas/numpy and exhausts the Windows paging file.
from pmtiles_bake_worker import (  # noqa: E402  (intentional top-level worker)
    _AMENITY_TILE_SQL,
    _GRID_TILE_SQL,
    _LAYER_AMENITIES,
    _LAYER_GRID,
    _LAYER_SERVICE_DESERTS,
    _LAYER_TRANSPORT_REALITY,
    _SERVICE_DESERT_TILE_SQL,
    _TRANSPORT_REALITY_TILE_SQL,
    _bake_chunk_worker,
    _resolution_for_zoom,
    _tile_mvt_bytes_by_flags,
)


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

# Chunk size: how many tile specs go into one worker task. Larger chunks
# amortize IPC overhead; smaller chunks improve load balancing across zooms.
# 512 is a happy medium for the Ireland workload (~400 chunks total).
_CHUNK_SIZE = 512


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
                    "tier": "String",
                    "name": "String",
                    "source": "String",
                    "source_ref": "String",
                    "conflict_class": "String",
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


def _iter_tile_specs(
    *,
    min_zoom: int,
    max_zoom: int,
    bbox: tuple[float, float, float, float],
    amenity_points: list[tuple[float, float]],
    transport_reality_points: list[tuple[float, float]],
    coarse_grid_max_zoom: int,
    amenity_min_zoom: int,
    transport_reality_min_zoom: int,
) -> Iterator[tuple[int, int, int, int]]:
    """Yield ``(z, x, y, layer_bitmask)`` tuples for every tile that will be baked.

    Iteration order matches the original sequential loop (low-zoom bbox scan
    first, then high-zoom point-derived tiles) so the PMTiles writer still
    sees an approximately-sorted stream when workers=1.
    """
    for zoom in range(min_zoom, max_zoom + 1):
        include_amenities = zoom >= amenity_min_zoom
        include_transport_reality = zoom >= transport_reality_min_zoom
        include_grid = zoom <= coarse_grid_max_zoom
        include_service_deserts = zoom <= coarse_grid_max_zoom

        layers = 0
        if include_grid:
            layers |= _LAYER_GRID
        if include_amenities:
            layers |= _LAYER_AMENITIES
        if include_transport_reality:
            layers |= _LAYER_TRANSPORT_REALITY
        if include_service_deserts:
            layers |= _LAYER_SERVICE_DESERTS
        if layers == 0:
            continue

        if include_grid or include_service_deserts:
            x_min, x_max, y_min, y_max = _tile_range_for_bbox(zoom, bbox)
            for x in range(x_min, x_max + 1):
                for y in range(y_min, y_max + 1):
                    yield zoom, x, y, layers
            continue

        tile_coords: set[tuple[int, int]] = set()
        if include_amenities:
            tile_coords.update(
                _point_tile_coordinates(amenity_points, zoom=zoom, bbox=bbox)
            )
        if include_transport_reality:
            tile_coords.update(
                _point_tile_coordinates(transport_reality_points, zoom=zoom, bbox=bbox)
            )
        for x, y in sorted(tile_coords):
            yield zoom, x, y, layers


def _chunked(iterable: Iterable, size: int) -> Iterator[list]:
    chunk: list = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _bake_sequential(
    connection,
    *,
    writer: Writer,
    build_key: str,
    tile_specs: Iterator[tuple[int, int, int, int]],
) -> tuple[int, int, dict[int, int]]:
    tiles_written = 0
    tiles_empty = 0
    per_zoom: dict[int, int] = {}
    for z, x, y, layers in tile_specs:
        payload = _tile_mvt_bytes_by_flags(
            connection,
            build_key=build_key,
            z=z,
            x=x,
            y=y,
            layers=layers,
        )
        if not payload:
            tiles_empty += 1
            continue
        writer.write_tile(zxy_to_tileid(z, x, y), gzip.compress(payload))
        tiles_written += 1
        per_zoom[z] = per_zoom.get(z, 0) + 1
    return tiles_written, tiles_empty, per_zoom


def _bake_parallel(
    *,
    writer: Writer,
    build_key: str,
    db_url: str,
    tile_specs: Iterable[tuple[int, int, int, int]],
    workers: int,
    total_tile_count: int,
) -> tuple[int, int, dict[int, int]]:
    chunks = list(_chunked(tile_specs, _CHUNK_SIZE))
    tiles_written = 0
    per_zoom: dict[int, int] = {}
    completed_specs = 0

    # Report progress roughly every 10% of tiles (but at least every chunk).
    report_every = max(1, total_tile_count // 10)
    next_report = report_every

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_bake_chunk_worker, chunk, build_key, db_url)
            for chunk in chunks
        ]
        for chunk_index, future in enumerate(futures):
            results = future.result()
            for tileid, blob in results:
                writer.write_tile(tileid, blob)
                z = tileid_to_zxy(tileid)[0]
                per_zoom[z] = per_zoom.get(z, 0) + 1
                tiles_written += 1
            completed_specs += len(chunks[chunk_index])
            if completed_specs >= next_report and total_tile_count > 0:
                print(
                    f"  bake progress: {completed_specs:,}/{total_tile_count:,} "
                    f"specs processed ({tiles_written:,} non-empty so far)"
                )
                next_report = completed_specs + report_every

    tiles_empty = max(0, completed_specs - tiles_written)
    return tiles_written, tiles_empty, per_zoom


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
    workers: int | None = None,
    **_legacy_kwargs,
) -> Path:
    """Bake the build into a PMTiles archive at ``output_path``.

    Iterates every (z, x, y) tile that intersects ``bbox`` between
    ``min_zoom`` and ``max_zoom`` (inclusive), asks PostGIS for the MVT
    bytes, gzips them, and writes them to a PMTiles file. Empty tiles are
    skipped. When ``workers > 1`` the per-tile work is dispatched to a
    ``ProcessPoolExecutor``; each worker opens its own engine on first use.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    min_lon, min_lat, max_lon, max_lat = bbox

    resolved_workers = int(workers if workers is not None else BAKE_PMTILES_WORKERS)
    if resolved_workers < 1:
        resolved_workers = 1

    with output_path.open("wb") as handle, engine.connect() as connection:
        writer = Writer(handle)
        amenity_points = _load_amenity_points(connection, build_key=build_key)
        transport_reality_points = _load_transport_reality_points(
            connection, build_key=build_key
        )
        _, coarse_grid_max_zoom = zoom_bounds_for_resolution(5000)

        tile_specs_list = list(
            _iter_tile_specs(
                min_zoom=min_zoom,
                max_zoom=max_zoom,
                bbox=bbox,
                amenity_points=amenity_points,
                transport_reality_points=transport_reality_points,
                coarse_grid_max_zoom=coarse_grid_max_zoom,
                amenity_min_zoom=amenity_min_zoom,
                transport_reality_min_zoom=transport_reality_min_zoom,
            )
        )
        total_specs = len(tile_specs_list)

        if resolved_workers <= 1 or total_specs == 0:
            print(f"  bake_pmtiles: sequential ({total_specs:,} tile specs)")
            tiles_written, tiles_empty, per_zoom = _bake_sequential(
                connection,
                writer=writer,
                build_key=build_key,
                tile_specs=iter(tile_specs_list),
            )
        else:
            print(
                f"  bake_pmtiles: parallel, {resolved_workers} workers "
                f"({total_specs:,} tile specs)"
            )
            # Release the main connection while workers run — their own
            # engines connect independently, and holding an idle connection
            # just wastes a slot.
            connection.close()
            tiles_written, tiles_empty, per_zoom = _bake_parallel(
                writer=writer,
                build_key=build_key,
                db_url=database_url(),
                tile_specs=tile_specs_list,
                workers=resolved_workers,
                total_tile_count=total_specs,
            )

        for zoom in sorted(per_zoom):
            print(f"  z{zoom}: {per_zoom[zoom]:,} non-empty tiles")

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
