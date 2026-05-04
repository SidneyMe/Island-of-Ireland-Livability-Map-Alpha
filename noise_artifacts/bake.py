"""Bake the published noise overlay into a standalone PMTiles archive."""

from __future__ import annotations

import gzip
import math
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Iterable, Iterator

from pmtiles.tile import Compression, TileType, tileid_to_zxy, zxy_to_tileid
from pmtiles.writer import Writer
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import BAKE_PMTILES_WORKERS, database_url
from pmtiles_bake_worker import _bake_noise_chunk_worker, _noise_tile_mvt_bytes


DEFAULT_BBOX = (-11.0, 51.3, -5.3, 55.5)
NOISE_MIN_ZOOM = 8
NOISE_SOURCE_MAX_ZOOM = 15
_CHUNK_SIZE = 512
_IN_FLIGHT_MULTIPLIER = 2


_NOISE_BOUNDS_SQL = text(
    """
    SELECT
        ST_XMin(n.geom) AS min_lon,
        ST_YMin(n.geom) AS min_lat,
        ST_XMax(n.geom) AS max_lon,
        ST_YMax(n.geom) AS max_lat
    FROM noise_polygons AS n
    WHERE n.build_key = :build_key
    """
)


def _metadata(*, min_zoom: int, max_zoom: int) -> dict[str, object]:
    return {
        "name": "noise",
        "attribution": "© OpenStreetMap contributors",
        "vector_layers": [
            {
                "id": "noise",
                "minzoom": min_zoom,
                "maxzoom": max_zoom,
                "fields": {
                    "jurisdiction": "String",
                    "source_type": "String",
                    "metric": "String",
                    "round": "Number",
                    "report_period": "String",
                    "db_low": "Number",
                    "db_high": "Number",
                    "db_value": "String",
                    "source_dataset": "String",
                    "source_layer": "String",
                    "source_ref": "String",
                },
            }
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


def _bounds_tile_coordinates(
    bounds: list[tuple[float, float, float, float]],
    *,
    zoom: int,
    bbox: tuple[float, float, float, float],
) -> list[tuple[int, int]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    max_index = (1 << zoom) - 1
    tile_coords: set[tuple[int, int]] = set()
    for left, bottom, right, top in bounds:
        clipped_left = max(float(left), min_lon)
        clipped_right = min(float(right), max_lon)
        clipped_bottom = max(float(bottom), min_lat)
        clipped_top = min(float(top), max_lat)
        if clipped_left > clipped_right or clipped_bottom > clipped_top:
            continue
        x_min = min(max(_lon_to_tile_x(clipped_left, zoom), 0), max_index)
        x_max = min(max(_lon_to_tile_x(clipped_right, zoom), 0), max_index)
        y_min = min(max(_lat_to_tile_y(clipped_top, zoom), 0), max_index)
        y_max = min(max(_lat_to_tile_y(clipped_bottom, zoom), 0), max_index)
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                tile_coords.add((x, y))
    return sorted(tile_coords)


def _load_noise_bounds(
    connection,
    *,
    build_key: str,
) -> list[tuple[float, float, float, float]]:
    rows = connection.execute(
        _NOISE_BOUNDS_SQL,
        {"build_key": build_key},
    ).mappings().all()
    return [
        (
            float(row["min_lon"]),
            float(row["min_lat"]),
            float(row["max_lon"]),
            float(row["max_lat"]),
        )
        for row in rows
        if row["min_lon"] is not None
    ]


def _iter_tile_specs(
    *,
    bounds: list[tuple[float, float, float, float]],
    bbox: tuple[float, float, float, float],
    min_zoom: int,
    max_zoom: int,
) -> Iterator[tuple[int, int, int]]:
    for zoom in range(min_zoom, max_zoom + 1):
        for x, y in _bounds_tile_coordinates(bounds, zoom=zoom, bbox=bbox):
            yield zoom, x, y


def _chunked(iterable: Iterable, size: int) -> Iterator[list]:
    chunk: list = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _temp_output_path(output_path: Path) -> Path:
    return output_path.with_name(output_path.name + ".tmp")


def _parallel_in_flight_limit(workers: int) -> int:
    return max(1, int(workers) * _IN_FLIGHT_MULTIPLIER)


def _bake_sequential(
    connection,
    *,
    writer: Writer,
    build_key: str,
    tile_specs: Iterable[tuple[int, int, int]],
) -> tuple[int, int, dict[int, int]]:
    tiles_written = 0
    tiles_empty = 0
    per_zoom: dict[int, int] = {}
    for z, x, y in tile_specs:
        payload = _noise_tile_mvt_bytes(
            connection,
            build_key=build_key,
            z=z,
            x=x,
            y=y,
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
    tile_specs: Iterable[tuple[int, int, int]],
    workers: int,
    total_tile_count: int,
) -> tuple[int, int, dict[int, int]]:
    tiles_written = 0
    completed_specs = 0
    per_zoom: dict[int, int] = {}
    in_flight_limit = _parallel_in_flight_limit(workers)
    chunk_iter = iter(_chunked(tile_specs, _CHUNK_SIZE))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending: dict[object, int] = {}

        def _submit_next_chunk() -> bool:
            chunk = next(chunk_iter, None)
            if chunk is None:
                return False
            future = pool.submit(_bake_noise_chunk_worker, chunk, build_key, db_url)
            pending[future] = len(chunk)
            return True

        while len(pending) < in_flight_limit and _submit_next_chunk():
            pass
        while pending:
            done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
            for future in done:
                chunk_size = pending.pop(future)
                for tileid, blob in future.result():
                    writer.write_tile(tileid, blob)
                    z = tileid_to_zxy(tileid)[0]
                    per_zoom[z] = per_zoom.get(z, 0) + 1
                    tiles_written += 1
                completed_specs += chunk_size
            while len(pending) < in_flight_limit and _submit_next_chunk():
                pass
    tiles_empty = max(0, total_tile_count - tiles_written)
    return tiles_written, tiles_empty, per_zoom


def bake_noise_pmtiles(
    engine: Engine,
    build_key: str,
    output_path: Path,
    *,
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    noise_min_zoom: int = NOISE_MIN_ZOOM,
    noise_max_zoom: int = 13,
    workers: int | None = None,
) -> Path | None:
    """Bake only ``noise_polygons`` for ``build_key`` into ``output_path``."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = _temp_output_path(output_path)
    source_max_zoom = min(int(noise_max_zoom), NOISE_SOURCE_MAX_ZOOM)
    min_zoom = min(int(noise_min_zoom), source_max_zoom)
    configured_workers = int(workers if workers is not None else BAKE_PMTILES_WORKERS)
    if configured_workers < 1:
        configured_workers = 1

    with engine.connect() as connection:
        bounds = _load_noise_bounds(connection, build_key=build_key)
    if not bounds:
        if temp_output_path.exists():
            temp_output_path.unlink()
        if output_path.exists():
            output_path.unlink()
        print(f"Noise PMTiles skipped: no noise rows for build_key={build_key}")
        return None

    tile_specs = list(
        _iter_tile_specs(
            bounds=bounds,
            bbox=bbox,
            min_zoom=min_zoom,
            max_zoom=source_max_zoom,
        )
    )
    min_lon, min_lat, max_lon, max_lat = bbox
    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    header = {
        "tile_type": TileType.MVT,
        "tile_compression": Compression.GZIP,
        "min_zoom": min_zoom,
        "max_zoom": source_max_zoom,
        "min_lon_e7": int(min_lon * 1e7),
        "min_lat_e7": int(min_lat * 1e7),
        "max_lon_e7": int(max_lon * 1e7),
        "max_lat_e7": int(max_lat * 1e7),
        "center_zoom": min_zoom,
        "center_lon_e7": int(center_lon * 1e7),
        "center_lat_e7": int(center_lat * 1e7),
    }

    if temp_output_path.exists():
        temp_output_path.unlink()
    try:
        with temp_output_path.open("wb") as handle:
            writer = Writer(handle)
            if configured_workers <= 1 or not tile_specs:
                print(f"  bake_noise_pmtiles: sequential ({len(tile_specs):,} tile specs)")
                with engine.connect() as connection:
                    tiles_written, tiles_empty, per_zoom = _bake_sequential(
                        connection,
                        writer=writer,
                        build_key=build_key,
                        tile_specs=tile_specs,
                    )
            else:
                print(
                    "  bake_noise_pmtiles: parallel, "
                    f"{configured_workers} workers ({len(tile_specs):,} tile specs)"
                )
                tiles_written, tiles_empty, per_zoom = _bake_parallel(
                    writer=writer,
                    build_key=build_key,
                    db_url=database_url(),
                    tile_specs=tile_specs,
                    workers=configured_workers,
                    total_tile_count=len(tile_specs),
                )
            for zoom in sorted(per_zoom):
                print(f"  noise z{zoom}: {per_zoom[zoom]:,} non-empty tiles")
            writer.finalize(header, _metadata(min_zoom=min_zoom, max_zoom=source_max_zoom))
        temp_output_path.replace(output_path)
        print(
            f"Noise PMTiles baked: {tiles_written:,} non-empty, "
            f"{tiles_empty:,} empty -> {output_path}"
        )
        return output_path
    except Exception:
        if temp_output_path.exists():
            temp_output_path.unlink()
        raise


def bake_noise_artifact_pmtiles(
    engine: Engine,
    noise_tile_hash: str,
    *,
    output_dir: Path,
    noise_max_zoom: int = 13,
) -> Path | None:
    """Compatibility wrapper for older callers that named the artifact hash."""
    output_path = output_dir / f"noise_{noise_tile_hash}.pmtiles"
    return bake_noise_pmtiles(
        engine,
        noise_tile_hash,
        output_path,
        noise_max_zoom=noise_max_zoom,
    )


__all__ = ["bake_noise_pmtiles", "bake_noise_artifact_pmtiles"]
