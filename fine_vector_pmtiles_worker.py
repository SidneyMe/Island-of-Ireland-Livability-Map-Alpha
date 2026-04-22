from __future__ import annotations

import gzip
import json
import math
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from pmtiles.tile import zxy_to_tileid
from sqlalchemy import create_engine

from config import (
    CANONICAL_BASE_RESOLUTION_M,
    FINE_RESOLUTIONS_M,
    SURFACE_SHARD_SIZE_M,
    TO_TARGET,
    TO_WGS84,
)
from mapbox_vector_tile import encode as encode_vector_tile
from pmtiles_bake_worker import (
    _LAYER_FINE_GRID,
    _tile_mvt_bytes_by_flags as _coarse_tile_mvt_bytes_by_flags,
)


_TILE_EXTENT = 4096
_TILE_BUFFER = 256
_FINE_GRID_RESOLUTIONS_BY_ZOOM: dict[int, tuple[int, ...]] = {
    12: (2500,),
    13: (1000,),
    14: (500,),
    15: (250, 100, 50),
}
_GRID_AMENITY_CATEGORIES = ("shops", "transport", "healthcare", "parks")
_RAW_SHELL_CACHE_LIMIT = 8
_RAW_SCORE_CACHE_LIMIT = 8
_AGGREGATED_CACHE_LIMIT = 16

_WORKER_ENGINE = None  # type: ignore[var-annotated]
_WORKER_DB_URL: str | None = None
_FINE_GRID_CONTEXTS: dict[tuple[str, str], "FineGridTileContext"] = {}


def _aligned_floor(value: float, step: int) -> int:
    return int(math.floor(float(value) / float(step)) * int(step))


def _aggregation_factor(resolution_m: int) -> int:
    normalized = int(resolution_m)
    if normalized == CANONICAL_BASE_RESOLUTION_M:
        return 1
    if normalized not in FINE_RESOLUTIONS_M:
        raise ValueError(f"Unsupported fine grid resolution: {resolution_m}")
    factor = normalized // CANONICAL_BASE_RESOLUTION_M
    if factor * CANONICAL_BASE_RESOLUTION_M != normalized:
        raise ValueError(f"Resolution {resolution_m} is not an exact multiple of 50m")
    return factor


def _weighted_mean_block_reduce(
    values: np.ndarray,
    weights: np.ndarray,
    factor: int,
) -> tuple[np.ndarray, np.ndarray]:
    if factor <= 0:
        raise ValueError("Aggregation factor must be positive")
    if factor == 1:
        valid_mask = np.asarray(weights, dtype=np.float32) > 0.0
        return (
            np.where(valid_mask, np.asarray(values, dtype=np.float32), np.nan).astype(np.float32),
            valid_mask,
        )

    values_f = np.asarray(values, dtype=np.float32)
    weights_f = np.asarray(weights, dtype=np.float32)
    rows, cols = values_f.shape
    if rows % factor != 0 or cols % factor != 0:
        raise ValueError("Surface dimensions must be exactly divisible by the aggregation factor")

    reshaped_values = np.nan_to_num(values_f, nan=0.0).reshape(rows // factor, factor, cols // factor, factor)
    reshaped_weights = weights_f.reshape(rows // factor, factor, cols // factor, factor)
    weight_sums = reshaped_weights.sum(axis=(1, 3), dtype=np.float32)
    weighted_sums = (reshaped_values * reshaped_weights).sum(axis=(1, 3), dtype=np.float32)
    aggregated = np.full(weight_sums.shape, np.nan, dtype=np.float32)
    valid_mask = weight_sums > 0.0
    aggregated[valid_mask] = weighted_sums[valid_mask] / weight_sums[valid_mask]
    return aggregated, valid_mask


def _load_manifest(surface_dir: Path) -> dict[str, Any]:
    with surface_dir.joinpath("manifest.json").open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid surface manifest at {surface_dir}")
    return payload


def _metric_tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    tile_count = 1 << int(z)
    lon_left = (float(x) / tile_count) * 360.0 - 180.0
    lon_right = (float(x + 1) / tile_count) * 360.0 - 180.0

    def _tile_y_to_lat(tile_y: float) -> float:
        mercator = math.pi * (1.0 - (2.0 * tile_y) / tile_count)
        return math.degrees(math.atan(math.sinh(mercator)))

    lat_top = _tile_y_to_lat(float(y))
    lat_bottom = _tile_y_to_lat(float(y + 1))
    x_min_m, y_min_m = TO_TARGET(lon_left, lat_bottom)
    x_max_m, y_max_m = TO_TARGET(lon_right, lat_top)
    return (
        min(float(x_min_m), float(x_max_m)),
        min(float(y_min_m), float(y_max_m)),
        max(float(x_min_m), float(x_max_m)),
        max(float(y_min_m), float(y_max_m)),
    )


def _lonlat_to_tile_coord(lon: float, lat: float, z: int, x: int, y: int) -> tuple[float, float]:
    safe_lat = max(min(float(lat), 85.05112878), -85.05112878)
    tile_count = float(1 << int(z))
    world_x = ((float(lon) + 180.0) / 360.0) * tile_count
    mercator = math.log(math.tan(math.radians(safe_lat)) + (1.0 / math.cos(math.radians(safe_lat))))
    world_y = (1.0 - (mercator / math.pi)) * tile_count / 2.0
    return (
        (world_x - float(x)) * _TILE_EXTENT,
        (world_y - float(y)) * _TILE_EXTENT,
    )


def _clip_polygon_axis(
    points: list[tuple[float, float]],
    *,
    inside,
    intersect,
) -> list[tuple[float, float]]:
    if not points:
        return []
    output: list[tuple[float, float]] = []
    previous = points[-1]
    previous_inside = inside(previous)
    for current in points:
        current_inside = inside(current)
        if current_inside:
            if not previous_inside:
                output.append(intersect(previous, current))
            output.append(current)
        elif previous_inside:
            output.append(intersect(previous, current))
        previous = current
        previous_inside = current_inside
    return output


def _clip_polygon_to_extent(
    points: list[tuple[float, float]],
    *,
    min_extent: float = float(-_TILE_BUFFER),
    max_extent: float = float(_TILE_EXTENT + _TILE_BUFFER),
) -> list[tuple[float, float]]:
    def _ring_area_twice(ring: list[tuple[float, float]]) -> float:
        area_twice = 0.0
        for index in range(len(ring) - 1):
            x0, y0 = ring[index]
            x1, y1 = ring[index + 1]
            area_twice += (x0 * y1) - (x1 * y0)
        return area_twice

    def _interpolate(
        start: tuple[float, float],
        end: tuple[float, float],
        *,
        boundary: float,
        axis: str,
    ) -> tuple[float, float]:
        start_x, start_y = start
        end_x, end_y = end
        if axis == "x":
            delta = end_x - start_x
            if math.isclose(delta, 0.0):
                return (boundary, start_y)
            ratio = (boundary - start_x) / delta
            return (boundary, start_y + ((end_y - start_y) * ratio))
        delta = end_y - start_y
        if math.isclose(delta, 0.0):
            return (start_x, boundary)
        ratio = (boundary - start_y) / delta
        return (start_x + ((end_x - start_x) * ratio), boundary)

    clipped = list(points)
    clipped = _clip_polygon_axis(
        clipped,
        inside=lambda point: point[0] >= min_extent,
        intersect=lambda start, end: _interpolate(start, end, boundary=min_extent, axis="x"),
    )
    clipped = _clip_polygon_axis(
        clipped,
        inside=lambda point: point[0] <= max_extent,
        intersect=lambda start, end: _interpolate(start, end, boundary=max_extent, axis="x"),
    )
    clipped = _clip_polygon_axis(
        clipped,
        inside=lambda point: point[1] >= min_extent,
        intersect=lambda start, end: _interpolate(start, end, boundary=min_extent, axis="y"),
    )
    clipped = _clip_polygon_axis(
        clipped,
        inside=lambda point: point[1] <= max_extent,
        intersect=lambda start, end: _interpolate(start, end, boundary=max_extent, axis="y"),
    )
    if len(clipped) < 3:
        return []
    normalized = [(round(point[0], 3), round(point[1], 3)) for point in clipped]
    deduped: list[tuple[float, float]] = []
    for point in normalized:
        if deduped and point == deduped[-1]:
            continue
        deduped.append(point)
    if len(deduped) < 3:
        return []
    if deduped[0] != deduped[-1]:
        deduped.append(deduped[0])
    if len(deduped) < 4:
        return []
    unique_vertices = set(deduped[:-1])
    if len(unique_vertices) < 3:
        return []
    if math.isclose(_ring_area_twice(deduped), 0.0, abs_tol=1e-6):
        return []
    return deduped


def _metric_cell_polygon_to_tile_ring(
    *,
    x_min_m: float,
    y_min_m: float,
    x_max_m: float,
    y_max_m: float,
    z: int,
    x: int,
    y: int,
) -> list[tuple[float, float]]:
    xs = np.asarray([x_min_m, x_max_m, x_max_m, x_min_m], dtype=np.float64)
    ys = np.asarray([y_max_m, y_max_m, y_min_m, y_min_m], dtype=np.float64)
    lons, lats = TO_WGS84(xs, ys)
    projected = [
        _lonlat_to_tile_coord(float(lon), float(lat), z, x, y)
        for lon, lat in zip(np.asarray(lons, dtype=np.float64), np.asarray(lats, dtype=np.float64))
    ]
    return _clip_polygon_to_extent(projected)


def _cell_id(resolution_m: int, raw_minx: float, raw_miny: float) -> str:
    resolution_value = int(round(float(resolution_m)))
    raw_minx_mm = int(round(float(raw_minx) * 1000.0))
    raw_miny_mm = int(round(float(raw_miny) * 1000.0))
    return f"{resolution_value}:{raw_minx_mm}:{raw_miny_mm}"


def _zero_score_properties() -> dict[str, float | int]:
    payload: dict[str, float | int] = {}
    for category in _GRID_AMENITY_CATEGORIES:
        payload[f"count_{category}"] = 0
        payload[f"cluster_{category}"] = 0
        payload[f"effective_units_{category}"] = 0.0
        payload[f"score_{category}"] = 0.0
    return payload


@dataclass(frozen=True)
class FineGridShardEntry:
    shard_id: str
    x_min_m: int
    y_min_m: int
    x_max_m: int
    y_max_m: int
    shell_path: str
    score_path: str


class FineGridTileContext:
    def __init__(self, *, shell_dir: Path, score_dir: Path) -> None:
        self.shell_dir = Path(shell_dir)
        self.score_dir = Path(score_dir)
        shell_manifest = _load_manifest(self.shell_dir)
        score_manifest = _load_manifest(self.score_dir)
        shell_inventory = {
            str(entry["shard_id"]): entry
            for entry in shell_manifest.get("shard_inventory", [])
            if isinstance(entry, dict) and "shard_id" in entry
        }
        score_inventory = {
            str(entry["shard_id"]): entry
            for entry in score_manifest.get("shard_inventory", [])
            if isinstance(entry, dict) and "shard_id" in entry
        }
        self.shard_entries: dict[str, FineGridShardEntry] = {}
        for shard_id, shell_entry in shell_inventory.items():
            score_entry = score_inventory.get(shard_id)
            if score_entry is None:
                continue
            self.shard_entries[shard_id] = FineGridShardEntry(
                shard_id=shard_id,
                x_min_m=int(shell_entry["x_min_m"]),
                y_min_m=int(shell_entry["y_min_m"]),
                x_max_m=int(shell_entry["x_max_m"]),
                y_max_m=int(shell_entry["y_max_m"]),
                shell_path=str(shell_entry["path"]),
                score_path=str(score_entry["path"]),
            )
        self.shard_size_m = int(shell_manifest.get("shard_size_m", SURFACE_SHARD_SIZE_M))
        self.base_resolution_m = int(shell_manifest.get("base_resolution_m", CANONICAL_BASE_RESOLUTION_M))
        self._shell_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._score_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._aggregated_cache: OrderedDict[tuple[str, int], tuple[np.ndarray, np.ndarray]] = OrderedDict()
        self._wgs84_bbox_cache: dict[str, tuple[float, float, float, float]] = {}

    @property
    def shard_count(self) -> int:
        return len(self.shard_entries)

    def _cache_get(self, cache: OrderedDict, key):
        value = cache.get(key)
        if value is None:
            return None
        cache.move_to_end(key)
        return value

    def _cache_put(self, cache: OrderedDict, key, value, *, limit: int) -> Any:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > int(limit):
            cache.popitem(last=False)
        return value

    def reset_large_caches(self) -> None:
        self._shell_cache.clear()
        self._score_cache.clear()
        self._aggregated_cache.clear()

    def cache_sizes(self) -> dict[str, int]:
        return {
            "shell": len(self._shell_cache),
            "score": len(self._score_cache),
            "aggregated": len(self._aggregated_cache),
            "wgs84_bbox": len(self._wgs84_bbox_cache),
        }

    def _load_shell_shard(self, shard_id: str) -> dict[str, Any]:
        cached = self._cache_get(self._shell_cache, shard_id)
        if cached is None:
            entry = self.shard_entries[shard_id]
            with np.load(self.shell_dir / entry.shell_path, allow_pickle=False) as data:
                cached = self._cache_put(
                    self._shell_cache,
                    shard_id,
                    {
                        "effective_area_ratio": np.asarray(data["effective_area_ratio"], dtype=np.float32),
                    },
                    limit=_RAW_SHELL_CACHE_LIMIT,
                )
        return cached

    def _load_score_shard(self, shard_id: str) -> dict[str, Any]:
        cached = self._cache_get(self._score_cache, shard_id)
        if cached is None:
            entry = self.shard_entries[shard_id]
            with np.load(self.score_dir / entry.score_path, allow_pickle=False) as data:
                cached = self._cache_put(
                    self._score_cache,
                    shard_id,
                    {
                        "total_score_50": np.asarray(data["total_score_50"], dtype=np.float32),
                    },
                    limit=_RAW_SCORE_CACHE_LIMIT,
                )
        return cached

    def aggregated_shard_surface(self, shard_id: str, resolution_m: int) -> tuple[np.ndarray, np.ndarray]:
        cache_key = (shard_id, int(resolution_m))
        cached = self._cache_get(self._aggregated_cache, cache_key)
        if cached is not None:
            return cached

        canonical_scores = np.asarray(
            self._load_score_shard(shard_id)["total_score_50"],
            dtype=np.float32,
        )
        canonical_weights = np.asarray(
            self._load_shell_shard(shard_id)["effective_area_ratio"],
            dtype=np.float32,
        )
        aggregated_scores, valid_mask = _weighted_mean_block_reduce(
            canonical_scores,
            canonical_weights,
            _aggregation_factor(int(resolution_m)),
        )
        return self._cache_put(
            self._aggregated_cache,
            cache_key,
            (aggregated_scores, valid_mask),
            limit=_AGGREGATED_CACHE_LIMIT,
        )

    def iter_intersecting_shards(
        self,
        metric_bounds: tuple[float, float, float, float],
    ) -> Iterable[FineGridShardEntry]:
        shard_min_x = _aligned_floor(metric_bounds[0], self.shard_size_m)
        shard_min_y = _aligned_floor(metric_bounds[1], self.shard_size_m)
        shard_max_x = _aligned_floor(metric_bounds[2], self.shard_size_m)
        shard_max_y = _aligned_floor(metric_bounds[3], self.shard_size_m)
        for shard_y in range(shard_min_y, shard_max_y + self.shard_size_m, self.shard_size_m):
            for shard_x in range(shard_min_x, shard_max_x + self.shard_size_m, self.shard_size_m):
                shard_id = f"{int(shard_x)}_{int(shard_y)}"
                entry = self.shard_entries.get(shard_id)
                if entry is not None:
                    yield entry

    def shard_wgs84_bbox(self, shard_id: str) -> tuple[float, float, float, float]:
        if shard_id in self._wgs84_bbox_cache:
            return self._wgs84_bbox_cache[shard_id]
        entry = self.shard_entries[shard_id]
        xs = np.asarray(
            [entry.x_min_m, entry.x_max_m, entry.x_max_m, entry.x_min_m],
            dtype=np.float64,
        )
        ys = np.asarray(
            [entry.y_min_m, entry.y_min_m, entry.y_max_m, entry.y_max_m],
            dtype=np.float64,
        )
        lons, lats = TO_WGS84(xs, ys)
        bbox = (
            float(np.min(lons)),
            float(np.min(lats)),
            float(np.max(lons)),
            float(np.max(lats)),
        )
        self._wgs84_bbox_cache[shard_id] = bbox
        return bbox

    def build_grid_layer(self, *, z: int, x: int, y: int) -> dict[str, Any] | None:
        resolutions = _FINE_GRID_RESOLUTIONS_BY_ZOOM.get(int(z), ())
        if not resolutions:
            return None

        metric_bounds = _metric_tile_bounds(int(z), int(x), int(y))
        features: list[dict[str, Any]] = []
        zero_properties = _zero_score_properties()
        for resolution_m in resolutions:
            block_size = int(resolution_m)
            expanded_metric_bounds = (
                metric_bounds[0] - block_size,
                metric_bounds[1] - block_size,
                metric_bounds[2] + block_size,
                metric_bounds[3] + block_size,
            )
            for shard_entry in self.iter_intersecting_shards(expanded_metric_bounds):
                aggregated_scores, valid_mask = self.aggregated_shard_surface(shard_entry.shard_id, block_size)
                rows, cols = aggregated_scores.shape
                start_col = max(
                    0,
                    int(math.floor((expanded_metric_bounds[0] - shard_entry.x_min_m) / float(block_size))),
                )
                end_col = min(
                    cols,
                    int(math.ceil((expanded_metric_bounds[2] - shard_entry.x_min_m) / float(block_size))),
                )
                start_row = max(
                    0,
                    int(math.floor((expanded_metric_bounds[1] - shard_entry.y_min_m) / float(block_size))),
                )
                end_row = min(
                    rows,
                    int(math.ceil((expanded_metric_bounds[3] - shard_entry.y_min_m) / float(block_size))),
                )
                if start_col >= end_col or start_row >= end_row:
                    continue

                window_valid = valid_mask[start_row:end_row, start_col:end_col]
                if not np.any(window_valid):
                    continue
                valid_rows, valid_cols = np.nonzero(window_valid)
                for local_row, local_col in zip(valid_rows.tolist(), valid_cols.tolist()):
                    row_index = start_row + int(local_row)
                    col_index = start_col + int(local_col)
                    total_score = float(aggregated_scores[row_index, col_index])
                    if not math.isfinite(total_score):
                        continue
                    cell_x_min = shard_entry.x_min_m + (col_index * block_size)
                    cell_y_min = shard_entry.y_min_m + (row_index * block_size)
                    cell_x_max = cell_x_min + block_size
                    cell_y_max = cell_y_min + block_size
                    ring = _metric_cell_polygon_to_tile_ring(
                        x_min_m=cell_x_min,
                        y_min_m=cell_y_min,
                        x_max_m=cell_x_max,
                        y_max_m=cell_y_max,
                        z=int(z),
                        x=int(x),
                        y=int(y),
                    )
                    if not ring:
                        continue
                    properties = dict(zero_properties)
                    properties.update(
                        {
                            "cell_id": _cell_id(block_size, cell_x_min, cell_y_min),
                            "resolution_m": block_size,
                            "total_score": total_score,
                        }
                    )
                    features.append(
                        {
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [ring],
                            },
                            "properties": properties,
                        }
                    )
        if not features:
            return None
        return {"name": "grid", "features": features}


def fine_grid_tile_coordinates_by_zoom(
    *,
    shell_dir: str | Path,
    score_dir: str | Path,
    zooms: Iterable[int] | None = None,
) -> dict[int, list[tuple[int, int]]]:
    context = FineGridTileContext(shell_dir=Path(shell_dir), score_dir=Path(score_dir))
    result: dict[int, list[tuple[int, int]]] = {}
    for zoom in sorted(set(int(value) for value in (zooms or _FINE_GRID_RESOLUTIONS_BY_ZOOM.keys()))):
        if zoom not in _FINE_GRID_RESOLUTIONS_BY_ZOOM:
            continue
        tile_coords: set[tuple[int, int]] = set()
        for shard_id in context.shard_entries:
            bbox = context.shard_wgs84_bbox(shard_id)
            x_min, x_max, y_min, y_max = _tile_range_for_bbox(zoom, bbox)
            for tile_x in range(x_min, x_max + 1):
                for tile_y in range(y_min, y_max + 1):
                    tile_coords.add((tile_x, tile_y))
        result[zoom] = sorted(tile_coords)
    return result


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
    zoom: int,
    bbox: tuple[float, float, float, float],
) -> tuple[int, int, int, int]:
    min_lon, min_lat, max_lon, max_lat = bbox
    max_index = (1 << zoom) - 1
    x_min = max(0, _lon_to_tile_x(min_lon, zoom))
    x_max = min(max_index, _lon_to_tile_x(max_lon, zoom))
    y_min = max(0, _lat_to_tile_y(max_lat, zoom))
    y_max = min(max_index, _lat_to_tile_y(min_lat, zoom))
    return x_min, x_max, y_min, y_max


def _worker_get_engine(db_url: str):
    global _WORKER_ENGINE, _WORKER_DB_URL
    if _WORKER_ENGINE is None or _WORKER_DB_URL != db_url:
        _WORKER_ENGINE = create_engine(db_url, future=True, pool_pre_ping=True)
        _WORKER_DB_URL = db_url
    return _WORKER_ENGINE


def _fine_grid_context(config: dict[str, Any] | None) -> FineGridTileContext | None:
    if not config:
        return None
    shell_dir = str(config["shell_dir"])
    score_dir = str(config["score_dir"])
    cache_key = (shell_dir, score_dir)
    context = _FINE_GRID_CONTEXTS.get(cache_key)
    if context is None:
        context = FineGridTileContext(shell_dir=Path(shell_dir), score_dir=Path(score_dir))
        _FINE_GRID_CONTEXTS[cache_key] = context
    return context


def _reset_fine_grid_worker_caches(config: dict[str, Any] | None = None) -> None:
    if not _FINE_GRID_CONTEXTS:
        return
    if config:
        context = _FINE_GRID_CONTEXTS.get((str(config["shell_dir"]), str(config["score_dir"])))
        if context is not None:
            context.reset_large_caches()
        return
    for context in _FINE_GRID_CONTEXTS.values():
        context.reset_large_caches()


def _fine_grid_tile_bytes(
    *,
    config: dict[str, Any] | None,
    z: int,
    x: int,
    y: int,
) -> bytes:
    context = _fine_grid_context(config)
    if context is None:
        return b""
    layer = context.build_grid_layer(z=int(z), x=int(x), y=int(y))
    if layer is None:
        return b""
    return encode_vector_tile([layer], default_options={"extents": _TILE_EXTENT})


def _tile_mvt_bytes_by_flags(
    connection,
    *,
    build_key: str,
    z: int,
    x: int,
    y: int,
    layers: int,
    fine_grid_config: dict[str, Any] | None = None,
) -> bytes:
    coarse_layers = int(layers) & (~_LAYER_FINE_GRID)
    payload = bytearray(
        _coarse_tile_mvt_bytes_by_flags(
            connection,
            build_key=build_key,
            z=int(z),
            x=int(x),
            y=int(y),
            layers=coarse_layers,
        )
    )
    if layers & _LAYER_FINE_GRID:
        try:
            payload.extend(
                _fine_grid_tile_bytes(
                    config=fine_grid_config,
                    z=int(z),
                    x=int(x),
                    y=int(y),
                )
            )
        except Exception as exc:
            raise RuntimeError(
                "Fine-grid tile generation failed for "
                f"z={int(z)} x={int(x)} y={int(y)} layers={int(layers)}"
            ) from exc
    return bytes(payload)


def _bake_chunk_worker(
    chunk: list[tuple[int, int, int, int]],
    build_key: str,
    db_url: str,
    fine_grid_config: dict[str, Any] | None = None,
) -> list[tuple[int, bytes]]:
    engine = _worker_get_engine(db_url)
    out: list[tuple[int, bytes]] = []
    try:
        with engine.connect() as connection:
            for z, x, y, layers in chunk:
                payload = _tile_mvt_bytes_by_flags(
                    connection,
                    build_key=build_key,
                    z=z,
                    x=x,
                    y=y,
                    layers=layers,
                    fine_grid_config=fine_grid_config,
                )
                if not payload:
                    continue
                out.append((zxy_to_tileid(z, x, y), gzip.compress(payload)))
    finally:
        _reset_fine_grid_worker_caches(fine_grid_config)
    return out


__all__ = [
    "FineGridTileContext",
    "_FINE_GRID_RESOLUTIONS_BY_ZOOM",
    "_bake_chunk_worker",
    "_fine_grid_tile_bytes",
    "_tile_mvt_bytes_by_flags",
    "fine_grid_tile_coordinates_by_zoom",
]
