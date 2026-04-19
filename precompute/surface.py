from __future__ import annotations

import json
import math
import struct
import tempfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from shapely import area as shapely_area
from shapely import box as vector_box
from shapely import intersection as shapely_intersection
from shapely.geometry import box as geometry_box

from config import (
    CANONICAL_BASE_RESOLUTION_M,
    CAPS,
    COARSE_VECTOR_RESOLUTIONS_M,
    ENABLE_FINE_RASTER_SURFACE,
    FINE_RESOLUTIONS_M,
    FINE_SURFACE_SCHEMA_VERSION,
    GRID_GEOMETRY_SCHEMA_VERSION,
    SURFACE_SCORE_RAMP,
    SURFACE_SHELL_SCHEMA_VERSION,
    SURFACE_SHARD_SIZE_M,
    SURFACE_TILE_SIZE_PX,
    SURFACE_ZOOM_BREAKS,
    TO_TARGET,
    TO_WGS84,
    hash_dict,
    resolution_for_zoom,
)
from .grid import build_cell_id, score_cell


SURFACE_MANIFEST_NAME = "manifest.json"
NODE_SCORES_FILENAME = "node_scores.npz"
SURFACE_SHELL_DIRNAME = "shards"
SURFACE_SCORE_DIRNAME = "shards"
SURFACE_TILE_CACHE_DIRNAME = "tiles"
SURFACE_SCORE_SCHEMA_VERSION = FINE_SURFACE_SCHEMA_VERSION
SURFACE_TILE_SCHEMA_VERSION = 1
_CATEGORY_ORDER = tuple(CAPS)
_CATEGORY_TO_INDEX = {category: index for index, category in enumerate(_CATEGORY_ORDER)}
_DENSITY_NORMALIZED_CATEGORIES = {"shops", "transport", "healthcare"}
_WEIGHTED_UNIT_CATEGORIES = {"shops", "healthcare", "parks"}
_MIN_DENSITY_AREA_RATIO = 0.25


@dataclass(frozen=True)
class ShardEntry:
    shard_id: str
    x_min_m: int
    y_min_m: int
    x_max_m: int
    y_max_m: int
    rows: int
    cols: int
    path: str


def surface_shell_dir(cache_dir: Path, *, surface_shell_hash: str) -> Path:
    return cache_dir / f"surface_shell_{surface_shell_hash}"


def surface_score_dir(cache_dir: Path, *, score_hash: str) -> Path:
    return cache_dir / f"surface_scores_{score_hash}"


def surface_tile_dir(cache_dir: Path, *, score_hash: str, render_hash: str) -> Path:
    return cache_dir / f"surface_tiles_{score_hash}_{render_hash}"


def surface_manifest_path(surface_dir: Path) -> Path:
    return surface_dir / SURFACE_MANIFEST_NAME


def surface_shell_shard_filename(shard_id: str) -> str:
    return f"{shard_id}.npz"


def surface_shell_shard_path(surface_dir: Path, shard_id: str) -> Path:
    return surface_dir / SURFACE_SHELL_DIRNAME / surface_shell_shard_filename(shard_id)


def surface_score_shard_filename(shard_id: str) -> str:
    return f"{shard_id}.npz"


def surface_score_shard_path(surface_dir: Path, shard_id: str) -> Path:
    return surface_dir / SURFACE_SCORE_DIRNAME / surface_score_shard_filename(shard_id)


def surface_score_node_scores_path(surface_dir: Path) -> Path:
    return surface_dir / NODE_SCORES_FILENAME


def surface_tile_cache_path(
    tile_dir: Path,
    *,
    resolution_m: int,
    z: int,
    x: int,
    y: int,
) -> Path:
    return (
        tile_dir
        / SURFACE_TILE_CACHE_DIRNAME
        / str(int(resolution_m))
        / str(int(z))
        / str(int(x))
        / f"{int(y)}.png"
    )


def load_surface_manifest(surface_dir: Path) -> dict[str, Any] | None:
    path = surface_manifest_path(surface_dir)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def write_surface_manifest(surface_dir: Path, payload: dict[str, Any]) -> None:
    surface_dir.mkdir(parents=True, exist_ok=True)
    path = surface_manifest_path(surface_dir)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    tmp_path.replace(path)


def build_surface_shell_hash(reach_hash: str) -> str:
    return hash_dict(
        {
            "reach_hash": str(reach_hash),
            "canonical_base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "surface_shard_size_m": SURFACE_SHARD_SIZE_M,
            "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
            "surface_shell_schema_version": SURFACE_SHELL_SCHEMA_VERSION,
        }
    )


def _manifest_shard_entries(manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    shard_entries = [] if manifest is None else manifest.get("shard_inventory", [])
    if not isinstance(shard_entries, list):
        return []
    return [entry for entry in shard_entries if isinstance(entry, dict)]


def surface_shell_ready(
    shell_dir: Path,
    *,
    expected_surface_shell_hash: str,
) -> bool:
    manifest = load_surface_manifest(shell_dir)
    if manifest is None:
        return False
    if manifest.get("status") != "complete":
        return False
    if int(manifest.get("schema_version", 0)) != SURFACE_SHELL_SCHEMA_VERSION:
        return False
    if str(manifest.get("surface_shell_hash", "")) != expected_surface_shell_hash:
        return False
    if int(manifest.get("base_resolution_m", 0)) != CANONICAL_BASE_RESOLUTION_M:
        return False
    if int(manifest.get("shard_size_m", 0)) != SURFACE_SHARD_SIZE_M:
        return False
    shard_entries = _manifest_shard_entries(manifest)
    if not shard_entries:
        return False
    total_shards = int(manifest.get("total_shards", len(shard_entries)))
    completed_shards = int(manifest.get("completed_shards", total_shards))
    if total_shards != len(shard_entries):
        return False
    if completed_shards < total_shards:
        return False
    for entry in shard_entries:
        shard_file = shell_dir / str(entry.get("path", ""))
        if not shard_file.exists():
            return False
    return True


def surface_score_ready(
    score_dir: Path,
    *,
    expected_score_hash: str,
    expected_surface_shell_hash: str,
) -> bool:
    manifest = load_surface_manifest(score_dir)
    if manifest is None:
        return False
    if manifest.get("status") != "complete":
        return False
    if int(manifest.get("schema_version", 0)) != SURFACE_SCORE_SCHEMA_VERSION:
        return False
    if str(manifest.get("score_hash", "")) != expected_score_hash:
        return False
    if str(manifest.get("surface_shell_hash", "")) != expected_surface_shell_hash:
        return False
    if not surface_score_node_scores_path(score_dir).exists():
        return False
    shard_entries = _manifest_shard_entries(manifest)
    if not shard_entries:
        return False
    for entry in shard_entries:
        shard_file = score_dir / str(entry.get("path", ""))
        if not shard_file.exists():
            return False
    return True


def surface_analysis_ready(
    shell_dir: Path,
    score_dir: Path,
    *,
    expected_surface_shell_hash: str,
    expected_score_hash: str,
) -> bool:
    return surface_shell_ready(
        shell_dir,
        expected_surface_shell_hash=expected_surface_shell_hash,
    ) and surface_score_ready(
        score_dir,
        expected_score_hash=expected_score_hash,
        expected_surface_shell_hash=expected_surface_shell_hash,
    )


def ensure_surface_tile_cache_manifest(
    tile_dir: Path,
    *,
    score_hash: str,
    render_hash: str,
) -> dict[str, Any]:
    manifest = load_surface_manifest(tile_dir)
    expected_payload = {
        "status": "ready",
        "schema_version": SURFACE_TILE_SCHEMA_VERSION,
        "score_hash": str(score_hash),
        "render_hash": str(render_hash),
        "tile_cache_dir": SURFACE_TILE_CACHE_DIRNAME,
    }
    if (
        manifest is not None
        and manifest.get("status") == "ready"
        and int(manifest.get("schema_version", 0)) == SURFACE_TILE_SCHEMA_VERSION
        and str(manifest.get("score_hash", "")) == str(score_hash)
        and str(manifest.get("render_hash", "")) == str(render_hash)
    ):
        return manifest
    write_surface_manifest(tile_dir, expected_payload)
    return expected_payload


def aligned_floor(value: float, step: int) -> int:
    return int(math.floor(float(value) / float(step)) * int(step))


def aligned_ceil(value: float, step: int) -> int:
    return int(math.ceil(float(value) / float(step)) * int(step))


def shard_id_for_origin(x_min_m: int, y_min_m: int) -> str:
    return f"{int(x_min_m)}_{int(y_min_m)}"


def shard_origin_from_id(shard_id: str) -> tuple[int, int]:
    x_text, y_text = str(shard_id).split("_", 1)
    return (int(x_text), int(y_text))


def canonical_cells_per_shard() -> int:
    return SURFACE_SHARD_SIZE_M // CANONICAL_BASE_RESOLUTION_M


def aggregation_factor(resolution_m: int) -> int:
    resolution_value = int(resolution_m)
    if resolution_value == CANONICAL_BASE_RESOLUTION_M:
        return 1
    if resolution_value not in FINE_RESOLUTIONS_M:
        raise ValueError(f"Unsupported fine surface resolution: {resolution_m}")
    factor = resolution_value // CANONICAL_BASE_RESOLUTION_M
    if factor * CANONICAL_BASE_RESOLUTION_M != resolution_value:
        raise ValueError(f"Resolution {resolution_m} is not an exact multiple of 50m")
    return factor


def iter_shard_entries(
    study_area_metric,
    *,
    shard_size_m: int = SURFACE_SHARD_SIZE_M,
    base_resolution_m: int = CANONICAL_BASE_RESOLUTION_M,
) -> list[ShardEntry]:
    if shard_size_m % base_resolution_m != 0:
        raise ValueError("Shard size must be exactly divisible by the canonical base resolution")

    minx, miny, maxx, maxy = study_area_metric.bounds
    start_x = aligned_floor(minx, shard_size_m)
    start_y = aligned_floor(miny, shard_size_m)
    end_x = aligned_ceil(maxx, shard_size_m)
    end_y = aligned_ceil(maxy, shard_size_m)
    rows = shard_size_m // base_resolution_m
    cols = shard_size_m // base_resolution_m

    entries: list[ShardEntry] = []
    for y_min_m in range(start_y, end_y, shard_size_m):
        for x_min_m in range(start_x, end_x, shard_size_m):
            shard_box = geometry_box(
                x_min_m,
                y_min_m,
                x_min_m + shard_size_m,
                y_min_m + shard_size_m,
            )
            if not study_area_metric.intersects(shard_box):
                continue
            shard_id = shard_id_for_origin(x_min_m, y_min_m)
            entries.append(
                ShardEntry(
                    shard_id=shard_id,
                    x_min_m=int(x_min_m),
                    y_min_m=int(y_min_m),
                    x_max_m=int(x_min_m + shard_size_m),
                    y_max_m=int(y_min_m + shard_size_m),
                    rows=int(rows),
                    cols=int(cols),
                    path=f"{SURFACE_SHELL_DIRNAME}/{surface_shell_shard_filename(shard_id)}",
                )
            )
    return entries


def _normalized_area_ratio_array(effective_area_ratio: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(effective_area_ratio, dtype=np.float32), 0.0, 1.0)
    return np.maximum(clipped, _MIN_DENSITY_AREA_RATIO)


def component_scores_for_nodes(
    counts_matrix: np.ndarray,
    weighted_units_matrix: np.ndarray,
    effective_area_ratio: np.ndarray,
) -> np.ndarray:
    ratios = _normalized_area_ratio_array(effective_area_ratio).astype(np.float32, copy=False)
    row_count = int(ratios.shape[0])
    scores = np.zeros((row_count, len(_CATEGORY_ORDER)), dtype=np.float32)

    for category_index, category in enumerate(_CATEGORY_ORDER):
        cap_value = float(CAPS[category])
        if category in _WEIGHTED_UNIT_CATEGORIES:
            effective_count = weighted_units_matrix[:row_count, category_index].astype(np.float32)
            if category == "parks" or category in _DENSITY_NORMALIZED_CATEGORIES:
                effective_count = effective_count / ratios
        elif category in _DENSITY_NORMALIZED_CATEGORIES:
            effective_count = counts_matrix[:row_count, category_index].astype(np.float32) / ratios
        else:
            effective_count = counts_matrix[:row_count, category_index].astype(np.float32)
        scores[:, category_index] = np.minimum(effective_count / cap_value, 1.0) * 25.0
    return scores


def build_node_score_arrays(
    walk_graph,
    walk_counts_by_node: dict[int, dict[str, int]],
    walk_weighted_units_by_node: dict[int, dict[str, int]],
) -> dict[str, Any]:
    node_count = int(walk_graph.vcount())
    counts_matrix = np.zeros((node_count, len(_CATEGORY_ORDER)), dtype=np.uint32)
    weighted_units_matrix = np.zeros((node_count, len(_CATEGORY_ORDER)), dtype=np.float32)

    for node_idx, category_counts in walk_counts_by_node.items():
        normalized_node_idx = int(node_idx)
        if normalized_node_idx < 0 or normalized_node_idx >= node_count:
            continue
        for category, count in category_counts.items():
            if category not in _CATEGORY_TO_INDEX:
                continue
            counts_matrix[normalized_node_idx, _CATEGORY_TO_INDEX[category]] = max(int(count), 0)

    for node_idx, category_units in walk_weighted_units_by_node.items():
        normalized_node_idx = int(node_idx)
        if normalized_node_idx < 0 or normalized_node_idx >= node_count:
            continue
        for category, units in dict(category_units).items():
            if category not in _CATEGORY_TO_INDEX:
                continue
            weighted_units_matrix[normalized_node_idx, _CATEGORY_TO_INDEX[category]] = max(
                float(units),
                0.0,
            )

    reference_scores = component_scores_for_nodes(
        counts_matrix,
        weighted_units_matrix,
        np.ones(node_count, dtype=np.float32),
    )
    reference_total = reference_scores.sum(axis=1, dtype=np.float32)
    return {
        "categories": list(_CATEGORY_ORDER),
        "counts_matrix": counts_matrix,
        "weighted_units_matrix": weighted_units_matrix,
        "reference_scores": reference_scores.astype(np.float32, copy=False),
        "reference_total": reference_total.astype(np.float32, copy=False),
    }


def save_node_score_arrays(score_dir: Path, payload: dict[str, Any]) -> None:
    score_dir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        surface_score_node_scores_path(score_dir),
        categories=np.asarray(payload["categories"], dtype="<U32"),
        counts_matrix=np.asarray(payload["counts_matrix"], dtype=np.uint32),
        weighted_units_matrix=np.asarray(payload["weighted_units_matrix"], dtype=np.float32),
        reference_scores=np.asarray(payload["reference_scores"], dtype=np.float32),
        reference_total=np.asarray(payload["reference_total"], dtype=np.float32),
    )


def load_node_score_arrays(score_dir: Path) -> dict[str, Any]:
    with np.load(surface_score_node_scores_path(score_dir), allow_pickle=False) as data:
        return {
            "categories": [str(value) for value in data["categories"].tolist()],
            "counts_matrix": np.asarray(data["counts_matrix"], dtype=np.uint32),
            "weighted_units_matrix": np.asarray(data["weighted_units_matrix"], dtype=np.float32),
            "reference_scores": np.asarray(data["reference_scores"], dtype=np.float32),
            "reference_total": np.asarray(data["reference_total"], dtype=np.float32),
        }


def weighted_mean_block_reduce(
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


def _canonical_cell_boxes(x_min_m: int, y_min_m: int) -> tuple[np.ndarray, np.ndarray]:
    cells_per_side = canonical_cells_per_shard()
    x_min = x_min_m + (np.arange(cells_per_side, dtype=np.float64) * CANONICAL_BASE_RESOLUTION_M)
    y_min = y_min_m + (np.arange(cells_per_side, dtype=np.float64) * CANONICAL_BASE_RESOLUTION_M)
    return x_min, y_min


def build_shard_payload(
    *,
    study_area_metric,
    walk_graph,
    shard_entry: ShardEntry,
    nearest_nodes,
) -> dict[str, Any]:
    shard_geom = study_area_metric.intersection(
        geometry_box(
            shard_entry.x_min_m,
            shard_entry.y_min_m,
            shard_entry.x_max_m,
            shard_entry.y_max_m,
        )
    )

    cells_per_side = canonical_cells_per_shard()
    ratios = np.zeros((cells_per_side, cells_per_side), dtype=np.float32)
    if not shard_geom.is_empty:
        full_shard_area = float(SURFACE_SHARD_SIZE_M * SURFACE_SHARD_SIZE_M)
        if math.isclose(float(shard_geom.area), full_shard_area, rel_tol=0.0, abs_tol=1e-6):
            ratios.fill(1.0)
        else:
            x_min_values, y_min_values = _canonical_cell_boxes(
                shard_entry.x_min_m,
                shard_entry.y_min_m,
            )
            x_mins = np.tile(x_min_values, cells_per_side)
            y_mins = np.repeat(y_min_values, cells_per_side)
            cell_boxes = vector_box(
                x_mins,
                y_mins,
                x_mins + CANONICAL_BASE_RESOLUTION_M,
                y_mins + CANONICAL_BASE_RESOLUTION_M,
            )
            ratios = np.clip(
                shapely_area(shapely_intersection(cell_boxes, shard_geom)).reshape(
                    cells_per_side,
                    cells_per_side,
                )
                / float(CANONICAL_BASE_RESOLUTION_M * CANONICAL_BASE_RESOLUTION_M),
                0.0,
                1.0,
            ).astype(np.float32)

    valid_land_mask = ratios > 0.0
    origin_node_idx = np.full((cells_per_side, cells_per_side), -1, dtype=np.int32)

    if np.any(valid_land_mask):
        x_centres = (
            shard_entry.x_min_m
            + (np.arange(cells_per_side, dtype=np.float64) * CANONICAL_BASE_RESOLUTION_M)
            + (CANONICAL_BASE_RESOLUTION_M / 2.0)
        )
        y_centres = (
            shard_entry.y_min_m
            + (np.arange(cells_per_side, dtype=np.float64) * CANONICAL_BASE_RESOLUTION_M)
            + (CANONICAL_BASE_RESOLUTION_M / 2.0)
        )
        flat_x = np.tile(x_centres, cells_per_side)
        flat_y = np.repeat(y_centres, cells_per_side)
        valid_flat = valid_land_mask.ravel()
        lons, lats = TO_WGS84(flat_x[valid_flat], flat_y[valid_flat])
        node_indexes = nearest_nodes(
            walk_graph,
            np.asarray(lons, dtype=np.float64).tolist(),
            np.asarray(lats, dtype=np.float64).tolist(),
        )
        origin_node_idx.ravel()[valid_flat] = np.asarray(node_indexes, dtype=np.int32)

    return {
        "origin_node_idx": origin_node_idx,
        "effective_area_ratio": ratios.astype(np.float32, copy=False),
        "valid_land_mask": valid_land_mask,
        "x_min_m": int(shard_entry.x_min_m),
        "y_min_m": int(shard_entry.y_min_m),
    }


def save_shell_shard_payload(shell_dir: Path, shard_entry: ShardEntry, payload: dict[str, Any]) -> None:
    target_path = surface_shell_shard_path(shell_dir, shard_entry.shard_id)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        target_path,
        origin_node_idx=np.asarray(payload["origin_node_idx"], dtype=np.int32),
        effective_area_ratio=np.asarray(payload["effective_area_ratio"], dtype=np.float32),
        valid_land_mask=np.asarray(payload["valid_land_mask"], dtype=bool),
        x_min_m=np.asarray([payload["x_min_m"]], dtype=np.int32),
        y_min_m=np.asarray([payload["y_min_m"]], dtype=np.int32),
    )


def load_shell_shard_payload(shell_dir: Path, shard_id: str) -> dict[str, Any]:
    with np.load(surface_shell_shard_path(shell_dir, shard_id), allow_pickle=False) as data:
        return {
            "origin_node_idx": np.asarray(data["origin_node_idx"], dtype=np.int32),
            "effective_area_ratio": np.asarray(data["effective_area_ratio"], dtype=np.float32),
            "valid_land_mask": np.asarray(data["valid_land_mask"], dtype=bool),
            "x_min_m": int(np.asarray(data["x_min_m"], dtype=np.int32)[0]),
            "y_min_m": int(np.asarray(data["y_min_m"], dtype=np.int32)[0]),
        }


def _canonical_total_scores_for_shard(
    shell_payload: dict[str, Any],
    node_scores: dict[str, Any],
) -> np.ndarray:
    weights = np.asarray(shell_payload["effective_area_ratio"], dtype=np.float32)
    total_scores = np.full(weights.shape, np.nan, dtype=np.float32)
    origin_node_idx = np.asarray(shell_payload["origin_node_idx"], dtype=np.int32)
    valid_land_mask = np.asarray(shell_payload["valid_land_mask"], dtype=bool)
    valid_flat = valid_land_mask.ravel() & (origin_node_idx.ravel() >= 0)
    if not np.any(valid_flat):
        return total_scores

    node_ids = origin_node_idx.ravel()[valid_flat]
    counts_matrix = node_scores["counts_matrix"][node_ids]
    weighted_units_matrix = node_scores["weighted_units_matrix"][node_ids]
    component_scores = component_scores_for_nodes(
        counts_matrix,
        weighted_units_matrix,
        weights.ravel()[valid_flat].astype(np.float32),
    )
    total_scores.ravel()[valid_flat] = component_scores.sum(axis=1, dtype=np.float32)
    return total_scores


def save_score_shard_payload(score_dir: Path, shard_id: str, total_score_50: np.ndarray) -> None:
    target_path = surface_score_shard_path(score_dir, shard_id)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        target_path,
        total_score_50=np.asarray(total_score_50, dtype=np.float32),
    )


def load_score_shard_payload(score_dir: Path, shard_id: str) -> dict[str, Any]:
    with np.load(surface_score_shard_path(score_dir, shard_id), allow_pickle=False) as data:
        return {
            "total_score_50": np.asarray(data["total_score_50"], dtype=np.float32),
        }


def ensure_surface_shell_cache(
    *,
    shell_dir: Path,
    surface_shell_hash: str,
    reach_hash: str,
    study_area_metric,
    graph_dir: Path,
    walkgraph_bin: str,
    node_count: int,
    threads: int | None = None,
    tracker=None,
) -> dict[str, Any]:
    from network.loader import run_surface_shell_build

    shell_dir.mkdir(parents=True, exist_ok=True)
    shard_entries = iter_shard_entries(study_area_metric)

    if tracker is not None:
        tracker.start_phase(
            "fine_surface",
            total_units=len(shard_entries),
            rebuild_total_units=0,
            unit_label="shards",
            detail="checking fine surface shell cache",
        )

    if surface_shell_ready(
        shell_dir,
        expected_surface_shell_hash=surface_shell_hash,
    ):
        manifest = load_surface_manifest(shell_dir) or {}
        if tracker is not None:
            tracker.finish_phase("fine_surface", "cached", detail=f"{len(shard_entries):,} shards ready")
        return manifest

    # Build config JSON for the Rust binary (verbatim manifest fields).
    config_payload = {
        "coarse_vector_resolutions_m": list(COARSE_VECTOR_RESOLUTIONS_M),
        "fine_resolutions_m": list(FINE_RESOLUTIONS_M),
        "surface_zoom_breaks": list(SURFACE_ZOOM_BREAKS),
        "tile_size_px": int(SURFACE_TILE_SIZE_PX),
    }

    # Progress callback: advance tracker on each "surface: shard X done (N/T)" line.
    total = len(shard_entries)

    def _progress_cb(event: str, **kwargs: Any) -> None:
        if tracker is None:
            return
        detail = kwargs.get("detail", event)
        if "shard" in detail and "done" in detail:
            tracker.advance_phase("fine_surface", units=1, rebuild_units=1, detail=detail)

    with (
        tempfile.NamedTemporaryFile(
            mode="w", suffix=".geojson", delete=False, encoding="utf-8"
        ) as geo_f,
        tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as cfg_f,
    ):
        geojson_path = Path(geo_f.name)
        config_json_path = Path(cfg_f.name)
        # Simplify to cell resolution before export — sub-50m detail is
        # below the grid precision and only bloats the Rust polygon index.
        export_geom = study_area_metric.simplify(
            float(CANONICAL_BASE_RESOLUTION_M), preserve_topology=True
        )
        json.dump(
            {"type": "Feature", "geometry": export_geom.__geo_interface__, "properties": {}},
            geo_f,
        )
        json.dump(config_payload, cfg_f)

    try:
        run_surface_shell_build(
            nodes_bin=graph_dir / "walk_graph.nodes.bin",
            study_area_geojson_path=geojson_path,
            shell_dir=shell_dir,
            walkgraph_bin=walkgraph_bin,
            surface_shell_hash=surface_shell_hash,
            reach_hash=reach_hash,
            node_count=node_count,
            config_json_path=config_json_path,
            shard_size_m=SURFACE_SHARD_SIZE_M,
            base_resolution_m=CANONICAL_BASE_RESOLUTION_M,
            threads=threads,
            progress_cb=_progress_cb,
        )
    finally:
        geojson_path.unlink(missing_ok=True)
        config_json_path.unlink(missing_ok=True)

    if tracker is not None:
        tracker.finish_phase("fine_surface", "completed", detail=f"{total:,} shards written")
    return load_surface_manifest(shell_dir) or {}


def collect_surface_origin_nodes(shell_dir: Path) -> list[int]:
    manifest = load_surface_manifest(shell_dir)
    if manifest is None:
        return []
    origin_nodes: set[int] = set()
    for entry in _manifest_shard_entries(manifest):
        shard_id = str(entry.get("shard_id", ""))
        if not shard_id:
            continue
        payload = load_shell_shard_payload(shell_dir, shard_id)
        shard_nodes = np.asarray(payload["origin_node_idx"], dtype=np.int32)
        valid_nodes = shard_nodes[shard_nodes >= 0]
        if valid_nodes.size == 0:
            continue
        origin_nodes.update(int(node) for node in np.unique(valid_nodes).tolist())
    return sorted(origin_nodes)


def ensure_surface_score_cache(
    *,
    shell_dir: Path,
    score_dir: Path,
    surface_shell_hash: str,
    score_hash: str,
    walk_graph,
    walk_counts_by_node: dict[int, dict[str, int]],
    walk_weighted_units_by_node: dict[int, dict[str, int]],
    tracker=None,
) -> dict[str, Any]:
    shell_manifest = load_surface_manifest(shell_dir)
    if shell_manifest is None:
        raise RuntimeError(f"Fine surface shell manifest not found at {surface_manifest_path(shell_dir)}")
    shard_entries = _manifest_shard_entries(shell_manifest)

    if tracker is not None:
        tracker.start_phase(
            "node_scores",
            total_units=1 + len(shard_entries),
            rebuild_total_units=0,
            unit_label="artifacts",
            detail="checking fine surface score cache",
        )

    if surface_score_ready(
        score_dir,
        expected_score_hash=score_hash,
        expected_surface_shell_hash=surface_shell_hash,
    ):
        manifest = load_surface_manifest(score_dir) or {}
        if tracker is not None:
            tracker.finish_phase(
                "node_scores",
                "cached",
                detail=f"{1 + len(shard_entries):,} score artifacts ready",
            )
        return manifest

    score_dir.mkdir(parents=True, exist_ok=True)
    node_scores = build_node_score_arrays(
        walk_graph,
        walk_counts_by_node,
        walk_weighted_units_by_node,
    )
    save_node_score_arrays(score_dir, node_scores)
    if tracker is not None:
        tracker.advance_phase(
            "node_scores",
            units=1,
            rebuild_units=1,
            detail="writing node score arrays",
        )

    manifest_payload = {
        "status": "building",
        "schema_version": SURFACE_SCORE_SCHEMA_VERSION,
        "score_hash": score_hash,
        "surface_shell_hash": surface_shell_hash,
        "base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
        "node_scores_file": NODE_SCORES_FILENAME,
        "shard_inventory": [],
    }
    write_surface_manifest(score_dir, manifest_payload)

    for entry in shard_entries:
        shard_id = str(entry["shard_id"])
        shell_payload = load_shell_shard_payload(shell_dir, shard_id)
        total_score_50 = _canonical_total_scores_for_shard(shell_payload, node_scores)
        save_score_shard_payload(score_dir, shard_id, total_score_50)
        if tracker is not None:
            tracker.advance_phase(
                "node_scores",
                units=1,
                rebuild_units=1,
                detail=f"writing canonical totals for shard {shard_id}",
            )

    manifest_payload["status"] = "complete"
    manifest_payload["categories"] = list(_CATEGORY_ORDER)
    manifest_payload["shard_inventory"] = [
        {
            "shard_id": str(entry["shard_id"]),
            "path": f"{SURFACE_SCORE_DIRNAME}/{surface_score_shard_filename(str(entry['shard_id']))}",
        }
        for entry in shard_entries
    ]
    write_surface_manifest(score_dir, manifest_payload)
    if tracker is not None:
        tracker.finish_phase(
            "node_scores",
            "completed",
            detail=f"{1 + len(shard_entries):,} score artifacts written",
        )
    return manifest_payload


def _metric_tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    tile_count = float(1 << int(z))
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


def _tile_pixel_metric_coordinates(z: int, x: int, y: int) -> tuple[np.ndarray, np.ndarray]:
    tile_size = float(SURFACE_TILE_SIZE_PX)
    offsets = (np.arange(SURFACE_TILE_SIZE_PX, dtype=np.float64) + 0.5) / tile_size
    tile_count = float(1 << int(z))
    lon = ((float(x) + offsets) / tile_count) * 360.0 - 180.0
    mercator = math.pi * (1.0 - (2.0 * (float(y) + offsets)) / tile_count)
    lat = np.degrees(np.arctan(np.sinh(mercator)))
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    metric_x, metric_y = TO_TARGET(lon_grid.ravel(), lat_grid.ravel())
    return (
        np.asarray(metric_x, dtype=np.float64).reshape(SURFACE_TILE_SIZE_PX, SURFACE_TILE_SIZE_PX),
        np.asarray(metric_y, dtype=np.float64).reshape(SURFACE_TILE_SIZE_PX, SURFACE_TILE_SIZE_PX),
    )


def _score_ramp_arrays() -> tuple[np.ndarray, np.ndarray]:
    stops = np.asarray([float(stop) for stop, _ in SURFACE_SCORE_RAMP], dtype=np.float32)
    colors = np.asarray(
        [
            (
                int(color[1:3], 16),
                int(color[3:5], 16),
                int(color[5:7], 16),
            )
            for _, color in SURFACE_SCORE_RAMP
        ],
        dtype=np.float32,
    )
    return stops, colors


def colorize_scores(scores: np.ndarray) -> np.ndarray:
    stops, colors = _score_ramp_arrays()
    flat_scores = np.asarray(scores, dtype=np.float32)
    output = np.zeros((flat_scores.shape[0], 4), dtype=np.uint8)
    valid_mask = np.isfinite(flat_scores)
    if not np.any(valid_mask):
        return output

    clipped = np.clip(flat_scores[valid_mask], stops[0], stops[-1])
    upper_index = np.searchsorted(stops, clipped, side="right")
    upper_index = np.clip(upper_index, 1, len(stops) - 1)
    lower_index = upper_index - 1
    lower_stop = stops[lower_index]
    upper_stop = stops[upper_index]
    stop_delta = np.maximum(upper_stop - lower_stop, 1e-6)
    mix = ((clipped - lower_stop) / stop_delta).reshape(-1, 1)
    rgb = colors[lower_index] + ((colors[upper_index] - colors[lower_index]) * mix)
    output[valid_mask, :3] = np.clip(np.round(rgb), 0, 255).astype(np.uint8)
    output[valid_mask, 3] = 255
    return output


def encode_png_rgba(rgba: np.ndarray) -> bytes:
    image = np.asarray(rgba, dtype=np.uint8)
    if image.ndim != 3 or image.shape[2] != 4:
        raise ValueError("PNG encoder requires an RGBA image")
    height, width, _ = image.shape

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        return (
            struct.pack("!I", len(data))
            + chunk_type
            + data
            + struct.pack("!I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
        )

    raw = b"".join(b"\x00" + image[row].tobytes() for row in range(height))
    return b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            _chunk(b"IHDR", struct.pack("!IIBBBBB", width, height, 8, 6, 0, 0, 0)),
            _chunk(b"IDAT", zlib.compress(raw, level=6)),
            _chunk(b"IEND", b""),
        ]
    )


class FineSurfaceRuntime:
    def __init__(self, shell_dir: Path, score_dir: Path, tile_dir: Path) -> None:
        self.shell_dir = Path(shell_dir)
        self.score_dir = Path(score_dir)
        self.tile_dir = Path(tile_dir)
        shell_manifest = load_surface_manifest(self.shell_dir)
        if shell_manifest is None:
            raise RuntimeError(f"Fine surface shell manifest not found at {surface_manifest_path(self.shell_dir)}")
        score_manifest = load_surface_manifest(self.score_dir)
        if score_manifest is None:
            raise RuntimeError(f"Fine surface score manifest not found at {surface_manifest_path(self.score_dir)}")
        self.shell_manifest = shell_manifest
        self.score_manifest = score_manifest
        self.shard_inventory = {
            str(entry["shard_id"]): entry
            for entry in _manifest_shard_entries(shell_manifest)
            if "shard_id" in entry
        }
        self._node_scores: dict[str, Any] | None = None
        self._shell_shard_cache: dict[str, dict[str, Any]] = {}
        self._score_shard_cache: dict[str, dict[str, Any]] = {}
        self._aggregated_cache: dict[tuple[str, int], tuple[np.ndarray, np.ndarray]] = {}

    def _load_node_scores(self) -> dict[str, Any]:
        if self._node_scores is None:
            self._node_scores = load_node_score_arrays(self.score_dir)
        return self._node_scores

    def _load_shell_shard(self, shard_id: str) -> dict[str, Any]:
        if shard_id not in self._shell_shard_cache:
            self._shell_shard_cache[shard_id] = load_shell_shard_payload(self.shell_dir, shard_id)
        return self._shell_shard_cache[shard_id]

    def _load_score_shard(self, shard_id: str) -> dict[str, Any]:
        if shard_id not in self._score_shard_cache:
            self._score_shard_cache[shard_id] = load_score_shard_payload(self.score_dir, shard_id)
        return self._score_shard_cache[shard_id]

    def canonical_shard_surface(self, shard_id: str) -> tuple[np.ndarray, np.ndarray]:
        cache_key = (shard_id, CANONICAL_BASE_RESOLUTION_M)
        if cache_key in self._aggregated_cache:
            return self._aggregated_cache[cache_key]

        canonical_scores = np.asarray(
            self._load_score_shard(shard_id)["total_score_50"],
            dtype=np.float32,
        )
        valid_mask = np.asarray(
            self._load_shell_shard(shard_id)["valid_land_mask"],
            dtype=bool,
        )
        self._aggregated_cache[cache_key] = (canonical_scores, valid_mask)
        return canonical_scores, valid_mask

    def aggregated_shard_surface(self, shard_id: str, resolution_m: int) -> tuple[np.ndarray, np.ndarray]:
        normalized_resolution = int(resolution_m)
        cache_key = (shard_id, normalized_resolution)
        if cache_key in self._aggregated_cache:
            return self._aggregated_cache[cache_key]

        canonical_scores, _ = self.canonical_shard_surface(shard_id)
        canonical_weights = np.asarray(
            self._load_shell_shard(shard_id)["effective_area_ratio"],
            dtype=np.float32,
        )
        aggregated_scores, valid_mask = weighted_mean_block_reduce(
            canonical_scores,
            canonical_weights,
            aggregation_factor(normalized_resolution),
        )
        self._aggregated_cache[cache_key] = (aggregated_scores, valid_mask)
        return aggregated_scores, valid_mask

    def _iter_intersecting_shards(self, metric_bounds: tuple[float, float, float, float]):
        shard_min_x = aligned_floor(metric_bounds[0], SURFACE_SHARD_SIZE_M)
        shard_min_y = aligned_floor(metric_bounds[1], SURFACE_SHARD_SIZE_M)
        shard_max_x = aligned_floor(metric_bounds[2], SURFACE_SHARD_SIZE_M)
        shard_max_y = aligned_floor(metric_bounds[3], SURFACE_SHARD_SIZE_M)
        for shard_y in range(shard_min_y, shard_max_y + SURFACE_SHARD_SIZE_M, SURFACE_SHARD_SIZE_M):
            for shard_x in range(shard_min_x, shard_max_x + SURFACE_SHARD_SIZE_M, SURFACE_SHARD_SIZE_M):
                shard_id = shard_id_for_origin(shard_x, shard_y)
                if shard_id in self.shard_inventory:
                    yield shard_id

    def render_tile(self, *, resolution_m: int, z: int, x: int, y: int) -> bytes:
        cache_path = surface_tile_cache_path(
            self.tile_dir,
            resolution_m=resolution_m,
            z=z,
            x=x,
            y=y,
        )
        if cache_path.exists():
            return cache_path.read_bytes()

        metric_bounds = _metric_tile_bounds(z, x, y)
        metric_x, metric_y = _tile_pixel_metric_coordinates(z, x, y)
        block_size = int(resolution_m)
        tile_block_x = np.floor(metric_x / float(block_size)).astype(np.int64) * block_size
        tile_block_y = np.floor(metric_y / float(block_size)).astype(np.int64) * block_size
        tile_scores = np.full((SURFACE_TILE_SIZE_PX, SURFACE_TILE_SIZE_PX), np.nan, dtype=np.float32)

        for shard_id in self._iter_intersecting_shards(metric_bounds):
            shard_entry = self.shard_inventory[shard_id]
            shard_x_min = int(shard_entry["x_min_m"])
            shard_y_min = int(shard_entry["y_min_m"])
            aggregated_scores, valid_mask = self.aggregated_shard_surface(shard_id, resolution_m)
            rows, cols = aggregated_scores.shape

            start_col = max(0, int(math.floor((metric_bounds[0] - shard_x_min) / float(block_size))))
            end_col = min(cols, int(math.ceil((metric_bounds[2] - shard_x_min) / float(block_size))))
            start_row = max(0, int(math.floor((metric_bounds[1] - shard_y_min) / float(block_size))))
            end_row = min(rows, int(math.ceil((metric_bounds[3] - shard_y_min) / float(block_size))))
            if start_col >= end_col or start_row >= end_row:
                continue

            window_scores = aggregated_scores[start_row:end_row, start_col:end_col]
            window_valid = valid_mask[start_row:end_row, start_col:end_col]
            window_x_min = shard_x_min + (start_col * block_size)
            window_x_max = shard_x_min + (end_col * block_size)
            window_y_min = shard_y_min + (start_row * block_size)
            window_y_max = shard_y_min + (end_row * block_size)
            pixel_mask = (
                (tile_block_x >= window_x_min)
                & (tile_block_x < window_x_max)
                & (tile_block_y >= window_y_min)
                & (tile_block_y < window_y_max)
            )
            if not np.any(pixel_mask):
                continue

            local_cols = ((tile_block_x[pixel_mask] - window_x_min) // block_size).astype(np.int64)
            local_rows = ((tile_block_y[pixel_mask] - window_y_min) // block_size).astype(np.int64)
            local_scores = np.where(
                window_valid[local_rows, local_cols],
                window_scores[local_rows, local_cols],
                np.nan,
            )
            tile_scores[pixel_mask] = local_scores.astype(np.float32, copy=False)

        rgba = colorize_scores(tile_scores.ravel()).reshape(
            SURFACE_TILE_SIZE_PX,
            SURFACE_TILE_SIZE_PX,
            4,
        )
        payload = encode_png_rgba(rgba)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(payload)
        return payload

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, Any]:
        metric_x, metric_y = TO_TARGET(float(lon), float(lat))
        cell_min_x = aligned_floor(metric_x, CANONICAL_BASE_RESOLUTION_M)
        cell_min_y = aligned_floor(metric_y, CANONICAL_BASE_RESOLUTION_M)
        shard_x = aligned_floor(cell_min_x, SURFACE_SHARD_SIZE_M)
        shard_y = aligned_floor(cell_min_y, SURFACE_SHARD_SIZE_M)
        shard_id = shard_id_for_origin(shard_x, shard_y)
        visible_resolution = None if zoom is None else resolution_for_zoom(zoom)

        payload: dict[str, Any] = {
            "lat": float(lat),
            "lon": float(lon),
            "resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "cell_id": build_cell_id(CANONICAL_BASE_RESOLUTION_M, cell_min_x, cell_min_y),
            "visible_resolution_m": visible_resolution,
            "valid_land": False,
            "effective_area_ratio": 0.0,
            "counts": {},
            "component_scores": {category: 0.0 for category in _CATEGORY_ORDER},
            "total_score": None,
            "park_area_units": 0.0,
        }
        if shard_id not in self.shard_inventory:
            return payload

        shard = self._load_shell_shard(shard_id)
        row_index = int((cell_min_y - shard["y_min_m"]) // CANONICAL_BASE_RESOLUTION_M)
        col_index = int((cell_min_x - shard["x_min_m"]) // CANONICAL_BASE_RESOLUTION_M)
        if row_index < 0 or col_index < 0:
            return payload
        if row_index >= shard["origin_node_idx"].shape[0] or col_index >= shard["origin_node_idx"].shape[1]:
            return payload

        effective_area_ratio = float(shard["effective_area_ratio"][row_index, col_index])
        payload["effective_area_ratio"] = effective_area_ratio
        if not bool(shard["valid_land_mask"][row_index, col_index]):
            return payload

        node_idx = int(shard["origin_node_idx"][row_index, col_index])
        if node_idx < 0:
            return payload

        node_scores = self._load_node_scores()
        counts_row = node_scores["counts_matrix"][node_idx]
        counts = {
            category: int(counts_row[index])
            for index, category in enumerate(node_scores["categories"])
            if int(counts_row[index]) > 0
        }
        weighted_units_row = node_scores["weighted_units_matrix"][node_idx]
        weighted_units = {
            str(category): float(weighted_units_row[index])
            for index, category in enumerate(node_scores["categories"])
            if float(weighted_units_row[index]) > 0.0
        }
        park_area_units = float(weighted_units.get("parks", 0.0))
        component_scores, total_score = score_cell(
            counts,
            effective_area_ratio=effective_area_ratio,
            weighted_units=weighted_units,
        )
        payload.update(
            {
                "valid_land": True,
                "origin_node_idx": node_idx,
                "counts": counts,
                "component_scores": component_scores,
                "total_score": float(total_score),
                "park_area_units": park_area_units,
            }
        )
        return payload


def node_scores_path(score_dir: Path) -> Path:
    return surface_score_node_scores_path(score_dir)


def shard_path(shell_dir: Path, shard_id: str) -> Path:
    return surface_shell_shard_path(shell_dir, shard_id)


def save_shard_payload(shell_dir: Path, shard_entry: ShardEntry, payload: dict[str, Any]) -> None:
    save_shell_shard_payload(shell_dir, shard_entry, payload)


def load_shard_payload(shell_dir: Path, shard_id: str) -> dict[str, Any]:
    return load_shell_shard_payload(shell_dir, shard_id)


__all__ = [
    "ENABLE_FINE_RASTER_SURFACE",
    "FineSurfaceRuntime",
    "ShardEntry",
    "aligned_ceil",
    "aligned_floor",
    "aggregation_factor",
    "build_node_score_arrays",
    "build_shard_payload",
    "build_surface_shell_hash",
    "canonical_cells_per_shard",
    "collect_surface_origin_nodes",
    "colorize_scores",
    "component_scores_for_nodes",
    "encode_png_rgba",
    "ensure_surface_tile_cache_manifest",
    "ensure_surface_score_cache",
    "ensure_surface_shell_cache",
    "iter_shard_entries",
    "load_node_score_arrays",
    "load_score_shard_payload",
    "load_shell_shard_payload",
    "load_shard_payload",
    "load_surface_manifest",
    "node_scores_path",
    "save_node_score_arrays",
    "save_score_shard_payload",
    "save_shell_shard_payload",
    "save_shard_payload",
    "shard_id_for_origin",
    "shard_origin_from_id",
    "shard_path",
    "surface_analysis_ready",
    "surface_manifest_path",
    "surface_score_dir",
    "surface_score_node_scores_path",
    "surface_score_ready",
    "surface_score_shard_path",
    "surface_shell_dir",
    "surface_shell_ready",
    "surface_shell_shard_path",
    "surface_tile_cache_path",
    "surface_tile_dir",
    "weighted_mean_block_reduce",
    "write_surface_manifest",
]
