from __future__ import annotations

from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    CACHE_SCHEMA_VERSION,
    FORCE_RECOMPUTE,
    MANIFEST_NAME,
    package_snapshot,
    python_version,
)

from . import surface as _surface
from . import tiers as _tiers
from ._cache_wrappers import _cache_load_for_finalize, _cache_load_large_for_finalize
from ._state import _STATE


def _write_tier_manifest(
    tier_dir: Path,
    tier_name: str,
    tier_hash: str,
    status: str,
    last_phase: str = "",
) -> None:
    _tiers.write_tier_manifest(
        tier_dir,
        tier_name,
        tier_hash,
        status,
        last_phase,
        manifest_name=MANIFEST_NAME,
        cache_schema_version=CACHE_SCHEMA_VERSION,
        python_version=python_version,
        package_snapshot=package_snapshot,
        render_hash=_STATE.hashes.render_hash,
    )


def _mark_building(tier_dir: Path, tier_name: str, tier_hash: str, phase: str) -> None:
    _tiers.mark_building(
        tier_dir,
        tier_name,
        tier_hash,
        phase,
        tiers_building=_STATE.tiers_building,
        write_tier_manifest=_write_tier_manifest,
    )


def _mark_complete(tier_dir: Path, tier_name: str, tier_hash: str, phase: str) -> None:
    _tiers.mark_complete(
        tier_dir,
        tier_name,
        tier_hash,
        phase,
        tiers_building=_STATE.tiers_building,
        tier_valid=_STATE.tier_valid,
        write_tier_manifest=_write_tier_manifest,
    )


def _can_finalize_geo_tier(study_area_metric: Any, study_area_wgs84: Any) -> bool:
    return _tiers.can_finalize_geo_tier(
        study_area_metric,
        study_area_wgs84,
        geo_cache_dir=_STATE.geo_cache_dir,
        cache_load_for_finalize=_cache_load_for_finalize,
    )


def _can_finalize_reach_tier(
    amenity_data: dict[str, list[tuple[float, float]]] | None,
) -> bool:
    return _tiers.can_finalize_reach_tier(
        amenity_data,
        reach_cache_dir=_STATE.reach_cache_dir,
        cache_load_for_finalize=_cache_load_for_finalize,
        cache_load_large_for_finalize=_cache_load_large_for_finalize,
    )


def _surface_analysis_ready() -> bool:
    return _surface.surface_analysis_ready(
        _STATE.surface_shell_dir,
        _STATE.surface_score_dir,
        expected_surface_shell_hash=_STATE.hashes.surface_shell_hash,
        expected_score_hash=_STATE.hashes.score_hash,
    )


def validate_all_tiers() -> None:
    _tiers.validate_all_tiers(
        geo_cache_dir=_STATE.geo_cache_dir,
        reach_cache_dir=_STATE.reach_cache_dir,
        score_cache_dir=_STATE.score_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        reach_hash=_STATE.hashes.reach_hash,
        score_hash=_STATE.hashes.score_hash,
        force_recompute=FORCE_RECOMPUTE,
        manifest_name=MANIFEST_NAME,
        cache_schema_version=CACHE_SCHEMA_VERSION,
        cache_load_for_finalize=_cache_load_for_finalize,
        cache_load_large_for_finalize=_cache_load_large_for_finalize,
        grid_sizes_m=list(_STATE.settings.grid_sizes_m),
        tier_valid=_STATE.tier_valid,
    )


def print_cache_status() -> None:
    _tiers.print_cache_status(
        cache_dir=CACHE_DIR,
        geo_cache_dir=_STATE.geo_cache_dir,
        reach_cache_dir=_STATE.reach_cache_dir,
        score_cache_dir=_STATE.score_cache_dir,
        geo_hash=_STATE.hashes.geo_hash,
        reach_hash=_STATE.hashes.reach_hash,
        score_hash=_STATE.hashes.score_hash,
        render_hash=_STATE.hashes.render_hash,
        manifest_name=MANIFEST_NAME,
    )
    for label, directory in (
        ("surface_shell", _STATE.surface_shell_dir),
        ("surface_scores", _STATE.surface_score_dir),
        ("surface_tiles", _STATE.surface_tile_dir),
    ):
        manifest = _surface.load_surface_manifest(directory)
        if manifest is None:
            print(f"  {label}  {directory.name}  (no manifest)")
            continue
        shard_inventory = manifest.get("shard_inventory", [])
        completed_shards = manifest.get("completed_shards")
        total_shards = manifest.get("total_shards")
        shard_progress = None
        if isinstance(total_shards, int) and total_shards >= 0:
            completed_value = (
                int(completed_shards)
                if isinstance(completed_shards, int) and completed_shards >= 0
                else 0
            )
            shard_progress = f"{completed_value}/{int(total_shards)}"
        elif isinstance(shard_inventory, list):
            shard_progress = str(len(shard_inventory))
            if label == "surface_shell" and manifest.get("status") == "building":
                shard_dir = directory / "shards"
                existing_files = len(list(shard_dir.glob("*.npz"))) if shard_dir.exists() else 0
                if existing_files > 0:
                    shard_progress = str(existing_files)
        print(
            f"  {label}  {directory.name}  "
            f"status={manifest.get('status', '?')}  "
            f"shards={shard_progress if shard_progress is not None else 0}"
        )
