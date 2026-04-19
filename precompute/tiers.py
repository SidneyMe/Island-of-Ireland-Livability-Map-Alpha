from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import importlib.metadata


GRAPH_REQUIRED_FILENAMES = (
    "walk_graph.meta.json",
    "walk_graph.nodes.bin",
    "walk_graph.edges.bin",
    "walk_graph.osmids.bin",
    "walk_graph.adjacency_offsets.bin",
    "walk_graph.adjacency_targets.bin",
    "walk_graph.adjacency_lengths.bin",
)


def _pkg_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _major(version_str: str) -> int:
    try:
        return int(version_str.split(".")[0])
    except (ValueError, IndexError, AttributeError):
        return -1


def write_tier_manifest(
    tier_dir: Path,
    tier_name: str,
    tier_hash: str,
    status: str,
    last_phase: str = "",
    *,
    manifest_name: str,
    cache_schema_version: int,
    python_version,
    package_snapshot,
    render_hash: str,
) -> None:
    tier_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = tier_dir / manifest_name
    now_utc = datetime.now(timezone.utc).isoformat()

    if status == "building":
        manifest: dict[str, Any] = {
            "status": "building",
            "tier": tier_name,
            "hash": tier_hash,
            "schema_version": cache_schema_version,
            "started_utc": now_utc,
            "last_phase": last_phase,
            "python_version": python_version(),
            "packages": package_snapshot(),
            "render_hash": render_hash,
        }
    else:
        existing: dict[str, Any] = {}
        if manifest_path.exists():
            try:
                with manifest_path.open(encoding="utf-8") as handle:
                    existing = json.load(handle)
            except (json.JSONDecodeError, OSError):
                existing = {}

        artefacts = (
            sorted(item.name for item in tier_dir.iterdir() if item.name != manifest_name)
            if tier_dir.exists()
            else []
        )

        manifest = {
            **existing,
            "status": status,
            "tier": tier_name,
            "hash": tier_hash,
            "schema_version": cache_schema_version,
            "completed_utc": now_utc,
            "last_phase": last_phase,
            "python_version": python_version(),
            "packages": package_snapshot(),
            "artefacts": artefacts,
            "render_hash": render_hash,
        }

    tmp_path = manifest_path.with_name(manifest_path.name + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, default=str)
    os.replace(tmp_path, manifest_path)


def mark_building(
    tier_dir: Path,
    tier_name: str,
    tier_hash: str,
    phase: str,
    *,
    tiers_building: set[Path],
    write_tier_manifest,
) -> None:
    tiers_building.add(tier_dir)
    write_tier_manifest(tier_dir, tier_name, tier_hash, "building", phase)


def mark_complete(
    tier_dir: Path,
    tier_name: str,
    tier_hash: str,
    phase: str,
    *,
    tiers_building: set[Path],
    tier_valid: dict[Path, bool],
    write_tier_manifest,
) -> None:
    if tier_dir in tiers_building:
        write_tier_manifest(tier_dir, tier_name, tier_hash, "complete", phase)
        tier_valid[tier_dir] = True


def can_finalize_geo_tier(
    ireland_geom_metric: Any,
    ireland_geom_wgs84: Any,
    *,
    geo_cache_dir: Path,
    cache_load_for_finalize,
) -> bool:
    graph_dir = geo_cache_dir / "walk_graph"
    return (
        ireland_geom_metric is not None
        and ireland_geom_wgs84 is not None
        and graph_dir.exists()
        and all((graph_dir / name).exists() for name in GRAPH_REQUIRED_FILENAMES)
    )


def can_finalize_reach_tier(
    amenity_data: dict[str, list[tuple[float, float]]] | None,
    *,
    reach_cache_dir: Path,
    cache_load_for_finalize,
    cache_load_large_for_finalize,
) -> bool:
    return (
        amenity_data is not None
        and cache_load_for_finalize("walk_nodes_by_cat", reach_cache_dir) is not None
        and cache_load_large_for_finalize("walk_counts_by_origin_node", reach_cache_dir) is not None
        and cache_load_large_for_finalize("walk_weighted_units_by_origin_node", reach_cache_dir)
        is not None
    )


def _has_recoverable_geo_artefacts(
    geo_cache_dir: Path,
    *,
    cache_load_for_finalize,
) -> bool:
    graph_dir = geo_cache_dir / "walk_graph"
    has_metric = cache_load_for_finalize("study_area_metric", geo_cache_dir) is not None
    has_wgs84 = cache_load_for_finalize("study_area_wgs84", geo_cache_dir) is not None
    if has_metric and has_wgs84:
        return True
    return graph_dir.exists() and all((graph_dir / name).exists() for name in GRAPH_REQUIRED_FILENAMES)


def _has_recoverable_reach_artefacts(
    reach_cache_dir: Path,
    *,
    cache_load_for_finalize,
    cache_load_large_for_finalize,
) -> bool:
    return any(
        (
            cache_load_for_finalize("amenities", reach_cache_dir) is not None,
            cache_load_for_finalize("walk_nodes_by_cat", reach_cache_dir) is not None,
            cache_load_large_for_finalize("walk_counts_by_origin_node", reach_cache_dir) is not None,
            cache_load_large_for_finalize(
                "walk_weighted_units_by_origin_node",
                reach_cache_dir,
            )
            is not None,
        )
    )


def _has_recoverable_score_artefacts(
    score_cache_dir: Path,
    *,
    grid_sizes_m: list[int],
    cache_load_for_finalize,
) -> bool:
    for size in grid_sizes_m:
        if cache_load_for_finalize(f"walk_cells_{size}", score_cache_dir) is not None:
            return True
    if not score_cache_dir.exists():
        return False
    return any(path.name.startswith("walk_origin_nodes__sizes_") for path in score_cache_dir.iterdir())


def validate_tier(
    tier_dir: Path,
    expected_hash: str,
    tier_name: str,
    *,
    force_recompute: bool,
    manifest_name: str,
    cache_schema_version: int,
    recoverable_check=None,
) -> bool:
    if force_recompute:
        return False

    manifest_path = tier_dir / manifest_name
    if not manifest_path.exists():
        return False

    try:
        with manifest_path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  [{tier_name}] manifest unreadable ({exc}) - bypassing cache")
        return False

    cached_schema = manifest.get("schema_version", 0)
    if cached_schema != cache_schema_version:
        print(
            f"  [{tier_name}] schema version mismatch "
            f"(cached={cached_schema}, current={cache_schema_version}) - bypassing cache"
        )
        return False

    if manifest.get("hash") != expected_hash:
        print(f"  [{tier_name}] config hash mismatch - bypassing cache")
        return False

    for package in ("igraph", "shapely", "numpy", "scikit-learn"):
        cached_version = manifest.get("packages", {}).get(package, "unknown")
        current_version = _pkg_version(package)
        if _major(cached_version) != _major(current_version) and _major(cached_version) >= 0:
            print(
                f"  [{tier_name}] {package} major version changed "
                f"(cached={cached_version}, current={current_version}) - bypassing cache"
            )
            return False

    status = manifest.get("status")
    if status == "complete":
        return True
    if status == "building" and recoverable_check is not None and recoverable_check(tier_dir):
        print(f"  [{tier_name}] status='building' but recoverable artefacts were found - reusing cache")
        return True

    print(f"  [{tier_name}] status={status!r} (interrupted run?) - bypassing cache")
    return False


def validate_all_tiers(
    *,
    geo_cache_dir: Path,
    reach_cache_dir: Path,
    score_cache_dir: Path,
    geo_hash: str,
    reach_hash: str,
    score_hash: str,
    force_recompute: bool,
    manifest_name: str,
    cache_schema_version: int,
    cache_load_for_finalize,
    cache_load_large_for_finalize,
    grid_sizes_m: list[int],
    tier_valid: dict[Path, bool],
) -> None:
    geo_ok = validate_tier(
        geo_cache_dir,
        geo_hash,
        "geo",
        force_recompute=force_recompute,
        manifest_name=manifest_name,
        cache_schema_version=cache_schema_version,
        recoverable_check=lambda tier_dir: _has_recoverable_geo_artefacts(
            tier_dir,
            cache_load_for_finalize=cache_load_for_finalize,
        ),
    )

    if not geo_ok:
        reach_ok = False
        score_ok = False
        if reach_cache_dir.exists():
            print("  [reach] bypassed (geo tier invalid)")
        if score_cache_dir.exists():
            print("  [score] bypassed (geo tier invalid)")
    else:
        reach_ok = validate_tier(
            reach_cache_dir,
            reach_hash,
            "reach",
            force_recompute=force_recompute,
            manifest_name=manifest_name,
            cache_schema_version=cache_schema_version,
            recoverable_check=lambda tier_dir: _has_recoverable_reach_artefacts(
                tier_dir,
                cache_load_for_finalize=cache_load_for_finalize,
                cache_load_large_for_finalize=cache_load_large_for_finalize,
            ),
        )
        if not reach_ok:
            score_ok = False
            if score_cache_dir.exists():
                print("  [score] bypassed (reach tier invalid)")
        else:
            score_ok = validate_tier(
                score_cache_dir,
                score_hash,
                "score",
                force_recompute=force_recompute,
                manifest_name=manifest_name,
                cache_schema_version=cache_schema_version,
                recoverable_check=lambda tier_dir: _has_recoverable_score_artefacts(
                    tier_dir,
                    grid_sizes_m=grid_sizes_m,
                    cache_load_for_finalize=cache_load_for_finalize,
                ),
            )

    tier_valid[geo_cache_dir] = geo_ok
    tier_valid[reach_cache_dir] = reach_ok
    tier_valid[score_cache_dir] = score_ok


def print_cache_status(
    *,
    cache_dir: Path,
    geo_cache_dir: Path,
    reach_cache_dir: Path,
    score_cache_dir: Path,
    geo_hash: str,
    reach_hash: str,
    score_hash: str,
    render_hash: str,
    manifest_name: str,
) -> None:
    tiers = [
        ("geo", geo_cache_dir, geo_hash),
        ("reach", reach_cache_dir, reach_hash),
        ("score", score_cache_dir, score_hash),
    ]

    for tier_name, tier_dir, _ in tiers:
        manifest_path = tier_dir / manifest_name
        if manifest_path.exists():
            try:
                with manifest_path.open(encoding="utf-8") as handle:
                    manifest = json.load(handle)
                status = manifest.get("status", "?")
                timestamp = manifest.get("completed_utc") or manifest.get("started_utc", "?")
                artefact_count = len(manifest.get("artefacts", []))
                print(
                    f"  {tier_name:5s}  {tier_dir.name}  "
                    f"status={status}  artefacts={artefact_count}  ts={timestamp}"
                )
            except (json.JSONDecodeError, OSError):
                print(f"  {tier_name:5s}  {tier_dir.name}  (manifest unreadable)")
        else:
            print(f"  {tier_name:5s}  {tier_dir.name}  (no manifest)")

    print(f"  render_hash = {render_hash}  (manifest-only; no directory)")

    if cache_dir.exists():
        current_dirs = {geo_cache_dir.name, reach_cache_dir.name, score_cache_dir.name}
        # Never touch the shared/persistent subdirs — they are keyed independently.
        protected_dirs = {"geo_shared", "exports"}
        prune_enabled = (
            os.getenv("LIVABILITY_KEEP_STALE_CACHES", "0").strip().lower()
            in {"0", "false", "no", "off", ""}
        )
        # Keep dirs prefixed with known tier types; only those are fair game to prune.
        tier_prefixes = ("geo_", "reach_", "score_", "surface_shell_", "surface_scores_", "surface_tiles_")
        stale_dirs = [
            item
            for item in cache_dir.iterdir()
            if item.is_dir()
            and item.name not in current_dirs
            and item.name not in protected_dirs
            and item.name.startswith(tier_prefixes)
        ]
        if stale_dirs:
            action_label = "pruning" if prune_enabled else "safe to delete manually"
            print(
                f"  NOTE: {len(stale_dirs)} stale cache dir(s) from previous configs - {action_label}:"
            )
            for stale_dir in sorted(stale_dirs):
                print(f"    {cache_dir / stale_dir.name}")
            if prune_enabled:
                removed = 0
                for stale_dir in stale_dirs:
                    try:
                        shutil.rmtree(stale_dir)
                        removed += 1
                    except OSError as exc:
                        print(f"    [skip] could not remove {stale_dir}: {exc}")
                if removed:
                    print(f"  pruned {removed} stale cache dir(s)")
