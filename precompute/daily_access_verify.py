"""Pinned-build verification helpers for daily-access-v4 artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from config import DAILY_ACCESS_SCORE_MODEL, normalize_build_profile
from db_postgis import (
    has_complete_import_manifest,
    has_complete_transit_reality_manifest,
    load_build_manifest,
    load_latest_scoring_build_manifest,
)
from study_area import load_island_geometry_metric, load_study_area_geometries

from .daily_access_build import (
    DAILY_ACCESS_CACHE_DIR,
    DailyAccessBuildError,
    LandMask,
    load_shell_shard,
    mask_identity,
    reach_tier_problems,
    surface_tier_problems,
)


@dataclass(frozen=True)
class VerificationSnapshot:
    engine: Any
    build_key: str
    profile: str
    import_fingerprint: str
    reality_fingerprint: str
    overture_dataset_signature: str
    graph_dir: Path
    shell_dir: Path
    reach_dir: Path
    score_dir: Path
    keys: dict[str, str]
    study_area_metric: Any
    study_area_wgs84: Any
    land_mask: LandMask


@dataclass(frozen=True)
class SampledCell:
    shard_id: str
    row: int
    col: int
    x_min_m: float
    y_min_m: float
    label: str | None = None


def _read_manifest(directory: Path) -> dict[str, Any]:
    path = directory / "manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DailyAccessBuildError(f"Cannot read v4 tier manifest {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DailyAccessBuildError(f"Invalid v4 tier manifest {path}")
    return value


def load_verification_snapshot(engine, *, profile: str, build_key: str | None = None) -> VerificationSnapshot:
    """Resolve one published build without reading GTFS zips or refreshing reality."""
    profile = normalize_build_profile(profile)
    manifest = (
        load_build_manifest(engine, build_key)
        if build_key
        else load_latest_scoring_build_manifest(
            engine, scoring_model=DAILY_ACCESS_SCORE_MODEL, profile=profile,
        )
    )
    if manifest is None or manifest.get("status") != "complete":
        target = f"build {build_key}" if build_key else f"latest {profile} {DAILY_ACCESS_SCORE_MODEL} build"
        raise DailyAccessBuildError(f"No complete published {target} was found")

    summary = manifest.get("summary_json") or {}
    if summary.get("scoring_model") != DAILY_ACCESS_SCORE_MODEL:
        raise DailyAccessBuildError(f"Build {manifest['build_key']} is not {DAILY_ACCESS_SCORE_MODEL}")
    if normalize_build_profile(summary.get("build_profile")) != profile:
        raise DailyAccessBuildError(
            f"Build {manifest['build_key']} has profile {summary.get('build_profile')!r}, expected {profile!r}"
        )
    info = summary.get("daily_access") or {}
    keys = info.get("keys") or {}
    missing_keys = [name for name in ("graph", "shell", "reach", "score") if not keys.get(name)]
    if missing_keys:
        raise DailyAccessBuildError(f"Build {manifest['build_key']} lacks v4 keys {missing_keys}")

    required_info = (
        "shell_dir", "reach_dir", "score_dir", "import_fingerprint",
        "transit_reality_fingerprint", "overture_dataset_signature",
    )
    missing_info = [name for name in required_info if not info.get(name)]
    if missing_info:
        raise DailyAccessBuildError(
            f"Build {manifest['build_key']} lacks pinned verification identity {missing_info}; republish it with the current code"
        )
    shell_dir = Path(info["shell_dir"])
    reach_dir = Path(info["reach_dir"])
    score_dir = Path(info["score_dir"])
    graph_dir = Path(info.get("graph_dir") or DAILY_ACCESS_CACHE_DIR / f"graph_{keys['graph']}" / "walk_graph")
    import_fingerprint = str(info["import_fingerprint"])
    reality_fingerprint = str(info["transit_reality_fingerprint"])
    overture_signature = str(info["overture_dataset_signature"])

    tier_manifests = {
        "shell": _read_manifest(shell_dir),
        "reach": _read_manifest(reach_dir),
        "score": _read_manifest(score_dir),
    }
    identity_problems: list[str] = []
    for name, tier_manifest in tier_manifests.items():
        if tier_manifest.get("key") != keys[name]:
            identity_problems.append(f"{name} key mismatch")
        identity_fields = {"profile": profile}
        if name != "shell":
            identity_fields.update({
                "import_fingerprint": import_fingerprint,
                "transit_reality_fingerprint": reality_fingerprint,
            })
        for field, expected in identity_fields.items():
            if tier_manifest.get(field) != expected:
                identity_problems.append(f"{name} {field} mismatch")
    if tier_manifests["score"].get("shell_key") != keys["shell"]:
        identity_problems.append("score shell link mismatch")
    if tier_manifests["score"].get("reach_key") != keys["reach"]:
        identity_problems.append("score reach link mismatch")
    if str(manifest.get("import_fingerprint")) != import_fingerprint:
        identity_problems.append("published import fingerprint mismatch")
    if identity_problems:
        raise DailyAccessBuildError(
            f"Build {manifest['build_key']} does not describe one immutable v4 snapshot: {identity_problems}"
        )
    if not has_complete_import_manifest(engine, import_fingerprint):
        raise DailyAccessBuildError(f"Pinned OSM import {import_fingerprint} is unavailable or incomplete")
    if not has_complete_transit_reality_manifest(
        engine, reality_fingerprint, import_fingerprint=import_fingerprint,
    ):
        raise DailyAccessBuildError(
            f"Pinned transit reality {reality_fingerprint} is unavailable, incomplete, or belongs to another import"
        )
    from overture.loader import dataset_signature

    current_overture_signature = dataset_signature()
    if current_overture_signature != overture_signature:
        raise DailyAccessBuildError(
            "Pinned Overture input is not available: "
            f"build has {overture_signature}, local dataset has {current_overture_signature}"
        )

    study_area_metric, study_area_wgs84 = load_study_area_geometries(profile=profile)
    land_mask = LandMask(load_island_geometry_metric())
    expected_mask = tier_manifests["shell"].get("mask_identity")
    if expected_mask != mask_identity(land_mask):
        raise DailyAccessBuildError("Current main-island mask does not match the pinned shell snapshot")
    return VerificationSnapshot(
        engine=engine,
        build_key=str(manifest["build_key"]),
        profile=profile,
        import_fingerprint=import_fingerprint,
        reality_fingerprint=reality_fingerprint,
        overture_dataset_signature=overture_signature,
        graph_dir=graph_dir,
        shell_dir=shell_dir,
        reach_dir=reach_dir,
        score_dir=score_dir,
        keys={name: str(keys[name]) for name in ("graph", "shell", "reach", "score")},
        study_area_metric=study_area_metric,
        study_area_wgs84=study_area_wgs84,
        land_mask=land_mask,
    )


def verify_snapshot_tiers(snapshot: VerificationSnapshot) -> list[str]:
    problems, shard_ids = surface_tier_problems(
        snapshot.shell_dir,
        snapshot.score_dir,
        shell_key=snapshot.keys["shell"],
        reach_key=snapshot.keys["reach"],
        score_key=snapshot.keys["score"],
    )
    problems.extend(
        f"reach: {problem}"
        for problem in reach_tier_problems(snapshot.reach_dir, key=snapshot.keys["reach"])
    )
    if not snapshot.graph_dir.is_dir():
        problems.append(f"graph: directory is missing: {snapshot.graph_dir}")
    return problems


def _sample_quotas(counts: np.ndarray, target: int, rng: np.random.Generator) -> np.ndarray:
    quotas = np.zeros(len(counts), dtype=np.int64)
    nonempty = np.flatnonzero(counts > 0)
    if target <= 0 or len(nonempty) == 0:
        return quotas
    if target < len(nonempty):
        probabilities = counts[nonempty] / counts[nonempty].sum()
        picked = rng.choice(nonempty, size=target, replace=False, p=probabilities)
        quotas[picked] = 1
        return quotas

    quotas[nonempty] = 1
    remaining = target - len(nonempty)
    capacity = counts - quotas
    while remaining:
        available = np.flatnonzero(capacity > 0)
        draw = min(remaining, int(capacity[available].sum()))
        if not draw:
            break
        exact = draw * capacity[available] / capacity[available].sum()
        assigned = np.minimum(np.floor(exact).astype(np.int64), capacity[available])
        quotas[available] += assigned
        capacity[available] -= assigned
        remaining -= int(assigned.sum())
        if remaining:
            available = np.flatnonzero(capacity > 0)
            order = available[np.argsort(rng.random(len(available)))]
            take = order[: min(remaining, len(order))]
            quotas[take] += 1
            capacity[take] -= 1
            remaining -= len(take)
    return quotas


def sample_valid_cells(
    shell_dir: Path,
    shard_ids: Iterable[str],
    *,
    sample_size: int,
    seed: int,
    step_m: float,
) -> tuple[list[SampledCell], int]:
    """Deterministically sample land cells with O(shards + sample) memory.

    At least one cell is selected from every non-empty shard when the requested
    sample is large enough.  Only one shard's flat valid-index array exists at
    a time; national cell tuples are never accumulated.
    """
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    ordered = list(shard_ids)
    counts = np.asarray(
        [int(np.count_nonzero(load_shell_shard(shell_dir, shard_id)["valid_land_mask"])) for shard_id in ordered],
        dtype=np.int64,
    )
    total = int(counts.sum())
    target = min(int(sample_size), total)
    rng = np.random.default_rng(seed)
    quotas = _sample_quotas(counts, target, rng)
    result: list[SampledCell] = []
    for index, shard_id in enumerate(ordered):
        quota = int(quotas[index])
        if not quota:
            continue
        shell = load_shell_shard(shell_dir, shard_id)
        valid = np.flatnonzero(shell["valid_land_mask"].ravel())
        picked = np.sort(rng.choice(valid, size=quota, replace=False))
        cols = int(shell["valid_land_mask"].shape[1])
        x0, y0 = int(shell["x_min_m"][0]), int(shell["y_min_m"][0])
        for flat in picked:
            row, col = divmod(int(flat), cols)
            result.append(SampledCell(shard_id, row, col, x0 + col * step_m, y0 + row * step_m))
    return result, total


__all__ = [
    "SampledCell",
    "VerificationSnapshot",
    "load_verification_snapshot",
    "sample_valid_cells",
    "verify_snapshot_tiers",
]
