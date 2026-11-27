"""Read-only candidate report before publishing v4 context effects.

geo_env.cmd python scripts/daily_access_context_report.py --shell-dir PATH --score-dir PATH

The baseline removes ONLY the new context effects from this candidate's
components. It is a counterfactual, not a comparison of two different imports.
Histograms and shard-at-a-time reads bound memory on full-island builds.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
from config import BASE_DIR, TO_TARGET, SURFACE_SHARD_SIZE_M
from precompute.daily_access_build import load_score_shard, load_shell_shard, surface_tier_problems
from precompute.daily_access_score import PENALTIES, BONUSES


def report(shell_dir: Path, score_dir: Path, fixture: Path) -> dict:
    problems, shards = surface_tier_problems(shell_dir, score_dir)
    if problems:
        raise ValueError(f"Invalid candidate: {problems[:5]}")
    locations = json.loads(fixture.read_text(encoding="utf-8"))["locations"]
    points_by_shard = {}
    for location in locations:
        x, y = TO_TARGET(location["lon"], location["lat"])
        sx, sy = (int(np.floor(value / SURFACE_SHARD_SIZE_M) * SURFACE_SHARD_SIZE_M) for value in (x, y))
        points_by_shard.setdefault(f"{sx}_{sy}", []).append((location, int((y-sy)//50), int((x-sx)//50)))
    histogram = np.zeros(20_001, dtype=np.int64)
    count, before_sum, after_sum = 0, 0.0, 0.0
    fixture_rows = []
    for shard in shards:
        score = load_score_shard(score_dir, shard)
        shell = load_shell_shard(shell_dir, shard)
        valid = shell["valid_land_mask"] & (score["total"] != 65535)
        before = np.clip(score["components"].astype(np.float64).sum(axis=0) - score["penalties"][:2].astype(np.float64).sum(axis=0), 0, 10_000) / 100
        after = score["total"].astype(np.float64) / 100
        delta = np.rint((after[valid] - before[valid]) * 100).astype(np.int32)
        histogram += np.bincount(np.clip(delta + 10_000, 0, 20_000), minlength=20_001)
        count += int(valid.sum())
        before_sum += float(before[valid].sum())
        after_sum += float(after[valid].sum())
        for location, row, col in points_by_shard.get(shard, []):
            if not valid[row, col]:
                continue
            fixture_rows.append({"name": location.get("name", location.get("id")),
                "before_context": round(float(before[row, col]), 2), "after": round(float(after[row, col]), 2),
                "delta": round(float(after[row, col] - before[row, col]), 2),
                "penalties": {name: int(score["penalties"][i, row, col])/100 for i, name in enumerate(PENALTIES)},
                "bonuses": {name: int(score["bonuses"][i, row, col])/100 for i, name in enumerate(BONUSES)}})
    cumulative = np.cumsum(histogram)
    return {"baseline": "same-input v4 without new context effects", "valid_cells": count,
            "mean_before": before_sum/count if count else None, "mean_after": after_sum/count if count else None,
            "delta_percentiles": {str(p): (int(np.searchsorted(cumulative, max(1, int(np.ceil(count*p/100))))) - 10_000)/100 if count else None for p in (0, 10, 50, 90, 100)},
            "fixture": fixture_rows, "fixture_outside_candidate_or_without_land": len(locations)-len(fixture_rows)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell-dir", type=Path, required=True)
    parser.add_argument("--score-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, default=BASE_DIR / "fixtures" / "livability_sanity.v1.json")
    args = parser.parse_args()
    print(json.dumps(report(args.shell_dir, args.score_dir, args.fixture), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
