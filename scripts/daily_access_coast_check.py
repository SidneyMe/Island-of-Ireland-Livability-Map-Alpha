"""Coastal connector check for a built daily access (v4) profile.

Re-runs the native entry search of every land-band cell (origin within 250 m
of the mask boundary) without the land check and compares the edge endpoint
nodes it reaches with the stored shell: cells that lose nodes (or the whole
network) to the check would otherwise have been connected across water
(estuaries, harbours). Prints counts per status plus sample locations (WGS84)
for manual review.

    python scripts/daily_access_coast_check.py --profile test
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from config import CACHE_DIR, CANONICAL_BASE_RESOLUTION_M, TO_WGS84
from precompute.daily_access_build import (
    DAILY_ACCESS_CACHE_DIR,
    load_score_shard,
    load_shell_shard,
)
from precompute.daily_access_native import (
    CELL_DTYPE,
    CELL_FLAG_INTERIOR,
    CELL_FLAG_ON_LAND,
    native_attach,
    write_mask_geojson,
)
from precompute.daily_access_snap import LandMask
from study_area import load_island_geometry_metric


def _latest(prefix: str) -> Path:
    candidates = sorted(DAILY_ACCESS_CACHE_DIR.glob(f"{prefix}_*/manifest.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise SystemExit(f"No complete {prefix} tier under {DAILY_ACCESS_CACHE_DIR}")
    return candidates[-1].parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--shell-dir", type=Path, default=None)
    parser.add_argument("--score-dir", type=Path, default=None)
    parser.add_argument("--graph-dir", type=Path, default=None)
    parser.add_argument("--samples", type=int, default=15)
    args = parser.parse_args(argv)
    shell_dir = args.shell_dir or _latest("shell")
    score_dir = args.score_dir or _latest("score")
    graph_dir = args.graph_dir or sorted(
        DAILY_ACCESS_CACHE_DIR.glob("graph_*/walk_graph/walk_graph.meta.json"), key=lambda p: p.stat().st_mtime
    )[-1].parent
    shard_ids = json.loads((shell_dir / "manifest.json").read_text(encoding="utf-8"))["shards"]
    land_mask = LandMask(load_island_geometry_metric())
    step = float(CANONICAL_BASE_RESOLUTION_M)

    report = {"band_cells": 0, "cells_losing_nodes": 0, "cells_left_without_network": 0,
              "status_of_band_cells": {}, "samples": []}
    with tempfile.TemporaryDirectory(dir=CACHE_DIR) as temp:
        temp_dir = Path(temp)
        mask_path = temp_dir / "mask.geojson"
        write_mask_geojson(land_mask, mask_path)
        for shard_id in shard_ids:
            shell = load_shell_shard(shell_dir, shard_id)
            score = load_score_shard(score_dir, shard_id)
            rows, cols = shell["valid_land_mask"].shape
            x0, y0 = int(shell["x_min_m"][0]), int(shell["y_min_m"][0])
            flags = shell["origin_flags"].ravel()
            band = np.flatnonzero((flags & CELL_FLAG_ON_LAND != 0) & (flags & CELL_FLAG_INTERIOR == 0))
            if not len(band):
                continue
            records = np.zeros(len(band), dtype=CELL_DTYPE)
            records["x"] = shell["origin_x"].ravel()[band]
            records["y"] = shell["origin_y"].ravel()[band]
            records["flags"] = flags[band]
            groups = np.arange(len(band), dtype=np.uint32)
            checked = native_attach(graph_dir, mask_path, records, temp_dir, groups=groups)
            unchecked = records.copy()
            unchecked["flags"] |= CELL_FLAG_INTERIOR
            loose = native_attach(graph_dir, mask_path, unchecked, temp_dir, groups=groups)
            stored_counts = np.diff(checked["group_offsets"].astype(np.int64))
            loose_counts = np.diff(loose["group_offsets"].astype(np.int64))
            lost = np.flatnonzero(loose_counts > stored_counts)
            status = score["network_status"].ravel()[band]
            report["band_cells"] += int(len(band))
            report["cells_losing_nodes"] += int(len(lost))
            report["cells_left_without_network"] += int(np.count_nonzero(stored_counts[lost] == 0))
            for code, count in zip(*np.unique(status, return_counts=True)):
                key = str(int(code))
                report["status_of_band_cells"][key] = report["status_of_band_cells"].get(key, 0) + int(count)
            for index in lost[: max(args.samples - len(report["samples"]), 0)]:
                lon, lat = TO_WGS84(float(records["x"][index]), float(records["y"][index]))
                report["samples"].append(
                    {"lat": round(lat, 6), "lon": round(lon, 6), "kept_nodes": int(stored_counts[index]),
                     "without_land_check": int(loose_counts[index])}
                )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
