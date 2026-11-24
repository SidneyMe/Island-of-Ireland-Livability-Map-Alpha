"""Check built daily access (v4) score shards against the Python reference.

Samples valid cells from one published build, recomputes them with
``compute_reference`` on the same graph, pinned OSM/GTFS/Overture inputs, green approaches and
cell-origin penalties, and compares the stored codes within the Python/Rust
tolerances (distances one 0.1 m step, access 0.0005, components 0.02,
total 0.03). Also checks that the reach cache's logical ids are the
reference's.

    python scripts/daily_access_verify_shards.py --profile test [--build-key KEY] [--sample 2000] [--points lat,lon ...]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shapely.ops import transform

from config import (
    CANONICAL_BASE_RESOLUTION_M,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_REACH_BUFFER_M,
    SURFACE_SHARD_SIZE_M,
    TO_TARGET,
    TO_WGS84,
)
from db_postgis import load_railway_corridor_rows, load_road_rows
from db_postgis import build_engine
from db_postgis.daily_access_green import stage_green_sites, stage_park_approaches
from precompute.daily_access_build import (
    CellPenalties,
    load_label_table,
    load_score_shard,
    load_v4_objects,
)
from precompute.daily_access_native import PARITY_LIMITS
from precompute.daily_access_reference import NETWORK_STATUS_CODES, compute_reference, load_reference_graph
from precompute.daily_access_score import CATEGORIES
from precompute.daily_access_verify import (
    SampledCell,
    load_verification_snapshot,
    sample_valid_cells,
    verify_snapshot_tiers,
)
from precompute.surface import aligned_floor, shard_id_for_origin


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", default="test")
    parser.add_argument("--build-key", help="published v4 build to verify (default: latest complete build for profile)")
    parser.add_argument("--sample", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--points", nargs="*", default=(), help="extra control points as lat,lon")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    snapshot = load_verification_snapshot(build_engine(), profile=args.profile, build_key=args.build_key)
    problems = verify_snapshot_tiers(snapshot)
    if problems:
        print(json.dumps({"build_key": snapshot.build_key, "problems": problems[:20]}, indent=2))
        return 2
    shell_manifest = json.loads((snapshot.shell_dir / "manifest.json").read_text(encoding="utf-8"))
    shard_ids = shell_manifest["shards"]
    step = float(CANONICAL_BASE_RESOLUTION_M)

    picked, valid_cells = sample_valid_cells(
        snapshot.shell_dir, shard_ids, sample_size=args.sample, seed=args.seed, step_m=step,
    )
    for text in args.points:
        lat, lon = (float(value) for value in text.split(","))
        x, y = TO_TARGET(lon, lat)
        cell_x, cell_y = aligned_floor(x, step), aligned_floor(y, step)
        shard_x, shard_y = aligned_floor(cell_x, SURFACE_SHARD_SIZE_M), aligned_floor(cell_y, SURFACE_SHARD_SIZE_M)
        shard_id = shard_id_for_origin(shard_x, shard_y)
        if shard_id in shard_ids:
            picked.append(SampledCell(
                shard_id, int((cell_y - shard_y) // step), int((cell_x - shard_x) // step), cell_x, cell_y, text,
            ))

    graph = load_reference_graph(snapshot.graph_dir)
    engine = snapshot.engine
    reach_metric = snapshot.study_area_metric.envelope.buffer(DAILY_ACCESS_REACH_BUFFER_M).envelope
    reach_area_wgs84 = transform(lambda x, y, z=None: TO_WGS84(x, y), reach_metric)
    places, overture_green, stops, _ = load_v4_objects(
        engine,
        import_fingerprint=snapshot.import_fingerprint,
        reality_fingerprint=snapshot.reality_fingerprint,
        reach_area_wgs84=reach_area_wgs84,
    )
    with engine.begin() as connection:
        green_sites, _ = stage_green_sites(
            connection, import_fingerprint=snapshot.import_fingerprint,
            study_area_wgs84=reach_area_wgs84, overture_green_rows=overture_green, buffer_m=0.0,
        )
        approaches, intervals, _ = stage_park_approaches(
            connection, green_sites, node_x=graph.node_x, node_y=graph.node_y, offsets=graph.offsets,
            targets=graph.targets, lengths=graph.lengths, edge_flags=graph.edge_flags,
        )
    penalty_fn = CellPenalties(
        load_road_rows(engine, snapshot.import_fingerprint),
        load_railway_corridor_rows(engine, snapshot.reality_fingerprint),
    )
    reference = compute_reference(
        graph=graph, land_mask=snapshot.land_mask, cells=[(s.x_min_m, s.y_min_m) for s in picked], cell_size_m=step,
        places=places, green_sites=green_sites, park_approaches=approaches, park_intervals=intervals,
        stops=stops, penalty_fn=penalty_fn,
    )

    scores = {
        shard_id: load_score_shard(snapshot.score_dir, shard_id)
        for shard_id in {sample.shard_id for sample in picked}
    }
    limits = {"total": PARITY_LIMITS["total"] * 100, "components": PARITY_LIMITS["component"] * 100,
              "service_access": PARITY_LIMITS["access"] * 10_000}
    mismatches = {"status": 0, "total": 0, "components": 0, "penalties": 0, "service_access": 0}
    max_diff = {"total": 0.0, "components": 0.0, "service_access": 0.0}
    examples, controls = [], []
    for sample, cell in zip(picked, reference.cells):
        shard_id, row, col, label = sample.shard_id, sample.row, sample.col, sample.label
        stored = scores[shard_id]
        diffs = {
            "total": abs(int(stored["total"][row, col]) - cell.quantized["total"]),
            "components": max(
                abs(int(stored["components"][i, row, col]) - cell.quantized["components"][name])
                for i, name in enumerate(CATEGORIES)
            ),
            "service_access": max(
                abs(int(stored["service_access"][i, row, col]) - cell.quantized["service_access"][need])
                for i, need in enumerate(DAILY_ACCESS_NEEDS)
            ),
        }
        checks = {
            "status": stored["network_status"][row, col] == NETWORK_STATUS_CODES[cell.network_status],
            "penalties": all(
                stored["penalties"][i, row, col] == cell.quantized["penalties"][name]
                for i, name in enumerate(("railway_proximity", "road_proximity"))
            ),
            **{name: diffs[name] <= limits[name] + 1e-9 for name in diffs},
        }
        for name in diffs:
            max_diff[name] = max(max_diff[name], diffs[name])
        for name, ok in checks.items():
            if not ok:
                mismatches[name] += 1
        if not all(checks.values()) and len(examples) < 5:
            examples.append({"shard": shard_id, "row": row, "col": col, "failed": [k for k, v in checks.items() if not v]})
        if label is not None:
            controls.append({"point": label, "status": cell.network_status, "total": cell.quantized["total"] / 100.0,
                             "components": {k: v / 100.0 for k, v in cell.quantized["components"].items()},
                             "stored_total": int(stored["total"][row, col]) / 100.0})
    identity_ok = load_label_table(snapshot.reach_dir) == reference.labels.as_json()
    penalised = sum(1 for cell in reference.cells if any(v > 0 for v in cell.score.penalties.values()))
    print(json.dumps({
        "build_key": snapshot.build_key,
        "profile": snapshot.profile,
        "input_snapshot": {
            "import_fingerprint": snapshot.import_fingerprint,
            "transit_reality_fingerprint": snapshot.reality_fingerprint,
            "overture_dataset_signature": snapshot.overture_dataset_signature,
            "keys": snapshot.keys,
        },
        "valid_cells": valid_cells,
        "shards": len(shard_ids),
        "sampled_cells": len(picked),
        "cells_with_penalties": penalised,
        "mismatches": mismatches,
        "max_diff_points": {"total": max_diff["total"] / 100.0, "components": max_diff["components"] / 100.0,
                            "service_access": max_diff["service_access"] / 10_000.0},
        "label_table_matches_reference": identity_ok,
        "controls": controls,
        "examples": examples,
        "seconds": round(time.perf_counter() - started, 1),
    }, indent=2))
    return 0 if not any(mismatches.values()) and identity_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
