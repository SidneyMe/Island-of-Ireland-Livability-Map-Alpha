"""Run the daily access (v4) Python reference on a small real area.

Read-only against the DB (temp tables only). Loads the walk graph, the land
mask, classified places, green sites with PostGIS approaches and GTFS stops
for the bbox plus the 2 km reach buffer, then scores the 50 m cells inside
the bbox. Prints a JSON summary; ``--out`` also saves per-cell arrays (the
Python side of the Python/Rust comparison).

    python scripts/daily_access_reference_run.py [--bbox minlon minlat maxlon maxlat] [--out cells.npz]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
from shapely.geometry import box
from shapely.ops import transform

from config import (
    CACHE_DIR,
    CANONICAL_BASE_RESOLUTION_M,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_REACH_BUFFER_M,
    TO_TARGET,
    TO_WGS84,
)
from db_postgis import build_engine, load_railway_corridor_rows, load_road_rows
from db_postgis._dependencies import text
from db_postgis.daily_access_green import stage_green_sites, stage_park_approaches
from precompute.daily_access_build import CellPenalties, load_v4_objects
from precompute.daily_access_reference import (
    NETWORK_STATUS_CODES,
    compute_reference,
    load_reference_graph,
)
from precompute.daily_access_native import compare_with_reference, compute_native
from precompute.daily_access_snap import LandMask
from study_area import load_island_geometry_metric

# Cork city centre, ~2 x 1.7 km.
DEFAULT_BBOX_WGS84 = (-8.49, 51.893, -8.46, 51.908)


def _latest(engine, sql: str) -> str:
    with engine.connect() as connection:
        value = connection.execute(text(sql)).scalar()
    if not value:
        raise SystemExit(f"No result for: {sql}")
    return str(value)


def _newest_walk_graph_dir() -> Path:
    candidates = sorted(
        CACHE_DIR.glob("geo_*/walk_graph/walk_graph.meta.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise SystemExit("No walk graph found under .livability_cache/geo_*/walk_graph")
    return candidates[0].parent


def run(
    bbox_wgs84: tuple[float, float, float, float],
    graph_dir: Path,
    *,
    use_overture: bool,
    native: bool = False,
) -> tuple[dict[str, Any], Any]:
    timings: dict[str, float] = {}
    started = time.perf_counter()

    def lap(name: str) -> None:
        nonlocal started
        now = time.perf_counter()
        timings[name] = round(now - started, 2)
        started = now

    engine = build_engine()
    import_fingerprint = _latest(
        engine,
        "SELECT import_fingerprint FROM osm_raw.import_manifest WHERE status = 'complete' "
        "ORDER BY completed_at DESC LIMIT 1",
    )
    reality_fingerprint = _latest(
        engine,
        "SELECT reality_fingerprint FROM transit_derived.reality_manifest WHERE status = 'complete' "
        "ORDER BY completed_at DESC LIMIT 1",
    )

    bbox_metric = transform(lambda x, y, z=None: TO_TARGET(x, y), box(*bbox_wgs84))
    reach_area_wgs84 = transform(
        lambda x, y, z=None: TO_WGS84(x, y), bbox_metric.buffer(DAILY_ACCESS_REACH_BUFFER_M).envelope
    )

    graph = load_reference_graph(graph_dir)
    land_mask = LandMask(load_island_geometry_metric())
    lap("load_graph_and_mask")

    if use_overture:
        places, overture_only_green, stops, class_stats = load_v4_objects(
            engine,
            import_fingerprint=import_fingerprint,
            reality_fingerprint=reality_fingerprint,
            reach_area_wgs84=reach_area_wgs84,
        )
    else:
        from unittest import mock

        with mock.patch("overture.loader.load_overture_amenity_rows", return_value=[]):
            places, overture_only_green, stops, class_stats = load_v4_objects(
                engine,
                import_fingerprint=import_fingerprint,
                reality_fingerprint=reality_fingerprint,
                reach_area_wgs84=reach_area_wgs84,
            )
    lap("load_objects")

    with engine.begin() as connection:
        green_sites, green_stats = stage_green_sites(
            connection,
            import_fingerprint=import_fingerprint,
            study_area_wgs84=reach_area_wgs84,
            overture_green_rows=overture_only_green,
            buffer_m=0.0,
        )
        approaches, intervals, approach_stats = stage_park_approaches(
            connection,
            green_sites,
            node_x=graph.node_x,
            node_y=graph.node_y,
            offsets=graph.offsets,
            targets=graph.targets,
            lengths=graph.lengths,
            edge_flags=graph.edge_flags,
        )
    lap("green_sites_and_approaches")

    step = float(CANONICAL_BASE_RESOLUTION_M)
    min_x, min_y, max_x, max_y = bbox_metric.bounds
    xs = np.arange(np.floor(min_x / step) * step, max_x, step)
    ys = np.arange(np.floor(min_y / step) * step, max_y, step)
    cells = [(float(x), float(y)) for y in ys for x in xs]
    from types import SimpleNamespace
    from precompute.daily_access_context import load_context_field
    context_field = load_context_field(SimpleNamespace(
        engine=engine, source_state=SimpleNamespace(import_fingerprint=import_fingerprint),
        study_area_metric=bbox_metric, land_mask=land_mask, reality_fingerprint=reality_fingerprint,
    ))
    inputs = dict(
        context_field=context_field,
        land_mask=land_mask,
        cells=cells,
        cell_size_m=step,
        places=places,
        green_sites=green_sites,
        park_approaches=approaches,
        park_intervals=intervals,
        stops=stops,
        penalty_fn=CellPenalties(
            load_road_rows(engine, import_fingerprint),
            load_railway_corridor_rows(engine, reality_fingerprint) if reality_fingerprint else [],
        ),
    )
    result = compute_reference(graph=graph, **inputs)
    lap("reference_scoring")
    parity = None
    if native:
        native_cells = compute_native(graph_dir=graph_dir, **inputs)
        lap("native_scoring")
        parity = compare_with_reference(result, native_cells)

    ok_cells = [cell for cell in result.cells if cell.network_status == "ok"]
    totals = np.asarray([cell.score.total for cell in ok_cells]) if ok_cells else np.zeros(1)
    summary = {
        "import_fingerprint": import_fingerprint,
        "transit_reality_fingerprint": reality_fingerprint,
        "graph_dir": str(graph_dir),
        "graph_has_edge_flags": graph.edge_flags is not None,
        "bbox_wgs84": list(bbox_wgs84),
        "cells": len(cells),
        "network_status": dict(Counter(cell.network_status for cell in result.cells)),
        "total_score": {
            "mean": round(float(totals.mean()), 2),
            "p10": round(float(np.percentile(totals, 10)), 2),
            "p50": round(float(np.percentile(totals, 50)), 2),
            "p90": round(float(np.percentile(totals, 90)), 2),
        },
        "mean_service_access": {
            need: round(float(np.mean([cell.score.service_access[need] for cell in ok_cells] or [0])), 4)
            for need in DAILY_ACCESS_NEEDS
        },
        "places_by_kind": dict(Counter(place.service_kind for place in places)),
        "stops": len(stops),
        "seeded_kinds": sorted(result.reach_by_kind),
        "losses": result.losses,
        "warnings": result.warnings,
        "classification": class_stats,
        "green": green_stats,
        "park_approaches": approach_stats,
        "timings_s": timings,
        "native_parity": parity,
    }
    return summary, result


def save_cells(path: Path, result) -> None:
    cells = result.cells
    np.savez_compressed(
        path,
        x_min=np.asarray([cell.x_min for cell in cells]),
        y_min=np.asarray([cell.y_min for cell in cells]),
        network_status=np.asarray([NETWORK_STATUS_CODES[cell.network_status] for cell in cells], dtype=np.uint8),
        total=np.asarray([cell.quantized["total"] for cell in cells], dtype=np.uint16),
        components=np.asarray(
            [[cell.quantized["components"][name] for name in ("shops", "transport", "healthcare", "parks")] for cell in cells],
            dtype=np.uint16,
        ),
        service_access=np.asarray(
            [[cell.quantized["service_access"][need] for need in DAILY_ACCESS_NEEDS] for cell in cells],
            dtype=np.uint16,
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bbox", nargs=4, type=float, default=DEFAULT_BBOX_WGS84)
    parser.add_argument("--graph-dir", type=Path, default=None)
    parser.add_argument("--no-overture", action="store_true")
    parser.add_argument("--out", type=Path, default=None, help="Save per-cell arrays (.npz).")
    parser.add_argument("--native", action="store_true", help="Also run the Rust path and report parity.")
    args = parser.parse_args(argv)
    summary, result = run(
        tuple(args.bbox),
        args.graph_dir or _newest_walk_graph_dir(),
        use_overture=not args.no_overture,
        native=args.native,
    )
    if args.out:
        save_cells(args.out, result)
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
