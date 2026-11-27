"""Daily access score (v4): drive the native (Rust) path and check parity.

``walkgraph daily-access-{attach,objects,reach,score}`` are the production
path; the Python reference (``daily_access_reference``) defines their
behaviour. This module writes their little-endian inputs, runs them, reads
the outputs back, and compares them with the reference.

Python decides only what needs the full shapely mask: cell origins, the
on-land / interior (deeper than the land band) flags of cells and objects,
and footprint rings. Rust does every edge entry, the reach and the scoring.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import shapely

from config import (
    DAILY_ACCESS_CATEGORY_MAX,
    DAILY_ACCESS_HEALTHCARE_MIX,
    DAILY_ACCESS_INTERVAL_MERGE_M,
    DAILY_ACCESS_DEFAULT_MODE,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_SAME_EDGE_ITERATIONS,
    DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE,
    DAILY_ACCESS_SERVICE_KINDS,
    DAILY_ACCESS_SHOP_MIX,
    DAILY_ACCESS_SNAP_RADIUS_M,
    DAILY_ACCESS_TOP3_WEIGHTS,
    DAILY_ACCESS_TRANSPORT_TIER_QUALITY,
    WALKGRAPH_BIN,
)
from walkgraph_support import ensure_walkgraph_subcommand_available

from .daily_access_classify import LogicalPlace
from .daily_access_geometry import footprint_rings
from .daily_access_reach import KindReach, Seed
from .daily_access_reference import (
    NETWORK_STATUS_CODES,
    LabelTable,
    SourceSet,
    TransportStop,
    build_sources,
    loss_report,
    service_kind_k,
    service_kind_need,
    service_kind_quality,
)
from .daily_access_score import CATEGORIES, need_thresholds_m
from .daily_access_snap import (
    REASON_NO_EDGE_WITHIN_RADIUS,
    REASON_NO_LAND_CONNECTOR,
    REASON_OUTSIDE_LAND_MASK,
    LandMask,
)


CELL_FLAG_ON_LAND = 1
CELL_FLAG_INTERIOR = 2
OBJECT_FLAG_ON_LAND = 1
OBJECT_FLAG_INTERIOR = 2
OBJECT_FLAG_FOOTPRINT = 4
SEED_DTYPE = np.dtype([("node", "<u4"), ("label", "<u4"), ("offset_m", "<f8")])
CELL_DTYPE = np.dtype([("x", "<f8"), ("y", "<f8"), ("flags", "u1")])
OBJECT_DTYPE = np.dtype(
    [("kind", "<u2"), ("flags", "u1"), ("pad", "u1"), ("label", "<u4"), ("x", "<f8"), ("y", "<f8"),
     ("first_ring", "<u4"), ("ring_count", "<u4")]
)
INTERVAL_DTYPE = np.dtype(
    [("kind", "<u2"), ("pad", "<u2"), ("label", "<u4"), ("edge", "<u4"), ("pad2", "<u4"), ("lo", "<f8"), ("hi", "<f8")]
)
# Every service kind, in the order of the kind index used by the native files.
ALL_KINDS: tuple[str, ...] = tuple(sorted(DAILY_ACCESS_SERVICE_KINDS)) + tuple(
    f"transport_tier_{tier}" for tier in sorted(DAILY_ACCESS_TRANSPORT_TIER_QUALITY)
)
KIND_INDEX = {kind: index for index, kind in enumerate(ALL_KINDS)}
REASON_CODES = {
    0: None,
    1: REASON_OUTSIDE_LAND_MASK,
    2: REASON_NO_EDGE_WITHIN_RADIUS,
    3: REASON_NO_LAND_CONNECTOR,
}


@dataclass
class NativeCells:
    """Per-cell native outputs, in input cell order."""

    network_status: np.ndarray  # uint8, see NETWORK_STATUS_CODES
    total: np.ndarray  # uint16, 0.01 points, 65535 = outside mask
    components: np.ndarray  # uint16 (n, 4): shops, transport, healthcare, parks
    penalties: np.ndarray  # uint16 (n, 2): railway, road (positive)
    service_access: np.ndarray  # uint16 (n, 6) in DAILY_ACCESS_NEEDS order
    origins: np.ndarray
    reach_by_kind: dict[str, KindReach]
    labels: LabelTable
    losses: dict[str, Any]


# Peak resident memory per walkgraph subcommand (bytes), for build reports.
NATIVE_PEAK_BYTES: dict[str, int] = {}


def process_peak_bytes(handle: int | None = None) -> int | None:
    """Peak working set of a process handle (Windows) or this process / its children (POSIX)."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = _Counters()
        counters.cb = ctypes.sizeof(_Counters)
        target = handle if handle is not None else ctypes.windll.kernel32.GetCurrentProcess()
        psapi = ctypes.WinDLL("psapi")
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(_Counters), wintypes.DWORD]
        if psapi.GetProcessMemoryInfo(wintypes.HANDLE(int(target)), ctypes.byref(counters), counters.cb):
            return int(counters.PeakWorkingSetSize)
        return None
    import resource

    who = resource.RUSAGE_SELF if handle is None else resource.RUSAGE_CHILDREN
    return int(resource.getrusage(who).ru_maxrss) * 1024


def _run(walkgraph_bin: str, args: Sequence[str]) -> None:
    process = subprocess.Popen(
        [walkgraph_bin, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _stdout, stderr = process.communicate()
    peak = process_peak_bytes(getattr(process, "_handle", 0) or 0)
    if peak is not None:
        NATIVE_PEAK_BYTES[args[0]] = max(NATIVE_PEAK_BYTES.get(args[0], 0), peak)
    if process.returncode != 0:
        raise RuntimeError(
            f"walkgraph {args[0]} failed ({process.returncode}): {stderr.strip()[-2000:]}"
        )


def score_params_json(mode: str = DAILY_ACCESS_DEFAULT_MODE) -> dict[str, Any]:
    return {
        "thresholds_m": {need: list(need_thresholds_m(need, mode)) for need in DAILY_ACCESS_NEEDS},
        "top3_weights": list(DAILY_ACCESS_TOP3_WEIGHTS),
        "category_max": dict(DAILY_ACCESS_CATEGORY_MAX),
        "shop_mix": [DAILY_ACCESS_SHOP_MIX["food"], DAILY_ACCESS_SHOP_MIX["other_retail"]],
        "healthcare_mix": [
            DAILY_ACCESS_HEALTHCARE_MIX["primary_care"],
            DAILY_ACCESS_HEALTHCARE_MIX["pharmacy"],
        ],
    }


def write_mask_geojson(land_mask: LandMask, path: Path) -> None:
    """Land mask in EPSG:2157 metres, as the Rust GeoJSON loader expects."""
    feature = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {}, "geometry": json.loads(shapely.to_geojson(land_mask.geometry))}
        ],
    }
    path.write_text(json.dumps(feature), encoding="utf-8")


def cell_records(
    cells: Iterable[tuple[float, float]],
    cell_size_m: float,
    land_mask: LandMask,
    *,
    local_mask=None,
) -> np.ndarray:
    """Origins (centre on land, else a point on the land part) + flags.

    Vectorised equivalent of ``cell_origin`` per cell: only cells whose centre
    is off land but whose box meets the mask take the per-cell geometry path.
    ``local_mask`` is the mask clipped to a box around these cells (same
    answers there, far cheaper GEOS work than the national coastline).
    """
    from .daily_access_snap import cell_origin

    corners = np.asarray(list(cells) if not isinstance(cells, np.ndarray) else cells, dtype=np.float64).reshape(-1, 2)
    mask_geometry = land_mask.geometry if local_mask is None else local_mask
    shapely.prepare(mask_geometry)
    size = float(cell_size_m)
    records = np.zeros(len(corners), dtype=CELL_DTYPE)
    centre_x = corners[:, 0] + size / 2.0
    centre_y = corners[:, 1] + size / 2.0
    centre_on_land = shapely.contains_xy(mask_geometry, centre_x, centre_y)
    records["x"][centre_on_land] = centre_x[centre_on_land]
    records["y"][centre_on_land] = centre_y[centre_on_land]
    records["flags"][centre_on_land] = CELL_FLAG_ON_LAND
    off = np.flatnonzero(~centre_on_land)
    if len(off):
        boxes = shapely.box(corners[off, 0], corners[off, 1], corners[off, 0] + size, corners[off, 1] + size)
        for index in off[shapely.intersects(boxes, mask_geometry)]:
            origin = cell_origin(
                float(corners[index, 0]), float(corners[index, 1]), size, land_mask, geometry=mask_geometry
            )
            if origin is not None:
                records[index] = (origin[0], origin[1], CELL_FLAG_ON_LAND)
    on_land = (records["flags"] & CELL_FLAG_ON_LAND) != 0
    interior = np.zeros(len(corners), dtype=bool)
    interior[on_land] = land_mask.in_interior(records["x"][on_land], records["y"][on_land])
    records["flags"][interior] |= CELL_FLAG_INTERIOR
    return records


# --- objects ----------------------------------------------------------------


@dataclass
class ObjectFiles:
    objects_bin: Path
    ring_offsets_bin: Path
    ring_vertices_bin: Path
    intervals_bin: Path
    count: int

    def as_json(self) -> dict[str, str]:
        return {
            "objects_bin": str(self.objects_bin),
            "ring_offsets_bin": str(self.ring_offsets_bin),
            "ring_vertices_bin": str(self.ring_vertices_bin),
        }


def write_objects(sources: SourceSet, land_mask: LandMask, out_dir: Path) -> ObjectFiles:
    """Object records (+ rings) and the zero-cost park intervals for Rust."""
    out_dir.mkdir(parents=True, exist_ok=True)
    records = np.zeros(len(sources.objects), dtype=OBJECT_DTYPE)
    ring_offsets = [0]
    vertices: list[np.ndarray] = []
    ring_count = 0
    points_x = np.asarray([spec.x for spec in sources.objects], dtype=np.float64)
    points_y = np.asarray([spec.y for spec in sources.objects], dtype=np.float64)
    point_interior = land_mask.in_interior(points_x, points_y) if len(points_x) else np.zeros(0, dtype=bool)
    for index, spec in enumerate(sources.objects):
        flags = 0
        first_ring = ring_count
        rings_here = 0
        if spec.footprint_wkb:
            footprint = shapely.from_wkb(spec.footprint_wkb)
        else:
            footprint = None
        if footprint is not None and footprint.geom_type in {"Polygon", "MultiPolygon"}:
            anchor = footprint.representative_point()
            if land_mask.part_index(float(anchor.x), float(anchor.y)) is not None:
                flags |= OBJECT_FLAG_ON_LAND
            if bool(land_mask.interior.covers(footprint)):
                flags |= OBJECT_FLAG_INTERIOR
            flags |= OBJECT_FLAG_FOOTPRINT
            for ring in footprint_rings(footprint):
                vertices.append(np.asarray(ring, dtype=np.float64))
                ring_offsets.append(ring_offsets[-1] + len(ring))
                rings_here += 1
            ring_count += rings_here
        else:
            if land_mask.part_index(spec.x, spec.y) is not None:
                flags |= OBJECT_FLAG_ON_LAND
            if bool(point_interior[index]):
                flags |= OBJECT_FLAG_INTERIOR
        records[index] = (KIND_INDEX[spec.service_kind], flags, 0, spec.label, spec.x, spec.y, first_ring, rings_here)
    files = ObjectFiles(
        objects_bin=out_dir / "objects.bin",
        ring_offsets_bin=out_dir / "ring_offsets.bin",
        ring_vertices_bin=out_dir / "ring_vertices.bin",
        intervals_bin=out_dir / "park_intervals.bin",
        count=len(records),
    )
    records.tofile(files.objects_bin)
    np.asarray(ring_offsets, dtype="<u8").tofile(files.ring_offsets_bin)
    (np.concatenate(vertices) if vertices else np.zeros((0, 2))).astype("<f8").tofile(files.ring_vertices_bin)
    intervals = [
        (KIND_INDEX[kind], 0, label, edge, 0, lo, hi)
        for kind, items in sorted(sources.area_intervals.items())
        for edge, label, lo, hi in items
    ]
    np.asarray(intervals, dtype=INTERVAL_DTYPE).tofile(files.intervals_bin)
    return files


def native_attach(
    graph_dir: Path,
    mask_geojson: Path,
    records: np.ndarray,
    work_dir: Path,
    *,
    groups: np.ndarray | None = None,
    walkgraph_bin: str = WALKGRAPH_BIN,
    radius_m: float = DAILY_ACCESS_SNAP_RADIUS_M,
) -> dict[str, np.ndarray]:
    """Entry flag per cell and the endpoint nodes each group needs."""
    ensure_walkgraph_subcommand_available(walkgraph_bin, "daily-access-attach")
    cells_bin = work_dir / "cells.bin"
    records.astype(CELL_DTYPE).tofile(cells_bin)
    attach_dir = work_dir / "attach"
    args = [
        "daily-access-attach",
        "--graph-dir", str(graph_dir),
        "--mask-geojson", str(mask_geojson),
        "--cells-bin", str(cells_bin),
        "--out-dir", str(attach_dir),
        "--radius-m", repr(float(radius_m)),
        "--merge-m", repr(float(DAILY_ACCESS_INTERVAL_MERGE_M)),
    ]
    if groups is not None:
        groups_bin = work_dir / "groups.bin"
        np.asarray(groups, dtype="<u4").tofile(groups_bin)
        args += ["--groups-bin", str(groups_bin)]
    _run(walkgraph_bin, args)
    return {
        "entry_flags": np.fromfile(attach_dir / "entry_flags.bin", dtype=np.uint8),
        "group_offsets": np.fromfile(attach_dir / "group_offsets.bin", dtype="<u8"),
        "group_nodes": np.fromfile(attach_dir / "group_nodes.bin", dtype="<u4"),
        "origins": np.fromfile(attach_dir / "origins.bin", dtype="<u4"),
    }


def native_objects(
    graph_dir: Path,
    mask_geojson: Path,
    files: ObjectFiles,
    work_dir: Path,
    *,
    walkgraph_bin: str = WALKGRAPH_BIN,
) -> tuple[dict[str, np.ndarray], list[str | None]]:
    """Per-kind node seeds (structured ``SEED_DTYPE``) and per-object reasons."""
    ensure_walkgraph_subcommand_available(walkgraph_bin, "daily-access-objects")
    out_dir = work_dir / "objects_out"
    config_path = work_dir / "objects.json"
    config_path.write_text(
        json.dumps(
            {
                "mask_geojson": str(mask_geojson),
                "objects": files.as_json(),
                "kind_count": len(ALL_KINDS),
                "radius_m": float(DAILY_ACCESS_SNAP_RADIUS_M),
                "merge_m": float(DAILY_ACCESS_INTERVAL_MERGE_M),
                "out_dir": str(out_dir),
            }
        ),
        encoding="utf-8",
    )
    _run(walkgraph_bin, ["daily-access-objects", "--graph-dir", str(graph_dir), "--config-json", str(config_path)])
    seeds = {
        kind: np.fromfile(out_dir / f"seeds_{index}.bin", dtype=SEED_DTYPE)
        for index, kind in enumerate(ALL_KINDS)
    }
    reasons = [REASON_CODES[int(code)] for code in np.fromfile(out_dir / "reasons.bin", dtype=np.uint8)]
    return {kind: value for kind, value in seeds.items() if len(value)}, reasons


def merge_area_seeds(seeds: Mapping[str, np.ndarray], area_seeds: Mapping[str, Sequence[Seed]]) -> dict[str, np.ndarray]:
    """Add park approach seeds; keep the minimum offset per ``(node, label)``."""
    merged: dict[str, np.ndarray] = {}
    for kind in sorted(set(seeds) | set(area_seeds)):
        parts = [np.asarray(seeds.get(kind, np.zeros(0, dtype=SEED_DTYPE)), dtype=SEED_DTYPE)]
        extra = area_seeds.get(kind, ())
        if extra:
            parts.append(
                np.asarray([(seed.node, seed.label, seed.offset_m) for seed in extra], dtype=SEED_DTYPE)
            )
        values = np.concatenate(parts)
        order = np.lexsort((values["offset_m"], values["label"], values["node"]))
        values = values[order]
        keep = np.ones(len(values), dtype=bool)
        keep[1:] = (values["node"][1:] != values["node"][:-1]) | (values["label"][1:] != values["label"][:-1])
        if keep.any():
            merged[kind] = values[keep]
    return merged


def native_reach(
    graph_dir: Path,
    seeds_by_kind: Mapping[str, np.ndarray],
    origins: np.ndarray,
    work_dir: Path,
    *,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    walkgraph_bin: str = WALKGRAPH_BIN,
    load_results: bool = True,
) -> tuple[dict[str, KindReach], list[dict[str, Any]]]:
    """Run the native reach; ``load_results=False`` leaves the outputs on disk."""
    ensure_walkgraph_subcommand_available(walkgraph_bin, "daily-access-reach")
    reach_dir = work_dir / "reach"
    reach_dir.mkdir(parents=True, exist_ok=True)
    origins_bin = reach_dir / "origins.bin"
    np.asarray(origins, dtype="<u4").tofile(origins_bin)
    kinds: list[dict[str, Any]] = []
    for kind in sorted(seeds_by_kind):
        seeds_bin = reach_dir / f"{kind}.seeds.bin"
        np.asarray(seeds_by_kind[kind], dtype=SEED_DTYPE).tofile(seeds_bin)
        need = service_kind_need(kind)
        kinds.append(
            {
                "kind": kind,
                "kind_index": KIND_INDEX[kind],
                "need": need,
                "quality": service_kind_quality(kind),
                "seeds_bin": str(seeds_bin),
                "k": service_kind_k(kind),
                "cutoff_m": float(DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE[mode][need]),
                "out_distances": str(reach_dir / f"{kind}.distances.bin"),
                "out_labels": str(reach_dir / f"{kind}.labels.bin"),
            }
        )
    config_path = reach_dir / "reach.json"
    config_path.write_text(json.dumps({"origins_bin": str(origins_bin), "kinds": kinds}), encoding="utf-8")
    _run(walkgraph_bin, ["daily-access-reach", "--graph-dir", str(graph_dir), "--config-json", str(config_path)])
    results: dict[str, KindReach] = {}
    for kind in kinds if load_results else ():
        k = int(kind["k"])
        results[kind["kind"]] = KindReach(
            service_kind=kind["kind"],
            origin_nodes=np.asarray(origins, dtype=np.int64),
            distance_codes=np.fromfile(kind["out_distances"], dtype="<u2").reshape(-1, k),
            labels=np.fromfile(kind["out_labels"], dtype="<u4").reshape(-1, k),
        )
    return results, kinds


def native_score(
    graph_dir: Path,
    mask_geojson: Path,
    records: np.ndarray,
    block_origins: np.ndarray,
    kinds: Sequence[Mapping[str, Any]],
    work_dir: Path,
    *,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    objects: ObjectFiles | None,
    penalties: np.ndarray | None = None,
    walkgraph_bin: str = WALKGRAPH_BIN,
) -> dict[str, np.ndarray]:
    """Score one block of cells.

    ``kinds`` carry ``distances``/``labels`` paths whose rows follow
    ``block_origins`` (only the block's nodes: arrays scale with the block).
    ``penalties`` is ``(n, 2)`` float32 railway/road at the cell origins.
    """
    ensure_walkgraph_subcommand_available(walkgraph_bin, "daily-access-score")
    score_dir = work_dir / "score"
    score_dir.mkdir(parents=True, exist_ok=True)
    cells_bin = score_dir / "cells.bin"
    records.astype(CELL_DTYPE).tofile(cells_bin)
    origins_bin = score_dir / "origins.bin"
    np.asarray(block_origins, dtype="<u4").tofile(origins_bin)
    config: dict[str, Any] = {
        "params": score_params_json(mode),
        "cells_bin": str(cells_bin),
        "mask_geojson": str(mask_geojson),
        "radius_m": float(DAILY_ACCESS_SNAP_RADIUS_M),
        "merge_m": float(DAILY_ACCESS_INTERVAL_MERGE_M),
        "same_edge_iterations": int(DAILY_ACCESS_SAME_EDGE_ITERATIONS),
        "objects": None if objects is None else objects.as_json(),
        "park_intervals_bin": None if objects is None else str(objects.intervals_bin),
        "origins_bin": str(origins_bin),
        "kinds": [
            {
                "kind_index": int(kind["kind_index"]),
                "need": kind["need"],
                "quality": kind["quality"],
                "k": kind["k"],
                "cutoff_m": kind["cutoff_m"],
                "distances": kind.get("distances"),
                "labels": kind.get("labels"),
            }
            for kind in kinds
        ],
        "penalties_bin": None,
        "out_dir": str(score_dir / "out"),
    }
    if penalties is not None:
        path = score_dir / "penalties.f32"
        np.asarray(penalties, dtype="<f4").reshape(-1, 2).tofile(path)
        config["penalties_bin"] = str(path)
    config_path = score_dir / "score.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    _run(walkgraph_bin, ["daily-access-score", "--graph-dir", str(graph_dir), "--config-json", str(config_path)])
    out = Path(config["out_dir"])
    return {
        "network_status": np.fromfile(out / "network_status.bin", dtype=np.uint8),
        "total": np.fromfile(out / "total.bin", dtype="<u2"),
        "components": np.fromfile(out / "components.bin", dtype="<u2").reshape(-1, len(CATEGORIES)),
        "penalties": np.fromfile(out / "penalties.bin", dtype="<u2").reshape(-1, 2),
        "service_access": np.fromfile(out / "service_access.bin", dtype="<u2").reshape(-1, len(DAILY_ACCESS_NEEDS)),
    }


def block_reach_files(
    kinds: Sequence[Mapping[str, Any]],
    origins: np.ndarray,
    rows_by_kind: Mapping[str, tuple[np.ndarray, np.ndarray]],
    block_nodes: np.ndarray,
    out_dir: Path,
) -> list[dict[str, Any]]:
    """Reach rows of ``block_nodes`` only (``rows_by_kind`` may be memory-mapped)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = np.searchsorted(origins, block_nodes)
    if len(block_nodes) and (np.any(rows >= len(origins)) or np.any(origins[np.minimum(rows, len(origins) - 1)] != block_nodes)):
        raise RuntimeError("block nodes are missing from the reach cache origins")
    block_kinds = []
    for kind in kinds:
        distances, labels = rows_by_kind[kind["kind"]]
        distance_path = out_dir / f"{kind['kind']}.distances.bin"
        label_path = out_dir / f"{kind['kind']}.labels.bin"
        np.ascontiguousarray(distances[rows]).astype("<u2").tofile(distance_path)
        np.ascontiguousarray(labels[rows]).astype("<u4").tofile(label_path)
        block_kinds.append({**kind, "distances": str(distance_path), "labels": str(label_path)})
    return block_kinds


def penalties_for_records(records: np.ndarray, penalty_fn) -> np.ndarray | None:
    if penalty_fn is None:
        return None
    on_land = (records["flags"] & CELL_FLAG_ON_LAND) != 0
    out = np.zeros((len(records), 2), dtype=np.float32)
    if np.any(on_land):
        railway, road = penalty_fn(records["x"][on_land], records["y"][on_land])
        out[on_land, 0] = np.asarray(railway, dtype=np.float32)
        out[on_land, 1] = np.asarray(road, dtype=np.float32)
    return out


def compute_native(
    *,
    graph_dir: Path,
    land_mask: LandMask,
    cells: Iterable[tuple[float, float]],
    cell_size_m: float,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    places: Sequence[LogicalPlace] = (),
    green_sites: Sequence[Any] = (),
    park_approaches: Mapping[str, Sequence[tuple[int, float]]] | None = None,
    park_intervals: Mapping[str, Sequence[tuple[int, float, float]]] | None = None,
    stops: Sequence[TransportStop] = (),
    penalty_fn=None,
    work_dir: Path | None = None,
    walkgraph_bin: str = WALKGRAPH_BIN,
) -> NativeCells:
    """Same inputs as ``compute_reference``; native entries, reach and score."""
    graph_dir = Path(graph_dir)
    sources = build_sources(
        places=places,
        green_sites=green_sites,
        park_approaches=park_approaches or {},
        park_intervals=park_intervals,
        stops=stops,
    )
    with tempfile.TemporaryDirectory(dir=work_dir) as temp:
        temp_dir = Path(temp)
        mask_path = temp_dir / "mask.geojson"
        write_mask_geojson(land_mask, mask_path)
        objects = write_objects(sources, land_mask, temp_dir / "objects")
        seeds, reasons = native_objects(graph_dir, mask_path, objects, temp_dir, walkgraph_bin=walkgraph_bin)
        seeds = merge_area_seeds(seeds, sources.area_seeds)
        losses, _warnings = loss_report(sources, reasons)
        records = cell_records(cells, cell_size_m, land_mask)
        attach = native_attach(graph_dir, mask_path, records, temp_dir, walkgraph_bin=walkgraph_bin)
        reach_by_kind, kinds = native_reach(
            graph_dir,
            seeds,
            attach["origins"],
            temp_dir,
            mode=mode,
            walkgraph_bin=walkgraph_bin,
        )
        score_kinds = [{**kind, "distances": kind["out_distances"], "labels": kind["out_labels"]} for kind in kinds]
        scored = native_score(
            graph_dir,
            mask_path,
            records,
            attach["origins"],
            score_kinds,
            temp_dir,
            mode=mode,
            objects=objects,
            penalties=penalties_for_records(records, penalty_fn),
            walkgraph_bin=walkgraph_bin,
        )
    return NativeCells(
        network_status=scored["network_status"],
        total=scored["total"],
        components=scored["components"],
        penalties=scored["penalties"],
        service_access=scored["service_access"],
        origins=attach["origins"],
        reach_by_kind=reach_by_kind,
        labels=sources.labels,
        losses=losses,
    )


# Python/Rust tolerances (spec): distances one 0.1 m step, access 0.0005,
# one component 0.02 points, total after quantisation 0.03 points.
PARITY_LIMITS = {"reach_steps": 1, "access": 0.0005, "component": 0.02, "total": 0.03}


def compare_with_reference(reference, native: NativeCells) -> dict[str, Any]:
    """Parity report against ``compute_reference`` output for the same cells."""
    report: dict[str, Any] = {"cells": len(reference.cells)}
    status_codes = np.asarray([NETWORK_STATUS_CODES[cell.network_status] for cell in reference.cells], dtype=np.uint8)
    report["status_mismatches"] = int(np.count_nonzero(status_codes != native.network_status))
    valid = status_codes != NETWORK_STATUS_CODES["outside_mask"]

    ref_total = np.asarray([cell.quantized["total"] for cell in reference.cells], dtype=np.int64)
    report["max_total_diff"] = float(np.max(np.abs(ref_total[valid] - native.total[valid].astype(np.int64)), initial=0) / 100.0)
    component_diffs = []
    for index, category in enumerate(CATEGORIES):
        ref_codes = np.asarray([cell.quantized["components"][category] for cell in reference.cells], dtype=np.int64)
        component_diffs.append(np.max(np.abs(ref_codes[valid] - native.components[valid, index].astype(np.int64)), initial=0))
    report["max_component_diff"] = float(max(component_diffs) / 100.0)
    access_diffs = []
    for index, need in enumerate(DAILY_ACCESS_NEEDS):
        ref_codes = np.asarray([cell.quantized["service_access"][need] for cell in reference.cells], dtype=np.int64)
        access_diffs.append(np.max(np.abs(ref_codes[valid] - native.service_access[valid, index].astype(np.int64)), initial=0))
    report["max_access_diff"] = float(max(access_diffs) / 10_000.0)
    penalty_diffs = []
    for index, penalty in enumerate(("railway_proximity", "road_proximity")):
        ref_codes = np.asarray([cell.quantized["penalties"][penalty] for cell in reference.cells], dtype=np.int64)
        penalty_diffs.append(np.max(np.abs(ref_codes[valid] - native.penalties[valid, index].astype(np.int64)), initial=0))
    report["max_penalty_diff"] = float(max(penalty_diffs) / 100.0)

    reach_step_diff = 0
    reach_label_mismatches = 0
    for kind, ref_reach in reference.reach_by_kind.items():
        native_reach = native.reach_by_kind.get(kind)
        if native_reach is None:
            reach_label_mismatches += 1
            continue
        row_of = {int(node): row for row, node in enumerate(native_reach.origin_nodes.tolist())}
        for ref_row, node in enumerate(ref_reach.origin_nodes.tolist()):
            native_row = row_of.get(int(node))
            if native_row is None:
                reach_label_mismatches += 1
                continue
            a = ref_reach.distance_codes[ref_row].astype(np.int64)
            b = native_reach.distance_codes[native_row].astype(np.int64)
            reach_step_diff = max(reach_step_diff, int(np.max(np.abs(a - b), initial=0)))
            # Equal-distance alternatives may swap ids; anything else is a real mismatch.
            if not np.array_equal(ref_reach.labels[ref_row], native_reach.labels[native_row]) and np.array_equal(a, b):
                if sorted(ref_reach.labels[ref_row].tolist()) != sorted(native_reach.labels[native_row].tolist()):
                    reach_label_mismatches += 1
    report["max_reach_step_diff"] = reach_step_diff
    report["reach_label_mismatches"] = reach_label_mismatches
    report["origin_mismatch"] = bool(
        set(int(n) for n in native.origins.tolist())
        != set(int(n) for kind in reference.reach_by_kind.values() for n in kind.origin_nodes.tolist())
    ) if reference.reach_by_kind else False
    report["within_limits"] = bool(
        report["status_mismatches"] == 0
        and reach_step_diff <= PARITY_LIMITS["reach_steps"]
        and report["max_access_diff"] <= PARITY_LIMITS["access"]
        and report["max_component_diff"] <= PARITY_LIMITS["component"]
        and report["max_total_diff"] <= PARITY_LIMITS["total"]
        and report["max_penalty_diff"] == 0.0
    )
    return report


__all__ = [
    "ALL_KINDS",
    "CELL_DTYPE",
    "CELL_FLAG_INTERIOR",
    "CELL_FLAG_ON_LAND",
    "KIND_INDEX",
    "NATIVE_PEAK_BYTES",
    "NativeCells",
    "ObjectFiles",
    "PARITY_LIMITS",
    "SEED_DTYPE",
    "block_reach_files",
    "cell_records",
    "compare_with_reference",
    "compute_native",
    "merge_area_seeds",
    "native_attach",
    "native_objects",
    "native_reach",
    "native_score",
    "penalties_for_records",
    "process_peak_bytes",
    "score_params_json",
    "write_mask_geojson",
    "write_objects",
]
