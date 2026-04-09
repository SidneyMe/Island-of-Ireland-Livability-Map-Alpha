from __future__ import annotations

from dataclasses import dataclass, field
import json
import subprocess
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from config import WALKGRAPH_FORMAT_VERSION

try:
    import igraph as ig
except ImportError:  # pragma: no cover - exercised when dependency is missing
    ig = None


GRAPH_FORMAT_VERSION = WALKGRAPH_FORMAT_VERSION
NODES_DTYPE = np.dtype([("lat", "<f4"), ("lon", "<f4")])
EDGES_DTYPE = np.dtype([("src", "<u4"), ("dst", "<u4"), ("length_m", "<f4")])
OSMIDS_DTYPE = np.dtype("<i8")
ADJ_OFFSETS_DTYPE = np.dtype("<u8")
ADJ_TARGETS_DTYPE = np.dtype("<u4")
ADJ_LENGTHS_DTYPE = np.dtype("<f4")
COMPACT_VERTEX_ATTR_THRESHOLD = 2_000_000
COMPACT_EDGE_ATTR_THRESHOLD = 4_000_000
REQUIRED_GRAPH_FILENAMES = (
    "walk_graph.meta.json",
    "walk_graph.nodes.bin",
    "walk_graph.edges.bin",
    "walk_graph.osmids.bin",
    "walk_graph.adjacency_offsets.bin",
    "walk_graph.adjacency_targets.bin",
    "walk_graph.adjacency_lengths.bin",
)


@dataclass
class WalkGraphIndex:
    graph_dir: Path
    meta: dict[str, Any]
    node_latitudes: np.ndarray
    node_longitudes: np.ndarray
    osm_ids: np.ndarray | None = None
    _graph_attrs: dict[str, object] = field(default_factory=dict)

    def attributes(self) -> list[str]:
        fixed = {"meta", "_node_latitudes", "_node_longitudes"}
        if self.osm_ids is not None:
            fixed.add("_osm_ids")
        return sorted(fixed | set(self._graph_attrs))

    def __getitem__(self, key: str):
        if key == "meta":
            return self.meta
        if key == "_node_latitudes":
            return self.node_latitudes
        if key == "_node_longitudes":
            return self.node_longitudes
        if key == "_osm_ids":
            return self.osm_ids
        return self._graph_attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self._graph_attrs[key] = value

    def vcount(self) -> int:
        return int(self.meta.get("node_count", len(self.node_latitudes)))

    def ecount(self) -> int:
        return int(self.meta.get("edge_count", 0))


def _normalized_bbox(bbox: tuple[float, float, float, float] | None) -> dict[str, float] | None:
    if bbox is None:
        return None
    return {
        "min_lat": round(float(bbox[0]), 8),
        "min_lon": round(float(bbox[1]), 8),
        "max_lat": round(float(bbox[2]), 8),
        "max_lon": round(float(bbox[3]), 8),
    }


def _normalized_padding_m(value: float) -> float:
    return round(float(value), 6)


def _required_graph_sidecars_exist(graph_dir: Path) -> bool:
    return all((graph_dir / name).exists() for name in REQUIRED_GRAPH_FILENAMES)


def _edge_endpoint_matrix(edges: np.ndarray) -> np.ndarray:
    # igraph accepts NumPy edge matrices directly, which avoids building
    # millions of intermediate Python tuples for large national graphs.
    endpoints = np.empty((len(edges), 2), dtype=np.uint32)
    endpoints[:, 0] = edges["src"]
    endpoints[:, 1] = edges["dst"]
    return endpoints


def _use_compact_graph_attrs(node_count: int, edge_count: int) -> bool:
    return (
        int(node_count) >= COMPACT_VERTEX_ATTR_THRESHOLD
        or int(edge_count) >= COMPACT_EDGE_ATTR_THRESHOLD
    )


def _require_igraph():
    if ig is None:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "python-igraph is required to load walk graph sidecars. "
            "Install requirements.txt before running --precompute."
        )
    return ig


def _emit_progress(progress_cb, detail: str) -> None:
    if progress_cb is None:
        return
    progress_cb("detail", detail=detail, force_log=True)


def _iter_stderr_lines(process: subprocess.Popen[str]) -> Iterable[str]:
    if process.stderr is None:
        return []
    for line in process.stderr:
        text = line.rstrip()
        if text:
            yield text


def run_walkgraph_build(
    pbf_path: Path,
    output_dir: Path,
    walkgraph_bin: str = "walkgraph",
    *,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_padding_m: float = 0.0,
    extract_fingerprint: str | None = None,
    progress_cb=None,
) -> None:
    normalized_padding_m = _normalized_padding_m(bbox_padding_m)
    command = [
        walkgraph_bin,
        "build",
        "--pbf",
        str(pbf_path),
        "--out",
        str(output_dir),
        "--bbox-padding-m",
        f"{normalized_padding_m:.6f}",
    ]
    if bbox is not None:
        normalized_bbox = _normalized_bbox(bbox)
        command += [
            "--bbox",
            ",".join(
                f"{normalized_bbox[key]:.8f}"
                for key in ("min_lat", "min_lon", "max_lat", "max_lon")
            ),
        ]
    if extract_fingerprint:
        command += ["--extract-fingerprint", extract_fingerprint]

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "walkgraph is required to build walk graph sidecars, but it was not found on PATH. "
            "Build the Rust CLI first or set WALKGRAPH_BIN."
        ) from exc
    try:
        for line in _iter_stderr_lines(process):
            _emit_progress(progress_cb, f"walkgraph: {line}")
        return_code = process.wait()
    finally:
        if process.stderr is not None and hasattr(process.stderr, "close"):
            process.stderr.close()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def run_walkgraph_reachability(
    graph_dir: Path,
    origins_bin: Path,
    amenity_weights_bin: Path,
    output_bin: Path,
    *,
    category_count: int,
    cutoff_m: float,
    walkgraph_bin: str = "walkgraph",
    progress_cb=None,
) -> None:
    command = [
        walkgraph_bin,
        "reachability",
        "--graph-dir",
        str(graph_dir),
        "--origins-bin",
        str(origins_bin),
        "--amenity-weights-bin",
        str(amenity_weights_bin),
        "--category-count",
        str(int(category_count)),
        "--cutoff-m",
        f"{float(cutoff_m):.6f}",
        "--out",
        str(output_bin),
    ]

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "walkgraph is required to run exact walk reachability, but it was not found on PATH. "
            "Build the Rust CLI first or set WALKGRAPH_BIN."
        ) from exc

    try:
        for line in _iter_stderr_lines(process):
            _emit_progress(progress_cb, f"walkgraph: {line}")
        return_code = process.wait()
    finally:
        if process.stderr is not None and hasattr(process.stderr, "close"):
            process.stderr.close()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def load_graph_meta(graph_dir: Path) -> dict[str, Any]:
    meta_path = graph_dir / "walk_graph.meta.json"
    with meta_path.open(encoding="utf-8") as handle:
        return json.load(handle)


def graph_meta_matches(
    graph_dir: Path,
    *,
    extract_fingerprint: str,
    bbox: tuple[float, float, float, float] | None,
    bbox_padding_m: float,
) -> bool:
    try:
        meta = load_graph_meta(graph_dir)
    except OSError:
        return False
    except json.JSONDecodeError:
        return False

    if int(meta.get("format_version", 0)) != GRAPH_FORMAT_VERSION:
        return False
    if str(meta.get("extract_fingerprint") or "") != str(extract_fingerprint):
        return False
    if not _required_graph_sidecars_exist(graph_dir):
        return False

    cached_bbox = meta.get("bbox")
    expected_bbox = _normalized_bbox(bbox)
    if cached_bbox != expected_bbox:
        return False
    return abs(float(meta.get("bbox_padding_m", 0.0)) - _normalized_padding_m(bbox_padding_m)) < 1e-9


def _load_walk_graph_sidecars(
    graph_dir: Path,
) -> tuple[dict[str, Any], np.ndarray, np.ndarray, np.ndarray]:
    meta = load_graph_meta(graph_dir)
    if int(meta.get("format_version", 0)) != GRAPH_FORMAT_VERSION:
        raise RuntimeError(
            f"Unsupported walk graph format_version={meta.get('format_version')!r}. "
            f"Expected {GRAPH_FORMAT_VERSION}."
        )
    if not _required_graph_sidecars_exist(graph_dir):
        raise RuntimeError(
            f"Walk graph sidecars are incomplete in '{graph_dir}'. "
            "Rebuild the graph sidecars before running nationwide precompute."
        )

    nodes = np.fromfile(graph_dir / "walk_graph.nodes.bin", dtype=NODES_DTYPE)
    edges = np.fromfile(graph_dir / "walk_graph.edges.bin", dtype=EDGES_DTYPE)
    osm_ids = np.fromfile(graph_dir / "walk_graph.osmids.bin", dtype=OSMIDS_DTYPE)
    return meta, nodes, edges, osm_ids


def load_walk_graph_index(graph_dir: Path) -> WalkGraphIndex:
    meta, nodes, edges, osm_ids = _load_walk_graph_sidecars(graph_dir)

    node_count = int(meta.get("node_count", len(nodes)))
    edge_count = int(meta.get("edge_count", len(edges)))
    if len(nodes) != node_count:
        raise RuntimeError(f"Expected {node_count} node records, found {len(nodes)}")
    if len(osm_ids) != node_count:
        raise RuntimeError(f"Expected {node_count} osmid records, found {len(osm_ids)}")
    if len(edges) != edge_count:
        raise RuntimeError(f"Expected {edge_count} edge records, found {len(edges)}")

    return WalkGraphIndex(
        graph_dir=graph_dir,
        meta=meta,
        node_latitudes=np.array(nodes["lat"], dtype=np.float64, copy=True),
        node_longitudes=np.array(nodes["lon"], dtype=np.float64, copy=True),
        osm_ids=np.array(osm_ids, dtype=np.int64, copy=True),
    )


def load_walk_graph(graph_dir: Path):
    igraph = _require_igraph()
    meta, nodes, edges, osm_ids = _load_walk_graph_sidecars(graph_dir)

    node_count = int(meta.get("node_count", len(nodes)))
    edge_count = int(meta.get("edge_count", len(edges)))

    if len(nodes) != node_count:
        raise RuntimeError(f"Expected {node_count} node records, found {len(nodes)}")
    if len(osm_ids) != node_count:
        raise RuntimeError(f"Expected {node_count} osmid records, found {len(osm_ids)}")
    if len(edges) != edge_count:
        raise RuntimeError(f"Expected {edge_count} edge records, found {len(edges)}")

    edge_endpoints = _edge_endpoint_matrix(edges)
    graph = igraph.Graph(
        n=node_count,
        edges=edge_endpoints,
        directed=True,
    )
    graph["meta"] = meta
    if _use_compact_graph_attrs(node_count, edge_count):
        graph["_node_latitudes"] = np.array(nodes["lat"], dtype=np.float32, copy=True)
        graph["_node_longitudes"] = np.array(nodes["lon"], dtype=np.float32, copy=True)
        graph["_edge_length_m"] = np.array(edges["length_m"], dtype=np.float64, copy=True)
        return graph

    graph.vs["lat"] = nodes["lat"].astype(float).tolist()
    graph.vs["lon"] = nodes["lon"].astype(float).tolist()
    graph.vs["osmid"] = osm_ids.astype(int).tolist()
    graph.es["length_m"] = edges["length_m"].astype(float).tolist()
    return graph
