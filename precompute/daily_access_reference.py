"""Daily access score (v4): end-to-end Python reference.

Ties together edge entries (``daily_access_snap`` / ``daily_access_geometry``),
per-kind reach (``daily_access_reach``) and the score math
(``daily_access_score``). The Rust port is checked against this module; it is
not the production path for national builds.

Distance from a cell to one logical object::

    min over admissible cell entries and object exits of
        cell connector + network path + object connector

* Objects become sources: every admissible exit on an edge gives its two
  endpoint nodes a seed (connector + along-edge cost, minimised exactly).
  One multi-source Dijkstra per service kind keeps the ``k`` nearest
  *distinct* logical ids per node (one per transport tier).
* A cell enters every admissible edge within 250 m; each endpoint gets the
  exact approach cost. Per logical id: ``min over nodes`` of approach + node
  distance, and, when the cell and the object enter the *same* edge, the
  direct walk between the two entries (never through an endpoint). Ids found
  through several entries collapse to their minimum distance before quality
  and the curve apply.
* Virtual entries never become transit bridges: seeds only start searches
  and cell approaches only end them.
* Penalties are read at the cell origin (``penalty_fn``), never at a node.
* No admissible entry -> ``no_walking_network``: valid land, zero access and
  zero positive components (penalties are still the cell's own).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np
import shapely

from config import (
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_SEARCH_CUTOFF_M,
    DAILY_ACCESS_SERVICE_KINDS,
    DAILY_ACCESS_TOP_K,
    DAILY_ACCESS_TRANSPORT_TIER_QUALITY,
    TO_TARGET,
)

from .daily_access_classify import LogicalPlace
from .daily_access_geometry import PIECE_LINE, PIECE_POINT, PIECE_ZERO, Piece, same_edge_distance
from .daily_access_reach import KindReach, LABEL_MISSING, Seed, reach_for_kind
from .daily_access_score import (
    CellScore,
    category_components,
    dequantize_distance,
    need_access,
    quantize_cell_score,
    score_cell_options,
    total_score,
)
from .daily_access_snap import (
    EdgeIndex,
    Entries,
    LandMask,
    cell_origin,
    footprint_entries,
    graph_components,
    point_entries,
)


LOSS_WARNING_SHARE = 0.05
NETWORK_STATUS_CODES = {"outside_mask": 0, "ok": 1, "no_walking_network": 2}
PenaltyFn = Callable[[np.ndarray, np.ndarray], tuple[np.ndarray, np.ndarray]]


@dataclass
class ReferenceGraph:
    node_x: np.ndarray
    node_y: np.ndarray
    offsets: np.ndarray
    targets: np.ndarray
    lengths: np.ndarray
    component_id: np.ndarray
    edge_flags: np.ndarray | None = None

    @classmethod
    def from_arrays(
        cls,
        *,
        node_x: np.ndarray,
        node_y: np.ndarray,
        offsets: np.ndarray,
        targets: np.ndarray,
        lengths: np.ndarray,
        edge_flags: np.ndarray | None = None,
        component_id: np.ndarray | None = None,
    ) -> "ReferenceGraph":
        offsets = np.asarray(offsets, dtype=np.int64)
        targets = np.asarray(targets, dtype=np.int64)
        if component_id is None:
            component_id = graph_components(offsets, targets, len(node_x))
        return cls(
            node_x=np.asarray(node_x, dtype=np.float64),
            node_y=np.asarray(node_y, dtype=np.float64),
            offsets=offsets,
            targets=targets,
            lengths=np.asarray(lengths, dtype=np.float32),
            component_id=np.asarray(component_id, dtype=np.int32),
            edge_flags=None if edge_flags is None else np.asarray(edge_flags, dtype=np.uint8),
        )

    def edge_index(self) -> EdgeIndex:
        return EdgeIndex(self.node_x, self.node_y, self.offsets, self.targets, self.lengths)


def load_reference_graph(graph_dir: Path, *, verify_components: bool = True) -> ReferenceGraph:
    """Read walkgraph sidecars; node coordinates are projected to EPSG:2157.

    ``walk_graph.edge_flags.bin`` (uint8 per CSR slot) is optional. With
    ``verify_components=False`` the stored ``walk_graph.components.bin`` is
    used as is (no scipy recomputation: the national build is memory-bound).
    Components are diagnostics only; scoring never depends on them.
    """
    graph_dir = Path(graph_dir)
    components_path = graph_dir / "walk_graph.components.bin"
    nodes = np.fromfile(graph_dir / "walk_graph.nodes.bin", dtype=np.dtype([("lat", "<f4"), ("lon", "<f4")]))
    node_x, node_y = TO_TARGET(nodes["lon"].astype(np.float64), nodes["lat"].astype(np.float64))
    flags_path = graph_dir / "walk_graph.edge_flags.bin"
    graph = ReferenceGraph.from_arrays(
        node_x=np.asarray(node_x),
        node_y=np.asarray(node_y),
        offsets=np.fromfile(graph_dir / "walk_graph.adjacency_offsets.bin", dtype="<u8"),
        targets=np.fromfile(graph_dir / "walk_graph.adjacency_targets.bin", dtype="<u4"),
        lengths=np.fromfile(graph_dir / "walk_graph.adjacency_lengths.bin", dtype="<f4"),
        edge_flags=np.fromfile(flags_path, dtype="<u1") if flags_path.exists() else None,
        component_id=(
            None
            if verify_components or not components_path.exists()
            else np.fromfile(components_path, dtype="<u4").astype(np.int32)
        ),
    )
    if verify_components and components_path.exists():
        stored = np.fromfile(components_path, dtype="<u4").astype(np.int32)
        if not np.array_equal(stored, graph.component_id):
            raise RuntimeError(f"{components_path} disagrees with the reference component ids")
    return graph


@dataclass
class TransportStop:
    stop_ref: str
    feed: str
    tier: int
    x: float
    y: float

    @property
    def logical_key(self) -> str:
        """``gtfs:<feed>:<stop_id>``; ``stop_ref`` is ``gtfs/<feed>/<stop_id>`` or a bare id."""
        parts = str(self.stop_ref).split("/", 2)
        stop_id = parts[2] if len(parts) == 3 else str(self.stop_ref)
        return f"gtfs:{self.feed}:{stop_id}"


@dataclass(frozen=True)
class ObjectSpec:
    """One geometric source of a logical id (a place member, a stop, a green point)."""

    service_kind: str
    label: int
    groups: tuple[str, ...]
    x: float
    y: float
    footprint_wkb: bytes | None = None


@dataclass
class LabelTable:
    """Compact uint32 label -> reproducible logical key -> source records, per kind."""

    by_kind: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))

    def add(self, kind: str, label: int, key: str, members: Sequence[str]) -> None:
        self.by_kind[kind].append({"label": int(label), "key": str(key), "members": list(members)})

    def key_of(self, kind: str, label: int) -> str | None:
        for entry in self.by_kind.get(kind, ()):
            if entry["label"] == int(label):
                return entry["key"]
        return None

    def as_json(self) -> dict[str, list[dict[str, Any]]]:
        return {kind: sorted(entries, key=lambda item: item["label"]) for kind, entries in sorted(self.by_kind.items())}


@dataclass
class CellResult:
    x_min: float
    y_min: float
    network_status: str
    origin: tuple[float, float] | None
    score: CellScore
    quantized: dict[str, Any]
    best_by_kind: dict[str, list[tuple[int, float]]] = field(default_factory=dict)


@dataclass
class ReferenceResult:
    cells: list[CellResult]
    reach_by_kind: dict[str, KindReach]
    losses: dict[str, Any]
    labels: LabelTable
    warnings: list[str] = field(default_factory=list)


def service_kind_need(service_kind: str) -> str:
    if service_kind.startswith("transport_tier_"):
        return "transport"
    return DAILY_ACCESS_SERVICE_KINDS[service_kind][0]


def service_kind_quality(service_kind: str) -> float:
    if service_kind.startswith("transport_tier_"):
        return float(DAILY_ACCESS_TRANSPORT_TIER_QUALITY[int(service_kind.rsplit("_", 1)[1])])
    return float(DAILY_ACCESS_SERVICE_KINDS[service_kind][1])


def service_kind_k(service_kind: str) -> int:
    return 1 if service_kind_need(service_kind) == "transport" else DAILY_ACCESS_TOP_K


def _metric_xy(member: Mapping[str, Any]) -> tuple[float, float]:
    if "x" in member and "y" in member:
        return float(member["x"]), float(member["y"])
    x, y = TO_TARGET(float(member["lon"]), float(member["lat"]))
    return float(x), float(y)


def member_ref(member: Mapping[str, Any]) -> str:
    """Namespaced source record id: ``osm:w/123``, ``overture:<id>``."""
    source = str(member.get("source") or "")
    ref = str(member.get("source_ref") or "")
    namespace = {"osm_local_pbf": "osm", "overture_places": "overture"}.get(source, source or "unknown")
    return f"{namespace}:{ref}"


class _LossLedger:
    def __init__(self) -> None:
        self.eligible: dict[str, int] = defaultdict(int)
        self.unbound: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def add(self, group: str, reason: str | None) -> None:
        self.eligible[group] += 1
        if reason is not None:
            self.unbound[group][reason] += 1

    def report(self) -> tuple[dict[str, Any], list[str]]:
        report: dict[str, Any] = {}
        warnings: list[str] = []
        for group in sorted(self.eligible):
            eligible = self.eligible[group]
            unbound = sum(self.unbound[group].values())
            share = unbound / eligible if eligible else 0.0
            report[group] = {
                "eligible": eligible,
                "unbound": unbound,
                "unbound_share": round(share, 6),
                "reasons": dict(sorted(self.unbound[group].items())),
            }
            if eligible and share >= LOSS_WARNING_SHARE:
                warnings.append(
                    f"{group}: {unbound}/{eligible} eligible objects ({share:.1%}) could not be attached"
                )
        return report, warnings


@dataclass
class SourceSet:
    """Everything the reach and the cell scoring need about objects."""

    objects: list[ObjectSpec]
    labels: LabelTable
    # site_key-free green area inputs: kind -> [(node, label, offset)] / [(edge, label, lo, hi)]
    area_seeds: dict[str, list[Seed]]
    area_intervals: dict[str, list[tuple[int, int, float, float]]]
    area_groups: list[tuple[str, bool]]


def build_sources(
    *,
    places: Sequence[LogicalPlace],
    green_sites: Sequence[Any],
    park_approaches: Mapping[str, Sequence[tuple[int, float]]],
    park_intervals: Mapping[str, Sequence[tuple[int, float, float]]] | None,
    stops: Sequence[TransportStop],
) -> SourceSet:
    """Logical ids and geometric sources for every kind (no geometry work).

    Labels are positions in the sorted logical keys of a kind, so they are
    reproducible for an unchanged set of groups; green labels are the
    existing root site ids (green roots span several green kinds).
    """
    labels = LabelTable()
    objects: list[ObjectSpec] = []

    by_kind: dict[str, list[LogicalPlace]] = defaultdict(list)
    for place in places:
        by_kind[place.service_kind].append(place)
    for kind in sorted(by_kind):
        for label, place in enumerate(sorted(by_kind[kind], key=lambda item: item.place_id)):
            refs = [member_ref(member) for member in place.members]
            labels.add(kind, label, place.place_id, refs)
            for member in place.members:
                footprint_wkb = member.get("footprint_wkb")
                if footprint_wkb:
                    centroid = shapely.from_wkb(footprint_wkb).centroid
                    x, y = float(centroid.x), float(centroid.y)
                else:
                    x, y = _metric_xy(member)
                objects.append(
                    ObjectSpec(kind, label, (place.need, f"kind:{kind}"), x, y, footprint_wkb or None)
                )

    stops_by_kind: dict[str, list[TransportStop]] = defaultdict(list)
    for stop in stops:
        if stop.tier in DAILY_ACCESS_TRANSPORT_TIER_QUALITY:
            stops_by_kind[f"transport_tier_{stop.tier}"].append(stop)
    for kind in sorted(stops_by_kind):
        keyed = sorted(stops_by_kind[kind], key=lambda stop: stop.logical_key)
        for label, stop in enumerate(keyed):
            labels.add(kind, label, stop.logical_key, [stop.logical_key])
            objects.append(
                ObjectSpec(kind, label, ("transport", f"transport_feed:{stop.feed}", f"kind:{kind}"), stop.x, stop.y)
            )

    area_seeds: dict[str, list[Seed]] = defaultdict(list)
    area_intervals: dict[str, list[tuple[int, int, float, float]]] = defaultdict(list)
    area_groups: list[tuple[str, bool]] = []
    seen_roots: set[tuple[str, int]] = set()
    for site in green_sites:
        root = int(site.root_site_id)
        if (site.service_kind, root) not in seen_roots:
            seen_roots.add((site.service_kind, root))
            labels.add(site.service_kind, root, str(site.root_site_key), list(site.member_refs))
        if site.is_area:
            approach = list(park_approaches.get(site.site_key, ()))
            intervals = list((park_intervals or {}).get(site.site_key, ()))
            area_seeds[site.service_kind].extend(Seed(int(node), float(offset), root) for node, offset in approach)
            area_intervals[site.service_kind].extend(
                (int(edge), root, float(lo), float(hi)) for edge, lo, hi in intervals
            )
            area_groups.append((site.service_kind, bool(approach or intervals)))
        else:
            point = shapely.from_wkb(site.geom_wkb)
            objects.append(ObjectSpec(site.service_kind, root, ("green", f"kind:{site.service_kind}"), float(point.x), float(point.y)))
    return SourceSet(objects, labels, dict(area_seeds), dict(area_intervals), area_groups)


def object_entries(index: EdgeIndex, land_mask: LandMask, spec: ObjectSpec) -> Entries:
    if spec.footprint_wkb:
        return footprint_entries(index, land_mask, shapely.from_wkb(spec.footprint_wkb))
    return point_entries(index, land_mask, spec.x, spec.y)


def loss_report(sources: SourceSet, reasons: Sequence[str | None]) -> tuple[dict[str, Any], list[str]]:
    """Losses by need, service kind, transport and GTFS feed (one per logical id).

    ``reasons[i]`` is ``None`` when ``sources.objects[i]`` attached; a
    logical id is lost only when none of its members attaches.
    """
    ledger = _LossLedger()
    reason_by_object: dict[tuple[str, int], list[str | None]] = defaultdict(list)
    groups_by_object: dict[tuple[str, int], tuple[str, ...]] = {}
    for spec, reason in zip(sources.objects, reasons):
        key = (spec.service_kind, spec.label)
        reason_by_object[key].append(reason)
        groups_by_object[key] = spec.groups
    for key, object_reasons in sorted(reason_by_object.items()):
        reason = None if any(item is None for item in object_reasons) else object_reasons[-1]
        for group in groups_by_object[key]:
            ledger.add(group, reason)
    for kind, has_access in sources.area_groups:
        ledger.add("green", None if has_access else "unknown_access")
        ledger.add(f"kind:{kind}", None if has_access else "unknown_access")
    return ledger.report()


def build_seeds(
    index: EdgeIndex,
    land_mask: LandMask,
    sources: SourceSet,
) -> tuple[dict[str, list[Seed]], dict[str, dict[int, list[tuple[int, Piece, float]]]], dict[str, Any], list[str]]:
    """Node seeds and same-edge pieces per kind, plus the loss ledger.

    Seeds keep the minimum offset per ``(node, label)``; pieces are
    ``edge -> [(label, piece, offset)]``.
    """
    seed_best: dict[str, dict[tuple[int, int], float]] = defaultdict(dict)
    pieces_by_kind: dict[str, dict[int, list[tuple[int, Piece, float]]]] = defaultdict(lambda: defaultdict(list))
    reasons: list[str | None] = []
    for spec in sources.objects:
        entries = object_entries(index, land_mask, spec)
        reasons.append(entries.reason)
        best = seed_best[spec.service_kind]
        for node, cost in entries.node_costs.items():
            slot = (node, spec.label)
            if cost < best.get(slot, np.inf):
                best[slot] = cost
        for edge, pieces in entries.pieces.items():
            pieces_by_kind[spec.service_kind][edge].extend((spec.label, piece, 0.0) for piece in pieces)
    for kind, seeds in sources.area_seeds.items():
        best = seed_best[kind]
        for seed in seeds:
            slot = (seed.node, seed.label)
            if seed.offset_m < best.get(slot, np.inf):
                best[slot] = seed.offset_m
    for kind, intervals in sources.area_intervals.items():
        for edge, label, lo, hi in intervals:
            pieces_by_kind[kind][edge].append((label, Piece(PIECE_ZERO, lo, hi), 0.0))

    seeds = {
        kind: [Seed(node, offset, label) for (node, label), offset in sorted(best.items())]
        for kind, best in seed_best.items()
    }
    report, warnings = loss_report(sources, reasons)
    return seeds, {kind: dict(value) for kind, value in pieces_by_kind.items()}, report, warnings


def run_reach(
    graph: ReferenceGraph,
    seeds_by_kind: Mapping[str, Sequence[Seed]],
    origin_nodes: np.ndarray,
    *,
    keep_raw: bool = False,
) -> dict[str, KindReach]:
    """One kind at a time: dense per-node state never exists for two kinds."""
    results: dict[str, KindReach] = {}
    for kind in sorted(seeds_by_kind):
        need = service_kind_need(kind)
        results[kind] = reach_for_kind(
            kind,
            graph.offsets,
            graph.targets,
            graph.lengths,
            seeds_by_kind[kind],
            origin_nodes,
            k=service_kind_k(kind),
            cutoff_m=DAILY_ACCESS_SEARCH_CUTOFF_M[need],
            keep_raw=keep_raw,
        )
    return results


def piece_floor(piece: Piece) -> float:
    """Smallest connector of a piece over its interval (a lower bound of any path through it)."""
    if piece.kind == PIECE_POINT:
        return piece.value(min(max(piece.a, piece.lo), piece.hi))
    if piece.kind == PIECE_LINE:
        if piece.a != 0.0 and piece.lo <= -piece.b / piece.a <= piece.hi:
            return 0.0
        return min(piece.value(piece.lo), piece.value(piece.hi))
    return 0.0


def kind_distances(
    entries: Entries,
    index: EdgeIndex,
    reach: KindReach | None,
    row_by_node: Mapping[int, int],
    pieces: Mapping[int, Sequence[tuple[int, Piece, float]]],
    *,
    use_raw: bool = False,
    k: int | None = None,
    cutoff_m: float | None = None,
) -> dict[int, float]:
    """Best distance per logical id of one kind for one cell.

    With ``k``/``cutoff_m`` a same-edge candidate whose lower bound (both
    connectors' minima) cannot beat the kind's current k-th distance is
    skipped: it cannot enter the kind's top ``k``, so access is unchanged
    (the Rust scorer applies the same rule).
    """
    best: dict[int, float] = {}
    if reach is not None:
        for node, cost in sorted(entries.node_costs.items()):
            row = row_by_node.get(int(node))
            if row is None:
                continue
            for column in range(reach.distance_codes.shape[1]):
                label = int(reach.labels[row, column])
                if label == int(LABEL_MISSING):
                    continue
                if use_raw and reach.raw_distances_m is not None:
                    network = float(reach.raw_distances_m[row, column])
                else:
                    network = dequantize_distance(int(reach.distance_codes[row, column]))
                    if network is None:
                        continue
                distance = network + cost
                if distance < best.get(label, np.inf):
                    best[label] = distance
    threshold = np.inf
    if k is not None:
        threshold = float(cutoff_m) if cutoff_m is not None else np.inf
        if len(best) >= k:
            threshold = min(threshold, sorted(best.values())[k - 1])
    for edge, cell_pieces in sorted(entries.pieces.items()):
        targets = pieces.get(edge)
        if not targets:
            continue
        frame = index.frame(edge)
        for label, target, offset in targets:
            target_floor = piece_floor(target) + offset
            for cell_piece in cell_pieces:
                floor = piece_floor(cell_piece) + target_floor
                if floor > threshold or best.get(label, np.inf) <= floor:
                    continue
                distance = same_edge_distance(frame, cell_piece, target, offset)
                if distance < best.get(label, np.inf):
                    best[label] = distance
    return best


def score_from_distances(
    distances_by_kind: Mapping[str, Mapping[int, float]],
    *,
    railway_penalty: float,
    road_penalty: float,
) -> CellScore:
    options: dict[str, list[tuple[object, float, float]]] = defaultdict(list)
    for kind, best in distances_by_kind.items():
        need = service_kind_need(kind)
        quality = service_kind_quality(kind)
        for label, distance in best.items():
            # Green roots are shared across green kinds; other labels are per kind.
            key: object = ("green", label) if need == "green" else (kind, label)
            options[need].append((key, quality, distance))
    return score_cell_options(options, railway_penalty=railway_penalty, road_penalty=road_penalty)


def no_network_score(*, railway_penalty: float, road_penalty: float) -> CellScore:
    components = category_components({})
    return CellScore(
        total=total_score(components, railway_penalty=railway_penalty, road_penalty=road_penalty),
        components=components,
        service_access={need: 0.0 for need in DAILY_ACCESS_NEEDS},
        penalties={"railway_proximity": max(float(railway_penalty), 0.0), "road_proximity": max(float(road_penalty), 0.0)},
    )


def compute_reference(
    *,
    graph: ReferenceGraph,
    land_mask: LandMask,
    cells: Iterable[tuple[float, float]],
    cell_size_m: float,
    places: Sequence[LogicalPlace] = (),
    green_sites: Sequence[Any] = (),
    park_approaches: Mapping[str, Sequence[tuple[int, float]]] | None = None,
    park_intervals: Mapping[str, Sequence[tuple[int, float, float]]] | None = None,
    stops: Sequence[TransportStop] = (),
    penalty_fn: PenaltyFn | None = None,
    use_raw_distances: bool = False,
    index: EdgeIndex | None = None,
) -> ReferenceResult:
    index = graph.edge_index() if index is None else index
    sources = build_sources(
        places=places,
        green_sites=green_sites,
        park_approaches=park_approaches or {},
        park_intervals=park_intervals,
        stops=stops,
    )
    seeds_by_kind, pieces_by_kind, losses, warnings = build_seeds(index, land_mask, sources)

    cell_inputs: list[tuple[float, float, tuple[float, float] | None, Entries | None]] = []
    for x_min, y_min in cells:
        origin = cell_origin(float(x_min), float(y_min), float(cell_size_m), land_mask)
        entries = point_entries(index, land_mask, *origin) if origin is not None else None
        cell_inputs.append((float(x_min), float(y_min), origin, entries))

    origin_nodes = np.asarray(
        sorted({node for _, _, _, entries in cell_inputs if entries is not None for node in entries.node_costs}),
        dtype=np.int64,
    )
    row_by_node = {int(node): row for row, node in enumerate(origin_nodes.tolist())}
    reach_by_kind = run_reach(graph, seeds_by_kind, origin_nodes, keep_raw=use_raw_distances)
    kinds = sorted(set(reach_by_kind) | set(pieces_by_kind))

    origins_xy = [origin for _, _, origin, _ in cell_inputs if origin is not None]
    if penalty_fn is not None and origins_xy:
        rail_values, road_values = penalty_fn(
            np.asarray([xy[0] for xy in origins_xy]), np.asarray([xy[1] for xy in origins_xy])
        )
    else:
        rail_values = np.zeros(len(origins_xy))
        road_values = np.zeros(len(origins_xy))

    results: list[CellResult] = []
    origin_position = 0
    for x_min, y_min, origin, entries in cell_inputs:
        best_by_kind: dict[str, list[tuple[int, float]]] = {}
        if origin is None:
            status = "outside_mask"
            score = no_network_score(railway_penalty=0.0, road_penalty=0.0)
        else:
            # Penalties are float32 per cell in the native path.
            railway = float(np.float32(rail_values[origin_position]))
            road = float(np.float32(road_values[origin_position]))
            origin_position += 1
            if entries is None or not entries.has_network:
                status = "no_walking_network"
                score = no_network_score(railway_penalty=railway, road_penalty=road)
            else:
                status = "ok"
                distances = {
                    kind: kind_distances(
                        entries,
                        index,
                        reach_by_kind.get(kind),
                        row_by_node,
                        pieces_by_kind.get(kind, {}),
                        use_raw=use_raw_distances,
                        k=service_kind_k(kind),
                        cutoff_m=DAILY_ACCESS_SEARCH_CUTOFF_M[service_kind_need(kind)],
                    )
                    for kind in kinds
                }
                score = score_from_distances(distances, railway_penalty=railway, road_penalty=road)
                best_by_kind = {
                    kind: sorted(((label, distance) for label, distance in best.items()), key=lambda item: (item[1], item[0]))[
                        : service_kind_k(kind)
                    ]
                    for kind, best in distances.items()
                    if best
                }
        results.append(
            CellResult(
                x_min=x_min,
                y_min=y_min,
                network_status=status,
                origin=origin,
                score=score,
                quantized=quantize_cell_score(score),
                best_by_kind=best_by_kind,
            )
        )
    return ReferenceResult(
        cells=results, reach_by_kind=reach_by_kind, losses=losses, labels=sources.labels, warnings=warnings
    )


__all__ = [
    "CellResult",
    "LOSS_WARNING_SHARE",
    "LabelTable",
    "NETWORK_STATUS_CODES",
    "ObjectSpec",
    "PenaltyFn",
    "ReferenceGraph",
    "ReferenceResult",
    "SourceSet",
    "TransportStop",
    "build_seeds",
    "build_sources",
    "compute_reference",
    "kind_distances",
    "load_reference_graph",
    "loss_report",
    "member_ref",
    "need_access",
    "no_network_score",
    "object_entries",
    "run_reach",
    "score_from_distances",
    "service_kind_k",
    "service_kind_need",
    "service_kind_quality",
]
