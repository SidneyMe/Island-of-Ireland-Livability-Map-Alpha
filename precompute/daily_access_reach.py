"""Daily access score (v4): multi-source Dijkstra per service_kind (reference).

The walk graph is symmetric (every OSM segment is stored in both directions),
so one search seeded from all objects of a kind gives each node its nearest
objects. A node keeps up to ``k`` *distinct* labels (logical place ids, or
green root site ids); transport keeps one per tier.

Seeds carry the object connector (or park-approach offset) as their start
distance; the cutoff applies to that full distance. Edge lengths are the
graph's float32 values summed in float64. Heap order is
``(distance, node, label)`` so ties break the same way in the Rust port.

Only rows for ``origin_nodes`` (nodes some cell attaches to) are returned:
``distance_codes`` uint16 at 0.1 m (65535 = none) and ``labels`` uint32
(0xFFFFFFFF = none), both shaped ``(len(origin_nodes), k)`` and sorted by
distance code, then label.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np

from config import DAILY_ACCESS_DISTANCE_MISSING

from .daily_access_score import quantize_distance


LABEL_MISSING = np.uint32(0xFFFFFFFF)


@dataclass(frozen=True)
class Seed:
    node: int
    offset_m: float
    label: int


@dataclass
class KindReach:
    service_kind: str
    origin_nodes: np.ndarray
    distance_codes: np.ndarray
    labels: np.ndarray
    raw_distances_m: np.ndarray | None = None


def k_nearest_labels(
    offsets: np.ndarray,
    targets: np.ndarray,
    lengths: np.ndarray,
    seeds: Iterable[Seed],
    *,
    k: int,
    cutoff_m: float,
) -> dict[int, list[tuple[float, int]]]:
    """Per reached node: up to ``k`` ``(distance_m, label)`` with distinct labels."""
    if k <= 0:
        raise ValueError("k must be positive")
    cutoff = float(cutoff_m)
    heap: list[tuple[float, int, int]] = []
    for seed in seeds:
        offset = float(seed.offset_m)
        if offset <= cutoff:
            heap.append((offset, int(seed.node), int(seed.label)))
    heapq.heapify(heap)

    settled: dict[int, list[tuple[float, int]]] = {}
    while heap:
        distance, node, label = heapq.heappop(heap)
        accepted = settled.setdefault(node, [])
        if len(accepted) >= k or any(existing == label for _, existing in accepted):
            continue
        accepted.append((distance, label))
        start = int(offsets[node])
        end = int(offsets[node + 1])
        for slot in range(start, end):
            neighbour = int(targets[slot])
            next_distance = distance + float(lengths[slot])
            if next_distance > cutoff:
                continue
            neighbour_accepted = settled.get(neighbour)
            if neighbour_accepted is not None and (
                len(neighbour_accepted) >= k
                or any(existing == label for _, existing in neighbour_accepted)
            ):
                continue
            heapq.heappush(heap, (next_distance, neighbour, label))
    return settled


def reach_for_kind(
    service_kind: str,
    offsets: np.ndarray,
    targets: np.ndarray,
    lengths: np.ndarray,
    seeds: Sequence[Seed],
    origin_nodes: np.ndarray,
    *,
    k: int,
    cutoff_m: float,
    keep_raw: bool = False,
) -> KindReach:
    origin_nodes = np.asarray(origin_nodes, dtype=np.int64)
    settled = k_nearest_labels(offsets, targets, lengths, seeds, k=k, cutoff_m=cutoff_m)
    distance_codes = np.full((len(origin_nodes), k), DAILY_ACCESS_DISTANCE_MISSING, dtype=np.uint16)
    labels = np.full((len(origin_nodes), k), LABEL_MISSING, dtype=np.uint32)
    raw = np.full((len(origin_nodes), k), np.nan, dtype=np.float64) if keep_raw else None
    for row, node in enumerate(origin_nodes.tolist()):
        entries = settled.get(int(node))
        if not entries:
            continue
        coded = sorted(
            ((quantize_distance(distance), label, distance) for distance, label in entries),
            key=lambda item: (item[0], item[1]),
        )
        for column, (code, label, distance) in enumerate(coded):
            distance_codes[row, column] = code
            labels[row, column] = label
            if raw is not None:
                raw[row, column] = distance
    return KindReach(
        service_kind=service_kind,
        origin_nodes=origin_nodes,
        distance_codes=distance_codes,
        labels=labels,
        raw_distances_m=raw,
    )


__all__ = [
    "KindReach",
    "LABEL_MISSING",
    "Seed",
    "k_nearest_labels",
    "reach_for_kind",
]
