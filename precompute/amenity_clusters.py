from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

import numpy as np
from sklearn.neighbors import BallTree

from config import TO_TARGET, VARIETY_CLUSTER_RADIUS_M


def _ordered_categories(
    categories: Iterable[str] | None,
    amenity_source_rows: list[dict[str, Any]],
) -> list[str]:
    present = {
        str(row.get("category") or "")
        for row in amenity_source_rows
        if str(row.get("category") or "")
    }
    if categories is None:
        return sorted(present)
    ordered = [str(category) for category in categories if str(category) in present]
    extras = sorted(present - set(ordered))
    return ordered + extras


def _metric_coordinates(rows: list[dict[str, Any]]) -> np.ndarray:
    points: list[tuple[float, float]] = []
    for row in rows:
        metric_x, metric_y = TO_TARGET(float(row["lon"]), float(row["lat"]))
        points.append((float(metric_x), float(metric_y)))
    return np.asarray(points, dtype=np.float64)


def _representative_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    name = str(row.get("name") or "").strip()
    return (
        -max(int(row.get("score_units") or 0), 0),
        0 if name else 1,
        str(row.get("source") or ""),
        str(row.get("source_ref") or ""),
        float(row.get("lat") or 0.0),
        float(row.get("lon") or 0.0),
    )


def _connected_components(points_xy: np.ndarray, *, radius_m: float) -> list[list[int]]:
    if points_xy.size == 0:
        return []
    if len(points_xy) == 1:
        return [[0]]

    tree = BallTree(points_xy, metric="euclidean")
    neighbours = tree.query_radius(points_xy, r=float(radius_m), return_distance=False)

    parents = list(range(len(points_xy)))

    def _find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def _union(left: int, right: int) -> None:
        left_root = _find(left)
        right_root = _find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parents[right_root] = left_root
        else:
            parents[left_root] = right_root

    for left_index, row_neighbours in enumerate(neighbours):
        for right_index in row_neighbours.tolist():
            normalized_right = int(right_index)
            if normalized_right <= left_index:
                continue
            _union(left_index, normalized_right)

    groups: dict[int, list[int]] = defaultdict(list)
    for index in range(len(points_xy)):
        groups[_find(index)].append(index)
    return [sorted(indexes) for _, indexes in sorted(groups.items())]


def _cluster_category_rows(
    rows: list[dict[str, Any]],
    *,
    radius_m: float,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    if len(rows) == 1:
        representative = dict(rows[0])
        representative["base_units"] = max(int(representative.get("score_units") or 0), 0)
        representative["cluster_size"] = 1
        return [representative]

    points_xy = _metric_coordinates(rows)
    cluster_rows: list[dict[str, Any]] = []
    for component_indexes in _connected_components(points_xy, radius_m=radius_m):
        members = [rows[index] for index in component_indexes]
        representative = dict(sorted(members, key=_representative_sort_key)[0])
        representative["base_units"] = max(int(representative.get("score_units") or 0), 0)
        representative["cluster_size"] = len(component_indexes)
        cluster_rows.append(representative)
    cluster_rows.sort(
        key=lambda row: (
            str(row.get("category") or ""),
            str(row.get("source") or ""),
            str(row.get("source_ref") or ""),
            float(row.get("lat") or 0.0),
            float(row.get("lon") or 0.0),
        )
    )
    return cluster_rows


def build_amenity_clusters(
    amenity_source_rows: list[dict[str, Any]],
    *,
    categories: Iterable[str] | None = None,
    cluster_radius_m: float = VARIETY_CLUSTER_RADIUS_M,
) -> tuple[dict[str, list[tuple[float, float]]], list[dict[str, Any]]]:
    cluster_data: dict[str, list[tuple[float, float]]] = {}
    cluster_rows: list[dict[str, Any]] = []

    ordered_categories = _ordered_categories(categories, amenity_source_rows)
    for category in ordered_categories:
        category_rows = [
            row
            for row in amenity_source_rows
            if str(row.get("category") or "") == category
        ]
        clustered_rows = _cluster_category_rows(
            category_rows,
            radius_m=cluster_radius_m,
        )
        cluster_data[category] = [
            (float(row["lat"]), float(row["lon"]))
            for row in clustered_rows
        ]
        cluster_rows.extend(clustered_rows)

    return cluster_data, cluster_rows


__all__ = [
    "build_amenity_clusters",
]
