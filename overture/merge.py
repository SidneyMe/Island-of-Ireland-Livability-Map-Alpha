from __future__ import annotations

import math
import re
from typing import Any, Iterable

from config import TAGS
from overture.loader import OVERTURE_CATEGORY_MAP


AUTO_MATCH_RADIUS_M = 35.0
NAME_MATCH_RADIUS_M = 75.0
OSM_SELF_DEDUPE_RADIUS_M = 10.0
EXPECTED_BASELINE_MERGE_CATEGORIES = ("healthcare", "parks", "shops")
_EXCLUDED_MERGE_CATEGORIES = frozenset({"transport"})
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def resolve_merge_categories(
    scoring_categories: Iterable[str] | None = None,
) -> tuple[str, ...]:
    if scoring_categories is None:
        scoring_categories = TAGS.keys()
    scoring_category_set = {
        str(category).strip()
        for category in scoring_categories
        if str(category).strip()
    }
    overture_category_set = {
        str(category).strip()
        for category in OVERTURE_CATEGORY_MAP.values()
        if str(category).strip()
    }
    resolved = sorted(
        (scoring_category_set & overture_category_set) - _EXCLUDED_MERGE_CATEGORIES
    )
    return tuple(resolved)


def merge_category_warning(
    merge_categories: Iterable[str],
) -> str | None:
    resolved = tuple(
        sorted({str(category) for category in merge_categories if str(category)})
    )
    baseline = tuple(sorted(EXPECTED_BASELINE_MERGE_CATEGORIES))
    if resolved == baseline:
        return None
    return (
        "resolved merge categories differ from baseline "
        f"{baseline!r}: {resolved!r}"
    )


def _point_lat_lon(row: dict[str, Any]) -> tuple[float, float]:
    geom = row["geom"]
    return float(geom.y), float(geom.x)


def _park_area_m2(row: dict[str, Any]) -> float:
    try:
        return max(float(row.get("park_area_m2", 0.0) or 0.0), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_alias(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    normalized = _NON_ALNUM_RE.sub(" ", text.strip().lower()).strip()
    return normalized or None


def _split_alias_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return [part.strip() for part in value.split(";") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for child in value:
            values.extend(_split_alias_values(child))
        return values
    return []


def build_osm_aliases(row: dict[str, Any]) -> tuple[str, ...]:
    tags_json = row.get("tags_json") or {}
    candidates: list[str] = []
    for key in (
        "name",
        "alt_name",
        "brand",
        "operator",
        "official_name",
        "short_name",
    ):
        if key in tags_json:
            candidates.extend(_split_alias_values(tags_json.get(key)))
    candidates.extend(_split_alias_values(row.get("name")))
    aliases = {
        normalized
        for candidate in candidates
        for normalized in (_normalize_alias(candidate),)
        if normalized
    }
    return tuple(sorted(aliases))


def build_overture_aliases(row: dict[str, Any]) -> tuple[str, ...]:
    candidates: list[str] = []
    candidates.extend(_split_alias_values(row.get("name")))
    candidates.extend(_split_alias_values(row.get("brand")))
    aliases = {
        normalized
        for candidate in candidates
        for normalized in (_normalize_alias(candidate),)
        if normalized
    }
    return tuple(sorted(aliases))


def _aliases_agree(
    left_aliases: tuple[str, ...],
    right_aliases: tuple[str, ...],
) -> bool:
    if not left_aliases or not right_aliases:
        return False
    return bool(set(left_aliases).intersection(right_aliases))


def _distance_m_from_lat_lon(
    left_lat: float,
    left_lon: float,
    right_lat: float,
    right_lon: float,
) -> float:
    left_lat_rad = math.radians(left_lat)
    right_lat_rad = math.radians(right_lat)
    delta_lat = right_lat_rad - left_lat_rad
    delta_lon = math.radians(right_lon - left_lon)
    a = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(left_lat_rad)
        * math.cos(right_lat_rad)
        * math.sin(delta_lon / 2.0) ** 2
    )
    return 2.0 * 6_371_000.0 * math.asin(math.sqrt(a))


def _candidate_match_key(
    distance_m: float,
    aliases_agree: bool,
    source_ref: str,
    overture_row_id: int,
) -> tuple[int, int, float, str, int]:
    return (
        0 if distance_m <= AUTO_MATCH_RADIUS_M else 1,
        0 if aliases_agree else 1,
        float(distance_m),
        str(source_ref),
        int(overture_row_id),
    )


def _clean_name(value: Any) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _has_explicit_name(row: dict[str, Any]) -> bool:
    return bool(_clean_name(row.get("name")))


def _canonical_name(
    osm_row: dict[str, Any],
    overture_row: dict[str, Any] | None = None,
) -> str | None:
    preferred = _clean_name(osm_row.get("name"))
    if preferred:
        return preferred
    if overture_row is not None:
        candidate = _clean_name(overture_row.get("name"))
        if candidate:
            return candidate
    return None


def _canonical_osm_row(
    osm_row: dict[str, Any],
    *,
    conflict_class: str,
    overture_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lat, lon = _point_lat_lon(osm_row)
    return {
        "category": str(osm_row["category"]),
        "lat": lat,
        "lon": lon,
        "source": "osm_local_pbf",
        "source_ref": str(osm_row["source_ref"]),
        "name": _canonical_name(osm_row, overture_row),
        "conflict_class": conflict_class,
        "park_area_m2": _park_area_m2(osm_row),
    }


def _canonical_overture_row(overture_row: dict[str, Any]) -> dict[str, Any]:
    lat, lon = _point_lat_lon(overture_row)
    name = _clean_name(overture_row.get("name"))
    return {
        "category": str(overture_row["category"]),
        "lat": lat,
        "lon": lon,
        "source": "overture_places",
        "source_ref": str(overture_row["source_ref"]),
        "name": name or None,
        "conflict_class": "overture_only",
        "park_area_m2": 0.0,
    }


def _stable_osm_row_sort_key(
    row: dict[str, Any],
) -> tuple[str, str, float, float, str, float]:
    lat, lon = _point_lat_lon(row)
    return (
        str(row.get("category") or ""),
        str(row.get("source_ref") or ""),
        float(lat),
        float(lon),
        _clean_name(row.get("name")),
        _park_area_m2(row),
    )


def _stable_overture_row_sort_key(
    row: dict[str, Any],
) -> tuple[str, str, float, float, str]:
    lat, lon = _point_lat_lon(row)
    return (
        str(row.get("category") or ""),
        str(row.get("source_ref") or ""),
        float(lat),
        float(lon),
        _clean_name(row.get("name")),
    )


def _prepared_osm_merge_sort_key(
    prepared_row: dict[str, Any],
) -> tuple[str, int, int, str, float, float, int]:
    return (
        str(prepared_row["category"]),
        0 if _has_explicit_name(prepared_row["row"]) else 1,
        -len(prepared_row["aliases"]),
        str(prepared_row["source_ref"]),
        float(prepared_row["lat"]),
        float(prepared_row["lon"]),
        int(prepared_row["row_id"]),
    )


def _prepared_osm_duplicate_canonical_key(
    prepared_row: dict[str, Any],
) -> tuple[int, int, float, str, int]:
    return (
        0 if _has_explicit_name(prepared_row["row"]) else 1,
        -len(prepared_row["aliases"]),
        -_park_area_m2(prepared_row["row"]),
        str(prepared_row["source_ref"]),
        int(prepared_row["row_id"]),
    )


def _prepare_osm_source_rows(
    osm_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    prepared_rows: list[dict[str, Any]] = []
    for row_id, row in enumerate(sorted(osm_rows, key=_stable_osm_row_sort_key), start=1):
        lat, lon = _point_lat_lon(row)
        prepared_rows.append(
            {
                "row_id": row_id,
                "category": str(row.get("category") or ""),
                "source_ref": str(row.get("source_ref") or ""),
                "lat": lat,
                "lon": lon,
                "aliases": build_osm_aliases(row),
                "row": row,
            }
        )
    return prepared_rows


def prepare_osm_rows_for_self_dedupe(
    osm_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return _prepare_osm_source_rows(osm_rows)


def _prepare_rows(
    osm_rows: list[dict[str, Any]],
    overture_rows: list[dict[str, Any]],
    *,
    merge_categories: Iterable[str] | None = None,
) -> dict[str, Any]:
    merge_category_set = set(
        resolve_merge_categories() if merge_categories is None else merge_categories
    )

    merge_osm = [
        row for row in osm_rows if str(row.get("category") or "") in merge_category_set
    ]
    passthrough_osm = [
        row for row in osm_rows if str(row.get("category") or "") not in merge_category_set
    ]
    merge_overture = [
        row for row in overture_rows if str(row.get("category") or "") in merge_category_set
    ]

    prepared_osm_rows = _prepare_osm_source_rows(merge_osm)
    prepared_osm_rows = sorted(prepared_osm_rows, key=_prepared_osm_merge_sort_key)
    for row_id, prepared_row in enumerate(prepared_osm_rows, start=1):
        prepared_row["row_id"] = row_id

    prepared_overture_rows: list[dict[str, Any]] = []
    for row_id, row in enumerate(
        sorted(merge_overture, key=_stable_overture_row_sort_key),
        start=1,
    ):
        lat, lon = _point_lat_lon(row)
        prepared_overture_rows.append(
            {
                "row_id": row_id,
                "category": str(row.get("category") or ""),
                "source_ref": str(row.get("source_ref") or ""),
                "lat": lat,
                "lon": lon,
                "aliases": build_overture_aliases(row),
                "row": row,
            }
        )

    return {
        "merge_categories": tuple(sorted(merge_category_set)),
        "prepared_osm_rows": prepared_osm_rows,
        "prepared_overture_rows": prepared_overture_rows,
        "passthrough_osm_rows": passthrough_osm,
    }


def _collapse_candidate_pairs(
    candidate_pairs: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    collapsed: dict[tuple[int, int], dict[str, Any]] = {}
    for candidate in candidate_pairs:
        osm_row_id = int(candidate["osm_row_id"])
        overture_row_id = int(candidate["overture_row_id"])
        key = (osm_row_id, overture_row_id)
        current = collapsed.get(key)
        same_category = bool(candidate.get("same_category", False))
        aliases_agree = bool(candidate.get("aliases_agree", False))
        distance_m = float(candidate.get("distance_m", math.inf))
        if current is None:
            collapsed[key] = {
                "osm_row_id": osm_row_id,
                "overture_row_id": overture_row_id,
                "same_category": same_category,
                "aliases_agree": aliases_agree,
                "distance_m": distance_m,
            }
            continue
        current["same_category"] = bool(current["same_category"] or same_category)
        current["aliases_agree"] = bool(current["aliases_agree"] or aliases_agree)
        current["distance_m"] = min(float(current["distance_m"]), distance_m)
    return sorted(
        collapsed.values(),
        key=lambda candidate: (
            int(candidate["osm_row_id"]),
            int(candidate["overture_row_id"]),
        ),
    )


def _collapse_osm_self_candidate_pairs(
    candidate_pairs: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    collapsed: dict[tuple[int, int], dict[str, Any]] = {}
    for candidate in candidate_pairs:
        left_osm_row_id = int(candidate["left_osm_row_id"])
        right_osm_row_id = int(candidate["right_osm_row_id"])
        if left_osm_row_id == right_osm_row_id:
            continue
        key = (
            min(left_osm_row_id, right_osm_row_id),
            max(left_osm_row_id, right_osm_row_id),
        )
        current = collapsed.get(key)
        distance_m = float(candidate.get("distance_m", math.inf))
        if current is None:
            collapsed[key] = {
                "left_osm_row_id": key[0],
                "right_osm_row_id": key[1],
                "distance_m": distance_m,
            }
            continue
        current["distance_m"] = min(float(current["distance_m"]), distance_m)
    return sorted(
        collapsed.values(),
        key=lambda candidate: (
            int(candidate["left_osm_row_id"]),
            int(candidate["right_osm_row_id"]),
        ),
    )


def _claim_best_candidate(
    candidates: list[dict[str, Any]],
    *,
    used_overture_ids: set[int],
) -> dict[str, Any] | None:
    for candidate in candidates:
        overture_row_id = int(candidate["overture_row_id"])
        if overture_row_id in used_overture_ids:
            continue
        return candidate
    return None


def collapse_prepared_osm_source_duplicates(
    prepared_osm_rows: list[dict[str, Any]],
    candidate_pairs: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    collapsed_candidate_pairs = _collapse_osm_self_candidate_pairs(candidate_pairs)
    parents = {
        int(prepared_row["row_id"]): int(prepared_row["row_id"])
        for prepared_row in prepared_osm_rows
    }

    def _find(row_id: int) -> int:
        parent = parents[row_id]
        if parent != row_id:
            parents[row_id] = _find(parent)
        return parents[row_id]

    def _union(left_row_id: int, right_row_id: int) -> None:
        left_root = _find(left_row_id)
        right_root = _find(right_row_id)
        if left_root == right_root:
            return
        if left_root < right_root:
            parents[right_root] = left_root
        else:
            parents[left_root] = right_root

    for candidate in collapsed_candidate_pairs:
        _union(
            int(candidate["left_osm_row_id"]),
            int(candidate["right_osm_row_id"]),
        )

    grouped_rows: dict[int, list[dict[str, Any]]] = {}
    for prepared_row in prepared_osm_rows:
        grouped_rows.setdefault(_find(int(prepared_row["row_id"])), []).append(prepared_row)

    deduped_rows: list[dict[str, Any]] = []
    duplicate_rows_removed = 0
    duplicate_cluster_count = 0
    duplicates_by_category: dict[str, int] = {}

    for group in grouped_rows.values():
        canonical_prepared_row = min(group, key=_prepared_osm_duplicate_canonical_key)
        canonical_row = dict(canonical_prepared_row["row"])
        canonical_row["park_area_m2"] = max(
            _park_area_m2(prepared_row["row"]) for prepared_row in group
        )
        deduped_rows.append(canonical_row)
        if len(group) <= 1:
            continue
        duplicate_cluster_count += 1
        removed_count = len(group) - 1
        duplicate_rows_removed += removed_count
        category = str(canonical_row.get("category") or "")
        duplicates_by_category[category] = duplicates_by_category.get(category, 0) + removed_count

    return (
        sorted(deduped_rows, key=_stable_osm_row_sort_key),
        {
            "osm_self_candidate_count": len(collapsed_candidate_pairs),
            "osm_duplicate_cluster_count": int(duplicate_cluster_count),
            "osm_duplicate_rows_removed": int(duplicate_rows_removed),
            "osm_duplicates_by_category": {
                str(category): int(count)
                for category, count in sorted(duplicates_by_category.items())
            },
        },
    )


def deduplicate_osm_source_rows_from_candidate_pairs(
    osm_rows: list[dict[str, Any]],
    candidate_pairs: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prepared_osm_rows = prepare_osm_rows_for_self_dedupe(osm_rows)
    return collapse_prepared_osm_source_duplicates(prepared_osm_rows, candidate_pairs)


def _naive_osm_self_candidate_pairs(
    prepared_osm_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidate_pairs: list[dict[str, Any]] = []
    for index, left_row in enumerate(prepared_osm_rows):
        if not left_row["aliases"]:
            continue
        for right_row in prepared_osm_rows[index + 1 :]:
            if left_row["category"] != right_row["category"]:
                continue
            if not right_row["aliases"]:
                continue
            if not _aliases_agree(left_row["aliases"], right_row["aliases"]):
                continue
            distance_m = _distance_m_from_lat_lon(
                float(left_row["lat"]),
                float(left_row["lon"]),
                float(right_row["lat"]),
                float(right_row["lon"]),
            )
            if distance_m > OSM_SELF_DEDUPE_RADIUS_M:
                continue
            candidate_pairs.append(
                {
                    "left_osm_row_id": int(left_row["row_id"]),
                    "right_osm_row_id": int(right_row["row_id"]),
                    "distance_m": distance_m,
                }
            )
    return candidate_pairs


def deduplicate_osm_source_rows(
    osm_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prepared_osm_rows = prepare_osm_rows_for_self_dedupe(osm_rows)
    candidate_pairs = _naive_osm_self_candidate_pairs(prepared_osm_rows)
    return collapse_prepared_osm_source_duplicates(prepared_osm_rows, candidate_pairs)


def _merge_prepared_rows(
    prepared: dict[str, Any],
    candidate_pairs: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    prepared_osm_rows = prepared["prepared_osm_rows"]
    prepared_overture_rows = prepared["prepared_overture_rows"]
    passthrough_osm_rows = prepared["passthrough_osm_rows"]

    collapsed_candidates = _collapse_candidate_pairs(candidate_pairs)
    candidates_by_osm_row: dict[int, list[dict[str, Any]]] = {}
    for candidate in collapsed_candidates:
        candidates_by_osm_row.setdefault(int(candidate["osm_row_id"]), []).append(candidate)

    prepared_overture_by_id = {
        int(row["row_id"]): row
        for row in prepared_overture_rows
    }

    merged_rows: list[dict[str, Any]] = [
        _canonical_osm_row(row, conflict_class="osm_only")
        for row in passthrough_osm_rows
    ]
    used_overture_ids: set[int] = set()

    for prepared_osm_row in prepared_osm_rows:
        osm_row_id = int(prepared_osm_row["row_id"])
        raw_candidates = candidates_by_osm_row.get(osm_row_id, [])
        same_category_candidates = sorted(
            (
                candidate
                for candidate in raw_candidates
                if bool(candidate.get("same_category", False))
            ),
            key=lambda candidate: _candidate_match_key(
                float(candidate["distance_m"]),
                bool(candidate["aliases_agree"]),
                prepared_overture_by_id[int(candidate["overture_row_id"])]["source_ref"],
                int(candidate["overture_row_id"]),
            ),
        )
        same_category_match = _claim_best_candidate(
            same_category_candidates,
            used_overture_ids=used_overture_ids,
        )
        if same_category_match is not None:
            overture_row_id = int(same_category_match["overture_row_id"])
            used_overture_ids.add(overture_row_id)
            merged_rows.append(
                _canonical_osm_row(
                    prepared_osm_row["row"],
                    conflict_class="source_agreement",
                    overture_row=prepared_overture_by_id[overture_row_id]["row"],
                )
            )
            continue

        cross_category_candidates = sorted(
            (
                candidate
                for candidate in raw_candidates
                if not bool(candidate.get("same_category", False))
            ),
            key=lambda candidate: _candidate_match_key(
                float(candidate["distance_m"]),
                bool(candidate["aliases_agree"]),
                prepared_overture_by_id[int(candidate["overture_row_id"])]["source_ref"],
                int(candidate["overture_row_id"]),
            ),
        )
        cross_category_match = _claim_best_candidate(
            cross_category_candidates,
            used_overture_ids=used_overture_ids,
        )
        if cross_category_match is not None:
            overture_row_id = int(cross_category_match["overture_row_id"])
            used_overture_ids.add(overture_row_id)
            merged_rows.append(
                _canonical_osm_row(
                    prepared_osm_row["row"],
                    conflict_class="source_conflict",
                    overture_row=prepared_overture_by_id[overture_row_id]["row"],
                )
            )
            continue

        merged_rows.append(
            _canonical_osm_row(prepared_osm_row["row"], conflict_class="osm_only")
        )

    if len(used_overture_ids) != len(set(used_overture_ids)):
        raise RuntimeError("Amenity merge assigned an Overture row more than once.")

    for prepared_overture_row in prepared_overture_rows:
        if int(prepared_overture_row["row_id"]) in used_overture_ids:
            continue
        merged_rows.append(_canonical_overture_row(prepared_overture_row["row"]))

    return sorted(
        merged_rows,
        key=lambda row: (
            str(row.get("category") or ""),
            str(row.get("source") or ""),
            str(row.get("source_ref") or ""),
            float(row.get("lat") or 0.0),
            float(row.get("lon") or 0.0),
        ),
    )


def _naive_candidate_pairs(prepared: dict[str, Any]) -> list[dict[str, Any]]:
    prepared_osm_rows = prepared["prepared_osm_rows"]
    prepared_overture_rows = prepared["prepared_overture_rows"]
    candidate_pairs: list[dict[str, Any]] = []
    for prepared_osm_row in prepared_osm_rows:
        for prepared_overture_row in prepared_overture_rows:
            same_category = (
                prepared_osm_row["category"] == prepared_overture_row["category"]
            )
            aliases_agree = _aliases_agree(
                prepared_osm_row["aliases"],
                prepared_overture_row["aliases"],
            )
            distance_m = _distance_m_from_lat_lon(
                float(prepared_osm_row["lat"]),
                float(prepared_osm_row["lon"]),
                float(prepared_overture_row["lat"]),
                float(prepared_overture_row["lon"]),
            )
            if distance_m > AUTO_MATCH_RADIUS_M and not (
                aliases_agree and distance_m <= NAME_MATCH_RADIUS_M
            ):
                continue
            candidate_pairs.append(
                {
                    "osm_row_id": int(prepared_osm_row["row_id"]),
                    "overture_row_id": int(prepared_overture_row["row_id"]),
                    "same_category": same_category,
                    "aliases_agree": aliases_agree,
                    "distance_m": distance_m,
                }
            )
    return candidate_pairs


def merge_source_amenity_rows_from_candidate_pairs(
    osm_rows: list[dict[str, Any]],
    overture_rows: list[dict[str, Any]],
    candidate_pairs: Iterable[dict[str, Any]],
    *,
    scoring_categories: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    prepared = prepare_rows_for_merge(
        osm_rows,
        overture_rows,
        scoring_categories=scoring_categories,
    )
    return _merge_prepared_rows(prepared, candidate_pairs)


def merge_source_amenity_rows(
    osm_rows: list[dict[str, Any]],
    overture_rows: list[dict[str, Any]],
    *,
    scoring_categories: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    deduped_osm_rows, _ = deduplicate_osm_source_rows(osm_rows)
    prepared = prepare_rows_for_merge(
        deduped_osm_rows,
        overture_rows,
        scoring_categories=scoring_categories,
    )
    candidate_pairs = _naive_candidate_pairs(prepared)
    return _merge_prepared_rows(prepared, candidate_pairs)


def prepare_rows_for_merge(
    osm_rows: list[dict[str, Any]],
    overture_rows: list[dict[str, Any]],
    *,
    scoring_categories: Iterable[str] | None = None,
) -> dict[str, Any]:
    return _prepare_rows(
        osm_rows,
        overture_rows,
        merge_categories=resolve_merge_categories(scoring_categories),
    )


__all__ = [
    "AUTO_MATCH_RADIUS_M",
    "EXPECTED_BASELINE_MERGE_CATEGORIES",
    "NAME_MATCH_RADIUS_M",
    "OSM_SELF_DEDUPE_RADIUS_M",
    "build_osm_aliases",
    "build_overture_aliases",
    "collapse_prepared_osm_source_duplicates",
    "deduplicate_osm_source_rows",
    "deduplicate_osm_source_rows_from_candidate_pairs",
    "merge_category_warning",
    "merge_source_amenity_rows",
    "merge_source_amenity_rows_from_candidate_pairs",
    "prepare_osm_rows_for_self_dedupe",
    "prepare_rows_for_merge",
    "resolve_merge_categories",
]
