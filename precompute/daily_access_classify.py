"""Daily access score (v4) classification of shops, medical and green rows.

Pure functions over source rows (OSM import rows with ``tags_json`` or Overture
rows with ``raw_primary_category``). Every eligible row maps to exactly one
``service_kind`` with a quality that is constant inside the kind; ineligible
rows carry a diagnostic exclusion reason instead.

Green rows only get a semantic type and an access verdict here: their quality
depends on the merged polygon area and is assigned in
``db_postgis/daily_access_green.py``.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from config import (
    DAILY_ACCESS_BRAND_AMBIGUOUS_SHOP_VALUES,
    DAILY_ACCESS_DEDUPE_RADIUS_M,
    DAILY_ACCESS_EXCLUDED_SHOP_VALUES,
    DAILY_ACCESS_GENERAL_SPECIALITIES,
    DAILY_ACCESS_GREEN_AREA_CLASSES,
    DAILY_ACCESS_GREEN_DENIED_ACCESS_VALUES,
    DAILY_ACCESS_GREEN_EXPLICIT_ACCESS_TYPES,
    DAILY_ACCESS_GREEN_FIXED_QUALITY,
    DAILY_ACCESS_GREEN_LARGEST_AREA_CLASS,
    DAILY_ACCESS_GREEN_PUBLIC_ACCESS_VALUES,
    DAILY_ACCESS_GREEN_UNKNOWN_AREA_QUALITY,
    DAILY_ACCESS_NON_NARROW_SPECIALITIES,
    DAILY_ACCESS_OVERTURE_BRAND_AMBIGUOUS_VALUES,
    DAILY_ACCESS_OVERTURE_FOOD_CONVENIENCE_VALUES,
    DAILY_ACCESS_OVERTURE_FOOD_FULL_VALUES,
    DAILY_ACCESS_OVERTURE_FOOD_SPECIALTY_VALUES,
    DAILY_ACCESS_OVERTURE_GREEN_TYPES,
    DAILY_ACCESS_PRIMARY_CENTRE_NAME_PATTERN,
    DAILY_ACCESS_SERVICE_KINDS,
    DAILY_ACCESS_SPECIALISED_NAME_TERMS,
    DAILY_ACCESS_SPECIALTY_FOOD_SHOP_VALUES,
    SHOP_CORNER_CHAINS,
    SHOP_SUPERMARKET_CHAINS,
    TO_TARGET,
)

from .amenity_tiers import _matches_chain


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_PRIMARY_CENTRE_NAME_RE = re.compile(DAILY_ACCESS_PRIMARY_CENTRE_NAME_PATTERN)

GREEN_SEMANTIC_TYPES = (
    "national_park",
    "nature_reserve",
    "park",
    "playground",
    "garden",
    "common",
    "village_green",
    "recreation_ground",
)
# Mirrors green_landuse_values in osm2pgsql_livability.lua.
_GREEN_LANDUSE_TYPES = frozenset({"village_green", "recreation_ground"})


@dataclass(frozen=True)
class DailyAccessClass:
    need: str
    service_kind: str
    quality: float
    reason: str


@dataclass(frozen=True)
class GreenClass:
    semantic_type: str
    reason: str


@dataclass(frozen=True)
class Exclusion:
    reason: str


@dataclass
class LogicalPlace:
    place_id: str
    service_kind: str
    need: str
    quality: float
    members: list[dict[str, Any]] = field(default_factory=list)


def _norm(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _tags(row: dict[str, Any]) -> dict[str, Any]:
    tags = row.get("tags_json")
    return tags if isinstance(tags, dict) else {}


def _is_overture(row: dict[str, Any]) -> bool:
    return str(row.get("source") or "") == "overture_places"


def normalized_name(value: Any) -> str:
    return " ".join(_NON_ALNUM_RE.sub(" ", _norm(value)).split())


def service_class(service_kind: str, reason: str) -> DailyAccessClass:
    need, quality = DAILY_ACCESS_SERVICE_KINDS[service_kind]
    return DailyAccessClass(
        need=need,
        service_kind=service_kind,
        quality=float(quality),
        reason=reason,
    )


# --- shops ---------------------------------------------------------------


def _brand_food_kind(row: dict[str, Any]) -> str | None:
    if _matches_chain(row, SHOP_SUPERMARKET_CHAINS):
        return "food_full"
    if _matches_chain(row, SHOP_CORNER_CHAINS):
        return "food_convenience"
    return None


def classify_shop(row: dict[str, Any]) -> DailyAccessClass | Exclusion:
    if _is_overture(row):
        return _classify_overture_shop(row)
    shop = _norm(_tags(row).get("shop"))
    if not shop:
        return Exclusion("no_shop_tag")
    if shop in DAILY_ACCESS_EXCLUDED_SHOP_VALUES:
        return Exclusion("excluded_shop_value")
    # An explicit supermarket tag wins over the Spar/Centra brand heuristic.
    if shop == "supermarket":
        return service_class("food_full", "explicit_supermarket")
    if shop == "convenience":
        return service_class("food_convenience", "explicit_convenience")
    if shop in DAILY_ACCESS_SPECIALTY_FOOD_SHOP_VALUES:
        return service_class("food_specialty", "specialty_food_shop")
    if shop in DAILY_ACCESS_BRAND_AMBIGUOUS_SHOP_VALUES:
        brand_kind = _brand_food_kind(row)
        if brand_kind is not None:
            return service_class(brand_kind, "brand_heuristic")
        if shop == "grocery":
            return service_class("food_convenience", "grocery_without_brand")
        if shop == "kiosk":
            return service_class("other_retail", "kiosk_without_food")
    return service_class("other_retail", "other_retail")


def _classify_overture_shop(row: dict[str, Any]) -> DailyAccessClass | Exclusion:
    category = _norm(row.get("raw_primary_category"))
    if category in DAILY_ACCESS_OVERTURE_FOOD_FULL_VALUES:
        return service_class("food_full", "overture_supermarket")
    if category in DAILY_ACCESS_OVERTURE_BRAND_AMBIGUOUS_VALUES:
        brand_kind = _brand_food_kind(row)
        if brand_kind is not None:
            return service_class(brand_kind, "brand_heuristic")
    if category in DAILY_ACCESS_OVERTURE_FOOD_CONVENIENCE_VALUES:
        return service_class("food_convenience", "overture_convenience")
    if category in DAILY_ACCESS_OVERTURE_FOOD_SPECIALTY_VALUES:
        return service_class("food_specialty", "overture_specialty_food")
    return service_class("other_retail", "overture_other_retail")


# --- medical -------------------------------------------------------------


def _speciality_values(tags: dict[str, Any]) -> list[str]:
    raw = tags.get("healthcare:speciality")
    if not isinstance(raw, str):
        return []
    return [part.strip().lower() for part in raw.split(";") if part.strip()]


def speciality_verdict(tags: dict[str, Any]) -> str:
    """``none`` (no tag), ``general`` or ``narrow``.

    A list that names any general/broad value counts as general (a GP with an
    extra speciality); only a list of narrow values is narrow.
    """
    values = _speciality_values(tags)
    if not values:
        return "none"
    broad = DAILY_ACCESS_GENERAL_SPECIALITIES | DAILY_ACCESS_NON_NARROW_SPECIALITIES
    if any(value in broad for value in values):
        return "general"
    return "narrow"


def has_specialised_name(name: Any) -> bool:
    tokens = normalized_name(name).split()
    return any(
        token.startswith(term)
        for token in tokens
        for term in DAILY_ACCESS_SPECIALISED_NAME_TERMS
    )


def is_primary_centre_name(name: Any) -> bool:
    normalized = normalized_name(name)
    if not normalized or has_specialised_name(normalized):
        return False
    return bool(_PRIMARY_CENTRE_NAME_RE.search(normalized))


def _row_name(row: dict[str, Any]) -> Any:
    return row.get("name") or _tags(row).get("name")


def _classify_centre(row: dict[str, Any], speciality: str) -> DailyAccessClass | Exclusion:
    # Explicit speciality beats the name; the name is the fallback.
    if speciality == "general":
        return service_class("primary_care_clinic", "centre_general_speciality")
    if speciality == "narrow":
        return Exclusion("specialised_care")
    if is_primary_centre_name(_row_name(row)):
        return service_class("primary_care_clinic", "primary_centre_name")
    return Exclusion("centre_without_primary_name")


def classify_medical(row: dict[str, Any]) -> DailyAccessClass | Exclusion:
    if _is_overture(row):
        return _classify_overture_medical(row)
    tags = _tags(row)
    amenity = _norm(tags.get("amenity"))
    healthcare = _norm(tags.get("healthcare"))
    if amenity == "pharmacy" or healthcare == "pharmacy":
        return service_class("pharmacy", "pharmacy")
    speciality = speciality_verdict(tags)
    if amenity == "doctors" or healthcare == "doctor":
        if speciality == "narrow":
            return Exclusion("specialised_care")
        return service_class("primary_care_gp", "doctors")
    if amenity == "health_centre" or healthcare == "centre":
        return _classify_centre(row, speciality)
    if amenity == "clinic" or healthcare == "clinic":
        if speciality == "narrow":
            return Exclusion("specialised_care")
        if speciality == "none" and has_specialised_name(_row_name(row)):
            return Exclusion("specialised_name")
        return service_class("primary_care_clinic", "general_clinic")
    if amenity == "hospital" or healthcare == "hospital":
        return Exclusion("hospital")
    if amenity == "dentist" or healthcare == "dentist":
        return Exclusion("dentistry")
    return Exclusion("not_primary_care")


def _classify_overture_medical(row: dict[str, Any]) -> DailyAccessClass | Exclusion:
    category = _norm(row.get("raw_primary_category"))
    if category == "pharmacy":
        return service_class("pharmacy", "overture_pharmacy")
    if category == "doctor":
        return service_class("primary_care_gp", "overture_doctor")
    if category == "medical_clinic":
        if has_specialised_name(_row_name(row)):
            return Exclusion("specialised_name")
        return service_class("primary_care_clinic", "overture_clinic")
    if category == "health_center":
        return _classify_centre(row, "none")
    if category == "hospital":
        return Exclusion("hospital")
    if category == "dentist":
        return Exclusion("dentistry")
    return Exclusion("not_primary_care")


# --- green ---------------------------------------------------------------


def green_semantic_type(row: dict[str, Any]) -> str | None:
    if _is_overture(row):
        return DAILY_ACCESS_OVERTURE_GREEN_TYPES.get(_norm(row.get("raw_primary_category")))
    tags = _tags(row)
    if _norm(tags.get("boundary")) == "national_park":
        return "national_park"
    leisure = _norm(tags.get("leisure"))
    if leisure in GREEN_SEMANTIC_TYPES and leisure != "national_park":
        return leisure
    landuse = _norm(tags.get("landuse"))
    if landuse in _GREEN_LANDUSE_TYPES:
        return landuse
    return None


def effective_foot_access(tags: dict[str, Any]) -> str:
    """Explicit ``foot`` overrides the general ``access`` tag."""
    foot = _norm(tags.get("foot"))
    if foot:
        return foot
    return _norm(tags.get("access"))


def classify_green(row: dict[str, Any]) -> GreenClass | Exclusion:
    semantic_type = green_semantic_type(row)
    if semantic_type is None:
        return Exclusion("not_green")
    tags = _tags(row)
    if semantic_type == "garden" and (
        _norm(tags.get("garden:type")) == "residential"
        or _norm(tags.get("garden")) == "residential"
    ):
        return Exclusion("residential_garden")
    access = effective_foot_access(tags)
    if access in DAILY_ACCESS_GREEN_DENIED_ACCESS_VALUES:
        return Exclusion("access_denied")
    if semantic_type in DAILY_ACCESS_GREEN_EXPLICIT_ACCESS_TYPES:
        if access not in DAILY_ACCESS_GREEN_PUBLIC_ACCESS_VALUES:
            return Exclusion("access_not_public")
    return GreenClass(semantic_type=semantic_type, reason="eligible")


def green_area_class(semantic_type: str, area_m2: float | None) -> tuple[str, float]:
    """(class, quality) for a green site; ``area_m2=None`` means a point."""
    fixed = DAILY_ACCESS_GREEN_FIXED_QUALITY.get(semantic_type)
    if fixed is not None:
        return semantic_type, float(fixed)
    if area_m2 is None:
        return "unknown_area", float(DAILY_ACCESS_GREEN_UNKNOWN_AREA_QUALITY)
    for upper_bound_m2, class_name, quality in DAILY_ACCESS_GREEN_AREA_CLASSES:
        if float(area_m2) < float(upper_bound_m2):
            return class_name, float(quality)
    class_name, quality = DAILY_ACCESS_GREEN_LARGEST_AREA_CLASS
    return class_name, float(quality)


def green_service_kind(quality: float) -> str:
    for kind, (need, kind_quality) in DAILY_ACCESS_SERVICE_KINDS.items():
        if need == "green" and abs(float(kind_quality) - float(quality)) < 1e-9:
            return kind
    raise ValueError(f"No green service_kind has quality {quality!r}")


# --- dispatch + logical places --------------------------------------------


def classify_daily_access_row(row: dict[str, Any]) -> DailyAccessClass | GreenClass | Exclusion:
    category = str(row.get("category") or "")
    if category == "shops":
        return classify_shop(row)
    if category == "healthcare":
        return classify_medical(row)
    if category == "parks":
        return classify_green(row)
    return Exclusion("unsupported_category")


def _row_lat_lon(row: dict[str, Any]) -> tuple[float, float]:
    geom = row.get("geom")
    if geom is not None:
        return float(geom.y), float(geom.x)
    return float(row["lat"]), float(row["lon"])


def classify_service_rows(
    rows: list[dict[str, Any]],
    *,
    source_row_lookup: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Classify merged shop/medical rows; green rows are skipped here.

    ``source_row_lookup`` maps ``(source, source_ref)`` to the pre-merge row so
    tags, Overture categories and shop footprints survive the merge.
    """
    lookup = source_row_lookup or {}
    classified: list[dict[str, Any]] = []
    input_rows: dict[str, int] = defaultdict(int)
    eligible_rows: dict[str, int] = defaultdict(int)
    excluded_rows: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    reasons: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        category = str(row.get("category") or "")
        if category not in {"shops", "healthcare"}:
            continue
        input_rows[category] += 1
        key = (str(row.get("source") or ""), str(row.get("source_ref") or ""))
        source_row = {**lookup.get(key, row), "category": category}
        verdict = classify_daily_access_row(source_row)
        if isinstance(verdict, Exclusion):
            excluded_rows[category][verdict.reason] += 1
            continue
        assert isinstance(verdict, DailyAccessClass)
        eligible_rows[verdict.service_kind] += 1
        reasons[verdict.service_kind][verdict.reason] += 1
        lat, lon = _row_lat_lon(row)
        classified.append(
            {
                "category": category,
                "source": key[0],
                "source_ref": key[1],
                "name": row.get("name"),
                "lat": lat,
                "lon": lon,
                "need": verdict.need,
                "service_kind": verdict.service_kind,
                "quality": verdict.quality,
                "class_reason": verdict.reason,
                "footprint_wkb": source_row.get("footprint_wkb"),
                # Dedupe evidence (v4): address tags, Overture address, confirmed ids.
                "tags_json": _tags(source_row),
                "address_freeform": source_row.get("address_freeform"),
                "address_postcode": source_row.get("address_postcode"),
                "identity_refs": tuple(source_row.get("identity_refs") or ()),
            }
        )
    stats = {
        "input_rows": dict(sorted(input_rows.items())),
        "eligible_rows_by_service_kind": dict(sorted(eligible_rows.items())),
        "excluded_rows": {
            category: dict(sorted(by_reason.items()))
            for category, by_reason in sorted(excluded_rows.items())
        },
        "class_reasons": {
            kind: dict(sorted(by_reason.items())) for kind, by_reason in sorted(reasons.items())
        },
    }
    return classified, stats


# --- logical places (v4 dedupe) ---------------------------------------------
#
# Rows are classified one by one *before* any merge; duplicates are then
# grouped inside one need only on evidence:
#
# * a confirmed id of the same object (``identity_refs`` intersect);
# * the same normalised name and the same full address;
# * the same normalised name and a confirmed shared footprint (one point
#   inside the other's footprint, or footprints overlapping by half).
#
# ``DAILY_ACCESS_DEDUPE_RADIUS_M`` only finds candidates. Groups are cliques:
# every pair satisfies the rule, so A~B and B~C never chain A to C.

# Lower is stronger evidence for the canonical service kind of a group.
_EVIDENCE_RANK = {
    "explicit_supermarket": 0,
    "explicit_convenience": 0,
    "specialty_food_shop": 0,
    "pharmacy": 0,
    "doctors": 0,
    "general_clinic": 0,
    "centre_general_speciality": 0,
    "overture_supermarket": 1,
    "overture_convenience": 1,
    "overture_specialty_food": 1,
    "overture_pharmacy": 1,
    "overture_doctor": 1,
    "overture_clinic": 1,
    "brand_heuristic": 2,
    "grocery_without_brand": 2,
    "kiosk_without_food": 2,
    "primary_centre_name": 2,
    "other_retail": 3,
    "overture_other_retail": 3,
}
_FOOTPRINT_OVERLAP_SHARE = 0.5


def evidence_rank(class_reason: Any) -> int:
    return _EVIDENCE_RANK.get(str(class_reason or ""), 4)


def namespaced_ref(row: dict[str, Any]) -> str:
    source = str(row.get("source") or "")
    namespace = {"osm_local_pbf": "osm", "overture_places": "overture"}.get(source, source or "unknown")
    return f"{namespace}:{row.get('source_ref') or ''}"


def normalized_address(row: dict[str, Any]) -> tuple[str, str] | None:
    """``(house number + street, postcode)`` when the address is complete."""
    if _is_overture(row):
        street = normalized_name(row.get("address_freeform"))
        postcode = normalized_name(row.get("address_postcode")).replace(" ", "")
        if not street or not any(token[0].isdigit() for token in street.split()):
            return None
        return street, postcode
    tags = _tags(row)
    number = normalized_name(tags.get("addr:housenumber"))
    street = normalized_name(tags.get("addr:street"))
    if not number or not street:
        return None
    return f"{number} {street}", normalized_name(tags.get("addr:postcode")).replace(" ", "")


def _identity_refs(row: dict[str, Any]) -> frozenset[str]:
    refs = {namespaced_ref(row)}
    refs.update(str(ref) for ref in row.get("identity_refs") or ())
    return frozenset(refs)


def _footprint(row: dict[str, Any]):
    wkb = row.get("footprint_wkb")
    if not wkb:
        return None
    import shapely

    geometry = shapely.from_wkb(wkb)
    return None if geometry.is_empty else geometry


def _same_footprint(a: dict[str, Any], b: dict[str, Any], xy_a, xy_b) -> bool:
    import shapely

    fa, fb = _footprint(a), _footprint(b)
    if fa is not None and fb is not None:
        overlap = shapely.area(shapely.intersection(fa, fb))
        return overlap >= _FOOTPRINT_OVERLAP_SHARE * min(fa.area, fb.area) > 0.0
    if fa is not None:
        return bool(shapely.covers(fa, shapely.Point(*xy_b)))
    if fb is not None:
        return bool(shapely.covers(fb, shapely.Point(*xy_a)))
    return False


def duplicate_evidence(a: dict[str, Any], b: dict[str, Any], xy_a, xy_b) -> str | None:
    """Why two classified rows of one need are one object, or ``None``."""
    if _identity_refs(a) & _identity_refs(b):
        return "confirmed_id"
    name_a = normalized_name(a.get("name"))
    if not name_a or name_a != normalized_name(b.get("name")):
        return None
    address_a, address_b = normalized_address(a), normalized_address(b)
    if address_a is not None and address_b is not None and address_a[0] == address_b[0]:
        if not address_a[1] or not address_b[1] or address_a[1] == address_b[1]:
            return "name_and_address"
    if _same_footprint(a, b, xy_a, xy_b):
        return "name_and_footprint"
    return None


def _candidate_pairs(rows: list[dict[str, Any]], points_xy: np.ndarray, radius_m: float) -> set[tuple[int, int]]:
    from scipy.spatial import cKDTree

    pairs: set[tuple[int, int]] = set()
    if len(rows) < 2:
        return pairs
    tree = cKDTree(points_xy)
    for i, j in tree.query_pairs(float(radius_m)):
        pairs.add((min(i, j), max(i, j)))
    # Footprints larger than the radius: points inside them are candidates too.
    for i, row in enumerate(rows):
        footprint = _footprint(row)
        if footprint is None:
            continue
        min_x, min_y, max_x, max_y = footprint.bounds
        half = max(max_x - min_x, max_y - min_y) / 2.0 + float(radius_m)
        for j in tree.query_ball_point(points_xy[i], half):
            if j != i:
                pairs.add((min(i, j), max(i, j)))
    by_ref: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        for ref in _identity_refs(row):
            by_ref[ref].append(index)
    for indexes in by_ref.values():
        for position, i in enumerate(indexes):
            for j in indexes[position + 1 :]:
                pairs.add((min(i, j), max(i, j)))
    return pairs


def _row_points_xy(rows: list[dict[str, Any]]) -> np.ndarray:
    points = []
    for row in rows:
        if "x" in row and "y" in row:
            points.append((float(row["x"]), float(row["y"])))
        else:
            x, y = TO_TARGET(float(row["lon"]), float(row["lat"]))
            points.append((float(x), float(y)))
    return np.asarray(points, dtype=np.float64).reshape(-1, 2)


def build_logical_places(
    rows: list[dict[str, Any]],
    *,
    radius_m: float = DAILY_ACCESS_DEDUPE_RADIUS_M,
    stats_out: dict[str, Any] | None = None,
) -> list[LogicalPlace]:
    """Group classified rows of one need into logical places (see above).

    Rows need ``lat``/``lon`` (or ``x``/``y``), ``source``, ``source_ref``,
    ``need``/``service_kind`` and ``class_reason``. The canonical service kind
    of a group comes from its strongest evidence (explicit OSM tag, then
    specific Overture category, then brand/name heuristics). ``place_id`` is
    reproducible: canonical kind + the sorted member refs.
    """
    by_need: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        need = str(row.get("need") or DAILY_ACCESS_SERVICE_KINDS[str(row["service_kind"])][0])
        by_need[need].append(row)

    evidence_counts: dict[str, int] = defaultdict(int)
    places: list[LogicalPlace] = []
    for need in sorted(by_need):
        need_rows = sorted(
            by_need[need],
            key=lambda row: (evidence_rank(row.get("class_reason")), namespaced_ref(row)),
        )
        points_xy = _row_points_xy(need_rows)
        partners: dict[int, dict[int, str]] = defaultdict(dict)
        for i, j in sorted(_candidate_pairs(need_rows, points_xy, radius_m)):
            evidence = duplicate_evidence(need_rows[i], need_rows[j], points_xy[i], points_xy[j])
            if evidence is not None:
                partners[i][j] = evidence
                partners[j][i] = evidence
        groups: list[list[int]] = []
        group_of: dict[int, int] = {}
        for index in range(len(need_rows)):
            joined = None
            for group_index in sorted({group_of[other] for other in partners[index] if other in group_of}):
                if all(member in partners[index] for member in groups[group_index]):
                    joined = group_index
                    break
            if joined is None:
                group_of[index] = len(groups)
                groups.append([index])
            else:
                for member in groups[joined]:
                    evidence_counts[partners[index][member]] += 1
                group_of[index] = joined
                groups[joined].append(index)
        for group in groups:
            members = [need_rows[index] for index in group]
            # Members are in evidence order: the first one decides the kind.
            service_kind = str(members[0]["service_kind"])
            _need, quality = DAILY_ACCESS_SERVICE_KINDS[service_kind]
            refs = sorted(namespaced_ref(member) for member in members)
            places.append(
                LogicalPlace(
                    place_id=f"{service_kind}|{','.join(refs)}",
                    service_kind=service_kind,
                    need=need,
                    quality=float(quality),
                    members=members,
                )
            )
    if stats_out is not None:
        stats_out["duplicate_pairs_by_evidence"] = dict(sorted(evidence_counts.items()))
        stats_out["logical_places"] = len(places)
        stats_out["merged_rows"] = sum(len(place.members) - 1 for place in places)
    return sorted(places, key=lambda place: (place.service_kind, place.place_id))


def object_connector_distance_m(footprint_metric, point_metric) -> float:
    """Distance from a point to a shop footprint: 0 inside, else to the outline.

    Both geometries are shapely objects in the metric CRS (EPSG:2157). A point
    footprint degenerates to point-to-point distance.
    """
    if footprint_metric is None or footprint_metric.is_empty:
        raise ValueError("footprint geometry is required")
    if footprint_metric.geom_type in {"Polygon", "MultiPolygon"}:
        if footprint_metric.covers(point_metric):
            return 0.0
        return float(footprint_metric.boundary.distance(point_metric))
    return float(footprint_metric.distance(point_metric))


__all__ = [
    "DailyAccessClass",
    "Exclusion",
    "GREEN_SEMANTIC_TYPES",
    "GreenClass",
    "LogicalPlace",
    "build_logical_places",
    "duplicate_evidence",
    "evidence_rank",
    "namespaced_ref",
    "normalized_address",
    "classify_daily_access_row",
    "classify_green",
    "classify_medical",
    "classify_service_rows",
    "classify_shop",
    "effective_foot_access",
    "green_area_class",
    "green_semantic_type",
    "green_service_kind",
    "has_specialised_name",
    "is_primary_centre_name",
    "normalized_name",
    "object_connector_distance_m",
    "service_class",
    "speciality_verdict",
]
