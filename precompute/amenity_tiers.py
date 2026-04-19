from __future__ import annotations

import re
from typing import Any

from config import (
    HEALTHCARE_CLINIC_VALUES,
    HEALTHCARE_EMERGENCY_VALUES,
    HEALTHCARE_HOSPITAL_VALUES,
    HEALTHCARE_LOCAL_VALUES,
    HEALTHCARE_TIER_UNITS,
    OVERTURE_HEALTHCARE_CLINIC_VALUES,
    OVERTURE_HEALTHCARE_LOCAL_VALUES,
    OVERTURE_PARK_NEIGHBOURHOOD_VALUES,
    OVERTURE_PARK_POCKET_VALUES,
    OVERTURE_PARK_REGIONAL_VALUES,
    OVERTURE_SHOP_CORNER_VALUES,
    PARK_DISTRICT_MAX_AREA_M2,
    PARK_NEIGHBOURHOOD_MAX_AREA_M2,
    PARK_POCKET_MAX_AREA_M2,
    PARK_TIER_UNITS,
    SHOP_CORNER_CHAINS,
    SHOP_CORNER_VALUES,
    SHOP_MALL_MIN_FOOTPRINT_M2,
    SHOP_MALL_VALUES,
    SHOP_SMALL_SUPERMARKET_MAX_FOOTPRINT_M2,
    SHOP_SUPERMARKET_CHAINS,
    SHOP_SUPERMARKET_VALUES,
    SHOP_TIER_UNITS,
)


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WEIGHTED_CATEGORIES = frozenset({"shops", "healthcare", "parks"})


def _normalized_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _flatten_text_values(value: Any, *, depth: int = 0) -> list[str]:
    if value is None or depth > 4:
        return []
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if isinstance(value, dict):
        values: list[str] = []
        for child in value.values():
            values.extend(_flatten_text_values(child, depth=depth + 1))
        return values
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for child in value:
            values.extend(_flatten_text_values(child, depth=depth + 1))
        return values
    try:
        as_dict = value._asdict()  # type: ignore[attr-defined]
    except AttributeError:
        as_dict = None
    if isinstance(as_dict, dict):
        return _flatten_text_values(as_dict, depth=depth + 1)
    return []


def _numeric_value(value: Any) -> float:
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(numeric, 0.0)


def _tokenized_text_values(row: dict[str, Any]) -> set[str]:
    values: list[str] = []
    tags_json = row.get("tags_json") or {}
    if isinstance(tags_json, dict):
        for key in ("name", "brand", "operator"):
            values.extend(_flatten_text_values(tags_json.get(key)))
    for key in ("name", "brand"):
        values.extend(_flatten_text_values(row.get(key)))

    tokens: set[str] = set()
    for value in values:
        normalized = _NON_ALNUM_RE.sub(" ", value.strip().lower()).strip()
        if not normalized:
            continue
        tokens.update(part for part in normalized.split(" ") if part)
        tokens.add(normalized)
    return tokens


def _matches_chain(row: dict[str, Any], chains: frozenset[str]) -> bool:
    tokens = _tokenized_text_values(row)
    return any(chain in tokens for chain in chains)


def _shop_value(row: dict[str, Any]) -> str:
    tags_json = row.get("tags_json") or {}
    if isinstance(tags_json, dict):
        value = _normalized_text(tags_json.get("shop"))
        if value:
            return value
    return _normalized_text(row.get("raw_primary_category"))


def _healthcare_value(row: dict[str, Any]) -> str:
    tags_json = row.get("tags_json") or {}
    if isinstance(tags_json, dict):
        for key in ("amenity", "healthcare"):
            value = _normalized_text(tags_json.get(key))
            if value:
                return value
    return _normalized_text(row.get("raw_primary_category"))


def _park_value(row: dict[str, Any]) -> str:
    tags_json = row.get("tags_json") or {}
    if isinstance(tags_json, dict):
        for key in ("leisure", "natural"):
            value = _normalized_text(tags_json.get(key))
            if value:
                return value
    return _normalized_text(row.get("raw_primary_category"))


def _emergency_value(row: dict[str, Any]) -> str:
    tags_json = row.get("tags_json") or {}
    if not isinstance(tags_json, dict):
        return ""
    return _normalized_text(tags_json.get("emergency"))


def _classify_shop_tier(row: dict[str, Any]) -> str:
    shop_value = _shop_value(row)
    footprint_area_m2 = _numeric_value(row.get("footprint_area_m2"))
    source = str(row.get("source") or "")

    if shop_value in SHOP_MALL_VALUES or footprint_area_m2 >= SHOP_MALL_MIN_FOOTPRINT_M2:
        return "mall"
    if source == "overture_places" and shop_value in OVERTURE_SHOP_CORNER_VALUES:
        return "corner"
    if shop_value in SHOP_CORNER_VALUES or _matches_chain(row, SHOP_CORNER_CHAINS):
        return "corner"
    if shop_value in SHOP_SUPERMARKET_VALUES or _matches_chain(row, SHOP_SUPERMARKET_CHAINS):
        if (
            shop_value == "supermarket"
            and 0.0 < footprint_area_m2 < SHOP_SMALL_SUPERMARKET_MAX_FOOTPRINT_M2
        ):
            return "regular"
        return "supermarket"
    return "regular"


def _classify_healthcare_tier(row: dict[str, Any]) -> str:
    healthcare_value = _healthcare_value(row)
    if (
        healthcare_value in HEALTHCARE_HOSPITAL_VALUES
        and _emergency_value(row) in HEALTHCARE_EMERGENCY_VALUES
    ):
        return "emergency_hospital"
    if healthcare_value in HEALTHCARE_LOCAL_VALUES:
        return "local"
    if healthcare_value in OVERTURE_HEALTHCARE_LOCAL_VALUES:
        return "local"
    if healthcare_value in HEALTHCARE_CLINIC_VALUES:
        return "clinic"
    if healthcare_value in OVERTURE_HEALTHCARE_CLINIC_VALUES:
        return "clinic"
    if healthcare_value in HEALTHCARE_HOSPITAL_VALUES:
        return "hospital"
    return "local"


def _classify_park_tier(row: dict[str, Any]) -> str:
    if str(row.get("source") or "") == "overture_places":
        park_value = _park_value(row)
        if park_value in OVERTURE_PARK_POCKET_VALUES:
            return "pocket"
        if park_value in OVERTURE_PARK_REGIONAL_VALUES:
            return "regional"
        if park_value in OVERTURE_PARK_NEIGHBOURHOOD_VALUES:
            return "neighbourhood"
        return "neighbourhood"

    park_area_m2 = _numeric_value(row.get("park_area_m2"))
    if park_area_m2 < PARK_POCKET_MAX_AREA_M2:
        return "pocket"
    if park_area_m2 < PARK_NEIGHBOURHOOD_MAX_AREA_M2:
        return "neighbourhood"
    if park_area_m2 < PARK_DISTRICT_MAX_AREA_M2:
        return "district"
    return "regional"


def classify_amenity_row(row: dict[str, Any]) -> tuple[str | None, int]:
    category = str(row.get("category") or "")
    if category == "shops":
        tier = _classify_shop_tier(row)
        return tier, int(SHOP_TIER_UNITS[tier])
    if category == "healthcare":
        tier = _classify_healthcare_tier(row)
        return tier, int(HEALTHCARE_TIER_UNITS[tier])
    if category == "parks":
        tier = _classify_park_tier(row)
        return tier, int(PARK_TIER_UNITS[tier])
    if category == "transport":
        return "stop", 1
    return None, 0


def annotate_amenity_row(row: dict[str, Any]) -> dict[str, Any]:
    tier, score_units = classify_amenity_row(row)
    annotated = dict(row)
    annotated["tier"] = tier
    annotated["score_units"] = int(score_units)
    return annotated


def uses_weighted_units(category: str) -> bool:
    return str(category) in _WEIGHTED_CATEGORIES


__all__ = [
    "annotate_amenity_row",
    "classify_amenity_row",
    "uses_weighted_units",
]
