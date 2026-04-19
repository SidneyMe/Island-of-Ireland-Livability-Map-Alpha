from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from shapely.geometry import Point


OVERTURE_PLACES_PATH = Path(__file__).resolve().parent.parent / "overture" / "ireland_places.geoparquet"
OVERTURE_PLACES_STATE_PATH = OVERTURE_PLACES_PATH.with_suffix(
    OVERTURE_PLACES_PATH.suffix + ".state"
)

# Overture categories.primary values -> our canonical amenity categories.
# All unmapped values are silently dropped.
OVERTURE_CATEGORY_MAP: dict[str, str] = {
    "retail": "shops",
    "shopping": "shops",
    "grocery": "shops",
    "supermarket": "shops",
    "convenience_store": "shops",
    "clothing_store": "shops",
    "hardware_store": "shops",
    "food_and_beverage": "shops",
    "bakery": "shops",
    "health_and_medical": "healthcare",
    "hospital": "healthcare",
    "pharmacy": "healthcare",
    "dentist": "healthcare",
    "doctor": "healthcare",
    "health_center": "healthcare",
    "medical_clinic": "healthcare",
    "urgent_care_center": "healthcare",
    "parks_and_outdoors": "parks",
    "park": "parks",
    "nature_reserve": "parks",
    "playground": "parks",
    "recreation_ground": "parks",
    "national_park": "parks",
}


def is_available() -> bool:
    return OVERTURE_PLACES_PATH.exists()


def _file_meta(path: Path) -> dict[str, int]:
    try:
        stat = path.stat()
        return {"mtime_ns": int(stat.st_mtime_ns), "size": int(stat.st_size)}
    except OSError:
        return {"mtime_ns": 0, "size": 0}


def _read_state_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def dataset_info() -> dict[str, Any]:
    state = _read_state_file(OVERTURE_PLACES_STATE_PATH)
    meta = _file_meta(OVERTURE_PLACES_PATH)
    payload: dict[str, Any] = {
        "available": bool(is_available()),
        "path": str(OVERTURE_PLACES_PATH),
        "state_path": str(OVERTURE_PLACES_STATE_PATH),
        "file_size": meta["size"],
        "file_mtime_ns": meta["mtime_ns"],
    }
    if state:
        payload.update(state)
    return payload


def dataset_signature() -> str:
    payload = dataset_info()
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]


def category_map_signature() -> str:
    return hashlib.sha256(
        json.dumps(OVERTURE_CATEGORY_MAP, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]


def _extract_primary_category(cats: Any) -> str | None:
    """Extract categories.primary from whatever geopandas gives us."""
    if cats is None:
        return None
    if isinstance(cats, str):
        return cats
    if isinstance(cats, dict):
        return cats.get("primary")
    try:
        return cats.primary  # type: ignore[union-attr]
    except AttributeError:
        return None


def _parquet_column_names(path: Path) -> set[str]:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return set()

    try:
        return {str(name) for name in pq.read_schema(path).names}
    except Exception:
        return set()


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

    for attr_name in ("primary", "common", "preferred", "name", "value", "names", "brand"):
        try:
            child = getattr(value, attr_name)
        except AttributeError:
            continue
        values = _flatten_text_values(child, depth=depth + 1)
        if values:
            return values
    return []


def _extract_first_text(value: Any) -> str | None:
    for candidate in _flatten_text_values(value):
        if candidate:
            return candidate
    return None


def _extract_confidence(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        for key in ("score", "value", "confidence"):
            if key in value:
                return _extract_confidence(value[key])
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    for attr_name in ("score", "value", "confidence"):
        try:
            return _extract_confidence(getattr(value, attr_name))
        except AttributeError:
            continue
    return None


def load_overture_amenity_rows(study_area_wgs84: Any) -> list[dict[str, Any]]:
    """Load Overture places, filter to study area, and map categories."""
    if not is_available() or study_area_wgs84 is None:
        return []

    import geopandas as gpd

    available_columns = _parquet_column_names(OVERTURE_PLACES_PATH)
    columns = ["id", "geometry", "categories"]
    for optional_column in ("names", "brand", "brands", "confidence", "confidence_score"):
        if optional_column in available_columns:
            columns.append(optional_column)

    bbox = tuple(study_area_wgs84.bounds)
    gdf = gpd.read_parquet(
        OVERTURE_PLACES_PATH,
        columns=columns,
        bbox=bbox,
    )

    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf.loc[gdf.geometry.within(study_area_wgs84)]

    ids = gdf["id"].to_numpy()
    cats = gdf["categories"].to_numpy()
    geoms = gdf.geometry.to_numpy()
    names = gdf["names"].to_numpy() if "names" in gdf.columns else [None] * len(gdf)
    brands = (
        gdf["brands"].to_numpy()
        if "brands" in gdf.columns
        else gdf["brand"].to_numpy() if "brand" in gdf.columns else [None] * len(gdf)
    )
    confidence_values = (
        gdf["confidence"].to_numpy()
        if "confidence" in gdf.columns
        else gdf["confidence_score"].to_numpy()
        if "confidence_score" in gdf.columns
        else [None] * len(gdf)
    )

    rows: list[dict[str, Any]] = []
    for fid, cat, geom, name_value, brand_value, confidence_value in zip(
        ids,
        cats,
        geoms,
        names,
        brands,
        confidence_values,
    ):
        if geom is None or geom.is_empty:
            continue

        primary_cat = _extract_primary_category(cat)
        mapped = OVERTURE_CATEGORY_MAP.get(str(primary_cat or "").lower())
        if mapped is None:
            continue

        point = geom if geom.geom_type == "Point" else geom.centroid
        rows.append(
            {
                "category": mapped,
                "source": "overture_places",
                "source_ref": str(fid or ""),
                "raw_primary_category": str(primary_cat or "").lower() or None,
                "name": _extract_first_text(name_value),
                "brand": _extract_first_text(brand_value),
                "confidence": _extract_confidence(confidence_value),
                "geom": Point(float(point.x), float(point.y)),
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            }
        )

    return rows
