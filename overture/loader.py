from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from shapely.geometry import Point


OVERTURE_PLACES_PATH = Path(__file__).resolve().parent.parent / "overture" / "ireland_places.geoparquet"

# Overture categories.primary values → our overlay category names.
# All unmapped values are silently dropped.
OVERTURE_CATEGORY_MAP: dict[str, str] = {
    # ov_shops
    "retail": "ov_shops",
    "shopping": "ov_shops",
    "grocery": "ov_shops",
    "supermarket": "ov_shops",
    "convenience_store": "ov_shops",
    "clothing_store": "ov_shops",
    "hardware_store": "ov_shops",
    "food_and_beverage": "ov_shops",
    "bakery": "ov_shops",
    # ov_healthcare
    "health_and_medical": "ov_healthcare",
    "hospital": "ov_healthcare",
    "pharmacy": "ov_healthcare",
    "dentist": "ov_healthcare",
    "doctor": "ov_healthcare",
    "health_center": "ov_healthcare",
    "medical_clinic": "ov_healthcare",
    "urgent_care_center": "ov_healthcare",
    # ov_parks
    "parks_and_outdoors": "ov_parks",
    "park": "ov_parks",
    "nature_reserve": "ov_parks",
    "playground": "ov_parks",
    "garden": "ov_parks",
    "recreation_ground": "ov_parks",
    "national_park": "ov_parks",
}


def is_available() -> bool:
    return OVERTURE_PLACES_PATH.exists()


def _extract_primary_category(cats: Any) -> str | None:
    """Extract categories.primary from whatever geopandas gives us (dict, namedtuple, str)."""
    if cats is None:
        return None
    if isinstance(cats, str):
        return cats
    if isinstance(cats, dict):
        return cats.get("primary")
    # namedtuple / struct row from pyarrow
    try:
        return cats.primary  # type: ignore[union-attr]
    except AttributeError:
        pass
    return None


def load_overture_amenity_rows(study_area_wgs84: Any) -> list[dict[str, Any]]:
    """Load Overture places, filter to study area, map categories.

    Returns rows with keys matching the amenity_source_rows format used by
    phase_amenities_impl: category, lat, lon, source_ref, park_area_m2.
    Returns [] if the GeoParquet file does not exist.
    """
    if not is_available():
        return []
    if study_area_wgs84 is None:
        return []

    import geopandas as gpd

    bbox = tuple(study_area_wgs84.bounds)
    gdf = gpd.read_parquet(
        OVERTURE_PLACES_PATH,
        columns=["id", "geometry", "categories"],
        bbox=bbox,
    )

    # Ensure WGS84
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Precise containment filter (bbox pushdown is coarse)
    mask = gdf.geometry.within(study_area_wgs84)
    gdf = gdf.loc[mask]

    rows: list[dict[str, Any]] = []
    ids = gdf["id"].to_numpy()
    cats = gdf["categories"].to_numpy()
    geoms = gdf.geometry.to_numpy()

    for fid, cat, geom in zip(ids, cats, geoms):
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
                "lat": float(point.y),
                "lon": float(point.x),
                "source_ref": str(fid or ""),
                "park_area_m2": 0.0,
            }
        )

    return rows


def iter_overture_db_rows(
    overture_source_rows: list[dict[str, Any]],
    created_at: datetime,
    *,
    hashes: Any,
) -> Iterator[dict[str, Any]]:
    """Yield publish-ready amenities table rows for Overture data.

    Mirrors the output shape of iter_amenity_rows_impl but with
    source="overture_places" and the Overture feature ID as source_ref.
    """
    for row in overture_source_rows:
        yield {
            "build_key": hashes.build_key,
            "config_hash": hashes.config_hash,
            "import_fingerprint": hashes.import_fingerprint,
            "category": row["category"],
            "geom": Point(row["lon"], row["lat"]),
            "source": "overture_places",
            "source_ref": row["source_ref"],
            "created_at": created_at,
        }
