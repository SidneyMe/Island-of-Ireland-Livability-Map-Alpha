"""Read-only inventory of the daily access (v4) inputs in the current import.

Classifies shops/medical through the real OSM/Overture merge path, groups
them into logical places, builds green sites, and prints counts with
exclusion reasons as JSON. Primary-care centres are reported both as source
records and as unique logical places, since one centre is often mapped more
than once.

    python scripts/daily_access_inventory.py [--profile test] [--no-overture]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shapely.geometry import box

from config import build_profile_settings
from db_postgis import build_engine, load_daily_access_source_rows, load_merged_source_amenity_rows
from db_postgis._dependencies import text
from db_postgis.daily_access_green import load_green_sites
from overture import loader as overture_loader
from precompute.daily_access_classify import (
    build_logical_places,
    classify_service_rows,
    is_primary_centre_name,
)

# Island of Ireland envelope (includes offshore islands; diagnostic only).
NATIONAL_BBOX_WGS84 = (-10.8, 51.3, -5.3, 55.5)


def _latest_import_fingerprint(engine) -> str:
    with engine.connect() as connection:
        fingerprint = connection.execute(
            text(
                "SELECT import_fingerprint FROM osm_raw.import_manifest "
                "WHERE status = 'complete' ORDER BY completed_at DESC LIMIT 1"
            )
        ).scalar()
    if not fingerprint:
        raise SystemExit("No complete OSM import manifest found.")
    return str(fingerprint)


def build_inventory(*, profile: str | None, use_overture: bool) -> dict[str, Any]:
    bbox = build_profile_settings(profile).study_area_bbox_wgs84 if profile else None
    study_area = box(*(bbox or NATIONAL_BBOX_WGS84))
    engine = build_engine()
    import_fingerprint = _latest_import_fingerprint(engine)

    load_stats: dict[str, Any] = {}
    osm_rows = load_daily_access_source_rows(
        engine, import_fingerprint, study_area, stats_out=load_stats
    )
    overture_rows = overture_loader.load_overture_amenity_rows(study_area) if use_overture else []
    merged_rows, merge_stats = load_merged_source_amenity_rows(
        engine,
        osm_rows,
        overture_rows,
        scoring_categories=("healthcare", "parks", "shops"),
    )
    lookup = {
        (str(row.get("source") or ""), str(row.get("source_ref") or "")): row
        for row in [*osm_rows, *overture_rows]
    }
    # v4: raw rows are classified one by one, then deduplicated on evidence
    # (the v3 merge above only selects Overture-only green points).
    service_rows = [row for row in [*osm_rows, *overture_rows] if row.get("category") in {"shops", "healthcare"}]
    classified, class_stats = classify_service_rows(service_rows)
    dedupe_stats: dict[str, Any] = {}
    places = build_logical_places(classified, stats_out=dedupe_stats)
    class_stats["dedupe"] = dedupe_stats

    places_by_kind = Counter(place.service_kind for place in places)
    # "Primary Care Centre" / "Health Centre" by name, whatever the tags
    # (most are tagged amenity=clinic or amenity=doctors).
    centre_rows = [row for row in classified if is_primary_centre_name(row.get("name"))]
    centre_places = [
        place
        for place in places
        if any(is_primary_centre_name(member.get("name")) for member in place.members)
    ]

    # Overture green rows the merge matched to OSM already exist as OSM members.
    overture_only_green = [
        lookup[(str(row["source"]), str(row["source_ref"]))]
        for row in merged_rows
        if row.get("category") == "parks" and row.get("source") == "overture_places"
    ]
    green_sites, green_stats = load_green_sites(
        engine,
        import_fingerprint=import_fingerprint,
        study_area_wgs84=study_area,
        overture_green_rows=overture_only_green,
    )
    return {
        "import_fingerprint": import_fingerprint,
        "study_area_bbox_wgs84": list(bbox or NATIONAL_BBOX_WGS84),
        "osm_load": load_stats,
        "overture_rows": len(overture_rows),
        "overture_green_rows": sum(1 for row in overture_rows if row["category"] == "parks"),
        "merged_rows": len(merged_rows),
        "overture_only_green_points": len(overture_only_green),
        "osm_duplicates_removed": int((merge_stats or {}).get("osm_duplicate_rows_removed", 0)),
        "classification": class_stats,
        "logical_places_by_service_kind": dict(sorted(places_by_kind.items())),
        "primary_care_centres": {
            "source_records": len(centre_rows),
            "source_records_by_service_kind": dict(
                sorted(Counter(row["service_kind"] for row in centre_rows).items())
            ),
            "logical_places": len(centre_places),
            "logical_places_by_service_kind": dict(
                sorted(Counter(place.service_kind for place in centre_places).items())
            ),
            "classified_by_name_rule_only": sum(
                1 for row in classified if row["class_reason"] == "primary_centre_name"
            ),
        },
        "green": green_stats,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", default=None, help="Use this build profile's bbox (e.g. test).")
    parser.add_argument("--no-overture", action="store_true", help="Skip Overture Places.")
    args = parser.parse_args(argv)
    inventory = build_inventory(profile=args.profile, use_overture=not args.no_overture)
    print(json.dumps(inventory, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
