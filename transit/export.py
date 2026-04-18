from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from config import (
    CACHE_DIR,
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_LOOKAHEAD_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    TRANSIT_REALITY_ALGO_VERSION,
)

from .models import GtfsStopReality


EXPORTS_DIR = CACHE_DIR / "exports"
GEOJSON_FILENAME = "transport-reality.geojson"
MANIFEST_FILENAME = "transport-reality.manifest.json"
README_FILENAME = "README.txt"
ZIP_FILENAME = "transport-reality.zip"


def _feature_properties(row: GtfsStopReality) -> dict[str, object]:
    return {
        "source_ref": row.source_ref,
        "stop_name": row.stop_name,
        "feed_id": row.feed_id,
        "stop_id": row.stop_id,
        "source_status": row.source_status,
        "reality_status": row.reality_status,
        "school_only_state": row.school_only_state,
        "public_departures_7d": row.public_departures_7d,
        "public_departures_30d": row.public_departures_30d,
        "school_only_departures_30d": row.school_only_departures_30d,
        "last_public_service_date": (
            row.last_public_service_date.isoformat()
            if row.last_public_service_date is not None
            else None
        ),
        "last_any_service_date": (
            row.last_any_service_date.isoformat()
            if row.last_any_service_date is not None
            else None
        ),
        "route_modes": list(row.route_modes),
        "source_reason_codes": list(row.source_reason_codes),
        "reality_reason_codes": list(row.reality_reason_codes),
    }


def build_transport_reality_geojson(rows: list[GtfsStopReality]) -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row.lon, row.lat],
                },
                "properties": _feature_properties(row),
            }
            for row in rows
        ],
    }


def _readme_text() -> str:
    return "\n".join(
        (
            "Island of Ireland transport reality dataset",
            "",
            "This export is derived directly from configured GTFS feeds.",
            "Each point represents a GTFS stop that appears in the current service analysis window.",
            "It distinguishes confirmed active stops, confirmed inactive stops, and confirmed school-only stops.",
            "",
            (
                f"Activity window: retrospective {GTFS_ANALYSIS_WINDOW_DAYS}-day window "
                f"plus a {GTFS_LOOKAHEAD_DAYS}-day upcoming-service lookahead."
            ),
            (
                f"Service desert window: retrospective {GTFS_SERVICE_DESERT_WINDOW_DAYS}-day base window "
                f"plus the same {GTFS_LOOKAHEAD_DAYS}-day upcoming-service lookahead."
            ),
            "",
            "Caveats:",
            "- This is a conservative GTFS-direct availability view, not a full frequency-weighted model.",
            "- Stops omitted from GTFS feeds cannot appear in this dataset.",
        )
    )


def export_transport_reality_bundle(
    rows: list[GtfsStopReality],
    *,
    analysis_date,
    export_dir: Path = EXPORTS_DIR,
) -> dict[str, Path]:
    export_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = export_dir / GEOJSON_FILENAME
    manifest_path = export_dir / MANIFEST_FILENAME
    readme_path = export_dir / README_FILENAME
    zip_path = export_dir / ZIP_FILENAME

    geojson_payload = build_transport_reality_geojson(rows)
    geojson_path.write_text(json.dumps(geojson_payload, indent=2), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "analysis_date": analysis_date.isoformat(),
                "feature_count": len(rows),
                "matcher_version": TRANSIT_REALITY_ALGO_VERSION,
                "reality_fingerprint": rows[0].reality_fingerprint if rows else None,
                "geojson_filename": GEOJSON_FILENAME,
                "readme_filename": README_FILENAME,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    readme_path.write_text(_readme_text(), encoding="utf-8")

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.write(geojson_path, GEOJSON_FILENAME)
        archive.write(manifest_path, MANIFEST_FILENAME)
        archive.write(readme_path, README_FILENAME)

    return {
        "geojson": geojson_path,
        "manifest": manifest_path,
        "readme": readme_path,
        "zip": zip_path,
    }
