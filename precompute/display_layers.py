"""Prepare the v4 map's display layers from inputs, never a published v3 build."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shapely.ops import transform

from config import (
    LANDUSE_CONTEXT_MAX_ZOOM, LANDUSE_CONTEXT_MIN_ZOOM, OUTPUT_HTML,
    RAILWAY_PROXIMITY_ACTIVE_MODES, RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
    RAILWAY_PROXIMITY_MAX_PENALTY, RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
    TAGS, TO_WGS84, WALK_RADIUS_M, build_profile_settings,
)
from db_postgis import (
    load_landuse_context_rows, load_merged_source_amenity_rows, load_railway_corridor_rows,
    load_road_rows, load_source_amenity_rows, load_transport_reality_points,
    load_transit_route_network_rows,
    load_transit_railway_corridor_manifest,
)
from network import load_walk_graph_index
from noise_artifacts.manifest import get_resolved_artifact_for_mode
from noise_artifacts.modes import resolve_noise_accuracy_mode
from overture import loader as overture

from . import publish
from .amenity_tiers import annotate_amenity_row
from .road_proximity import road_proximity_summary
from .service_deserts import compute_service_desert_rows
from transit.route_assets import publish_route_assets


DISPLAY_LAYERS_VERSION = 3


@dataclass
class DisplayLayers:
    amenities: list[dict[str, Any]] = field(default_factory=list)
    transport: list[dict[str, Any]] = field(default_factory=list)
    route_patterns: list[dict[str, Any]] = field(default_factory=list)
    route_stops: list[dict[str, Any]] = field(default_factory=list)
    transfer_hubs: list[dict[str, Any]] = field(default_factory=list)
    deserts: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    noise_artifact: Any = None
    warnings: list[str] = field(default_factory=list)

    def counts(self):
        return {"amenities": len(self.amenities), "transport_reality": len(self.transport),
                "transport_route_patterns": len(self.route_patterns),
                "transport_transfer_hubs": len(self.transfer_hubs),
                "service_deserts": len(self.deserts)}


def prepare_amenity_sources(engine, *, import_fingerprint, reality_fingerprint, study_area_wgs84):
    """Keep the existing POI display categories/tiers without v3 scoring/clusters."""
    source_stats = {}
    osm = load_source_amenity_rows(
        engine, import_fingerprint, study_area_wgs84,
        transit_reality_fingerprint=reality_fingerprint, stats_out=source_stats,
    )
    transport = [row for row in osm if row["category"] == "transport"]
    osm = [row for row in osm if row["category"] != "transport"]
    extra = overture.load_overture_amenity_rows(study_area_wgs84)
    lookup = {(str(row.get("source") or ""), str(row["source_ref"])): row for row in [*osm, *transport, *extra]}
    merged, merge_stats = load_merged_source_amenity_rows(engine, osm, extra, scoring_categories=TAGS)
    result = []
    for row in sorted([*merged, *transport], key=lambda row: (row["category"], str(row.get("source") or ""), row["source_ref"])):
        source = lookup.get((str(row.get("source") or ""), str(row["source_ref"])), row)
        geom = row.get("geom")
        result.append({
            "category": row["category"], "source": str(row.get("source") or "osm_local_pbf"),
            "source_ref": row["source_ref"], "name": row.get("name"),
            "lat": float(geom.y if geom is not None else row["lat"]),
            "lon": float(geom.x if geom is not None else row["lon"]),
            "conflict_class": str(row.get("conflict_class") or "osm_only"),
            "tier": annotate_amenity_row(source).get("tier"),
        })
    return result, {**(merge_stats or {}), **source_stats}


def _railway_summary(manifest, corridors):
    manifest = manifest or {}
    return {
        "railway_proximity_enabled": bool(manifest),
        "railway_proximity_data_present": bool(corridors),
        "railway_proximity_hash": manifest.get("railway_proximity_hash"),
        "railway_proximity_source_kind": manifest.get("source_kind"),
        **{f"railway_proximity_{key}": int(manifest.get(key, 0) or 0) for key in (
            "active_feed_count", "active_service_count", "active_trip_count", "active_shape_count", "warning_count",
        )},
        "railway_proximity_materialized_corridor_count": len(corridors),
        "railway_proximity_full_penalty_distance_m": RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
        "railway_proximity_zero_penalty_distance_m": RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
        "railway_proximity_max_penalty": RAILWAY_PROXIMITY_MAX_PENALTY,
        "railway_proximity_active_modes": list(RAILWAY_PROXIMITY_ACTIVE_MODES),
    }


def select_noise_artifact(context):
    from .daily_access_build import DailyAccessBuildError

    mode = "accurate" if context.profile == "full" else resolve_noise_accuracy_mode(profile=context.profile)
    artifact = get_resolved_artifact_for_mode(context.engine, mode)
    warnings = []
    if artifact is None:
        if context.profile == "full":
            raise DailyAccessBuildError(
                "No complete accurate noise artifact. Prepare it separately first: "
                "scripts\\win\\prepare_noise_artifact_accurate.cmd "
                "(Linux: NOISE_ACCURACY_MODE=accurate python -m noise_artifacts --profile full)."
            )
        warnings.append(f"Noise overlay disabled: no complete {mode} artifact; no raw datasets were read.")
    return artifact, warnings


def prepare_display_layers(context, cells_by_resolution, *, hashes, created_at, allow_missing_transport=False, noise_selection=None):
    from .daily_access_build import DailyAccessBuildError

    engine = context.engine
    reality = context.transit_reality_state
    if reality is None or reality.reality_fingerprint != context.reality_fingerprint:
        raise DailyAccessBuildError("Display layers need the same GTFS snapshot as the v4 score build.")
    artifact, warnings = select_noise_artifact(context) if noise_selection is None else noise_selection
    mode = "accurate" if context.profile == "full" else resolve_noise_accuracy_mode(profile=context.profile)

    sources, merge = prepare_amenity_sources(
        engine, import_fingerprint=hashes.import_fingerprint,
        reality_fingerprint=context.reality_fingerprint, study_area_wgs84=context.study_area_wgs84,
    )
    reality_rows = load_transport_reality_points(engine, context.reality_fingerprint)
    if not reality_rows and not allow_missing_transport:
        raise DailyAccessBuildError("No GTFS transport display rows for the v4 snapshot; refusing to publish.")
    transport = publish.transport_rows_impl(reality_rows, created_at, hashes=hashes)
    derived_patterns, route_stops, derived_hubs = load_transit_route_network_rows(
        engine, context.reality_fingerprint
    )
    route_patterns = [
        {
            "build_key": hashes.build_key,
            "config_hash": hashes.config_hash,
            "import_fingerprint": hashes.import_fingerprint,
            **{
                key: value
                for key, value in row.items()
                if key not in {"route_network_hash", "stop_ids_json", "created_at"}
            },
            "created_at": created_at,
        }
        for row in derived_patterns
    ]
    transfer_hubs = [
        {
            "build_key": hashes.build_key,
            "config_hash": hashes.config_hash,
            "import_fingerprint": hashes.import_fingerprint,
            **{
                key: value
                for key, value in row.items()
                if key not in {"stop_refs_json", "created_at"}
            },
            "created_at": created_at,
        }
        for row in derived_hubs
    ]
    route_index = None
    if derived_patterns:
        route_index = publish_route_assets(
            build_key=hashes.build_key,
            reality_fingerprint=context.reality_fingerprint,
            patterns=derived_patterns,
            stops=route_stops,
            transfer_hubs=derived_hubs,
        )
    # The test graph is regional. Do not snap distant national stops to its boundary.
    reach_area = context.study_area_metric.envelope.buffer(WALK_RADIUS_M).envelope
    reach_area = transform(lambda x, y, z=None: TO_WGS84(x, y), reach_area)
    local_stops = [row for row in reality_rows if reach_area.intersects(row["geom"])]
    deserts = compute_service_desert_rows(
        load_walk_graph_index(context.graph_dir), cells_by_resolution, local_stops,
        hashes=hashes, reality_state=reality, study_area_metric=context.study_area_metric, created_at=created_at,
    ) if local_stops else []
    deserts = [{**{key: value for key, value in row.items() if key != "reality_fingerprint"},
                "config_hash": hashes.config_hash} for row in deserts]
    amenities = publish.amenity_rows_impl(sources, created_at, hashes=hashes)
    points = {category: [(row["lat"], row["lon"]) for row in sources if row["category"] == category] for category in TAGS}
    settings = build_profile_settings(context.profile)
    summary = publish.summary_json_impl(
        context.study_area_wgs84, cells_by_resolution, points, sources,
        transport_reality_rows=transport, amenity_merge=merge, hashes=hashes,
        build_profile=context.profile, source_state=context.source_state,
        osm_extract_path=context.source_state.extract_path, grid_sizes_m=list(cells_by_resolution),
        fine_resolutions_m=list(settings.fine_resolutions_m), output_html=OUTPUT_HTML,
        zoom_breaks=list(settings.surface_zoom_breaks), transit_reality_state=reality,
        transit_analysis_window_days=context.transit_analysis_window_days,
        transit_service_desert_window_days=context.transit_service_desert_window_days,
        transport_reality_download_url="/exports/transport-reality.zip", service_deserts_enabled=True,
        overture_dataset=overture.dataset_info(),
    )
    landuse = load_landuse_context_rows(engine, hashes.import_fingerprint, context.study_area_wgs84)
    landuse_counts = {}
    for row in landuse:
        key = str(row.get("landuse_class") or "").strip()
        if key:
            landuse_counts[key] = landuse_counts.get(key, 0) + 1
    roads = road_proximity_summary(load_road_rows(engine, hashes.import_fingerprint))
    rail = _railway_summary(
        load_transit_railway_corridor_manifest(engine, context.reality_fingerprint),
        load_railway_corridor_rows(engine, context.reality_fingerprint),
    )
    summary.update({
        "mode_aware_scoring_enabled": False, "mode_aware_score_weights": {},
        "transport_reality_enabled": bool(transport),
        "transport_network_enabled": bool(route_patterns),
        "transport_route_index_url": (
            f"/transit/routes/{hashes.build_key}/index.json" if route_index else None
        ),
        "transport_route_counts": dict((route_index or {}).get("counts") or {}),
        "transport_route_min_zooms": {"backbone": 6, "regional": 8, "local": 10},
        "landuse_context_enabled": bool(landuse_counts), "landuse_context_counts": landuse_counts,
        "landuse_context_classes": sorted(landuse_counts),
        "landuse_context_min_zoom": LANDUSE_CONTEXT_MIN_ZOOM, "landuse_context_max_zoom": LANDUSE_CONTEXT_MAX_ZOOM,
        **rail, "railway_proximity": rail, "road_proximity": roads,
        "road_proximity_enabled": roads["enabled"], "road_proximity_data_present": roads["data_present"],
        "road_proximity_row_count": roads["row_count"], "noise_accuracy_mode": mode,
        "noise_band_counts_by_metric": {}, "display_layers": {
            "version": DISPLAY_LAYERS_VERSION, "import_fingerprint": hashes.import_fingerprint,
            "transit_reality_fingerprint": context.reality_fingerprint,
            "noise_artifact_hash": artifact.artifact_hash if artifact else None,
            "warnings": warnings,
            "counts": {"amenities": len(amenities), "transport_reality": len(transport),
                       "transport_route_patterns": len(route_patterns),
                       "transport_transfer_hubs": len(transfer_hubs),
                       "service_deserts": len(deserts)},
        },
    })
    return DisplayLayers(
        amenities=amenities,
        transport=transport,
        route_patterns=route_patterns,
        route_stops=route_stops,
        transfer_hubs=transfer_hubs,
        deserts=deserts,
        summary=summary,
        noise_artifact=artifact,
        warnings=warnings,
    )
