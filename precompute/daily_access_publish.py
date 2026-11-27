"""Daily access score (v4): explicit publish step.

    python main.py daily-access --profile <profile> --publish [--allow-missing-transport]

Display rows and metadata are prepared from the selected OSM/GTFS/noise
inputs; no v3 build is read. Grid values come only from the v4 score shards.
Rows are staged before the archives are baked and their stamps checked.
The DB/archive switch remains separate; deployment-wide atomic activation
is a later release task.
"""

from __future__ import annotations

import dataclasses
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import shapely
from shapely.geometry import box
from shapely.ops import transform

from config import (
    CANONICAL_BASE_RESOLUTION_M,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_DEFAULT_MODE,
    DAILY_ACCESS_MODES,
    DAILY_ACCESS_SCORE_MODEL,
    OSM_EXTRACT_PATH,
    noise_pmtiles_output_path,
    resolve_noise_max_zoom,
    SURFACE_SHARD_SIZE_M,
    TO_WGS84,
    build_profile_settings,
    daily_access_mode_aware_params,
    hash_dict,
    package_snapshot,
    pmtiles_output_path,
    python_version,
)

from .daily_access_build import (
    BuildContext,
    DailyAccessBuildError,
    model_versions,
    reach_tier_problems,
    surface_tier_problems,
    transport_gate,
)
from .daily_access_runtime import LEGACY_UNAVAILABLE_FIELDS, DailyAccessSurfaceRuntime
from .daily_access_score import CATEGORIES, PENALTIES, BONUSES, SCORE_CHANNEL_SCHEMA
from .display_layers import DISPLAY_LAYERS_VERSION, prepare_display_layers, select_noise_artifact
from .grid import build_cell_id

ProgressFn = Callable[[str], None]
STAGING_SUFFIX = "~daily-access-v4-staging"


def feed_loss_warnings(losses: dict[str, Any]) -> list[str]:
    from .daily_access_reference import LOSS_WARNING_SHARE

    return [
        f"{group}: {entry['unbound']}/{entry['eligible']} eligible objects lost ({entry['unbound_share']:.1%})"
        for group, entry in sorted(losses.items())
        if entry.get("eligible") and float(entry.get("unbound_share", 0.0)) >= LOSS_WARNING_SHARE
    ]


def check_publishable(context: BuildContext, *, allow_missing_transport: bool) -> dict[str, Any]:
    """Every tier complete and intact, versions current, transport usable."""
    surface_problems, shard_ids = surface_tier_problems(
        context.shell_dir,
        context.score_dir,
        shell_key=context.keys.shell,
        reach_key=context.keys.reach,
        score_key=context.keys.score,
    )
    problems = {
        "surface": surface_problems,
        "reach": reach_tier_problems(context.reach_dir, key=context.keys.reach),
    }
    bike_keys = getattr(context, "bike_keys", None)
    bike_shell_dir = getattr(context, "bike_shell_dir", None)
    bike_reach_dir = getattr(context, "bike_reach_dir", None)
    bike_score_dir = getattr(context, "bike_score_dir", None)
    if any(
        value is None
        for value in (
            bike_keys,
            bike_shell_dir,
            bike_reach_dir,
            bike_score_dir,
        )
    ):
        problems["bike"] = ["bike build context is incomplete"]
        bike_shard_ids: list[str] = []
    else:
        assert bike_keys is not None
        assert bike_shell_dir is not None and bike_score_dir is not None
        assert bike_reach_dir is not None
        bike_surface_problems, bike_shard_ids = surface_tier_problems(
            bike_shell_dir,
            bike_score_dir,
            shell_key=bike_keys.shell,
            reach_key=bike_keys.reach,
            score_key=bike_keys.score,
        )
        problems["bike_surface"] = bike_surface_problems
        problems["bike_reach"] = reach_tier_problems(
            bike_reach_dir,
            key=bike_keys.reach,
            mode="bike",
        )
        if bike_shard_ids != shard_ids:
            problems["bike_alignment"] = ["walk and bike shard inventories differ"]
    problems = {tier: found for tier, found in problems.items() if found}
    if problems:
        raise DailyAccessBuildError(
            "daily access tiers are not publishable (run `main.py daily-access` first): "
            + json.dumps({tier: found[:5] for tier, found in problems.items()})
        )
    reach = json.loads((context.reach_dir / "manifest.json").read_text(encoding="utf-8"))
    for directory in (context.score_dir, context.bike_score_dir):
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("context_source_score_key") != context.keys.score:
            raise DailyAccessBuildError("walk/bike context artifacts do not reference the same walk score key")
    losses = reach.get("losses") or {}
    warnings = transport_gate(losses, allow_missing_transport=allow_missing_transport) + feed_loss_warnings(losses)
    bike_reach = json.loads((bike_reach_dir / "manifest.json").read_text(encoding="utf-8"))
    bike_losses = bike_reach.get("losses") or {}
    warnings += [f"bike: {warning}" for warning in feed_loss_warnings(bike_losses)]
    warnings += json.loads((context.score_dir / "manifest.json").read_text(encoding="utf-8")).get("stats", {}).get("context_warnings", [])
    warnings += [
        f"bike: {warning}"
        for warning in transport_gate(
            bike_losses,
            allow_missing_transport=allow_missing_transport,
        )
    ]
    return {
        "shard_ids": shard_ids,
        "losses": losses,
        "bike_losses": bike_losses,
        "warnings": warnings,
    }


def coarse_cells(
    runtime: DailyAccessSurfaceRuntime,
    shard_ids: list[str],
    resolution_m: int,
    study_area_metric,
) -> list[dict[str, Any]]:
    """Land-weighted coarse cells of one resolution from the score shards."""
    if SURFACE_SHARD_SIZE_M % int(resolution_m):
        raise DailyAccessBuildError(f"{resolution_m} m does not tile the {SURFACE_SHARD_SIZE_M} m shards")
    shapely.prepare(study_area_metric)
    channels = [*runtime.channels, *runtime.network_coverage_modes]
    cells: list[dict[str, Any]] = []
    cell_area = float(resolution_m) ** 2
    for shard_id in sorted(shard_ids):
        shard_x, shard_y = (int(value) for value in shard_id.split("_", 1))
        means = {channel: runtime.aggregated(shard_id, resolution_m, channel)[0] for channel in channels}
        land = runtime.aggregated(shard_id, resolution_m, "network_coverage")[1]
        for row, col in zip(*np.nonzero(land > 0.0)):
            x0 = shard_x + int(col) * int(resolution_m)
            y0 = shard_y + int(row) * int(resolution_m)
            square = box(x0, y0, x0 + resolution_m, y0 + resolution_m)
            geometry = square if study_area_metric.covers(square) else shapely.intersection(square, study_area_metric)
            if geometry.is_empty:
                continue

            def value(channel: str) -> float:
                result = means[channel][row, col]
                return 0.0 if not np.isfinite(result) else float(result)

            scores = {category: value(category) for category in CATEGORIES}
            # v3 tile convention: penalties are negative score contributions.
            scores.update({penalty: -value(penalty) for penalty in PENALTIES})
            scores.update({name: value(name) for name in BONUSES})
            scores.update({f"access_{need}": value(f"access_{need}") for need in DAILY_ACCESS_NEEDS})
            scores["network_coverage_share"] = value("network_coverage")
            mode_scores: dict[str, Any] = {}
            for mode in runtime.available_modes:
                if mode == "walk":
                    mode_scores[mode] = {
                        "total": value("total"),
                        "components": {category: value(category) for category in CATEGORIES},
                        "service_access": {
                            need: value(f"access_{need}") for need in DAILY_ACCESS_NEEDS
                        },
                        "network_coverage_share": value("network_coverage"),
                    }
                    continue
                mode_scores[mode] = {
                    "total": value(f"{mode}_total"),
                    "components": {
                        category: value(f"{mode}_{category}") for category in CATEGORIES
                    },
                    "service_access": {
                        need: value(f"{mode}_access_{need}") for need in DAILY_ACCESS_NEEDS
                    },
                    "network_coverage_share": value(f"{mode}_network_coverage"),
                }
            scores["modes"] = mode_scores
            for mode, values in mode_scores.items():
                prefix = "" if mode == "walk" else f"{mode}_"
                values["penalties"] = {name: value(prefix + name) for name in PENALTIES}
                values["bonuses"] = {name: value(prefix + name) for name in BONUSES}
            land_m2 = float(land[row, col])
            cells.append(
                {
                    "cell_id": build_cell_id(resolution_m, x0, y0),
                    "geometry_metric": geometry,
                    "effective_area_m2": land_m2,
                    "effective_area_ratio": min(land_m2 / cell_area, 1.0),
                    "scores": scores,
                    "total": value("total"),
                }
            )
    return cells


def _walk_rows(cells_by_resolution: dict[int, list[dict[str, Any]]], hashes, created_at) -> list[dict[str, Any]]:
    to_wgs84 = lambda x, y, z=None: TO_WGS84(x, y)  # noqa: E731
    rows = []
    for resolution_m, cells in cells_by_resolution.items():
        for cell in cells:
            geometry = transform(to_wgs84, cell["geometry_metric"])
            centre = transform(to_wgs84, cell["geometry_metric"].representative_point())
            rows.append(
                {
                    "build_key": hashes.build_key,
                    "config_hash": hashes.config_hash,
                    "import_fingerprint": hashes.import_fingerprint,
                    "resolution_m": int(resolution_m),
                    "cell_id": cell["cell_id"],
                    "centre_geom": centre,
                    "cell_geom": geometry,
                    "effective_area_m2": cell["effective_area_m2"],
                    "effective_area_ratio": cell["effective_area_ratio"],
                    "counts_json": {},
                    "cluster_counts_json": {},
                    "effective_units_json": {},
                    "scores_json": cell["scores"],
                    "total_score": cell["total"],
                    "created_at": created_at,
                }
            )
    return rows


def _write_staged_build(engine, *, hashes, extract_path, walk_rows, summary_json, display_layers):
    """Write all display rows and metadata together; never delete an active build.

    Return False for an already active identical build: its immutable rows can
    be reused for an archive rebake without touching the live data.
    """
    from sqlalchemy import delete, insert, select, update
    from db_postgis.tables import (
        amenities, build_manifest, grid_walk, noise_polygons, service_deserts,
        transport_reality, transport_route_patterns, transport_transfer_hubs,
    )
    from db_postgis.write_common import _bulk_insert
    from db_postgis.write_noise import copy_noise_artifact_to_noise_polygons, _update_noise_summary_from_database

    artifact = display_layers.noise_artifact
    with engine.begin() as connection:
        existing = connection.execute(select(build_manifest.c.config_hash, build_manifest.c.status)
                                      .where(build_manifest.c.build_key == hashes.build_key).with_for_update()).first()
        if existing and existing[0] != hashes.config_hash:
            if existing[0] == hashes.config_hash.removesuffix(STAGING_SUFFIX) and existing[1] == "complete":
                return False
            raise DailyAccessBuildError(f"Refusing to replace non-staging build {hashes.build_key}")
        for table in (
            grid_walk, amenities, transport_reality, transport_route_patterns,
            transport_transfer_hubs, service_deserts, noise_polygons, build_manifest,
        ):
            connection.execute(delete(table).where(table.c.build_key == hashes.build_key))
        now = datetime.now(timezone.utc)
        connection.execute(insert(build_manifest), [{
            "build_key": hashes.build_key, "config_hash": hashes.config_hash,
            "import_fingerprint": hashes.import_fingerprint, "extract_path": extract_path,
            "geo_hash": hashes.geo_hash, "reach_hash": hashes.reach_hash,
            "score_hash": hashes.score_hash, "render_hash": hashes.render_hash,
            "noise_processing_hash": artifact.artifact_hash if artifact else None,
            "noise_artifact_hash": artifact.artifact_hash if artifact else None,
            "noise_mode": "artifact", "status": "building", "created_at": now,
            "python_version": python_version(), "packages_json": package_snapshot(), "summary_json": summary_json,
        }])
        for table, rows in (
            (grid_walk, walk_rows),
            (amenities, display_layers.amenities),
            (transport_reality, display_layers.transport),
            (transport_route_patterns, display_layers.route_patterns),
            (transport_transfer_hubs, display_layers.transfer_hubs),
            (service_deserts, display_layers.deserts),
        ):
            _bulk_insert(connection, table, rows)
        if artifact is not None:
            count = copy_noise_artifact_to_noise_polygons(
                connection, noise_resolved_hash=artifact.artifact_hash, build_key=hashes.build_key,
                config_hash=hashes.config_hash, import_fingerprint=hashes.import_fingerprint,
                study_area_wgs84=None, summary_json=summary_json,
            )
            if count <= 0:
                raise DailyAccessBuildError("Selected noise artifact produced no display rows; prepare a valid artifact first.")
            _update_noise_summary_from_database(connection, build_key=hashes.build_key, summary_json=summary_json)
            summary_json["display_layers"]["counts"]["noise_polygons"] = count
        connection.execute(update(build_manifest).where(build_manifest.c.build_key == hashes.build_key)
                           .values(status="complete", completed_at=now, summary_json=summary_json))
    return True


def _delete_build(engine, build_key: str, staging_config_hash: str) -> None:
    from sqlalchemy import delete

    from db_postgis.tables import (
        amenities, build_manifest, grid_walk, noise_polygons, service_deserts,
        transport_reality, transport_route_patterns, transport_transfer_hubs,
    )

    with engine.begin() as connection:
        for table in (
            grid_walk, amenities, transport_reality, transport_route_patterns,
            transport_transfer_hubs, service_deserts, noise_polygons, build_manifest,
        ):
            connection.execute(delete(table).where(table.c.build_key == build_key).where(table.c.config_hash == staging_config_hash))


def switch_active_build(engine, *, build_key: str, staging_config_hash: str, live_config_hash: str) -> None:
    """Make the staged build the one runtimes select (one transaction)."""
    from sqlalchemy import update

    from db_postgis.tables import (
        amenities, build_manifest, grid_walk, noise_polygons, service_deserts,
        transport_reality, transport_route_patterns, transport_transfer_hubs,
    )

    with engine.begin() as connection:
        for table in (
            grid_walk, amenities, transport_reality, transport_route_patterns,
            transport_transfer_hubs, service_deserts, noise_polygons,
        ):
            connection.execute(update(table).where(table.c.build_key == build_key).values(config_hash=live_config_hash))
        updated = connection.execute(
            update(build_manifest)
            .where(build_manifest.c.build_key == build_key)
            .where(build_manifest.c.config_hash == staging_config_hash)
            .values(config_hash=live_config_hash, completed_at=datetime.now(timezone.utc))
        )
        if int(updated.rowcount or 0) != 1:
            raise DailyAccessBuildError(f"staged build {build_key} disappeared before the switch")


def publish_daily_access(
    context: BuildContext,
    *,
    allow_missing_transport: bool = False,
    progress: ProgressFn = print,
    bake_pmtiles=None,
    archive_build_key=None,
) -> dict[str, Any]:
    from .bake_pmtiles import bake_pmtiles as default_bake
    from .workflow import pmtiles_archive_build_key

    bake_pmtiles = bake_pmtiles or default_bake
    archive_build_key = archive_build_key or pmtiles_archive_build_key
    started = time.perf_counter()
    engine = context.engine
    checked = check_publishable(context, allow_missing_transport=allow_missing_transport)
    for warning in checked["warnings"]:
        progress(f"[daily-access publish] warning: {warning}")

    noise_selection = select_noise_artifact(context)
    artifact, display_warnings = noise_selection
    for warning in display_warnings:
        progress(f"[daily-access publish] warning: {warning}")
    live = context.hashes
    build_key = hash_dict({
        "input_build_key": live.build_key, "model": DAILY_ACCESS_SCORE_MODEL, "score_key": context.keys.score,
        "bike_score_key": None if context.bike_keys is None else context.bike_keys.score,
        "mode_aware": daily_access_mode_aware_params(),
        "display_layers_version": DISPLAY_LAYERS_VERSION, "noise_artifact_hash": artifact.artifact_hash if artifact else None,
    })
    staging_config_hash = f"{live.config_hash}{STAGING_SUFFIX}"
    staged = dataclasses.replace(live, build_key=build_key, config_hash=staging_config_hash)

    runtime = DailyAccessSurfaceRuntime(
        context.shell_dir,
        context.score_dir,
        profile=context.profile,
        bike_shell_dir=context.bike_shell_dir,
        bike_score_dir=context.bike_score_dir,
    )
    resolutions = list(build_profile_settings(context.profile).coarse_vector_resolutions_m)
    cells_by_resolution = {
        resolution: coarse_cells(runtime, checked["shard_ids"], resolution, context.study_area_metric)
        for resolution in resolutions
    }
    progress(
        "[daily-access publish] coarse cells: "
        + ", ".join(f"{resolution} m={len(cells):,}" for resolution, cells in cells_by_resolution.items())
    )

    created_at = datetime.now(timezone.utc)
    layers = prepare_display_layers(
        context, cells_by_resolution, hashes=staged, created_at=created_at,
        allow_missing_transport=allow_missing_transport, noise_selection=noise_selection,
    )
    summary = layers.summary
    summary.update(
        {
            "scoring_model": DAILY_ACCESS_SCORE_MODEL,
            "config_hash": live.config_hash,
            "build_profile": context.profile,
            "grid_sizes_m": resolutions,
            # Fine vector cells (when the profile has them) are baked from the v4 shards.
            "fine_resolutions_m": list(build_profile_settings(context.profile).fine_resolutions_m),
            "surface_zoom_breaks": [list(item) for item in build_profile_settings(context.profile).surface_zoom_breaks],
            "legacy_fields_unavailable": list(LEGACY_UNAVAILABLE_FIELDS),
            "scoring_model_version": model_versions()["score_model_version"],
            "daily_access": {
                **model_versions(),
                "keys": dataclasses.asdict(context.keys),
                "available_modes": [*DAILY_ACCESS_MODES, "mode_aware"],
                "default_mode": DAILY_ACCESS_DEFAULT_MODE,
                "mode_aware": daily_access_mode_aware_params(),
                "channel_schema": SCORE_CHANNEL_SCHEMA,
                "context_warnings": json.loads((context.score_dir / "manifest.json").read_text(encoding="utf-8")).get("stats", {}).get("context_warnings", []),
                "graph_dir": str(context.graph_dir),
                "shell_dir": str(context.shell_dir),
                "reach_dir": str(context.reach_dir),
                "score_dir": str(context.score_dir),
                "modes": {
                    "walk": {
                        "keys": dataclasses.asdict(context.keys),
                        "graph_dir": str(context.graph_dir),
                        "shell_dir": str(context.shell_dir),
                        "reach_dir": str(context.reach_dir),
                        "score_dir": str(context.score_dir),
                    },
                    "bike": {
                        "keys": dataclasses.asdict(context.bike_keys),
                        "graph_dir": str(context.bike_graph_dir),
                        "shell_dir": str(context.bike_shell_dir),
                        "reach_dir": str(context.bike_reach_dir),
                        "score_dir": str(context.bike_score_dir),
                    },
                },
                "import_fingerprint": context.source_state.import_fingerprint,
                "transit_reality_fingerprint": context.reality_fingerprint,
                "overture_dataset_signature": context.overture_dataset_signature,
                "resolution_m": CANONICAL_BASE_RESOLUTION_M,
                "channels": list(runtime.channels),
                "losses": checked["losses"],
                "warnings": checked["warnings"],
                "fine_png_resolutions_m": list(build_profile_settings(context.profile).fine_resolutions_m),
            },
        }
    )

    archive = Path(pmtiles_output_path(context.profile))
    staging_archive = archive.with_name(archive.stem + ".v4-staging" + archive.suffix)
    noise_archive = noise_pmtiles_output_path(context.profile)
    staging_noise_archive = noise_archive.with_name(noise_archive.stem + ".v4-staging" + noise_archive.suffix)
    written = False
    try:
        written = _write_staged_build(
            engine,
            hashes=staged,
            extract_path=str(context.source_state.extract_path or OSM_EXTRACT_PATH),
            walk_rows=_walk_rows(cells_by_resolution, staged, created_at),
            summary_json=summary, display_layers=layers,
        )
        progress(f"[daily-access publish] prepared build {build_key} (display layers: {layers.counts()})")
        # Fine vector cells come from the v4 shards (real components), never
        # from v3 node scores; profiles without fine resolutions bake coarse only.
        fine_grid_config = (
            {
                "shell_dir": str(context.shell_dir),
                "score_dir": str(context.score_dir),
                "bike_shell_dir": str(context.bike_shell_dir),
                "bike_score_dir": str(context.bike_score_dir),
            }
            if build_profile_settings(context.profile).fine_resolutions_m
            else {}
        )
        bake_pmtiles(engine, build_key, staging_archive, fine_grid_config=fine_grid_config,
                     bbox=context.study_area_wgs84.bounds)
        if artifact is not None:
            from noise_artifacts.bake import bake_noise_pmtiles
            bake_noise_pmtiles(engine, build_key, staging_noise_archive,
                               noise_max_zoom=resolve_noise_max_zoom(profile=context.profile))
            if archive_build_key(staging_noise_archive) != build_key:
                raise DailyAccessBuildError("Noise archive build key does not match the staged v4 build")
        stamped = archive_build_key(staging_archive)
        if stamped != build_key:
            raise DailyAccessBuildError(f"baked archive carries build key {stamped!r}, expected {build_key!r}")
    except Exception:
        if written:
            _delete_build(engine, build_key, staging_config_hash)
        for temporary in (staging_archive, staging_noise_archive):
            if temporary.exists():
                temporary.unlink()
        raise

    if written:
        switch_active_build(engine, build_key=build_key, staging_config_hash=staging_config_hash, live_config_hash=live.config_hash)
    try:
        if artifact is not None:
            os.replace(staging_noise_archive, noise_archive)
        os.replace(staging_archive, archive)
    except OSError:
        # Put the previous build back in front before reporting the failure.
        if written:
            switch_active_build(engine, build_key=build_key, staging_config_hash=live.config_hash, live_config_hash=staging_config_hash)
        raise
    if not written:
        # Re-select this v4 build even if a v3 publication completed since its
        # previous publication. Refresh only after the rebaked files are ready.
        switch_active_build(engine, build_key=build_key,
                            staging_config_hash=live.config_hash, live_config_hash=live.config_hash)
    return {
        "build_key": build_key,
        "config_hash": live.config_hash,
        "pmtiles": str(archive),
        "coarse_cells": {str(resolution): len(cells) for resolution, cells in cells_by_resolution.items()},
        "point_layers_prepared": layers.counts(),
        "warnings": checked["warnings"] + display_warnings,
        "seconds": round(time.perf_counter() - started, 1),
    }


__all__ = [
    "STAGING_SUFFIX",
    "check_publishable",
    "coarse_cells",
    "feed_loss_warnings",
    "publish_daily_access",
    "switch_active_build",
]
