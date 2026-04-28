from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BuildHashes

from ._dependencies import Connection, Engine, delete, insert, text, update
from .common import ProgressCallback, _table_key, root_module
from .schema import _quote_identifier
from .tables import (
    amenities,
    build_manifest,
    features,
    grid_walk,
    import_manifest,
    noise_polygons,
    service_deserts,
    transport_reality,
    transit_calendar_dates,
    transit_calendar_services,
    transit_feed_manifest,
    transit_gtfs_stop_reality,
    transit_gtfs_stop_service_summary,
    transit_reality_manifest,
    transit_routes,
    transit_service_classification,
    transit_stop_times,
    transit_stops,
    transit_trips,
)
from .write_common import (
    _bulk_copy,
    _bulk_insert,
    _chunked,
    _copy_from_csv_file,
    _create_temp_csv_stage,
    _csv_copy_value,
    _prepare_chunk,
    _qualified_table_name,
    _table_column_names,
)
from .write_noise import (
    NOISE_STAGE_BATCH_SIZE,
    _NOISE_STAGE_COLUMNS,
    _clone_noise_polygons_from_prior_build,
    _count_noise_values,
    _create_noise_stage_indexes,
    _drain_iterable,
    _materialize_noise_group_round,
    _materialize_noise_polygons_from_stage,
    _noise_stage_groups,
    _noise_stage_table,
    _prepare_noise_stage_chunk,
    _stage_noise_candidate_rows,
    _stage_noise_candidate_rows_via_copy,
    _study_area_wkb,
    _update_noise_summary_from_database,
    copy_noise_artifact_to_noise_polygons,
)
from .write_transit import (
    replace_gtfs_feed_rows,
    replace_service_desert_rows,
    replace_transit_reality_rows,
)


def clear_import_artifacts(engine: Engine, import_fingerprint: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            delete(features).where(features.c.import_fingerprint == import_fingerprint)
        )
        connection.execute(
            delete(import_manifest).where(import_manifest.c.import_fingerprint == import_fingerprint)
        )


def clear_normalized_import_artifacts(engine: Engine, import_fingerprint: str) -> None:
    clear_import_artifacts(engine, import_fingerprint)


def clear_normalized_network_rows(engine: Engine, import_fingerprint: str) -> None:
    del engine, import_fingerprint


def _copy_stops_csv_into_table(
    connection: Connection,
    *,
    csv_path: Path,
    progress_cb: ProgressCallback | None = None,
) -> None:
    target_name = _qualified_table_name(transit_stops)
    staging_name = "_tmp_transit_stops_stage"
    staging_quoted = _create_temp_csv_stage(
        connection,
        target_table=transit_stops,
        staging_name=staging_name,
        deferred_target_columns=("geom",),
    )
    staging_columns = _table_column_names(transit_stops, exclude=("geom",))
    insert_columns = _table_column_names(transit_stops)
    select_columns = [
        "ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)" if name == "geom" else _quote_identifier(name)
        for name in insert_columns
    ]
    _copy_from_csv_file(
        connection,
        qualified_table=staging_quoted,
        table_key=_table_key(transit_stops),
        column_names=staging_columns,
        csv_path=csv_path,
        progress_cb=progress_cb,
    )
    connection.execute(
        text(
            f"INSERT INTO {target_name} "
            f"({', '.join(_quote_identifier(name) for name in insert_columns)}) "
            f"SELECT {', '.join(select_columns)} FROM {staging_quoted}"
        )
    )


def replace_gtfs_feed_rows_from_artifacts(
    engine: Engine,
    *,
    feed_fingerprint: str,
    feed_id: str,
    analysis_date,
    source_path: str,
    source_url: str | None,
    artifacts_dir: Path,
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for table in (
            transit_stops,
            transit_routes,
            transit_trips,
            transit_stop_times,
            transit_calendar_services,
            transit_calendar_dates,
            transit_feed_manifest,
        ):
            connection.execute(delete(table).where(table.c.feed_id == feed_id))

        connection.execute(
            insert(transit_feed_manifest),
            [
                {
                    "feed_fingerprint": feed_fingerprint,
                    "feed_id": feed_id,
                    "analysis_date": analysis_date,
                    "source_path": source_path,
                    "source_url": source_url,
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                }
            ],
        )

        _copy_stops_csv_into_table(
            connection,
            csv_path=artifacts_dir / "stops.csv",
            progress_cb=progress_cb,
        )
        for table, file_name in (
            (transit_routes, "routes.csv"),
            (transit_trips, "trips.csv"),
            (transit_stop_times, "stop_times.csv"),
            (transit_calendar_services, "calendar_services.csv"),
            (transit_calendar_dates, "calendar_dates.csv"),
        ):
            _copy_from_csv_file(
                connection,
                qualified_table=_qualified_table_name(table),
                table_key=_table_key(table),
                column_names=_table_column_names(table),
                csv_path=artifacts_dir / file_name,
                progress_cb=progress_cb,
            )

        connection.execute(
            update(transit_feed_manifest)
            .where(transit_feed_manifest.c.feed_fingerprint == feed_fingerprint)
            .values(status="complete", completed_at=datetime.now(timezone.utc))
        )


def _copy_gtfs_stop_reality_csv_into_table(
    connection: Connection,
    *,
    csv_path: Path,
    progress_cb: ProgressCallback | None = None,
) -> None:
    target_name = _qualified_table_name(transit_gtfs_stop_reality)
    staging_name = "_tmp_transit_gtfs_stop_reality_stage"
    staging_quoted = _create_temp_csv_stage(
        connection,
        target_table=transit_gtfs_stop_reality,
        staging_name=staging_name,
        deferred_target_columns=("geom",),
        extra_stage_columns=(("lat", "DOUBLE PRECISION"), ("lon", "DOUBLE PRECISION")),
    )
    staging_columns = _table_column_names(transit_gtfs_stop_reality, exclude=("geom",)) + ["lat", "lon"]
    insert_columns = _table_column_names(transit_gtfs_stop_reality)
    select_columns = [
        "ST_SetSRID(ST_MakePoint(lon, lat), 4326)" if name == "geom" else _quote_identifier(name)
        for name in insert_columns
    ]
    _copy_from_csv_file(
        connection,
        qualified_table=staging_quoted,
        table_key=_table_key(transit_gtfs_stop_reality),
        column_names=staging_columns,
        csv_path=csv_path,
        progress_cb=progress_cb,
    )
    connection.execute(
        text(
            f"INSERT INTO {target_name} "
            f"({', '.join(_quote_identifier(name) for name in insert_columns)}) "
            f"SELECT {', '.join(select_columns)} FROM {staging_quoted}"
        )
    )


def replace_transit_reality_rows_from_artifacts(
    engine: Engine,
    *,
    reality_fingerprint: str,
    import_fingerprint: str,
    analysis_date,
    transit_config_hash: str,
    feed_fingerprints_json: dict[str, str],
    artifacts_dir: Path,
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for table in (
            transit_service_classification,
            transit_gtfs_stop_service_summary,
            transit_gtfs_stop_reality,
            transit_reality_manifest,
        ):
            connection.execute(delete(table).where(table.c.reality_fingerprint == reality_fingerprint))
        connection.execute(
            insert(transit_reality_manifest),
            [
                {
                    "reality_fingerprint": reality_fingerprint,
                    "import_fingerprint": import_fingerprint,
                    "analysis_date": analysis_date,
                    "transit_config_hash": transit_config_hash,
                    "feed_fingerprints_json": feed_fingerprints_json,
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                }
            ],
        )

        for table, file_name in (
            (transit_service_classification, "service_classification.csv"),
            (transit_gtfs_stop_service_summary, "gtfs_stop_service_summary.csv"),
        ):
            _copy_from_csv_file(
                connection,
                qualified_table=_qualified_table_name(table),
                table_key=_table_key(table),
                column_names=_table_column_names(table),
                csv_path=artifacts_dir / file_name,
                progress_cb=progress_cb,
            )
        _copy_gtfs_stop_reality_csv_into_table(
            connection,
            csv_path=artifacts_dir / "gtfs_stop_reality.csv",
            progress_cb=progress_cb,
        )

        connection.execute(
            update(transit_reality_manifest)
            .where(transit_reality_manifest.c.reality_fingerprint == reality_fingerprint)
            .values(status="complete", completed_at=datetime.now(timezone.utc))
        )


_UNSET = object()  # sentinel for noise_study_area_wgs84


def _publish_noise_polygons(
    connection: Connection,
    *,
    noise_rows,
    build_key: str,
    config_hash: str,
    import_fingerprint: str,
    render_hash: str,
    noise_processing_hash: str | None = None,
    noise_artifact_hash: str | None = None,
    created_at: datetime,
    study_area_wgs84,
    summary_json: dict[str, Any],
    progress_cb: ProgressCallback | None = None,
) -> None:
    # Artifact mode: sentinel object → direct copy, skip all legacy staging
    from precompute._rows import _ArtifactNoiseReference  # local to avoid circular import
    if isinstance(noise_rows, _ArtifactNoiseReference):
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    f"artifact mode: direct-copying noise from "
                    f"noise_resolved_hash={noise_rows.noise_resolved_hash}"
                ),
                force_log=True,
            )
        n = copy_noise_artifact_to_noise_polygons(
            connection,
            noise_resolved_hash=noise_rows.noise_resolved_hash,
            build_key=build_key,
            config_hash=config_hash,
            import_fingerprint=import_fingerprint,
            study_area_wgs84=study_area_wgs84,
        )
        _update_noise_summary_from_database(
            connection, build_key=build_key, summary_json=summary_json
        )
        if progress_cb is not None:
            progress_cb("detail", detail=f"copied {n:,} noise polygons from artifact", force_log=True)
        return

    from config import NOISE_MODE as _NOISE_MODE
    if _NOISE_MODE == "artifact":
        raise RuntimeError(
            "BUG: NOISE_MODE=artifact but _publish_noise_polygons received "
            f"{type(noise_rows).__name__!r}, not _ArtifactNoiseReference. "
            "Artifact mode must direct-copy from noise_resolved_display and must "
            "never stage raw noise candidate rows. "
            "Ensure NOISE_MODE=artifact is set before Python starts (not after import). "
            "Check: python -c \"import config; print(config.NOISE_MODE)\""
        )

    cloned = _clone_noise_polygons_from_prior_build(
        connection,
        new_build_key=build_key,
        new_config_hash=config_hash,
        new_import_fingerprint=import_fingerprint,
        render_hash=render_hash,
        noise_processing_hash=noise_processing_hash,
        created_at=created_at,
        progress_cb=progress_cb,
    )
    if cloned > 0:
        # Drain the iterator so the upstream loader/cache wrapper still runs to
        # completion (cache writes happen on the trailing side of the generator).
        drained = _drain_iterable(noise_rows)
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    f"skipped noise stage+materialize "
                    f"({drained:,} candidate rows discarded after clone)"
                ),
                force_log=True,
            )
        _update_noise_summary_from_database(
            connection,
            build_key=build_key,
            summary_json=summary_json,
        )
        return

    staging_quoted = _create_temp_csv_stage(
        connection,
        target_table=noise_polygons,
        staging_name="_tmp_noise_polygons_stage",
    )
    staged_rows = _stage_noise_candidate_rows(
        connection,
        staging_quoted=staging_quoted,
        noise_rows=noise_rows,
        progress_cb=progress_cb,
    )
    if staged_rows > 0:
        _create_noise_stage_indexes(
            connection,
            staging_quoted=staging_quoted,
            progress_cb=progress_cb,
        )
        final_rows = _materialize_noise_polygons_from_stage(
            connection,
            staging_quoted=staging_quoted,
            build_key=build_key,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
        )
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=f"materialized {final_rows:,} final noise polygons",
                force_log=True,
            )
    _update_noise_summary_from_database(
        connection,
        build_key=build_key,
        summary_json=summary_json,
    )


def publish_precomputed_artifacts(
    engine: Engine,
    *,
    hashes: BuildHashes,
    extract_path: str,
    walk_rows: Iterable[dict[str, Any]],
    amenity_rows: Iterable[dict[str, Any]],
    python_version: str,
    packages_json: dict[str, Any],
    summary_json: dict[str, Any],
    transport_reality_rows: Iterable[dict[str, Any]] = (),
    service_desert_rows: Iterable[dict[str, Any]] = (),
    noise_rows: Iterable[dict[str, Any]] = (),
    study_area_wgs84=None,
    noise_study_area_wgs84=_UNSET,
    noise_processing_hash: str | None = None,
    noise_artifact_hash: str | None = None,
    progress_cb: ProgressCallback | None = None,
) -> None:
    # If noise_study_area_wgs84 is explicitly provided (even as None), use it for
    # noise polygon clipping; otherwise fall back to study_area_wgs84.
    # Passing None disables clipping — correct for full-island profiles where the
    # artifact already covers the whole island and ST_Intersection is wasted work.
    effective_noise_area = (
        study_area_wgs84 if noise_study_area_wgs84 is _UNSET else noise_study_area_wgs84
    )
    created_at = datetime.now(timezone.utc)

    with engine.begin() as connection:
        if progress_cb is not None:
            progress_cb("detail", detail="deleting existing rows")
        for table in (
            grid_walk,
            amenities,
            transport_reality,
            service_deserts,
            noise_polygons,
            build_manifest,
        ):
            connection.execute(delete(table).where(table.c.build_key == hashes.build_key))

        if progress_cb is not None:
            progress_cb("detail", detail="writing manifest")
        connection.execute(
            insert(build_manifest),
            [
                {
                    "build_key": hashes.build_key,
                    "config_hash": hashes.config_hash,
                    "import_fingerprint": hashes.import_fingerprint,
                    "extract_path": extract_path,
                    "geo_hash": hashes.geo_hash,
                    "reach_hash": hashes.reach_hash,
                    "score_hash": hashes.score_hash,
                    "render_hash": hashes.render_hash,
                    "noise_processing_hash": noise_processing_hash,
                    "noise_artifact_hash": noise_artifact_hash,
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                    "python_version": python_version,
                    "packages_json": packages_json,
                    "summary_json": summary_json,
                }
            ],
        )

        _bulk_insert(connection, grid_walk, walk_rows, progress_cb=progress_cb)
        _bulk_insert(connection, amenities, amenity_rows, progress_cb=progress_cb)
        _bulk_insert(
            connection,
            transport_reality,
            transport_reality_rows,
            progress_cb=progress_cb,
        )
        _bulk_insert(
            connection,
            service_deserts,
            service_desert_rows,
            progress_cb=progress_cb,
        )
        _publish_noise_polygons(
            connection,
            noise_rows=noise_rows,
            build_key=hashes.build_key,
            config_hash=hashes.config_hash,
            import_fingerprint=hashes.import_fingerprint,
            render_hash=hashes.render_hash,
            noise_processing_hash=noise_processing_hash,
            noise_artifact_hash=noise_artifact_hash,
            created_at=created_at,
            study_area_wgs84=effective_noise_area,
            summary_json=summary_json,
            progress_cb=progress_cb,
        )

        if progress_cb is not None:
            progress_cb("detail", detail="finalizing manifest")
        connection.execute(
            update(build_manifest)
            .where(build_manifest.c.build_key == hashes.build_key)
            .values(
                status="complete",
                completed_at=datetime.now(timezone.utc),
                summary_json=summary_json,
                packages_json=packages_json,
                python_version=python_version,
            )
        )
