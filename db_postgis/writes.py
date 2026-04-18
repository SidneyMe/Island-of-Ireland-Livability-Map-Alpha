from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BuildHashes

from ._dependencies import Connection, Engine, Table, delete, insert, text, update
from .common import BATCH_SIZE, ProgressCallback, _table_key, root_module
from .schema import _quote_identifier
from .tables import (
    amenities,
    build_manifest,
    features,
    grid_walk,
    import_manifest,
    service_deserts,
    transport_reality,
    transit_calendar_dates,
    transit_calendar_services,
    transit_feed_manifest,
    transit_gtfs_stop_service_summary,
    transit_gtfs_stop_reality,
    transit_reality_manifest,
    transit_routes,
    transit_service_classification,
    transit_service_desert_cells,
    transit_stop_times,
    transit_stops,
    transit_trips,
)


def _chunked(rows: Iterable[dict[str, Any]], size: int = BATCH_SIZE) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _prepare_chunk(table: Table, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    geometry_fields = root_module().GEOMETRY_FIELDS.get(_table_key(table), ())
    root = root_module()
    for row in rows:
        payload = dict(row)
        for field_name in geometry_fields:
            payload[field_name] = root.from_shape(payload[field_name], srid=4326)
        prepared.append(payload)
    return prepared


def _bulk_insert(
    connection: Connection,
    table: Table,
    rows: Iterable[dict[str, Any]],
    *,
    progress_cb: ProgressCallback | None = None,
) -> None:
    chunk_index = 0
    total_inserted = 0
    root = root_module()

    table_key = _table_key(table)

    if progress_cb is not None:
        progress_cb("live_start", detail=f"inserting {table_key}")
    else:
        print(f"  inserting {table_key}...", flush=True)

    for chunk in root._chunked(rows):
        chunk_index += 1
        chunk_size = len(chunk)
        connection.execute(insert(table), root._prepare_chunk(table, chunk))
        total_inserted += chunk_size

        if progress_cb is None:
            if chunk_index % 10 == 0:
                print(f"  inserting {table_key}: {total_inserted:,} rows so far...", flush=True)
            continue

        progress_cb("advance", units=chunk_size)
        if chunk_index % 2 != 0:
            continue
        progress_cb(
            "detail",
            detail=(
                f"inserting {table_key} batch {chunk_index:,}: "
                f"{chunk_size:,} rows this batch | "
                f"{total_inserted:,} rows total"
            ),
            force_log=True,
        )


def _bulk_copy(
    connection: Connection,
    table: Table,
    rows: Iterable[dict[str, Any]],
    *,
    progress_cb: ProgressCallback | None = None,
) -> None:
    root = root_module()
    table_key = _table_key(table)
    if table_key in root.GEOMETRY_FIELDS:
        raise ValueError(f"_bulk_copy cannot handle geometry columns on {table_key}")

    if progress_cb is not None:
        progress_cb("live_start", detail=f"inserting {table_key}")
    else:
        print(f"  inserting {table_key}...", flush=True)

    column_names = [col.name for col in table.columns]
    column_list = ", ".join(_quote_identifier(name) for name in column_names)
    if table.schema:
        qualified = f"{_quote_identifier(table.schema)}.{_quote_identifier(table.name)}"
    else:
        qualified = _quote_identifier(table.name)
    copy_sql = f"COPY {qualified} ({column_list}) FROM STDIN"

    dbapi_conn = connection.connection.driver_connection
    cursor = dbapi_conn.cursor()
    chunk_index = 0
    total_inserted = 0
    batch_count = 0
    try:
        with cursor.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(tuple(row.get(name) for name in column_names))
                total_inserted += 1
                batch_count += 1
                if batch_count < BATCH_SIZE:
                    continue
                chunk_index += 1
                reported = batch_count
                batch_count = 0
                if progress_cb is None:
                    if chunk_index % 10 == 0:
                        print(
                            f"  inserting {table_key}: {total_inserted:,} rows so far...",
                            flush=True,
                        )
                    continue
                progress_cb("advance", units=reported)
                if chunk_index % 2 != 0:
                    continue
                progress_cb(
                    "detail",
                    detail=(
                        f"inserting {table_key} batch {chunk_index:,}: "
                        f"{reported:,} rows this batch | "
                        f"{total_inserted:,} rows total"
                    ),
                    force_log=True,
                )
            if batch_count > 0 and progress_cb is not None:
                progress_cb("advance", units=batch_count)
    finally:
        cursor.close()


def _qualified_table_name(table: Table) -> str:
    if table.schema:
        return f"{_quote_identifier(table.schema)}.{_quote_identifier(table.name)}"
    return _quote_identifier(table.name)


def _table_column_names(table: Table, *, exclude: Iterable[str] = ()) -> list[str]:
    excluded = {str(name) for name in exclude}
    return [col.name for col in table.columns if col.name not in excluded]


def _csv_copy_value(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    if raw_value == "":
        return None
    return raw_value


def _copy_from_csv_file(
    connection: Connection,
    *,
    qualified_table: str,
    table_key: str,
    column_names: list[str],
    csv_path: Path,
    progress_cb: ProgressCallback | None = None,
) -> int:
    if progress_cb is not None:
        progress_cb("live_start", detail=f"inserting {table_key}")
    else:
        print(f"  inserting {table_key}...", flush=True)

    column_list = ", ".join(_quote_identifier(name) for name in column_names)
    copy_sql = f"COPY {qualified_table} ({column_list}) FROM STDIN"
    dbapi_conn = connection.connection.driver_connection
    cursor = dbapi_conn.cursor()
    chunk_index = 0
    total_inserted = 0
    batch_count = 0

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle, cursor.copy(copy_sql) as copy:
            reader = csv.DictReader(handle)
            for row in reader:
                copy.write_row(tuple(_csv_copy_value(row.get(name)) for name in column_names))
                total_inserted += 1
                batch_count += 1
                if batch_count < BATCH_SIZE:
                    continue
                chunk_index += 1
                reported = batch_count
                batch_count = 0
                if progress_cb is None:
                    if chunk_index % 10 == 0:
                        print(
                            f"  inserting {table_key}: {total_inserted:,} rows so far...",
                            flush=True,
                        )
                    continue
                progress_cb("advance", units=reported)
                if chunk_index % 2 != 0:
                    continue
                progress_cb(
                    "detail",
                    detail=(
                        f"inserting {table_key} batch {chunk_index:,}: "
                        f"{reported:,} rows this batch | "
                        f"{total_inserted:,} rows total"
                    ),
                    force_log=True,
                )
            if batch_count > 0 and progress_cb is not None:
                progress_cb("advance", units=batch_count)
    finally:
        cursor.close()

    return total_inserted


def _create_temp_csv_stage(
    connection: Connection,
    *,
    target_table: Table,
    staging_name: str,
    deferred_target_columns: Iterable[str] = (),
    extra_stage_columns: Iterable[tuple[str, str]] = (),
) -> str:
    target_name = _qualified_table_name(target_table)
    staging_quoted = _quote_identifier(staging_name)
    connection.execute(text(f"DROP TABLE IF EXISTS {staging_quoted}"))
    connection.execute(
        text(
            f"CREATE TEMP TABLE {staging_quoted} "
            f"(LIKE {target_name} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
    )
    for column_name in deferred_target_columns:
        connection.execute(
            text(
                f"ALTER TABLE {staging_quoted} "
                f"ALTER COLUMN {_quote_identifier(column_name)} DROP NOT NULL"
            )
        )
    if extra_stage_columns:
        connection.execute(
            text(
                f"ALTER TABLE {staging_quoted} "
                + ", ".join(
                    f"ADD COLUMN {_quote_identifier(column_name)} {column_type}"
                    for column_name, column_type in extra_stage_columns
                )
            )
        )
    return staging_quoted


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


def replace_gtfs_feed_rows(
    engine: Engine,
    *,
    feed_fingerprint: str,
    feed_id: str,
    analysis_date,
    source_path: str,
    source_url: str | None,
    stops_rows: Iterable[dict[str, Any]],
    route_rows: Iterable[dict[str, Any]],
    trip_rows: Iterable[dict[str, Any]],
    stop_time_rows: Iterable[dict[str, Any]],
    calendar_rows: Iterable[dict[str, Any]],
    calendar_date_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()
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
        root._bulk_insert(connection, transit_stops, stops_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_routes, route_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_trips, trip_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_stop_times, stop_time_rows, progress_cb=progress_cb)
        root._bulk_copy(
            connection,
            transit_calendar_services,
            calendar_rows,
            progress_cb=progress_cb,
        )
        root._bulk_copy(
            connection,
            transit_calendar_dates,
            calendar_date_rows,
            progress_cb=progress_cb,
        )
        connection.execute(
            update(transit_feed_manifest)
            .where(transit_feed_manifest.c.feed_fingerprint == feed_fingerprint)
            .values(status="complete", completed_at=datetime.now(timezone.utc))
        )


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


def replace_transit_reality_rows(
    engine: Engine,
    *,
    reality_fingerprint: str,
    import_fingerprint: str,
    analysis_date,
    transit_config_hash: str,
    feed_fingerprints_json: dict[str, str],
    service_classification_rows: Iterable[dict[str, Any]],
    stop_summary_rows: Iterable[dict[str, Any]],
    gtfs_stop_reality_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()
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
        root._bulk_insert(
            connection,
            transit_service_classification,
            service_classification_rows,
            progress_cb=progress_cb,
        )
        root._bulk_insert(
            connection,
            transit_gtfs_stop_service_summary,
            stop_summary_rows,
            progress_cb=progress_cb,
        )
        root._bulk_insert(
            connection,
            transit_gtfs_stop_reality,
            gtfs_stop_reality_rows,
            progress_cb=progress_cb,
        )
        connection.execute(
            update(transit_reality_manifest)
            .where(transit_reality_manifest.c.reality_fingerprint == reality_fingerprint)
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


def replace_service_desert_rows(
    engine: Engine,
    *,
    build_key: str,
    desert_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    root = root_module()
    with engine.begin() as connection:
        connection.execute(
            delete(transit_service_desert_cells).where(transit_service_desert_cells.c.build_key == build_key)
        )
        root._bulk_insert(
            connection,
            transit_service_desert_cells,
            desert_rows,
            progress_cb=progress_cb,
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
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()

    with engine.begin() as connection:
        if progress_cb is not None:
            progress_cb("detail", detail="deleting existing rows")
        for table in (grid_walk, amenities, transport_reality, service_deserts, build_manifest):
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
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                    "python_version": python_version,
                    "packages_json": packages_json,
                    "summary_json": summary_json,
                }
            ],
        )

        root._bulk_insert(connection, grid_walk, walk_rows, progress_cb=progress_cb)
        root._bulk_insert(connection, amenities, amenity_rows, progress_cb=progress_cb)
        root._bulk_insert(
            connection,
            transport_reality,
            transport_reality_rows,
            progress_cb=progress_cb,
        )
        root._bulk_insert(
            connection,
            service_deserts,
            service_desert_rows,
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
