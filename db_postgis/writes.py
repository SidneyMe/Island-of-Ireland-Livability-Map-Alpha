from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BuildHashes, NOISE_PUBLISH_USE_COPY

from ._dependencies import (
    Column,
    Connection,
    DateTime,
    Engine,
    Float,
    Geometry,
    Integer,
    MetaData,
    Table,
    Text,
    delete,
    insert,
    text,
    update,
)
from .common import BATCH_SIZE, ProgressCallback, _table_key, root_module
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


NOISE_STAGE_BATCH_SIZE = 256


def _noise_stage_table(staging_name: str) -> Table:
    return Table(
        staging_name,
        MetaData(),
        Column("build_key", Text, nullable=False),
        Column("config_hash", Text, nullable=False),
        Column("import_fingerprint", Text, nullable=False),
        Column("jurisdiction", Text, nullable=False),
        Column("source_type", Text, nullable=False),
        Column("metric", Text, nullable=False),
        Column("round_number", Integer, nullable=False),
        Column("report_period", Text, nullable=True),
        Column("db_low", Float, nullable=True),
        Column("db_high", Float, nullable=True),
        Column("db_value", Text, nullable=False),
        Column("source_dataset", Text, nullable=False),
        Column("source_layer", Text, nullable=False),
        Column("source_ref", Text, nullable=False),
        Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )


def _prepare_noise_stage_chunk(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    root = root_module()
    prepared: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["geom"] = root.from_shape(payload["geom"], srid=4326)
        prepared.append(payload)
    return prepared


_NOISE_STAGE_COLUMNS: tuple[str, ...] = (
    "build_key",
    "config_hash",
    "import_fingerprint",
    "jurisdiction",
    "source_type",
    "metric",
    "round_number",
    "report_period",
    "db_low",
    "db_high",
    "db_value",
    "source_dataset",
    "source_layer",
    "source_ref",
    "geom",
    "created_at",
)


def _stage_noise_candidate_rows_via_copy(
    connection: Connection,
    *,
    staging_quoted: str,
    noise_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> int:
    import shapely.wkb as shapely_wkb

    column_list = ", ".join(_quote_identifier(name) for name in _NOISE_STAGE_COLUMNS)
    copy_sql = f"COPY {staging_quoted} ({column_list}) FROM STDIN"
    dbapi_conn = connection.connection.driver_connection
    cursor = dbapi_conn.cursor()
    total_inserted = 0
    progress_step = 5_000
    next_progress = progress_step
    if progress_cb is not None:
        progress_cb("live_start", detail="staging noise_polygons candidates (COPY)")
    else:
        print("  staging noise_polygons candidates (COPY)...", flush=True)
    try:
        with cursor.copy(copy_sql) as copy:
            for row in noise_rows:
                geom = row["geom"]
                ewkb_hex = shapely_wkb.dumps(geom, hex=True, srid=4326)
                copy.write_row(
                    (
                        row["build_key"],
                        row["config_hash"],
                        row["import_fingerprint"],
                        row["jurisdiction"],
                        row["source_type"],
                        row["metric"],
                        int(row["round_number"]),
                        row.get("report_period"),
                        row.get("db_low"),
                        row.get("db_high"),
                        row["db_value"],
                        row["source_dataset"],
                        row["source_layer"],
                        row["source_ref"],
                        ewkb_hex,
                        row["created_at"],
                    )
                )
                total_inserted += 1
                if progress_cb is not None and total_inserted >= next_progress:
                    progress_cb(
                        "detail",
                        detail=f"COPY staged {total_inserted:,} noise candidate rows",
                        force_log=True,
                    )
                    next_progress += progress_step
                elif progress_cb is None and total_inserted % progress_step == 0:
                    print(
                        f"  staging noise_polygons candidates (COPY): {total_inserted:,} rows so far...",
                        flush=True,
                    )
    finally:
        cursor.close()
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"COPY staged {total_inserted:,} noise candidate rows total",
            force_log=True,
        )
    return total_inserted


def _stage_noise_candidate_rows(
    connection: Connection,
    *,
    staging_quoted: str,
    noise_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> int:
    if NOISE_PUBLISH_USE_COPY:
        return _stage_noise_candidate_rows_via_copy(
            connection,
            staging_quoted=staging_quoted,
            noise_rows=noise_rows,
            progress_cb=progress_cb,
        )

    stage_table = _noise_stage_table(staging_quoted.strip('"'))
    total_inserted = 0
    chunk_index = 0
    if progress_cb is not None:
        progress_cb("live_start", detail="staging noise_polygons candidates")
    else:
        print("  staging noise_polygons candidates...", flush=True)

    for chunk in _chunked(noise_rows, size=NOISE_STAGE_BATCH_SIZE):
        chunk_index += 1
        chunk_size = len(chunk)
        connection.execute(insert(stage_table), _prepare_noise_stage_chunk(chunk))
        total_inserted += chunk_size
        if progress_cb is None:
            if chunk_index % 10 == 0:
                print(
                    f"  staging noise_polygons candidates: {total_inserted:,} rows so far...",
                    flush=True,
                )
            continue
        if chunk_index % 10 == 0 or chunk_size < NOISE_STAGE_BATCH_SIZE:
            progress_cb(
                "detail",
                detail=(
                    f"staged noise candidates batch {chunk_index:,}: "
                    f"{chunk_size:,} rows this batch | {total_inserted:,} rows total"
                ),
                force_log=True,
            )

    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"staged {total_inserted:,} noise candidate rows",
            force_log=True,
        )
    return total_inserted


def _create_noise_stage_indexes(
    connection: Connection,
    *,
    staging_quoted: str,
    progress_cb: ProgressCallback | None = None,
) -> None:
    if progress_cb is not None:
        progress_cb("detail", detail="indexing staged noise candidates", force_log=True)
    connection.execute(
        text(
            f"CREATE INDEX ON {staging_quoted} "
            "(build_key, jurisdiction, source_type, metric, round_number)"
        )
    )
    connection.execute(text(f"CREATE INDEX ON {staging_quoted} USING GIST (geom)"))
    connection.execute(text(f"ANALYZE {staging_quoted}"))


def _study_area_wkb(study_area_wgs84, *, simplify_tolerance: float = 0.0) -> bytes:
    if study_area_wgs84 is None:
        from shapely.geometry import box

        study_area_wgs84 = box(-180.0, -90.0, 180.0, 90.0)
    if simplify_tolerance > 0.0:
        study_area_wgs84 = study_area_wgs84.simplify(
            simplify_tolerance, preserve_topology=True
        )
    return bytes(study_area_wgs84.wkb)


def _noise_stage_groups(connection: Connection, *, staging_quoted: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            text(
                f"""
                SELECT
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    COUNT(*) AS row_count
                FROM {staging_quoted}
                GROUP BY jurisdiction, source_type, metric, round_number
                ORDER BY jurisdiction, source_type, metric, round_number DESC
                """
            )
        ).mappings()
    ]


def _materialize_noise_group_round(
    connection: Connection,
    *,
    staging_quoted: str,
    study_area_wkb: bytes,
    build_key: str,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
) -> int:
    target_name = _qualified_table_name(noise_polygons)
    insert_columns = _table_column_names(noise_polygons)
    quoted_insert_columns = ", ".join(_quote_identifier(name) for name in insert_columns)
    attr_columns = [name for name in insert_columns if name != "geom"]
    base_columns = ", ".join(f"s.{_quote_identifier(name)}" for name in attr_columns)
    clipped_columns = ", ".join(f"c.{_quote_identifier(name)}" for name in attr_columns)
    select_columns = ", ".join(f"r.{_quote_identifier(name)}" for name in attr_columns)
    final_select_columns = ", ".join(
        "geom" if name == "geom" else _quote_identifier(name)
        for name in insert_columns
    )
    result = connection.execute(
        text(
            f"""
            WITH study AS (
                SELECT ST_SetSRID(ST_GeomFromWKB(:study_area_wkb), 4326) AS geom
            ),
            clipped AS (
                SELECT
                    {base_columns},
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Intersection(
                                CASE
                                    WHEN ST_IsValid(s.geom) THEN s.geom
                                    ELSE ST_MakeValid(s.geom)
                                END,
                                study.geom
                            )
                        ),
                        3
                    ) AS geom
                FROM {staging_quoted} AS s
                CROSS JOIN study
                WHERE s.build_key = :build_key
                  AND s.jurisdiction = :jurisdiction
                  AND s.source_type = :source_type
                  AND s.metric = :metric
                  AND s.round_number = :round_number
                  AND s.geom && study.geom
                  AND ST_Intersects(s.geom, study.geom)
            ),
            resolved AS (
                SELECT
                    {clipped_columns},
                    CASE
                        WHEN newer.geom IS NULL OR ST_IsEmpty(newer.geom) THEN c.geom
                        ELSE ST_CollectionExtract(
                            ST_MakeValid(ST_Difference(c.geom, newer.geom)),
                            3
                        )
                    END AS geom
                FROM clipped AS c
                LEFT JOIN LATERAL (
                    SELECT ST_UnaryUnion(ST_Collect(n.geom)) AS geom
                    FROM {target_name} AS n
                    WHERE n.build_key = :build_key
                      AND n.jurisdiction = :jurisdiction
                      AND n.source_type = :source_type
                      AND n.metric = :metric
                      AND n.geom && c.geom
                      AND ST_Intersects(n.geom, c.geom)
                ) AS newer ON TRUE
                WHERE c.geom IS NOT NULL
                  AND NOT ST_IsEmpty(c.geom)
            ),
            pieces AS (
                SELECT
                    {select_columns},
                    piece.geom AS geom
                FROM resolved AS r
                CROSS JOIN LATERAL ST_Subdivide(r.geom, 256) AS piece(geom)
                WHERE r.geom IS NOT NULL
                  AND NOT ST_IsEmpty(r.geom)
            )
            INSERT INTO {target_name} ({quoted_insert_columns})
            SELECT
                {final_select_columns}
            FROM pieces
            WHERE NOT ST_IsEmpty(geom)
              AND ST_Dimension(geom) = 2
              AND ST_Area(geom) > 0
            """
        ),
        {
            "study_area_wkb": study_area_wkb,
            "build_key": build_key,
            "jurisdiction": jurisdiction,
            "source_type": source_type,
            "metric": metric,
            "round_number": int(round_number),
        },
    )
    return max(int(result.rowcount or 0), 0)


def _count_noise_values(
    connection: Connection,
    *,
    build_key: str,
    field_name: str,
) -> dict[str, int]:
    target_name = _qualified_table_name(noise_polygons)
    quoted_field = _quote_identifier(field_name)
    rows = connection.execute(
        text(
            f"""
            SELECT {quoted_field} AS value, COUNT(*) AS row_count
            FROM {target_name}
            WHERE build_key = :build_key
            GROUP BY {quoted_field}
            ORDER BY {quoted_field}
            """
        ),
        {"build_key": build_key},
    ).mappings()
    return {
        str(row["value"]): int(row["row_count"])
        for row in rows
        if row["value"] is not None
    }


def _update_noise_summary_from_database(
    connection: Connection,
    *,
    build_key: str,
    summary_json: dict[str, Any],
) -> None:
    noise_counts = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="jurisdiction",
    )
    summary_json["noise_enabled"] = bool(noise_counts)
    summary_json["noise_counts"] = noise_counts
    summary_json["noise_source_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="source_type",
    )
    summary_json["noise_metric_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="metric",
    )
    summary_json["noise_band_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="db_value",
    )


def _materialize_noise_polygons_from_stage(
    connection: Connection,
    *,
    staging_quoted: str,
    build_key: str,
    study_area_wgs84,
    progress_cb: ProgressCallback | None = None,
) -> int:
    groups = _noise_stage_groups(connection, staging_quoted=staging_quoted)
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"materializing noise fallback in PostGIS for {len(groups):,} group-rounds",
            force_log=True,
        )
    # Simplify the study area before using it as a clip boundary in PostGIS.
    # The ungeneralised island boundary is ~10 MB of WKB; ST_Intersection against
    # it for tens of thousands of rows takes hours. 0.005° ≈ 500 m tolerance is
    # more than adequate for trimming noise polygons to the island boundary.
    study_wkb = _study_area_wkb(study_area_wgs84, simplify_tolerance=0.005)
    total_inserted = 0
    for group_index, group in enumerate(groups, start=1):
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    "materializing noise "
                    f"{group_index:,}/{len(groups):,}: "
                    f"{group['jurisdiction']} {group['source_type']} "
                    f"{group['metric']} round {group['round_number']} "
                    f"({int(group['row_count']):,} staged rows)"
                ),
                force_log=True,
            )
        inserted = _materialize_noise_group_round(
            connection,
            staging_quoted=staging_quoted,
            study_area_wkb=study_wkb,
            build_key=build_key,
            jurisdiction=str(group["jurisdiction"]),
            source_type=str(group["source_type"]),
            metric=str(group["metric"]),
            round_number=int(group["round_number"]),
        )
        total_inserted += inserted
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    "materialized noise "
                    f"{group['jurisdiction']} {group['source_type']} "
                    f"{group['metric']} round {group['round_number']}: "
                    f"{inserted:,} final polygons"
                ),
                force_log=True,
            )
    return total_inserted


def _clone_noise_polygons_from_prior_build(
    connection: Connection,
    *,
    new_build_key: str,
    new_config_hash: str,
    new_import_fingerprint: str,
    render_hash: str,
    noise_processing_hash: str | None,
    created_at: datetime,
    progress_cb: ProgressCallback | None = None,
) -> int:
    target_name = _qualified_table_name(noise_polygons)
    # Match on render_hash (config unchanged) OR noise_processing_hash (noise
    # inputs unchanged even if other config changed).  The noise_processing_hash
    # column is NULL for builds before migration 000013 so those are correctly
    # ignored by the IS NOT DISTINCT FROM / = comparison.
    params: dict[str, Any] = {
        "render_hash": render_hash,
        "new_build_key": new_build_key,
        "noise_processing_hash": noise_processing_hash,
    }
    prior = connection.execute(
        text(
            """
            SELECT build_key
            FROM build_manifest
            WHERE (
                render_hash = :render_hash
                OR (
                    CAST(:noise_processing_hash AS TEXT) IS NOT NULL
                    AND noise_processing_hash = :noise_processing_hash
                )
            )
              AND status = 'complete'
              AND build_key <> :new_build_key
              AND EXISTS (
                  SELECT 1 FROM noise_polygons np
                  WHERE np.build_key = build_manifest.build_key
              )
            ORDER BY completed_at DESC NULLS LAST
            LIMIT 1
            """
        ),
        params,
    ).scalar_one_or_none()
    if not prior:
        return 0
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=(
                f"reusing noise_polygons from prior build {str(prior)[:12]} "
                f"(render_hash match)"
            ),
            force_log=True,
        )
    result = connection.execute(
        text(
            f"""
            INSERT INTO {target_name} (
                build_key, config_hash, import_fingerprint, jurisdiction,
                source_type, metric, round_number, report_period, db_low,
                db_high, db_value, source_dataset, source_layer, source_ref,
                geom, created_at
            )
            SELECT
                :new_build_key, :new_config_hash, :new_import_fingerprint,
                jurisdiction, source_type, metric, round_number, report_period,
                db_low, db_high, db_value, source_dataset, source_layer,
                source_ref, geom, :created_at
            FROM {target_name}
            WHERE build_key = :prior_build_key
            """
        ),
        {
            "new_build_key": new_build_key,
            "new_config_hash": new_config_hash,
            "new_import_fingerprint": new_import_fingerprint,
            "prior_build_key": str(prior),
            "created_at": created_at,
        },
    )
    cloned = max(int(result.rowcount or 0), 0)
    if progress_cb is not None and cloned > 0:
        progress_cb(
            "detail",
            detail=f"cloned {cloned:,} noise_polygons rows from prior build",
            force_log=True,
        )
    return cloned


def _drain_iterable(rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    for _ in rows:
        count += 1
    return count


def copy_noise_artifact_to_noise_polygons(
    connection: Connection,
    *,
    noise_resolved_hash: str,
    build_key: str,
    config_hash: str,
    import_fingerprint: str,
    study_area_wgs84=None,
) -> int:
    """
    Artifact mode direct copy: noise_resolved_display (EPSG:2157) → noise_polygons (EPSG:4326).

    No candidate staging, no ST_Difference, no ST_Subdivide.
    source_dataset/source_layer/source_ref are lossy compatibility placeholders.
    Returns row count inserted.
    """
    study_area_clause = ""
    params: dict[str, Any] = {
        "build_key": build_key,
        "config_hash": config_hash,
        "import_fingerprint": import_fingerprint,
        "noise_resolved_hash": noise_resolved_hash,
    }
    if study_area_wgs84 is not None:
        study_area_clause = (
            "AND ST_Intersects(d.geom, "
            "ST_Transform(ST_SetSRID(ST_GeomFromWKB(:study_wkb), 4326), 2157))"
        )
        params["study_wkb"] = study_area_wgs84.wkb

    result = connection.execute(
        text(
            f"""
            INSERT INTO noise_polygons (
                build_key, config_hash, import_fingerprint,
                jurisdiction, source_type, metric, round_number, report_period,
                db_low, db_high, db_value,
                source_dataset, source_layer, source_ref,
                geom, created_at
            )
            SELECT
                :build_key, :config_hash, :import_fingerprint,
                d.jurisdiction, d.source_type, d.metric,
                d.round_number, d.report_period,
                d.db_low, d.db_high, d.db_value,
                COALESCE(MIN(p.source_dataset), 'noise_artifact')  AS source_dataset,
                COALESCE(MIN(p.source_layer),  :noise_resolved_hash) AS source_layer,
                d.noise_feature_id::text                           AS source_ref,
                ST_Transform(d.geom, 4326)                         AS geom,
                now()
            FROM noise_resolved_display d
            LEFT JOIN noise_resolved_provenance p
                ON  p.noise_resolved_hash = d.noise_resolved_hash
                AND p.jurisdiction        = d.jurisdiction
                AND p.source_type         = d.source_type
                AND p.metric              = d.metric
                AND p.round_number        = d.round_number
            WHERE d.noise_resolved_hash = :noise_resolved_hash
            {study_area_clause}
            GROUP BY
                d.noise_resolved_hash, d.noise_feature_id,
                d.jurisdiction, d.source_type, d.metric,
                d.round_number, d.report_period,
                d.db_low, d.db_high, d.db_value, d.geom
            """
        ),
        params,
    )
    return max(int(result.rowcount or 0), 0)


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
    noise_processing_hash: str | None = None,
    noise_artifact_hash: str | None = None,
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()

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
            study_area_wgs84=study_area_wgs84,
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
