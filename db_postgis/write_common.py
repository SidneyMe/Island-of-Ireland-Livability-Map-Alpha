from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from ._dependencies import Connection, Table, insert, text
from .common import BATCH_SIZE, ProgressCallback, _table_key, root_module
from .schema import _quote_identifier


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
