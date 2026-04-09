from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

from config import BuildHashes

from ._dependencies import Connection, Engine, Table, delete, insert, update
from .common import BATCH_SIZE, ProgressCallback, _table_key, root_module
from .tables import amenities, build_manifest, features, grid_walk, import_manifest


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

    if progress_cb is not None:
        progress_cb("live_start", detail=f"inserting {_table_key(table)}")

    for chunk in root._chunked(rows):
        chunk_index += 1
        chunk_size = len(chunk)
        connection.execute(insert(table), root._prepare_chunk(table, chunk))
        total_inserted += chunk_size

        if progress_cb is None:
            continue

        progress_cb("advance", units=chunk_size)
        if chunk_index % 2 != 0:
            continue
        progress_cb(
            "detail",
            detail=(
                f"inserting {_table_key(table)} batch {chunk_index:,}: "
                f"{chunk_size:,} rows this batch | "
                f"{total_inserted:,} rows total"
            ),
            force_log=True,
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
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()

    with engine.begin() as connection:
        if progress_cb is not None:
            progress_cb("detail", detail="deleting existing rows")
        for table in (grid_walk, amenities, build_manifest):
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
