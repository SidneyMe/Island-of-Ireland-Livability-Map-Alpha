from __future__ import annotations

import os
import queue
import subprocess
import threading
import time
from datetime import datetime, timezone

from sqlalchemy.engine.url import make_url

from config import (
    OSM2PGSQL_CACHE_MB,
    OSM2PGSQL_FLAT_NODES_PATH,
    OSM2PGSQL_NUMBER_PROCESSES,
    OSM_IMPORTER_BIN,
    OSM_IMPORTER_CONFIG,
    OSM_IMPORT_SCHEMA,
    SourceState,
    build_source_state,
    database_url,
)
from db_postgis import (
    begin_import_manifest,
    clear_normalized_import_artifacts,
    complete_import_manifest,
    drop_importer_owned_raw_tables,
    ensure_managed_raw_support_tables,
    import_payload_ready,
    osm2pgsql_properties_exists,
    raw_import_ready,
)

from . import orchestrator as _orchestrator
from . import osm2pgsql as _osm2pgsql
from . import rules as _rules


_PRIVATE_VALUES = set(_rules.PRIVATE_VALUES)
_WALK_EXCLUDED = set(_rules.WALK_EXCLUDED)


def _emit_detail(progress_cb, detail: str) -> None:
    if progress_cb is None:
        print(detail, flush=True)
        return
    progress_cb("detail", detail=detail, force_log=True)


def detect_importer_version() -> str:
    return _osm2pgsql.detect_importer_version_impl(
        OSM_IMPORTER_BIN,
        subprocess_module=subprocess,
    )


def resolve_source_state() -> SourceState:
    return _osm2pgsql.resolve_source_state_impl(
        build_source_state_fn=build_source_state,
        detect_importer_version_fn=detect_importer_version,
    )


def _query_value(url, key: str) -> str | None:
    return _osm2pgsql.query_value_impl(url, key)


def _connection_arguments() -> tuple[list[str], dict[str, str]]:
    return _osm2pgsql.connection_arguments_impl(
        database_url_fn=database_url,
        make_url_fn=make_url,
    )


def _stream_subprocess_lines(process: subprocess.Popen[str], progress_cb=None) -> list[str]:
    return _osm2pgsql.stream_subprocess_lines_impl(
        process,
        progress_cb=progress_cb,
        emit_detail_fn=_emit_detail,
        queue_module=queue,
        threading_module=threading,
        time_module=time,
    )


def _run_osm2pgsql_import(source_state: SourceState, progress_cb=None) -> None:
    _osm2pgsql.run_osm2pgsql_import_impl(
        source_state,
        progress_cb=progress_cb,
        importer_bin=OSM_IMPORTER_BIN,
        importer_config=OSM_IMPORTER_CONFIG,
        import_schema=OSM_IMPORT_SCHEMA,
        connection_arguments_fn=_connection_arguments,
        emit_detail_fn=_emit_detail,
        cache_mb=OSM2PGSQL_CACHE_MB,
        flat_nodes_path=OSM2PGSQL_FLAT_NODES_PATH,
        number_processes=OSM2PGSQL_NUMBER_PROCESSES,
        subprocess_module=subprocess,
        stream_subprocess_lines_fn=_stream_subprocess_lines,
        os_module=os,
        datetime_cls=datetime,
        timezone_cls=timezone,
    )


def _is_private(tags: dict[str, object], *keys: str) -> bool:
    return _rules.is_private_impl(tags, *keys, private_values=_PRIVATE_VALUES)


def _is_walkable(tags: dict[str, object]) -> bool:
    return _rules.is_walkable_impl(
        tags,
        is_private_fn=_is_private,
        walk_excluded=_WALK_EXCLUDED,
    )


def ensure_local_osm_import(
    engine,
    source_state: SourceState,
    *,
    study_area_wgs84,
    normalization_scope_hash: str,
    force_refresh: bool = False,
    progress_cb=None,
) -> None:
    _orchestrator.ensure_local_osm_import_impl(
        engine,
        source_state,
        study_area_wgs84=study_area_wgs84,
        normalization_scope_hash=normalization_scope_hash,
        force_refresh=force_refresh,
        progress_cb=progress_cb,
        import_payload_ready_fn=import_payload_ready,
        raw_import_ready_fn=raw_import_ready,
        osm2pgsql_properties_exists_fn=osm2pgsql_properties_exists,
        drop_importer_owned_raw_tables_fn=drop_importer_owned_raw_tables,
        run_osm2pgsql_import_fn=_run_osm2pgsql_import,
        ensure_managed_raw_support_tables_fn=ensure_managed_raw_support_tables,
        begin_import_manifest_fn=begin_import_manifest,
        complete_import_manifest_fn=complete_import_manifest,
        clear_normalized_import_artifacts_fn=clear_normalized_import_artifacts,
        emit_detail_fn=_emit_detail,
    )


__all__ = [
    "OSM_IMPORTER_BIN",
    "OSM_IMPORTER_CONFIG",
    "OSM_IMPORT_SCHEMA",
    "SourceState",
    "build_source_state",
    "database_url",
    "detect_importer_version",
    "ensure_local_osm_import",
    "resolve_source_state",
    "_PRIVATE_VALUES",
    "_WALK_EXCLUDED",
    "_connection_arguments",
    "_emit_detail",
    "_is_private",
    "_is_walkable",
    "_query_value",
    "_run_osm2pgsql_import",
    "_stream_subprocess_lines",
]
