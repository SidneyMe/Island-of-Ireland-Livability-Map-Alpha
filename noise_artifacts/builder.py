"""
Orchestration layer for the noise artifact pipeline.

Called only by `python -m noise_artifacts`. The livability build does NOT call this.
"""
from __future__ import annotations

import logging
import os
import shutil
import time
import traceback
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .dev_fast import (
    apply_accurate_simplification,
    build_dev_fast_road_rail_grid,
    materialize_dev_fast_resolved,
)
from .dissolve import dissolve_noise_into_staging, drop_staging_tables
from .ingest import INGEST_SCHEMA_VERSION, ingest_noise_normalized
from .manifest import (
    mark_artifact_complete,
    mark_artifact_failed,
    record_lineage,
    reset_artifact_for_retry,
    set_active_artifact,
    upsert_artifact,
)
from .resolve import materialize_resolved_display

log = logging.getLogger(__name__)

_MIN_FREE_DISK_GB_ENV = "NOISE_MIN_FREE_DISK_GB"
_MIN_FREE_DISK_GB_DEV_ENV = "NOISE_DEV_FAST_MIN_FREE_DISK_GB"
_MIN_FREE_DISK_GB_DEV_ENV_LEGACY = "NOISE_MIN_FREE_DISK_GB_DEV_FAST"
_MIN_FREE_DISK_GB_ACCURATE_ENV = "NOISE_MIN_FREE_DISK_GB_ACCURATE"
_DEFAULT_MIN_FREE_DISK_GB_DEV = 10.0
_DEFAULT_MIN_FREE_DISK_GB_ACCURATE = 30.0


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


def _min_free_disk_gb_for_mode(mode: str) -> float:
    raw_global = (os.getenv(_MIN_FREE_DISK_GB_ENV) or "").strip()
    if raw_global:
        return float(raw_global)
    if mode == "accurate":
        raw_mode = (os.getenv(_MIN_FREE_DISK_GB_ACCURATE_ENV) or "").strip()
        if raw_mode:
            return float(raw_mode)
        return _DEFAULT_MIN_FREE_DISK_GB_ACCURATE
    raw_mode = (os.getenv(_MIN_FREE_DISK_GB_DEV_ENV) or "").strip()
    if raw_mode:
        return float(raw_mode)
    raw_mode_legacy = (os.getenv(_MIN_FREE_DISK_GB_DEV_ENV_LEGACY) or "").strip()
    if raw_mode_legacy:
        return float(raw_mode_legacy)
    return _DEFAULT_MIN_FREE_DISK_GB_DEV


def _free_disk_gb(path: str) -> float:
    usage = shutil.disk_usage(path)
    return float(usage.free) / (1024.0 ** 3)


def _assert_disk_preflight(engine: Engine, *, mode: str, progress_cb=None) -> None:
    min_free_gb = _min_free_disk_gb_for_mode(mode)
    cache_root = os.path.abspath(".livability_cache")
    with engine.connect() as conn:
        pg_data_directory = str(conn.execute(text("SHOW data_directory")).scalar_one())
    cache_free = _free_disk_gb(cache_root)
    pg_free = _free_disk_gb(pg_data_directory)
    _progress(progress_cb, f"disk preflight: mode={mode} min_free_gb={min_free_gb:.1f}")
    _progress(progress_cb, f"disk preflight: cache_root={cache_root} free_gb={cache_free:.1f}")
    _progress(progress_cb, f"disk preflight: pg_data={pg_data_directory} free_gb={pg_free:.1f}")
    if cache_free < min_free_gb:
        raise RuntimeError(
            f"insufficient free disk in cache root for noise build: {cache_free:.1f}GB < {min_free_gb:.1f}GB"
        )
    if pg_free < min_free_gb:
        raise RuntimeError(
            f"insufficient free disk in postgres data_directory for noise build: {pg_free:.1f}GB < {min_free_gb:.1f}GB"
        )


def _advisory_lock_key(resolved_hash: str) -> str:
    return f"noise_artifact_build:{resolved_hash}"


def _source_row_count(engine: Engine, source_hash: str) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text("SELECT COUNT(*) FROM noise_normalized WHERE noise_source_hash = :h"),
            {"h": source_hash},
        ).scalar_one_or_none()
    if value is None:
        return 0
    return max(int(value), 0)


def _delete_source_types(engine: Engine, source_hash: str, source_types: tuple[str, ...]) -> int:
    source_types_norm = tuple(str(item).strip().lower() for item in source_types)
    if source_types_norm == ("road", "rail") or source_types_norm == ("rail", "road"):
        delete_sql = """
            DELETE FROM noise_normalized
            WHERE noise_source_hash = :h
              AND source_type IN ('road', 'rail')
        """
        params = {"h": source_hash}
    else:
        delete_sql = """
            DELETE FROM noise_normalized
            WHERE noise_source_hash = :h
              AND source_type = ANY(:source_types)
        """
        params = {"h": source_hash, "source_types": list(source_types)}
    with engine.begin() as conn:
        deleted = conn.execute(
            text(delete_sql),
            params,
        )
    return int(deleted.rowcount or 0)


def _assert_no_dev_fast_road_rail_normalized(engine: Engine, source_hash: str) -> None:
    with engine.connect() as conn:
        n = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM noise_normalized
                WHERE noise_source_hash = :h
                  AND source_type IN ('road', 'rail')
                """
            ),
            {"h": source_hash},
        ).scalar_one()
    if int(n or 0) > 0:
        raise RuntimeError(
            "BUG: dev_fast must not leave road/rail rows in noise_normalized. "
            f"Found {n} rows for noise_source_hash={source_hash}."
        )


def build_noise_artifact(
    engine: Engine,
    *,
    data_dir,
    domain_wgs84,
    domain_wkb: bytes,
    source_hash: str,
    domain_hash: str,
    resolved_hash: str,
    tile_size_metres: float = 10_000.0,
    topology_grid_metres: float = 0.1,
    noise_accuracy_mode: str = "dev_fast",
    grid_size_m: int = 1000,
    accurate_simplify_m: float = 25.0,
    latest_rounds_by_group: dict[str, int] | None = None,
    force: bool = False,
    force_resolved: bool = False,
    reimport_source: bool = False,
    progress_cb=None,
) -> dict[str, Any]:
    """
    Run the full noise artifact pipeline:

    1. Create/reset artifact manifest rows (source, domain, resolved)
    2. Record lineage: resolved -> source, resolved -> domain
    3. Ingest raw source into noise_normalized (EPSG:2157)
    4. Two-pass chunked dissolve into UNLOGGED staging tables
    5. Round-priority resolve into noise_resolved_display
    6. Mark artifacts complete and set noise_active_artifact pointer

    Force semantics:
      - force_resolved=True: rebuild resolved artifact while reusing source rows.
      - reimport_source=True: re-import source rows, then rebuild resolved artifact.
      - legacy force=True: same as enabling both flags.

    Returns summary dict. The caller (livability build) never calls this.
    """
    total_started = time.perf_counter()
    force_resolved = bool(force_resolved or force)
    reimport_source = bool(reimport_source or force)
    if reimport_source:
        force_resolved = True

    _progress(progress_cb, f"artifact build start: source={source_hash} resolved={resolved_hash}")
    lock_key = _advisory_lock_key(resolved_hash)
    with engine.connect() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_lock(hashtext(:lock_key))"), {"lock_key": lock_key})
        _progress(progress_cb, f"acquired noise artifact build lock for resolved_hash={resolved_hash}")
        try:
            _progress(progress_cb, "ensuring artifact manifests")
            src_status = _ensure_artifact(engine, source_hash, "source", {}, force=reimport_source)
            _ensure_artifact(engine, domain_hash, "domain", {}, force=False)
            res_status = _ensure_artifact(engine, resolved_hash, "resolved", {}, force=force_resolved)

            if res_status == "already_complete":
                log.info("resolved artifact already complete (resolved_hash=%s); skipping rebuild", resolved_hash)
                set_active_artifact(engine, "resolved", resolved_hash)
                counts = _compute_artifact_counts(engine, resolved_hash)
                return {**counts, "resolved_hash": resolved_hash, "source_hash": source_hash}

            record_lineage(engine, resolved_hash, source_hash)
            record_lineage(engine, resolved_hash, domain_hash)

            dissolve_table = None
            round_table = None
            try:
                mode_norm = str(noise_accuracy_mode or "dev_fast").strip().lower()
                _assert_disk_preflight(engine, mode=mode_norm, progress_cb=progress_cb)
                _progress(progress_cb, "ingest start")
                log.info("ingesting raw source -> noise_normalized (source_hash=%s)", source_hash)
                ingest_started = time.perf_counter()
                latest_round_only = True
                if mode_norm == "dev_fast":
                    exact_source_types = {"airport", "industry"}
                    _progress(
                        progress_cb,
                        "dev-fast source split: exact=airport,industry grid=road,rail "
                        "(road/rail raw staging disabled)",
                    )
                    deleted_road_rail = _delete_source_types(engine, source_hash, ("road", "rail"))
                    if deleted_road_rail > 0:
                        _progress(
                            progress_cb,
                            f"dev-fast cleanup: removed stale road/rail noise_normalized rows={deleted_road_rail:,} "
                            f"for noise_source_hash={source_hash}",
                        )
                    n_ingested = ingest_noise_normalized(
                        engine,
                        source_hash,
                        data_dir,
                        domain_wgs84,
                        reimport_source=(reimport_source or src_status == "reset"),
                        source_types=exact_source_types,
                        latest_round_only=latest_round_only,
                        progress_cb=progress_cb,
                    )
                else:
                    _progress(
                        progress_cb,
                        "accurate source split: exact=airport,industry and road/rail simplified polygons",
                    )
                    n_ingested = ingest_noise_normalized(
                        engine,
                        source_hash,
                        data_dir,
                        domain_wgs84,
                        reimport_source=(reimport_source or src_status == "reset"),
                        source_types=None,
                        latest_round_only=latest_round_only,
                        progress_cb=progress_cb,
                    )
                _timing(progress_cb, "ingest.total", time.perf_counter() - ingest_started)
                source_rows_total = _source_row_count(engine, source_hash)
                _progress(progress_cb, f"source ingest complete rows={source_rows_total:,}")
                _progress(progress_cb, f"ingest done: {n_ingested} rows")
                if mode_norm == "dev_fast":
                    _progress(progress_cb, "dev-fast: building road/rail coarse grid artifact")
                    grid_started = time.perf_counter()
                    grid_result = build_dev_fast_road_rail_grid(
                        engine,
                        data_dir=data_dir,
                        noise_source_hash=source_hash,
                        artifact_hash=resolved_hash,
                        grid_size_m=int(grid_size_m),
                        progress_cb=progress_cb,
                    )
                    _timing(progress_cb, "dev_fast.grid_build", time.perf_counter() - grid_started)
                    _progress(
                        progress_cb,
                        "dev-fast road/rail grid: "
                        f"grid_size={int(grid_size_m)}m source_rows={int(grid_result.get('source_rows', 0)):,} "
                        f"cells={int(grid_result.get('cell_rows', 0)):,}",
                    )
                    resolve_started = time.perf_counter()
                    resolved_rows = materialize_dev_fast_resolved(
                        engine,
                        noise_source_hash=source_hash,
                        noise_resolved_hash=resolved_hash,
                        grid_size_m=int(grid_size_m),
                        progress_cb=progress_cb,
                    )
                    _assert_no_dev_fast_road_rail_normalized(engine, source_hash)
                    _timing(progress_cb, "resolve.total", time.perf_counter() - resolve_started)
                    _progress(progress_cb, f"resolve done: {resolved_rows:,} rows")
                else:
                    _progress(
                        progress_cb,
                        f"accurate: simplifying road/rail polygons tolerance={float(accurate_simplify_m):.2f}m",
                    )
                    apply_accurate_simplification(
                        engine,
                        noise_source_hash=source_hash,
                        simplify_tolerance_m=float(accurate_simplify_m),
                        progress_cb=progress_cb,
                    )
                    _progress(progress_cb, f"dissolve start source_hash={source_hash}")
                    _progress(
                        progress_cb,
                        f"dissolve start: tile_size={tile_size_metres}m grid={topology_grid_metres}m",
                    )
                    log.info("running two-pass dissolve (source_hash=%s resolved_hash=%s)", source_hash, resolved_hash)
                    dissolve_started = time.perf_counter()
                    dissolve_table, round_table = dissolve_noise_into_staging(
                        engine,
                        source_hash=source_hash,
                        resolved_hash=resolved_hash,
                        tile_size_metres=tile_size_metres,
                        topology_grid_metres=topology_grid_metres,
                        progress_cb=progress_cb,
                    )
                    _timing(progress_cb, "dissolve.total", time.perf_counter() - dissolve_started)

                    _progress(progress_cb, "resolve start")
                    log.info("materializing resolved display (resolved_hash=%s)", resolved_hash)
                    resolve_started = time.perf_counter()
                    resolve_result = materialize_resolved_display(
                        engine,
                        noise_resolved_hash=resolved_hash,
                        round_table=round_table,
                        domain_wkb=domain_wkb,
                        topology_grid_metres=topology_grid_metres,
                        progress_cb=progress_cb,
                    )
                    resolve_elapsed = time.perf_counter() - resolve_started
                    _timing(progress_cb, "resolve.total", resolve_elapsed)
                    groups_processed = int(resolve_result.get("groups_processed", 0) or 0)
                    if groups_processed > 0:
                        _progress(progress_cb, f"resolve start groups={groups_processed}")
                    _progress(progress_cb, f"resolve complete rows={resolve_result['total_inserted']} elapsed={resolve_elapsed:.1f}s")
                    _progress(progress_cb, f"resolve done: {resolve_result['total_inserted']} rows")
                    log.info("resolved: %s", resolve_result)

                counts = _compute_artifact_counts(engine, resolved_hash)
                mark_artifact_complete(
                    engine,
                    resolved_hash,
                    updated_manifest_json={
                        **counts,
                        "noise_accuracy_mode": mode_norm,
                        "grid_size_m": int(grid_size_m),
                        "accurate_simplify_m": float(accurate_simplify_m),
                        "latest_rounds_by_group": dict(latest_rounds_by_group or {}),
                    },
                )
                mark_artifact_complete(
                    engine,
                    source_hash,
                    updated_manifest_json={
                        "row_count": source_rows_total,
                        "ingest_schema_version": INGEST_SCHEMA_VERSION,
                        "ingest_complete": True,
                        "noise_accuracy_mode": mode_norm,
                        "grid_size_m": int(grid_size_m),
                        "accurate_simplify_m": float(accurate_simplify_m),
                        "latest_rounds_by_group": dict(latest_rounds_by_group or {}),
                    },
                )
                mark_artifact_complete(engine, domain_hash, updated_manifest_json={})
                set_active_artifact(engine, "resolved", resolved_hash)

                _progress(progress_cb, f"artifact build complete resolved_hash={resolved_hash}")
                _progress(progress_cb, f"artifact complete: resolved_hash={resolved_hash} rows={counts}")
                _timing(progress_cb, "build.total", time.perf_counter() - total_started)
                log.info("artifact complete: resolved_hash=%s rows=%s", resolved_hash, counts)
                return {**counts, "resolved_hash": resolved_hash, "source_hash": source_hash}

            except Exception:
                detail = traceback.format_exc()
                log.error("artifact build failed: %s", detail)
                for h in (resolved_hash, source_hash, domain_hash):
                    try:
                        mark_artifact_failed(engine, h, error_detail=detail)
                    except Exception:
                        log.warning("failed to mark artifact failed: %s", h, exc_info=True)
                raise
            finally:
                if dissolve_table and round_table:
                    try:
                        drop_staging_tables(engine, dissolve_table, round_table)
                    except Exception:
                        log.warning("failed to drop staging tables", exc_info=True)
        finally:
            try:
                lock_conn.execute(text("SELECT pg_advisory_unlock(hashtext(:lock_key))"), {"lock_key": lock_key})
            finally:
                _progress(progress_cb, f"released noise artifact build lock for resolved_hash={resolved_hash}")

def _ensure_artifact(
    engine: Engine,
    artifact_hash: str,
    artifact_type: str,
    manifest_json: dict,
    *,
    force: bool,
) -> str:
    """
    Create or reset an artifact row.

    Returns:
        "created"          – new row inserted
        "reset"            – existing row reset (force or non-complete status)
        "already_complete" – existing complete row, force=False (caller may skip rebuild)
    """
    with engine.connect() as conn:
        existing = conn.execute(
            text(
                "SELECT status FROM noise_artifact_manifest "
                "WHERE artifact_hash = :h"
            ),
            {"h": artifact_hash},
        ).mappings().first()

    if existing is None:
        upsert_artifact(engine, artifact_hash, artifact_type, manifest_json)
        return "created"

    status = str(existing["status"])

    if force or status != "complete":
        reset_artifact_for_retry(engine, artifact_hash, manifest_json)
        _delete_prior_canonical_rows(engine, artifact_hash, artifact_type)
        return "reset"

    return "already_complete"


def _delete_prior_canonical_rows(
    engine: Engine, artifact_hash: str, artifact_type: str
) -> None:
    """Delete any partial canonical data left from a prior failed build."""
    with engine.begin() as conn:
        if artifact_type == "source":
            conn.execute(
                text("DELETE FROM noise_normalized WHERE noise_source_hash = :h"),
                {"h": artifact_hash},
            )
        elif artifact_type == "resolved":
            conn.execute(
                text("DELETE FROM noise_resolved_provenance WHERE noise_resolved_hash = :h"),
                {"h": artifact_hash},
            )
            conn.execute(
                text("DELETE FROM noise_resolved_display WHERE noise_resolved_hash = :h"),
                {"h": artifact_hash},
            )
            conn.execute(
                text("DELETE FROM noise_grid_artifact WHERE artifact_hash = :h"),
                {"h": artifact_hash},
            )


def _compute_artifact_counts(engine: Engine, resolved_hash: str) -> dict:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT jurisdiction) AS jurisdiction_count,
                    COUNT(DISTINCT source_type) AS source_type_count,
                    COUNT(DISTINCT metric) AS metric_count
                FROM noise_resolved_display
                WHERE noise_resolved_hash = :h
                """
            ),
            {"h": resolved_hash},
        ).mappings().first()

    return {
        "row_count": int(row["row_count"] or 0),
        "jurisdiction_count": int(row["jurisdiction_count"] or 0),
        "source_type_count": int(row["source_type_count"] or 0),
        "metric_count": int(row["metric_count"] or 0),
    }





