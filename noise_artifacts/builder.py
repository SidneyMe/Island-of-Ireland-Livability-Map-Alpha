"""
Orchestration layer for the noise artifact pipeline.

Called only by `python -m noise_artifacts`. The livability build does NOT call this.
"""
from __future__ import annotations

import logging
import time
import traceback
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .dissolve import dissolve_noise_into_staging, drop_staging_tables
from .ingest import ingest_noise_normalized
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


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


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
    force: bool = False,
    force_resolved: bool = False,
    reimport_source: bool = False,
    progress_cb=None,
) -> dict[str, Any]:
    """
    Run the full noise artifact pipeline:

    1. Create/reset artifact manifest rows (source, domain, resolved)
    2. Record lineage: resolved → source, resolved → domain
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

    # --- 1. Create/reset artifact rows (parent rows BEFORE lineage) ---
    _progress(progress_cb, "ensuring artifact manifests")
    src_status = _ensure_artifact(engine, source_hash, "source", {}, force=reimport_source)
    _ensure_artifact(engine, domain_hash, "domain", {}, force=False)
    res_status = _ensure_artifact(engine, resolved_hash, "resolved", {}, force=force_resolved)

    # --- Fast path: resolved artifact is already complete and we're not forcing ---
    if res_status == "already_complete":
        log.info("resolved artifact already complete (resolved_hash=%s); skipping rebuild", resolved_hash)
        set_active_artifact(engine, "resolved", resolved_hash)
        counts = _compute_artifact_counts(engine, resolved_hash)
        return {**counts, "resolved_hash": resolved_hash, "source_hash": source_hash}

    # --- 2. Lineage (parents already exist above) ---
    record_lineage(engine, resolved_hash, source_hash)
    record_lineage(engine, resolved_hash, domain_hash)

    dissolve_table = None
    round_table = None
    try:
        # --- 3. Ingest raw source ---
        _progress(progress_cb, "ingest start")
        log.info("ingesting raw source → noise_normalized (source_hash=%s)", source_hash)
        ingest_started = time.perf_counter()
        n_ingested = ingest_noise_normalized(
            engine, source_hash, data_dir, domain_wgs84,
            reimport_source=(reimport_source or src_status == "reset"),
            progress_cb=progress_cb,
        )
        _timing(progress_cb, "ingest.total", time.perf_counter() - ingest_started)
        _progress(progress_cb, f"ingest done: {n_ingested} rows")

        # --- 4. Two-pass dissolve ---
        _progress(
            progress_cb,
            f"dissolve start: tile_size={tile_size_metres}m grid={topology_grid_metres}m",
        )
        log.info("running two-pass dissolve (source_hash=%s resolved_hash=%s)",
                 source_hash, resolved_hash)
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

        # --- 5. Round-priority resolve ---
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
        _timing(progress_cb, "resolve.total", time.perf_counter() - resolve_started)
        _progress(progress_cb, f"resolve done: {resolve_result['total_inserted']} rows")
        log.info("resolved: %s", resolve_result)

        # --- 6. Compute counts and mark complete ---
        counts = _compute_artifact_counts(engine, resolved_hash)
        mark_artifact_complete(engine, resolved_hash, updated_manifest_json=counts)
        mark_artifact_complete(engine, source_hash, updated_manifest_json={})
        mark_artifact_complete(engine, domain_hash, updated_manifest_json={})
        set_active_artifact(engine, "resolved", resolved_hash)

        _progress(
            progress_cb,
            f"artifact complete: resolved_hash={resolved_hash} rows={counts}",
        )
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




