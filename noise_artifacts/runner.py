"""
Shared noise artifact build runner.

Encapsulates hash computation, domain loading, and the full build
orchestration so it can be called from both:
  - `python -m noise_artifacts` (standalone CLI)
  - `precompute/workflow.py` (auto-build when no active artifact exists)

Neither caller needs to duplicate source-signature or domain-hash logic.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

# Version constants — bump any to force re-ingest/re-resolve of all artifacts.
PARSER_VERSION = 1
SOURCE_SCHEMA_VERSION = 1
TOPOLOGY_RULES_VERSION = 1
DISSOLVE_RULES_VERSION = 1
ROUND_PRIORITY_VERSION = 1
EXTENT_VERSION = 1


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _load_domain_boundary_bytes(config) -> bytes:
    """SHA-256 over both ROI and NI boundary file contents for deterministic domain hashing."""
    h = hashlib.sha256()
    for path in (config.ROI_BOUNDARY_PATH, config.NI_BOUNDARY_PATH):
        h.update(str(path).encode("utf-8"))
        try:
            h.update(path.read_bytes())
        except OSError:
            pass
    return h.digest()


def _load_domain(config):
    """Return (domain_wgs84_shapely, domain_wkb_bytes) covering the full Island of Ireland."""
    from shapely.ops import transform
    from study_area import load_island_geometry_metric

    domain_2157 = load_island_geometry_metric()
    domain_wgs84 = transform(config.TO_WGS84, domain_2157)
    return domain_wgs84, domain_wgs84.wkb


def build_default_noise_artifact(
    engine: Engine,
    *,
    force: bool = False,
    data_dir: Path | None = None,
    progress_cb=None,
) -> dict[str, Any]:
    """
    Build (or verify current) the default noise artifact using standard config.

    Computes deterministic hashes from raw source files and the full-island domain
    boundary, then runs ingest → dissolve → resolve if needed.

    When ``force=False`` and the active artifact already matches the computed hash,
    skips rebuild and returns immediately.

    Returns dict with keys:
      status:        "up_to_date" | "built"
      artifact_hash: resolved artifact hash (str)
      row_count:     rows inserted into noise_resolved_display (0 if up_to_date)
    """
    import config
    from noise.loader import NOISE_DATA_DIR, dataset_signature

    from .builder import build_noise_artifact
    from .manifest import (
        get_active_artifact,
        noise_domain_hash,
        noise_resolved_hash,
        noise_source_hash,
    )

    resolved_data_dir = Path(data_dir) if data_dir is not None else NOISE_DATA_DIR

    _progress(progress_cb, f"computing raw noise source signature from {resolved_data_dir}")
    log.info("computing source signature from %s", resolved_data_dir)

    source_sig = dataset_signature(resolved_data_dir)
    src_hash = noise_source_hash(source_sig, PARSER_VERSION, SOURCE_SCHEMA_VERSION)
    _progress(progress_cb, f"source hash: {src_hash}")

    _progress(progress_cb, "loading island domain")
    domain_boundary_bytes = _load_domain_boundary_bytes(config)
    dom_hash = noise_domain_hash(domain_boundary_bytes, EXTENT_VERSION)
    _progress(progress_cb, f"domain hash: {dom_hash}")

    topology_grid_m = float(getattr(config, "NOISE_TOPOLOGY_GRID_METRES", 0.1))
    res_hash = noise_resolved_hash(
        src_hash, dom_hash,
        TOPOLOGY_RULES_VERSION, DISSOLVE_RULES_VERSION, ROUND_PRIORITY_VERSION,
        topology_grid_m,
    )
    _progress(progress_cb, f"resolved hash: {res_hash}")

    if not force:
        active = get_active_artifact(engine, "resolved")
        if active is not None and active.artifact_hash == res_hash:
            log.info("noise artifact already up to date (artifact_hash=%s)", res_hash)
            _progress(progress_cb, f"noise artifact up to date: {res_hash}")
            return {"status": "up_to_date", "artifact_hash": res_hash, "row_count": 0}

    domain_wgs84, domain_wkb = _load_domain(config)
    tile_size_m = float(getattr(config, "NOISE_DISSOLVE_TILE_SIZE_METRES", 10_000.0))

    result = build_noise_artifact(
        engine,
        data_dir=resolved_data_dir,
        domain_wgs84=domain_wgs84,
        domain_wkb=domain_wkb,
        source_hash=src_hash,
        domain_hash=dom_hash,
        resolved_hash=res_hash,
        tile_size_metres=tile_size_m,
        topology_grid_metres=topology_grid_m,
        force=force,
        progress_cb=progress_cb,
    )

    return {**result, "status": "built", "artifact_hash": res_hash}
