"""
PMTiles bake stub for noise artifacts.

Phase 8A: establishes the atomic temp-to-final file write pattern.
Phase 8B (Milestone B): will implement the real tippecanoe pipeline
reading from noise_resolved_display with ST_Transform to EPSG:4326.

The livability build continues to bake noise from noise_polygons
(populated by the Phase 9A direct copy) until Phase 8B + 9B are complete.
"""
from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy.engine import Engine


def bake_noise_artifact_pmtiles(
    engine: Engine,
    noise_tile_hash: str,
    *,
    output_dir: Path,
    noise_max_zoom: int = 13,
) -> Path:
    """
    Bake a noise-only PMTiles file from noise_resolved_display.

    Phase 8A stub: raises NotImplementedError.
    The atomic temp→final rename pattern is already in place for Phase 8B.

    Tile properties (once implemented):
      metric, source_type, db_value, db_low, db_high,
      jurisdiction, round_number, report_period
    """
    output_path = output_dir / f"noise_{noise_tile_hash}.pmtiles"
    tmp_path = output_dir / f".tmp_noise_{noise_tile_hash}.pmtiles"
    try:
        raise NotImplementedError(
            "Phase 8B PMTiles bake not yet implemented. "
            "Livability build continues to bake noise from noise_polygons. "
            "Implement: query noise_resolved_display → ST_Transform(4326) → "
            "ogr2ogr FlatGeobuf → tippecanoe → PMTiles."
        )
    except NotImplementedError:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    # Phase 8B will reach here:
    os.replace(tmp_path, output_path)  # atomic on both POSIX and Windows
    return output_path
