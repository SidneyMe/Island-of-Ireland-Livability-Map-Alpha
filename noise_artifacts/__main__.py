"""
Standalone noise artifact CLI.

Usage:
    python -m noise_artifacts [OPTIONS]
    python -m noise_artifacts compare --artifact-hash HASH --build-key KEY
    python -m noise_artifacts --bake-pmtiles --output-dir DIR  (Phase 8B stub)

The normal livability build must NOT call this module.
This is the only entry point that computes source hashes and calls noise.loader.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)

# Version constants — bump these to force re-ingest/re-resolve
PARSER_VERSION = 1
SOURCE_SCHEMA_VERSION = 1
TOPOLOGY_RULES_VERSION = 1
DISSOLVE_RULES_VERSION = 1
ROUND_PRIORITY_VERSION = 1
EXTENT_VERSION = 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m noise_artifacts",
        description="Build or query noise artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # Default command: build
    build_p = argparse.ArgumentParser(add_help=False)
    build_p.add_argument("--force", action="store_true",
                         help="Re-run even if a current artifact exists")
    build_p.add_argument("--profile", default=None,
                         help="Build profile (full|dev|test)")
    build_p.add_argument("--data-dir", default=None,
                         help="Path to noise_datasets/ directory")
    build_p.add_argument("--bake-pmtiles", action="store_true",
                         help="Also bake standalone PMTiles (Phase 8B stub)")
    build_p.add_argument("--output-dir", default=None,
                         help="Output directory for PMTiles (required with --bake-pmtiles)")

    # compare subcommand
    compare_p = subparsers.add_parser("compare", help="Compare artifact to legacy build")
    compare_p.add_argument("--artifact-hash", required=True)
    compare_p.add_argument("--build-key", required=True)

    # Parse: if no subcommand, treat all args as build args
    args, remaining = parser.parse_known_args(argv)

    if args.command == "compare":
        return _run_compare(args)

    # Build command
    build_args = build_p.parse_args(remaining)
    return _run_build(build_args)


def _run_build(args) -> int:
    import config
    from db_postgis.engine import build_engine
    from noise.loader import NOISE_DATA_DIR, dataset_signature

    from .builder import build_noise_artifact
    from .manifest import (
        get_active_artifact,
        noise_domain_hash,
        noise_resolved_hash,
        noise_source_hash,
    )

    data_dir = Path(args.data_dir) if args.data_dir else NOISE_DATA_DIR
    engine = build_engine()

    # Hash computation (only this module touches raw files / data_dir)
    log.info("computing source signature from %s", data_dir)
    source_sig = dataset_signature(data_dir)
    src_hash = noise_source_hash(source_sig, PARSER_VERSION, SOURCE_SCHEMA_VERSION)

    domain_boundary_bytes = _load_domain_boundary_bytes(config)
    dom_hash = noise_domain_hash(domain_boundary_bytes, EXTENT_VERSION)

    topology_grid_m = getattr(config, "NOISE_TOPOLOGY_GRID_METRES", 0.1)
    res_hash = noise_resolved_hash(
        src_hash, dom_hash,
        TOPOLOGY_RULES_VERSION, DISSOLVE_RULES_VERSION, ROUND_PRIORITY_VERSION,
        topology_grid_m,
    )

    # Check if already current
    if not args.force:
        active = get_active_artifact(engine, "resolved")
        if active is not None and active.artifact_hash == res_hash:
            print(f"noise artifact already up to date (artifact_hash={res_hash})")
            return 0

    # Load domain geometry
    domain_wgs84, domain_wkb = _load_domain(config)

    tile_size_m = getattr(config, "NOISE_DISSOLVE_TILE_SIZE_METRES", 10_000.0)

    result = build_noise_artifact(
        engine,
        data_dir=data_dir,
        domain_wgs84=domain_wgs84,
        domain_wkb=domain_wkb,
        source_hash=src_hash,
        domain_hash=dom_hash,
        resolved_hash=res_hash,
        tile_size_metres=tile_size_m,
        topology_grid_metres=topology_grid_m,
        force=args.force,
    )
    print(f"noise artifact built: resolved_hash={res_hash} rows={result.get('row_count', 0)}")

    if getattr(args, "bake_pmtiles", False):
        from .bake import bake_noise_artifact_pmtiles
        output_dir = Path(args.output_dir) if args.output_dir else Path(".")
        from .manifest import noise_tile_hash
        tile_hash = noise_tile_hash(
            res_hash, 1, 8, getattr(config, "NOISE_MAX_ZOOM", 13),
            ("metric", "source_type", "db_value", "db_low", "db_high",
             "jurisdiction", "round_number", "report_period"),
            (getattr(config, "NOISE_TILE_SIMPLIFY_METRES_LZ", 10.0),
             getattr(config, "NOISE_TILE_SIMPLIFY_METRES_HZ", 5.0)),
        )
        bake_noise_artifact_pmtiles(engine, tile_hash, output_dir=output_dir)

    return 0


def _run_compare(args) -> int:
    from db_postgis.engine import build_engine
    from .compare import compare_artifact_to_legacy

    engine = build_engine()
    result = compare_artifact_to_legacy(
        engine,
        noise_resolved_hash=args.artifact_hash,
        legacy_build_key=args.build_key,
    )
    print(f"groups_matching={result['groups_matching']}")
    print(f"groups_diverging={result['groups_diverging']}")
    if result["groups_diverging"]:
        for group, ratio in result["area_ratio_by_group"].items():
            if ratio is None:
                print(f"  MISSING {group}: no comparable legacy area")
            elif abs(ratio - 1.0) > 0.01:
                print(f"  DIVERGING {group}: ratio={ratio:.4f}")
        return 1
    return 0


def _load_domain_boundary_bytes(config) -> bytes:
    """Hash content of both ROI and NI boundary files for deterministic domain hashing."""
    import hashlib

    h = hashlib.sha256()
    for path in (config.ROI_BOUNDARY_PATH, config.NI_BOUNDARY_PATH):
        h.update(str(path).encode("utf-8"))
        try:
            h.update(path.read_bytes())
        except OSError:
            pass
    return h.digest()


def _load_domain(config):
    """Return (domain_wgs84_shapely, domain_wkb_bytes) for the full Island of Ireland."""
    from shapely.ops import transform
    from study_area import load_island_geometry_metric

    domain_2157 = load_island_geometry_metric()
    domain_wgs84 = transform(config.TO_WGS84, domain_2157)
    return domain_wgs84, domain_wgs84.wkb


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
