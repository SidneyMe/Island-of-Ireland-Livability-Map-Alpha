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

# Version constants re-exported for CLI consumers; canonical home is runner.py.
from .runner import (  # noqa: E402
    PARSER_VERSION,
    SOURCE_SCHEMA_VERSION,
    TOPOLOGY_RULES_VERSION,
    DISSOLVE_RULES_VERSION,
    ROUND_PRIORITY_VERSION,
    EXTENT_VERSION,
)


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
    from noise.loader import NOISE_DATA_DIR

    from .runner import build_default_noise_artifact

    data_dir = Path(args.data_dir) if args.data_dir else NOISE_DATA_DIR
    engine = build_engine()

    result = build_default_noise_artifact(engine, force=args.force, data_dir=data_dir)

    if result["status"] == "up_to_date":
        print(f"noise artifact already up to date (artifact_hash={result['artifact_hash']})")
    else:
        print(
            f"noise artifact built: resolved_hash={result['artifact_hash']} "
            f"rows={result.get('row_count', 0)}"
        )

    if getattr(args, "bake_pmtiles", False):
        print(
            "Standalone noise artifact PMTiles bake is not yet implemented. "
            "Use `python main.py --precompute` to bake livability PMTiles from noise_polygons.",
            file=__import__("sys").stderr,
        )
        return 1

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
