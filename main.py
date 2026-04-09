from __future__ import annotations

import argparse

DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 8000


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Island of Ireland livability map precompute and local web app entrypoint.",
    )
    parser.add_argument(
        "--refresh-import",
        action="store_true",
        help="Refresh the raw local OSM amenity import without running full scoring.",
    )
    parser.add_argument(
        "--precompute",
        action="store_true",
        help="Run derived livability precompute using the existing raw OSM import state.",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="Start the local MapLibre web app (legacy alias for --serve).",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start the local MapLibre web app from static assets and PostGIS runtime data.",
    )
    parser.add_argument(
        "--force-precompute",
        action="store_true",
        help="Rebuild and replace the current PostGIS build even if a complete manifest already exists.",
    )
    parser.add_argument(
        "--auto-refresh-import",
        action="store_true",
        help="Allow --precompute to refresh raw OSM import state when it is missing instead of failing fast.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_SERVER_HOST,
        help=f"Bind host for the local web app (default: {DEFAULT_SERVER_HOST}).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVER_PORT,
        help=f"Bind port for the local web app (default: {DEFAULT_SERVER_PORT}).",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.force_precompute and not args.precompute:
        parser.error("--force-precompute requires --precompute")
    if args.auto_refresh_import and not args.precompute:
        parser.error("--auto-refresh-import requires --precompute")

    run_render = args.render or args.serve or (not args.precompute and not args.refresh_import)

    try:
        if args.refresh_import:
            from precompute import refresh_local_import as _refresh_local_import

            _refresh_local_import()
        if args.precompute:
            from precompute import run_precompute as _run_precompute

            _run_precompute(
                force_precompute=args.force_precompute,
                auto_refresh_import=args.auto_refresh_import,
            )
        if run_render:
            from render_from_db import run_render_from_db as _run_render_from_db

            _run_render_from_db(host=args.host, port=args.port)
    except (RuntimeError, ModuleNotFoundError) as exc:
        print(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
