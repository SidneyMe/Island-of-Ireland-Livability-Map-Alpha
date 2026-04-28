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
        "--refresh-transit",
        action="store_true",
        help="Refresh GTFS-derived transit reality, reusing unchanged manifests when possible.",
    )
    parser.add_argument(
        "--force-transit-refresh",
        action="store_true",
        help="Force a full GTFS transit rebuild even if the current transit reality manifest matches.",
    )
    parser.add_argument(
        "--precompute",
        action="store_true",
        help="Run derived livability precompute using the existing raw OSM import state.",
    )
    parser.add_argument(
        "--precompute-dev",
        action="store_true",
        help="Run the coarse-only dev precompute profile using the existing raw OSM import state.",
    )
    parser.add_argument(
        "--precompute-test",
        action="store_true",
        help="Run the Cork-only test precompute profile with the full 20km to 50m resolution ladder.",
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
        "--render-dev",
        action="store_true",
        help="Start the coarse-only dev MapLibre web app (legacy alias for --serve-dev).",
    )
    parser.add_argument(
        "--serve-dev",
        action="store_true",
        help="Start the coarse-only dev MapLibre web app from static assets and PostGIS runtime data.",
    )
    parser.add_argument(
        "--render-test",
        action="store_true",
        help="Start the Cork-only test MapLibre web app (legacy alias for --serve-test).",
    )
    parser.add_argument(
        "--serve-test",
        action="store_true",
        help="Start the Cork-only test MapLibre web app from static assets and PostGIS runtime data.",
    )
    parser.add_argument(
        "--force-precompute",
        action="store_true",
        help="Rebuild and replace the current PostGIS build even if a complete manifest already exists.",
    )
    parser.add_argument(
        "--refresh-noise-artifact",
        action="store_true",
        help=(
            "Before precompute, rebuild the noise artifact if it is missing or stale "
            "(source files changed). No-op if the artifact is already up to date. "
            "Requires --precompute / --precompute-dev / --precompute-test."
        ),
    )
    parser.add_argument(
        "--force-noise-artifact",
        action="store_true",
        help=(
            "Before precompute, force a full rebuild of the noise artifact even if one exists. "
            "Implies --refresh-noise-artifact. "
            "Requires --precompute / --precompute-dev / --precompute-test."
        ),
    )
    parser.add_argument(
        "--auto-refresh-import",
        action="store_true",
        help="Allow --precompute/--precompute-dev/--precompute-test to refresh raw OSM import state when it is missing instead of failing fast.",
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

    precompute_requested = args.precompute or args.precompute_dev or args.precompute_test
    serve_full_requested = args.render or args.serve
    serve_dev_requested = args.render_dev or args.serve_dev
    serve_test_requested = args.render_test or args.serve_test

    if sum(
        bool(value)
        for value in (args.precompute, args.precompute_dev, args.precompute_test)
    ) > 1:
        parser.error("--precompute, --precompute-dev, and --precompute-test are mutually exclusive")
    if sum(
        bool(value)
        for value in (serve_full_requested, serve_dev_requested, serve_test_requested)
    ) > 1:
        parser.error(
            "--serve/--render, --serve-dev/--render-dev, and --serve-test/--render-test are mutually exclusive"
        )
    if args.force_precompute and not precompute_requested:
        parser.error("--force-precompute requires --precompute, --precompute-dev, or --precompute-test")
    if args.refresh_noise_artifact and not precompute_requested:
        parser.error("--refresh-noise-artifact requires --precompute, --precompute-dev, or --precompute-test")
    if args.force_noise_artifact and not precompute_requested:
        parser.error("--force-noise-artifact requires --precompute, --precompute-dev, or --precompute-test")
    if args.force_transit_refresh and not args.refresh_transit:
        parser.error("--force-transit-refresh requires --refresh-transit")
    if args.auto_refresh_import and not precompute_requested:
        parser.error("--auto-refresh-import requires --precompute, --precompute-dev, or --precompute-test")

    run_render = serve_full_requested or serve_dev_requested or serve_test_requested or (
        not precompute_requested and not args.refresh_import and not args.refresh_transit
    )
    serve_profile = "dev" if serve_dev_requested else "test" if serve_test_requested else "full"

    try:
        if args.refresh_import:
            from precompute import refresh_local_import as _refresh_local_import

            _refresh_local_import()
        if args.refresh_transit:
            print("Starting GTFS transit refresh...", flush=True)
            from transit_refresh_runner import refresh_transit as _refresh_transit

            reality_fingerprint = _refresh_transit(
                force_refresh=args.force_transit_refresh,
                refresh_download=True,
            )
            print(
                f"GTFS transit refresh complete -> {reality_fingerprint}",
                flush=True,
            )
        if precompute_requested:
            from precompute import run_precompute as _run_precompute

            _run_precompute(
                profile="dev" if args.precompute_dev else "test" if args.precompute_test else "full",
                force_precompute=args.force_precompute,
                auto_refresh_import=args.auto_refresh_import,
                force_noise_artifact=args.force_noise_artifact,
                refresh_noise_artifact=args.refresh_noise_artifact or args.force_noise_artifact,
            )
        if run_render:
            from render_from_db import run_render_from_db as _run_render_from_db

            _run_render_from_db(profile=serve_profile, host=args.host, port=args.port)
    except (RuntimeError, ModuleNotFoundError) as exc:
        print(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
