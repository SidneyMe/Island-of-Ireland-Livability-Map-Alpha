from __future__ import annotations

import argparse
import sys

DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 8000
_COMMAND_SERVE = "serve"
_COMMAND_IMPORT = "import"
_COMMAND_TRANSIT = "transit"
_COMMAND_PRECOMPUTE = "precompute"
_COMMAND_GTFS = "gtfs"
_GTFS_STATUS = "status"
_GTFS_REFRESH = "refresh"
_PROFILE_CHOICES = ("full", "dev", "test")


def _add_profile_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile",
        choices=_PROFILE_CHOICES,
        default="full",
        help="Select the build profile to run.",
    )


def _add_gtfs_force_refresh_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--force-gtfs-refresh",
        action="store_true",
        help="Force GTFS ZIP re-download before reading or refreshing the cache.",
    )


def _add_precompute_noise_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--refresh-noise-artifact",
        action="store_true",
        help=(
            "Before precompute, rebuild the noise artifact if it is missing or stale "
            "(source files changed). No-op if the artifact is already up to date."
        ),
    )
    parser.add_argument(
        "--force-noise-artifact",
        action="store_true",
        help=(
            "Before precompute, force a resolved artifact rebuild even if one exists, "
            "reusing existing source rows when available (no raw source re-import)."
        ),
    )
    parser.add_argument(
        "--reimport-noise-source",
        action="store_true",
        help=(
            "Before precompute, re-import raw noise source rows into noise_normalized "
            "for the current source hash, then rebuild resolved artifact."
        ),
    )
    parser.add_argument(
        "--force-noise-all",
        action="store_true",
        help=(
            "Before precompute, force both source re-import and resolved rebuild. "
            "Equivalent to --force-noise-artifact + --reimport-noise-source."
        ),
    )
    parser.add_argument(
        "--noise-accurate",
        action="store_true",
        help=(
            "Use accurate noise processing mode for artifact build/publish. "
            "Default is dev-fast mode."
        ),
    )
    parser.add_argument(
        "--require-active-noise-artifact",
        action="store_true",
        help=(
            "Require an existing active resolved noise artifact for the selected "
            "noise mode and fail fast if none is available."
        ),
    )

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Island of Ireland livability map precompute and local web app entrypoint.",
    )
    subparsers = parser.add_subparsers(dest="command")

    import_parser = subparsers.add_parser(
        _COMMAND_IMPORT,
        help="Refresh the raw local OSM amenity import.",
    )
    import_parser.set_defaults(command=_COMMAND_IMPORT)

    gtfs_parser = subparsers.add_parser(
        _COMMAND_GTFS,
        help="Inspect or refresh cached public static GTFS feeds.",
    )
    gtfs_subparsers = gtfs_parser.add_subparsers(dest="gtfs_command", required=True)

    gtfs_status_parser = gtfs_subparsers.add_parser(
        _GTFS_STATUS,
        help="Print GTFS cache freshness diagnostics.",
    )
    _add_gtfs_force_refresh_argument(gtfs_status_parser)
    gtfs_status_parser.set_defaults(command=_COMMAND_GTFS, gtfs_command=_GTFS_STATUS)

    gtfs_refresh_parser = gtfs_subparsers.add_parser(
        _GTFS_REFRESH,
        help="Refresh cached public static GTFS ZIP feeds.",
    )
    _add_gtfs_force_refresh_argument(gtfs_refresh_parser)
    gtfs_refresh_parser.set_defaults(command=_COMMAND_GTFS, gtfs_command=_GTFS_REFRESH)

    transit_parser = subparsers.add_parser(
        _COMMAND_TRANSIT,
        help="Refresh GTFS-derived transit reality.",
    )
    transit_parser.add_argument(
        "--force-transit-refresh",
        action="store_true",
        help="Force a full GTFS transit rebuild even if the current manifest matches.",
    )
    transit_parser.add_argument(
        "--auto-refresh-gtfs",
        action="store_true",
        help="Refresh GTFS cache if feeds are stale or missing before transit processing.",
    )
    _add_gtfs_force_refresh_argument(transit_parser)
    transit_parser.set_defaults(command=_COMMAND_TRANSIT)

    precompute_parser = subparsers.add_parser(
        _COMMAND_PRECOMPUTE,
        help="Run the derived livability precompute pipeline.",
    )
    _add_profile_argument(precompute_parser)
    precompute_parser.add_argument(
        "--force-precompute",
        action="store_true",
        help="Rebuild and replace the current PostGIS build even if a complete manifest already exists.",
    )
    precompute_parser.add_argument(
        "--explain",
        action="store_true",
        help="Print the precompute planner decision without running the expensive execution phases.",
    )
    precompute_parser.add_argument(
        "--auto-refresh-import",
        action="store_true",
        help="Allow precompute to refresh raw OSM import state when it is missing instead of failing fast.",
    )
    _add_precompute_noise_arguments(precompute_parser)
    precompute_parser.set_defaults(command=_COMMAND_PRECOMPUTE)

    serve_parser = subparsers.add_parser(
        _COMMAND_SERVE,
        help="Run the local MapLibre web app.",
    )
    _add_profile_argument(serve_parser)
    serve_parser.add_argument(
        "--host",
        default=DEFAULT_SERVER_HOST,
        help=f"Bind host for the local web app (default: {DEFAULT_SERVER_HOST}).",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVER_PORT,
        help=f"Bind port for the local web app (default: {DEFAULT_SERVER_PORT}).",
    )
    serve_parser.set_defaults(command=_COMMAND_SERVE)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    normalized_argv = list(sys.argv[1:] if argv is None else argv)
    if not normalized_argv:
        normalized_argv = [_COMMAND_SERVE]
    return parser.parse_args(normalized_argv)


def _handle_gtfs_status(force_gtfs_refresh: bool) -> None:
    print("GTFS static feed status", flush=True)
    from transit_refresh_runner import gtfs_status as _gtfs_status

    statuses = _gtfs_status(force_refresh=force_gtfs_refresh)
    for status in statuses:
        print(
            "GTFS feed "
            f"{status.feed_id}: "
            f"path={status.path} "
            f"sha256={status.sha256 or 'unknown'} "
            f"downloaded_at_utc={status.downloaded_at_utc or 'unknown'} "
            f"checked_at_utc={status.checked_at_utc or 'unknown'} "
            f"etag={status.etag or 'none'} "
            f"last_modified={status.last_modified or 'none'} "
            f"calendar_min_date={status.calendar_min_date or 'unknown'} "
            f"calendar_max_date={status.calendar_max_date or 'unknown'} "
            f"days_until_calendar_end={status.days_until_calendar_end if status.days_until_calendar_end is not None else 'unknown'} "
            f"cache_age_hours={f'{status.cache_age_hours:.2f}' if status.cache_age_hours is not None else 'unknown'} "
            f"freshness_decision={status.freshness_decision} "
            f"network_request={status.network_request}",
            flush=True,
        )


def _handle_gtfs_refresh(force_gtfs_refresh: bool) -> None:
    print("Starting GTFS static feed refresh...", flush=True)
    from transit_refresh_runner import refresh_gtfs as _refresh_gtfs

    results = _refresh_gtfs(force_refresh=force_gtfs_refresh)
    for result in results:
        print(
            "GTFS feed "
            f"{result.feed_id}: changed={result.changed} "
            f"sha256={result.sha256 or 'unknown'} "
            f"size={result.content_length or 0} "
            f"valid={result.zip_valid} "
            f"calendar={result.calendar_min_date or 'unknown'}..{result.calendar_max_date or 'unknown'} "
            f"path={result.path}",
            flush=True,
        )
    print("GTFS static feed refresh complete", flush=True)


def _handle_transit_refresh(
    force_transit_refresh: bool,
    auto_refresh_gtfs: bool,
    force_gtfs_refresh: bool,
) -> None:
    print("Starting GTFS transit refresh...", flush=True)
    from transit_refresh_runner import refresh_transit as _refresh_transit

    reality_fingerprint = _refresh_transit(
        force_refresh=force_transit_refresh,
        auto_refresh_gtfs=auto_refresh_gtfs or force_gtfs_refresh,
        force_gtfs_refresh=force_gtfs_refresh,
    )
    print(
        f"GTFS transit refresh complete -> {reality_fingerprint}",
        flush=True,
    )


def _handle_precompute(args: argparse.Namespace) -> None:
    print(f"Preparing livability precompute ({args.profile})...", flush=True)
    from precompute import run_precompute as _run_precompute

    precompute_kwargs = dict(
        profile=args.profile,
        force_precompute=args.force_precompute,
        auto_refresh_import=args.auto_refresh_import,
        force_noise_artifact=args.force_noise_artifact,
        reimport_noise_source=args.reimport_noise_source,
        force_noise_all=args.force_noise_all,
        noise_accurate=args.noise_accurate,
        require_active_noise_artifact=args.require_active_noise_artifact,
        refresh_noise_artifact=(
            args.refresh_noise_artifact
            or args.force_noise_artifact
            or args.reimport_noise_source
            or args.force_noise_all
        ),
    )
    if args.explain:
        precompute_kwargs["explain"] = True
    _run_precompute(**precompute_kwargs)


def _handle_serve(args: argparse.Namespace) -> None:
    from render_from_db import run_render_from_db as _run_render_from_db

    _run_render_from_db(profile=args.profile, host=args.host, port=args.port)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parse_args(argv)

    try:
        if args.command == _COMMAND_IMPORT:
            from precompute import refresh_local_import as _refresh_local_import

            _refresh_local_import()
        elif args.command == _COMMAND_GTFS:
            if args.gtfs_command == _GTFS_STATUS:
                _handle_gtfs_status(force_gtfs_refresh=args.force_gtfs_refresh)
            elif args.gtfs_command == _GTFS_REFRESH:
                _handle_gtfs_refresh(force_gtfs_refresh=args.force_gtfs_refresh)
            else:
                parser.error("gtfs subcommand must be status or refresh")
        elif args.command == _COMMAND_TRANSIT:
            _handle_transit_refresh(
                force_transit_refresh=args.force_transit_refresh,
                auto_refresh_gtfs=args.auto_refresh_gtfs,
                force_gtfs_refresh=args.force_gtfs_refresh,
            )
        elif args.command == _COMMAND_PRECOMPUTE:
            if args.require_active_noise_artifact and (
                args.refresh_noise_artifact
                or args.force_noise_artifact
                or args.reimport_noise_source
                or args.force_noise_all
            ):
                parser.error(
                    "--require-active-noise-artifact cannot be combined with --refresh-noise-artifact, "
                    "--reimport-noise-source, --force-noise-artifact, or --force-noise-all"
                )
            _handle_precompute(args)
        elif args.command == _COMMAND_SERVE:
            _handle_serve(args)
        else:
            parser.error("a subcommand is required")
    except (RuntimeError, ModuleNotFoundError) as exc:
        print(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
