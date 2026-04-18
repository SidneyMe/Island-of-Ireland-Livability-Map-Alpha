from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

from config import (
    PROJECT_TEMP_DIR,
    TransitRealityState,
    build_transit_reality_state,
    transit_feed_configs,
)
from db_postgis.manifests import (
    has_complete_transit_feed_manifest,
    has_complete_transit_reality_manifest,
)
from db_postgis.reads import load_transport_reality_points
from db_postgis.writes import (
    replace_gtfs_feed_rows_from_artifacts,
    replace_transit_reality_rows_from_artifacts,
)

from .export import EXPORTS_DIR, ZIP_FILENAME, export_transport_reality_bundle
from .models import GtfsStopReality
from .rust_gtfs import load_gtfs_stop_reality_models, run_walkgraph_gtfs_refresh
from .sources import ensure_feed_zip


ProgressFn = Callable[..., None] | None


def _emit(progress_cb: ProgressFn, detail: str) -> None:
    if progress_cb is None:
        print(detail, flush=True)
        return
    progress_cb("detail", detail=detail, force_log=True)


def prepare_transit_reality_state(*, refresh_download: bool = False) -> TransitRealityState:
    feed_configs = transit_feed_configs()
    for feed_config in feed_configs:
        ensure_feed_zip(feed_config, refresh_download=refresh_download)
    return build_transit_reality_state(feed_configs=feed_configs)


def transit_reality_refresh_required(
    engine,
    *,
    import_fingerprint: str | None = None,
    refresh_download: bool = False,
    force_refresh: bool = False,
    reality_state: TransitRealityState | None = None,
) -> tuple[TransitRealityState, bool]:
    prepared_state = (
        prepare_transit_reality_state(refresh_download=refresh_download)
        if reality_state is None
        else reality_state
    )
    refresh_required = force_refresh or not has_complete_transit_reality_manifest(
        engine,
        prepared_state.reality_fingerprint,
    )
    return prepared_state, refresh_required


def ensure_transit_reality(
    engine,
    *,
    import_fingerprint: str,
    refresh_download: bool = False,
    force_refresh: bool = False,
    progress_cb: ProgressFn = None,
    reality_state: TransitRealityState | None = None,
) -> TransitRealityState:
    reality_state = (
        prepare_transit_reality_state(refresh_download=refresh_download)
        if reality_state is None
        else reality_state
    )
    export_zip_path = EXPORTS_DIR / ZIP_FILENAME
    if not force_refresh and has_complete_transit_reality_manifest(
        engine,
        reality_state.reality_fingerprint,
    ):
        _emit(progress_cb, f"reusing transit reality {reality_state.reality_fingerprint}")
        if not export_zip_path.exists():
            existing_rows = [
                GtfsStopReality(
                    reality_fingerprint=reality_state.reality_fingerprint,
                    import_fingerprint=import_fingerprint,
                    source_ref=row["source_ref"],
                    stop_name=row["stop_name"],
                    feed_id=row["feed_id"],
                    stop_id=row["stop_id"],
                    source_status=row["source_status"],
                    reality_status=row["reality_status"],
                    school_only_state=row["school_only_state"],
                    public_departures_7d=row["public_departures_7d"],
                    public_departures_30d=row["public_departures_30d"],
                    school_only_departures_30d=row["school_only_departures_30d"],
                    last_public_service_date=row["last_public_service_date"],
                    last_any_service_date=row["last_any_service_date"],
                    route_modes=tuple(row["route_modes_json"]),
                    source_reason_codes=tuple(row["source_reason_codes_json"]),
                    reality_reason_codes=tuple(row["reality_reason_codes_json"]),
                    lat=row["geom"].y,
                    lon=row["geom"].x,
                )
                for row in load_transport_reality_points(engine, reality_state.reality_fingerprint)
            ]
            export_transport_reality_bundle(existing_rows, analysis_date=reality_state.analysis_date)
        return reality_state

    PROJECT_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=PROJECT_TEMP_DIR) as tmp_name:
        artifacts_dir = run_walkgraph_gtfs_refresh(
            feed_states=reality_state.feed_states,
            import_fingerprint=import_fingerprint,
            reality_fingerprint=reality_state.reality_fingerprint,
            output_dir=Path(tmp_name) / "gtfs_refresh",
            progress_cb=progress_cb,
        )
        for feed_state in reality_state.feed_states:
            if not force_refresh and has_complete_transit_feed_manifest(engine, feed_state.feed_fingerprint):
                _emit(
                    progress_cb,
                    f"reusing GTFS raw feed {feed_state.feed_id} ({feed_state.feed_fingerprint})",
                )
                continue
            _emit(
                progress_cb,
                f"loading GTFS raw feed {feed_state.feed_id} ({feed_state.feed_fingerprint})",
            )
            replace_gtfs_feed_rows_from_artifacts(
                engine,
                feed_fingerprint=feed_state.feed_fingerprint,
                feed_id=feed_state.feed_id,
                analysis_date=feed_state.analysis_date,
                source_path=str(feed_state.zip_path),
                source_url=feed_state.source_url,
                artifacts_dir=artifacts_dir / "raw" / feed_state.feed_id,
                progress_cb=progress_cb,
            )

        replace_transit_reality_rows_from_artifacts(
            engine,
            reality_fingerprint=reality_state.reality_fingerprint,
            import_fingerprint=import_fingerprint,
            analysis_date=reality_state.analysis_date,
            transit_config_hash=reality_state.transit_config_hash,
            feed_fingerprints_json=reality_state.feed_fingerprints,
            artifacts_dir=artifacts_dir / "derived",
            progress_cb=progress_cb,
        )
        export_transport_reality_bundle(
            load_gtfs_stop_reality_models(artifacts_dir / "derived" / "gtfs_stop_reality.csv"),
            analysis_date=reality_state.analysis_date,
        )
    return reality_state
