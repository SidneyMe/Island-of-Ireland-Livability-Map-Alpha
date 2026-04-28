from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from ._dependencies import Engine, delete, insert, update
from .common import ProgressCallback, root_module
from .tables import (
    transit_calendar_dates,
    transit_calendar_services,
    transit_feed_manifest,
    transit_gtfs_stop_reality,
    transit_gtfs_stop_service_summary,
    transit_reality_manifest,
    transit_routes,
    transit_service_classification,
    transit_service_desert_cells,
    transit_stop_times,
    transit_stops,
    transit_trips,
)


def replace_gtfs_feed_rows(
    engine: Engine,
    *,
    feed_fingerprint: str,
    feed_id: str,
    analysis_date,
    source_path: str,
    source_url: str | None,
    stops_rows: Iterable[dict[str, Any]],
    route_rows: Iterable[dict[str, Any]],
    trip_rows: Iterable[dict[str, Any]],
    stop_time_rows: Iterable[dict[str, Any]],
    calendar_rows: Iterable[dict[str, Any]],
    calendar_date_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()
    with engine.begin() as connection:
        for table in (
            transit_stops,
            transit_routes,
            transit_trips,
            transit_stop_times,
            transit_calendar_services,
            transit_calendar_dates,
            transit_feed_manifest,
        ):
            connection.execute(delete(table).where(table.c.feed_id == feed_id))

        connection.execute(
            insert(transit_feed_manifest),
            [
                {
                    "feed_fingerprint": feed_fingerprint,
                    "feed_id": feed_id,
                    "analysis_date": analysis_date,
                    "source_path": source_path,
                    "source_url": source_url,
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                }
            ],
        )
        root._bulk_insert(connection, transit_stops, stops_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_routes, route_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_trips, trip_rows, progress_cb=progress_cb)
        root._bulk_copy(connection, transit_stop_times, stop_time_rows, progress_cb=progress_cb)
        root._bulk_copy(
            connection,
            transit_calendar_services,
            calendar_rows,
            progress_cb=progress_cb,
        )
        root._bulk_copy(
            connection,
            transit_calendar_dates,
            calendar_date_rows,
            progress_cb=progress_cb,
        )
        connection.execute(
            update(transit_feed_manifest)
            .where(transit_feed_manifest.c.feed_fingerprint == feed_fingerprint)
            .values(status="complete", completed_at=datetime.now(timezone.utc))
        )


def replace_transit_reality_rows(
    engine: Engine,
    *,
    reality_fingerprint: str,
    import_fingerprint: str,
    analysis_date,
    transit_config_hash: str,
    feed_fingerprints_json: dict[str, str],
    service_classification_rows: Iterable[dict[str, Any]],
    stop_summary_rows: Iterable[dict[str, Any]],
    gtfs_stop_reality_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    created_at = datetime.now(timezone.utc)
    root = root_module()
    with engine.begin() as connection:
        for table in (
            transit_service_classification,
            transit_gtfs_stop_service_summary,
            transit_gtfs_stop_reality,
            transit_reality_manifest,
        ):
            connection.execute(delete(table).where(table.c.reality_fingerprint == reality_fingerprint))
        connection.execute(
            insert(transit_reality_manifest),
            [
                {
                    "reality_fingerprint": reality_fingerprint,
                    "import_fingerprint": import_fingerprint,
                    "analysis_date": analysis_date,
                    "transit_config_hash": transit_config_hash,
                    "feed_fingerprints_json": feed_fingerprints_json,
                    "status": "building",
                    "created_at": created_at,
                    "completed_at": None,
                }
            ],
        )
        root._bulk_insert(
            connection,
            transit_service_classification,
            service_classification_rows,
            progress_cb=progress_cb,
        )
        root._bulk_insert(
            connection,
            transit_gtfs_stop_service_summary,
            stop_summary_rows,
            progress_cb=progress_cb,
        )
        root._bulk_insert(
            connection,
            transit_gtfs_stop_reality,
            gtfs_stop_reality_rows,
            progress_cb=progress_cb,
        )
        connection.execute(
            update(transit_reality_manifest)
            .where(transit_reality_manifest.c.reality_fingerprint == reality_fingerprint)
            .values(status="complete", completed_at=datetime.now(timezone.utc))
        )


def replace_service_desert_rows(
    engine: Engine,
    *,
    build_key: str,
    desert_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> None:
    root = root_module()
    with engine.begin() as connection:
        connection.execute(
            delete(transit_service_desert_cells).where(transit_service_desert_cells.c.build_key == build_key)
        )
        root._bulk_insert(
            connection,
            transit_service_desert_cells,
            desert_rows,
            progress_cb=progress_cb,
        )
