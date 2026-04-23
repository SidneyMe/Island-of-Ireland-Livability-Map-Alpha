from __future__ import annotations

from .models import FeedDataset, GtfsStopReality, StopInfo, StopServiceSummary


def _stop_info_by_key(feed_datasets: list[FeedDataset]) -> dict[tuple[str, str], StopInfo]:
    return {
        (dataset.feed_id, stop_id): stop
        for dataset in feed_datasets
        for stop_id, stop in dataset.stops.items()
    }


def _summarized_parent_keys(
    stop_summaries: list[StopServiceSummary],
    stop_info_by_key: dict[tuple[str, str], StopInfo],
) -> set[tuple[str, str]]:
    parent_keys: set[tuple[str, str]] = set()
    for summary in stop_summaries:
        stop_info = stop_info_by_key.get((summary.feed_id, summary.stop_id))
        if stop_info is None or not stop_info.parent_station:
            continue
        parent_keys.add((summary.feed_id, stop_info.parent_station))
    return parent_keys


def derive_gtfs_stop_reality(
    feed_datasets: list[FeedDataset],
    stop_summaries: list[StopServiceSummary],
    *,
    reality_fingerprint: str,
    import_fingerprint: str,
) -> list[GtfsStopReality]:
    stop_info_by_key = _stop_info_by_key(feed_datasets)
    summarized_parent_keys = _summarized_parent_keys(stop_summaries, stop_info_by_key)
    reality_rows: list[GtfsStopReality] = []

    for summary in stop_summaries:
        stop_key = (summary.feed_id, summary.stop_id)
        stop_info = stop_info_by_key.get(stop_key)
        if stop_info is None:
            continue
        if stop_info.location_type == 1 and stop_key in summarized_parent_keys:
            continue

        if summary.is_unscheduled_stop:
            reality_status = "inactive_confirmed"
            school_only_state = "no"
            primary_reason = "unscheduled_stop"
        elif summary.public_departures_30d > 0:
            reality_status = "active_confirmed"
            school_only_state = "no"
            primary_reason = "public_departures_present"
        elif summary.school_only_departures_30d > 0:
            reality_status = "school_only_confirmed"
            school_only_state = "yes"
            primary_reason = "school_only_departures_present"
        else:
            reality_status = "inactive_confirmed"
            school_only_state = "no"
            primary_reason = "zero_public_departures_window"

        reality_reason_codes = tuple(sorted({primary_reason, *summary.reason_codes}))
        reality_rows.append(
            GtfsStopReality(
                reality_fingerprint=reality_fingerprint,
                import_fingerprint=import_fingerprint,
                source_ref=f"gtfs/{summary.feed_id}/{summary.stop_id}",
                stop_name=stop_info.stop_name,
                feed_id=summary.feed_id,
                stop_id=summary.stop_id,
                source_status="gtfs_direct",
                reality_status=reality_status,
                school_only_state=school_only_state,
                public_departures_7d=summary.public_departures_7d,
                public_departures_30d=summary.public_departures_30d,
                school_only_departures_30d=summary.school_only_departures_30d,
                last_public_service_date=summary.last_public_service_date,
                last_any_service_date=summary.last_any_service_date,
                route_modes=summary.route_modes,
                source_reason_codes=("gtfs_direct_source",),
                reality_reason_codes=reality_reason_codes,
                lat=stop_info.stop_lat,
                lon=stop_info.stop_lon,
                bus_active_days_mask_7d=summary.bus_active_days_mask_7d,
                bus_service_subtier=summary.bus_service_subtier,
                is_unscheduled_stop=summary.is_unscheduled_stop,
                has_exception_only_service=summary.has_exception_only_service,
                has_any_bus_service=summary.has_any_bus_service,
                has_daily_bus_service=summary.has_daily_bus_service,
            )
        )

    reality_rows.sort(key=lambda row: (row.feed_id, row.stop_name or "", row.stop_id))
    return reality_rows
