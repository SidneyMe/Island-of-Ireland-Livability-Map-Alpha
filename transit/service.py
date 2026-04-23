from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from .models import CalendarService, FeedDataset, ServiceClassification, StopInfo, StopServiceSummary


_BUS_SUBTIER_BY_MASK = {
    "1111111": "mon_sun",
    "1111110": "mon_sat",
    "0111111": "tue_sun",
    "1111100": "weekdays_only",
    "0000011": "weekends_only",
}


def _exception_dates_by_service(dataset: FeedDataset) -> dict[str, dict[date, int]]:
    exceptions: dict[str, dict[date, int]] = defaultdict(dict)
    for row in dataset.calendar_dates:
        exceptions[row.service_id][row.service_date] = row.exception_type
    return exceptions


def _weekday_mask_for_weekdays(weekdays: set[int]) -> str:
    return "".join("1" if weekday in weekdays else "0" for weekday in range(7))


def _calendar_weekday_indexes(calendar: CalendarService) -> set[int]:
    weekday_flags = (
        calendar.monday,
        calendar.tuesday,
        calendar.wednesday,
        calendar.thursday,
        calendar.friday,
        calendar.saturday,
        calendar.sunday,
    )
    return {weekday for weekday, flag in enumerate(weekday_flags) if flag}


def _exception_only_service_ids(dataset: FeedDataset) -> set[str]:
    calendar_service_ids = set(dataset.calendar_services)
    return {
        row.service_id
        for row in dataset.calendar_dates
        if row.service_id not in calendar_service_ids
    }


def _is_boarding_stop(stop_info: StopInfo) -> bool:
    return stop_info.location_type in {None, 0}


def _bus_subtier_for_mask(mask: str | None) -> str | None:
    if not mask or mask == "0000000":
        return None
    direct = _BUS_SUBTIER_BY_MASK.get(mask)
    if direct is not None:
        return direct
    if mask.count("1") == 1:
        return "single_day_only"
    return "partial_week"


def expand_service_windows(
    dataset: FeedDataset,
    *,
    analysis_date: date,
    window_days: int,
    desert_window_days: int,
    lookahead_days: int = 0,
) -> dict[str, dict[str, object]]:
    exception_map = _exception_dates_by_service(dataset)
    analysis_start = analysis_date - timedelta(days=window_days - 1)
    analysis_end = analysis_date + timedelta(days=lookahead_days)
    desert_start = analysis_date - timedelta(days=desert_window_days - 1)
    service_windows: dict[str, dict[str, object]] = {}

    service_ids = set(dataset.calendar_services).union(exception_map)
    for service_id in sorted(service_ids):
        calendar = dataset.calendar_services.get(service_id)
        active_dates_30d: set[date] = set()
        active_dates_7d: set[date] = set()
        weekday_dates = 0
        weekend_dates = 0

        if calendar is not None:
            current_date = max(calendar.start_date, analysis_start)
            end_date = min(calendar.end_date, analysis_end)
            weekday_flags = (
                calendar.monday,
                calendar.tuesday,
                calendar.wednesday,
                calendar.thursday,
                calendar.friday,
                calendar.saturday,
                calendar.sunday,
            )
            while current_date <= end_date:
                if weekday_flags[current_date.weekday()]:
                    active_dates_30d.add(current_date)
                    if current_date >= desert_start:
                        active_dates_7d.add(current_date)
                current_date += timedelta(days=1)

        for exception_date, exception_type in exception_map.get(service_id, {}).items():
            if exception_date < analysis_start or exception_date > analysis_end:
                continue
            if exception_type == 1:
                active_dates_30d.add(exception_date)
                if exception_date >= desert_start:
                    active_dates_7d.add(exception_date)
            elif exception_type == 2:
                active_dates_30d.discard(exception_date)
                active_dates_7d.discard(exception_date)

        for active_date in active_dates_30d:
            if active_date.weekday() >= 5:
                weekend_dates += 1
            else:
                weekday_dates += 1

        service_windows[service_id] = {
            "dates_30d": tuple(sorted(active_dates_30d)),
            "dates_7d": tuple(sorted(active_dates_7d)),
            "weekday_dates": weekday_dates,
            "weekend_dates": weekend_dates,
        }

    return service_windows


def summarize_gtfs_stops(
    dataset: FeedDataset,
    *,
    reality_fingerprint: str,
    service_windows: dict[str, dict[str, object]],
    service_classifications: dict[str, ServiceClassification],
) -> list[StopServiceSummary]:
    per_stop: dict[str, dict[str, object]] = {}
    exception_only_service_ids = _exception_only_service_ids(dataset)
    stop_ids_with_stop_times = {
        stop_id for stop_id, _, _, _ in dataset.stop_service_occurrences
    }

    for (stop_id, service_id, route_id, mode), occurrences in dataset.stop_service_occurrences.items():
        window = service_windows.get(service_id) or {}
        dates_30d = tuple(window.get("dates_30d", ()))
        dates_7d = tuple(window.get("dates_7d", ()))
        classification = service_classifications.get(service_id)
        stop_payload = per_stop.setdefault(
            stop_id,
            {
                "public_departures_7d": 0,
                "public_departures_30d": 0,
                "school_only_departures_30d": 0,
                "last_public_service_date": None,
                "last_any_service_date": None,
                "route_modes": set(),
                "route_ids": set(),
                "reason_codes": set(),
                "is_unscheduled_stop": False,
                "has_exception_only_service": False,
                "has_any_bus_service": False,
                "has_daily_bus_service": False,
                "base_weekly_bus_weekdays": set(),
            },
        )
        stop_payload["route_modes"].add(mode)
        stop_payload["route_ids"].add(route_id)
        if mode == "bus":
            stop_payload["has_any_bus_service"] = True
            calendar = dataset.calendar_services.get(service_id)
            if calendar is not None:
                stop_payload["base_weekly_bus_weekdays"].update(
                    _calendar_weekday_indexes(calendar)
                )
            if service_id in exception_only_service_ids:
                stop_payload["has_exception_only_service"] = True
        if dates_30d:
            stop_payload["last_any_service_date"] = max(
                filter(
                    None,
                    (
                        stop_payload["last_any_service_date"],
                        dates_30d[-1],
                    ),
                ),
                default=None,
            )

        if classification is not None and classification.school_only_state == "yes":
            stop_payload["school_only_departures_30d"] += occurrences * len(dates_30d)
            stop_payload["reason_codes"].add("school_only_service_present")
            continue

        stop_payload["public_departures_7d"] += occurrences * len(dates_7d)
        stop_payload["public_departures_30d"] += occurrences * len(dates_30d)
        if dates_30d:
            stop_payload["last_public_service_date"] = max(
                filter(
                    None,
                    (
                        stop_payload["last_public_service_date"],
                        dates_30d[-1],
                    ),
                ),
                default=None,
            )

    unscheduled_stop_ids = {
        stop_id
        for stop_id, stop_info in dataset.stops.items()
        if _is_boarding_stop(stop_info) and stop_id not in stop_ids_with_stop_times
    }
    for stop_id in sorted(unscheduled_stop_ids):
        per_stop[stop_id] = {
            "public_departures_7d": 0,
            "public_departures_30d": 0,
            "school_only_departures_30d": 0,
            "last_public_service_date": None,
            "last_any_service_date": None,
            "route_modes": set(),
            "route_ids": set(),
            "reason_codes": {"unscheduled_stop"},
            "is_unscheduled_stop": True,
            "has_exception_only_service": False,
            "has_any_bus_service": False,
            "has_daily_bus_service": False,
            "base_weekly_bus_weekdays": set(),
        }

    summaries: list[StopServiceSummary] = []
    for stop_id, payload in sorted(per_stop.items()):
        base_weekly_bus_mask = None
        bus_service_subtier = None
        has_daily_bus_service = False
        if payload["has_any_bus_service"]:
            base_weekly_bus_mask = _weekday_mask_for_weekdays(
                set(payload["base_weekly_bus_weekdays"])
            )
            bus_service_subtier = _bus_subtier_for_mask(base_weekly_bus_mask)
            has_daily_bus_service = base_weekly_bus_mask == "1111111"
        if payload["public_departures_30d"] > 0:
            payload["reason_codes"].add("public_service_present")
        if payload["school_only_departures_30d"] > 0:
            payload["reason_codes"].add("school_only_service_present")
        if payload["last_any_service_date"] is None:
            payload["reason_codes"].add("no_service_window")
        summaries.append(
            StopServiceSummary(
                reality_fingerprint=reality_fingerprint,
                feed_id=dataset.feed_id,
                stop_id=stop_id,
                public_departures_7d=int(payload["public_departures_7d"]),
                public_departures_30d=int(payload["public_departures_30d"]),
                school_only_departures_30d=int(payload["school_only_departures_30d"]),
                last_public_service_date=payload["last_public_service_date"],
                last_any_service_date=payload["last_any_service_date"],
                route_modes=tuple(sorted(payload["route_modes"])),
                route_ids=tuple(sorted(payload["route_ids"])),
                reason_codes=tuple(sorted(payload["reason_codes"])),
                # Legacy export field name; semantics are now the base weekly bus mask.
                bus_active_days_mask_7d=base_weekly_bus_mask,
                bus_service_subtier=bus_service_subtier,
                is_unscheduled_stop=bool(payload["is_unscheduled_stop"]),
                has_exception_only_service=bool(payload["has_exception_only_service"]),
                has_any_bus_service=bool(payload["has_any_bus_service"]),
                has_daily_bus_service=has_daily_bus_service,
            )
        )

    return summaries
