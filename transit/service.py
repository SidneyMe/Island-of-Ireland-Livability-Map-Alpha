from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import math

from config import (
    GTFS_COMMUTE_AM_END_HOUR,
    GTFS_COMMUTE_AM_START_HOUR,
    GTFS_COMMUTE_PM_END_HOUR,
    GTFS_COMMUTE_PM_START_HOUR,
    GTFS_FRIDAY_EVENING_END_HOUR,
    GTFS_FRIDAY_EVENING_START_HOUR,
)
from .models import CalendarService, FeedDataset, ServiceClassification, StopInfo, StopServiceSummary


_BUS_SUBTIER_BY_MASK = {
    "1111111": "mon_sun",
    "1111110": "mon_sat",
    "0111111": "tue_sun",
    "1111100": "weekdays_only",
    "0000011": "weekends_only",
}

_SECONDS_PER_HOUR = 3600
_FRIDAY_WEEKDAY = 4
_SATURDAY_WEEKDAY = 5
_SUNDAY_WEEKDAY = 6


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


def _hour_window_contains(seconds: int | None, *, start_hour: int, end_hour: int) -> bool:
    if seconds is None:
        return False
    start_seconds = int(start_hour) * _SECONDS_PER_HOUR
    end_seconds = int(end_hour) * _SECONDS_PER_HOUR
    if end_seconds <= start_seconds:
        end_seconds += 24 * _SECONDS_PER_HOUR
    return start_seconds <= int(seconds) < end_seconds


def _is_weekday_morning_peak(seconds: int | None) -> bool:
    return _hour_window_contains(
        seconds,
        start_hour=GTFS_COMMUTE_AM_START_HOUR,
        end_hour=GTFS_COMMUTE_AM_END_HOUR,
    )


def _is_weekday_evening_peak(seconds: int | None) -> bool:
    return _hour_window_contains(
        seconds,
        start_hour=GTFS_COMMUTE_PM_START_HOUR,
        end_hour=GTFS_COMMUTE_PM_END_HOUR,
    )


def _is_friday_evening(seconds: int | None) -> bool:
    return _hour_window_contains(
        seconds,
        start_hour=GTFS_FRIDAY_EVENING_START_HOUR,
        end_hour=GTFS_FRIDAY_EVENING_END_HOUR,
    )


def _avg(total: int, dates: set[date]) -> float:
    if not dates:
        return 0.0
    return float(total) / float(len(dates))


def transport_score_units_from_frequency(
    *,
    weekday_morning_peak_deps: float,
    weekday_evening_peak_deps: float,
    weekday_offpeak_deps: float,
    saturday_deps: float,
    sunday_deps: float,
    friday_evening_deps: float,
    public_departures_30d: int,
) -> int:
    if public_departures_30d <= 0:
        return 0

    peak_am = min(max(float(weekday_morning_peak_deps), 0.0) / 16.0, 1.0)
    peak_pm = min(max(float(weekday_evening_peak_deps), 0.0) / 16.0, 1.0)
    commute = 0.6 * min(peak_am, peak_pm) + 0.4 * ((peak_am + peak_pm) / 2.0)
    friday = min(max(float(friday_evening_deps), 0.0) / 24.0, 1.0)
    offpeak = min(max(float(weekday_offpeak_deps), 0.0) / 32.0, 1.0)
    weekend = min(
        ((max(float(saturday_deps), 0.0) + max(float(sunday_deps), 0.0)) / 2.0) / 24.0,
        1.0,
    )
    frequency = 0.60 * commute + 0.20 * friday + 0.10 * offpeak + 0.10 * weekend
    return min(max(int(math.ceil(frequency * 5.0)), 1), 5)


def _iter_stop_service_time_occurrences(
    dataset: FeedDataset,
):
    if dataset.stop_service_time_occurrences:
        for (
            stop_id,
            service_id,
            route_id,
            mode,
            event_seconds,
        ), occurrences in dataset.stop_service_time_occurrences.items():
            yield stop_id, service_id, route_id, mode, event_seconds, occurrences
        return

    for (stop_id, service_id, route_id, mode), occurrences in dataset.stop_service_occurrences.items():
        yield stop_id, service_id, route_id, mode, None, occurrences


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

    for (
        stop_id,
        service_id,
        route_id,
        mode,
        event_seconds,
        occurrences,
    ) in _iter_stop_service_time_occurrences(dataset):
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
                "weekday_morning_peak_total": 0,
                "weekday_evening_peak_total": 0,
                "weekday_offpeak_total": 0,
                "saturday_total": 0,
                "sunday_total": 0,
                "friday_evening_total": 0,
                "weekday_dates": set(),
                "saturday_dates": set(),
                "sunday_dates": set(),
                "friday_dates": set(),
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
        for active_date in dates_30d:
            weekday = active_date.weekday()
            if weekday < _SATURDAY_WEEKDAY:
                stop_payload["weekday_dates"].add(active_date)
                if _is_weekday_morning_peak(event_seconds):
                    stop_payload["weekday_morning_peak_total"] += occurrences
                elif _is_weekday_evening_peak(event_seconds):
                    stop_payload["weekday_evening_peak_total"] += occurrences
                else:
                    stop_payload["weekday_offpeak_total"] += occurrences
            if weekday == _SATURDAY_WEEKDAY:
                stop_payload["saturday_dates"].add(active_date)
                stop_payload["saturday_total"] += occurrences
            elif weekday == _SUNDAY_WEEKDAY:
                stop_payload["sunday_dates"].add(active_date)
                stop_payload["sunday_total"] += occurrences
            if weekday == _FRIDAY_WEEKDAY:
                stop_payload["friday_dates"].add(active_date)
                if _is_friday_evening(event_seconds):
                    stop_payload["friday_evening_total"] += occurrences
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
            "weekday_morning_peak_total": 0,
            "weekday_evening_peak_total": 0,
            "weekday_offpeak_total": 0,
            "saturday_total": 0,
            "sunday_total": 0,
            "friday_evening_total": 0,
            "weekday_dates": set(),
            "saturday_dates": set(),
            "sunday_dates": set(),
            "friday_dates": set(),
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
        weekday_morning_peak_deps = _avg(
            int(payload["weekday_morning_peak_total"]),
            set(payload["weekday_dates"]),
        )
        weekday_evening_peak_deps = _avg(
            int(payload["weekday_evening_peak_total"]),
            set(payload["weekday_dates"]),
        )
        weekday_offpeak_deps = _avg(
            int(payload["weekday_offpeak_total"]),
            set(payload["weekday_dates"]),
        )
        saturday_deps = _avg(int(payload["saturday_total"]), set(payload["saturday_dates"]))
        sunday_deps = _avg(int(payload["sunday_total"]), set(payload["sunday_dates"]))
        friday_evening_deps = _avg(
            int(payload["friday_evening_total"]),
            set(payload["friday_dates"]),
        )
        transport_score_units = transport_score_units_from_frequency(
            weekday_morning_peak_deps=weekday_morning_peak_deps,
            weekday_evening_peak_deps=weekday_evening_peak_deps,
            weekday_offpeak_deps=weekday_offpeak_deps,
            saturday_deps=saturday_deps,
            sunday_deps=sunday_deps,
            friday_evening_deps=friday_evening_deps,
            public_departures_30d=int(payload["public_departures_30d"]),
        )
        summaries.append(
            StopServiceSummary(
                reality_fingerprint=reality_fingerprint,
                feed_id=dataset.feed_id,
                stop_id=stop_id,
                public_departures_7d=int(payload["public_departures_7d"]),
                public_departures_30d=int(payload["public_departures_30d"]),
                school_only_departures_30d=int(payload["school_only_departures_30d"]),
                weekday_morning_peak_deps=weekday_morning_peak_deps,
                weekday_evening_peak_deps=weekday_evening_peak_deps,
                weekday_offpeak_deps=weekday_offpeak_deps,
                saturday_deps=saturday_deps,
                sunday_deps=sunday_deps,
                friday_evening_deps=friday_evening_deps,
                transport_score_units=transport_score_units,
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
