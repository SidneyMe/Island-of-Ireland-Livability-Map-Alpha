from __future__ import annotations

import csv
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

from shapely.geometry import Point

from config import (
    GTFS_SCHOOL_AM_END_HOUR,
    GTFS_SCHOOL_AM_START_HOUR,
    GTFS_SCHOOL_KEYWORDS,
    GTFS_SCHOOL_PM_END_HOUR,
    GTFS_SCHOOL_PM_START_HOUR,
    TransitFeedState,
)

from .models import (
    CalendarDateException,
    CalendarService,
    FeedDataset,
    RouteInfo,
    StopInfo,
    TripInfo,
)
from .naming import token_set


REQUIRED_GTFS_FILENAMES = (
    "stops.txt",
    "stop_times.txt",
    "trips.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
)


def _member_name(zip_file: ZipFile, expected_name: str) -> str:
    for member_name in zip_file.namelist():
        if member_name.endswith(expected_name):
            return member_name
    raise RuntimeError(
        f"GTFS feed zip is missing required file '{expected_name}'."
    )


def _csv_rows(zip_file: ZipFile, expected_name: str):
    member_name = _member_name(zip_file, expected_name)
    with zip_file.open(member_name, "r") as handle:
        wrapper = TextIOWrapper(handle, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(wrapper)
        for row in reader:
            yield {str(key or "").strip(): (value or "").strip() for key, value in row.items()}


def _required_text(row: dict[str, str], field_name: str, *, file_name: str) -> str:
    value = (row.get(field_name) or "").strip()
    if value:
        return value
    raise RuntimeError(f"GTFS {file_name} is missing required field '{field_name}'.")


def _optional_text(row: dict[str, str], field_name: str) -> str | None:
    value = (row.get(field_name) or "").strip()
    return value or None


def _optional_int(row: dict[str, str], field_name: str, *, file_name: str) -> int | None:
    raw_value = (row.get(field_name) or "").strip()
    if not raw_value:
        return None
    try:
        return int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"GTFS {file_name} field '{field_name}' must be an integer; got {raw_value!r}."
        ) from exc


def _required_float(row: dict[str, str], field_name: str, *, file_name: str) -> float:
    raw_value = _required_text(row, field_name, file_name=file_name)
    try:
        return float(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"GTFS {file_name} field '{field_name}' must be numeric; got {raw_value!r}."
        ) from exc


def _required_date(row: dict[str, str], field_name: str, *, file_name: str):
    raw_value = _required_text(row, field_name, file_name=file_name)
    try:
        return datetime.strptime(raw_value, "%Y%m%d").date()
    except ValueError as exc:
        raise RuntimeError(
            f"GTFS {file_name} field '{field_name}' must use YYYYMMDD; got {raw_value!r}."
        ) from exc


def _parse_hhmmss_to_seconds(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized:
        return None
    parts = normalized.split(":")
    if len(parts) != 3:
        raise RuntimeError(f"GTFS stop_times time must use HH:MM:SS; got {raw_value!r}.")
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError as exc:
        raise RuntimeError(f"GTFS stop_times time must use HH:MM:SS; got {raw_value!r}.") from exc
    if minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60 or hours < 0:
        raise RuntimeError(f"GTFS stop_times time must use HH:MM:SS; got {raw_value!r}.")
    return hours * 3600 + minutes * 60 + seconds


def route_mode(route_type: int | None) -> str:
    if route_type in {0, 900, 901, 902, 903, 904, 905, 906}:
        return "tram"
    if route_type in {1, 100, 101, 102, 103, 105, 106, 107, 108, 109, 110, 111, 112, 113}:
        return "rail"
    if route_type in {2, 3, 200, 201, 202, 204, 700, 701, 702, 704, 705, 711, 712, 713}:
        return "bus"
    if route_type in {4, 1000, 1200}:
        return "ferry"
    return "other"


def _school_tokens(value: str | None) -> set[str]:
    return set(token_set(value)).intersection(GTFS_SCHOOL_KEYWORDS)


def _time_bucket(departure_seconds: int | None) -> str:
    if departure_seconds is None:
        return "offpeak"
    hour = (departure_seconds // 3600) % 24
    if GTFS_SCHOOL_AM_START_HOUR <= hour <= GTFS_SCHOOL_AM_END_HOUR:
        return "morning"
    if GTFS_SCHOOL_PM_START_HOUR <= hour <= GTFS_SCHOOL_PM_END_HOUR:
        return "afternoon"
    return "offpeak"


def parse_gtfs_zip(feed_state: TransitFeedState) -> FeedDataset:
    zip_path = Path(feed_state.zip_path)
    if not zip_path.exists():
        raise RuntimeError(f"GTFS feed zip was not found at '{zip_path}'.")

    created_at = datetime.now(timezone.utc)
    dataset = FeedDataset(
        feed_id=feed_state.feed_id,
        feed_fingerprint=feed_state.feed_fingerprint,
        analysis_date=feed_state.analysis_date,
        created_at=created_at,
        source_path=str(zip_path),
        source_url=feed_state.source_url,
    )

    with ZipFile(zip_path) as zip_file:
        for required_name in REQUIRED_GTFS_FILENAMES:
            _member_name(zip_file, required_name)

        for row in _csv_rows(zip_file, "stops.txt"):
            stop_id = _required_text(row, "stop_id", file_name="stops.txt")
            stop_name = _required_text(row, "stop_name", file_name="stops.txt")
            stop_lat = _required_float(row, "stop_lat", file_name="stops.txt")
            stop_lon = _required_float(row, "stop_lon", file_name="stops.txt")
            stop_info = StopInfo(
                feed_id=feed_state.feed_id,
                stop_id=stop_id,
                stop_code=_optional_text(row, "stop_code"),
                stop_name=stop_name,
                stop_desc=_optional_text(row, "stop_desc"),
                stop_lat=stop_lat,
                stop_lon=stop_lon,
                parent_station=_optional_text(row, "parent_station"),
                zone_id=_optional_text(row, "zone_id"),
                location_type=_optional_int(row, "location_type", file_name="stops.txt"),
                wheelchair_boarding=_optional_int(
                    row,
                    "wheelchair_boarding",
                    file_name="stops.txt",
                ),
                platform_code=_optional_text(row, "platform_code"),
            )
            dataset.stops[stop_id] = stop_info
            dataset.raw_stop_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "stop_id": stop_id,
                    "stop_code": stop_info.stop_code,
                    "stop_name": stop_name,
                    "stop_desc": stop_info.stop_desc,
                    "stop_lat": stop_lat,
                    "stop_lon": stop_lon,
                    "parent_station": stop_info.parent_station,
                    "zone_id": stop_info.zone_id,
                    "location_type": stop_info.location_type,
                    "wheelchair_boarding": stop_info.wheelchair_boarding,
                    "platform_code": stop_info.platform_code,
                    "geom": Point(stop_lon, stop_lat),
                    "created_at": created_at,
                }
            )

        for row in _csv_rows(zip_file, "routes.txt"):
            route_id = _required_text(row, "route_id", file_name="routes.txt")
            route_info = RouteInfo(
                feed_id=feed_state.feed_id,
                route_id=route_id,
                agency_id=_optional_text(row, "agency_id"),
                route_short_name=_optional_text(row, "route_short_name"),
                route_long_name=_optional_text(row, "route_long_name"),
                route_desc=_optional_text(row, "route_desc"),
                route_type=_optional_int(row, "route_type", file_name="routes.txt"),
                route_url=_optional_text(row, "route_url"),
                route_color=_optional_text(row, "route_color"),
                route_text_color=_optional_text(row, "route_text_color"),
            )
            dataset.routes[route_id] = route_info
            dataset.raw_route_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "route_id": route_id,
                    "agency_id": route_info.agency_id,
                    "route_short_name": route_info.route_short_name,
                    "route_long_name": route_info.route_long_name,
                    "route_desc": route_info.route_desc,
                    "route_type": route_info.route_type,
                    "route_url": route_info.route_url,
                    "route_color": route_info.route_color,
                    "route_text_color": route_info.route_text_color,
                    "created_at": created_at,
                }
            )

        for row in _csv_rows(zip_file, "trips.txt"):
            trip_id = _required_text(row, "trip_id", file_name="trips.txt")
            route_id = _required_text(row, "route_id", file_name="trips.txt")
            service_id = _required_text(row, "service_id", file_name="trips.txt")
            trip_info = TripInfo(
                feed_id=feed_state.feed_id,
                trip_id=trip_id,
                route_id=route_id,
                service_id=service_id,
                trip_headsign=_optional_text(row, "trip_headsign"),
                trip_short_name=_optional_text(row, "trip_short_name"),
                direction_id=_optional_int(row, "direction_id", file_name="trips.txt"),
                block_id=_optional_text(row, "block_id"),
                shape_id=_optional_text(row, "shape_id"),
            )
            dataset.trips[trip_id] = trip_info
            dataset.raw_trip_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "route_id": route_id,
                    "service_id": service_id,
                    "trip_id": trip_id,
                    "trip_headsign": trip_info.trip_headsign,
                    "trip_short_name": trip_info.trip_short_name,
                    "direction_id": trip_info.direction_id,
                    "block_id": trip_info.block_id,
                    "shape_id": trip_info.shape_id,
                    "created_at": created_at,
                }
            )

            route_info = dataset.routes.get(route_id)
            if route_info is not None:
                dataset.service_route_ids.setdefault(service_id, set()).add(route_id)
                dataset.service_route_modes.setdefault(service_id, set()).add(
                    route_mode(route_info.route_type)
                )
                route_text = " ".join(
                    part
                    for part in (
                        route_info.route_short_name,
                        route_info.route_long_name,
                        route_info.route_desc,
                        trip_info.trip_headsign,
                        trip_info.trip_short_name,
                    )
                    if part
                )
                keywords = _school_tokens(route_text)
                if keywords:
                    dataset.service_keywords.setdefault(service_id, set()).update(keywords)

        for row in _csv_rows(zip_file, "calendar.txt"):
            service_id = _required_text(row, "service_id", file_name="calendar.txt")
            calendar_service = CalendarService(
                feed_id=feed_state.feed_id,
                service_id=service_id,
                monday=int(_required_text(row, "monday", file_name="calendar.txt")),
                tuesday=int(_required_text(row, "tuesday", file_name="calendar.txt")),
                wednesday=int(_required_text(row, "wednesday", file_name="calendar.txt")),
                thursday=int(_required_text(row, "thursday", file_name="calendar.txt")),
                friday=int(_required_text(row, "friday", file_name="calendar.txt")),
                saturday=int(_required_text(row, "saturday", file_name="calendar.txt")),
                sunday=int(_required_text(row, "sunday", file_name="calendar.txt")),
                start_date=_required_date(row, "start_date", file_name="calendar.txt"),
                end_date=_required_date(row, "end_date", file_name="calendar.txt"),
            )
            dataset.calendar_services[service_id] = calendar_service
            dataset.raw_calendar_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "service_id": service_id,
                    "monday": calendar_service.monday,
                    "tuesday": calendar_service.tuesday,
                    "wednesday": calendar_service.wednesday,
                    "thursday": calendar_service.thursday,
                    "friday": calendar_service.friday,
                    "saturday": calendar_service.saturday,
                    "sunday": calendar_service.sunday,
                    "start_date": calendar_service.start_date,
                    "end_date": calendar_service.end_date,
                    "created_at": created_at,
                }
            )

        for row in _csv_rows(zip_file, "calendar_dates.txt"):
            exception = CalendarDateException(
                feed_id=feed_state.feed_id,
                service_id=_required_text(row, "service_id", file_name="calendar_dates.txt"),
                service_date=_required_date(row, "date", file_name="calendar_dates.txt"),
                exception_type=int(_required_text(row, "exception_type", file_name="calendar_dates.txt")),
            )
            dataset.calendar_dates.append(exception)
            dataset.raw_calendar_date_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "service_id": exception.service_id,
                    "service_date": exception.service_date,
                    "exception_type": exception.exception_type,
                    "created_at": created_at,
                }
            )

        for row in _csv_rows(zip_file, "stop_times.txt"):
            trip_id = _required_text(row, "trip_id", file_name="stop_times.txt")
            trip_info = dataset.trips.get(trip_id)
            if trip_info is None:
                raise RuntimeError(
                    f"GTFS stop_times.txt references unknown trip_id {trip_id!r} in feed {feed_state.feed_id}."
                )
            stop_id = _required_text(row, "stop_id", file_name="stop_times.txt")
            if stop_id not in dataset.stops:
                raise RuntimeError(
                    f"GTFS stop_times.txt references unknown stop_id {stop_id!r} in feed {feed_state.feed_id}."
                )
            departure_seconds = _parse_hhmmss_to_seconds(_optional_text(row, "departure_time"))
            arrival_seconds = _parse_hhmmss_to_seconds(_optional_text(row, "arrival_time"))
            stop_sequence = _optional_int(row, "stop_sequence", file_name="stop_times.txt")
            if stop_sequence is None:
                raise RuntimeError("GTFS stop_times.txt is missing required field 'stop_sequence'.")
            route_info = dataset.routes.get(trip_info.route_id)
            mode = route_mode(route_info.route_type if route_info is not None else None)
            occurrence_key = (stop_id, trip_info.service_id, trip_info.route_id, mode)
            dataset.stop_service_occurrences[occurrence_key] = (
                dataset.stop_service_occurrences.get(occurrence_key, 0) + 1
            )
            bucket = _time_bucket(departure_seconds if departure_seconds is not None else arrival_seconds)
            bucket_counts = dataset.service_time_buckets.setdefault(
                trip_info.service_id,
                {"morning": 0, "afternoon": 0, "offpeak": 0},
            )
            bucket_counts[bucket] += 1
            dataset.raw_stop_time_rows.append(
                {
                    "feed_fingerprint": dataset.feed_fingerprint,
                    "feed_id": dataset.feed_id,
                    "trip_id": trip_id,
                    "arrival_seconds": arrival_seconds,
                    "departure_seconds": departure_seconds,
                    "stop_id": stop_id,
                    "stop_sequence": stop_sequence,
                    "pickup_type": _optional_int(row, "pickup_type", file_name="stop_times.txt"),
                    "drop_off_type": _optional_int(row, "drop_off_type", file_name="stop_times.txt"),
                    "created_at": created_at,
                }
            )

    return dataset
