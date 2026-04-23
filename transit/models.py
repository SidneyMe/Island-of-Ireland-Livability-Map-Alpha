from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class StopInfo:
    feed_id: str
    stop_id: str
    stop_code: str | None
    stop_name: str
    stop_desc: str | None
    stop_lat: float
    stop_lon: float
    parent_station: str | None
    zone_id: str | None
    location_type: int | None
    wheelchair_boarding: int | None
    platform_code: str | None


@dataclass(frozen=True, slots=True)
class RouteInfo:
    feed_id: str
    route_id: str
    agency_id: str | None
    route_short_name: str | None
    route_long_name: str | None
    route_desc: str | None
    route_type: int | None
    route_url: str | None
    route_color: str | None
    route_text_color: str | None


@dataclass(frozen=True, slots=True)
class TripInfo:
    feed_id: str
    trip_id: str
    route_id: str
    service_id: str
    trip_headsign: str | None
    trip_short_name: str | None
    direction_id: int | None
    block_id: str | None
    shape_id: str | None


@dataclass(frozen=True, slots=True)
class CalendarService:
    feed_id: str
    service_id: str
    monday: int
    tuesday: int
    wednesday: int
    thursday: int
    friday: int
    saturday: int
    sunday: int
    start_date: date
    end_date: date


@dataclass(frozen=True, slots=True)
class CalendarDateException:
    feed_id: str
    service_id: str
    service_date: date
    exception_type: int


@dataclass(frozen=True, slots=True)
class ServiceClassification:
    reality_fingerprint: str
    feed_id: str
    service_id: str
    school_only_state: str
    route_ids: tuple[str, ...]
    route_modes: tuple[str, ...]
    reason_codes: tuple[str, ...]
    time_bucket_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class StopServiceSummary:
    reality_fingerprint: str
    feed_id: str
    stop_id: str
    public_departures_7d: int
    public_departures_30d: int
    school_only_departures_30d: int
    last_public_service_date: date | None
    last_any_service_date: date | None
    route_modes: tuple[str, ...]
    route_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    weekday_morning_peak_deps: float = 0.0
    weekday_evening_peak_deps: float = 0.0
    weekday_offpeak_deps: float = 0.0
    saturday_deps: float = 0.0
    sunday_deps: float = 0.0
    friday_evening_deps: float = 0.0
    transport_score_units: int = 0
    bus_active_days_mask_7d: str | None = None
    bus_service_subtier: str | None = None
    is_unscheduled_stop: bool = False
    has_exception_only_service: bool = False
    has_any_bus_service: bool = False
    has_daily_bus_service: bool = False


@dataclass(frozen=True, slots=True)
class GtfsStopReality:
    reality_fingerprint: str
    import_fingerprint: str
    source_ref: str
    stop_name: str | None
    feed_id: str
    stop_id: str
    source_status: str
    reality_status: str
    school_only_state: str
    public_departures_7d: int
    public_departures_30d: int
    school_only_departures_30d: int
    last_public_service_date: date | None
    last_any_service_date: date | None
    route_modes: tuple[str, ...]
    source_reason_codes: tuple[str, ...]
    reality_reason_codes: tuple[str, ...]
    lat: float
    lon: float
    weekday_morning_peak_deps: float = 0.0
    weekday_evening_peak_deps: float = 0.0
    weekday_offpeak_deps: float = 0.0
    saturday_deps: float = 0.0
    sunday_deps: float = 0.0
    friday_evening_deps: float = 0.0
    transport_score_units: int = 0
    bus_active_days_mask_7d: str | None = None
    bus_service_subtier: str | None = None
    is_unscheduled_stop: bool = False
    has_exception_only_service: bool = False
    has_any_bus_service: bool = False
    has_daily_bus_service: bool = False


@dataclass(frozen=True, slots=True)
class ServiceDesertCell:
    build_key: str
    reality_fingerprint: str
    import_fingerprint: str
    resolution_m: int
    cell_id: str
    analysis_date: date
    baseline_reachable_stop_count: int
    reachable_public_departures_7d: int
    reason_codes: tuple[str, ...]
    geometry: object


@dataclass(slots=True)
class FeedDataset:
    feed_id: str
    feed_fingerprint: str
    analysis_date: date
    created_at: datetime
    source_path: str
    source_url: str | None
    stops: dict[str, StopInfo] = field(default_factory=dict)
    routes: dict[str, RouteInfo] = field(default_factory=dict)
    trips: dict[str, TripInfo] = field(default_factory=dict)
    calendar_services: dict[str, CalendarService] = field(default_factory=dict)
    calendar_dates: list[CalendarDateException] = field(default_factory=list)
    stop_service_occurrences: dict[tuple[str, str, str, str], int] = field(default_factory=dict)
    stop_service_time_occurrences: dict[tuple[str, str, str, str, int | None], int] = field(default_factory=dict)
    service_time_buckets: dict[str, dict[str, int]] = field(default_factory=dict)
    service_route_ids: dict[str, set[str]] = field(default_factory=dict)
    service_route_modes: dict[str, set[str]] = field(default_factory=dict)
    service_keywords: dict[str, set[str]] = field(default_factory=dict)
    raw_stop_rows: list[dict[str, object]] = field(default_factory=list)
    raw_route_rows: list[dict[str, object]] = field(default_factory=list)
    raw_trip_rows: list[dict[str, object]] = field(default_factory=list)
    raw_stop_time_rows: list[dict[str, object]] = field(default_factory=list)
    raw_calendar_rows: list[dict[str, object]] = field(default_factory=list)
    raw_calendar_date_rows: list[dict[str, object]] = field(default_factory=list)
