from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TripStopTime:
    trip_id: str
    stop_id: str
    arrival_seconds: int | None
    departure_seconds: int | None
    stop_sequence: int
    public: bool = True
    quality: float = 1.0


@dataclass(frozen=True)
class StopReach:
    destination_stop_id: str
    travel_seconds: int
    transfer_count: int
    quality: float


def _time_value(row: TripStopTime, *, arrival: bool) -> int | None:
    value = row.arrival_seconds if arrival else row.departure_seconds
    if value is None:
        value = row.departure_seconds if arrival else row.arrival_seconds
    if value is None:
        return None
    return int(value)


def _direct_legs(rows: Iterable[TripStopTime], *, max_travel_seconds: int) -> dict[str, list[StopReach]]:
    by_trip: dict[str, list[TripStopTime]] = defaultdict(list)
    for row in rows:
        if row.public:
            by_trip[str(row.trip_id)].append(row)

    direct: dict[str, list[StopReach]] = defaultdict(list)
    for trip_rows in by_trip.values():
        ordered = sorted(trip_rows, key=lambda row: int(row.stop_sequence))
        for origin_index, origin in enumerate(ordered):
            depart = _time_value(origin, arrival=False)
            if depart is None:
                continue
            for destination in ordered[origin_index + 1 :]:
                arrive = _time_value(destination, arrival=True)
                if arrive is None or arrive < depart:
                    continue
                travel = int(arrive - depart)
                if travel > max_travel_seconds:
                    continue
                direct[str(origin.stop_id)].append(
                    StopReach(
                        destination_stop_id=str(destination.stop_id),
                        travel_seconds=travel,
                        transfer_count=0,
                        quality=max(min(float(origin.quality), float(destination.quality)), 0.0),
                    )
                )
    return dict(direct)


def derive_transfer_stop_reach(
    rows: Iterable[TripStopTime],
    *,
    max_transfer_count: int,
    max_wait_seconds: int,
    max_travel_seconds: int,
) -> dict[str, list[StopReach]]:
    """Build bounded public stop-to-stop reach with optional one-transfer paths."""

    direct = _direct_legs(rows, max_travel_seconds=max_travel_seconds)
    best: dict[tuple[str, str], StopReach] = {}
    for origin, reaches in direct.items():
        for reach in reaches:
            key = (origin, reach.destination_stop_id)
            current = best.get(key)
            if current is None or (reach.transfer_count, reach.travel_seconds, -reach.quality) < (
                current.transfer_count,
                current.travel_seconds,
                -current.quality,
            ):
                best[key] = reach

    if int(max_transfer_count) <= 0:
        return _group_best(best)

    direct_by_origin = direct
    # Conservative transfer expansion: if A reaches B directly, B may board any
    # later direct leg. We only have aggregate leg times here, so max_wait_seconds
    # acts as a bounded transfer allowance added to travel time.
    for origin, first_legs in direct_by_origin.items():
        for first in first_legs:
            second_legs = direct_by_origin.get(first.destination_stop_id, [])
            for second in second_legs:
                total_travel = (
                    int(first.travel_seconds)
                    + min(max(int(max_wait_seconds), 0), int(max_wait_seconds))
                    + int(second.travel_seconds)
                )
                if total_travel > int(max_travel_seconds):
                    continue
                quality = min(float(first.quality), float(second.quality)) * 0.85
                reach = StopReach(
                    destination_stop_id=second.destination_stop_id,
                    travel_seconds=total_travel,
                    transfer_count=1,
                    quality=max(quality, 0.0),
                )
                key = (origin, reach.destination_stop_id)
                current = best.get(key)
                if current is None or (reach.transfer_count, reach.travel_seconds, -reach.quality) < (
                    current.transfer_count,
                    current.travel_seconds,
                    -current.quality,
                ):
                    best[key] = reach

    return _group_best(best)


def _group_best(best: dict[tuple[str, str], StopReach]) -> dict[str, list[StopReach]]:
    grouped: dict[str, list[StopReach]] = defaultdict(list)
    for (origin, _destination), reach in best.items():
        grouped[origin].append(reach)
    return {
        origin: sorted(reaches, key=lambda reach: (reach.transfer_count, reach.travel_seconds, reach.destination_stop_id))
        for origin, reaches in grouped.items()
    }
