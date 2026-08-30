#!/usr/bin/env python3
"""Benchmark the local Valhalla sources_to_targets endpoint.

This benchmark is intentionally independent from the project's PostGIS build
state. It prepares routeable points by snapping jittered city-center samples to
 Valhalla's road graph via ``/locate``, then measures steady-state matrix
 requests against ``/sources_to_targets``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import random
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


CITY_CENTERS: tuple[tuple[str, float, float], ...] = (
    ("Dublin", 53.3498, -6.2603),
    ("Cork", 51.8985, -8.4756),
    ("Galway", 53.2707, -9.0568),
    ("Limerick", 52.6638, -8.6267),
    ("Waterford", 52.2593, -7.1101),
    ("Belfast", 54.5973, -5.9301),
    ("Derry", 54.9966, -7.3086),
    ("Drogheda", 53.7179, -6.3561),
    ("Sligo", 54.2766, -8.4761),
    ("Athlone", 53.4239, -7.9407),
)


@dataclass(frozen=True)
class Point:
    city: str
    lat: float
    lon: float


def _format_seconds(seconds: float) -> str:
    if seconds >= 86400:
        return f"{seconds / 86400:.2f} days"
    if seconds >= 3600:
        return f"{seconds / 3600:.2f} hours"
    if seconds >= 60:
        return f"{seconds / 60:.2f} min"
    return f"{seconds:.1f} sec"


def _post_json(url: str, payload: dict, timeout: float) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _endpoint_available(url: str, timeout: float) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return int(response.status) == 200
    except Exception:
        return False


def _jittered_candidates(
    city: str,
    lat: float,
    lon: float,
    *,
    grid_radius: int,
    step_lat: float,
    rng: random.Random,
) -> list[Point]:
    candidates: list[Point] = []
    cos_lat = max(0.3, math.cos(math.radians(lat)))
    step_lon = step_lat / cos_lat
    for ix in range(-grid_radius, grid_radius + 1):
        for iy in range(-grid_radius, grid_radius + 1):
            jitter_lat = (rng.random() - 0.5) * step_lat * 0.35
            jitter_lon = (rng.random() - 0.5) * step_lon * 0.35
            candidates.append(
                Point(
                    city=city,
                    lat=lat + (ix * step_lat) + jitter_lat,
                    lon=lon + (iy * step_lon) + jitter_lon,
                )
            )
    return candidates


def _snap_points(
    locate_url: str,
    candidates: list[Point],
    *,
    batch_size: int,
    timeout: float,
) -> list[Point]:
    snapped: list[Point] = []
    seen: set[tuple[str, int, int]] = set()
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        payload = {
            "verbose": False,
            "locations": [{"lat": point.lat, "lon": point.lon} for point in batch],
        }
        response = _post_json(locate_url, payload, timeout)
        if isinstance(response, dict):
            items = response.get("value", [])
        elif isinstance(response, list):
            items = response
        else:
            items = []
        for point, item in zip(batch, items):
            nodes = item.get("nodes") or []
            if not nodes:
                continue
            lat = float(nodes[0]["lat"])
            lon = float(nodes[0]["lon"])
            key = (point.city, round(lat * 1_000_000), round(lon * 1_000_000))
            if key in seen:
                continue
            seen.add(key)
            snapped.append(Point(city=point.city, lat=lat, lon=lon))
    return snapped


def _plan_jobs(points: list[Point], *, targets_per_request: int, request_count: int) -> list[tuple[Point, list[Point]]]:
    by_city: dict[str, list[Point]] = {}
    for point in points:
        by_city.setdefault(point.city, []).append(point)

    jobs: list[tuple[Point, list[Point]]] = []
    for city_points in by_city.values():
        if len(city_points) <= targets_per_request:
            continue
        for source in city_points:
            nearby = sorted(
                (target for target in city_points if target != source),
                key=lambda target: _distance_key(source, target),
            )
            targets = nearby[:targets_per_request]
            if len(targets) == targets_per_request:
                jobs.append((source, targets))

    jobs.sort(key=lambda job: (job[0].city, job[0].lat, job[0].lon))
    return jobs[:request_count]


def _distance_key(a: Point, b: Point) -> float:
    lat_scale = 111_320.0
    lon_scale = math.cos(math.radians((a.lat + b.lat) * 0.5)) * lat_scale
    d_lat = (a.lat - b.lat) * lat_scale
    d_lon = (a.lon - b.lon) * lon_scale
    return d_lat * d_lat + d_lon * d_lon


def _call_matrix(url: str, timeout: float, job: tuple[Point, list[Point]]) -> tuple[bool, float, int, str | None]:
    source, targets = job
    payload = {
        "sources": [{"lat": source.lat, "lon": source.lon}],
        "targets": [{"lat": target.lat, "lon": target.lon} for target in targets],
        "costing": "pedestrian",
        "verbose": False,
    }
    started = time.perf_counter()
    try:
        response = _post_json(url, payload, timeout)
        durations = response["sources_to_targets"]["durations"][0]
        route_count = sum(1 for value in durations if value is not None)
        return True, time.perf_counter() - started, route_count, None
    except Exception as exc:  # pragma: no cover - benchmark reporting path
        return False, time.perf_counter() - started, 0, repr(exc)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, math.ceil(len(values) * pct) - 1))
    return values[index]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8002/sources_to_targets")
    parser.add_argument("--locate-url", default="http://127.0.0.1:8002/locate")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--targets", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--grid-radius", type=int, default=12)
    parser.add_argument("--grid-step-deg", type=float, default=0.0025)
    parser.add_argument("--locate-batch-size", type=int, default=64)
    parser.add_argument("--total-cells", type=int, default=33_768_400)
    args = parser.parse_args(argv)

    if args.requests < 1 or args.workers < 1 or args.targets < 1:
        parser.error("--requests, --workers, and --targets must be positive integers")

    if not _endpoint_available(args.url.replace("/sources_to_targets", "/status"), timeout=5):
        print(f"Valhalla status endpoint is not reachable near {args.url}", file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    raw_candidates: list[Point] = []
    for city, lat, lon in CITY_CENTERS:
        raw_candidates.extend(
            _jittered_candidates(
                city,
                lat,
                lon,
                grid_radius=args.grid_radius,
                step_lat=args.grid_step_deg,
                rng=rng,
            )
        )

    prep_started = time.perf_counter()
    points = _snap_points(
        args.locate_url,
        raw_candidates,
        batch_size=args.locate_batch_size,
        timeout=args.timeout,
    )
    jobs = _plan_jobs(
        points,
        targets_per_request=args.targets,
        request_count=args.requests,
    )
    prep_seconds = time.perf_counter() - prep_started

    if len(jobs) < args.warmup:
        print(
            f"Prepared only {len(jobs)} benchmark jobs; increase grid density before benchmarking.",
            file=sys.stderr,
        )
        return 3

    city_counts: dict[str, int] = {}
    for point in points:
        city_counts[point.city] = city_counts.get(point.city, 0) + 1

    print("VALHALLA BENCHMARK")
    print("==================")
    print(f"Endpoint:        {args.url}")
    print(f"Workers:         {args.workers}")
    print(f"Requests:        {len(jobs)}")
    print(f"Targets/request: {args.targets}")
    print(f"Warmup:          {args.warmup}")
    print(f"Prep time:       {prep_seconds:.2f}s")
    print("Prepared points:")
    for city, count in sorted(city_counts.items()):
        print(f"  {city:10} {count:4d}")

    warmup_jobs = jobs[: args.warmup]
    measured_jobs = jobs

    for job in warmup_jobs:
        ok, latency, route_count, error = _call_matrix(args.url, args.timeout, job)
        if not ok or route_count == 0:
            print(f"Warmup failed after {latency:.3f}s: {error}", file=sys.stderr)
            return 4

    latencies_ms: list[float] = []
    errors: list[str] = []
    ok_count = 0
    route_count = 0
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(_call_matrix, args.url, args.timeout, job)
            for job in measured_jobs
        ]
        for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            ok, latency, routes, error = future.result()
            latencies_ms.append(latency * 1000.0)
            if ok:
                ok_count += 1
                route_count += routes
            elif error is not None:
                errors.append(error)
            if index % 100 == 0 or index == len(futures):
                print(f"Progress: {index}/{len(futures)}")
    wall_seconds = time.perf_counter() - started

    latencies_ms.sort()
    failures = len(measured_jobs) - ok_count
    request_rps = ok_count / wall_seconds if wall_seconds else 0.0
    route_rps = route_count / wall_seconds if wall_seconds else 0.0
    projected_seconds = args.total_cells / request_rps if request_rps else float("inf")

    print()
    print("Results")
    print("-------")
    print(f"Successful requests: {ok_count:,}")
    print(f"Failed requests:     {failures:,}")
    print(f"Resolved routes:     {route_count:,}")
    print(f"Wall time:           {wall_seconds:.3f}s")
    print(f"Requests/sec:        {request_rps:,.2f}")
    print(f"Routes/sec:          {route_rps:,.2f}")
    print(f"Latency mean:        {statistics.fmean(latencies_ms):.1f} ms")
    print(f"Latency p50:         {_percentile(latencies_ms, 0.50):.1f} ms")
    print(f"Latency p95:         {_percentile(latencies_ms, 0.95):.1f} ms")
    print(f"Latency p99:         {_percentile(latencies_ms, 0.99):.1f} ms")
    print(f"Whole-island proxy:  {_format_seconds(projected_seconds)}")
    if errors:
        print("Sample error:        " + errors[0])

    return 0 if failures == 0 else 5


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
