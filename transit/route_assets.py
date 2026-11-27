from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from shapely.geometry import mapping

from config import CACHE_DIR


ROUTE_ASSETS_VERSION = 2
ROUTE_ASSETS_DIR = CACHE_DIR / "transit_routes"


def route_assets_dir(build_key: str) -> Path:
    return ROUTE_ASSETS_DIR / str(build_key)


def _asset_name(route_key: str) -> str:
    return hashlib.sha256(route_key.encode("utf-8")).hexdigest()[:24] + ".geojson"


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def _feature(geometry, **properties) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": mapping(geometry),
        "properties": properties,
    }


def publish_route_assets(
    *,
    build_key: str,
    reality_fingerprint: str,
    patterns: list[dict[str, Any]],
    stops: list[dict[str, Any]],
    transfer_hubs: list[dict[str, Any]],
) -> dict[str, Any]:
    target_dir = route_assets_dir(build_key)
    index_path = target_dir / "index.json"
    if index_path.is_file():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
            if (
                existing.get("version") == ROUTE_ASSETS_VERSION
                and existing.get("build_key") == build_key
                and existing.get("reality_fingerprint") == reality_fingerprint
            ):
                return existing
        except (OSError, json.JSONDecodeError):
            pass

    stops_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    stops_by_pattern: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stops:
        stops_by_route[str(row["route_key"])].append(row)
        stops_by_pattern[str(row["pattern_id"])].append(row)
    hubs_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in transfer_hubs:
        for key in row.get("route_keys_json") or []:
            hubs_by_route[str(key)].append(row)
    patterns_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in patterns:
        patterns_by_route[str(row["route_key"])].append(row)

    index_routes: list[dict[str, Any]] = []
    for key in sorted(patterns_by_route):
        route_patterns = patterns_by_route[key]
        first = route_patterns[0]
        asset_name = _asset_name(key)
        route_features: list[dict[str, Any]] = []
        bounds = [180.0, 90.0, -180.0, -90.0]
        directions: list[dict[str, Any]] = []
        endpoint_sequences: dict[str, set[int]] = {}
        for pattern in route_patterns:
            geom = pattern["geom"]
            minx, miny, maxx, maxy = geom.bounds
            bounds = [
                min(bounds[0], minx),
                min(bounds[1], miny),
                max(bounds[2], maxx),
                max(bounds[3], maxy),
            ]
            direction_label = pattern.get("headsign") or (
                f"{pattern.get('origin_name') or '?'} → {pattern.get('destination_name') or '?'}"
            )
            directions.append(
                {
                    "pattern_id": pattern["pattern_id"],
                    "direction_id": pattern.get("direction_id"),
                    "label": direction_label,
                    "origin_name": pattern.get("origin_name"),
                    "destination_name": pattern.get("destination_name"),
                }
            )
            pattern_stops = stops_by_pattern.get(str(pattern["pattern_id"]), [])
            if pattern_stops:
                sequences = [int(row["stop_sequence"]) for row in pattern_stops]
                endpoint_sequences[str(pattern["pattern_id"])] = {min(sequences), max(sequences)}
            route_features.append(
                _feature(
                    geom,
                    feature_kind="pattern",
                    route_key=key,
                    pattern_id=pattern["pattern_id"],
                    mode=pattern["mode"],
                    route_color=pattern["route_color"],
                    direction_id=pattern.get("direction_id"),
                    headsign=pattern.get("headsign") or "",
                    origin_name=pattern.get("origin_name") or "",
                    destination_name=pattern.get("destination_name") or "",
                    geometry_source=pattern["geometry_source"],
                    is_approximate=bool(pattern["is_approximate"]),
                )
            )
        seen_stops: set[tuple[str, str, str]] = set()
        for stop in stops_by_route.get(key, []):
            identity = (str(stop["pattern_id"]), str(stop["feed_id"]), str(stop["stop_id"]))
            if identity in seen_stops:
                continue
            seen_stops.add(identity)
            route_features.append(
                _feature(
                    stop["geom"],
                    feature_kind="stop",
                    route_key=key,
                    pattern_id=stop["pattern_id"],
                    feed_id=stop["feed_id"],
                    stop_id=stop["stop_id"],
                    stop_sequence=int(stop["stop_sequence"]),
                    stop_name=stop.get("stop_name") or "",
                    mode=first["mode"],
                    is_endpoint=int(stop["stop_sequence"]) in endpoint_sequences.get(str(stop["pattern_id"]), set()),
                )
            )
        for hub in hubs_by_route.get(key, []):
            route_features.append(
                _feature(
                    hub["geom"],
                    feature_kind="transfer",
                    route_key=key,
                    hub_id=hub["hub_id"],
                    hub_name=hub.get("hub_name") or "",
                    route_keys=list(hub.get("route_keys_json") or []),
                    route_count=int(hub.get("route_count") or 0),
                    modes=",".join(str(value) for value in (hub.get("modes_json") or [])),
                )
            )

        _atomic_json(
            target_dir / asset_name,
            {"type": "FeatureCollection", "features": route_features},
        )
        index_routes.append(
            {
                "route_key": key,
                "route_id": first["route_id"],
                "feed_id": first["feed_id"],
                "route_short_name": first.get("route_short_name"),
                "route_long_name": first.get("route_long_name"),
                "mode": first["mode"],
                "route_color": first["route_color"],
                "route_text_color": first["route_text_color"],
                "visibility_class": min(
                    (row["visibility_class"] for row in route_patterns),
                    key={"backbone": 0, "regional": 1, "local": 2}.get,
                ),
                "bounds": bounds,
                "directions": directions,
                "asset_url": f"/transit/routes/{build_key}/{asset_name}",
            }
        )

    counts: dict[str, int] = defaultdict(int)
    counts["total"] = len(index_routes)
    for route in index_routes:
        counts[str(route["visibility_class"])] += 1
        counts[str(route["mode"])] += 1
    index = {
        "version": ROUTE_ASSETS_VERSION,
        "build_key": build_key,
        "reality_fingerprint": reality_fingerprint,
        "route_count": len(index_routes),
        "counts": dict(sorted(counts.items())),
        "routes": index_routes,
    }
    _atomic_json(index_path, index)
    return index


__all__ = [
    "ROUTE_ASSETS_DIR",
    "ROUTE_ASSETS_VERSION",
    "publish_route_assets",
    "route_assets_dir",
]
