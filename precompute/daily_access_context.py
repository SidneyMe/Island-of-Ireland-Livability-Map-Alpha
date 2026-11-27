"""Versioned v4 context fields. All geometry and distances are EPSG:2157.

The field is independent of routing mode. Walking-dependent bonuses are
attached once to canonical walk cells, then reused by bike and mode-aware.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import shapely
from shapely.geometry import Point
from shapely.strtree import STRtree

from config import DAILY_ACCESS_CONTEXT_PARAMS as PARAMS
from config import DAILY_ACCESS_EXCLUDED_SHOP_VALUES

CONTEXT_PENALTIES = ("industrial_context", "over_concentration", "pitch_noise")
BONUSES = ("rail_station_access", "green_buffer")
METRICS = ("landuse_coverage", "residential_share", "industrial_share", "confidence",
           "nightlife_density", "retail_density", "green_area_share")


def smooth(value: float) -> float:
    t = min(max(float(value), 0.0), 1.0)
    return t * t * (3.0 - 2.0 * t)


def station_bonus(mode: str, distance: float, quality: float, corridor_distance: float) -> float:
    start, full, plateau, end, maximum = PARAMS["station"][mode]
    curve = smooth((distance - start) / (full - start))
    curve *= 1.0 - smooth((distance - plateau) / (end - plateau))
    near, far = PARAMS["corridor"]
    return maximum * quality * curve * smooth((corridor_distance - near) / (far - near))


def effects(metrics: dict[str, float], *, pitch: float = 0.0, walk_green: float = 0.0,
            station: float = 0.0) -> tuple[dict[str, float], dict[str, float]]:
    r, i, q = (metrics.get(key, 0.0) for key in ("residential_share", "industrial_share", "confidence"))
    low, high, maximum = PARAMS["industrial"]
    industrial = maximum * q * smooth((i - low) / (high - low))
    low, high, maximum = PARAMS["nightlife"]
    effective_r = q * r + (1.0 - q) * 0.5
    nightlife = maximum * smooth((metrics.get("nightlife_density", 0.0) - low) / (high - low)) * (0.5 + 0.5 * effective_r)
    low, high, maximum = PARAMS["retail"]
    retail = maximum * q * smooth((metrics.get("retail_density", 0.0) - low) / (high - low)) * (1.0 - r)
    penalties = dict(zip(CONTEXT_PENALTIES, (industrial, nightlife + retail, max(pitch, 0.0))))
    total = sum(penalties.values())
    if total > PARAMS["penalty_cap"]:
        penalties = {key: value * PARAMS["penalty_cap"] / total for key, value in penalties.items()}
    low, high, green_share, maximum = PARAMS["residential_green"]
    green = maximum * q * smooth((r - low) / (high - low))
    green *= smooth(metrics.get("green_area_share", 0.0) / green_share) * min(max(walk_green, 0.0), 1.0)
    return penalties, dict(zip(BONUSES, (max(station, 0.0), green)))


def operational(tags: dict[str, Any]) -> bool:
    if any(key.startswith(("disused:", "abandoned:", "construction:", "proposed:"))
           and str(value).lower() not in ("", "no", "false", "0") for key, value in tags.items()):
        return False
    return not any(str(tags.get(key, "")).lower() not in ("", "no", "false", "0")
                   for key in ("disused", "abandoned", "construction", "proposed"))


def valid_geometry(geometry):
    if geometry is None or geometry.is_empty:
        return None
    geometry = shapely.make_valid(geometry)
    return None if geometry.is_empty else geometry


class GeometryIndex:
    def __init__(self, rows):
        self.rows = []
        self.geometries = []
        for row in rows:
            geom = valid_geometry(row.get("geom"))
            if geom is not None:
                self.rows.append(row)
                self.geometries.append(geom)
        self.tree = STRtree(self.geometries)

    def intersect(self, geometry):
        for index in self.tree.query(geometry, predicate="intersects"):
            yield self.rows[int(index)], self.geometries[int(index)]


class ContextField:
    def __init__(self, *, land, landuse=(), nuisance=(), green=()):
        self.land = valid_geometry(land)
        if self.land is None:
            raise ValueError("ContextField requires a non-empty land mask")
        # Most canonical cells are well inland. Preparing the national mask
        # lets ``window`` prove that their small circles are fully covered and
        # skip an expensive overlay against the full coastline geometry.
        shapely.prepare(self.land)
        self.landuse = GeometryIndex(landuse)
        self.green = GeometryIndex(green)
        venues, pitches = [], []
        # Reuse v4 evidence-based, non-chaining grouping. Every nuisance role
        # uses the same grouping kind; distinct shops in one building survive.
        from .daily_access_classify import build_logical_places
        candidates = []
        for row in nuisance:
            tags = row.get("tags_json") or {}
            if not operational(tags):
                continue
            if tags.get("leisure") == "pitch":
                pitches.append(row)
                continue
            amenity, shop = tags.get("amenity"), tags.get("shop")
            n = 3.0 if amenity == "nightclub" else 1.0 if amenity in ("pub", "bar") else 0.0
            t = 2.0 if amenity == "fast_food" else 1.0 if shop and shop not in DAILY_ACCESS_EXCLUDED_SHOP_VALUES else 0.0
            if not (n or t):
                continue
            geom = valid_geometry(row.get("geom"))
            if geom is None:
                continue
            point = geom.representative_point()
            candidates.append({**row, "service_kind": "other_retail", "need": "other_retail", "quality": 1.0,
                               "x": point.x, "y": point.y, "_n": n, "_t": t,
                               "footprint_wkb": shapely.to_wkb(geom) if geom.area else None})
        for place in build_logical_places(candidates):
            member = max(place.members, key=lambda row: (row["_n"], row["_t"], str(row.get("source_ref"))))
            # One logical venue, one strongest role across the two densities.
            n, t = max((row["_n"] for row in place.members)), max((row["_t"] for row in place.members))
            if n >= t:
                t = 0.0
            else:
                n = 0.0
            venues.append({**member, "geom": Point(member["x"], member["y"]), "n": n, "t": t})
        self.venues = GeometryIndex(venues)
        qualifying = []
        for row in pitches:
            geom = valid_geometry(row.get("geom"))
            if geom is None:
                continue
            lit = str((row.get("tags_json") or {}).get("lit", "")).lower() == "yes"
            metrics = self.land_metrics(geom.representative_point())
            dense = metrics["confidence"] >= 1.0 - 1e-12 and metrics["residential_share"] >= PARAMS["pitch"][4]
            if lit or dense:
                qualifying.append({**row, "maximum": PARAMS["pitch"][2 if lit else 3],
                                   "reason": "lit=yes" if lit else "dense_residential"})
        self.pitches = GeometryIndex(qualifying)

    def records(self, records, *, progress=None):
        """Bounded per-block inputs to Rust: three penalties, green coefficient, corridor distance."""
        values = np.zeros((len(records), 5), dtype=np.float32)
        metrics = np.zeros((len(records), len(METRICS)), dtype=np.float32)
        pitch_ref = np.full(len(records), "", dtype="U32")
        pitch_reason = np.full(len(records), "", dtype="U24")
        pitch_distance = np.full(len(records), np.nan, dtype=np.float32)
        valid_total = int(np.count_nonzero(records["flags"] & 1))
        progress_step = max(1_000, valid_total // 20)
        processed = 0
        if progress is not None:
            progress(f"[daily-access context] 0/{valid_total:,} valid cells")
        for index, record in enumerate(records):
            if not (record["flags"] & 1):
                continue
            metric, pitch, details = self.at(record["x"], record["y"])
            penalties, bonuses = effects(metric, pitch=pitch, walk_green=1.0)
            values[index, :3] = [penalties[name] for name in CONTEXT_PENALTIES]
            values[index, 3] = bonuses["green_buffer"]
            metrics[index] = [metric[name] for name in METRICS]
            if details:
                pitch_ref[index] = details["source_ref"] or ""
                pitch_reason[index] = details["reason"]
                pitch_distance[index] = details["distance_m"]
            processed += 1
            if progress is not None and (processed % progress_step == 0 or processed == valid_total):
                progress(f"[daily-access context] {processed:,}/{valid_total:,} valid cells")
        values[:, 4] = self.corridors.distances(records["x"], records["y"])
        return {"context_values": values, "context_metrics": metrics, "pitch_ref": pitch_ref,
                "pitch_reason": pitch_reason, "pitch_distance": pitch_distance}

    def window(self, point, radius):
        circle = point.buffer(radius)
        if shapely.covers(self.land, circle):
            return circle
        return circle.intersection(self.land)

    def land_metrics(self, point, *, window=None):
        if window is None:
            window = self.window(point, PARAMS["radius_m"])
        area = window.area
        shares = {}
        candidates = list(self.landuse.intersect(window))
        if not candidates:
            return {"landuse_coverage": 0.0, "residential_share": 0.0,
                    "industrial_share": 0.0, "confidence": 0.0}
        by_kind = {}
        for row, geometry in candidates:
            by_kind.setdefault(row["landuse_class"], []).append(geometry)
        occupied = None
        for kind in PARAMS["landuse_priority"]:
            geometries = by_kind.get(kind)
            if not geometries:
                shares[kind] = 0.0
                continue
            union = shapely.union_all([geometry.intersection(window) for geometry in geometries])
            if occupied is not None:
                union = union.difference(occupied)
            shares[kind] = union.area / area if area else 0.0
            occupied = union if occupied is None else occupied.union(union)
        coverage = min(sum(shares.values()), 1.0)
        low, high = PARAMS["coverage"]
        return {"landuse_coverage": coverage, "residential_share": shares.get("residential", 0.0),
                "industrial_share": shares.get("industrial", 0.0), "confidence": smooth((coverage - low) / (high - low))}

    def at(self, x, y):
        point = Point(float(x), float(y))
        window = self.window(point, PARAMS["radius_m"])
        metrics = self.land_metrics(point, window=window)
        area_km2 = window.area / 1_000_000.0
        venues = [row for row, _ in self.venues.intersect(window)]
        metrics["nightlife_density"] = sum(row["n"] for row in venues) / area_km2 if area_km2 else 0.0
        metrics["retail_density"] = sum(row["t"] for row in venues) / area_km2 if area_km2 else 0.0
        window = self.window(point, PARAMS["green_radius_m"])
        union = shapely.union_all([geom.intersection(window) for _, geom in self.green.intersect(window)])
        metrics["green_area_share"] = min(union.area / window.area, 1.0) if window.area else 0.0
        full, zero = PARAMS["pitch"][:2]
        best, details = 0.0, None
        for row, geom in self.pitches.intersect(point.buffer(zero)):
            distance = geom.distance(point)
            value = row["maximum"] * (1.0 - smooth((distance - full) / (zero - full)))
            if value > best:
                best = value
                details = {"source_ref": row.get("source_ref"), "distance_m": distance, "reason": row["reason"]}
        return metrics, best, details


def load_context_field(context):
    from config import TO_TARGET, TO_WGS84
    from shapely.ops import transform
    from db_postgis.reads import load_landuse_context_rows, load_nuisance_rows, load_railway_corridor_rows
    from db_postgis.daily_access_green import stage_green_sites
    from transit.railway_corridors import RailwayPenaltyField
    area = transform(TO_WGS84, context.study_area_metric.envelope.buffer(600.0))
    landuse = load_landuse_context_rows(context.engine, context.source_state.import_fingerprint, area)
    for row in landuse:
        row["geom"] = transform(TO_TARGET, row["geom"])
    nuisance = load_nuisance_rows(context.engine, context.source_state.import_fingerprint, area)
    with context.engine.begin() as connection:
        sites, _ = stage_green_sites(connection, import_fingerprint=context.source_state.import_fingerprint,
                                    study_area_wgs84=area, buffer_m=0.0)
    green = [{"geom": shapely.from_wkb(site.geom_wkb)} for site in sites if site.is_area and site.geom_wkb]
    field = ContextField(land=context.land_mask.geometry, landuse=landuse, nuisance=nuisance, green=green)
    corridors = (load_railway_corridor_rows(context.engine, context.reality_fingerprint)
                 if context.reality_fingerprint != "transit-unavailable" else [])
    field.corridors = RailwayPenaltyField(corridors)
    field.corridor_feeds = {str(row["feed_id"]) for row in corridors if row.get("geom") is not None}
    return field
