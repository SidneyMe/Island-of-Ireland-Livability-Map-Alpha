from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Iterator

from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.ops import unary_union

from .extract import _emit_progress
from .signature import NOISE_DATA_DIR


def _make_valid(geom):
    if geom is None or geom.is_empty:
        return None
    if geom.is_valid:
        return geom
    from shapely import make_valid

    repaired = make_valid(geom)
    if repaired is None or repaired.is_empty:
        return None
    return repaired


def _polygon_parts(geom) -> Iterator[Polygon]:
    if geom is None or geom.is_empty:
        return
    if isinstance(geom, Polygon):
        yield geom
        return
    if isinstance(geom, MultiPolygon):
        yield from geom.geoms
        return
    if isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            yield from _polygon_parts(part)


def _clip_to_study_area(geom, study_area_wgs84):
    repaired = _make_valid(geom)
    if repaired is None:
        return None
    clipped = repaired.intersection(study_area_wgs84)
    if clipped.is_empty:
        return None
    return _make_valid(clipped)


def materialize_effective_noise_rows(
    candidate_rows: Iterable[dict[str, Any]],
    study_area_wgs84,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in candidate_rows:
        key = (
            str(row.get("jurisdiction") or ""),
            str(row.get("source_type") or ""),
            str(row.get("metric") or ""),
        )
        if not all(key):
            continue
        grouped.setdefault(key, []).append(row)

    output: list[dict[str, Any]] = []
    for rows in grouped.values():
        covered = None
        round_numbers = sorted({int(row["round_number"]) for row in rows}, reverse=True)
        for round_number in round_numbers:
            round_effective_geoms = []
            for row in [item for item in rows if int(item["round_number"]) == round_number]:
                geom = _clip_to_study_area(row.get("geom"), study_area_wgs84)
                if geom is None:
                    continue
                if covered is not None and not covered.is_empty:
                    if covered.covers(geom):
                        continue
                    if covered.intersects(geom):
                        geom = _make_valid(geom.difference(covered))
                        if geom is None or geom.is_empty:
                            continue
                for part in _polygon_parts(geom):
                    if part.is_empty or part.area <= 0:
                        continue
                    payload = dict(row)
                    payload["geom"] = part
                    output.append(payload)
                    round_effective_geoms.append(part)
            if round_effective_geoms:
                round_coverage = unary_union(round_effective_geoms)
                covered = round_coverage if covered is None else unary_union([covered, round_coverage])
    return output


def load_noise_rows(study_area_wgs84, *, data_dir: Path = NOISE_DATA_DIR, progress_cb=None) -> list[dict[str, Any]]:
    # Import here to avoid a circular dependency; loader.py is the normal entry point.
    from .loader import iter_noise_candidate_rows

    candidates = list(
        iter_noise_candidate_rows(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
        )
    )
    if not candidates:
        return []
    _emit_progress(progress_cb, f"materializing newest-round noise fallback for {len(candidates):,} polygons")
    return materialize_effective_noise_rows(candidates, study_area_wgs84)
