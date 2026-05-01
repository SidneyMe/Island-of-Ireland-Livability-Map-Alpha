from __future__ import annotations

import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .ogr_ingest import _selected_ni_member_specs, _selected_roi_specs


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _progress_event(progress_cb, *, phase: str, source: str, rows: int, elapsed: float) -> None:
    _progress(
        progress_cb,
        f"[noise:progress] phase={phase} source={source} rows={rows} elapsed={elapsed:.1f}s",
    )


def _metric_from_time(value: Any) -> str | None:
    from noise.loader import _metric_from_value

    metric = _metric_from_value(value)
    if metric not in {"Lden", "Lnight"}:
        return None
    return str(metric)


def _lookup_ci(row: Any, *names: str) -> Any:
    wanted = {name.strip().lower() for name in names}
    for key in row.index:
        if str(key).strip().lower() in wanted:
            return row.get(key)
    return None


def _normalize_roi_band(row: Any) -> tuple[float | None, float | None, str | None]:
    from noise.loader import normalize_noise_band

    raw_value = _lookup_ci(row, "DbValue", "dB_Value", "DBVALUE", "DB_VALUE")
    db_low = _lookup_ci(row, "Db_Low", "dB_Low", "DB_LOW")
    db_high = _lookup_ci(row, "Db_High", "dB_High", "DB_HIGH")
    try:
        low, high, db_value = normalize_noise_band(raw_value, db_low=db_low, db_high=db_high)
    except Exception:
        return None, None, None
    return low, high, db_value


def _normalize_ni_band(
    *,
    round_number: int,
    source_type: str,
    metric: str,
    row: Any,
) -> tuple[float | None, float | None, str | None]:
    from noise.loader import normalize_ni_gridcode_band

    raw_gridcode = _lookup_ci(row, "gridcode", "GRIDCODE", "GridCode")
    if raw_gridcode is None:
        return None, None, None
    try:
        low, high, db_value = normalize_ni_gridcode_band(
            raw_gridcode,
            round_number=int(round_number),
            source_type=source_type,
            metric=metric,
        )
    except Exception:
        return None, None, None
    return low, high, db_value


def _band_rank(db_low: float | None, db_high: float | None, db_value: str | None) -> float:
    if db_high is not None:
        return float(db_high)
    if db_low is not None:
        return float(db_low)
    raw = str(db_value or "").strip()
    if raw.endswith("+"):
        try:
            return float(raw[:-1])
        except ValueError:
            return -1.0
    if "-" in raw:
        head = raw.split("-", 1)[0]
        try:
            return float(head)
        except ValueError:
            return -1.0
    return -1.0


@dataclass
class _GridRow:
    jurisdiction: str
    source_type: str
    metric: str
    round_number: int
    report_period: str
    db_low: float | None
    db_high: float | None
    db_value: str
    rank: float


def _add_geom_cells(
    accumulator: dict[tuple[str, str, str, int, int], _GridRow],
    *,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
    report_period: str,
    db_low: float | None,
    db_high: float | None,
    db_value: str,
    geom,
    grid_size_m: int,
) -> int:
    from shapely.geometry import box

    if geom is None or geom.is_empty:
        return 0
    if not geom.is_valid:
        try:
            from shapely.validation import make_valid
            geom = make_valid(geom)
        except Exception:
            geom = geom.buffer(0)
    if geom is None or geom.is_empty:
        return 0
    minx, miny, maxx, maxy = geom.bounds
    if not math.isfinite(minx) or not math.isfinite(miny) or not math.isfinite(maxx) or not math.isfinite(maxy):
        return 0
    if maxx <= minx or maxy <= miny:
        return 0
    min_cell_x = int(math.floor(minx / float(grid_size_m)))
    max_cell_x = int(math.floor((maxx - 1e-9) / float(grid_size_m)))
    min_cell_y = int(math.floor(miny / float(grid_size_m)))
    max_cell_y = int(math.floor((maxy - 1e-9) / float(grid_size_m)))
    if max_cell_x < min_cell_x or max_cell_y < min_cell_y:
        return 0
    rank = _band_rank(db_low, db_high, db_value)
    touched = 0
    for cell_x in range(min_cell_x, max_cell_x + 1):
        cell_minx = cell_x * grid_size_m
        cell_maxx = cell_minx + grid_size_m
        for cell_y in range(min_cell_y, max_cell_y + 1):
            cell_miny = cell_y * grid_size_m
            cell_maxy = cell_miny + grid_size_m
            cell_poly = box(cell_minx, cell_miny, cell_maxx, cell_maxy)
            if not geom.intersects(cell_poly):
                continue
            key = (jurisdiction, source_type, metric, cell_x, cell_y)
            existing = accumulator.get(key)
            if existing is None or rank >= existing.rank:
                accumulator[key] = _GridRow(
                    jurisdiction=jurisdiction,
                    source_type=source_type,
                    metric=metric,
                    round_number=int(round_number),
                    report_period=str(report_period),
                    db_low=db_low,
                    db_high=db_high,
                    db_value=str(db_value),
                    rank=float(rank),
                )
            touched += 1
    return touched


def build_dev_fast_road_rail_grid(
    engine: Engine,
    *,
    data_dir: Path,
    noise_source_hash: str,
    artifact_hash: str,
    grid_size_m: int,
    progress_cb=None,
) -> dict[str, int]:
    import pyogrio
    from noise.loader import ROI_SOURCE_SPECS

    started = time.perf_counter()
    road_rail = {"road", "rail"}
    selected_roi_specs, _, _ = _selected_roi_specs(
        list(ROI_SOURCE_SPECS),
        source_types=road_rail,
        latest_round_only=True,
    )
    selected_ni_specs, _, _ = _selected_ni_member_specs(
        data_dir=Path(data_dir),
        source_types=road_rail,
        latest_round_only=True,
    )
    acc: dict[tuple[str, str, str, int, int], _GridRow] = {}
    source_rows = 0

    from .ogr_ingest import extract_source_archive_if_needed

    for spec in selected_roi_specs:
        zip_path = Path(data_dir) / spec.zip_name
        extracted = extract_source_archive_if_needed(zip_path)
        source_path = extracted / spec.member
        if not source_path.exists():
            continue
        layer_name = source_path.stem if str(spec.file_format).lower() == "gdb" else None
        frame = pyogrio.read_dataframe(str(source_path), layer=layer_name)
        if frame.empty:
            continue
        if frame.crs is not None and str(frame.crs).upper() != "EPSG:2157":
            frame = frame.to_crs(2157)
        row_touched = 0
        for _, row in frame.iterrows():
            metric = _metric_from_time(_lookup_ci(row, "Time", "TIME"))
            if metric is None:
                continue
            db_low, db_high, db_value = _normalize_roi_band(row)
            if db_value is None:
                continue
            geom = row.geometry
            row_touched += _add_geom_cells(
                acc,
                jurisdiction="roi",
                source_type=str(spec.source_type),
                metric=metric,
                round_number=int(spec.round_number),
                report_period=f"Round {int(spec.round_number)}",
                db_low=db_low,
                db_high=db_high,
                db_value=str(db_value),
                geom=geom,
                grid_size_m=int(grid_size_m),
            )
            source_rows += 1
        _progress_event(
            progress_cb,
            phase="dev_fast_grid_roi",
            source=f"{spec.zip_name}:{spec.member}",
            rows=row_touched,
            elapsed=max(time.perf_counter() - started, 0.0),
        )

    for spec in selected_ni_specs:
        source_path = Path(spec["source_path"])
        frame = pyogrio.read_dataframe(str(source_path))
        if frame.empty:
            continue
        if frame.crs is not None and str(frame.crs).upper() != "EPSG:2157":
            frame = frame.to_crs(2157)
        row_touched = 0
        for _, row in frame.iterrows():
            db_low, db_high, db_value = _normalize_ni_band(
                round_number=int(spec["round_number"]),
                source_type=str(spec["source_type"]),
                metric=str(spec["metric"]),
                row=row,
            )
            if db_value is None:
                continue
            geom = row.geometry
            row_touched += _add_geom_cells(
                acc,
                jurisdiction="ni",
                source_type=str(spec["source_type"]),
                metric=str(spec["metric"]),
                round_number=int(spec["round_number"]),
                report_period=f"Round {int(spec['round_number'])}",
                db_low=db_low,
                db_high=db_high,
                db_value=str(db_value),
                geom=geom,
                grid_size_m=int(grid_size_m),
            )
            source_rows += 1
        _progress_event(
            progress_cb,
            phase="dev_fast_grid_ni",
            source=f"{spec['zip_name']}:{spec['member']}",
            rows=row_touched,
            elapsed=max(time.perf_counter() - started, 0.0),
        )

    rows = []
    for (jurisdiction, source_type, metric, cell_x, cell_y), rec in acc.items():
        minx = int(cell_x) * int(grid_size_m)
        miny = int(cell_y) * int(grid_size_m)
        maxx = minx + int(grid_size_m)
        maxy = miny + int(grid_size_m)
        rows.append(
            {
                "artifact_hash": artifact_hash,
                "noise_source_hash": noise_source_hash,
                "jurisdiction": jurisdiction,
                "source_type": source_type,
                "metric": metric,
                "grid_size_m": int(grid_size_m),
                "cell_x": int(cell_x),
                "cell_y": int(cell_y),
                "round_number": int(rec.round_number),
                "report_period": rec.report_period,
                "db_low": rec.db_low,
                "db_high": rec.db_high,
                "db_value": rec.db_value,
                "minx": float(minx),
                "miny": float(miny),
                "maxx": float(maxx),
                "maxy": float(maxy),
            }
        )

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM noise_grid_artifact WHERE artifact_hash = :h"),
            {"h": artifact_hash},
        )
        if rows:
            conn.execute(
                text(
                    """
                    INSERT INTO noise_grid_artifact (
                        artifact_hash,
                        noise_source_hash,
                        jurisdiction,
                        source_type,
                        metric,
                        grid_size_m,
                        cell_x,
                        cell_y,
                        round_number,
                        report_period,
                        db_low,
                        db_high,
                        db_value,
                        geom
                    )
                    VALUES (
                        :artifact_hash,
                        :noise_source_hash,
                        :jurisdiction,
                        :source_type,
                        :metric,
                        :grid_size_m,
                        :cell_x,
                        :cell_y,
                        :round_number,
                        :report_period,
                        :db_low,
                        :db_high,
                        :db_value,
                        ST_Multi(
                            ST_SetSRID(
                                ST_MakeEnvelope(:minx, :miny, :maxx, :maxy),
                                2157
                            )
                        )
                    )
                    """
                ),
                rows,
            )
    _progress(
        progress_cb,
        f"road/rail grid build complete: grid_size={grid_size_m}m source_rows={source_rows:,} cells={len(rows):,}",
    )
    return {"source_rows": int(source_rows), "cell_rows": int(len(rows))}


def materialize_dev_fast_resolved(
    engine: Engine,
    *,
    noise_source_hash: str,
    noise_resolved_hash: str,
    grid_size_m: int,
    progress_cb=None,
) -> int:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM noise_resolved_display WHERE noise_resolved_hash = :h"),
            {"h": noise_resolved_hash},
        )
        conn.execute(
            text("DELETE FROM noise_resolved_provenance WHERE noise_resolved_hash = :h"),
            {"h": noise_resolved_hash},
        )
        # Exact categories remain polygon-based in dev-fast.
        conn.execute(
            text(
                """
                INSERT INTO noise_resolved_display (
                    noise_resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    report_period,
                    db_low,
                    db_high,
                    db_value,
                    geom
                )
                SELECT
                    :resolved_hash,
                    n.jurisdiction,
                    n.source_type,
                    n.metric,
                    n.round_number,
                    n.report_period,
                    n.db_low,
                    n.db_high,
                    n.db_value,
                    n.geom
                FROM noise_normalized n
                WHERE n.noise_source_hash = :source_hash
                  AND n.source_type IN ('airport', 'industry')
                """
            ),
            {"resolved_hash": noise_resolved_hash, "source_hash": noise_source_hash},
        )
        # Road/rail source-of-truth in dev-fast is the coarse grid artifact only.
        conn.execute(
            text(
                """
                INSERT INTO noise_resolved_display (
                    noise_resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    report_period,
                    db_low,
                    db_high,
                    db_value,
                    geom
                )
                SELECT
                    g.artifact_hash,
                    g.jurisdiction,
                    g.source_type,
                    g.metric,
                    g.round_number,
                    g.report_period,
                    g.db_low,
                    g.db_high,
                    g.db_value,
                    g.geom
                FROM noise_grid_artifact g
                WHERE g.artifact_hash = :resolved_hash
                  AND g.grid_size_m = :grid_size_m
                  AND g.source_type IN ('road', 'rail')
                """
            ),
            {"resolved_hash": noise_resolved_hash, "grid_size_m": int(grid_size_m)},
        )
        exact_prov = conn.execute(
            text(
                """
                INSERT INTO noise_resolved_provenance (
                    noise_resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    source_dataset,
                    source_layer,
                    source_ref_count,
                    source_refs_hash
                )
                SELECT
                    :resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    source_dataset,
                    source_layer,
                    COUNT(*) AS source_ref_count,
                    encode(
                        sha256(string_agg(COALESCE(source_ref, '') ORDER BY COALESCE(source_ref, ''))::bytea),
                        'hex'
                    ) AS source_refs_hash
                FROM noise_normalized
                WHERE noise_source_hash = :source_hash
                  AND source_type IN ('airport', 'industry')
                GROUP BY jurisdiction, source_type, metric, round_number, source_dataset, source_layer
                """
            ),
            {"resolved_hash": noise_resolved_hash, "source_hash": noise_source_hash},
        )
        grid_prov = conn.execute(
            text(
                """
                INSERT INTO noise_resolved_provenance (
                    noise_resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    source_dataset,
                    source_layer,
                    source_ref_count,
                    source_refs_hash
                )
                SELECT
                    :resolved_hash,
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    'noise_grid_artifact' AS source_dataset,
                    :layer_name AS source_layer,
                    COUNT(*) AS source_ref_count,
                    encode(
                        sha256(
                            string_agg((cell_x::text || ':' || cell_y::text) ORDER BY cell_x, cell_y)::bytea
                        ),
                        'hex'
                    ) AS source_refs_hash
                FROM noise_grid_artifact
                WHERE artifact_hash = :resolved_hash
                  AND grid_size_m = :grid_size_m
                  AND source_type IN ('road', 'rail')
                GROUP BY jurisdiction, source_type, metric, round_number
                """
            ),
            {
                "resolved_hash": noise_resolved_hash,
                "grid_size_m": int(grid_size_m),
                "layer_name": f"grid_{int(grid_size_m)}m",
            },
        )
        del exact_prov, grid_prov
        row_count = conn.execute(
            text("SELECT COUNT(*) FROM noise_resolved_display WHERE noise_resolved_hash = :h"),
            {"h": noise_resolved_hash},
        ).scalar_one_or_none()
    total = int(row_count or 0)
    _progress(
        progress_cb,
        f"dev-fast resolved materialization complete: resolved_hash={noise_resolved_hash} rows={total:,}",
    )
    return total


def apply_accurate_simplification(
    engine: Engine,
    *,
    noise_source_hash: str,
    simplify_tolerance_m: float,
    progress_cb=None,
) -> int:
    with engine.begin() as conn:
        updated = conn.execute(
            text(
                """
                UPDATE noise_normalized
                SET geom = ST_Multi(
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_SimplifyPreserveTopology(geom, :tol)
                        ),
                        3
                    )
                )
                WHERE noise_source_hash = :h
                  AND source_type IN ('road', 'rail')
                """
            ),
            {"h": noise_source_hash, "tol": float(simplify_tolerance_m)},
        )
        conn.execute(
            text(
                """
                DELETE FROM noise_normalized
                WHERE noise_source_hash = :h
                  AND source_type IN ('road', 'rail')
                  AND (geom IS NULL OR ST_IsEmpty(geom) OR ST_Area(geom) <= 0)
                """
            ),
            {"h": noise_source_hash},
        )
    updated_rows = int(updated.rowcount or 0)
    _progress(
        progress_cb,
        f"accurate simplify applied: tolerance={simplify_tolerance_m:.2f}m updated_rows={updated_rows:,}",
    )
    return updated_rows
