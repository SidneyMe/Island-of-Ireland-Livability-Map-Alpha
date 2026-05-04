from __future__ import annotations

import math
import os
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError
from .ogr_ingest import (
    _available_ogr_fields,
    _ensure_road_gdb_canonical_cache,
    _noise_ogr_candidate_fields,
    _selected_ni_member_specs,
    _selected_roi_specs,
    _select_existing_noise_fields,
)


DEV_FAST_GRID_ALGO_VERSION = 1


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
    if hasattr(row, "_asdict"):
        data = row._asdict()
        for key, value in data.items():
            if str(key).strip().lower() in wanted:
                return value
        return None
    if hasattr(row, "index") and hasattr(row, "get"):
        for key in row.index:
            if str(key).strip().lower() in wanted:
                return row.get(key)
        return None
    row_dict = dict(row) if isinstance(row, dict) else {}
    for key, value in row_dict.items():
        if str(key).strip().lower() in wanted:
            return value
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


def _dev_fast_read_chunk_size() -> int:
    raw = os.getenv("NOISE_DEV_FAST_READ_CHUNK_SIZE", "25")
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_READ_CHUNK_SIZE must be a positive integer, got {raw!r}"
        ) from exc
    if value <= 0:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_READ_CHUNK_SIZE must be a positive integer, got {raw!r}"
        )
    return value


def _dev_fast_min_grid_size_m() -> int:
    raw = os.getenv("NOISE_DEV_FAST_MIN_GRID_SIZE_M", "500")
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_MIN_GRID_SIZE_M must be a positive integer, got {raw!r}"
        ) from exc
    return max(1, value)


def _dev_fast_allow_tiny_grid() -> bool:
    return (os.getenv("NOISE_ALLOW_TINY_NOISE_GRID") or "").strip().lower() in {"1", "true", "yes", "on"}


def _validate_dev_fast_grid_size(grid_size_m: int) -> None:
    minimum = _dev_fast_min_grid_size_m()
    if int(grid_size_m) < minimum and not _dev_fast_allow_tiny_grid():
        raise NoiseIngestError(
            f"NOISE_GRID_SIZE_M={grid_size_m} is too small for dev_fast. "
            f"Minimum is {minimum}m unless NOISE_ALLOW_TINY_NOISE_GRID=1. "
            "Use --noise-accurate for high-fidelity road/rail instead."
        )


def _max_cells_per_feature() -> int:
    raw = os.getenv("NOISE_DEV_FAST_MAX_CELLS_PER_FEATURE", "250000")
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_MAX_CELLS_PER_FEATURE must be a positive integer, got {raw!r}"
        ) from exc
    return max(1, value)


def _env_flag(name: str, *, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _dev_fast_use_arrow() -> bool:
    return _env_flag("NOISE_DEV_FAST_USE_ARROW", default=True)


def _dev_fast_use_vsizip() -> bool:
    return _env_flag("NOISE_DEV_FAST_USE_VSIZIP", default=True)


def _dev_fast_parallel_specs() -> bool:
    return _env_flag("NOISE_DEV_FAST_PARALLEL_SPECS", default=True)


def _dev_fast_parallel_max_workers() -> int:
    raw = os.getenv("NOISE_DEV_FAST_MAX_WORKERS", "2")
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_MAX_WORKERS must be a positive integer, got {raw!r}"
        ) from exc
    if value <= 0:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_MAX_WORKERS must be a positive integer, got {raw!r}"
        )
    return value


def _rebuild_dev_fast_grid() -> bool:
    return _env_flag("NOISE_REBUILD_DEV_FAST_GRID", default=False)


def dev_fast_grid_artifact_hash(
    *,
    noise_source_hash: str,
    grid_size_m: int,
    latest_rounds_by_group: dict[str, int],
) -> str:
    payload = {
        "algorithm": "dev_fast_road_rail_grid",
        "algorithm_version": DEV_FAST_GRID_ALGO_VERSION,
        "grid_size_m": int(grid_size_m),
        "latest_rounds_by_group": {
            str(key): int(value)
            for key, value in sorted((latest_rounds_by_group or {}).items())
        },
        "noise_source_hash": str(noise_source_hash),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "noise-grid-" + hashlib.sha256(blob).hexdigest()


def _dev_fast_arrow_batch_size() -> int:
    raw = os.getenv("NOISE_DEV_FAST_ARROW_BATCH_SIZE", "16384")
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(
            f"NOISE_DEV_FAST_ARROW_BATCH_SIZE must be a positive integer, got {raw!r}"
        ) from exc
    return max(1, value)


def _vsizip_uri(zip_path: Path, member: str) -> str:
    return f"/vsizip/{Path(zip_path).resolve().as_posix()}/{str(member).replace(chr(92), '/')}"


def _read_feature_count(source_path: Path, layer_name: str | None) -> int | None:
    import pyogrio

    try:
        info = pyogrio.read_info(
            str(source_path),
            layer=layer_name,
            force_feature_count=True,
        )
    except TypeError:
        info = pyogrio.read_info(str(source_path), layer=layer_name)
    raw = info.get("features")
    if raw is None:
        raw = info.get("feature_count")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def _available_columns(source_path: Path, layer_name: str | None) -> list[str]:
    import pyogrio

    info = pyogrio.read_info(str(source_path), layer=layer_name)
    fields = info.get("fields") if isinstance(info, dict) else None
    if not isinstance(fields, list):
        return []
    return [str(field) for field in fields]


def _existing_requested_columns(
    source_path: Path,
    layer_name: str | None,
    requested: list[str],
) -> list[str] | None:
    available = _available_columns(source_path, layer_name)
    if not available:
        return None
    by_lower: dict[str, str] = {}
    for col in available:
        key = col.strip().lower()
        if key and key not in by_lower:
            by_lower[key] = col
    selected: list[str] = []
    seen: set[str] = set()
    for col in requested:
        key = str(col).strip().lower()
        resolved = by_lower.get(key)
        if resolved is None:
            continue
        if resolved in seen:
            continue
        selected.append(resolved)
        seen.add(resolved)
    return selected or None


def _iter_noise_gdf_chunks(
    *,
    source_path: Path,
    layer_name: str | None,
    columns: list[str] | None,
    target_crs: int = 2157,
    chunk_size: int,
    source_label: str,
    progress_cb=None,
):
    import pyogrio

    feature_count = _read_feature_count(source_path, layer_name)
    if feature_count is None:
        chunk_count = None
        offset = 0
        chunk_index = 1
        while True:
            started = time.perf_counter()
            frame = pyogrio.read_dataframe(
                str(source_path),
                layer=layer_name,
                columns=columns,
                skip_features=offset,
                max_features=chunk_size,
            )
            if frame.empty:
                break
            if frame.crs is not None and str(frame.crs).upper() != "EPSG:2157":
                frame = frame.to_crs(target_crs)
            _progress(
                progress_cb,
                f"[noise:progress] phase=dev_fast_read_chunk source={source_label} "
                f"chunk={chunk_index} offset={offset} rows={len(frame)} elapsed={time.perf_counter() - started:.1f}s",
            )
            yield frame, chunk_index, chunk_count
            offset += chunk_size
            chunk_index += 1
        return

    chunk_count = max(1, math.ceil(feature_count / chunk_size))
    for chunk_index, offset in enumerate(range(0, feature_count, chunk_size), start=1):
        started = time.perf_counter()
        frame = pyogrio.read_dataframe(
            str(source_path),
            layer=layer_name,
            columns=columns,
            skip_features=offset,
            max_features=chunk_size,
        )
        if frame.empty:
            continue
        if frame.crs is not None and str(frame.crs).upper() != "EPSG:2157":
            frame = frame.to_crs(target_crs)
        _progress(
            progress_cb,
            f"[noise:progress] phase=dev_fast_read_chunk source={source_label} "
            f"chunk={chunk_index}/{chunk_count} offset={offset} rows={len(frame)} "
            f"elapsed={time.perf_counter() - started:.1f}s",
        )
        yield frame, chunk_index, chunk_count


def _iter_noise_arrow_batches(
    *,
    source_path_or_uri,
    layer_name: str | None,
    columns: list[str] | None,
    target_crs: int = 2157,
    source_label: str,
    progress_cb=None,
):
    """Stream OGR features as GeoDataFrame batches via pyogrio.raw.open_arrow.

    Single sequential GDAL pass, bounded memory per batch. Replaces
    `_iter_noise_gdf_chunks` for the common case (skip_features=offset is
    O(N) per call on FileGDB and most other drivers, making the legacy
    chunked path O(N^2) over the source).
    """
    import geopandas as gpd
    import shapely
    from pyogrio.raw import open_arrow

    batch_size = _dev_fast_arrow_batch_size()
    started_total = time.perf_counter()

    with open_arrow(
        str(source_path_or_uri),
        layer=layer_name,
        columns=columns,
        use_pyarrow=True,
        batch_size=batch_size,
    ) as source:
        meta, reader = source
        geom_col = meta.get("geometry_name") or "wkb_geometry"
        crs = meta.get("crs")
        chunk_index = 0
        for batch in reader:
            if batch.num_rows == 0:
                continue
            chunk_index += 1
            chunk_started = time.perf_counter()
            df = batch.to_pandas()
            geoms = shapely.from_wkb(df[geom_col].to_numpy())
            if geom_col != "geometry":
                df = df.drop(columns=[geom_col])
            df["geometry"] = geoms
            frame = gpd.GeoDataFrame(df, geometry="geometry", crs=crs)
            if frame.crs is not None and str(frame.crs).upper() != f"EPSG:{int(target_crs)}":
                frame = frame.to_crs(target_crs)
            _progress(
                progress_cb,
                f"[noise:progress] phase=dev_fast_read_chunk source={source_label} "
                f"chunk={chunk_index} rows={len(frame)} "
                f"elapsed={time.perf_counter() - chunk_started:.1f}s",
            )
            yield frame, chunk_index, None
    _ = started_total  # silence unused warning when reader yields no batches


def _iter_noise_frames(
    *,
    source_path_or_uri,
    layer_name: str | None,
    columns: list[str] | None,
    target_crs: int = 2157,
    chunk_size: int,
    source_label: str,
    progress_cb=None,
):
    """Dispatch between the new Arrow-streaming path and the legacy offset-chunked path."""
    source_text = str(source_path_or_uri).lower()
    arrow_required = source_text.endswith(".gpkg") or ".gdb" in source_text
    if arrow_required and not _dev_fast_use_arrow():
        raise NoiseIngestError(
            "NOISE_DEV_FAST_USE_ARROW=0 is not allowed for FileGDB/GPKG dev-fast "
            f"road/rail sources because the fallback reader is O(N^2). source={source_label}"
        )
    if _dev_fast_use_arrow():
        try:
            yield from _iter_noise_arrow_batches(
                source_path_or_uri=source_path_or_uri,
                layer_name=layer_name,
                columns=columns,
                target_crs=target_crs,
                source_label=source_label,
                progress_cb=progress_cb,
            )
            return
        except Exception as exc:
            if arrow_required:
                raise NoiseIngestError(
                    "Arrow streaming failed for a FileGDB/GPKG dev-fast noise source; "
                    "legacy skip_features fallback is intentionally disabled because it is O(N^2). "
                    f"source={source_label} error={exc!r}"
                ) from exc
            _progress(
                progress_cb,
                f"[noise] arrow streaming failed for source={source_label}: {exc!r}; "
                "falling back to legacy offset-chunked read",
            )
    yield from _iter_noise_gdf_chunks(
        source_path=source_path_or_uri,  # str or Path; legacy reader str()s it
        layer_name=layer_name,
        columns=columns,
        target_crs=target_crs,
        chunk_size=chunk_size,
        source_label=source_label,
        progress_cb=progress_cb,
    )


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
    estimated_cells = (max_cell_x - min_cell_x + 1) * (max_cell_y - min_cell_y + 1)
    limit = _max_cells_per_feature()
    if estimated_cells > limit:
        raise NoiseIngestError(
            f"dev_fast grid explosion guard: one geometry would scan {estimated_cells:,} cells "
            f"at grid_size_m={grid_size_m}, limit={limit:,}. "
            "Increase NOISE_GRID_SIZE_M or use --noise-accurate."
        )
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


_ROI_REQUESTED_FIELDS = ["Time", "Db_Low", "Db_High", "DbValue", "ReportPeriod", "ReportPeri"]
_NI_REQUESTED_FIELDS = [
    "GRIDCODE",
    "GridCode",
    "gridcode",
    "NoiseBand",
    "noiseband",
    "Noise_Cl",
    "NOISE_CL",
    "noise_cl",
    "NoiseCl",
    "NOISECL",
]


def _merge_grid_acc(
    global_acc: dict[tuple[str, str, str, int, int], _GridRow],
    local_acc: dict[tuple[str, str, str, int, int], _GridRow],
) -> None:
    """Merge a per-spec accumulator into the global one, preserving the original
    tie-break rule (`>=` keeps the most-recent rank-tied record).

    Iteration order over `local_acc` is the per-spec insertion order, which
    matches what the serial code path observed when mutating the shared dict.
    """
    for key, rec in local_acc.items():
        existing = global_acc.get(key)
        if existing is None or rec.rank >= existing.rank:
            global_acc[key] = rec


def _process_roi_spec_grid(
    spec,
    *,
    data_dir: Path,
    grid_size_m: int,
    chunk_size: int,
    use_vsizip: bool,
    started: float,
    progress_cb=None,
) -> tuple[dict[tuple[str, str, str, int, int], _GridRow], int]:
    from .ogr_ingest import extract_source_archive_if_needed

    local_acc: dict[tuple[str, str, str, int, int], _GridRow] = {}
    local_rows = 0
    zip_path = Path(data_dir) / spec.zip_name
    file_format = str(spec.file_format).lower()
    columns: list[str] | None
    if file_format == "gdb":
        # FileGDB cannot be streamed efficiently via /vsizip/. Extract once,
        # then convert to a canonical GPKG (cached on disk), then stream that.
        extracted = extract_source_archive_if_needed(zip_path)
        gdb_path = extracted / spec.member
        if not gdb_path.exists():
            return local_acc, local_rows
        gdb_layer = gdb_path.stem
        available_fields = _available_ogr_fields(gdb_path, gdb_layer)
        candidate_fields = _noise_ogr_candidate_fields(
            jurisdiction="roi",
            source_type=str(spec.source_type),
            round_number=int(spec.round_number),
        )
        selected_fields = _select_existing_noise_fields(
            available_fields,
            candidate_fields,
            source_path=gdb_path,
            layer_name=gdb_layer,
        )
        _progress(
            progress_cb,
            f"dev-fast Road GDB selected fields for {gdb_layer}: {','.join(selected_fields)}",
        )
        canonical_path = _ensure_road_gdb_canonical_cache(
            source_path=gdb_path,
            layer_name=gdb_layer,
            selected_fields=selected_fields,
            progress_cb=progress_cb,
        )
        source_path_or_uri: Any = str(canonical_path)
        layer_name: str | None = "road_raw"
        columns = selected_fields or None
    elif use_vsizip:
        source_path_or_uri = _vsizip_uri(zip_path, spec.member)
        layer_name = None
        columns = _existing_requested_columns(
            source_path_or_uri, None, _ROI_REQUESTED_FIELDS
        )
    else:
        extracted = extract_source_archive_if_needed(zip_path)
        member_path = extracted / spec.member
        if not member_path.exists():
            return local_acc, local_rows
        source_path_or_uri = str(member_path)
        layer_name = None
        columns = _existing_requested_columns(member_path, None, _ROI_REQUESTED_FIELDS)

    source_label = f"{spec.zip_name}:{spec.member}"
    row_touched = 0
    for frame, chunk_index, chunk_count in _iter_noise_frames(
        source_path_or_uri=source_path_or_uri,
        layer_name=layer_name,
        columns=columns,
        chunk_size=chunk_size,
        source_label=source_label,
        progress_cb=progress_cb,
    ):
        chunk_touched = 0
        for row in frame.itertuples(index=False):
            metric = _metric_from_time(_lookup_ci(row, "Time", "TIME"))
            if metric is None:
                continue
            db_low, db_high, db_value = _normalize_roi_band(row)
            if db_value is None:
                continue
            geom = getattr(row, "geometry", None)
            chunk_touched += _add_geom_cells(
                local_acc,
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
            local_rows += 1
        row_touched += chunk_touched
        chunk_pos = (
            f"{chunk_index}/{chunk_count}" if chunk_count is not None else str(chunk_index)
        )
        _progress(
            progress_cb,
            f"[noise:progress] phase=dev_fast_grid_roi_chunk source={source_label} "
            f"chunk={chunk_pos} rows={len(frame)} touched={chunk_touched}",
        )
        del frame
    _progress_event(
        progress_cb,
        phase="dev_fast_grid_roi",
        source=source_label,
        rows=row_touched,
        elapsed=max(time.perf_counter() - started, 0.0),
    )
    return local_acc, local_rows


def _process_ni_spec_grid(
    spec: dict[str, Any],
    *,
    grid_size_m: int,
    chunk_size: int,
    started: float,
    progress_cb=None,
) -> tuple[dict[tuple[str, str, str, int, int], _GridRow], int]:
    local_acc: dict[tuple[str, str, str, int, int], _GridRow] = {}
    local_rows = 0
    source_path = Path(spec["source_path"])
    columns = _existing_requested_columns(source_path, None, _NI_REQUESTED_FIELDS)
    source_label = f"{spec['zip_name']}:{spec['member']}"
    row_touched = 0
    for frame, chunk_index, chunk_count in _iter_noise_frames(
        source_path_or_uri=str(source_path),
        layer_name=None,
        columns=columns,
        chunk_size=chunk_size,
        source_label=source_label,
        progress_cb=progress_cb,
    ):
        chunk_touched = 0
        for row in frame.itertuples(index=False):
            db_low, db_high, db_value = _normalize_ni_band(
                round_number=int(spec["round_number"]),
                source_type=str(spec["source_type"]),
                metric=str(spec["metric"]),
                row=row,
            )
            if db_value is None:
                continue
            geom = getattr(row, "geometry", None)
            chunk_touched += _add_geom_cells(
                local_acc,
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
            local_rows += 1
        row_touched += chunk_touched
        chunk_pos = (
            f"{chunk_index}/{chunk_count}" if chunk_count is not None else str(chunk_index)
        )
        _progress(
            progress_cb,
            f"[noise:progress] phase=dev_fast_grid_ni_chunk source={source_label} "
            f"chunk={chunk_pos} rows={len(frame)} touched={chunk_touched}",
        )
        del frame
    _progress_event(
        progress_cb,
        phase="dev_fast_grid_ni",
        source=source_label,
        rows=row_touched,
        elapsed=max(time.perf_counter() - started, 0.0),
    )
    return local_acc, local_rows


def _existing_dev_fast_grid_rows(
    engine: Engine,
    *,
    artifact_hash: str,
    grid_size_m: int,
) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM noise_grid_artifact
                WHERE artifact_hash = :artifact_hash
                  AND grid_size_m = :grid_size_m
                  AND source_type IN ('road', 'rail')
                """
            ),
            {"artifact_hash": artifact_hash, "grid_size_m": int(grid_size_m)},
        ).scalar_one_or_none()
    return int(value or 0)


def build_dev_fast_road_rail_grid(
    engine: Engine,
    *,
    data_dir: Path,
    noise_source_hash: str,
    artifact_hash: str,
    grid_size_m: int,
    progress_cb=None,
) -> dict[str, int]:
    from noise.loader import ROI_SOURCE_SPECS

    started = time.perf_counter()
    _validate_dev_fast_grid_size(int(grid_size_m))
    chunk_size = _dev_fast_read_chunk_size()
    if not _rebuild_dev_fast_grid():
        existing_rows = _existing_dev_fast_grid_rows(
            engine,
            artifact_hash=artifact_hash,
            grid_size_m=int(grid_size_m),
        )
        if existing_rows > 0:
            _progress(
                progress_cb,
                "road/rail grid cache hit: "
                f"artifact_hash={artifact_hash} grid_size={int(grid_size_m)}m "
                f"cells={existing_rows:,}",
            )
            return {"source_rows": 0, "cell_rows": existing_rows, "reused": 1}
    else:
        _progress(
            progress_cb,
            f"NOISE_REBUILD_DEV_FAST_GRID=1; rebuilding grid artifact_hash={artifact_hash}",
        )
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
    use_vsizip = _dev_fast_use_vsizip()
    parallel = _dev_fast_parallel_specs()

    total_specs = len(selected_roi_specs) + len(selected_ni_specs)
    max_workers_cap = _dev_fast_parallel_max_workers()
    if parallel and max_workers_cap > 1 and total_specs > 1:
        max_workers = min(max_workers_cap, total_specs)
        roi_results: list[tuple[dict, int]] = [({}, 0)] * len(selected_roi_specs)
        ni_results: list[tuple[dict, int]] = [({}, 0)] * len(selected_ni_specs)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            roi_futs = {
                ex.submit(
                    _process_roi_spec_grid,
                    spec,
                    data_dir=data_dir,
                    grid_size_m=int(grid_size_m),
                    chunk_size=chunk_size,
                    use_vsizip=use_vsizip,
                    started=started,
                    progress_cb=progress_cb,
                ): idx
                for idx, spec in enumerate(selected_roi_specs)
            }
            ni_futs = {
                ex.submit(
                    _process_ni_spec_grid,
                    spec,
                    grid_size_m=int(grid_size_m),
                    chunk_size=chunk_size,
                    started=started,
                    progress_cb=progress_cb,
                ): idx
                for idx, spec in enumerate(selected_ni_specs)
            }
            for fut, idx in roi_futs.items():
                roi_results[idx] = fut.result()
            for fut, idx in ni_futs.items():
                ni_results[idx] = fut.result()
        # Merge in original spec order to preserve tie-break ordering.
        for local_acc, local_rows in roi_results:
            _merge_grid_acc(acc, local_acc)
            source_rows += local_rows
        for local_acc, local_rows in ni_results:
            _merge_grid_acc(acc, local_acc)
            source_rows += local_rows
    else:
        for spec in selected_roi_specs:
            local_acc, local_rows = _process_roi_spec_grid(
                spec,
                data_dir=data_dir,
                grid_size_m=int(grid_size_m),
                chunk_size=chunk_size,
                use_vsizip=use_vsizip,
                started=started,
                progress_cb=progress_cb,
            )
            _merge_grid_acc(acc, local_acc)
            source_rows += local_rows
        for spec in selected_ni_specs:
            local_acc, local_rows = _process_ni_spec_grid(
                spec,
                grid_size_m=int(grid_size_m),
                chunk_size=chunk_size,
                started=started,
                progress_cb=progress_cb,
            )
            _merge_grid_acc(acc, local_acc)
            source_rows += local_rows
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
            _bulk_load_grid_rows(conn, rows)
    _progress(
        progress_cb,
        f"road/rail grid build complete: grid_size={grid_size_m}m source_rows={source_rows:,} cells={len(rows):,}",
    )
    return {"source_rows": int(source_rows), "cell_rows": int(len(rows))}


_GRID_STAGE_COLUMNS = (
    "artifact_hash",
    "noise_source_hash",
    "jurisdiction",
    "source_type",
    "metric",
    "grid_size_m",
    "cell_x",
    "cell_y",
    "round_number",
    "report_period",
    "db_low",
    "db_high",
    "db_value",
    "minx",
    "miny",
    "maxx",
    "maxy",
)


def _bulk_load_grid_rows(conn, rows: list[dict]) -> None:
    """Load grid rows into noise_grid_artifact.

    Uses psycopg3 COPY into a TEMP staging table when available
    (mirrors `_copy_rows_into_stage_via_psycopg` in noise_artifacts/ingest.py);
    otherwise falls back to a single executemany-style INSERT. The driver
    detection happens before any SQL is issued, so a fallback is taken on a
    clean transaction state — never mid-statement.
    """
    driver_conn = None
    try:
        driver_conn = conn.connection.driver_connection
    except Exception:
        driver_conn = None
    if driver_conn is not None:
        _bulk_load_grid_rows_via_copy(conn, driver_conn, rows)
        return
    conn.execute(
        text(
            """
            INSERT INTO noise_grid_artifact (
                artifact_hash, noise_source_hash, jurisdiction, source_type, metric,
                grid_size_m, cell_x, cell_y, round_number, report_period,
                db_low, db_high, db_value, geom
            )
            VALUES (
                :artifact_hash, :noise_source_hash, :jurisdiction, :source_type, :metric,
                :grid_size_m, :cell_x, :cell_y, :round_number, :report_period,
                :db_low, :db_high, :db_value,
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


def _bulk_load_grid_rows_via_copy(conn, driver_conn, rows: list[dict]) -> None:
    cols = ", ".join(_GRID_STAGE_COLUMNS)
    conn.execute(
        text(
            """
            CREATE TEMP TABLE _stage_noise_grid (
                artifact_hash text,
                noise_source_hash text,
                jurisdiction text,
                source_type text,
                metric text,
                grid_size_m int,
                cell_x int,
                cell_y int,
                round_number int,
                report_period text,
                db_low double precision,
                db_high double precision,
                db_value text,
                minx double precision,
                miny double precision,
                maxx double precision,
                maxy double precision
            ) ON COMMIT DROP
            """
        )
    )
    copy_sql = f"COPY _stage_noise_grid ({cols}) FROM STDIN"
    with driver_conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(tuple(row.get(c) for c in _GRID_STAGE_COLUMNS))
    conn.execute(
        text(
            """
            INSERT INTO noise_grid_artifact (
                artifact_hash, noise_source_hash, jurisdiction, source_type, metric,
                grid_size_m, cell_x, cell_y, round_number, report_period,
                db_low, db_high, db_value, geom
            )
            SELECT
                artifact_hash, noise_source_hash, jurisdiction, source_type, metric,
                grid_size_m, cell_x, cell_y, round_number, report_period,
                db_low, db_high, db_value,
                ST_Multi(
                    ST_SetSRID(
                        ST_MakeEnvelope(minx, miny, maxx, maxy),
                        2157
                    )
                )
            FROM _stage_noise_grid
            """
        )
    )


def materialize_dev_fast_resolved(
    engine: Engine,
    *,
    noise_source_hash: str,
    noise_resolved_hash: str,
    grid_artifact_hash: str,
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
                    :resolved_hash,
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
                WHERE g.artifact_hash = :grid_artifact_hash
                  AND g.grid_size_m = :grid_size_m
                  AND g.source_type IN ('road', 'rail')
                """
            ),
            {
                "resolved_hash": noise_resolved_hash,
                "grid_artifact_hash": grid_artifact_hash,
                "grid_size_m": int(grid_size_m),
            },
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
                WHERE artifact_hash = :grid_artifact_hash
                  AND grid_size_m = :grid_size_m
                  AND source_type IN ('road', 'rail')
                GROUP BY jurisdiction, source_type, metric, round_number
                """
            ),
            {
                "resolved_hash": noise_resolved_hash,
                "grid_artifact_hash": grid_artifact_hash,
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
    del engine, noise_source_hash, simplify_tolerance_m, progress_cb
    raise NoiseIngestError(
        "apply_accurate_simplification is disabled because accurate mode must not "
        "mutate canonical noise_normalized rows. Pass simplify_tolerance_m to "
        "dissolve_noise_into_staging instead."
    )
