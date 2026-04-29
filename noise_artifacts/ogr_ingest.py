"""ogr2ogr-backed source ingest for noise artifacts."""
from __future__ import annotations

import hashlib
import logging
import os
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError

log = logging.getLogger(__name__)

_GDAL_CACHE_ENV = "NOISE_GDAL_CACHE_DIR"
_DEFAULT_GDAL_CACHE_DIR = Path(".livability_cache") / "noise_gdal"
_OGR2OGR_TIMEOUT_ENV = "NOISE_OGR2OGR_TIMEOUT_SECONDS"
_OGR2OGR_GDB_CHUNK_SIZE_ENV = "NOISE_OGR2OGR_GDB_CHUNK_SIZE"
_DEFAULT_OGR2OGR_GDB_CHUNK_SIZE = 25
_OGR2OGR_FID_START_ENV = "NOISE_OGR2OGR_FID_START"
_DEFAULT_OGR2OGR_FID_START = 0
_OGR2OGR_GDB_WORKERS_ENV = "NOISE_OGR2OGR_GDB_WORKERS"
_DEFAULT_OGR2OGR_GDB_WORKERS = 4
_MAX_OGR2OGR_GDB_WORKERS = 6
_ROI_OGR_CANDIDATE_FIELDS = [
    "Time",
    "TIME",
    "DB_LO",
    "DB_LOW",
    "DBLOW",
    "DB_HI",
    "DB_HIGH",
    "DBHI",
    "DB_VALUE",
    "DBVALUE",
    "DB_VAL",
    "BAND",
    "Band",
    "Legend",
    "LEGEND",
    "NoiseBand",
    "NOISEBAND",
    "ReportPeri",
    "ReportPeriod",
    "REPORTPERI",
    "REPORTPERIOD",
    "Round",
    "ROUND",
    "OBJECTID",
    "ObjectID",
    "FID",
    "ID",
]
_NI_OGR_CANDIDATE_FIELDS = [
    "gridcode",
    "GRIDCODE",
    "GridCode",
    "Noise_Cl",
    "NOISE_CL",
    "noise_cl",
    "NoiseCl",
    "NOISECL",
    "OBJECTID",
    "ObjectID",
    "FID",
    "ID",
]
_OGR_FIELD_DENYLIST = {
    "shape_star",
    "shape_leng",
    "shape_area",
    "shape_length",
    "shape__area",
    "shape__length",
    "shape_area_",
    "shape_len",
    "shape",
}


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(f"{name} must be an integer; got {raw!r}") from exc
    return value


def _ogr2ogr_timeout_seconds() -> float | None:
    value = _env_int(_OGR2OGR_TIMEOUT_ENV, 0)
    if value <= 0:
        return None
    return float(value)


def _ogr2ogr_gdb_chunk_size() -> int:
    value = _env_int(_OGR2OGR_GDB_CHUNK_SIZE_ENV, _DEFAULT_OGR2OGR_GDB_CHUNK_SIZE)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_GDB_CHUNK_SIZE_ENV} must be > 0; got {value}")
    return value


def _ogr2ogr_fid_start() -> int:
    value = _env_int(_OGR2OGR_FID_START_ENV, _DEFAULT_OGR2OGR_FID_START)
    if value < 0:
        raise NoiseIngestError(f"{_OGR2OGR_FID_START_ENV} must be >= 0; got {value}")
    return value


def _ogr2ogr_gdb_workers() -> int:
    raw = (os.getenv(_OGR2OGR_GDB_WORKERS_ENV) or "").strip()
    if not raw:
        default = min(_DEFAULT_OGR2OGR_GDB_WORKERS, os.cpu_count() or 1)
        return max(1, min(default, _MAX_OGR2OGR_GDB_WORKERS))
    value = _env_int(_OGR2OGR_GDB_WORKERS_ENV, _DEFAULT_OGR2OGR_GDB_WORKERS)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_GDB_WORKERS_ENV} must be > 0; got {value}")
    return max(1, min(value, _MAX_OGR2OGR_GDB_WORKERS))


def _format_elapsed(seconds: float) -> str:
    total = max(int(seconds), 0)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def ogr2ogr_available() -> bool:
    return shutil.which("ogr2ogr") is not None


def _noise_gdal_cache_dir() -> Path:
    raw = (os.getenv(_GDAL_CACHE_ENV) or "").strip()
    if raw:
        return Path(raw)
    return _DEFAULT_GDAL_CACHE_DIR


def _source_cache_key(zip_path: Path) -> str:
    st = zip_path.stat()
    payload = f"{zip_path.resolve()}|{int(st.st_size)}|{int(st.st_mtime_ns)}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def extract_source_archive_if_needed(zip_path: Path) -> Path:
    if not zip_path.exists():
        raise NoiseIngestError(f"noise source archive missing: {zip_path}")

    cache_root = _noise_gdal_cache_dir()
    cache_root.mkdir(parents=True, exist_ok=True)

    key = _source_cache_key(zip_path)
    target_dir = cache_root / f"{zip_path.stem}-{key}"
    marker = target_dir / ".extracted"
    if marker.exists():
        return target_dir

    tmp_parent = cache_root / f"{target_dir.name}.tmp"
    if tmp_parent.exists():
        shutil.rmtree(tmp_parent, ignore_errors=True)
    tmp_parent.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_parent)
        marker_tmp = tmp_parent / ".extracted"
        marker_tmp.write_text("ok", encoding="utf-8")
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        tmp_parent.replace(target_dir)
    finally:
        if tmp_parent.exists():
            shutil.rmtree(tmp_parent, ignore_errors=True)

    return target_dir


def _canonical_field_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


_OGR_FIELD_DENYLIST_CANONICAL = {_canonical_field_name(name) for name in _OGR_FIELD_DENYLIST}


def _is_denylisted_field(name: str) -> bool:
    return _canonical_field_name(name) in _OGR_FIELD_DENYLIST_CANONICAL


def _find_column(columns: list[str], *candidates: str) -> str | None:
    by_key = {_canonical_field_name(col): col for col in columns}
    for cand in candidates:
        found = by_key.get(_canonical_field_name(cand))
        if found is not None:
            return found
    return None


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _quote_ogr_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '\\"') + '"'


def _col_expr(alias: str, col_name: str | None, cast_sql: str | None = None) -> str:
    if col_name is None:
        return "NULL"
    expr = f"{alias}.{_quote_ident(col_name)}"
    if cast_sql:
        return f"CAST({expr} AS {cast_sql})"
    return expr


def _table_columns(conn, table_name: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name
            ORDER BY ordinal_position
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return [str(row[0]) for row in rows]


def _stage_raw_geom_stats(conn, stage_table: str) -> tuple[int, int, int]:
    row = conn.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE geom IS NULL) AS raw_null,
                COUNT(*) FILTER (WHERE geom IS NOT NULL AND ST_IsEmpty(geom)) AS raw_empty
            FROM {_quote_ident(stage_table)}
            """
        )
    ).fetchone()
    if row is None:
        return 0, 0, 0
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def _stage_clean_geom_ready_counts(conn, stage_table: str) -> tuple[int, int]:
    row = conn.execute(
        text(
            f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE s.geom IS NOT NULL
                      AND NOT ST_IsEmpty(s.geom)
                      AND ST_Area(s.geom) > 0
                ) AS raw_geom_ready,
                COUNT(*) FILTER (
                    WHERE s.geom IS NOT NULL
                      AND NOT ST_IsEmpty(s.geom)
                      AND ST_Area(s.geom) > 0
                      AND g.clean_geom IS NOT NULL
                      AND NOT ST_IsEmpty(g.clean_geom)
                      AND ST_Area(g.clean_geom) > 0
                ) AS clean_geom_ready
            FROM {_quote_ident(stage_table)} s
            CROSS JOIN LATERAL (
                SELECT ST_Multi(
                    ST_CollectionExtract(
                        ST_MakeValid(s.geom),
                        3
                    )
                ) AS clean_geom
            ) g
            """
        )
    ).fetchone()
    if row is None:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def _source_ref_expr(alias: str, columns: list[str], *, source_dataset: str, source_layer: str) -> tuple[str, dict[str, str]]:
    fid_candidates = [
        _find_column(columns, "source_fid", "SOURCE_FID"),
        _find_column(columns, "ogc_fid", "OGC_FID"),
        _find_column(columns, "fid", "FID"),
    ]
    fid_parts = [f"CAST({alias}.{_quote_ident(col)} AS text)" for col in fid_candidates if col]
    if fid_parts:
        fid_expr = "COALESCE(" + ", ".join(fid_parts + ["replace(CAST(" + alias + ".ctid AS text), ',', ':')"]) + ")"
    else:
        fid_expr = "replace(CAST(" + alias + ".ctid AS text), ',', ':')"
    return (
        ":source_dataset || ':' || :source_layer || ':' || " + fid_expr,
        {"source_dataset": source_dataset, "source_layer": source_layer},
    )


def _pg_ogr_conn_string(engine: Engine) -> str:
    url = engine.url
    parts: list[str] = []
    if url.host:
        parts.append(f"host={url.host}")
    if url.port:
        parts.append(f"port={int(url.port)}")
    if url.database:
        parts.append(f"dbname={url.database}")
    if url.username:
        parts.append(f"user={url.username}")
    if url.password:
        parts.append(f"password={url.password}")

    sslmode = None
    try:
        sslmode = (url.query or {}).get("sslmode")
    except Exception:
        sslmode = None
    if sslmode:
        parts.append(f"sslmode={sslmode}")

    if not parts:
        raise NoiseIngestError("could not derive PostgreSQL connection details for ogr2ogr")
    return "PG:" + " ".join(parts)


def _redact_pg_password(cmd: list[str]) -> str:
    redacted_parts = [re.sub(r"(?i)(password=)([^ ]+)", r"\1***", part) for part in cmd]
    return " ".join(redacted_parts)


def _noise_ogr_candidate_fields(
    *,
    jurisdiction: str,
    source_type: str,
    metric: str | None = None,
    round_number: int | None = None,
) -> list[str]:
    del source_type, metric, round_number
    key = str(jurisdiction or "").strip().lower()
    if key == "roi":
        return list(_ROI_OGR_CANDIDATE_FIELDS)
    if key == "ni":
        return list(_NI_OGR_CANDIDATE_FIELDS)
    raise NoiseIngestError(f"unsupported noise jurisdiction for ogr field selection: {jurisdiction!r}")


def _available_ogr_fields(source_path: Path, layer_name: str | None) -> list[str]:
    info_kwargs: dict[str, Any] = {}
    if layer_name:
        info_kwargs["layer"] = layer_name

    pyogrio_exc: Exception | None = None
    try:
        import pyogrio

        info = pyogrio.read_info(str(source_path), **info_kwargs)
        fields = info.get("fields", ()) if isinstance(info, dict) else ()
        return [str(field) for field in fields if str(field)]
    except ImportError:
        pyogrio_exc = None
    except Exception as exc:  # pragma: no cover - exercised when GDAL bindings mismatch runtime data
        pyogrio_exc = exc

    fiona_exc: Exception | None = None
    try:
        import fiona

        with fiona.open(str(source_path), **info_kwargs) as src:
            properties = (src.schema or {}).get("properties") or {}
            return [str(field) for field in properties.keys()]
    except Exception as exc:
        fiona_exc = exc

    layer_label = layer_name or "<default>"
    detail = ""
    if pyogrio_exc is not None:
        detail = f"; pyogrio error: {pyogrio_exc}"
    if fiona_exc is not None:
        detail = f"{detail}; fiona error: {fiona_exc}" if detail else f"; fiona error: {fiona_exc}"
    raise NoiseIngestError(
        f"could not discover source fields for {source_path} (layer={layer_label}){detail}"
    )


def _select_existing_noise_fields(
    available_fields: list[str],
    candidate_fields: list[str],
    *,
    source_path: Path | None = None,
    layer_name: str | None = None,
) -> list[str]:
    available_by_key: dict[str, str] = {}
    for field_name in available_fields:
        key = _canonical_field_name(field_name)
        if key and key not in available_by_key:
            available_by_key[key] = field_name

    selected: list[str] = []
    seen: set[str] = set()
    for candidate in candidate_fields:
        key = _canonical_field_name(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        if key in _OGR_FIELD_DENYLIST_CANONICAL:
            continue
        matched = available_by_key.get(key)
        if matched and not _is_denylisted_field(matched):
            selected.append(matched)

    if selected:
        return selected

    source_label = str(source_path) if source_path is not None else "<unknown>"
    layer_label = layer_name or "<default>"
    raise NoiseIngestError(
        "No usable noise fields for ogr2ogr import. "
        f"source={source_label}; layer={layer_label}; "
        f"available_fields={available_fields}; candidate_fields={candidate_fields}"
    )


def _denylisted_available_fields(available_fields: list[str]) -> list[str]:
    return [field for field in available_fields if _is_denylisted_field(field)]


def _build_roi_normalize_insert_sql(
    *,
    stage_table: str,
    map_table: str,
    metric_case_expr: str,
    report_expr: str,
    source_ref_expr: str,
    db_value_expr: str,
    db_low_expr: str,
    db_high_expr: str,
) -> str:
    return f"""
            INSERT INTO noise_normalized (
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            )
            SELECT
                :noise_source_hash,
                'roi',
                :source_type,
                mm.metric,
                :round_number,
                COALESCE({report_expr}, :default_report_period),
                m.norm_db_low,
                m.norm_db_high,
                m.norm_db_value,
                :source_dataset,
                :source_layer,
                {source_ref_expr},
                g.clean_geom
            FROM {_quote_ident(stage_table)} s
            CROSS JOIN LATERAL (
                SELECT ST_Multi(
                    ST_CollectionExtract(
                        ST_MakeValid(s.geom),
                        3
                    )
                ) AS clean_geom
            ) g
            CROSS JOIN LATERAL (
                SELECT {metric_case_expr} AS metric
            ) mm
            JOIN {map_table} m
              ON m.raw_db_value IS NOT DISTINCT FROM {db_value_expr}
             AND m.raw_db_low IS NOT DISTINCT FROM {db_low_expr}
             AND m.raw_db_high IS NOT DISTINCT FROM {db_high_expr}
            WHERE s.geom IS NOT NULL
              AND NOT ST_IsEmpty(s.geom)
              AND ST_Area(s.geom) > 0
              AND g.clean_geom IS NOT NULL
              AND NOT ST_IsEmpty(g.clean_geom)
              AND ST_Area(g.clean_geom) > 0
              AND mm.metric IS NOT NULL
            """


def _build_ni_normalize_insert_sql(
    *,
    stage_table: str,
    map_table: str,
    report_expr: str,
    source_ref_expr: str,
    grid_expr: str,
) -> str:
    return f"""
            INSERT INTO noise_normalized (
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            )
            SELECT
                :noise_source_hash,
                'ni',
                :source_type,
                :metric,
                :round_number,
                COALESCE({report_expr}, :default_report_period),
                m.norm_db_low,
                m.norm_db_high,
                m.norm_db_value,
                :source_dataset,
                :source_layer,
                {source_ref_expr},
                g.clean_geom
            FROM {_quote_ident(stage_table)} s
            CROSS JOIN LATERAL (
                SELECT ST_Multi(
                    ST_CollectionExtract(
                        ST_MakeValid(s.geom),
                        3
                    )
                ) AS clean_geom
            ) g
            JOIN {map_table} m
              ON m.raw_gridcode = {grid_expr}
            WHERE s.geom IS NOT NULL
              AND NOT ST_IsEmpty(s.geom)
              AND ST_Area(s.geom) > 0
              AND g.clean_geom IS NOT NULL
              AND NOT ST_IsEmpty(g.clean_geom)
              AND ST_Area(g.clean_geom) > 0
            """


def build_ogr2ogr_command(
    *,
    engine: Engine,
    source_path: Path,
    stage_table: str,
    layer_name: str | None = None,
    selected_fields: list[str] | None = None,
    append: bool = False,
    where_clause: str | None = None,
    progress: bool = True,
) -> list[str]:
    if append and selected_fields:
        raise NoiseIngestError("Internal bug: ogr2ogr command cannot combine -append and -select")

    cmd = [
        "ogr2ogr",
        "--config",
        "PG_USE_COPY",
        "YES",
        "-f",
        "PostgreSQL",
        _pg_ogr_conn_string(engine),
        str(source_path),
        "-nln",
        stage_table,
        "-append" if append else "-overwrite",
        "-t_srs",
        "EPSG:2157",
        "-nlt",
        "MULTIPOLYGON",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=source_fid",
        "-lco",
        "PRECISION=NO",
        "-preserve_fid",
    ]
    if progress:
        cmd.append("-progress")
    if where_clause:
        cmd.extend(["-where", where_clause])
    if selected_fields:
        cmd.extend(["-select", ",".join(selected_fields)])
    if layer_name:
        cmd.append(layer_name)
    return cmd


def _run_ogr2ogr_import(
    *,
    engine: Engine,
    source_path: Path,
    stage_table: str,
    layer_name: str | None,
    selected_fields: list[str] | None = None,
    append: bool = False,
    where_clause: str | None = None,
    progress_cb=None,
    timeout_seconds: float | None = None,
) -> None:
    cmd = build_ogr2ogr_command(
        engine=engine,
        source_path=source_path,
        stage_table=stage_table,
        layer_name=layer_name,
        selected_fields=selected_fields,
        append=append,
        where_clause=where_clause,
        progress=True,
    )
    safe_cmd = _redact_pg_password(cmd)
    _progress(
        progress_cb,
        f"starting ogr2ogr import: stage={stage_table} source={source_path.name} append={append} cmd={safe_cmd}",
    )

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if proc.stdout is None:
        proc.kill()
        raise NoiseIngestError(f"ogr2ogr did not provide stdout pipe for stage={stage_table}")

    output_queue: queue.Queue[str] = queue.Queue()

    def _pump_output() -> None:
        assert proc.stdout is not None
        for raw_line in iter(proc.stdout.readline, ""):
            output_queue.put(raw_line)
        proc.stdout.close()

    pump_thread = threading.Thread(target=_pump_output, daemon=True)
    pump_thread.start()

    start = time.monotonic()
    last_line_log = start
    last_heartbeat = start
    lines: list[str] = []

    while True:
        try:
            line = output_queue.get(timeout=1.0)
        except queue.Empty:
            line = None

        now = time.monotonic()
        elapsed = now - start

        if line is not None:
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
                if "ERROR" in stripped.upper() or (now - last_line_log) >= 15.0:
                    _progress(progress_cb, f"ogr2ogr {stage_table}: {stripped}")
                    last_line_log = now

        if (now - last_heartbeat) >= 30.0 and proc.poll() is None:
            _progress(
                progress_cb,
                f"ogr2ogr still running: source={source_path.name} stage={stage_table} elapsed={_format_elapsed(elapsed)}",
            )
            last_heartbeat = now

        if timeout_seconds is not None and elapsed > timeout_seconds and proc.poll() is None:
            proc.kill()
            proc.wait()
            raise NoiseIngestError(
                "ogr2ogr import timed out "
                f"for {source_path} -> {stage_table} after {_format_elapsed(elapsed)}; cmd={safe_cmd}"
            )

        if proc.poll() is not None and output_queue.empty():
            break

    rc = proc.wait()
    if rc != 0:
        raise NoiseIngestError(
            f"ogr2ogr import failed for {source_path} -> {stage_table}: returncode={rc}\n"
            + "\n".join(lines[-50:])
        )


def _prepare_stage_table(conn, stage_table: str) -> None:
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS {_quote_ident(stage_table + '_geom_gist')} ON {_quote_ident(stage_table)} USING GIST (geom)"))
    conn.execute(text(f"ANALYZE {_quote_ident(stage_table)}"))


def _normalize_roi_stage(
    conn,
    *,
    stage_table: str,
    noise_source_hash: str,
    round_number: int,
    source_type: str,
    source_dataset: str,
    source_layer: str,
    progress_cb=None,
) -> int:
    from noise.loader import normalize_noise_band

    cols = _table_columns(conn, stage_table)
    time_col = _find_column(cols, "Time")
    db_value_col = _find_column(cols, "DbValue", "dB_Value")
    db_low_col = _find_column(cols, "Db_Low", "dB_Low")
    db_high_col = _find_column(cols, "Db_High", "dB_High")
    report_col = _find_column(cols, "ReportPeriod")

    if time_col is None:
        raise NoiseIngestError(f"ROI layer missing Time column: {source_layer}")

    db_value_expr = _col_expr("s", db_value_col, "text")
    db_low_expr = _col_expr("s", db_low_col, "double precision")
    db_high_expr = _col_expr("s", db_high_col, "double precision")

    distinct_rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT
                {db_value_expr} AS raw_db_value,
                {db_low_expr} AS raw_db_low,
                {db_high_expr} AS raw_db_high
            FROM {_quote_ident(stage_table)} s
            """
        )
    ).fetchall()

    band_map_rows: list[dict[str, Any]] = []
    for raw_db_value, raw_db_low, raw_db_high in distinct_rows:
        try:
            norm_low, norm_high, norm_value = normalize_noise_band(
                raw_db_value,
                db_low=raw_db_low,
                db_high=raw_db_high,
            )
        except ValueError as exc:
            raise NoiseIngestError(
                f"ROI band normalization failed for {source_dataset}:{source_layer} "
                f"(db_value={raw_db_value!r}, db_low={raw_db_low!r}, db_high={raw_db_high!r}): {exc}"
            ) from exc
        if norm_value is None:
            continue
        band_map_rows.append(
            {
                "raw_db_value": raw_db_value,
                "raw_db_low": raw_db_low,
                "raw_db_high": raw_db_high,
                "norm_db_low": norm_low,
                "norm_db_high": norm_high,
                "norm_db_value": norm_value,
            }
        )

    if not band_map_rows:
        return 0

    map_table = "noise_roi_band_map"
    conn.execute(text(f"DROP TABLE IF EXISTS {map_table}"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE {map_table} (
                raw_db_value TEXT NULL,
                raw_db_low DOUBLE PRECISION NULL,
                raw_db_high DOUBLE PRECISION NULL,
                norm_db_low DOUBLE PRECISION NULL,
                norm_db_high DOUBLE PRECISION NULL,
                norm_db_value TEXT NOT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            f"""
            INSERT INTO {map_table} (
                raw_db_value, raw_db_low, raw_db_high,
                norm_db_low, norm_db_high, norm_db_value
            ) VALUES (
                :raw_db_value, :raw_db_low, :raw_db_high,
                :norm_db_low, :norm_db_high, :norm_db_value
            )
            """
        ),
        band_map_rows,
    )

    metric_source = _col_expr("s", time_col, "text")
    metric_case_expr = (
        "CASE "
        f"WHEN lower(trim({metric_source})) = 'lden' THEN 'Lden' "
        f"WHEN lower(trim({metric_source})) IN ('lnight','lngt','lnght') THEN 'Lnight' "
        "ELSE NULL END"
    )
    report_expr = _col_expr("s", report_col, "text") if report_col else ":default_report_period"
    source_ref_expr, source_ref_params = _source_ref_expr(
        "s",
        cols,
        source_dataset=source_dataset,
        source_layer=source_layer,
    )

    total_rows, raw_null_rows, raw_empty_rows = _stage_raw_geom_stats(conn, stage_table)
    raw_geom_ready, clean_geom_ready = _stage_clean_geom_ready_counts(conn, stage_table)
    _progress(
        progress_cb,
        f"roi stage geometry stats ({source_layer}): total={total_rows}, raw_null={raw_null_rows}, raw_empty={raw_empty_rows}",
    )

    result = conn.execute(
        text(
            _build_roi_normalize_insert_sql(
                stage_table=stage_table,
                map_table=map_table,
                metric_case_expr=metric_case_expr,
                report_expr=report_expr,
                source_ref_expr=source_ref_expr,
                db_value_expr=db_value_expr,
                db_low_expr=db_low_expr,
                db_high_expr=db_high_expr,
            )
        ),
        {
            "noise_source_hash": noise_source_hash,
            "source_type": source_type,
            "round_number": int(round_number),
            "default_report_period": f"Round {int(round_number)}",
            "source_dataset": source_dataset,
            "source_layer": source_layer,
            **source_ref_params,
        },
    )
    inserted = max(int(result.rowcount or 0), 0)
    skipped_after_clean = max(raw_geom_ready - clean_geom_ready, 0)
    _progress(
        progress_cb,
        "roi stage normalize rows "
        f"({source_layer}): raw_geom_ready={raw_geom_ready}, clean_geom_ready={clean_geom_ready}, "
        f"normalized_inserted={inserted}, skipped_after_geometry_cleaning={skipped_after_clean}",
    )
    return inserted


def _normalize_ni_stage(
    conn,
    *,
    stage_table: str,
    noise_source_hash: str,
    round_number: int,
    source_type: str,
    metric: str,
    source_dataset: str,
    source_layer: str,
    progress_cb=None,
) -> int:
    from noise.loader import normalize_ni_gridcode_band

    cols = _table_columns(conn, stage_table)
    gridcode_col = _find_column(cols, "GRIDCODE", "gridcode")
    report_col = _find_column(cols, "ReportPeriod")

    if gridcode_col is None:
        raise NoiseIngestError(f"NI layer missing GRIDCODE column: {source_layer}")

    grid_expr = _col_expr("s", gridcode_col, "integer")
    distinct_gridcodes = conn.execute(
        text(f"SELECT DISTINCT {grid_expr} AS raw_gridcode FROM {_quote_ident(stage_table)} s")
    ).fetchall()

    band_map_rows: list[dict[str, Any]] = []
    for (raw_gridcode,) in distinct_gridcodes:
        if raw_gridcode is None:
            continue
        try:
            norm_low, norm_high, norm_value = normalize_ni_gridcode_band(
                raw_gridcode,
                round_number=round_number,
                source_type=source_type,
                metric=metric,
            )
        except ValueError as exc:
            raise NoiseIngestError(
                f"{exc} (source_dataset={source_dataset}, source_layer={source_layer}, raw_gridcode={raw_gridcode})"
            ) from exc
        if norm_value is None:
            continue
        band_map_rows.append(
            {
                "raw_gridcode": int(raw_gridcode),
                "norm_db_low": norm_low,
                "norm_db_high": norm_high,
                "norm_db_value": norm_value,
            }
        )

    if not band_map_rows:
        return 0

    map_table = "noise_ni_band_map"
    conn.execute(text(f"DROP TABLE IF EXISTS {map_table}"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE {map_table} (
                raw_gridcode INTEGER PRIMARY KEY,
                norm_db_low DOUBLE PRECISION NULL,
                norm_db_high DOUBLE PRECISION NULL,
                norm_db_value TEXT NOT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            f"""
            INSERT INTO {map_table} (raw_gridcode, norm_db_low, norm_db_high, norm_db_value)
            VALUES (:raw_gridcode, :norm_db_low, :norm_db_high, :norm_db_value)
            """
        ),
        band_map_rows,
    )

    report_expr = _col_expr("s", report_col, "text") if report_col else ":default_report_period"
    source_ref_expr, source_ref_params = _source_ref_expr(
        "s",
        cols,
        source_dataset=source_dataset,
        source_layer=source_layer,
    )

    total_rows, raw_null_rows, raw_empty_rows = _stage_raw_geom_stats(conn, stage_table)
    raw_geom_ready, clean_geom_ready = _stage_clean_geom_ready_counts(conn, stage_table)
    _progress(
        progress_cb,
        f"ni stage geometry stats ({source_layer}): total={total_rows}, raw_null={raw_null_rows}, raw_empty={raw_empty_rows}",
    )

    result = conn.execute(
        text(
            _build_ni_normalize_insert_sql(
                stage_table=stage_table,
                map_table=map_table,
                report_expr=report_expr,
                source_ref_expr=source_ref_expr,
                grid_expr=grid_expr,
            )
        ),
        {
            "noise_source_hash": noise_source_hash,
            "source_type": source_type,
            "metric": metric,
            "round_number": int(round_number),
            "default_report_period": f"Round {int(round_number)}",
            "source_dataset": source_dataset,
            "source_layer": source_layer,
            **source_ref_params,
        },
    )
    inserted = max(int(result.rowcount or 0), 0)
    skipped_after_clean = max(raw_geom_ready - clean_geom_ready, 0)
    _progress(
        progress_cb,
        "ni stage normalize rows "
        f"({source_layer}): raw_geom_ready={raw_geom_ready}, clean_geom_ready={clean_geom_ready}, "
        f"normalized_inserted={inserted}, skipped_after_geometry_cleaning={skipped_after_clean}",
    )
    return inserted


def _iter_ni_members(extracted_zip_dir: Path) -> list[str]:
    from noise.loader import _preferred_ni_entries

    members: list[str] = []
    for path in extracted_zip_dir.rglob("*.shp"):
        rel = path.relative_to(extracted_zip_dir).as_posix()
        members.append(rel)
    return _preferred_ni_entries(members)


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _stage_row_count(conn, stage_table: str) -> int:
    row = conn.execute(text(f"SELECT COUNT(*) FROM {_quote_ident(stage_table)}")).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def _supports_chunked_road_gdb_import(
    *,
    source_type: str,
    file_format: str,
    round_number: int,
) -> bool:
    return (
        str(source_type).strip().lower() == "road"
        and str(file_format).strip().lower() == "gdb"
        and int(round_number) == 4
    )


def _fid_chunk_ranges(*, feature_count: int, fid_start: int, chunk_size: int) -> list[tuple[int, int]]:
    if feature_count <= 0:
        return []
    ranges: list[tuple[int, int]] = []
    for offset in range(0, feature_count, chunk_size):
        low = fid_start + offset
        high = fid_start + min(offset + chunk_size, feature_count) - 1
        ranges.append((low, high))
    return ranges


def _discover_road_gdb_chunks(source_path: Path, layer_name: str) -> tuple[str, list[tuple[int, int]], int]:
    try:
        import pyogrio
    except ImportError as exc:
        raise NoiseIngestError(
            "ROI Round 4 road chunked import requires pyogrio for feature discovery"
        ) from exc

    info_kwargs: dict[str, Any] = {"layer": layer_name}
    try:
        info_kwargs["force_feature_count"] = True
        info = pyogrio.read_info(str(source_path), **info_kwargs)
    except TypeError:
        info_kwargs.pop("force_feature_count", None)
        info = pyogrio.read_info(str(source_path), **info_kwargs)
    except Exception as exc:
        raise NoiseIngestError(
            f"failed to inspect Road GDB layer for chunking: source={source_path} layer={layer_name}: {exc}"
        ) from exc

    feature_count = int(((info or {}).get("features", 0) if isinstance(info, dict) else 0) or 0)
    chunk_size = _ogr2ogr_gdb_chunk_size()
    fid_start = _ogr2ogr_fid_start()
    ranges = _fid_chunk_ranges(
        feature_count=feature_count,
        fid_start=fid_start,
        chunk_size=chunk_size,
    )
    return "fid", ranges, feature_count


def _road_chunk_stage_table_name(stage_table: str, idx: int) -> str:
    return f"{stage_table}_c{idx:03d}"


def _import_one_road_chunk(
    *,
    engine: Engine,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
    chunk_stage_table: str,
    idx: int,
    total_chunks: int,
    chunk_low: int,
    chunk_high: int,
    where_clause: str,
    timeout_seconds: float | None,
    progress_cb=None,
) -> dict[str, int | float | str]:
    _progress(
        progress_cb,
        f"ogr2ogr Road GDB chunk {idx}/{total_chunks} fid {chunk_low}-{chunk_high} -> {chunk_stage_table}",
    )
    chunk_started = time.perf_counter()
    _run_ogr2ogr_import(
        engine=engine,
        source_path=source_path,
        stage_table=chunk_stage_table,
        layer_name=layer_name,
        selected_fields=selected_fields,
        append=False,
        where_clause=where_clause,
        progress_cb=progress_cb,
        timeout_seconds=timeout_seconds,
    )
    with engine.connect() as chunk_conn:
        row_count = _stage_row_count(chunk_conn, chunk_stage_table)
    elapsed = time.perf_counter() - chunk_started
    _progress(
        progress_cb,
        f"ogr2ogr Road GDB chunk {idx}/{total_chunks} done rows={row_count:,} elapsed={elapsed:.1f}s",
    )
    return {
        "idx": idx,
        "chunk_stage_table": chunk_stage_table,
        "chunk_low": chunk_low,
        "chunk_high": chunk_high,
        "rows": row_count,
        "elapsed_seconds": elapsed,
    }


def _merge_road_chunk_tables(
    conn,
    *,
    stage_table: str,
    chunk_stage_tables: list[str],
    expected_rows: int,
    progress_cb=None,
) -> int:
    if not chunk_stage_tables:
        raise NoiseIngestError("internal bug: no chunk stage tables to merge")

    conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))
    union_sql = " UNION ALL ".join(f"SELECT * FROM {_quote_ident(name)}" for name in chunk_stage_tables)
    conn.execute(text(f"CREATE TABLE {_quote_ident(stage_table)} AS {union_sql}"))
    final_rows = _stage_row_count(conn, stage_table)
    if final_rows != expected_rows:
        raise NoiseIngestError(
            "Road GDB chunk merge row mismatch: "
            f"stage_table={stage_table} expected_rows={expected_rows} merged_rows={final_rows}"
        )
    _progress(
        progress_cb,
        f"ogr2ogr Road GDB merged {len(chunk_stage_tables)} chunk tables -> {stage_table} rows={final_rows:,}",
    )
    for chunk_stage_table in chunk_stage_tables:
        conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(chunk_stage_table)}"))
    return final_rows


def _imported_stage_row_count_or_fail(
    conn,
    *,
    stage_table: str,
    source_label: str,
    elapsed_seconds: float,
    progress_cb=None,
) -> int:
    row_count = _stage_row_count(conn, stage_table)
    _progress(
        progress_cb,
        f"ogr2ogr imported {source_label} -> {stage_table} rows={row_count:,} elapsed={elapsed_seconds:.1f}s",
    )
    if row_count <= 0:
        raise NoiseIngestError(f"ogr2ogr imported zero rows for {source_label} -> {stage_table}")
    return row_count


def ingest_noise_normalized_ogr2ogr(
    engine: Engine,
    noise_source_hash: str,
    data_dir,
    domain_wgs84,
    *,
    progress_cb=None,
) -> int:
    del domain_wgs84  # ogr2ogr path does not materialize shapely candidates.

    from noise.loader import (
        NI_ZIP_BY_ROUND,
        ROI_SOURCE_SPECS,
        _metric_from_ni_member,
        _source_type_from_ni_member,
    )

    total_started = time.perf_counter()
    raw_extract_seconds = 0.0
    normalize_insert_seconds = 0.0
    total_inserted = 0
    timeout_seconds = _ogr2ogr_timeout_seconds()

    with engine.connect() as conn:
        for spec in ROI_SOURCE_SPECS:
            zip_path = Path(data_dir) / spec.zip_name
            extracted = extract_source_archive_if_needed(zip_path)
            source_path = extracted / spec.member
            if not source_path.exists():
                raise NoiseIngestError(f"missing ROI source member after extraction: {source_path}")

            layer_name = source_path.stem if spec.file_format == "gdb" else None
            available_fields = _available_ogr_fields(source_path, layer_name)
            candidate_fields = _noise_ogr_candidate_fields(
                jurisdiction="roi",
                source_type=spec.source_type,
                round_number=spec.round_number,
            )
            selected_fields = _select_existing_noise_fields(
                available_fields,
                candidate_fields,
                source_path=source_path,
                layer_name=layer_name,
            )
            skipped_fields = _denylisted_available_fields(available_fields)
            layer_label = layer_name or spec.member
            _progress(
                progress_cb,
                f"ogr2ogr selected fields for {layer_label}: {','.join(selected_fields)}",
            )
            if skipped_fields:
                _progress(
                    progress_cb,
                    f"ogr2ogr skipping geometry metadata fields: {','.join(skipped_fields)}",
                )

            stage_table = f"_noise_raw_{_short_hash(f'roi:{spec.zip_name}:{spec.member}')}"
            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))

            import_started = time.perf_counter()
            used_chunking = False
            if layer_name and _supports_chunked_road_gdb_import(
                source_type=spec.source_type,
                file_format=spec.file_format,
                round_number=spec.round_number,
            ):
                id_column, chunk_ranges, feature_count = _discover_road_gdb_chunks(source_path, layer_name)
                if chunk_ranges:
                    used_chunking = True
                    configured_fid_start = _ogr2ogr_fid_start()
                    chunk_size = _ogr2ogr_gdb_chunk_size()
                    workers = _ogr2ogr_gdb_workers()
                    active_fid_start = configured_fid_start
                    while True:
                        current_ranges = (
                            _fid_chunk_ranges(
                                feature_count=feature_count,
                                fid_start=active_fid_start,
                                chunk_size=chunk_size,
                            )
                            if active_fid_start != configured_fid_start
                            else chunk_ranges
                        )
                        total_chunks = len(current_ranges)
                        if total_chunks <= 0:
                            break
                        _progress(
                            progress_cb,
                            "ogr2ogr Road GDB parallel chunking enabled: "
                            f"workers={workers} chunks={total_chunks} chunk_size={chunk_size}",
                        )
                        chunk_stage_tables = [
                            _road_chunk_stage_table_name(stage_table, idx)
                            for idx in range(1, total_chunks + 1)
                        ]
                        for chunk_stage_table in chunk_stage_tables:
                            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(chunk_stage_table)}"))

                        first_low, first_high = current_ranges[0]
                        first_stage_table = chunk_stage_tables[0]
                        first_result = _import_one_road_chunk(
                            engine=engine,
                            source_path=source_path,
                            layer_name=layer_name,
                            selected_fields=selected_fields,
                            chunk_stage_table=first_stage_table,
                            idx=1,
                            total_chunks=total_chunks,
                            chunk_low=first_low,
                            chunk_high=first_high,
                            where_clause=f"{id_column} >= {first_low} AND {id_column} <= {first_high}",
                            timeout_seconds=timeout_seconds,
                            progress_cb=progress_cb,
                        )
                        first_rows = int(first_result["rows"] or 0)
                        if first_rows <= 0 and active_fid_start == 0:
                            _progress(
                                progress_cb,
                                "ogr2ogr Road GDB first chunk returned 0 rows with fid_start=0; retrying with fid_start=1",
                            )
                            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(first_stage_table)}"))
                            active_fid_start = 1
                            continue

                        chunk_rows_by_index: dict[int, int] = {1: first_rows}
                        if total_chunks > 1:
                            worker_count = max(1, min(workers, total_chunks - 1))
                            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                                futures = []
                                for idx, (chunk_low, chunk_high) in enumerate(current_ranges[1:], start=2):
                                    chunk_stage_table = chunk_stage_tables[idx - 1]
                                    futures.append(
                                        executor.submit(
                                            _import_one_road_chunk,
                                            engine=engine,
                                            source_path=source_path,
                                            layer_name=layer_name,
                                            selected_fields=selected_fields,
                                            chunk_stage_table=chunk_stage_table,
                                            idx=idx,
                                            total_chunks=total_chunks,
                                            chunk_low=chunk_low,
                                            chunk_high=chunk_high,
                                            where_clause=f"{id_column} >= {chunk_low} AND {id_column} <= {chunk_high}",
                                            timeout_seconds=timeout_seconds,
                                            progress_cb=progress_cb,
                                        )
                                    )
                                for future in as_completed(futures):
                                    result = future.result()
                                    chunk_rows_by_index[int(result["idx"])] = int(result["rows"] or 0)

                        expected_rows = sum(chunk_rows_by_index.values())
                        _merge_road_chunk_tables(
                            conn,
                            stage_table=stage_table,
                            chunk_stage_tables=chunk_stage_tables,
                            expected_rows=expected_rows,
                            progress_cb=progress_cb,
                        )
                        break
                else:
                    _progress(
                        progress_cb,
                        (
                            "WARNING: Road GDB chunk plan unavailable; falling back to one-shot import "
                            f"for {source_path.name} layer={layer_name}. This may run for a long time."
                        ),
                    )
            if not used_chunking:
                _run_ogr2ogr_import(
                    engine=engine,
                    source_path=source_path,
                    stage_table=stage_table,
                    layer_name=layer_name,
                    selected_fields=selected_fields,
                    progress_cb=progress_cb,
                    timeout_seconds=timeout_seconds,
                )
            import_elapsed = time.perf_counter() - import_started
            raw_extract_seconds += import_elapsed
            _imported_stage_row_count_or_fail(
                conn,
                stage_table=stage_table,
                source_label=f"ROI layer {spec.member}",
                elapsed_seconds=import_elapsed,
                progress_cb=progress_cb,
            )

            _prepare_stage_table(conn, stage_table)
            started = time.perf_counter()
            inserted = _normalize_roi_stage(
                conn,
                stage_table=stage_table,
                noise_source_hash=noise_source_hash,
                round_number=spec.round_number,
                source_type=spec.source_type,
                source_dataset=spec.zip_name,
                source_layer=spec.member,
                progress_cb=progress_cb,
            )
            normalize_insert_seconds += time.perf_counter() - started
            total_inserted += inserted
            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))
            conn.commit()

        for round_number, zip_name in sorted(NI_ZIP_BY_ROUND.items(), reverse=True):
            zip_path = Path(data_dir) / zip_name
            extracted = extract_source_archive_if_needed(zip_path)
            members = _iter_ni_members(extracted)
            for member in members:
                metric = _metric_from_ni_member(member)
                source_type = _source_type_from_ni_member(member)
                if metric is None or source_type is None:
                    continue

                source_path = extracted / member
                if not source_path.exists():
                    continue

                available_fields = _available_ogr_fields(source_path, None)
                candidate_fields = _noise_ogr_candidate_fields(
                    jurisdiction="ni",
                    source_type=source_type,
                    metric=metric,
                    round_number=int(round_number),
                )
                selected_fields = _select_existing_noise_fields(
                    available_fields,
                    candidate_fields,
                    source_path=source_path,
                    layer_name=member,
                )
                skipped_fields = _denylisted_available_fields(available_fields)
                _progress(
                    progress_cb,
                    f"ogr2ogr selected fields for {member}: {','.join(selected_fields)}",
                )
                if skipped_fields:
                    _progress(
                        progress_cb,
                        f"ogr2ogr skipping geometry metadata fields: {','.join(skipped_fields)}",
                    )

                stage_table = f"_noise_raw_{_short_hash(f'ni:{zip_name}:{member}')}"
                conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))

                started = time.perf_counter()
                _run_ogr2ogr_import(
                    engine=engine,
                    source_path=source_path,
                    stage_table=stage_table,
                    layer_name=None,
                    selected_fields=selected_fields,
                    progress_cb=progress_cb,
                    timeout_seconds=timeout_seconds,
                )
                import_elapsed = time.perf_counter() - started
                raw_extract_seconds += import_elapsed
                _imported_stage_row_count_or_fail(
                    conn,
                    stage_table=stage_table,
                    source_label=f"NI layer {member}",
                    elapsed_seconds=import_elapsed,
                    progress_cb=progress_cb,
                )

                _prepare_stage_table(conn, stage_table)
                started = time.perf_counter()
                inserted = _normalize_ni_stage(
                    conn,
                    stage_table=stage_table,
                    noise_source_hash=noise_source_hash,
                    round_number=int(round_number),
                    source_type=source_type,
                    metric=metric,
                    source_dataset=zip_name,
                    source_layer=member,
                    progress_cb=progress_cb,
                )
                normalize_insert_seconds += time.perf_counter() - started
                total_inserted += inserted
                conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))
                conn.commit()

    _progress(progress_cb, f"ingest done: read via ogr2ogr; inserted {total_inserted:,}")
    _timing(progress_cb, "ingest.raw_extract", raw_extract_seconds)
    _timing(progress_cb, "ingest.normalize_insert", normalize_insert_seconds)
    _timing(progress_cb, "ingest.total", time.perf_counter() - total_started)
    return total_inserted
