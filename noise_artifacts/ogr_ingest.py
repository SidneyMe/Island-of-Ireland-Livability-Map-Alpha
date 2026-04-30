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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError

log = logging.getLogger(__name__)

_GDAL_CACHE_ENV = "NOISE_GDAL_CACHE_DIR"
_DEFAULT_GDAL_CACHE_DIR = Path(".livability_cache") / "noise_gdal"
_OGR2OGR_TIMEOUT_ENV = "NOISE_OGR2OGR_TIMEOUT_SECONDS"
_OGR2OGR_IDLE_TIMEOUT_ENV = "NOISE_OGR2OGR_IDLE_TIMEOUT_SEC"
_OGR2OGR_TOTAL_TIMEOUT_ENV = "NOISE_OGR2OGR_TOTAL_TIMEOUT_SEC"
_OGR2OGR_ROAD_CHUNK_TIMEOUT_ENV = "NOISE_OGR2OGR_ROAD_CHUNK_TIMEOUT_SEC"
_DEFAULT_OGR2OGR_TIMEOUT_SECONDS = 300
_DEFAULT_OGR2OGR_IDLE_TIMEOUT_SECONDS = 600
_DEFAULT_OGR2OGR_TOTAL_TIMEOUT_SECONDS = 1800
_DEFAULT_OGR2OGR_ROAD_CHUNK_TIMEOUT_SECONDS = 1200
_SMALL_SOURCE_OGR2OGR_TIMEOUT_SECONDS = 120
_STALE_IMPORT_BACKEND_MIN_AGE_SECONDS = 300
_OGR2OGR_GDB_CHUNK_SIZE_ENV = "NOISE_OGR2OGR_GDB_CHUNK_SIZE"
_DEFAULT_OGR2OGR_GDB_CHUNK_SIZE = 25
_OGR2OGR_FID_START_ENV = "NOISE_OGR2OGR_FID_START"
_DEFAULT_OGR2OGR_FID_START = 0
_OGR2OGR_GDB_WORKERS_ENV = "NOISE_OGR2OGR_GDB_WORKERS"
_DEFAULT_OGR2OGR_GDB_WORKERS = 2
_MAX_OGR2OGR_GDB_WORKERS = 6
_WINDOWS_DEFAULT_OGR2OGR_GDB_WORKERS = 2
_WINDOWS_ROAD_GDB_TIMEOUT_SECONDS = 900
_DEFAULT_ROAD_GDB_TIMEOUT_SECONDS = 1800
_MIN_FREE_DISK_GB_ENV = "NOISE_MIN_FREE_DISK_GB"
_DEFAULT_MIN_FREE_DISK_GB = 30.0
_KEEP_FAILED_STAGE_TABLES_ENV = "NOISE_KEEP_FAILED_STAGE_TABLES"
_ROAD_GDB_CANONICAL_CACHE_ENV = "NOISE_ROAD_GDB_CANONICAL_CACHE"
_REBUILD_ROAD_GDB_CACHE_ENV = "NOISE_REBUILD_ROAD_GDB_CACHE"
_ROAD_NORMALIZE_BATCH_SIZE_ENV = "NOISE_ROAD_NORMALIZE_BATCH_SIZE"
_SQL_STATEMENT_TIMEOUT_SECONDS_ENV = "NOISE_SQL_STATEMENT_TIMEOUT_SECONDS"
_SQL_LOCK_TIMEOUT_SECONDS_ENV = "NOISE_SQL_LOCK_TIMEOUT_SECONDS"
_DEFAULT_ROAD_NORMALIZE_BATCH_SIZE = 5000
_DEFAULT_SQL_STATEMENT_TIMEOUT_SECONDS = 900
_DEFAULT_SQL_LOCK_TIMEOUT_SECONDS = 30
_TERMINATE_STALE_IMPORT_BACKENDS_ENV = "NOISE_TERMINATE_STALE_IMPORT_BACKENDS"
_TERMINATE_STALE_STAGE_LOCKS_ENV = "NOISE_TERMINATE_STALE_STAGE_LOCKS"
_ROAD_GDB_CANONICAL_VERSION = "road-gdb-canonical-v1"
_ACTIVE_OGR_PROCS_LOCK = threading.Lock()
_ACTIVE_OGR_PROCS: dict[int, tuple[subprocess.Popen, str]] = {}
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


def _ogr2ogr_idle_timeout_seconds() -> float:
    raw = (os.getenv(_OGR2OGR_IDLE_TIMEOUT_ENV) or "").strip()
    if not raw:
        return float(_DEFAULT_OGR2OGR_IDLE_TIMEOUT_SECONDS)
    value = _env_int(_OGR2OGR_IDLE_TIMEOUT_ENV, 0)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_IDLE_TIMEOUT_ENV} must be > 0; got {value}")
    return float(value)


def _ogr2ogr_total_timeout_seconds(*, default: int = _DEFAULT_OGR2OGR_TOTAL_TIMEOUT_SECONDS) -> float:
    raw = (os.getenv(_OGR2OGR_TOTAL_TIMEOUT_ENV) or "").strip()
    if not raw:
        return float(default)
    value = _env_int(_OGR2OGR_TOTAL_TIMEOUT_ENV, 0)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_TOTAL_TIMEOUT_ENV} must be > 0; got {value}")
    return float(value)


def _ogr2ogr_road_chunk_timeout_seconds() -> float:
    raw = (os.getenv(_OGR2OGR_ROAD_CHUNK_TIMEOUT_ENV) or "").strip()
    if not raw:
        return float(_DEFAULT_OGR2OGR_ROAD_CHUNK_TIMEOUT_SECONDS)
    value = _env_int(_OGR2OGR_ROAD_CHUNK_TIMEOUT_ENV, 0)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_ROAD_CHUNK_TIMEOUT_ENV} must be > 0; got {value}")
    return float(value)


def _ogr2ogr_timeout_seconds(*, default: int = _DEFAULT_OGR2OGR_TIMEOUT_SECONDS, road_gdb_chunk: bool = False) -> float:
    raw = (os.getenv(_OGR2OGR_TIMEOUT_ENV) or "").strip()
    if raw:
        value = _env_int(_OGR2OGR_TIMEOUT_ENV, 0)
        if value <= 0:
            raise NoiseIngestError(f"{_OGR2OGR_TIMEOUT_ENV} must be > 0; got {value}")
        return float(value)
    if road_gdb_chunk:
        return _ogr2ogr_road_chunk_timeout_seconds()
    return _ogr2ogr_total_timeout_seconds(default=default)


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
        default_target = (
            _WINDOWS_DEFAULT_OGR2OGR_GDB_WORKERS
            if os.name == "nt"
            else _DEFAULT_OGR2OGR_GDB_WORKERS
        )
        default = min(default_target, os.cpu_count() or 1)
        return max(1, min(default, _MAX_OGR2OGR_GDB_WORKERS))
    value = _env_int(_OGR2OGR_GDB_WORKERS_ENV, _DEFAULT_OGR2OGR_GDB_WORKERS)
    if value <= 0:
        raise NoiseIngestError(f"{_OGR2OGR_GDB_WORKERS_ENV} must be > 0; got {value}")
    return max(1, min(value, _MAX_OGR2OGR_GDB_WORKERS))


def _min_free_disk_gb() -> float:
    raw = (os.getenv(_MIN_FREE_DISK_GB_ENV) or "").strip()
    if not raw:
        return float(_DEFAULT_MIN_FREE_DISK_GB)
    try:
        value = float(raw)
    except ValueError as exc:
        raise NoiseIngestError(f"{_MIN_FREE_DISK_GB_ENV} must be a number; got {raw!r}") from exc
    if value <= 0:
        raise NoiseIngestError(f"{_MIN_FREE_DISK_GB_ENV} must be > 0; got {value}")
    return float(value)


def _keep_failed_stage_tables() -> bool:
    return (os.getenv(_KEEP_FAILED_STAGE_TABLES_ENV) or "").strip().lower() in {"1", "true", "yes"}


def _terminate_stale_import_backends_enabled() -> bool:
    return (os.getenv(_TERMINATE_STALE_IMPORT_BACKENDS_ENV) or "").strip().lower() in {"1", "true", "yes"}


def _terminate_stale_stage_locks_enabled() -> bool:
    return (os.getenv(_TERMINATE_STALE_STAGE_LOCKS_ENV) or "").strip().lower() in {"1", "true", "yes"}


def _road_gdb_canonical_cache_enabled() -> bool:
    raw = (os.getenv(_ROAD_GDB_CANONICAL_CACHE_ENV) or "").strip().lower()
    if not raw:
        return True
    return raw in {"1", "true", "yes"}


def _rebuild_road_gdb_cache() -> bool:
    return (os.getenv(_REBUILD_ROAD_GDB_CACHE_ENV) or "").strip().lower() in {"1", "true", "yes"}


def _road_normalize_batch_size() -> int:
    value = _env_int(_ROAD_NORMALIZE_BATCH_SIZE_ENV, _DEFAULT_ROAD_NORMALIZE_BATCH_SIZE)
    if value <= 0:
        raise NoiseIngestError(f"{_ROAD_NORMALIZE_BATCH_SIZE_ENV} must be > 0; got {value}")
    return value


def _sql_statement_timeout_seconds() -> int:
    value = _env_int(_SQL_STATEMENT_TIMEOUT_SECONDS_ENV, _DEFAULT_SQL_STATEMENT_TIMEOUT_SECONDS)
    if value <= 0:
        raise NoiseIngestError(f"{_SQL_STATEMENT_TIMEOUT_SECONDS_ENV} must be > 0; got {value}")
    return value


def _sql_lock_timeout_seconds() -> int:
    value = _env_int(_SQL_LOCK_TIMEOUT_SECONDS_ENV, _DEFAULT_SQL_LOCK_TIMEOUT_SECONDS)
    if value <= 0:
        raise NoiseIngestError(f"{_SQL_LOCK_TIMEOUT_SECONDS_ENV} must be > 0; got {value}")
    return value


def _free_disk_gb(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return float(usage.free) / (1024.0 ** 3)


def _nearest_existing_path(path: Path) -> Path:
    current = path.resolve()
    while not current.exists() and current.parent != current:
        current = current.parent
    return current


def _pg_data_directory(conn) -> Path:
    value = conn.execute(text("SHOW data_directory")).scalar_one_or_none()
    if not value:
        raise NoiseIngestError("could not resolve PostgreSQL data_directory via SHOW data_directory")
    return Path(str(value))


def _assert_road_gdb_disk_preflight(conn, progress_cb=None) -> None:
    cache_path = _nearest_existing_path(_noise_gdal_cache_dir())
    pg_data_dir = _nearest_existing_path(_pg_data_directory(conn))
    cache_free_gb = _free_disk_gb(cache_path)
    pg_free_gb = _free_disk_gb(pg_data_dir)
    min_gb = _min_free_disk_gb()
    _progress(
        progress_cb,
        f"disk preflight: cache_free={cache_free_gb:.2f}GB path={cache_path}",
    )
    _progress(
        progress_cb,
        f"disk preflight: postgres_free={pg_free_gb:.2f}GB data_directory={pg_data_dir}",
    )
    _progress(
        progress_cb,
        f"disk preflight: min={min_gb:.2f}GB",
    )
    if cache_free_gb < min_gb:
        raise NoiseIngestError(
            "Not enough free disk for Road GDB ingest: "
            f"cache path has {cache_free_gb:.2f} GB free, requires at least {min_gb:.2f} GB. "
            "Clean stale _noise_raw_* tables and .livability_cache, or lower "
            "NOISE_MIN_FREE_DISK_GB only if you know what you are doing."
        )
    if pg_free_gb < min_gb:
        raise NoiseIngestError(
            "Not enough free disk for Road GDB ingest: "
            f"PostgreSQL data directory has {pg_free_gb:.2f} GB free, requires at least {min_gb:.2f} GB. "
            "Clean stale _noise_raw_* tables and .livability_cache, or lower "
            "NOISE_MIN_FREE_DISK_GB only if you know what you are doing."
        )


def _format_elapsed(seconds: float) -> str:
    total = max(int(seconds), 0)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _ogr_source_feature_count(source_path: Path, layer_name: str | None) -> int | None:
    try:
        import pyogrio
    except ImportError:
        return None
    try:
        info = pyogrio.read_info(str(source_path), layer=layer_name, force_feature_count=True)
    except TypeError:
        info = pyogrio.read_info(str(source_path), layer=layer_name)
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    try:
        return int((info.get("features", 0) or 0))
    except (TypeError, ValueError):
        return None


def _ogr2ogr_timeout_for_source(*, source_path: Path, layer_name: str | None) -> float:
    if source_path.suffix.lower() != ".shp":
        return _ogr2ogr_timeout_seconds()
    feature_count = _ogr_source_feature_count(source_path, layer_name)
    if feature_count is not None and feature_count < 1000:
        return min(float(_SMALL_SOURCE_OGR2OGR_TIMEOUT_SECONDS), _ogr2ogr_timeout_seconds())
    return _ogr2ogr_timeout_seconds()


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


def _road_gdb_canonical_cache_dir() -> Path:
    path = _noise_gdal_cache_dir() / "canonical"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _road_gdb_canonical_cache_key(
    *,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
) -> str:
    st = source_path.stat()
    payload = "|".join(
        [
            _ROAD_GDB_CANONICAL_VERSION,
            str(source_path.resolve()),
            str(int(st.st_size)),
            str(int(st.st_mtime_ns)),
            layer_name,
            ",".join(selected_fields),
            "EPSG:2157",
        ]
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _road_gdb_canonical_cache_path(
    *,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
) -> Path:
    key = _road_gdb_canonical_cache_key(
        source_path=source_path,
        layer_name=layer_name,
        selected_fields=selected_fields,
    )
    return _road_gdb_canonical_cache_dir() / f"roi_round4_road_{key}.gpkg"


def _cache_dir_size_gb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue
        try:
            total += file_path.stat().st_size
        except OSError:
            continue
    return float(total) / (1024.0 ** 3)


def _build_ogr2ogr_gdb_to_gpkg_command(
    *,
    source_path: Path,
    layer_name: str,
    cache_gpkg_path: Path,
    selected_fields: list[str],
) -> list[str]:
    cmd = [
        "ogr2ogr",
        "-f",
        "GPKG",
        str(cache_gpkg_path),
        str(source_path),
        layer_name,
        "-nln",
        "road_raw",
        "-overwrite",
        "-t_srs",
        "EPSG:2157",
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-progress",
    ]
    if selected_fields:
        cmd.extend(["-select", ",".join(selected_fields)])
    return cmd


def _road_gdb_canonical_cache_feature_count(path: Path) -> int:
    try:
        import pyogrio
    except ImportError:
        return 0
    try:
        info = pyogrio.read_info(str(path), layer="road_raw", force_feature_count=True)
    except TypeError:
        info = pyogrio.read_info(str(path), layer="road_raw")
    except Exception:
        return 0
    return int(((info or {}).get("features", 0) if isinstance(info, dict) else 0) or 0)


def _apply_sql_timeouts(conn) -> None:
    statement_timeout_seconds = _sql_statement_timeout_seconds()
    lock_timeout_seconds = _sql_lock_timeout_seconds()
    # PostgreSQL does not accept bind params in SET LOCAL for these GUCs in this path.
    # Values are strict positive ints from env parsing, so literal formatting is safe.
    conn.execute(text(f"SET LOCAL statement_timeout = '{statement_timeout_seconds}s'"))
    conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout_seconds}s'"))


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
    extra_where_sql: str = "",
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
              {extra_where_sql}
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


def _run_ogr2ogr_command(
    *,
    cmd: list[str],
    source_name: str,
    target_label: str,
    progress_cb=None,
    timeout_seconds: float | None = None,
    operation_context: str | None = None,
) -> None:
    safe_cmd = _redact_pg_password(cmd)
    context_label = operation_context or f"target={target_label}"
    _progress(
        progress_cb,
        f"starting ogr2ogr import: source={source_name} target={target_label} context={context_label} cmd={safe_cmd}",
    )

    popen_kwargs: dict[str, Any] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
        "bufsize": 1,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    proc = subprocess.Popen(cmd, **popen_kwargs)
    with _ACTIVE_OGR_PROCS_LOCK:
        _ACTIVE_OGR_PROCS[id(proc)] = (proc, context_label)
    if proc.stdout is None:
        proc.kill()
        with _ACTIVE_OGR_PROCS_LOCK:
            _ACTIVE_OGR_PROCS.pop(id(proc), None)
        raise NoiseIngestError(f"ogr2ogr did not provide stdout pipe for target={target_label}")

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
    last_output_at = start
    last_output_line = ""
    lines: list[str] = []
    effective_timeout = (
        float(timeout_seconds)
        if timeout_seconds is not None and float(timeout_seconds) > 0.0
        else _ogr2ogr_timeout_seconds()
    )
    idle_timeout = _ogr2ogr_idle_timeout_seconds()

    try:
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
                    last_output_at = now
                    last_output_line = stripped
                    if "ERROR" in stripped.upper() or (now - last_line_log) >= 15.0:
                        _progress(progress_cb, f"ogr2ogr {target_label}: {stripped}")
                        last_line_log = now

            if (now - last_heartbeat) >= 30.0 and proc.poll() is None:
                last_output_age = max(now - last_output_at, 0.0)
                pid = getattr(proc, "pid", "unknown")
                _progress(
                    progress_cb,
                    "ogr2ogr still running: "
                    f"pid={pid} source={source_name} target={target_label} "
                    f"elapsed={_format_elapsed(elapsed)} timeout={_format_elapsed(effective_timeout)} "
                    f"idle_timeout={_format_elapsed(idle_timeout)} last_output_age={int(last_output_age)}s "
                    f"last_line={last_output_line or '(none)'} context={context_label}",
                )
                last_heartbeat = now

            if (now - last_output_at) > idle_timeout and proc.poll() is None:
                _kill_process_tree_windows(proc, context_label=context_label, progress_cb=progress_cb)
                _terminate_ogr_process(proc, context_label=context_label, progress_cb=progress_cb)
                _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                last_lines = "\n".join(lines[-20:])
                raise NoiseIngestError(
                    "ogr2ogr import idle timeout "
                    f"for source={source_name} target={target_label} context={context_label} "
                    f"pid={getattr(proc, 'pid', 'unknown')} "
                    f"after {_format_elapsed(elapsed)} idle_timeout={_format_elapsed(idle_timeout)} "
                    f"last_line={last_output_line or '(none)'}; cmd={safe_cmd}\n"
                    f"last_output_lines:\n{last_lines}"
                )

            if elapsed > effective_timeout and proc.poll() is None:
                _kill_process_tree_windows(proc, context_label=context_label, progress_cb=progress_cb)
                _terminate_ogr_process(proc, context_label=context_label, progress_cb=progress_cb)
                _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                last_lines = "\n".join(lines[-20:])
                raise NoiseIngestError(
                    "ogr2ogr import timed out "
                    f"for source={source_name} target={target_label} context={context_label} "
                    f"pid={getattr(proc, 'pid', 'unknown')} "
                    f"after {_format_elapsed(elapsed)} timeout={_format_elapsed(effective_timeout)} "
                    f"last_line={last_output_line or '(none)'}; cmd={safe_cmd}\n"
                    f"last_output_lines:\n{last_lines}"
                )

            if proc.poll() is not None and output_queue.empty():
                break
    except BaseException:
        if proc.poll() is None:
            _terminate_ogr_process(proc, context_label=context_label, progress_cb=progress_cb)
        raise
    finally:
        with _ACTIVE_OGR_PROCS_LOCK:
            _ACTIVE_OGR_PROCS.pop(id(proc), None)
        pump_thread.join(timeout=1.0)

    rc = proc.wait()
    if rc != 0:
        raise NoiseIngestError(
            f"ogr2ogr import failed for source={source_name} target={target_label}: returncode={rc}\n"
            + "\n".join(lines[-50:])
        )


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
    operation_context: str | None = None,
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
    context_label = operation_context or f"stage={stage_table}"
    _run_ogr2ogr_command(
        cmd=cmd,
        source_name=source_path.name,
        target_label=stage_table,
        progress_cb=progress_cb,
        timeout_seconds=timeout_seconds,
        operation_context=f"{context_label} layer={layer_name or '(default)'} where={where_clause or '(none)'}",
    )


def _source_is_small_shapefile(*, source_path: Path, layer_name: str | None) -> bool:
    if source_path.suffix.lower() != ".shp":
        return False
    feature_count = _ogr_source_feature_count(source_path, layer_name)
    if feature_count is None:
        return False
    return feature_count < 1000


def _run_small_shapefile_fallback_import(
    *,
    engine: Engine,
    source_path: Path,
    stage_table: str,
    selected_fields: list[str] | None,
    progress_cb=None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="noise_ogr2ogr_fallback_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        gpkg_path = tmp_path / f"{source_path.stem}_fallback.gpkg"
        gpkg_layer = "noise_raw_fallback"
        cmd = [
            "ogr2ogr",
            "-f",
            "GPKG",
            str(gpkg_path),
            str(source_path),
            "-nln",
            gpkg_layer,
            "-overwrite",
            "-t_srs",
            "EPSG:2157",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-progress",
        ]
        if selected_fields:
            cmd.extend(["-select", ",".join(selected_fields)])
        _progress(
            progress_cb,
            "ogr2ogr fallback: converting small shapefile to local gpkg "
            f"source={source_path.name} gpkg={gpkg_path.name}",
        )
        _run_ogr2ogr_command(
            cmd=cmd,
            source_name=source_path.name,
            target_label=str(gpkg_path),
            progress_cb=progress_cb,
            timeout_seconds=_ogr2ogr_timeout_seconds(default=_DEFAULT_OGR2OGR_TIMEOUT_SECONDS),
            operation_context="small-shapefile-fallback-extract",
        )
        _progress(progress_cb, "ogr2ogr fallback: importing local gpkg to PostgreSQL stage")
        _run_ogr2ogr_import(
            engine=engine,
            source_path=gpkg_path,
            stage_table=stage_table,
            layer_name=gpkg_layer,
            selected_fields=None,
            progress_cb=progress_cb,
            timeout_seconds=_ogr2ogr_timeout_seconds(default=_DEFAULT_OGR2OGR_TIMEOUT_SECONDS),
            operation_context="small-shapefile-fallback-gpkg-to-postgres",
        )


def _run_road_gdb_canonical_extraction(
    *,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
    cache_gpkg_path: Path,
    progress_cb=None,
) -> None:
    cache_gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = _build_ogr2ogr_gdb_to_gpkg_command(
        source_path=source_path,
        layer_name=layer_name,
        cache_gpkg_path=cache_gpkg_path,
        selected_fields=selected_fields,
    )
    _progress(
        progress_cb,
        "Road GDB extracting FileGDB -> GPKG... "
        f"source={source_path} cache={cache_gpkg_path} selected_fields={','.join(selected_fields)}",
    )
    started = time.perf_counter()
    try:
        _run_ogr2ogr_command(
            cmd=cmd,
            source_name=source_path.name,
            target_label=str(cache_gpkg_path),
            progress_cb=progress_cb,
            timeout_seconds=_ogr2ogr_timeout_seconds(road_gdb_chunk=True),
            operation_context=f"road-gdb-canonical-extract layer={layer_name}",
        )
    except Exception:
        try:
            cache_gpkg_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    elapsed = time.perf_counter() - started
    _progress(progress_cb, f"Road GDB extracted cache elapsed={elapsed:.1f}s path={cache_gpkg_path}")


def _ensure_road_gdb_canonical_cache(
    *,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
    progress_cb=None,
) -> Path:
    cache_path = _road_gdb_canonical_cache_path(
        source_path=source_path,
        layer_name=layer_name,
        selected_fields=selected_fields,
    )
    _progress(progress_cb, "Road GDB canonical cache lookup...")
    if cache_path.exists() and not _rebuild_road_gdb_cache():
        rows = _road_gdb_canonical_cache_feature_count(cache_path)
        _progress(progress_cb, f"Road GDB canonical cache hit: {cache_path} rows={rows}")
        return cache_path
    _progress(progress_cb, f"Road GDB canonical cache miss: {cache_path}")
    _run_road_gdb_canonical_extraction(
        source_path=source_path,
        layer_name=layer_name,
        selected_fields=selected_fields,
        cache_gpkg_path=cache_path,
        progress_cb=progress_cb,
    )
    rows = _road_gdb_canonical_cache_feature_count(cache_path)
    _progress(progress_cb, f"Road GDB extracted cache rows={rows}")
    return cache_path


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
    fid_min: int | None = None,
    fid_max: int | None = None,
    phase_name: str = "roi_normalize",
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

    _apply_sql_timeouts(conn)
    extra_where_sql = ""
    if fid_min is not None and fid_max is not None:
        extra_where_sql = "AND s.source_fid >= :fid_min AND s.source_fid <= :fid_max"
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
                extra_where_sql=extra_where_sql,
            )
        ),
        {
            "noise_source_hash": noise_source_hash,
            "source_type": source_type,
            "round_number": int(round_number),
            "default_report_period": f"Round {int(round_number)}",
            "source_dataset": source_dataset,
            "source_layer": source_layer,
            "fid_min": fid_min,
            "fid_max": fid_max,
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

    _apply_sql_timeouts(conn)
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


def _normalize_roi_stage_batched(
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
    batch_size = _road_normalize_batch_size()
    min_max = conn.execute(
        text(
            f"""
            SELECT MIN(source_fid), MAX(source_fid)
            FROM {_quote_ident(stage_table)}
            WHERE source_fid IS NOT NULL
            """
        )
    ).fetchone()
    if not min_max or min_max[0] is None or min_max[1] is None:
        _progress(progress_cb, "Road GDB normalization fallback: no source_fid ranges found")
        return _normalize_roi_stage(
            conn,
            stage_table=stage_table,
            noise_source_hash=noise_source_hash,
            round_number=round_number,
            source_type=source_type,
            source_dataset=source_dataset,
            source_layer=source_layer,
            progress_cb=progress_cb,
        )

    min_fid = int(min_max[0])
    max_fid = int(min_max[1])
    total_span = max_fid - min_fid + 1
    batch_count = (total_span + batch_size - 1) // batch_size
    inserted_total = 0

    for idx in range(batch_count):
        fid_low = min_fid + idx * batch_size
        fid_high = min(fid_low + batch_size - 1, max_fid)
        started = time.perf_counter()
        rows_input = conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {_quote_ident(stage_table)}
                WHERE source_fid >= :fid_low AND source_fid <= :fid_high
                """
            ),
            {"fid_low": fid_low, "fid_high": fid_high},
        ).scalar_one_or_none()
        try:
            inserted = _normalize_roi_stage(
                conn,
                stage_table=stage_table,
                noise_source_hash=noise_source_hash,
                round_number=round_number,
                source_type=source_type,
                source_dataset=source_dataset,
                source_layer=source_layer,
                fid_min=fid_low,
                fid_max=fid_high,
                phase_name="road_batch_normalize",
                progress_cb=progress_cb,
            )
        except Exception as exc:
            raise NoiseIngestError(
                "Road GDB normalization failed: "
                f"stage_table={stage_table} phase=road_batch_normalize "
                f"operation=batch_insert source_layer={source_layer} "
                f"fid_range={fid_low}-{fid_high}. Check pg_stat_activity for blockers/timeouts. "
                f"error={exc}"
            ) from exc
        conn.commit()
        inserted_total += inserted
        elapsed = time.perf_counter() - started
        _progress(
            progress_cb,
            f"Road normalize batch {idx + 1}/{batch_count} rows_input={int(rows_input or 0):,} "
            f"inserted={inserted:,} elapsed={elapsed:.1f}s",
        )
    return inserted_total


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

    fields: list[str] = []
    if isinstance(info, dict):
        raw_fields = info.get("fields", [])
        if isinstance(raw_fields, (list, tuple)):
            fields = [str(item) for item in raw_fields if item is not None]
    feature_count = int(((info or {}).get("features", 0) if isinstance(info, dict) else 0) or 0)
    fields_by_canonical = {_canonical_field_name(name): name for name in fields}
    # For SQLite-backed sources (GPKG), rowid is always addressable and avoids
    # driver-specific FID alias surprises (for example "no such column: fid").
    source_suffix = source_path.suffix.lower()
    if source_suffix == ".gpkg":
        id_column = "rowid"
    else:
        id_column = "fid"
    for candidate in ("source_fid", "OBJECTID", "ObjectID", "OGC_FID", "FID", "ID"):
        found = fields_by_canonical.get(_canonical_field_name(candidate))
        if found:
            id_column = found
            break
    if isinstance(info, dict):
        for key in ("fid_column", "fidcolumn", "fidColumn"):
            fid_name = info.get(key)
            if isinstance(fid_name, str) and fid_name.strip():
                id_column = fid_name.strip()
                break
    chunk_size = _ogr2ogr_gdb_chunk_size()
    fid_start = _ogr2ogr_fid_start()
    ranges = _fid_chunk_ranges(
        feature_count=feature_count,
        fid_start=fid_start,
        chunk_size=chunk_size,
    )
    return id_column, ranges, feature_count


def _kill_process_tree_windows(proc, *, context_label: str, progress_cb=None) -> None:
    if os.name != "nt":
        return
    pid = getattr(proc, "pid", None)
    if not isinstance(pid, int) or pid <= 0:
        return
    try:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        _progress(progress_cb, f"taskkill process tree issued: pid={pid} context={context_label}")
    except Exception:
        return


def _terminate_ogr_process(proc, *, context_label: str, progress_cb=None) -> None:
    if proc.poll() is not None:
        return
    _progress(progress_cb, f"terminating ogr2ogr after interrupt/error: context={context_label}")
    try:
        proc.terminate()
    except Exception:
        pass
    try:
        proc.wait(timeout=5.0)
    except Exception:
        _progress(progress_cb, f"killing ogr2ogr after terminate timeout: context={context_label}")
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=5.0)
        except Exception:
            pass
    if proc.poll() is None:
        _kill_process_tree_windows(proc, context_label=context_label, progress_cb=progress_cb)


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
        operation_context=f"road-chunk idx={idx}/{total_chunks} fid={chunk_low}-{chunk_high}",
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


def _drop_tables_best_effort(conn_or_engine, table_names: list[str], progress_cb=None) -> int:
    unique_names = [name for name in dict.fromkeys(table_names) if str(name).strip()]
    if not unique_names:
        return 0

    dropped = 0

    def _drop_from_conn(conn) -> int:
        local_dropped = 0
        for table_name in unique_names:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(table_name)} CASCADE"))
                local_dropped += 1
            except Exception as exc:
                _progress(progress_cb, f"Road GDB drop failed for {table_name}: {exc}")
        return local_dropped

    if hasattr(conn_or_engine, "execute"):
        dropped = _drop_from_conn(conn_or_engine)
    else:
        with conn_or_engine.connect() as drop_conn:
            dropped = _drop_from_conn(drop_conn)
            try:
                drop_conn.commit()
            except Exception:
                pass
    return dropped


def _terminate_active_ogr2ogr_processes(progress_cb=None) -> int:
    with _ACTIVE_OGR_PROCS_LOCK:
        active = list(_ACTIVE_OGR_PROCS.values())
    terminated = 0
    for proc, context_label in active:
        if proc.poll() is not None:
            continue
        terminated += 1
        _progress(progress_cb, f"terminating active ogr2ogr process: pid={getattr(proc, 'pid', 'unknown')} context={context_label}")
        _terminate_ogr_process(proc, context_label=context_label, progress_cb=progress_cb)
    return terminated


def _cleanup_stale_road_stage_tables(conn, *, stage_table: str, progress_cb=None) -> int:
    rows = conn.execute(
        text(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
              AND (tablename = :stage_table OR tablename LIKE :chunk_like)
            """
        ),
        {"stage_table": stage_table, "chunk_like": f"{stage_table}_c%"},
    ).fetchall()
    table_names = [str(row[0]) for row in rows if row and row[0]]
    dropped = 0
    if table_names:
        dropped = _drop_tables_best_effort(conn, table_names, progress_cb=progress_cb)
    _progress(
        progress_cb,
        f"Road GDB stale cleanup: dropped {dropped} stale chunk/stage tables for prefix {stage_table}",
    )
    return dropped


def _cleanup_stale_noise_import_backends(conn, progress_cb=None) -> int:
    try:
        rows = conn.execute(
            text(
                """
                SELECT
                    pid,
                    state,
                    wait_event_type,
                    wait_event,
                    EXTRACT(EPOCH FROM (now() - COALESCE(query_start, xact_start, backend_start)))::bigint AS age_seconds,
                    LEFT(COALESCE(query, ''), 220) AS query
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND (
                        query ILIKE '%_noise_raw_%'
                     OR query ILIKE '%COPY%'
                     OR query ILIKE '%noise_normalized%'
                     OR query ILIKE '%noise_artifact%'
                  )
                ORDER BY age_seconds DESC NULLS LAST
                """
            )
        ).fetchall()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        _progress(progress_cb, f"stale backend diagnostic skipped: {exc}")
        return 0
    if not rows:
        return 0

    _progress(progress_cb, f"found {len(rows)} potential stale noise import backend(s)")
    terminated = 0
    allow_terminate = _terminate_stale_import_backends_enabled()
    for row in rows:
        pid = int(row[0]) if row and row[0] is not None else -1
        state = row[1] if len(row) > 1 else None
        wait_event_type = row[2] if len(row) > 2 else None
        wait_event = row[3] if len(row) > 3 else None
        age_seconds = int(row[4]) if len(row) > 4 and row[4] is not None else 0
        query = str(row[5]) if len(row) > 5 and row[5] is not None else ""
        _progress(
            progress_cb,
            "stale backend candidate: "
            f"pid={pid} state={state} wait={wait_event_type}:{wait_event} age={age_seconds}s query={query}",
        )
        if allow_terminate and age_seconds >= _STALE_IMPORT_BACKEND_MIN_AGE_SECONDS and pid > 0:
            try:
                conn.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
                terminated += 1
                _progress(progress_cb, f"terminated stale backend pid={pid} age={age_seconds}s")
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                _progress(progress_cb, f"failed to terminate stale backend pid={pid}: {exc}")
    return terminated


def _diagnose_stage_table_locks(conn, *, stage_table: str, progress_cb=None) -> int:
    try:
        lock_rows = conn.execute(
            text(
                """
                WITH target AS (
                    SELECT to_regclass(current_schema() || '.' || CAST(:table_name AS text)) AS reg
                )
                SELECT
                    a.pid,
                    a.state,
                    a.wait_event_type,
                    a.wait_event,
                    EXTRACT(EPOCH FROM (now() - COALESCE(a.query_start, a.xact_start, a.backend_start)))::bigint AS age_seconds,
                    LEFT(COALESCE(a.query, ''), 220) AS query
                FROM target t
                JOIN pg_locks l ON l.relation = t.reg
                JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE t.reg IS NOT NULL
                  AND a.pid <> pg_backend_pid()
                ORDER BY age_seconds DESC NULLS LAST
                """
            ),
            {"table_name": stage_table},
        ).fetchall()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        _progress(progress_cb, f"stage lock diagnostic skipped for {stage_table}: {exc}")
        return 0
    if not lock_rows:
        return 0

    _progress(progress_cb, f"stage table {stage_table} has active locks before import: count={len(lock_rows)}")
    terminated = 0
    allow_terminate = _terminate_stale_stage_locks_enabled()
    for row in lock_rows:
        pid = int(row[0]) if row and row[0] is not None else -1
        state = row[1] if len(row) > 1 else None
        wait_event_type = row[2] if len(row) > 2 else None
        wait_event = row[3] if len(row) > 3 else None
        age_seconds = int(row[4]) if len(row) > 4 and row[4] is not None else 0
        query = str(row[5]) if len(row) > 5 and row[5] is not None else ""
        _progress(
            progress_cb,
            "stage lock: "
            f"pid={pid} state={state} wait={wait_event_type}:{wait_event} age={age_seconds}s query={query}",
        )
        if allow_terminate and age_seconds >= _STALE_IMPORT_BACKEND_MIN_AGE_SECONDS and pid > 0:
            try:
                conn.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
                terminated += 1
                _progress(progress_cb, f"terminated stage lock backend pid={pid}")
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                _progress(progress_cb, f"failed to terminate stage lock backend pid={pid}: {exc}")
    return terminated


def _prepare_stage_table_for_external_import(conn, *, stage_table: str, progress_cb=None) -> None:
    _diagnose_stage_table_locks(conn, stage_table=stage_table, progress_cb=progress_cb)
    stale_rows = conn.execute(
        text(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
              AND (tablename = :stage_table OR tablename LIKE :chunk_like)
            """
        ),
        {"stage_table": stage_table, "chunk_like": f"{stage_table}_c%"},
    ).fetchall()
    stale_names = [str(row[0]) for row in stale_rows if row and row[0]]
    dropped = 0
    for name in stale_names:
        conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(name)} CASCADE"))
        dropped += 1
    _progress(progress_cb, f"stale stage cleanup: dropped {dropped} table(s) for prefix {stage_table}")
    conn.commit()


def _cleanup_road_chunk_tables_after_failure(
    conn,
    *,
    stage_table: str,
    known_chunk_tables: set[str],
    progress_cb=None,
) -> None:
    if _keep_failed_stage_tables():
        _progress(
            progress_cb,
            "Road GDB keeping failed chunk staging tables because NOISE_KEEP_FAILED_STAGE_TABLES=1",
        )
        return

    discovered_chunk_tables: list[str] = []
    try:
        rows = conn.execute(
            text(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = current_schema()
                  AND tablename LIKE :chunk_like
                """
            ),
            {"chunk_like": f"{stage_table}_c%"},
        ).fetchall()
        discovered_chunk_tables = [str(row[0]) for row in rows if row and row[0]]
    except Exception:
        discovered_chunk_tables = []

    known_list = [name for name in sorted(known_chunk_tables) if name]
    chunk_tables = list(dict.fromkeys([*known_list, *discovered_chunk_tables]))
    dropped = 0
    if chunk_tables:
        dropped = _drop_tables_best_effort(conn, chunk_tables, progress_cb=progress_cb)
    _progress(
        progress_cb,
        (
            "Road GDB cleanup after failure: dropped "
            f"{dropped}/{len(chunk_tables)} known chunk staging tables"
        ),
    )
    try:
        conn.commit()
    except Exception:
        pass


def _run_road_gdb_chunks_fail_fast(
    conn,
    *,
    engine: Engine,
    source_path: Path,
    layer_name: str,
    selected_fields: list[str],
    stage_table: str,
    id_column: str,
    chunk_ranges: list[tuple[int, int]],
    worker_count: int,
    timeout_seconds: float | None,
    noise_source_hash: str,
    round_number: int,
    source_type: str,
    source_dataset: str,
    source_layer: str,
    progress_cb=None,
) -> tuple[int, int]:
    expected_chunks = len(chunk_ranges)
    if expected_chunks <= 0:
        return 0, 0

    chunk_jobs = [
        (
            idx,
            chunk_low,
            chunk_high,
            _road_chunk_stage_table_name(stage_table, idx),
        )
        for idx, (chunk_low, chunk_high) in enumerate(chunk_ranges, start=1)
    ]
    known_chunk_tables = {job[3] for job in chunk_jobs}
    imported_rows_total = 0
    inserted_rows_total = 0
    completed_chunks = 0
    run_started = time.perf_counter()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        running: dict[Any, tuple[int, int, int, str]] = {}
        next_job_index = 0

        _progress(
            progress_cb,
            f"Road GDB chunk execution starting: chunks={expected_chunks} workers={worker_count}",
        )

        def _submit_next() -> None:
            nonlocal next_job_index
            if next_job_index >= len(chunk_jobs):
                return
            idx, chunk_low, chunk_high, chunk_stage_table = chunk_jobs[next_job_index]
            next_job_index += 1
            future = executor.submit(
                _import_one_road_chunk,
                engine=engine,
                source_path=source_path,
                layer_name=layer_name,
                selected_fields=selected_fields,
                chunk_stage_table=chunk_stage_table,
                idx=idx,
                total_chunks=expected_chunks,
                chunk_low=chunk_low,
                chunk_high=chunk_high,
                where_clause=f"{id_column} >= {chunk_low} AND {id_column} <= {chunk_high}",
                timeout_seconds=timeout_seconds,
                progress_cb=progress_cb,
            )
            running[future] = (idx, chunk_low, chunk_high, chunk_stage_table)

        for _ in range(min(worker_count, len(chunk_jobs))):
            _submit_next()

        while running:
            done, _ = wait(set(running.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                idx, chunk_low, chunk_high, chunk_stage_table = running.pop(future)
                try:
                    result = future.result()
                except BaseException as exc:
                    for pending in running:
                        pending.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                    _cleanup_road_chunk_tables_after_failure(
                        conn,
                        stage_table=stage_table,
                        known_chunk_tables=known_chunk_tables,
                        progress_cb=progress_cb,
                    )
                    if isinstance(exc, KeyboardInterrupt):
                        raise
                    raise NoiseIngestError(
                        "Road GDB chunk failed: "
                        f"idx={idx}/{expected_chunks} fid={chunk_low}-{chunk_high} "
                        f"stage={chunk_stage_table}: {exc}"
                    ) from exc

                chunk_rows = int(result.get("rows", 0) or 0)
                chunk_elapsed = float(result.get("elapsed_seconds", 0.0) or 0.0)
                try:
                    imported_rows_total += chunk_rows
                    completed_chunks += 1

                    if chunk_rows > 0:
                        _progress(
                            progress_cb,
                            f"Road GDB normalizing chunk {idx}/{expected_chunks} table={chunk_stage_table}",
                        )
                        inserted = _normalize_roi_stage(
                            conn,
                            stage_table=chunk_stage_table,
                            noise_source_hash=noise_source_hash,
                            round_number=round_number,
                            source_type=source_type,
                            source_dataset=source_dataset,
                            source_layer=source_layer,
                            progress_cb=progress_cb,
                        )
                        inserted_rows_total += inserted
                        _progress(
                            progress_cb,
                            f"Road GDB normalized chunk {idx}/{expected_chunks} inserted={inserted:,}",
                        )
                    else:
                        _progress(
                            progress_cb,
                            f"Road GDB chunk {idx}/{expected_chunks} imported zero rows for fid {chunk_low}-{chunk_high}",
                        )

                    _drop_tables_best_effort(conn, [chunk_stage_table], progress_cb=progress_cb)
                    _progress(progress_cb, f"Road GDB dropped chunk table {chunk_stage_table}")
                    known_chunk_tables.discard(chunk_stage_table)
                    conn.commit()
                    total_elapsed = max(time.perf_counter() - run_started, 0.001)
                    rows_per_sec = imported_rows_total / total_elapsed
                    _progress(
                        progress_cb,
                        "Road GDB chunk progress: "
                        f"completed={completed_chunks}/{expected_chunks} active_workers={len(running)} "
                        f"last_chunk={idx}/{expected_chunks} last_chunk_rows={chunk_rows:,} "
                        f"last_chunk_elapsed={chunk_elapsed:.1f}s imported_rows={imported_rows_total:,} "
                        f"inserted_rows={inserted_rows_total:,} rows_per_sec={rows_per_sec:.1f}",
                    )
                    _submit_next()
                except BaseException as exc:
                    for pending in running:
                        pending.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                    _cleanup_road_chunk_tables_after_failure(
                        conn,
                        stage_table=stage_table,
                        known_chunk_tables=known_chunk_tables,
                        progress_cb=progress_cb,
                    )
                    if isinstance(exc, KeyboardInterrupt):
                        raise
                    raise NoiseIngestError(
                        "Road GDB chunk post-import processing failed: "
                        f"idx={idx}/{expected_chunks} fid={chunk_low}-{chunk_high} "
                        f"stage={chunk_stage_table}: {exc}"
                    ) from exc

    if completed_chunks != expected_chunks:
        raise NoiseIngestError(
            "Road GDB chunk execution incomplete: "
            f"completed={completed_chunks} expected={expected_chunks}"
        )
    return imported_rows_total, inserted_rows_total


def _normalize_roi_stage_union(
    conn,
    *,
    stage_tables: list[str],
    noise_source_hash: str,
    round_number: int,
    source_type: str,
    source_dataset: str,
    source_layer: str,
    default_report_period: str | None = None,
    progress_cb=None,
) -> int:
    from noise.loader import normalize_noise_band

    if not stage_tables:
        raise NoiseIngestError("internal bug: stage_tables must not be empty for union normalization")

    cols = _table_columns(conn, stage_tables[0])
    time_col = _find_column(cols, "Time")
    db_value_col = _find_column(cols, "DbValue", "dB_Value")
    db_low_col = _find_column(cols, "Db_Low", "dB_Low")
    db_high_col = _find_column(cols, "Db_High", "dB_High")
    report_col = _find_column(cols, "ReportPeriod")

    if time_col is None:
        raise NoiseIngestError(f"ROI layer missing Time column: {source_layer}")

    db_value_expr_s = _col_expr("s", db_value_col, "text")
    db_low_expr_s = _col_expr("s", db_low_col, "double precision")
    db_high_expr_s = _col_expr("s", db_high_col, "double precision")
    db_value_expr_p = _col_expr("p", db_value_col, "text")
    db_low_expr_p = _col_expr("p", db_low_col, "double precision")
    db_high_expr_p = _col_expr("p", db_high_col, "double precision")
    metric_source = _col_expr("s", time_col, "text")
    metric_case_expr = (
        "CASE "
        f"WHEN lower(trim({metric_source})) = 'lden' THEN 'Lden' "
        f"WHEN lower(trim({metric_source})) IN ('lnight','lngt','lnght') THEN 'Lnight' "
        "ELSE NULL END"
    )
    report_expr = _col_expr("p", report_col, "text") if report_col else ":default_report_period"
    source_ref_expr, source_ref_params = _source_ref_expr(
        "p",
        cols,
        source_dataset=source_dataset,
        source_layer=source_layer,
    )

    union_select = " UNION ALL ".join(
        f"SELECT s.*, {repr(name)}::text AS _chunk_table FROM {_quote_ident(name)} s"
        for name in stage_tables
    )
    raw_union_cte = f"WITH raw_union AS ({union_select})"

    distinct_rows = conn.execute(
        text(
            f"""
            {raw_union_cte}
            SELECT DISTINCT
                {db_value_expr_s} AS raw_db_value,
                {db_low_expr_s} AS raw_db_low,
                {db_high_expr_s} AS raw_db_high
            FROM raw_union s
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

    result = conn.execute(
        text(
            f"""
            {raw_union_cte},
            prepared AS (
                SELECT
                    s.*,
                    {metric_case_expr} AS norm_metric,
                    ST_Multi(
                        ST_CollectionExtract(
                            ST_MakeValid(s.geom),
                            3
                        )
                    ) AS clean_geom
                FROM raw_union s
                WHERE s.geom IS NOT NULL
                  AND NOT ST_IsEmpty(s.geom)
                  AND ST_Area(s.geom) > 0
            )
            INSERT INTO noise_normalized (
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            )
            SELECT
                :noise_source_hash,
                'roi',
                :source_type,
                p.norm_metric,
                :round_number,
                COALESCE({report_expr}, :default_report_period),
                m.norm_db_low,
                m.norm_db_high,
                m.norm_db_value,
                :source_dataset,
                :source_layer,
                COALESCE(
                    {source_ref_expr},
                    :source_dataset || ':' || :source_layer || ':' || p._chunk_table
                ),
                p.clean_geom
            FROM prepared p
            JOIN {map_table} m
              ON m.raw_db_value IS NOT DISTINCT FROM {db_value_expr_p}
             AND m.raw_db_low IS NOT DISTINCT FROM {db_low_expr_p}
             AND m.raw_db_high IS NOT DISTINCT FROM {db_high_expr_p}
            WHERE p.norm_metric IS NOT NULL
              AND p.clean_geom IS NOT NULL
              AND NOT ST_IsEmpty(p.clean_geom)
              AND ST_Area(p.clean_geom) > 0
            """
        ),
        {
            "noise_source_hash": noise_source_hash,
            "source_type": source_type,
            "round_number": int(round_number),
            "default_report_period": default_report_period or f"Round {int(round_number)}",
            "source_dataset": source_dataset,
            "source_layer": source_layer,
            **source_ref_params,
        },
    )
    return max(int(result.rowcount or 0), 0)


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
    with engine.connect() as conn:
        _cleanup_stale_noise_import_backends(conn, progress_cb=progress_cb)
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
            _prepare_stage_table_for_external_import(conn, stage_table=stage_table, progress_cb=progress_cb)

            import_started = time.perf_counter()
            used_chunking = False
            if layer_name and _supports_chunked_road_gdb_import(
                source_type=spec.source_type,
                file_format=spec.file_format,
                round_number=spec.round_number,
            ):
                if _road_gdb_canonical_cache_enabled():
                    used_chunking = True
                    configured_workers = _ogr2ogr_gdb_workers()
                    if os.name == "nt" and configured_workers > 2:
                        _progress(
                            progress_cb,
                            "WARNING: parallel Road GDB ogr2ogr workers on Windows are experimental and may "
                            "hang/fail with GDAL/WinSock errors. Use NOISE_OGR2OGR_GDB_WORKERS=2 for safer ingest.",
                        )
                    _assert_road_gdb_disk_preflight(conn, progress_cb=progress_cb)
                    cache_root = _noise_gdal_cache_dir()
                    _progress(
                        progress_cb,
                        f"Road GDB cache directory size={_cache_dir_size_gb(cache_root):.2f}GB path={cache_root}",
                    )
                    try:
                        db_size = conn.execute(text("SELECT pg_database_size(current_database())")).scalar_one_or_none()
                        if db_size is not None:
                            _progress(progress_cb, f"Road GDB postgres database size={float(db_size) / (1024.0 ** 3):.2f}GB")
                    except Exception:
                        pass

                    _cleanup_stale_road_stage_tables(conn, stage_table=stage_table, progress_cb=progress_cb)
                    try:
                        canonical_cache = _ensure_road_gdb_canonical_cache(
                            source_path=source_path,
                            layer_name=layer_name,
                            selected_fields=selected_fields,
                            progress_cb=progress_cb,
                        )
                        try:
                            id_column, chunk_ranges, feature_count = _discover_road_gdb_chunks(
                                canonical_cache,
                                "road_raw",
                            )
                        except NoiseIngestError as exc:
                            _progress(
                                progress_cb,
                                "Road GDB chunk discovery failed; falling back to single-stage import. "
                                f"error={exc}",
                            )
                            _progress(progress_cb, "Road GDB importing GPKG -> PostgreSQL stage...")
                            _run_ogr2ogr_import(
                                engine=engine,
                                source_path=canonical_cache,
                                stage_table=stage_table,
                                layer_name="road_raw",
                                selected_fields=None,
                                progress_cb=progress_cb,
                                timeout_seconds=_ogr2ogr_timeout_seconds(road_gdb_chunk=True),
                                operation_context="road-gpkg-to-postgres-fallback",
                            )
                            imported_rows = _imported_stage_row_count_or_fail(
                                conn,
                                stage_table=stage_table,
                                source_label=f"ROI Road canonical layer {spec.member}",
                                elapsed_seconds=time.perf_counter() - import_started,
                                progress_cb=progress_cb,
                            )
                            _progress(progress_cb, "Road GDB normalizing raw stage...")
                            norm_started = time.perf_counter()
                            inserted_rows = _normalize_roi_stage_batched(
                                conn,
                                stage_table=stage_table,
                                noise_source_hash=noise_source_hash,
                                round_number=spec.round_number,
                                source_type=spec.source_type,
                                source_dataset=spec.zip_name,
                                source_layer=spec.member,
                                progress_cb=progress_cb,
                            )
                            normalize_insert_seconds += time.perf_counter() - norm_started
                            total_inserted += inserted_rows
                            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))
                            conn.commit()
                            _progress(
                                progress_cb,
                                f"Road GDB fallback normalized rows={inserted_rows:,} imported_rows={imported_rows:,}",
                            )
                        else:
                            if feature_count <= 0:
                                raise NoiseIngestError(
                                    f"Road GDB canonical cache has no features: source={canonical_cache}"
                                )
                            chunk_size = _ogr2ogr_gdb_chunk_size()
                            active_fid_start = _ogr2ogr_fid_start()
                            imported_rows = 0
                            inserted_rows = 0
                            while True:
                                active_ranges = _fid_chunk_ranges(
                                    feature_count=feature_count,
                                    fid_start=active_fid_start,
                                    chunk_size=chunk_size,
                                )
                                if not active_ranges:
                                    raise NoiseIngestError(
                                        f"Road GDB chunking produced no ranges for feature_count={feature_count}"
                                    )
                                worker_count = max(1, min(configured_workers, len(active_ranges)))
                                _progress(
                                    progress_cb,
                                    "ogr2ogr Road GDB parallel chunking enabled: "
                                    f"workers={worker_count} chunks={len(active_ranges)} chunk_size={chunk_size}",
                                )
                                imported_rows, inserted_rows = _run_road_gdb_chunks_fail_fast(
                                    conn,
                                    engine=engine,
                                    source_path=canonical_cache,
                                    layer_name="road_raw",
                                    selected_fields=[],
                                    stage_table=stage_table,
                                    id_column=id_column,
                                    chunk_ranges=active_ranges,
                                    worker_count=worker_count,
                                    timeout_seconds=_ogr2ogr_road_chunk_timeout_seconds(),
                                    noise_source_hash=noise_source_hash,
                                    round_number=spec.round_number,
                                    source_type=spec.source_type,
                                    source_dataset=spec.zip_name,
                                    source_layer=spec.member,
                                    progress_cb=progress_cb,
                                )
                                if imported_rows > 0:
                                    break
                                if active_fid_start == 0:
                                    _progress(
                                        progress_cb,
                                        "Road GDB fid_start=0 produced zero rows; retrying with fid_start=1",
                                    )
                                    _cleanup_stale_road_stage_tables(conn, stage_table=stage_table, progress_cb=progress_cb)
                                    active_fid_start = 1
                                    continue
                                raise NoiseIngestError(
                                    f"Road GDB chunk import produced zero rows after retries: source={canonical_cache}"
                                )
                            total_inserted += inserted_rows
                            normalize_insert_seconds += 0.0
                            _progress(
                                progress_cb,
                                f"Road GDB total imported rows={imported_rows:,} normalized rows={inserted_rows:,}",
                            )
                    except BaseException:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        if _keep_failed_stage_tables():
                            _progress(
                                progress_cb,
                                "Road GDB keeping failed chunk staging tables because NOISE_KEEP_FAILED_STAGE_TABLES=1",
                            )
                        else:
                            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)} CASCADE"))
                            try:
                                conn.commit()
                            except Exception:
                                pass
                        _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                        raise
                else:
                    _progress(
                        progress_cb,
                        "WARNING: NOISE_ROAD_GDB_CANONICAL_CACHE=0; falling back to direct FileGDB import path",
                    )
            if not used_chunking:
                timeout_for_source = _ogr2ogr_timeout_for_source(source_path=source_path, layer_name=layer_name)
                try:
                    _run_ogr2ogr_import(
                        engine=engine,
                        source_path=source_path,
                        stage_table=stage_table,
                        layer_name=layer_name,
                        selected_fields=selected_fields,
                        progress_cb=progress_cb,
                        timeout_seconds=timeout_for_source,
                    )
                except NoiseIngestError as exc:
                    if "timed out" not in str(exc).lower() or not _source_is_small_shapefile(
                        source_path=source_path,
                        layer_name=layer_name,
                    ):
                        raise
                    _progress(
                        progress_cb,
                        "ogr2ogr direct import timed out for small shapefile; retrying via local gpkg fallback",
                    )
                    _prepare_stage_table_for_external_import(conn, stage_table=stage_table, progress_cb=progress_cb)
                    _run_small_shapefile_fallback_import(
                        engine=engine,
                        source_path=source_path,
                        stage_table=stage_table,
                        selected_fields=selected_fields,
                        progress_cb=progress_cb,
                    )
            import_elapsed = time.perf_counter() - import_started
            raw_extract_seconds += import_elapsed
            try:
                if not used_chunking:
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
            except Exception:
                if used_chunking:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if _keep_failed_stage_tables():
                    _progress(
                        progress_cb,
                        "Road GDB keeping failed chunk staging tables because NOISE_KEEP_FAILED_STAGE_TABLES=1",
                    )
                else:
                    discovered_chunk_tables: list[str] = []
                    try:
                        rows = conn.execute(
                            text(
                                """
                                SELECT tablename
                                FROM pg_tables
                                WHERE schemaname = current_schema()
                                  AND tablename LIKE :chunk_like
                                """
                            ),
                            {"chunk_like": f"{stage_table}_c%"},
                        ).fetchall()
                        discovered_chunk_tables = [
                            str(row[0]) for row in rows if row and row[0]
                        ]
                    except Exception:
                        discovered_chunk_tables = []
                    chunk_tables = list(dict.fromkeys(discovered_chunk_tables))
                    dropped = 0
                    if chunk_tables:
                        dropped = _drop_tables_best_effort(conn, chunk_tables, progress_cb=progress_cb)
                    _progress(
                        progress_cb,
                        (
                            "Road GDB cleanup after failure: dropped "
                            f"{dropped}/{len(chunk_tables)} known chunk staging tables"
                        ),
                    )
                    try:
                        conn.commit()
                    except Exception:
                        pass
                _terminate_active_ogr2ogr_processes(progress_cb=progress_cb)
                raise

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
                _prepare_stage_table_for_external_import(conn, stage_table=stage_table, progress_cb=progress_cb)

                started = time.perf_counter()
                timeout_for_source = _ogr2ogr_timeout_for_source(source_path=source_path, layer_name=None)
                try:
                    _run_ogr2ogr_import(
                        engine=engine,
                        source_path=source_path,
                        stage_table=stage_table,
                        layer_name=None,
                        selected_fields=selected_fields,
                        progress_cb=progress_cb,
                        timeout_seconds=timeout_for_source,
                    )
                except NoiseIngestError as exc:
                    if "timed out" not in str(exc).lower() or not _source_is_small_shapefile(
                        source_path=source_path,
                        layer_name=None,
                    ):
                        raise
                    _progress(
                        progress_cb,
                        "ogr2ogr direct import timed out for small shapefile; retrying via local gpkg fallback",
                    )
                    _prepare_stage_table_for_external_import(conn, stage_table=stage_table, progress_cb=progress_cb)
                    _run_small_shapefile_fallback_import(
                        engine=engine,
                        source_path=source_path,
                        stage_table=stage_table,
                        selected_fields=selected_fields,
                        progress_cb=progress_cb,
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
