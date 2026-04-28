"""ogr2ogr-backed source ingest for noise artifacts."""
from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError

log = logging.getLogger(__name__)

_GDAL_CACHE_ENV = "NOISE_GDAL_CACHE_DIR"
_DEFAULT_GDAL_CACHE_DIR = Path(".livability_cache") / "noise_gdal"


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


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


def _find_column(columns: list[str], *candidates: str) -> str | None:
    by_key = {_canonical_field_name(col): col for col in columns}
    for cand in candidates:
        found = by_key.get(_canonical_field_name(cand))
        if found is not None:
            return found
    return None


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


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


def build_ogr2ogr_command(
    *,
    engine: Engine,
    source_path: Path,
    stage_table: str,
    layer_name: str | None = None,
) -> list[str]:
    cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        _pg_ogr_conn_string(engine),
        str(source_path),
        "-nln",
        stage_table,
        "-overwrite",
        "-t_srs",
        "EPSG:2157",
        "-nlt",
        "MULTIPOLYGON",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=source_fid",
    ]
    if layer_name:
        cmd.append(layer_name)
    return cmd


def _run_ogr2ogr_import(
    *,
    engine: Engine,
    source_path: Path,
    stage_table: str,
    layer_name: str | None,
) -> None:
    cmd = build_ogr2ogr_command(
        engine=engine,
        source_path=source_path,
        stage_table=stage_table,
        layer_name=layer_name,
    )
    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise NoiseIngestError(
            f"ogr2ogr import failed for {source_path} -> {stage_table}: {stderr}"
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
    metric_expr = (
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

    result = conn.execute(
        text(
            f"""
            INSERT INTO noise_normalized (
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            )
            SELECT
                :noise_source_hash,
                'roi',
                :source_type,
                {metric_expr},
                :round_number,
                COALESCE({report_expr}, :default_report_period),
                m.norm_db_low,
                m.norm_db_high,
                m.norm_db_value,
                :source_dataset,
                :source_layer,
                {source_ref_expr},
                ST_Multi(ST_CollectionExtract(ST_MakeValid(s.geom), 3))
            FROM {_quote_ident(stage_table)} s
            JOIN {map_table} m
              ON m.raw_db_value IS NOT DISTINCT FROM {db_value_expr}
             AND m.raw_db_low IS NOT DISTINCT FROM {db_low_expr}
             AND m.raw_db_high IS NOT DISTINCT FROM {db_high_expr}
            WHERE s.geom IS NOT NULL
              AND NOT ST_IsEmpty(s.geom)
              AND ST_Area(s.geom) > 0
              AND {metric_expr} IS NOT NULL
            """
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
    return max(int(result.rowcount or 0), 0)


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

    result = conn.execute(
        text(
            f"""
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
                ST_Multi(ST_CollectionExtract(ST_MakeValid(s.geom), 3))
            FROM {_quote_ident(stage_table)} s
            JOIN {map_table} m
              ON m.raw_gridcode = {grid_expr}
            WHERE s.geom IS NOT NULL
              AND NOT ST_IsEmpty(s.geom)
              AND ST_Area(s.geom) > 0
            """
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
    return max(int(result.rowcount or 0), 0)


def _iter_ni_members(extracted_zip_dir: Path) -> list[str]:
    from noise.loader import _preferred_ni_entries

    members: list[str] = []
    for path in extracted_zip_dir.rglob("*.shp"):
        rel = path.relative_to(extracted_zip_dir).as_posix()
        members.append(rel)
    return _preferred_ni_entries(members)


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


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
        for spec in ROI_SOURCE_SPECS:
            zip_path = Path(data_dir) / spec.zip_name
            extracted = extract_source_archive_if_needed(zip_path)
            source_path = extracted / spec.member
            if not source_path.exists():
                raise NoiseIngestError(f"missing ROI source member after extraction: {source_path}")

            layer_name = source_path.stem if spec.file_format == "gdb" else None
            stage_table = f"_noise_raw_{_short_hash(f'roi:{spec.zip_name}:{spec.member}')}"
            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))

            started = time.perf_counter()
            _run_ogr2ogr_import(
                engine=engine,
                source_path=source_path,
                stage_table=stage_table,
                layer_name=layer_name,
            )
            raw_extract_seconds += time.perf_counter() - started
            _progress(progress_cb, f"ogr2ogr imported ROI layer {spec.member} -> {stage_table}")

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

                stage_table = f"_noise_raw_{_short_hash(f'ni:{zip_name}:{member}')}"
                conn.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(stage_table)}"))

                started = time.perf_counter()
                _run_ogr2ogr_import(
                    engine=engine,
                    source_path=source_path,
                    stage_table=stage_table,
                    layer_name=None,
                )
                raw_extract_seconds += time.perf_counter() - started
                _progress(progress_cb, f"ogr2ogr imported NI layer {member} -> {stage_table}")

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
