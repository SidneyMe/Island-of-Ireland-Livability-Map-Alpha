"""
Source ingest for the noise artifact pipeline.

Primary path selection:
- NOISE_INGEST_MODE=auto    -> prefer ogr2ogr when available; fallback to python COPY staging
- NOISE_INGEST_MODE=ogr2ogr -> require ogr2ogr
- NOISE_INGEST_MODE=python  -> force python COPY staging
"""
from __future__ import annotations

import logging
import os
import re
import time
from binascii import unhexlify
from decimal import Decimal
from itertools import islice
from typing import Any, Iterable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError

log = logging.getLogger(__name__)

# Schema version bump here forces re-ingest for all existing source hashes.
INGEST_SCHEMA_VERSION = 1

_BAND_RE = re.compile(r"^\d{2}-\d{2}$|^\d{2}\+$")

_META_KEYS = (
    "source_ref", "jurisdiction", "source_type", "metric",
    "round_number", "report_period", "db_low", "db_high", "db_value",
    "source_dataset", "source_layer", "raw_gridcode",
)

_STAGE_TABLE_NAME = "noise_ingest_stage"
_STAGE_COLUMNS = (
    "noise_source_hash",
    "jurisdiction",
    "source_type",
    "metric",
    "round_number",
    "report_period",
    "db_low",
    "db_high",
    "db_value",
    "source_dataset",
    "source_layer",
    "source_ref",
    "raw_gridcode",
    "geom_wkb",
)

_INGEST_MODE_ENV = "NOISE_INGEST_MODE"
_ALLOWED_INGEST_MODES = {"auto", "ogr2ogr", "python"}
_COPY_BATCH_ROWS_ENV = "NOISE_INGEST_COPY_BATCH_ROWS"
_FLUSH_ROWS_ENV = "NOISE_INGEST_FLUSH_ROWS"
_DEFAULT_COPY_BATCH_ROWS = 5_000
_DEFAULT_FLUSH_ROWS = 25_000


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


def _resolve_ingest_mode() -> str:
    raw = (os.getenv(_INGEST_MODE_ENV) or "auto").strip().lower()
    if raw not in _ALLOWED_INGEST_MODES:
        allowed = ", ".join(sorted(_ALLOWED_INGEST_MODES))
        raise NoiseIngestError(
            f"{_INGEST_MODE_ENV} must be one of {{{allowed}}}, got {raw!r}"
        )
    return raw


def _resolve_positive_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise NoiseIngestError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise NoiseIngestError(f"{name} must be > 0, got {value}")
    return value


def _existing_source_row_count(engine: Engine, noise_source_hash: str) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text("SELECT COUNT(*) FROM noise_normalized WHERE noise_source_hash = :h"),
            {"h": noise_source_hash},
        ).scalar_one()
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal, str)):
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return parsed if parsed > 0 else 0
    return 0


def _source_manifest_state(engine: Engine, noise_source_hash: str) -> dict[str, Any] | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT status, manifest_json
                    FROM noise_artifact_manifest
                    WHERE artifact_hash = :h
                      AND artifact_type = 'source'
                    """
                ),
                {"h": noise_source_hash},
            ).mappings().first()
    except Exception:
        return None
    if row is None:
        return None
    manifest_json = row.get("manifest_json") or {}
    if not isinstance(manifest_json, dict):
        manifest_json = {}
    return {
        "status": str(row.get("status") or ""),
        "manifest_json": manifest_json,
    }


def _delete_source_rows(engine: Engine, noise_source_hash: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM noise_normalized WHERE noise_source_hash = :h"),
            {"h": noise_source_hash},
        )


def _validate_noise_row(row: dict) -> None:
    db_value = str(row.get("db_value") or "")
    if not _BAND_RE.match(db_value):
        jurisdiction = str(row.get("jurisdiction") or "").lower()
        if jurisdiction == "ni":
            lines = [
                "Invalid NI noise band:",
                f"  source_dataset={row.get('source_dataset')}",
                f"  source_layer={row.get('source_layer')}",
                f"  source_ref={row.get('source_ref')}",
                f"  jurisdiction={row.get('jurisdiction')}",
                f"  source_type={row.get('source_type')}",
                f"  metric={row.get('metric')}",
                f"  round_number={row.get('round_number')}",
                f"  raw_gridcode={row.get('raw_gridcode')}",
                f"  produced db_value={db_value!r}",
                "",
                "This looks like a class-coded Round 1 gridcode, not a dB threshold.",
                "Add round-aware NI gridcode mapping.",
            ]
            raise NoiseIngestError("\n".join(lines))
        raise NoiseIngestError(
            f"invalid db_value {db_value!r}: expected 'NN-NN' or 'NN+' "
            f"(jurisdiction={row.get('jurisdiction')!r}, "
            f"source_type={row.get('source_type')!r}, "
            f"metric={row.get('metric')!r}, "
            f"source_dataset={row.get('source_dataset')!r}, "
            f"source_ref={row.get('source_ref')!r})"
        )
    db_low = row.get("db_low")
    db_high = row.get("db_high")
    if db_low is not None and db_high is not None:
        try:
            if float(db_high) < float(db_low):
                raise NoiseIngestError(
                    f"db_high ({db_high}) < db_low ({db_low}) "
                    f"for db_value={db_value!r} "
                    f"(jurisdiction={row.get('jurisdiction')!r})"
                )
        except (TypeError, ValueError):
            pass


def _diagnose_ingest_integrity_error(
    engine: Engine,
    conn,
    noise_source_hash: str,
    batch: list[dict],
    exc,
) -> NoiseIngestError:
    constraint_name = None
    try:
        constraint_name = exc.orig.diag.constraint_name
    except AttributeError:
        pass

    suspicious = []
    for row in batch:
        db_value = str(row.get("db_value") or "")
        db_low = row.get("db_low")
        db_high = row.get("db_high")
        is_bad = not _BAND_RE.match(db_value)
        if not is_bad and db_low is not None and db_high is not None:
            try:
                if float(db_high) < float(db_low):
                    is_bad = True
            except (TypeError, ValueError):
                pass
        if is_bad:
            suspicious.append(row)

    lines = [
        f"Noise ingest failed: DB constraint "
        f"{constraint_name or '(unknown)'} rejected a row."
    ]

    if suspicious:
        first = suspicious[0]
        lines.append("First suspicious row:")
        for key in _META_KEYS:
            val = first.get(key)
            if val is not None:
                lines.append(f"  {key}={val}")
        if constraint_name == "noise_normalized_db_value_check":
            lines.append("")
            lines.append(
                "This usually means schema allowed bands are too narrow. "
                "Do not relabel open-ended bands; relax DB constraint."
            )
    else:
        lines.append(
            f"No obviously suspicious rows found "
            f"(batch size: {len(batch)}, source_hash: {noise_source_hash})."
        )
        lines.append("First 5 rows (metadata only, no geometry):")
        for row in batch[:5]:
            meta = {k: row[k] for k in _META_KEYS if k in row}
            lines.append(f"  {meta!r}")

    return NoiseIngestError("\n".join(lines))


def _create_ingest_stage_table(conn) -> None:
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS {_STAGE_TABLE_NAME} (
                noise_source_hash TEXT NOT NULL,
                jurisdiction TEXT NOT NULL,
                source_type TEXT NOT NULL,
                metric TEXT NOT NULL,
                round_number INTEGER NOT NULL,
                report_period TEXT NULL,
                db_low DOUBLE PRECISION NULL,
                db_high DOUBLE PRECISION NULL,
                db_value TEXT NOT NULL,
                source_dataset TEXT NOT NULL,
                source_layer TEXT NOT NULL,
                source_ref TEXT NULL,
                raw_gridcode TEXT NULL,
                geom_wkb BYTEA NULL
            )
            """
        )
    )
    conn.execute(text(f"TRUNCATE TABLE {_STAGE_TABLE_NAME}"))


def _drop_ingest_stage_table(conn) -> None:
    conn.execute(text(f"DROP TABLE IF EXISTS {_STAGE_TABLE_NAME}"))


def _copy_batch_to_stage(conn, noise_source_hash: str, batch: list[dict]) -> int:
    if not batch:
        return 0

    rows: list[dict] = []
    for row in batch:
        geom = row.get("geom")
        if geom is None:
            continue
        try:
            wkb_bytes = bytes(geom.wkb)
        except AttributeError:
            wkb_hex = getattr(geom, "wkb_hex", None)
            if isinstance(wkb_hex, str):
                try:
                    wkb_bytes = unhexlify(wkb_hex)
                except Exception:
                    continue
            else:
                continue

        rows.append({
            "noise_source_hash": noise_source_hash,
            "jurisdiction": str(row.get("jurisdiction") or ""),
            "source_type": str(row.get("source_type") or ""),
            "metric": str(row.get("metric") or ""),
            "round_number": int(row.get("round_number") or 0),
            "report_period": row.get("report_period"),
            "db_low": row.get("db_low"),
            "db_high": row.get("db_high"),
            "db_value": str(row.get("db_value") or ""),
            "source_dataset": str(row.get("source_dataset") or ""),
            "source_layer": str(row.get("source_layer") or ""),
            "source_ref": row.get("source_ref"),
            "raw_gridcode": (
                str(row.get("raw_gridcode"))
                if row.get("raw_gridcode") is not None
                else None
            ),
            "geom_wkb": wkb_bytes,
        })

    if not rows:
        return 0

    _copy_rows_into_stage(conn, rows)
    return len(rows)


def _copy_rows_into_stage(conn, rows: list[dict]) -> None:
    copied = _copy_rows_into_stage_via_psycopg(conn, rows)
    if copied:
        return

    cols = ", ".join(_STAGE_COLUMNS)
    placeholders = ", ".join(f":{col}" for col in _STAGE_COLUMNS)
    conn.execute(
        text(f"INSERT INTO {_STAGE_TABLE_NAME} ({cols}) VALUES ({placeholders})"),
        rows,
    )


def _copy_rows_into_stage_via_psycopg(conn, rows: list[dict]) -> bool:
    try:
        driver_conn = conn.connection.driver_connection
    except Exception:
        return False
    if driver_conn is None:
        return False

    copy_sql = (
        f"COPY {_STAGE_TABLE_NAME} ({', '.join(_STAGE_COLUMNS)}) "
        "FROM STDIN"
    )
    try:
        with driver_conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(tuple(row.get(col) for col in _STAGE_COLUMNS))
        return True
    except Exception:
        return False


def _flush_stage_to_normalized(conn) -> int:
    result = conn.execute(
        text(
            f"""
            INSERT INTO noise_normalized (
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            )
            SELECT
                noise_source_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                source_dataset, source_layer, source_ref, geom
            FROM (
                SELECT
                    s.noise_source_hash, s.jurisdiction, s.source_type, s.metric,
                    s.round_number, s.report_period, s.db_low, s.db_high, s.db_value,
                    s.source_dataset, s.source_layer, s.source_ref,
                    ST_Multi(ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Transform(ST_SetSRID(ST_GeomFromWKB(s.geom_wkb), 4326), 2157)
                        ), 3
                    )) AS geom
                FROM {_STAGE_TABLE_NAME} AS s
                WHERE s.geom_wkb IS NOT NULL
            ) AS converted
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_Area(geom) > 0
            """
        )
    )
    return max(int(result.rowcount or 0), 0)


def _truncate_ingest_stage(conn) -> None:
    conn.execute(text(f"TRUNCATE TABLE {_STAGE_TABLE_NAME}"))


def _ingest_noise_normalized_python_copy(
    engine: Engine,
    noise_source_hash: str,
    data_dir,
    domain_wgs84,
    *,
    progress_cb=None,
) -> int:
    from noise.loader import iter_noise_candidate_rows_cached

    copy_batch_rows = _resolve_positive_int_env(_COPY_BATCH_ROWS_ENV, _DEFAULT_COPY_BATCH_ROWS)
    flush_rows = _resolve_positive_int_env(_FLUSH_ROWS_ENV, _DEFAULT_FLUSH_ROWS)

    total_started = time.perf_counter()
    total_read = 0
    total_staged = 0
    total_inserted = 0
    staged_since_flush = 0

    raw_extract_seconds = 0.0
    copy_stage_seconds = 0.0
    normalize_insert_seconds = 0.0

    rows_iter = iter_noise_candidate_rows_cached(
        data_dir=data_dir,
        study_area_wgs84=domain_wgs84,
        progress_cb=progress_cb,
        use_cache=True,
    )

    with engine.connect() as conn:
        _create_ingest_stage_table(conn)
        try:
            batch_pull_started = time.perf_counter()
            for batch in _batched(rows_iter, copy_batch_rows):
                batch_ready_at = time.perf_counter()
                raw_extract_seconds += batch_ready_at - batch_pull_started
                total_read += len(batch)

                for row in batch:
                    _validate_noise_row(row)

                copy_started = time.perf_counter()
                copied = _copy_batch_to_stage(conn, noise_source_hash, batch)
                copy_stage_seconds += time.perf_counter() - copy_started
                total_staged += copied
                staged_since_flush += copied

                if total_read % 10_000 < copy_batch_rows:
                    _progress(
                        progress_cb,
                        f"ingest streamed {total_read:,} candidates; staged {total_staged:,}",
                    )

                if staged_since_flush >= flush_rows:
                    insert_started = time.perf_counter()
                    inserted = _flush_stage_to_normalized(conn)
                    normalize_insert_seconds += time.perf_counter() - insert_started
                    total_inserted += inserted
                    _truncate_ingest_stage(conn)
                    conn.commit()
                    _progress(progress_cb, f"ingest copied {total_staged:,} staged rows")
                    _progress(progress_cb, f"ingest inserted {total_inserted:,} normalized rows")
                    staged_since_flush = 0

                batch_pull_started = time.perf_counter()

            if staged_since_flush > 0:
                insert_started = time.perf_counter()
                inserted = _flush_stage_to_normalized(conn)
                normalize_insert_seconds += time.perf_counter() - insert_started
                total_inserted += inserted
                _truncate_ingest_stage(conn)
                conn.commit()
                _progress(progress_cb, f"ingest copied {total_staged:,} staged rows")
                _progress(progress_cb, f"ingest inserted {total_inserted:,} normalized rows")
        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                _drop_ingest_stage_table(conn)
                conn.commit()
            except Exception:
                conn.rollback()

    _progress(progress_cb, f"ingest done: read {total_read:,}; inserted {total_inserted:,}")
    _timing(progress_cb, "ingest.raw_extract", raw_extract_seconds)
    _timing(progress_cb, "ingest.copy_stage", copy_stage_seconds)
    _timing(progress_cb, "ingest.normalize_insert", normalize_insert_seconds)
    _timing(progress_cb, "ingest.total", time.perf_counter() - total_started)
    return total_inserted


def ingest_noise_normalized(
    engine: Engine,
    noise_source_hash: str,
    data_dir,
    domain_wgs84,
    *,
    force: bool = False,
    reimport_source: bool = False,
    source_types: set[str] | None = None,
    latest_round_only: bool = False,
    progress_cb=None,
) -> int:
    """
    Load raw noise source rows into noise_normalized (EPSG:2157).

    If force=True or reimport_source=True, deletes existing rows for this source_hash first.
    Returns total rows inserted.
    """
    reimport_source = bool(reimport_source or force)

    cache_lookup_started = time.perf_counter()
    existing_rows = _existing_source_row_count(engine, noise_source_hash)
    source_manifest = _source_manifest_state(engine, noise_source_hash)
    _timing(progress_cb, "ingest.cache_lookup", time.perf_counter() - cache_lookup_started)

    if reimport_source:
        _progress(
            progress_cb,
            "reimport_source=True; deleting existing source rows and rebuilding",
        )
        _delete_source_rows(engine, noise_source_hash)
        log.info("deleted prior noise_normalized rows for source_hash=%s", noise_source_hash)
        existing_rows = 0
    elif existing_rows > 0:
        status = str((source_manifest or {}).get("status") or "")
        manifest_json = dict((source_manifest or {}).get("manifest_json") or {})
        manifest_rows = manifest_json.get("row_count")
        manifest_schema = manifest_json.get("ingest_schema_version")
        ingest_complete = bool(manifest_json.get("ingest_complete"))
        manifest_rows_int = None
        manifest_schema_int = None
        try:
            if manifest_rows is not None:
                manifest_rows_int = int(manifest_rows)
        except (TypeError, ValueError):
            manifest_rows_int = None
        try:
            if manifest_schema is not None:
                manifest_schema_int = int(manifest_schema)
        except (TypeError, ValueError):
            manifest_schema_int = None

        cache_hit = (
            status == "complete"
            and ingest_complete
            and manifest_rows_int is not None
            and manifest_rows_int == existing_rows
            and manifest_schema_int == INGEST_SCHEMA_VERSION
        )
        if cache_hit:
            _progress(
                progress_cb,
                f"source ingest cache hit: source_hash={noise_source_hash} rows={existing_rows:,}",
            )
            _timing(progress_cb, "ingest.total", 0.0)
            return 0

        _progress(
            progress_cb,
            "source ingest incomplete/stale; deleting partial rows and rebuilding",
        )
        _delete_source_rows(engine, noise_source_hash)
        existing_rows = 0

    mode = _resolve_ingest_mode()
    _progress(progress_cb, f"ingest mode: {mode}")

    if mode in {"auto", "ogr2ogr"}:
        from .ogr_ingest import ingest_noise_normalized_ogr2ogr, ogr2ogr_available

        if ogr2ogr_available():
            return ingest_noise_normalized_ogr2ogr(
                engine,
                noise_source_hash,
                data_dir,
                domain_wgs84,
                source_types=source_types,
                latest_round_only=latest_round_only,
                progress_cb=progress_cb,
            )

        if mode == "ogr2ogr":
            raise NoiseIngestError(
                "NOISE_INGEST_MODE=ogr2ogr but ogr2ogr was not found on PATH"
            )

        _progress(progress_cb, "ogr2ogr not available; falling back to python COPY staging")

    return _ingest_noise_normalized_python_copy(
        engine,
        noise_source_hash,
        data_dir,
        domain_wgs84,
        progress_cb=progress_cb,
    )


def _batched(iterable: Iterable, n: int) -> Iterator[list]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch
