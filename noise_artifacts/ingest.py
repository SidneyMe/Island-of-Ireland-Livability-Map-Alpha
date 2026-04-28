"""
Source ingest for the noise artifact pipeline.

PHASE 4A TEMPORARY: uses noise.loader (Python/Fiona/Shapely).
Phase 4B / Phase 13 will replace this with GDAL/ogr2ogr + PostGIS import:
  ogr2ogr -f PostgreSQL PG:"..." <source> -t_srs EPSG:2157 -nln noise_normalized ...

This is the ONLY module in the noise_artifacts package that opens raw
ZIP/FileGDB/SHP files. The builder calls this; the livability build does not.
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from binascii import unhexlify
from decimal import Decimal
from itertools import islice
from typing import Iterable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .exceptions import NoiseIngestError

log = logging.getLogger(__name__)

_BATCH_SIZE = 500
# Schema version bump here forces re-ingest for all existing source hashes.
INGEST_SCHEMA_VERSION = 1

_BAND_RE = re.compile(r"^\d{2}-\d{2}$|^\d{2}\+$")

_META_KEYS = (
    "source_ref", "jurisdiction", "source_type", "metric",
    "round_number", "report_period", "db_low", "db_high", "db_value",
    "source_dataset", "source_layer", "raw_gridcode",
)

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
    "wkb",
)


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


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


def ingest_noise_normalized(
    engine: Engine,
    noise_source_hash: str,
    data_dir,
    domain_wgs84,
    *,
    force: bool = False,
    reimport_source: bool = False,
    progress_cb=None,
) -> int:
    """
    Load raw noise source rows into noise_normalized (EPSG:2157).

    If force=True or reimport_source=True, deletes existing rows for this source_hash first.
    Returns total rows inserted.
    """
    reimport_source = bool(reimport_source or force)
    total_started = time.perf_counter()
    cache_lookup_started = time.perf_counter()
    existing_rows = _existing_source_row_count(engine, noise_source_hash)
    _timing(progress_cb, "ingest.cache_lookup", time.perf_counter() - cache_lookup_started)

    if reimport_source:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM noise_normalized WHERE noise_source_hash = :h"),
                {"h": noise_source_hash},
            )
            log.info("deleted prior noise_normalized rows for source_hash=%s", noise_source_hash)
        existing_rows = 0
    elif existing_rows > 0:
        _progress(
            progress_cb,
            f"source normalized rows already exist for source={noise_source_hash}; skipping raw ingest",
        )
        _timing(progress_cb, "ingest.total", time.perf_counter() - total_started)
        return 0

    from noise.loader import iter_noise_candidate_rows_cached

    total_read = 0
    total_inserted = 0
    raw_read_seconds = 0.0
    db_insert_seconds = 0.0
    rows_iter = iter_noise_candidate_rows_cached(
        data_dir=data_dir,
        study_area_wgs84=domain_wgs84,
        progress_cb=progress_cb,
        use_cache=True,
    )

    batch_pull_started = time.perf_counter()
    for batch in _batched(rows_iter, _BATCH_SIZE):
        batch_ready_at = time.perf_counter()
        raw_read_seconds += batch_ready_at - batch_pull_started
        total_read += len(batch)

        # Pre-ingest validation before opening a DB transaction.
        for row in batch:
            _validate_noise_row(row)

        db_started = time.perf_counter()
        with engine.begin() as conn:
            try:
                n = _insert_batch(conn, noise_source_hash, batch)
            except Exception as exc:
                from sqlalchemy.exc import IntegrityError
                if isinstance(exc, IntegrityError):
                    raise _diagnose_ingest_integrity_error(
                        engine, conn, noise_source_hash, batch, exc
                    ) from exc
                raise
            total_inserted += n
        db_insert_seconds += time.perf_counter() - db_started
        log.debug(
            "ingested %d rows (running total=%d; read=%d)",
            n,
            total_inserted,
            total_read,
        )
        batch_pull_started = time.perf_counter()

        if total_read % 10_000 < _BATCH_SIZE:
            _progress(
                progress_cb,
                f"ingest streamed {total_read:,} candidates; inserted {total_inserted:,} normalized rows",
            )

    _progress(
        progress_cb,
        f"ingest done: read {total_read:,}; inserted {total_inserted:,}",
    )
    _timing(progress_cb, "ingest.raw_read", raw_read_seconds)
    _timing(progress_cb, "ingest.db_insert", db_insert_seconds)
    _timing(progress_cb, "ingest.total", time.perf_counter() - total_started)
    log.info(
        "ingest complete: source_hash=%s read_rows=%d inserted_rows=%d",
        noise_source_hash,
        total_read,
        total_inserted,
    )
    return total_inserted


def _insert_batch(conn, noise_source_hash: str, batch: list[dict]) -> int:
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
            "wkb": wkb_bytes,
        })

    if not rows:
        return 0

    stage_table = f"noise_ingest_stage_{uuid.uuid4().hex}"
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE {stage_table} (
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
                wkb BYTEA NULL
            ) ON COMMIT DROP
            """
        )
    )

    _copy_rows_into_stage(conn, stage_table, rows)
    conn.execute(
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
                            ST_Transform(ST_SetSRID(ST_GeomFromWKB(s.wkb), 4326), 2157)
                        ), 3
                    )) AS geom
                FROM {stage_table} AS s
                WHERE s.wkb IS NOT NULL
            ) AS converted
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_Area(geom) > 0
            """
        )
    )
    return len(rows)


def _copy_rows_into_stage(conn, stage_table: str, rows: list[dict]) -> None:
    copied = _copy_rows_into_stage_via_psycopg(conn, stage_table, rows)
    if copied:
        return

    # Fallback path for non-psycopg DBAPI implementations used in tests.
    cols = ", ".join(_STAGE_COLUMNS)
    placeholders = ", ".join(f":{col}" for col in _STAGE_COLUMNS)
    conn.execute(
        text(f"INSERT INTO {stage_table} ({cols}) VALUES ({placeholders})"),
        rows,
    )


def _copy_rows_into_stage_via_psycopg(conn, stage_table: str, rows: list[dict]) -> bool:
    try:
        driver_conn = conn.connection.driver_connection
    except Exception:
        return False
    if driver_conn is None:
        return False

    copy_sql = (
        f"COPY {stage_table} ({', '.join(_STAGE_COLUMNS)}) "
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


def _batched(iterable: Iterable, n: int) -> Iterator[list]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch
