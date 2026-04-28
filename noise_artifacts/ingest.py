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
    "source_dataset", "source_layer",
)


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _validate_noise_row(row: dict) -> None:
    db_value = str(row.get("db_value") or "")
    if not _BAND_RE.match(db_value):
        raise NoiseIngestError(
            f"invalid db_value {db_value!r}: expected 'NN-NN' or 'NN+' "
            f"(jurisdiction={row.get('jurisdiction')!r}, "
            f"source_type={row.get('source_type')!r}, "
            f"metric={row.get('metric')!r}, "
            f"source_dataset={row.get('source_dataset')!r})"
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
    progress_cb=None,
) -> int:
    """
    Load raw noise source rows into noise_normalized (EPSG:2157).

    If force=True, deletes any existing rows for this source_hash first.
    Returns total rows inserted.
    """
    if force:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM noise_normalized WHERE noise_source_hash = :h"),
                {"h": noise_source_hash},
            )
            log.info("deleted prior noise_normalized rows for source_hash=%s", noise_source_hash)

    # Phase 4A: use legacy loader (Python/Fiona/Shapely)
    from noise.loader import iter_noise_candidate_rows

    # Collect all candidate rows so we can emit a count before batching.
    candidate_rows = list(iter_noise_candidate_rows(data_dir=data_dir, study_area_wgs84=domain_wgs84))
    _progress(progress_cb, f"ingest read {len(candidate_rows)} candidate rows")

    total = 0
    for batch in _batched(candidate_rows, _BATCH_SIZE):
        # Pre-ingest validation before opening a DB transaction.
        for row in batch:
            _validate_noise_row(row)

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
            total += n
        log.debug("ingested %d rows (running total=%d)", n, total)

    _progress(progress_cb, f"ingest inserted {total} normalized rows")
    log.info("ingest complete: source_hash=%s total_rows=%d", noise_source_hash, total)
    return total


def _insert_batch(conn, noise_source_hash: str, batch: list[dict]) -> int:
    if not batch:
        return 0

    rows = []
    for row in batch:
        geom = row.get("geom")
        if geom is None:
            continue
        try:
            wkb_hex = geom.wkb_hex
        except AttributeError:
            # shapely geometry: use .wkb
            import binascii
            wkb_hex = binascii.hexlify(geom.wkb).decode()

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
            "wkb_hex": wkb_hex,
        })

    if not rows:
        return 0

    # Insert with ST_Transform from EPSG:4326 (loader output) to EPSG:2157 (canonical).
    # Wrap in subquery so the computed geom alias can be filtered in the outer WHERE.
    conn.execute(
        text(
            """
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
                    v.noise_source_hash, v.jurisdiction, v.source_type, v.metric,
                    v.round_number, v.report_period, v.db_low, v.db_high, v.db_value,
                    v.source_dataset, v.source_layer, v.source_ref,
                    ST_Multi(ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Transform(ST_SetSRID(ST_GeomFromWKB(decode(v.wkb_hex, 'hex')), 4326), 2157)
                        ), 3
                    )) AS geom
                FROM (VALUES """ + ", ".join(
                "(:noise_source_hash_{i}, :jurisdiction_{i}, :source_type_{i}, :metric_{i}, "
                ":round_number_{i}, :report_period_{i}, :db_low_{i}, :db_high_{i}, :db_value_{i}, "
                ":source_dataset_{i}, :source_layer_{i}, :source_ref_{i}, :wkb_hex_{i})".format(i=i)
                for i in range(len(rows))
            ) + """
                ) AS v(noise_source_hash, jurisdiction, source_type, metric,
                       round_number, report_period, db_low, db_high, db_value,
                       source_dataset, source_layer, source_ref, wkb_hex)
                WHERE v.wkb_hex IS NOT NULL
            ) AS converted
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_Area(geom) > 0
            """
        ),
        {f"{k}_{i}": v for i, row in enumerate(rows) for k, v in row.items()},
    )
    return len(rows)


def _batched(iterable: Iterable, n: int) -> Iterator[list]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch
