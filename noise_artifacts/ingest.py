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
from itertools import islice
from typing import Iterable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_BATCH_SIZE = 500
# Schema version bump here forces re-ingest for all existing source hashes.
INGEST_SCHEMA_VERSION = 1


def ingest_noise_normalized(
    engine: Engine,
    noise_source_hash: str,
    data_dir,
    domain_wgs84,
    *,
    force: bool = False,
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

    total = 0
    for batch in _batched(iter_noise_candidate_rows(data_dir=data_dir, study_area_wgs84=domain_wgs84), _BATCH_SIZE):
        with engine.begin() as conn:
            n = _insert_batch(conn, noise_source_hash, batch)
            total += n
        log.debug("ingested %d rows (running total=%d)", n, total)

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
