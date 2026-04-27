"""
Round-priority resolution for the noise artifact pipeline.

Groups: (jurisdiction, source_type, metric).
Within each group, rounds are processed highest-to-lowest.
Newer rounds mask older rounds via ST_Difference (EPSG:2157).
Provenance is group-level aggregate — no expensive per-polygon spatial joins.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def materialize_resolved_display(
    engine: Engine,
    *,
    noise_resolved_hash: str,
    round_table: str,
    domain_wkb: bytes,
    topology_grid_metres: float = 0.1,
) -> dict:
    """
    Populate noise_resolved_display and noise_resolved_provenance from round_table.

    Processes each (jurisdiction, source_type, metric) group; within each group,
    rounds are inserted from highest to lowest so that the 'newer_coverage' CTE
    accumulates already-inserted higher-round geometry to mask lower rounds.

    Returns dict(total_inserted, groups_processed).
    """
    created_at = datetime.now(timezone.utc)
    total_inserted = 0
    groups_processed = 0

    with engine.connect() as conn:
        groups = _fetch_groups(conn, round_table)

    for jurisdiction, source_type, metric in groups:
        with engine.connect() as conn:
            rounds = _fetch_rounds(conn, round_table, jurisdiction, source_type, metric)

        for round_number in rounds:
            with engine.begin() as conn:
                n = _insert_resolved_round(
                    conn,
                    noise_resolved_hash=noise_resolved_hash,
                    round_table=round_table,
                    jurisdiction=jurisdiction,
                    source_type=source_type,
                    metric=metric,
                    round_number=round_number,
                    domain_wkb=domain_wkb,
                    topology_grid_metres=topology_grid_metres,
                    created_at=created_at,
                )
                total_inserted += n
                _insert_provenance(
                    conn,
                    noise_resolved_hash=noise_resolved_hash,
                    round_table=round_table,
                    jurisdiction=jurisdiction,
                    source_type=source_type,
                    metric=metric,
                    round_number=round_number,
                )

        groups_processed += 1
        log.debug(
            "resolved group %s/%s/%s: %d total rows so far",
            jurisdiction, source_type, metric, total_inserted,
        )

    return {"total_inserted": total_inserted, "groups_processed": groups_processed}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_groups(conn, round_table: str) -> list[tuple[str, str, str]]:
    rows = conn.execute(text(
        f"""
        SELECT DISTINCT jurisdiction, source_type, metric
        FROM "{round_table}"
        ORDER BY jurisdiction, source_type, metric
        """
    )).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def _fetch_rounds(conn, round_table: str, jurisdiction: str, source_type: str,
                  metric: str) -> list[int]:
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT round_number
            FROM "{round_table}"
            WHERE jurisdiction = :jur
              AND source_type = :stype
              AND metric = :metric
            ORDER BY round_number DESC
            """
        ),
        {"jur": jurisdiction, "stype": source_type, "metric": metric},
    ).fetchall()
    return [int(r[0]) for r in rows]


def _insert_resolved_round(
    conn,
    *,
    noise_resolved_hash: str,
    round_table: str,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
    domain_wkb: bytes,
    topology_grid_metres: float,
    created_at: datetime,
) -> int:
    result = conn.execute(
        text(
            f"""
            WITH domain_2157 AS (
                SELECT ST_Transform(ST_SetSRID(ST_GeomFromWKB(:domain_wkb), 4326), 2157) AS geom
            ),
            newer_coverage AS (
                SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
                FROM noise_resolved_display
                WHERE noise_resolved_hash = :resolved_hash
                  AND jurisdiction = :jur
                  AND source_type = :stype
                  AND metric = :metric
            ),
            source_round AS (
                SELECT *,
                       ST_ReducePrecision(geom, :topology_grid_m) AS precise_geom
                FROM "{round_table}"
                WHERE jurisdiction = :jur
                  AND source_type = :stype
                  AND metric = :metric
                  AND round_number = :round_number
            ),
            clipped AS (
                SELECT
                    s.jurisdiction, s.source_type, s.metric,
                    s.round_number, s.report_period,
                    s.db_low, s.db_high, s.db_value,
                    ST_Multi(ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Intersection(s.precise_geom, d.geom)
                        ), 3
                    )) AS clipped_geom
                FROM source_round s
                CROSS JOIN domain_2157 d
                WHERE s.precise_geom && d.geom
                  AND ST_Area(ST_Intersection(s.precise_geom, d.geom)) > 0
            ),
            masked AS (
                SELECT
                    c.jurisdiction, c.source_type, c.metric,
                    c.round_number, c.report_period,
                    c.db_low, c.db_high, c.db_value,
                    CASE
                        WHEN nc.geom IS NULL OR ST_IsEmpty(nc.geom) THEN c.clipped_geom
                        ELSE ST_Multi(ST_CollectionExtract(
                                 ST_MakeValid(
                                     ST_Difference(
                                         c.clipped_geom,
                                         ST_ReducePrecision(nc.geom, :topology_grid_m)
                                     )
                                 ), 3
                             ))
                    END AS effective_geom
                FROM clipped c
                CROSS JOIN newer_coverage nc
                WHERE c.clipped_geom IS NOT NULL
                  AND NOT ST_IsEmpty(c.clipped_geom)
            ),
            subdivided AS (
                SELECT
                    m.jurisdiction, m.source_type, m.metric,
                    m.round_number, m.report_period,
                    m.db_low, m.db_high, m.db_value,
                    ST_Multi((ST_Dump(ST_Subdivide(m.effective_geom, 256))).geom) AS geom
                FROM masked m
                WHERE m.effective_geom IS NOT NULL
                  AND NOT ST_IsEmpty(m.effective_geom)
                  AND ST_Area(m.effective_geom) > 0
            )
            INSERT INTO noise_resolved_display (
                noise_resolved_hash, jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value, geom
            )
            SELECT
                :resolved_hash,
                jurisdiction, source_type, metric,
                round_number, report_period, db_low, db_high, db_value,
                geom
            FROM subdivided
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_Area(geom) > 0
            """
        ),
        {
            "resolved_hash": noise_resolved_hash,
            "domain_wkb": domain_wkb,
            "jur": jurisdiction,
            "stype": source_type,
            "metric": metric,
            "round_number": round_number,
            "topology_grid_m": topology_grid_metres,
        },
    )
    return max(int(result.rowcount or 0), 0)


def _insert_provenance(
    conn,
    *,
    noise_resolved_hash: str,
    round_table: str,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
) -> None:
    """Insert group-level provenance from the round staging table."""
    conn.execute(
        text(
            f"""
            INSERT INTO noise_resolved_provenance (
                noise_resolved_hash, jurisdiction, source_type, metric,
                round_number, source_dataset, source_layer,
                source_ref_count, source_refs_hash
            )
            SELECT
                :resolved_hash,
                jurisdiction, source_type, metric, round_number,
                source_dataset, source_layer,
                SUM(source_ref_count) AS source_ref_count,
                encode(
                    sha256(string_agg(source_refs_hash ORDER BY source_refs_hash)::bytea),
                    'hex'
                ) AS source_refs_hash
            FROM "{round_table}"
            WHERE jurisdiction = :jur
              AND source_type = :stype
              AND metric = :metric
              AND round_number = :round_number
            GROUP BY jurisdiction, source_type, metric, round_number,
                     source_dataset, source_layer
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "resolved_hash": noise_resolved_hash,
            "jur": jurisdiction,
            "stype": source_type,
            "metric": metric,
            "round_number": round_number,
        },
    )
