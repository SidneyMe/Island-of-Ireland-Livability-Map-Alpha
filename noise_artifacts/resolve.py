"""
Round-priority resolution for the noise artifact pipeline.

Groups: (jurisdiction, source_type, metric).
Within each group, rounds are processed highest-to-lowest.
Newer rounds mask older rounds via ST_Difference (EPSG:2157) against the
GiST-indexed newer pieces that touch each row; the island domain is split
into indexed parts so clipping never intersects the whole coastline at once.
Provenance is group-level aggregate — no expensive per-polygon spatial joins.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


def materialize_resolved_display(
    engine: Engine,
    *,
    noise_resolved_hash: str,
    round_table: str,
    domain_wkb: bytes,
    topology_grid_metres: float = 0.1,
    progress_cb=None,
) -> dict:
    """
    Populate noise_resolved_display and noise_resolved_provenance from round_table.

    Processes each (jurisdiction, source_type, metric) group; within each group,
    rounds are inserted from highest to lowest so that already-inserted
    higher-round geometry masks lower rounds where it overlaps them.

    Returns dict(total_inserted, groups_processed).
    """
    total_started = time.perf_counter()
    created_at = datetime.now(timezone.utc)
    total_inserted = 0
    groups_processed = 0

    group_fetch_started = time.perf_counter()
    with engine.connect() as conn:
        groups = _fetch_groups(conn, round_table)

    total_groups = len(groups)
    _timing(progress_cb, "resolve.group_count", time.perf_counter() - group_fetch_started)
    _progress(progress_cb, f"resolve groups discovered: {total_groups}")

    domain_table = domain_parts_table_name(noise_resolved_hash)
    newer_table = newer_pieces_table_name(noise_resolved_hash)
    domain_started = time.perf_counter()
    with engine.begin() as conn:
        domain_parts = _create_domain_parts_table(conn, domain_table, domain_wkb)
        _create_newer_pieces_table(conn, newer_table)
    _timing(progress_cb, "resolve.domain_parts", time.perf_counter() - domain_started)
    _progress(progress_cb, f"resolve domain split into {domain_parts:,} indexed parts")
    try:
        for jurisdiction, source_type, metric in groups:
            group_num = groups_processed + 1
            group_started = time.perf_counter()
            _progress(
                progress_cb,
                f"resolve group {group_num}/{total_groups}: {jurisdiction} {source_type} {metric}",
            )
            with engine.connect() as conn:
                rounds = _fetch_rounds(conn, round_table, jurisdiction, source_type, metric)
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE "{newer_table}"'))

            for round_number in rounds:
                round_started = time.perf_counter()
                with engine.begin() as conn:
                    n, fallbacks = _insert_resolved_round(
                        conn,
                        noise_resolved_hash=noise_resolved_hash,
                        round_table=round_table,
                        domain_table=domain_table,
                        newer_table=newer_table,
                        jurisdiction=jurisdiction,
                        source_type=source_type,
                        metric=metric,
                        round_number=round_number,
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
                _timing(
                    progress_cb,
                    f"resolve.group_{group_num}.round_{round_number} rows={n:,}",
                    time.perf_counter() - round_started,
                )
                if fallbacks:
                    _progress(
                        progress_cb,
                        f"resolve GEOS fallbacks in {jurisdiction} {source_type} {metric} "
                        f"round {round_number}: {fallbacks}",
                    )

            groups_processed += 1
            _timing(
                progress_cb,
                f"resolve.group_{group_num}",
                time.perf_counter() - group_started,
            )
            log.debug(
                "resolved group %s/%s/%s: %d total rows so far",
                jurisdiction, source_type, metric, total_inserted,
            )
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{domain_table}"'))
            conn.execute(text(f'DROP TABLE IF EXISTS "{newer_table}"'))

    _timing(progress_cb, "resolve.total", time.perf_counter() - total_started)
    return {"total_inserted": total_inserted, "groups_processed": groups_processed}


def domain_parts_table_name(noise_resolved_hash: str) -> str:
    return f"_noise_domain_parts_{noise_resolved_hash[:12]}"


def newer_pieces_table_name(noise_resolved_hash: str) -> str:
    return f"_noise_newer_pieces_{noise_resolved_hash[:12]}"


def _create_newer_pieces_table(conn, table_name: str) -> None:
    """GiST-only mirror of the current group's already-resolved pieces.

    Looking neighbours up in noise_resolved_display let the planner pick the
    group btree index and scan the whole group per piece; a table whose only
    index is GiST keeps every lookup spatial.
    """
    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
    conn.execute(text(f'CREATE UNLOGGED TABLE "{table_name}" (geom GEOMETRY(MultiPolygon, 2157))'))
    conn.execute(text(f'CREATE INDEX ON "{table_name}" USING GIST (geom)'))


def _create_domain_parts_table(conn, table_name: str, domain_wkb: bytes) -> int:
    """Split the island domain into small indexed parts for clipping.

    Intersecting every round row with the whole island polygon (hundreds of
    thousands of vertices) was the resolve bottleneck; small GiST-indexed
    parts make each clip touch only the nearby coastline.
    """
    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
    conn.execute(text(f'CREATE UNLOGGED TABLE "{table_name}" (geom GEOMETRY(Polygon, 2157))'))
    conn.execute(
        text(
            f"""
            INSERT INTO "{table_name}" (geom)
            SELECT parts.geom
            FROM (
                SELECT (ST_Dump(ST_Subdivide(
                    ST_CollectionExtract(ST_MakeValid(
                        ST_Transform(ST_SetSRID(ST_GeomFromWKB(:domain_wkb), 4326), 2157)
                    ), 3),
                    256
                ))).geom AS geom
            ) AS parts
            WHERE GeometryType(parts.geom) = 'POLYGON'
            """
        ),
        {"domain_wkb": domain_wkb},
    )
    conn.execute(text(f'CREATE INDEX ON "{table_name}" USING GIST (geom)'))
    conn.execute(text(f'ANALYZE "{table_name}"'))
    return int(conn.execute(text(f'SELECT count(*) FROM "{table_name}"')).scalar() or 0)


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


_SAFE_OVERLAY_SQL = """
CREATE TEMP TABLE IF NOT EXISTS noise_resolve_fallbacks (kind TEXT NOT NULL);
TRUNCATE pg_temp.noise_resolve_fallbacks;

CREATE OR REPLACE FUNCTION pg_temp.noise_safe_intersection(a geometry, b geometry)
RETURNS geometry LANGUAGE plpgsql AS $fn$
BEGIN
    RETURN ST_Intersection(a, b);
EXCEPTION WHEN OTHERS THEN
    BEGIN
        INSERT INTO pg_temp.noise_resolve_fallbacks VALUES ('intersection_makevalid');
        RETURN ST_Intersection(ST_Buffer(ST_MakeValid(a), 0), ST_Buffer(ST_MakeValid(b), 0));
    EXCEPTION WHEN OTHERS THEN
        -- Keep the piece unclipped rather than lose its coverage.
        INSERT INTO pg_temp.noise_resolve_fallbacks VALUES ('intersection_unclipped');
        RETURN a;
    END;
END;
$fn$;

CREATE OR REPLACE FUNCTION pg_temp.noise_safe_difference(a geometry, b geometry, grid float8)
RETURNS geometry LANGUAGE plpgsql AS $fn$
BEGIN
    RETURN ST_Difference(a, b, grid);
EXCEPTION WHEN OTHERS THEN
    BEGIN
        INSERT INTO pg_temp.noise_resolve_fallbacks VALUES ('difference_floating');
        RETURN ST_Difference(a, b);
    EXCEPTION WHEN OTHERS THEN
        BEGIN
            INSERT INTO pg_temp.noise_resolve_fallbacks VALUES ('difference_buffered');
            RETURN ST_Difference(ST_Buffer(ST_MakeValid(a), 0), ST_Buffer(ST_MakeValid(b), 0));
        EXCEPTION WHEN OTHERS THEN
            -- Last resort: keep the older-round piece unmasked.
            INSERT INTO pg_temp.noise_resolve_fallbacks VALUES ('difference_unmasked');
            RETURN a;
        END;
    END;
END;
$fn$;
"""


def _ensure_safe_overlay_functions(conn) -> None:
    """Session-local (pg_temp) overlay wrappers that survive GEOS TopologyExceptions.

    Across hundreds of thousands of road-noise polygons some single overlay
    eventually raises (e.g. "side location conflict", "Ring edge missing"),
    which used to abort the whole artifact build. The wrappers retry with
    progressively more forgiving operations and record each fallback in
    pg_temp.noise_resolve_fallbacks so the caller can report it. No schema
    objects are created: pg_temp disappears with the session.
    """
    conn.exec_driver_sql(_SAFE_OVERLAY_SQL)


def _insert_resolved_round(
    conn,
    *,
    noise_resolved_hash: str,
    round_table: str,
    domain_table: str,
    newer_table: str,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
    topology_grid_metres: float,
    created_at: datetime,
) -> tuple[int, dict[str, int]]:
    # Clip against nearby domain parts and subtract only the newer-round pieces
    # that touch each clipped piece (both via GiST), instead of one whole-island
    # intersection and one whole-group union per row. Every CTE is MATERIALIZED:
    # inlined, PostgreSQL re-evaluated ST_Intersection / ST_Difference once per
    # referencing filter. The inserted pieces are mirrored into newer_table (a
    # GiST-only table for this group) so lower rounds always find neighbours
    # through the spatial index.
    _ensure_safe_overlay_functions(conn)
    result = conn.execute(
        text(
            f"""
            WITH source_round AS MATERIALIZED (
                SELECT
                    jurisdiction, source_type, metric,
                    round_number, report_period,
                    db_low, db_high, db_value,
                    ST_ReducePrecision(geom, :topology_grid_m) AS precise_geom
                FROM "{round_table}"
                WHERE jurisdiction = :jur
                  AND source_type = :stype
                  AND metric = :metric
                  AND round_number = :round_number
            ),
            pairs AS MATERIALIZED (
                SELECT
                    s.jurisdiction, s.source_type, s.metric,
                    s.round_number, s.report_period,
                    s.db_low, s.db_high, s.db_value,
                    pg_temp.noise_safe_intersection(s.precise_geom, d.geom) AS ix
                FROM source_round s
                JOIN "{domain_table}" d ON s.precise_geom && d.geom
            ),
            clipped AS MATERIALIZED (
                SELECT
                    jurisdiction, source_type, metric,
                    round_number, report_period,
                    db_low, db_high, db_value,
                    ST_Multi(ST_CollectionExtract(ST_MakeValid(ix), 3)) AS clipped_geom
                FROM pairs
                WHERE ix IS NOT NULL
                  AND NOT ST_IsEmpty(ix)
                  AND ST_Area(ix) > 0
            ),
            masked AS MATERIALIZED (
                SELECT
                    c.jurisdiction, c.source_type, c.metric,
                    c.round_number, c.report_period,
                    c.db_low, c.db_high, c.db_value,
                    CASE
                        WHEN nb.geom IS NULL OR ST_IsEmpty(nb.geom) THEN c.clipped_geom
                        -- Fixed-precision OverlayNG (gridSize) instead of
                        -- ST_ReducePrecision(union) + ST_Difference: snapping a
                        -- valid neighbour union separately raised GEOS
                        -- "side location conflict" TopologyExceptions.
                        ELSE ST_Multi(ST_CollectionExtract(
                                 ST_MakeValid(
                                     pg_temp.noise_safe_difference(
                                         c.clipped_geom,
                                         nb.geom,
                                         :topology_grid_m
                                     )
                                 ), 3
                             ))
                    END AS effective_geom
                FROM clipped c
                LEFT JOIN LATERAL (
                    SELECT ST_UnaryUnion(ST_Collect(r.geom)) AS geom
                    FROM "{newer_table}" r
                    WHERE r.geom && c.clipped_geom
                ) nb ON true
                WHERE c.clipped_geom IS NOT NULL
                  AND NOT ST_IsEmpty(c.clipped_geom)
            ),
            subdivided AS MATERIALIZED (
                SELECT
                    m.jurisdiction, m.source_type, m.metric,
                    m.round_number, m.report_period,
                    m.db_low, m.db_high, m.db_value,
                    ST_Multi((ST_Dump(ST_Subdivide(m.effective_geom, 256))).geom) AS geom
                FROM masked m
                WHERE m.effective_geom IS NOT NULL
                  AND NOT ST_IsEmpty(m.effective_geom)
                  AND ST_Area(m.effective_geom) > 0
            ),
            inserted AS (
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
                RETURNING geom
            )
            INSERT INTO "{newer_table}" (geom)
            SELECT geom FROM inserted
            """
        ),
        {
            "resolved_hash": noise_resolved_hash,
            "jur": jurisdiction,
            "stype": source_type,
            "metric": metric,
            "round_number": round_number,
            "topology_grid_m": topology_grid_metres,
        },
    )
    conn.execute(text(f'ANALYZE "{newer_table}"'))
    fallbacks = {
        str(kind): int(count)
        for kind, count in conn.execute(
            text("SELECT kind, count(*) FROM pg_temp.noise_resolve_fallbacks GROUP BY kind")
        )
    }
    return max(int(result.rowcount or 0), 0), fallbacks


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
                    sha256(string_agg(source_refs_hash, '' ORDER BY source_refs_hash)::bytea),
                    'hex'
                ) AS source_refs_hash
            -- Dissolve pass 2 copies one band group's count onto every dumped
            -- polygon; summing those rows multiplied it (billions for NI road)
            -- and overflowed the INTEGER column. Count each band group once.
            FROM (
                SELECT DISTINCT
                    jurisdiction, source_type, metric, round_number,
                    report_period, db_low, db_high, db_value,
                    source_dataset, source_layer,
                    source_ref_count, source_refs_hash
                FROM "{round_table}"
                WHERE jurisdiction = :jur
                  AND source_type = :stype
                  AND metric = :metric
                  AND round_number = :round_number
            ) AS band_groups
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
