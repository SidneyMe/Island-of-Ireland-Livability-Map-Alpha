"""
Two-pass chunked dissolve for the noise artifact pipeline.

Pass 1: dissolve within EPSG:2157 processing tiles (default 10 km grid).
Pass 2: dissolve across tile boundaries.

Both passes run entirely in PostGIS on EPSG:2157 geometry.
Staging tables are UNLOGGED (rebuildable; WAL durability not needed).
"""
from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

# PostGIS version threshold for ST_SquareGrid availability
_POSTGIS_SQUAREGRID_MIN = (3, 1)


def _progress(progress_cb, message: str) -> None:
    if progress_cb:
        progress_cb("detail", detail=message, force_log=True)
    else:
        print(f"[noise] {message}", flush=True)


def _timing(progress_cb, label: str, seconds: float) -> None:
    _progress(progress_cb, f"[noise:timing] {label} {seconds:.1f}s")


def _postgis_version(engine: Engine) -> tuple[int, int]:
    with engine.connect() as conn:
        raw = conn.execute(text("SELECT PostGIS_Lib_Version()")).scalar() or "0.0"
    parts = str(raw).split(".")
    try:
        return (int(parts[0]), int(parts[1]))
    except (IndexError, ValueError):
        return (0, 0)


def _has_square_grid(engine: Engine) -> bool:
    ver = _postgis_version(engine)
    return ver >= _POSTGIS_SQUAREGRID_MIN


def _staging_table_name(source_hash: str, suffix: str) -> str:
    return f"_noise_{suffix}_{source_hash[:12]}"


def dissolve_noise_into_staging(
    engine: Engine,
    *,
    source_hash: str,
    resolved_hash: str,
    tile_size_metres: float = 10_000.0,
    topology_grid_metres: float = 0.1,
    progress_cb=None,
) -> tuple[str, str]:
    """
    Run two-pass chunked dissolve over noise_normalized for a given source_hash.

    Returns (dissolve_staging_name, round_staging_name) — the caller is responsible
    for dropping these tables after noise_resolved_display is populated.
    """
    total_started = time.perf_counter()
    use_square_grid = _has_square_grid(engine)
    dissolve_table = _staging_table_name(source_hash, "dissolve_staging")
    round_table = _staging_table_name(resolved_hash, "round_staging")

    indexes_seconds = 0.0
    with engine.begin() as conn:
        _create_dissolve_staging(conn, dissolve_table)
        _progress(progress_cb, "dissolve pass 1 start")
        pass1_started = time.perf_counter()
        n1 = _pass1_dissolve(
            conn, dissolve_table,
            source_hash=source_hash,
            tile_size_metres=tile_size_metres,
            topology_grid_metres=topology_grid_metres,
            use_square_grid=use_square_grid,
        )
        _timing(progress_cb, "dissolve.pass1", time.perf_counter() - pass1_started)
        _progress(progress_cb, f"dissolve pass 1 done: {n1} rows")
        indexes_started = time.perf_counter()
        _add_staging_indexes(conn, dissolve_table)
        indexes_seconds += time.perf_counter() - indexes_started
        _create_round_staging(conn, round_table)
        _progress(progress_cb, "dissolve pass 2 start")
        pass2_started = time.perf_counter()
        n2 = _pass2_dissolve(conn, dissolve_table, round_table)
        _timing(progress_cb, "dissolve.pass2", time.perf_counter() - pass2_started)
        _progress(progress_cb, f"dissolve pass 2 done: {n2} rows")
        indexes_started = time.perf_counter()
        _add_staging_indexes(conn, round_table)
        indexes_seconds += time.perf_counter() - indexes_started

    _timing(progress_cb, "dissolve.indexes", indexes_seconds)
    _timing(progress_cb, "dissolve.total", time.perf_counter() - total_started)
    log.info("dissolve complete: dissolve_table=%s round_table=%s", dissolve_table, round_table)
    return dissolve_table, round_table


def drop_staging_tables(engine: Engine, dissolve_table: str, round_table: str) -> None:
    """Drop both staging tables created by dissolve_noise_into_staging."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{dissolve_table}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{round_table}"'))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_STAGING_COLUMNS = """
    jurisdiction  TEXT,
    source_type   TEXT,
    metric        TEXT,
    round_number  INTEGER,
    report_period TEXT,
    db_low        DOUBLE PRECISION,
    db_high       DOUBLE PRECISION,
    db_value      TEXT,
    source_dataset TEXT,
    source_layer   TEXT,
    source_ref_count   INTEGER,
    source_refs_hash   TEXT,
    geom          GEOMETRY(MultiPolygon, 2157)
"""


def _create_dissolve_staging(conn, table_name: str) -> None:
    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
    conn.execute(text(
        f'CREATE UNLOGGED TABLE "{table_name}" ({_STAGING_COLUMNS})'
    ))


def _create_round_staging(conn, table_name: str) -> None:
    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
    conn.execute(text(
        f'CREATE UNLOGGED TABLE "{table_name}" ({_STAGING_COLUMNS})'
    ))


def _pass1_dissolve(
    conn,
    dissolve_table: str,
    *,
    source_hash: str,
    tile_size_metres: float,
    topology_grid_metres: float,
    use_square_grid: bool,
) -> int:
    """Dissolve within processing tiles (EPSG:2157 grid)."""
    if use_square_grid:
        grid_cte = """
            processing_grid AS (
                SELECT (ST_SquareGrid(:tile_size_m, (SELECT ext FROM domain_bounds))).geom AS tile_geom
            )"""
    else:
        # Fallback: generate_series grid for PostGIS < 3.1
        grid_cte = """
            processing_grid AS (
                SELECT ST_MakeEnvelope(
                    ST_XMin(ext) + x_off * :tile_size_m,
                    ST_YMin(ext) + y_off * :tile_size_m,
                    ST_XMin(ext) + (x_off + 1) * :tile_size_m,
                    ST_YMin(ext) + (y_off + 1) * :tile_size_m,
                    2157
                ) AS tile_geom
                FROM domain_bounds,
                     generate_series(0, CEIL((ST_XMax(ext) - ST_XMin(ext)) / :tile_size_m)::int - 1) AS x_off,
                     generate_series(0, CEIL((ST_YMax(ext) - ST_YMin(ext)) / :tile_size_m)::int - 1) AS y_off
            )"""

    result = conn.execute(
        text(
            f"""
            WITH domain_bounds AS (
                SELECT ST_Extent(geom)::geometry AS ext
                FROM noise_normalized
                WHERE noise_source_hash = :source_hash
            ),
            {grid_cte},
            tiled AS (
                SELECT
                    n.jurisdiction, n.source_type, n.metric, n.round_number,
                    n.report_period, n.db_low, n.db_high, n.db_value,
                    n.source_dataset, n.source_layer, n.source_ref,
                    g.tile_geom,
                    ST_ReducePrecision(
                        ST_Multi(ST_CollectionExtract(
                            ST_MakeValid(i.ix), 3
                        )),
                        :topology_grid_m
                    ) AS tiled_geom
                FROM noise_normalized n
                JOIN processing_grid g ON n.geom && g.tile_geom
                JOIN LATERAL (
                    SELECT ST_Intersection(n.geom, g.tile_geom) AS ix
                ) i ON true
                WHERE n.noise_source_hash = :source_hash
                  AND i.ix IS NOT NULL
                  AND NOT ST_IsEmpty(i.ix)
                  AND ST_Area(i.ix) > 0
            ),
            pass1 AS (
                SELECT
                    jurisdiction, source_type, metric, round_number,
                    report_period, db_low, db_high, db_value,
                    source_dataset, source_layer, tile_geom,
                    COUNT(DISTINCT source_ref) AS source_ref_count,
                    encode(
                        sha256(string_agg(COALESCE(source_ref,'') ORDER BY source_ref)::bytea),
                        'hex'
                    ) AS source_refs_hash,
                    ST_Multi(ST_CollectionExtract(
                        ST_MakeValid(ST_UnaryUnion(ST_Collect(tiled_geom))), 3
                    )) AS dissolved_geom
                FROM tiled
                GROUP BY jurisdiction, source_type, metric, round_number,
                         report_period, db_low, db_high, db_value,
                         source_dataset, source_layer, tile_geom
            )
            INSERT INTO "{dissolve_table}" (
                jurisdiction, source_type, metric, round_number,
                report_period, db_low, db_high, db_value,
                source_dataset, source_layer,
                source_ref_count, source_refs_hash, geom
            )
            SELECT
                jurisdiction, source_type, metric, round_number,
                report_period, db_low, db_high, db_value,
                source_dataset, source_layer,
                source_ref_count, source_refs_hash, dissolved_geom
            FROM pass1
            WHERE dissolved_geom IS NOT NULL
              AND NOT ST_IsEmpty(dissolved_geom)
              AND ST_Area(dissolved_geom) > 0
            """
        ),
        {
            "source_hash": source_hash,
            "tile_size_m": tile_size_metres,
            "topology_grid_m": topology_grid_metres,
        },
    )
    return max(int(result.rowcount or 0), 0)


def _add_staging_indexes(conn, table_name: str) -> None:
    """Add GiST geometry index and composite btree index to a staging table, then ANALYZE."""
    conn.execute(text(
        f'CREATE INDEX ON "{table_name}" '
        f'(jurisdiction, source_type, metric, round_number)'
    ))
    conn.execute(text(
        f'CREATE INDEX ON "{table_name}" USING GIST (geom)'
    ))
    conn.execute(text(f'ANALYZE "{table_name}"'))


def _pass2_dissolve(conn, dissolve_table: str, round_table: str) -> int:
    """Dissolve across tile boundaries, producing the final pre-resolve staging."""
    result = conn.execute(
        text(
            f"""
            WITH pass2 AS (
                SELECT
                    jurisdiction, source_type, metric, round_number,
                    report_period, db_low, db_high, db_value,
                    source_dataset, source_layer,
                    SUM(source_ref_count) AS source_ref_count,
                    encode(
                        sha256(string_agg(source_refs_hash ORDER BY source_refs_hash)::bytea),
                        'hex'
                    ) AS source_refs_hash,
                    ST_Multi(ST_CollectionExtract(
                        ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))), 3
                    )) AS final_geom
                FROM "{dissolve_table}"
                GROUP BY jurisdiction, source_type, metric, round_number,
                         report_period, db_low, db_high, db_value,
                         source_dataset, source_layer
            ),
            exploded AS (
                SELECT
                    jurisdiction, source_type, metric, round_number,
                    report_period, db_low, db_high, db_value,
                    source_dataset, source_layer,
                    source_ref_count, source_refs_hash,
                    ST_Multi((ST_Dump(final_geom)).geom) AS geom
                FROM pass2
                WHERE final_geom IS NOT NULL AND NOT ST_IsEmpty(final_geom)
            )
            INSERT INTO "{round_table}" (
                jurisdiction, source_type, metric, round_number,
                report_period, db_low, db_high, db_value,
                source_dataset, source_layer,
                source_ref_count, source_refs_hash, geom
            )
            SELECT
                jurisdiction, source_type, metric, round_number,
                report_period, db_low, db_high, db_value,
                source_dataset, source_layer,
                source_ref_count, source_refs_hash, geom
            FROM exploded
            WHERE geom IS NOT NULL
              AND NOT ST_IsEmpty(geom)
              AND ST_Area(geom) > 0
            """
        ),
    )
    return max(int(result.rowcount or 0), 0)

