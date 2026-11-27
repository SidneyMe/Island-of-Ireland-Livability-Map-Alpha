"""
Two-pass chunked dissolve for the noise artifact pipeline.

Pass 1: dissolve within EPSG:2157 processing tiles (default 10 km grid).
Pass 2: dissolve across tile boundaries.

Both passes run entirely in PostGIS on EPSG:2157 geometry.
Staging tables are UNLOGGED (rebuildable; WAL durability not needed).
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

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
    simplify_tolerance_m: float | None = None,
    progress_cb=None,
) -> tuple[str, str]:
    """
    Run two-pass chunked dissolve over noise_normalized for a given source_hash.

    Returns (dissolve_staging_name, round_staging_name) — the caller is responsible
    for dropping these tables after noise_resolved_display is populated.
    """
    total_started = time.perf_counter()
    dissolve_table = _staging_table_name(source_hash, "dissolve_staging")
    round_table = _staging_table_name(resolved_hash, "round_staging")

    # Both passes commit in one transaction and the GiST index is added last,
    # so an indexed, non-empty round table is a finished dissolve for this
    # resolved_hash (kept by the builder after a later failure). Reuse it
    # instead of repeating hours of dissolve work.
    reused_rows = round_staging_complete_rows(engine, round_table)
    if reused_rows:
        _progress(
            progress_cb,
            f"reusing round staging {round_table} rows={reused_rows:,} (dissolve skipped)",
        )
        return dissolve_table, round_table

    use_square_grid = _has_square_grid(engine)
    workers = _dissolve_workers()
    if workers > 1:
        _parallel_dissolve(
            engine,
            dissolve_table=dissolve_table,
            round_table=round_table,
            source_hash=source_hash,
            tile_size_metres=tile_size_metres,
            topology_grid_metres=topology_grid_metres,
            simplify_tolerance_m=float(simplify_tolerance_m or 0.0),
            use_square_grid=use_square_grid,
            workers=workers,
            progress_cb=progress_cb,
        )
        _timing(progress_cb, "dissolve.total", time.perf_counter() - total_started)
        log.info("dissolve complete: dissolve_table=%s round_table=%s", dissolve_table, round_table)
        return dissolve_table, round_table

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
            simplify_tolerance_m=float(simplify_tolerance_m or 0.0),
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


def round_staging_complete_rows(engine: Engine, round_table: str) -> int:
    """Row count of a finished round staging table, or 0 when absent/incomplete.

    UNLOGGED tables come back empty after a PostgreSQL crash, so an empty table
    is treated as incomplete even when its index survived.
    """
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:qualified) IS NOT NULL"),
            {"qualified": f'"{round_table}"'},
        ).scalar()
        if not exists:
            return 0
        has_gist = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_indexes "
                "WHERE tablename = :table AND indexdef ILIKE '%USING gist%')"
            ),
            {"table": round_table},
        ).scalar()
        if not has_gist:
            return 0
        return int(conn.execute(text(f'SELECT count(*) FROM "{round_table}"')).scalar() or 0)


def drop_staging_tables(engine: Engine, dissolve_table: str, round_table: str | None) -> None:
    """Drop staging tables created by dissolve_noise_into_staging (round_table=None keeps it)."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{dissolve_table}"'))
        if round_table:
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
    simplify_tolerance_m: float,
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
            WITH source_rows AS (
                SELECT
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    report_period,
                    db_low,
                    db_high,
                    db_value,
                    source_dataset,
                    source_layer,
                    source_ref,
                    CASE
                        WHEN :simplify_m > 0
                             AND source_type IN ('road', 'rail')
                        THEN ST_Multi(ST_CollectionExtract(
                            ST_MakeValid(
                                ST_SimplifyPreserveTopology(geom, :simplify_m)
                            ),
                            3
                        ))
                        ELSE geom
                    END AS geom
                FROM noise_normalized
                WHERE noise_source_hash = :source_hash
            ),
            domain_bounds AS (
                -- box2d carries no SRID; without ST_SetSRID the square-grid
                -- tiles are SRID 0 and ST_Intersection with 2157 rows fails.
                SELECT ST_SetSRID(ST_Extent(geom)::geometry, 2157) AS ext
                FROM source_rows
                WHERE geom IS NOT NULL
                  AND NOT ST_IsEmpty(geom)
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
                FROM source_rows n
                JOIN processing_grid g ON n.geom && g.tile_geom
                JOIN LATERAL (
                    SELECT ST_Intersection(n.geom, g.tile_geom) AS ix
                ) i ON true
                WHERE i.ix IS NOT NULL
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
                        sha256(string_agg(COALESCE(source_ref,''), '' ORDER BY source_ref)::bytea),
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
            "simplify_m": float(simplify_tolerance_m),
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
                        sha256(string_agg(source_refs_hash, '' ORDER BY source_refs_hash)::bytea),
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


# ---------------------------------------------------------------------------
# Parallel dissolve (NOISE_DISSOLVE_WORKERS > 1)
# ---------------------------------------------------------------------------
#
# The serial path above is one INSERT ... SELECT, which PostgreSQL never runs
# in parallel, so the whole accurate dissolve (~190M road vertices) used one
# core. The work splits cleanly: source-row simplification is per row, pass 1
# groups by processing tile, and pass 2 by attribute group, so each bucket of
# rows/tiles/groups is computed by the same expressions as the serial
# statement, on its own connection.

_BUCKETS_PER_WORKER = 8

_SOURCE_GEOM_SQL = """
    CASE
        WHEN :simplify_m > 0
             AND source_type IN ('road', 'rail')
        THEN ST_Multi(ST_CollectionExtract(
            ST_MakeValid(
                ST_SimplifyPreserveTopology(geom, :simplify_m)
            ),
            3
        ))
        ELSE geom
    END
"""

_GROUP_KEY_SQL = (
    "concat_ws('|', jurisdiction, source_type, metric, round_number::text, report_period, "
    "db_low::text, db_high::text, db_value, source_dataset, source_layer)"
)


def _dissolve_workers() -> int:
    raw = (os.getenv("NOISE_DISSOLVE_WORKERS") or "").strip()
    if not raw:
        return 1
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"NOISE_DISSOLVE_WORKERS must be an integer; got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"NOISE_DISSOLVE_WORKERS must be > 0; got {value}")
    return value


def _bucket_expr(key_sql: str) -> str:
    # hashtext() is int4; shift into [0, 2^32) so the modulo is never negative.
    return f"mod(hashtext({key_sql})::bigint + 2147483648, :buckets)"


def _grid_cte(use_square_grid: bool) -> str:
    if use_square_grid:
        return """
            processing_grid AS (
                SELECT (ST_SquareGrid(:tile_size_m, (SELECT ext FROM domain_bounds))).geom AS tile_geom
            )"""
    return """
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


def _run_bucketed(engine: Engine, sql: str, params: dict, *, workers: int, label: str, progress_cb) -> int:
    """Run ``sql`` once per bucket (``:buckets``/``:bucket``) across ``workers`` connections."""
    buckets = workers * _BUCKETS_PER_WORKER
    # NullPool: every worker gets its own backend regardless of the shared
    # engine's pool size.
    worker_engine = create_engine(engine.url, poolclass=NullPool, future=True)
    started = time.perf_counter()

    def _one(bucket: int) -> int:
        with worker_engine.begin() as conn:
            result = conn.execute(text(sql), {**params, "buckets": buckets, "bucket": bucket})
            return max(int(result.rowcount or 0), 0)

    total = 0
    done = 0
    report_every = max(1, buckets // 8)
    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="noise-dissolve") as executor:
            for rows in executor.map(_one, range(buckets)):
                total += rows
                done += 1
                if done % report_every == 0 or done == buckets:
                    _progress(
                        progress_cb,
                        f"{label}: {done}/{buckets} buckets rows={total:,} "
                        f"elapsed={time.perf_counter() - started:.0f}s",
                    )
    finally:
        worker_engine.dispose()
    return total


def _parallel_dissolve(
    engine: Engine,
    *,
    dissolve_table: str,
    round_table: str,
    source_hash: str,
    tile_size_metres: float,
    topology_grid_metres: float,
    simplify_tolerance_m: float,
    use_square_grid: bool,
    workers: int,
    progress_cb,
) -> None:
    src_table = f"{dissolve_table}_src"
    tiles_table = f"{dissolve_table}_tiles"
    _progress(progress_cb, f"dissolve parallel: workers={workers}")
    with engine.begin() as conn:
        _create_dissolve_staging(conn, dissolve_table)
        _create_round_staging(conn, round_table)
        conn.execute(text(f'DROP TABLE IF EXISTS "{src_table}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{tiles_table}"'))
        conn.execute(text(
            f'CREATE UNLOGGED TABLE "{src_table}" ('
            "jurisdiction TEXT, source_type TEXT, metric TEXT, round_number INTEGER, "
            "report_period TEXT, db_low DOUBLE PRECISION, db_high DOUBLE PRECISION, "
            "db_value TEXT, source_dataset TEXT, source_layer TEXT, source_ref TEXT, "
            "geom geometry)"
        ))

    # Same per-row expression as the serial source_rows CTE.
    step_started = time.perf_counter()
    source_key = "concat_ws('|', source_dataset, source_layer, metric, db_value, source_ref)"
    _run_bucketed(
        engine,
        f"""
        INSERT INTO "{src_table}"
        SELECT
            jurisdiction, source_type, metric, round_number, report_period,
            db_low, db_high, db_value, source_dataset, source_layer, source_ref,
            {_SOURCE_GEOM_SQL} AS geom
        FROM noise_normalized
        WHERE noise_source_hash = :source_hash
          AND {_bucket_expr(source_key)} = :bucket
        """,
        {"source_hash": source_hash, "simplify_m": float(simplify_tolerance_m)},
        workers=workers,
        label="dissolve simplify",
        progress_cb=progress_cb,
    )
    _timing(progress_cb, "dissolve.simplify", time.perf_counter() - step_started)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE INDEX ON "{src_table}" USING GIST (geom)'))
        conn.execute(text(f'ANALYZE "{src_table}"'))
        conn.execute(
            text(
                f"""
                CREATE UNLOGGED TABLE "{tiles_table}" AS
                WITH domain_bounds AS (
                    SELECT ST_SetSRID(ST_Extent(geom)::geometry, 2157) AS ext
                    FROM "{src_table}"
                    WHERE geom IS NOT NULL
                      AND NOT ST_IsEmpty(geom)
                ),
                {_grid_cte(use_square_grid)}
                SELECT row_number() OVER () AS tile_id, tile_geom
                FROM processing_grid
                """
            ),
            {"tile_size_m": tile_size_metres},
        )
        tile_count = int(conn.execute(text(f'SELECT count(*) FROM "{tiles_table}"')).scalar() or 0)
    _progress(progress_cb, f"dissolve parallel: {tile_count:,} processing tiles")

    _progress(progress_cb, "dissolve pass 1 start")
    step_started = time.perf_counter()
    n1 = _run_bucketed(
        engine,
        f"""
        WITH tiled AS (
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
            FROM "{src_table}" n
            JOIN "{tiles_table}" g ON n.geom && g.tile_geom
            JOIN LATERAL (
                SELECT ST_Intersection(n.geom, g.tile_geom) AS ix
            ) i ON true
            WHERE mod(g.tile_id, :buckets) = :bucket
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
                    sha256(string_agg(COALESCE(source_ref,''), '' ORDER BY source_ref)::bytea),
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
        """,
        {"topology_grid_m": topology_grid_metres},
        workers=workers,
        label="dissolve pass 1",
        progress_cb=progress_cb,
    )
    _timing(progress_cb, "dissolve.pass1", time.perf_counter() - step_started)
    _progress(progress_cb, f"dissolve pass 1 done: {n1} rows")

    indexes_seconds = 0.0
    indexes_started = time.perf_counter()
    with engine.begin() as conn:
        _add_staging_indexes(conn, dissolve_table)
        conn.execute(text(f'DROP TABLE IF EXISTS "{src_table}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{tiles_table}"'))
    indexes_seconds += time.perf_counter() - indexes_started

    _progress(progress_cb, "dissolve pass 2 start")
    step_started = time.perf_counter()
    n2 = _run_bucketed(
        engine,
        f"""
        WITH pass2 AS (
            SELECT
                jurisdiction, source_type, metric, round_number,
                report_period, db_low, db_high, db_value,
                source_dataset, source_layer,
                SUM(source_ref_count) AS source_ref_count,
                encode(
                    sha256(string_agg(source_refs_hash, '' ORDER BY source_refs_hash)::bytea),
                    'hex'
                ) AS source_refs_hash,
                ST_Multi(ST_CollectionExtract(
                    ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))), 3
                )) AS final_geom
            FROM "{dissolve_table}"
            WHERE {_bucket_expr(_GROUP_KEY_SQL)} = :bucket
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
        """,
        {},
        workers=workers,
        label="dissolve pass 2",
        progress_cb=progress_cb,
    )
    _timing(progress_cb, "dissolve.pass2", time.perf_counter() - step_started)
    _progress(progress_cb, f"dissolve pass 2 done: {n2} rows")

    # The GiST index on the round table marks a finished dissolve (see
    # round_staging_complete_rows), so it is added only after every bucket.
    indexes_started = time.perf_counter()
    with engine.begin() as conn:
        _add_staging_indexes(conn, round_table)
    indexes_seconds += time.perf_counter() - indexes_started
    _timing(progress_cb, "dissolve.indexes", indexes_seconds)

