from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from config import NOISE_PUBLISH_USE_COPY

from ._dependencies import (
    Column,
    Connection,
    DateTime,
    Float,
    Geometry,
    Integer,
    MetaData,
    Table,
    Text,
    insert,
    text,
)
from .common import ProgressCallback
from .schema import _quote_identifier
from .tables import noise_polygons
from .write_common import _chunked, _qualified_table_name, _table_column_names


NOISE_STAGE_BATCH_SIZE = 256


def _noise_stage_table(staging_name: str) -> Table:
    return Table(
        staging_name,
        MetaData(),
        Column("build_key", Text, nullable=False),
        Column("config_hash", Text, nullable=False),
        Column("import_fingerprint", Text, nullable=False),
        Column("jurisdiction", Text, nullable=False),
        Column("source_type", Text, nullable=False),
        Column("metric", Text, nullable=False),
        Column("round_number", Integer, nullable=False),
        Column("report_period", Text, nullable=True),
        Column("db_low", Float, nullable=True),
        Column("db_high", Float, nullable=True),
        Column("db_value", Text, nullable=False),
        Column("source_dataset", Text, nullable=False),
        Column("source_layer", Text, nullable=False),
        Column("source_ref", Text, nullable=False),
        Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )


def _prepare_noise_stage_chunk(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .common import root_module
    root = root_module()
    prepared: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["geom"] = root.from_shape(payload["geom"], srid=4326)
        prepared.append(payload)
    return prepared


_NOISE_STAGE_COLUMNS: tuple[str, ...] = (
    "build_key",
    "config_hash",
    "import_fingerprint",
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
    "geom",
    "created_at",
)


def _stage_noise_candidate_rows_via_copy(
    connection: Connection,
    *,
    staging_quoted: str,
    noise_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> int:
    import shapely.wkb as shapely_wkb

    column_list = ", ".join(_quote_identifier(name) for name in _NOISE_STAGE_COLUMNS)
    copy_sql = f"COPY {staging_quoted} ({column_list}) FROM STDIN"
    dbapi_conn = connection.connection.driver_connection
    cursor = dbapi_conn.cursor()
    total_inserted = 0
    progress_step = 5_000
    next_progress = progress_step
    if progress_cb is not None:
        progress_cb("live_start", detail="staging noise_polygons candidates (COPY)")
    else:
        print("  staging noise_polygons candidates (COPY)...", flush=True)
    try:
        with cursor.copy(copy_sql) as copy:
            for row in noise_rows:
                geom = row["geom"]
                ewkb_hex = shapely_wkb.dumps(geom, hex=True, srid=4326)
                copy.write_row(
                    (
                        row["build_key"],
                        row["config_hash"],
                        row["import_fingerprint"],
                        row["jurisdiction"],
                        row["source_type"],
                        row["metric"],
                        int(row["round_number"]),
                        row.get("report_period"),
                        row.get("db_low"),
                        row.get("db_high"),
                        row["db_value"],
                        row["source_dataset"],
                        row["source_layer"],
                        row["source_ref"],
                        ewkb_hex,
                        row["created_at"],
                    )
                )
                total_inserted += 1
                if progress_cb is not None and total_inserted >= next_progress:
                    progress_cb(
                        "detail",
                        detail=f"COPY staged {total_inserted:,} noise candidate rows",
                        force_log=True,
                    )
                    next_progress += progress_step
                elif progress_cb is None and total_inserted % progress_step == 0:
                    print(
                        f"  staging noise_polygons candidates (COPY): {total_inserted:,} rows so far...",
                        flush=True,
                    )
    finally:
        cursor.close()
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"COPY staged {total_inserted:,} noise candidate rows total",
            force_log=True,
        )
    return total_inserted


def _stage_noise_candidate_rows(
    connection: Connection,
    *,
    staging_quoted: str,
    noise_rows: Iterable[dict[str, Any]],
    progress_cb: ProgressCallback | None = None,
) -> int:
    if NOISE_PUBLISH_USE_COPY:
        return _stage_noise_candidate_rows_via_copy(
            connection,
            staging_quoted=staging_quoted,
            noise_rows=noise_rows,
            progress_cb=progress_cb,
        )

    stage_table = _noise_stage_table(staging_quoted.strip('"'))
    total_inserted = 0
    chunk_index = 0
    if progress_cb is not None:
        progress_cb("live_start", detail="staging noise_polygons candidates")
    else:
        print("  staging noise_polygons candidates...", flush=True)

    for chunk in _chunked(noise_rows, size=NOISE_STAGE_BATCH_SIZE):
        chunk_index += 1
        chunk_size = len(chunk)
        connection.execute(insert(stage_table), _prepare_noise_stage_chunk(chunk))
        total_inserted += chunk_size
        if progress_cb is None:
            if chunk_index % 10 == 0:
                print(
                    f"  staging noise_polygons candidates: {total_inserted:,} rows so far...",
                    flush=True,
                )
            continue
        if chunk_index % 10 == 0 or chunk_size < NOISE_STAGE_BATCH_SIZE:
            progress_cb(
                "detail",
                detail=(
                    f"staged noise candidates batch {chunk_index:,}: "
                    f"{chunk_size:,} rows this batch | {total_inserted:,} rows total"
                ),
                force_log=True,
            )

    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"staged {total_inserted:,} noise candidate rows",
            force_log=True,
        )
    return total_inserted


def _create_noise_stage_indexes(
    connection: Connection,
    *,
    staging_quoted: str,
    progress_cb: ProgressCallback | None = None,
) -> None:
    if progress_cb is not None:
        progress_cb("detail", detail="indexing staged noise candidates", force_log=True)
    connection.execute(
        text(
            f"CREATE INDEX ON {staging_quoted} "
            "(build_key, jurisdiction, source_type, metric, round_number)"
        )
    )
    connection.execute(text(f"CREATE INDEX ON {staging_quoted} USING GIST (geom)"))
    connection.execute(text(f"ANALYZE {staging_quoted}"))


def _study_area_wkb(study_area_wgs84, *, simplify_tolerance: float = 0.0) -> bytes:
    if study_area_wgs84 is None:
        from shapely.geometry import box

        study_area_wgs84 = box(-180.0, -90.0, 180.0, 90.0)
    if simplify_tolerance > 0.0:
        study_area_wgs84 = study_area_wgs84.simplify(
            simplify_tolerance, preserve_topology=True
        )
    return bytes(study_area_wgs84.wkb)


def _noise_stage_groups(connection: Connection, *, staging_quoted: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            text(
                f"""
                SELECT
                    jurisdiction,
                    source_type,
                    metric,
                    round_number,
                    COUNT(*) AS row_count
                FROM {staging_quoted}
                GROUP BY jurisdiction, source_type, metric, round_number
                ORDER BY jurisdiction, source_type, metric, round_number DESC
                """
            )
        ).mappings()
    ]


def _materialize_noise_group_round(
    connection: Connection,
    *,
    staging_quoted: str,
    study_area_wkb: bytes,
    build_key: str,
    jurisdiction: str,
    source_type: str,
    metric: str,
    round_number: int,
) -> int:
    target_name = _qualified_table_name(noise_polygons)
    insert_columns = _table_column_names(noise_polygons)
    quoted_insert_columns = ", ".join(_quote_identifier(name) for name in insert_columns)
    attr_columns = [name for name in insert_columns if name != "geom"]
    base_columns = ", ".join(f"s.{_quote_identifier(name)}" for name in attr_columns)
    clipped_columns = ", ".join(f"c.{_quote_identifier(name)}" for name in attr_columns)
    select_columns = ", ".join(f"r.{_quote_identifier(name)}" for name in attr_columns)
    final_select_columns = ", ".join(
        "geom" if name == "geom" else _quote_identifier(name)
        for name in insert_columns
    )
    result = connection.execute(
        text(
            f"""
            WITH study AS (
                SELECT ST_SetSRID(ST_GeomFromWKB(:study_area_wkb), 4326) AS geom
            ),
            clipped AS (
                SELECT
                    {base_columns},
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Intersection(
                                CASE
                                    WHEN ST_IsValid(s.geom) THEN s.geom
                                    ELSE ST_MakeValid(s.geom)
                                END,
                                study.geom
                            )
                        ),
                        3
                    ) AS geom
                FROM {staging_quoted} AS s
                CROSS JOIN study
                WHERE s.build_key = :build_key
                  AND s.jurisdiction = :jurisdiction
                  AND s.source_type = :source_type
                  AND s.metric = :metric
                  AND s.round_number = :round_number
                  AND s.geom && study.geom
                  AND ST_Intersects(s.geom, study.geom)
            ),
            resolved AS (
                SELECT
                    {clipped_columns},
                    CASE
                        WHEN newer.geom IS NULL OR ST_IsEmpty(newer.geom) THEN c.geom
                        ELSE ST_CollectionExtract(
                            ST_MakeValid(ST_Difference(c.geom, newer.geom)),
                            3
                        )
                    END AS geom
                FROM clipped AS c
                LEFT JOIN LATERAL (
                    SELECT ST_UnaryUnion(ST_Collect(n.geom)) AS geom
                    FROM {target_name} AS n
                    WHERE n.build_key = :build_key
                      AND n.jurisdiction = :jurisdiction
                      AND n.source_type = :source_type
                      AND n.metric = :metric
                      AND n.geom && c.geom
                      AND ST_Intersects(n.geom, c.geom)
                ) AS newer ON TRUE
                WHERE c.geom IS NOT NULL
                  AND NOT ST_IsEmpty(c.geom)
            ),
            pieces AS (
                SELECT
                    {select_columns},
                    piece.geom AS geom
                FROM resolved AS r
                CROSS JOIN LATERAL ST_Subdivide(r.geom, 256) AS piece(geom)
                WHERE r.geom IS NOT NULL
                  AND NOT ST_IsEmpty(r.geom)
            )
            INSERT INTO {target_name} ({quoted_insert_columns})
            SELECT
                {final_select_columns}
            FROM pieces
            WHERE NOT ST_IsEmpty(geom)
              AND ST_Dimension(geom) = 2
              AND ST_Area(geom) > 0
            """
        ),
        {
            "study_area_wkb": study_area_wkb,
            "build_key": build_key,
            "jurisdiction": jurisdiction,
            "source_type": source_type,
            "metric": metric,
            "round_number": int(round_number),
        },
    )
    return max(int(result.rowcount or 0), 0)


def _count_noise_values(
    connection: Connection,
    *,
    build_key: str,
    field_name: str,
) -> dict[str, int]:
    target_name = _qualified_table_name(noise_polygons)
    quoted_field = _quote_identifier(field_name)
    rows = connection.execute(
        text(
            f"""
            SELECT {quoted_field} AS value, COUNT(*) AS row_count
            FROM {target_name}
            WHERE build_key = :build_key
            GROUP BY {quoted_field}
            ORDER BY {quoted_field}
            """
        ),
        {"build_key": build_key},
    ).mappings()
    return {
        str(row["value"]): int(row["row_count"])
        for row in rows
        if row["value"] is not None
    }


def _update_noise_summary_from_database(
    connection: Connection,
    *,
    build_key: str,
    summary_json: dict[str, Any],
) -> None:
    noise_counts = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="jurisdiction",
    )
    summary_json["noise_enabled"] = bool(noise_counts)
    summary_json["noise_counts"] = noise_counts
    summary_json["noise_source_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="source_type",
    )
    summary_json["noise_metric_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="metric",
    )
    summary_json["noise_band_counts"] = _count_noise_values(
        connection,
        build_key=build_key,
        field_name="db_value",
    )
    target_name = _qualified_table_name(noise_polygons)
    metric_band_rows = connection.execute(
        text(
            f"""
            SELECT metric, db_value, COUNT(*) AS row_count
            FROM {target_name}
            WHERE build_key = :build_key
              AND metric IS NOT NULL
              AND db_value IS NOT NULL
            GROUP BY metric, db_value
            ORDER BY metric, db_value
            """
        ),
        {"build_key": build_key},
    ).mappings()
    band_counts_by_metric: dict[str, dict[str, int]] = {}
    for row in metric_band_rows:
        metric = str(row.get("metric") or "").strip()
        band = str(row.get("db_value") or "").strip()
        row_count = int(row.get("row_count") or 0)
        if not metric or not band or row_count <= 0:
            continue
        band_counts_by_metric.setdefault(metric, {})[band] = row_count
    summary_json["noise_band_counts_by_metric"] = band_counts_by_metric


def _materialize_noise_polygons_from_stage(
    connection: Connection,
    *,
    staging_quoted: str,
    build_key: str,
    study_area_wgs84,
    progress_cb: ProgressCallback | None = None,
) -> int:
    groups = _noise_stage_groups(connection, staging_quoted=staging_quoted)
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=f"materializing noise fallback in PostGIS for {len(groups):,} group-rounds",
            force_log=True,
        )
    # Simplify the study area before using it as a clip boundary in PostGIS.
    # The ungeneralised island boundary is ~10 MB of WKB; ST_Intersection against
    # it for tens of thousands of rows takes hours. 0.005° ≈ 500 m tolerance is
    # more than adequate for trimming noise polygons to the island boundary.
    study_wkb = _study_area_wkb(study_area_wgs84, simplify_tolerance=0.005)
    total_inserted = 0
    for group_index, group in enumerate(groups, start=1):
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    "materializing noise "
                    f"{group_index:,}/{len(groups):,}: "
                    f"{group['jurisdiction']} {group['source_type']} "
                    f"{group['metric']} round {group['round_number']} "
                    f"({int(group['row_count']):,} staged rows)"
                ),
                force_log=True,
            )
        inserted = _materialize_noise_group_round(
            connection,
            staging_quoted=staging_quoted,
            study_area_wkb=study_wkb,
            build_key=build_key,
            jurisdiction=str(group["jurisdiction"]),
            source_type=str(group["source_type"]),
            metric=str(group["metric"]),
            round_number=int(group["round_number"]),
        )
        total_inserted += inserted
        if progress_cb is not None:
            progress_cb(
                "detail",
                detail=(
                    "materialized noise "
                    f"{group['jurisdiction']} {group['source_type']} "
                    f"{group['metric']} round {group['round_number']}: "
                    f"{inserted:,} final polygons"
                ),
                force_log=True,
            )
    return total_inserted


def _clone_noise_polygons_from_prior_build(
    connection: Connection,
    *,
    new_build_key: str,
    new_config_hash: str,
    new_import_fingerprint: str,
    render_hash: str,
    noise_processing_hash: str | None,
    created_at: datetime,
    progress_cb: ProgressCallback | None = None,
) -> int:
    target_name = _qualified_table_name(noise_polygons)
    # Match on render_hash (config unchanged) OR noise_processing_hash (noise
    # inputs unchanged even if other config changed).  The noise_processing_hash
    # column is NULL for builds before migration 000013 so those are correctly
    # ignored by the IS NOT DISTINCT FROM / = comparison.
    params: dict[str, Any] = {
        "render_hash": render_hash,
        "new_build_key": new_build_key,
        "noise_processing_hash": noise_processing_hash,
    }
    prior = connection.execute(
        text(
            """
            SELECT build_key
            FROM build_manifest
            WHERE (
                render_hash = :render_hash
                OR (
                    CAST(:noise_processing_hash AS TEXT) IS NOT NULL
                    AND noise_processing_hash = :noise_processing_hash
                )
            )
              AND status = 'complete'
              AND build_key <> :new_build_key
              AND EXISTS (
                  SELECT 1 FROM noise_polygons np
                  WHERE np.build_key = build_manifest.build_key
              )
            ORDER BY completed_at DESC NULLS LAST
            LIMIT 1
            """
        ),
        params,
    ).scalar_one_or_none()
    if not prior:
        return 0
    if progress_cb is not None:
        progress_cb(
            "detail",
            detail=(
                f"reusing noise_polygons from prior build {str(prior)[:12]} "
                f"(render_hash match)"
            ),
            force_log=True,
        )
    result = connection.execute(
        text(
            f"""
            INSERT INTO {target_name} (
                build_key, config_hash, import_fingerprint, jurisdiction,
                source_type, metric, round_number, report_period, db_low,
                db_high, db_value, source_dataset, source_layer, source_ref,
                geom, created_at
            )
            SELECT
                :new_build_key, :new_config_hash, :new_import_fingerprint,
                jurisdiction, source_type, metric, round_number, report_period,
                db_low, db_high, db_value, source_dataset, source_layer,
                source_ref, geom, :created_at
            FROM {target_name}
            WHERE build_key = :prior_build_key
            """
        ),
        {
            "new_build_key": new_build_key,
            "new_config_hash": new_config_hash,
            "new_import_fingerprint": new_import_fingerprint,
            "prior_build_key": str(prior),
            "created_at": created_at,
        },
    )
    cloned = max(int(result.rowcount or 0), 0)
    if progress_cb is not None and cloned > 0:
        progress_cb(
            "detail",
            detail=f"cloned {cloned:,} noise_polygons rows from prior build",
            force_log=True,
        )
    return cloned


def _drain_iterable(rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    for _ in rows:
        count += 1
    return count


_GRID_ARTIFACT_BAND_COUNTS_BY_METRIC_SQL = text(
    """
    WITH raw_cells AS (
        SELECT
            g.metric,
            CASE
                WHEN g.db_value ~ '^[0-9]+-[0-9]+$' THEN split_part(g.db_value, '-', 1)::float8
                WHEN g.db_value ~ '^[0-9]+\\+$' THEN regexp_replace(g.db_value, '\\+', '')::float8
                WHEN g.db_low IS NOT NULL THEN g.db_low::float8
                ELSE NULL
            END AS raw_band_min
        FROM noise_grid_artifact g
        WHERE g.artifact_hash = :grid_artifact_hash
          AND g.source_type IN ('road', 'rail')
          AND g.metric IN ('Lden', 'Lnight')
    ),
    snapped AS (
        SELECT
            metric,
            CASE
                WHEN raw_band_min IS NULL THEN NULL
                WHEN metric = 'Lden' AND raw_band_min < 57.5 THEN 55
                WHEN metric = 'Lden' AND raw_band_min < 62.5 THEN 60
                WHEN metric = 'Lden' AND raw_band_min < 67.5 THEN 65
                WHEN metric = 'Lden' AND raw_band_min < 72.5 THEN 70
                WHEN metric = 'Lden' AND raw_band_min < 77.5 THEN 75
                WHEN metric = 'Lden' THEN 80
                WHEN metric = 'Lnight' AND raw_band_min < 50 THEN 45
                WHEN metric = 'Lnight' AND raw_band_min < 55 THEN 50
                WHEN metric = 'Lnight' AND raw_band_min < 60 THEN 55
                WHEN metric = 'Lnight' AND raw_band_min < 65 THEN 60
                WHEN metric = 'Lnight' AND raw_band_min < 70 THEN 65
                WHEN metric = 'Lnight' THEN 70
                ELSE NULL
            END::int AS band_min
        FROM raw_cells
    )
    SELECT metric, band_min, COUNT(*) AS row_count
    FROM snapped
    WHERE band_min IS NOT NULL
    GROUP BY metric, band_min
    ORDER BY metric, band_min
    """
)

_GRID_ARTIFACT_SOURCE_METRIC_COUNTS_SQL = text(
    """
    SELECT
        g.source_type,
        g.metric,
        COUNT(*) AS row_count
    FROM noise_grid_artifact g
    WHERE g.artifact_hash = :grid_artifact_hash
      AND g.source_type IN ('road', 'rail')
      AND g.metric IN ('Lden', 'Lnight')
    GROUP BY g.source_type, g.metric
    ORDER BY g.source_type, g.metric
    """
)

_GRID_ARTIFACT_HASH_SQL = text(
    """
    SELECT COALESCE(m.manifest_json ->> 'grid_artifact_hash', '') AS grid_artifact_hash
    FROM noise_artifact_manifest m
    WHERE m.artifact_hash = :noise_resolved_hash
      AND m.artifact_type = 'resolved'
      AND m.status = 'complete'
    LIMIT 1
    """
)

_INSERT_ROAD_PROXY_FROM_GRID_SQL = text(
    """
    WITH study_area_2157 AS (
        SELECT
            CASE
                WHEN :has_study_area THEN
                    ST_MakeValid(
                        ST_Transform(ST_SetSRID(ST_GeomFromWKB(:study_wkb), 4326), 2157)
                    )
                ELSE NULL
            END AS geom
    ),
    proxy_cells AS (
        SELECT
            g.source_type,
            g.metric,
            COALESCE(g.round_number, 4) AS round_number,
            CASE
                WHEN g.db_value ~ '^[0-9]+-[0-9]+$' THEN split_part(g.db_value, '-', 1)::float8
                WHEN g.db_value ~ '^[0-9]+\\+$' THEN regexp_replace(g.db_value, '\\+', '')::float8
                WHEN g.db_low IS NOT NULL THEN g.db_low::float8
                ELSE NULL
            END AS raw_band_min,
            CASE
                WHEN :has_study_area THEN
                    ST_Multi(
                        ST_CollectionExtract(
                            ST_MakeValid(
                                ST_Intersection(ST_MakeValid(g.geom), s.geom)
                            ),
                            3
                        )
                    )
                ELSE
                    ST_Multi(ST_CollectionExtract(ST_MakeValid(g.geom), 3))
            END AS geom
        FROM noise_grid_artifact g
        CROSS JOIN study_area_2157 s
        WHERE g.artifact_hash = :grid_artifact_hash
          AND g.source_type IN ('road', 'rail')
          AND g.metric IN ('Lden', 'Lnight')
          AND g.geom IS NOT NULL
          AND NOT ST_IsEmpty(g.geom)
          AND (
              NOT :has_study_area
              OR ST_Intersects(ST_MakeValid(g.geom), s.geom)
          )
    ),
    snapped AS (
        SELECT
            p.source_type,
            p.metric,
            p.round_number,
            CASE
                WHEN p.raw_band_min IS NULL THEN NULL
                WHEN p.metric = 'Lden' AND p.raw_band_min < 57.5 THEN 55
                WHEN p.metric = 'Lden' AND p.raw_band_min < 62.5 THEN 60
                WHEN p.metric = 'Lden' AND p.raw_band_min < 67.5 THEN 65
                WHEN p.metric = 'Lden' AND p.raw_band_min < 72.5 THEN 70
                WHEN p.metric = 'Lden' AND p.raw_band_min < 77.5 THEN 75
                WHEN p.metric = 'Lden' THEN 80
                WHEN p.metric = 'Lnight' AND p.raw_band_min < 50 THEN 45
                WHEN p.metric = 'Lnight' AND p.raw_band_min < 55 THEN 50
                WHEN p.metric = 'Lnight' AND p.raw_band_min < 60 THEN 55
                WHEN p.metric = 'Lnight' AND p.raw_band_min < 65 THEN 60
                WHEN p.metric = 'Lnight' AND p.raw_band_min < 70 THEN 65
                WHEN p.metric = 'Lnight' THEN 70
                ELSE NULL
            END::int AS band_min,
            p.geom
        FROM proxy_cells p
        WHERE p.raw_band_min IS NOT NULL
          AND p.geom IS NOT NULL
          AND NOT ST_IsEmpty(p.geom)
    )
    INSERT INTO noise_polygons (
        build_key, config_hash, import_fingerprint,
        jurisdiction, source_type, metric, round_number, report_period,
        db_low, db_high, db_value,
        source_dataset, source_layer, source_ref,
        geom, created_at
    )
    SELECT
        :build_key,
        :config_hash,
        :import_fingerprint,
        'proxy' AS jurisdiction,
        s.source_type AS source_type,
        s.metric AS metric,
        s.round_number AS round_number,
        :proxy_class AS report_period,
        s.band_min::float8 AS db_low,
        CASE
            WHEN s.metric = 'Lden' THEN CASE s.band_min
                WHEN 55 THEN 35
                WHEN 60 THEN 50
                WHEN 65 THEN 65
                WHEN 70 THEN 80
                WHEN 75 THEN 95
                WHEN 80 THEN 100
                ELSE 35
            END
            WHEN s.metric = 'Lnight' THEN CASE s.band_min
                WHEN 45 THEN 30
                WHEN 50 THEN 45
                WHEN 55 THEN 60
                WHEN 60 THEN 75
                WHEN 65 THEN 90
                WHEN 70 THEN 100
                ELSE 30
            END
            ELSE 0
        END::float8 AS db_high,
        CASE
            WHEN s.metric = 'Lden' THEN CASE s.band_min
                WHEN 55 THEN '55-59'
                WHEN 60 THEN '60-64'
                WHEN 65 THEN '65-69'
                WHEN 70 THEN '70-74'
                WHEN 75 THEN '75+'
                WHEN 80 THEN '80+'
                ELSE '55-59'
            END
            WHEN s.metric = 'Lnight' THEN CASE s.band_min
                WHEN 45 THEN '45-49'
                WHEN 50 THEN '50-54'
                WHEN 55 THEN '55-59'
                WHEN 60 THEN '60-64'
                WHEN 65 THEN '65-69'
                WHEN 70 THEN '70+'
                ELSE '45-49'
            END
            ELSE 'unknown'
        END AS db_value,
        'noise_grid_artifact' AS source_dataset,
        'grid_1000m' AS source_layer,
        '1' AS source_ref,
        ST_Transform(s.geom, 4326) AS geom,
        now() AS created_at
    FROM snapped s
    WHERE s.band_min IS NOT NULL
      AND s.geom IS NOT NULL
      AND NOT ST_IsEmpty(s.geom)
      AND ST_Area(s.geom) > 0
    """
)

_RESOLVED_AIRPORT_SOURCE_METRIC_COUNTS_SQL = text(
    """
    SELECT
        r.source_type,
        r.metric,
        COUNT(*) AS row_count
    FROM noise_resolved_display r
    WHERE r.noise_resolved_hash = :noise_resolved_hash
      AND r.source_type = 'airport'
      AND r.metric IN ('Lden', 'Lnight')
    GROUP BY r.source_type, r.metric
    ORDER BY r.source_type, r.metric
    """
)

_RESOLVED_AIRPORT_BAND_COUNTS_BY_METRIC_SQL = text(
    """
    WITH raw_rows AS (
        SELECT
            r.metric,
            CASE
                WHEN r.db_value ~ '^[0-9]+-[0-9]+$' THEN split_part(r.db_value, '-', 1)::float8
                WHEN r.db_value ~ '^[0-9]+\\+$' THEN regexp_replace(r.db_value, '\\+', '')::float8
                WHEN r.db_low IS NOT NULL THEN r.db_low::float8
                ELSE NULL
            END AS raw_band_min
        FROM noise_resolved_display r
        WHERE r.noise_resolved_hash = :noise_resolved_hash
          AND r.source_type = 'airport'
          AND r.metric IN ('Lden', 'Lnight')
    ),
    snapped AS (
        SELECT
            metric,
            CASE
                WHEN raw_band_min IS NULL THEN NULL
                WHEN metric = 'Lden' AND raw_band_min < 57.5 THEN 55
                WHEN metric = 'Lden' AND raw_band_min < 62.5 THEN 60
                WHEN metric = 'Lden' AND raw_band_min < 67.5 THEN 65
                WHEN metric = 'Lden' AND raw_band_min < 72.5 THEN 70
                WHEN metric = 'Lden' AND raw_band_min < 77.5 THEN 75
                WHEN metric = 'Lden' THEN 80
                WHEN metric = 'Lnight' AND raw_band_min < 50 THEN 45
                WHEN metric = 'Lnight' AND raw_band_min < 55 THEN 50
                WHEN metric = 'Lnight' AND raw_band_min < 60 THEN 55
                WHEN metric = 'Lnight' AND raw_band_min < 65 THEN 60
                WHEN metric = 'Lnight' AND raw_band_min < 70 THEN 65
                WHEN metric = 'Lnight' THEN 70
                ELSE NULL
            END::int AS band_min
        FROM raw_rows
    )
    SELECT metric, band_min, COUNT(*) AS row_count
    FROM snapped
    WHERE band_min IS NOT NULL
    GROUP BY metric, band_min
    ORDER BY metric, band_min
    """
)

_INSERT_AIRPORT_FROM_RESOLVED_SQL = text(
    """
    WITH study_area_2157 AS (
        SELECT
            CASE
                WHEN :has_study_area THEN
                    ST_MakeValid(
                        ST_Transform(ST_SetSRID(ST_GeomFromWKB(:study_wkb), 4326), 2157)
                    )
                ELSE NULL
            END AS geom
    ),
    resolved_rows AS (
        SELECT
            COALESCE(NULLIF(r.jurisdiction, ''), 'proxy') AS jurisdiction,
            r.metric,
            COALESCE(r.round_number, 4) AS round_number,
            r.noise_feature_id::text AS source_ref,
            CASE
                WHEN r.db_value ~ '^[0-9]+-[0-9]+$' THEN split_part(r.db_value, '-', 1)::float8
                WHEN r.db_value ~ '^[0-9]+\\+$' THEN regexp_replace(r.db_value, '\\+', '')::float8
                WHEN r.db_low IS NOT NULL THEN r.db_low::float8
                ELSE NULL
            END AS raw_band_min,
            CASE
                WHEN :has_study_area THEN
                    ST_Multi(
                        ST_CollectionExtract(
                            ST_MakeValid(
                                ST_Intersection(ST_MakeValid(r.geom), s.geom)
                            ),
                            3
                        )
                    )
                ELSE
                    ST_Multi(ST_CollectionExtract(ST_MakeValid(r.geom), 3))
            END AS geom
        FROM noise_resolved_display r
        CROSS JOIN study_area_2157 s
        WHERE r.noise_resolved_hash = :noise_resolved_hash
          AND r.source_type = 'airport'
          AND r.metric IN ('Lden', 'Lnight')
          AND r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
          AND (
              NOT :has_study_area
              OR ST_Intersects(ST_MakeValid(r.geom), s.geom)
          )
    ),
    snapped AS (
        SELECT
            r.jurisdiction,
            r.metric,
            r.round_number,
            r.source_ref,
            CASE
                WHEN r.raw_band_min IS NULL THEN NULL
                WHEN r.metric = 'Lden' AND r.raw_band_min < 57.5 THEN 55
                WHEN r.metric = 'Lden' AND r.raw_band_min < 62.5 THEN 60
                WHEN r.metric = 'Lden' AND r.raw_band_min < 67.5 THEN 65
                WHEN r.metric = 'Lden' AND r.raw_band_min < 72.5 THEN 70
                WHEN r.metric = 'Lden' AND r.raw_band_min < 77.5 THEN 75
                WHEN r.metric = 'Lden' THEN 80
                WHEN r.metric = 'Lnight' AND r.raw_band_min < 50 THEN 45
                WHEN r.metric = 'Lnight' AND r.raw_band_min < 55 THEN 50
                WHEN r.metric = 'Lnight' AND r.raw_band_min < 60 THEN 55
                WHEN r.metric = 'Lnight' AND r.raw_band_min < 65 THEN 60
                WHEN r.metric = 'Lnight' AND r.raw_band_min < 70 THEN 65
                WHEN r.metric = 'Lnight' THEN 70
                ELSE NULL
            END::int AS band_min,
            r.geom
        FROM resolved_rows r
        WHERE r.raw_band_min IS NOT NULL
          AND r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
    )
    INSERT INTO noise_polygons (
        build_key, config_hash, import_fingerprint,
        jurisdiction, source_type, metric, round_number, report_period,
        db_low, db_high, db_value,
        source_dataset, source_layer, source_ref,
        geom, created_at
    )
    SELECT
        :build_key,
        :config_hash,
        :import_fingerprint,
        s.jurisdiction AS jurisdiction,
        'airport' AS source_type,
        s.metric AS metric,
        s.round_number AS round_number,
        :proxy_class AS report_period,
        s.band_min::float8 AS db_low,
        CASE
            WHEN s.metric = 'Lden' THEN CASE s.band_min
                WHEN 55 THEN 35
                WHEN 60 THEN 50
                WHEN 65 THEN 65
                WHEN 70 THEN 80
                WHEN 75 THEN 95
                WHEN 80 THEN 100
                ELSE 35
            END
            WHEN s.metric = 'Lnight' THEN CASE s.band_min
                WHEN 45 THEN 30
                WHEN 50 THEN 45
                WHEN 55 THEN 60
                WHEN 60 THEN 75
                WHEN 65 THEN 90
                WHEN 70 THEN 100
                ELSE 30
            END
            ELSE 0
        END::float8 AS db_high,
        CASE
            WHEN s.metric = 'Lden' THEN CASE s.band_min
                WHEN 55 THEN '55-59'
                WHEN 60 THEN '60-64'
                WHEN 65 THEN '65-69'
                WHEN 70 THEN '70-74'
                WHEN 75 THEN '75+'
                WHEN 80 THEN '80+'
                ELSE '55-59'
            END
            WHEN s.metric = 'Lnight' THEN CASE s.band_min
                WHEN 45 THEN '45-49'
                WHEN 50 THEN '50-54'
                WHEN 55 THEN '55-59'
                WHEN 60 THEN '60-64'
                WHEN 65 THEN '65-69'
                WHEN 70 THEN '70+'
                ELSE '45-49'
            END
            ELSE 'unknown'
        END AS db_value,
        'noise_resolved_display' AS source_dataset,
        'resolved_display' AS source_layer,
        COALESCE(NULLIF(s.source_ref, ''), '1') AS source_ref,
        ST_Transform(s.geom, 4326) AS geom,
        now() AS created_at
    FROM snapped s
    WHERE s.band_min IS NOT NULL
      AND s.geom IS NOT NULL
      AND NOT ST_IsEmpty(s.geom)
      AND ST_Area(s.geom) > 0
    """
)

_LDEN_PROXY_SCORE_BY_BAND_MIN: dict[int, int] = {
    55: 35,
    60: 50,
    65: 65,
    70: 80,
    75: 95,
    80: 100,
}


_LNIGHT_PROXY_SCORE_BY_BAND_MIN: dict[int, int] = {
    45: 30,
    50: 45,
    55: 60,
    60: 75,
    65: 90,
    70: 100,
}


_PROXY_BAND_MINS_BY_METRIC: dict[str, tuple[int, ...]] = {
    "Lden": tuple(sorted(_LDEN_PROXY_SCORE_BY_BAND_MIN.keys())),
    "Lnight": tuple(sorted(_LNIGHT_PROXY_SCORE_BY_BAND_MIN.keys())),
}

_GRID_PROXY_SOURCE_TYPES: tuple[str, ...] = ("road", "rail")


def _load_band_counts_by_metric(
    connection: Connection,
    *,
    grid_artifact_hash: str,
) -> dict[str, dict[int, int]]:
    rows = connection.execute(
        _GRID_ARTIFACT_BAND_COUNTS_BY_METRIC_SQL,
        {"grid_artifact_hash": grid_artifact_hash},
    ).mappings()
    band_counts: dict[str, dict[int, int]] = {}
    for row in rows:
        metric = str(row.get("metric") or "").strip()
        if metric not in _PROXY_BAND_MINS_BY_METRIC:
            continue
        band_min = int(row.get("band_min"))
        row_count = int(row.get("row_count") or 0)
        if row_count <= 0:
            continue
        band_counts.setdefault(metric, {})[band_min] = row_count
    return band_counts


def _load_source_metric_counts(
    connection: Connection,
    *,
    grid_artifact_hash: str,
) -> dict[str, dict[str, int]]:
    rows = connection.execute(
        _GRID_ARTIFACT_SOURCE_METRIC_COUNTS_SQL,
        {"grid_artifact_hash": grid_artifact_hash},
    ).mappings()
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        source_type = str(row.get("source_type") or "").strip()
        metric = str(row.get("metric") or "").strip()
        row_count = int(row.get("row_count") or 0)
        if source_type not in _GRID_PROXY_SOURCE_TYPES:
            continue
        if metric not in _PROXY_BAND_MINS_BY_METRIC:
            continue
        if row_count <= 0:
            continue
        counts.setdefault(source_type, {})[metric] = row_count
    return counts


def _load_airport_source_metric_counts_from_resolved(
    connection: Connection,
    *,
    noise_resolved_hash: str,
) -> dict[str, dict[str, int]]:
    rows = connection.execute(
        _RESOLVED_AIRPORT_SOURCE_METRIC_COUNTS_SQL,
        {"noise_resolved_hash": noise_resolved_hash},
    ).mappings()
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        source_type = str(row.get("source_type") or "").strip()
        metric = str(row.get("metric") or "").strip()
        row_count = int(row.get("row_count") or 0)
        if source_type != "airport":
            continue
        if metric not in _PROXY_BAND_MINS_BY_METRIC:
            continue
        if row_count <= 0:
            continue
        counts.setdefault(source_type, {})[metric] = row_count
    return counts


def _load_airport_band_counts_by_metric_from_resolved(
    connection: Connection,
    *,
    noise_resolved_hash: str,
) -> dict[str, dict[int, int]]:
    rows = connection.execute(
        _RESOLVED_AIRPORT_BAND_COUNTS_BY_METRIC_SQL,
        {"noise_resolved_hash": noise_resolved_hash},
    ).mappings()
    band_counts: dict[str, dict[int, int]] = {}
    for row in rows:
        metric = str(row.get("metric") or "").strip()
        if metric not in _PROXY_BAND_MINS_BY_METRIC:
            continue
        band_min = int(row.get("band_min"))
        row_count = int(row.get("row_count") or 0)
        if row_count <= 0:
            continue
        band_counts.setdefault(metric, {})[band_min] = row_count
    return band_counts


def _merge_band_counts_by_metric(
    primary: dict[str, dict[int, int]],
    secondary: dict[str, dict[int, int]],
) -> dict[str, dict[int, int]]:
    merged: dict[str, dict[int, int]] = {
        metric: dict(counts)
        for metric, counts in primary.items()
    }
    for metric, counts in secondary.items():
        metric_counts = merged.setdefault(metric, {})
        for band_min, row_count in counts.items():
            metric_counts[int(band_min)] = int(metric_counts.get(int(band_min), 0)) + int(row_count)
    return merged


def _merge_source_metric_counts(
    primary: dict[str, dict[str, int]],
    secondary: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    merged: dict[str, dict[str, int]] = {
        source_type: dict(metric_counts)
        for source_type, metric_counts in primary.items()
    }
    for source_type, metric_counts in secondary.items():
        source_counts = merged.setdefault(source_type, {})
        for metric, row_count in metric_counts.items():
            source_counts[metric] = int(source_counts.get(metric, 0)) + int(row_count)
    return merged


def _append_noise_proxy_summary(
    summary_json: dict[str, Any] | None,
    *,
    band_counts_by_metric: dict[str, dict[int, int]],
    source_metric_counts: dict[str, dict[str, int]],
    class_available: bool,
) -> None:
    if summary_json is None:
        return
    summary_json["noise_proxy_label"] = "Official-derived transport noise proxy"
    summary_json["noise_proxy_caveat"] = (
        "Approximate road/rail grid proxy and official-derived airport noise contours. "
        "Not measured point noise."
    )
    summary_json["noise_proxy_phase"] = "phase_d2_road_rail_grid_airport_resolved_lden_lnight"
    summary_json["noise_proxy_band_counts_by_metric"] = {
        metric: {
            str(int(band_min)): int(row_count)
            for band_min, row_count in sorted((band_counts or {}).items(), key=lambda item: int(item[0]))
        }
        for metric, band_counts in sorted(band_counts_by_metric.items())
    }
    summary_json["noise_proxy_source_metric_counts"] = {
        source_type: {
            metric: int(row_count)
            for metric, row_count in sorted((metric_counts or {}).items())
        }
        for source_type, metric_counts in sorted(source_metric_counts.items())
    }
    summary_json["noise_proxy_class_differentiation_available"] = bool(class_available)
    if not class_available:
        summary_json["noise_proxy_class_differentiation_note"] = (
            "Road/rail/airport class tags are unavailable in this phase; using unclassified overlay rows."
        )


def copy_noise_artifact_to_noise_polygons(
    connection: Connection,
    *,
    noise_resolved_hash: str,
    build_key: str,
    config_hash: str,
    import_fingerprint: str,
    study_area_wgs84=None,
    summary_json: dict[str, Any] | None = None,
) -> int:
    if summary_json is not None:
        summary_json["noise_proxy_phase"] = "phase_d2_road_rail_grid_airport_resolved_lden_lnight"
    has_study_area = study_area_wgs84 is not None
    params: dict[str, Any] = {
        "noise_resolved_hash": noise_resolved_hash,
        "build_key": build_key,
        "config_hash": config_hash,
        "import_fingerprint": import_fingerprint,
        "has_study_area": has_study_area,
        "study_wkb": study_area_wgs84.wkb if has_study_area else None,
        "proxy_class": "unclassified",
    }

    grid_row = connection.execute(
        _GRID_ARTIFACT_HASH_SQL,
        {"noise_resolved_hash": noise_resolved_hash},
    ).mappings().first()
    grid_artifact_hash = str((grid_row or {}).get("grid_artifact_hash") or "").strip()
    if not grid_artifact_hash:
        if summary_json is not None:
            summary_json["noise_proxy_blocked"] = True
            summary_json["noise_proxy_blocked_reason"] = (
                "No transport grid artifact hash was available for lightweight proxy corridors; "
                "prepare a noise artifact before publish."
            )
        return 0
    params["grid_artifact_hash"] = grid_artifact_hash
    band_counts_by_metric = _load_band_counts_by_metric(
        connection,
        grid_artifact_hash=grid_artifact_hash,
    )
    source_metric_counts = _load_source_metric_counts(
        connection,
        grid_artifact_hash=grid_artifact_hash,
    )
    grid_result = connection.execute(_INSERT_ROAD_PROXY_FROM_GRID_SQL, params)
    inserted_grid = max(int(grid_result.rowcount or 0), 0)
    resolved_result = connection.execute(_INSERT_AIRPORT_FROM_RESOLVED_SQL, params)
    inserted_airport = max(int(resolved_result.rowcount or 0), 0)
    inserted = inserted_grid + inserted_airport

    airport_band_counts = _load_airport_band_counts_by_metric_from_resolved(
        connection,
        noise_resolved_hash=noise_resolved_hash,
    )
    airport_source_metric_counts = _load_airport_source_metric_counts_from_resolved(
        connection,
        noise_resolved_hash=noise_resolved_hash,
    )
    merged_band_counts_by_metric = _merge_band_counts_by_metric(
        band_counts_by_metric,
        airport_band_counts,
    )
    merged_source_metric_counts = _merge_source_metric_counts(
        source_metric_counts,
        airport_source_metric_counts,
    )

    _append_noise_proxy_summary(
        summary_json,
        band_counts_by_metric=merged_band_counts_by_metric,
        source_metric_counts=merged_source_metric_counts,
        class_available=False,
    )
    if summary_json is not None:
        rail_metric_counts = merged_source_metric_counts.get("rail", {})
        rail_lden_count = int(rail_metric_counts.get("Lden", 0))
        rail_lnight_count = int(rail_metric_counts.get("Lnight", 0))
        rail_rows_present = rail_lden_count > 0 and rail_lnight_count > 0
        summary_json["noise_proxy_rail_source_rows_missing"] = not rail_rows_present
        if not rail_rows_present:
            summary_json["noise_proxy_rail_source_rows_missing_reason"] = (
                "rail source rows missing"
            )
        else:
            summary_json.pop("noise_proxy_rail_source_rows_missing_reason", None)
        airport_metric_counts = merged_source_metric_counts.get("airport", {})
        airport_lden_count = int(airport_metric_counts.get("Lden", 0))
        airport_lnight_count = int(airport_metric_counts.get("Lnight", 0))
        airport_rows_present = airport_lden_count > 0 and airport_lnight_count > 0
        summary_json["noise_proxy_airport_source_rows_missing"] = not airport_rows_present
        if not airport_rows_present:
            summary_json["noise_proxy_airport_source_rows_missing_reason"] = (
                "airport source rows missing"
            )
        else:
            summary_json.pop("noise_proxy_airport_source_rows_missing_reason", None)
        summary_json["noise_proxy_blocked"] = inserted <= 0
        if inserted <= 0:
            summary_json["noise_proxy_blocked_reason"] = (
                "No road/rail grid proxy rows and no airport resolved rows were available for publish."
            )
        else:
            summary_json.pop("noise_proxy_blocked_reason", None)
    return inserted
