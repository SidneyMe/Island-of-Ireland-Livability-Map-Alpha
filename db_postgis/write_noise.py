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


def copy_noise_artifact_to_noise_polygons(
    connection: Connection,
    *,
    noise_resolved_hash: str,
    build_key: str,
    config_hash: str,
    import_fingerprint: str,
    study_area_wgs84=None,
) -> int:
    """
    Artifact mode direct copy: noise_resolved_display (EPSG:2157) → noise_polygons (EPSG:4326).

    When study_area_wgs84 is provided, geometries are clipped (ST_Intersection) to the
    study area rather than merely filtered with ST_Intersects, so sub-area profiles do not
    publish polygons that extend beyond the bbox/county boundary.

    No candidate staging, no ST_Difference, no ST_Subdivide.
    source_dataset/source_layer/source_ref are lossy compatibility placeholders.
    Returns row count inserted.
    """
    has_study_area = study_area_wgs84 is not None
    params: dict[str, Any] = {
        "build_key": build_key,
        "config_hash": config_hash,
        "import_fingerprint": import_fingerprint,
        "noise_resolved_hash": noise_resolved_hash,
        "has_study_area": has_study_area,
        "study_wkb": study_area_wgs84.wkb if has_study_area else None,
    }

    result = connection.execute(
        text(
            """
            WITH study_area_2157 AS (
                SELECT
                    CASE
                        WHEN :has_study_area THEN
                            ST_Transform(ST_SetSRID(ST_GeomFromWKB(:study_wkb), 4326), 2157)
                        ELSE NULL
                    END AS geom
            ),
            display_rows AS (
                SELECT
                    d.noise_resolved_hash,
                    d.noise_feature_id,
                    d.jurisdiction, d.source_type, d.metric,
                    d.round_number, d.report_period,
                    d.db_low, d.db_high, d.db_value,
                    CASE
                        WHEN :has_study_area THEN
                            ST_Multi(ST_CollectionExtract(
                                ST_MakeValid(ST_Intersection(d.geom, s.geom)), 3
                            ))
                        ELSE d.geom
                    END AS clipped_geom
                FROM noise_resolved_display d
                CROSS JOIN study_area_2157 s
                WHERE d.noise_resolved_hash = :noise_resolved_hash
                  AND d.geom IS NOT NULL
                  AND NOT ST_IsEmpty(d.geom)
                  AND (
                      NOT :has_study_area
                      OR ST_Intersects(d.geom, s.geom)
                  )
            )
            INSERT INTO noise_polygons (
                build_key, config_hash, import_fingerprint,
                jurisdiction, source_type, metric, round_number, report_period,
                db_low, db_high, db_value,
                source_dataset, source_layer, source_ref,
                geom, created_at
            )
            SELECT
                :build_key, :config_hash, :import_fingerprint,
                dr.jurisdiction, dr.source_type, dr.metric,
                dr.round_number, dr.report_period,
                dr.db_low, dr.db_high, dr.db_value,
                COALESCE(MIN(p.source_dataset), 'noise_artifact') AS source_dataset,
                COALESCE(MIN(p.source_layer), :noise_resolved_hash) AS source_layer,
                dr.noise_feature_id::text AS source_ref,
                ST_Transform(dr.clipped_geom, 4326) AS geom,
                now()
            FROM display_rows dr
            LEFT JOIN noise_resolved_provenance p
                ON  p.noise_resolved_hash = dr.noise_resolved_hash
                AND p.jurisdiction        = dr.jurisdiction
                AND p.source_type         = dr.source_type
                AND p.metric              = dr.metric
                AND p.round_number        = dr.round_number
            WHERE dr.clipped_geom IS NOT NULL
              AND NOT ST_IsEmpty(dr.clipped_geom)
              AND ST_Area(dr.clipped_geom) > 0
            GROUP BY
                dr.noise_resolved_hash, dr.noise_feature_id,
                dr.jurisdiction, dr.source_type, dr.metric,
                dr.round_number, dr.report_period,
                dr.db_low, dr.db_high, dr.db_value, dr.clipped_geom
            """
        ),
        params,
    )
    return max(int(result.rowcount or 0), 0)
