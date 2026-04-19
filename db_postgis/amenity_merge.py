from __future__ import annotations

import time
from typing import Any, Iterable

from overture import merge as overture_merge

from ._dependencies import Engine, text
from .common import BATCH_SIZE


_TMP_OSM_SELF_ROWS = '"_tmp_osm_source_dedupe_rows"'
_TMP_OSM_SELF_ALIASES = '"_tmp_osm_source_dedupe_aliases"'
_TMP_OSM_SELF_CANDIDATES = '"_tmp_osm_source_dedupe_candidates"'
_TMP_OSM_ROWS = '"_tmp_osm_amenity_merge_rows"'
_TMP_OVERTURE_ROWS = '"_tmp_overture_amenity_merge_rows"'
_TMP_OSM_ALIASES = '"_tmp_osm_amenity_aliases"'
_TMP_OVERTURE_ALIASES = '"_tmp_overture_amenity_aliases"'
_TMP_RAW_CANDIDATES = '"_tmp_amenity_merge_candidates_raw"'
_TMP_COLLAPSED_CANDIDATES = '"_tmp_amenity_merge_candidates"'
_CANDIDATE_PATHS = (
    "same_category_near",
    "same_category_alias",
    "cross_category_near",
    "cross_category_alias",
)


def _chunked(
    rows: Iterable[dict[str, Any]],
    size: int = BATCH_SIZE,
) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _create_temp_merge_stage(connection) -> None:
    for table_name in (
        _TMP_COLLAPSED_CANDIDATES,
        _TMP_RAW_CANDIDATES,
        _TMP_OSM_ALIASES,
        _TMP_OVERTURE_ALIASES,
        _TMP_OSM_ROWS,
        _TMP_OVERTURE_ROWS,
        _TMP_OSM_SELF_CANDIDATES,
        _TMP_OSM_SELF_ALIASES,
        _TMP_OSM_SELF_ROWS,
    ):
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OSM_SELF_ROWS} (
                osm_row_id INTEGER NOT NULL PRIMARY KEY,
                category TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                name TEXT,
                park_area_m2 DOUBLE PRECISION NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL,
                geom_metric geometry(Point, 2157) NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OSM_SELF_ALIASES} (
                osm_row_id INTEGER NOT NULL,
                alias TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OSM_SELF_CANDIDATES} (
                left_osm_row_id INTEGER NOT NULL,
                right_osm_row_id INTEGER NOT NULL,
                distance_m DOUBLE PRECISION NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OSM_ROWS} (
                osm_row_id INTEGER NOT NULL PRIMARY KEY,
                category TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                name TEXT,
                park_area_m2 DOUBLE PRECISION NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL,
                geom_metric geometry(Point, 2157) NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OVERTURE_ROWS} (
                overture_row_id INTEGER NOT NULL PRIMARY KEY,
                category TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                name TEXT,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL,
                geom_metric geometry(Point, 2157) NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OSM_ALIASES} (
                osm_row_id INTEGER NOT NULL,
                alias TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_OVERTURE_ALIASES} (
                overture_row_id INTEGER NOT NULL,
                alias TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_RAW_CANDIDATES} (
                osm_row_id INTEGER NOT NULL,
                overture_row_id INTEGER NOT NULL,
                same_category BOOLEAN NOT NULL,
                aliases_agree BOOLEAN NOT NULL,
                distance_m DOUBLE PRECISION NOT NULL,
                path TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
    )


def _stage_prepared_osm_rows(
    connection,
    prepared_osm_rows: list[dict[str, Any]],
    *,
    table_name: str,
) -> None:
    if not prepared_osm_rows:
        return
    statement = text(
        f"""
        INSERT INTO {table_name}
            (osm_row_id, category, source_ref, name, park_area_m2, lat, lon, geom_metric)
        VALUES (
            :osm_row_id,
            :category,
            :source_ref,
            :name,
            :park_area_m2,
            :lat,
            :lon,
            ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 2157)
        )
        """
    )
    payload_rows = [
        {
            "osm_row_id": int(prepared_row["row_id"]),
            "category": prepared_row["category"],
            "source_ref": prepared_row["source_ref"],
            "name": prepared_row["row"].get("name"),
            "park_area_m2": float(prepared_row["row"].get("park_area_m2", 0.0) or 0.0),
            "lat": float(prepared_row["lat"]),
            "lon": float(prepared_row["lon"]),
        }
        for prepared_row in prepared_osm_rows
    ]
    for chunk in _chunked(payload_rows):
        connection.execute(statement, chunk)


def _stage_prepared_overture_rows(
    connection,
    prepared_overture_rows: list[dict[str, Any]],
) -> None:
    if not prepared_overture_rows:
        return
    statement = text(
        f"""
        INSERT INTO {_TMP_OVERTURE_ROWS}
            (overture_row_id, category, source_ref, name, lat, lon, geom_metric)
        VALUES (
            :overture_row_id,
            :category,
            :source_ref,
            :name,
            :lat,
            :lon,
            ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 2157)
        )
        """
    )
    payload_rows = [
        {
            "overture_row_id": int(prepared_row["row_id"]),
            "category": prepared_row["category"],
            "source_ref": prepared_row["source_ref"],
            "name": prepared_row["row"].get("name"),
            "lat": float(prepared_row["lat"]),
            "lon": float(prepared_row["lon"]),
        }
        for prepared_row in prepared_overture_rows
    ]
    for chunk in _chunked(payload_rows):
        connection.execute(statement, chunk)


def _stage_alias_rows(
    connection,
    prepared_rows: list[dict[str, Any]],
    *,
    alias_table: str,
    row_id_key: str,
) -> int:
    payload_rows: list[dict[str, Any]] = []
    for prepared_row in prepared_rows:
        for alias in prepared_row["aliases"]:
            payload_rows.append(
                {
                    row_id_key: int(prepared_row["row_id"]),
                    "alias": str(alias),
                }
            )
    if not payload_rows:
        return 0
    statement = text(
        f"""
        INSERT INTO {alias_table}
            ({row_id_key}, alias)
        VALUES (
            :{row_id_key},
            :alias
        )
        """
    )
    for chunk in _chunked(payload_rows):
        connection.execute(statement, chunk)
    return len(payload_rows)


def _create_osm_self_stage_indexes(connection) -> None:
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_SELF_ROWS} USING GIST (geom_metric)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_SELF_ROWS} (category)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_SELF_ALIASES} (alias, osm_row_id)"))


def _create_merge_stage_indexes(connection) -> None:
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_ROWS} USING GIST (geom_metric)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OVERTURE_ROWS} USING GIST (geom_metric)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_ROWS} (category)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OVERTURE_ROWS} (category)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OSM_ALIASES} (alias, osm_row_id)"))
    connection.execute(text(f"CREATE INDEX ON {_TMP_OVERTURE_ALIASES} (alias, overture_row_id)"))


def _insert_osm_self_candidates(connection) -> None:
    connection.execute(
        text(
            f"""
            INSERT INTO {_TMP_OSM_SELF_CANDIDATES}
                (left_osm_row_id, right_osm_row_id, distance_m)
            SELECT
                left_row.osm_row_id,
                right_row.osm_row_id,
                MIN(ST_Distance(left_row.geom_metric, right_row.geom_metric)) AS distance_m
            FROM {_TMP_OSM_SELF_ROWS} AS left_row
            JOIN {_TMP_OSM_SELF_ROWS} AS right_row
              ON left_row.category = right_row.category
             AND left_row.osm_row_id < right_row.osm_row_id
            JOIN {_TMP_OSM_SELF_ALIASES} AS left_alias
              ON left_alias.osm_row_id = left_row.osm_row_id
            JOIN {_TMP_OSM_SELF_ALIASES} AS right_alias
              ON right_alias.osm_row_id = right_row.osm_row_id
             AND right_alias.alias = left_alias.alias
            WHERE ST_DWithin(
                left_row.geom_metric,
                right_row.geom_metric,
                :osm_self_dedupe_radius_m
            )
            GROUP BY left_row.osm_row_id, right_row.osm_row_id
            """
        ),
        {"osm_self_dedupe_radius_m": float(overture_merge.OSM_SELF_DEDUPE_RADIUS_M)},
    )


def _load_osm_self_candidate_rows(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        text(
            f"""
            SELECT
                left_osm_row_id,
                right_osm_row_id,
                distance_m
            FROM {_TMP_OSM_SELF_CANDIDATES}
            ORDER BY left_osm_row_id, right_osm_row_id
            """
        )
    ).mappings().all()
    return [
        {
            "left_osm_row_id": int(row["left_osm_row_id"]),
            "right_osm_row_id": int(row["right_osm_row_id"]),
            "distance_m": float(row["distance_m"]),
        }
        for row in rows
    ]


def _insert_raw_candidates(connection) -> None:
    radius_params = {
        "auto_match_radius_m": float(overture_merge.AUTO_MATCH_RADIUS_M),
        "name_match_radius_m": float(overture_merge.NAME_MATCH_RADIUS_M),
    }
    candidate_statements = (
        text(
            f"""
            INSERT INTO {_TMP_RAW_CANDIDATES}
                (osm_row_id, overture_row_id, same_category, aliases_agree, distance_m, path)
            SELECT
                o.osm_row_id,
                v.overture_row_id,
                TRUE,
                FALSE,
                ST_Distance(o.geom_metric, v.geom_metric) AS distance_m,
                'same_category_near'
            FROM {_TMP_OSM_ROWS} AS o
            JOIN {_TMP_OVERTURE_ROWS} AS v
              ON o.category = v.category
            WHERE ST_DWithin(o.geom_metric, v.geom_metric, :auto_match_radius_m)
            """
        ),
        text(
            f"""
            INSERT INTO {_TMP_RAW_CANDIDATES}
                (osm_row_id, overture_row_id, same_category, aliases_agree, distance_m, path)
            SELECT DISTINCT
                o.osm_row_id,
                v.overture_row_id,
                TRUE,
                TRUE,
                ST_Distance(o.geom_metric, v.geom_metric) AS distance_m,
                'same_category_alias'
            FROM {_TMP_OSM_ROWS} AS o
            JOIN {_TMP_OVERTURE_ROWS} AS v
              ON o.category = v.category
            JOIN {_TMP_OSM_ALIASES} AS oa
              ON oa.osm_row_id = o.osm_row_id
            JOIN {_TMP_OVERTURE_ALIASES} AS va
              ON va.overture_row_id = v.overture_row_id
             AND va.alias = oa.alias
            WHERE ST_DWithin(o.geom_metric, v.geom_metric, :name_match_radius_m)
            """
        ),
        text(
            f"""
            INSERT INTO {_TMP_RAW_CANDIDATES}
                (osm_row_id, overture_row_id, same_category, aliases_agree, distance_m, path)
            SELECT
                o.osm_row_id,
                v.overture_row_id,
                FALSE,
                FALSE,
                ST_Distance(o.geom_metric, v.geom_metric) AS distance_m,
                'cross_category_near'
            FROM {_TMP_OSM_ROWS} AS o
            JOIN {_TMP_OVERTURE_ROWS} AS v
              ON o.category <> v.category
            WHERE ST_DWithin(o.geom_metric, v.geom_metric, :auto_match_radius_m)
            """
        ),
        text(
            f"""
            INSERT INTO {_TMP_RAW_CANDIDATES}
                (osm_row_id, overture_row_id, same_category, aliases_agree, distance_m, path)
            SELECT DISTINCT
                o.osm_row_id,
                v.overture_row_id,
                FALSE,
                TRUE,
                ST_Distance(o.geom_metric, v.geom_metric) AS distance_m,
                'cross_category_alias'
            FROM {_TMP_OSM_ROWS} AS o
            JOIN {_TMP_OVERTURE_ROWS} AS v
              ON o.category <> v.category
            JOIN {_TMP_OSM_ALIASES} AS oa
              ON oa.osm_row_id = o.osm_row_id
            JOIN {_TMP_OVERTURE_ALIASES} AS va
              ON va.overture_row_id = v.overture_row_id
             AND va.alias = oa.alias
            WHERE ST_DWithin(o.geom_metric, v.geom_metric, :name_match_radius_m)
            """
        ),
    )
    for statement in candidate_statements:
        connection.execute(statement, radius_params)
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {_TMP_COLLAPSED_CANDIDATES}
            ON COMMIT DROP AS
            SELECT
                osm_row_id,
                overture_row_id,
                BOOL_OR(same_category) AS same_category,
                BOOL_OR(aliases_agree) AS aliases_agree,
                MIN(distance_m) AS distance_m
            FROM {_TMP_RAW_CANDIDATES}
            GROUP BY osm_row_id, overture_row_id
            """
        )
    )


def _load_collapsed_candidate_rows(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        text(
            f"""
            SELECT
                osm_row_id,
                overture_row_id,
                same_category,
                aliases_agree,
                distance_m
            FROM {_TMP_COLLAPSED_CANDIDATES}
            ORDER BY osm_row_id, overture_row_id
            """
        )
    ).mappings().all()
    return [
        {
            "osm_row_id": int(row["osm_row_id"]),
            "overture_row_id": int(row["overture_row_id"]),
            "same_category": bool(row["same_category"]),
            "aliases_agree": bool(row["aliases_agree"]),
            "distance_m": float(row["distance_m"]),
        }
        for row in rows
    ]


def _collect_candidate_stats(
    connection,
    *,
    merge_categories: tuple[str, ...],
    merge_warning: str | None,
    osm_rows: int,
    overture_rows: int,
    osm_alias_rows: int,
    overture_alias_rows: int,
    stage_ms: dict[str, float],
) -> dict[str, Any]:
    path_counts = {path: 0 for path in _CANDIDATE_PATHS}
    for row in connection.execute(
        text(
            f"""
            SELECT path, COUNT(*) AS pair_count
            FROM {_TMP_RAW_CANDIDATES}
            GROUP BY path
            ORDER BY path
            """
        )
    ).mappings():
        path = str(row["path"])
        if path in path_counts:
            path_counts[path] = int(row["pair_count"] or 0)

    collapsed_totals = connection.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS candidate_pair_count,
                COALESCE(SUM(CASE WHEN same_category THEN 1 ELSE 0 END), 0) AS same_category_candidate_count,
                COALESCE(SUM(CASE WHEN NOT same_category THEN 1 ELSE 0 END), 0) AS cross_category_candidate_count
            FROM {_TMP_COLLAPSED_CANDIDATES}
            """
        )
    ).mappings().one()

    category_counts = {
        str(row["category"]): int(row["pair_count"] or 0)
        for row in connection.execute(
            text(
                f"""
                SELECT
                    o.category AS category,
                    COUNT(*) AS pair_count
                FROM {_TMP_COLLAPSED_CANDIDATES} AS c
                JOIN {_TMP_OSM_ROWS} AS o
                  ON o.osm_row_id = c.osm_row_id
                GROUP BY o.category
                ORDER BY pair_count DESC, category ASC
                """
            )
        ).mappings()
    }

    stats: dict[str, Any] = {
        "merge_categories_resolved": list(merge_categories),
        "osm_rows": int(osm_rows),
        "overture_rows": int(overture_rows),
        "osm_alias_rows": int(osm_alias_rows),
        "overture_alias_rows": int(overture_alias_rows),
        "candidate_pair_count": int(collapsed_totals["candidate_pair_count"] or 0),
        "same_category_candidate_count": int(
            collapsed_totals["same_category_candidate_count"] or 0
        ),
        "cross_category_candidate_count": int(
            collapsed_totals["cross_category_candidate_count"] or 0
        ),
        "candidate_pairs_by_path": path_counts,
        "candidate_pairs_by_osm_category": category_counts,
        "stage_ms": {name: float(value) for name, value in stage_ms.items()},
    }
    if merge_warning:
        stats["merge_categories_warning"] = merge_warning
    return stats


def load_merged_source_amenity_rows(
    engine: Engine,
    osm_rows: list[dict[str, Any]],
    overture_rows: list[dict[str, Any]],
    *,
    scoring_categories: Iterable[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    stage_ms = {
        "filter_osm_rows": 0.0,
        "generate_osm_self_candidates": 0.0,
        "collapse_osm_duplicates": 0.0,
        "generate_overture_candidates": 0.0,
        "greedy_assignment": 0.0,
    }

    started_at = time.perf_counter()
    prepared_osm_rows = overture_merge.prepare_osm_rows_for_self_dedupe(osm_rows)
    stage_ms["filter_osm_rows"] = (time.perf_counter() - started_at) * 1000.0

    osm_self_candidate_rows: list[dict[str, Any]] = []
    with engine.begin() as connection:
        _create_temp_merge_stage(connection)

        _stage_prepared_osm_rows(
            connection,
            prepared_osm_rows,
            table_name=_TMP_OSM_SELF_ROWS,
        )
        _stage_alias_rows(
            connection,
            prepared_osm_rows,
            alias_table=_TMP_OSM_SELF_ALIASES,
            row_id_key="osm_row_id",
        )

        started_at = time.perf_counter()
        _create_osm_self_stage_indexes(connection)
        _insert_osm_self_candidates(connection)
        osm_self_candidate_rows = _load_osm_self_candidate_rows(connection)
        stage_ms["generate_osm_self_candidates"] = (
            time.perf_counter() - started_at
        ) * 1000.0

        started_at = time.perf_counter()
        deduped_osm_rows, dedupe_stats = overture_merge.collapse_prepared_osm_source_duplicates(
            prepared_osm_rows,
            osm_self_candidate_rows,
        )
        stage_ms["collapse_osm_duplicates"] = (
            time.perf_counter() - started_at
        ) * 1000.0

        prepared = overture_merge.prepare_rows_for_merge(
            deduped_osm_rows,
            overture_rows,
            scoring_categories=scoring_categories,
        )
        merge_categories = tuple(prepared["merge_categories"])
        merge_warning = overture_merge.merge_category_warning(merge_categories)
        osm_alias_row_count = sum(
            len(prepared_row["aliases"]) for prepared_row in prepared["prepared_osm_rows"]
        )
        overture_alias_row_count = sum(
            len(prepared_row["aliases"])
            for prepared_row in prepared["prepared_overture_rows"]
        )

        if (
            merge_categories
            and prepared["prepared_osm_rows"]
            and prepared["prepared_overture_rows"]
        ):
            started_at = time.perf_counter()
            _stage_prepared_osm_rows(
                connection,
                prepared["prepared_osm_rows"],
                table_name=_TMP_OSM_ROWS,
            )
            _stage_prepared_overture_rows(connection, prepared["prepared_overture_rows"])
            _stage_alias_rows(
                connection,
                prepared["prepared_osm_rows"],
                alias_table=_TMP_OSM_ALIASES,
                row_id_key="osm_row_id",
            )
            _stage_alias_rows(
                connection,
                prepared["prepared_overture_rows"],
                alias_table=_TMP_OVERTURE_ALIASES,
                row_id_key="overture_row_id",
            )
            _create_merge_stage_indexes(connection)
            _insert_raw_candidates(connection)
            stage_ms["generate_overture_candidates"] = (
                time.perf_counter() - started_at
            ) * 1000.0

            stats = _collect_candidate_stats(
                connection,
                merge_categories=merge_categories,
                merge_warning=merge_warning,
                osm_rows=len(prepared["prepared_osm_rows"]),
                overture_rows=len(prepared["prepared_overture_rows"]),
                osm_alias_rows=int(osm_alias_row_count),
                overture_alias_rows=int(overture_alias_row_count),
                stage_ms=stage_ms,
            )
            collapsed_candidate_rows = _load_collapsed_candidate_rows(connection)
        else:
            stats = {
                "merge_categories_resolved": list(merge_categories),
                "osm_rows": len(prepared["prepared_osm_rows"]),
                "overture_rows": len(prepared["prepared_overture_rows"]),
                "osm_alias_rows": int(osm_alias_row_count),
                "overture_alias_rows": int(overture_alias_row_count),
                "candidate_pair_count": 0,
                "same_category_candidate_count": 0,
                "cross_category_candidate_count": 0,
                "candidate_pairs_by_path": {path: 0 for path in _CANDIDATE_PATHS},
                "candidate_pairs_by_osm_category": {},
                "stage_ms": {name: float(value) for name, value in stage_ms.items()},
                **(
                    {"merge_categories_warning": merge_warning}
                    if merge_warning
                    else {}
                ),
            }
            collapsed_candidate_rows = []

    started_at = time.perf_counter()
    merged_rows = overture_merge.merge_source_amenity_rows_from_candidate_pairs(
        deduped_osm_rows,
        overture_rows,
        collapsed_candidate_rows,
        scoring_categories=scoring_categories,
    )
    stage_ms["greedy_assignment"] = (time.perf_counter() - started_at) * 1000.0
    stats.update(dedupe_stats)
    stats["stage_ms"] = {name: float(value) for name, value in stage_ms.items()}
    return merged_rows, stats


__all__ = [
    "load_merged_source_amenity_rows",
]
