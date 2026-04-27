"""
Side-by-side comparison of artifact output vs legacy noise_polygons.

Used in Phase 11 to validate that the artifact pipeline produces results
within acceptable tolerance of the legacy pipeline before switching the default.
"""
from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_DIVERGENCE_THRESHOLD = 0.01  # 1% relative area difference


def compare_artifact_to_legacy(
    engine: Engine,
    *,
    noise_resolved_hash: str,
    legacy_build_key: str,
    threshold: float = _DIVERGENCE_THRESHOLD,
) -> dict:
    """
    Compare noise_resolved_display (artifact, EPSG:2157) vs noise_polygons (legacy)
    by (jurisdiction, source_type, metric) group.

    Area comparison uses EPSG:2157 for both sources — noise_polygons geometry
    is ST_Transform'd from 4326 to 2157 for a like-for-like comparison.

    Returns:
        groups_matching: count of groups within threshold
        groups_diverging: count of groups exceeding threshold
        area_ratio_by_group: dict of group_key → artifact_area / legacy_area
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH artifact AS (
                    SELECT
                        jurisdiction, source_type, metric,
                        COUNT(*) AS row_count,
                        SUM(ST_Area(geom)) AS area_m2
                    FROM noise_resolved_display
                    WHERE noise_resolved_hash = :resolved_hash
                    GROUP BY jurisdiction, source_type, metric
                ),
                legacy AS (
                    SELECT
                        jurisdiction, source_type, metric,
                        COUNT(*) AS row_count,
                        SUM(ST_Area(ST_Transform(geom, 2157))) AS area_m2
                    FROM noise_polygons
                    WHERE build_key = :build_key
                    GROUP BY jurisdiction, source_type, metric
                ),
                combined AS (
                    SELECT
                        COALESCE(a.jurisdiction, l.jurisdiction) AS jurisdiction,
                        COALESCE(a.source_type,  l.source_type)  AS source_type,
                        COALESCE(a.metric,       l.metric)       AS metric,
                        COALESCE(a.row_count, 0) AS artifact_rows,
                        COALESCE(l.row_count, 0) AS legacy_rows,
                        COALESCE(a.area_m2, 0)   AS artifact_area_m2,
                        COALESCE(l.area_m2, 0)   AS legacy_area_m2
                    FROM artifact a
                    FULL OUTER JOIN legacy l
                        ON a.jurisdiction = l.jurisdiction
                        AND a.source_type = l.source_type
                        AND a.metric = l.metric
                )
                SELECT
                    jurisdiction, source_type, metric,
                    artifact_rows, legacy_rows,
                    artifact_area_m2, legacy_area_m2,
                    CASE
                        WHEN legacy_area_m2 > 0
                        THEN artifact_area_m2 / legacy_area_m2
                        ELSE NULL
                    END AS area_ratio
                FROM combined
                ORDER BY jurisdiction, source_type, metric
                """
            ),
            {"resolved_hash": noise_resolved_hash, "build_key": legacy_build_key},
        ).mappings().fetchall()

    area_ratio_by_group = {}
    groups_matching = 0
    groups_diverging = 0

    for row in rows:
        key = f"{row['jurisdiction']}/{row['source_type']}/{row['metric']}"
        ratio = float(row["area_ratio"]) if row["area_ratio"] is not None else None
        area_ratio_by_group[key] = ratio

        if ratio is None:
            groups_diverging += 1
            log.warning("group %s: no legacy data for comparison", key)
        elif abs(ratio - 1.0) > threshold:
            groups_diverging += 1
            log.warning(
                "group %s: area_ratio=%.4f exceeds threshold %.2f%%",
                key, ratio, threshold * 100,
            )
        else:
            groups_matching += 1
            log.debug("group %s: area_ratio=%.4f OK", key, ratio)

    return {
        "groups_matching": groups_matching,
        "groups_diverging": groups_diverging,
        "area_ratio_by_group": area_ratio_by_group,
    }
