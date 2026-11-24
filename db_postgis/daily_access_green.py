"""Daily access score (v4) green sites built in PostGIS.

Pipeline (one DB session, temp tables only; the persistent schema is
untouched):

1. Read every ``parks`` row of the import nationally and classify it in Python
   (semantic type, residential garden, foot/access rules).
2. Stage eligible OSM geometries plus Overture-only green points in EPSG:2157.
3. Union touching/overlapping polygons of the same semantic type
   (``ST_ClusterDBSCAN`` with ``eps=0``) *before* any profile clipping, so the
   area class is that of the whole territory.
4. Keep sites within ``DAILY_ACCESS_REACH_BUFFER_M`` of the study area and read
   their full-cover containment pairs.
5. In Python: area class -> quality -> service_kind, and nesting roots.

``_tmp_daily_green_sites`` stays in the session for the park-approach step.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import shapely

from config import (
    DAILY_ACCESS_EDGE_FLAG_RECREATION,
    DAILY_ACCESS_PARK_BOUNDARY_TOLERANCE_M,
    DAILY_ACCESS_REACH_BUFFER_M,
    OSM_IMPORT_SCHEMA,
)
from precompute.daily_access_classify import (
    Exclusion,
    classify_green,
    green_area_class,
    green_service_kind,
)

from ._dependencies import Engine, text
from .common import BATCH_SIZE
from .reads import _is_non_operational_amenity


TMP_GREEN_MEMBERS = '"_tmp_daily_green_members"'
TMP_GREEN_GEOMS = '"_tmp_daily_green_geoms"'
TMP_GREEN_SITES = '"_tmp_daily_green_sites"'
TMP_GREEN_WINDOW = '"_tmp_daily_green_window"'


@dataclass(frozen=True)
class GreenSite:
    site_key: str
    site_index: int
    root_site_key: str
    root_site_id: int
    semantic_type: str
    area_class: str
    quality: float
    service_kind: str
    is_area: bool
    approximate_point: bool
    area_m2: float | None
    member_refs: tuple[str, ...]
    name: str | None
    geom_wkb: bytes | None = None


def osm_member_ref(osm_type: Any, osm_id: Any) -> str:
    return f"{str(osm_type).strip()}/{int(osm_id)}"


def overture_member_ref(source_ref: Any) -> str:
    return f"overture:{source_ref}"


def classify_green_source_rows(
    rows: Iterable[dict[str, Any]],
    stats: dict[str, Any],
) -> list[dict[str, Any]]:
    """Eligible rows with ``semantic_type`` set; exclusion reasons counted in stats."""
    eligible: list[dict[str, Any]] = []
    input_by_type = stats.setdefault("input_rows", {})
    excluded = stats.setdefault("excluded_rows", {})
    for row in rows:
        tags = row.get("tags_json") or {}
        source = str(row.get("source") or "osm_local_pbf")
        input_by_type[source] = int(input_by_type.get(source, 0)) + 1
        if isinstance(tags, dict) and _is_non_operational_amenity(tags):
            excluded["non_operational"] = int(excluded.get("non_operational", 0)) + 1
            continue
        verdict = classify_green(row)
        if isinstance(verdict, Exclusion):
            excluded[verdict.reason] = int(excluded.get(verdict.reason, 0)) + 1
            continue
        eligible.append({**row, "semantic_type": verdict.semantic_type})
    return eligible


def resolve_green_sites(
    site_rows: list[dict[str, Any]],
    containment_pairs: Iterable[tuple[str, str]],
) -> list[GreenSite]:
    """Assign quality and nesting roots.

    ``site_rows`` need ``site_key``, ``semantic_type``, ``is_area``,
    ``area_m2``, ``member_refs`` and optionally ``name``/``geom_wkb``.
    ``containment_pairs`` are ``(inner_key, outer_key)`` with the outer area
    covering the inner site completely.

    A fully contained site joins the root of its best container whose quality
    is at least its own (highest quality first, then the stable site key). Its
    own quality is kept. Sites are processed largest first, so a container is
    always resolved before what it contains; identical geometries resolve by
    quality and then site key, which keeps the relation acyclic.
    """
    resolved: dict[str, dict[str, Any]] = {}
    for row in site_rows:
        is_area = bool(row["is_area"])
        area_m2 = float(row["area_m2"]) if is_area and row.get("area_m2") is not None else None
        area_class, quality = green_area_class(str(row["semantic_type"]), area_m2)
        resolved[str(row["site_key"])] = {
            **row,
            "is_area": is_area,
            "area_m2": area_m2,
            "area_class": area_class,
            "quality": quality,
        }

    order = sorted(
        resolved,
        key=lambda key: (
            -(resolved[key]["area_m2"] or 0.0),
            -resolved[key]["quality"],
            key,
        ),
    )
    position = {key: index for index, key in enumerate(order)}
    containers: dict[str, set[str]] = {}
    for inner_key, outer_key in containment_pairs:
        if inner_key in resolved and outer_key in resolved and inner_key != outer_key:
            containers.setdefault(inner_key, set()).add(outer_key)

    root_of: dict[str, str] = {}
    for key in order:
        quality = resolved[key]["quality"]
        candidates = [
            outer
            for outer in containers.get(key, ())
            if resolved[outer]["quality"] >= quality and position[outer] < position[key]
        ]
        if candidates:
            parent = min(candidates, key=lambda outer: (-resolved[outer]["quality"], outer))
            root_of[key] = root_of[parent]
        else:
            root_of[key] = key

    root_ids = {root: index for index, root in enumerate(sorted(set(root_of.values())))}
    sites: list[GreenSite] = []
    for site_index, key in enumerate(sorted(resolved)):
        row = resolved[key]
        sites.append(
            GreenSite(
                site_key=key,
                site_index=site_index,
                root_site_key=root_of[key],
                root_site_id=root_ids[root_of[key]],
                semantic_type=str(row["semantic_type"]),
                area_class=str(row["area_class"]),
                quality=float(row["quality"]),
                service_kind=green_service_kind(float(row["quality"])),
                is_area=bool(row["is_area"]),
                approximate_point=not bool(row["is_area"]),
                area_m2=row["area_m2"],
                member_refs=tuple(row.get("member_refs") or ()),
                name=row.get("name"),
                geom_wkb=row.get("geom_wkb"),
            )
        )
    return sites


def _create_stage(connection) -> None:
    for table_name in (TMP_GREEN_WINDOW, TMP_GREEN_SITES, TMP_GREEN_GEOMS, TMP_GREEN_MEMBERS):
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {TMP_GREEN_MEMBERS} (
                member_ref TEXT PRIMARY KEY,
                osm_type TEXT,
                osm_id BIGINT,
                lon DOUBLE PRECISION,
                lat DOUBLE PRECISION,
                semantic_type TEXT NOT NULL,
                name TEXT
            )
            """
        )
    )


def _load_osm_green_rows(connection, import_fingerprint: str) -> list[dict[str, Any]]:
    result = connection.execute(
        text(
            f"""
            SELECT osm_type, osm_id, name, tags_json
            FROM {OSM_IMPORT_SCHEMA}.features
            WHERE import_fingerprint = :import_fingerprint
              AND category = 'parks'
            ORDER BY osm_type, osm_id
            """
        ),
        {"import_fingerprint": import_fingerprint},
    ).mappings()
    return [
        {
            "category": "parks",
            "source": "osm_local_pbf",
            "osm_type": str(row["osm_type"]).strip(),
            "osm_id": int(row["osm_id"]),
            "name": row["name"],
            "tags_json": dict(row["tags_json"] or {}),
        }
        for row in result
    ]


def _insert_members(connection, members: list[dict[str, Any]]) -> None:
    statement = text(
        f"""
        INSERT INTO {TMP_GREEN_MEMBERS}
            (member_ref, osm_type, osm_id, lon, lat, semantic_type, name)
        VALUES (:member_ref, :osm_type, :osm_id, :lon, :lat, :semantic_type, :name)
        """
    )
    for start in range(0, len(members), BATCH_SIZE):
        connection.execute(statement, members[start : start + BATCH_SIZE])


def _build_sites(connection, import_fingerprint: str) -> None:
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {TMP_GREEN_GEOMS} AS
            WITH source AS (
                SELECT
                    m.member_ref,
                    m.semantic_type,
                    m.name,
                    ST_Transform(f.geom, 2157) AS geom
                FROM {TMP_GREEN_MEMBERS} m
                JOIN {OSM_IMPORT_SCHEMA}.features f
                  ON f.import_fingerprint = :import_fingerprint
                 AND f.osm_type::text = m.osm_type
                 AND f.osm_id = m.osm_id
                 AND f.category = 'parks'
                WHERE m.osm_type IS NOT NULL
            ),
            areal AS (
                SELECT
                    member_ref,
                    semantic_type,
                    name,
                    geom,
                    CASE
                        WHEN ST_Dimension(geom) = 2
                        THEN ST_CollectionExtract(ST_MakeValid(geom), 3)
                    END AS area_geom
                FROM source
            )
            SELECT
                member_ref,
                semantic_type,
                name,
                (area_geom IS NOT NULL AND NOT ST_IsEmpty(area_geom)) AS is_area,
                CASE
                    WHEN area_geom IS NOT NULL AND NOT ST_IsEmpty(area_geom) THEN area_geom
                    ELSE ST_PointOnSurface(geom)
                END AS geom
            FROM areal
            UNION ALL
            SELECT
                member_ref,
                semantic_type,
                name,
                FALSE AS is_area,
                ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 2157) AS geom
            FROM {TMP_GREEN_MEMBERS}
            WHERE osm_type IS NULL
            """
        ),
        {"import_fingerprint": import_fingerprint},
    )
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {TMP_GREEN_SITES} AS
            WITH clustered AS (
                SELECT
                    member_ref,
                    semantic_type,
                    name,
                    geom,
                    ST_ClusterDBSCAN(geom, eps := 0, minpoints := 1)
                        OVER (PARTITION BY semantic_type) AS cluster_id
                FROM {TMP_GREEN_GEOMS}
                WHERE is_area
            )
            SELECT
                semantic_type || ':' || MIN(member_ref) AS site_key,
                semantic_type,
                TRUE AS is_area,
                ST_UnaryUnion(ST_Collect(geom)) AS geom,
                ARRAY_AGG(member_ref ORDER BY member_ref) AS member_refs,
                (ARRAY_AGG(name ORDER BY member_ref) FILTER (WHERE name IS NOT NULL))[1] AS name
            FROM clustered
            GROUP BY semantic_type, cluster_id
            UNION ALL
            SELECT
                semantic_type || ':' || member_ref AS site_key,
                semantic_type,
                FALSE AS is_area,
                geom,
                ARRAY[member_ref] AS member_refs,
                name
            FROM {TMP_GREEN_GEOMS}
            WHERE NOT is_area
            """
        )
    )
    connection.execute(
        text(f"ALTER TABLE {TMP_GREEN_SITES} ADD COLUMN area_m2 DOUBLE PRECISION")
    )
    connection.execute(
        text(f"UPDATE {TMP_GREEN_SITES} SET area_m2 = ST_Area(geom) WHERE is_area")
    )
    connection.execute(
        text(f"ALTER TABLE {TMP_GREEN_SITES} ADD COLUMN boundary geometry")
    )
    connection.execute(
        text(f"UPDATE {TMP_GREEN_SITES} SET boundary = ST_Boundary(geom) WHERE is_area")
    )
    connection.execute(
        text(f'CREATE INDEX "_tmp_daily_green_sites_geom_idx" ON {TMP_GREEN_SITES} USING GIST (geom)')
    )
    connection.execute(
        text(f'CREATE UNIQUE INDEX "_tmp_daily_green_sites_key_idx" ON {TMP_GREEN_SITES} (site_key)')
    )
    connection.execute(text(f"ANALYZE {TMP_GREEN_SITES}"))


def _select_window(connection, study_area_wkb: bytes, buffer_m: float) -> None:
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {TMP_GREEN_WINDOW} AS
            WITH study AS (
                SELECT ST_Transform(ST_GeomFromWKB(:study_area_wkb, 4326), 2157) AS geom
            )
            SELECT s.site_key
            FROM {TMP_GREEN_SITES} s, study
            WHERE ST_DWithin(s.geom, study.geom, :buffer_m)
            """
        ),
        {"study_area_wkb": study_area_wkb, "buffer_m": float(buffer_m)},
    )


def _read_window(connection) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    site_rows = [
        {
            "site_key": row["site_key"],
            "semantic_type": row["semantic_type"],
            "is_area": bool(row["is_area"]),
            "area_m2": row["area_m2"],
            "member_refs": tuple(row["member_refs"] or ()),
            "name": row["name"],
            "geom_wkb": bytes(row["geom_wkb"]),
        }
        for row in connection.execute(
            text(
                f"""
                SELECT s.site_key, s.semantic_type, s.is_area, s.area_m2,
                       s.member_refs, s.name, ST_AsBinary(s.geom) AS geom_wkb
                FROM {TMP_GREEN_SITES} s
                JOIN {TMP_GREEN_WINDOW} w USING (site_key)
                ORDER BY s.site_key
                """
            )
        ).mappings()
    ]
    pairs = [
        (str(row["inner_key"]), str(row["outer_key"]))
        for row in connection.execute(
            text(
                f"""
                SELECT i.site_key AS inner_key, o.site_key AS outer_key
                FROM {TMP_GREEN_SITES} i
                JOIN {TMP_GREEN_WINDOW} w ON w.site_key = i.site_key
                JOIN {TMP_GREEN_SITES} o
                  ON o.is_area
                 AND o.site_key <> i.site_key
                 AND o.geom && i.geom
                 AND ST_Covers(o.geom, i.geom)
                ORDER BY 1, 2
                """
            )
        ).mappings()
    ]
    return site_rows, pairs


def stage_green_sites(
    connection,
    *,
    import_fingerprint: str,
    study_area_wgs84,
    overture_green_rows: Iterable[dict[str, Any]] = (),
    buffer_m: float = DAILY_ACCESS_REACH_BUFFER_M,
) -> tuple[list[GreenSite], dict[str, Any]]:
    """Build green sites on ``connection``; temp tables stay for later steps.

    ``overture_green_rows`` should be the Overture-only green rows left after
    the OSM/Overture merge (rows the merge matched to OSM already exist as OSM
    members). They need ``source_ref``, ``raw_primary_category`` and a point
    ``geom`` (or ``lat``/``lon``).
    """
    stats: dict[str, Any] = {}
    eligible_osm = classify_green_source_rows(
        _load_osm_green_rows(connection, import_fingerprint), stats
    )
    eligible_overture = classify_green_source_rows(
        [{**row, "category": "parks"} for row in overture_green_rows], stats
    )
    members: list[dict[str, Any]] = [
        {
            "member_ref": osm_member_ref(row["osm_type"], row["osm_id"]),
            "osm_type": row["osm_type"],
            "osm_id": int(row["osm_id"]),
            "lon": None,
            "lat": None,
            "semantic_type": row["semantic_type"],
            "name": row.get("name"),
        }
        for row in eligible_osm
    ]
    for row in eligible_overture:
        geom = row.get("geom")
        lon = float(geom.x) if geom is not None else float(row["lon"])
        lat = float(geom.y) if geom is not None else float(row["lat"])
        members.append(
            {
                "member_ref": overture_member_ref(row["source_ref"]),
                "osm_type": None,
                "osm_id": None,
                "lon": lon,
                "lat": lat,
                "semantic_type": row["semantic_type"],
                "name": row.get("name"),
            }
        )

    _create_stage(connection)
    _insert_members(connection, members)
    _build_sites(connection, import_fingerprint)
    _select_window(connection, bytes(study_area_wgs84.wkb), buffer_m)
    site_rows, pairs = _read_window(connection)
    sites = resolve_green_sites(site_rows, pairs)

    stats["eligible_members"] = len(members)
    stats["sites_in_window"] = len(sites)
    stats["unioned_sites"] = sum(1 for site in sites if len(site.member_refs) > 1)
    stats["nested_sites"] = sum(1 for site in sites if site.root_site_key != site.site_key)
    stats["approximate_point_sites"] = sum(1 for site in sites if site.approximate_point)
    sites_by_kind: dict[str, int] = {}
    for site in sites:
        sites_by_kind[site.service_kind] = sites_by_kind.get(site.service_kind, 0) + 1
    stats["sites_by_service_kind"] = dict(sorted(sites_by_kind.items()))
    stats["root_sites"] = len({site.root_site_id for site in sites})
    return sites, stats


def load_green_sites(
    engine: Engine,
    *,
    import_fingerprint: str,
    study_area_wgs84,
    overture_green_rows: Iterable[dict[str, Any]] = (),
    buffer_m: float = DAILY_ACCESS_REACH_BUFFER_M,
) -> tuple[list[GreenSite], dict[str, Any]]:
    with engine.begin() as connection:
        return stage_green_sites(
            connection,
            import_fingerprint=import_fingerprint,
            study_area_wgs84=study_area_wgs84,
            overture_green_rows=overture_green_rows,
            buffer_m=buffer_m,
        )


TMP_WALK_EDGES = '"_tmp_daily_walk_edges"'
_EDGE_FILTER_CELL_M = 500.0


def candidate_edge_slots(
    node_x: np.ndarray,
    node_y: np.ndarray,
    offsets: np.ndarray,
    targets: np.ndarray,
    envelopes: Iterable[tuple[float, float, float, float]],
    *,
    cell_m: float = _EDGE_FILTER_CELL_M,
) -> np.ndarray:
    """CSR slots (one per undirected edge, ``src < dst``) whose bbox may meet an envelope.

    Envelopes are rasterised onto a coarse grid. An edge spanning at most two
    grid cells per axis is kept when any corner cell of its bbox is flagged,
    which covers every cell of its bbox; longer edges are always kept. The
    result is a superset of the edges touching any envelope.
    """
    envelopes = list(envelopes)
    node_x = np.asarray(node_x, dtype=np.float64)
    node_y = np.asarray(node_y, dtype=np.float64)
    counts = np.diff(np.asarray(offsets, dtype=np.int64))
    src = np.repeat(np.arange(len(counts), dtype=np.int64), counts)
    dst = np.asarray(targets, dtype=np.int64)
    slots = np.flatnonzero(src < dst)
    if not envelopes or len(slots) == 0:
        return np.zeros(0, dtype=np.int64)
    src, dst = src[slots], dst[slots]
    origin_x = min(env[0] for env in envelopes)
    origin_y = min(env[1] for env in envelopes)
    width = int((max(env[2] for env in envelopes) - origin_x) // cell_m) + 1
    height = int((max(env[3] for env in envelopes) - origin_y) // cell_m) + 1
    flagged = np.zeros((height + 2, width + 2), dtype=bool)
    for min_x, min_y, max_x, max_y in envelopes:
        flagged[
            int((min_y - origin_y) // cell_m) + 1 : int((max_y - origin_y) // cell_m) + 2,
            int((min_x - origin_x) // cell_m) + 1 : int((max_x - origin_x) // cell_m) + 2,
        ] = True

    def _cell(values: np.ndarray, origin: float, limit: int) -> np.ndarray:
        return np.clip(np.floor((values - origin) / cell_m).astype(np.int64) + 1, 0, limit + 1)

    x0 = _cell(np.minimum(node_x[src], node_x[dst]), origin_x, width)
    x1 = _cell(np.maximum(node_x[src], node_x[dst]), origin_x, width)
    y0 = _cell(np.minimum(node_y[src], node_y[dst]), origin_y, height)
    y1 = _cell(np.maximum(node_y[src], node_y[dst]), origin_y, height)
    long_edge = ((x1 - x0) > 1) | ((y1 - y0) > 1)
    keep = long_edge | flagged[y0, x0] | flagged[y0, x1] | flagged[y1, x0] | flagged[y1, x1]
    return slots[keep]


def _stage_walk_edges(
    connection,
    *,
    node_x: np.ndarray,
    node_y: np.ndarray,
    offsets: np.ndarray,
    targets: np.ndarray,
    lengths: np.ndarray,
    edge_flags: np.ndarray | None,
    slots: np.ndarray,
) -> None:
    connection.execute(text(f"DROP TABLE IF EXISTS {TMP_WALK_EDGES}"))
    connection.execute(
        text(
            f"""
            CREATE TEMP TABLE {TMP_WALK_EDGES} (
                edge_id BIGINT GENERATED ALWAYS AS IDENTITY,
                slot BIGINT NOT NULL,
                src INTEGER NOT NULL,
                dst INTEGER NOT NULL,
                length_m DOUBLE PRECISION NOT NULL,
                recreation BOOLEAN NOT NULL,
                x1 DOUBLE PRECISION NOT NULL,
                y1 DOUBLE PRECISION NOT NULL,
                x2 DOUBLE PRECISION NOT NULL,
                y2 DOUBLE PRECISION NOT NULL
            )
            """
        )
    )
    counts = np.diff(np.asarray(offsets, dtype=np.int64))
    src = np.repeat(np.arange(len(counts), dtype=np.int64), counts)[slots]
    dst = np.asarray(targets, dtype=np.int64)[slots]
    length = np.asarray(lengths, dtype=np.float32)[slots].astype(np.float64)
    if edge_flags is None:
        recreation = np.zeros(len(slots), dtype=bool)
    else:
        recreation = (
            np.asarray(edge_flags, dtype=np.uint8)[slots] & DAILY_ACCESS_EDGE_FLAG_RECREATION
        ) != 0
    cursor = connection.connection.driver_connection.cursor()
    with cursor.copy(
        f"COPY {TMP_WALK_EDGES} (slot, src, dst, length_m, recreation, x1, y1, x2, y2) FROM STDIN"
    ) as copy:
        for index in range(len(slots)):
            copy.write_row(
                (
                    int(slots[index]),
                    int(src[index]),
                    int(dst[index]),
                    float(length[index]),
                    bool(recreation[index]),
                    float(node_x[src[index]]),
                    float(node_y[src[index]]),
                    float(node_x[dst[index]]),
                    float(node_y[dst[index]]),
                )
            )
    for statement in (
        f"ALTER TABLE {TMP_WALK_EDGES} ADD COLUMN geom geometry(LineString, 2157)",
        f"UPDATE {TMP_WALK_EDGES} SET geom = "
        "ST_SetSRID(ST_MakeLine(ST_MakePoint(x1, y1), ST_MakePoint(x2, y2)), 2157)",
        f'CREATE INDEX "_tmp_daily_walk_edges_geom_idx" ON {TMP_WALK_EDGES} USING GIST (geom)',
        f'CREATE UNIQUE INDEX "_tmp_daily_walk_edges_id_idx" ON {TMP_WALK_EDGES} (edge_id)',
        f"ANALYZE {TMP_WALK_EDGES}",
    ):
        connection.execute(text(statement))


# Graph nodes are float32 lat/lon (~0.2 m jitter), so a footway sharing
# nodes with a park outline wobbles across it. "Inside" therefore means more
# than DAILY_ACCESS_PARK_BOUNDARY_TOLERANCE_M from the outline.
#
# Park polygons (national parks have 10^4-10^5 vertices) and their outlines
# are cut into <= 256-vertex pieces first; every predicate then runs against
# small indexed pieces instead of the whole polygon.
TMP_GREEN_PIECES = '"_tmp_daily_green_pieces"'
TMP_GREEN_OUTLINE = '"_tmp_daily_green_outline"'
TMP_APPROACH_PAIRS = '"_tmp_daily_approach_pairs"'
_SUBDIVIDE_VERTICES = 256

_APPROACH_SETUP_SQL = (
    f"DROP TABLE IF EXISTS {TMP_APPROACH_PAIRS}",
    f"DROP TABLE IF EXISTS {TMP_GREEN_PIECES}",
    f"DROP TABLE IF EXISTS {TMP_GREEN_OUTLINE}",
    f"""
    CREATE TEMP TABLE {TMP_GREEN_PIECES} AS
    SELECT s.site_key, s.semantic_type, ST_Subdivide(s.geom, {_SUBDIVIDE_VERTICES}) AS geom
    FROM {TMP_GREEN_SITES} s
    JOIN {TMP_GREEN_WINDOW} w ON w.site_key = s.site_key
    WHERE s.is_area
    """,
    f'CREATE INDEX "_tmp_daily_green_pieces_geom_idx" ON {TMP_GREEN_PIECES} USING GIST (geom)',
    f'CREATE INDEX "_tmp_daily_green_pieces_key_idx" ON {TMP_GREEN_PIECES} (site_key)',
    f"ANALYZE {TMP_GREEN_PIECES}",
    f"""
    CREATE TEMP TABLE {TMP_GREEN_OUTLINE} AS
    SELECT s.site_key, ST_Subdivide(s.boundary, {_SUBDIVIDE_VERTICES}) AS geom
    FROM {TMP_GREEN_SITES} s
    JOIN {TMP_GREEN_WINDOW} w ON w.site_key = s.site_key
    WHERE s.is_area
    """,
    f'CREATE INDEX "_tmp_daily_green_outline_geom_idx" ON {TMP_GREEN_OUTLINE} USING GIST (geom)',
    f"ANALYZE {TMP_GREEN_OUTLINE}",
    # Candidate (site, edge) pairs; national parks only use recreation edges.
    f"""
    CREATE TEMP TABLE {TMP_APPROACH_PAIRS} AS
    SELECT DISTINCT p.site_key, e.edge_id
    FROM {TMP_GREEN_PIECES} p
    JOIN {TMP_WALK_EDGES} e
      ON e.geom && p.geom
     AND ST_Intersects(e.geom, p.geom)
    WHERE p.semantic_type <> 'national_park' OR e.recreation
    """,
    f"ALTER TABLE {TMP_APPROACH_PAIRS} ADD COLUMN src_in BOOLEAN, ADD COLUMN dst_in BOOLEAN",
    f"""
    UPDATE {TMP_APPROACH_PAIRS} c
       SET src_in = EXISTS (
               SELECT 1 FROM {TMP_GREEN_PIECES} p
               WHERE p.site_key = c.site_key AND ST_Intersects(p.geom, ST_StartPoint(e.geom))
           ) AND NOT EXISTS (
               SELECT 1 FROM {TMP_GREEN_OUTLINE} o
               WHERE o.site_key = c.site_key AND ST_DWithin(o.geom, ST_StartPoint(e.geom), :tolerance_m)
           ),
           dst_in = EXISTS (
               SELECT 1 FROM {TMP_GREEN_PIECES} p
               WHERE p.site_key = c.site_key AND ST_Intersects(p.geom, ST_EndPoint(e.geom))
           ) AND NOT EXISTS (
               SELECT 1 FROM {TMP_GREEN_OUTLINE} o
               WHERE o.site_key = c.site_key AND ST_DWithin(o.geom, ST_EndPoint(e.geom), :tolerance_m)
           )
      FROM {TMP_WALK_EDGES} e
     WHERE e.edge_id = c.edge_id
    """,
)

_APPROACH_SQL = f"""
WITH clipped AS (
    -- The edge inside the park, re-joined across piece cuts.
    SELECT c.site_key, c.edge_id,
           ST_LineMerge(ST_UnaryUnion(ST_Collect(
               ST_CollectionExtract(ST_Intersection(e.geom, p.geom), 2)
           ))) AS inside
    FROM {TMP_APPROACH_PAIRS} c
    JOIN {TMP_WALK_EDGES} e ON e.edge_id = c.edge_id
    JOIN {TMP_GREEN_PIECES} p
      ON p.site_key = c.site_key
     AND p.geom && e.geom
     AND ST_Intersects(p.geom, e.geom)
    WHERE NOT (c.src_in AND c.dst_in)
    GROUP BY c.site_key, c.edge_id
),
parts AS (
    SELECT site_key, edge_id, (ST_Dump(inside)).geom AS part
    FROM clipped
    WHERE inside IS NOT NULL AND NOT ST_IsEmpty(inside)
),
entering AS (
    -- A part whose midpoint hugs the outline only runs along the boundary.
    SELECT pt.site_key, pt.edge_id,
           LEAST(ST_LineLocatePoint(e.geom, ST_StartPoint(pt.part)),
                 ST_LineLocatePoint(e.geom, ST_EndPoint(pt.part))) AS t0,
           GREATEST(ST_LineLocatePoint(e.geom, ST_StartPoint(pt.part)),
                    ST_LineLocatePoint(e.geom, ST_EndPoint(pt.part))) AS t1
    FROM parts pt
    JOIN {TMP_WALK_EDGES} e ON e.edge_id = pt.edge_id
    WHERE ST_Length(pt.part) > 0
      AND EXISTS (
          SELECT 1 FROM {TMP_GREEN_PIECES} p
          WHERE p.site_key = pt.site_key AND ST_Intersects(p.geom, ST_LineInterpolatePoint(pt.part, 0.5))
      )
      AND NOT EXISTS (
          SELECT 1 FROM {TMP_GREEN_OUTLINE} o
          WHERE o.site_key = pt.site_key
            AND ST_DWithin(o.geom, ST_LineInterpolatePoint(pt.part, 0.5), :tolerance_m)
      )
),
seeds AS (
    SELECT c.site_key, e.src AS node, 0.0::double precision AS offset_m
    FROM {TMP_APPROACH_PAIRS} c JOIN {TMP_WALK_EDGES} e ON e.edge_id = c.edge_id
    WHERE c.src_in
    UNION ALL
    SELECT c.site_key, e.dst, 0.0
    FROM {TMP_APPROACH_PAIRS} c JOIN {TMP_WALK_EDGES} e ON e.edge_id = c.edge_id
    WHERE c.dst_in
    UNION ALL
    SELECT en.site_key, e.src, MIN(en.t0) * MIN(e.length_m)
    FROM entering en
    JOIN {TMP_APPROACH_PAIRS} c ON c.site_key = en.site_key AND c.edge_id = en.edge_id
    JOIN {TMP_WALK_EDGES} e ON e.edge_id = en.edge_id
    WHERE NOT c.src_in
    GROUP BY en.site_key, e.src, en.edge_id
    UNION ALL
    SELECT en.site_key, e.dst, (1.0 - MAX(en.t1)) * MIN(e.length_m)
    FROM entering en
    JOIN {TMP_APPROACH_PAIRS} c ON c.site_key = en.site_key AND c.edge_id = en.edge_id
    JOIN {TMP_WALK_EDGES} e ON e.edge_id = en.edge_id
    WHERE NOT c.dst_in
    GROUP BY en.site_key, e.dst, en.edge_id
)
SELECT site_key, node, MIN(offset_m) AS offset_m
FROM seeds
GROUP BY site_key, node
ORDER BY site_key, node
"""

# Zero-cost same-edge intervals: the in-park parts of entering edges and
# whole edges inside the park, as positions (metres) along the straight edge.
_INTERVAL_SQL = f"""
WITH clipped AS (
    SELECT c.site_key, c.edge_id,
           ST_LineMerge(ST_UnaryUnion(ST_Collect(
               ST_CollectionExtract(ST_Intersection(e.geom, p.geom), 2)
           ))) AS inside
    FROM {TMP_APPROACH_PAIRS} c
    JOIN {TMP_WALK_EDGES} e ON e.edge_id = c.edge_id
    JOIN {TMP_GREEN_PIECES} p
      ON p.site_key = c.site_key
     AND p.geom && e.geom
     AND ST_Intersects(p.geom, e.geom)
    WHERE NOT (c.src_in AND c.dst_in)
    GROUP BY c.site_key, c.edge_id
),
parts AS (
    SELECT site_key, edge_id, (ST_Dump(inside)).geom AS part
    FROM clipped
    WHERE inside IS NOT NULL AND NOT ST_IsEmpty(inside)
)
SELECT pt.site_key, e.slot,
       LEAST(ST_LineLocatePoint(e.geom, ST_StartPoint(pt.part)),
             ST_LineLocatePoint(e.geom, ST_EndPoint(pt.part))) * ST_Length(e.geom) AS lo_m,
       GREATEST(ST_LineLocatePoint(e.geom, ST_StartPoint(pt.part)),
                ST_LineLocatePoint(e.geom, ST_EndPoint(pt.part))) * ST_Length(e.geom) AS hi_m
FROM parts pt
JOIN {TMP_WALK_EDGES} e ON e.edge_id = pt.edge_id
WHERE ST_Length(pt.part) > 0
  AND EXISTS (
      SELECT 1 FROM {TMP_GREEN_PIECES} p
      WHERE p.site_key = pt.site_key AND ST_Intersects(p.geom, ST_LineInterpolatePoint(pt.part, 0.5))
  )
  AND NOT EXISTS (
      SELECT 1 FROM {TMP_GREEN_OUTLINE} o
      WHERE o.site_key = pt.site_key
        AND ST_DWithin(o.geom, ST_LineInterpolatePoint(pt.part, 0.5), :tolerance_m)
  )
UNION ALL
SELECT c.site_key, e.slot, 0.0, ST_Length(e.geom)
FROM {TMP_APPROACH_PAIRS} c JOIN {TMP_WALK_EDGES} e ON e.edge_id = c.edge_id
WHERE c.src_in AND c.dst_in
ORDER BY 1, 2, 3
"""


def stage_park_approaches(
    connection,
    sites: Iterable[GreenSite],
    *,
    node_x: np.ndarray,
    node_y: np.ndarray,
    offsets: np.ndarray,
    targets: np.ndarray,
    lengths: np.ndarray,
    edge_flags: np.ndarray | None,
) -> tuple[dict[str, list[tuple[int, float]]], dict[str, list[tuple[int, float, float]]], dict[str, Any]]:
    """Approach seeds ``site_key -> [(node, offset_m)]`` for areal green sites.

    Also returns the zero-cost same-edge intervals
    ``site_key -> [(edge_id, lo_m, hi_m)]`` (edge id = position among the CSR
    slots with ``src < dst``, positions along the straight edge from ``src``)
    so a cell entering an in-park path is not routed through its endpoints.

    Needs the temp tables of :func:`stage_green_sites` on the same connection;
    ``node_x``/``node_y`` are EPSG:2157 metres. Ordinary parks: every walk
    edge entering the polygon interior. Nodes strictly inside get offset 0; an
    outside endpoint gets the along-edge distance to the entry point,
    proportional to its position on the graph edge length. Touching the
    boundary without entering is no approach. National parks: recreation
    edges only (``edge_flags``); without flags they get no approach. Sites
    with no approach have ``unknown_access``.
    """
    areal = [site for site in sites if site.is_area]
    stats: dict[str, Any] = {"areal_sites": len(areal)}
    envelopes = [shapely.from_wkb(site.geom_wkb).bounds for site in areal if site.geom_wkb]
    slots = candidate_edge_slots(node_x, node_y, offsets, targets, envelopes)
    stats["staged_edges"] = int(len(slots))
    _stage_walk_edges(
        connection,
        node_x=node_x,
        node_y=node_y,
        offsets=offsets,
        targets=targets,
        lengths=lengths,
        edge_flags=edge_flags,
        slots=slots,
    )
    params = {"tolerance_m": DAILY_ACCESS_PARK_BOUNDARY_TOLERANCE_M}
    for statement in _APPROACH_SETUP_SQL:
        if ":tolerance_m" in statement:
            connection.execute(text(statement), params)
        else:
            connection.execute(text(statement))
    approaches: dict[str, list[tuple[int, float]]] = {}
    for row in connection.execute(
        text(_APPROACH_SQL), {"tolerance_m": DAILY_ACCESS_PARK_BOUNDARY_TOLERANCE_M}
    ).mappings():
        approaches.setdefault(str(row["site_key"]), []).append(
            (int(row["node"]), float(row["offset_m"]))
        )
    counts = np.diff(np.asarray(offsets, dtype=np.int64))
    all_src = np.repeat(np.arange(len(counts), dtype=np.int64), counts)
    undirected = np.flatnonzero(all_src < np.asarray(targets, dtype=np.int64))
    intervals: dict[str, list[tuple[int, float, float]]] = {}
    for row in connection.execute(
        text(_INTERVAL_SQL), {"tolerance_m": DAILY_ACCESS_PARK_BOUNDARY_TOLERANCE_M}
    ).mappings():
        edge = int(np.searchsorted(undirected, int(row["slot"])))
        intervals.setdefault(str(row["site_key"]), []).append(
            (edge, float(row["lo_m"]), float(row["hi_m"]))
        )
    stats["sites_with_approach"] = len(approaches)
    stats["sites_with_in_park_edges"] = len(intervals)
    stats["unknown_access_sites"] = sum(1 for site in areal if site.site_key not in approaches)
    stats["national_parks_without_edge_flags"] = (
        sum(1 for site in areal if site.semantic_type == "national_park") if edge_flags is None else 0
    )
    return approaches, intervals, stats


__all__ = [
    "GreenSite",
    "candidate_edge_slots",
    "stage_park_approaches",
    "classify_green_source_rows",
    "load_green_sites",
    "osm_member_ref",
    "overture_member_ref",
    "resolve_green_sites",
    "stage_green_sites",
]
