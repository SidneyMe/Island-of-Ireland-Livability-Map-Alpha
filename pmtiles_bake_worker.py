"""Minimal worker module for the parallel PMTiles bake.

This module is **deliberately tiny** on imports. When the bake dispatches
tile chunks via ``ProcessPoolExecutor``, Windows ``spawn`` subprocesses
re-import the module that owns the worker function. Keeping it out of the
``precompute`` package means subprocesses avoid re-running
``precompute/__init__.py`` — which pulls in scipy / sklearn / geopandas /
pandas / numpy and exhausts the Windows paging file at 8-way concurrency.

All code here must only import:
  - stdlib (``gzip``)
  - ``pmtiles.tile`` (tiny, pure-Python)
  - ``sqlalchemy`` (imported lazily inside the worker-engine getter)

Do not add package-relative imports or anything that transitively pulls the
scientific stack.
"""

from __future__ import annotations

import gzip

from pmtiles.tile import zxy_to_tileid
from sqlalchemy import text


# ── Layer bitmask encoding ──────────────────────────────────────────────────

_LAYER_GRID = 1 << 0
_LAYER_AMENITIES = 1 << 1
_LAYER_TRANSPORT_REALITY = 1 << 2
_LAYER_SERVICE_DESERTS = 1 << 3
_LAYER_FINE_GRID = 1 << 4
_LAYER_NOISE = 1 << 5


# ── Per-tile SQL (one ST_AsMVT call per layer) ──────────────────────────────

_GRID_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            g.cell_id,
            g.resolution_m,
            g.total_score,
            COALESCE((g.counts_json ->> 'shops')::integer, 0) AS count_shops,
            COALESCE((g.cluster_counts_json ->> 'shops')::integer, 0) AS cluster_shops,
            COALESCE((g.effective_units_json ->> 'shops')::double precision, 0.0) AS effective_units_shops,
            COALESCE((g.scores_json ->> 'shops')::double precision, 0.0) AS score_shops,
            COALESCE((g.counts_json ->> 'transport')::integer, 0) AS count_transport,
            COALESCE((g.cluster_counts_json ->> 'transport')::integer, 0) AS cluster_transport,
            COALESCE((g.effective_units_json ->> 'transport')::double precision, 0.0) AS effective_units_transport,
            COALESCE((g.scores_json ->> 'transport')::double precision, 0.0) AS score_transport,
            COALESCE((g.counts_json ->> 'healthcare')::integer, 0) AS count_healthcare,
            COALESCE((g.cluster_counts_json ->> 'healthcare')::integer, 0) AS cluster_healthcare,
            COALESCE((g.effective_units_json ->> 'healthcare')::double precision, 0.0) AS effective_units_healthcare,
            COALESCE((g.scores_json ->> 'healthcare')::double precision, 0.0) AS score_healthcare,
            COALESCE((g.counts_json ->> 'parks')::integer, 0) AS count_parks,
            COALESCE((g.cluster_counts_json ->> 'parks')::integer, 0) AS cluster_parks,
            COALESCE((g.effective_units_json ->> 'parks')::double precision, 0.0) AS effective_units_parks,
            COALESCE((g.scores_json ->> 'parks')::double precision, 0.0) AS score_parks,
            ST_AsMVTGeom(
                ST_Transform(g.cell_geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM grid_walk AS g, tile
        WHERE g.build_key = :build_key
          AND g.resolution_m = :resolution_m
          AND g.cell_geom && tile.env_4326
          AND ST_Intersects(g.cell_geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'grid', 4096, 'geom') FROM mvtgeom
    """
)


_AMENITY_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            a.category,
            a.tier,
            a.name,
            a.source,
            a.source_ref,
            a.conflict_class,
            ST_AsMVTGeom(
                ST_Transform(a.geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM amenities AS a, tile
        WHERE a.build_key = :build_key
          AND a.geom && tile.env_4326
          AND ST_Intersects(a.geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'amenities', 4096, 'geom') FROM mvtgeom
    """
)


_TRANSPORT_REALITY_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            t.source_ref,
            COALESCE(NULLIF(BTRIM(t.stop_name), ''), t.source_ref) AS stop_name,
            t.feed_id,
            t.stop_id,
            t.reality_status,
            t.source_status,
            t.school_only_state,
            t.public_departures_7d,
            t.public_departures_30d,
            t.school_only_departures_30d,
            t.weekday_morning_peak_deps,
            t.weekday_evening_peak_deps,
            t.weekday_offpeak_deps,
            t.saturday_deps,
            t.sunday_deps,
            t.friday_evening_deps,
            t.transport_score_units,
            t.bus_daytime_deps,
            COALESCE(t.bus_daytime_headway_min, 0.0) AS bus_daytime_headway_min,
            COALESCE(t.bus_frequency_tier, '') AS bus_frequency_tier,
            t.bus_frequency_score_units,
            COALESCE(t.bus_active_days_mask_7d, '') AS bus_active_days_mask_7d,
            COALESCE(t.bus_service_subtier, '') AS bus_service_subtier,
            COALESCE(
                array_to_string(ARRAY(SELECT jsonb_array_elements_text(t.route_modes_json)), ','),
                ''
            ) AS route_modes,
            CASE WHEN t.is_unscheduled_stop THEN 1 ELSE 0 END AS is_unscheduled_stop,
            CASE WHEN t.has_exception_only_service THEN 1 ELSE 0 END AS has_exception_only_service,
            CASE WHEN t.has_any_bus_service THEN 1 ELSE 0 END AS has_any_bus_service,
            CASE WHEN t.has_daily_bus_service THEN 1 ELSE 0 END AS has_daily_bus_service,
            ST_AsMVTGeom(
                ST_Transform(t.geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM transport_reality AS t, tile
        WHERE t.build_key = :build_key
          AND t.geom && tile.env_4326
          AND ST_Intersects(t.geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'transport_reality', 4096, 'geom') FROM mvtgeom
    """
)


_SERVICE_DESERT_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            s.cell_id,
            s.resolution_m,
            s.baseline_reachable_stop_count,
            s.reachable_public_departures_7d,
            ST_AsMVTGeom(
                ST_Transform(s.cell_geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM service_deserts AS s, tile
        WHERE s.build_key = :build_key
          AND s.resolution_m = :resolution_m
          AND s.cell_geom && tile.env_4326
          AND ST_Intersects(s.cell_geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'service_deserts', 4096, 'geom') FROM mvtgeom
    """
)


_NOISE_TILE_SQL = text(
    """
    WITH tile AS (
        SELECT
            ST_TileEnvelope(:z, :x, :y) AS env_3857,
            ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326) AS env_4326
    ),
    mvtgeom AS (
        SELECT
            n.jurisdiction,
            n.source_type,
            n.metric,
            n.round_number AS round,
            COALESCE(n.report_period, '') AS report_period,
            COALESCE(n.db_low, 0.0) AS db_low,
            COALESCE(n.db_high, 0.0) AS db_high,
            n.db_value,
            n.source_dataset,
            n.source_layer,
            n.source_ref,
            ST_AsMVTGeom(
                ST_Transform(n.geom, 3857),
                tile.env_3857,
                4096,
                64,
                true
            ) AS geom
        FROM noise_polygons AS n, tile
        WHERE n.build_key = :build_key
          AND n.geom && tile.env_4326
          AND ST_Intersects(n.geom, tile.env_4326)
    )
    SELECT ST_AsMVT(mvtgeom, 'noise', 4096, 'geom') FROM mvtgeom
    """
)


def _resolution_for_zoom(zoom: int) -> int:
    if zoom >= 10:
        return 5000
    if zoom >= 8:
        return 10000
    return 20000


def _tile_mvt_bytes_by_flags(
    connection,
    *,
    build_key: str,
    z: int,
    x: int,
    y: int,
    layers: int,
) -> bytes:
    """Build a single MVT for ``(z, x, y)`` containing the layers selected by ``layers``.

    The MVT format is a top-level repeated ``Layer`` message, so independent
    ``ST_AsMVT`` calls can be concatenated into a valid multi-layer tile.
    """
    chunks: list[bytes] = []
    if layers & _LAYER_GRID:
        resolution_m = _resolution_for_zoom(z)
        grid_bytes = (
            connection.execute(
                _GRID_TILE_SQL,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "build_key": build_key,
                    "resolution_m": resolution_m,
                },
            ).scalar()
            or b""
        )
        chunks.append(bytes(grid_bytes))
    if layers & _LAYER_AMENITIES:
        amenity_bytes = (
            connection.execute(
                _AMENITY_TILE_SQL,
                {"z": z, "x": x, "y": y, "build_key": build_key},
            ).scalar()
            or b""
        )
        chunks.append(bytes(amenity_bytes))
    if layers & _LAYER_TRANSPORT_REALITY:
        transport_reality_bytes = (
            connection.execute(
                _TRANSPORT_REALITY_TILE_SQL,
                {"z": z, "x": x, "y": y, "build_key": build_key},
            ).scalar()
            or b""
        )
        chunks.append(bytes(transport_reality_bytes))
    if layers & _LAYER_SERVICE_DESERTS:
        service_desert_bytes = (
            connection.execute(
                _SERVICE_DESERT_TILE_SQL,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "build_key": build_key,
                    "resolution_m": _resolution_for_zoom(z),
                },
            ).scalar()
            or b""
        )
        chunks.append(bytes(service_desert_bytes))
    if layers & _LAYER_NOISE:
        noise_bytes = (
            connection.execute(
                _NOISE_TILE_SQL,
                {"z": z, "x": x, "y": y, "build_key": build_key},
            ).scalar()
            or b""
        )
        chunks.append(bytes(noise_bytes))
    return b"".join(chunks)


# ── Worker-process state ────────────────────────────────────────────────────

_WORKER_ENGINE = None  # type: ignore[var-annotated]
_WORKER_DB_URL: str | None = None


def _worker_get_engine(db_url: str):
    global _WORKER_ENGINE, _WORKER_DB_URL
    if _WORKER_ENGINE is None or _WORKER_DB_URL != db_url:
        from sqlalchemy import create_engine

        _WORKER_ENGINE = create_engine(db_url, future=True, pool_pre_ping=True)
        _WORKER_DB_URL = db_url
    return _WORKER_ENGINE


def _bake_chunk_worker(
    chunk: list[tuple[int, int, int, int]],
    build_key: str,
    db_url: str,
) -> list[tuple[int, bytes]]:
    """Run inside a worker process. Bake one chunk of tile specs."""
    engine = _worker_get_engine(db_url)
    out: list[tuple[int, bytes]] = []
    with engine.connect() as connection:
        for z, x, y, layers in chunk:
            payload = _tile_mvt_bytes_by_flags(
                connection,
                build_key=build_key,
                z=z,
                x=x,
                y=y,
                layers=layers,
            )
            if not payload:
                continue
            out.append((zxy_to_tileid(z, x, y), gzip.compress(payload)))
    return out


def _noise_tile_mvt_bytes(
    connection,
    *,
    build_key: str,
    z: int,
    x: int,
    y: int,
) -> bytes:
    noise_bytes = (
        connection.execute(
            _NOISE_TILE_SQL,
            {"z": z, "x": x, "y": y, "build_key": build_key},
        ).scalar()
        or b""
    )
    return bytes(noise_bytes)


def _bake_noise_chunk_worker(
    chunk: list[tuple[int, int, int]],
    build_key: str,
    db_url: str,
) -> list[tuple[int, bytes]]:
    """Run inside a worker process. Bake one chunk of noise tile specs."""
    engine = _worker_get_engine(db_url)
    out: list[tuple[int, bytes]] = []
    with engine.connect() as connection:
        for z, x, y in chunk:
            payload = _noise_tile_mvt_bytes(
                connection,
                build_key=build_key,
                z=z,
                x=x,
                y=y,
            )
            if not payload:
                continue
            out.append((zxy_to_tileid(z, x, y), gzip.compress(payload)))
    return out


__all__ = [
    "_LAYER_GRID",
    "_LAYER_AMENITIES",
    "_LAYER_TRANSPORT_REALITY",
    "_LAYER_SERVICE_DESERTS",
    "_LAYER_FINE_GRID",
    "_LAYER_NOISE",
    "_GRID_TILE_SQL",
    "_AMENITY_TILE_SQL",
    "_TRANSPORT_REALITY_TILE_SQL",
    "_SERVICE_DESERT_TILE_SQL",
    "_NOISE_TILE_SQL",
    "_resolution_for_zoom",
    "_tile_mvt_bytes_by_flags",
    "_noise_tile_mvt_bytes",
    "_worker_get_engine",
    "_bake_chunk_worker",
    "_bake_noise_chunk_worker",
]
