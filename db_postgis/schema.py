from __future__ import annotations

from dataclasses import dataclass

from config import OSM_IMPORT_SCHEMA

from ._dependencies import Engine, inspect, text
from .common import _count_import_rows, root_module
from .tables import (
    IMPORTER_OWNED_RAW_TABLES,
    MANAGED_RAW_SUPPORT_TABLES,
    REQUIRED_PUBLIC_TABLES,
    REQUIRED_RAW_TABLES,
    features,
)


@dataclass(frozen=True)
class _ManagedRawSupportIndexSpec:
    table_name: str
    index_name: str
    columns: tuple[str, ...]
    unique: bool = False


_MANAGED_RAW_SUPPORT_INDEX_SPECS = ()

_EXPECTED_SERVE_INDEXES = (
    "grid_walk_build_resolution_cell_idx",
    "grid_walk_build_resolution_idx",
    "grid_walk_centre_geom_gist",
    "grid_walk_cell_geom_gist",
    "amenities_geom_gist",
)


def table_exists(engine: Engine, table_name: str, schema: str | None = None) -> bool:
    return table_name in inspect(engine).get_table_names(schema=schema)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def osm2pgsql_properties_exists(engine: Engine) -> bool:
    return table_exists(engine, "osm2pgsql_properties", schema=OSM_IMPORT_SCHEMA)


def ensure_managed_raw_support_tables(engine: Engine) -> None:
    with engine.begin() as connection:
        for table in MANAGED_RAW_SUPPORT_TABLES:
            table.create(connection, checkfirst=True)
        connection.execute(
            text(
                f"ALTER TABLE {OSM_IMPORT_SCHEMA}.import_manifest "
                "ADD COLUMN IF NOT EXISTS normalization_scope_hash TEXT NOT NULL DEFAULT ''"
            )
        )


def drop_importer_owned_raw_tables(engine: Engine) -> None:
    schema_sql = _quote_identifier(OSM_IMPORT_SCHEMA)
    with engine.begin() as connection:
        for table_name in IMPORTER_OWNED_RAW_TABLES:
            table_sql = _quote_identifier(table_name)
            connection.execute(text(f"DROP TABLE IF EXISTS {schema_sql}.{table_sql}"))


def ensure_database_ready(engine: Engine) -> None:
    try:
        with engine.begin() as connection:
            connection.execute(text("SELECT 1"))
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OSM_IMPORT_SCHEMA}"))
            has_postgis = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            ).scalar_one()
    except Exception as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "Unable to connect to PostgreSQL/PostGIS. Check DATABASE_URL or POSTGRES_* "
            f"settings. Original error: {exc}"
        ) from exc

    if not has_postgis:
        raise RuntimeError(
            "PostGIS extension is not enabled in the target database. Run schema.sql, "
            "which begins with CREATE EXTENSION IF NOT EXISTS postgis;"
        )

    root_module().ensure_managed_raw_support_tables(engine)

    public_tables = set(inspect(engine).get_table_names())
    missing_public = REQUIRED_PUBLIC_TABLES.difference(public_tables)
    if missing_public:
        raise RuntimeError(
            "Database schema is incomplete. Missing tables: "
            + ", ".join(sorted(missing_public))
            + ". Apply schema.sql first."
        )

    raw_tables = set(inspect(engine).get_table_names(schema=OSM_IMPORT_SCHEMA))
    missing_raw = REQUIRED_RAW_TABLES.difference(raw_tables)
    if missing_raw:
        raise RuntimeError(
            "Database raw schema is incomplete. Missing tables in "
            f"{OSM_IMPORT_SCHEMA}: "
            + ", ".join(sorted(missing_raw))
            + ". Apply schema.sql first."
        )


def find_missing_serve_indexes(engine: Engine) -> list[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename IN ('grid_walk', 'amenities')
                """
            )
        ).all()
    present = {str(row[0]) for row in rows}
    return [name for name in _EXPECTED_SERVE_INDEXES if name not in present]


def import_payload_ready(engine: Engine, import_fingerprint: str, normalization_scope_hash: str) -> bool:
    del normalization_scope_hash
    return raw_import_ready(engine, import_fingerprint)


def raw_import_ready(engine: Engine, import_fingerprint: str) -> bool:
    root = root_module()
    if not root.osm2pgsql_properties_exists(engine):
        return False
    if not root.table_exists(engine, "features", schema=OSM_IMPORT_SCHEMA):
        return False
    with engine.connect() as connection:
        feature_count = _count_import_rows(connection, features, import_fingerprint)
    return bool(feature_count)


def assert_import_payload_ready(engine: Engine, import_fingerprint: str, normalization_scope_hash: str) -> None:
    del normalization_scope_hash
    root = root_module()
    if not root.osm2pgsql_properties_exists(engine):
        raise RuntimeError(
            "No previous osm2pgsql import metadata is available in "
            f"{OSM_IMPORT_SCHEMA}.osm2pgsql_properties. A fresh raw import is required."
        )
    manifest = root.load_import_manifest(engine, import_fingerprint)
    if manifest is None or manifest.get("status") != "complete":
        raise RuntimeError(
            "No complete raw OSM import is available for the current import_fingerprint "
            f"({import_fingerprint})."
        )
    if not root.table_exists(engine, "features", schema=OSM_IMPORT_SCHEMA):
        raise RuntimeError(
            f"Expected raw table {OSM_IMPORT_SCHEMA}.features is missing for "
            f"import_fingerprint={import_fingerprint}."
        )
    if not root.import_payload_ready(engine, import_fingerprint, ""):
        raise RuntimeError(
            "The raw OSM import exists but amenity features are missing or empty "
            f"for import_fingerprint={import_fingerprint}. Re-run --precompute with "
            "--force-precompute to refresh the import."
        )
