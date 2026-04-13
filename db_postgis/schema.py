from __future__ import annotations

from dataclasses import dataclass

from config import BASE_DIR, OSM_IMPORT_SCHEMA

try:
    from alembic import command
    from alembic.config import Config as AlembicConfig
except ImportError as exc:  # pragma: no cover - depends on installed dependencies
    raise RuntimeError(
        "Missing Alembic dependency. Install requirements.txt before running the DB-backed pipeline."
    ) from exc

from ._dependencies import Engine, inspect, text
from .common import _count_import_rows, root_module
from .tables import (
    IMPORTER_OWNED_RAW_TABLES,
    REQUIRED_PUBLIC_TABLES,
    REQUIRED_RAW_TABLES,
    amenities,
    build_manifest,
    features,
    grid_walk,
    import_manifest,
)


ALEMBIC_INI_PATH = BASE_DIR / "alembic.ini"
ALEMBIC_SCRIPT_LOCATION = BASE_DIR / "db_postgis" / "migrations"
ALEMBIC_INITIAL_REVISION = "20260411_000001"


@dataclass(frozen=True)
class _ManagedIndexSpec:
    index_name: str
    table_name: str
    schema: str = "public"


_MANAGED_TABLES = (
    grid_walk,
    amenities,
    build_manifest,
    import_manifest,
)

_MANAGED_INDEX_SPECS = (
    _ManagedIndexSpec("grid_walk_build_resolution_cell_idx", "grid_walk"),
    _ManagedIndexSpec("grid_walk_build_resolution_idx", "grid_walk"),
    _ManagedIndexSpec("grid_walk_config_resolution_idx", "grid_walk"),
    _ManagedIndexSpec("grid_walk_centre_geom_gist", "grid_walk"),
    _ManagedIndexSpec("grid_walk_cell_geom_gist", "grid_walk"),
    _ManagedIndexSpec("amenities_build_category_idx", "amenities"),
    _ManagedIndexSpec("amenities_config_category_idx", "amenities"),
    _ManagedIndexSpec("amenities_geom_gist", "amenities"),
    _ManagedIndexSpec("build_manifest_status_idx", "build_manifest"),
    _ManagedIndexSpec("build_manifest_extract_config_idx", "build_manifest"),
    _ManagedIndexSpec("build_manifest_import_idx", "build_manifest"),
    _ManagedIndexSpec(
        "osm_raw_import_manifest_path_idx",
        "import_manifest",
        schema=OSM_IMPORT_SCHEMA,
    ),
)

_EXPECTED_SERVE_INDEXES = (
    "grid_walk_build_resolution_cell_idx",
    "grid_walk_build_resolution_idx",
    "grid_walk_centre_geom_gist",
    "grid_walk_cell_geom_gist",
    "amenities_geom_gist",
)


def _table_schema_name(table) -> str:
    return str(table.schema or "public")


def _table_column_names(table) -> tuple[str, ...]:
    return tuple(column.name for column in table.columns)


def _table_names_by_schema(inspector) -> dict[str, set[str]]:
    public_tables = {str(name) for name in inspector.get_table_names()}
    raw_tables = {str(name) for name in inspector.get_table_names(schema=OSM_IMPORT_SCHEMA)}
    return {
        "public": public_tables,
        OSM_IMPORT_SCHEMA: raw_tables,
    }


def _present_index_names(inspector, table_name: str, *, schema: str) -> set[str]:
    return {
        str(index.get("name"))
        for index in inspector.get_indexes(table_name, schema=None if schema == "public" else schema)
        if index.get("name")
    }


def _managed_schema_mismatches(inspector) -> list[str]:
    mismatches: list[str] = []
    tables_by_schema = _table_names_by_schema(inspector)

    for table in _MANAGED_TABLES:
        schema_name = _table_schema_name(table)
        table_name = str(table.name)
        if table_name not in tables_by_schema.get(schema_name, set()):
            mismatches.append(f"missing table {schema_name}.{table_name}")
            continue

        present_columns = {
            str(column["name"])
            for column in inspector.get_columns(
                table_name,
                schema=None if schema_name == "public" else schema_name,
            )
        }
        missing_columns = [
            column_name
            for column_name in _table_column_names(table)
            if column_name not in present_columns
        ]
        mismatches.extend(
            f"missing column {schema_name}.{table_name}.{column_name}"
            for column_name in missing_columns
        )

    for index_spec in _MANAGED_INDEX_SPECS:
        if index_spec.table_name not in tables_by_schema.get(index_spec.schema, set()):
            continue
        present_indexes = _present_index_names(
            inspector,
            index_spec.table_name,
            schema=index_spec.schema,
        )
        if index_spec.index_name not in present_indexes:
            mismatches.append(
                f"missing index {index_spec.schema}.{index_spec.index_name}"
            )

    return mismatches


def _managed_schema_is_empty(inspector) -> bool:
    tables_by_schema = _table_names_by_schema(inspector)
    return not (
        REQUIRED_PUBLIC_TABLES.intersection(tables_by_schema["public"])
        or REQUIRED_RAW_TABLES.intersection(tables_by_schema[OSM_IMPORT_SCHEMA])
    )


def _alembic_config(connection=None) -> AlembicConfig:
    config = AlembicConfig(str(ALEMBIC_INI_PATH))
    config.set_main_option("script_location", str(ALEMBIC_SCRIPT_LOCATION))
    config.attributes["configure_logger"] = False
    if connection is not None:
        config.attributes["connection"] = connection
        engine = getattr(connection, "engine", None)
        if engine is not None:
            rendered_url = engine.url.render_as_string(hide_password=False)
            config.set_main_option(
                "sqlalchemy.url",
                str(rendered_url),
            )
    return config


def _alembic_version_exists(inspector) -> bool:
    return "alembic_version" in {str(name) for name in inspector.get_table_names()}


def _apply_schema_migrations(engine: Engine) -> None:
    with engine.connect() as connection:
        inspector = inspect(connection)
        has_alembic_version = _alembic_version_exists(inspector)
        migration_config = _alembic_config(connection)

        if not has_alembic_version:
            if _managed_schema_is_empty(inspector):
                pass
            else:
                mismatches = _managed_schema_mismatches(inspector)
                if mismatches:
                    raise RuntimeError(
                        "Managed schema exists without alembic_version but does not match the "
                        "supported legacy schema shape. Unsupported drift/manual intervention "
                        f"required: {', '.join(mismatches)}."
                    )
                command.stamp(migration_config, ALEMBIC_INITIAL_REVISION)

        command.upgrade(migration_config, "head")


def table_exists(engine: Engine, table_name: str, schema: str | None = None) -> bool:
    return table_name in inspect(engine).get_table_names(schema=schema)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def osm2pgsql_properties_exists(engine: Engine) -> bool:
    return table_exists(engine, "osm2pgsql_properties", schema=OSM_IMPORT_SCHEMA)


def ensure_managed_raw_support_tables(engine: Engine) -> None:
    inspector = inspect(engine)
    mismatches = [
        mismatch
        for mismatch in _managed_schema_mismatches(inspector)
        if mismatch.startswith(f"missing table {OSM_IMPORT_SCHEMA}.")
        or mismatch.startswith(f"missing column {OSM_IMPORT_SCHEMA}.")
        or mismatch.startswith(f"missing index {OSM_IMPORT_SCHEMA}.")
    ]
    if mismatches:
        raise RuntimeError(
            "Managed raw-support schema is incomplete. Run startup migrations first. "
            + ", ".join(mismatches)
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
            try:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            except Exception as exc:
                raise RuntimeError(
                    "PostGIS extension is not enabled and could not be created automatically. "
                    "Grant permission for CREATE EXTENSION or enable postgis manually. "
                    f"Original error: {exc}"
                ) from exc
            has_postgis = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            ).scalar_one()
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OSM_IMPORT_SCHEMA}"))
    except RuntimeError:
        raise
    except Exception as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "Unable to connect to PostgreSQL/PostGIS. Check DATABASE_URL or POSTGRES_* "
            f"settings. Original error: {exc}"
        ) from exc

    if not has_postgis:
        raise RuntimeError(
            "PostGIS extension is not enabled in the target database and automatic creation "
            "did not succeed."
        )

    _apply_schema_migrations(engine)

    inspector = inspect(engine)
    mismatches = _managed_schema_mismatches(inspector)
    if mismatches:
        raise RuntimeError(
            "Automatic schema migration completed but the managed schema is still incomplete: "
            + ", ".join(mismatches)
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
