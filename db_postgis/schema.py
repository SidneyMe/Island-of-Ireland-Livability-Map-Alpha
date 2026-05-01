from __future__ import annotations

from dataclasses import dataclass

from config import BASE_DIR, OSM_IMPORT_SCHEMA, TRANSIT_DERIVED_SCHEMA, TRANSIT_RAW_SCHEMA

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
    amenities,
    build_manifest,
    features,
    grid_walk,
    import_manifest,
    noise_active_artifact,
    noise_artifact_lineage,
    noise_artifact_manifest,
    noise_grid_artifact,
    noise_normalized,
    noise_polygons,
    noise_resolved_display,
    noise_resolved_provenance,
    service_deserts,
    transport_reality,
    transit_calendar_dates,
    transit_calendar_services,
    transit_feed_manifest,
    transit_gtfs_stop_service_summary,
    transit_gtfs_stop_reality,
    transit_reality_manifest,
    transit_routes,
    transit_service_classification,
    transit_service_desert_cells,
    transit_stop_times,
    transit_stops,
    transit_trips,
    REQUIRED_MANAGED_SCHEMA_TABLES,
)


ALEMBIC_INI_PATH = BASE_DIR / "alembic.ini"
ALEMBIC_SCRIPT_LOCATION = BASE_DIR / "db_postgis" / "migrations"
ALEMBIC_INITIAL_REVISION = "20260411_000001"


@dataclass(frozen=True)
class _ManagedIndexSpec:
    index_name: str
    table_name: str
    schema: str = "public"
    columns: tuple[str, ...] | None = None


_MANAGED_TABLES = (
    grid_walk,
    amenities,
    transport_reality,
    service_deserts,
    noise_polygons,
    noise_artifact_manifest,
    noise_artifact_lineage,
    noise_active_artifact,
    noise_grid_artifact,
    noise_normalized,
    noise_resolved_display,
    noise_resolved_provenance,
    build_manifest,
    import_manifest,
    transit_feed_manifest,
    transit_stops,
    transit_routes,
    transit_trips,
    transit_stop_times,
    transit_calendar_services,
    transit_calendar_dates,
    transit_reality_manifest,
    transit_service_classification,
    transit_gtfs_stop_service_summary,
    transit_gtfs_stop_reality,
    transit_service_desert_cells,
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
    _ManagedIndexSpec("transport_reality_build_source_idx", "transport_reality"),
    _ManagedIndexSpec("transport_reality_status_idx", "transport_reality"),
    _ManagedIndexSpec("transport_reality_geom_gist", "transport_reality"),
    _ManagedIndexSpec("service_deserts_build_resolution_cell_idx", "service_deserts"),
    _ManagedIndexSpec("service_deserts_geom_gist", "service_deserts"),
    _ManagedIndexSpec("noise_polygons_build_metric_idx", "noise_polygons"),
    _ManagedIndexSpec("noise_polygons_source_metric_idx", "noise_polygons"),
    _ManagedIndexSpec("noise_polygons_geom_gist", "noise_polygons"),
    _ManagedIndexSpec("noise_artifact_manifest_status_idx", "noise_artifact_manifest"),
    _ManagedIndexSpec("noise_artifact_manifest_type_status_idx", "noise_artifact_manifest"),
    _ManagedIndexSpec("noise_normalized_geom_gist", "noise_normalized"),
    _ManagedIndexSpec("noise_normalized_group_idx", "noise_normalized"),
    _ManagedIndexSpec(
        "noise_grid_artifact_key_idx",
        "noise_grid_artifact",
        columns=(
            "artifact_hash",
            "jurisdiction",
            "source_type",
            "metric",
            "grid_size_m",
            "cell_x",
            "cell_y",
        ),
    ),
    _ManagedIndexSpec("noise_grid_artifact_geom_gist", "noise_grid_artifact"),
    _ManagedIndexSpec("noise_resolved_display_geom_gist", "noise_resolved_display"),
    _ManagedIndexSpec("noise_resolved_display_filter_idx", "noise_resolved_display"),
    _ManagedIndexSpec("build_manifest_status_idx", "build_manifest"),
    _ManagedIndexSpec("build_manifest_extract_config_idx", "build_manifest"),
    _ManagedIndexSpec("build_manifest_import_idx", "build_manifest"),
    _ManagedIndexSpec(
        "osm_raw_import_manifest_path_idx",
        "import_manifest",
        schema=OSM_IMPORT_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_feed_manifest_feed_idx",
        "feed_manifest",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_stops_feed_stop_idx",
        "stops",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_stops_geom_gist",
        "stops",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_routes_feed_route_idx",
        "routes",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_trips_feed_trip_idx",
        "trips",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_trips_feed_service_idx",
        "trips",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_stop_times_feed_trip_seq_idx",
        "stop_times",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_stop_times_feed_stop_idx",
        "stop_times",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_calendar_services_feed_service_idx",
        "calendar_services",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_raw_calendar_dates_feed_service_date_idx",
        "calendar_dates",
        schema=TRANSIT_RAW_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_reality_manifest_import_idx",
        "reality_manifest",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_service_classification_reality_service_idx",
        "service_classification",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_gtfs_stop_service_summary_reality_stop_idx",
        "gtfs_stop_service_summary",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_gtfs_stop_reality_reality_source_idx",
        "gtfs_stop_reality",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_gtfs_stop_reality_status_idx",
        "gtfs_stop_reality",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_gtfs_stop_reality_geom_gist",
        "gtfs_stop_reality",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_service_desert_cells_build_resolution_cell_idx",
        "service_desert_cells",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
    _ManagedIndexSpec(
        "transit_derived_service_desert_cells_geom_gist",
        "service_desert_cells",
        schema=TRANSIT_DERIVED_SCHEMA,
    ),
)

_LEGACY_MANAGED_TABLES = (
    grid_walk,
    amenities,
    build_manifest,
    import_manifest,
)

_LEGACY_MANAGED_INDEX_SPECS = tuple(
    spec
    for spec in _MANAGED_INDEX_SPECS
    if spec.schema in {"public", OSM_IMPORT_SCHEMA}
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
    schemas = ("public", OSM_IMPORT_SCHEMA, TRANSIT_RAW_SCHEMA, TRANSIT_DERIVED_SCHEMA)
    tables_by_schema: dict[str, set[str]] = {}
    for schema_name in schemas:
        table_names = inspector.get_table_names(
            schema=None if schema_name == "public" else schema_name
        )
        tables_by_schema[schema_name] = {str(name) for name in table_names}
    return tables_by_schema


def _present_indexes(inspector, table_name: str, *, schema: str) -> dict[str, tuple[str, ...] | None]:
    indexes = inspector.get_indexes(table_name, schema=None if schema == "public" else schema)
    present: dict[str, tuple[str, ...] | None] = {}
    for index in indexes:
        name = index.get("name")
        if not name:
            continue
        cols = index.get("column_names")
        if isinstance(cols, list):
            present[str(name)] = tuple(str(col) for col in cols)
        else:
            present[str(name)] = None
    return present


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
        present_indexes = _present_indexes(
            inspector,
            index_spec.table_name,
            schema=index_spec.schema,
        )
        present_cols = present_indexes.get(index_spec.index_name)
        if present_cols is None and index_spec.index_name not in present_indexes:
            mismatches.append(
                f"missing index {index_spec.schema}.{index_spec.index_name}"
            )
            continue
        if index_spec.columns is not None and present_cols is not None and tuple(index_spec.columns) != tuple(present_cols):
            mismatches.append(
                "mismatched index columns "
                f"{index_spec.schema}.{index_spec.index_name}: "
                f"expected {index_spec.columns}, found {present_cols}"
            )

    return mismatches


def _legacy_managed_schema_mismatches(inspector) -> list[str]:
    mismatches: list[str] = []
    tables_by_schema = _table_names_by_schema(inspector)

    for table in _LEGACY_MANAGED_TABLES:
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

    for index_spec in _LEGACY_MANAGED_INDEX_SPECS:
        if index_spec.table_name not in tables_by_schema.get(index_spec.schema, set()):
            continue
        present_indexes = _present_indexes(
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
    return not any(
        required_tables.intersection(tables_by_schema.get(schema_name, set()))
        for schema_name, required_tables in REQUIRED_MANAGED_SCHEMA_TABLES
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
    needs_stamp = False
    with engine.connect() as connection:
        inspector = inspect(connection)
        has_alembic_version = _alembic_version_exists(inspector)

        if not has_alembic_version:
            if not _managed_schema_is_empty(inspector):
                mismatches = _legacy_managed_schema_mismatches(inspector)
                if mismatches:
                    raise RuntimeError(
                        "Managed schema exists without alembic_version but does not match the "
                        "supported legacy schema shape. Unsupported drift/manual intervention "
                        f"required: {', '.join(mismatches)}."
                    )
                needs_stamp = True

    # Run Alembic without an external connection so it manages its own
    # connection and transaction commits. Passing an external connection
    # from engine.connect() caused DDL to be rolled back when the outer
    # SQLAlchemy context manager exited without an explicit commit().
    migration_config = _alembic_config()
    if needs_stamp:
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
            try:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            except Exception as exc:
                raise RuntimeError(
                    "pgcrypto extension could not be created automatically. "
                    "The noise artifact pipeline requires pgcrypto for sha256() in SQL. "
                    "Grant permission or run: CREATE EXTENSION pgcrypto; manually. "
                    f"Original error: {exc}"
                ) from exc
            has_postgis = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            ).scalar_one()
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OSM_IMPORT_SCHEMA}"))
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TRANSIT_RAW_SCHEMA}"))
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TRANSIT_DERIVED_SCHEMA}"))
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
    root = root_module()
    if not raw_import_ready(engine, import_fingerprint):
        return False
    manifest = root.load_import_manifest(engine, import_fingerprint)
    return root._manifest_matches_scope(manifest, normalization_scope_hash)


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
    if not root._manifest_matches_scope(manifest, normalization_scope_hash):
        raise RuntimeError(
            "The raw OSM import manifest does not match the current normalization scope "
            f"for import_fingerprint={import_fingerprint}. Re-run --precompute with "
            "--auto-refresh-import or refresh the import explicitly."
        )
    if not root.import_payload_ready(engine, import_fingerprint, normalization_scope_hash):
        raise RuntimeError(
            "The raw OSM import exists but amenity features are missing or empty "
            f"for import_fingerprint={import_fingerprint}. Re-run --precompute with "
            "--force-precompute to refresh the import."
        )
