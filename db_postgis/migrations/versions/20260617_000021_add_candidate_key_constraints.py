from __future__ import annotations

from typing import Any, NamedTuple

from alembic import op
import sqlalchemy as sa


revision = "20260617_000021"
down_revision = "20260501_000020"
branch_labels = None
depends_on = None


class IntegrityCheckResult(NamedTuple):
    duplicate_group_count: int
    duplicate_sample_rows: tuple[dict[str, Any], ...]
    null_issues: tuple[tuple[str, int], ...]
    null_sample_rows: tuple[dict[str, Any], ...]

    @property
    def has_duplicates(self) -> bool:
        return self.duplicate_group_count > 0

    @property
    def has_nulls(self) -> bool:
        return bool(self.null_issues)


class PrimaryKeySpec(NamedTuple):
    schema: str
    table: str
    columns: tuple[str, ...]
    constraint_name: str
    using_index_name: str | None = None

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.table}"


class ForeignKeySpec(NamedTuple):
    schema: str
    table: str
    columns: tuple[str, ...]
    constraint_name: str
    referenced_schema: str
    referenced_table: str
    referenced_columns: tuple[str, ...]
    ondelete: str | None = None

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.table}"


PRIMARY_KEY_SPECS: tuple[PrimaryKeySpec, ...] = (
    PrimaryKeySpec(
        schema="public",
        table="grid_walk",
        columns=("build_key", "resolution_m", "cell_id"),
        constraint_name="grid_walk_build_resolution_cell_pkey",
        using_index_name="grid_walk_build_resolution_cell_idx",
    ),
    PrimaryKeySpec(
        schema="public",
        table="service_deserts",
        columns=("build_key", "resolution_m", "cell_id"),
        constraint_name="service_deserts_build_resolution_cell_pkey",
        using_index_name="service_deserts_build_resolution_cell_idx",
    ),
    PrimaryKeySpec(
        schema="transit_derived",
        table="service_classification",
        columns=("reality_fingerprint", "feed_id", "service_id"),
        constraint_name="transit_derived_service_classification_pkey",
    ),
    PrimaryKeySpec(
        schema="transit_derived",
        table="gtfs_stop_service_summary",
        columns=("reality_fingerprint", "feed_id", "stop_id"),
        constraint_name="transit_derived_gtfs_stop_service_summary_pkey",
    ),
    PrimaryKeySpec(
        schema="transit_derived",
        table="gtfs_stop_reality",
        columns=("reality_fingerprint", "source_ref"),
        constraint_name="transit_derived_gtfs_stop_reality_pkey",
    ),
    PrimaryKeySpec(
        schema="transit_derived",
        table="service_desert_cells",
        columns=("build_key", "resolution_m", "cell_id"),
        constraint_name="transit_derived_service_desert_cells_pkey",
    ),
)


FOREIGN_KEY_SPECS: tuple[ForeignKeySpec, ...] = (
    ForeignKeySpec(
        schema="public",
        table="grid_walk",
        columns=("build_key",),
        constraint_name="grid_walk_build_key_fkey",
        referenced_schema="public",
        referenced_table="build_manifest",
        referenced_columns=("build_key",),
        ondelete="CASCADE",
    ),
    ForeignKeySpec(
        schema="public",
        table="service_deserts",
        columns=("build_key",),
        constraint_name="service_deserts_build_key_fkey",
        referenced_schema="public",
        referenced_table="build_manifest",
        referenced_columns=("build_key",),
        ondelete="CASCADE",
    ),
    ForeignKeySpec(
        schema="transit_derived",
        table="service_desert_cells",
        columns=("build_key",),
        constraint_name="transit_derived_service_desert_cells_build_key_fkey",
        referenced_schema="public",
        referenced_table="build_manifest",
        referenced_columns=("build_key",),
        ondelete="CASCADE",
    ),
)


DEFAULT_SAMPLE_LIMIT = 5


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_table_name(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _column_list(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote_identifier(column) for column in columns)


def _table_exists(inspector, schema: str, table: str) -> bool:
    return table in inspector.get_table_names(schema=schema)


def _matching_primary_key_exists(inspector, spec: PrimaryKeySpec) -> bool:
    pk = inspector.get_pk_constraint(spec.table, schema=spec.schema) or {}
    return tuple(pk.get("constrained_columns") or ()) == spec.columns


def _primary_key_name(inspector, spec: PrimaryKeySpec) -> str | None:
    pk = inspector.get_pk_constraint(spec.table, schema=spec.schema) or {}
    name = pk.get("name")
    return None if name is None else str(name)


def _matching_unique_constraint_exists(inspector, spec: PrimaryKeySpec) -> bool:
    unique_constraints = inspector.get_unique_constraints(spec.table, schema=spec.schema) or []
    for unique_constraint in unique_constraints:
        if tuple(unique_constraint.get("column_names") or ()) == spec.columns:
            return True
    return False


def _matching_foreign_key_exists(inspector, spec: ForeignKeySpec) -> bool:
    foreign_keys = inspector.get_foreign_keys(spec.table, schema=spec.schema) or []
    for foreign_key in foreign_keys:
        if tuple(foreign_key.get("constrained_columns") or ()) != spec.columns:
            continue
        if tuple(foreign_key.get("referred_columns") or ()) != spec.referenced_columns:
            continue
        if foreign_key.get("referred_schema") != spec.referenced_schema:
            continue
        if foreign_key.get("referred_table") != spec.referenced_table:
            continue
        return True
    return False


def _referenced_key_is_primary_or_unique(inspector, spec: ForeignKeySpec) -> bool:
    pk = inspector.get_pk_constraint(spec.referenced_table, schema=spec.referenced_schema) or {}
    if tuple(pk.get("constrained_columns") or ()) == spec.referenced_columns:
        return True
    unique_constraints = inspector.get_unique_constraints(
        spec.referenced_table,
        schema=spec.referenced_schema,
    ) or []
    for unique_constraint in unique_constraints:
        if tuple(unique_constraint.get("column_names") or ()) == spec.referenced_columns:
            return True
    return False


def _foreign_key_name_matches(inspector, spec: ForeignKeySpec) -> bool:
    foreign_keys = inspector.get_foreign_keys(spec.table, schema=spec.schema) or []
    for foreign_key in foreign_keys:
        if foreign_key.get("name") != spec.constraint_name:
            continue
        if tuple(foreign_key.get("constrained_columns") or ()) != spec.columns:
            continue
        if tuple(foreign_key.get("referred_columns") or ()) != spec.referenced_columns:
            continue
        if foreign_key.get("referred_schema") != spec.referenced_schema:
            continue
        if foreign_key.get("referred_table") != spec.referenced_table:
            continue
        return True
    return False


def _duplicate_group_count_sql(spec: PrimaryKeySpec) -> str:
    table_sql = _qualified_table_name(spec.schema, spec.table)
    columns_sql = _column_list(spec.columns)
    return (
        "SELECT COUNT(*) FROM ("
        f"SELECT {columns_sql} FROM {table_sql} "
        f"GROUP BY {columns_sql} HAVING COUNT(*) > 1"
        ") AS duplicate_groups"
    )


def _duplicate_sample_sql(spec: PrimaryKeySpec, *, sample_limit: int = DEFAULT_SAMPLE_LIMIT) -> str:
    table_sql = _qualified_table_name(spec.schema, spec.table)
    columns_sql = _column_list(spec.columns)
    return (
        f"SELECT {columns_sql}, COUNT(*) AS duplicate_rows "
        f"FROM {table_sql} "
        f"GROUP BY {columns_sql} "
        f"HAVING COUNT(*) > 1 "
        f"ORDER BY {columns_sql} "
        f"LIMIT {int(sample_limit)}"
    )


def _null_count_sql(spec: PrimaryKeySpec) -> str:
    table_sql = _qualified_table_name(spec.schema, spec.table)
    count_expressions = ", ".join(
        f"COUNT(*) FILTER (WHERE {_quote_identifier(column)} IS NULL) AS {_quote_identifier(column + '_null_count')}"
        for column in spec.columns
    )
    return f"SELECT {count_expressions} FROM {table_sql}"


def _null_sample_sql(spec: PrimaryKeySpec, *, sample_limit: int = DEFAULT_SAMPLE_LIMIT) -> str:
    table_sql = _qualified_table_name(spec.schema, spec.table)
    columns_sql = _column_list(spec.columns)
    null_predicate = " OR ".join(f"{_quote_identifier(column)} IS NULL" for column in spec.columns)
    return (
        f"SELECT {columns_sql} FROM {table_sql} "
        f"WHERE {null_predicate} "
        f"ORDER BY {columns_sql} "
        f"LIMIT {int(sample_limit)}"
    )


def _check_integrity(
    connection,
    spec: PrimaryKeySpec,
    *,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> IntegrityCheckResult:
    duplicate_group_count = int(connection.execute(sa.text(_duplicate_group_count_sql(spec))).scalar_one())
    null_count_rows = connection.execute(sa.text(_null_count_sql(spec))).mappings().all()
    null_row = dict(null_count_rows[0]) if null_count_rows else {}
    null_issues = tuple(
        (
            column,
            int(null_row.get(f"{column}_null_count") or 0),
        )
        for column in spec.columns
        if int(null_row.get(f"{column}_null_count") or 0) > 0
    )

    duplicate_sample_rows: tuple[dict[str, Any], ...] = ()
    if duplicate_group_count > 0:
        duplicate_sample_rows = tuple(
            dict(row)
            for row in connection.execute(sa.text(_duplicate_sample_sql(spec, sample_limit=sample_limit))).mappings().all()
        )

    null_sample_rows: tuple[dict[str, Any], ...] = ()
    if null_issues:
        null_sample_rows = tuple(
            dict(row)
            for row in connection.execute(sa.text(_null_sample_sql(spec, sample_limit=sample_limit))).mappings().all()
        )

    return IntegrityCheckResult(
        duplicate_group_count=duplicate_group_count,
        duplicate_sample_rows=duplicate_sample_rows,
        null_issues=null_issues,
        null_sample_rows=null_sample_rows,
    )


def _format_integrity_failure(spec: PrimaryKeySpec, result: IntegrityCheckResult) -> str:
    columns = ", ".join(spec.columns)
    lines = [f"Constraint preflight failed for {spec.label} ({columns})"]
    if result.has_duplicates:
        lines.append(
            f"  - duplicate groups found: {result.duplicate_group_count} for ({columns})"
        )
        if result.duplicate_sample_rows:
            lines.append("  - sample duplicate keys:")
            for row in result.duplicate_sample_rows:
                parts = [f"{column}={row.get(column)!r}" for column in spec.columns]
                duplicate_rows = row.get("duplicate_rows")
                suffix = "" if duplicate_rows is None else f" (duplicate rows: {duplicate_rows})"
                lines.append("    - " + ", ".join(parts) + suffix)
    if result.has_nulls:
        lines.append(f"  - NULL key values found for ({columns})")
        for column_name, null_row_count in result.null_issues:
            plural = "row" if null_row_count == 1 else "rows"
            lines.append(f"    - {column_name}: {null_row_count} {plural} with NULL")
        if result.null_sample_rows:
            lines.append("  - sample NULL rows:")
            for row in result.null_sample_rows:
                null_columns = [
                    column for column in spec.columns if row.get(column) is None
                ]
                parts = [f"{column}={row.get(column)!r}" for column in spec.columns]
                suffix = ""
                if null_columns:
                    suffix = f" (NULL columns: {', '.join(null_columns)})"
                lines.append("    - " + ", ".join(parts) + suffix)
    return "\n".join(lines)


def _ensure_clean_candidate_keys(connection, specs: tuple[PrimaryKeySpec, ...]) -> None:
    failures: list[str] = []
    for spec in specs:
        if not _table_exists(sa.inspect(connection), spec.schema, spec.table):
            continue
        result = _check_integrity(connection, spec)
        if result.has_duplicates or result.has_nulls:
            failures.append(_format_integrity_failure(spec, result))
    if failures:
        raise RuntimeError(
            "Cannot add candidate-key constraints until the database is clean.\n"
            + "\n\n".join(failures)
        )


def _ensure_referenced_keys_valid(inspector, specs: tuple[ForeignKeySpec, ...]) -> None:
    failures: list[str] = []
    for spec in specs:
        if not _table_exists(inspector, spec.schema, spec.table):
            continue
        if not _table_exists(inspector, spec.referenced_schema, spec.referenced_table):
            continue
        if _referenced_key_is_primary_or_unique(inspector, spec):
            continue
        failures.append(
            f"{spec.label} -> {spec.referenced_schema}.{spec.referenced_table}"
            f"({', '.join(spec.referenced_columns)})"
        )
    if failures:
        raise RuntimeError(
            "Cannot add foreign keys until the referenced keys are primary or unique: "
            + "; ".join(failures)
        )


def _apply_primary_key(inspector, spec: PrimaryKeySpec) -> None:
    if not _table_exists(inspector, spec.schema, spec.table):
        return
    if _matching_primary_key_exists(inspector, spec) or _matching_unique_constraint_exists(inspector, spec):
        return
    if spec.using_index_name:
        indexes = {index.get("name") for index in inspector.get_indexes(spec.table, schema=spec.schema) or []}
        if spec.using_index_name in indexes:
            op.execute(
                f"ALTER TABLE {_qualified_table_name(spec.schema, spec.table)} "
                f"ADD CONSTRAINT {_quote_identifier(spec.constraint_name)} "
                f"PRIMARY KEY USING INDEX {_quote_identifier(spec.using_index_name)}"
            )
            return
    op.create_primary_key(
        spec.constraint_name,
        spec.table,
        list(spec.columns),
        schema=spec.schema,
    )


def _apply_foreign_key(inspector, spec: ForeignKeySpec) -> None:
    if not _table_exists(inspector, spec.schema, spec.table):
        return
    if not _table_exists(inspector, spec.referenced_schema, spec.referenced_table):
        return
    if not _referenced_key_is_primary_or_unique(inspector, spec):
        raise RuntimeError(
            f"Cannot add foreign key {spec.constraint_name}: "
            f"referenced key {spec.referenced_schema}.{spec.referenced_table} "
            f"({', '.join(spec.referenced_columns)}) is not primary/unique."
        )
    if _matching_foreign_key_exists(inspector, spec):
        return
    op.create_foreign_key(
        spec.constraint_name,
        source_table=spec.table,
        referent_table=spec.referenced_table,
        local_cols=list(spec.columns),
        remote_cols=list(spec.referenced_columns),
        source_schema=spec.schema,
        referent_schema=spec.referenced_schema,
        ondelete=spec.ondelete,
    )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    _ensure_clean_candidate_keys(bind, PRIMARY_KEY_SPECS)
    _ensure_referenced_keys_valid(inspector, FOREIGN_KEY_SPECS)

    for spec in PRIMARY_KEY_SPECS:
        _apply_primary_key(inspector, spec)
    for spec in FOREIGN_KEY_SPECS:
        _apply_foreign_key(inspector, spec)


def _drop_foreign_key(inspector, spec: ForeignKeySpec) -> None:
    if not _table_exists(inspector, spec.schema, spec.table):
        return
    if not _foreign_key_name_matches(inspector, spec):
        return
    op.execute(
        f"ALTER TABLE {_qualified_table_name(spec.schema, spec.table)} "
        f"DROP CONSTRAINT IF EXISTS {_quote_identifier(spec.constraint_name)}"
    )


def _drop_primary_key(inspector, spec: PrimaryKeySpec) -> None:
    if not _table_exists(inspector, spec.schema, spec.table):
        return
    if _primary_key_name(inspector, spec) != spec.constraint_name:
        return
    op.execute(
        f"ALTER TABLE {_qualified_table_name(spec.schema, spec.table)} "
        f"DROP CONSTRAINT IF EXISTS {_quote_identifier(spec.constraint_name)}"
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    for spec in reversed(FOREIGN_KEY_SPECS):
        _drop_foreign_key(inspector, spec)
    for spec in reversed(PRIMARY_KEY_SPECS):
        _drop_primary_key(inspector, spec)
