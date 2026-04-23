from __future__ import annotations

from alembic import op


revision = "20260423_000010"
down_revision = "20260422_000009"
branch_labels = None
depends_on = None


_FREQUENCY_COLUMNS = (
    ("weekday_morning_peak_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("weekday_evening_peak_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("weekday_offpeak_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("saturday_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("sunday_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("friday_evening_deps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
    ("transport_score_units", "INTEGER NOT NULL DEFAULT 0"),
)


def _qualified(schema_name: str | None, table_name: str) -> str:
    return table_name if schema_name is None else f"{schema_name}.{table_name}"


def _add_frequency_columns(schema_name: str | None, table_name: str) -> None:
    qualified = _qualified(schema_name, table_name)
    column_sql = ",\n            ".join(
        f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        for column_name, column_type in _FREQUENCY_COLUMNS
    )
    op.execute(
        f"""
        ALTER TABLE {qualified}
            {column_sql}
        """
    )


def _drop_frequency_columns(schema_name: str | None, table_name: str) -> None:
    qualified = _qualified(schema_name, table_name)
    column_sql = ",\n            ".join(
        f"DROP COLUMN IF EXISTS {column_name}"
        for column_name, _ in reversed(_FREQUENCY_COLUMNS)
    )
    op.execute(
        f"""
        ALTER TABLE {qualified}
            {column_sql}
        """
    )


def upgrade() -> None:
    _add_frequency_columns("transit_derived", "gtfs_stop_service_summary")
    _add_frequency_columns("transit_derived", "gtfs_stop_reality")
    _add_frequency_columns(None, "transport_reality")


def downgrade() -> None:
    _drop_frequency_columns(None, "transport_reality")
    _drop_frequency_columns("transit_derived", "gtfs_stop_reality")
    _drop_frequency_columns("transit_derived", "gtfs_stop_service_summary")
