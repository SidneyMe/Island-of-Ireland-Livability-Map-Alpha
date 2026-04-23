from __future__ import annotations

from alembic import op


revision = "20260422_000009"
down_revision = "20260420_000008"
branch_labels = None
depends_on = None


def _add_transit_bus_subtier_columns(schema_name: str | None, table_name: str) -> None:
    qualified = table_name if schema_name is None else f"{schema_name}.{table_name}"
    op.execute(
        f"""
        ALTER TABLE {qualified}
            ADD COLUMN IF NOT EXISTS bus_active_days_mask_7d TEXT,
            ADD COLUMN IF NOT EXISTS bus_service_subtier TEXT,
            ADD COLUMN IF NOT EXISTS is_unscheduled_stop BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS has_exception_only_service BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS has_any_bus_service BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS has_daily_bus_service BOOLEAN NOT NULL DEFAULT FALSE
        """
    )


def _drop_transit_bus_subtier_columns(schema_name: str | None, table_name: str) -> None:
    qualified = table_name if schema_name is None else f"{schema_name}.{table_name}"
    op.execute(
        f"""
        ALTER TABLE {qualified}
            DROP COLUMN IF EXISTS has_daily_bus_service,
            DROP COLUMN IF EXISTS has_any_bus_service,
            DROP COLUMN IF EXISTS has_exception_only_service,
            DROP COLUMN IF EXISTS is_unscheduled_stop,
            DROP COLUMN IF EXISTS bus_service_subtier,
            DROP COLUMN IF EXISTS bus_active_days_mask_7d
        """
    )


def upgrade() -> None:
    _add_transit_bus_subtier_columns("transit_derived", "gtfs_stop_service_summary")
    _add_transit_bus_subtier_columns("transit_derived", "gtfs_stop_reality")
    _add_transit_bus_subtier_columns(None, "transport_reality")


def downgrade() -> None:
    _drop_transit_bus_subtier_columns(None, "transport_reality")
    _drop_transit_bus_subtier_columns("transit_derived", "gtfs_stop_reality")
    _drop_transit_bus_subtier_columns("transit_derived", "gtfs_stop_service_summary")
