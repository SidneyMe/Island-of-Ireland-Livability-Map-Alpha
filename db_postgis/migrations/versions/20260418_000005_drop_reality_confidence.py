from __future__ import annotations

from alembic import op


revision = "20260418_000005"
down_revision = "20260417_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for schema, table in (
        (None, "transport_reality"),
        ("transit_derived", "gtfs_stop_reality"),
    ):
        schema_name = schema or "public"
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                      AND table_name = '{table}'
                      AND column_name = 'reality_confidence'
                ) THEN
                    ALTER TABLE {f"{schema}.{table}" if schema else table}
                        DROP COLUMN reality_confidence;
                END IF;
            END
            $$;
            """
        )


def downgrade() -> None:
    for schema, table in (
        (None, "transport_reality"),
        ("transit_derived", "gtfs_stop_reality"),
    ):
        op.execute(
            f"""
            ALTER TABLE {f"{schema}.{table}" if schema else table}
                ADD COLUMN IF NOT EXISTS reality_confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0
            """
        )
