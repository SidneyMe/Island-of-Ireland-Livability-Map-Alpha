from __future__ import annotations

from alembic import op


revision = "20260428_000018"
down_revision = "20260427_000017"
branch_labels = None
depends_on = None


_NEW_CHECK = r"""
CHECK (
    (
        db_value ~ '^[0-9]{2}-[0-9]{2}$'
        AND db_low IS NOT NULL
        AND db_high IS NOT NULL
        AND db_high >= db_low
    )
    OR (
        db_value ~ '^[0-9]{2}\+$'
        AND db_low IS NOT NULL
        AND db_high IS NOT NULL
        AND db_high >= db_low
    )
)
"""


def upgrade() -> None:
    op.execute("""
        ALTER TABLE noise_normalized
        DROP CONSTRAINT IF EXISTS noise_normalized_db_value_check
    """)
    op.execute(f"""
        ALTER TABLE noise_normalized
        ADD CONSTRAINT noise_normalized_db_value_check
        {_NEW_CHECK}
    """)

    op.execute("""
        ALTER TABLE noise_resolved_display
        DROP CONSTRAINT IF EXISTS noise_resolved_display_db_value_check
    """)
    op.execute(f"""
        ALTER TABLE noise_resolved_display
        ADD CONSTRAINT noise_resolved_display_db_value_check
        {_NEW_CHECK}
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE noise_normalized
        DROP CONSTRAINT IF EXISTS noise_normalized_db_value_check
    """)
    op.execute("""
        ALTER TABLE noise_normalized
        ADD CONSTRAINT noise_normalized_db_value_check
        CHECK (db_value IN ('45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75+'))
    """)

    op.execute("""
        ALTER TABLE noise_resolved_display
        DROP CONSTRAINT IF EXISTS noise_resolved_display_db_value_check
    """)
    op.execute("""
        ALTER TABLE noise_resolved_display
        ADD CONSTRAINT noise_resolved_display_db_value_check
        CHECK (db_value IN ('45-49', '50-54', '55-59', '60-64', '65-69', '70-74', '75+'))
    """)
