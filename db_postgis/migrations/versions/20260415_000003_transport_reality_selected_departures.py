from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260415_000003"
down_revision = "20260414_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "transport_reality",
        sa.Column("selected_public_departures_30d", sa.Integer(), nullable=True),
    )
    op.add_column(
        "osm_stop_reality",
        sa.Column("selected_public_departures_30d", sa.Integer(), nullable=True),
        schema="transit_derived",
    )


def downgrade() -> None:
    op.drop_column(
        "osm_stop_reality",
        "selected_public_departures_30d",
        schema="transit_derived",
    )
    op.drop_column("transport_reality", "selected_public_departures_30d")
