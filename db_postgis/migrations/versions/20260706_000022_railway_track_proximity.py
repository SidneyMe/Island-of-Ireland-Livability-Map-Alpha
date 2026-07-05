from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision = "20260706_000022"
down_revision = "20260617_000021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "railway_corridor_manifest",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("transit_config_hash", sa.Text(), nullable=False),
        sa.Column("railway_proximity_hash", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("active_feed_count", sa.Integer(), nullable=False),
        sa.Column("active_service_count", sa.Integer(), nullable=False),
        sa.Column("active_trip_count", sa.Integer(), nullable=False),
        sa.Column("active_shape_count", sa.Integer(), nullable=False),
        sa.Column("warning_count", sa.Integer(), nullable=False),
        sa.Column("warnings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("reality_fingerprint"),
        schema="transit_derived",
    )

    op.create_table(
        "railway_corridors",
        sa.Column("reality_fingerprint", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("railway_proximity_hash", sa.Text(), nullable=False),
        sa.Column("feed_id", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("active_service_count", sa.Integer(), nullable=False),
        sa.Column("active_trip_count", sa.Integer(), nullable=False),
        sa.Column("active_shape_count", sa.Integer(), nullable=False),
        sa.Column("dissolved_shape_count", sa.Integer(), nullable=False),
        sa.Column("route_modes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        schema="transit_derived",
    )
    op.create_index(
        "transit_derived_railway_corridors_reality_feed_idx",
        "railway_corridors",
        ["reality_fingerprint", "feed_id"],
        unique=False,
        schema="transit_derived",
    )


def downgrade() -> None:
    op.drop_index(
        "transit_derived_railway_corridors_reality_feed_idx",
        table_name="railway_corridors",
        schema="transit_derived",
    )
    op.drop_table("railway_corridors", schema="transit_derived")
    op.drop_table("railway_corridor_manifest", schema="transit_derived")
