from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision = "20260411_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS osm_raw")

    op.create_table(
        "grid_walk",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("resolution_m", sa.Integer(), nullable=False),
        sa.Column("cell_id", sa.Text(), nullable=False),
        sa.Column("centre_geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
        sa.Column("effective_area_m2", sa.Float(), nullable=False),
        sa.Column("effective_area_ratio", sa.Float(), nullable=False),
        sa.Column("counts_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("scores_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("total_score", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "grid_walk_build_resolution_cell_idx",
        "grid_walk",
        ["build_key", "resolution_m", "cell_id"],
        unique=True,
    )
    op.create_index(
        "grid_walk_build_resolution_idx",
        "grid_walk",
        ["build_key", "resolution_m"],
        unique=False,
    )
    op.create_index(
        "grid_walk_config_resolution_idx",
        "grid_walk",
        ["config_hash", "resolution_m"],
        unique=False,
    )
    op.create_index(
        "grid_walk_centre_geom_gist",
        "grid_walk",
        ["centre_geom"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        "grid_walk_cell_geom_gist",
        "grid_walk",
        ["cell_geom"],
        unique=False,
        postgresql_using="gist",
    )

    op.create_table(
        "amenities",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("geom", Geometry("POINT", srid=4326), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "amenities_build_category_idx",
        "amenities",
        ["build_key", "category"],
        unique=False,
    )
    op.create_index(
        "amenities_config_category_idx",
        "amenities",
        ["config_hash", "category"],
        unique=False,
    )
    op.create_index(
        "amenities_geom_gist",
        "amenities",
        ["geom"],
        unique=False,
        postgresql_using="gist",
    )

    op.create_table(
        "build_manifest",
        sa.Column("build_key", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("extract_path", sa.Text(), nullable=False),
        sa.Column("geo_hash", sa.Text(), nullable=False),
        sa.Column("reach_hash", sa.Text(), nullable=False),
        sa.Column("score_hash", sa.Text(), nullable=False),
        sa.Column("render_hash", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("python_version", sa.Text(), nullable=False),
        sa.Column("packages_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("build_key"),
    )
    op.create_index(
        "build_manifest_status_idx",
        "build_manifest",
        ["status"],
        unique=False,
    )
    op.execute(
        "CREATE INDEX build_manifest_extract_config_idx "
        "ON build_manifest (extract_path, config_hash, completed_at DESC)"
    )
    op.create_index(
        "build_manifest_import_idx",
        "build_manifest",
        ["import_fingerprint"],
        unique=False,
    )

    op.create_table(
        "import_manifest",
        sa.Column("import_fingerprint", sa.Text(), nullable=False),
        sa.Column("extract_path", sa.Text(), nullable=False),
        sa.Column("extract_fingerprint", sa.Text(), nullable=False),
        sa.Column("importer_version", sa.Text(), nullable=False),
        sa.Column("importer_config_hash", sa.Text(), nullable=False),
        sa.Column("normalization_scope_hash", sa.Text(), server_default="", nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("import_fingerprint"),
        schema="osm_raw",
    )
    op.execute(
        "CREATE INDEX osm_raw_import_manifest_path_idx "
        "ON osm_raw.import_manifest (extract_path, completed_at DESC)"
    )


def downgrade() -> None:
    op.drop_index("osm_raw_import_manifest_path_idx", table_name="import_manifest", schema="osm_raw")
    op.drop_table("import_manifest", schema="osm_raw")

    op.drop_index("build_manifest_import_idx", table_name="build_manifest")
    op.drop_index("build_manifest_extract_config_idx", table_name="build_manifest")
    op.drop_index("build_manifest_status_idx", table_name="build_manifest")
    op.drop_table("build_manifest")

    op.drop_index("amenities_geom_gist", table_name="amenities")
    op.drop_index("amenities_config_category_idx", table_name="amenities")
    op.drop_index("amenities_build_category_idx", table_name="amenities")
    op.drop_table("amenities")

    op.drop_index("grid_walk_cell_geom_gist", table_name="grid_walk")
    op.drop_index("grid_walk_centre_geom_gist", table_name="grid_walk")
    op.drop_index("grid_walk_config_resolution_idx", table_name="grid_walk")
    op.drop_index("grid_walk_build_resolution_idx", table_name="grid_walk")
    op.drop_index("grid_walk_build_resolution_cell_idx", table_name="grid_walk")
    op.drop_table("grid_walk")
