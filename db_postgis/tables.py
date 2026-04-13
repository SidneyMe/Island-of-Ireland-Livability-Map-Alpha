from __future__ import annotations

from config import OSM_IMPORT_SCHEMA

from ._dependencies import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Geometry,
    Integer,
    JSONB,
    MetaData,
    Table,
    Text,
)
from .common import _table_key


metadata = MetaData()


grid_walk = Table(
    "grid_walk",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("resolution_m", Integer, nullable=False),
    Column("cell_id", Text, nullable=False),
    Column("centre_geom", Geometry("POINT", srid=4326), nullable=False),
    Column("cell_geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("effective_area_m2", Float, nullable=False),
    Column("effective_area_ratio", Float, nullable=False),
    Column("counts_json", JSONB, nullable=False),
    Column("scores_json", JSONB, nullable=False),
    Column("total_score", Float, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

amenities = Table(
    "amenities",
    metadata,
    Column("build_key", Text, nullable=False),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("geom", Geometry("POINT", srid=4326), nullable=False),
    Column("source", Text, nullable=False),
    Column("source_ref", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

build_manifest = Table(
    "build_manifest",
    metadata,
    Column("build_key", Text, primary_key=True),
    Column("config_hash", Text, nullable=False),
    Column("import_fingerprint", Text, nullable=False),
    Column("extract_path", Text, nullable=False),
    Column("geo_hash", Text, nullable=False),
    Column("reach_hash", Text, nullable=False),
    Column("score_hash", Text, nullable=False),
    Column("render_hash", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("python_version", Text, nullable=False),
    Column("packages_json", JSONB, nullable=False),
    Column("summary_json", JSONB, nullable=False),
)

import_manifest = Table(
    "import_manifest",
    metadata,
    Column("import_fingerprint", Text, primary_key=True),
    Column("extract_path", Text, nullable=False),
    Column("extract_fingerprint", Text, nullable=False),
    Column("importer_version", Text, nullable=False),
    Column("importer_config_hash", Text, nullable=False),
    Column("normalization_scope_hash", Text, nullable=False, server_default=""),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    schema=OSM_IMPORT_SCHEMA,
)

features = Table(
    "features",
    metadata,
    Column("import_fingerprint", Text, nullable=False),
    Column("osm_type", Text, nullable=False),
    Column("osm_id", BigInteger, nullable=False),
    Column("category", Text, nullable=False),
    Column("name", Text, nullable=True),
    Column("tags_json", JSONB, nullable=False),
    Column("geom", Geometry("GEOMETRY", srid=4326), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    schema=OSM_IMPORT_SCHEMA,
)


REQUIRED_PUBLIC_TABLES = {
    "grid_walk",
    "amenities",
    "build_manifest",
}

REQUIRED_RAW_TABLES = {
    "import_manifest",
}

OPTIONAL_IMPORTED_TABLES: set[str] = {"features"}

MANAGED_RAW_SUPPORT_TABLES = (import_manifest,)

IMPORTER_OWNED_RAW_TABLES = ("features",)

GEOMETRY_FIELDS = {
    _table_key(grid_walk): ("centre_geom", "cell_geom"),
    _table_key(amenities): ("geom",),
    _table_key(features): ("geom",),
}
