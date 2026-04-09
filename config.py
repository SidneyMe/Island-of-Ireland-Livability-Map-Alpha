from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from pyproj import Transformer

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional until dependencies are installed
    def load_dotenv(*args, **kwargs) -> bool:
        return False


BASE_DIR = Path(__file__).resolve().parent
BOUNDARIES_DIR = BASE_DIR / "boundaries"
OSM_DIR = BASE_DIR / "osm"

load_dotenv(BASE_DIR / ".env")


ROI_BOUNDARY_PATH = BOUNDARIES_DIR / "Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson"
ROI_BOUNDARY_LAYER = None

NI_BOUNDARY_PATH = BOUNDARIES_DIR / "osni_open_data_largescale_boundaries_ni_outline.geojson"
NI_BOUNDARY_LAYER = None


OSM_EXTRACT_NAME = "ireland-and-northern-ireland-260405.osm.pbf"
OSM_EXTRACT_PATH = OSM_DIR / OSM_EXTRACT_NAME
OSM_IMPORT_SCHEMA = "osm_raw"
OSM_IMPORTER_BIN = os.getenv("OSM2PGSQL_BIN", "osm2pgsql")
OSM_IMPORTER_CONFIG = BASE_DIR / "osm2pgsql_livability.lua"
IMPORTER_CONFIG_VERSION = "2026-04-08"


def _default_walkgraph_bin() -> str:
    walkgraph_dir = BASE_DIR / "walkgraph" / "target"
    candidates = [
        walkgraph_dir / "release" / "walkgraph.exe",
        walkgraph_dir / "debug" / "walkgraph.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return "walkgraph"


WALKGRAPH_BIN = os.getenv("WALKGRAPH_BIN", _default_walkgraph_bin())
WALKGRAPH_FORMAT_VERSION = 3


TARGET_CRS = "EPSG:2157"
DISPLAY_CRS = "EPSG:4326"

TO_WGS84 = Transformer.from_crs(TARGET_CRS, DISPLAY_CRS, always_xy=True).transform
TO_TARGET = Transformer.from_crs(DISPLAY_CRS, TARGET_CRS, always_xy=True).transform


STUDY_AREA_KIND = "ireland"
M1_CORRIDOR_BUFFER_M = 10_000
M1_CORRIDOR_ANCHORS_WGS84 = [
    (-6.2267, 53.4238),
    (-6.2180, 53.4610),
    (-6.2560, 53.5870),
    (-6.3470, 53.7170),
    (-6.3910, 54.0180),
    (-6.3390, 54.1750),
    (-6.0830, 54.4720),
    (-5.9300, 54.5970),
]


GRID_SIZES_M = [20000, 10000, 5000]
ZOOM_BREAKS = [(11, 5000), (8, 10000), (0, 20000)]


WALK_RADIUS_M = 500
WALKGRAPH_BBOX_PADDING_M = WALK_RADIUS_M


CAPS = {"shops": 5, "transport": 5, "healthcare": 3, "parks": 2}
OUTPUT_HTML = "ireland_livability.html"
ENABLE_STREET_SEARCH = False
DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 8000


TAGS = {
    "shops": {"shop": True},
    "transport": {
        "highway": "bus_stop",
        "railway": ["station", "tram_stop", "halt"],
    },
    "healthcare": {
        "amenity": ["pharmacy", "hospital", "clinic", "doctors", "dentist", "health_centre"],
    },
    "parks": {
        "leisure": ["park", "playground", "nature_reserve", "garden"],
    },
}


CATEGORY_COLORS = {
    "shops": "#2166ac",
    "transport": "#762a83",
    "healthcare": "#d6604d",
    "parks": "#1a9850",
}


CACHE_DIR = BASE_DIR / ".livability_cache"
PMTILES_OUTPUT_PATH = CACHE_DIR / "livability.pmtiles"
PROJECT_TEMP_DIR = BASE_DIR / ".tmp"
PMTILES_SCHEMA_VERSION = 2
GRID_GEOMETRY_SCHEMA_VERSION = 2
CACHE_SCHEMA_VERSION = 7
FORCE_RECOMPUTE = False
USE_COMPRESSED_CACHE = True
MANIFEST_NAME = "manifest.json"


def _file_meta(path: Path) -> dict[str, int]:
    try:
        stat = path.stat()
        return {"mtime_ns": stat.st_mtime_ns, "size": stat.st_size}
    except OSError:
        return {"mtime_ns": 0, "size": 0}


def _content_hash(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 16), b""):
                digest.update(chunk)
        return digest.hexdigest()[:16]
    except OSError:
        return "missing"


def hash_dict(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]


def validate_local_osm_extract(path: Path = OSM_EXTRACT_PATH) -> Path:
    normalized_name = path.name.lower()
    if normalized_name.endswith(".osm.pdf"):
        raise RuntimeError(
            f"Configured OSM extract is '{path}'. This pipeline requires an OSM PBF file "
            "with a '.osm.pbf' extension, not a PDF."
        )
    if not normalized_name.endswith(".osm.pbf"):
        raise RuntimeError(
            f"Configured OSM extract is '{path}'. This pipeline requires a local '.osm.pbf' file."
        )
    if not path.exists():
        raise RuntimeError(
            f"Required local OSM extract was not found at '{path}'. Place the "
            f"file there before running --precompute."
        )
    return path


@dataclass(frozen=True)
class ConfigHashes:
    geo_hash: str
    reach_hash: str
    score_hash: str
    render_hash: str
    config_hash: str


@dataclass(frozen=True)
class SourceState:
    extract_path: Path
    extract_fingerprint: str
    importer_version: str
    importer_config_hash: str
    import_fingerprint: str


@dataclass(frozen=True)
class BuildHashes:
    geo_hash: str
    reach_hash: str
    score_hash: str
    render_hash: str
    config_hash: str
    import_fingerprint: str
    build_key: str


def build_config_hashes() -> ConfigHashes:
    roi_meta = _file_meta(ROI_BOUNDARY_PATH)
    ni_meta = _file_meta(NI_BOUNDARY_PATH)

    geo_params = {
        "roi_path": str(ROI_BOUNDARY_PATH),
        "roi_layer": str(ROI_BOUNDARY_LAYER),
        "roi_mtime_ns": roi_meta["mtime_ns"],
        "roi_size": roi_meta["size"],
        "ni_path": str(NI_BOUNDARY_PATH),
        "ni_layer": str(NI_BOUNDARY_LAYER),
        "ni_mtime_ns": ni_meta["mtime_ns"],
        "ni_size": ni_meta["size"],
        "target_crs": TARGET_CRS,
        "display_crs": DISPLAY_CRS,
        "study_area_kind": STUDY_AREA_KIND,
        "m1_corridor_buffer_m": M1_CORRIDOR_BUFFER_M,
        "m1_corridor_anchors_wgs84": list(M1_CORRIDOR_ANCHORS_WGS84),
        "walk_radius_m": WALK_RADIUS_M,
        "walkgraph_format_version": WALKGRAPH_FORMAT_VERSION,
        "walkgraph_bbox_padding_m": WALKGRAPH_BBOX_PADDING_M,
        "schema_version": CACHE_SCHEMA_VERSION,
    }
    geo_hash = hash_dict(geo_params)

    reach_params = {
        "geo_hash": geo_hash,
        "tags": TAGS,
        "walk_radius_m": WALK_RADIUS_M,
    }
    reach_hash = hash_dict(reach_params)

    score_params = {
        "reach_hash": reach_hash,
        "caps": CAPS,
        "grid_sizes_m": sorted(GRID_SIZES_M),
        "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
    }
    score_hash = hash_dict(score_params)

    render_params = {
        "score_hash": score_hash,
        "zoom_breaks": sorted(ZOOM_BREAKS),
        "output_html": OUTPUT_HTML,
        "category_colors": CATEGORY_COLORS,
        "pmtiles_schema_version": PMTILES_SCHEMA_VERSION,
    }
    render_hash = hash_dict(render_params)

    config_hash = hash_dict(
        {
            "score_hash": score_hash,
            "render_hash": render_hash,
            "schema_version": CACHE_SCHEMA_VERSION,
        }
    )

    return ConfigHashes(
        geo_hash=geo_hash,
        reach_hash=reach_hash,
        score_hash=score_hash,
        render_hash=render_hash,
        config_hash=config_hash,
    )


def extract_fingerprint(path: Path) -> str:
    validated_path = validate_local_osm_extract(path)
    extract_meta = _file_meta(validated_path)
    return hash_dict(
        {
            "extract_path": str(validated_path.resolve()),
            "extract_size": extract_meta["size"],
            "extract_mtime_ns": extract_meta["mtime_ns"],
            "extract_content_hash": _content_hash(validated_path),
        }
    )


def importer_config_hash() -> str:
    return hash_dict(
        {
            "import_schema": OSM_IMPORT_SCHEMA,
            "importer_bin": OSM_IMPORTER_BIN,
            "importer_config_path": str(OSM_IMPORTER_CONFIG),
            "importer_config_content": _content_hash(OSM_IMPORTER_CONFIG),
            "importer_config_version": IMPORTER_CONFIG_VERSION,
        }
    )


def build_source_state(importer_version: str, path: Path = OSM_EXTRACT_PATH) -> SourceState:
    validated_path = validate_local_osm_extract(path)
    extract_hash = extract_fingerprint(validated_path)
    importer_hash = importer_config_hash()
    import_hash = hash_dict(
        {
            "extract_fingerprint": extract_hash,
            "importer_version": importer_version,
            "importer_config_hash": importer_hash,
        }
    )
    return SourceState(
        extract_path=validated_path,
        extract_fingerprint=extract_hash,
        importer_version=importer_version,
        importer_config_hash=importer_hash,
        import_fingerprint=import_hash,
    )


def build_hashes_for_import(import_fingerprint: str) -> BuildHashes:
    geo_hash = hash_dict(
        {
            "base_geo_hash": HASHES.geo_hash,
            "import_fingerprint": import_fingerprint,
        }
    )
    reach_hash = hash_dict(
        {
            "geo_hash": geo_hash,
            "tags": TAGS,
            "walk_radius_m": WALK_RADIUS_M,
        }
    )
    score_hash = hash_dict(
        {
            "reach_hash": reach_hash,
            "caps": CAPS,
            "grid_sizes_m": sorted(GRID_SIZES_M),
            "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
        }
    )
    build_key = hash_dict(
        {
            "import_fingerprint": import_fingerprint,
            "config_hash": HASHES.config_hash,
        }
    )
    return BuildHashes(
        geo_hash=geo_hash,
        reach_hash=reach_hash,
        score_hash=score_hash,
        render_hash=HASHES.render_hash,
        config_hash=HASHES.config_hash,
        import_fingerprint=import_fingerprint,
        build_key=build_key,
    )


HASHES = build_config_hashes()


def current_normalization_scope_hash() -> str:
    return HASHES.geo_hash


def package_snapshot() -> dict[str, str]:
    packages = (
        "geopandas",
        "shapely",
        "pyproj",
        "sqlalchemy",
        "geoalchemy2",
        "psycopg",
        "python-dotenv",
        "numpy",
        "scikit-learn",
        "igraph",
    )
    snapshot: dict[str, str] = {}
    for package in packages:
        try:
            snapshot[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            snapshot[package] = "unknown"
    return snapshot


def python_version() -> str:
    return sys.version.split()[0]


def database_url() -> str:
    raw_url = os.getenv("DATABASE_URL")
    if raw_url:
        if raw_url.startswith("postgres://"):
            return "postgresql+psycopg://" + raw_url[len("postgres://"):]
        if raw_url.startswith("postgresql://"):
            return "postgresql+psycopg://" + raw_url[len("postgresql://"):]
        return raw_url

    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    missing = [
        name
        for name, value in (
            ("POSTGRES_HOST", host),
            ("POSTGRES_DB", database),
            ("POSTGRES_USER", user),
            ("POSTGRES_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Database configuration is missing. Set DATABASE_URL or "
            + ", ".join(missing)
            + "."
        )

    return (
        "postgresql+psycopg://"
        f"{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(database)}"
    )
