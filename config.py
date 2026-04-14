from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
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


def _optional_positive_int_env(name: str) -> int | None:
    raw_value = os.getenv(name)
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized:
        return None
    try:
        value = int(normalized)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer when set; got {raw_value!r}.") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer when set; got {raw_value!r}.")
    return value


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
LIVABILITY_SURFACE_THREADS = _optional_positive_int_env("LIVABILITY_SURFACE_THREADS")


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


BuildProfile = Literal["full", "dev"]
DEFAULT_BUILD_PROFILE: BuildProfile = "full"


@dataclass(frozen=True)
class BuildProfileSettings:
    name: BuildProfile
    coarse_vector_resolutions_m: tuple[int, ...]
    fine_resolutions_m: tuple[int, ...]
    surface_zoom_breaks: tuple[tuple[int, int], ...]
    fine_surface_enabled: bool

    @property
    def surface_resolutions_m(self) -> list[int]:
        return list(self.coarse_vector_resolutions_m + self.fine_resolutions_m)

    @property
    def grid_sizes_m(self) -> list[int]:
        return list(self.coarse_vector_resolutions_m)


_BUILD_PROFILE_SETTINGS: dict[BuildProfile, BuildProfileSettings] = {
    "full": BuildProfileSettings(
        name="full",
        coarse_vector_resolutions_m=(20_000, 10_000, 5_000),
        fine_resolutions_m=(2_500, 1_000, 500, 250, 100, 50),
        surface_zoom_breaks=(
            (18, 50),
            (16, 100),
            (15, 250),
            (14, 500),
            (13, 1_000),
            (12, 2_500),
            (10, 5_000),
            (8, 10_000),
            (0, 20_000),
        ),
        fine_surface_enabled=True,
    ),
    "dev": BuildProfileSettings(
        name="dev",
        coarse_vector_resolutions_m=(20_000, 10_000, 5_000),
        fine_resolutions_m=(),
        surface_zoom_breaks=(
            (10, 5_000),
            (8, 10_000),
            (0, 20_000),
        ),
        fine_surface_enabled=False,
    ),
}


def normalize_build_profile(profile: str | None = None) -> BuildProfile:
    normalized = DEFAULT_BUILD_PROFILE if profile is None else str(profile).strip().lower()
    if normalized not in _BUILD_PROFILE_SETTINGS:
        raise ValueError(f"Unsupported build profile: {profile!r}")
    return cast(BuildProfile, normalized)


def build_profile_settings(profile: str | None = None) -> BuildProfileSettings:
    return _BUILD_PROFILE_SETTINGS[normalize_build_profile(profile)]


FULL_BUILD_PROFILE_SETTINGS = build_profile_settings("full")


COARSE_VECTOR_RESOLUTIONS_M = list(FULL_BUILD_PROFILE_SETTINGS.coarse_vector_resolutions_m)
FINE_RESOLUTIONS_M = list(FULL_BUILD_PROFILE_SETTINGS.fine_resolutions_m)
CANONICAL_BASE_RESOLUTION_M = 50
SURFACE_RESOLUTIONS_M = COARSE_VECTOR_RESOLUTIONS_M + FINE_RESOLUTIONS_M
SURFACE_ZOOM_BREAKS = list(FULL_BUILD_PROFILE_SETTINGS.surface_zoom_breaks)
GRID_SIZES_M = list(COARSE_VECTOR_RESOLUTIONS_M)
ZOOM_BREAKS = list(SURFACE_ZOOM_BREAKS)
SURFACE_MIN_ZOOM = 5
SURFACE_MAX_ZOOM = 19
SURFACE_DEFAULT_ZOOM = 6
SURFACE_SHARD_SIZE_M = 20_000
SURFACE_TILE_SIZE_PX = 256
SURFACE_SCORE_RAMP = [
    (0.0, "#440154"),
    (25.0, "#3b528b"),
    (50.0, "#21908c"),
    (75.0, "#5dc863"),
    (100.0, "#fde725"),
]
SURFACE_SHELL_SCHEMA_VERSION = 1
FINE_SURFACE_SCHEMA_VERSION = 1
ENABLE_FINE_RASTER_SURFACE = (
    os.getenv("LIVABILITY_FINE_RASTER_SURFACE", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)

COASTAL_ARTIFACT_WIDTH_M = 75
COASTAL_COMPONENT_PRESERVE_AREA_M2 = 100_000
COASTAL_CLEANUP_ALGORITHM_VERSION = 3


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
PROJECT_TEMP_DIR = BASE_DIR / ".tmp"
PMTILES_SCHEMA_VERSION = 2
GRID_GEOMETRY_SCHEMA_VERSION = 4
CACHE_SCHEMA_VERSION = 9
FORCE_RECOMPUTE = False
USE_COMPRESSED_CACHE = True
MANIFEST_NAME = "manifest.json"


def profile_fine_surface_enabled(profile: str | None = None) -> bool:
    settings = build_profile_settings(profile)
    return bool(settings.fine_surface_enabled and ENABLE_FINE_RASTER_SURFACE)


def pmtiles_filename(profile: str | None = None) -> str:
    normalized_profile = normalize_build_profile(profile)
    if normalized_profile == "full":
        return "livability.pmtiles"
    return f"livability-{normalized_profile}.pmtiles"


def pmtiles_output_path(profile: str | None = None) -> Path:
    return CACHE_DIR / pmtiles_filename(profile)


def pmtiles_url_path(profile: str | None = None) -> str:
    return f"/tiles/{pmtiles_filename(profile)}"


PMTILES_OUTPUT_PATH = pmtiles_output_path(DEFAULT_BUILD_PROFILE)


def resolution_for_zoom(zoom: int | float, *, profile: str | None = None) -> int:
    zoom_value = int(math.floor(float(zoom)))
    ladder = build_profile_settings(profile).surface_zoom_breaks
    for min_zoom, resolution_m in ladder:
        if zoom_value >= int(min_zoom):
            return int(resolution_m)
    return int(ladder[-1][1])


def zoom_bounds_for_resolution(
    resolution_m: int,
    *,
    profile: str | None = None,
) -> tuple[int, int]:
    normalized = int(resolution_m)
    ladder = [
        (int(min_zoom), int(size))
        for min_zoom, size in build_profile_settings(profile).surface_zoom_breaks
    ]
    for index, (min_zoom, size) in enumerate(ladder):
        if size != normalized:
            continue
        if index == 0:
            return (min_zoom, SURFACE_MAX_ZOOM)
        previous_min_zoom = ladder[index - 1][0]
        return (min_zoom, previous_min_zoom - 1)
    raise ValueError(f"Unsupported surface resolution: {resolution_m}")


def is_coarse_vector_resolution(resolution_m: int, *, profile: str | None = None) -> bool:
    return int(resolution_m) in build_profile_settings(profile).coarse_vector_resolutions_m


def is_fine_surface_resolution(resolution_m: int, *, profile: str | None = None) -> bool:
    return int(resolution_m) in build_profile_settings(profile).fine_resolutions_m


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
    build_profile: BuildProfile
    geo_hash: str
    reach_hash: str
    surface_shell_hash: str
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
    build_profile: BuildProfile
    geo_hash: str
    reach_hash: str
    surface_shell_hash: str
    score_hash: str
    render_hash: str
    config_hash: str
    import_fingerprint: str
    build_key: str


def build_config_hashes(profile: str | None = None) -> ConfigHashes:
    normalized_profile = normalize_build_profile(profile)
    profile_settings = build_profile_settings(normalized_profile)
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
        "coastal_artifact_width_m": COASTAL_ARTIFACT_WIDTH_M,
        "coastal_component_preserve_area_m2": COASTAL_COMPONENT_PRESERVE_AREA_M2,
        "coastal_cleanup_algorithm_version": COASTAL_CLEANUP_ALGORITHM_VERSION,
        "schema_version": CACHE_SCHEMA_VERSION,
    }
    geo_hash = hash_dict(geo_params)

    reach_params = {
        "geo_hash": geo_hash,
        "tags": TAGS,
        "walk_radius_m": WALK_RADIUS_M,
    }
    reach_hash = hash_dict(reach_params)

    surface_shell_hash = hash_dict(
        {
            "reach_hash": reach_hash,
            "canonical_base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "surface_shard_size_m": SURFACE_SHARD_SIZE_M,
            "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
            "surface_shell_schema_version": SURFACE_SHELL_SCHEMA_VERSION,
        }
    )

    score_params = {
        "reach_hash": reach_hash,
        "caps": CAPS,
        "coarse_vector_resolutions_m": sorted(COARSE_VECTOR_RESOLUTIONS_M),
        "canonical_base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
        "surface_shard_size_m": SURFACE_SHARD_SIZE_M,
        "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
        "fine_surface_schema_version": FINE_SURFACE_SCHEMA_VERSION,
    }
    score_hash = hash_dict(score_params)

    render_params = {
        "build_profile": normalized_profile,
        "score_hash": score_hash,
        "surface_zoom_breaks": sorted(profile_settings.surface_zoom_breaks),
        "surface_score_ramp": list(SURFACE_SCORE_RAMP),
        "profile_fine_surface_enabled": profile_settings.fine_surface_enabled,
        "runtime_fine_surface_enabled": profile_fine_surface_enabled(normalized_profile),
        "surface_max_zoom": SURFACE_MAX_ZOOM,
        "output_html": OUTPUT_HTML,
        "category_colors": CATEGORY_COLORS,
        "pmtiles_schema_version": PMTILES_SCHEMA_VERSION,
    }
    render_hash = hash_dict(render_params)

    config_hash = hash_dict(
        {
            "build_profile": normalized_profile,
            "score_hash": score_hash,
            "render_hash": render_hash,
            "schema_version": CACHE_SCHEMA_VERSION,
        }
    )

    return ConfigHashes(
        build_profile=normalized_profile,
        geo_hash=geo_hash,
        reach_hash=reach_hash,
        surface_shell_hash=surface_shell_hash,
        score_hash=score_hash,
        render_hash=render_hash,
        config_hash=config_hash,
    )


def extract_fingerprint(path: Path) -> str:
    validated_path = validate_local_osm_extract(path)
    extract_meta = _file_meta(validated_path)
    return hash_dict(
        {
            "extract_size": extract_meta["size"],
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


def build_hashes_for_import(
    import_fingerprint: str,
    profile: str | None = None,
) -> BuildHashes:
    normalized_profile = normalize_build_profile(profile)
    base_hashes = build_config_hashes(normalized_profile)
    geo_hash = hash_dict(
        {
            "base_geo_hash": base_hashes.geo_hash,
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
    surface_shell_hash = hash_dict(
        {
            "reach_hash": reach_hash,
            "canonical_base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "surface_shard_size_m": SURFACE_SHARD_SIZE_M,
            "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
            "surface_shell_schema_version": SURFACE_SHELL_SCHEMA_VERSION,
        }
    )
    score_hash = hash_dict(
        {
            "reach_hash": reach_hash,
            "caps": CAPS,
            "coarse_vector_resolutions_m": sorted(COARSE_VECTOR_RESOLUTIONS_M),
            "canonical_base_resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "surface_shard_size_m": SURFACE_SHARD_SIZE_M,
            "grid_geometry_schema_version": GRID_GEOMETRY_SCHEMA_VERSION,
            "fine_surface_schema_version": FINE_SURFACE_SCHEMA_VERSION,
        }
    )
    build_key = hash_dict(
        {
            "build_profile": normalized_profile,
            "import_fingerprint": import_fingerprint,
            "config_hash": base_hashes.config_hash,
        }
    )
    return BuildHashes(
        build_profile=normalized_profile,
        geo_hash=geo_hash,
        reach_hash=reach_hash,
        surface_shell_hash=surface_shell_hash,
        score_hash=score_hash,
        render_hash=base_hashes.render_hash,
        config_hash=base_hashes.config_hash,
        import_fingerprint=import_fingerprint,
        build_key=build_key,
    )


HASHES = build_config_hashes()


def current_normalization_scope_hash(profile: str | None = None) -> str:
    return build_config_hashes(profile).geo_hash


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
