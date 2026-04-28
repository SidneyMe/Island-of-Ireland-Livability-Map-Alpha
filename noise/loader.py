from __future__ import annotations

import gzip
import hashlib
import gc
import json
import os
import pickle
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, box
from shapely.ops import transform as shapely_transform
from shapely.ops import unary_union


NOISE_DATA_DIR = Path(__file__).resolve().parent.parent / "noise_datasets"
NOISE_DATASET_VERSION = 2
SUPPORTED_METRICS = frozenset({"Lden", "Lnight"})
NO_DATA_GRIDCODE = 1000
NOISE_FILEGDB_CHUNKS = 4
NOISE_FILEGDB_ALLOWED_CHUNKS = frozenset({2, 4})

NOISE_CANDIDATES_CACHE_VERSION = 3
NOISE_CANDIDATES_CACHE_DIR_ENV = "NOISE_CANDIDATES_CACHE_DIR"
DEFAULT_NOISE_CANDIDATES_CACHE_DIR = (
    Path(__file__).resolve().parent.parent / ".livability_cache"
)
ROI_READ_COLUMNS = (
    "Time",
    "DbValue",
    "dB_Value",
    "Db_Low",
    "dB_Low",
    "Db_High",
    "dB_High",
    "ReportPeriod",
    "ReportPeri",
)


@dataclass(frozen=True)
class _RoiSourceSpec:
    round_number: int
    zip_name: str
    source_type: str
    member: str
    file_format: str


ROI_SOURCE_SPECS: tuple[_RoiSourceSpec, ...] = (
    _RoiSourceSpec(4, "NOISE_Round4.zip", "airport", "Noise R4 DataDownload/Noise_R4_Airport.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "industry", "Noise R4 DataDownload/Noise_R4_Industry.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "rail", "Noise R4 DataDownload/Noise_R4_Rail.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "road", "Noise R4 DataDownload/Noise_R4_Road.gdb", "gdb"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "airport", "Airport/NOISE_Rd3_Airport.shp", "shp"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "rail", "Rail/NOISE_Rd3_Rail.shp", "shp"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "road", "Road/NOISE_Rd3_Road.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "airport", "NOISE_Rd2_Airport.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "rail", "NOISE_Rd2_Rail.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "road", "NOISE_Rd2_Road.shp", "shp"),
)

NI_ZIP_BY_ROUND = {
    3: "end_noisedata_round3.zip",
    2: "end_noisedata_round2.zip",
    1: "end_noisedata_round1.zip",
}

# Verified from the source archives:
# - Round 1 uses class codes (1..7) with Noise_Cl labels.
# - Round 2/3 use threshold-style GRIDCODE values.
_NI_ROUND1_CLASS_BANDS: dict[str, dict[int, tuple[float, float, str]]] = {
    "Lden": {
        1: (45.0, 49.0, "45-49"),  # Noise_Cl "< 50"
        2: (50.0, 54.0, "50-54"),
        3: (55.0, 59.0, "55-59"),
        4: (60.0, 64.0, "60-64"),
        5: (65.0, 69.0, "65-69"),
        6: (70.0, 74.0, "70-74"),
        7: (75.0, 99.0, "75+"),    # Noise_Cl ">= 75"
    },
    "Lnight": {
        1: (45.0, 49.0, "45-49"),  # Noise_Cl "< 45" (clipped to canonical floor)
        2: (45.0, 49.0, "45-49"),
        3: (50.0, 54.0, "50-54"),
        4: (55.0, 59.0, "55-59"),
        5: (60.0, 64.0, "60-64"),
        6: (65.0, 69.0, "65-69"),
        7: (70.0, 99.0, "70+"),    # Noise_Cl ">= 70"
    },
}

_NI_THRESHOLD_GRIDCODE_BANDS: dict[int, tuple[float, float, str]] = {
    45: (45.0, 49.0, "45-49"),
    49: (50.0, 54.0, "50-54"),
    50: (50.0, 54.0, "50-54"),
    54: (55.0, 59.0, "55-59"),
    59: (60.0, 64.0, "60-64"),
    64: (65.0, 69.0, "65-69"),
    69: (70.0, 74.0, "70-74"),
    74: (75.0, 99.0, "75+"),
}


NOISE_SIMPLIFY_TOLERANCE = 0.00005  # ~5 m at Irish latitudes (WGS84 degrees)

# ogr2ogr GPKG pipeline — converts sources to GeoPackage using GDAL's native C
# reader (much faster than Python/fiona for FileGDB), simplifies in metres.
NOISE_SOURCE_GPKG_PIPELINE_VERSION = 1
NOISE_SOURCE_GPKG_SIMPLIFY_VERSION = 1
NOISE_SIMPLIFY_TOLERANCE_M = 5.0  # metres; only applies to the GPKG pipeline


def _simplify_noise_geom(geom):
    if geom is None or geom.is_empty:
        return geom
    simplified = geom.simplify(NOISE_SIMPLIFY_TOLERANCE, preserve_topology=True)
    if simplified is None or simplified.is_empty:
        return geom
    return simplified


def _file_meta(path: Path) -> dict[str, int]:
    try:
        stat = path.stat()
        return {"mtime_ns": int(stat.st_mtime_ns), "size": int(stat.st_size)}
    except OSError:
        return {"mtime_ns": 0, "size": 0}


def _file_signature(path: Path) -> dict[str, Any]:
    meta = _file_meta(path)
    return {"path": str(path), "mtime_ns": meta["mtime_ns"], "size": meta["size"]}


def _ogr2ogr_available() -> bool:
    return shutil.which("ogr2ogr") is not None


def _source_gpkg_raw_key(zip_path: Path, member: str, target_crs: str, pipeline_version: int) -> str:
    sig = _file_signature(zip_path)
    parts = {
        "sig": sig,
        "member": member,
        "target_crs": target_crs,
        "pipeline_version": pipeline_version,
    }
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _source_gpkg_simplified_key(raw_key: str, tol_m: float, simplify_version: int) -> str:
    parts = {"raw_key": raw_key, "tol_m": tol_m, "simplify_version": simplify_version}
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]


def _all_source_simplified_key(data_dir: Path) -> str:
    """Stable key over all source files + current GPKG pipeline settings.

    Changes when any ZIP file changes, or when tolerance/version constants change.
    Does NOT include study area — that is the whole point: this key is stable
    across study area changes, so the candidate pickle only misses when source
    data or pipeline settings change, not when the study area geometry changes.
    """
    parts = {
        "dataset_signature": dataset_signature(data_dir),
        "tolerance_m": NOISE_SIMPLIFY_TOLERANCE_M,
        "pipeline_version": NOISE_SOURCE_GPKG_PIPELINE_VERSION,
        "simplify_version": NOISE_SOURCE_GPKG_SIMPLIFY_VERSION,
    }
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]


def _assert_gpkg_valid(path: Path) -> None:
    stat = path.stat()
    if stat.st_size < 4096:
        raise RuntimeError(
            f"GPKG output appears truncated ({stat.st_size} bytes): {path}"
        )


def _ensure_noise_source_gpkg_raw(
    zip_path: Path,
    member: str,
    cache_dir: Path,
    key: str,
) -> Path | None:
    """Convert zip_path/member to a raw (projected, un-simplified) GeoPackage.

    Reprojects to EPSG:2157 (Irish Transverse Mercator, metres) so the
    subsequent simplification step operates in correct metric units.
    Returns the GPKG path, or None if ogr2ogr is unavailable or conversion fails.
    """
    gpkg_path = cache_dir / f"noise_src_raw_{key}.gpkg"
    if gpkg_path.exists():
        print(f"[noise] source raw GPKG cache hit: {gpkg_path.name}", flush=True)
        return gpkg_path
    source = f"/vsizip/{zip_path.resolve().as_posix()}/{member}"
    print(f"[noise] source raw GPKG cache miss: building from {zip_path.name}/{member}...", flush=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp = gpkg_path.with_suffix(".tmp.gpkg")
    tmp.unlink(missing_ok=True)
    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "ogr2ogr", "-f", "GPKG", str(tmp),
                source,
                "-t_srs", "EPSG:2157",
                "-makevalid",
            ],
            capture_output=True,
        )
        if result.returncode != 0:
            print(
                f"[noise] ogr2ogr raw GPKG failed (rc={result.returncode}): "
                f"{result.stderr.decode(errors='replace')[:300]}",
                flush=True,
            )
            tmp.unlink(missing_ok=True)
            return None
        _assert_gpkg_valid(tmp)
        tmp.replace(gpkg_path)
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        print(f"[noise] ogr2ogr raw GPKG error: {exc}", flush=True)
        return None
    print(
        f"[noise] raw GPKG built in {time.perf_counter() - t0:.1f}s"
        f" -> {gpkg_path.name}",
        flush=True,
    )
    return gpkg_path


def _ensure_noise_source_gpkg_simplified(
    raw_gpkg: Path,
    cache_dir: Path,
    key: str,
    tol_m: float,
) -> Path | None:
    """Simplify raw_gpkg at tol_m metres and write to a separate cached GPKG.

    Simplification after the metre-CRS reproject is safe: tolerance is in
    metres, not degrees.  Kept separate from the raw GPKG so scoring code
    can still use the raw version if it needs full precision.
    Returns the simplified GPKG path, or None on failure.
    """
    gpkg_path = cache_dir / f"noise_src_simplified_{key}.gpkg"
    if gpkg_path.exists():
        print(
            f"[noise] simplified GPKG cache hit: {gpkg_path.name}"
            f" tolerance={tol_m}m",
            flush=True,
        )
        return gpkg_path
    print(f"[noise] simplified GPKG cache miss: simplifying at {tol_m}m...", flush=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp = gpkg_path.with_suffix(".tmp.gpkg")
    tmp.unlink(missing_ok=True)
    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "ogr2ogr", "-f", "GPKG", str(tmp),
                str(raw_gpkg),
                "-simplify", str(tol_m),
                "-makevalid",
            ],
            capture_output=True,
        )
        if result.returncode != 0:
            print(
                f"[noise] ogr2ogr simplified GPKG failed (rc={result.returncode}): "
                f"{result.stderr.decode(errors='replace')[:300]}",
                flush=True,
            )
            tmp.unlink(missing_ok=True)
            return None
        _assert_gpkg_valid(tmp)
        tmp.replace(gpkg_path)
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        print(f"[noise] ogr2ogr simplified GPKG error: {exc}", flush=True)
        return None
    print(
        f"[noise] simplified GPKG in {time.perf_counter() - t0:.1f}s"
        f" -> {gpkg_path.name}",
        flush=True,
    )
    return gpkg_path


def _get_or_build_spec_gpkg(
    zip_path: Path,
    member: str,
    cache_dir: Path,
) -> Path | None:
    """Return path to simplified source GPKG for (zip_path, member), building if needed.

    Returns None if ogr2ogr is unavailable or any stage fails — caller falls
    back to the existing fiona/pyogrio path.
    """
    if not _ogr2ogr_available():
        return None
    raw_key = _source_gpkg_raw_key(
        zip_path, member, "EPSG:2157", NOISE_SOURCE_GPKG_PIPELINE_VERSION
    )
    raw_gpkg = _ensure_noise_source_gpkg_raw(zip_path, member, cache_dir, raw_key)
    if raw_gpkg is None:
        return None
    simp_key = _source_gpkg_simplified_key(
        raw_key, NOISE_SIMPLIFY_TOLERANCE_M, NOISE_SOURCE_GPKG_SIMPLIFY_VERSION
    )
    return _ensure_noise_source_gpkg_simplified(
        raw_gpkg, cache_dir, simp_key, NOISE_SIMPLIFY_TOLERANCE_M
    )


def dataset_info(data_dir: Path = NOISE_DATA_DIR) -> dict[str, Any]:
    expected_files = sorted(
        {spec.zip_name for spec in ROI_SOURCE_SPECS}.union(NI_ZIP_BY_ROUND.values())
    )
    files = {}
    for file_name in expected_files:
        path = Path(data_dir) / file_name
        meta = _file_meta(path)
        files[file_name] = {
            "available": path.exists(),
            "file_size": meta["size"],
            "file_mtime_ns": meta["mtime_ns"],
        }
    return {
        "version": NOISE_DATASET_VERSION,
        "available": any(file_info["available"] for file_info in files.values()),
        "data_dir": str(Path(data_dir)),
        "files": files,
    }


def dataset_signature(data_dir: Path = NOISE_DATA_DIR) -> str:
    info = dataset_info(data_dir)
    signature_payload = {
        "version": info["version"],
        "files": info["files"],
    }
    return hashlib.sha256(
        json.dumps(signature_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]


def _emit_progress(progress_cb, detail: str) -> None:
    if progress_cb is not None:
        progress_cb("detail", detail=detail, force_log=True)


def _process_memory_detail() -> str:
    try:
        import psutil
    except Exception:
        return ""
    try:
        rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        return ""
    return f" rss={rss_mb:,.0f}MiB"


def _noise_filegdb_chunk_count() -> int:
    raw_value = os.getenv("NOISE_FILEGDB_CHUNKS")
    if raw_value is None or not raw_value.strip():
        value = NOISE_FILEGDB_CHUNKS
    else:
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise ValueError("NOISE_FILEGDB_CHUNKS must be either 2 or 4.") from exc
    if value not in NOISE_FILEGDB_ALLOWED_CHUNKS:
        raise ValueError("NOISE_FILEGDB_CHUNKS must be either 2 or 4.")
    return value


def _filegdb_chunk_windows(feature_count: int, chunk_count: int) -> list[tuple[int, int]]:
    feature_count = max(int(feature_count), 0)
    chunk_count = max(int(chunk_count), 1)
    if feature_count == 0:
        return []
    chunk_count = min(chunk_count, feature_count)
    base_size, remainder = divmod(feature_count, chunk_count)
    windows: list[tuple[int, int]] = []
    offset = 0
    for chunk_index in range(chunk_count):
        limit = base_size + (1 if chunk_index < remainder else 0)
        windows.append((offset, limit))
        offset += limit
    return windows


def _lookup_value(row: Any, names: Iterable[str]) -> Any:
    lower_names = {name.lower() for name in names}
    for key, value in row.items():
        if str(key).lower() in lower_names:
            return value
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_noise_band(
    value: Any = None,
    *,
    db_low: float | int | str | None = None,
    db_high: float | int | str | None = None,
) -> tuple[float | None, float | None, str]:
    low = _to_float(db_low)
    high = _to_float(db_high)
    text = str(value or "").strip()
    if text.endswith("+"):
        low = _to_float(text[:-1])
        high = 99.0 if low is not None else high
        match = None
    else:
        match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)?\s*$", text)
    if match:
        low = float(match.group(1))
        if match.group(2):
            high = float(match.group(2))

    if low is None and high is None:
        return None, None, text
    if low is None:
        low = high
    if high is None:
        high = low
    if high >= 99:
        return low, high, f"{int(round(low))}+"
    return low, high, f"{int(round(low))}-{int(round(high))}"


def normalize_ni_gridcode_band(
    gridcode: Any,
    *,
    round_number: int | None = None,
    source_type: str | None = None,
    metric: str | None = None,
) -> tuple[float | None, float | None, str | None]:
    code = _to_float(gridcode)
    if code is None:
        return None, None, None
    code_int = int(round(code))
    if code_int == NO_DATA_GRIDCODE:
        return None, None, None

    source_label = source_type or "unknown source"
    metric_name = _metric_from_value(metric) if metric is not None else None
    metric_label = metric_name or (str(metric) if metric is not None else "unknown metric")

    if round_number == 1:
        mapping = _NI_ROUND1_CLASS_BANDS.get(metric_name or "")
        if mapping is None or code_int not in mapping:
            raise ValueError(
                f"NI Round 1 gridcode {code_int} for {source_label} {metric_label} "
                "is a class code; missing mapping"
            )
        return mapping[code_int]

    mapped = _NI_THRESHOLD_GRIDCODE_BANDS.get(code_int)
    if mapped is not None:
        return mapped

    if round_number in {2, 3}:
        raise ValueError(
            f"NI Round {round_number} gridcode {code_int} for {source_label} {metric_label} "
            "has no verified threshold mapping"
        )

    # Backward-compatible fallback for unknown rounds in direct callers/tests.
    low = float(code_int + 1)
    high = 99.0 if low >= 75 else float(code_int + 5)
    _, _, label = normalize_noise_band(db_low=low, db_high=high)
    return low, high, label


def _metric_from_value(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized == "lden":
        return "Lden"
    if normalized in {"lnight", "lngt", "lnght"}:
        return "Lnight"
    return None


def _metric_from_ni_member(member: str) -> str | None:
    name = Path(member).stem.lower()
    if "_lden" in name:
        return "Lden"
    if any(token in name for token in ("_lnight", "_lngt", "_lnght")):
        return "Lnight"
    return None


def _source_type_from_ni_member(member: str) -> str | None:
    parts = [part.lower() for part in Path(member).parts]
    joined = "/".join(parts)
    if "consolidated" in parts:
        return "consolidated"
    if "industry" in parts:
        return "industry"
    if "roads" in parts or "major_roads" in parts or "mroad" in joined:
        return "road"
    if "rail" in parts or "major_rail" in parts or "mrail" in joined:
        return "rail"
    if "major_airports" in parts or "bca" in parts or "bia" in parts:
        return "airport"
    return None


def _extract_member_family(zip_file: zipfile.ZipFile, member: str, target_dir: Path) -> Path:
    member_path = Path(member)
    if member_path.suffix.lower() == ".shp":
        stem = member_path.with_suffix("")
        for entry in zip_file.infolist():
            entry_path = Path(entry.filename)
            if entry.is_dir():
                continue
            if entry_path.with_suffix("") != stem:
                continue
            zip_file.extract(entry, target_dir)
        return target_dir / member_path

    prefix = member.rstrip("/") + "/"
    for entry in zip_file.infolist():
        if entry.filename == member or entry.filename.startswith(prefix):
            zip_file.extract(entry, target_dir)
    return target_dir / member


def _source_bbox_from_wgs84(bounds_wgs84, source_crs) -> tuple[float, float, float, float] | None:
    if bounds_wgs84 is None:
        return None
    source_crs = source_crs or "EPSG:29902"
    if str(source_crs).upper() in {"EPSG:4326", "4326"}:
        return tuple(float(value) for value in bounds_wgs84)
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:4326", source_crs, always_xy=True)
    return shapely_transform(transformer.transform, box(*bounds_wgs84)).bounds


def _read_vector_member(zip_path: Path, member: str, *, bbox_wgs84=None):
    with zipfile.ZipFile(zip_path) as zip_file, tempfile.TemporaryDirectory(
        prefix="noise-vector-"
    ) as tmp_name:
        tmp_dir = Path(tmp_name)
        vector_path = _extract_member_family(zip_file, member, tmp_dir)
        import geopandas as gpd

        read_kwargs = {}
        source_crs = None
        if bbox_wgs84 is not None:
            try:
                import pyogrio

                source_crs = (pyogrio.read_info(vector_path).get("crs") or None)
            except Exception:
                source_crs = None
            source_bbox = _source_bbox_from_wgs84(
                bbox_wgs84,
                source_crs or "EPSG:29902",
            )
            if source_bbox is not None:
                read_kwargs["bbox"] = source_bbox

        gdf = gpd.read_file(vector_path, **read_kwargs)
        if gdf.crs is None:
            gdf = gdf.set_crs(source_crs or 29902)
        return gdf.to_crs(4326)


def _read_gdb_member_chunks(zip_path: Path, member: str, *, bbox_wgs84=None, progress_cb=None):
    try:
        import pyogrio
    except ImportError as exc:  # pragma: no cover - depends on installed deps
        raise RuntimeError(
            "Noise Round 4 road data is packaged as FileGDB. Install pyogrio/GDAL "
            "support so the up-to-date road noise layer can be read."
        ) from exc

    from pyproj import Transformer

    gdb_path = f"/vsizip/{zip_path.resolve().as_posix()}/{member}"
    try:
        layer_rows = pyogrio.list_layers(gdb_path)
        layer_names = [str(row[0]) for row in layer_rows]
        if not layer_names:
            raise RuntimeError(f"No readable layers were found in FileGDB: {zip_path}!{member}")
        for layer_name in layer_names:
            info = pyogrio.read_info(
                gdb_path,
                layer=layer_name,
                force_feature_count=True,
            )
            feature_count = int(info.get("features") or 0)
            fields = {str(field) for field in info.get("fields", ())}
            columns = [column for column in ROI_READ_COLUMNS if column in fields]
            source_crs = info.get("crs") or "EPSG:29902"
            source_bbox = _source_bbox_from_wgs84(bbox_wgs84, source_crs)
            if source_bbox is not None:
                _emit_progress(
                    progress_cb,
                    "FileGDB noise layer "
                    f"{layer_name}: post-filtering to source bbox "
                    f"{tuple(round(float(value), 2) for value in source_bbox)}",
                )
            windows = _filegdb_chunk_windows(
                feature_count,
                _noise_filegdb_chunk_count(),
            )
            _emit_progress(
                progress_cb,
                "FileGDB noise layer "
                f"{layer_name}: {feature_count:,} features, {len(windows):,} chunks",
            )
            for chunk_index, (offset, limit) in enumerate(windows, start=1):
                _emit_progress(
                    progress_cb,
                    "loading FileGDB noise chunk "
                    f"{chunk_index}/{len(windows)} layer={layer_name} "
                    f"offset={offset:,} limit={limit:,}{_process_memory_detail()}",
                )
                frame = pyogrio.read_dataframe(
                    gdb_path,
                    layer=layer_name,
                    columns=columns,
                    skip_features=offset,
                    max_features=limit,
                    fid_as_index=True,
                )
                if frame.empty:
                    continue
                frame["__noise_layer_name"] = layer_name
                transformer = Transformer.from_crs(
                    getattr(frame, "crs", None) or source_crs,
                    "EPSG:4326",
                    always_xy=True,
                )
                yield frame, {
                    "layer_name": layer_name,
                    "chunk_index": chunk_index,
                    "chunk_count": len(windows),
                    "offset": offset,
                    "limit": limit,
                    "transformer": transformer,
                }
                del frame
                gc.collect()
    except Exception as exc:
        raise RuntimeError(
            "Failed to read the ROI Round 4 road FileGDB noise layer. "
            f"Source: {zip_path}!{member}"
        ) from exc


def _iter_roi_gdfs_from_gpkg(gpkg_path: Path, spec: _RoiSourceSpec, *, study_area_wgs84=None, progress_cb=None):
    """Read all layers from a pre-built simplified GPKG and yield (gdf_wgs84, None).

    Geometries are already projected to EPSG:2157 and simplified; this function
    reprojects them to WGS84 as a single vectorised .to_crs() call and applies
    the study area filter using geopandas' vectorised .intersects().
    """
    import geopandas as gpd

    try:
        import fiona
        layers = fiona.listlayers(str(gpkg_path))
    except Exception:
        layers = [""]  # geopandas will read the default (first) layer

    t0 = time.perf_counter()
    for layer_name in layers:
        try:
            read_kwargs: dict[str, Any] = {}
            if layer_name:
                read_kwargs["layer"] = layer_name
            gdf = gpd.read_file(str(gpkg_path), **read_kwargs)
        except Exception as exc:
            _emit_progress(progress_cb, f"[noise] GPKG read failed for layer {layer_name!r}: {exc}")
            continue
        if gdf.empty:
            continue
        gdf = gdf.to_crs(4326)
        if study_area_wgs84 is not None:
            mask = gdf.geometry.intersects(study_area_wgs84)
            gdf = gdf[mask].reset_index(drop=True)
        if layer_name:
            gdf["__noise_layer_name"] = layer_name
        if gdf.empty:
            continue
        yield gdf, None
    _emit_progress(
        progress_cb,
        f"[noise] loaded {spec.zip_name}/{spec.member} from GPKG"
        f" in {time.perf_counter() - t0:.1f}s",
    )


def _iter_roi_gdfs(
    data_dir: Path,
    spec: _RoiSourceSpec,
    *,
    bbox_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
    study_area_wgs84=None,
):
    zip_path = Path(data_dir) / spec.zip_name
    if not zip_path.exists():
        return
    if gpkg_cache_dir is not None:
        gpkg = _get_or_build_spec_gpkg(zip_path, spec.member, gpkg_cache_dir)
        if gpkg is not None:
            yield from _iter_roi_gdfs_from_gpkg(
                gpkg, spec, study_area_wgs84=study_area_wgs84, progress_cb=progress_cb
            )
            return
    if spec.file_format == "gdb":
        yield from _read_gdb_member_chunks(
            zip_path,
            spec.member,
            bbox_wgs84=bbox_wgs84,
            progress_cb=progress_cb,
        )
        return
    yield _read_vector_member(zip_path, spec.member, bbox_wgs84=bbox_wgs84), None


def _transform_geometry_to_wgs84(geom, transformer):
    if transformer is None:
        return geom
    return shapely_transform(transformer.transform, geom)


def _roi_spec_candidate_rows(
    data_dir: Path,
    spec: _RoiSourceSpec,
    *,
    study_area_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
) -> Iterator[dict[str, Any]]:
    bbox_wgs84 = study_area_wgs84.bounds if study_area_wgs84 is not None else None
    zip_path = Path(data_dir) / spec.zip_name
    if not zip_path.exists():
        return
    _emit_progress(progress_cb, f"loading ROI noise {spec.zip_name} {spec.source_type}")
    for gdf, chunk_meta in _iter_roi_gdfs(
        data_dir,
        spec,
        bbox_wgs84=bbox_wgs84,
        progress_cb=progress_cb,
        gpkg_cache_dir=gpkg_cache_dir,
        study_area_wgs84=study_area_wgs84,
    ):
        # When _iter_roi_gdfs returned from the GPKG path, gdf is already in
        # WGS84 and already simplified; mark it so the per-feature steps below
        # are skipped.
        gpkg_preprocessed = chunk_meta is None and gpkg_cache_dir is not None
        if gdf is None or gdf.empty:
            continue
        emitted_rows = 0
        scanned_rows = 0
        transformer = chunk_meta.get("transformer") if chunk_meta is not None else None
        for index, row in gdf.iterrows():
            scanned_rows += 1
            metric = _metric_from_value(_lookup_value(row, ("Time",)))
            if metric not in SUPPORTED_METRICS:
                continue
            db_low, db_high, db_value = normalize_noise_band(
                _lookup_value(row, ("DbValue", "dB_Value")),
                db_low=_lookup_value(row, ("Db_Low", "dB_Low")),
                db_high=_lookup_value(row, ("Db_High", "dB_High")),
            )
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            if not gpkg_preprocessed:
                try:
                    geom = _transform_geometry_to_wgs84(geom, transformer)
                except Exception as exc:
                    raise RuntimeError(
                        "Failed to reproject ROI FileGDB noise feature "
                        f"{spec.zip_name}:{spec.member}:{index}"
                    ) from exc
                if study_area_wgs84 is not None and not geom.intersects(study_area_wgs84):
                    continue
                geom = _simplify_noise_geom(geom)
            emitted_rows += 1
            if chunk_meta is not None and scanned_rows % 25 == 0:
                _emit_progress(
                    progress_cb,
                    "FileGDB noise chunk "
                    f"{chunk_meta['chunk_index']}/{chunk_meta['chunk_count']} "
                    f"processed {scanned_rows:,}/{chunk_meta['limit']:,} features; "
                    f"emitted {emitted_rows:,}{_process_memory_detail()}",
                )
            yield {
                "jurisdiction": "roi",
                "source_type": spec.source_type,
                "metric": metric,
                "round_number": int(spec.round_number),
                "report_period": str(_lookup_value(row, ("ReportPeri", "ReportPeriod")) or f"Round {spec.round_number}"),
                "db_low": db_low,
                "db_high": db_high,
                "db_value": db_value,
                "source_dataset": spec.zip_name,
                "source_layer": str(row.get("__noise_layer_name") or spec.member),
                "source_ref": f"{spec.zip_name}:{spec.member}:{index}",
                "geom": geom,
            }
        if chunk_meta is not None:
            _emit_progress(
                progress_cb,
                "FileGDB noise chunk "
                f"{chunk_meta['chunk_index']}/{chunk_meta['chunk_count']} "
                f"emitted {emitted_rows:,} candidate rows{_process_memory_detail()}",
            )
        del gdf
        gc.collect()


def _roi_candidate_rows(
    data_dir: Path,
    *,
    study_area_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
) -> Iterator[dict[str, Any]]:
    for spec in ROI_SOURCE_SPECS:
        yield from _roi_spec_candidate_rows(
            data_dir,
            spec,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            gpkg_cache_dir=gpkg_cache_dir,
        )


def _preferred_ni_entries(entries: list[str]) -> list[str]:
    grouped: dict[tuple[str, str, str], list[str]] = {}
    for member in entries:
        metric = _metric_from_ni_member(member)
        source_type = _source_type_from_ni_member(member)
        if metric is None or source_type is None:
            continue
        parent = str(Path(member).parent).lower()
        key = (parent, source_type, metric)
        grouped.setdefault(key, []).append(member)

    selected: list[str] = []
    for members in grouped.values():
        normalized = sorted(members)
        lngt_members = [member for member in normalized if "_lngt" in Path(member).stem.lower()]
        if lngt_members:
            normalized = [
                member
                for member in normalized
                if "_lnght" not in Path(member).stem.lower()
            ]
        nom_members = [member for member in normalized if "_nom" in Path(member).stem.lower()]
        if nom_members:
            normalized = [
                member
                for member in normalized
                if "_all" not in Path(member).stem.lower()
            ]
        selected.extend(normalized)
    return sorted(set(selected))


def _ni_round_candidate_rows(
    data_dir: Path,
    round_number: int,
    zip_name: str,
    *,
    study_area_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
) -> Iterator[dict[str, Any]]:
    bbox_wgs84 = study_area_wgs84.bounds if study_area_wgs84 is not None else None
    zip_path = Path(data_dir) / zip_name
    if not zip_path.exists():
        return
    with zipfile.ZipFile(zip_path) as zip_file:
        entries = [
            entry.filename
            for entry in zip_file.infolist()
            if not entry.is_dir() and entry.filename.lower().endswith(".shp")
        ]
    for member in _preferred_ni_entries(entries):
        metric = _metric_from_ni_member(member)
        source_type = _source_type_from_ni_member(member)
        if metric not in SUPPORTED_METRICS or source_type is None:
            continue
        _emit_progress(progress_cb, f"loading NI noise {zip_name} {member}")
        gpkg_preprocessed = False
        if gpkg_cache_dir is not None:
            gpkg = _get_or_build_spec_gpkg(zip_path, member, gpkg_cache_dir)
            if gpkg is not None:
                import geopandas as gpd
                t0 = time.perf_counter()
                gdf = gpd.read_file(str(gpkg))
                gdf = gdf.to_crs(4326)
                if study_area_wgs84 is not None:
                    mask = gdf.geometry.intersects(study_area_wgs84)
                    gdf = gdf[mask].reset_index(drop=True)
                _emit_progress(
                    progress_cb,
                    f"[noise] loaded {zip_name}/{member} from GPKG"
                    f" in {time.perf_counter() - t0:.1f}s",
                )
                gpkg_preprocessed = True
            else:
                gdf = _read_vector_member(zip_path, member, bbox_wgs84=bbox_wgs84)
        else:
            gdf = _read_vector_member(zip_path, member, bbox_wgs84=bbox_wgs84)
        if gdf.empty:
            continue
        for index, row in gdf.iterrows():
            raw_gridcode = row.get("gridcode")
            if raw_gridcode is None:
                raw_gridcode = row.get("GRIDCODE")
            db_low, db_high, db_value = normalize_ni_gridcode_band(
                raw_gridcode,
                round_number=round_number,
                source_type=source_type,
                metric=metric,
            )
            if db_value is None:
                continue
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            if not gpkg_preprocessed:
                if study_area_wgs84 is not None and not geom.intersects(study_area_wgs84):
                    continue
                geom = _simplify_noise_geom(geom)
            yield {
                "jurisdiction": "ni",
                "source_type": source_type,
                "metric": metric,
                "round_number": int(round_number),
                "report_period": f"Round {round_number}",
                "db_low": db_low,
                "db_high": db_high,
                "db_value": db_value,
                "source_dataset": zip_name,
                "source_layer": member,
                "source_ref": f"{zip_name}:{member}:{index}",
                "raw_gridcode": raw_gridcode,
                "geom": geom,
            }


def _ni_candidate_rows(
    data_dir: Path,
    *,
    study_area_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
) -> Iterator[dict[str, Any]]:
    for round_number, zip_name in sorted(NI_ZIP_BY_ROUND.items(), reverse=True):
        yield from _ni_round_candidate_rows(
            data_dir,
            round_number,
            zip_name,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            gpkg_cache_dir=gpkg_cache_dir,
        )


def iter_noise_candidate_rows(
    *,
    data_dir: Path = NOISE_DATA_DIR,
    study_area_wgs84=None,
    progress_cb=None,
    gpkg_cache_dir: Path | None = None,
) -> Iterator[dict[str, Any]]:
    yield from _roi_candidate_rows(
        data_dir,
        study_area_wgs84=study_area_wgs84,
        progress_cb=progress_cb,
        gpkg_cache_dir=gpkg_cache_dir,
    )
    yield from _ni_candidate_rows(
        data_dir,
        study_area_wgs84=study_area_wgs84,
        progress_cb=progress_cb,
        gpkg_cache_dir=gpkg_cache_dir,
    )


def _candidates_cache_dir(override: Path | None = None) -> Path:
    if override is not None:
        return Path(override)
    raw = os.getenv(NOISE_CANDIDATES_CACHE_DIR_ENV, "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_NOISE_CANDIDATES_CACHE_DIR


def _study_area_signature(study_area_wgs84) -> str:
    if study_area_wgs84 is None:
        return "none"
    try:
        payload = study_area_wgs84.wkb
    except Exception:
        bounds = getattr(study_area_wgs84, "bounds", None)
        payload = repr(bounds).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def candidates_cache_key(
    study_area_wgs84,
    data_dir: Path = NOISE_DATA_DIR,
) -> str:
    if _ogr2ogr_available():
        # GPKG pipeline active: source key does NOT include study_area, so the
        # pickle only misses when source files or pipeline settings change —
        # not when the study area geometry changes.  Study area is still in the
        # key so a different study area gets its own pickle.
        parts = {
            "cache_version": NOISE_CANDIDATES_CACHE_VERSION,
            "source_key": _all_source_simplified_key(data_dir),
            "study_area": _study_area_signature(study_area_wgs84),
            "loader_options_version": 1,
        }
    else:
        # Legacy / fallback path (no ogr2ogr): keep the original key scheme so
        # existing pickle files remain valid when ogr2ogr is absent.
        parts = {
            "cache_version": NOISE_CANDIDATES_CACHE_VERSION,
            "dataset_version": NOISE_DATASET_VERSION,
            "dataset_signature": dataset_signature(data_dir),
            "study_area": _study_area_signature(study_area_wgs84),
        }
    blob = json.dumps(parts, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def _candidates_cache_path(cache_dir: Path, key: str) -> Path:
    return Path(cache_dir) / f"noise_candidates_{key}.pkl.gz"


def _serialize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    geom = row.get("geom")
    serialized = {key: value for key, value in row.items() if key != "geom"}
    serialized["geom_wkb"] = geom.wkb if geom is not None else None
    return serialized


def _deserialize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    from shapely import wkb as shapely_wkb

    payload = dict(row)
    wkb_bytes = payload.pop("geom_wkb", None)
    payload["geom"] = shapely_wkb.loads(wkb_bytes) if wkb_bytes else None
    return payload


def _load_cached_candidates(path: Path) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rb") as handle:
            payload = pickle.load(handle)
    except (pickle.UnpicklingError, EOFError, OSError, gzip.BadGzipFile):
        try:
            path.unlink()
        except OSError:
            pass
        return None
    if not isinstance(payload, list):
        return None
    return payload


def _save_cached_candidates(path: Path, serialized_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with gzip.open(tmp, "wb", compresslevel=6) as handle:
            pickle.dump(serialized_rows, handle, protocol=pickle.HIGHEST_PROTOCOL)
        if sys.platform == "win32" and path.exists():
            path.unlink()
        tmp.replace(path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _study_area_wkb_or_none(study_area_wgs84) -> bytes | None:
    if study_area_wgs84 is None:
        return None
    return bytes(study_area_wgs84.wkb)


def _maybe_loads_study_area_wkb(study_area_wkb: bytes | None):
    if study_area_wkb is None:
        return None
    from shapely import wkb as shapely_wkb

    return shapely_wkb.loads(study_area_wkb)


def _spec_to_dict(spec: _RoiSourceSpec) -> dict[str, Any]:
    return {
        "round_number": spec.round_number,
        "zip_name": spec.zip_name,
        "source_type": spec.source_type,
        "member": spec.member,
        "file_format": spec.file_format,
    }


def _serialize_rows_for_roi_spec_worker(args: tuple) -> tuple[str, list[dict[str, Any]]]:
    data_dir_str, spec_payload, study_area_wkb, gpkg_cache_dir_str = args
    spec = _RoiSourceSpec(**spec_payload)
    study_area = _maybe_loads_study_area_wkb(study_area_wkb)
    gpkg_cache_dir = Path(gpkg_cache_dir_str) if gpkg_cache_dir_str else None
    serialized = [
        _serialize_candidate(row)
        for row in _roi_spec_candidate_rows(
            Path(data_dir_str),
            spec,
            study_area_wgs84=study_area,
            progress_cb=None,
            gpkg_cache_dir=gpkg_cache_dir,
        )
    ]
    label = f"roi:{spec.zip_name}:{spec.source_type}"
    return label, serialized


def _serialize_rows_for_ni_round_worker(args: tuple) -> tuple[str, list[dict[str, Any]]]:
    data_dir_str, round_number, zip_name, study_area_wkb, gpkg_cache_dir_str = args
    study_area = _maybe_loads_study_area_wkb(study_area_wkb)
    gpkg_cache_dir = Path(gpkg_cache_dir_str) if gpkg_cache_dir_str else None
    serialized = [
        _serialize_candidate(row)
        for row in _ni_round_candidate_rows(
            Path(data_dir_str),
            int(round_number),
            str(zip_name),
            study_area_wgs84=study_area,
            progress_cb=None,
            gpkg_cache_dir=gpkg_cache_dir,
        )
    ]
    label = f"ni:{zip_name}"
    return label, serialized


def _serial_collect_serialized_candidates(
    *,
    data_dir: Path,
    study_area_wgs84,
    progress_cb,
    gpkg_cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    return [
        _serialize_candidate(row)
        for row in iter_noise_candidate_rows(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            gpkg_cache_dir=gpkg_cache_dir,
        )
    ]


def _parallel_collect_serialized_candidates(
    *,
    data_dir: Path,
    study_area_wgs84,
    progress_cb,
    workers: int,
    gpkg_cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from concurrent.futures.process import BrokenProcessPool

    data_dir_str = str(Path(data_dir))
    study_area_wkb = _study_area_wkb_or_none(study_area_wgs84)
    gpkg_cache_dir_str = str(gpkg_cache_dir) if gpkg_cache_dir is not None else None

    parallel_roi_tasks: list[tuple[str, _RoiSourceSpec, tuple]] = []
    serial_roi_specs: list[_RoiSourceSpec] = []
    for spec in ROI_SOURCE_SPECS:
        zip_path = Path(data_dir) / spec.zip_name
        if not zip_path.exists():
            continue
        # Round 4 ROI road is the only FileGDB source. Each worker that touches
        # it loads multi-GB of GDAL state and is the most likely cause of
        # BrokenProcessPool. Run it serially after the pool has finished so
        # parallel workers have memory headroom.
        #
        # EXCEPTION: when the GPKG cache is active, the GDB has already been
        # converted to a GeoPackage and the worker just reads the fast GPKG —
        # no multi-GB GDAL state is loaded.  In that case promote it to a
        # parallel task so it runs concurrently with the others.
        if spec.file_format == "gdb" and gpkg_cache_dir is None:
            serial_roi_specs.append(spec)
            continue
        label = f"roi:{spec.zip_name}:{spec.source_type}"
        parallel_roi_tasks.append(
            (label, spec, (data_dir_str, _spec_to_dict(spec), study_area_wkb, gpkg_cache_dir_str))
        )

    ni_tasks: list[tuple[str, tuple]] = []
    for round_number, zip_name in sorted(NI_ZIP_BY_ROUND.items(), reverse=True):
        zip_path = Path(data_dir) / zip_name
        if not zip_path.exists():
            continue
        label = f"ni:{zip_name}"
        ni_tasks.append(
            (label, (data_dir_str, round_number, zip_name, study_area_wkb, gpkg_cache_dir_str))
        )

    parallel_total = len(parallel_roi_tasks) + len(ni_tasks)
    serial_total = len(serial_roi_specs)
    if parallel_total + serial_total == 0:
        return []

    aggregated: list[dict[str, Any]] = []

    if parallel_total > 0:
        effective_workers = max(1, min(int(workers), parallel_total))
        _emit_progress(
            progress_cb,
            (
                f"noise loader: dispatching {parallel_total} parallel tasks "
                f"across {effective_workers} workers"
                + (
                    f" (+{serial_total} FileGDB task deferred to serial)"
                    if serial_total
                    else ""
                )
            ),
        )

        completed = 0
        future_labels: dict[Any, str] = {}
        try:
            with ProcessPoolExecutor(max_workers=effective_workers) as pool:
                for label, _spec, args in parallel_roi_tasks:
                    fut = pool.submit(_serialize_rows_for_roi_spec_worker, args)
                    future_labels[fut] = label
                for label, args in ni_tasks:
                    fut = pool.submit(_serialize_rows_for_ni_round_worker, args)
                    future_labels[fut] = label
                for future in as_completed(future_labels):
                    label, rows = future.result()
                    aggregated.extend(rows)
                    completed += 1
                    _emit_progress(
                        progress_cb,
                        f"noise loader: completed {completed}/{parallel_total} "
                        f"({label}: {len(rows):,} rows)",
                    )
        except BrokenProcessPool as exc:
            pending = [
                future_labels[fut]
                for fut in future_labels
                if not fut.done()
            ]
            _emit_progress(
                progress_cb,
                (
                    "noise loader: parallel pool died "
                    f"({type(exc).__name__}: {exc}); pending={pending}; "
                    "falling back to serial load"
                ),
            )
            return _serial_collect_serialized_candidates(
                data_dir=data_dir,
                study_area_wgs84=study_area_wgs84,
                progress_cb=progress_cb,
                gpkg_cache_dir=gpkg_cache_dir,
            )

    if serial_total > 0:
        _emit_progress(
            progress_cb,
            f"noise loader: running {serial_total} FileGDB task(s) serially after pool",
        )
        for spec_index, spec in enumerate(serial_roi_specs, start=1):
            serial_rows = [
                _serialize_candidate(row)
                for row in _roi_spec_candidate_rows(
                    Path(data_dir),
                    spec,
                    study_area_wgs84=study_area_wgs84,
                    progress_cb=progress_cb,
                    gpkg_cache_dir=gpkg_cache_dir,
                )
            ]
            aggregated.extend(serial_rows)
            _emit_progress(
                progress_cb,
                (
                    f"noise loader: serial FileGDB {spec_index}/{serial_total} "
                    f"({spec.zip_name}:{spec.source_type}): {len(serial_rows):,} rows"
                ),
            )

    return aggregated


def _resolve_loader_workers() -> int:
    raw = os.getenv("NOISE_LOADER_WORKERS")
    if raw is None or not raw.strip():
        try:
            from config import NOISE_LOADER_WORKERS as configured

            return int(configured)
        except Exception:
            return min(os.cpu_count() or 4, 6)
    try:
        return max(1, min(16, int(raw)))
    except ValueError:
        return min(os.cpu_count() or 4, 6)


def iter_noise_candidate_rows_cached(
    *,
    data_dir: Path = NOISE_DATA_DIR,
    study_area_wgs84=None,
    progress_cb=None,
    cache_dir: Path | None = None,
    use_cache: bool = True,
    workers: int | None = None,
) -> Iterator[dict[str, Any]]:
    resolved_cache_dir = _candidates_cache_dir(cache_dir)

    # Use the resolved cache dir as the GPKG source cache dir so GPKG files
    # live alongside the pickle files and are found on subsequent runs.
    gpkg_cache_dir: Path | None = resolved_cache_dir if _ogr2ogr_available() else None

    if not use_cache:
        yield from iter_noise_candidate_rows(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            gpkg_cache_dir=gpkg_cache_dir,
        )
        return

    key = candidates_cache_key(study_area_wgs84, data_dir)
    cache_path = _candidates_cache_path(resolved_cache_dir, key)
    cached = _load_cached_candidates(cache_path)
    if cached is not None:
        _emit_progress(
            progress_cb,
            f"noise candidate cache hit ({len(cached):,} rows) at {cache_path.name}",
        )
        try:
            deserialized = [_deserialize_candidate(row) for row in cached]
        except Exception as exc:
            _emit_progress(
                progress_cb,
                f"noise candidate cache corrupt ({type(exc).__name__}: {exc}); deleting and rebuilding",
            )
            try:
                cache_path.unlink()
            except OSError:
                pass
        else:
            yield from deserialized
            return

    _emit_progress(
        progress_cb,
        f"noise candidate cache miss; rebuilding ({cache_path.name})",
    )

    effective_workers = workers if workers is not None else _resolve_loader_workers()
    if effective_workers > 1:
        serialized = _parallel_collect_serialized_candidates(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            workers=effective_workers,
            gpkg_cache_dir=gpkg_cache_dir,
        )
    else:
        serialized = [
            _serialize_candidate(row)
            for row in iter_noise_candidate_rows(
                data_dir=data_dir,
                study_area_wgs84=study_area_wgs84,
                progress_cb=progress_cb,
                gpkg_cache_dir=gpkg_cache_dir,
            )
        ]

    try:
        _save_cached_candidates(cache_path, serialized)
        _emit_progress(
            progress_cb,
            f"saved noise candidate cache ({len(serialized):,} rows) -> {cache_path.name}",
        )
    except OSError as exc:
        _emit_progress(progress_cb, f"failed to save noise candidate cache: {exc}")

    for row in serialized:
        yield _deserialize_candidate(row)


def _make_valid(geom):
    if geom is None or geom.is_empty:
        return None
    if geom.is_valid:
        return geom
    from shapely import make_valid

    repaired = make_valid(geom)
    if repaired is None or repaired.is_empty:
        return None
    return repaired


def _polygon_parts(geom) -> Iterator[Polygon]:
    if geom is None or geom.is_empty:
        return
    if isinstance(geom, Polygon):
        yield geom
        return
    if isinstance(geom, MultiPolygon):
        yield from geom.geoms
        return
    if isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            yield from _polygon_parts(part)


def _clip_to_study_area(geom, study_area_wgs84):
    repaired = _make_valid(geom)
    if repaired is None:
        return None
    clipped = repaired.intersection(study_area_wgs84)
    if clipped.is_empty:
        return None
    return _make_valid(clipped)


def materialize_effective_noise_rows(
    candidate_rows: Iterable[dict[str, Any]],
    study_area_wgs84,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in candidate_rows:
        key = (
            str(row.get("jurisdiction") or ""),
            str(row.get("source_type") or ""),
            str(row.get("metric") or ""),
        )
        if not all(key):
            continue
        grouped.setdefault(key, []).append(row)

    output: list[dict[str, Any]] = []
    for rows in grouped.values():
        covered = None
        round_numbers = sorted({int(row["round_number"]) for row in rows}, reverse=True)
        for round_number in round_numbers:
            round_effective_geoms = []
            for row in [item for item in rows if int(item["round_number"]) == round_number]:
                geom = _clip_to_study_area(row.get("geom"), study_area_wgs84)
                if geom is None:
                    continue
                if covered is not None and not covered.is_empty:
                    if covered.covers(geom):
                        continue
                    if covered.intersects(geom):
                        geom = _make_valid(geom.difference(covered))
                        if geom is None or geom.is_empty:
                            continue
                for part in _polygon_parts(geom):
                    if part.is_empty or part.area <= 0:
                        continue
                    payload = dict(row)
                    payload["geom"] = part
                    output.append(payload)
                    round_effective_geoms.append(part)
            if round_effective_geoms:
                round_coverage = unary_union(round_effective_geoms)
                covered = round_coverage if covered is None else unary_union([covered, round_coverage])
    return output


def load_noise_rows(study_area_wgs84, *, data_dir: Path = NOISE_DATA_DIR, progress_cb=None) -> list[dict[str, Any]]:
    candidates = list(
        iter_noise_candidate_rows(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
        )
    )
    if not candidates:
        return []
    _emit_progress(progress_cb, f"materializing newest-round noise fallback for {len(candidates):,} polygons")
    return materialize_effective_noise_rows(candidates, study_area_wgs84)
