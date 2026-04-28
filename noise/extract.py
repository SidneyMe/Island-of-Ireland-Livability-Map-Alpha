from __future__ import annotations

import gc
import hashlib
import json
import os
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Iterable, Iterator

from shapely.geometry import box
from shapely.ops import transform as shapely_transform

from .signature import (
    NOISE_DATA_DIR,
    ROI_SOURCE_SPECS,
    _RoiSourceSpec,
    _file_signature,
    dataset_signature,
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

NOISE_FILEGDB_CHUNKS = 4
NOISE_FILEGDB_ALLOWED_CHUNKS = frozenset({2, 4})

NOISE_SIMPLIFY_TOLERANCE = 0.00005  # ~5 m at Irish latitudes (WGS84 degrees)

# ogr2ogr GPKG pipeline — converts sources to GeoPackage using GDAL's native C
# reader (much faster than Python/fiona for FileGDB), simplifies in metres.
NOISE_SOURCE_GPKG_PIPELINE_VERSION = 1
NOISE_SOURCE_GPKG_SIMPLIFY_VERSION = 1
NOISE_SIMPLIFY_TOLERANCE_M = 5.0  # metres; only applies to the GPKG pipeline


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


def _simplify_noise_geom(geom):
    if geom is None or geom.is_empty:
        return geom
    simplified = geom.simplify(NOISE_SIMPLIFY_TOLERANCE, preserve_topology=True)
    if simplified is None or simplified.is_empty:
        return geom
    return simplified


def _ogr2ogr_available() -> bool:
    import shutil
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
