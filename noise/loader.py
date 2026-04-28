"""Noise loader — public API and implementation core.

Most code lives in the sub-modules imported below.  This file keeps only the
functions whose internal call-sites must remain in one module namespace so that
the test suite can patch them via ``mock.patch.object(noise_loader, …)``:

- ``_ni_round_candidate_rows`` (tests patch _preferred_ni_entries, _read_vector_member)
- ``_ni_candidate_rows`` (calls _ni_round_candidate_rows)
- ``iter_noise_candidate_rows`` (patched directly; calls both _roi/_ni candidate rows)
- ``candidates_cache_key`` (tests patch dataset_signature)
- ``iter_noise_candidate_rows_cached`` (tests patch iter_noise_candidate_rows,
  _deserialize_candidate, _ogr2ogr_available)
- worker / parallel / serial collect helpers (call _ni_round_candidate_rows)
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile  # kept as module attribute; tests patch tempfile.TemporaryDirectory via noise_loader.tempfile
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any, Iterator

# ── sub-module imports ─────────────────────────────────────────────────────────
# All names imported here become attributes on noise.loader so that test mocks
# via mock.patch.object(noise_loader, 'X') affect calls made from within this
# module's global namespace.

from .cache import (
    NOISE_CANDIDATE_CACHE_SCHEMA_VERSION,
    NOISE_CANDIDATES_CACHE_DIR_ENV,
    NOISE_CANDIDATES_CACHE_PART_ROWS,
    NOISE_CANDIDATES_CACHE_SUBDIR,
    NOISE_CANDIDATES_CACHE_VERSION,
    DEFAULT_NOISE_CANDIDATES_CACHE_DIR,
    _CandidateCacheWriter,
    _candidates_cache_dir,
    _candidates_cache_entry_dir,
    _candidates_cache_manifest_path,
    _candidates_cache_path,
    _delete_chunked_cache,
    _deserialize_candidate,
    _iter_chunked_cached_candidates,
    _load_cached_candidates,
    _load_chunked_cache_manifest,
    _maybe_loads_study_area_wkb,
    _save_cached_candidates,
    _serialize_candidate,
    _study_area_signature,
    _study_area_wkb_or_none,
)
from .extract import (
    NOISE_FILEGDB_ALLOWED_CHUNKS,
    NOISE_FILEGDB_CHUNKS,
    NOISE_SIMPLIFY_TOLERANCE,
    NOISE_SIMPLIFY_TOLERANCE_M,
    NOISE_SOURCE_GPKG_PIPELINE_VERSION,
    NOISE_SOURCE_GPKG_SIMPLIFY_VERSION,
    ROI_READ_COLUMNS,
    _all_source_simplified_key,
    _emit_progress,
    _filegdb_chunk_windows,
    _get_or_build_spec_gpkg,
    _iter_roi_gdfs,
    _iter_roi_gdfs_from_gpkg,
    _noise_filegdb_chunk_count,
    _ogr2ogr_available,
    _process_memory_detail,
    _read_gdb_member_chunks,
    _read_vector_member,
    _simplify_noise_geom,
    _source_bbox_from_wgs84,
    _transform_geometry_to_wgs84,
)
from .materialize import (
    _clip_to_study_area,
    _make_valid,
    _polygon_parts,
    load_noise_rows,
    materialize_effective_noise_rows,
)
from .normalize import (
    NO_DATA_GRIDCODE,
    SUPPORTED_METRICS,
    _NI_ROUND1_CLASS_BANDS,
    _NI_THRESHOLD_GRIDCODE_BANDS,
    _find_noise_class_column,
    _metric_from_ni_member,
    _metric_from_value,
    _source_type_from_ni_member,
    _to_float,
    normalize_ni_gridcode_band,
    normalize_noise_band,
)
from .signature import (
    NOISE_DATA_DIR,
    NOISE_DATASET_VERSION,
    NI_ZIP_BY_ROUND,
    ROI_SOURCE_SPECS,
    _RoiSourceSpec,
    _file_meta,
    _file_signature,
    dataset_info,
    dataset_signature,
)
from .source_discovery import _preferred_ni_entries, ni_round1_class_snapshot
from .candidates import _lookup_value, _roi_candidate_rows, _roi_spec_candidate_rows


# ── NI candidate rows (must live here; tests patch _preferred_ni_entries and
#    _read_vector_member on this module) ────────────────────────────────────────

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
            try:
                db_low, db_high, db_value = normalize_ni_gridcode_band(
                    raw_gridcode,
                    round_number=round_number,
                    source_type=source_type,
                    metric=metric,
                )
            except ValueError as exc:
                source_ref = f"{zip_name}:{member}:{index}"
                raise ValueError(
                    f"{exc} (source_dataset={zip_name}, source_layer={member}, "
                    f"source_ref={source_ref}, raw_gridcode={raw_gridcode})"
                ) from exc
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


# ── Public candidate row iterator (patched directly by tests) ─────────────────

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


# ── Cache key (must live here; tests patch dataset_signature on this module) ───

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


# ── Parallel / serial worker helpers ──────────────────────────────────────────

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


# ── Cached public iterator (must live here; tests patch multiple names on this
#    module that this function calls) ───────────────────────────────────────────

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
    entry_dir = _candidates_cache_entry_dir(resolved_cache_dir, key)
    legacy_cache_path = _candidates_cache_path(resolved_cache_dir, key)

    manifest = _load_chunked_cache_manifest(entry_dir)
    if manifest is not None:
        row_count = int(manifest.get("row_count") or 0)
        _emit_progress(
            progress_cb,
            f"noise candidate cache hit ({row_count:,} rows) at {entry_dir}",
        )
        try:
            for row in _iter_chunked_cached_candidates(entry_dir, manifest):
                yield _deserialize_candidate(row)
        except Exception as exc:
            _emit_progress(
                progress_cb,
                f"noise candidate cache corrupt ({type(exc).__name__}: {exc}); deleting and rebuilding",
            )
            _delete_chunked_cache(entry_dir)
        else:
            return

    # Legacy single-file fallback (opt-in compatibility path only).
    cached = _load_cached_candidates(legacy_cache_path)
    if cached is not None:
        allow_legacy = (os.getenv("NOISE_ALLOW_LEGACY_CANDIDATE_CACHE") or "").strip() == "1"
        if not allow_legacy:
            _emit_progress(
                progress_cb,
                (
                    "legacy noise candidate cache hit; deleting and rebuilding chunked cache "
                    "(set NOISE_ALLOW_LEGACY_CANDIDATE_CACHE=1 to temporarily allow legacy reads)"
                ),
            )
            try:
                legacy_cache_path.unlink()
            except OSError:
                pass
        else:
            _emit_progress(
                progress_cb,
                f"noise candidate legacy cache hit ({len(cached):,} rows) at {legacy_cache_path.name}",
            )
            try:
                for row in cached:
                    yield _deserialize_candidate(row)
            except Exception as exc:
                _emit_progress(
                    progress_cb,
                    f"noise candidate legacy cache corrupt ({type(exc).__name__}: {exc}); deleting and rebuilding",
                )
                try:
                    legacy_cache_path.unlink()
                except OSError:
                    pass
            else:
                return

    _emit_progress(
        progress_cb,
        f"noise candidate cache miss; rebuilding ({entry_dir})",
    )

    effective_workers = workers if workers is not None else _resolve_loader_workers()
    if effective_workers > 1:
        _emit_progress(
            progress_cb,
            (
                "noise candidate cache rebuild: using serial streaming path to avoid "
                f"full-list buffering (requested workers={effective_workers})"
            ),
        )

    source_signature = dataset_signature(data_dir)
    study_sig = _study_area_signature(study_area_wgs84)
    writer = _CandidateCacheWriter(
        entry_dir=entry_dir,
        cache_key=key,
        source_signature=source_signature,
        study_area_signature=study_sig,
    )

    emitted = 0
    try:
        source_iter = iter_noise_candidate_rows(
            data_dir=data_dir,
            study_area_wgs84=study_area_wgs84,
            progress_cb=progress_cb,
            gpkg_cache_dir=gpkg_cache_dir,
        )
        pending_part: list[dict[str, Any]] = []
        for source_row in source_iter:
            serialized_row = _serialize_candidate(source_row)
            pending_part.append(serialized_row)
            emitted += 1
            yield _deserialize_candidate(serialized_row)
            if len(pending_part) >= NOISE_CANDIDATES_CACHE_PART_ROWS:
                writer.write_batch(pending_part)
                pending_part = []
            if emitted % 10_000 == 0:
                _emit_progress(
                    progress_cb,
                    f"noise candidate cache rebuild streamed {emitted:,} rows",
                )
        if pending_part:
            writer.write_batch(pending_part)
    except Exception:
        _delete_chunked_cache(writer.tmp_dir)
        raise

    try:
        writer.finalize()
        _emit_progress(
            progress_cb,
            f"saved noise candidate cache ({emitted:,} rows) -> {entry_dir}",
        )
        if legacy_cache_path.exists():
            try:
                legacy_cache_path.unlink()
            except OSError:
                pass
    except OSError as exc:
        _emit_progress(progress_cb, f"failed to save noise candidate cache: {exc}")


# ── CLI ────────────────────────────────────────────────────────────────────────

from .cli import _main, _print_ni_round1_class_snapshot  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(_main())
