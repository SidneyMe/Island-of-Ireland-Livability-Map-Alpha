from __future__ import annotations

import gc
from pathlib import Path
from typing import Any, Iterator

from .extract import (
    _emit_progress,
    _get_or_build_spec_gpkg,
    _iter_roi_gdfs,
    _process_memory_detail,
    _simplify_noise_geom,
    _transform_geometry_to_wgs84,
)
from .normalize import (
    SUPPORTED_METRICS,
    _metric_from_value,
    normalize_noise_band,
)
from .signature import NOISE_DATA_DIR, ROI_SOURCE_SPECS, _RoiSourceSpec


def _lookup_value(row: Any, names) -> Any:
    lower_names = {name.lower() for name in names}
    for key, value in row.items():
        if str(key).lower() in lower_names:
            return value
    return None


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
