from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path


def _bootstrap_workflow_context(
    *,
    build_engine,
    ensure_database_ready,
    resolve_source_state,
    activate_build_hashes,
    get_hashes,
    set_source_state,
):
    engine = build_engine()
    ensure_database_ready(engine)
    source_state = resolve_source_state()
    set_source_state(source_state)
    activate_build_hashes(source_state.import_fingerprint)
    hashes = get_hashes()
    return engine, source_state, hashes


def _print_build_context(source_state, hashes) -> None:
    print(f"Local extract: {source_state.extract_path}")
    print(f"Import fingerprint: {source_state.import_fingerprint}")
    print(f"Build profile: {getattr(hashes, 'build_profile', 'full')}")
    print(f"Config hash: {hashes.config_hash}")
    transit_fingerprint = getattr(hashes, "transit_reality_fingerprint", "transit-unavailable")
    print(f"Transit reality fingerprint: {transit_fingerprint}")
    print(f"Build key: {hashes.build_key}")
    print()


def _import_not_ready_error(source_state) -> RuntimeError:
    return RuntimeError(
        "Raw OSM import is not ready for "
        f"import_fingerprint={source_state.import_fingerprint}. "
        "Run the import-refresh workflow first, or rerun precompute with "
        "auto_refresh_import=True."
    )


def _print_geometry_bounds(study_area_metric) -> None:
    minx, miny, maxx, maxy = study_area_metric.bounds
    print(
        f"Study area bounds (ITM metres): x {minx:.0f} -> {maxx:.0f}, "
        f"y {miny:.0f} -> {maxy:.0f}\n"
    )


def _pmtiles_bake_configured(bake_pmtiles, pmtiles_output_path) -> bool:
    return bake_pmtiles is not None and pmtiles_output_path is not None


def _resolve_noise_processing_hash(noise_processing_hash, engine):
    """Call noise_processing_hash with engine if supported, else without."""
    if not callable(noise_processing_hash):
        return noise_processing_hash
    try:
        return noise_processing_hash(engine)
    except TypeError:
        return noise_processing_hash()


def _run_pmtiles_bake(
    *,
    bake_pmtiles,
    engine,
    build_key: str,
    pmtiles_output_path: Path,
    noise_max_zoom: int | None = None,
) -> float:
    bake_started_at = time.perf_counter()
    bake_kwargs: dict = {}
    if noise_max_zoom is not None:
        bake_kwargs["noise_max_zoom"] = int(noise_max_zoom)
    bake_pmtiles(
        engine,
        build_key,
        pmtiles_output_path,
        **bake_kwargs,
    )
    return time.perf_counter() - bake_started_at


def run_import_refresh_impl(
    force_refresh: bool = True,
    *,
    cache_dir: Path,
    current_normalization_scope_hash,
    build_engine,
    ensure_database_ready,
    resolve_source_state,
    activate_build_hashes,
    phase_geometry,
    import_payload_ready,
    ensure_local_osm_import,
    tracker_factory,
    get_hashes,
    set_source_state,
) -> str:
    total_started_at = time.perf_counter()
    print("=== Livability Raw OSM Import Refresh ===\n")

    engine, source_state, hashes = _bootstrap_workflow_context(
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=activate_build_hashes,
        get_hashes=get_hashes,
        set_source_state=set_source_state,
    )
    _print_build_context(source_state, hashes)

    normalization_scope_hash = current_normalization_scope_hash()
    import_was_ready = import_payload_ready(
        engine,
        source_state.import_fingerprint,
        normalization_scope_hash,
    )
    if import_was_ready and not force_refresh:
        print("Raw OSM import already ready. Skipping refresh.")
        return hashes.build_key

    tracker = tracker_factory(cache_dir / "precompute_timing_stats.json")
    for phase_name in ("amenities", "networks", "reachability", "grids", "publish"):
        tracker.set_phase_expected(phase_name, False)

    study_area_metric, study_area_wgs84 = phase_geometry(tracker)
    _print_geometry_bounds(study_area_metric)

    tracker.start_phase("import", detail="refreshing raw OSM import")
    ensure_local_osm_import(
        engine,
        source_state,
        study_area_wgs84=study_area_wgs84,
        normalization_scope_hash=normalization_scope_hash,
        force_refresh=force_refresh,
        progress_cb=tracker.phase_callback("import"),
    )
    tracker.finish_phase(
        "import",
        "cached" if import_was_ready and not force_refresh else "completed",
        detail="raw OSM import ready",
    )
    tracker.save_successful_timings()

    hashes = get_hashes()
    print(f"Import fingerprint: {hashes.import_fingerprint}")
    print(f"Build key: {hashes.build_key}")
    print(f"Total wall time: {time.perf_counter() - total_started_at:.1f}s")
    return hashes.build_key


def run_precompute_impl(
    force_precompute: bool = False,
    auto_refresh_import: bool = False,
    force_noise_artifact: bool = False,
    reimport_noise_source: bool = False,
    force_noise_all: bool = False,
    noise_accurate: bool = False,
    require_active_noise_artifact: bool = False,
    refresh_noise_artifact: bool = False,
    *,
    build_profile: str = "full",
    cache_dir: Path,
    current_normalization_scope_hash,
    build_engine,
    ensure_database_ready,
    resolve_source_state,
    activate_build_hashes,
    print_cache_status,
    validate_all_tiers,
    phase_geometry,
    phase_amenities,
    phase_grids,
    score_grid_fast_path_candidate,
    has_complete_build,
    import_payload_ready,
    ensure_local_osm_import,
    tracker_factory,
    walk_rows,
    amenity_rows,
    transport_reality_rows,
    service_desert_rows,
    noise_rows,
    dispatch_noise_loader=None,
    compute_service_deserts,
    noise_processing_hash=None,
    publish_precomputed_artifacts,
    summary_json,
    package_snapshot,
    python_version,
    get_hashes,
    set_source_state,
    ensure_transit_reality=None,
    transit_preflight=None,
    fine_surface_ready=None,
    bake_pmtiles=None,
    pmtiles_output_path: Path | None = None,
    noise_max_zoom: int | None = None,
) -> str:
    total_started_at = time.perf_counter()
    print(f"=== Livability Score Map Precompute ({build_profile}) ===\n")
    import config as _config
    print(f"[config] NOISE_MODE={_config.NOISE_MODE}", flush=True)
    from noise_artifacts.modes import noise_accuracy_mode_label as _mode_label
    from noise_artifacts.modes import resolve_noise_accuracy_mode as _resolve_accuracy_mode

    noise_accuracy_mode = _resolve_accuracy_mode(cli_noise_accurate=bool(noise_accurate))
    print(f"[noise] noise mode: {_mode_label(noise_accuracy_mode)}", flush=True)

    engine, source_state, hashes = _bootstrap_workflow_context(
        build_engine=build_engine,
        ensure_database_ready=ensure_database_ready,
        resolve_source_state=resolve_source_state,
        activate_build_hashes=activate_build_hashes,
        get_hashes=get_hashes,
        set_source_state=set_source_state,
    )
    _print_build_context(source_state, hashes)

    normalization_scope_hash = current_normalization_scope_hash()
    import_was_ready = import_payload_ready(
        engine,
        source_state.import_fingerprint,
        normalization_scope_hash,
    )
    if not import_was_ready and not auto_refresh_import:
        raise _import_not_ready_error(source_state)

    # Noise artifact preflight — run early so a missing/broken artifact fails
    # before the expensive geometry and transit phases.
    # build_default_noise_artifact loads its own full-island domain; it does not
    # need the study area computed by phase_geometry below.
    if _config.NOISE_MODE == "artifact":
        if not hasattr(engine, "connect"):
            if require_active_noise_artifact:
                raise RuntimeError(
                    "BUG: --require-active-noise-artifact requires an engine with DB connectivity."
                )
            # Unit tests often pass sentinel/mock engines that intentionally do
            # not implement DB connectivity; skip artifact DB preflight there.
            print(
                "[noise] skipping artifact preflight: engine does not expose connect()",
                flush=True,
            )
            _active_artifact = None
        else:
            from noise_artifacts.manifest import (
                get_active_artifact as _get_active_artifact,
                get_resolved_artifact_for_mode as _get_resolved_artifact_for_mode,
            )
            if require_active_noise_artifact:
                _active_artifact = _get_resolved_artifact_for_mode(engine, noise_accuracy_mode)
                if _active_artifact is None:
                    if noise_accuracy_mode == "accurate":
                        raise RuntimeError(
                            "Noise artifact missing for mode=accurate. "
                            "DevReuse never builds noise artifacts. "
                            "Run scripts\\win\\prepare_noise_artifact_accurate.cmd first."
                        )
                    raise RuntimeError(
                        "Noise artifact missing for mode=dev_fast. "
                        "DevReuse never builds noise artifacts. "
                        "Run scripts\\win\\prepare_noise_artifact_dev.cmd first."
                    )
                print(
                    "[noise] active resolved artifact required and found: "
                    f"{_active_artifact.artifact_hash} (mode={noise_accuracy_mode})",
                    flush=True,
                )
            else:
                _active_artifact = _get_active_artifact(engine, "resolved")

                _force_resolved = bool(force_noise_artifact or force_noise_all)
                _reimport_source = bool(reimport_noise_source or force_noise_all)
                if _reimport_source:
                    _force_resolved = True

                _need_build = (
                    _active_artifact is None
                    or refresh_noise_artifact
                    or _force_resolved
                    or _reimport_source
                )
                if _need_build:
                    if _active_artifact is None:
                        _build_reason = "no active resolved artifact"
                    elif force_noise_all:
                        _build_reason = "--force-noise-all"
                    elif reimport_noise_source:
                        _build_reason = "--reimport-noise-source"
                    elif force_noise_artifact:
                        _build_reason = "--force-noise-artifact"
                    else:
                        _build_reason = "--refresh-noise-artifact"
                    print(f"[noise] {_build_reason}; building noise artifact...", flush=True)
                    from noise_artifacts.runner import build_default_noise_artifact as _build_artifact

                    def _noise_progress_cb(action, *, detail="", force_log=False):
                        if detail:
                            print(f"[noise] {detail}", flush=True)

                    _build_result = _build_artifact(
                        engine,
                        force_resolved=_force_resolved,
                        reimport_source=_reimport_source,
                        noise_accuracy_mode=noise_accuracy_mode,
                        progress_cb=_noise_progress_cb,
                    )
                    if _build_result["status"] == "built":
                        print(
                            f"[noise] artifact built: {_build_result['artifact_hash']} "
                            f"rows={_build_result.get('row_count', 0)}",
                            flush=True,
                        )
                    elif _build_result["status"] == "up_to_date":
                        print(
                            f"[noise] artifact already up to date: {_build_result['artifact_hash']}",
                            flush=True,
                        )
                    _active_artifact = _get_active_artifact(engine, "resolved")

                if _active_artifact is None:
                    raise RuntimeError(
                        "BUG: noise artifact build completed but no active resolved artifact exists. "
                        "Check noise_artifact_manifest for errors."
                    )
                print(f"[noise] active resolved artifact: {_active_artifact.artifact_hash}", flush=True)
    elif _config.NOISE_MODE == "legacy":
        print(
            "[noise] WARNING: NOISE_MODE=legacy is the slow debug path. "
            "It reads raw noise ZIP/FileGDB files and runs full PostGIS materialization. "
            "The default (artifact mode) is much faster and does not read raw files.",
            flush=True,
        )

    if transit_preflight is not None:
        transit_preflight(engine)

    tracker = tracker_factory(cache_dir / "precompute_timing_stats.json")
    if import_was_ready:
        tracker.start_phase("import", detail="checking raw OSM import manifest")
        tracker.finish_phase("import", "cached", detail="raw OSM import ready")

    study_area_metric, study_area_wgs84 = phase_geometry(tracker)
    _print_geometry_bounds(study_area_metric)

    if dispatch_noise_loader is not None:
        try:
            dispatch_noise_loader()
        except Exception as exc:  # noqa: BLE001 - background dispatch must not block main pipeline
            print(f"[noise] background dispatch failed: {exc}; falling back to inline load")

    if not import_was_ready:
        tracker.start_phase("import", detail="refreshing raw OSM import")
        ensure_local_osm_import(
            engine,
            source_state,
            study_area_wgs84=study_area_wgs84,
            normalization_scope_hash=normalization_scope_hash,
            force_refresh=False,
            progress_cb=tracker.phase_callback("import"),
        )
        tracker.finish_phase("import", "completed", detail="raw OSM import ready")

    if ensure_transit_reality is not None:
        ensure_transit_reality(
            engine,
            import_fingerprint=source_state.import_fingerprint,
            study_area_wgs84=study_area_wgs84,
            progress_cb=tracker.phase_callback("transit"),
        )
        hashes = get_hashes()
        _print_build_context(source_state, hashes)

    print("Cache:")
    print_cache_status()

    print()
    print("Tier validation:")
    validate_all_tiers()
    print()

    if has_complete_build(engine, hashes.build_key) and not force_precompute:
        # Before skipping, verify the stored noise_processing_hash matches the current one.
        # If the active artifact changed since the last build, we must rebuild.
        # Use engine-aware helper so artifact hash can be read from DB before _noise_rows() runs.
        _current_noise_hash = _resolve_noise_processing_hash(noise_processing_hash, engine)
        _noise_hash_matches = True
        if _current_noise_hash is not None:
            try:
                from sqlalchemy import text as _text
                with engine.connect() as _conn:
                    _stored_noise_hash = _conn.execute(
                        _text(
                            "SELECT noise_processing_hash FROM build_manifest "
                            "WHERE build_key = :bk AND status = 'complete'"
                        ),
                        {"bk": hashes.build_key},
                    ).scalar_one_or_none()
                _noise_hash_matches = (_stored_noise_hash == _current_noise_hash)
            except Exception:
                _noise_hash_matches = True  # on query error, allow skip

        if not _noise_hash_matches:
            print(
                f"Complete build exists for build_key={hashes.build_key} "
                "but active noise artifact changed; rebuilding."
            )
        else:
            bake_configured = _pmtiles_bake_configured(bake_pmtiles, pmtiles_output_path)
            pmtiles_missing = bake_configured and not pmtiles_output_path.exists()
            surface_missing = callable(fine_surface_ready) and not bool(fine_surface_ready())
            if not pmtiles_missing and not surface_missing:
                print(
                    f"Complete PostGIS precompute already exists for build_key={hashes.build_key}. "
                    "Skipping. Use --force-precompute to rebuild."
                )
                return hashes.build_key
            if not surface_missing:
                print(
                    f"Complete PostGIS precompute exists for build_key={hashes.build_key}, "
                    f"but PMTiles archive is missing at {pmtiles_output_path}. "
                    "Re-baking PMTiles only."
                )
                bake_seconds = _run_pmtiles_bake(
                    bake_pmtiles=bake_pmtiles,
                    engine=engine,
                    build_key=hashes.build_key,
                    pmtiles_output_path=pmtiles_output_path,
                    noise_max_zoom=noise_max_zoom,
                )
                print(
                    f"PMTiles bake completed in {bake_seconds:.1f}s -> {pmtiles_output_path}"
                )
                return hashes.build_key
            print(
                f"Complete coarse PostGIS build exists for build_key={hashes.build_key}, "
                "but the fine surface cache is missing. Rebuilding fine raster artifacts."
            )

    if score_grid_fast_path_candidate():
        tracker.set_phase_expected("networks", False)
        tracker.set_phase_expected("reachability", False)

    amenity_data, amenity_source_rows = phase_amenities(engine, study_area_wgs84, tracker)
    print()

    walk_grids = phase_grids(
        engine,
        study_area_metric,
        amenity_data,
        amenity_source_rows,
        tracker,
    )
    print()

    if compute_service_deserts is not None:
        compute_service_deserts(engine, walk_grids)

    publish_started_at = datetime.now(timezone.utc)
    publish_progress_cb = tracker.phase_callback("publish")

    tracker.start_phase("publish", detail="preparing rows (no DB writes yet)")
    walk_row_payload = walk_rows(
        walk_grids,
        publish_started_at,
        progress_cb=publish_progress_cb,
    )
    amenity_row_payload = amenity_rows(
        amenity_source_rows,
        publish_started_at,
        progress_cb=publish_progress_cb,
    )
    transport_reality_row_payload = transport_reality_rows(
        engine,
        publish_started_at,
        progress_cb=publish_progress_cb,
    )
    service_desert_row_payload = service_desert_rows(
        engine,
        publish_started_at,
        progress_cb=publish_progress_cb,
    )
    noise_row_payload = noise_rows(
        engine,
        publish_started_at,
        progress_cb=publish_progress_cb,
    )
    summary_started_at = time.perf_counter()
    summary_payload = summary_json(
        study_area_wgs84,
        walk_grids,
        amenity_data,
        amenity_source_rows,
        transport_reality_rows=transport_reality_row_payload,
        noise_rows=None,
    )
    tracker.record_substep(
        "publish",
        "summary_prep",
        time.perf_counter() - summary_started_at,
        force_log=True,
    )
    publish_total_rows = (
        len(walk_row_payload)
        + len(amenity_row_payload)
        + len(transport_reality_row_payload)
        + len(service_desert_row_payload)
        + len(noise_row_payload)
    )
    tracker.set_phase_totals(
        "publish",
        total_units=publish_total_rows,
        rebuild_total_units=publish_total_rows,
        unit_label="rows",
        detail="writing manifest",
        force_log=True,
    )
    publish_write_started_at = time.perf_counter()
    resolved_noise_hash = _resolve_noise_processing_hash(noise_processing_hash, engine)
    from precompute._rows import _ArtifactNoiseReference
    resolved_artifact_hash = (
        noise_row_payload.noise_resolved_hash
        if isinstance(noise_row_payload, _ArtifactNoiseReference)
        else None
    )
    # Full-island profile: artifact already covers the whole island so clipping
    # noise polygons to study_area_wgs84 with ST_Intersection is wasted work.
    # Pass None to skip clipping; for county/bbox profiles keep the clip.
    noise_study_area = None if build_profile == "full" else study_area_wgs84
    publish_precomputed_artifacts(
        engine,
        hashes=hashes,
        extract_path=str(source_state.extract_path),
        walk_rows=walk_row_payload,
        amenity_rows=amenity_row_payload,
        python_version=python_version(),
        packages_json=package_snapshot(),
        summary_json=summary_payload,
        transport_reality_rows=transport_reality_row_payload,
        service_desert_rows=service_desert_row_payload,
        noise_rows=noise_row_payload,
        study_area_wgs84=study_area_wgs84,
        noise_study_area_wgs84=noise_study_area,
        noise_processing_hash=resolved_noise_hash,
        noise_artifact_hash=resolved_artifact_hash,
        progress_cb=publish_progress_cb,
    )
    walk_stats = getattr(walk_row_payload, "stats", None)
    walk_prep_seconds = 0.0
    if walk_stats is not None:
        walk_prep_seconds = (
            getattr(walk_stats, "geometry_materialize_seconds", 0.0)
            + getattr(walk_stats, "row_assembly_seconds", 0.0)
        )
        tracker.record_substep(
            "publish",
            "walk_geometry_materialize",
            getattr(walk_stats, "geometry_materialize_seconds", 0.0),
            force_log=True,
        )
        tracker.record_substep(
            "publish",
            "walk_row_assembly",
            getattr(walk_stats, "row_assembly_seconds", 0.0),
            force_log=True,
        )
    amenity_stats = getattr(amenity_row_payload, "stats", None)
    amenity_prep_seconds = 0.0
    if amenity_stats is not None:
        amenity_prep_seconds = getattr(amenity_stats, "row_assembly_seconds", 0.0)
        tracker.record_substep(
            "publish",
            "amenity_row_assembly",
            getattr(amenity_stats, "row_assembly_seconds", 0.0),
            force_log=True,
        )
    transport_reality_stats = getattr(transport_reality_row_payload, "stats", None)
    if transport_reality_stats is not None:
        tracker.record_substep(
            "publish",
            "transport_reality_row_assembly",
            getattr(transport_reality_stats, "row_assembly_seconds", 0.0),
            force_log=True,
        )
    service_desert_stats = getattr(service_desert_row_payload, "stats", None)
    if service_desert_stats is not None:
        tracker.record_substep(
            "publish",
            "service_desert_row_assembly",
            getattr(service_desert_stats, "row_assembly_seconds", 0.0),
            force_log=True,
        )
    noise_stats = getattr(noise_row_payload, "stats", None)
    if noise_stats is not None:
        tracker.record_substep(
            "publish",
            "noise_row_assembly",
            getattr(noise_stats, "row_assembly_seconds", 0.0),
            force_log=True,
        )
    publish_write_seconds = max(
        time.perf_counter() - publish_write_started_at - walk_prep_seconds - amenity_prep_seconds,
        0.0,
    )
    tracker.record_substep(
        "publish",
        "write",
        publish_write_seconds,
        force_log=True,
    )
    tracker.finish_phase("publish", "completed", detail=f"{publish_total_rows:,} rows written")

    if _pmtiles_bake_configured(bake_pmtiles, pmtiles_output_path):
        bake_seconds = _run_pmtiles_bake(
            bake_pmtiles=bake_pmtiles,
            engine=engine,
            build_key=hashes.build_key,
            pmtiles_output_path=pmtiles_output_path,
            noise_max_zoom=noise_max_zoom,
        )
        tracker.record_substep(
            "publish",
            "bake_pmtiles",
            bake_seconds,
            force_log=True,
        )

    tracker.save_successful_timings()

    hashes = get_hashes()
    print(f"Import fingerprint: {hashes.import_fingerprint}")
    print(f"Build key: {hashes.build_key}")
    print(f"Total wall time: {time.perf_counter() - total_started_at:.1f}s")
    return hashes.build_key
