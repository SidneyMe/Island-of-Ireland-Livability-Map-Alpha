from __future__ import annotations


def ensure_local_osm_import_impl(
    engine,
    source_state,
    *,
    study_area_wgs84,
    normalization_scope_hash: str,
    import_payload_ready_fn,
    raw_import_ready_fn,
    osm2pgsql_properties_exists_fn,
    drop_importer_owned_raw_tables_fn,
    run_osm2pgsql_import_fn,
    ensure_managed_raw_support_tables_fn,
    begin_import_manifest_fn,
    complete_import_manifest_fn,
    clear_normalized_import_artifacts_fn,
    emit_detail_fn,
    force_refresh: bool = False,
    progress_cb=None,
) -> None:
    del study_area_wgs84
    payload_ready = import_payload_ready_fn(
        engine,
        source_state.import_fingerprint,
        normalization_scope_hash,
    )
    if not force_refresh and payload_ready:
        return

    raw_ready = raw_import_ready_fn(engine, source_state.import_fingerprint)
    has_osm2pgsql_metadata = osm2pgsql_properties_exists_fn(engine)
    should_run_osm2pgsql = force_refresh or not raw_ready

    if force_refresh:
        emit_detail_fn(
            progress_cb,
            "Force refresh requested; dropping importer-owned raw tables before rerunning local .osm.pbf import."
        )
        drop_importer_owned_raw_tables_fn(engine)
    elif not raw_ready and not has_osm2pgsql_metadata:
        emit_detail_fn(
            progress_cb,
            "No previous osm2pgsql import metadata found; dropping importer-owned raw tables before initial local .osm.pbf import."
        )
        drop_importer_owned_raw_tables_fn(engine)
    elif not raw_ready:
        emit_detail_fn(
            progress_cb,
            (
                "Raw amenity payload is missing or incomplete for this import_fingerprint; "
                "dropping importer-owned raw tables before refresh."
            ),
        )
        drop_importer_owned_raw_tables_fn(engine)
    else:
        emit_detail_fn(
            progress_cb,
            "Raw amenity payload is ready for this import_fingerprint; reusing osm2pgsql tables."
        )

    if should_run_osm2pgsql:
        run_osm2pgsql_import_fn(source_state, progress_cb=progress_cb)
    ensure_managed_raw_support_tables_fn(engine)
    begin_import_manifest_fn(
        engine,
        import_fingerprint=source_state.import_fingerprint,
        extract_path=str(source_state.extract_path),
        extract_fingerprint=source_state.extract_fingerprint,
        importer_version=source_state.importer_version,
        importer_config_hash=source_state.importer_config_hash,
        normalization_scope_hash=normalization_scope_hash,
    )
    try:
        complete_import_manifest_fn(engine, source_state.import_fingerprint)
    except Exception:
        clear_normalized_import_artifacts_fn(engine, source_state.import_fingerprint)
        raise
