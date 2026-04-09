from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ._dependencies import Engine, delete, insert, select, update
from .common import root_module
from .tables import build_manifest, import_manifest


def load_import_manifest(engine: Engine, import_fingerprint: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            select(import_manifest)
            .where(import_manifest.c.import_fingerprint == import_fingerprint)
        ).mappings().first()
    return dict(row) if row is not None else None


def has_complete_import_manifest(engine: Engine, import_fingerprint: str) -> bool:
    manifest = root_module().load_import_manifest(engine, import_fingerprint)
    return manifest is not None and manifest.get("status") == "complete"


def _manifest_matches_scope(
    manifest: dict[str, Any] | None,
    normalization_scope_hash: str,
) -> bool:
    if manifest is None or manifest.get("status") != "complete":
        return False
    return str(manifest.get("normalization_scope_hash") or "") == normalization_scope_hash


def load_build_manifest(engine: Engine, build_key: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            select(build_manifest)
            .where(build_manifest.c.build_key == build_key)
        ).mappings().first()
    return dict(row) if row is not None else None


def has_complete_build(engine: Engine, build_key: str) -> bool:
    manifest = root_module().load_build_manifest(engine, build_key)
    return manifest is not None and manifest.get("status") == "complete"


def load_complete_manifest(engine: Engine, build_key: str) -> dict[str, Any] | None:
    manifest = root_module().load_build_manifest(engine, build_key)
    if manifest is None or manifest.get("status") != "complete":
        return None
    return manifest


def load_runtime_manifest(engine: Engine, *, extract_path: str, config_hash: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            select(build_manifest)
            .where(build_manifest.c.extract_path == extract_path)
            .where(build_manifest.c.config_hash == config_hash)
            .where(build_manifest.c.status == "complete")
            .order_by(build_manifest.c.completed_at.desc(), build_manifest.c.created_at.desc())
            .limit(1)
        ).mappings().first()
    return dict(row) if row is not None else None


def begin_import_manifest(
    engine: Engine,
    *,
    import_fingerprint: str,
    extract_path: str,
    extract_fingerprint: str,
    importer_version: str,
    importer_config_hash: str,
    normalization_scope_hash: str,
) -> None:
    created_at = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            delete(import_manifest)
            .where(import_manifest.c.import_fingerprint == import_fingerprint)
        )
        connection.execute(
            insert(import_manifest),
            [{
                "import_fingerprint": import_fingerprint,
                "extract_path": extract_path,
                "extract_fingerprint": extract_fingerprint,
                "importer_version": importer_version,
                "importer_config_hash": importer_config_hash,
                "normalization_scope_hash": normalization_scope_hash,
                "status": "building",
                "created_at": created_at,
                "completed_at": None,
            }],
        )


def complete_import_manifest(engine: Engine, import_fingerprint: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            update(import_manifest)
            .where(import_manifest.c.import_fingerprint == import_fingerprint)
            .values(
                status="complete",
                completed_at=datetime.now(timezone.utc),
            )
        )
