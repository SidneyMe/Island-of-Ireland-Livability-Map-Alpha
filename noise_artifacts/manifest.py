"""
Artifact manifest management for the noise artifact pipeline.

All artifact identity hashes are deterministic: same inputs → same hash.
Multiple failed/retried attempts of the same artifact reset status to 'building'
via reset_artifact_for_retry() on --force; they do not create new rows.

The livability build must ONLY call SQL metadata lookup helpers from this
module. It must NOT compute source hashes, read data_dir, or call noise.loader.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Artifact identity hash functions — deterministic, no I/O
# ---------------------------------------------------------------------------

def noise_source_hash(
    source_signature: str,
    parser_version: int,
    source_schema_version: int,
) -> str:
    """
    Deterministic identity hash for a 'source' artifact.

    source_signature: opaque hash over raw file inventory (e.g. from dataset_signature()).
                      Computed by the caller; this function does no file I/O.
    """
    raw = f"{source_signature}:{parser_version}:{source_schema_version}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def noise_domain_hash(
    domain_boundary_bytes: bytes,
    extent_version: int,
) -> str:
    """Deterministic identity hash for a 'domain' artifact."""
    h = hashlib.sha256(domain_boundary_bytes)
    h.update(str(extent_version).encode())
    return h.hexdigest()[:16]


def noise_resolved_hash(
    source_hash: str,
    domain_hash: str,
    topology_rules_version: int,
    dissolve_rules_version: int,
    round_priority_version: int,
    topology_grid_metres: float,
    identity_payload: dict[str, Any] | None = None,
) -> str:
    """Deterministic identity hash for a 'resolved' artifact."""
    if identity_payload:
        import json
        payload = json.dumps(identity_payload, sort_keys=True, default=str)
    else:
        payload = ""
    raw = (
        f"{source_hash}:{domain_hash}"
        f":{topology_rules_version}:{dissolve_rules_version}"
        f":{round_priority_version}:{topology_grid_metres}:{payload}"
    ).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def noise_tile_hash(
    resolved_hash: str,
    tile_schema_version: int,
    min_zoom: int,
    max_zoom: int,
    exported_properties: tuple[str, ...],
    simplify_tolerances: tuple[float, ...],
) -> str:
    """Deterministic identity hash for a 'tiles' artifact."""
    props = ",".join(sorted(exported_properties))
    tols = ",".join(str(t) for t in simplify_tolerances)
    raw = (
        f"{resolved_hash}:{tile_schema_version}"
        f":{min_zoom}:{max_zoom}:{props}:{tols}"
    ).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


# ---------------------------------------------------------------------------
# ArtifactManifest dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ArtifactManifest:
    artifact_hash: str
    artifact_type: str       # source | domain | resolved | tiles | exposure
    status: str              # building | complete | failed | superseded
    manifest_json: dict[str, Any]
    created_at: datetime
    completed_at: datetime | None


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def upsert_artifact(
    engine: Engine,
    artifact_hash: str,
    artifact_type: str,
    manifest_json: dict[str, Any],
) -> None:
    """
    Insert an artifact row with status='building'.
    ON CONFLICT DO NOTHING — idempotent for first-time creation.
    Use reset_artifact_for_retry() to re-run a failed/building artifact.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO noise_artifact_manifest
                    (artifact_hash, artifact_type, manifest_json, status, created_at)
                VALUES (:artifact_hash, :artifact_type, CAST(:manifest_json AS jsonb), 'building', now())
                ON CONFLICT (artifact_hash) DO NOTHING
                """
            ),
            {
                "artifact_hash": artifact_hash,
                "artifact_type": artifact_type,
                "manifest_json": _json_dumps(manifest_json),
            },
        )


def reset_artifact_for_retry(
    engine: Engine,
    artifact_hash: str,
    manifest_json: dict[str, Any],
) -> None:
    """
    Reset a failed/building artifact back to status='building' for a retry.
    Use on --force when artifact_hash already exists with status != 'complete'.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE noise_artifact_manifest
                SET status = 'building',
                    manifest_json = CAST(:manifest_json AS jsonb),
                    completed_at = NULL
                WHERE artifact_hash = :artifact_hash
                """
            ),
            {
                "artifact_hash": artifact_hash,
                "manifest_json": _json_dumps(manifest_json),
            },
        )


def mark_artifact_complete(
    engine: Engine,
    artifact_hash: str,
    *,
    updated_manifest_json: dict[str, Any],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE noise_artifact_manifest
                SET status = 'complete',
                    completed_at = now(),
                    manifest_json = CAST(:manifest_json AS jsonb)
                WHERE artifact_hash = :artifact_hash
                """
            ),
            {
                "artifact_hash": artifact_hash,
                "manifest_json": _json_dumps(updated_manifest_json),
            },
        )


def mark_artifact_failed(
    engine: Engine,
    artifact_hash: str,
    *,
    error_detail: str,
) -> None:
    error_detail_text = str(error_detail)[:4000]
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE noise_artifact_manifest
                SET status = 'failed',
                    manifest_json = COALESCE(manifest_json, '{}'::jsonb)
                        || jsonb_build_object(
                            'error_detail',
                            CAST(:error_detail AS text)
                        )
                WHERE artifact_hash = :artifact_hash
                """
            ),
            {
                "artifact_hash": artifact_hash,
                "error_detail": error_detail_text,
            },
        )


def record_lineage(
    engine: Engine,
    artifact_hash: str,
    parent_hash: str,
) -> None:
    """
    Record that artifact_hash was derived from parent_hash.
    Parent row must already exist (FK enforced in DB).
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO noise_artifact_lineage (artifact_hash, parent_hash)
                VALUES (:artifact_hash, :parent_hash)
                ON CONFLICT DO NOTHING
                """
            ),
            {"artifact_hash": artifact_hash, "parent_hash": parent_hash},
        )


def set_active_artifact(
    engine: Engine,
    artifact_type: str,
    artifact_hash: str,
) -> None:
    """
    Set the active artifact pointer for a given type.
    The livability build reads this to find the current artifact without touching raw files.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO noise_active_artifact (artifact_type, artifact_hash)
                VALUES (:artifact_type, :artifact_hash)
                ON CONFLICT (artifact_type) DO UPDATE
                    SET artifact_hash = EXCLUDED.artifact_hash
                """
            ),
            {"artifact_type": artifact_type, "artifact_hash": artifact_hash},
        )


def get_active_artifact(
    engine: Engine,
    artifact_type: str,
) -> ArtifactManifest | None:
    """
    Return the active complete artifact for the given type, or None.

    This is the ONLY function the livability build should call.
    It does not read raw files, compute source hashes, or access data_dir.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    m.artifact_hash,
                    m.artifact_type,
                    m.status,
                    m.manifest_json,
                    m.created_at,
                    m.completed_at
                FROM noise_active_artifact a
                JOIN noise_artifact_manifest m
                    ON m.artifact_hash = a.artifact_hash
                WHERE a.artifact_type = :artifact_type
                  AND m.status = 'complete'
                """
            ),
            {"artifact_type": artifact_type},
        ).mappings().first()

    if row is None:
        return None

    return ArtifactManifest(
        artifact_hash=str(row["artifact_hash"]),
        artifact_type=str(row["artifact_type"]),
        status=str(row["status"]),
        manifest_json=dict(row["manifest_json"] or {}),
        created_at=_ensure_utc(row["created_at"]),
        completed_at=_ensure_utc(row["completed_at"]) if row["completed_at"] else None,
    )


def get_resolved_artifact_for_mode(
    engine: Engine,
    required_mode: str,
) -> ArtifactManifest | None:
    """
    Resolve a completed resolved artifact for a canonical mode.

    Lookup order:
      1. active resolved pointer when manifest mode matches required_mode
      2. latest completed resolved artifact with matching manifest mode

    Mode matching is strict against manifest_json->>'noise_accuracy_mode'.
    Missing/null/unknown values are treated as non-match.
    """
    mode = str(required_mode or "").strip().lower()
    if mode not in {"dev_fast", "accurate"}:
        raise ValueError(
            f"required_mode must be one of 'dev_fast' or 'accurate', got {required_mode!r}"
        )

    with engine.connect() as conn:
        active_row = conn.execute(
            text(
                """
                SELECT
                    m.artifact_hash,
                    m.artifact_type,
                    m.status,
                    m.manifest_json,
                    m.created_at,
                    m.completed_at
                FROM noise_active_artifact a
                JOIN noise_artifact_manifest m
                    ON m.artifact_hash = a.artifact_hash
                WHERE a.artifact_type = 'resolved'
                  AND m.artifact_type = 'resolved'
                  AND m.status = 'complete'
                  AND COALESCE(m.manifest_json ->> 'noise_accuracy_mode', '') = :required_mode
                LIMIT 1
                """
            ),
            {"required_mode": mode},
        ).mappings().first()
        if active_row is not None:
            return _row_to_manifest(active_row)

        fallback_row = conn.execute(
            text(
                """
                SELECT
                    m.artifact_hash,
                    m.artifact_type,
                    m.status,
                    m.manifest_json,
                    m.created_at,
                    m.completed_at
                FROM noise_artifact_manifest m
                WHERE m.artifact_type = 'resolved'
                  AND m.status = 'complete'
                  AND COALESCE(m.manifest_json ->> 'noise_accuracy_mode', '') = :required_mode
                ORDER BY
                    m.completed_at DESC NULLS LAST,
                    m.created_at DESC,
                    m.artifact_hash DESC
                LIMIT 1
                """
            ),
            {"required_mode": mode},
        ).mappings().first()

    if fallback_row is None:
        return None
    return _row_to_manifest(fallback_row)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_dumps(obj: dict[str, Any]) -> str:
    import json
    return json.dumps(obj, default=str)


def _ensure_utc(dt: datetime) -> datetime:
    if dt is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _row_to_manifest(row) -> ArtifactManifest:
    return ArtifactManifest(
        artifact_hash=str(row["artifact_hash"]),
        artifact_type=str(row["artifact_type"]),
        status=str(row["status"]),
        manifest_json=dict(row["manifest_json"] or {}),
        created_at=_ensure_utc(row["created_at"]),
        completed_at=_ensure_utc(row["completed_at"]) if row["completed_at"] else None,
    )
