"""
noise_artifacts — standalone noise artifact pipeline.

The normal livability build uses metadata lookups from this package only.
All other functions are used exclusively by `python -m noise_artifacts`.
"""
from __future__ import annotations

from .manifest import (
    ArtifactManifest,
    get_active_artifact,
    get_resolved_artifact_for_mode,
    mark_artifact_complete,
    mark_artifact_failed,
    noise_domain_hash,
    noise_resolved_hash,
    noise_source_hash,
    noise_tile_hash,
    record_lineage,
    reset_artifact_for_retry,
    set_active_artifact,
    upsert_artifact,
)

from .builder import build_noise_artifact

__all__ = [
    "ArtifactManifest",
    "build_noise_artifact",
    "get_active_artifact",
    "get_resolved_artifact_for_mode",
    "mark_artifact_complete",
    "mark_artifact_failed",
    "noise_domain_hash",
    "noise_resolved_hash",
    "noise_source_hash",
    "noise_tile_hash",
    "record_lineage",
    "reset_artifact_for_retry",
    "set_active_artifact",
    "upsert_artifact",
]
