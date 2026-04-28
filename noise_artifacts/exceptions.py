"""Custom exceptions for the noise artifact pipeline."""
from __future__ import annotations


class NoiseArtifactError(RuntimeError):
    """Top-level noise artifact pipeline error."""


class NoiseIngestError(NoiseArtifactError):
    """Error during noise data ingest into noise_normalized."""
