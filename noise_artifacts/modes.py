from __future__ import annotations

import os
from typing import Literal


NoiseAccuracyMode = Literal["dev_fast", "accurate"]

_ACCURACY_MODE_ENV = "NOISE_ACCURACY_MODE"
_VALID_MODES = {"dev_fast", "accurate"}


def normalize_noise_accuracy_mode(value: str | None) -> NoiseAccuracyMode:
    raw = (value or "").strip().lower().replace("-", "_")
    if not raw:
        raw = "dev_fast"
    if raw not in _VALID_MODES:
        allowed = ", ".join(sorted(_VALID_MODES))
        raise ValueError(f"{_ACCURACY_MODE_ENV} must be one of {{{allowed}}}, got {value!r}")
    return "accurate" if raw == "accurate" else "dev_fast"


def resolve_noise_accuracy_mode(
    *,
    cli_noise_accurate: bool = False,
    profile: str | None = None,
) -> NoiseAccuracyMode:
    # Precedence: CLI flag > NOISE_ACCURACY_MODE env > build profile default.
    # The published full profile defaults to accurate; dev/test stay dev_fast.
    if cli_noise_accurate:
        return "accurate"
    raw_env = (os.getenv(_ACCURACY_MODE_ENV) or "").strip()
    if raw_env:
        return normalize_noise_accuracy_mode(raw_env)
    if (profile or "").strip().lower() == "full":
        return "accurate"
    return "dev_fast"


def noise_accuracy_mode_label(mode: NoiseAccuracyMode) -> str:
    return "accurate" if mode == "accurate" else "dev-fast"
