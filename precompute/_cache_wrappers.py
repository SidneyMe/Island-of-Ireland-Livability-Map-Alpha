from __future__ import annotations

from pathlib import Path
from typing import Any

from config import FORCE_RECOMPUTE, USE_COMPRESSED_CACHE

from . import cache as _cache
from ._state import _STATE


def cache_exists(key: str, cache_dir: Path) -> bool:
    return _cache.cache_exists(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
    )


def cache_load(key: str, cache_dir: Path):
    return _cache.cache_load(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
    )


def cache_save(key: str, data: Any, cache_dir: Path) -> None:
    _cache.cache_save(key, data, cache_dir)


def cache_exists_large(key: str, cache_dir: Path) -> bool:
    return _cache.cache_exists_large(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_load_large(key: str, cache_dir: Path):
    return _cache.cache_load_large(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        tier_valid=_STATE.tier_valid,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def _cache_load_for_finalize(key: str, cache_dir: Path):
    return _cache.cache_load_for_finalize(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
    )


def _cache_load_large_for_finalize(key: str, cache_dir: Path):
    return _cache.cache_load_large_for_finalize(
        key,
        cache_dir,
        force_recompute=FORCE_RECOMPUTE,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_save_large(key: str, data: Any, cache_dir: Path) -> None:
    _cache.cache_save_large(
        key,
        data,
        cache_dir,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_save_large_append_frame(key: str, frame: Any, cache_dir: Path) -> None:
    _cache.cache_save_large_append_frame(
        key,
        frame,
        cache_dir,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )


def cache_reset_large_frames(key: str, cache_dir: Path) -> None:
    _cache.cache_reset_large_frames(
        key,
        cache_dir,
        use_compressed_cache=USE_COMPRESSED_CACHE,
    )
