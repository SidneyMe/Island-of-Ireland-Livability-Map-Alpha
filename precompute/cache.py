from __future__ import annotations

import gzip
import pickle
import time
from pathlib import Path
from typing import Any


def _pkl_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.pkl"


def _gz_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.pkl.gz"


def _quarantine(path: Path) -> None:
    bad_path = path.with_suffix(path.suffix + ".bad")
    if bad_path.exists():
        bad_path = path.with_suffix(f"{path.suffix}.bad.{time.time_ns()}")
    try:
        path.rename(bad_path)
        print(f"  [cache] quarantined {path.name} -> {bad_path.name}")
    except OSError as exc:
        print(f"  [cache] could not quarantine {path.name} ({exc})")


def _load_pickle_cache(path: Path, cache_dir: Path):
    if not path.exists():
        return None
    try:
        with path.open("rb") as handle:
            return pickle.load(handle)
    except (pickle.UnpicklingError, EOFError, OSError) as exc:
        print(
            f"  [cache] {cache_dir.name}/{path.name}: corrupted ({type(exc).__name__}) - will rebuild"
        )
        _quarantine(path)
        return None
    except Exception as exc:  # pragma: no cover - defensive cache handling
        print(
            f"  [cache] {cache_dir.name}/{path.name}: read error ({type(exc).__name__}) - will rebuild"
        )
        _quarantine(path)
        return None


def _load_gzip_cache(path: Path, cache_dir: Path):
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rb") as handle:
            return pickle.load(handle)
    except (pickle.UnpicklingError, EOFError, OSError, gzip.BadGzipFile) as exc:
        print(
            f"  [cache] {cache_dir.name}/{path.name}: corrupted ({type(exc).__name__}) - will rebuild"
        )
        _quarantine(path)
        return None
    except Exception as exc:  # pragma: no cover - defensive cache handling
        print(
            f"  [cache] {cache_dir.name}/{path.name}: read error ({type(exc).__name__}) - will rebuild"
        )
        _quarantine(path)
        return None


def cache_exists(
    key: str,
    cache_dir: Path,
    *,
    force_recompute: bool,
    tier_valid: dict[Path, bool],
) -> bool:
    if force_recompute or not tier_valid.get(cache_dir, False):
        return False
    return _pkl_path(key, cache_dir).exists()


def cache_load(
    key: str,
    cache_dir: Path,
    *,
    force_recompute: bool,
    tier_valid: dict[Path, bool],
):
    if force_recompute or not tier_valid.get(cache_dir, False):
        return None
    return _load_pickle_cache(_pkl_path(key, cache_dir), cache_dir)


def cache_save(key: str, data: Any, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    final = _pkl_path(key, cache_dir)
    tmp = final.with_suffix(".pkl.tmp")
    try:
        with tmp.open("wb") as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(final)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def cache_exists_large(
    key: str,
    cache_dir: Path,
    *,
    force_recompute: bool,
    tier_valid: dict[Path, bool],
    use_compressed_cache: bool,
) -> bool:
    if force_recompute or not tier_valid.get(cache_dir, False):
        return False
    if use_compressed_cache:
        return _gz_path(key, cache_dir).exists()
    return _pkl_path(key, cache_dir).exists()


def cache_load_large(
    key: str,
    cache_dir: Path,
    *,
    force_recompute: bool,
    tier_valid: dict[Path, bool],
    use_compressed_cache: bool,
):
    if force_recompute or not tier_valid.get(cache_dir, False):
        return None
    if use_compressed_cache:
        return _load_gzip_cache(_gz_path(key, cache_dir), cache_dir)
    return _load_pickle_cache(_pkl_path(key, cache_dir), cache_dir)


def cache_load_for_finalize(key: str, cache_dir: Path, *, force_recompute: bool):
    if force_recompute:
        return None
    return _load_pickle_cache(_pkl_path(key, cache_dir), cache_dir)


def cache_load_large_for_finalize(
    key: str,
    cache_dir: Path,
    *,
    force_recompute: bool,
    use_compressed_cache: bool,
):
    if force_recompute:
        return None
    if use_compressed_cache:
        return _load_gzip_cache(_gz_path(key, cache_dir), cache_dir)
    return _load_pickle_cache(_pkl_path(key, cache_dir), cache_dir)


def cache_save_large(
    key: str,
    data: Any,
    cache_dir: Path,
    *,
    use_compressed_cache: bool,
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    if use_compressed_cache:
        final = _gz_path(key, cache_dir)
        tmp = final.with_suffix(".gz.tmp")
        try:
            with gzip.open(tmp, "wb", compresslevel=6) as handle:
                pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
            tmp.replace(final)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return
    cache_save(key, data, cache_dir)
