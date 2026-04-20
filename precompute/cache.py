from __future__ import annotations

import gzip
import pickle
import sys
import time
from pathlib import Path
from typing import Any


def _safe_replace(tmp: Path, final: Path) -> None:
    # Windows: os.replace fails (WinError 5) if target exists and is open
    if sys.platform == "win32" and final.exists():
        final.unlink()
    tmp.replace(final)


def _pkl_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.pkl"


def _gz_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.pkl.gz"


def _chunks_gz_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.chunks.pkl.gz"


def _chunks_pkl_path(key: str, cache_dir: Path) -> Path:
    return cache_dir / f"{key}.chunks.pkl"


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


def _load_chunks_stream(path: Path, cache_dir: Path, *, gzip_stream: bool):
    if not path.exists():
        return None
    merged: dict = {}
    try:
        opener = gzip.open if gzip_stream else open
        with opener(path, "rb") as handle:
            while True:
                try:
                    frame = pickle.load(handle)
                except EOFError:
                    break
                if isinstance(frame, dict):
                    merged.update(frame)
        return merged
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


def _load_large_cache_value(
    key: str,
    cache_dir: Path,
    *,
    use_compressed_cache: bool,
):
    if use_compressed_cache:
        base_path = _gz_path(key, cache_dir)
        chunk_path = _chunks_gz_path(key, cache_dir)
        base_value = _load_gzip_cache(base_path, cache_dir)
        chunk_value = (
            _load_chunks_stream(chunk_path, cache_dir, gzip_stream=True)
            if chunk_path.exists()
            else None
        )
    else:
        base_path = _pkl_path(key, cache_dir)
        chunk_path = _chunks_pkl_path(key, cache_dir)
        base_value = _load_pickle_cache(base_path, cache_dir)
        chunk_value = (
            _load_chunks_stream(chunk_path, cache_dir, gzip_stream=False)
            if chunk_path.exists()
            else None
        )

    if chunk_value is None:
        return base_value
    if base_value is None:
        return chunk_value
    if isinstance(base_value, dict) and isinstance(chunk_value, dict):
        merged = dict(base_value)
        merged.update(chunk_value)
        return merged
    return chunk_value


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
        _safe_replace(tmp, final)
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
        return _gz_path(key, cache_dir).exists() or _chunks_gz_path(key, cache_dir).exists()
    return _pkl_path(key, cache_dir).exists() or _chunks_pkl_path(key, cache_dir).exists()


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
    return _load_large_cache_value(
        key,
        cache_dir,
        use_compressed_cache=use_compressed_cache,
    )


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
    return _load_large_cache_value(
        key,
        cache_dir,
        use_compressed_cache=use_compressed_cache,
    )


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
            _safe_replace(tmp, final)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return
    cache_save(key, data, cache_dir)


def cache_save_large_append_frame(
    key: str,
    frame: Any,
    cache_dir: Path,
    *,
    use_compressed_cache: bool,
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    if use_compressed_cache:
        path = _chunks_gz_path(key, cache_dir)
        with gzip.open(path, "ab", compresslevel=6) as handle:
            pickle.dump(frame, handle, protocol=pickle.HIGHEST_PROTOCOL)
        return
    path = _chunks_pkl_path(key, cache_dir)
    with path.open("ab") as handle:
        pickle.dump(frame, handle, protocol=pickle.HIGHEST_PROTOCOL)


def cache_reset_large_frames(
    key: str,
    cache_dir: Path,
    *,
    use_compressed_cache: bool,
) -> None:
    _chunks_gz_path(key, cache_dir).unlink(missing_ok=True)
    _chunks_pkl_path(key, cache_dir).unlink(missing_ok=True)
    if use_compressed_cache:
        _gz_path(key, cache_dir).unlink(missing_ok=True)
    else:
        _pkl_path(key, cache_dir).unlink(missing_ok=True)
