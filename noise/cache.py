from __future__ import annotations

import gzip
import hashlib
import json
import os
import pickle
import shutil
import sys
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator


NOISE_CANDIDATES_CACHE_VERSION = 4
NOISE_CANDIDATE_CACHE_SCHEMA_VERSION = 1
NOISE_CANDIDATES_CACHE_PART_ROWS = 2000
NOISE_CANDIDATES_CACHE_SUBDIR = "noise_candidates"
NOISE_CANDIDATES_CACHE_DIR_ENV = "NOISE_CANDIDATES_CACHE_DIR"
DEFAULT_NOISE_CANDIDATES_CACHE_DIR = (
    Path(__file__).resolve().parent.parent / ".livability_cache"
)


def _candidates_cache_dir(override: Path | None = None) -> Path:
    if override is not None:
        return Path(override)
    raw = os.getenv(NOISE_CANDIDATES_CACHE_DIR_ENV, "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_NOISE_CANDIDATES_CACHE_DIR


def _study_area_signature(study_area_wgs84) -> str:
    if study_area_wgs84 is None:
        return "none"
    try:
        payload = study_area_wgs84.wkb
    except Exception:
        bounds = getattr(study_area_wgs84, "bounds", None)
        payload = repr(bounds).encode("utf-8")
    else:
        if isinstance(payload, memoryview):
            payload = payload.tobytes()
        elif isinstance(payload, bytearray):
            payload = bytes(payload)
        elif not isinstance(payload, bytes):
            payload = repr(payload).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _candidates_cache_path(cache_dir: Path, key: str) -> Path:
    # Legacy single-file cache format (kept for backward-compatible reads only).
    return Path(cache_dir) / f"noise_candidates_{key}.pkl.gz"


def _candidates_cache_entry_dir(cache_dir: Path, key: str) -> Path:
    return Path(cache_dir) / NOISE_CANDIDATES_CACHE_SUBDIR / key


def _candidates_cache_manifest_path(entry_dir: Path) -> Path:
    return entry_dir / "manifest.json"


def _load_chunked_cache_manifest(entry_dir: Path) -> dict[str, Any] | None:
    manifest_path = _candidates_cache_manifest_path(entry_dir)
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    parts = payload.get("parts")
    if not isinstance(parts, list):
        return None
    return payload


def _iter_chunked_cached_candidates(
    entry_dir: Path,
    manifest: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    parts = manifest.get("parts") or []
    for part_name in parts:
        if not isinstance(part_name, str):
            raise ValueError("invalid cache manifest: part name must be a string")
        part_path = entry_dir / part_name
        with gzip.open(part_path, "rb") as handle:
            payload = pickle.load(handle)
        if not isinstance(payload, list):
            raise ValueError(f"invalid cache part payload (expected list): {part_path.name}")
        for row in payload:
            if not isinstance(row, dict):
                raise ValueError(f"invalid cache row payload (expected dict): {part_path.name}")
            yield row


def _delete_chunked_cache(entry_dir: Path) -> None:
    try:
        shutil.rmtree(entry_dir)
    except FileNotFoundError:
        return
    except OSError:
        return


class _CandidateCacheWriter:
    def __init__(
        self,
        *,
        entry_dir: Path,
        cache_key: str,
        source_signature: str,
        study_area_signature: str,
    ) -> None:
        self.entry_dir = Path(entry_dir)
        self.tmp_dir = self.entry_dir.parent / f"{self.entry_dir.name}.tmp"
        self.cache_key = str(cache_key)
        self.source_signature = str(source_signature)
        self.study_area_signature = str(study_area_signature)
        self.parts: list[str] = []
        self.row_count = 0

        _delete_chunked_cache(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def write_batch(self, serialized_rows: list[dict[str, Any]]) -> None:
        if not serialized_rows:
            return
        part_name = f"part-{len(self.parts) + 1:06d}.pkl.gz"
        part_path = self.tmp_dir / part_name
        with gzip.open(part_path, "wb", compresslevel=6) as handle:
            pickle.dump(serialized_rows, handle, protocol=pickle.HIGHEST_PROTOCOL)
        self.parts.append(part_name)
        self.row_count += len(serialized_rows)

    def finalize(self) -> Path:
        manifest = {
            "cache_schema_version": NOISE_CANDIDATE_CACHE_SCHEMA_VERSION,
            "cache_version": NOISE_CANDIDATES_CACHE_VERSION,
            "cache_key": self.cache_key,
            "source_signature": self.source_signature,
            "study_area_signature": self.study_area_signature,
            "row_count": int(self.row_count),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parts": list(self.parts),
        }
        manifest_path = _candidates_cache_manifest_path(self.tmp_dir)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

        self.entry_dir.parent.mkdir(parents=True, exist_ok=True)
        if self.entry_dir.exists():
            shutil.rmtree(self.entry_dir)
        self.tmp_dir.replace(self.entry_dir)
        return self.entry_dir


def _serialize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    geom = row.get("geom")
    serialized = {key: value for key, value in row.items() if key != "geom"}
    serialized["geom_wkb"] = geom.wkb if geom is not None else None
    return serialized


def _deserialize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    from shapely import wkb as shapely_wkb

    payload = dict(row)
    wkb_bytes = payload.pop("geom_wkb", None)
    payload["geom"] = shapely_wkb.loads(wkb_bytes) if wkb_bytes else None
    return payload


def _load_cached_candidates(path: Path) -> list[dict[str, Any]] | None:
    # Legacy single-file cache reader.
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rb") as handle:
            payload = pickle.load(handle)
    except (pickle.UnpicklingError, EOFError, OSError, gzip.BadGzipFile):
        try:
            path.unlink()
        except OSError:
            pass
        return None
    if not isinstance(payload, list):
        return None
    return payload


def _save_cached_candidates(path: Path, serialized_rows: list[dict[str, Any]]) -> None:
    # Legacy single-file cache writer retained for compatibility helpers/tests.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with gzip.open(tmp, "wb", compresslevel=6) as handle:
            pickle.dump(serialized_rows, handle, protocol=pickle.HIGHEST_PROTOCOL)
        if sys.platform == "win32" and path.exists():
            path.unlink()
        tmp.replace(path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _batched(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    if size <= 0:
        raise ValueError("batch size must be > 0")
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


def _study_area_wkb_or_none(study_area_wgs84) -> bytes | None:
    if study_area_wgs84 is None:
        return None
    return bytes(study_area_wgs84.wkb)


def _maybe_loads_study_area_wkb(study_area_wkb: bytes | None):
    if study_area_wkb is None:
        return None
    from shapely import wkb as shapely_wkb

    return shapely_wkb.loads(study_area_wkb)
