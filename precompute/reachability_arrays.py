import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np


FORMAT_NAME = "livability.reachability_matrix"
FORMAT_VERSION = 1
ORIGIN_DTYPE = np.dtype("<u8")
COUNTS_DTYPE = np.dtype("<u4")
EFFECTIVE_UNITS_DTYPE = np.dtype("<f4")


def matrix_dtype_for_value_kind(value_kind: str) -> np.dtype:
    if value_kind == "counts":
        return COUNTS_DTYPE
    if value_kind == "effective_units":
        return EFFECTIVE_UNITS_DTYPE
    raise ValueError(f"Unsupported reachability value kind: {value_kind!r}")


def _cache_root(cache_name: str, cache_dir: Path) -> Path:
    return cache_dir / "reachability_arrays" / cache_name


def _manifest_path(root: Path) -> Path:
    return root / "manifest.json"


def _base_path(root: Path) -> Path:
    return root / "base.npz"


def _chunks_dir(root: Path) -> Path:
    return root / "chunks"


def _quarantine(path: Path) -> None:
    if not path.exists():
        return
    bad_path = path.with_suffix(path.suffix + ".bad")
    if bad_path.exists():
        bad_path = path.with_suffix(f"{path.suffix}.bad.{time.time_ns()}")
    try:
        path.rename(bad_path)
        print(f"  [reachability_cache] quarantined {path.name} -> {bad_path.name}")
    except OSError as exc:
        print(f"  [reachability_cache] could not quarantine {path.name} ({exc})")


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _atomic_write_matrix(path: Path, matrix: "ReachabilityMatrix") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp.open("wb") as handle:
            np.savez_compressed(
                handle,
                origin_ids=np.asarray(matrix.origin_ids, dtype=ORIGIN_DTYPE),
                matrix=np.asarray(matrix.matrix, dtype=matrix.matrix.dtype),
            )
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _normalize_categories(categories: Sequence[str] | Iterable[str]) -> tuple[str, ...]:
    return tuple(str(category) for category in categories)


def _normalize_origin_ids(origin_ids: Iterable[int] | np.ndarray) -> np.ndarray:
    raw = np.asarray(list(origin_ids) if not isinstance(origin_ids, np.ndarray) else origin_ids)
    if raw.ndim != 1:
        raise ValueError("origin_ids must be a one-dimensional array")
    if raw.size == 0:
        return np.asarray([], dtype=ORIGIN_DTYPE)
    if np.issubdtype(raw.dtype, np.signedinteger):
        if bool(np.any(raw < 0)):
            raise ValueError("origin_ids must be non-negative")
    elif not np.issubdtype(raw.dtype, np.unsignedinteger):
        raw = np.asarray([int(value) for value in raw], dtype=object)
        if any(int(value) < 0 for value in raw):
            raise ValueError("origin_ids must be non-negative")
    return np.asarray(raw, dtype=ORIGIN_DTYPE)


def _require_sorted_unique(origin_ids: np.ndarray) -> None:
    if origin_ids.size > 1 and bool(np.any(origin_ids[1:] <= origin_ids[:-1])):
        raise ValueError("origin_ids must be sorted ascending and unique")


@dataclass(frozen=True)
class ReachabilityMatrix:
    origin_ids: np.ndarray
    categories: tuple[str, ...]
    matrix: np.ndarray
    value_kind: str

    @classmethod
    def from_arrays(
        cls,
        origin_ids: Iterable[int] | np.ndarray,
        categories: Sequence[str] | Iterable[str],
        matrix: np.ndarray,
        *,
        value_kind: str,
        strict_dtype: bool = False,
    ) -> "ReachabilityMatrix":
        normalized_origin_ids = _normalize_origin_ids(origin_ids)
        _require_sorted_unique(normalized_origin_ids)
        normalized_categories = _normalize_categories(categories)
        expected_dtype = matrix_dtype_for_value_kind(value_kind)
        raw_matrix = np.asarray(matrix)
        if strict_dtype and raw_matrix.dtype != expected_dtype:
            raise ValueError(
                f"matrix dtype mismatch: expected {expected_dtype.str}, got {raw_matrix.dtype.str}"
            )
        normalized_matrix = np.asarray(raw_matrix, dtype=expected_dtype)
        expected_shape = (int(normalized_origin_ids.size), len(normalized_categories))
        if normalized_matrix.shape != expected_shape:
            raise ValueError(
                f"matrix shape mismatch: expected {expected_shape}, got {normalized_matrix.shape}"
            )
        return cls(
            origin_ids=normalized_origin_ids,
            categories=normalized_categories,
            matrix=normalized_matrix,
            value_kind=str(value_kind),
        )

    @classmethod
    def empty(
        cls,
        categories: Sequence[str] | Iterable[str],
        *,
        value_kind: str,
    ) -> "ReachabilityMatrix":
        normalized_categories = _normalize_categories(categories)
        matrix = np.zeros((0, len(normalized_categories)), dtype=matrix_dtype_for_value_kind(value_kind))
        return cls.from_arrays([], normalized_categories, matrix, value_kind=value_kind)

    @classmethod
    def for_origins(
        cls,
        origin_ids: Iterable[int] | np.ndarray,
        categories: Sequence[str] | Iterable[str],
        *,
        value_kind: str,
    ) -> "ReachabilityMatrix":
        normalized_origin_ids = _normalize_origin_ids(origin_ids)
        _require_sorted_unique(normalized_origin_ids)
        normalized_categories = _normalize_categories(categories)
        matrix = np.zeros(
            (int(normalized_origin_ids.size), len(normalized_categories)),
            dtype=matrix_dtype_for_value_kind(value_kind),
        )
        return cls.from_arrays(
            normalized_origin_ids,
            normalized_categories,
            matrix,
            value_kind=value_kind,
        )

    @classmethod
    def from_sparse_dict(
        cls,
        values_by_origin: dict[Any, dict[str, Any]],
        categories: Sequence[str] | Iterable[str],
        *,
        value_kind: str,
        strict_categories: bool = True,
    ) -> "ReachabilityMatrix":
        normalized_categories = _normalize_categories(categories)
        category_index = {category: index for index, category in enumerate(normalized_categories)}
        origin_values = sorted((int(origin), dict(values)) for origin, values in values_by_origin.items())
        origin_ids = [origin for origin, _ in origin_values]
        matrix = np.zeros(
            (len(origin_values), len(normalized_categories)),
            dtype=matrix_dtype_for_value_kind(value_kind),
        )
        for row_index, (_origin, row_values) in enumerate(origin_values):
            unknown = {str(category) for category in row_values if str(category) not in category_index}
            if strict_categories and unknown:
                raise ValueError(f"legacy reachability row has unknown categories: {sorted(unknown)!r}")
            for category, value in row_values.items():
                normalized_category = str(category)
                if normalized_category not in category_index:
                    continue
                column = category_index[normalized_category]
                if value_kind == "counts":
                    matrix[row_index, column] = max(int(value), 0)
                else:
                    matrix[row_index, column] = max(float(value), 0.0)
        return cls.from_arrays(origin_ids, normalized_categories, matrix, value_kind=value_kind)

    def missing_origin_ids(self, requested_origin_ids: Iterable[int] | np.ndarray) -> tuple[int, ...]:
        requested = np.unique(_normalize_origin_ids(requested_origin_ids))
        if requested.size == 0:
            return ()
        if self.origin_ids.size == 0:
            return tuple(int(value) for value in requested.tolist())
        positions = np.searchsorted(self.origin_ids, requested)
        present = (
            (positions < self.origin_ids.size)
            & (self.origin_ids[np.minimum(positions, self.origin_ids.size - 1)] == requested)
        )
        return tuple(int(value) for value in requested[~present].tolist())

    def row_index(self, origin_id: int) -> int | None:
        value = int(origin_id)
        if value < 0 or self.origin_ids.size == 0:
            return None
        origin_value = np.asarray(value, dtype=ORIGIN_DTYPE)
        index = int(np.searchsorted(self.origin_ids, origin_value))
        if index >= self.origin_ids.size or int(self.origin_ids[index]) != value:
            return None
        return index

    def get(self, origin_id: int, default=None):
        index = self.row_index(int(origin_id))
        if index is None:
            return default
        return self.sparse_row(index)

    def sparse_row(self, row_index: int) -> dict[str, int] | dict[str, float]:
        row = self.matrix[int(row_index)]
        if self.value_kind == "counts":
            return {
                category: int(row[index])
                for index, category in enumerate(self.categories)
                if int(row[index]) > 0
            }
        return {
            category: float(row[index])
            for index, category in enumerate(self.categories)
            if float(row[index]) > 0.0
        }

    def to_sparse_dict(self) -> dict[int, dict[str, int] | dict[str, float]]:
        return {
            int(origin_id): self.sparse_row(row_index)
            for row_index, origin_id in enumerate(self.origin_ids.tolist())
        }

    def to_lookup(self) -> "ReachabilityLookup":
        return ReachabilityLookup(self)

    def __contains__(self, origin_id: object) -> bool:
        try:
            return self.row_index(int(origin_id)) is not None
        except (TypeError, ValueError):
            return False

    def __len__(self) -> int:
        return int(self.origin_ids.size)


@dataclass(frozen=True)
class ReachabilityLookup:
    matrix: ReachabilityMatrix

    def get(self, origin_id: int, default=None):
        return self.matrix.get(origin_id, default)

    def __contains__(self, origin_id: object) -> bool:
        return origin_id in self.matrix

    def __len__(self) -> int:
        return len(self.matrix)


@dataclass(frozen=True)
class ReachabilityCacheState:
    matrix: ReachabilityMatrix
    is_complete: bool


def merge_reachability_matrices(
    matrices: Sequence[ReachabilityMatrix],
    *,
    categories: Sequence[str] | Iterable[str],
    value_kind: str,
) -> ReachabilityMatrix:
    normalized_categories = _normalize_categories(categories)
    non_empty = [matrix for matrix in matrices if len(matrix) > 0]
    for matrix in matrices:
        if matrix.categories != normalized_categories:
            raise ValueError("Cannot merge reachability matrices with different categories")
        if matrix.value_kind != value_kind:
            raise ValueError("Cannot merge reachability matrices with different value kinds")
    if not non_empty:
        return ReachabilityMatrix.empty(normalized_categories, value_kind=value_kind)

    origin_ids = np.concatenate([matrix.origin_ids for matrix in non_empty]).astype(ORIGIN_DTYPE, copy=False)
    values = np.concatenate([matrix.matrix for matrix in non_empty], axis=0)
    if origin_ids.size == 0:
        return ReachabilityMatrix.empty(normalized_categories, value_kind=value_kind)

    positions = np.arange(origin_ids.size, dtype=np.int64)
    order = np.lexsort((-positions, origin_ids))
    sorted_origin_ids = origin_ids[order]
    sorted_values = values[order]
    keep = np.ones(sorted_origin_ids.size, dtype=bool)
    keep[1:] = sorted_origin_ids[1:] != sorted_origin_ids[:-1]
    return ReachabilityMatrix.from_arrays(
        sorted_origin_ids[keep],
        normalized_categories,
        sorted_values[keep],
        value_kind=value_kind,
    )


def _manifest_payload(
    *,
    cache_name: str,
    value_kind: str,
    categories: tuple[str, ...],
    status: str,
    chunks: list[str],
    legacy_migrated_from: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "format": FORMAT_NAME,
        "version": FORMAT_VERSION,
        "cache_name": str(cache_name),
        "value_kind": str(value_kind),
        "origin_dtype": ORIGIN_DTYPE.str,
        "matrix_dtype": matrix_dtype_for_value_kind(value_kind).str,
        "categories": list(categories),
        "category_count": len(categories),
        "status": str(status),
        "chunks": list(chunks),
        "updated_at": time.time(),
    }
    if legacy_migrated_from is not None:
        payload["legacy_migrated_from"] = legacy_migrated_from
    return payload


def _read_manifest(root: Path) -> dict[str, Any] | None:
    path = _manifest_path(root)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  [reachability_cache] {root.name}/manifest.json invalid ({type(exc).__name__})")
        _quarantine(path)
        return None


def _validate_manifest(
    manifest: dict[str, Any],
    *,
    cache_name: str,
    categories: tuple[str, ...] | None,
    value_kind: str | None,
) -> bool:
    try:
        version = int(manifest.get("version", -1))
        category_count = int(manifest.get("category_count", -1))
    except (TypeError, ValueError):
        return False
    if manifest.get("format") != FORMAT_NAME or version != FORMAT_VERSION:
        return False
    if str(manifest.get("cache_name") or "") != str(cache_name):
        return False
    if value_kind is not None and str(manifest.get("value_kind") or "") != str(value_kind):
        return False
    manifest_value_kind = str(manifest.get("value_kind") or "")
    try:
        expected_matrix_dtype = matrix_dtype_for_value_kind(manifest_value_kind).str
    except ValueError:
        return False
    if manifest.get("origin_dtype") != ORIGIN_DTYPE.str:
        return False
    if manifest.get("matrix_dtype") != expected_matrix_dtype:
        return False
    manifest_categories = tuple(str(category) for category in manifest.get("categories") or [])
    if category_count != len(manifest_categories):
        return False
    if categories is not None and manifest_categories != categories:
        return False
    if str(manifest.get("status") or "") not in {"building", "complete"}:
        return False
    if not isinstance(manifest.get("chunks", []), list):
        return False
    return True


def _load_npz_matrix(
    path: Path,
    *,
    categories: tuple[str, ...],
    value_kind: str,
    quarantine_bad: bool,
) -> ReachabilityMatrix | None:
    if not path.exists():
        return None
    try:
        with np.load(path, allow_pickle=False) as data:
            if set(data.files) != {"origin_ids", "matrix"}:
                raise ValueError(f"unexpected fields: {data.files!r}")
            origin_ids = np.asarray(data["origin_ids"])
            matrix = np.asarray(data["matrix"])
        if origin_ids.dtype != ORIGIN_DTYPE:
            raise ValueError(f"origin_ids dtype mismatch: {origin_ids.dtype.str}")
        return ReachabilityMatrix.from_arrays(
            origin_ids,
            categories,
            matrix,
            value_kind=value_kind,
            strict_dtype=True,
        )
    except Exception as exc:
        print(f"  [reachability_cache] {path.name} invalid ({type(exc).__name__})")
        if quarantine_bad:
            _quarantine(path)
        return None


def load_reachability_cache(
    cache_name: str,
    cache_dir: Path,
    *,
    categories: Sequence[str] | Iterable[str],
    value_kind: str,
) -> ReachabilityCacheState | None:
    root = _cache_root(cache_name, cache_dir)
    manifest = _read_manifest(root)
    normalized_categories = _normalize_categories(categories)
    if manifest is None:
        return None
    if not _validate_manifest(
        manifest,
        cache_name=cache_name,
        categories=normalized_categories,
        value_kind=value_kind,
    ):
        print(f"  [reachability_cache] {cache_name}: manifest mismatch - will rebuild")
        return None

    base = _load_npz_matrix(
        _base_path(root),
        categories=normalized_categories,
        value_kind=value_kind,
        quarantine_bad=True,
    )
    if manifest.get("status") == "complete" and base is None:
        return None

    matrices: list[ReachabilityMatrix] = []
    if base is not None:
        matrices.append(base)

    for chunk_name in manifest.get("chunks", []):
        chunk_path = _chunks_dir(root) / str(chunk_name)
        chunk = _load_npz_matrix(
            chunk_path,
            categories=normalized_categories,
            value_kind=value_kind,
            quarantine_bad=True,
        )
        if chunk is not None:
            matrices.append(chunk)

    if not matrices:
        matrix = ReachabilityMatrix.empty(normalized_categories, value_kind=value_kind)
    else:
        matrix = merge_reachability_matrices(
            matrices,
            categories=normalized_categories,
            value_kind=value_kind,
        )
    return ReachabilityCacheState(
        matrix=matrix,
        is_complete=str(manifest.get("status") or "") == "complete",
    )


def reset_reachability_cache(cache_name: str, cache_dir: Path) -> None:
    root = _cache_root(cache_name, cache_dir)
    _manifest_path(root).unlink(missing_ok=True)
    _base_path(root).unlink(missing_ok=True)
    chunks = _chunks_dir(root)
    if chunks.exists():
        for child in chunks.iterdir():
            if child.is_file():
                child.unlink(missing_ok=True)


def save_reachability_cache(
    cache_name: str,
    cache_dir: Path,
    matrix: ReachabilityMatrix,
    *,
    legacy_migrated_from: str | None = None,
) -> None:
    root = _cache_root(cache_name, cache_dir)
    _atomic_write_matrix(_base_path(root), matrix)
    _atomic_write_json(
        _manifest_path(root),
        _manifest_payload(
            cache_name=cache_name,
            value_kind=matrix.value_kind,
            categories=matrix.categories,
            status="complete",
            chunks=[],
            legacy_migrated_from=legacy_migrated_from,
        ),
    )
    chunks = _chunks_dir(root)
    if chunks.exists():
        for child in chunks.iterdir():
            if child.is_file() and child.suffix == ".npz":
                child.unlink(missing_ok=True)


def append_reachability_cache_chunk(
    cache_name: str,
    cache_dir: Path,
    matrix: ReachabilityMatrix,
) -> None:
    root = _cache_root(cache_name, cache_dir)
    manifest = _read_manifest(root)
    if manifest is None:
        chunks: list[str] = []
    else:
        if not _validate_manifest(
            manifest,
            cache_name=cache_name,
            categories=matrix.categories,
            value_kind=matrix.value_kind,
        ):
            raise ValueError(f"Cannot append reachability chunk to mismatched cache {cache_name!r}")
        chunks = [str(value) for value in manifest.get("chunks", [])]

    chunk_name = f"chunk_{len(chunks) + 1:06d}.npz"
    _atomic_write_matrix(_chunks_dir(root) / chunk_name, matrix)
    chunks.append(chunk_name)
    _atomic_write_json(
        _manifest_path(root),
        _manifest_payload(
            cache_name=cache_name,
            value_kind=matrix.value_kind,
            categories=matrix.categories,
            status="building",
            chunks=chunks,
        ),
    )


def reachability_cache_ready(
    cache_name: str,
    cache_dir: Path,
    *,
    categories: Sequence[str] | Iterable[str] | None = None,
    value_kind: str | None = None,
) -> bool:
    root = _cache_root(cache_name, cache_dir)
    manifest = _read_manifest(root)
    normalized_categories = None if categories is None else _normalize_categories(categories)
    if manifest is None:
        return False
    if not _validate_manifest(
        manifest,
        cache_name=cache_name,
        categories=normalized_categories,
        value_kind=value_kind,
    ):
        return False
    return str(manifest.get("status") or "") == "complete" and _base_path(root).exists()


def reachability_cache_recoverable(cache_name: str, cache_dir: Path) -> bool:
    root = _cache_root(cache_name, cache_dir)
    manifest = _read_manifest(root)
    if manifest is None:
        return False
    if not _validate_manifest(manifest, cache_name=cache_name, categories=None, value_kind=None):
        return False
    if _base_path(root).exists():
        return True
    return any((_chunks_dir(root) / str(chunk)).exists() for chunk in manifest.get("chunks", []))


def migrate_legacy_sparse_cache(
    cache_name: str,
    cache_dir: Path,
    *,
    categories: Sequence[str] | Iterable[str],
    value_kind: str,
    legacy_cache_load_large,
) -> ReachabilityMatrix | None:
    legacy = legacy_cache_load_large(cache_name, cache_dir)
    if legacy is None:
        return None
    if not isinstance(legacy, dict):
        return None
    matrix = ReachabilityMatrix.from_sparse_dict(
        legacy,
        categories,
        value_kind=value_kind,
        strict_categories=True,
    )
    save_reachability_cache(
        cache_name,
        cache_dir,
        matrix,
        legacy_migrated_from="pickle_large_v0",
    )
    return matrix
