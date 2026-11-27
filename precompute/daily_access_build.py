"""Daily access score (v4) build: canonical 50 m shell/reach/score artifacts.

Runs next to the v3 pipeline; ``--publish`` (``daily_access_publish``) is the
separate step that turns complete tiers into the active map build:

    python main.py daily-access --profile test [--publish]

Tiers under ``.livability_cache/daily_access/`` (``manifest.json`` goes
``building`` -> ``complete``; a tier is reused only when complete, of the
same versions and with every shard present and matching its checksum):

* ``graph_<key>/``  walk graph format 4, profile bbox + reach buffer + snap radius;
* ``shell_<key>/``  per 20 km shard: land area, validity, cell origin + flags,
  network-entry flag, the edge endpoint nodes the shard's cells need;
* ``reach_<key>/``  objects (native records), per service kind uint16 0.1 m
  distances + uint32 logical ids for every used node, the label table
  (``uint32 -> logical key -> source records``) and the loss ledger;
* ``score_<key>/`` per shard: total, four components, five penalties and two
  bonuses (uint16, 0.01), six access values, network status. Walk shards
  additionally own the common context artifact and source diagnostics.

Keys: geometry/reach algorithm, classification/dedupe and the score model
are versioned separately; weights or curves only change the score key.
Heavy work runs in ``walkgraph daily-access-*``; temporary arrays scale with
``DAILY_ACCESS_SCORE_BLOCK_CELLS``, never with the country.
"""

from __future__ import annotations

import gc
import hashlib
import json
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import shapely
from shapely.geometry import box as geometry_box
from shapely.ops import transform

from config import (
    CACHE_DIR,
    CANONICAL_BASE_RESOLUTION_M,
    DAILY_ACCESS_ACCESS_STEP,
    DAILY_ACCESS_CLASSIFICATION_VERSION,
    DAILY_ACCESS_DISTANCE_STEP_M,
    DAILY_ACCESS_GEOMETRY_VERSION,
    DAILY_ACCESS_DEFAULT_MODE,
    DAILY_ACCESS_MODES,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_REACH_BUFFER_M,
    DAILY_ACCESS_REACH_CACHE_VERSION,
    DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE,
    DAILY_ACCESS_SCORE_BLOCK_CELLS,
    DAILY_ACCESS_SCORE_MODEL,
    DAILY_ACCESS_SCORE_MODEL_VERSION,
    DAILY_ACCESS_SCORE_SHARD_VERSION,
    DAILY_ACCESS_SCORE_STEP,
    DAILY_ACCESS_SHELL_SHARD_VERSION,
    DAILY_ACCESS_SNAP_RADIUS_M,
    DAILY_ACCESS_UINT16_MISSING,
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    OSM_EXTRACT_PATH,
    RAILWAY_PROXIMITY_ACTIVE_MODES,
    RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
    RAILWAY_PROXIMITY_MAX_PENALTY,
    RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
    ROAD_PROXIMITY_ALGORITHM_VERSION,
    ROAD_PROXIMITY_CLASS_SETTINGS,
    ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER,
    ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER,
    SURFACE_SHARD_SIZE_M,
    TO_TARGET,
    TO_WGS84,
    WALK_ROUTING_BACKEND,
    WALKGRAPH_BIN,
    WALKGRAPH_FORMAT_VERSION,
    daily_access_classification_params,
    daily_access_geometry_params,
    daily_access_mode_aware_params,
    daily_access_score_params,
    hash_dict,
    normalize_build_profile,
)

from .daily_access_classify import build_logical_places, classify_service_rows
from .daily_access_native import (
    ALL_KINDS,
    CELL_DTYPE,
    CELL_FLAG_ON_LAND,
    INTERVAL_DTYPE,
    KIND_INDEX,
    NATIVE_PEAK_BYTES,
    OBJECT_DTYPE,
    ObjectFiles,
    block_reach_files,
    cell_records,
    merge_area_seeds,
    native_attach,
    native_objects,
    native_reach,
    native_score,
    process_peak_bytes,
    write_mask_geojson,
    write_objects,
)
from .daily_access_reference import (
    NETWORK_STATUS_CODES,
    TransportStop,
    build_sources,
    loss_report,
    service_kind_k,
    service_kind_need,
    service_kind_quality,
    service_kind_cutoff,
)
from .daily_access_score import CATEGORIES, PENALTIES, BONUSES, SCORE_CHANNEL_SCHEMA
from .daily_access_context import METRICS, load_context_field
from .daily_access_snap import LandMask
from .surface import ShardEntry, iter_shard_entries

DAILY_ACCESS_CACHE_DIR = CACHE_DIR / "daily_access"
CELL_AREA_M2 = float(CANONICAL_BASE_RESOLUTION_M * CANONICAL_BASE_RESOLUTION_M)
_MAX_CELLS_PER_NATIVE_CALL = 8_000_000
SHELL_FIELDS = (
    "land_area_m2", "valid_land_mask", "origin_x", "origin_y", "origin_flags",
    "entry_flags", "used_nodes", "x_min_m", "y_min_m",
)
SCORE_FIELDS = ("total", "components", "penalties", "bonuses", "service_access", "network_status", "x_min_m", "y_min_m")
CONTEXT_FIELDS = ("context_values", "context_metrics", "pitch_ref", "pitch_reason", "pitch_distance", "station_labels", "station_distances")
QUANTIZATION = {
    "distance_step_m": DAILY_ACCESS_DISTANCE_STEP_M,
    "score_step": DAILY_ACCESS_SCORE_STEP,
    "access_step": DAILY_ACCESS_ACCESS_STEP,
    "uint16_missing": DAILY_ACCESS_UINT16_MISSING,
    "label_dtype": "uint32",
}

ProgressFn = Callable[[str], None]


def _print_progress(message: str) -> None:
    print(message, flush=True)


def _walkgraph_progress(progress: ProgressFn):
    def emit(_event: str, *, detail: str, **_kwargs) -> None:
        progress(detail)

    return emit


class DailyAccessBuildError(RuntimeError):
    pass


def ensure_supported_routing_backend(backend: str | None = None) -> None:
    """v4 reach is walkgraph-only; fail before any heavy phase."""
    backend = WALK_ROUTING_BACKEND if backend is None else backend
    if str(backend) != "walkgraph":
        raise DailyAccessBuildError(
            f"WALK_ROUTING_BACKEND={backend!r} is not supported by the daily access "
            "score (v4); unset it or set WALK_ROUTING_BACKEND=walkgraph."
        )


def model_versions() -> dict[str, Any]:
    return {
        "model": DAILY_ACCESS_SCORE_MODEL,
        "score_model_version": DAILY_ACCESS_SCORE_MODEL_VERSION,
        "geometry_version": DAILY_ACCESS_GEOMETRY_VERSION,
        "classification_version": DAILY_ACCESS_CLASSIFICATION_VERSION,
        "shell_shard_version": DAILY_ACCESS_SHELL_SHARD_VERSION,
        "reach_cache_version": DAILY_ACCESS_REACH_CACHE_VERSION,
        "score_shard_version": DAILY_ACCESS_SCORE_SHARD_VERSION,
    }


def penalty_params() -> dict[str, Any]:
    """Penalty formulas (score key): they are read at the cell origin."""
    from config import DAILY_ACCESS_CONTEXT_PARAMS
    return {
        "context": DAILY_ACCESS_CONTEXT_PARAMS,
        "channels": SCORE_CHANNEL_SCHEMA,
        "location": "cell_origin",
        "road_algorithm_version": ROAD_PROXIMITY_ALGORITHM_VERSION,
        "road_class_settings": ROAD_PROXIMITY_CLASS_SETTINGS,
        "road_speed_multiplier": [ROAD_PROXIMITY_MIN_SPEED_MULTIPLIER, ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER],
        "railway": [
            RAILWAY_PROXIMITY_FULL_PENALTY_DISTANCE_M,
            RAILWAY_PROXIMITY_ZERO_PENALTY_DISTANCE_M,
            RAILWAY_PROXIMITY_MAX_PENALTY,
            sorted(RAILWAY_PROXIMITY_ACTIVE_MODES),
        ],
    }


@dataclass(frozen=True)
class TierKeys:
    graph: str
    shell: str
    reach: str
    score: str


def tier_keys(
    *,
    extract_fingerprint: str,
    graph_bbox: tuple[float, float, float, float],
    graph_padding_m: float,
    geo_hash: str,
    mask_identity: str,
    reach_inputs: str,
    score_inputs: str | None = None,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
) -> TierKeys:
    """Chained keys: shell <- geometry + mask, reach <- shell + inputs, score <- reach + model.

    ``reach_inputs`` identifies objects, green grouping and penalty data
    (import / transit / Overture state and classification). ``score_inputs``
    defaults to the current weights, curves and penalty formulas.
    """
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in DAILY_ACCESS_MODES:
        raise ValueError(f"Unsupported daily access mode: {mode!r}")
    graph_payload = {
            "extract_fingerprint": extract_fingerprint,
            "bbox": [round(value, 8) for value in graph_bbox],
            "padding_m": graph_padding_m,
            "format_version": WALKGRAPH_FORMAT_VERSION,
    }
    if normalized_mode != DAILY_ACCESS_DEFAULT_MODE:
        graph_payload["graph_profile"] = normalized_mode
    graph = hash_dict(graph_payload)
    geometry = daily_access_geometry_params()
    shell = hash_dict(
        {
            "geo_hash": geo_hash,
            "graph": graph,
            "mask": mask_identity,
            "geometry": geometry,
            "shard_size_m": SURFACE_SHARD_SIZE_M,
            "resolution_m": CANONICAL_BASE_RESOLUTION_M,
            "version": DAILY_ACCESS_SHELL_SHARD_VERSION,
        }
    )
    reach = hash_dict(
        {
            "inputs": reach_inputs,
            "shell": shell,
            "geometry": geometry,
            "classification": daily_access_classification_params(),
            **(
                {
                    "mode": normalized_mode,
                    "search_cutoff_m": DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE[normalized_mode],
                }
                if normalized_mode != DAILY_ACCESS_DEFAULT_MODE
                else {}
            ),
            "version": DAILY_ACCESS_REACH_CACHE_VERSION,
        }
    )
    if score_inputs is None:
        score_inputs = hash_dict(
            {"score": daily_access_score_params(normalized_mode), "penalties": penalty_params()}
        )
    score = hash_dict({"inputs": score_inputs, "reach": reach, "version": DAILY_ACCESS_SCORE_SHARD_VERSION})
    return TierKeys(graph=graph, shell=shell, reach=reach, score=score)


def mask_identity(land_mask: LandMask) -> str:
    return hashlib.sha256(shapely.to_wkb(land_mask.geometry, output_dimension=2)).hexdigest()[:16]


# --- manifests --------------------------------------------------------------


def _read_manifest(directory: Path) -> dict[str, Any] | None:
    try:
        return json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_manifest(directory: Path, payload: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    temp = directory / "manifest.json.tmp"
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    temp.replace(directory / "manifest.json")


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()[:16]


def begin_tier(directory: Path, tier: str, key: str, extra: dict[str, Any]) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    _write_manifest(directory, {"status": "building", "tier": tier, "key": key, **model_versions(),
                                **({"channel_schema": SCORE_CHANNEL_SCHEMA, "context_source_score_key": key} if tier == "score" else {}), **extra})


def complete_tier(
    directory: Path,
    payload: dict[str, Any],
    *,
    shard_dir: str | None = "shards",
    inventory_files: bool = False,
) -> None:
    """``building`` -> ``complete`` with checksums for its immutable files."""
    manifest = _read_manifest(directory) or {}
    if shard_dir is not None:
        manifest["shard_files"] = {
            path.name: _file_digest(path) for path in sorted((directory / shard_dir).glob("*.npz"))
        }
    if inventory_files:
        manifest["artifact_files"] = {
            path.relative_to(directory).as_posix(): _file_digest(path)
            for path in sorted(directory.rglob("*"))
            if path.is_file() and path.name != "manifest.json"
        }
    manifest.update(payload)
    manifest["status"] = "complete"
    manifest["completed_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_manifest(directory, manifest)


def tier_problems(
    directory: Path,
    *,
    key: str | None = None,
    shard_ids: Iterable[str] | None = None,
    fields: Iterable[str] = (),
    verify_checksums: bool = True,
) -> list[str]:
    """Why a tier cannot be used (empty list = usable).

    Incomplete, of another version or key, missing or corrupt shard files all
    disqualify it: a build is never declared ready over a broken shard.
    """
    manifest = _read_manifest(directory)
    if manifest is None:
        return ["no manifest"]
    problems = []
    if manifest.get("status") != "complete":
        problems.append(f"status {manifest.get('status')!r}")
    if key is not None and manifest.get("key") != key:
        problems.append("key mismatch")
    if manifest.get("tier") == "score" and manifest.get("channel_schema") != SCORE_CHANNEL_SCHEMA:
        problems.append("score channel schema mismatch")
    tier_version_fields = {
        "shell": ("model", "geometry_version", "shell_shard_version"),
        "reach": (
            "model", "geometry_version", "classification_version",
            "shell_shard_version", "reach_cache_version",
        ),
        "score": tuple(model_versions()),
    }
    version_fields = tier_version_fields.get(str(manifest.get("tier")), tuple(model_versions()))
    versions = model_versions()
    for name in version_fields:
        value = versions[name]
        if manifest.get(name) != value:
            problems.append(f"{name} {manifest.get(name)!r} != {value!r}")
    if shard_ids is None:
        return problems
    if manifest.get("tier") == "score" and manifest.get("mode", "walk") == "walk" and set(SCORE_FIELDS).issubset(fields):
        fields = (*fields, *CONTEXT_FIELDS)
    recorded = manifest.get("shard_files") or {}
    for shard_id in shard_ids:
        path = directory / "shards" / f"{shard_id}.npz"
        if not path.exists():
            problems.append(f"missing shard {shard_id}")
            continue
        if verify_checksums and recorded.get(path.name) != _file_digest(path):
            problems.append(f"checksum mismatch {shard_id}")
            continue
        try:
            with np.load(path, allow_pickle=False) as data:
                missing = [field for field in fields if field not in data.files]
        except Exception as exc:  # noqa: BLE001 - any read failure disqualifies the shard
            problems.append(f"unreadable shard {shard_id}: {exc}")
            continue
        if missing:
            problems.append(f"shard {shard_id} lacks {missing}")
    return problems


def surface_tier_problems(
    shell_dir: Path,
    score_dir: Path,
    *,
    shell_key: str | None = None,
    reach_key: str | None = None,
    score_key: str | None = None,
    verify_checksums: bool = True,
) -> tuple[list[str], list[str]]:
    """Validate a complete shell/score snapshot without enumerating its cells.

    The manifests contain one id and checksum per 20 km shard.  Reading that
    bounded inventory is enough to validate every file; the verifier must not
    first build a Python list containing every 50 m land cell in the country.
    """
    shell_manifest = _read_manifest(shell_dir)
    score_manifest = _read_manifest(score_dir)
    problems: list[str] = []
    if shell_manifest is None:
        return ["shell: no manifest"], []
    if score_manifest is None:
        return ["score: no manifest"], []

    def inventory(name: str, manifest: dict[str, Any]) -> list[str]:
        value = manifest.get("shards")
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            problems.append(f"{name}: manifest has no valid shard inventory")
            return []
        if len(value) != len(set(value)):
            problems.append(f"{name}: manifest shard inventory contains duplicates")
        return list(value)

    shell_shards = inventory("shell", shell_manifest)
    score_shards = inventory("score", score_manifest)
    if shell_shards != score_shards:
        problems.append("surface: shell and score shard inventories differ")
    shard_ids = shell_shards if shell_shards == score_shards else []

    for name, manifest, expected in (
        ("shell", shell_manifest, shell_shards),
        ("score", score_manifest, score_shards),
    ):
        recorded = manifest.get("shard_files")
        expected_files = {f"{shard_id}.npz" for shard_id in expected}
        if not isinstance(recorded, dict) or set(recorded) != expected_files:
            problems.append(f"{name}: checksum inventory does not match shard inventory")

    linked_shell_key = score_manifest.get("shell_key")
    actual_shell_key = shell_manifest.get("key")
    if linked_shell_key != actual_shell_key:
        problems.append("surface: score manifest links to another shell key")
    if reach_key is not None and score_manifest.get("reach_key") != reach_key:
        problems.append("surface: score manifest links to another reach key")
    common_fields = (
        "profile", "resolution_m", "shard_size_m", "mask_identity",
        "quantization", "study_area_bounds_metric", "graph_bbox_wgs84",
        "graph_padding_m", "reach_buffer_m",
    )
    for field in common_fields:
        if field not in shell_manifest or field not in score_manifest:
            problems.append(f"surface: required manifest field {field!r} is missing")
            continue
        if shell_manifest.get(field) != score_manifest.get(field):
            problems.append(f"surface: shell/score {field} mismatch")

    problems.extend(
        f"shell: {problem}" for problem in tier_problems(
            shell_dir, key=shell_key, shard_ids=shell_shards, fields=SHELL_FIELDS,
            verify_checksums=verify_checksums,
        )
    )
    problems.extend(
        f"score: {problem}" for problem in tier_problems(
            score_dir, key=score_key, shard_ids=score_shards, fields=SCORE_FIELDS,
            verify_checksums=verify_checksums,
        )
    )

    for shard_id in shard_ids:
        try:
            shell = load_shell_shard(shell_dir, shard_id)
            score = load_score_shard(score_dir, shard_id)
        except Exception as exc:  # noqa: BLE001 - corrupt data is a verification result
            problems.append(f"surface: unreadable paired shard {shard_id}: {exc}")
            continue
        context_required = score_manifest.get("mode", "walk") == "walk"
        missing_fields = set((*SCORE_FIELDS, *(CONTEXT_FIELDS if context_required else ()))) - set(score)
        if missing_fields:
            problems.append(f"score: shard {shard_id} lacks {sorted(missing_fields)}")
            continue
        shape = shell["valid_land_mask"].shape
        resolution_m = int(shell_manifest.get("resolution_m") or 0)
        shard_size_m = int(shell_manifest.get("shard_size_m") or 0)
        expected_side = shard_size_m // resolution_m if resolution_m and shard_size_m % resolution_m == 0 else 0
        if not expected_side or shape != (expected_side, expected_side):
            problems.append(f"surface: shard {shard_id} shape {shape} does not match manifest geometry")
        shell_shapes = {
            field: shell[field].shape
            for field in ("land_area_m2", "origin_x", "origin_y", "origin_flags", "entry_flags")
        }
        expected_score_shapes = {
            "total": shape,
            "components": (len(CATEGORIES), *shape),
            "penalties": (len(PENALTIES), *shape),
            "bonuses": (len(BONUSES), *shape),
            "service_access": (len(DAILY_ACCESS_NEEDS), *shape),
            "network_status": shape,
            "context_values": (*shape, 5),
            "context_metrics": (*shape, len(METRICS)),
            "pitch_ref": shape, "pitch_reason": shape, "pitch_distance": shape,
            "station_labels": (2, *shape), "station_distances": (2, *shape),
        }
        if not context_required:
            expected_score_shapes = {key: value for key, value in expected_score_shapes.items() if key not in CONTEXT_FIELDS}
        if any(value != shape for value in shell_shapes.values()):
            problems.append(f"shell: shard {shard_id} has incompatible array dimensions")
        if any(score[field].shape != expected for field, expected in expected_score_shapes.items()):
            problems.append(f"score: shard {shard_id} has incompatible array dimensions")
        if shell["used_nodes"].ndim != 1:
            problems.append(f"shell: shard {shard_id} used_nodes is not one-dimensional")
        expected_dtypes = {
            "shell.land_area_m2": (shell["land_area_m2"].dtype, np.dtype("<f4")),
            "shell.valid_land_mask": (shell["valid_land_mask"].dtype, np.dtype("?")),
            "shell.origin_x": (shell["origin_x"].dtype, np.dtype("<f8")),
            "shell.origin_y": (shell["origin_y"].dtype, np.dtype("<f8")),
            "shell.origin_flags": (shell["origin_flags"].dtype, np.dtype("u1")),
            "shell.entry_flags": (shell["entry_flags"].dtype, np.dtype("u1")),
            "shell.used_nodes": (shell["used_nodes"].dtype, np.dtype("<u4")),
            "score.total": (score["total"].dtype, np.dtype("<u2")),
            "score.components": (score["components"].dtype, np.dtype("<u2")),
            "score.penalties": (score["penalties"].dtype, np.dtype("<u2")),
            "score.bonuses": (score["bonuses"].dtype, np.dtype("<u2")),
            "score.service_access": (score["service_access"].dtype, np.dtype("<u2")),
            "score.network_status": (score["network_status"].dtype, np.dtype("u1")),
        }
        for field, (actual, expected) in expected_dtypes.items():
            if actual != expected:
                problems.append(f"surface: shard {shard_id} {field} dtype {actual} != {expected}")
        shell_origin = (int(shell["x_min_m"][0]), int(shell["y_min_m"][0]))
        score_origin = (int(score["x_min_m"][0]), int(score["y_min_m"][0]))
        try:
            id_origin = tuple(int(value) for value in shard_id.split("_", 1))
        except ValueError:
            problems.append(f"surface: invalid shard id {shard_id!r}")
            continue
        if shell_origin != id_origin or score_origin != id_origin:
            problems.append(f"surface: shard {shard_id} origin does not match its id")
    return problems, shard_ids


def reach_tier_problems(
    directory: Path,
    *,
    key: str | None = None,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    verify_checksums: bool = True,
) -> list[str]:
    """Validate the complete immutable reach cache and its array contracts."""
    problems = [f"manifest: {problem}" for problem in tier_problems(directory, key=key)]
    manifest = _read_manifest(directory)
    if manifest is None:
        return problems
    recorded = manifest.get("artifact_files")
    if not isinstance(recorded, dict) or not all(
        isinstance(name, str) and isinstance(digest, str) for name, digest in recorded.items()
    ):
        problems.append("manifest has no valid reach artifact inventory")
        return problems

    actual = {
        path.relative_to(directory).as_posix(): path
        for path in directory.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if set(actual) != set(recorded):
        missing = sorted(set(recorded) - set(actual))
        unexpected = sorted(set(actual) - set(recorded))
        if missing:
            problems.append(f"missing reach artifacts {missing[:5]}")
        if unexpected:
            problems.append(f"unrecorded reach artifacts {unexpected[:5]}")
    if verify_checksums:
        for name in sorted(set(actual) & set(recorded)):
            if _file_digest(actual[name]) != recorded[name]:
                problems.append(f"checksum mismatch {name}")

    required = {
        "origins.npy", "kinds.json", "labels.json",
        "objects/objects.bin", "objects/ring_offsets.bin",
        "objects/ring_vertices.bin", "objects/park_intervals.bin",
    }
    absent = sorted(required - set(recorded))
    if absent:
        problems.append(f"reach inventory lacks required artifacts {absent}")
        return problems
    try:
        kinds = json.loads((directory / "kinds.json").read_text(encoding="utf-8"))
        labels = json.loads((directory / "labels.json").read_text(encoding="utf-8"))
        if not isinstance(kinds, list) or not isinstance(labels, dict):
            raise ValueError("kinds.json must be a list and labels.json must be an object")
        names = [str(item["kind"]) for item in kinds]
        if len(names) != len(set(names)):
            problems.append("kinds.json contains duplicate service kinds")
        unknown_kinds = sorted(set(names) - set(ALL_KINDS))
        if unknown_kinds:
            problems.append(f"kinds.json contains unknown service kinds {unknown_kinds}")
        origins = np.load(directory / "origins.npy", mmap_mode="r", allow_pickle=False)
        if origins.ndim != 1 or origins.dtype != np.dtype("<u4"):
            problems.append(f"origins.npy has shape/dtype {origins.shape}/{origins.dtype}, expected 1-D uint32")
        if manifest.get("origins") is not None and int(manifest["origins"]) != len(origins):
            problems.append("origins.npy length does not match manifest origins")
        for item in kinds:
            required_kind_fields = {"kind", "kind_index", "need", "quality", "k", "cutoff_m"}
            if not isinstance(item, dict) or not required_kind_fields.issubset(item):
                problems.append("kinds.json entry lacks required metadata fields")
                continue
            kind = str(item["kind"])
            if kind not in ALL_KINDS:
                continue
            k = int(item["k"])
            need = service_kind_need(kind)
            expected_metadata = (
                KIND_INDEX[kind], need, service_kind_quality(kind), service_kind_k(kind),
                service_kind_cutoff(kind, mode),
            )
            actual_metadata = (
                int(item["kind_index"]), str(item["need"]), float(item["quality"]), k, float(item["cutoff_m"]),
            )
            if actual_metadata != expected_metadata:
                problems.append(f"kind {kind} metadata does not match the current reach contract")
            if k <= 0:
                problems.append(f"kind {kind} has invalid k={k}")
                continue
            distance_name = f"{kind}.distances.npy"
            label_name = f"{kind}.labels.npy"
            if distance_name not in recorded or label_name not in recorded:
                problems.append(f"reach inventory lacks arrays for {kind}")
                continue
            distances = np.load(directory / distance_name, mmap_mode="r", allow_pickle=False)
            kind_labels = np.load(directory / label_name, mmap_mode="r", allow_pickle=False)
            expected_shape = (len(origins), k)
            if distances.shape != expected_shape or distances.dtype != np.dtype("<u2"):
                problems.append(
                    f"{distance_name} has shape/dtype {distances.shape}/{distances.dtype}, "
                    f"expected {expected_shape}/uint16"
                )
            if kind_labels.shape != expected_shape or kind_labels.dtype != np.dtype("<u4"):
                problems.append(
                    f"{label_name} has shape/dtype {kind_labels.shape}/{kind_labels.dtype}, "
                    f"expected {expected_shape}/uint32"
                )
    except Exception as exc:  # noqa: BLE001 - any parse/read error disqualifies the cache
        problems.append(f"unreadable reach metadata/arrays: {exc}")

    structural_files = {
        "objects/objects.bin": OBJECT_DTYPE.itemsize,
        "objects/ring_offsets.bin": np.dtype("<u8").itemsize,
        "objects/ring_vertices.bin": 2 * np.dtype("<f8").itemsize,
        "objects/park_intervals.bin": INTERVAL_DTYPE.itemsize,
    }
    for name, itemsize in structural_files.items():
        path = actual.get(name)
        if path is not None and path.stat().st_size % itemsize:
            problems.append(f"{name} size is not a multiple of {itemsize}")
    offsets = actual.get("objects/ring_offsets.bin")
    if offsets is not None and offsets.stat().st_size < np.dtype("<u8").itemsize:
        problems.append("objects/ring_offsets.bin has no initial offset")
    objects = actual.get("objects/objects.bin")
    if objects is not None and manifest.get("objects") is not None:
        if objects.stat().st_size // OBJECT_DTYPE.itemsize != int(manifest["objects"]):
            problems.append("objects/objects.bin count does not match manifest objects")
    try:
        if all(name in actual for name in structural_files):
            object_rows = np.memmap(actual["objects/objects.bin"], dtype=OBJECT_DTYPE, mode="r") \
                if actual["objects/objects.bin"].stat().st_size else np.zeros(0, dtype=OBJECT_DTYPE)
            ring_offsets = np.fromfile(actual["objects/ring_offsets.bin"], dtype="<u8")
            vertex_count = actual["objects/ring_vertices.bin"].stat().st_size // (2 * np.dtype("<f8").itemsize)
            intervals = np.memmap(actual["objects/park_intervals.bin"], dtype=INTERVAL_DTYPE, mode="r") \
                if actual["objects/park_intervals.bin"].stat().st_size else np.zeros(0, dtype=INTERVAL_DTYPE)
            if len(ring_offsets) and (
                int(ring_offsets[0]) != 0
                or np.any(ring_offsets[1:] < ring_offsets[:-1])
                or int(ring_offsets[-1]) != vertex_count
            ):
                problems.append("object ring offsets are not monotone from zero to the vertex count")
            ring_count = max(0, len(ring_offsets) - 1)
            if len(object_rows) and (
                np.any(object_rows["kind"] >= len(ALL_KINDS))
                or np.any(object_rows["first_ring"].astype(np.uint64) + object_rows["ring_count"] > ring_count)
            ):
                problems.append("objects/objects.bin contains invalid kind or ring references")
            if len(intervals) and (
                np.any(intervals["kind"] >= len(ALL_KINDS))
                or np.any(intervals["hi"] < intervals["lo"])
            ):
                problems.append("objects/park_intervals.bin contains invalid kind or interval bounds")
    except Exception as exc:  # noqa: BLE001 - binary parse failure disqualifies the cache
        problems.append(f"unreadable reach object artifacts: {exc}")
    return problems


def _tier_complete(directory: Path, key: str | None = None, shard_ids=None, fields=()) -> bool:
    return not tier_problems(directory, key=key, shard_ids=shard_ids, fields=fields)


# --- shells -----------------------------------------------------------------


def shard_cell_corners(entry: ShardEntry) -> np.ndarray:
    """Lower-left corners, row-major (row = y), matching the v3 shard layout."""
    step = float(CANONICAL_BASE_RESOLUTION_M)
    xs = entry.x_min_m + np.arange(entry.cols, dtype=np.float64) * step
    ys = entry.y_min_m + np.arange(entry.rows, dtype=np.float64) * step
    return np.column_stack([np.tile(xs, entry.rows), np.repeat(ys, entry.cols)])


def shard_land_area(entry: ShardEntry, corners: np.ndarray, study_area_metric) -> np.ndarray:
    """Exact cell ∩ study-area areas (GEOS, batched); inland cells are 2 500 m²."""
    shard_box = geometry_box(entry.x_min_m, entry.y_min_m, entry.x_max_m, entry.y_max_m)
    if study_area_metric.covers(shard_box):
        return np.full(len(corners), CELL_AREA_M2, dtype=np.float32)
    step = float(CANONICAL_BASE_RESOLUTION_M)
    boxes = shapely.box(corners[:, 0], corners[:, 1], corners[:, 0] + step, corners[:, 1] + step)
    shapely.prepare(study_area_metric)
    areas = np.zeros(len(corners), dtype=np.float64)
    touching = shapely.intersects(boxes, study_area_metric)
    covered = touching & shapely.contains(study_area_metric, boxes)
    areas[covered] = CELL_AREA_M2
    partial = np.flatnonzero(touching & ~covered)
    if len(partial):
        areas[partial] = shapely.area(shapely.intersection(boxes[partial], study_area_metric))
    return np.clip(areas, 0.0, CELL_AREA_M2).astype(np.float32)


def _shell_path(shell_dir: Path, shard_id: str) -> Path:
    return shell_dir / "shards" / f"{shard_id}.npz"


def _score_path(score_dir: Path, shard_id: str) -> Path:
    return score_dir / "shards" / f"{shard_id}.npz"


def load_shell_shard(shell_dir: Path, shard_id: str) -> dict[str, np.ndarray]:
    with np.load(_shell_path(shell_dir, shard_id), allow_pickle=False) as data:
        return {name: np.asarray(data[name]) for name in data.files}


def load_score_shard(score_dir: Path, shard_id: str) -> dict[str, np.ndarray]:
    with np.load(_score_path(score_dir, shard_id), allow_pickle=False) as data:
        return {name: np.asarray(data[name]) for name in data.files}


def shell_records(shell: dict[str, np.ndarray]) -> np.ndarray:
    records = np.zeros(shell["origin_flags"].size, dtype=CELL_DTYPE)
    records["x"] = shell["origin_x"].ravel()
    records["y"] = shell["origin_y"].ravel()
    records["flags"] = shell["origin_flags"].ravel()
    return records


def build_shells(
    *,
    graph_dir: Path,
    land_mask: LandMask,
    study_area_metric,
    entries: list[ShardEntry],
    shell_dir: Path,
    work_dir: Path,
    progress: ProgressFn,
) -> dict[str, Any]:
    mask_path = work_dir / "mask.geojson"
    write_mask_geojson(land_mask, mask_path)
    stats = {"cells_valid_land": 0, "valid_land_without_origin": 0, "cells_with_network": 0, "used_nodes": 0}
    batch: list[tuple[ShardEntry, np.ndarray, np.ndarray]] = []
    batch_cells = 0
    shard_counter = [0]

    def flush() -> None:
        nonlocal batch, batch_cells
        if not batch:
            return
        records = np.concatenate([item[2] for item in batch])
        groups = np.concatenate([np.full(len(item[2]), index, dtype=np.uint32) for index, item in enumerate(batch)])
        attach = native_attach(graph_dir, mask_path, records, work_dir, groups=groups)
        start = 0
        for index, (entry, land_area, shard_records) in enumerate(batch):
            count = len(shard_records)
            nodes = attach["group_nodes"][int(attach["group_offsets"][index]) : int(attach["group_offsets"][index + 1])]
            entry_flags = attach["entry_flags"][start : start + count]
            shape = (entry.rows, entry.cols)
            np.savez_compressed(
                _shell_path(shell_dir, entry.shard_id),
                land_area_m2=land_area.reshape(shape),
                valid_land_mask=(land_area > 0.0).reshape(shape),
                origin_x=shard_records["x"].reshape(shape),
                origin_y=shard_records["y"].reshape(shape),
                origin_flags=shard_records["flags"].reshape(shape),
                entry_flags=entry_flags.reshape(shape),
                used_nodes=np.asarray(nodes, dtype=np.uint32),
                x_min_m=np.asarray([entry.x_min_m], dtype=np.int32),
                y_min_m=np.asarray([entry.y_min_m], dtype=np.int32),
            )
            stats["cells_with_network"] += int(np.count_nonzero(entry_flags & (land_area > 0.0)))
            stats["used_nodes"] += int(len(nodes))
            start += count
        progress(f"shell: {len(batch)} shard(s), {len(records):,} cells, {shard_counter[0]}/{len(entries)} shards")
        batch, batch_cells = [], 0

    (shell_dir / "shards").mkdir(parents=True, exist_ok=True)
    margin = float(CANONICAL_BASE_RESOLUTION_M)
    for entry in entries:
        corners = shard_cell_corners(entry)
        # National outlines have ~10^6 vertices; clip them to the shard first.
        clip = (entry.x_min_m - margin, entry.y_min_m - margin, entry.x_max_m + margin, entry.y_max_m + margin)
        local_study = shapely.clip_by_rect(study_area_metric, *clip)
        land_area = shard_land_area(entry, corners, local_study)
        valid = land_area > 0.0
        records = np.zeros(len(corners), dtype=CELL_DTYPE)
        if np.any(valid):
            records[valid] = cell_records(
                corners[valid],
                CANONICAL_BASE_RESOLUTION_M,
                land_mask,
                local_mask=shapely.clip_by_rect(land_mask.geometry, *clip),
            )
        stats["cells_valid_land"] += int(valid.sum())
        stats["valid_land_without_origin"] += int((valid & ((records["flags"] & CELL_FLAG_ON_LAND) == 0)).sum())
        batch.append((entry, land_area, records))
        batch_cells += len(records)
        shard_counter[0] += 1
        if batch_cells >= _MAX_CELLS_PER_NATIVE_CALL:
            flush()
    flush()
    return stats


def collect_origin_nodes(shell_dir: Path, entries: list[ShardEntry]) -> np.ndarray:
    nodes = [load_shell_shard(shell_dir, entry.shard_id)["used_nodes"] for entry in entries]
    if not nodes:
        return np.zeros(0, dtype=np.uint32)
    return np.unique(np.concatenate(nodes)).astype(np.uint32)


# --- reach cache --------------------------------------------------------------


def save_reach_cache(reach_dir: Path, origins: np.ndarray, kinds: list[dict[str, Any]], labels: dict[str, Any]) -> None:
    """Distances and real logical ids for *every* kind, plus the label table."""
    np.save(reach_dir / "origins.npy", np.asarray(origins, dtype=np.uint32))
    stored = []
    for kind in kinds:
        k = int(kind["k"])
        np.save(reach_dir / f"{kind['kind']}.distances.npy", np.fromfile(kind["out_distances"], dtype="<u2").reshape(-1, k))
        np.save(reach_dir / f"{kind['kind']}.labels.npy", np.fromfile(kind["out_labels"], dtype="<u4").reshape(-1, k))
        stored.append({key: kind[key] for key in ("kind", "kind_index", "need", "quality", "k", "cutoff_m")})
    (reach_dir / "kinds.json").write_text(json.dumps(stored, indent=2), encoding="utf-8")
    (reach_dir / "labels.json").write_text(json.dumps(labels, sort_keys=True), encoding="utf-8")


def load_reach_cache(reach_dir: Path, *, mmap: bool = True) -> tuple[np.ndarray, list[dict[str, Any]], dict[str, tuple[np.ndarray, np.ndarray]]]:
    """Origins, kinds and per-kind ``(distances, labels)``; memory-mapped by default."""
    mode = "r" if mmap else None
    origins = np.load(reach_dir / "origins.npy")
    kinds = json.loads((reach_dir / "kinds.json").read_text(encoding="utf-8"))
    rows = {
        kind["kind"]: (
            np.load(reach_dir / f"{kind['kind']}.distances.npy", mmap_mode=mode),
            np.load(reach_dir / f"{kind['kind']}.labels.npy", mmap_mode=mode),
        )
        for kind in kinds
    }
    return origins, kinds, rows


def load_label_table(reach_dir: Path) -> dict[str, list[dict[str, Any]]]:
    return json.loads((reach_dir / "labels.json").read_text(encoding="utf-8"))


def reach_object_files(reach_dir: Path) -> ObjectFiles:
    base = reach_dir / "objects"
    return ObjectFiles(
        objects_bin=base / "objects.bin",
        ring_offsets_bin=base / "ring_offsets.bin",
        ring_vertices_bin=base / "ring_vertices.bin",
        intervals_bin=base / "park_intervals.bin",
        count=int((base / "objects.bin").stat().st_size // 32),
    )


# --- penalties ------------------------------------------------------------------


class CellPenalties:
    """Railway / road penalties at cell origins (indexes built once, batched)."""

    def __init__(self, road_rows: list[dict[str, Any]], corridor_rows: list[dict[str, Any]]) -> None:
        from transit.railway_corridors import RailwayPenaltyField

        from .road_proximity import RoadPenaltyField

        self.road = RoadPenaltyField(road_rows)
        self.railway = RailwayPenaltyField(corridor_rows)

    def __call__(self, x: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        return self.railway.at(x, y), self.road.at(x, y)


# --- score shards -------------------------------------------------------------


def _iter_blocks(entries: list[ShardEntry], block_cells: int):
    block: list[ShardEntry] = []
    cells = 0
    for entry in entries:
        block.append(entry)
        cells += entry.rows * entry.cols
        if cells >= block_cells:
            yield block
            block, cells = [], 0
    if block:
        yield block


def build_scores(
    *,
    graph_dir: Path,
    mask_path: Path,
    shell_dir: Path,
    reach_dir: Path,
    score_dir: Path,
    entries: list[ShardEntry],
    penalty_fn,
    work_dir: Path,
    progress: ProgressFn,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    block_cells: int = DAILY_ACCESS_SCORE_BLOCK_CELLS,
    context_field=None,
    walk_score_dir: Path | None = None,
) -> dict[str, Any]:
    from .daily_access_native import penalties_for_records

    (score_dir / "shards").mkdir(parents=True, exist_ok=True)
    origins, kinds, rows = load_reach_cache(reach_dir)
    objects = reach_object_files(reach_dir)
    labels = load_label_table(reach_dir)
    # Runtime resolves selected station ids from the pinned reach label table.
    manifest = _read_manifest(score_dir)
    if manifest is not None:
        _write_manifest(score_dir, {**manifest, "reach_dir": str(reach_dir), "channel_schema": SCORE_CHANNEL_SCHEMA,
                                    "context_source_score_key": (_read_manifest(walk_score_dir) or {}).get("key") if walk_score_dir else manifest["key"]})
    station_quality = {}
    warnings = []
    for kind in ("rail_station", "tram_station"):
        entries_for_kind = labels.get(kind, [])
        qualities = []
        for item in entries_for_kind:
            available = context_field is not None and item.get("feed") in context_field.corridor_feeds
            qualities.append(float(item.get("quality", 0.0)) if available else 0.0)
            if not available and mode == "walk":
                warnings.append(f"Missing corridor geometry for {item.get('feed')}; station bonus disabled for that feed")
        station_quality[kind] = qualities
    for warning in sorted(set(warnings)):
        progress(f"[daily-access context] warning: {warning}")
    status_counts = {name: 0 for name in NETWORK_STATUS_CODES}
    code_to_status = {code: name for name, code in NETWORK_STATUS_CODES.items()}
    total_sum = 0.0
    total_cells = 0
    missing_valid = 0
    peak_block_cells = 0
    for block_index, block in enumerate(_iter_blocks(entries, block_cells)):
        shells = [(entry, load_shell_shard(shell_dir, entry.shard_id)) for entry in block]
        records = np.concatenate([shell_records(shell) for _, shell in shells])
        peak_block_cells = max(peak_block_cells, len(records))
        block_nodes = np.unique(np.concatenate([shell["used_nodes"] for _, shell in shells])).astype(np.uint32)
        block_dir = work_dir / "score_block"
        if block_dir.exists():
            shutil.rmtree(block_dir)
        block_kinds = block_reach_files(kinds, origins, rows, block_nodes, block_dir / "reach")
        shared_bonuses = None
        if walk_score_dir is not None:
            walk_scores = [load_score_shard(walk_score_dir, entry.shard_id) for entry, _ in shells]
            context_data = {name: np.concatenate([s[name].reshape(-1, s[name].shape[-1]) if name in ("context_values", "context_metrics") else s[name].ravel() for s in walk_scores])
                            for name in ("context_values", "context_metrics", "pitch_ref", "pitch_reason", "pitch_distance")}
            shared_bonuses = np.concatenate([s["bonuses"].reshape(len(BONUSES), -1).T for s in walk_scores]).astype(np.float32) * DAILY_ACCESS_SCORE_STEP
        elif context_field is not None:
            context_data = context_field.records(records, progress=progress)
        else:
            context_data = {"context_values": np.zeros((len(records), 5), dtype=np.float32),
                            "context_metrics": np.zeros((len(records), len(METRICS)), dtype=np.float32),
                            "pitch_ref": np.full(len(records), "", dtype="U96"), "pitch_reason": np.full(len(records), "", dtype="U24"),
                            "pitch_distance": np.full(len(records), np.nan, dtype=np.float32)}
        scored = native_score(
            graph_dir,
            mask_path,
            records,
            block_nodes,
            block_kinds,
            block_dir,
            mode=mode,
            objects=objects,
            penalties=penalties_for_records(records, penalty_fn),
            context_values=context_data["context_values"], shared_bonuses=shared_bonuses,
            station_quality=station_quality,
        )
        if walk_score_dir is not None:
            for name in ("station_labels", "station_distances"):
                scored[name] = np.concatenate([s[name].reshape(2, -1).T for s in walk_scores])
        cursor = 0
        for entry, shell in shells:
            shape = (entry.rows, entry.cols)
            cells = slice(cursor, cursor + entry.rows * entry.cols)
            cursor += entry.rows * entry.cols
            valid = shell["valid_land_mask"].ravel()
            status = scored["network_status"][cells]
            total = scored["total"][cells]
            # Valid land whose origin fell outside the mask must not look like water.
            missing_valid += int(np.count_nonzero(valid & (status == NETWORK_STATUS_CODES["outside_mask"])))
            np.savez_compressed(
                _score_path(score_dir, entry.shard_id),
                total=total.reshape(shape),
                components=scored["components"][cells].T.reshape(len(CATEGORIES), *shape),
                penalties=scored["penalties"][cells].T.reshape(len(PENALTIES), *shape),
                bonuses=scored["bonuses"][cells].T.reshape(len(BONUSES), *shape),
                **({"context_values": context_data["context_values"][cells].reshape(*shape, 5),
                    "context_metrics": context_data["context_metrics"][cells].reshape(*shape, len(METRICS)),
                    "pitch_ref": context_data["pitch_ref"][cells].reshape(shape),
                    "pitch_reason": context_data["pitch_reason"][cells].reshape(shape),
                    "pitch_distance": context_data["pitch_distance"][cells].reshape(shape),
                    "station_labels": scored["station_labels"][cells].T.reshape(2, *shape),
                    "station_distances": scored["station_distances"][cells].T.reshape(2, *shape)} if mode == "walk" else {}),
                service_access=scored["service_access"][cells].T.reshape(len(DAILY_ACCESS_NEEDS), *shape),
                network_status=status.reshape(shape),
                x_min_m=np.asarray([entry.x_min_m], dtype=np.int32),
                y_min_m=np.asarray([entry.y_min_m], dtype=np.int32),
            )
            for code, count in zip(*np.unique(status[valid], return_counts=True)):
                status_counts[code_to_status[int(code)]] += int(count)
            ok = valid & (status == NETWORK_STATUS_CODES["ok"])
            total_sum += float(total[ok].astype(np.float64).sum()) / 100.0
            total_cells += int(ok.sum())
        progress(f"score: block {block_index + 1}: {len(block)} shard(s), {len(records):,} cells, {len(block_nodes):,} nodes")
        del shells, records, scored, block_kinds
        gc.collect()
    progress(f"score: {len(entries)} shard(s) written")
    if missing_valid:
        raise DailyAccessBuildError(
            f"{missing_valid} valid land cell(s) have no origin inside the land mask; "
            "the study area and the land mask disagree."
        )
    return {
        "network_status_valid_cells": status_counts,
        "mean_total_ok_cells": round(total_sum / total_cells, 3) if total_cells else None,
        "peak_block_cells": peak_block_cells,
        "context_warnings": sorted(set(warnings)),
        "channel_schema": SCORE_CHANNEL_SCHEMA,
    }


def build_bike_tiers(
    context: "BuildContext",
    *,
    entries: list[ShardEntry],
    mask_path: Path,
    work_dir: Path,
    force: bool,
    allow_missing_transport: bool,
    progress: ProgressFn,
) -> dict[str, Any]:
    """Build the exact v4 pipeline on the bike graph; failures are publish-fatal."""
    from db_postgis import load_railway_corridor_rows, load_road_rows
    from db_postgis.daily_access_green import stage_green_sites, stage_park_approaches
    from network import graph_meta_matches, run_walkgraph_build

    from .daily_access_reference import load_reference_graph

    keys = context.bike_keys
    graph_dir = context.bike_graph_dir
    shell_dir = context.bike_shell_dir
    reach_dir = context.bike_reach_dir
    score_dir = context.bike_score_dir
    graph_padding_m = context.bike_graph_padding_m
    if any(value is None for value in (keys, graph_dir, shell_dir, reach_dir, score_dir, graph_padding_m)):
        raise DailyAccessBuildError("bike build context is incomplete")
    assert keys is not None and graph_dir is not None and shell_dir is not None
    assert reach_dir is not None and score_dir is not None and graph_padding_m is not None

    if force or not graph_meta_matches(
        graph_dir,
        extract_fingerprint=context.source_state.extract_fingerprint,
        bbox=context.graph_bbox,
        bbox_padding_m=graph_padding_m,
        graph_profile="bike",
    ):
        run_walkgraph_build(
            Path(context.source_state.extract_path or OSM_EXTRACT_PATH),
            graph_dir,
            walkgraph_bin=WALKGRAPH_BIN,
            bbox=context.graph_bbox,
            bbox_padding_m=graph_padding_m,
            extract_fingerprint=context.source_state.extract_fingerprint,
            graph_profile="bike",
            progress_cb=_walkgraph_progress(progress),
        )

    shard_ids = [entry.shard_id for entry in entries]
    reach_buffer_m = max(DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE["bike"].values())
    common = {
        "profile": context.profile,
        "mode": "bike",
        "graph_profile": "bike",
        "resolution_m": CANONICAL_BASE_RESOLUTION_M,
        "shard_size_m": SURFACE_SHARD_SIZE_M,
        "quantization": QUANTIZATION,
        "mask_identity": context.mask_identity,
        "mask": "main-island land mask (sea/estuaries; inland water is not cut out), never clipped to the profile",
        "study_area_bounds_metric": [round(value, 3) for value in context.study_area_metric.bounds],
        "graph_bbox_wgs84": list(context.graph_bbox),
        "graph_padding_m": graph_padding_m,
        "reach_buffer_m": reach_buffer_m,
        "import_fingerprint": context.source_state.import_fingerprint,
        "transit_reality_fingerprint": context.reality_fingerprint,
        "overture_dataset_signature": context.overture_dataset_signature,
    }

    if force or tier_problems(shell_dir, key=keys.shell, shard_ids=shard_ids, fields=SHELL_FIELDS):
        begin_tier(shell_dir, "shell", keys.shell, {**common, "shards": shard_ids})
        bike_shell_work = work_dir / "bike_shell"
        bike_shell_work.mkdir(parents=True, exist_ok=True)
        shell_stats = build_shells(
            graph_dir=graph_dir,
            land_mask=context.land_mask,
            study_area_metric=context.study_area_metric,
            entries=entries,
            shell_dir=shell_dir,
            work_dir=bike_shell_work,
            progress=progress,
        )
        complete_tier(shell_dir, {"stats": shell_stats})
    shell_manifest = _read_manifest(shell_dir) or {}
    shell_stats = dict(shell_manifest.get("stats") or {})
    if shell_stats.get("valid_land_without_origin"):
        raise DailyAccessBuildError(
            f"{shell_stats['valid_land_without_origin']} valid land cell(s) have no bike origin"
        )

    if force or reach_tier_problems(reach_dir, key=keys.reach, mode="bike"):
        begin_tier(reach_dir, "reach", keys.reach, common)
        graph = load_reference_graph(graph_dir, verify_components=False)
        reach_metric = context.study_area_metric.envelope.buffer(reach_buffer_m).envelope
        reach_area_wgs84 = transform(lambda x, y, z=None: TO_WGS84(x, y), reach_metric)
        places, overture_green, stops, class_stats = load_v4_objects(
            context.engine,
            import_fingerprint=context.source_state.import_fingerprint,
            reality_fingerprint=context.reality_fingerprint,
            reach_area_wgs84=reach_area_wgs84,
        )
        with context.engine.begin() as connection:
            green_sites, green_stats = stage_green_sites(
                connection,
                import_fingerprint=context.source_state.import_fingerprint,
                study_area_wgs84=reach_area_wgs84,
                overture_green_rows=overture_green,
                buffer_m=0.0,
            )
            approaches, intervals, approach_stats = stage_park_approaches(
                connection,
                green_sites,
                node_x=graph.node_x,
                node_y=graph.node_y,
                offsets=graph.offsets,
                targets=graph.targets,
                lengths=graph.lengths,
                edge_flags=graph.edge_flags,
            )
        del graph
        gc.collect()
        sources = build_sources(
            places=places,
            green_sites=green_sites,
            park_approaches=approaches,
            park_intervals=intervals,
            stops=stops,
        )
        objects = write_objects(sources, context.land_mask, reach_dir / "objects")
        bike_work = work_dir / "bike_reach"
        bike_work.mkdir(parents=True, exist_ok=True)
        seeds, reasons = native_objects(graph_dir, mask_path, objects, bike_work)
        seeds = merge_area_seeds(seeds, sources.area_seeds)
        losses, warnings = loss_report(sources, reasons)
        warnings = warnings + transport_gate(losses, allow_missing_transport=allow_missing_transport)
        for warning in warnings:
            progress(f"[daily-access bike] warning: {warning}")
        labels = sources.labels.as_json()
        del sources, places, green_sites, approaches, intervals, stops, overture_green
        gc.collect()
        origins = collect_origin_nodes(shell_dir, entries)
        _, kinds = native_reach(
            graph_dir,
            seeds,
            origins,
            bike_work,
            mode="bike",
            load_results=False,
        )
        seed_counts = {kind: int(len(values)) for kind, values in seeds.items()}
        del seeds
        gc.collect()
        save_reach_cache(reach_dir, origins, kinds, labels)
        complete_tier(
            reach_dir,
            {
                "origins": int(len(origins)),
                "losses": losses,
                "warnings": warnings,
                "classification": class_stats,
                "green": green_stats,
                "park_approaches": approach_stats,
                "seeds_by_kind": seed_counts,
                "objects": objects.count,
            },
            shard_dir=None,
            inventory_files=True,
        )

    if force or tier_problems(score_dir, key=keys.score, shard_ids=shard_ids, fields=SCORE_FIELDS):
        begin_tier(
            score_dir,
            "score",
            keys.score,
            {
                **common,
                "shards": shard_ids,
                "reach_key": keys.reach,
                "shell_key": keys.shell,
                "score_params": daily_access_score_params("bike"),
                "penalty_params": penalty_params(),
            },
        )
        penalty_fn = CellPenalties(
            load_road_rows(context.engine, context.source_state.import_fingerprint),
            load_railway_corridor_rows(context.engine, context.reality_fingerprint)
            if context.reality_fingerprint and context.reality_fingerprint != "transit-unavailable"
            else [],
        )
        score_stats = build_scores(
            graph_dir=graph_dir,
            mask_path=mask_path,
            shell_dir=shell_dir,
            reach_dir=reach_dir,
            score_dir=score_dir,
            entries=entries,
            penalty_fn=penalty_fn,
            work_dir=work_dir / "bike_score",
            progress=progress,
            mode="bike",
            walk_score_dir=context.score_dir,
        )
        complete_tier(score_dir, {"stats": score_stats})

    reach_manifest = _read_manifest(reach_dir) or {}
    score_manifest = _read_manifest(score_dir) or {}
    return {
        "keys": keys.__dict__,
        "paths": {
            "graph": str(graph_dir),
            "shell": str(shell_dir),
            "reach": str(reach_dir),
            "score": str(score_dir),
        },
        "shell": shell_stats,
        "reach": {
            key: reach_manifest.get(key)
            for key in ("origins", "losses", "warnings", "park_approaches")
        },
        "score": dict(score_manifest.get("stats") or {}),
    }


# --- orchestration --------------------------------------------------------------


def _graph_bbox(study_area_wgs84) -> tuple[float, float, float, float]:
    min_lon, min_lat, max_lon, max_lat = study_area_wgs84.bounds
    return (float(min_lat), float(min_lon), float(max_lat), float(max_lon))


def load_v4_objects(engine, *, import_fingerprint, reality_fingerprint, reach_area_wgs84):
    """Raw OSM + Overture rows classified one by one, then v4 dedupe.

    The v3 OSM/Overture merge is used only to pick Overture-only green points
    (green grouping rules are unchanged); shops and medical objects never go
    through it.
    """
    from db_postgis import (
        load_daily_access_source_rows,
        load_merged_source_amenity_rows,
        load_transport_reality_rows_for_scoring,
    )
    from overture import loader as overture_loader

    osm_rows = load_daily_access_source_rows(engine, import_fingerprint, reach_area_wgs84)
    overture_rows = overture_loader.load_overture_amenity_rows(reach_area_wgs84)
    service_rows = [row for row in [*osm_rows, *overture_rows] if row.get("category") in {"shops", "healthcare"}]
    classified, class_stats = classify_service_rows(service_rows)
    dedupe_stats: dict[str, Any] = {}
    places = build_logical_places(classified, stats_out=dedupe_stats)
    class_stats["dedupe"] = dedupe_stats

    green_osm = [row for row in osm_rows if row.get("category") == "parks"]
    green_overture = [row for row in overture_rows if row.get("category") == "parks"]
    merged_rows, _ = load_merged_source_amenity_rows(engine, green_osm, green_overture, scoring_categories=("parks",))
    lookup = {(str(row.get("source") or ""), str(row.get("source_ref") or "")): row for row in green_overture}
    overture_only_green = [
        lookup[(str(row["source"]), str(row["source_ref"]))]
        for row in merged_rows
        if row.get("category") == "parks" and row.get("source") == "overture_places"
        and (str(row["source"]), str(row["source_ref"])) in lookup
    ]
    stops: list[TransportStop] = []
    if reality_fingerprint and reality_fingerprint != "transit-unavailable":
        from db_postgis.reads import load_public_station_modes
        station_modes = load_public_station_modes(engine, reality_fingerprint)
        for row in load_transport_reality_rows_for_scoring(engine, reality_fingerprint, reach_area_wgs84):
            x, y = TO_TARGET(float(row["geom"].x), float(row["geom"].y))
            parts = str(row["source_ref"]).split("/")
            stops.append(
                TransportStop(
                    stop_ref=str(row["source_ref"]),
                    feed=parts[1] if len(parts) > 2 else "unknown",
                    tier=int(row["transport_score_units"]),
                    x=float(x),
                    y=float(y),
                    modes=tuple(station_modes.get(str(row["source_ref"]), ())),
                )
            )
    return places, overture_only_green, stops, class_stats


def transport_gate(losses: dict[str, Any], *, allow_missing_transport: bool) -> list[str]:
    """Block when no eligible GTFS stop attaches; warn per feed from 5 % lost."""
    transport = losses.get("transport") or {}
    eligible = int(transport.get("eligible", 0))
    attached = eligible - int(transport.get("unbound", 0))
    if attached <= 0 and not allow_missing_transport:
        raise DailyAccessBuildError(
            f"No usable GTFS transport stops for the daily access score ({eligible} eligible, "
            f"{attached} attached); refusing to continue without --allow-missing-transport."
        )
    return [] if attached > 0 else ["no usable GTFS transport stops (--allow-missing-transport)"]


@dataclass
class BuildContext:
    engine: Any
    profile: str
    source_state: Any
    reality_fingerprint: str | None
    hashes: Any
    study_area_metric: Any
    study_area_wgs84: Any
    land_mask: LandMask
    mask_identity: str
    graph_bbox: tuple[float, float, float, float]
    graph_padding_m: float
    keys: TierKeys
    graph_dir: Path
    shell_dir: Path
    reach_dir: Path
    score_dir: Path
    transit_reality_state: Any = None
    transit_analysis_window_days: int = GTFS_ANALYSIS_WINDOW_DAYS
    transit_service_desert_window_days: int = GTFS_SERVICE_DESERT_WINDOW_DAYS
    overture_dataset_signature: str = ""
    import_completed_at: Any = None
    bike_graph_padding_m: float | None = None
    bike_keys: TierKeys | None = None
    bike_graph_dir: Path | None = None
    bike_shell_dir: Path | None = None
    bike_reach_dir: Path | None = None
    bike_score_dir: Path | None = None


def resolve_build_context(profile: str | None, *, engine=None) -> BuildContext:
    """Inputs, keys and tier directories of a profile (shared by build, verify, publish, runtime)."""
    from db_postgis import build_engine, load_import_manifest
    from local_osm_import import resolve_source_state
    from overture.loader import dataset_signature as overture_dataset_signature
    from study_area import load_island_geometry_metric, load_study_area_geometries

    from config import build_hashes_for_import

    profile = normalize_build_profile(profile)
    engine = build_engine() if engine is None else engine
    source_state = resolve_source_state()
    manifest = load_import_manifest(engine, source_state.import_fingerprint)
    if not manifest or manifest.get("status") != "complete":
        raise DailyAccessBuildError(
            f"OSM import {source_state.import_fingerprint} is not complete; run "
            "`python main.py import` to prepare the import (also refreshes the basemap; needs Docker). "
            "On Windows run it through scripts\\win\\geo_env.cmd."
        )
    from transit import ensure_transit_reality

    # Reuses the current transit reality when it is fresh (no feed download).
    reality = ensure_transit_reality(engine, import_fingerprint=source_state.import_fingerprint)
    reality_fingerprint = getattr(reality, "reality_fingerprint", None)
    hashes = build_hashes_for_import(
        source_state.import_fingerprint,
        transit_reality_fingerprint=reality_fingerprint or "transit-unavailable",
        profile=profile,
    )
    study_area_metric, study_area_wgs84 = load_study_area_geometries(profile=profile)
    land_mask = LandMask(load_island_geometry_metric())
    graph_bbox = _graph_bbox(study_area_wgs84)
    graph_padding_m = float(DAILY_ACCESS_REACH_BUFFER_M + DAILY_ACCESS_SNAP_RADIUS_M)
    overture_signature = overture_dataset_signature()
    reach_inputs = hash_dict(
        {
            "import_fingerprint": source_state.import_fingerprint,
            "transit_reality_fingerprint": reality_fingerprint or "transit-unavailable",
            "overture_dataset_signature": overture_signature,
            "reach_buffer_m": DAILY_ACCESS_REACH_BUFFER_M,
        }
    )
    mask_id = mask_identity(land_mask)
    keys = tier_keys(
        extract_fingerprint=source_state.extract_fingerprint,
        graph_bbox=graph_bbox,
        graph_padding_m=graph_padding_m,
        geo_hash=hashes.geo_hash,
        mask_identity=mask_id,
        reach_inputs=reach_inputs,
    )
    bike_graph_padding_m = float(
        max(DAILY_ACCESS_SEARCH_CUTOFF_M_BY_MODE["bike"].values())
        + DAILY_ACCESS_SNAP_RADIUS_M
    )
    bike_keys = tier_keys(
        extract_fingerprint=source_state.extract_fingerprint,
        graph_bbox=graph_bbox,
        graph_padding_m=bike_graph_padding_m,
        geo_hash=hashes.geo_hash,
        mask_identity=mask_id,
        reach_inputs=reach_inputs,
        mode="bike",
    )
    root = DAILY_ACCESS_CACHE_DIR
    return BuildContext(
        engine=engine,
        profile=profile,
        source_state=source_state,
        reality_fingerprint=reality_fingerprint,
        hashes=hashes,
        study_area_metric=study_area_metric,
        study_area_wgs84=study_area_wgs84,
        land_mask=land_mask,
        mask_identity=mask_id,
        graph_bbox=graph_bbox,
        graph_padding_m=graph_padding_m,
        keys=keys,
        graph_dir=root / f"graph_{keys.graph}" / "walk_graph",
        shell_dir=root / f"shell_{keys.shell}",
        reach_dir=root / f"reach_{keys.reach}",
        score_dir=root / f"score_{keys.score}",
        transit_reality_state=reality,
        overture_dataset_signature=overture_signature,
        import_completed_at=manifest.get("completed_at"),
        bike_graph_padding_m=bike_graph_padding_m,
        bike_keys=bike_keys,
        bike_graph_dir=root / f"graph_{bike_keys.graph}" / "bike_graph",
        bike_shell_dir=root / f"shell_bike_{bike_keys.shell}",
        bike_reach_dir=root / f"reach_bike_{bike_keys.reach}",
        bike_score_dir=root / f"score_bike_{bike_keys.score}",
    )


def run_daily_access_build(
    profile: str | None = None,
    *,
    force: bool = False,
    allow_missing_transport: bool = False,
    publish: bool = False,
    progress: ProgressFn = _print_progress,
) -> dict[str, Any]:
    ensure_supported_routing_backend()
    from db_postgis import load_railway_corridor_rows, load_road_rows
    from db_postgis.daily_access_green import stage_green_sites, stage_park_approaches
    from network import graph_meta_matches, run_walkgraph_build

    from .daily_access_reference import load_reference_graph

    timings: dict[str, float] = {}
    started = time.perf_counter()

    def lap(name: str) -> None:
        nonlocal started
        now = time.perf_counter()
        timings[name] = round(now - started, 2)
        started = now
        progress(f"[daily-access] {name}: {timings[name]:.1f}s")

    context = resolve_build_context(profile)
    engine = context.engine
    profile = context.profile
    source_state = context.source_state
    reality_fingerprint = context.reality_fingerprint
    hashes = context.hashes
    study_area_metric = context.study_area_metric
    land_mask = context.land_mask
    mask_id = context.mask_identity
    graph_bbox = context.graph_bbox
    graph_padding_m = context.graph_padding_m
    keys = context.keys
    graph_dir, shell_dir, reach_dir, score_dir = context.graph_dir, context.shell_dir, context.reach_dir, context.score_dir
    lap("inputs")

    if force or not graph_meta_matches(
        graph_dir,
        extract_fingerprint=source_state.extract_fingerprint,
        bbox=graph_bbox,
        bbox_padding_m=graph_padding_m,
    ):
        run_walkgraph_build(
            Path(source_state.extract_path or OSM_EXTRACT_PATH),
            graph_dir,
            walkgraph_bin=WALKGRAPH_BIN,
            bbox=graph_bbox,
            bbox_padding_m=graph_padding_m,
            extract_fingerprint=source_state.extract_fingerprint,
            progress_cb=_walkgraph_progress(progress),
        )
    lap("graph")

    entries = iter_shard_entries(study_area_metric)
    shard_ids = [entry.shard_id for entry in entries]
    common = {
        "profile": profile,
        "resolution_m": CANONICAL_BASE_RESOLUTION_M,
        "shard_size_m": SURFACE_SHARD_SIZE_M,
        "quantization": QUANTIZATION,
        "mask_identity": mask_id,
        "mask": "main-island land mask (sea/estuaries; inland water is not cut out), never clipped to the profile",
        "study_area_bounds_metric": [round(value, 3) for value in study_area_metric.bounds],
        "graph_bbox_wgs84": list(graph_bbox),
        "graph_padding_m": graph_padding_m,
        "reach_buffer_m": DAILY_ACCESS_REACH_BUFFER_M,
        "import_fingerprint": source_state.import_fingerprint,
        "transit_reality_fingerprint": reality_fingerprint,
        "overture_dataset_signature": context.overture_dataset_signature,
    }
    summary: dict[str, Any] = {
        "profile": profile,
        "model": DAILY_ACCESS_SCORE_MODEL,
        "versions": model_versions(),
        "import_fingerprint": source_state.import_fingerprint,
        "transit_reality_fingerprint": reality_fingerprint,
        "keys": keys.__dict__,
        "mode_aware": daily_access_mode_aware_params(),
        "shards": len(entries),
        "graph_dir": str(graph_dir),
    }

    with tempfile.TemporaryDirectory(dir=CACHE_DIR) as temp:
        work_dir = Path(temp)
        mask_path = work_dir / "mask.geojson"
        write_mask_geojson(land_mask, mask_path)
        if force or tier_problems(shell_dir, key=keys.shell, shard_ids=shard_ids, fields=SHELL_FIELDS):
            begin_tier(shell_dir, "shell", keys.shell, {**common, "shards": shard_ids})
            shell_stats = build_shells(
                graph_dir=graph_dir,
                land_mask=land_mask,
                study_area_metric=study_area_metric,
                entries=entries,
                shell_dir=shell_dir,
                work_dir=work_dir,
                progress=progress,
            )
            complete_tier(shell_dir, {"stats": shell_stats})
        summary["shell"] = (_read_manifest(shell_dir) or {})["stats"]
        if summary["shell"].get("valid_land_without_origin"):
            raise DailyAccessBuildError(
                f"{summary['shell']['valid_land_without_origin']} valid land cell(s) have no origin inside "
                "the land mask; the study area and the land mask disagree."
            )
        lap("shell")

        if force or reach_tier_problems(reach_dir, key=keys.reach):
            begin_tier(reach_dir, "reach", keys.reach, common)
            graph = load_reference_graph(graph_dir, verify_components=False)
            reach_metric = study_area_metric.envelope.buffer(DAILY_ACCESS_REACH_BUFFER_M).envelope
            reach_area_wgs84 = transform(lambda x, y, z=None: TO_WGS84(x, y), reach_metric)
            places, overture_green, stops, class_stats = load_v4_objects(
                engine,
                import_fingerprint=source_state.import_fingerprint,
                reality_fingerprint=reality_fingerprint,
                reach_area_wgs84=reach_area_wgs84,
            )
            with engine.begin() as connection:
                green_sites, green_stats = stage_green_sites(
                    connection,
                    import_fingerprint=source_state.import_fingerprint,
                    study_area_wgs84=reach_area_wgs84,
                    overture_green_rows=overture_green,
                    buffer_m=0.0,
                )
                approaches, intervals, approach_stats = stage_park_approaches(
                    connection,
                    green_sites,
                    node_x=graph.node_x,
                    node_y=graph.node_y,
                    offsets=graph.offsets,
                    targets=graph.targets,
                    lengths=graph.lengths,
                    edge_flags=graph.edge_flags,
                )
            del graph
            gc.collect()
            lap("objects_and_green")
            sources = build_sources(
                places=places, green_sites=green_sites, park_approaches=approaches,
                park_intervals=intervals, stops=stops,
            )
            objects = write_objects(sources, land_mask, reach_dir / "objects")
            seeds, reasons = native_objects(graph_dir, mask_path, objects, work_dir)
            seeds = merge_area_seeds(seeds, sources.area_seeds)
            losses, warnings = loss_report(sources, reasons)
            warnings = warnings + transport_gate(losses, allow_missing_transport=allow_missing_transport)
            for warning in warnings:
                progress(f"[daily-access] warning: {warning}")
            labels = sources.labels.as_json()
            del sources, places, green_sites, approaches, intervals, stops, overture_green
            gc.collect()
            origins = collect_origin_nodes(shell_dir, entries)
            _, kinds = native_reach(graph_dir, seeds, origins, work_dir, load_results=False)
            seed_counts = {kind: int(len(values)) for kind, values in seeds.items()}
            del seeds
            gc.collect()
            save_reach_cache(reach_dir, origins, kinds, labels)
            complete_tier(
                reach_dir,
                {
                    "origins": int(len(origins)),
                    "losses": losses,
                    "warnings": warnings,
                    "classification": class_stats,
                    "green": green_stats,
                    "park_approaches": approach_stats,
                    "seeds_by_kind": seed_counts,
                    "objects": objects.count,
                },
                shard_dir=None,
                inventory_files=True,
            )
            lap("reach")
        reach_manifest = _read_manifest(reach_dir) or {}
        summary["reach"] = {key: reach_manifest.get(key) for key in ("origins", "losses", "warnings", "park_approaches")}

        if force or tier_problems(score_dir, key=keys.score, shard_ids=shard_ids, fields=SCORE_FIELDS):
            begin_tier(score_dir, "score", keys.score, {**common, "shards": shard_ids, "reach_key": keys.reach,
                                                        "shell_key": keys.shell, "score_params": daily_access_score_params(),
                                                        "penalty_params": penalty_params()})
            penalty_fn = CellPenalties(
                load_road_rows(engine, source_state.import_fingerprint),
                load_railway_corridor_rows(engine, reality_fingerprint)
                if reality_fingerprint and reality_fingerprint != "transit-unavailable"
                else [],
            )
            score_stats = build_scores(
                graph_dir=graph_dir,
                mask_path=mask_path,
                shell_dir=shell_dir,
                reach_dir=reach_dir,
                score_dir=score_dir,
                entries=entries,
                penalty_fn=penalty_fn,
                work_dir=work_dir,
                progress=progress,
                context_field=load_context_field(context),
            )
            complete_tier(score_dir, {"stats": score_stats})
            lap("score")
        summary["score"] = (_read_manifest(score_dir) or {})["stats"]
        summary["bike"] = build_bike_tiers(
            context,
            entries=entries,
            mask_path=mask_path,
            work_dir=work_dir,
            force=force,
            allow_missing_transport=allow_missing_transport,
            progress=progress,
        )
        lap("bike")
    summary["paths"] = {
        "shell": str(shell_dir),
        "reach": str(reach_dir),
        "score": str(score_dir),
        "bike_shell": str(context.bike_shell_dir),
        "bike_reach": str(context.bike_reach_dir),
        "bike_score": str(context.bike_score_dir),
    }
    summary["cache_bytes"] = {
        name: sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())
        for name, directory in (
            ("shell", shell_dir),
            ("reach", reach_dir),
            ("score", score_dir),
            ("bike_shell", context.bike_shell_dir),
            ("bike_reach", context.bike_reach_dir),
            ("bike_score", context.bike_score_dir),
        )
        if directory is not None
    }
    summary["timings_s"] = timings
    summary["peak_memory_gib"] = {
        "python": round((process_peak_bytes() or 0) / 2**30, 2),
        **{name: round(value / 2**30, 2) for name, value in NATIVE_PEAK_BYTES.items()},
    }
    if publish:
        from .daily_access_publish import publish_daily_access

        summary["publish"] = publish_daily_access(
            context, allow_missing_transport=allow_missing_transport, progress=progress
        )
    return summary


__all__ = [
    "CellPenalties",
    "DAILY_ACCESS_CACHE_DIR",
    "DailyAccessBuildError",
    "SCORE_FIELDS",
    "SHELL_FIELDS",
    "BuildContext",
    "begin_tier",
    "build_bike_tiers",
    "build_scores",
    "build_shells",
    "collect_origin_nodes",
    "complete_tier",
    "ensure_supported_routing_backend",
    "load_label_table",
    "load_reach_cache",
    "load_score_shard",
    "load_shell_shard",
    "load_v4_objects",
    "mask_identity",
    "model_versions",
    "penalty_params",
    "reach_object_files",
    "reach_tier_problems",
    "resolve_build_context",
    "run_daily_access_build",
    "save_reach_cache",
    "shard_cell_corners",
    "shard_land_area",
    "shell_records",
    "surface_tier_problems",
    "tier_keys",
    "tier_problems",
    "transport_gate",
]
