"""Daily access score (v4) runtime: inspect, aggregation and PNG tiles.

Everything is read from the v4 score shards (``total``, four components,
five penalties, two bonuses, six service access values, network status) and the shell
shards (land area). Nothing is rescored here and no node-level score is
consulted.

Aggregation to a coarser cell (visible inspect cell, coarse grid rows, PNG
tiles) is the land-area weighted mean of the *stored, decoded* values:

* total, components, penalties and access are averaged independently (the
  curves and the top-3 rule are never applied again);
* land without a walking network counts with its zero access;
* water (no land area) is excluded;
* the network status is not averaged: the aggregate reports a status and the
  network-covered share of the land.

Caches are bounded by bytes (shell 64 MiB, score 128 MiB, aggregates 64 MiB)
and aggregates are cached per requested channel only.
"""

from __future__ import annotations

import math
import os
import tempfile
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from config import (
    CANONICAL_BASE_RESOLUTION_M,
    DAILY_ACCESS_ACCESS_STEP,
    DAILY_ACCESS_CATEGORY_MAX,
    DAILY_ACCESS_DEFAULT_MODE,
    DAILY_ACCESS_HEALTHCARE_MIX,
    DAILY_ACCESS_MODE_AWARE_SECONDARY_BONUS,
    DAILY_ACCESS_MODES,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_RUNTIME_AGGREGATE_CACHE_BYTES,
    DAILY_ACCESS_RUNTIME_SCORE_CACHE_BYTES,
    DAILY_ACCESS_RUNTIME_SHELL_CACHE_BYTES,
    DAILY_ACCESS_SCORE_MODEL,
    DAILY_ACCESS_SCORE_STEP,
    DAILY_ACCESS_SHOP_MIX,
    DAILY_ACCESS_UINT16_MISSING,
    RAILWAY_PROXIMITY_MAX_PENALTY,
    ROAD_PROXIMITY_CLASS_SETTINGS,
    ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER,
    SURFACE_SHARD_SIZE_M,
    SURFACE_TILE_SIZE_PX,
    TO_TARGET,
    resolution_for_zoom,
)

from .daily_access_reference import NETWORK_STATUS_CODES
from .daily_access_score import CATEGORIES, PENALTIES, BONUSES

LEGACY_UNAVAILABLE_FIELDS = ("counts", "cluster_counts", "effective_units")
_STATUS_BY_CODE = {code: name for name, code in NETWORK_STATUS_CODES.items()}
_ROAD_MAX = max(float(s["max_penalty"]) for s in ROAD_PROXIMITY_CLASS_SETTINGS.values()) * float(
    ROAD_PROXIMITY_MAX_SPEED_MULTIPLIER
)


def _base_channels() -> dict[str, tuple[str, int, float, float]]:
    """channel -> (shard field, index, decode scale, max for colouring)."""
    channels: dict[str, tuple[str, int, float, float]] = {
        "total": ("total", -1, DAILY_ACCESS_SCORE_STEP, 100.0)
    }
    for index, category in enumerate(CATEGORIES):
        channels[category] = ("components", index, DAILY_ACCESS_SCORE_STEP, float(DAILY_ACCESS_CATEGORY_MAX[category]))
    channels["railway_proximity"] = ("penalties", 0, DAILY_ACCESS_SCORE_STEP, float(RAILWAY_PROXIMITY_MAX_PENALTY))
    channels["road_proximity"] = ("penalties", 1, DAILY_ACCESS_SCORE_STEP, _ROAD_MAX)
    for index, name in enumerate(PENALTIES[2:], 2):
        channels[name] = ("penalties", index, DAILY_ACCESS_SCORE_STEP, {"industrial_context": 8.0, "over_concentration": 12.0, "pitch_noise": 4.0}[name])
    for index, name in enumerate(BONUSES):
        channels[name] = ("bonuses", index, DAILY_ACCESS_SCORE_STEP, 3.0 if index == 0 else 2.0)
    for index, need in enumerate(DAILY_ACCESS_NEEDS):
        channels[f"access_{need}"] = ("service_access", index, DAILY_ACCESS_ACCESS_STEP, 1.0)
    return channels


BASE_CHANNELS = _base_channels()
CHANNELS: dict[str, tuple[str, str, int, float, float]] = {}
for _mode in (*DAILY_ACCESS_MODES, "mode_aware"):
    for _channel, (_field, _index, _scale, _maximum) in BASE_CHANNELS.items():
        _name = _channel if _mode == DAILY_ACCESS_DEFAULT_MODE else f"{_mode}_{_channel}"
        CHANNELS[_name] = (_mode, _field, _index, _scale, _maximum)

NETWORK_COVERAGE_MODES = {
    "network_coverage": "walk",
    "bike_network_coverage": "bike",
    "mode_aware_network_coverage": "mode_aware",
}


class ByteLRU:
    """Thread-safe LRU cache bounded by the summed ``nbytes`` of its values."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = int(max_bytes)
        self._items: OrderedDict[Any, tuple[Any, int]] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    @staticmethod
    def _size(value: Any) -> int:
        if isinstance(value, np.ndarray):
            return int(value.nbytes)
        if isinstance(value, dict):
            return sum(ByteLRU._size(item) for item in value.values())
        if isinstance(value, (tuple, list)):
            return sum(ByteLRU._size(item) for item in value)
        return 64

    @property
    def bytes(self) -> int:
        return self._bytes

    def get(self, key: Any) -> Any | None:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            self._items.move_to_end(key)
            return item[0]

    def put(self, key: Any, value: Any) -> Any:
        size = self._size(value)
        with self._lock:
            if key in self._items:
                self._bytes -= self._items.pop(key)[1]
            if size > self.max_bytes:
                return value  # never cached, still returned
            self._items[key] = (value, size)
            self._bytes += size
            while self._bytes > self.max_bytes and self._items:
                _old, (_, old_size) = self._items.popitem(last=False)
                self._bytes -= old_size
        return value


def decode(field: np.ndarray, scale: float) -> np.ndarray:
    values = np.asarray(field, dtype=np.float64) * scale
    return np.where(np.asarray(field) == DAILY_ACCESS_UINT16_MISSING, np.nan, values)


def block_reduce_weighted(values: np.ndarray, weights: np.ndarray, factor: int) -> tuple[np.ndarray, np.ndarray]:
    """Weighted means over ``factor x factor`` blocks (NaN where no weight) and the block weights."""
    rows, cols = values.shape
    out_rows, out_cols = math.ceil(rows / factor), math.ceil(cols / factor)
    pad_rows, pad_cols = out_rows * factor - rows, out_cols * factor - cols
    w = np.pad(np.where(np.isfinite(values), weights, 0.0), ((0, pad_rows), (0, pad_cols)))
    v = np.pad(np.nan_to_num(values, nan=0.0), ((0, pad_rows), (0, pad_cols)))
    w_blocks = w.reshape(out_rows, factor, out_cols, factor).sum(axis=(1, 3))
    v_blocks = (v * w).reshape(out_rows, factor, out_cols, factor).sum(axis=(1, 3))
    with np.errstate(invalid="ignore", divide="ignore"):
        means = np.where(w_blocks > 0.0, v_blocks / np.where(w_blocks > 0.0, w_blocks, 1.0), np.nan)
    return means, w_blocks


def aggregate_status(coverage: float | None, land_m2: float) -> str:
    if land_m2 <= 0.0 or coverage is None:
        return "outside_mask"
    if coverage >= 1.0:
        return "ok"
    if coverage <= 0.0:
        return "no_walking_network"
    return "partial"


class DailyAccessSurfaceRuntime:
    """Serves one published (or dev) v4 build straight from its shards."""

    def __init__(
        self,
        shell_dir: Path,
        score_dir: Path,
        tile_dir: Path | None = None,
        *,
        profile: str | None = None,
        bike_shell_dir: Path | None = None,
        bike_score_dir: Path | None = None,
    ) -> None:
        import json

        self.shell_dir = Path(shell_dir)
        self.score_dir = Path(score_dir)
        self.bike_shell_dir = None if bike_shell_dir is None else Path(bike_shell_dir)
        self.bike_score_dir = None if bike_score_dir is None else Path(bike_score_dir)
        self.tile_dir = None if tile_dir is None else Path(tile_dir)
        self.profile = profile
        manifest = json.loads((self.score_dir / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("status") != "complete":
            raise RuntimeError(f"daily access score tier {self.score_dir} is not complete")
        self.manifest = manifest
        import json
        reach_dir = manifest.get("reach_dir")
        labels_path = Path(reach_dir) / "labels.json" if reach_dir else None
        self.station_labels = json.loads(labels_path.read_text(encoding="utf-8")) if labels_path and labels_path.exists() else {}
        self.shard_ids = frozenset(str(shard) for shard in manifest.get("shards") or ())
        self.available_modes = ["walk"]
        if (self.bike_shell_dir is None) != (self.bike_score_dir is None):
            raise RuntimeError("daily access bike shell and score tiers must be supplied together")
        if self.bike_score_dir is not None:
            assert self.bike_shell_dir is not None
            bike_shell_manifest = json.loads(
                (self.bike_shell_dir / "manifest.json").read_text(encoding="utf-8")
            )
            bike_manifest = json.loads((self.bike_score_dir / "manifest.json").read_text(encoding="utf-8"))
            bike_shell_shards = frozenset(
                str(shard) for shard in bike_shell_manifest.get("shards") or ()
            )
            bike_shards = frozenset(str(shard) for shard in bike_manifest.get("shards") or ())
            if (
                bike_shell_manifest.get("status") != "complete"
                or bike_manifest.get("status") != "complete"
                or bike_shell_shards != self.shard_ids
                or bike_shards != self.shard_ids
            ):
                raise RuntimeError("daily access bike shell/score tiers are incomplete or misaligned")
            self.available_modes.extend(["bike", "mode_aware"])
        self.channels = {
            name: spec for name, spec in CHANNELS.items() if spec[0] in self.available_modes
        }
        self.network_coverage_modes = {
            name: mode
            for name, mode in NETWORK_COVERAGE_MODES.items()
            if mode in self.available_modes
        }
        self._shells = ByteLRU(DAILY_ACCESS_RUNTIME_SHELL_CACHE_BYTES)
        self._scores = ByteLRU(DAILY_ACCESS_RUNTIME_SCORE_CACHE_BYTES)
        self._aggregates = ByteLRU(DAILY_ACCESS_RUNTIME_AGGREGATE_CACHE_BYTES)

    # -- shard access ----------------------------------------------------------

    def cache_bytes(self) -> dict[str, int]:
        return {"shell": self._shells.bytes, "score": self._scores.bytes, "aggregate": self._aggregates.bytes}

    def _shell(self, shard_id: str) -> dict[str, np.ndarray]:
        cached = self._shells.get(shard_id)
        if cached is None:
            with np.load(self.shell_dir / "shards" / f"{shard_id}.npz", allow_pickle=False) as data:
                cached = self._shells.put(shard_id, {name: np.asarray(data[name]) for name in ("land_area_m2", "x_min_m", "y_min_m")})
        return cached

    def _raw_score(self, shard_id: str, mode: str = "walk") -> dict[str, np.ndarray]:
        cache_key = (mode, shard_id)
        cached = self._scores.get(cache_key)
        if cached is None:
            directory = self.score_dir if mode == "walk" else self.bike_score_dir
            if directory is None:
                raise RuntimeError(f"daily access mode {mode!r} is unavailable")
            with np.load(directory / "shards" / f"{shard_id}.npz", allow_pickle=False) as data:
                cached = {name: np.asarray(data[name]) for name in ("total", "components", "penalties", "bonuses", "service_access", "network_status") if name in data}
                # Read compatibility for already-published pre-context v4 builds.
                if cached["penalties"].shape[0] < len(PENALTIES):
                    cached["penalties"] = np.pad(cached["penalties"], ((0, len(PENALTIES) - cached["penalties"].shape[0]), (0, 0), (0, 0)))
                if "bonuses" not in cached:
                    cached["bonuses"] = np.zeros((len(BONUSES), *cached["total"].shape), dtype="<u2")
                cached = self._scores.put(cache_key, cached)
        return cached

    @staticmethod
    def _quantize(values: np.ndarray, scale: float, *, missing: np.ndarray) -> np.ndarray:
        codes = np.floor(np.clip(values, 0.0, None) / scale + 0.5).astype(np.uint16)
        return np.where(missing, DAILY_ACCESS_UINT16_MISSING, codes).astype(np.uint16)

    def _mode_aware_score(self, shard_id: str) -> dict[str, np.ndarray]:
        cache_key = ("mode_aware", shard_id)
        cached = self._scores.get(cache_key)
        if cached is not None:
            return cached
        walk = self._raw_score(shard_id, "walk")
        bike = self._raw_score(shard_id, "bike")
        outside = (walk["network_status"] == NETWORK_STATUS_CODES["outside_mask"])
        walk_access = np.where(
            walk["service_access"] == DAILY_ACCESS_UINT16_MISSING,
            0.0,
            walk["service_access"].astype(np.float64) * DAILY_ACCESS_ACCESS_STEP,
        )
        bike_access = np.where(
            bike["service_access"] == DAILY_ACCESS_UINT16_MISSING,
            0.0,
            bike["service_access"].astype(np.float64) * DAILY_ACCESS_ACCESS_STEP,
        )
        best = np.maximum(walk_access, bike_access)
        second = np.minimum(walk_access, bike_access)
        access = np.minimum(
            best + DAILY_ACCESS_MODE_AWARE_SECONDARY_BONUS * second * (1.0 - best),
            1.0,
        )
        components = np.empty((len(CATEGORIES), *outside.shape), dtype=np.float64)
        components[0] = DAILY_ACCESS_CATEGORY_MAX["shops"] * (
            DAILY_ACCESS_SHOP_MIX["food"] * access[0]
            + DAILY_ACCESS_SHOP_MIX["other_retail"] * access[1]
        )
        components[1] = DAILY_ACCESS_CATEGORY_MAX["transport"] * access[5]
        components[2] = DAILY_ACCESS_CATEGORY_MAX["healthcare"] * (
            DAILY_ACCESS_HEALTHCARE_MIX["primary_care"] * access[2]
            + DAILY_ACCESS_HEALTHCARE_MIX["pharmacy"] * access[3]
        )
        components[3] = DAILY_ACCESS_CATEGORY_MAX["parks"] * access[4]
        penalties = np.where(
            walk["penalties"] == DAILY_ACCESS_UINT16_MISSING,
            0.0,
            walk["penalties"].astype(np.float64) * DAILY_ACCESS_SCORE_STEP,
        )
        bonuses = np.nan_to_num(decode(walk["bonuses"], DAILY_ACCESS_SCORE_STEP), nan=0.0)
        total = np.clip(components.sum(axis=0) + bonuses.sum(axis=0) - penalties.sum(axis=0), 0.0, 100.0)
        ok = (
            (walk["network_status"] == NETWORK_STATUS_CODES["ok"])
            | (bike["network_status"] == NETWORK_STATUS_CODES["ok"])
        )
        status = np.where(
            outside,
            NETWORK_STATUS_CODES["outside_mask"],
            np.where(ok, NETWORK_STATUS_CODES["ok"], NETWORK_STATUS_CODES["no_walking_network"]),
        ).astype(np.uint8)
        cached = {
            "total": self._quantize(total, DAILY_ACCESS_SCORE_STEP, missing=outside),
            "components": self._quantize(components, DAILY_ACCESS_SCORE_STEP, missing=outside[None, ...]),
            "penalties": self._quantize(penalties, DAILY_ACCESS_SCORE_STEP, missing=outside[None, ...]),
            "bonuses": self._quantize(bonuses, DAILY_ACCESS_SCORE_STEP, missing=outside[None, ...]),
            "service_access": self._quantize(access, DAILY_ACCESS_ACCESS_STEP, missing=outside[None, ...]),
            "network_status": status,
        }
        return self._scores.put(cache_key, cached)

    def _score(self, shard_id: str, mode: str = "walk") -> dict[str, np.ndarray]:
        return self._mode_aware_score(shard_id) if mode == "mode_aware" else self._raw_score(shard_id, mode)

    @staticmethod
    def _shard_origin(x: float, y: float) -> tuple[int, int]:
        step = SURFACE_SHARD_SIZE_M
        return int(math.floor(x / step) * step), int(math.floor(y / step) * step)

    def _channel_field(self, shard_id: str, channel: str) -> np.ndarray:
        mode, field, index, scale, _ = self.channels[channel]
        raw = self._score(shard_id, mode)[field]
        return decode(raw if index < 0 else raw[index], scale)

    def aggregated(self, shard_id: str, resolution_m: int, channel: str) -> tuple[np.ndarray, np.ndarray]:
        """(means, land area m²) of one channel at ``resolution_m`` for one shard."""
        key = (shard_id, int(resolution_m), channel)
        cached = self._aggregates.get(key)
        if cached is not None:
            return cached
        weights = np.asarray(self._shell(shard_id)["land_area_m2"], dtype=np.float64)
        factor = int(resolution_m) // CANONICAL_BASE_RESOLUTION_M
        if channel in self.network_coverage_modes:
            mode = self.network_coverage_modes[channel]
            status = self._score(shard_id, mode)["network_status"]
            values = np.where(status == NETWORK_STATUS_CODES["outside_mask"], np.nan, (status == NETWORK_STATUS_CODES["ok"]).astype(np.float64))
        else:
            values = self._channel_field(shard_id, channel)
        result = block_reduce_weighted(values, weights, max(factor, 1))
        return self._aggregates.put(key, result)

    # -- inspect -----------------------------------------------------------------

    @staticmethod
    def _mode_channel(mode: str, channel: str) -> str:
        return channel if mode == "walk" else f"{mode}_{channel}"

    def _canonical_mode(self, shard_id: str, row: int, col: int, mode: str) -> dict[str, Any]:
        score = self._score(shard_id, mode)
        status = _STATUS_BY_CODE.get(int(score["network_status"][row, col]), "outside_mask")
        land = float(self._shell(shard_id)["land_area_m2"][row, col])
        payload: dict[str, Any] = {
            "network_status": status,
            "land_area_m2": land,
            "total_score": None,
            "component_scores": None,
            "penalties": None,
            "service_access": None,
        }
        if status == "outside_mask" or land <= 0.0:
            return payload
        scale = DAILY_ACCESS_SCORE_STEP
        payload.update(
            {
                "total_score": float(score["total"][row, col]) * scale,
                "component_scores": {
                    category: float(score["components"][index, row, col]) * scale for index, category in enumerate(CATEGORIES)
                },
                "penalties": {
                    penalty: float(score["penalties"][index, row, col]) * scale for index, penalty in enumerate(PENALTIES)
                },
                "bonuses": {name: float(score["bonuses"][index, row, col]) * scale for index, name in enumerate(BONUSES)},
                "service_access": {
                    need: float(score["service_access"][index, row, col]) * DAILY_ACCESS_ACCESS_STEP
                    for index, need in enumerate(DAILY_ACCESS_NEEDS)
                },
            }
        )
        return payload

    def _context_at(self, shard_id: str, row: int, col: int) -> dict[str, Any]:
        with np.load(self.score_dir / "shards" / f"{shard_id}.npz", allow_pickle=False) as data:
            walk = {name: np.asarray(data[name]) for name in ("context_metrics", "station_labels", "station_distances", "pitch_ref", "pitch_reason", "pitch_distance") if name in data}
        payload = {}
        if "context_metrics" in walk:
            from .daily_access_context import METRICS
            stations = {}
            for index, name in enumerate(("rail", "tram")):
                label = int(walk["station_labels"][index, row, col])
                distance = float(walk["station_distances"][index, row, col])
                entries = self.station_labels.get(f"{name}_station", [])
                source = next((entry for entry in entries if entry["label"] == label), None)
                stations[name] = {"source_ref": source.get("key") if source else None,
                                  "distance_m": distance if math.isfinite(distance) else None}
            payload["context"] = {
                **dict(zip(METRICS, (float(v) for v in walk["context_metrics"][row, col]))),
                "stations": stations,
                "pitch": {"source_ref": str(walk["pitch_ref"][row, col]) or None,
                          "reason": str(walk["pitch_reason"][row, col]) or None,
                          "distance_m": float(walk["pitch_distance"][row, col]) if np.isfinite(walk["pitch_distance"][row, col]) else None},
            }
        return payload

    def _canonical(self, shard_id: str, row: int, col: int) -> dict[str, Any]:
        modes = {
            mode: self._canonical_mode(shard_id, row, col, mode)
            for mode in self.available_modes
        }
        context = self._context_at(shard_id, row, col)
        for payload in modes.values():
            payload.update(context)
        return {
            **modes["walk"],
            "available_modes": list(self.available_modes),
            "default_mode": DAILY_ACCESS_DEFAULT_MODE,
            "modes": modes,
        }

    def _visible(self, metric_x: float, metric_y: float, resolution_m: int) -> dict[str, Any]:
        """Aggregate of the canonical cells of the visible cell (may span shards)."""
        cell_x = math.floor(metric_x / resolution_m) * resolution_m
        cell_y = math.floor(metric_y / resolution_m) * resolution_m
        sums: dict[str, float] = {}
        land_by_channel: dict[str, float] = {}
        land_total = 0.0
        for shard_id, row0, row1, col0, col1 in self._windows(cell_x, cell_y, cell_x + resolution_m, cell_y + resolution_m):
            weights = np.asarray(self._shell(shard_id)["land_area_m2"][row0:row1, col0:col1], dtype=np.float64)
            status = self._score(shard_id, "walk")["network_status"][row0:row1, col0:col1]
            weights = np.where(status == NETWORK_STATUS_CODES["outside_mask"], 0.0, weights)
            land_total += float(weights.sum())
            for coverage_channel, mode in self.network_coverage_modes.items():
                mode_status = self._score(shard_id, mode)["network_status"][row0:row1, col0:col1]
                covered = (mode_status == NETWORK_STATUS_CODES["ok"]).astype(np.float64)
                sums[coverage_channel] = sums.get(coverage_channel, 0.0) + float((covered * weights).sum())
                land_by_channel[coverage_channel] = land_by_channel.get(coverage_channel, 0.0) + float(weights.sum())
            for channel, (mode, field, index, scale, _maximum) in self.channels.items():
                raw = self._score(shard_id, mode)[field]
                values = decode((raw if index < 0 else raw[index])[row0:row1, col0:col1], scale)
                finite = np.isfinite(values)
                sums[channel] = sums.get(channel, 0.0) + float(np.where(finite, values * weights, 0.0).sum())
                land_by_channel[channel] = land_by_channel.get(channel, 0.0) + float(np.where(finite, weights, 0.0).sum())

        def mean(channel: str) -> float | None:
            land = land_by_channel.get(channel, 0.0)
            return sums[channel] / land if land > 0.0 else None

        coverage = mean("network_coverage")
        modes: dict[str, dict[str, Any]] = {}
        for mode in self.available_modes:
            prefix = "" if mode == "walk" else f"{mode}_"
            mode_coverage = mean(f"{prefix}network_coverage")
            modes[mode] = {
                "network_status": aggregate_status(mode_coverage, land_total),
                "network_coverage_share": mode_coverage,
                "total_score": mean(self._mode_channel(mode, "total")),
                "component_scores": {
                    category: mean(self._mode_channel(mode, category)) for category in CATEGORIES
                },
                "penalties": {
                    penalty: mean(self._mode_channel(mode, penalty)) for penalty in PENALTIES
                },
                "bonuses": {name: mean(self._mode_channel(mode, name)) for name in BONUSES},
                "service_access": {
                    need: mean(self._mode_channel(mode, f"access_{need}"))
                    for need in DAILY_ACCESS_NEEDS
                },
            }
        visible: dict[str, Any] = {
            "resolution_m": int(resolution_m),
            "cell_min_x_m": int(cell_x),
            "cell_min_y_m": int(cell_y),
            "land_area_m2": land_total,
            "network_status": aggregate_status(coverage, land_total),
            "network_coverage_share": coverage,
            **modes["walk"],
            "modes": modes,
        }
        return visible

    def _windows(self, min_x: float, min_y: float, max_x: float, max_y: float) -> Iterator[tuple[str, int, int, int, int]]:
        step = CANONICAL_BASE_RESOLUTION_M
        shard = SURFACE_SHARD_SIZE_M
        for shard_y in range(int(math.floor(min_y / shard) * shard), int(max_y), shard):
            for shard_x in range(int(math.floor(min_x / shard) * shard), int(max_x), shard):
                shard_id = f"{shard_x}_{shard_y}"
                if shard_id not in self.shard_ids:
                    continue
                rows, cols = self._shell(shard_id)["land_area_m2"].shape
                col0 = max(0, int(math.floor((min_x - shard_x) / step)))
                col1 = min(cols, int(math.ceil((max_x - shard_x) / step)))
                row0 = max(0, int(math.floor((min_y - shard_y) / step)))
                row1 = min(rows, int(math.ceil((max_y - shard_y) / step)))
                if col0 < col1 and row0 < row1:
                    yield shard_id, row0, row1, col0, col1

    def inspect(self, *, lat: float, lon: float, zoom: float | None = None) -> dict[str, Any]:
        metric_x, metric_y = TO_TARGET(float(lon), float(lat))
        step = CANONICAL_BASE_RESOLUTION_M
        cell_x = int(math.floor(metric_x / step) * step)
        cell_y = int(math.floor(metric_y / step) * step)
        shard_x, shard_y = self._shard_origin(cell_x, cell_y)
        shard_id = f"{shard_x}_{shard_y}"
        payload: dict[str, Any] = {
            "scoring_model": DAILY_ACCESS_SCORE_MODEL,
            "lat": float(lat),
            "lon": float(lon),
            "resolution_m": step,
            "cell_id": f"{step}:{cell_x}:{cell_y}",
            "valid_land": False,
            "network_status": "outside_mask",
            "total_score": None,
            "component_scores": None,
            "penalties": None,
            "service_access": None,
            "visible_resolution_m": None,
            "visible": None,
            "legacy_fields_unavailable": list(LEGACY_UNAVAILABLE_FIELDS),
            **{field: None for field in LEGACY_UNAVAILABLE_FIELDS},
        }
        if zoom is not None:
            resolution = int(resolution_for_zoom(zoom, profile=self.profile))
            payload["visible_resolution_m"] = resolution
            payload["visible"] = self._visible(metric_x, metric_y, resolution)
        if shard_id not in self.shard_ids:
            return payload
        row = int((cell_y - shard_y) // step)
        col = int((cell_x - shard_x) // step)
        canonical = self._canonical(shard_id, row, col)
        payload.update(canonical)
        payload["valid_land"] = canonical["land_area_m2"] > 0.0 and canonical["network_status"] != "outside_mask"
        return payload

    # -- tiles -----------------------------------------------------------------------

    def render_tile(self, *, resolution_m: int, z: int, x: int, y: int, channel: str = "total") -> bytes:
        """PNG of one channel (only that channel is aggregated and cached)."""
        from .surface import _metric_tile_bounds, _tile_pixel_metric_coordinates, colorize_scores, encode_png_rgba

        if channel not in self.channels and channel not in self.network_coverage_modes:
            raise ValueError(f"Unsupported daily access channel: {channel}")
        cache_path = None
        if self.tile_dir is not None:
            cache_path = self.tile_dir / "v4" / channel / str(int(resolution_m)) / str(int(z)) / str(int(x)) / f"{int(y)}.png"
            if cache_path.exists():
                return cache_path.read_bytes()
        max_value = (
            1.0
            if channel in self.network_coverage_modes
            else self.channels[channel][4]
        )
        bounds = _metric_tile_bounds(z, x, y)
        metric_x, metric_y = _tile_pixel_metric_coordinates(z, x, y)
        block = int(resolution_m)
        pixel_block_x = np.floor(metric_x / block).astype(np.int64) * block
        pixel_block_y = np.floor(metric_y / block).astype(np.int64) * block
        values = np.full((SURFACE_TILE_SIZE_PX, SURFACE_TILE_SIZE_PX), np.nan, dtype=np.float32)
        shard = SURFACE_SHARD_SIZE_M
        for shard_y in range(int(math.floor(bounds[1] / shard) * shard), int(bounds[3]) + 1, shard):
            for shard_x in range(int(math.floor(bounds[0] / shard) * shard), int(bounds[2]) + 1, shard):
                shard_id = f"{shard_x}_{shard_y}"
                if shard_id not in self.shard_ids:
                    continue
                means, _weights = self.aggregated(shard_id, block, channel)
                inside = (
                    (pixel_block_x >= shard_x) & (pixel_block_x < shard_x + shard)
                    & (pixel_block_y >= shard_y) & (pixel_block_y < shard_y + shard)
                )
                if not np.any(inside):
                    continue
                local_cols = ((pixel_block_x[inside] - shard_x) // block).astype(np.int64)
                local_rows = ((pixel_block_y[inside] - shard_y) // block).astype(np.int64)
                ok = (local_rows < means.shape[0]) & (local_cols < means.shape[1])
                picked = np.full(local_rows.shape, np.nan, dtype=np.float64)
                picked[ok] = means[local_rows[ok], local_cols[ok]]
                values[inside] = picked.astype(np.float32)
        # Colour on the shared 0..100 ramp: each channel relative to its maximum.
        scaled = values * np.float32(100.0 / max_value)
        payload = encode_png_rgba(colorize_scores(scaled.ravel()).reshape(SURFACE_TILE_SIZE_PX, SURFACE_TILE_SIZE_PX, 4))
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile("wb", dir=cache_path.parent, delete=False, suffix=".tmp")
            try:
                handle.write(payload)
                handle.close()
                os.replace(handle.name, cache_path)
            finally:
                if os.path.exists(handle.name):
                    os.unlink(handle.name)
        return payload


__all__ = [
    "ByteLRU",
    "CHANNELS",
    "DailyAccessSurfaceRuntime",
    "LEGACY_UNAVAILABLE_FIELDS",
    "aggregate_status",
    "block_reduce_weighted",
    "decode",
]
