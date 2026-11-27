"""Daily access score (v4): curves, aggregation and quantisation (reference).

All math is plain float64 Python so the Rust port can be compared to it
line by line. A cell's inputs are, per need, candidate options
``(label, quality, distance_m)`` where ``distance_m`` already includes the
object connector, the network path and the cell connector.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping

from config import (
    DAILY_ACCESS_ACCESS_STEP,
    DAILY_ACCESS_CATEGORY_MAX,
    DAILY_ACCESS_DISTANCE_MISSING,
    DAILY_ACCESS_DISTANCE_STEP_M,
    DAILY_ACCESS_DEFAULT_MODE,
    DAILY_ACCESS_HEALTHCARE_MIX,
    DAILY_ACCESS_LIMITED_ACCESS_THRESHOLD,
    DAILY_ACCESS_MODE_AWARE_SECONDARY_BONUS,
    DAILY_ACCESS_MODE_SPEED_M_PER_MIN,
    DAILY_ACCESS_NEED_THRESHOLDS_MIN,
    DAILY_ACCESS_NEEDS,
    DAILY_ACCESS_SCORE_STEP,
    DAILY_ACCESS_SHOP_MIX,
    DAILY_ACCESS_TOP3_WEIGHTS,
    DAILY_ACCESS_TOP_K,
    DAILY_ACCESS_UINT16_MISSING,
)


from daily_access_contract import CATEGORIES, PENALTIES, BONUSES, SCORE_CHANNEL_SCHEMA


@dataclass(frozen=True)
class CellScore:
    total: float
    components: dict[str, float]
    service_access: dict[str, float]
    penalties: dict[str, float]
    bonuses: dict[str, float] = field(default_factory=lambda: {name: 0.0 for name in BONUSES})


def need_thresholds_m(
    need: str,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
) -> tuple[float, float]:
    normalized_mode = str(mode).strip().lower()
    try:
        speed_m_per_min = float(DAILY_ACCESS_MODE_SPEED_M_PER_MIN[normalized_mode])
    except KeyError as exc:
        raise ValueError(f"Unsupported daily access mode: {mode!r}") from exc
    full_min, zero_min = DAILY_ACCESS_NEED_THRESHOLDS_MIN[need]
    return (
        float(full_min) * speed_m_per_min,
        float(zero_min) * speed_m_per_min,
    )


def decay(distance_m: float, need: str, mode: str = DAILY_ACCESS_DEFAULT_MODE) -> float:
    """1 up to the full threshold, 0 after the zero threshold, smoothstep between."""
    full_m, zero_m = need_thresholds_m(need, mode)
    distance = float(distance_m)
    if distance <= full_m:
        return 1.0
    if distance >= zero_m:
        return 0.0
    x = (distance - full_m) / (zero_m - full_m)
    return 1.0 - 3.0 * x * x + 2.0 * x * x * x


def top3_access(contributions: Iterable[float]) -> float:
    """``0.80 u1 + 0.15 u2 + 0.05 u3`` over the best contributions."""
    ordered = sorted((float(value) for value in contributions), reverse=True)[:DAILY_ACCESS_TOP_K]
    return sum(weight * value for weight, value in zip(DAILY_ACCESS_TOP3_WEIGHTS, ordered))


def need_access(
    need: str,
    options: Iterable[tuple[object, float, float]],
    *,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
) -> float:
    """Access in [0, 1] for one need from ``(label, quality, distance_m)`` options.

    Options sharing a label (one logical place / one green root) collapse to
    their best contribution after the curve. Transport takes the single best
    stop; every other need takes the top three distinct labels.
    """
    best_by_label: dict[object, float] = {}
    for label, quality, distance_m in options:
        contribution = float(quality) * decay(distance_m, need, mode)
        if contribution > best_by_label.get(label, -1.0):
            best_by_label[label] = contribution
    if not best_by_label:
        return 0.0
    if need == "transport":
        return max(best_by_label.values())
    return top3_access(best_by_label.values())


def category_components(service_access: Mapping[str, float]) -> dict[str, float]:
    access = {need: float(service_access.get(need, 0.0)) for need in DAILY_ACCESS_NEEDS}
    return {
        "shops": DAILY_ACCESS_CATEGORY_MAX["shops"]
        * (
            DAILY_ACCESS_SHOP_MIX["food"] * access["food"]
            + DAILY_ACCESS_SHOP_MIX["other_retail"] * access["other_retail"]
        ),
        "transport": DAILY_ACCESS_CATEGORY_MAX["transport"] * access["transport"],
        "healthcare": DAILY_ACCESS_CATEGORY_MAX["healthcare"]
        * (
            DAILY_ACCESS_HEALTHCARE_MIX["primary_care"] * access["primary_care"]
            + DAILY_ACCESS_HEALTHCARE_MIX["pharmacy"] * access["pharmacy"]
        ),
        "parks": DAILY_ACCESS_CATEGORY_MAX["parks"] * access["green"],
    }


def total_score(
    components: Mapping[str, float],
    *,
    railway_penalty: float = 0.0,
    road_penalty: float = 0.0,
) -> float:
    """Sum of components minus (positive) penalties, clamped to 0..100."""
    raw = sum(float(components[category]) for category in CATEGORIES)
    raw -= max(float(railway_penalty), 0.0) + max(float(road_penalty), 0.0)
    return min(max(raw, 0.0), 100.0)


def score_cell_options(
    options_by_need: Mapping[str, Iterable[tuple[object, float, float]]],
    *,
    mode: str = DAILY_ACCESS_DEFAULT_MODE,
    railway_penalty: float = 0.0,
    road_penalty: float = 0.0,
) -> CellScore:
    service_access = {
        need: need_access(need, options_by_need.get(need, ()), mode=mode)
        for need in DAILY_ACCESS_NEEDS
    }
    components = category_components(service_access)
    return CellScore(
        total=total_score(components, railway_penalty=railway_penalty, road_penalty=road_penalty),
        components=components,
        service_access=service_access,
        penalties={
            "railway_proximity": max(float(railway_penalty), 0.0),
            "road_proximity": max(float(road_penalty), 0.0),
        },
    )


def combine_mode_access(
    walk_access: Mapping[str, float],
    bike_access: Mapping[str, float],
    *,
    secondary_bonus: float = DAILY_ACCESS_MODE_AWARE_SECONDARY_BONUS,
) -> dict[str, float]:
    """Combine independently scored access without ever reducing the best mode."""
    bonus = min(max(float(secondary_bonus), 0.0), 1.0)
    combined: dict[str, float] = {}
    for need in DAILY_ACCESS_NEEDS:
        walk = min(max(float(walk_access.get(need, 0.0)), 0.0), 1.0)
        bike = min(max(float(bike_access.get(need, 0.0)), 0.0), 1.0)
        best, second = max(walk, bike), min(walk, bike)
        combined[need] = min(best + bonus * second * (1.0 - best), 1.0)
    return combined


def score_mode_access(
    walk_access: Mapping[str, float],
    bike_access: Mapping[str, float],
    *,
    railway_penalty: float = 0.0,
    road_penalty: float = 0.0,
    secondary_bonus: float = DAILY_ACCESS_MODE_AWARE_SECONDARY_BONUS,
) -> CellScore:
    service_access = combine_mode_access(
        walk_access,
        bike_access,
        secondary_bonus=secondary_bonus,
    )
    components = category_components(service_access)
    return CellScore(
        total=total_score(
            components,
            railway_penalty=railway_penalty,
            road_penalty=road_penalty,
        ),
        components=components,
        service_access=service_access,
        penalties={
            "railway_proximity": max(float(railway_penalty), 0.0),
            "road_proximity": max(float(road_penalty), 0.0),
        },
    )


def zero_cell_score() -> CellScore:
    return CellScore(
        total=0.0,
        components={category: 0.0 for category in CATEGORIES},
        service_access={need: 0.0 for need in DAILY_ACCESS_NEEDS},
        penalties={penalty: 0.0 for penalty in PENALTIES},
    )


# --- quantisation ----------------------------------------------------------


def _round_half_up(value: float, scale: int) -> int:
    return int(math.floor(float(value) * scale + 0.5))


_SCORE_SCALE = int(round(1.0 / DAILY_ACCESS_SCORE_STEP))
_ACCESS_SCALE = int(round(1.0 / DAILY_ACCESS_ACCESS_STEP))
_DISTANCE_SCALE = int(round(1.0 / DAILY_ACCESS_DISTANCE_STEP_M))


def quantize_score(value: float) -> int:
    """Points -> uint16 code with a 0.01 step (half up)."""
    code = _round_half_up(value, _SCORE_SCALE)
    if not 0 <= code < DAILY_ACCESS_UINT16_MISSING:
        raise ValueError(f"score {value!r} does not fit the uint16 code")
    return code


def quantize_access(value: float) -> int:
    """Access in [0, 1] -> uint16 code with a 0.0001 step (half up)."""
    code = _round_half_up(value, _ACCESS_SCALE)
    if not 0 <= code < DAILY_ACCESS_UINT16_MISSING:
        raise ValueError(f"access {value!r} does not fit the uint16 code")
    return code


def quantize_distance(distance_m: float) -> int:
    """Metres -> uint16 code with a 0.1 m step; too far -> missing."""
    code = _round_half_up(distance_m, _DISTANCE_SCALE)
    if code < 0:
        raise ValueError(f"negative distance {distance_m!r}")
    if code >= DAILY_ACCESS_DISTANCE_MISSING:
        return DAILY_ACCESS_DISTANCE_MISSING
    return code


def dequantize_score(code: int) -> float:
    return int(code) / _SCORE_SCALE


def dequantize_access(code: int) -> float:
    return int(code) / _ACCESS_SCALE


def dequantize_distance(code: int) -> float | None:
    if int(code) == DAILY_ACCESS_DISTANCE_MISSING:
        return None
    return int(code) / _DISTANCE_SCALE


def quantize_cell_score(score: CellScore) -> dict[str, object]:
    """Total and components rounded independently, after clamping the total."""
    return {
        "total": quantize_score(score.total),
        "components": {category: quantize_score(score.components[category]) for category in CATEGORIES},
        "penalties": {penalty: quantize_score(score.penalties.get(penalty, 0.0)) for penalty in PENALTIES},
        "bonuses": {name: quantize_score(score.bonuses.get(name, 0.0)) for name in BONUSES},
        "service_access": {
            need: quantize_access(score.service_access[need]) for need in DAILY_ACCESS_NEEDS
        },
    }


def access_status(value: float) -> str:
    if float(value) <= 0.0:
        return "no_options"
    if float(value) < DAILY_ACCESS_LIMITED_ACCESS_THRESHOLD:
        return "limited"
    return "options_found"


__all__ = [
    "CATEGORIES",
    "CellScore",
    "PENALTIES",
    "access_status",
    "category_components",
    "combine_mode_access",
    "decay",
    "dequantize_access",
    "dequantize_distance",
    "dequantize_score",
    "need_access",
    "need_thresholds_m",
    "quantize_access",
    "quantize_cell_score",
    "quantize_distance",
    "quantize_score",
    "score_cell_options",
    "score_mode_access",
    "top3_access",
    "total_score",
    "zero_cell_score",
]
