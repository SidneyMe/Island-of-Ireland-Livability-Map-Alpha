from __future__ import annotations

from typing import Any

from config import (
    MODE_AWARE_SCORING_ENABLED,
    MODE_AWARE_SCORE_WEIGHTS,
    SCORING_MODEL_VERSION,
)


SCORE_MODES = ("walk", "bike", "transit")
MODE_AWARE_KEY = "mode_aware"


def _positive_units(units: dict[str, Any] | None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for category, value in dict(units or {}).items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric > 0.0:
            normalized[str(category)] = numeric
    return normalized


def _component_scores(
    *,
    score_cell,
    counts: dict[str, int],
    cluster_counts: dict[str, int],
    effective_area_ratio: float,
    effective_units: dict[str, float],
) -> dict[str, float]:
    scores, _ = score_cell(
        counts,
        cluster_counts=cluster_counts,
        effective_area_ratio=effective_area_ratio,
        effective_units=effective_units,
        railway_proximity_penalty=0.0,
        road_proximity_penalty=0.0,
    )
    return {
        category: float(score)
        for category, score in scores.items()
        if category not in {"railway_proximity", "road_proximity"}
    }


def build_mode_aware_payload(
    *,
    score_cell,
    counts: dict[str, int],
    cluster_counts: dict[str, int],
    effective_area_ratio: float,
    walk_effective_units: dict[str, float] | None,
    walk_component_scores: dict[str, float],
    walk_total_score: float,
    bike_effective_units: dict[str, float] | None = None,
    transit_effective_units: dict[str, float] | None = None,
    railway_proximity_penalty: float = 0.0,
    road_proximity_penalty: float = 0.0,
) -> dict[str, Any]:
    """Return a validation payload without changing the published walk score.

    The transport category is intentionally walk-access only. Transit-chained
    scoring describes what amenities can be reached after using transit; it
    must not reward the transport category through itself.
    """

    walk_units = _positive_units(walk_effective_units)
    mode_units = {
        "walk": walk_units,
        "bike": _positive_units(bike_effective_units),
        "transit": {
            category: value
            for category, value in _positive_units(transit_effective_units).items()
            if category != "transport"
        },
    }
    mode_available = {
        "walk": True,
        "bike": bool(mode_units["bike"]),
        "transit": bool(mode_units["transit"]),
    }
    mode_scores: dict[str, dict[str, float]] = {
        "walk": {
            category: float(score)
            for category, score in walk_component_scores.items()
            if category not in {"railway_proximity", "road_proximity"}
        }
    }
    for mode in ("bike", "transit"):
        mode_scores[mode] = _component_scores(
            score_cell=score_cell,
            counts=counts,
            cluster_counts=cluster_counts,
            effective_area_ratio=effective_area_ratio,
            effective_units=mode_units[mode],
        )

    categories = tuple(mode_scores["walk"])
    blended_scores: dict[str, float] = {}
    for category in categories:
        weighted_total = 0.0
        available_weight = 0.0
        for mode in SCORE_MODES:
            if category == "transport" and mode == "transit":
                continue
            if mode != "walk" and not mode_available[mode]:
                continue
            weight = float(MODE_AWARE_SCORE_WEIGHTS.get(mode, 0.0))
            if weight <= 0.0:
                continue
            weighted_total += float(mode_scores[mode].get(category, 0.0)) * weight
            available_weight += weight
        blended_scores[category] = (
            weighted_total / available_weight if available_weight > 0.0 else 0.0
        )

    blended_scores["railway_proximity"] = -max(float(railway_proximity_penalty), 0.0)
    blended_scores["road_proximity"] = -max(float(road_proximity_penalty), 0.0)
    total_score = max(sum(blended_scores.values()), 0.0)

    return {
        "enabled": bool(MODE_AWARE_SCORING_ENABLED),
        "available": bool(MODE_AWARE_SCORING_ENABLED and (mode_available["bike"] or mode_available["transit"])),
        "scoring_model_version": SCORING_MODEL_VERSION,
        "weights": {mode: float(MODE_AWARE_SCORE_WEIGHTS.get(mode, 0.0)) for mode in SCORE_MODES},
        "mode_available": mode_available,
        "mode_effective_units": mode_units,
        "mode_component_scores": mode_scores,
        "component_scores": blended_scores,
        "total_score": float(total_score),
        "walk_only_total_score": float(walk_total_score),
    }
