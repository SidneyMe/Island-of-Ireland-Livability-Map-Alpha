from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable


SUPPORTED_METRICS = frozenset({"Lden", "Lnight"})
NO_DATA_GRIDCODE = 1000

# Verified from the source archives:
# - Round 1 uses class codes (1..7) with Noise_Cl labels.
# - Round 2/3 use threshold-style GRIDCODE values.
_NI_ROUND1_CLASS_BANDS: dict[str, dict[int, tuple[float, float, str]]] = {
    "Lden": {
        1: (45.0, 49.0, "45-49"),  # Noise_Cl "< 50"
        2: (50.0, 54.0, "50-54"),
        3: (55.0, 59.0, "55-59"),
        4: (60.0, 64.0, "60-64"),
        5: (65.0, 69.0, "65-69"),
        6: (70.0, 74.0, "70-74"),
        7: (75.0, 99.0, "75+"),    # Noise_Cl ">= 75"
    },
    "Lnight": {
        1: (45.0, 49.0, "45-49"),  # Noise_Cl "< 45" (clipped to canonical floor)
        2: (45.0, 49.0, "45-49"),
        3: (50.0, 54.0, "50-54"),
        4: (55.0, 59.0, "55-59"),
        5: (60.0, 64.0, "60-64"),
        6: (65.0, 69.0, "65-69"),
        7: (70.0, 99.0, "70+"),    # Noise_Cl ">= 70"
    },
}

_NI_THRESHOLD_GRIDCODE_BANDS: dict[int, tuple[float, float, str]] = {
    45: (45.0, 49.0, "45-49"),
    49: (50.0, 54.0, "50-54"),
    50: (50.0, 54.0, "50-54"),
    54: (55.0, 59.0, "55-59"),
    59: (60.0, 64.0, "60-64"),
    64: (65.0, 69.0, "65-69"),
    69: (70.0, 74.0, "70-74"),
    74: (75.0, 99.0, "75+"),
}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_noise_band(
    value: Any = None,
    *,
    db_low: float | int | str | None = None,
    db_high: float | int | str | None = None,
) -> tuple[float | None, float | None, str]:
    low = _to_float(db_low)
    high = _to_float(db_high)
    text = str(value or "").strip()
    if text.endswith("+"):
        low = _to_float(text[:-1])
        high = 99.0 if low is not None else high
        match = None
    else:
        match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)?\s*$", text)
    if match:
        low = float(match.group(1))
        if match.group(2):
            high = float(match.group(2))

    if low is None and high is None:
        return None, None, text
    if low is None:
        low = high
    if high is None:
        high = low
    if high >= 99:
        return low, high, f"{int(round(low))}+"
    return low, high, f"{int(round(low))}-{int(round(high))}"


def _metric_from_value(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized == "lden":
        return "Lden"
    if normalized in {"lnight", "lngt", "lnght"}:
        return "Lnight"
    return None


def _metric_from_ni_member(member: str) -> str | None:
    name = Path(member).stem.lower()
    if "_lden" in name:
        return "Lden"
    if any(token in name for token in ("_lnight", "_lngt", "_lnght")):
        return "Lnight"
    return None


def _source_type_from_ni_member(member: str) -> str | None:
    parts = [part.lower() for part in Path(member).parts]
    joined = "/".join(parts)
    if "consolidated" in parts:
        return "consolidated"
    if "industry" in parts:
        return "industry"
    if "roads" in parts or "major_roads" in parts or "mroad" in joined:
        return "road"
    if "rail" in parts or "major_rail" in parts or "mrail" in joined:
        return "rail"
    if "major_airports" in parts or "bca" in parts or "bia" in parts:
        return "airport"
    return None


def _find_noise_class_column(columns: Iterable[str]) -> str | None:
    for name in columns:
        lowered = str(name).lower()
        if lowered in {"noise_cl", "noiseclass", "noise_class"}:
            return str(name)
        if lowered.startswith("noise_cl"):
            return str(name)
    return None


def normalize_ni_gridcode_band(
    gridcode: Any,
    *,
    round_number: int | None = None,
    source_type: str | None = None,
    metric: str | None = None,
) -> tuple[float | None, float | None, str | None]:
    code = _to_float(gridcode)
    if code is None:
        return None, None, None
    code_int = int(round(code))
    if code_int == NO_DATA_GRIDCODE:
        return None, None, None

    source_label = source_type or "unknown source"
    metric_name = _metric_from_value(metric) if metric is not None else None
    metric_label = metric_name or (str(metric) if metric is not None else "unknown metric")

    if round_number == 1:
        mapping = _NI_ROUND1_CLASS_BANDS.get(metric_name or "")
        if mapping is None or code_int not in mapping:
            raise ValueError(
                f"NI Round 1 gridcode {code_int} for {source_label} {metric_label} "
                "is a class code; missing mapping"
            )
        return mapping[code_int]

    mapped = _NI_THRESHOLD_GRIDCODE_BANDS.get(code_int)
    if mapped is not None:
        return mapped

    if round_number in {2, 3}:
        raise ValueError(
            f"NI Round {round_number} gridcode {code_int} for {source_label} {metric_label} "
            "has no verified threshold mapping"
        )

    # Backward-compatible fallback for unknown rounds in direct callers/tests.
    low = float(code_int + 1)
    high = 99.0 if low >= 75 else float(code_int + 5)
    _, _, label = normalize_noise_band(db_low=low, db_high=high)
    return low, high, label
