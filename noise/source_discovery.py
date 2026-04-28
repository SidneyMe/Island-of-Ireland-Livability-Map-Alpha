from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any

from .normalize import (
    SUPPORTED_METRICS,
    _find_noise_class_column,
    _metric_from_ni_member,
    _source_type_from_ni_member,
)
from .signature import NOISE_DATA_DIR, NI_ZIP_BY_ROUND


def _preferred_ni_entries(entries: list[str]) -> list[str]:
    grouped: dict[tuple[str, str, str], list[str]] = {}
    for member in entries:
        metric = _metric_from_ni_member(member)
        source_type = _source_type_from_ni_member(member)
        if metric is None or source_type is None:
            continue
        parent = str(Path(member).parent).lower()
        key = (parent, source_type, metric)
        grouped.setdefault(key, []).append(member)

    selected: list[str] = []
    for members in grouped.values():
        normalized = sorted(members)
        lngt_members = [member for member in normalized if "_lngt" in Path(member).stem.lower()]
        if lngt_members:
            normalized = [
                member
                for member in normalized
                if "_lnght" not in Path(member).stem.lower()
            ]
        nom_members = [member for member in normalized if "_nom" in Path(member).stem.lower()]
        if nom_members:
            normalized = [
                member
                for member in normalized
                if "_all" not in Path(member).stem.lower()
            ]
        selected.extend(normalized)
    return sorted(set(selected))


def ni_round1_class_snapshot(
    *,
    data_dir: Path = NOISE_DATA_DIR,
) -> list[dict[str, Any]]:
    """
    Return NI Round 1 class-coded grid metadata rows.

    Reads only attributes (no geometries) and returns rows with:
      source_layer, source_type, metric, raw_gridcode, noise_class_label, row_count
    """
    try:
        import pyogrio
    except ImportError as exc:  # pragma: no cover - depends on installed deps
        raise RuntimeError("pyogrio is required to inspect NI Round 1 class metadata.") from exc

    zip_name = NI_ZIP_BY_ROUND[1]
    zip_path = Path(data_dir) / zip_name
    if not zip_path.exists():
        raise FileNotFoundError(f"NI Round 1 archive not found: {zip_path}")

    with zipfile.ZipFile(zip_path) as zip_file:
        members = [
            entry.filename
            for entry in zip_file.infolist()
            if not entry.is_dir() and entry.filename.lower().endswith(".shp")
        ]

    rows: list[dict[str, Any]] = []
    for member in _preferred_ni_entries(members):
        metric = _metric_from_ni_member(member)
        source_type = _source_type_from_ni_member(member)
        if metric not in SUPPORTED_METRICS or source_type is None:
            continue

        src = f"/vsizip/{zip_path.resolve().as_posix()}/{member}"
        frame = pyogrio.read_dataframe(src, read_geometry=False)
        if frame.empty:
            continue

        grid_col = "gridcode" if "gridcode" in frame.columns else ("GRIDCODE" if "GRIDCODE" in frame.columns else None)
        class_col = _find_noise_class_column(frame.columns)
        if grid_col is None or class_col is None:
            continue

        pairs = (
            frame[[grid_col, class_col]]
            .dropna()
            .groupby([grid_col, class_col], dropna=False)
            .size()
            .reset_index(name="row_count")
            .sort_values([grid_col, class_col])
        )
        for _, item in pairs.iterrows():
            rows.append(
                {
                    "source_dataset": zip_name,
                    "source_layer": member,
                    "source_type": source_type,
                    "metric": metric,
                    "raw_gridcode": int(item[grid_col]),
                    "noise_class_label": str(item[class_col]).strip(),
                    "row_count": int(item["row_count"]),
                }
            )

    rows.sort(
        key=lambda item: (
            item["source_layer"],
            item["metric"],
            int(item["raw_gridcode"]),
            item["noise_class_label"],
        )
    )
    return rows
