from __future__ import annotations

import csv
import json
import subprocess
from collections.abc import Callable
from datetime import date
from pathlib import Path

from config import (
    GTFS_ANALYSIS_WINDOW_DAYS,
    GTFS_LOOKAHEAD_DAYS,
    GTFS_SCHOOL_AM_END_HOUR,
    GTFS_SCHOOL_AM_START_HOUR,
    GTFS_SCHOOL_KEYWORDS,
    GTFS_SCHOOL_PM_END_HOUR,
    GTFS_SCHOOL_PM_START_HOUR,
    GTFS_SERVICE_DESERT_WINDOW_DAYS,
    TRANSIT_REALITY_ALGO_VERSION,
    WALKGRAPH_BIN,
    TransitFeedState,
)
from walkgraph_support import ensure_walkgraph_subcommand_available, walkgraph_runtime_error

from .models import GtfsStopReality


ProgressFn = Callable[..., None] | None


def _emit_progress(progress_cb: ProgressFn, detail: str) -> None:
    if progress_cb is None:
        return
    progress_cb("detail", detail=detail, force_log=True)


def _iter_stderr_lines(process: subprocess.Popen[str]):
    if process.stderr is None:
        return []
    for line in process.stderr:
        text = line.rstrip()
        if text:
            yield text


def run_walkgraph_gtfs_refresh(
    *,
    feed_states: tuple[TransitFeedState, ...],
    import_fingerprint: str,
    reality_fingerprint: str,
    output_dir: Path,
    walkgraph_bin: str = WALKGRAPH_BIN,
    progress_cb: ProgressFn = None,
) -> Path:
    ensure_walkgraph_subcommand_available(walkgraph_bin, "gtfs-refresh")
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir.parent / "gtfs_refresh_config.json"
    config_path.write_text(
        json.dumps(
            {
                "analysis_date": feed_states[0].analysis_date.isoformat(),
                "analysis_window_days": GTFS_ANALYSIS_WINDOW_DAYS,
                "service_desert_window_days": GTFS_SERVICE_DESERT_WINDOW_DAYS,
                "lookahead_days": GTFS_LOOKAHEAD_DAYS,
                "matcher_version": TRANSIT_REALITY_ALGO_VERSION,
                "import_fingerprint": import_fingerprint,
                "reality_fingerprint": reality_fingerprint,
                "school_keywords": list(GTFS_SCHOOL_KEYWORDS),
                "school_am_start_hour": GTFS_SCHOOL_AM_START_HOUR,
                "school_am_end_hour": GTFS_SCHOOL_AM_END_HOUR,
                "school_pm_start_hour": GTFS_SCHOOL_PM_START_HOUR,
                "school_pm_end_hour": GTFS_SCHOOL_PM_END_HOUR,
                "feeds": [
                    {
                        "feed_id": feed_state.feed_id,
                        "label": feed_state.label,
                        "zip_path": str(feed_state.zip_path),
                        "feed_fingerprint": feed_state.feed_fingerprint,
                        "source_url": feed_state.source_url,
                    }
                    for feed_state in feed_states
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    command = [
        walkgraph_bin,
        "gtfs-refresh",
        "--config-json",
        str(config_path),
        "--out-dir",
        str(output_dir),
    ]
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise walkgraph_runtime_error(
            f"walkgraph binary '{walkgraph_bin}' was not found before subcommand 'gtfs-refresh' could run."
        ) from exc

    try:
        for line in _iter_stderr_lines(process):
            _emit_progress(progress_cb, f"walkgraph: {line}")
        return_code = process.wait()
    finally:
        if process.stderr is not None and hasattr(process.stderr, "close"):
            process.stderr.close()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    return output_dir


def load_gtfs_stop_reality_models(csv_path: Path) -> list[GtfsStopReality]:
    rows: list[GtfsStopReality] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                GtfsStopReality(
                    reality_fingerprint=row["reality_fingerprint"],
                    import_fingerprint=row["import_fingerprint"],
                    source_ref=row["source_ref"],
                    stop_name=row["stop_name"] or None,
                    feed_id=row["feed_id"],
                    stop_id=row["stop_id"],
                    source_status=row["source_status"],
                    reality_status=row["reality_status"],
                    school_only_state=row["school_only_state"],
                    public_departures_7d=int(row["public_departures_7d"] or 0),
                    public_departures_30d=int(row["public_departures_30d"] or 0),
                    school_only_departures_30d=int(row["school_only_departures_30d"] or 0),
                    last_public_service_date=(
                        None
                        if not row["last_public_service_date"]
                        else date.fromisoformat(row["last_public_service_date"])
                    ),
                    last_any_service_date=(
                        None
                        if not row["last_any_service_date"]
                        else date.fromisoformat(row["last_any_service_date"])
                    ),
                    route_modes=tuple(json.loads(row["route_modes_json"] or "[]")),
                    source_reason_codes=tuple(json.loads(row["source_reason_codes_json"] or "[]")),
                    reality_reason_codes=tuple(json.loads(row["reality_reason_codes_json"] or "[]")),
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                )
            )
    return rows
