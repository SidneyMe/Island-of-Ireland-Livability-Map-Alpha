from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


PHASE_ORDER = (
    "import",
    "geometry",
    "amenities",
    "networks",
    "reachability",
    "grids",
    "publish",
)

FINAL_PHASE_STATUSES = {"completed", "cached", "skipped"}


def _format_hms(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


@dataclass
class PhaseState:
    name: str
    index: int
    expected: bool = True
    started_at: float | None = None
    finished_at: float | None = None
    status: str | None = None
    detail: str = ""
    total_units: int | None = None
    completed_units: int = 0
    rebuild_total_units: int = 0
    rebuild_completed_units: int = 0
    unit_label: str = "units"
    rebuild_started_at: float | None = None
    last_progress_log_at: float | None = None
    last_progress_bucket: int = -1


class PrecomputeProgressTracker:
    def __init__(
        self,
        stats_path: Path,
        *,
        progress_interval_seconds: float = 15.0,
        percent_step: int = 5,
    ) -> None:
        self.stats_path = stats_path
        self.progress_interval_seconds = progress_interval_seconds
        self.percent_step = percent_step
        self.run_started_at = time.perf_counter()
        self._disabled = False
        self._warning_emitted = False
        self._history = self._load_history()
        self.substeps: dict[str, dict[str, float]] = {}
        self.phases = {
            name: PhaseState(name=name, index=index)
            for index, name in enumerate(PHASE_ORDER, start=1)
        }

    def set_phase_expected(self, phase_name: str, expected: bool) -> None:
        self._call_safely(self._set_phase_expected, phase_name, expected)

    def start_phase(
        self,
        phase_name: str,
        *,
        total_units: int | None = None,
        rebuild_total_units: int | None = None,
        unit_label: str | None = None,
        detail: str | None = None,
    ) -> None:
        self._call_safely(
            self._start_phase,
            phase_name,
            total_units=total_units,
            rebuild_total_units=rebuild_total_units,
            unit_label=unit_label,
            detail=detail,
        )

    def set_phase_totals(
        self,
        phase_name: str,
        *,
        total_units: int | None = None,
        rebuild_total_units: int | None = None,
        unit_label: str | None = None,
        detail: str | None = None,
        force_log: bool = False,
    ) -> None:
        self._call_safely(
            self._set_phase_totals,
            phase_name,
            total_units=total_units,
            rebuild_total_units=rebuild_total_units,
            unit_label=unit_label,
            detail=detail,
            force_log=force_log,
        )

    def set_live_work(
        self,
        phase_name: str,
        *,
        detail: str | None = None,
        rebuild_total_units: int | None = None,
    ) -> None:
        self._call_safely(
            self._set_live_work,
            phase_name,
            detail=detail,
            rebuild_total_units=rebuild_total_units,
        )

    def set_phase_detail(self, phase_name: str, detail: str, *, force_log: bool = False) -> None:
        self._call_safely(self._set_phase_detail, phase_name, detail, force_log=force_log)

    def credit_phase(
        self,
        phase_name: str,
        units: int,
        *,
        detail: str | None = None,
        force_log: bool = False,
    ) -> None:
        self._call_safely(
            self._advance_phase,
            phase_name,
            units=units,
            rebuild_units=0,
            detail=detail,
            force_log=force_log,
        )

    def advance_phase(
        self,
        phase_name: str,
        *,
        units: int = 1,
        rebuild_units: int | None = None,
        detail: str | None = None,
        force_log: bool = False,
    ) -> None:
        self._call_safely(
            self._advance_phase,
            phase_name,
            units=units,
            rebuild_units=rebuild_units,
            detail=detail,
            force_log=force_log,
        )

    def finish_phase(self, phase_name: str, status: str, *, detail: str | None = None) -> None:
        self._call_safely(self._finish_phase, phase_name, status, detail=detail)

    def skip_phase(self, phase_name: str, *, detail: str | None = None) -> None:
        self._call_safely(self._skip_phase, phase_name, detail=detail)

    def phase_callback(self, phase_name: str) -> Callable[..., None]:
        def _callback(event: str, **info: Any) -> None:
            self._call_safely(self._handle_progress, phase_name, event, **info)

        return _callback

    def save_successful_timings(self) -> None:
        self._call_safely(self._save_successful_timings)

    def record_substep(
        self,
        phase_name: str,
        substep_name: str,
        seconds: float,
        *,
        force_log: bool = False,
    ) -> None:
        self._call_safely(
            self._record_substep,
            phase_name,
            substep_name,
            seconds,
            force_log=force_log,
        )

    def total_elapsed_seconds(self) -> float:
        return time.perf_counter() - self.run_started_at

    def _call_safely(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if self._disabled:
            return None
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive progress tracking
            self._disabled = True
            self._warn_once(
                f"progress tracking disabled after {type(exc).__name__}: {exc}"
            )
            return None

    def _warn_once(self, message: str) -> None:
        if self._warning_emitted:
            return
        self._warning_emitted = True
        print(f"[progress] {message}", flush=True)

    def _load_history(self) -> dict[str, Any]:
        if not self.stats_path.exists():
            return {"last_total_seconds": None, "phases": {}, "substeps": {}}
        try:
            with self.stats_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {"last_total_seconds": None, "phases": {}, "substeps": {}}

        phase_payload = payload.get("phases", {})
        phases: dict[str, float] = {}
        if isinstance(phase_payload, dict):
            for name in PHASE_ORDER:
                value = phase_payload.get(name)
                if isinstance(value, (int, float)) and value >= 0:
                    phases[name] = float(value)

        last_total = payload.get("last_total_seconds")
        if not isinstance(last_total, (int, float)) or last_total < 0:
            last_total = None

        substep_payload = payload.get("substeps", {})
        substeps: dict[str, dict[str, float]] = {}
        if isinstance(substep_payload, dict):
            for phase_name, phase_substeps in substep_payload.items():
                if phase_name not in PHASE_ORDER or not isinstance(phase_substeps, dict):
                    continue
                substeps[phase_name] = {
                    substep_name: float(value)
                    for substep_name, value in phase_substeps.items()
                    if isinstance(value, (int, float)) and value >= 0
                }

        return {
            "last_total_seconds": float(last_total) if last_total is not None else None,
            "phases": phases,
            "substeps": substeps,
        }

    def _save_successful_timings(self) -> None:
        phase_history = dict(self._history.get("phases", {}))
        substep_history = {
            phase_name: dict(phase_substeps)
            for phase_name, phase_substeps in self._history.get("substeps", {}).items()
        }
        for name, phase in self.phases.items():
            if phase.status != "completed" or phase.started_at is None or phase.finished_at is None:
                continue
            phase_history[name] = max(phase.finished_at - phase.started_at, 0.0)
            if name in self.substeps:
                substep_history[name] = {
                    substep_name: max(value, 0.0)
                    for substep_name, value in self.substeps[name].items()
                }

        payload = {
            "last_total_seconds": max(self.total_elapsed_seconds(), 0.0),
            "phases": phase_history,
            "substeps": substep_history,
        }

        self.stats_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.stats_path.with_suffix(self.stats_path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        temp_path.replace(self.stats_path)
        self._history = payload

    def _record_substep(
        self,
        phase_name: str,
        substep_name: str,
        seconds: float,
        *,
        force_log: bool = False,
    ) -> None:
        phase_substeps = self.substeps.setdefault(phase_name, {})
        phase_substeps[substep_name] = phase_substeps.get(substep_name, 0.0) + max(
            float(seconds),
            0.0,
        )
        if force_log:
            print(
                f"[timing] {phase_name}.{substep_name} {phase_substeps[substep_name]:.3f}s",
                flush=True,
            )

    def _set_phase_expected(self, phase_name: str, expected: bool) -> None:
        self.phases[phase_name].expected = expected

    def _start_phase(
        self,
        phase_name: str,
        *,
        total_units: int | None = None,
        rebuild_total_units: int | None = None,
        unit_label: str | None = None,
        detail: str | None = None,
    ) -> None:
        phase = self.phases[phase_name]
        now = time.perf_counter()
        phase.expected = True
        if phase.started_at is None:
            phase.started_at = now
            phase.last_progress_log_at = now
            phase.last_progress_bucket = self._progress_bucket(phase)
        if total_units is not None:
            phase.total_units = max(int(total_units), 0)
        if rebuild_total_units is not None:
            phase.rebuild_total_units = max(int(rebuild_total_units), 0)
        if unit_label is not None:
            phase.unit_label = unit_label
        if detail is not None:
            phase.detail = detail
        self._emit_phase_line(phase, kind="start", now=now)

    def _set_phase_totals(
        self,
        phase_name: str,
        *,
        total_units: int | None = None,
        rebuild_total_units: int | None = None,
        unit_label: str | None = None,
        detail: str | None = None,
        force_log: bool = False,
    ) -> None:
        phase = self.phases[phase_name]
        if phase.started_at is None:
            self._start_phase(phase_name)
            phase = self.phases[phase_name]
        if total_units is not None:
            phase.total_units = max(int(total_units), 0)
        if rebuild_total_units is not None:
            phase.rebuild_total_units = max(int(rebuild_total_units), 0)
        if unit_label is not None:
            phase.unit_label = unit_label
        if detail is not None:
            phase.detail = detail
        self._clamp_units(phase)
        self._maybe_emit_progress(phase, force=force_log)

    def _set_live_work(
        self,
        phase_name: str,
        *,
        detail: str | None = None,
        rebuild_total_units: int | None = None,
    ) -> None:
        phase = self.phases[phase_name]
        if phase.started_at is None:
            self._start_phase(phase_name)
            phase = self.phases[phase_name]
        if rebuild_total_units is not None:
            phase.rebuild_total_units = max(int(rebuild_total_units), 0)
        if detail is not None:
            phase.detail = detail
        if phase.rebuild_started_at is None:
            phase.rebuild_started_at = time.perf_counter()

    def _set_phase_detail(self, phase_name: str, detail: str, *, force_log: bool = False) -> None:
        phase = self.phases[phase_name]
        if phase.started_at is None:
            self._start_phase(phase_name, detail=detail)
            return
        phase.detail = detail
        self._maybe_emit_progress(phase, force=force_log)

    def _advance_phase(
        self,
        phase_name: str,
        *,
        units: int,
        rebuild_units: int | None = None,
        detail: str | None = None,
        force_log: bool = False,
    ) -> None:
        phase = self.phases[phase_name]
        if phase.started_at is None:
            self._start_phase(phase_name)
            phase = self.phases[phase_name]

        credited_units = max(int(units), 0)
        live_units = credited_units if rebuild_units is None else max(int(rebuild_units), 0)

        phase.completed_units += credited_units
        phase.rebuild_completed_units += live_units
        if live_units > 0 and phase.rebuild_started_at is None:
            phase.rebuild_started_at = time.perf_counter()
        if detail is not None:
            phase.detail = detail
        self._clamp_units(phase)
        self._maybe_emit_progress(phase, force=force_log)

    def _finish_phase(self, phase_name: str, status: str, *, detail: str | None = None) -> None:
        if status not in FINAL_PHASE_STATUSES:
            raise ValueError(f"Unsupported phase status: {status}")
        phase = self.phases[phase_name]
        now = time.perf_counter()
        if phase.started_at is None:
            phase.started_at = now
        if detail is not None:
            phase.detail = detail
        if phase.total_units is not None and status in {"completed", "cached"}:
            phase.completed_units = phase.total_units
        if phase.rebuild_total_units and status == "completed":
            phase.rebuild_completed_units = phase.rebuild_total_units
        phase.status = status
        phase.finished_at = now
        self._emit_phase_line(phase, kind="finish", now=now)

    def _skip_phase(self, phase_name: str, *, detail: str | None = None) -> None:
        phase = self.phases[phase_name]
        phase.expected = False
        self._finish_phase(phase_name, "skipped", detail=detail)

    def _handle_progress(self, phase_name: str, event: str, **info: Any) -> None:
        if event == "detail":
            detail = info.get("detail")
            if isinstance(detail, str):
                self._set_phase_detail(
                    phase_name,
                    detail,
                    force_log=bool(info.get("force_log", False)),
                )
            return

        if event == "live_start":
            detail = info.get("detail")
            rebuild_total_units = info.get("rebuild_total_units")
            if isinstance(info.get("total_units"), int) and rebuild_total_units is None:
                rebuild_total_units = info["total_units"]
            self._set_live_work(
                phase_name,
                detail=detail if isinstance(detail, str) else None,
                rebuild_total_units=rebuild_total_units if isinstance(rebuild_total_units, int) else None,
            )
            return

        if event == "totals":
            self._set_phase_totals(
                phase_name,
                total_units=info.get("total_units") if isinstance(info.get("total_units"), int) else None,
                rebuild_total_units=info.get("rebuild_total_units") if isinstance(info.get("rebuild_total_units"), int) else None,
                unit_label=info.get("unit_label") if isinstance(info.get("unit_label"), str) else None,
                detail=info.get("detail") if isinstance(info.get("detail"), str) else None,
                force_log=bool(info.get("force_log", False)),
            )
            return

        if event == "credit":
            self._advance_phase(
                phase_name,
                units=int(info.get("units", 1)),
                rebuild_units=0,
                detail=info.get("detail") if isinstance(info.get("detail"), str) else None,
                force_log=bool(info.get("force_log", False)),
            )
            return

        if event == "advance":
            rebuild_units = info.get("rebuild_units")
            if rebuild_units is not None and not isinstance(rebuild_units, int):
                rebuild_units = None
            self._advance_phase(
                phase_name,
                units=int(info.get("units", 1)),
                rebuild_units=rebuild_units,
                detail=info.get("detail") if isinstance(info.get("detail"), str) else None,
                force_log=bool(info.get("force_log", False)),
            )
            return

    def _clamp_units(self, phase: PhaseState) -> None:
        if phase.total_units is not None:
            phase.completed_units = min(phase.completed_units, phase.total_units)
        if phase.rebuild_total_units:
            phase.rebuild_completed_units = min(phase.rebuild_completed_units, phase.rebuild_total_units)

    def _progress_bucket(self, phase: PhaseState) -> int:
        if phase.total_units is None or phase.total_units <= 0:
            return -1
        percent = (phase.completed_units / phase.total_units) * 100.0
        return int(percent // self.percent_step)

    def _maybe_emit_progress(self, phase: PhaseState, *, force: bool = False) -> None:
        now = time.perf_counter()
        if phase.status in FINAL_PHASE_STATUSES:
            return
        if force:
            self._emit_phase_line(phase, kind="progress", now=now)
            return
        last_logged_at = phase.last_progress_log_at
        if last_logged_at is not None and (now - last_logged_at) < self.progress_interval_seconds:
            return
        bucket = self._progress_bucket(phase)
        if bucket > phase.last_progress_bucket or last_logged_at is None or (now - last_logged_at) >= self.progress_interval_seconds:
            self._emit_phase_line(phase, kind="progress", now=now)

    def _emit_phase_line(self, phase: PhaseState, *, kind: str, now: float) -> None:
        base = (
            f"[Elapsed {_format_hms(now - self.run_started_at)}] "
            f"Phase {phase.index}/{len(PHASE_ORDER)} {phase.name}"
        )

        progress_text = self._progress_text(phase)
        if progress_text:
            base = f"{base} {progress_text}"

        extras: list[str] = []
        if kind == "finish" and phase.status is not None:
            duration = self._phase_duration(phase, now)
            extras.append(phase.status)
            extras.append(f"Phase {_format_hms(duration)}")
        if phase.detail:
            extras.append(phase.detail)

        if kind != "finish":
            remaining_seconds, has_remaining_work = self._total_remaining_seconds(phase, now)
            if remaining_seconds is None and has_remaining_work:
                extras.append("ETA ~estimating...")
            elif remaining_seconds is not None:
                extras.append(f"ETA ~{_format_hms(remaining_seconds)}")
                finish_at = datetime.now().astimezone().timestamp() + remaining_seconds
                extras.append(
                    f"Finish ~{datetime.fromtimestamp(finish_at).astimezone().strftime('%H:%M')}"
                )

        line = base
        if extras:
            line = f"{line} | " + " | ".join(extras)
        print(line, flush=True)

        if kind in {"start", "progress"}:
            phase.last_progress_log_at = now
            phase.last_progress_bucket = self._progress_bucket(phase)

    def _progress_text(self, phase: PhaseState) -> str:
        if phase.total_units is None or phase.total_units <= 0:
            return ""
        percent = (phase.completed_units / phase.total_units) * 100.0
        return (
            f"{phase.completed_units:,}/{phase.total_units:,} "
            f"{phase.unit_label} ({percent:.1f}%)"
        )

    def _phase_duration(self, phase: PhaseState, now: float) -> float:
        if phase.started_at is None:
            return 0.0
        endpoint = phase.finished_at if phase.finished_at is not None else now
        return max(endpoint - phase.started_at, 0.0)

    def _historical_phase_seconds(self, phase_name: str) -> float | None:
        value = self._history.get("phases", {}).get(phase_name)
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def _total_remaining_seconds(
        self,
        current_phase: PhaseState,
        now: float,
    ) -> tuple[float | None, bool]:
        remaining_parts: list[float] = []
        current_remaining = self._current_phase_remaining(current_phase, now)
        has_remaining_work = current_phase.status not in FINAL_PHASE_STATUSES

        if current_remaining is None and has_remaining_work:
            return None, True
        if current_remaining is not None and current_remaining > 0:
            remaining_parts.append(current_remaining)

        for phase_name in PHASE_ORDER[current_phase.index:]:
            phase = self.phases[phase_name]
            if not phase.expected or phase.status in FINAL_PHASE_STATUSES:
                continue
            has_remaining_work = True
            historical_seconds = self._historical_phase_seconds(phase_name)
            if historical_seconds is None:
                return None, True
            remaining_parts.append(historical_seconds)

        if not has_remaining_work:
            return 0.0, False
        return sum(remaining_parts), True

    def _current_phase_remaining(self, phase: PhaseState, now: float) -> float | None:
        if phase.status in FINAL_PHASE_STATUSES:
            return 0.0

        historical_seconds = self._historical_phase_seconds(phase.name)
        if phase.total_units is None or phase.total_units <= 0:
            if historical_seconds is None or phase.started_at is None:
                return None
            return max(historical_seconds - (now - phase.started_at), 0.0)

        remaining_units = max(phase.total_units - phase.completed_units, 0)
        if remaining_units == 0:
            return 0.0

        live_remaining_units = max(phase.rebuild_total_units - phase.rebuild_completed_units, 0)
        if phase.rebuild_completed_units > 0 and phase.rebuild_started_at is not None:
            live_elapsed = max(now - phase.rebuild_started_at, 0.0)
            if self._has_enough_live_signal(phase, live_elapsed):
                rate = live_elapsed / phase.rebuild_completed_units
                return rate * live_remaining_units

        if historical_seconds is None:
            return None

        if phase.total_units > 0:
            return historical_seconds * (remaining_units / phase.total_units)
        return historical_seconds

    def _has_enough_live_signal(self, phase: PhaseState, live_elapsed: float) -> bool:
        if phase.rebuild_completed_units <= 0:
            return False
        if live_elapsed >= 10.0:
            return True
        if phase.rebuild_total_units <= 0:
            return False
        return (phase.rebuild_completed_units / phase.rebuild_total_units) >= 0.05
