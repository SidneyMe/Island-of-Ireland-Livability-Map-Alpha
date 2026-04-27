from __future__ import annotations

from typing import Any

from transit import ensure_transit_reality as _ensure_transit_reality_impl

from ._state import _STATE


def _ensure_transit_reality(
    engine,
    *,
    import_fingerprint: str,
    study_area_wgs84=None,
    force_refresh: bool = False,
    refresh_download: bool = False,
    progress_cb=None,
    reality_state: Any = None,
) -> Any:
    del study_area_wgs84
    transit_state = _ensure_transit_reality_impl(
        engine,
        import_fingerprint=import_fingerprint,
        refresh_download=refresh_download,
        force_refresh=force_refresh,
        progress_cb=progress_cb,
        reality_state=reality_state,
    )
    _STATE.transit_reality_state = transit_state
    _STATE.activate(
        import_fingerprint,
        transit_reality_fingerprint=transit_state.reality_fingerprint,
        profile=_STATE.profile,
    )
    return transit_state


