from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config import (
    CACHE_DIR,
    build_hashes_for_import,
    build_profile_settings,
    normalize_build_profile,
    profile_fine_surface_enabled,
)

from . import surface as _surface


@dataclass
class _BuildState:
    profile: str
    settings: Any
    hashes: Any
    geo_cache_dir: Path
    reach_cache_dir: Path
    score_cache_dir: Path
    surface_shell_dir: Path
    surface_score_dir: Path
    surface_tile_dir: Path
    tier_valid: dict[Path, bool] = field(default_factory=dict)
    tiers_building: set[Path] = field(default_factory=set)
    source_state: Any = None
    transit_reality_state: Any = None
    study_area_metric: Any = None
    study_area_wgs84: Any = None

    @classmethod
    def bootstrap(cls) -> _BuildState:
        profile = normalize_build_profile()
        settings = build_profile_settings(profile)
        hashes = build_hashes_for_import(
            "bootstrap",
            transit_reality_fingerprint="bootstrap",
            profile=profile,
        )
        return cls(
            profile=profile,
            settings=settings,
            hashes=hashes,
            geo_cache_dir=CACHE_DIR / f"geo_{hashes.geo_hash}",
            reach_cache_dir=CACHE_DIR / f"reach_{hashes.reach_hash}",
            score_cache_dir=CACHE_DIR / f"score_{hashes.score_hash}",
            surface_shell_dir=_surface.surface_shell_dir(
                CACHE_DIR,
                surface_shell_hash=hashes.surface_shell_hash,
            ),
            surface_score_dir=_surface.surface_score_dir(
                CACHE_DIR,
                score_hash=hashes.score_hash,
            ),
            surface_tile_dir=_surface.surface_tile_dir(
                CACHE_DIR,
                score_hash=hashes.score_hash,
                render_hash=hashes.render_hash,
            ),
        )

    def activate(
        self,
        import_fingerprint: str,
        *,
        transit_reality_fingerprint: str = "transit-unavailable",
        profile: str,
    ) -> None:
        self.profile = normalize_build_profile(profile)
        self.settings = build_profile_settings(self.profile)
        self.hashes = build_hashes_for_import(
            import_fingerprint,
            transit_reality_fingerprint=transit_reality_fingerprint,
            profile=self.profile,
        )
        self.geo_cache_dir = CACHE_DIR / f"geo_{self.hashes.geo_hash}"
        self.reach_cache_dir = CACHE_DIR / f"reach_{self.hashes.reach_hash}"
        self.score_cache_dir = CACHE_DIR / f"score_{self.hashes.score_hash}"
        self.surface_shell_dir = _surface.surface_shell_dir(
            CACHE_DIR,
            surface_shell_hash=self.hashes.surface_shell_hash,
        )
        self.surface_score_dir = _surface.surface_score_dir(
            CACHE_DIR,
            score_hash=self.hashes.score_hash,
        )
        self.surface_tile_dir = _surface.surface_tile_dir(
            CACHE_DIR,
            score_hash=self.hashes.score_hash,
            render_hash=self.hashes.render_hash,
        )


_STATE = _BuildState.bootstrap()


def _active_fine_surface_enabled() -> bool:
    return profile_fine_surface_enabled(_STATE.profile)


def _elapsed(started_at: float) -> str:
    return f"[{time.perf_counter() - started_at:.1f}s]"
