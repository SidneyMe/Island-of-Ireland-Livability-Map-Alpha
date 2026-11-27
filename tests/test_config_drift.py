"""Values that live in several places on purpose must stay in step with config.py.

The Lua importer cannot import Python, the PMTiles workers hold their own zoom tables
and scripts/fetch_datasets.py is stdlib-only, so drift is caught here instead.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest import TestCase

import config
import fine_vector_pmtiles_worker as fine_worker
import pmtiles_bake_worker as coarse_worker
from db_postgis import reads
from overture import loader as overture_loader
from precompute import daily_access_classify
from precompute.bake_pmtiles import DEFAULT_MIN_ZOOM, PMTILES_SOURCE_MAX_ZOOM
from scripts.fetch_datasets import DATASETS


def _lua_set(name: str) -> set[str]:
    text = (config.BASE_DIR / "osm2pgsql_livability.lua").read_text(encoding="utf-8")
    match = re.search(rf"^local {name} = \{{(.*?)^\}}", text, re.MULTILINE | re.DOTALL)
    if match is None:
        raise AssertionError(f"osm2pgsql_livability.lua has no table {name!r}")
    return set(re.findall(r"(\w+)\s*=\s*true", match.group(1)))


class LuaDriftTests(TestCase):
    def test_landuse_classes(self) -> None:
        self.assertEqual(_lua_set("landuse_values"), set(config.LANDUSE_CONTEXT_CLASSES))
        self.assertEqual(set(reads._LANDUSE_CONTEXT_VALUES), set(config.LANDUSE_CONTEXT_CLASSES))

    def test_road_classes(self) -> None:
        self.assertEqual(_lua_set("road_values"), set(config.ROAD_PROXIMITY_CLASS_SETTINGS))

    def test_healthcare_values(self) -> None:
        self.assertEqual(_lua_set("healthcare_values"), set(config.TAGS["healthcare"]["amenity"]))

    def test_park_values_cover_both_models(self) -> None:
        # The importer is wider than TAGS on purpose: TAGS is the v3 selection
        # (reads.legacy_v3_amenity_category), v4 classifies the extra green types.
        park_values = _lua_set("park_values")
        self.assertLessEqual(set(config.TAGS["parks"]["leisure"]), park_values)
        green_types = set(daily_access_classify.GREEN_SEMANTIC_TYPES)
        self.assertEqual(park_values | {"national_park"}, green_types)
        self.assertEqual(_lua_set("green_landuse_values"), set(daily_access_classify._GREEN_LANDUSE_TYPES))


class ZoomLadderDriftTests(TestCase):
    def test_coarse_worker_matches_every_profile(self) -> None:
        fine_min_zoom = min(fine_worker._FINE_GRID_RESOLUTIONS_BY_ZOOM)
        for profile in ("full", "dev", "test"):
            for zoom in range(DEFAULT_MIN_ZOOM, fine_min_zoom):
                with self.subTest(profile=profile, zoom=zoom):
                    self.assertEqual(
                        coarse_worker._resolution_for_zoom(zoom),
                        config.resolution_for_zoom(zoom, profile=profile),
                    )

    def test_fine_worker_matches_fine_profiles(self) -> None:
        for profile in ("full", "test"):
            expected: dict[int, set[int]] = {}
            for zoom in range(min(fine_worker._FINE_GRID_RESOLUTIONS_BY_ZOOM), config.SURFACE_MAX_ZOOM + 1):
                # The archive stops at its source max zoom; deeper zooms overzoom it.
                source_zoom = min(zoom, PMTILES_SOURCE_MAX_ZOOM)
                expected.setdefault(source_zoom, set()).add(config.resolution_for_zoom(zoom, profile=profile))
            actual = {zoom: set(sizes) for zoom, sizes in fine_worker._FINE_GRID_RESOLUTIONS_BY_ZOOM.items()}
            with self.subTest(profile=profile):
                self.assertEqual(actual, expected)


class DatasetPathDriftTests(TestCase):
    def test_fetch_targets_match_runtime_paths(self) -> None:
        feeds = {feed.feed_id: Path(feed.zip_path) for feed in config.transit_feed_configs()}
        expected = {
            "osm": config.OSM_EXTRACT_PATH,
            "roi_boundary": config.ROI_BOUNDARY_PATH,
            "ni_boundary": config.NI_BOUNDARY_PATH,
            "main_island": config.MAIN_ISLAND_BOUNDARY_PATH,
            "gtfs_nta": feeds["nta"],
            "gtfs_translink": feeds["translink"],
            "overture": overture_loader.OVERTURE_PLACES_PATH,
            "overture_state": overture_loader.OVERTURE_PLACES_STATE_PATH,
        }
        datasets = {dataset.key: dataset for dataset in DATASETS}
        for key, path in expected.items():
            with self.subTest(dataset=key):
                self.assertEqual(datasets[key].ready_path(config.BASE_DIR).resolve(), Path(path).resolve())
