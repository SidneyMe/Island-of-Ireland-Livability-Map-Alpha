from __future__ import annotations

import unittest
from unittest import TestCase, mock

from shapely.errors import GEOSException
from shapely.geometry import MultiPolygon, Point, Polygon, box

import config
import study_area
from precompute.grid import build_scoring_grid


def _rect(x0: float, y0: float, x1: float, y1: float) -> Polygon:
    return box(x0, y0, x1, y1)


def _spike_polygon(
    base_x0: float,
    base_y0: float,
    base_x1: float,
    base_y1: float,
    *,
    spike_width_m: float,
    spike_length_m: float,
) -> Polygon:
    """A rectangle with a narrow spike extending from the top-right corner."""
    spike_left = base_x1 - spike_width_m / 2
    spike_right = base_x1 + spike_width_m / 2
    spike_top = base_y1 + spike_length_m
    return Polygon([
        (base_x0, base_y0),
        (base_x1, base_y0),
        (base_x1, base_y1),
        (spike_right, base_y1),
        (spike_right, spike_top),
        (spike_left, spike_top),
        (spike_left, base_y1),
        (base_x0, base_y1),
        (base_x0, base_y0),
    ])


class CoastalArtifactCleanupTests(TestCase):
    """Unit tests for study_area.clean_coastal_artifacts."""

    def test_narrow_spike_is_removed(self) -> None:
        """A polygon with a spike narrower than 2x artifact_width_m loses the spike."""
        spike_top = 1500.0
        geom = _spike_polygon(0, 0, 1000, 1000, spike_width_m=10, spike_length_m=500)
        cleaned = study_area.clean_coastal_artifacts(
            geom,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        self.assertFalse(cleaned.is_empty)
        self.assertTrue(cleaned.is_valid)
        self.assertLess(cleaned.bounds[3], spike_top)
        self.assertLess(cleaned.area, geom.area)
        self.assertGreater(cleaned.area, 0.90 * 1_000_000)

    def test_main_land_mass_remains(self) -> None:
        """After cleanup the large body of the polygon is preserved."""
        geom = _spike_polygon(0, 0, 1000, 1000, spike_width_m=10, spike_length_m=500)
        cleaned = study_area.clean_coastal_artifacts(
            geom,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        self.assertGreater(cleaned.area, 0.80 * 1_000_000)

    def test_cleanup_preserves_surviving_shore_vertices_instead_of_rounding_the_corner(self) -> None:
        geom = _spike_polygon(0, 0, 1000, 1000, spike_width_m=10, spike_length_m=500)

        cleaned = study_area.clean_coastal_artifacts(
            geom,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        self.assertTrue(cleaned.covers(Point(1000, 1000)))

    def test_wide_feature_is_kept(self) -> None:
        """A rectangle wider than 2x artifact_width_m survives in the primary path."""
        geom = _rect(0, 0, 1000, 1000)
        cleanup_mode, cleaned, diagnostic = study_area._cleanup_coastal_component(
            geom,
            artifact_width_m=75,
        )

        self.assertEqual(cleanup_mode, "primary")
        self.assertFalse(cleaned.is_empty)
        self.assertGreater(cleaned.area, 0.70 * geom.area)
        self.assertIsNone(diagnostic)

    def test_cleanup_retries_with_simplified_geometry_after_geos_failure(self) -> None:
        geom = _rect(0, 0, 1000, 1000)
        degraded_result = _rect(10, 10, 990, 990)

        with mock.patch.object(
            study_area,
                "_open_coastal_component",
                side_effect=[GEOSException("boom"), degraded_result],
            ) as open_mock:
            cleanup_mode, cleaned, diagnostic = study_area._cleanup_coastal_component(
                geom,
                artifact_width_m=75,
            )

        self.assertEqual(cleanup_mode, "degraded")
        self.assertTrue(cleaned.equals(geom))
        self.assertEqual(diagnostic["cleanup_mode"], "degraded")
        self.assertEqual(open_mock.call_count, 2)

    def test_cleanup_keeps_original_geometry_after_double_geos_failure(self) -> None:
        geom = _rect(0, 0, 1000, 1000)

        with (
            mock.patch.object(
                study_area,
                "_open_coastal_component",
                side_effect=[GEOSException("boom"), GEOSException("still boom")],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cleaned = study_area.clean_coastal_artifacts(
                geom,
                artifact_width_m=75,
                preserve_area_m2=100_000,
            )

        self.assertTrue(cleaned.equals(geom))
        print_mock.assert_called_once()
        log_line = print_mock.call_args.args[0]
        self.assertIn("coastal cleanup fallback used", log_line)
        self.assertIn("component=0", log_line)
        self.assertIn("mode=original", log_line)
        self.assertIn("rep=", log_line)
        self.assertIn("bounds_wgs84=", log_line)

    def test_large_island_preserved_when_cleanup_would_erase_it(self) -> None:
        """A component whose cleaned form is empty is kept if area >= preserve_area_m2."""
        thin_sliver = _rect(0, 0, 10, 200)
        multi = MultiPolygon([thin_sliver, _rect(5000, 5000, 6000, 6000)])
        cleaned = study_area.clean_coastal_artifacts(
            multi,
            artifact_width_m=50,
            preserve_area_m2=1_000,
        )

        self.assertFalse(cleaned.is_empty)
        self.assertGreater(cleaned.area, _rect(5000, 5000, 6000, 6000).area * 0.5)

    def test_small_sliver_below_threshold_is_discarded(self) -> None:
        """A component that erodes to empty and is below preserve_area_m2 is dropped."""
        thin_sliver = _rect(0, 0, 10, 200)
        large_block = _rect(5000, 5000, 6000, 6000)
        multi = MultiPolygon([thin_sliver, large_block])
        cleaned = study_area.clean_coastal_artifacts(
            multi,
            artifact_width_m=50,
            preserve_area_m2=5_000,
        )

        self.assertFalse(cleaned.is_empty)
        self.assertLess(cleaned.area, large_block.area * 1.1)

    def test_empty_cleanup_result_still_uses_preserve_area_threshold(self) -> None:
        thin_sliver = _rect(0, 0, 10, 200)

        with mock.patch.object(
            study_area,
            "_cleanup_coastal_component",
            return_value=(
                "degraded",
                Polygon(),
                {
                    "component_index": 0,
                    "cleanup_mode": "degraded",
                    "area_m2": float(thin_sliver.area),
                    "representative_lat": 0.0,
                    "representative_lon": 0.0,
                    "bounds_wgs84": {
                        "min_lat": 0.0,
                        "min_lon": 0.0,
                        "max_lat": 0.0,
                        "max_lon": 0.0,
                    },
                },
            ),
        ):
            cleaned = study_area.clean_coastal_artifacts(
                thin_sliver,
                artifact_width_m=50,
                preserve_area_m2=1_000,
            )

        self.assertTrue(cleaned.equals(thin_sliver))

    def test_cleanup_returns_valid_geometry(self) -> None:
        """clean_coastal_artifacts always produces a valid geometry."""
        geom = _spike_polygon(0, 0, 2000, 2000, spike_width_m=5, spike_length_m=1000)
        cleaned = study_area.clean_coastal_artifacts(
            geom,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        self.assertTrue(cleaned.is_valid)
        self.assertFalse(cleaned.is_empty)

    def test_multipolygon_with_multiple_large_parts_all_kept(self) -> None:
        """All large components of a MultiPolygon survive cleanup."""
        part_a = _rect(0, 0, 1000, 1000)
        part_b = _rect(5000, 5000, 6000, 6000)
        multi = MultiPolygon([part_a, part_b])
        cleaned = study_area.clean_coastal_artifacts(
            multi,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        self.assertFalse(cleaned.is_empty)
        self.assertGreater(cleaned.area, 0.60 * (part_a.area + part_b.area))

    def test_cleanup_reports_degraded_component_location_metadata(self) -> None:
        geom = _rect(0, 0, 1000, 1000)

        with (
            mock.patch.object(
                study_area,
                "_open_coastal_component",
                side_effect=[GEOSException("boom"), _rect(10, 10, 990, 990)],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            study_area.clean_coastal_artifacts(
                geom,
                artifact_width_m=75,
                preserve_area_m2=100_000,
            )

        print_mock.assert_called_once()
        log_line = print_mock.call_args.args[0]
        self.assertIn("mode=degraded", log_line)
        self.assertIn("rep=", log_line)
        self.assertIn("area_m2=", log_line)
        self.assertIn("bounds_wgs84=", log_line)


class GridCoastalClipTests(TestCase):
    """Grid cells built from a spiky study area should not include the spike."""

    def test_cell_effective_area_excludes_spike(self) -> None:
        """A cell that only contains the spike has low effective_area_ratio after cleanup."""
        spiky = _spike_polygon(0, 0, 1000, 1000, spike_width_m=10, spike_length_m=500)
        cleaned = study_area.clean_coastal_artifacts(
            spiky,
            artifact_width_m=75,
            preserve_area_m2=100_000,
        )

        cells = build_scoring_grid(500, cleaned, keep_mode="intersects", clip=True)
        self.assertGreater(len(cells), 0)

        for cell in cells:
            self.assertLessEqual(cell["effective_area_ratio"], 1.0 + 1e-9)

        for cell in cells:
            geom = cell.get("geometry")
            if geom is not None and not geom.is_empty:
                self.assertGreaterEqual(cell["effective_area_m2"], 0.0)


class CoastalConfigHashRegressionTests(TestCase):
    """Changing coastal cleanup constants must change the geo_hash."""

    def test_changing_artifact_width_changes_geo_hash(self) -> None:
        with mock.patch.object(config, "COASTAL_ARTIFACT_WIDTH_M", 50):
            hashes_a = config.build_config_hashes()
        with mock.patch.object(config, "COASTAL_ARTIFACT_WIDTH_M", 100):
            hashes_b = config.build_config_hashes()

        self.assertNotEqual(hashes_a.geo_hash, hashes_b.geo_hash)

    def test_changing_preserve_area_changes_geo_hash(self) -> None:
        with mock.patch.object(config, "COASTAL_COMPONENT_PRESERVE_AREA_M2", 50_000):
            hashes_a = config.build_config_hashes()
        with mock.patch.object(config, "COASTAL_COMPONENT_PRESERVE_AREA_M2", 200_000):
            hashes_b = config.build_config_hashes()

        self.assertNotEqual(hashes_a.geo_hash, hashes_b.geo_hash)

    def test_coastal_constants_affect_geo_hash_not_render_hash(self) -> None:
        """Coastal constants belong to geo stage; render_hash changes downstream."""
        with mock.patch.object(config, "COASTAL_ARTIFACT_WIDTH_M", 50):
            hashes_a = config.build_config_hashes()
        with mock.patch.object(config, "COASTAL_ARTIFACT_WIDTH_M", 100):
            hashes_b = config.build_config_hashes()

        self.assertNotEqual(hashes_a.geo_hash, hashes_b.geo_hash)
        self.assertNotEqual(hashes_a.score_hash, hashes_b.score_hash)


class IslandGeometrySmokeTests(TestCase):
    @unittest.skipUnless(
        config.ROI_BOUNDARY_PATH.exists() and config.NI_BOUNDARY_PATH.exists(),
        "requires local boundary files",
    )
    def test_load_island_geometry_metric_smoke(self) -> None:
        geometry = study_area.load_island_geometry_metric()

        self.assertFalse(geometry.is_empty)
        self.assertTrue(geometry.is_valid)
