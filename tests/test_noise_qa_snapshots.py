"""
Phase 1 QA snapshots: deterministic invariants of the current noise system.

These tests capture the legacy behaviour before any architecture changes.
Later artifact-mode outputs will be compared against these baselines.
All tests are pure-Python (no DB, no files) and deterministic.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

from shapely.geometry import box

import db_postgis.writes as _writes
from noise.loader import (
    materialize_effective_noise_rows,
    normalize_ni_gridcode_band,
    normalize_noise_band,
)
from precompute import publish as _publish


# ---------------------------------------------------------------------------
# normalize_noise_band snapshots
# ---------------------------------------------------------------------------

class NoiseNormalizationSnapshots(TestCase):
    """Table-driven tests for normalize_noise_band and normalize_ni_gridcode_band."""

    # (args, expected_low, expected_high, expected_label)
    BAND_CASES: list[tuple] = [
        # Standard range strings
        (("45-49",), 45.0, 49.0, "45-49"),
        (("50-54",), 50.0, 54.0, "50-54"),
        (("55-59",), 55.0, 59.0, "55-59"),
        (("60-64",), 60.0, 64.0, "60-64"),
        (("65-69",), 65.0, 69.0, "65-69"),
        (("70-74",), 70.0, 74.0, "70-74"),
        # "75+" → high=99, label includes "+"
        (("75+",), 75.0, 99.0, "75+"),
        # None/empty → no information, label is the empty string
        ((None,), None, None, ""),
        (("",), None, None, ""),
        # One-sided range via match: "50-" parses low=50, high stays None → high=low=50
        (("50-",), 50.0, 50.0, "50-50"),
        # Keyword args only
        ((), 55.0, 59.0, "55-59"),   # handled via _band_kw_cases below
    ]

    # Separate table for keyword-only calls (db_low/db_high kwargs)
    BAND_KW_CASES: list[tuple] = [
        # (db_low, db_high, expected_low, expected_high, expected_label)
        (55.0, 59.0, 55.0, 59.0, "55-59"),
        (75.0, 99.0, 75.0, 99.0, "75+"),
        (45.0, 49.0, 45.0, 49.0, "45-49"),
        # High < 99 but both supplied
        (60.0, 64.0, 60.0, 64.0, "60-64"),
        # Both None → passthrough
        (None, None, None, None, ""),
    ]

    def test_normalize_noise_band_positional(self) -> None:
        for args, exp_low, exp_high, exp_label in self.BAND_CASES[:-1]:
            with self.subTest(args=args):
                low, high, label = normalize_noise_band(*args)
                if exp_low is None:
                    self.assertIsNone(low)
                else:
                    self.assertAlmostEqual(low, exp_low, places=6)
                if exp_high is None:
                    self.assertIsNone(high)
                else:
                    self.assertAlmostEqual(high, exp_high, places=6)
                self.assertEqual(label, exp_label)

    def test_normalize_noise_band_keyword(self) -> None:
        for db_low, db_high, exp_low, exp_high, exp_label in self.BAND_KW_CASES:
            with self.subTest(db_low=db_low, db_high=db_high):
                low, high, label = normalize_noise_band(db_low=db_low, db_high=db_high)
                if exp_low is None:
                    self.assertIsNone(low)
                else:
                    self.assertAlmostEqual(low, exp_low, places=6)
                if exp_high is None:
                    self.assertIsNone(high)
                else:
                    self.assertAlmostEqual(high, exp_high, places=6)
                self.assertEqual(label, exp_label)

    # (gridcode, expected_low, expected_high, expected_label)
    NI_GRIDCODE_CASES: list[tuple] = [
        # Sentinel: no data
        (1000, None, None, None),
        # None input → treated as missing
        (None, None, None, None),
        # gridcode=49 → low=50, high=54
        (49, 50.0, 54.0, "50-54"),
        # gridcode=54 → low=55, high=59
        (54, 55.0, 59.0, "55-59"),
        # gridcode=59 → low=60, high=64
        (59, 60.0, 64.0, "60-64"),
        # gridcode=64 → low=65, high=69
        (64, 65.0, 69.0, "65-69"),
        # gridcode=69 → low=70, high=74
        (69, 70.0, 74.0, "70-74"),
        # gridcode=74 → low=75, high=99 (≥75 threshold)
        (74, 75.0, 99.0, "75+"),
        # gridcode=75 → low=76, high=99 (still above threshold)
        (75, 76.0, 99.0, "76+"),
    ]

    def test_normalize_ni_gridcode_band(self) -> None:
        for gridcode, exp_low, exp_high, exp_label in self.NI_GRIDCODE_CASES:
            with self.subTest(gridcode=gridcode):
                low, high, label = normalize_ni_gridcode_band(gridcode)
                if exp_low is None:
                    self.assertIsNone(low)
                else:
                    self.assertAlmostEqual(low, exp_low, places=6)
                if exp_high is None:
                    self.assertIsNone(high)
                else:
                    self.assertAlmostEqual(high, exp_high, places=6)
                self.assertEqual(label, exp_label)


# ---------------------------------------------------------------------------
# materialize_effective_noise_rows snapshots
# ---------------------------------------------------------------------------

class NoiseMaterializeSnapshots(TestCase):
    """
    Deterministic geometry snapshot for round-priority deduplication.

    Layout (all boxes in a flat coordinate space):
      Round N (newer):
        55-59 → box(0, 0, 1, 1)    area=1
        60-64 → box(3, 0, 4, 1)    area=1
        65-69 → box(6, 0, 7, 1)    area=1

      Round N-1 (older):
        55-59 → box(0, 0, 1, 1)    IDENTICAL to newer → fully covered → 0 effective area
        60-64 → box(1.5, 0, 2.5, 1)  outside covered → full area=1
        65-69 → box(6.5, 0, 7.5, 1)  half-overlaps Round N box → effective box(7,0,7.5,1) area=0.5

    Three groups are created with this same layout:
      (roi, road, Lden)   — rounds 4 and 3
      (roi, rail, Lden)   — rounds 4 and 3
      (ni, road, Lnight)  — rounds 3 and 2

    Expected per group: 5 rows, total area ≈ 4.5
    Expected overall:  15 rows, total area ≈ 13.5
    """

    # Large study area; no clipping occurs.
    STUDY_AREA = box(-100, -100, 100, 100)

    # Round number pairs (newer, older) per group
    GROUP_SPECS = [
        ("roi", "road", "Lden", 4, 3),
        ("roi", "rail", "Lden", 4, 3),
        ("ni",  "road", "Lnight", 3, 2),
    ]

    BANDS = ["55-59", "60-64", "65-69"]
    BAND_BOXES_NEWER = {
        "55-59": box(0, 0, 1, 1),
        "60-64": box(3, 0, 4, 1),
        "65-69": box(6, 0, 7, 1),
    }
    BAND_BOXES_OLDER = {
        "55-59": box(0, 0, 1, 1),          # identical → 0 effective
        "60-64": box(1.5, 0, 2.5, 1),      # outside covered → full
        "65-69": box(6.5, 0, 7.5, 1),      # half-overlap → effective = box(7,0,7.5,1)
    }

    def _make_candidates(self) -> list[dict]:
        rows = []
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            for band, geom in self.BAND_BOXES_NEWER.items():
                rows.append({
                    "jurisdiction": jur, "source_type": stype, "metric": metric,
                    "round_number": r_new, "report_period": f"Round {r_new}",
                    "db_value": band, "db_low": None, "db_high": None,
                    "source_dataset": "test.zip", "source_layer": "test_layer",
                    "source_ref": f"{jur}_{stype}_{band}_{r_new}",
                    "geom": geom,
                })
            for band, geom in self.BAND_BOXES_OLDER.items():
                rows.append({
                    "jurisdiction": jur, "source_type": stype, "metric": metric,
                    "round_number": r_old, "report_period": f"Round {r_old}",
                    "db_value": band, "db_low": None, "db_high": None,
                    "source_dataset": "test.zip", "source_layer": "test_layer",
                    "source_ref": f"{jur}_{stype}_{band}_{r_old}",
                    "geom": geom,
                })
        return rows

    def test_total_row_count(self) -> None:
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        self.assertEqual(len(output), 15)

    def test_per_group_row_count(self) -> None:
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            group_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
            ]
            with self.subTest(jurisdiction=jur, source_type=stype, metric=metric):
                self.assertEqual(len(group_rows), 5)

    def test_total_geometry_area(self) -> None:
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        total_area = sum(row["geom"].area for row in output)
        self.assertAlmostEqual(total_area, 13.5, places=6)

    def test_per_group_geometry_area(self) -> None:
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            group_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
            ]
            group_area = sum(r["geom"].area for r in group_rows)
            with self.subTest(jurisdiction=jur, source_type=stype, metric=metric):
                self.assertAlmostEqual(group_area, 4.5, places=6)

    def test_newer_round_rows_are_unclipped(self) -> None:
        """Rows from the newer round must be preserved unchanged."""
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            newer_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
                and r["round_number"] == r_new
            ]
            with self.subTest(jurisdiction=jur, source_type=stype, round=r_new):
                self.assertEqual(len(newer_rows), 3)
                total = sum(r["geom"].area for r in newer_rows)
                self.assertAlmostEqual(total, 3.0, places=6)

    def test_duplicate_newer_round_box_excluded_from_older_round(self) -> None:
        """55-59 band in older round is identical to newer → must not appear in output."""
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            older_55_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
                and r["round_number"] == r_old
                and r["db_value"] == "55-59"
            ]
            with self.subTest(jurisdiction=jur, source_type=stype):
                self.assertEqual(len(older_55_rows), 0,
                                 "55-59 in older round must be masked by identical newer-round box")

    def test_non_overlapping_older_band_included_at_full_area(self) -> None:
        """60-64 band in older round is outside newer coverage → full area."""
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            older_60_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
                and r["round_number"] == r_old
                and r["db_value"] == "60-64"
            ]
            with self.subTest(jurisdiction=jur, source_type=stype):
                self.assertEqual(len(older_60_rows), 1)
                self.assertAlmostEqual(older_60_rows[0]["geom"].area, 1.0, places=6)

    def test_partially_overlapping_older_band_clipped(self) -> None:
        """65-69 band in older round partially overlaps newer → only non-covered part remains."""
        candidates = self._make_candidates()
        output = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        for jur, stype, metric, r_new, r_old in self.GROUP_SPECS:
            older_65_rows = [
                r for r in output
                if r["jurisdiction"] == jur
                and r["source_type"] == stype
                and r["metric"] == metric
                and r["round_number"] == r_old
                and r["db_value"] == "65-69"
            ]
            with self.subTest(jurisdiction=jur, source_type=stype):
                self.assertEqual(len(older_65_rows), 1)
                self.assertAlmostEqual(older_65_rows[0]["geom"].area, 0.5, places=6)

    def test_result_is_deterministic(self) -> None:
        """Same inputs produce same outputs on repeated calls."""
        candidates = self._make_candidates()
        out1 = materialize_effective_noise_rows(candidates, self.STUDY_AREA)
        out2 = materialize_effective_noise_rows(self._make_candidates(), self.STUDY_AREA)
        areas1 = sorted(r["geom"].area for r in out1)
        areas2 = sorted(r["geom"].area for r in out2)
        self.assertEqual(len(areas1), len(areas2))
        for a, b in zip(areas1, areas2):
            self.assertAlmostEqual(a, b, places=6)


# ---------------------------------------------------------------------------
# summary_json_impl noise contract snapshots
# ---------------------------------------------------------------------------

class NoiseSummaryJsonSnapshots(TestCase):
    """
    The noise_* fields produced by summary_json_impl must match this contract.
    These fields drive the frontend's noise controls (source_counts, band_counts, etc.).
    """

    EXPECTED_NOISE_KEYS = {
        "noise_enabled",
        "noise_counts",
        "noise_source_counts",
        "noise_metric_counts",
        "noise_band_counts",
    }

    SYNTHETIC_NOISE_ROWS = [
        # roi road Lden 55-59
        {"jurisdiction": "roi", "source_type": "road", "metric": "Lden", "db_value": "55-59"},
        # roi road Lden 60-64
        {"jurisdiction": "roi", "source_type": "road", "metric": "Lden", "db_value": "60-64"},
        # roi rail Lnight 55-59
        {"jurisdiction": "roi", "source_type": "rail", "metric": "Lnight", "db_value": "55-59"},
        # ni road Lden 55-59
        {"jurisdiction": "ni", "source_type": "road", "metric": "Lden", "db_value": "55-59"},
        # ni road Lnight 75+
        {"jurisdiction": "ni", "source_type": "road", "metric": "Lnight", "db_value": "75+"},
    ]

    EXPECTED_SUMMARY = {
        "noise_enabled": True,
        "noise_counts": {"roi": 3, "ni": 2},
        "noise_source_counts": {"road": 4, "rail": 1},
        "noise_metric_counts": {"Lden": 3, "Lnight": 2},
        "noise_band_counts": {"55-59": 3, "60-64": 1, "75+": 1},
    }

    def _make_hashes(self):
        return SimpleNamespace(
            build_key="test-build-key",
            config_hash="test-config-hash",
            import_fingerprint="test-import-fp",
        )

    def _call_summary_json(self, noise_rows):
        study_area = box(-10, 51, -6, 55)
        return _publish.summary_json_impl(
            study_area_wgs84=study_area,
            walk_grids={100: [{"cell": 1}]},
            amenity_data={"parks": [(53.0, -7.0)]},
            amenity_source_rows=None,
            transport_reality_rows=None,
            noise_rows=noise_rows,
            hashes=self._make_hashes(),
            build_profile="test",
            source_state=None,
            osm_extract_path=Path("/dev/null"),
            grid_sizes_m=[100],
            fine_resolutions_m=[],
            output_html=False,
            zoom_breaks=[],
        )

    def test_all_noise_keys_present(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        for key in self.EXPECTED_NOISE_KEYS:
            with self.subTest(key=key):
                self.assertIn(key, result)

    def test_noise_enabled_true_when_rows_present(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        self.assertTrue(result["noise_enabled"])

    def test_noise_enabled_false_when_no_rows(self) -> None:
        result = self._call_summary_json([])
        self.assertFalse(result["noise_enabled"])

    def test_noise_enabled_false_when_rows_none(self) -> None:
        result = self._call_summary_json(None)
        self.assertFalse(result["noise_enabled"])

    def test_noise_counts_by_jurisdiction(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        self.assertEqual(result["noise_counts"], self.EXPECTED_SUMMARY["noise_counts"])

    def test_noise_source_counts(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        self.assertEqual(result["noise_source_counts"],
                         self.EXPECTED_SUMMARY["noise_source_counts"])

    def test_noise_metric_counts(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        self.assertEqual(result["noise_metric_counts"],
                         self.EXPECTED_SUMMARY["noise_metric_counts"])

    def test_noise_band_counts(self) -> None:
        result = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        self.assertEqual(result["noise_band_counts"],
                         self.EXPECTED_SUMMARY["noise_band_counts"])

    def test_summary_is_deterministic(self) -> None:
        r1 = self._call_summary_json(self.SYNTHETIC_NOISE_ROWS)
        r2 = self._call_summary_json(list(self.SYNTHETIC_NOISE_ROWS))
        for key in self.EXPECTED_NOISE_KEYS:
            self.assertEqual(r1[key], r2[key])


# ---------------------------------------------------------------------------
# _materialize_noise_group_round SQL contract snapshots
# ---------------------------------------------------------------------------

class NoiseDbWriteSqlSnapshots(TestCase):
    """
    Asserts that the PostGIS SQL used for materialization contains the
    required spatial operations. These keywords are the correctness contract
    for the legacy pipeline and must also appear in the new artifact pipeline.
    """

    def _get_materialize_source(self) -> str:
        func = getattr(_writes, "_materialize_noise_group_round", None)
        self.assertIsNotNone(func, "_materialize_noise_group_round not found in db_postgis.writes")
        return inspect.getsource(func)

    def _get_publish_source(self) -> str:
        func = getattr(_writes, "_publish_noise_polygons", None)
        self.assertIsNotNone(func, "_publish_noise_polygons not found in db_postgis.writes")
        return inspect.getsource(func)

    def test_materialize_uses_st_make_valid(self) -> None:
        self.assertIn("ST_MakeValid", self._get_materialize_source())

    def test_materialize_uses_st_collection_extract(self) -> None:
        self.assertIn("ST_CollectionExtract", self._get_materialize_source())

    def test_materialize_uses_st_subdivide(self) -> None:
        self.assertIn("ST_Subdivide", self._get_materialize_source())

    def test_materialize_uses_st_difference(self) -> None:
        self.assertIn("ST_Difference", self._get_materialize_source())

    def test_materialize_uses_st_intersection(self) -> None:
        self.assertIn("ST_Intersection", self._get_materialize_source())

    def test_materialize_filters_zero_area(self) -> None:
        src = self._get_materialize_source()
        self.assertIn("ST_Area", src)

    def test_publish_calls_stage_before_materialize(self) -> None:
        """
        The publish function must reference staging before materialization
        (i.e., _stage_noise_candidate_rows before _materialize_noise_polygons_from_stage).
        """
        src = self._get_publish_source()
        stage_pos = src.find("_stage_noise_candidate_rows")
        materialize_pos = src.find("_materialize_noise_polygons_from_stage")
        self.assertGreater(stage_pos, -1, "_stage_noise_candidate_rows not found")
        self.assertGreater(materialize_pos, -1, "_materialize_noise_polygons_from_stage not found")
        self.assertLess(stage_pos, materialize_pos,
                        "_stage_noise_candidate_rows must appear before _materialize_noise_polygons_from_stage")

    def test_publish_calls_summary_update_last(self) -> None:
        """_update_noise_summary_from_database must appear after materialize in source."""
        src = self._get_publish_source()
        materialize_pos = src.find("_materialize_noise_polygons_from_stage")
        summary_pos = src.rfind("_update_noise_summary_from_database")
        self.assertGreater(summary_pos, -1, "_update_noise_summary_from_database not found")
        # There may be two calls (clone path and normal path); rfind gets the last one
        self.assertGreater(summary_pos, materialize_pos,
                           "_update_noise_summary_from_database must appear after materialize")

    def test_clone_check_uses_render_hash(self) -> None:
        """Clone shortcut must match on render_hash so unchanged builds are fast."""
        src = getattr(_writes, "_clone_noise_polygons_from_prior_build", None)
        if src is None:
            self.skipTest("_clone_noise_polygons_from_prior_build not found")
        source_text = inspect.getsource(src)
        self.assertIn("render_hash", source_text)

    def test_clone_check_uses_noise_processing_hash(self) -> None:
        """Clone shortcut must also match on noise_processing_hash for partial reuse."""
        src = getattr(_writes, "_clone_noise_polygons_from_prior_build", None)
        if src is None:
            self.skipTest("_clone_noise_polygons_from_prior_build not found")
        source_text = inspect.getsource(src)
        self.assertIn("noise_processing_hash", source_text)
