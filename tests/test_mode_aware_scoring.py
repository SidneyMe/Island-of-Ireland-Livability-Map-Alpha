from __future__ import annotations

from unittest import TestCase, mock

import config
from precompute.grid import score_cell
from precompute.mode_aware import MODE_AWARE_KEY
from transit.chained_reach import TripStopTime, derive_transfer_stop_reach


class ModeAwareScoringTests(TestCase):
    def test_score_cell_keeps_walk_total_and_adds_mode_aware_payload(self) -> None:
        scores, total = score_cell(
            {"shops": 1, "transport": 1, "healthcare": 0, "parks": 0},
            effective_units={"shops": 3.0, "transport": 5.0},
            bike_effective_units={"shops": 6.0, "transport": 5.0},
            transit_effective_units={"shops": 0.0, "transport": 5.0},
        )

        self.assertAlmostEqual(total, 25.0 * (3.0 / 6.0) + 25.0)
        payload = scores[MODE_AWARE_KEY]
        self.assertEqual(payload["weights"], {"walk": 0.5, "bike": 0.25, "transit": 0.25})
        self.assertTrue(payload["mode_available"]["bike"])
        self.assertFalse(payload["mode_available"]["transit"])
        self.assertAlmostEqual(payload["component_scores"]["shops"], (12.5 * 0.5 + 25.0 * 0.25) / 0.75)
        self.assertAlmostEqual(payload["component_scores"]["transport"], 25.0)

    def test_mode_weight_changes_invalidate_score_hash(self) -> None:
        with mock.patch.object(config, "MODE_AWARE_SCORE_WEIGHTS", {"walk": 0.5, "bike": 0.25, "transit": 0.25}):
            previous = config.build_config_hashes()
        with mock.patch.object(config, "MODE_AWARE_SCORE_WEIGHTS", {"walk": 0.4, "bike": 0.3, "transit": 0.3}):
            current = config.build_config_hashes()

        self.assertEqual(previous.surface_shell_hash, current.surface_shell_hash)
        self.assertNotEqual(previous.score_hash, current.score_hash)


class TransitChainedReachTests(TestCase):
    def test_direct_and_one_transfer_reach_are_derived(self) -> None:
        rows = [
            TripStopTime("T1", "A", 0, 0, 1, quality=1.0),
            TripStopTime("T1", "B", 600, 660, 2, quality=1.0),
            TripStopTime("T2", "B", 900, 900, 1, quality=0.5),
            TripStopTime("T2", "C", 1_500, 1_500, 2, quality=0.5),
        ]

        reach = derive_transfer_stop_reach(
            rows,
            max_transfer_count=1,
            max_wait_seconds=300,
            max_travel_seconds=2_000,
        )

        destinations = {entry.destination_stop_id: entry for entry in reach["A"]}
        self.assertEqual(destinations["B"].transfer_count, 0)
        self.assertEqual(destinations["C"].transfer_count, 1)
        self.assertLess(destinations["C"].quality, destinations["B"].quality)

    def test_non_public_rows_are_excluded(self) -> None:
        rows = [
            TripStopTime("SCHOOL", "A", 0, 0, 1, public=False),
            TripStopTime("SCHOOL", "B", 600, 600, 2, public=False),
        ]

        reach = derive_transfer_stop_reach(
            rows,
            max_transfer_count=1,
            max_wait_seconds=300,
            max_travel_seconds=2_000,
        )

        self.assertEqual(reach, {})
