from __future__ import annotations

from unittest import TestCase, mock

import config
from precompute.grid import score_cell
from precompute.mode_aware import MODE_AWARE_KEY


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
