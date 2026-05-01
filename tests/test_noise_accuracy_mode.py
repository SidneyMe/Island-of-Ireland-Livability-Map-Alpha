from __future__ import annotations

import os
from unittest import TestCase
from unittest.mock import patch

from noise_artifacts.modes import (
    noise_accuracy_mode_label,
    normalize_noise_accuracy_mode,
    resolve_noise_accuracy_mode,
)


class NoiseAccuracyModeTests(TestCase):
    def test_default_mode_is_dev_fast(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(resolve_noise_accuracy_mode(cli_noise_accurate=False), "dev_fast")

    def test_env_override_accurate(self) -> None:
        with patch.dict(os.environ, {"NOISE_ACCURACY_MODE": "accurate"}, clear=True):
            self.assertEqual(resolve_noise_accuracy_mode(cli_noise_accurate=False), "accurate")

    def test_cli_flag_has_precedence_over_env(self) -> None:
        with patch.dict(os.environ, {"NOISE_ACCURACY_MODE": "dev_fast"}, clear=True):
            self.assertEqual(resolve_noise_accuracy_mode(cli_noise_accurate=True), "accurate")

    def test_normalize_accepts_hyphen_variant(self) -> None:
        self.assertEqual(normalize_noise_accuracy_mode("dev-fast"), "dev_fast")

    def test_invalid_mode_raises(self) -> None:
        with self.assertRaises(ValueError):
            normalize_noise_accuracy_mode("banana")

    def test_label_dev_fast(self) -> None:
        self.assertEqual(noise_accuracy_mode_label("dev_fast"), "dev-fast")
