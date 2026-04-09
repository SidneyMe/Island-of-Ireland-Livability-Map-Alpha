from __future__ import annotations

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from unittest import TestCase, mock


def _load_cache_module() -> ModuleType:
    cache_path = Path(__file__).resolve().parents[1] / "precompute" / "cache.py"
    spec = importlib.util.spec_from_file_location("precompute_cache_under_test", cache_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load cache module from {cache_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cache = _load_cache_module()


class PrecomputeCacheTests(TestCase):
    def test_pickle_cache_round_trip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            payload = {"cells": [1, 2, 3]}

            cache.cache_save("grid", payload, cache_dir)

            self.assertTrue(
                cache.cache_exists(
                    "grid",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                )
            )
            self.assertEqual(
                cache.cache_load(
                    "grid",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                ),
                payload,
            )

    def test_compressed_cache_round_trip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            payload = [{"cell_id": "a"}, {"cell_id": "b"}]

            cache.cache_save_large(
                "walk",
                payload,
                cache_dir,
                use_compressed_cache=True,
            )

            self.assertTrue(
                cache.cache_exists_large(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                    use_compressed_cache=True,
                )
            )
            self.assertEqual(
                cache.cache_load_large(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                    use_compressed_cache=True,
                ),
                payload,
            )

    def test_force_recompute_and_invalid_tier_skip_cache_reads(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            cache.cache_save("grid", {"cached": True}, cache_dir)

            self.assertFalse(
                cache.cache_exists(
                    "grid",
                    cache_dir,
                    force_recompute=True,
                    tier_valid={cache_dir: True},
                )
            )
            self.assertIsNone(
                cache.cache_load(
                    "grid",
                    cache_dir,
                    force_recompute=True,
                    tier_valid={cache_dir: True},
                )
            )
            self.assertFalse(
                cache.cache_exists(
                    "grid",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: False},
                )
            )
            self.assertIsNone(
                cache.cache_load(
                    "grid",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: False},
                )
            )

    def test_corrupted_pickle_cache_returns_none_and_is_quarantined(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            bad_path = cache_dir / "bad.pkl"
            bad_path.write_bytes(b"not a pickle")

            with mock.patch("builtins.print"):
                value = cache.cache_load(
                    "bad",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                )

            self.assertIsNone(value)
            self.assertFalse(bad_path.exists())
            self.assertTrue(list(cache_dir.glob("bad.pkl.bad*")))

    def test_corrupted_gzip_cache_returns_none_and_is_quarantined(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            bad_path = cache_dir / "badgz.pkl.gz"
            bad_path.write_bytes(b"not gzip")

            with mock.patch("builtins.print"):
                value = cache.cache_load_large(
                    "badgz",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                    use_compressed_cache=True,
                )

            self.assertIsNone(value)
            self.assertFalse(bad_path.exists())
            self.assertTrue(list(cache_dir.glob("badgz.pkl.gz.bad*")))
