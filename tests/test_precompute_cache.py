from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from unittest import TestCase, mock

import numpy as np


def _load_cache_module() -> ModuleType:
    cache_path = Path(__file__).resolve().parents[1] / "precompute" / "cache.py"
    spec = importlib.util.spec_from_file_location("precompute_cache_under_test", cache_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load cache module from {cache_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cache = _load_cache_module()


def _load_reachability_arrays_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "precompute" / "reachability_arrays.py"
    spec = importlib.util.spec_from_file_location("reachability_arrays_under_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load reachability array module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


reach_arrays = _load_reachability_arrays_module()


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

    def test_chunk_only_large_cache_counts_as_existing_and_loads(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            payload = {2: {"shops": 2}}

            cache.cache_save_large_append_frame(
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

    def test_large_cache_load_merges_base_blob_and_chunk_overlay(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            base_payload = {1: {"shops": 1}}
            chunk_payload = {2: {"shops": 2}}

            cache.cache_save_large(
                "walk",
                base_payload,
                cache_dir,
                use_compressed_cache=True,
            )
            cache.cache_save_large_append_frame(
                "walk",
                chunk_payload,
                cache_dir,
                use_compressed_cache=True,
            )

            self.assertEqual(
                cache.cache_load_large(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                    use_compressed_cache=True,
                ),
                {**base_payload, **chunk_payload},
            )

    def test_large_cache_finalize_load_merges_base_blob_and_chunk_overlay(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            base_payload = {1: {"shops": 1}}
            chunk_payload = {2: {"shops": 2}}

            cache.cache_save_large(
                "walk",
                base_payload,
                cache_dir,
                use_compressed_cache=True,
            )
            cache.cache_save_large_append_frame(
                "walk",
                chunk_payload,
                cache_dir,
                use_compressed_cache=True,
            )

            self.assertEqual(
                cache.cache_load_large_for_finalize(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    use_compressed_cache=True,
                ),
                {**base_payload, **chunk_payload},
            )

    def test_large_cache_finalize_exists_checks_presence_without_loading(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            (cache_dir / "walk.chunks.pkl.gz").write_bytes(b"present but not gzip")

            self.assertTrue(
                cache.cache_exists_large_for_finalize(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    use_compressed_cache=True,
                )
            )
            self.assertFalse(list(cache_dir.glob("walk.chunks.pkl.gz.bad*")))

    def test_corrupted_chunk_cache_falls_back_to_base_blob(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            base_payload = {1: {"shops": 1}}
            chunk_path = cache_dir / "walk.chunks.pkl.gz"

            cache.cache_save_large(
                "walk",
                base_payload,
                cache_dir,
                use_compressed_cache=True,
            )
            chunk_path.write_bytes(b"not gzip")

            with mock.patch("builtins.print"):
                value = cache.cache_load_large(
                    "walk",
                    cache_dir,
                    force_recompute=False,
                    tier_valid={cache_dir: True},
                    use_compressed_cache=True,
                )

            self.assertEqual(value, base_payload)
            self.assertFalse(chunk_path.exists())
            self.assertTrue(list(cache_dir.glob("walk.chunks.pkl.gz.bad*")))
            self.assertTrue((cache_dir / "walk.pkl.gz").exists())

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


class ReachabilityArrayCacheTests(TestCase):
    def test_counts_array_cache_round_trip_uses_uint64_origins_and_manifest_categories(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            matrix = reach_arrays.ReachabilityMatrix.from_arrays(
                [1, 2],
                ["shops", "transport"],
                np.array([[3, 0], [0, 4]], dtype=np.uint32),
                value_kind="counts",
            )

            reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, matrix)
            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops", "transport"],
                value_kind="counts",
            )
            npz_path = cache_dir / "reachability_arrays" / "walk_counts_by_origin_node" / "base.npz"
            with np.load(npz_path, allow_pickle=False) as data:
                self.assertEqual(set(data.files), {"origin_ids", "matrix"})
                self.assertEqual(data["origin_ids"].dtype, np.dtype("<u8"))

        self.assertIsNotNone(loaded)
        self.assertTrue(loaded.is_complete)
        self.assertEqual(loaded.matrix.get(1), {"shops": 3})
        self.assertEqual(loaded.matrix.get(2), {"transport": 4})

    def test_effective_units_array_cache_round_trip(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            matrix = reach_arrays.ReachabilityMatrix.from_arrays(
                [7],
                ["parks"],
                np.array([[1.5]], dtype=np.float32),
                value_kind="effective_units",
            )

            reach_arrays.save_reachability_cache(
                "walk_effective_units_by_origin_node",
                cache_dir,
                matrix,
            )
            loaded = reach_arrays.load_reachability_cache(
                "walk_effective_units_by_origin_node",
                cache_dir,
                categories=["parks"],
                value_kind="effective_units",
            )

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.matrix.get(7), {"parks": 1.5})

    def test_empty_categories_cache_marks_requested_origins_present(self) -> None:
        matrix = reach_arrays.ReachabilityMatrix.for_origins(
            [3, 5],
            [],
            value_kind="counts",
        )

        self.assertEqual(matrix.matrix.shape, (2, 0))
        self.assertEqual(matrix.get(3), {})
        self.assertEqual(matrix.missing_origin_ids([3, 5]), ())
        self.assertEqual(matrix.missing_origin_ids([3, 4, 5]), (4,))

    def test_metadata_mismatch_is_stale(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            matrix = reach_arrays.ReachabilityMatrix.from_arrays(
                [1],
                ["shops"],
                np.array([[1]], dtype=np.uint32),
                value_kind="counts",
            )
            reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, matrix)

            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["transport"],
                value_kind="counts",
            )

        self.assertIsNone(loaded)

    def test_duplicate_origin_ids_inside_one_chunk_are_invalid(self) -> None:
        with self.assertRaises(ValueError):
            reach_arrays.ReachabilityMatrix.from_arrays(
                [1, 1],
                ["shops"],
                np.array([[1], [2]], dtype=np.uint32),
                value_kind="counts",
            )

    def test_chunk_resume_merges_later_chunk_wins(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            base = reach_arrays.ReachabilityMatrix.from_arrays(
                [1],
                ["shops"],
                np.array([[1]], dtype=np.uint32),
                value_kind="counts",
            )
            first = reach_arrays.ReachabilityMatrix.from_arrays(
                [2],
                ["shops"],
                np.array([[2]], dtype=np.uint32),
                value_kind="counts",
            )
            second = reach_arrays.ReachabilityMatrix.from_arrays(
                [2, 3],
                ["shops"],
                np.array([[5], [6]], dtype=np.uint32),
                value_kind="counts",
            )
            reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, base)
            reach_arrays.append_reachability_cache_chunk("walk_counts_by_origin_node", cache_dir, first)
            reach_arrays.append_reachability_cache_chunk("walk_counts_by_origin_node", cache_dir, second)

            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops"],
                value_kind="counts",
            )

        self.assertIsNotNone(loaded)
        self.assertFalse(loaded.is_complete)
        self.assertEqual(
            loaded.matrix.to_sparse_dict(),
            {1: {"shops": 1}, 2: {"shops": 5}, 3: {"shops": 6}},
        )

    def test_orphan_tmp_file_is_ignored(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            matrix = reach_arrays.ReachabilityMatrix.from_arrays(
                [1],
                ["shops"],
                np.array([[1]], dtype=np.uint32),
                value_kind="counts",
            )
            reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, matrix)
            root = cache_dir / "reachability_arrays" / "walk_counts_by_origin_node"
            (root / "base.npz.tmp").write_bytes(b"not a npz")

            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops"],
                value_kind="counts",
            )

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.matrix.get(1), {"shops": 1})

    def test_atomic_matrix_write_failure_leaves_no_final_or_tmp_file(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            matrix = reach_arrays.ReachabilityMatrix.from_arrays(
                [1],
                ["shops"],
                np.array([[1]], dtype=np.uint32),
                value_kind="counts",
            )
            root = cache_dir / "reachability_arrays" / "walk_counts_by_origin_node"

            with (
                mock.patch.object(reach_arrays.os, "replace", side_effect=OSError("blocked")),
                self.assertRaises(OSError),
            ):
                reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, matrix)

            self.assertFalse((root / "base.npz").exists())
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_corrupt_chunk_is_quarantined_and_ignored(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            base = reach_arrays.ReachabilityMatrix.from_arrays(
                [1],
                ["shops"],
                np.array([[1]], dtype=np.uint32),
                value_kind="counts",
            )
            chunk = reach_arrays.ReachabilityMatrix.from_arrays(
                [2],
                ["shops"],
                np.array([[2]], dtype=np.uint32),
                value_kind="counts",
            )
            reach_arrays.save_reachability_cache("walk_counts_by_origin_node", cache_dir, base)
            reach_arrays.append_reachability_cache_chunk("walk_counts_by_origin_node", cache_dir, chunk)
            chunk_path = (
                cache_dir
                / "reachability_arrays"
                / "walk_counts_by_origin_node"
                / "chunks"
                / "chunk_000001.npz"
            )
            chunk_path.write_bytes(b"not a npz")

            with mock.patch("builtins.print"):
                loaded = reach_arrays.load_reachability_cache(
                    "walk_counts_by_origin_node",
                    cache_dir,
                    categories=["shops"],
                    value_kind="counts",
                )
            bad_chunks = list(chunk_path.parent.glob("chunk_000001.npz.bad*"))

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.matrix.to_sparse_dict(), {1: {"shops": 1}})
        self.assertEqual(loaded.matrix.missing_origin_ids([1, 2]), (2,))
        self.assertTrue(bad_chunks)

    def test_legacy_cache_is_not_migrated_unless_explicitly_requested(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            cache.cache_save_large(
                "walk_counts_by_origin_node",
                {1: {"shops": 2}},
                cache_dir,
                use_compressed_cache=True,
            )

            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops"],
                value_kind="counts",
            )

        self.assertIsNone(loaded)

    def test_opt_in_legacy_migration_writes_v1_cache(self) -> None:
        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            cache.cache_save_large(
                "walk_counts_by_origin_node",
                {1: {"shops": 2}},
                cache_dir,
                use_compressed_cache=True,
            )

            migrated = reach_arrays.migrate_legacy_sparse_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops"],
                value_kind="counts",
                legacy_cache_load_large=lambda key, directory: cache.cache_load_large(
                    key,
                    directory,
                    force_recompute=False,
                    tier_valid={directory: True},
                    use_compressed_cache=True,
                ),
            )
            loaded = reach_arrays.load_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                categories=["shops"],
                value_kind="counts",
            )

        self.assertIsNotNone(migrated)
        self.assertIsNotNone(loaded)
        self.assertTrue(loaded.is_complete)
        self.assertEqual(loaded.matrix.get(1), {"shops": 2})
