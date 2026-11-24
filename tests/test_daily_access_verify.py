from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import TestCase, mock

import numpy as np
from shapely.geometry import box

from precompute import daily_access_build as build
from precompute import daily_access_verify as verify


def _write_sampling_shard(directory: Path, shard_id: str, mask: np.ndarray) -> None:
    x0, y0 = (int(value) for value in shard_id.split("_", 1))
    np.savez_compressed(
        directory / "shards" / f"{shard_id}.npz",
        valid_land_mask=mask,
        x_min_m=np.asarray([x0], dtype=np.int32),
        y_min_m=np.asarray([y0], dtype=np.int32),
    )


class BoundedShardSamplingTests(TestCase):
    def test_sampling_is_deterministic_and_covers_each_nonempty_shard(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            shell_dir = Path(temp) / "shell"
            (shell_dir / "shards").mkdir(parents=True)
            masks = {
                "0_0": np.asarray([[1, 0], [0, 1]], dtype=bool),
                "20000_0": np.asarray([[1, 1], [1, 1]], dtype=bool),
                "40000_0": np.asarray([[1, 1, 1], [1, 1, 1]], dtype=bool),
            }
            for shard_id, mask in masks.items():
                _write_sampling_shard(shell_dir, shard_id, mask)

            first, total = verify.sample_valid_cells(
                shell_dir, masks, sample_size=3, seed=17, step_m=50.0,
            )
            second, repeated_total = verify.sample_valid_cells(
                shell_dir, masks, sample_size=3, seed=17, step_m=50.0,
            )

        self.assertEqual(total, 12)
        self.assertEqual(repeated_total, total)
        self.assertEqual(first, second)
        self.assertEqual({sample.shard_id for sample in first}, set(masks))

    def test_sampling_caps_at_valid_population_and_rejects_negative_size(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            shell_dir = Path(temp) / "shell"
            (shell_dir / "shards").mkdir(parents=True)
            _write_sampling_shard(shell_dir, "0_0", np.asarray([[1, 0], [1, 0]], dtype=bool))
            sampled, total = verify.sample_valid_cells(
                shell_dir, ["0_0"], sample_size=99, seed=0, step_m=50.0,
            )
            self.assertEqual((len(sampled), total), (2, 2))
            with self.assertRaisesRegex(ValueError, "non-negative"):
                verify.sample_valid_cells(shell_dir, ["0_0"], sample_size=-1, seed=0, step_m=50.0)


class PinnedSnapshotTests(TestCase):
    def test_snapshot_uses_published_identities_without_refreshing_gtfs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            graph_dir = root / "graph"
            shell_dir = root / "shell"
            reach_dir = root / "reach"
            score_dir = root / "score"
            for directory in (graph_dir, shell_dir, reach_dir, score_dir):
                directory.mkdir()
            mask_geometry = box(0, 0, 100, 100)
            mask_id = build.mask_identity(build.LandMask(mask_geometry))
            common = {
                "status": "complete",
                "profile": "test",
                "import_fingerprint": "osm-pinned",
                "transit_reality_fingerprint": "gtfs-pinned",
                "mask_identity": mask_id,
            }
            (shell_dir / "manifest.json").write_text(
                json.dumps({
                    **common, "tier": "shell", "key": "shell-key",
                    "import_fingerprint": "older-shell-import",
                    "transit_reality_fingerprint": "older-shell-gtfs",
                }),
                encoding="utf-8",
            )
            (reach_dir / "manifest.json").write_text(
                json.dumps({**common, "tier": "reach", "key": "reach-key"}), encoding="utf-8",
            )
            (score_dir / "manifest.json").write_text(
                json.dumps({
                    **common, "tier": "score", "key": "score-key",
                    "shell_key": "shell-key", "reach_key": "reach-key",
                }),
                encoding="utf-8",
            )
            published = {
                "build_key": "published-v4",
                "status": "complete",
                "import_fingerprint": "osm-pinned",
                "summary_json": {
                    "scoring_model": "daily-access-v4",
                    "build_profile": "test",
                    "daily_access": {
                        "keys": {"graph": "graph-key", "shell": "shell-key", "reach": "reach-key", "score": "score-key"},
                        "graph_dir": str(graph_dir),
                        "shell_dir": str(shell_dir),
                        "reach_dir": str(reach_dir),
                        "score_dir": str(score_dir),
                        "import_fingerprint": "osm-pinned",
                        "transit_reality_fingerprint": "gtfs-pinned",
                        "overture_dataset_signature": "overture-pinned",
                    },
                },
            }
            with (
                mock.patch.object(verify, "load_build_manifest", return_value=published),
                mock.patch.object(verify, "has_complete_import_manifest", return_value=True),
                mock.patch.object(verify, "has_complete_transit_reality_manifest", return_value=True),
                mock.patch.object(verify, "load_study_area_geometries", return_value=(mask_geometry, mask_geometry)),
                mock.patch.object(verify, "load_island_geometry_metric", return_value=mask_geometry),
                mock.patch("overture.loader.dataset_signature", return_value="overture-pinned"),
                mock.patch("transit.ensure_transit_reality", side_effect=AssertionError("must not refresh")),
            ):
                snapshot = verify.load_verification_snapshot(
                    mock.sentinel.engine, profile="test", build_key="published-v4",
                )

        self.assertEqual(snapshot.build_key, "published-v4")
        self.assertEqual(snapshot.reality_fingerprint, "gtfs-pinned")
        self.assertEqual(snapshot.graph_dir, graph_dir)

    def test_legacy_publication_without_pinned_identity_is_rejected(self) -> None:
        published = {
            "build_key": "legacy-v4",
            "status": "complete",
            "summary_json": {
                "scoring_model": "daily-access-v4",
                "build_profile": "test",
                "daily_access": {"keys": {name: name for name in ("graph", "shell", "reach", "score")}},
            },
        }
        with mock.patch.object(verify, "load_build_manifest", return_value=published):
            with self.assertRaisesRegex(build.DailyAccessBuildError, "republish"):
                verify.load_verification_snapshot(mock.sentinel.engine, profile="test", build_key="legacy-v4")
