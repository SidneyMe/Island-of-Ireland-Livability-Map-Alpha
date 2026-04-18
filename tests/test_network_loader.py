from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock

import numpy as np

from network import loader
from walkgraph_support import walkgraph_runtime_error


class _FakeAttrSeq:
    def __init__(self) -> None:
        self.attrs: dict[str, list[object]] = {}

    def __getitem__(self, key: str):
        return self.attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self.attrs[key] = list(value)


class _FakeGraph:
    def __init__(self, n: int, edges, directed: bool) -> None:
        self.n = n
        self.edges = [
            tuple(int(value) for value in edge)
            for edge in edges
        ]
        self.directed = directed
        self.vs = _FakeAttrSeq()
        self.es = _FakeAttrSeq()
        self.graph_attrs: dict[str, object] = {}

    def __getitem__(self, key: str):
        return self.graph_attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self.graph_attrs[key] = value

    def attributes(self) -> list[str]:
        return list(self.graph_attrs)


class _FakeProcess:
    def __init__(self, stderr_lines: list[str], returncode: int = 0) -> None:
        self.stderr = iter(stderr_lines)
        self.returncode = returncode

    def wait(self) -> int:
        return self.returncode


def _detail_messages(progress_cb: mock.Mock) -> list[str]:
    return [
        call.kwargs["detail"]
        for call in progress_cb.call_args_list
        if call.args and call.args[0] == "detail"
    ]


def _write_adjacency_sidecars(
    graph_dir: Path,
    offsets: list[int],
    targets: list[int],
    lengths: list[float],
) -> None:
    np.asarray(offsets, dtype=loader.ADJ_OFFSETS_DTYPE).tofile(
        graph_dir / "walk_graph.adjacency_offsets.bin"
    )
    np.asarray(targets, dtype=loader.ADJ_TARGETS_DTYPE).tofile(
        graph_dir / "walk_graph.adjacency_targets.bin"
    )
    np.asarray(lengths, dtype=loader.ADJ_LENGTHS_DTYPE).tofile(
        graph_dir / "walk_graph.adjacency_lengths.bin"
    )


class NetworkLoaderTests(TestCase):
    def test_run_walkgraph_build_invokes_cli_and_streams_stderr(self) -> None:
        progress = mock.Mock()
        process = _FakeProcess(["pass 1/2\n", "pass 2/2\n"])

        with (
            mock.patch.object(loader, "ensure_walkgraph_subcommand_available"),
            mock.patch.object(loader.subprocess, "Popen", return_value=process) as popen_mock,
        ):
            loader.run_walkgraph_build(
                Path("ireland.osm.pbf"),
                Path("cache/walk_graph"),
                walkgraph_bin="walkgraph-dev",
                bbox=(51.4, -10.5, 55.4, -5.3),
                bbox_padding_m=500.0,
                extract_fingerprint="extract-fp",
                progress_cb=progress,
            )

        command = popen_mock.call_args.args[0]
        self.assertEqual(command[:2], ["walkgraph-dev", "build"])
        self.assertIn("--pbf", command)
        self.assertIn("--out", command)
        self.assertIn("--bbox", command)
        self.assertIn("--bbox-padding-m", command)
        self.assertIn("--extract-fingerprint", command)
        self.assertEqual(
            _detail_messages(progress),
            ["walkgraph: pass 1/2", "walkgraph: pass 2/2"],
        )

    def test_run_walkgraph_reachability_invokes_cli_and_streams_stderr(self) -> None:
        progress = mock.Mock()
        process = _FakeProcess(["loading graph\n", "completed\n"])

        with (
            mock.patch.object(loader, "ensure_walkgraph_subcommand_available"),
            mock.patch.object(loader.subprocess, "Popen", return_value=process) as popen_mock,
        ):
            loader.run_walkgraph_reachability(
                Path("cache/walk_graph"),
                Path("cache/origins.bin"),
                Path("cache/amenity_weights.bin"),
                Path("cache/out.bin"),
                category_count=4,
                cutoff_m=500.0,
                walkgraph_bin="walkgraph-dev",
                progress_cb=progress,
            )

        command = popen_mock.call_args.args[0]
        self.assertEqual(command[:2], ["walkgraph-dev", "reachability"])
        self.assertIn("--graph-dir", command)
        self.assertIn("--origins-bin", command)
        self.assertIn("--amenity-weights-bin", command)
        self.assertIn("--category-count", command)
        self.assertIn("--cutoff-m", command)
        self.assertIn("--out", command)
        self.assertEqual(
            _detail_messages(progress),
            ["walkgraph: loading graph", "walkgraph: completed"],
        )

    def test_run_surface_shell_build_invokes_cli_and_streams_stderr(self) -> None:
        progress = mock.Mock()
        process = _FakeProcess(["queued shard 1\n", "queued shard 2\n"])

        with (
            mock.patch.object(loader, "ensure_walkgraph_subcommand_available"),
            mock.patch.object(loader.subprocess, "Popen", return_value=process) as popen_mock,
        ):
            loader.run_surface_shell_build(
                Path("cache/nodes.bin"),
                Path("cache/study_area.geojson"),
                Path("cache/shell"),
                walkgraph_bin="walkgraph-dev",
                surface_shell_hash="shell-hash-123",
                reach_hash="reach-hash-123",
                node_count=10,
                config_json_path=Path("cache/config.json"),
                progress_cb=progress,
            )

        command = popen_mock.call_args.args[0]
        self.assertEqual(command[:2], ["walkgraph-dev", "surface"])
        self.assertIn("--nodes-bin", command)
        self.assertIn("--study-area", command)
        self.assertIn("--shell-dir", command)
        self.assertIn("--surface-shell-hash", command)
        self.assertIn("--reach-hash", command)
        self.assertIn("--node-count", command)
        self.assertIn("--config-json", command)
        self.assertEqual(
            _detail_messages(progress),
            ["surface: queued shard 1", "surface: queued shard 2"],
        )

    def test_run_walkgraph_build_wraps_missing_binary(self) -> None:
        with (
            mock.patch.object(loader, "ensure_walkgraph_subcommand_available"),
            mock.patch.object(loader.subprocess, "Popen", side_effect=FileNotFoundError("missing")),
        ):
            with self.assertRaisesRegex(RuntimeError, "before subcommand 'build' could run"):
                loader.run_walkgraph_build(Path("ireland.osm.pbf"), Path("cache/walk_graph"))

    def test_run_walkgraph_build_fails_before_spawn_when_subcommand_is_missing(self) -> None:
        with (
            mock.patch.object(
                loader,
                "ensure_walkgraph_subcommand_available",
                side_effect=walkgraph_runtime_error(
                    "Configured walkgraph binary 'walkgraph-dev' does not support required subcommand 'build'."
                ),
            ),
            mock.patch.object(loader.subprocess, "Popen") as popen_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "required subcommand 'build'"):
                loader.run_walkgraph_build(
                    Path("ireland.osm.pbf"),
                    Path("cache/walk_graph"),
                    walkgraph_bin="walkgraph-dev",
                )

        popen_mock.assert_not_called()

    def test_run_walkgraph_reachability_fails_before_spawn_when_subcommand_is_missing(self) -> None:
        with (
            mock.patch.object(
                loader,
                "ensure_walkgraph_subcommand_available",
                side_effect=walkgraph_runtime_error(
                    "Configured walkgraph binary 'walkgraph-dev' does not support required subcommand 'reachability'."
                ),
            ),
            mock.patch.object(loader.subprocess, "Popen") as popen_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "required subcommand 'reachability'"):
                loader.run_walkgraph_reachability(
                    Path("cache/walk_graph"),
                    Path("cache/origins.bin"),
                    Path("cache/amenity_weights.bin"),
                    Path("cache/out.bin"),
                    category_count=4,
                    cutoff_m=500.0,
                    walkgraph_bin="walkgraph-dev",
                )

        popen_mock.assert_not_called()

    def test_run_surface_shell_build_fails_before_spawn_when_subcommand_is_missing(self) -> None:
        with (
            mock.patch.object(
                loader,
                "ensure_walkgraph_subcommand_available",
                side_effect=walkgraph_runtime_error(
                    "Configured walkgraph binary 'walkgraph-dev' does not support required subcommand 'surface'."
                ),
            ),
            mock.patch.object(loader.subprocess, "Popen") as popen_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "required subcommand 'surface'"):
                loader.run_surface_shell_build(
                    Path("cache/nodes.bin"),
                    Path("cache/study_area.geojson"),
                    Path("cache/shell"),
                    walkgraph_bin="walkgraph-dev",
                    surface_shell_hash="shell-hash-123",
                    reach_hash="reach-hash-123",
                    node_count=10,
                    config_json_path=Path("cache/config.json"),
                )

        popen_mock.assert_not_called()

    def test_load_walk_graph_reads_sidecars(self) -> None:
        with TemporaryDirectory() as temp_dir:
            graph_dir = Path(temp_dir)
            nodes = np.array([(53.0, -6.0), (54.0, -5.0)], dtype=loader.NODES_DTYPE)
            edges = np.array([(0, 1, 12.5), (1, 0, 12.5)], dtype=loader.EDGES_DTYPE)
            osmids = np.array([101, 202], dtype=loader.OSMIDS_DTYPE)
            nodes.tofile(graph_dir / "walk_graph.nodes.bin")
            edges.tofile(graph_dir / "walk_graph.edges.bin")
            osmids.tofile(graph_dir / "walk_graph.osmids.bin")
            _write_adjacency_sidecars(graph_dir, [0, 1, 2], [1, 0], [12.5, 12.5])
            (graph_dir / "walk_graph.meta.json").write_text(
                json.dumps(
                    {
                        "format_version": loader.GRAPH_FORMAT_VERSION,
                        "node_count": 2,
                        "edge_count": 2,
                        "extract_fingerprint": "extract-fp",
                        "bbox": None,
                        "bbox_padding_m": 0.0,
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(loader, "ig", SimpleNamespace(Graph=_FakeGraph)):
                graph = loader.load_walk_graph(graph_dir)

        self.assertEqual(graph.n, 2)
        self.assertEqual(graph.edges, [(0, 1), (1, 0)])
        self.assertTrue(graph.directed)
        self.assertEqual(graph.vs["lat"], [53.0, 54.0])
        self.assertEqual(graph.vs["lon"], [-6.0, -5.0])
        self.assertEqual(graph.vs["osmid"], [101, 202])
        self.assertEqual(graph.es["length_m"], [12.5, 12.5])

    def test_load_walk_graph_index_reads_compact_sidecars_without_igraph(self) -> None:
        with TemporaryDirectory() as temp_dir:
            graph_dir = Path(temp_dir)
            nodes = np.array([(53.0, -6.0), (54.0, -5.0)], dtype=loader.NODES_DTYPE)
            edges = np.array([(0, 1, 12.5), (1, 0, 12.5)], dtype=loader.EDGES_DTYPE)
            osmids = np.array([101, 202], dtype=loader.OSMIDS_DTYPE)
            nodes.tofile(graph_dir / "walk_graph.nodes.bin")
            edges.tofile(graph_dir / "walk_graph.edges.bin")
            osmids.tofile(graph_dir / "walk_graph.osmids.bin")
            _write_adjacency_sidecars(graph_dir, [0, 1, 2], [1, 0], [12.5, 12.5])
            (graph_dir / "walk_graph.meta.json").write_text(
                json.dumps(
                    {
                        "format_version": loader.GRAPH_FORMAT_VERSION,
                        "node_count": 2,
                        "edge_count": 2,
                        "extract_fingerprint": "extract-fp",
                        "bbox": None,
                        "bbox_padding_m": 0.0,
                    }
                ),
                encoding="utf-8",
            )

            graph = loader.load_walk_graph_index(graph_dir)

        self.assertEqual(graph.vcount(), 2)
        self.assertEqual(graph.ecount(), 2)
        self.assertEqual(graph["_node_latitudes"].tolist(), [53.0, 54.0])
        self.assertEqual(graph["_node_longitudes"].tolist(), [-6.0, -5.0])

    def test_load_walk_graph_uses_compact_arrays_for_large_graphs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            graph_dir = Path(temp_dir)
            nodes = np.array([(53.0, -6.0), (54.0, -5.0)], dtype=loader.NODES_DTYPE)
            edges = np.array([(0, 1, 12.5), (1, 0, 12.5)], dtype=loader.EDGES_DTYPE)
            osmids = np.array([101, 202], dtype=loader.OSMIDS_DTYPE)
            nodes.tofile(graph_dir / "walk_graph.nodes.bin")
            edges.tofile(graph_dir / "walk_graph.edges.bin")
            osmids.tofile(graph_dir / "walk_graph.osmids.bin")
            _write_adjacency_sidecars(graph_dir, [0, 1, 2], [1, 0], [12.5, 12.5])
            (graph_dir / "walk_graph.meta.json").write_text(
                json.dumps(
                    {
                        "format_version": loader.GRAPH_FORMAT_VERSION,
                        "node_count": 2,
                        "edge_count": 2,
                        "extract_fingerprint": "extract-fp",
                        "bbox": None,
                        "bbox_padding_m": 0.0,
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(loader, "ig", SimpleNamespace(Graph=_FakeGraph)),
                mock.patch.object(loader, "COMPACT_VERTEX_ATTR_THRESHOLD", 1),
                mock.patch.object(loader, "COMPACT_EDGE_ATTR_THRESHOLD", 1),
            ):
                graph = loader.load_walk_graph(graph_dir)

        self.assertEqual(graph.n, 2)
        self.assertEqual(graph.edges, [(0, 1), (1, 0)])
        self.assertEqual(graph["_node_latitudes"].tolist(), [53.0, 54.0])
        self.assertEqual(graph["_node_longitudes"].tolist(), [-6.0, -5.0])
        self.assertEqual(graph["_edge_length_m"].tolist(), [12.5, 12.5])
        self.assertEqual(graph.vs.attrs, {})
        self.assertEqual(graph.es.attrs, {})

    def test_graph_meta_matches_requires_sidecars_and_matching_fingerprint(self) -> None:
        with TemporaryDirectory() as temp_dir:
            graph_dir = Path(temp_dir)
            (graph_dir / "walk_graph.meta.json").write_text(
                json.dumps(
                    {
                        "format_version": loader.GRAPH_FORMAT_VERSION,
                        "extract_fingerprint": "extract-fp",
                        "bbox": {
                            "min_lat": 51.4,
                            "min_lon": -10.5,
                            "max_lat": 55.4,
                            "max_lon": -5.3,
                        },
                        "bbox_padding_m": 500.0,
                    }
                ),
                encoding="utf-8",
            )

            self.assertFalse(
                loader.graph_meta_matches(
                    graph_dir,
                    extract_fingerprint="extract-fp",
                    bbox=(51.4, -10.5, 55.4, -5.3),
                    bbox_padding_m=500.0,
                )
            )

            np.array([(53.0, -6.0)], dtype=loader.NODES_DTYPE).tofile(graph_dir / "walk_graph.nodes.bin")
            np.array([(0, 0, 1.0)], dtype=loader.EDGES_DTYPE).tofile(graph_dir / "walk_graph.edges.bin")
            np.array([1], dtype=loader.OSMIDS_DTYPE).tofile(graph_dir / "walk_graph.osmids.bin")
            _write_adjacency_sidecars(graph_dir, [0, 1], [0], [1.0])

            self.assertTrue(
                loader.graph_meta_matches(
                    graph_dir,
                    extract_fingerprint="extract-fp",
                    bbox=(51.4, -10.5, 55.4, -5.3),
                    bbox_padding_m=500.0,
                )
            )
            self.assertFalse(
                loader.graph_meta_matches(
                    graph_dir,
                    extract_fingerprint="other-fp",
                    bbox=(51.4, -10.5, 55.4, -5.3),
                    bbox_padding_m=500.0,
                )
            )
