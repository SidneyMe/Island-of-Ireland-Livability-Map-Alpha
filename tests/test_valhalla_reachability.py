from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock
import urllib.error

from precompute.cache import cache_load, cache_save
from precompute.phases import phase_reachability_impl
from precompute.reachability_arrays import ReachabilityMatrix
from precompute.reachability_arrays import save_reachability_cache
from precompute.valhalla_reachability import (
    ValhallaClient,
    ValhallaReachabilityError,
    ValhallaSettings,
    route_valhalla_reachability,
)


class _Graph:
    def __init__(self, latitudes: list[float], longitudes: list[float]) -> None:
        self._attrs = {
            "_node_latitudes": list(latitudes),
            "_node_longitudes": list(longitudes),
        }

    def attributes(self) -> list[str]:
        return list(self._attrs)

    def __getitem__(self, key: str):
        return self._attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self._attrs[key] = value

    def vcount(self) -> int:
        return len(self._attrs["_node_latitudes"])


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self) -> bytes:
        if isinstance(self.payload, bytes):
            return self.payload
        return json.dumps(self.payload).encode("utf-8")


def _settings(*, max_targets: int = 100) -> ValhallaSettings:
    return ValhallaSettings(
        url="http://valhalla.test:8002",
        expected_version="3.8.3",
        workers=2,
        timeout_s=5.0,
        max_targets_per_request=max_targets,
    )


def _route(
    graph: _Graph,
    raw: dict[str, list[int]],
    clusters: dict[str, list[int]],
    units: dict[str, list[tuple[int, int]]],
    *,
    missing_raw=(0,),
    missing_clusters=(0,),
    missing_effective=(0,),
    settings: ValhallaSettings | None = None,
):
    return route_valhalla_reachability(
        graph,
        raw,
        clusters,
        units,
        missing_count_nodes=missing_raw,
        missing_cluster_count_nodes=missing_clusters,
        missing_effective_nodes=missing_effective,
        raw_categories=["shops"],
        cluster_categories=["shops"],
        effective_categories=["shops"],
        radius_m=500.0,
        half_distances_m={"shops": 150.0},
        settings=settings or _settings(),
    )


class ValhallaReachabilityTests(TestCase):
    def test_no_straight_line_candidate_does_not_make_http_request(self) -> None:
        graph = _Graph([53.0, 53.01], [-6.0, -6.0])
        with mock.patch.object(ValhallaClient, "route_distances_m") as route_mock:
            raw, clusters, units = _route(
                graph, {"shops": [1]}, {"shops": [1]}, {"shops": [(1, 2)]}
            )

        route_mock.assert_not_called()
        self.assertEqual(raw.to_sparse_dict(), {0: {}})
        self.assertEqual(clusters.to_sparse_dict(), {0: {}})
        self.assertEqual(units.to_sparse_dict(), {0: {}})

    def test_network_distance_over_radius_is_rejected_after_prefilter(self) -> None:
        graph = _Graph([53.0, 53.0001], [-6.0, -6.0])
        with mock.patch.object(ValhallaClient, "route_distances_m", return_value=[501.0]) as route_mock:
            raw, clusters, units = _route(
                graph, {"shops": [1]}, {"shops": [1]}, {"shops": [(1, 2)]}
            )

        route_mock.assert_called_once()
        self.assertEqual(raw.to_sparse_dict(), {0: {}})
        self.assertEqual(clusters.to_sparse_dict(), {0: {}})
        self.assertEqual(units.to_sparse_dict(), {0: {}})

    def test_one_routed_target_preserves_multiplicity_and_feeds_all_metrics(self) -> None:
        graph = _Graph([53.0, 53.0001], [-6.0, -6.0])
        with mock.patch.object(ValhallaClient, "route_distances_m", return_value=[150.0]) as route_mock:
            raw, clusters, units = _route(
                graph,
                {"shops": [1, 1, 1]},
                {"shops": [1]},
                {"shops": [(1, 2)]},
            )

        route_mock.assert_called_once()
        self.assertEqual(raw.to_sparse_dict(), {0: {"shops": 3}})
        self.assertEqual(clusters.to_sparse_dict(), {0: {"shops": 1}})
        self.assertEqual(units.to_sparse_dict(), {0: {"shops": 1.0}})

    def test_target_batches_are_all_routed_and_no_route_is_unreachable(self) -> None:
        graph = _Graph([53.0, 53.0001, 53.0002, 53.0003], [-6.0] * 4)
        with mock.patch.object(
            ValhallaClient, "route_distances_m", side_effect=[[10.0, None], [20.0]]
        ) as route_mock:
            raw, clusters, units = _route(
                graph,
                {"shops": [1, 2, 3]},
                {"shops": [1, 2, 3]},
                {"shops": [(1, 1), (2, 1), (3, 1)]},
                settings=_settings(max_targets=2),
            )

        self.assertEqual(route_mock.call_count, 2)
        self.assertEqual(raw.to_sparse_dict(), {0: {"shops": 2}})
        self.assertEqual(clusters.to_sparse_dict(), {0: {"shops": 2}})
        self.assertAlmostEqual(units.to_sparse_dict()[0]["shops"], 2 ** (-10.0 / 150.0) + 2 ** (-20.0 / 150.0), places=6)

    def test_partial_metric_origins_are_routed_once_and_only_requested_rows_exist(self) -> None:
        graph = _Graph([53.0, 53.0001, 53.0002], [-6.0, -6.0, -6.0])
        with mock.patch.object(ValhallaClient, "route_distances_m", return_value=[10.0]) as route_mock:
            raw, clusters, units = _route(
                graph,
                {"shops": [2]},
                {"shops": [2]},
                {"shops": [(2, 1)]},
                missing_raw=(0,),
                missing_clusters=(1,),
                missing_effective=(0, 1),
            )

        self.assertEqual(route_mock.call_count, 2)
        self.assertEqual(raw.origin_ids.tolist(), [0])
        self.assertEqual(clusters.origin_ids.tolist(), [1])
        self.assertEqual(units.origin_ids.tolist(), [0, 1])

    def test_status_is_checked_once_and_response_errors_are_clear(self) -> None:
        client = ValhallaClient(_settings())
        responses = [
            _Response({"version": "3.8.3"}),
            _Response({"units": "kilometers", "sources_to_targets": {"distances": [[0.1]]}}),
            _Response({"units": "kilometers", "sources_to_targets": {"distances": [[0.2]]}}),
        ]
        with mock.patch("urllib.request.urlopen", side_effect=responses) as urlopen_mock:
            self.assertEqual(client.route_distances_m(53.0, -6.0, [53.1], [-6.1]), [100.0])
            self.assertEqual(client.route_distances_m(53.0, -6.0, [53.1], [-6.1]), [200.0])
        self.assertEqual(urlopen_mock.call_count, 3)

        bad_client = ValhallaClient(_settings())
        with mock.patch("urllib.request.urlopen", return_value=_Response({"version": "3.8.2"})):
            with self.assertRaisesRegex(ValhallaReachabilityError, "version mismatch"):
                bad_client.route_distances_m(53.0, -6.0, [53.1], [-6.1])

        malformed_client = ValhallaClient(_settings())
        with mock.patch(
            "urllib.request.urlopen",
            side_effect=[_Response({"version": "3.8.3"}), _Response(b"not json")],
        ):
            with self.assertRaisesRegex(ValhallaReachabilityError, "malformed JSON"):
                malformed_client.route_distances_m(53.0, -6.0, [53.1], [-6.1])

        unavailable_client = ValhallaClient(_settings())
        with mock.patch(
            "urllib.request.urlopen", side_effect=urllib.error.URLError("offline")
        ):
            with self.assertRaisesRegex(ValhallaReachabilityError, "request failed"):
                unavailable_client.route_distances_m(53.0, -6.0, [53.1], [-6.1])

    def test_checkpoint_callback_receives_only_missing_metric_rows(self) -> None:
        graph = _Graph([53.0, 53.0001], [-6.0, -6.0])
        captured: list[tuple[ReachabilityMatrix, ReachabilityMatrix, ReachabilityMatrix]] = []
        with mock.patch.object(ValhallaClient, "route_distances_m", return_value=[10.0]):
            result = route_valhalla_reachability(
                graph,
                {"shops": [1]},
                {"shops": [1]},
                {"shops": [(1, 1)]},
                missing_count_nodes=[0],
                missing_cluster_count_nodes=[],
                missing_effective_nodes=[0],
                raw_categories=["shops"],
                cluster_categories=["shops"],
                effective_categories=["shops"],
                radius_m=500.0,
                half_distances_m={"shops": 150.0},
                settings=_settings(),
                save_chunk_cb=lambda raw, clusters, units: captured.append((raw, clusters, units)),
            )

        self.assertEqual(result[0].origin_ids.tolist(), [])
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0].origin_ids.tolist(), [0])
        self.assertEqual(captured[0][1].origin_ids.tolist(), [])
        self.assertEqual(captured[0][2].origin_ids.tolist(), [0])

    def test_phase_resumes_partial_metric_caches_with_one_valhalla_call(self) -> None:
        graph = _Graph([53.0, 53.0001], [-6.0, -6.0])
        routed = []

        def fake_router(*args, **kwargs):
            routed.append(
                (
                    kwargs["missing_count_nodes"],
                    kwargs["missing_cluster_count_nodes"],
                    kwargs["missing_effective_nodes"],
                )
            )
            callback = kwargs["save_chunk_cb"]
            callback(
                ReachabilityMatrix.from_sparse_dict(
                    {1: {"shops": 1}}, ["shops"], value_kind="counts"
                ),
                ReachabilityMatrix.from_sparse_dict(
                    {0: {"shops": 1}}, ["shops"], value_kind="counts"
                ),
                ReachabilityMatrix.from_sparse_dict(
                    {1: {"shops": 0.5}}, ["shops"], value_kind="effective_units"
                ),
            )
            return (
                ReachabilityMatrix.empty(["shops"], value_kind="counts"),
                ReachabilityMatrix.empty(["shops"], value_kind="counts"),
                ReachabilityMatrix.empty(["shops"], value_kind="effective_units"),
            )

        with TemporaryDirectory() as temp_name:
            cache_dir = Path(temp_name)
            cache_save("walk_nodes_by_cat", {"shops": [1]}, cache_dir)
            cache_save(
                "amenity_clusters",
                [{"category": "shops", "lat": 53.0001, "lon": -6.0, "base_units": 1}],
                cache_dir,
            )
            cache_save("walk_cluster_nodes_by_cat", {"shops": [1]}, cache_dir)
            save_reachability_cache(
                "walk_counts_by_origin_node",
                cache_dir,
                ReachabilityMatrix.from_sparse_dict(
                    {0: {"shops": 1}}, ["shops"], value_kind="counts"
                ),
            )
            save_reachability_cache(
                "walk_cluster_counts_by_origin_node",
                cache_dir,
                ReachabilityMatrix.from_sparse_dict(
                    {1: {"shops": 1}}, ["shops"], value_kind="counts"
                ),
            )
            save_reachability_cache(
                "walk_effective_units_by_origin_node",
                cache_dir,
                ReachabilityMatrix.from_sparse_dict(
                    {0: {"shops": 1.0}}, ["shops"], value_kind="effective_units"
                ),
            )
            tracker = mock.Mock()
            tracker.phase_callback.return_value = lambda *args, **kwargs: None
            _, raw, clusters, units = phase_reachability_impl(
                graph,
                {},
                [],
                tracker,
                walk_origin_node_ids=[0, 1],
                cache_dir=cache_dir,
                reach_hash="reach-hash",
                tiers_building=set(),
                walk_radius_m=500.0,
                cache_load=lambda key, directory: cache_load(
                    key, directory, force_recompute=False, tier_valid={directory: True}
                ),
                cache_save=cache_save,
                cache_load_large=None,
                mark_building=lambda *args: None,
                mark_complete=lambda *args: None,
                snap_amenities=lambda *args: {"shops": [1]},
                normalize_origin_node_ids=lambda nodes: sorted(set(nodes)),
                precompute_walk_count_matrix_by_origin_node=mock.Mock(),
                precompute_walk_decayed_units_matrix_by_origin_node=mock.Mock(),
                walk_routing_backend="valhalla",
                valhalla_settings=_settings(),
                route_valhalla_reachability=fake_router,
            )

        self.assertEqual(routed, [((1,), (0,), (1,))])
        self.assertEqual(raw.to_sparse_dict(), {0: {"shops": 1}, 1: {"shops": 1}})
        self.assertEqual(clusters.to_sparse_dict(), {0: {"shops": 1}, 1: {"shops": 1}})
        self.assertEqual(units.to_sparse_dict(), {0: {"shops": 1.0}, 1: {"shops": 0.5}})
