from __future__ import annotations

from contextlib import ExitStack
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

from shapely.geometry import Point, box
from shapely.ops import transform

from config import TO_TARGET, TO_WGS84
from precompute import display_layers as display
from precompute import service_deserts as deserts
from precompute.daily_access_build import DailyAccessBuildError


NOW = datetime(2026, 10, 23, tzinfo=timezone.utc)


def stop(**changes):
    row = {
        "source_ref": "gtfs/nta/one", "stop_name": "Example", "feed_id": "nta", "stop_id": "one",
        "reality_status": "active", "source_status": "gtfs_direct", "school_only_state": "not_school_only",
        "public_departures_7d": 0, "public_departures_30d": 10, "school_only_departures_30d": 0,
        "last_public_service_date": None, "last_any_service_date": None,
        "route_modes_json": ["bus"], "source_reason_codes_json": [], "reality_reason_codes_json": [],
        "geom": Point(-8.48, 51.90), "transport_score_units": 2,
        "bus_frequency_tier": "infrequent", "has_any_bus_service": True,
    }
    return {**row, **changes}


def context(profile="test"):
    metric = transform(lambda x, y, z=None: TO_TARGET(x, y), box(-8.50, 51.89, -8.46, 51.91))
    return SimpleNamespace(
        engine=mock.sentinel.engine, profile=profile, graph_dir=Path("graph"),
        reality_fingerprint="gtfs-current", source_state=SimpleNamespace(extract_path=Path("input.pbf")),
        study_area_wgs84=box(-8.50, 51.89, -8.46, 51.91), study_area_metric=metric,
        transit_reality_state=SimpleNamespace(reality_fingerprint="gtfs-current", analysis_date=date(2026, 10, 23)),
        transit_analysis_window_days=30, transit_service_desert_window_days=7,
    )


HASHES = SimpleNamespace(build_key="v4-key", config_hash="stage", import_fingerprint="osm-current")


class DisplayPreparationTests(TestCase):
    def inputs(self, stack, *, reality_rows=None, source_rows=None):
        source = {"category": "shops", "source": "osm_local_pbf", "source_ref": "node/1",
                  "name": "Shop", "tags_json": {"shop": "supermarket"}, "geom": Point(-8.48, 51.90)}
        source_rows = [source] if source_rows is None else source_rows
        mocks = {
            "get_resolved_artifact_for_mode": None, "load_source_amenity_rows": source_rows,
            "load_merged_source_amenity_rows": (source_rows, {}),
            "load_transport_reality_points": [stop()] if reality_rows is None else reality_rows,
            "load_walk_graph_index": mock.sentinel.graph, "compute_service_desert_rows": [],
            "load_landuse_context_rows": [], "load_road_rows": [],
            "load_transit_route_network_rows": ([], [], []),
            "load_transit_railway_corridor_manifest": None, "load_railway_corridor_rows": [],
        }
        handles = {name: stack.enter_context(mock.patch.object(display, name, return_value=value)) for name, value in mocks.items()}
        stack.enter_context(mock.patch.object(display.overture, "load_overture_amenity_rows", return_value=[]))
        stack.enter_context(mock.patch.object(display.overture, "dataset_info", return_value={}))
        return handles

    def test_layers_and_summary_come_from_inputs_without_a_v3_build(self):
        with ExitStack() as stack:
            handles = self.inputs(stack)
            result = display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)
        self.assertEqual(result.counts(), {
            "amenities": 1, "transport_reality": 1, "transport_route_patterns": 0,
            "transport_transfer_hubs": 0, "service_deserts": 0,
        })
        self.assertEqual(result.amenities[0]["name"], "Shop")
        self.assertEqual(result.amenities[0]["build_key"], "v4-key")
        self.assertEqual(result.summary["amenity_counts"]["shops"], 1)
        self.assertEqual(result.summary["transit_analysis_date"], "2026-10-23")
        self.assertEqual(result.summary["display_layers"]["transit_reality_fingerprint"], "gtfs-current")
        self.assertEqual(result.summary["landuse_context_counts"], {})
        self.assertFalse(result.summary["noise_enabled"])
        self.assertTrue(result.warnings)
        handles["load_transport_reality_points"].assert_called_once_with(mock.sentinel.engine, "gtfs-current")

    def test_optional_empty_layers_and_cached_publish_are_deterministic(self):
        with ExitStack() as stack:
            self.inputs(stack, source_rows=[])
            first = display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)
            cached = display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)
        self.assertEqual(first.amenities, [])
        self.assertEqual(first.summary, cached.summary)
        self.assertEqual(first.transport, cached.transport)
        self.assertEqual(first.summary["amenity_counts"], dict.fromkeys(display.TAGS, 0))

    def test_missing_transport_and_failed_source_do_not_silently_disable_layers(self):
        with ExitStack() as stack:
            handles = self.inputs(stack, reality_rows=[])
            with self.assertRaisesRegex(DailyAccessBuildError, "No GTFS transport display"):
                display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)
            allowed = display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW, allow_missing_transport=True)
            self.assertEqual(allowed.transport, [])
            handles["load_source_amenity_rows"].side_effect = RuntimeError("source unavailable")
            with self.assertRaisesRegex(RuntimeError, "source unavailable"):
                display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)

    def test_full_requires_accurate_artifact_even_with_dev_env(self):
        with mock.patch.object(display, "get_resolved_artifact_for_mode", return_value=None) as lookup:
            with self.assertRaisesRegex(DailyAccessBuildError, "accurate noise artifact"):
                display.select_noise_artifact(context("full"))
        lookup.assert_called_once_with(mock.sentinel.engine, "accurate")

    def test_display_rejects_a_different_gtfs_snapshot(self):
        ctx = context()
        ctx.transit_reality_state.reality_fingerprint = "wrong"
        with self.assertRaisesRegex(DailyAccessBuildError, "same GTFS snapshot"):
            display.prepare_display_layers(ctx, {}, hashes=HASHES, created_at=NOW)

    def test_distant_stops_do_not_snap_to_the_regional_graph(self):
        with ExitStack() as stack:
            handles = self.inputs(stack, reality_rows=[stop(), stop(geom=Point(-6.26, 53.35))])
            result = display.prepare_display_layers(context(), {}, hashes=HASHES, created_at=NOW)
        self.assertEqual(len(result.transport), 2)
        self.assertEqual(len(handles["compute_service_desert_rows"].call_args.args[2]), 1)


class ServiceDesertTests(TestCase):
    def test_no_departures_public_departures_and_no_reachable_stop(self):
        ctx = context()
        cells = {5000: [{"cell_id": name, "geometry": box(-8.49, 51.89, -8.47, 51.91)}
                        for name in ("desert", "served", "no-stop")]}
        with (
            mock.patch.object(deserts.network, "snap_amenities", return_value={"transport": [0]}),
            mock.patch.object(deserts.network, "nearest_nodes", return_value=[0]),
            mock.patch.object(deserts.network, "precompute_walk_counts_by_origin_node", return_value={0: {"transport": 1}, 1: {"transport": 1}}),
            mock.patch.object(deserts.network, "precompute_walk_weighted_totals_by_origin_node", return_value={1: {"public_departures_7d": 4}}),
        ):
            rows = deserts.compute_service_desert_rows(
                mock.sentinel.graph, cells, [stop()], hashes=HASHES, reality_state=ctx.transit_reality_state,
                study_area_metric=ctx.study_area_metric, created_at=NOW, cell_nodes_by_resolution={5000: [0, 1, 2]},
            )
        self.assertEqual([row["cell_id"] for row in rows], ["desert"])
        self.assertEqual(rows[0]["analysis_date"], date(2026, 10, 23))

    def test_no_baseline_stops_is_a_valid_empty_result(self):
        ctx = context()
        rows = deserts.compute_service_desert_rows(
            mock.sentinel.graph, {}, [], hashes=HASHES, reality_state=ctx.transit_reality_state,
            study_area_metric=ctx.study_area_metric, created_at=NOW,
        )
        self.assertEqual(rows, [])
