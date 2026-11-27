from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from shapely.geometry import LineString, Point

from transit.route_assets import publish_route_assets
from transit import workflow as transit_workflow
from db_postgis.tables import (
    transport_reality,
    transport_route_patterns,
    transport_transfer_hubs,
    transit_route_pattern_stops,
    transit_route_patterns,
    transit_transfer_hubs,
)
from transit.route_network import (
    BACKBONE_MIN_ZOOM,
    LOCAL_MIN_ZOOM,
    REGIONAL_MIN_ZOOM,
    RoutePattern,
    RouteStop,
    _build_transfer_hubs,
    _station_connection_count,
    _visibility_class,
    build_route_network_materialization,
    display_route_mode,
    route_colors,
    route_key,
    route_network_hash_for_state,
)
from transit.translink_geometry import (
    MatchedGeometry,
    TranslinkGeometrySourceState,
    TranslinkGeometrySources,
    _RouteGraph,
    line_diva_service_key,
    load_translink_geometry_sources,
    normalize_service_key,
)


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return iter(self._rows)


class _RowsEngine:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table

    @contextmanager
    def connect(self):
        engine = self

        class _Connection:
            def execute(self, statement):
                table = statement.get_final_froms()[0]
                return _RowsResult(engine.rows_by_table.get(table.name, []))

        yield _Connection()


class _Router:
    def __init__(self, *, available: bool, geometry=None):
        self.available = available
        self.geometry = geometry
        self.calls = 0

    def preflight(self):
        return self.available

    def route(self, points):
        self.calls += 1
        return self.geometry


def _stop(route: str, stop_id: str, x: float, y: float, *, name: str, parent: str | None = None) -> RouteStop:
    return RouteStop(route, "pattern-" + route, "feed", stop_id, 0, name, parent, Point(x, y))


class TransitRouteNetworkTests(TestCase):
    def _source_state(self, suffix: str = "a") -> TranslinkGeometrySourceState:
        return TranslinkGeometrySourceState(
            ulsterbus_path=Path("ulster.zip"),
            metro_path=Path("metro.zip"),
            rail_path=Path("rail.geojson"),
            ulsterbus_fingerprint=f"ulster-{suffix}",
            metro_fingerprint=f"metro-{suffix}",
            rail_fingerprint=f"rail-{suffix}",
        )

    def test_route_tables_have_stable_keys_and_stop_membership_column(self) -> None:
        self.assertIn("route_keys_json", transport_reality.c)
        expected = {
            transport_route_patterns: {"build_key", "pattern_id"},
            transport_transfer_hubs: {"build_key", "hub_id"},
            transit_route_patterns: {"reality_fingerprint", "pattern_id"},
            transit_route_pattern_stops: {"reality_fingerprint", "pattern_id", "stop_sequence"},
            transit_transfer_hubs: {"reality_fingerprint", "hub_id"},
        }
        for table, columns in expected.items():
            with self.subTest(table=table.fullname):
                self.assertEqual({column.name for column in table.primary_key.columns}, columns)

    def _network_rows(self):
        return {
            "routes": [
                {"feed_id": "a", "route_id": "10", "route_type": 3, "route_short_name": "10", "route_long_name": "Alpha", "route_color": "112233", "route_text_color": None},
                {"feed_id": "b", "route_id": "10", "route_type": 3, "route_short_name": "10", "route_long_name": "Beta", "route_color": None, "route_text_color": None},
            ],
            "service_classification": [
                {"reality_fingerprint": "reality", "feed_id": "a", "service_id": "active", "school_only_state": "no", "reason_codes_json": ["weekday_service_present"]},
                {"reality_fingerprint": "reality", "feed_id": "a", "service_id": "school", "school_only_state": "yes", "reason_codes_json": ["weekday_service_present"]},
                {"reality_fingerprint": "reality", "feed_id": "b", "service_id": "active", "school_only_state": "no", "reason_codes_json": ["weekend_service_present"]},
            ],
            "trips": [
                {"feed_id": "a", "trip_id": "a1", "route_id": "10", "service_id": "active", "direction_id": 0, "shape_id": "shape-a", "trip_headsign": "A2"},
                {"feed_id": "a", "trip_id": "a2", "route_id": "10", "service_id": "active", "direction_id": 0, "shape_id": "shape-a", "trip_headsign": "A2"},
                {"feed_id": "a", "trip_id": "a3", "route_id": "10", "service_id": "active", "direction_id": 1, "shape_id": "shape-a", "trip_headsign": "A1"},
                {"feed_id": "a", "trip_id": "school", "route_id": "10", "service_id": "school", "direction_id": 1, "shape_id": "", "trip_headsign": "School"},
                {"feed_id": "b", "trip_id": "b1", "route_id": "10", "service_id": "active", "direction_id": 0, "shape_id": "", "trip_headsign": "B2"},
            ],
            "stop_times": [
                {"feed_id": "a", "trip_id": trip, "stop_id": stop, "stop_sequence": sequence}
                for trip in ("a1", "a2", "school")
                for sequence, stop in enumerate(("a-stop-1", "a-stop-2"))
            ] + [
                {"feed_id": "a", "trip_id": "a3", "stop_id": stop, "stop_sequence": sequence}
                for sequence, stop in enumerate(("a-stop-2", "a-stop-1"))
            ] + [
                {"feed_id": "b", "trip_id": "b1", "stop_id": stop, "stop_sequence": sequence}
                for sequence, stop in enumerate(("b-stop-1", "b-stop-2"))
            ],
            "stops": [
                {"feed_id": "a", "stop_id": "a-stop-1", "stop_name": "A1", "parent_station": None, "geom": Point(-6.2, 53.3)},
                {"feed_id": "a", "stop_id": "a-stop-2", "stop_name": "A2", "parent_station": None, "geom": Point(-6.1, 53.4)},
                {"feed_id": "b", "stop_id": "b-stop-1", "stop_name": "B1", "parent_station": None, "geom": Point(-7.2, 54.3)},
                {"feed_id": "b", "stop_id": "b-stop-2", "stop_name": "B2", "parent_station": None, "geom": Point(-7.1, 54.4)},
            ],
        }

    def _reality_state(self):
        return SimpleNamespace(
            reality_fingerprint="reality",
            feed_states=(
                SimpleNamespace(feed_id="a", zip_path=Path("a.zip")),
                SimpleNamespace(feed_id="b", zip_path=Path("b.zip")),
            ),
        )

    def test_materialization_scopes_route_ids_filters_school_and_dedupes_patterns(self) -> None:
        router = _Router(available=False)
        with (
            mock.patch("transit.route_network.to_shape", side_effect=lambda value: value),
            mock.patch("transit.route_network._shape_points_by_id", side_effect=lambda path: {"shape-a": [(0, 53.3, -6.2), (1, 53.4, -6.1)]} if path.name == "a.zip" else {}),
        ):
            result = build_route_network_materialization(
                _RowsEngine(self._network_rows()), reality_state=self._reality_state(), valhalla_router=router
            )
        self.assertEqual({row["route_key"] for row in result.patterns}, {"a:10", "b:10"})
        self.assertEqual(len(result.patterns), 3)
        self.assertEqual({row["headsign"] for row in result.patterns}, {"A1", "A2", "B2"})
        self.assertEqual(
            [row["geometry_source"] for row in result.patterns if row["route_key"] == "a:10"],
            ["gtfs_shape", "gtfs_shape"],
        )
        self.assertEqual(
            [row["geometry_source"] for row in result.patterns if row["route_key"] == "b:10"],
            ["stop_sequence_approximate"],
        )

    def test_materialization_uses_valhalla_once_when_shape_is_missing(self) -> None:
        rows = self._network_rows()
        rows["routes"] = [row for row in rows["routes"] if row["feed_id"] == "b"]
        rows["trips"] = [row for row in rows["trips"] if row["feed_id"] == "b"]
        rows["stop_times"] = [row for row in rows["stop_times"] if row["feed_id"] == "b"]
        rows["stops"] = [row for row in rows["stops"] if row["feed_id"] == "b"]
        router = _Router(available=True, geometry=LineString([(-7.2, 54.3), (-7.15, 54.35), (-7.1, 54.4)]))
        with (
            mock.patch("transit.route_network.to_shape", side_effect=lambda value: value),
            mock.patch("transit.route_network._shape_points_by_id", return_value={}),
        ):
            result = build_route_network_materialization(
                _RowsEngine(rows), reality_state=self._reality_state(), valhalla_router=router
            )
        self.assertEqual(router.calls, 1)
        self.assertEqual(result.patterns[0]["geometry_source"], "valhalla_auto")

    def test_translink_official_geometry_precedes_valhalla(self) -> None:
        rows = {
            "routes": [{
                "feed_id": "translink", "route_id": "7b", "agency_id": "51-49-ULB",
                "route_type": 3, "route_short_name": "7B", "route_long_name": "Official",
                "route_color": None, "route_text_color": None,
            }],
            "service_classification": [{
                "reality_fingerprint": "reality", "feed_id": "translink", "service_id": "active",
                "school_only_state": "no", "reason_codes_json": ["weekday_service_present"],
            }],
            "trips": [{
                "feed_id": "translink", "trip_id": "t", "route_id": "7b", "service_id": "active",
                "direction_id": 0, "shape_id": "", "trip_headsign": "B",
            }],
            "stop_times": [
                {"feed_id": "translink", "trip_id": "t", "stop_id": "a", "stop_sequence": 0},
                {"feed_id": "translink", "trip_id": "t", "stop_id": "b", "stop_sequence": 1},
            ],
            "stops": [
                {"feed_id": "translink", "stop_id": "a", "stop_name": "A", "parent_station": None, "geom": Point(-6.1, 54.5)},
                {"feed_id": "translink", "stop_id": "b", "stop_name": "B", "parent_station": None, "geom": Point(-6.0, 54.6)},
            ],
        }
        official = mock.Mock(warnings=[])
        official.match_bus_pattern.return_value = MatchedGeometry(
            LineString([(-6.1, 54.5), (-6.05, 54.56), (-6.0, 54.6)]),
            "translink_route_gis",
            1.0,
            0.0,
        )
        router = _Router(available=True, geometry=LineString([(-6.1, 54.5), (-6.0, 54.6)]))
        reality_state = SimpleNamespace(
            reality_fingerprint="reality",
            feed_states=(SimpleNamespace(feed_id="translink", zip_path=Path("translink.zip")),),
        )
        with (
            mock.patch("transit.route_network.to_shape", side_effect=lambda value: value),
            mock.patch("transit.route_network._shape_points_by_id", return_value={}),
            mock.patch("transit.route_network.load_translink_geometry_sources", return_value=official),
        ):
            result = build_route_network_materialization(
                _RowsEngine(rows),
                reality_state=reality_state,
                valhalla_router=router,
                translink_source_state=self._source_state(),
            )
        self.assertEqual(result.patterns[0]["geometry_source"], "translink_route_gis")
        self.assertFalse(result.patterns[0]["is_approximate"])
        self.assertEqual(router.calls, 0)

    def test_translink_service_normalization_and_spatial_candidate_selection(self) -> None:
        self.assertEqual(line_diva_service_key("19007B"), "7B")
        self.assertEqual(line_diva_service_key("220G1"), "G1")
        self.assertEqual(normalize_service_key(" 007-b "), "7B")
        near = _RouteGraph("19007B")
        near.add_edge("a", "b", LineString([(-6.1, 54.5), (-6.0, 54.6)]))
        far = _RouteGraph("99007B")
        far.add_edge("x", "y", LineString([(-8.1, 54.5), (-8.0, 54.6)]))
        sources = TranslinkGeometrySources(
            bus_graphs={"ulsterbus": {"7B": [far, near]}},
            rail_graph=None,
            warnings=[],
        )
        match = sources.match_bus_pattern(
            agency_id="51-49-ULB",
            route_short_name="7b",
            stops=[Point(-6.1, 54.5), Point(-6.0, 54.6)],
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.source, "translink_route_gis")
        self.assertEqual(list(match.geometry.coords)[0], (-6.1, 54.5))

    def test_route_network_hash_includes_official_geometry_fingerprints(self) -> None:
        first = route_network_hash_for_state(
            "reality", translink_source_state=self._source_state("a")
        )
        second = route_network_hash_for_state(
            "reality", translink_source_state=self._source_state("b")
        )
        self.assertNotEqual(first, second)

    def test_missing_official_geometry_sources_are_non_fatal(self) -> None:
        with TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            state = TranslinkGeometrySourceState(
                ulsterbus_path=root / "missing-ulster.zip",
                metro_path=root / "missing-metro.zip",
                rail_path=root / "missing-rail.geojson",
                ulsterbus_fingerprint="missing",
                metro_fingerprint="missing",
                rail_fingerprint="missing",
            )
            sources = load_translink_geometry_sources(state)
        self.assertEqual(sources.bus_graphs, {"ulsterbus": {}, "metro": {}})
        self.assertIsNone(sources.rail_graph)
        self.assertEqual(len(sources.warnings), 3)

    def test_workflow_reuses_matching_route_network_hash(self) -> None:
        reality_state = self._reality_state()
        with (
            mock.patch.object(transit_workflow, "has_complete_transit_route_network_manifest", return_value=True) as complete,
            mock.patch.object(transit_workflow, "build_route_network_materialization") as build,
            mock.patch.object(transit_workflow, "replace_transit_route_network") as replace,
        ):
            transit_workflow._ensure_route_network(mock.sentinel.engine, reality_state=reality_state)
        complete.assert_called_once()
        build.assert_not_called()
        replace.assert_not_called()

    def test_route_keys_and_colors_are_feed_scoped_and_stable(self) -> None:
        self.assertEqual(route_key("feed-a", "10"), "feed-a:10")
        self.assertNotEqual(route_key("feed-a", "10"), route_key("feed-b", "10"))
        first = route_colors("feed-a:10", None, None)
        self.assertEqual(first, route_colors("feed-a:10", "invalid", None))
        self.assertEqual(route_colors("feed-a:10", "FF00AA", "000000"), ("#ff00aa", "#000000"))

    def test_supported_route_modes_and_visibility_thresholds(self) -> None:
        self.assertEqual(display_route_mode(0), "tram")
        self.assertEqual(display_route_mode(2), "rail")
        self.assertEqual(display_route_mode(3), "bus")
        self.assertEqual(_visibility_class("rail", 1000, 0), ("backbone", BACKBONE_MIN_ZOOM))
        self.assertEqual(_visibility_class("bus", 40_000, 0), ("backbone", BACKBONE_MIN_ZOOM))
        self.assertEqual(_visibility_class("bus", 15_000, 0), ("regional", REGIONAL_MIN_ZOOM))
        self.assertEqual(_visibility_class("bus", 14_999, 0), ("local", LOCAL_MIN_ZOOM))
        self.assertEqual(_visibility_class("bus", 1000, 2), ("backbone", BACKBONE_MIN_ZOOM))

    def test_transfer_hubs_use_parent_then_nearby_normalized_names(self) -> None:
        stops = [
            _stop("feed:bus", "b1", -6.2600, 53.3500, name="Central Station", parent="P"),
            _stop("feed:rail", "r1", -6.2601, 53.3501, name="Central", parent="P"),
            _stop("feed:tram", "t1", -6.2700, 53.3400, name="Main-Street"),
            _stop("feed:bus2", "b2", -6.2704, 53.3400, name="Main Street"),
        ]
        hubs = _build_transfer_hubs(
            stops,
            {key: "local" for key in {stop.route_key for stop in stops}},
            {"feed:bus": "bus", "feed:rail": "rail", "feed:tram": "tram", "feed:bus2": "bus"},
        )
        self.assertEqual(len(hubs), 2)
        self.assertEqual(sorted(hub["route_count"] for hub in hubs), [2, 2])

    def test_bus_connections_count_distinct_rail_nodes_within_250m(self) -> None:
        pattern = RoutePattern(
            "feed:bus", "p", "feed", "bus", None, None, "bus", "#000000", "#ffffff",
            0, None, None, None, "gtfs_shape", False, 1000, 0, "local", 10, ("b1", "b2"),
            LineString([(-6.26, 53.35), (-6.25, 53.36)]),
        )
        bus_stops = [
            _stop("feed:bus", "b1", -6.2600, 53.3500, name="A"),
            _stop("feed:bus", "b2", -6.2500, 53.3600, name="B"),
        ]
        rail_stops = [
            _stop("feed:rail", "r1", -6.2602, 53.3500, name="R1", parent="station-1"),
            _stop("feed:rail2", "r2", -6.2502, 53.3600, name="R2", parent="station-2"),
        ]
        self.assertEqual(_station_connection_count(pattern, bus_stops, rail_stops), 2)

    def test_route_assets_use_hashed_names_and_include_endpoints(self) -> None:
        with TemporaryDirectory() as tmp_name, mock.patch(
            "transit.route_assets.ROUTE_ASSETS_DIR", Path(tmp_name)
        ):
            patterns = [{
                "route_key": "feed:10/unsafe", "pattern_id": "p", "feed_id": "feed", "route_id": "10/unsafe",
                "route_short_name": "10", "route_long_name": "Town", "mode": "bus", "route_color": "#123456",
                "route_text_color": "#ffffff", "direction_id": 0, "headsign": "Town", "origin_name": "A",
                "destination_name": "B", "geometry_source": "stop_sequence_approximate", "is_approximate": True,
                "visibility_class": "regional", "geom": LineString([(0, 0), (1, 1)]),
            }]
            stops = [
                {"route_key": "feed:10/unsafe", "pattern_id": "p", "feed_id": "feed", "stop_id": "a", "stop_sequence": 0, "stop_name": "A", "geom": Point(0, 0)},
                {"route_key": "feed:10/unsafe", "pattern_id": "p", "feed_id": "feed", "stop_id": "b", "stop_sequence": 1, "stop_name": "B", "geom": Point(1, 1)},
            ]
            index = publish_route_assets(
                build_key="build", reality_fingerprint="reality", patterns=patterns, stops=stops, transfer_hubs=[]
            )
            self.assertEqual(index["counts"]["total"], 1)
            asset_name = Path(index["routes"][0]["asset_url"]).name
            self.assertRegex(asset_name, r"^[0-9a-f]{24}\.geojson$")
            payload = json.loads((Path(tmp_name) / "build" / asset_name).read_text(encoding="utf-8"))
            endpoint_flags = [
                feature["properties"].get("is_endpoint")
                for feature in payload["features"]
                if feature["properties"]["feature_kind"] == "stop"
            ]
            self.assertEqual(endpoint_flags, [True, True])

