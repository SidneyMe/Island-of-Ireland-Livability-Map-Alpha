from __future__ import annotations

import tempfile
from pathlib import Path
from unittest import TestCase, mock

import numpy as np
import shapely
from shapely.geometry import Point, box

from precompute.daily_access_context import ContextField, effects, station_bonus
from precompute.daily_access_reference import TransportStop, compute_reference, load_reference_graph
from precompute.daily_access_native import compare_with_reference, compute_native
from precompute.daily_access_snap import LandMask
from tests.test_daily_access_native import X0, Y0, _native_available, write_walkgraph


def landuse(kind, geom):
    return {"landuse_class": kind, "geom": geom}


def venue(ref, geom, **tags):
    return {"source": "osm_local_pbf", "source_ref": ref, "name": tags.pop("name", ""),
            "tags_json": tags, "geom": geom}


class ContextTests(TestCase):
    def test_context_records_prepare_land_and_report_valid_cell_progress(self):
        from precompute.daily_access_native import CELL_DTYPE

        field = ContextField(land=box(-1000, -1000, 1000, 1000))
        self.assertTrue(shapely.is_prepared(field.land))
        field.corridors = mock.Mock()
        field.corridors.distances.return_value = np.zeros(3, dtype=np.float32)
        records = np.zeros(3, dtype=CELL_DTYPE)
        records["x"] = [0, 5000, 25]
        records["y"] = [0, 5000, 25]
        records["flags"] = [1, 0, 1]
        messages = []

        result = field.records(records, progress=messages.append)

        self.assertEqual(messages, ["[daily-access context] 0/2 valid cells",
                                    "[daily-access context] 2/2 valid cells"])
        self.assertEqual(result["context_values"].shape, (3, 5))

    def test_candidate_report_has_fixture_before_after_and_bounded_histogram(self):
        import json
        from config import TO_WGS84
        from scripts.daily_access_context_report import report
        from tests.test_daily_access_runtime import write_tiers, X0 as SX, Y0 as SY
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shell, score = write_tiers(root)
            lon, lat = TO_WGS84(SX + 25, SY + 25)
            fixture = root / "fixture.json"
            fixture.write_text(json.dumps({"locations": [{"name": "Control", "lat": lat, "lon": lon}]}), encoding="utf-8")
            result = report(shell, score, fixture)
            self.assertEqual(result["fixture"][0]["before_context"], 58.5)
            self.assertEqual(result["fixture"][0]["after"], 60)
            self.assertEqual(result["fixture"][0]["delta"], 1.5)
            self.assertGreater(result["valid_cells"], 0)

    def test_station_modes_only_include_active_public_services(self):
        from datetime import date
        from db_postgis.reads import load_public_station_modes
        engine = mock.MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        def rows(values):
            result = mock.MagicMock()
            result.mappings.return_value.all.return_value = values
            return result
        manifest = mock.MagicMock()
        manifest.mappings.return_value.one.return_value = {"analysis_date": date(2026, 11, 27), "feed_fingerprints_json": {"nta": "feed"}}
        calendars = []
        for service, end in (("public", date(2026, 12, 31)), ("school", date(2026, 12, 31)), ("expired", date(2026, 1, 1))):
            calendars.append(dict(feed_id="nta", service_id=service, monday=1, tuesday=1, wednesday=1, thursday=1,
                                  friday=1, saturday=1, sunday=1, start_date=date(2026, 1, 1), end_date=end))
        classes = [{"feed_id": "nta", "service_id": service, "school_only_state": state} for service, state in (("public", "no"), ("school", "yes"), ("expired", "no"))]
        connection.execute.side_effect = [manifest, rows(classes), rows(calendars), rows([]), rows([{"stop_id": "S1", "route_type": 1}, {"stop_id": "S2", "route_type": 0}])]
        self.assertEqual(load_public_station_modes(engine, "reality"), {"gtfs/nta/S1": ("rail",), "gtfs/nta/S2": ("tram",)})
        params = connection.execute.call_args.args[0].compile().params
        self.assertIn(["public"], list(params.values()))
        self.assertNotIn(["school"], list(params.values()))

    def test_context_threshold_changes_reuse_graph_shell_and_reach(self):
        from precompute.daily_access_build import tier_keys
        from config import DAILY_ACCESS_CONTEXT_PARAMS
        kwargs = dict(extract_fingerprint="extract", graph_bbox=(0, 0, 1, 1), graph_padding_m=2250,
                      geo_hash="geometry", mask_identity="mask", reach_inputs="sources")
        before = tier_keys(**kwargs)
        with mock.patch.dict(DAILY_ACCESS_CONTEXT_PARAMS, industrial=[.1, .5, 7.0]):
            after = tier_keys(**kwargs)
        self.assertEqual((before.graph, before.shell, before.reach), (after.graph, after.shell, after.reach))
        self.assertNotEqual(before.score, after.score)

    def test_station_curve_and_corridor_damping(self):
        for distance, expected in ((0, 0), (100, 0), (200, 1.5), (300, 3), (400, 3), (500, 3), (1200, 0)):
            self.assertAlmostEqual(station_bonus("rail", distance, 1.0, 200), expected)
        self.assertEqual(station_bonus("rail", 400, 1.0, 50), 0)
        self.assertAlmostEqual(station_bonus("rail", 400, .5, 125), .75)
        self.assertEqual(station_bonus("tram", 400, 1.0, 200), 2)
        self.assertEqual(station_bonus("tram", 1000, 1.0, 200), 0)

    def test_penalty_cap_and_green_bonus(self):
        penalties, bonuses = effects({"residential_share": 1, "industrial_share": 1, "confidence": 1,
                                      "nightlife_density": 100, "retail_density": 1000, "green_area_share": .5},
                                     pitch=10, walk_green=1, station=3)
        self.assertAlmostEqual(sum(penalties.values()), 20)
        self.assertEqual(bonuses, {"rail_station_access": 3, "green_buffer": 2})
        neutral, bonus = effects({})
        self.assertEqual(sum(neutral.values()) + sum(bonus.values()), 0)

    def test_overlap_precedence_and_unknown_coverage(self):
        land = box(-1000, -1000, 1000, 1000)
        field = ContextField(land=land, landuse=[landuse("residential", land), landuse("industrial", land)])
        metrics, _, _ = field.at(0, 0)
        self.assertAlmostEqual(metrics["landuse_coverage"], 1)
        self.assertEqual(metrics["residential_share"], 0)
        self.assertAlmostEqual(metrics["industrial_share"], 1)
        penalties, _ = effects(metrics)
        self.assertAlmostEqual(penalties["industrial_context"], 8)
        self.assertEqual(ContextField(land=land).at(0, 0)[0]["confidence"], 0)

    def test_coast_denominator_and_public_green_union(self):
        land = box(0, -1000, 1000, 1000)
        field = ContextField(land=land, landuse=[landuse("residential", land)], green=[{"geom": land}, {"geom": land}])
        metric, _, _ = field.at(0, 0)
        self.assertAlmostEqual(metric["residential_share"], 1)
        self.assertAlmostEqual(metric["green_area_share"], 1)
        self.assertAlmostEqual(effects(metric, walk_green=.5)[1]["green_buffer"], 1)

    def test_single_pub_and_ordinary_retail_no_penalty(self):
        land = box(-1000, -1000, 1000, 1000)
        field = ContextField(land=land, nuisance=[venue("n/1", Point(0, 0), amenity="pub"), venue("n/2", Point(5, 0), shop="convenience")])
        metric, _, _ = field.at(0, 0)
        self.assertEqual(effects(metric)[0]["over_concentration"], 0)

    def test_dedupe_lifecycle_and_distinct_venues(self):
        land = box(-1000, -1000, 1000, 1000)
        rows = [venue("w/1", box(-5, -5, 5, 5), amenity="pub", name="Same"),
                venue("n/1", Point(0, 0), amenity="pub", name="Same"),
                venue("n/2", Point(1, 1), amenity="pub", name="Different"),
                venue("n/3", Point(2, 2), amenity="nightclub", disused="yes"),
                venue("n/4", Point(3, 3), shop="vacant")]
        field = ContextField(land=land, nuisance=rows)
        self.assertEqual(len(field.venues.rows), 2)

    def test_pitch_lit_dense_unknown_and_strongest_only(self):
        land = box(-1000, -1000, 1000, 1000)
        lit = venue("w/1", box(-10, -10, 10, 10), leisure="pitch", lit="yes")
        unlit = venue("w/2", box(-10, -10, 10, 10), leisure="pitch", lit="no")
        field = ContextField(land=land, nuisance=[lit, lit])
        self.assertEqual(field.at(0, 0)[1], 4)
        self.assertEqual(field.at(35, 0)[1], 4)
        self.assertAlmostEqual(field.at(160, 0)[1], 0)
        self.assertEqual(ContextField(land=land, nuisance=[unlit]).at(0, 0)[1], 0)
        dense = ContextField(land=land, landuse=[landuse("residential", land)], nuisance=[unlit])
        self.assertEqual(dense.at(0, 0)[1], 2)
        self.assertEqual(dense.at(0, 0)[2]["reason"], "dense_residential")

    def test_mixed_residential_softens_retail_and_amplifies_nightlife(self):
        common = {"confidence": 1, "nightlife_density": 40, "retail_density": 0}
        self.assertGreater(effects({**common, "residential_share": 1})[0]["over_concentration"],
                           effects({**common, "residential_share": 0})[0]["over_concentration"])
        common.update(nightlife_density=0, retail_density=300)
        self.assertEqual(effects({**common, "residential_share": 1})[0]["over_concentration"], 0)


class ContextNativeTests(TestCase):
    def test_native_reference_nearest_station_and_missing_corridor(self):
        if not _native_available():
            self.skipTest("walkgraph executable unavailable")
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "graph"
            write_walkgraph(path, [(0, 0), (1600, 0)], [(0, 1)])
            graph = load_reference_graph(path)
            land = LandMask(box(X0 - 500, Y0 - 500, X0 + 2000, Y0 + 1000))
            field = ContextField(land=land.geometry, landuse=[landuse("residential", land.geometry)],
                                 nuisance=[venue("n/1", Point(X0, Y0), leisure="pitch", lit="yes")],
                                 green=[{"geom": land.geometry}])
            field.corridors = mock.Mock()
            field.corridors.distances.side_effect = lambda x, y: np.full(len(x), 300.0)
            field.corridor_feeds = {"nta"}
            for near, available in ((False, True), (True, True), (False, False)):
                field.corridor_feeds = {"nta"} if available else set()
                stops = [TransportStop("gtfs/nta/far", "nta", 5, X0 + 425, Y0, ("rail",))]
                if near:
                    stops.append(TransportStop("gtfs/nta/near", "nta", 5, X0 + 75, Y0, ("rail",)))
                kwargs = dict(land_mask=land, cells=[(X0, Y0), (X0 + 800, Y0)], cell_size_m=50,
                              stops=stops, context_field=field)
                reference = compute_reference(graph=graph, **kwargs)
                native = compute_native(graph_dir=path, **kwargs)
                report = compare_with_reference(reference, native)
                self.assertTrue(report["within_limits"], report)
                self.assertAlmostEqual(native.bonuses[0, 0] / 100, 0 if near or not available else 3)
