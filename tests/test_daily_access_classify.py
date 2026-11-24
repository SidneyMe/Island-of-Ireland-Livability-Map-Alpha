from __future__ import annotations

import os
import unittest
import uuid
from unittest import TestCase

from shapely.geometry import Point, box

from config import DAILY_ACCESS_SERVICE_KINDS, TO_TARGET
from db_postgis.daily_access_green import classify_green_source_rows, resolve_green_sites
from precompute.daily_access_classify import (
    DailyAccessClass,
    Exclusion,
    GreenClass,
    build_logical_places,
    classify_medical,
    classify_service_rows,
    classify_shop,
    classify_green,
    green_area_class,
    is_primary_centre_name,
    object_connector_distance_m,
)


def _shop(tags, **extra):
    return {"category": "shops", "source": "osm_local_pbf", "tags_json": tags, **extra}


def _med(tags, name=None):
    return {"category": "healthcare", "source": "osm_local_pbf", "tags_json": tags, "name": name}


def _overture(category, raw, name=None, brand=None):
    return {
        "category": category,
        "source": "overture_places",
        "raw_primary_category": raw,
        "name": name,
        "brand": brand,
    }


class ServiceKindInvariantTests(TestCase):
    def test_quality_is_constant_per_service_kind(self) -> None:
        # One (need, quality) pair per kind is the nearest-candidate invariant.
        for kind, value in DAILY_ACCESS_SERVICE_KINDS.items():
            need, quality = value
            self.assertTrue(0.0 < quality <= 1.0, kind)
            self.assertIn(need, {"food", "other_retail", "primary_care", "pharmacy", "green"})


class ShopClassificationTests(TestCase):
    def _kind(self, row) -> str:
        verdict = classify_shop(row)
        self.assertIsInstance(verdict, DailyAccessClass)
        return verdict.service_kind

    def test_explicit_supermarket_beats_spar_centra_brand(self) -> None:
        self.assertEqual(self._kind(_shop({"shop": "supermarket", "brand": "Spar"})), "food_full")
        self.assertEqual(self._kind(_shop({"shop": "supermarket", "name": "Centra"})), "food_full")

    def test_brand_heuristic_only_for_ambiguous_tags(self) -> None:
        self.assertEqual(self._kind(_shop({"shop": "yes", "brand": "Centra"})), "food_convenience")
        self.assertEqual(self._kind(_shop({"shop": "general", "brand": "Tesco"})), "food_full")
        self.assertEqual(self._kind(_shop({"shop": "convenience", "brand": "Tesco"})), "food_convenience")
        self.assertEqual(self._kind(_shop({"shop": "clothes", "brand": "Dunnes"})), "other_retail")

    def test_kiosk_without_food_is_other_retail(self) -> None:
        self.assertEqual(self._kind(_shop({"shop": "kiosk"})), "other_retail")
        self.assertEqual(self._kind(_shop({"shop": "kiosk", "brand": "Spar"})), "food_convenience")

    def test_specialty_food_values(self) -> None:
        for value in ("bakery", "butcher", "greengrocer", "fishmonger", "health_food", "food"):
            verdict = classify_shop(_shop({"shop": value}))
            self.assertEqual(verdict.service_kind, "food_specialty", value)
            self.assertAlmostEqual(verdict.quality, 0.35)

    def test_excluded_shop_values(self) -> None:
        for value in ("vacant", "car_repair", "motorcycle", "boat", "funeral_directors"):
            self.assertEqual(classify_shop(_shop({"shop": value})), Exclusion("excluded_shop_value"))

    def test_other_retail_quality(self) -> None:
        verdict = classify_shop(_shop({"shop": "hardware"}))
        self.assertEqual((verdict.need, verdict.quality), ("other_retail", 1.0))

    def test_overture_shops(self) -> None:
        self.assertEqual(self._kind(_overture("shops", "supermarket")), "food_full")
        self.assertEqual(self._kind(_overture("shops", "convenience_store")), "food_convenience")
        self.assertEqual(self._kind(_overture("shops", "retail", brand="Lidl")), "food_full")
        self.assertEqual(self._kind(_overture("shops", "clothing_store", brand="Lidl")), "other_retail")
        self.assertEqual(self._kind(_overture("shops", "bakery")), "food_specialty")


class MedicalClassificationTests(TestCase):
    def test_gp_pharmacy_and_clinic(self) -> None:
        self.assertEqual(classify_medical(_med({"amenity": "doctors"})).service_kind, "primary_care_gp")
        self.assertEqual(classify_medical(_med({"healthcare": "doctor"})).quality, 1.0)
        self.assertEqual(classify_medical(_med({"healthcare": "pharmacy"})).service_kind, "pharmacy")
        clinic = classify_medical(_med({"amenity": "clinic"}))
        self.assertEqual((clinic.service_kind, clinic.quality), ("primary_care_clinic", 0.6))

    def test_hospital_dentist_and_narrow_speciality_are_not_primary_care(self) -> None:
        self.assertEqual(classify_medical(_med({"amenity": "hospital"})), Exclusion("hospital"))
        self.assertEqual(classify_medical(_med({"healthcare": "dentist"})), Exclusion("dentistry"))
        self.assertEqual(
            classify_medical(_med({"amenity": "doctors", "healthcare:speciality": "dermatology"})),
            Exclusion("specialised_care"),
        )
        self.assertEqual(
            classify_medical(_med({"healthcare": "physiotherapist"})),
            Exclusion("not_primary_care"),
        )

    def test_general_and_broad_specialities_are_not_narrow(self) -> None:
        for value in ("general", "family_medicine", "gp", "community", "public_health", "general;paediatrics"):
            verdict = classify_medical(
                _med({"amenity": "doctors", "healthcare:speciality": value})
            )
            self.assertEqual(verdict.service_kind, "primary_care_gp", value)

    def test_centre_without_speciality_uses_primary_name(self) -> None:
        for name in (
            "Ballymun Primary Care Centre",
            "Primary Care Center",
            "Primary Health Care Centre",
            "Carlow Health Centre",
            "HEALTH CENTER",
        ):
            verdict = classify_medical(_med({"healthcare": "centre"}, name=name))
            self.assertEqual((verdict.service_kind, verdict.quality), ("primary_care_clinic", 0.6), name)
        self.assertEqual(
            classify_medical(_med({"healthcare": "centre"}, name="Community Resource Centre")),
            Exclusion("centre_without_primary_name"),
        )

    def test_specialised_centre_names_are_excluded(self) -> None:
        for name in (
            "Mental Health Centre",
            "Dental Health Centre",
            "Sexual Health Centre",
            "Addiction Health Centre",
            "Physiotherapy Health Centre",
            "Rehabilitation Health Centre",
        ):
            self.assertFalse(is_primary_centre_name(name), name)

    def test_explicit_speciality_beats_centre_name(self) -> None:
        self.assertEqual(
            classify_medical(
                _med({"healthcare": "centre", "healthcare:speciality": "psychiatry"}, name="Health Centre")
            ),
            Exclusion("specialised_care"),
        )
        verdict = classify_medical(
            _med({"healthcare": "centre", "healthcare:speciality": "general"}, name="Riverside")
        )
        self.assertEqual(verdict.service_kind, "primary_care_clinic")

    def test_overture_medical(self) -> None:
        self.assertEqual(classify_medical(_overture("healthcare", "doctor")).service_kind, "primary_care_gp")
        self.assertEqual(classify_medical(_overture("healthcare", "pharmacy")).service_kind, "pharmacy")
        self.assertEqual(
            classify_medical(_overture("healthcare", "health_center", name="Tallaght Health Centre")).service_kind,
            "primary_care_clinic",
        )
        self.assertEqual(classify_medical(_overture("healthcare", "hospital")), Exclusion("hospital"))


class GreenClassificationTests(TestCase):
    def _green(self, tags):
        return classify_green({"category": "parks", "source": "osm_local_pbf", "tags_json": tags})

    def test_semantic_types(self) -> None:
        self.assertEqual(self._green({"boundary": "national_park"}), GreenClass("national_park", "eligible"))
        self.assertEqual(self._green({"leisure": "park"}).semantic_type, "park")
        self.assertEqual(self._green({"landuse": "village_green"}).semantic_type, "village_green")
        self.assertEqual(self._green({"leisure": "common"}).semantic_type, "common")

    def test_garden_and_recreation_ground_need_explicit_public_access(self) -> None:
        self.assertEqual(self._green({"leisure": "garden"}), Exclusion("access_not_public"))
        self.assertEqual(self._green({"leisure": "garden", "access": "yes"}).semantic_type, "garden")
        self.assertEqual(
            self._green({"landuse": "recreation_ground", "access": "permissive"}).semantic_type,
            "recreation_ground",
        )
        self.assertEqual(
            self._green({"leisure": "garden", "garden:type": "residential", "access": "yes"}),
            Exclusion("residential_garden"),
        )

    def test_denied_access_and_foot_priority(self) -> None:
        for value in ("private", "no", "customers"):
            self.assertEqual(self._green({"leisure": "park", "access": value}), Exclusion("access_denied"))
        self.assertEqual(self._green({"leisure": "park", "foot": "no", "access": "yes"}), Exclusion("access_denied"))
        self.assertEqual(self._green({"leisure": "park", "access": "private", "foot": "yes"}).semantic_type, "park")
        self.assertEqual(
            self._green({"leisure": "garden", "access": "private", "foot": "permissive"}).semantic_type,
            "garden",
        )

    def test_area_classes(self) -> None:
        self.assertEqual(green_area_class("park", 4_999.0), ("pocket", 0.6))
        self.assertEqual(green_area_class("park", 5_000.0), ("neighbourhood", 0.85))
        self.assertEqual(green_area_class("park", 50_000.0), ("district", 1.0))
        self.assertEqual(green_area_class("park", 250_000.0), ("regional", 1.0))
        self.assertEqual(green_area_class("park", None), ("unknown_area", 0.6))
        self.assertEqual(green_area_class("playground", 900_000.0), ("playground", 0.4))
        self.assertEqual(green_area_class("national_park", 10.0), ("national_park", 1.0))

    def test_overture_recreation_ground_has_no_explicit_access(self) -> None:
        stats: dict = {}
        eligible = classify_green_source_rows(
            [
                {**_overture("parks", "recreation_ground"), "tags_json": None},
                {**_overture("parks", "playground"), "tags_json": None},
                {**_overture("parks", "parks_and_outdoors"), "tags_json": None},
            ],
            stats,
        )
        self.assertEqual([row["semantic_type"] for row in eligible], ["playground"])
        self.assertEqual(stats["excluded_rows"], {"access_not_public": 1, "not_green": 1})


class GreenNestingTests(TestCase):
    def _site(self, key, semantic_type, area, is_area=True):
        return {
            "site_key": key,
            "semantic_type": semantic_type,
            "is_area": is_area,
            "area_m2": area,
            "member_refs": (key,),
        }

    def test_nested_site_joins_higher_or_equal_quality_root_and_keeps_quality(self) -> None:
        sites = resolve_green_sites(
            [
                self._site("park:W/1", "park", 300_000.0),
                self._site("playground:W/2", "playground", 800.0),
                self._site("park:W/3", "park", 20_000.0),
            ],
            [("playground:W/2", "park:W/3"), ("playground:W/2", "park:W/1"), ("park:W/3", "park:W/1")],
        )
        by_key = {site.site_key: site for site in sites}
        self.assertEqual(by_key["playground:W/2"].root_site_key, "park:W/1")
        self.assertEqual(by_key["park:W/3"].root_site_key, "park:W/1")
        self.assertEqual(by_key["playground:W/2"].quality, 0.4)
        self.assertEqual(by_key["park:W/3"].service_kind, "green_neighbourhood")
        self.assertEqual(len({site.root_site_id for site in sites}), 1)

    def test_lower_quality_container_does_not_absorb(self) -> None:
        sites = resolve_green_sites(
            [
                self._site("playground:W/9", "playground", 90_000.0),
                self._site("park:W/4", "park", 6_000.0),
            ],
            [("park:W/4", "playground:W/9")],
        )
        by_key = {site.site_key: site for site in sites}
        self.assertEqual(by_key["park:W/4"].root_site_key, "park:W/4")
        self.assertNotEqual(by_key["park:W/4"].root_site_id, by_key["playground:W/9"].root_site_id)

    def test_identical_equal_quality_sites_resolve_by_stable_key(self) -> None:
        sites = resolve_green_sites(
            [self._site("park:W/8", "park", 6_000.0), self._site("garden:W/7", "garden", 6_000.0)],
            [("park:W/8", "garden:W/7"), ("garden:W/7", "park:W/8")],
        )
        roots = {site.site_key: site.root_site_key for site in sites}
        self.assertEqual(roots, {"garden:W/7": "garden:W/7", "park:W/8": "garden:W/7"})

    def test_point_green_is_approximate(self) -> None:
        (site,) = resolve_green_sites([self._site("park:N/5", "park", None, is_area=False)], [])
        self.assertTrue(site.approximate_point)
        self.assertEqual((site.area_class, site.quality, site.service_kind), ("unknown_area", 0.6, "green_pocket"))


class LogicalPlaceTests(TestCase):
    """v4 dedupe: classify each raw row first, merge only on evidence, no chains."""

    def _row(self, kind, ref, x, y, *, source="osm_local_pbf", name=None, reason=None, **extra):
        need = DAILY_ACCESS_SERVICE_KINDS[kind][0]
        return {
            "service_kind": kind, "need": need, "source": source, "source_ref": ref,
            "x": x, "y": y, "name": name, "class_reason": reason or "other_retail", **extra,
        }

    def _summary(self, places):
        return sorted((place.service_kind, tuple(sorted(m["source_ref"] for m in place.members))) for place in places)

    def test_two_different_shops_next_to_each_other_are_two_places(self) -> None:
        places = build_logical_places([
            self._row("other_retail", "N/1", 0.0, 0.0, name="Chemist Warehouse"),
            self._row("other_retail", "N/2", 6.0, 0.0, name="Book Nook"),
            self._row("other_retail", "N/3", 3.0, 0.0),  # no name: no evidence at all
        ])
        self.assertEqual(len(places), 3)

    def test_gp_and_pharmacy_at_one_address_never_merge(self) -> None:
        address = {"tags_json": {"addr:housenumber": "1", "addr:street": "Main Street"}}
        places = build_logical_places([
            self._row("primary_care_gp", "N/1", 0.0, 0.0, name="Main St Medical", reason="doctors", **address),
            self._row("pharmacy", "N/2", 0.0, 0.0, name="Main St Medical", reason="pharmacy", **address),
        ])
        self.assertEqual(self._summary(places), [("pharmacy", ("N/2",)), ("primary_care_gp", ("N/1",))])

    def test_osm_and_overture_copies_of_one_place_count_once(self) -> None:
        osm = self._row(
            "food_convenience", "N/1", 0.0, 0.0, name="SuperValu Ranelagh", reason="explicit_convenience",
            tags_json={"addr:housenumber": "12", "addr:street": "Main Street", "addr:postcode": "D06 X1Y2"},
        )
        overture = self._row(
            "food_full", "ov1", 14.0, 0.0, source="overture_places", name="Supervalu ranelagh",
            reason="overture_supermarket", address_freeform="12 Main Street", address_postcode="D06X1Y2",
        )
        stats: dict = {}
        (place,) = build_logical_places([overture, osm], stats_out=stats)
        # Different source subtypes of one need; the explicit OSM tag decides.
        self.assertEqual(place.service_kind, "food_convenience")
        self.assertEqual(place.place_id, "food_convenience|osm:N/1,overture:ov1")
        self.assertEqual(stats["duplicate_pairs_by_evidence"], {"name_and_address": 1})

    def test_name_and_footprint_and_confirmed_id(self) -> None:
        footprint = box(0.0, 0.0, 60.0, 40.0).wkb
        places = build_logical_places([
            self._row("other_retail", "W/1", 30.0, 20.0, name="Penneys", footprint_wkb=footprint),
            self._row("other_retail", "ov2", 55.0, 35.0, source="overture_places", name="PENNEYS", reason="overture_other_retail"),
            self._row("other_retail", "ov3", 200.0, 0.0, source="overture_places", name="Other", identity_refs=("osm:N/9",)),
            self._row("other_retail", "N/9", 210.0, 0.0, name="Different name"),
        ])
        self.assertEqual(
            self._summary(places),
            [("other_retail", ("N/9", "ov3")), ("other_retail", ("W/1", "ov2"))],
        )

    def test_proximity_chain_does_not_merge_three_rows(self) -> None:
        # B's footprint covers both points: A~B and B~C, but A and C (two
        # unaddressed points 18 m apart) have no evidence between them.
        b = self._row("other_retail", "W/1", 10.0, 0.0, name="Spar", footprint_wkb=box(-5.0, -5.0, 25.0, 5.0).wkb)
        a = self._row("other_retail", "N/1", 1.0, 0.0, name="Spar")
        c = self._row("other_retail", "N/2", 19.0, 0.0, name="Spar")
        places = build_logical_places([a, b, c])
        self.assertEqual(sorted(len(place.members) for place in places), [1, 2])
        for place in places:
            self.assertNotEqual({m["source_ref"] for m in place.members} & {"N/1", "N/2"}, {"N/1", "N/2"})
        # The old 25 m connected components would have chained all three.
        self.assertEqual(len(build_logical_places([dict(a, name=None), dict(c, name=None)])), 2)

    def test_place_ids_are_reproducible_for_unchanged_groups(self) -> None:
        rows = [self._row("pharmacy", f"N/{i}", float(i) * 100.0, 0.0, name=f"P{i}", reason="pharmacy") for i in range(5)]
        first = [place.place_id for place in build_logical_places(rows)]
        second = [place.place_id for place in build_logical_places(list(reversed(rows)))]
        self.assertEqual(first, second)

    def test_classify_service_rows_keeps_source_tags_after_merge(self) -> None:
        merged = [
            {"category": "shops", "source": "osm_local_pbf", "source_ref": "W/1", "lat": 53.0, "lon": -6.0},
            {"category": "healthcare", "source": "osm_local_pbf", "source_ref": "N/2", "lat": 53.0, "lon": -6.0},
            {"category": "parks", "source": "osm_local_pbf", "source_ref": "W/3", "lat": 53.0, "lon": -6.0},
        ]
        lookup = {
            ("osm_local_pbf", "W/1"): {"tags_json": {"shop": "supermarket"}, "footprint_wkb": b"x"},
            ("osm_local_pbf", "N/2"): {"tags_json": {"amenity": "dentist"}},
        }
        rows, stats = classify_service_rows(merged, source_row_lookup=lookup)
        self.assertEqual([(row["service_kind"], row["footprint_wkb"]) for row in rows], [("food_full", b"x")])
        self.assertEqual(stats["input_rows"], {"healthcare": 1, "shops": 1})
        self.assertEqual(stats["excluded_rows"], {"healthcare": {"dentistry": 1}})


class ConnectorGeometryTests(TestCase):
    def test_polygon_shop_connector_is_zero_inside_and_outline_distance_outside(self) -> None:
        footprint = box(0.0, 0.0, 40.0, 20.0)
        self.assertEqual(object_connector_distance_m(footprint, Point(10.0, 10.0)), 0.0)
        self.assertEqual(object_connector_distance_m(footprint, Point(50.0, 10.0)), 10.0)
        self.assertEqual(object_connector_distance_m(Point(0.0, 0.0), Point(3.0, 4.0)), 5.0)


@unittest.skipUnless(
    os.getenv("DAILY_ACCESS_DB_TESTS") == "1",
    "set DAILY_ACCESS_DB_TESTS=1 to run against the local PostGIS (rolled back)",
)
class GreenSitesPostgisTests(TestCase):
    def test_union_area_class_nesting_and_window(self) -> None:
        from db_postgis._dependencies import text
        from db_postgis.daily_access_green import stage_green_sites
        from db_postgis.engine import build_engine
        from config import OSM_IMPORT_SCHEMA, TO_WGS84

        fingerprint = f"daily-access-test-{uuid.uuid4().hex}"
        x0, y0 = TO_TARGET(-6.26, 53.35)

        def wkt(minx, miny, maxx, maxy):
            corners = [TO_WGS84(x0 + x, y0 + y) for x, y in ((minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny))]
            return "POLYGON((" + ",".join(f"{lon} {lat}" for lon, lat in corners) + "))"

        features = [
            # Two touching 100x100 m park halves -> one 20 000 m2 neighbourhood park.
            ("W", 1, {"leisure": "park"}, wkt(0, 0, 100, 100)),
            ("W", 2, {"leisure": "park"}, wkt(100, 0, 200, 100)),
            # Playground fully inside the merged park -> nested, keeps 0.4.
            ("W", 3, {"leisure": "playground"}, wkt(120, 20, 140, 40)),
            # Private garden -> excluded before union.
            ("W", 4, {"leisure": "garden", "access": "private"}, wkt(200, 0, 260, 100)),
            # Point park -> approximate.
            ("N", 5, {"leisure": "park"}, None),
            # Far away (>2 km) -> outside the window.
            ("W", 6, {"leisure": "park"}, wkt(10_000, 0, 10_100, 100)),
        ]
        engine = build_engine()
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                for osm_type, osm_id, tags, polygon in features:
                    geom_sql = (
                        "ST_GeomFromText(:wkt, 4326)"
                        if polygon
                        else "ST_SetSRID(ST_MakePoint(-6.255, 53.351), 4326)"
                    )
                    connection.execute(
                        text(
                            f"""
                            INSERT INTO {OSM_IMPORT_SCHEMA}.features
                                (osm_type, osm_id, import_fingerprint, category, name, tags_json, geom, created_at)
                            VALUES (:osm_type, :osm_id, :fp, 'parks', NULL, CAST(:tags AS jsonb), {geom_sql}, now())
                            """
                        ),
                        {
                            "osm_type": osm_type,
                            "osm_id": osm_id,
                            "fp": fingerprint,
                            "tags": __import__("json").dumps(tags),
                            "wkt": polygon,
                        },
                    )
                study_area = box(*TO_WGS84(x0 - 100, y0 - 100), *TO_WGS84(x0 + 300, y0 + 200))
                sites, stats = stage_green_sites(
                    connection,
                    import_fingerprint=fingerprint,
                    study_area_wgs84=study_area,
                )
            finally:
                transaction.rollback()

        by_type = {(site.semantic_type, site.is_area): site for site in sites}
        park = by_type[("park", True)]
        self.assertEqual(park.member_refs, ("W/1", "W/2"))
        self.assertAlmostEqual(park.area_m2, 20_000.0, delta=50.0)
        self.assertEqual(park.service_kind, "green_neighbourhood")
        playground = by_type[("playground", True)]
        self.assertEqual(playground.root_site_key, park.site_key)
        self.assertEqual(playground.quality, 0.4)
        self.assertTrue(by_type[("park", False)].approximate_point)
        self.assertEqual(stats["excluded_rows"], {"access_denied": 1})
        self.assertEqual(len(sites), 3)


if __name__ == "__main__":
    unittest.main()
