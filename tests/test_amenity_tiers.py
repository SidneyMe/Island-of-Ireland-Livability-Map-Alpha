from __future__ import annotations

from unittest import TestCase

from precompute.amenity_tiers import classify_amenity_row


class AmenityTierClassificationTests(TestCase):
    def test_osm_shop_tiers_respect_chain_and_footprint_rules(self) -> None:
        cases = [
            (
                {
                    "category": "shops",
                    "source": "osm_local_pbf",
                    "tags_json": {"shop": "convenience"},
                    "footprint_area_m2": 120.0,
                },
                ("corner", 1),
            ),
            (
                {
                    "category": "shops",
                    "source": "osm_local_pbf",
                    "name": "Tesco Express",
                    "tags_json": {"shop": "supermarket", "brand": "Tesco"},
                    "footprint_area_m2": 1_499.0,
                },
                ("regular", 2),
            ),
            (
                {
                    "category": "shops",
                    "source": "osm_local_pbf",
                    "name": "Lidl",
                    "tags_json": {"shop": "supermarket", "brand": "Lidl"},
                    "footprint_area_m2": 1_500.0,
                },
                ("supermarket", 3),
            ),
            (
                {
                    "category": "shops",
                    "source": "osm_local_pbf",
                    "tags_json": {"shop": "clothes"},
                    "footprint_area_m2": 8_000.0,
                },
                ("mall", 5),
            ),
        ]

        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(classify_amenity_row(row), expected)

    def test_osm_healthcare_tiers_respect_emergency_and_clinic_rules(self) -> None:
        cases = [
            (
                {
                    "category": "healthcare",
                    "source": "osm_local_pbf",
                    "tags_json": {"amenity": "pharmacy"},
                },
                ("local", 1),
            ),
            (
                {
                    "category": "healthcare",
                    "source": "osm_local_pbf",
                    "tags_json": {"amenity": "clinic"},
                },
                ("clinic", 2),
            ),
            (
                {
                    "category": "healthcare",
                    "source": "osm_local_pbf",
                    "tags_json": {"amenity": "hospital"},
                },
                ("hospital", 3),
            ),
            (
                {
                    "category": "healthcare",
                    "source": "osm_local_pbf",
                    "tags_json": {"amenity": "hospital", "emergency": "yes"},
                },
                ("emergency_hospital", 4),
            ),
        ]

        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(classify_amenity_row(row), expected)

    def test_osm_park_tiers_respect_area_boundaries(self) -> None:
        cases = [
            (
                {
                    "category": "parks",
                    "source": "osm_local_pbf",
                    "park_area_m2": 4_999.0,
                },
                ("pocket", 1),
            ),
            (
                {
                    "category": "parks",
                    "source": "osm_local_pbf",
                    "park_area_m2": 5_000.0,
                },
                ("neighbourhood", 2),
            ),
            (
                {
                    "category": "parks",
                    "source": "osm_local_pbf",
                    "park_area_m2": 50_000.0,
                },
                ("district", 3),
            ),
            (
                {
                    "category": "parks",
                    "source": "osm_local_pbf",
                    "park_area_m2": 250_000.0,
                },
                ("regional", 4),
            ),
        ]

        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(classify_amenity_row(row), expected)

    def test_overture_fallbacks_stay_conservative(self) -> None:
        cases = [
            (
                {
                    "category": "shops",
                    "source": "overture_places",
                    "raw_primary_category": "convenience_store",
                },
                ("corner", 1),
            ),
            (
                {
                    "category": "shops",
                    "source": "overture_places",
                    "brand": "Tesco",
                    "raw_primary_category": "retail",
                },
                ("supermarket", 3),
            ),
            (
                {
                    "category": "shops",
                    "source": "overture_places",
                    "raw_primary_category": "retail",
                },
                ("regular", 2),
            ),
            (
                {
                    "category": "healthcare",
                    "source": "overture_places",
                    "raw_primary_category": "medical_clinic",
                },
                ("clinic", 2),
            ),
            (
                {
                    "category": "healthcare",
                    "source": "overture_places",
                    "raw_primary_category": "hospital",
                },
                ("hospital", 3),
            ),
            (
                {
                    "category": "parks",
                    "source": "overture_places",
                    "raw_primary_category": "playground",
                },
                ("pocket", 1),
            ),
            (
                {
                    "category": "parks",
                    "source": "overture_places",
                    "raw_primary_category": "park",
                },
                ("neighbourhood", 2),
            ),
            (
                {
                    "category": "parks",
                    "source": "overture_places",
                    "raw_primary_category": "nature_reserve",
                },
                ("regional", 4),
            ),
        ]

        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(classify_amenity_row(row), expected)

    def test_transport_tier_uses_gtfs_frequency_score_units(self) -> None:
        self.assertEqual(
            classify_amenity_row({"category": "transport", "transport_score_units": 4}),
            ("stop", 4),
        )
        self.assertEqual(
            classify_amenity_row({"category": "transport", "transport_score_units": 0}),
            ("stop", 0),
        )
        self.assertEqual(classify_amenity_row({"category": "transport"}), ("stop", 1))
