from __future__ import annotations

from unittest import TestCase, mock

import geopandas as gpd
from shapely.geometry import Point, box

import overture.loader as overture_loader


class OvertureLoaderTests(TestCase):
    def test_load_overture_amenity_rows_excludes_garden_and_keeps_real_parks(self) -> None:
        study_area = box(-6.3, 53.34, -6.2, 53.36)
        gdf = gpd.GeoDataFrame(
            {
                "id": ["garden-1", "park-1", "playground-1"],
                "geometry": [
                    Point(-6.2500, 53.3500),
                    Point(-6.2490, 53.3505),
                    Point(-6.2480, 53.3507),
                ],
                "categories": [
                    {"primary": "garden"},
                    {"primary": "park"},
                    {"primary": "playground"},
                ],
            },
            geometry="geometry",
            crs="EPSG:4326",
        )

        with (
            mock.patch.object(overture_loader, "is_available", return_value=True),
            mock.patch("geopandas.read_parquet", return_value=gdf),
        ):
            rows = overture_loader.load_overture_amenity_rows(study_area)

        refs = [row["source_ref"] for row in rows]
        self.assertNotIn("garden-1", refs)
        self.assertIn("park-1", refs)
        self.assertIn("playground-1", refs)
        self.assertTrue(all(row["category"] == "parks" for row in rows))
