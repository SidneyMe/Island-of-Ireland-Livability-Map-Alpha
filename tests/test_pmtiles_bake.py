from __future__ import annotations

import importlib
from unittest import TestCase


bake_pmtiles = importlib.import_module("precompute.bake_pmtiles")


class PmtilesBakeContractTests(TestCase):
    def test_grid_tile_sql_exports_popup_score_and_count_fields(self) -> None:
        sql = str(bake_pmtiles._GRID_TILE_SQL)

        for category in bake_pmtiles.GRID_AMENITY_CATEGORIES:
            with self.subTest(category=category):
                self.assertIn(
                    f"COALESCE((g.counts_json ->> '{category}')::integer, 0) "
                    f"AS count_{category}",
                    sql,
                )
                self.assertIn(
                    f"COALESCE((g.scores_json ->> '{category}')::double precision, 0.0) "
                    f"AS score_{category}",
                    sql,
                )

    def test_grid_layer_metadata_declares_popup_score_and_count_fields(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            amenity_min_zoom=9,
        )
        grid_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "grid"
        )

        for category in bake_pmtiles.GRID_AMENITY_CATEGORIES:
            with self.subTest(category=category):
                self.assertEqual(grid_layer["fields"][f"count_{category}"], "Number")
                self.assertEqual(grid_layer["fields"][f"score_{category}"], "Number")
