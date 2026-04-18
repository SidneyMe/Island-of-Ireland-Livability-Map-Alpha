from __future__ import annotations

import importlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock


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
            grid_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
        )
        grid_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "grid"
        )

        for category in bake_pmtiles.GRID_AMENITY_CATEGORIES:
            with self.subTest(category=category):
                self.assertEqual(grid_layer["fields"][f"count_{category}"], "Number")
                self.assertEqual(grid_layer["fields"][f"score_{category}"], "Number")

    def test_pmtiles_metadata_declares_transit_reality_and_service_desert_layers(self) -> None:
        metadata = bake_pmtiles._pmtiles_metadata(
            min_zoom=5,
            max_zoom=14,
            grid_max_zoom=11,
            amenity_min_zoom=9,
            transport_reality_min_zoom=9,
        )

        transport_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "transport_reality"
        )
        desert_layer = next(
            layer for layer in metadata["vector_layers"] if layer["id"] == "service_deserts"
        )

        self.assertEqual(transport_layer["fields"]["reality_status"], "String")
        self.assertEqual(transport_layer["fields"]["public_departures_30d"], "Number")
        self.assertEqual(transport_layer["fields"]["source_status"], "String")
        self.assertEqual(transport_layer["fields"]["school_only_departures_30d"], "Number")
        self.assertEqual(desert_layer["fields"]["baseline_reachable_stop_count"], "Number")
        self.assertEqual(desert_layer["fields"]["reachable_public_departures_7d"], "Number")

    def test_transport_reality_tile_sql_exports_gtfs_direct_fields(self) -> None:
        sql = str(bake_pmtiles._TRANSPORT_REALITY_TILE_SQL)

        self.assertIn("t.source_status", sql)
        self.assertIn("t.school_only_departures_30d", sql)

    def test_bake_pmtiles_bbox_scans_only_coarse_zooms_and_uses_amenity_tiles_above_that(self) -> None:
        class _FakeWriter:
            def __init__(self, handle) -> None:
                self.handle = handle

            def write_tile(self, tile_id, payload) -> None:
                del tile_id, payload

            def finalize(self, header, metadata) -> None:
                del header, metadata

        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            output_path = Path(tmp_name) / "livability.pmtiles"
            with (
                mock.patch.object(bake_pmtiles, "Writer", _FakeWriter),
                mock.patch.object(bake_pmtiles, "_load_amenity_points", return_value=[(-6.2, 53.4)]),
                mock.patch.object(bake_pmtiles, "_load_transport_reality_points", return_value=[(-6.1, 53.5)]),
                mock.patch.object(bake_pmtiles, "_tile_range_for_bbox", return_value=(0, 0, 0, 0)) as bbox_mock,
                mock.patch.object(bake_pmtiles, "_point_tile_coordinates", return_value=[(1, 2)]) as point_tiles_mock,
                mock.patch.object(bake_pmtiles, "_tile_mvt_bytes", return_value=b"mvt"),
            ):
                bake_pmtiles.bake_pmtiles(
                    _FakeEngine(),
                    "build-key-123",
                    output_path,
                    min_zoom=11,
                    max_zoom=12,
                    amenity_min_zoom=9,
                )

        self.assertEqual([call.args[0] for call in bbox_mock.call_args_list], [11])
        self.assertEqual([call.kwargs["zoom"] for call in point_tiles_mock.call_args_list], [12, 12])
