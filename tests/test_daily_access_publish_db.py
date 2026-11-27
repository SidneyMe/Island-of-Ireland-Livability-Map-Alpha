"""Opt-in publication test in a newly created, disposable PostGIS database.

DAILY_ACCESS_PUBLISH_DB_TESTS=1 requires CREATEDB on the configured local server.
The configured database is never modified; only the uniquely named test DB is.
"""
from __future__ import annotations

import dataclasses
import gzip
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
from geoalchemy2.shape import from_shape
from pmtiles.reader import Reader, all_tiles
from shapely.geometry import MultiPolygon, Point, box
from shapely.ops import transform
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, create_engine, func, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB

from config import BuildHashes, SourceState, TO_WGS84
from db_postgis import build_engine, ensure_database_ready, load_latest_scoring_build_manifest
from db_postgis import tables
from db_postgis.write_common import _bulk_insert
from precompute import daily_access_build as build
from precompute import daily_access_publish as publish
from precompute.bake_pmtiles import bake_pmtiles
from precompute.display_layers import prepare_display_layers
from precompute.daily_access_snap import LandMask
from tests.test_daily_access_display import NOW, stop
from tests.test_daily_access_native import X0, Y0, write_walkgraph
from tests.test_daily_access_runtime import SHARD, write_empty_reach_tier, write_tiers
from tests.test_fine_vector_pmtiles_worker import _decode_vector_tile


@unittest.skipUnless(os.getenv("DAILY_ACCESS_PUBLISH_DB_TESTS") == "1", "set DAILY_ACCESS_PUBLISH_DB_TESTS=1 (creates a disposable database)")
class StandalonePublicationDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = build_engine()
        cls.database_name = "livability_v4_test_" + uuid.uuid4().hex[:12]
        cls.admin = create_engine(source.url, isolation_level="AUTOCOMMIT")
        cls.database_url = source.url.set(database=cls.database_name)
        source.dispose()
        with cls.admin.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{cls.database_name}"'))
        cls.engine = create_engine(cls.database_url)
        cls.addClassCleanup(cls.cleanup_database)
        cls.environment = mock.patch.dict(os.environ, {"DATABASE_URL": cls.database_url.render_as_string(hide_password=False)})
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        ensure_database_ready(cls.engine)
        # Importer-owned tables are deliberately outside Alembic.
        tables.features.create(cls.engine, checkfirst=True)
        tables.roads.create(cls.engine, checkfirst=True)

    @classmethod
    def cleanup_database(cls):
        import re
        cls.engine.dispose()
        assert re.fullmatch(r"livability_v4_test_[0-9a-f]{12}", cls.database_name)
        assert cls.database_name != cls.admin.url.database
        with cls.admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{cls.database_name}" WITH (FORCE)'))
        cls.admin.dispose()

    def fixture(self, root):
        shell_dir, score_dir = write_tiers(
            root,
            profile="test",
            import_fingerprint="fixture-import_fingerprint",
            reality_fingerprint="fixture-gtfs",
        )
        # Four real 50 m cells, keeping the national shard layout/encoding.
        for directory in (shell_dir, score_dir):
            path = directory / "shards" / f"{SHARD}.npz"
            with np.load(path) as data:
                arrays = {name: data[name] for name in data.files}
            if directory == shell_dir:
                arrays["land_area_m2"][:] = 0
                arrays["valid_land_mask"][:] = False
                arrays["land_area_m2"][220:222, 140:142] = 2500
                arrays["valid_land_mask"][220:222, 140:142] = True
            np.savez_compressed(path, **arrays)
            build.complete_tier(directory, {})
        reach_dir = write_empty_reach_tier(
            root,
            losses={"transport": {"eligible": 1, "unbound": 0}},
            profile="test",
            import_fingerprint="fixture-import_fingerprint",
            reality_fingerprint="fixture-gtfs",
        )
        bike_root = root / "bike"
        bike_shell_dir, bike_score_dir = write_tiers(
            bike_root,
            profile="test",
            import_fingerprint="fixture-import_fingerprint",
            reality_fingerprint="fixture-gtfs",
        )
        bike_reach_dir = write_empty_reach_tier(
            bike_root,
            losses={"transport": {"eligible": 1, "unbound": 0}},
            profile="test",
            import_fingerprint="fixture-import_fingerprint",
            reality_fingerprint="fixture-gtfs",
            mode="bike",
        )
        graph_dir = root / "graph"
        write_walkgraph(graph_dir, [(0, 0), (100, 0), (0, 100), (100, 100)], [(0, 1), (0, 2), (1, 3), (2, 3)])
        offsets = np.fromfile(graph_dir / "walk_graph.adjacency_offsets.bin", dtype="<u8")
        targets = np.fromfile(graph_dir / "walk_graph.adjacency_targets.bin", dtype="<u4")
        lengths = np.fromfile(graph_dir / "walk_graph.adjacency_lengths.bin", dtype="<f4")
        edges = np.zeros(len(targets), dtype=[("src", "<u4"), ("dst", "<u4"), ("length_m", "<f4")])
        edges["src"] = np.repeat(np.arange(4), np.diff(offsets).astype(int))
        edges["dst"], edges["length_m"] = targets, lengths
        edges.tofile(graph_dir / "walk_graph.edges.bin")
        np.arange(4, dtype="<i8").tofile(graph_dir / "walk_graph.osmids.bin")
        metric = box(X0, Y0, X0 + 100, Y0 + 100)
        wgs = transform(lambda x, y, z=None: TO_WGS84(x, y), metric)
        hashes = BuildHashes(**{name: "fixture-" + name for name in BuildHashes.__dataclass_fields__})
        hashes = dataclasses.replace(hashes, build_profile="test")
        source = SourceState(root / "input.pbf", "extract", "importer", "importer-config", hashes.import_fingerprint)
        ctx = build.BuildContext(
            self.engine, "test", source, "fixture-gtfs", hashes, metric, wgs,
            LandMask(metric), "mask", (wgs.bounds[1], wgs.bounds[0], wgs.bounds[3], wgs.bounds[2]), 2250,
            build.TierKeys("g", "s", "r", "c"), graph_dir, shell_dir, reach_dir, score_dir,
            SimpleNamespace(
                reality_fingerprint="fixture-gtfs",
                analysis_date=NOW.date(),
                feed_states=(SimpleNamespace(
                    feed_id="fixture-feed", label="Fixture GTFS",
                    feed_fingerprint="fixture-feed-fingerprint",
                ),),
            ),
            overture_dataset_signature="fixture-overture",
        )
        ctx.bike_graph_padding_m = 5250
        ctx.import_completed_at = NOW
        ctx.bike_keys = build.TierKeys("bike-g", "s", "r", "c")
        ctx.bike_graph_dir = graph_dir
        ctx.bike_shell_dir = bike_shell_dir
        ctx.bike_reach_dir = bike_reach_dir
        ctx.bike_score_dir = bike_score_dir
        lon, lat = TO_WGS84(X0 + 25, Y0 + 25)
        with self.engine.begin() as connection:
            connection.execute(insert(tables.import_manifest), [{
                "import_fingerprint": hashes.import_fingerprint,
                "extract_path": str(source.extract_path),
                "extract_fingerprint": source.extract_fingerprint,
                "importer_version": source.importer_version,
                "importer_config_hash": source.importer_config_hash,
                "normalization_scope_hash": "fixture-scope",
                "status": "complete",
                "created_at": NOW,
                "completed_at": NOW,
            }])
            _bulk_insert(connection, tables.features, [
                {"import_fingerprint": hashes.import_fingerprint, "osm_type": "node", "osm_id": 1,
                 "category": "shops", "name": "Fixture supermarket", "tags_json": {"shop": "supermarket"},
                 "geom": Point(lon, lat), "created_at": NOW},
                {"import_fingerprint": hashes.import_fingerprint, "osm_type": "way", "osm_id": 2,
                 "category": "landuse", "name": "Fixture housing", "tags_json": {"landuse": "residential"},
                 "geom": wgs, "created_at": NOW},
            ])
            row = stop(geom=Point(lon, lat), reality_fingerprint="fixture-gtfs", import_fingerprint=hashes.import_fingerprint,
                       created_at=NOW, school_only_state="not_school_only", bus_frequency_tier="very_low_frequency")
            for column in tables.transit_gtfs_stop_reality.columns:
                if column.name in row or column.nullable:
                    continue
                row[column.name] = ([] if isinstance(column.type, JSONB) else False if isinstance(column.type, Boolean)
                                    else NOW if isinstance(column.type, DateTime) else NOW.date() if isinstance(column.type, Date)
                                    else 0 if isinstance(column.type, (Integer, Float)) else "")
            _bulk_insert(connection, tables.transit_gtfs_stop_reality, [row])
        return ctx

    def test_test_profile_publishes_without_v3_and_ignores_stale_builds(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ctx = self.fixture(root)
            archive = root / "map.pmtiles"
            captured = []

            def prepare(*args, **kwargs):
                layers = prepare_display_layers(*args, **kwargs)
                captured.append(layers)
                return layers

            def bake(engine, key, path, **kwargs):
                return bake_pmtiles(engine, key, path, workers=1, **kwargs)

            with (
                mock.patch.object(publish, "pmtiles_output_path", return_value=archive),
                mock.patch.object(publish, "noise_pmtiles_output_path", return_value=root / "noise.pmtiles"),
                mock.patch.object(publish, "prepare_display_layers", side_effect=prepare),
                mock.patch("precompute.display_layers.overture.load_overture_amenity_rows", return_value=[]),
                mock.patch("precompute.display_layers.overture.dataset_info", return_value={}),
            ):
                result = publish.publish_daily_access(ctx, progress=lambda _: None, bake_pmtiles=bake)
                self.assertEqual(result["point_layers_prepared"], {"amenities": 2, "transport_reality": 1, "service_deserts": 3})
                key = result["build_key"]
                with self.engine.connect() as connection:
                    manifest = connection.execute(select(tables.build_manifest).where(tables.build_manifest.c.build_key == key)).mappings().one()
                    self.assertEqual(connection.execute(select(func.count()).select_from(tables.build_manifest)).scalar_one(), 1)
                    self.assertEqual(manifest["summary_json"]["amenity_counts"]["shops"], 1)
                    self.assertEqual(manifest["summary_json"]["landuse_context_counts"], {"residential": 1})
                    original_cells = connection.execute(select(tables.grid_walk.c.total_score).where(tables.grid_walk.c.build_key == key)).scalars().all()
                    self.assertEqual(original_cells, [60.0] * 3)
                self.assertEqual(
                    load_latest_scoring_build_manifest(
                        self.engine, scoring_model="daily-access-v4", profile="test",
                    )["build_key"],
                    key,
                )
                # An unrelated published v3 manifest is not an input to v4.
                stale = dict(manifest)
                stale.update(build_key=ctx.hashes.build_key, created_at=NOW, completed_at=datetime.now(timezone.utc),
                             summary_json={"amenity_counts": {"shops": 999999}})
                with self.engine.begin() as connection:
                    connection.execute(insert(tables.build_manifest), [stale])
                self.assertEqual(
                    load_latest_scoring_build_manifest(
                        self.engine, scoring_model="daily-access-v4", profile="test",
                    )["build_key"],
                    key,
                )
                repeated = publish.publish_daily_access(ctx, progress=lambda _: None, bake_pmtiles=bake)
                self.assertEqual(result["build_key"], repeated["build_key"])
                def without_time(rows):
                    return [{key: value for key, value in row.items() if key != "created_at"} for row in rows]
                self.assertEqual(without_time(captured[0].amenities), without_time(captured[1].amenities))
                self.assertEqual(captured[0].summary["amenity_counts"], captured[1].summary["amenity_counts"])
                # Exercise the actual CLI/cache path using the prepared fixture inputs.
                import main
                from contextlib import redirect_stdout
                import io
                with (
                    mock.patch.object(build, "resolve_build_context", return_value=ctx),
                    mock.patch("network.graph_meta_matches", return_value=True),
                    mock.patch("precompute.bake_pmtiles.bake_pmtiles", side_effect=bake),
                    redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(main.main(["daily-access", "--profile", "test", "--publish"]), 0)
                with self.engine.connect() as connection:
                    self.assertEqual(connection.execute(select(func.count()).select_from(tables.amenities)
                                                       .where(tables.amenities.c.build_key == key)).scalar_one(), 2)
                    self.assertEqual(connection.execute(select(tables.build_manifest.c.build_key)
                                                       .where(tables.build_manifest.c.config_hash == ctx.hashes.config_hash)
                                                       .order_by(tables.build_manifest.c.completed_at.desc()).limit(1)).scalar_one(), key)
                # A bake failure on a repeated publication must preserve live rows/files.
                previous = archive.read_bytes()
                with self.assertRaisesRegex(RuntimeError, "bake failed"):
                    publish.publish_daily_access(ctx, progress=lambda _: None, bake_pmtiles=mock.Mock(side_effect=RuntimeError("bake failed")))
                self.assertEqual(archive.read_bytes(), previous)
                with self.engine.connect() as connection:
                    self.assertEqual(connection.execute(select(tables.build_manifest.c.config_hash)
                                                       .where(tables.build_manifest.c.build_key == key)).scalar_one(), ctx.hashes.config_hash)
                # Prepared noise is sufficient; no source archive/ingest is involved.
                with self.engine.begin() as connection:
                    connection.execute(insert(tables.noise_artifact_manifest), [{
                        "artifact_hash": "fixture-accurate", "artifact_type": "resolved", "status": "complete",
                        "manifest_json": {"noise_accuracy_mode": "accurate"}, "created_at": NOW, "completed_at": NOW,
                    }])
                    connection.execute(insert(tables.noise_resolved_display), [{
                        "noise_resolved_hash": "fixture-accurate", "noise_feature_id": 1, "jurisdiction": "roi",
                        "source_type": "road", "metric": "Lden", "round_number": 4, "report_period": "fixture",
                        "db_low": 55, "db_high": 59, "db_value": "55-59",
                        "geom": from_shape(MultiPolygon([ctx.study_area_metric]), srid=2157),
                    }])
                with mock.patch.dict(os.environ, {"NOISE_ACCURACY_MODE": "accurate"}):
                    with self.assertRaisesRegex(RuntimeError, "noise candidate bake failed"):
                        publish.publish_daily_access(ctx, progress=lambda _: None,
                                                     bake_pmtiles=mock.Mock(side_effect=RuntimeError("noise candidate bake failed")))
                    with self.engine.connect() as connection:
                        self.assertEqual(connection.execute(select(func.count()).select_from(tables.noise_polygons)).scalar_one(), 0)
                    self.assertEqual(archive.read_bytes(), previous)
                    noisy = publish.publish_daily_access(ctx, progress=lambda _: None, bake_pmtiles=bake)
                self.assertNotEqual(noisy["build_key"], key)
                key = noisy["build_key"]
                with self.engine.connect() as connection:
                    summary = connection.execute(select(tables.build_manifest.c.summary_json)
                                                 .where(tables.build_manifest.c.build_key == key)).scalar_one()
                    self.assertEqual(summary["noise_source_counts"], {"road": 1})
                    self.assertEqual(summary["display_layers"]["counts"]["noise_polygons"], 1)
                    self.assertEqual(summary["config_hash"], ctx.hashes.config_hash)
                    self.assertEqual(connection.execute(select(tables.grid_walk.c.total_score)
                                                       .where(tables.grid_walk.c.build_key == key)).scalars().all(), original_cells)
                with (root / "noise.pmtiles").open("rb") as noise_handle:
                    def read_noise(offset, length):
                        noise_handle.seek(offset)
                        return noise_handle.read(length)
                    self.assertEqual(Reader(read_noise).metadata()["build_key"], key)
                import serve_from_db
                with (
                    mock.patch.object(serve_from_db, "OSM_EXTRACT_PATH", ctx.source_state.extract_path),
                    mock.patch.object(serve_from_db, "build_config_hashes", return_value=ctx.hashes),
                ):
                    service = serve_from_db.RuntimeService(self.engine, profile="test")
                    service._noise_pmtiles_path = root / "noise.pmtiles"
                    runtime = service.get_runtime()
                    self.assertEqual(runtime["build_key"], key)
                    self.assertEqual(runtime["scoring_model"], "daily-access-v4")
                    self.assertEqual(runtime["data_provenance"]["freshness"]["status"], "stale")
                    self.assertEqual(runtime["data_provenance"]["gtfs"]["sources"][0]["label"], "Fixture GTFS")
                    self.assertEqual(runtime["amenity_counts"]["shops"], 1)
                    self.assertTrue(runtime["noise_enabled"])
                    lon, lat = TO_WGS84(X0 + 25, Y0 + 25)
                    self.assertEqual(service.inspect(lat=lat, lon=lon)["total_score"], 60.0)
            layers = set()
            with archive.open("rb") as handle:
                def read(offset, length):
                    handle.seek(offset)
                    return handle.read(length)
                self.assertEqual(Reader(read).metadata()["build_key"], key)
                for _tile, data in all_tiles(read):
                    layers.update(_decode_vector_tile(gzip.decompress(data)))
            self.assertTrue({"grid", "amenities", "transport_reality", "service_deserts", "landuse_context"}.issubset(layers), layers)
