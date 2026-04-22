from __future__ import annotations

import importlib
import json
import math
import struct
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import numpy as np


fine_worker = importlib.import_module("fine_vector_pmtiles_worker")


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _aligned_span_start(min_value: float, max_value: float, *, span: int, step: int) -> int:
    lower_bound = int(math.ceil(float(min_value) / float(step)) * step)
    upper_bound = int(math.floor((float(max_value) - float(span)) / float(step)) * step)
    if upper_bound < lower_bound:
        raise AssertionError("Tile bounds are too small for the synthetic shard fixture")
    midpoint = (float(min_value) + float(max_value) - float(span)) / 2.0
    candidate = int(math.floor(midpoint / float(step)) * step)
    return max(lower_bound, min(candidate, upper_bound))


def _find_fixture_tile_and_shard(lon: float, lat: float, *, zoom: int, shard_size_m: int) -> tuple[int, int, int, int]:
    base_tile_x = fine_worker._lon_to_tile_x(lon, zoom)
    base_tile_y = fine_worker._lat_to_tile_y(lat, zoom)
    tile_span = 1 << zoom
    for delta_y in range(-12, 13):
        tile_y = base_tile_y + delta_y
        if tile_y < 0 or tile_y >= tile_span:
            continue
        for delta_x in range(-12, 13):
            tile_x = base_tile_x + delta_x
            if tile_x < 0 or tile_x >= tile_span:
                continue
            tile_bounds = fine_worker._metric_tile_bounds(zoom, tile_x, tile_y)
            try:
                shard_x = _aligned_span_start(
                    tile_bounds[0],
                    tile_bounds[2],
                    span=shard_size_m,
                    step=shard_size_m,
                )
                shard_y = _aligned_span_start(
                    tile_bounds[1],
                    tile_bounds[3],
                    span=shard_size_m,
                    step=shard_size_m,
                )
            except AssertionError:
                continue
            return tile_x, tile_y, shard_x, shard_y
    raise AssertionError("Could not find a nearby z15 tile that contains a full aligned shard")


def _build_surface_fixture(
    root: Path,
    *,
    shard_count: int,
    shard_size_m: int = 500,
) -> tuple[Path, Path, list[str]]:
    shell_dir = root / "shell"
    score_dir = root / "scores"
    shard_dir_shell = shell_dir / "shards"
    shard_dir_score = score_dir / "shards"
    shard_dir_shell.mkdir(parents=True, exist_ok=True)
    shard_dir_score.mkdir(parents=True, exist_ok=True)

    shell_inventory = []
    score_inventory = []
    shard_ids: list[str] = []
    cells_per_side = shard_size_m // 50
    for index in range(shard_count):
        shard_x = index * shard_size_m
        shard_y = 0
        shard_id = f"{shard_x}_{shard_y}"
        shard_ids.append(shard_id)
        effective_area_ratio = np.ones((cells_per_side, cells_per_side), dtype=np.float32)
        total_score_50 = np.full((cells_per_side, cells_per_side), float(index + 1), dtype=np.float32)
        np.savez_compressed(
            shard_dir_shell / f"{shard_id}.npz",
            effective_area_ratio=effective_area_ratio,
        )
        np.savez_compressed(
            shard_dir_score / f"{shard_id}.npz",
            total_score_50=total_score_50,
        )
        shell_inventory.append(
            {
                "shard_id": shard_id,
                "x_min_m": shard_x,
                "y_min_m": shard_y,
                "x_max_m": shard_x + shard_size_m,
                "y_max_m": shard_y + shard_size_m,
                "path": f"shards/{shard_id}.npz",
            }
        )
        score_inventory.append(
            {
                "shard_id": shard_id,
                "path": f"shards/{shard_id}.npz",
            }
        )

    _write_manifest(
        shell_dir / "manifest.json",
        {
            "status": "complete",
            "schema_version": 1,
            "base_resolution_m": 50,
            "shard_size_m": shard_size_m,
            "shard_inventory": shell_inventory,
        },
    )
    _write_manifest(
        score_dir / "manifest.json",
        {
            "status": "complete",
            "schema_version": 1,
            "base_resolution_m": 50,
            "shard_inventory": score_inventory,
        },
    )
    return shell_dir, score_dir, shard_ids


def _write_single_shard_fixture(
    root: Path,
    *,
    zoom: int,
    shard_size_m: int,
    lon: float = -6.2603,
    lat: float = 53.3498,
) -> tuple[Path, Path, int, int, int, int]:
    shell_dir = root / "shell"
    score_dir = root / "scores"
    shard_dir_shell = shell_dir / "shards"
    shard_dir_score = score_dir / "shards"
    shard_dir_shell.mkdir(parents=True, exist_ok=True)
    shard_dir_score.mkdir(parents=True, exist_ok=True)

    tile_x, tile_y, shard_x, shard_y = _find_fixture_tile_and_shard(
        lon,
        lat,
        zoom=zoom,
        shard_size_m=shard_size_m,
    )
    shard_id = f"{shard_x}_{shard_y}"
    cells_per_side = shard_size_m // 50
    effective_area_ratio = np.ones((cells_per_side, cells_per_side), dtype=np.float32)
    total_score_50 = np.ones((cells_per_side, cells_per_side), dtype=np.float32)

    np.savez_compressed(
        shard_dir_shell / f"{shard_id}.npz",
        effective_area_ratio=effective_area_ratio,
    )
    np.savez_compressed(
        shard_dir_score / f"{shard_id}.npz",
        total_score_50=total_score_50,
    )

    _write_manifest(
        shell_dir / "manifest.json",
        {
            "status": "complete",
            "schema_version": 1,
            "base_resolution_m": 50,
            "shard_size_m": shard_size_m,
            "shard_inventory": [
                {
                    "shard_id": shard_id,
                    "x_min_m": shard_x,
                    "y_min_m": shard_y,
                    "x_max_m": shard_x + shard_size_m,
                    "y_max_m": shard_y + shard_size_m,
                    "path": f"shards/{shard_id}.npz",
                }
            ],
        },
    )
    _write_manifest(
        score_dir / "manifest.json",
        {
            "status": "complete",
            "schema_version": 1,
            "base_resolution_m": 50,
            "shard_inventory": [
                {
                    "shard_id": shard_id,
                    "path": f"shards/{shard_id}.npz",
                }
            ],
        },
    )
    return shell_dir, score_dir, tile_x, tile_y, shard_x, shard_y


def _decoded_grid_features(payload: bytes) -> list[dict[str, object]]:
    decoded = _decode_vector_tile(payload)
    layer = decoded.get("grid", {})
    if not isinstance(layer, dict):
        return []
    features = layer.get("features", [])
    if not isinstance(features, list):
        return []
    return features


def _polygon_x_values(feature: dict[str, object]) -> list[float]:
    geometry = feature.get("geometry", {})
    if not isinstance(geometry, dict):
        return []
    coordinates = geometry.get("coordinates", [])
    if not isinstance(coordinates, list):
        return []
    x_values: list[float] = []
    for ring in coordinates:
        if not isinstance(ring, list):
            continue
        for point in ring:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            x_values.append(float(point[0]))
    return x_values


def _read_varint(payload: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    value = 0
    cursor = int(offset)
    while True:
        byte = payload[cursor]
        cursor += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return value, cursor
        shift += 7


def _read_length_delimited(payload: bytes, offset: int) -> tuple[bytes, int]:
    size, cursor = _read_varint(payload, offset)
    end = cursor + size
    return payload[cursor:end], end


def _zigzag_decode(value: int) -> int:
    normalized = int(value)
    return (normalized >> 1) ^ (-(normalized & 1))


def _decode_value_message(payload: bytes) -> object:
    cursor = 0
    while cursor < len(payload):
        key, cursor = _read_varint(payload, cursor)
        field_number = key >> 3
        wire_type = key & 0x7
        if wire_type == 2:
            raw, cursor = _read_length_delimited(payload, cursor)
            if field_number == 1:
                return raw.decode("utf-8")
            continue
        if wire_type == 1 and field_number == 3:
            value = struct.unpack("<d", payload[cursor:cursor + 8])[0]
            cursor += 8
            return float(value)
        if wire_type == 0:
            value, cursor = _read_varint(payload, cursor)
            if field_number == 4:
                return value
            if field_number == 5:
                return value
            if field_number == 6:
                return _zigzag_decode(value)
            if field_number == 7:
                return bool(value)
    return None


def _decode_geometry(payload: bytes) -> dict[str, object]:
    cursor = 0
    cursor_x = 0
    cursor_y = 0
    rings: list[list[list[int]]] = []
    current_ring: list[list[int]] = []
    while cursor < len(payload):
        command, cursor = _read_varint(payload, cursor)
        command_id = command & 0x7
        count = command >> 3
        if command_id == 1:
            if current_ring:
                rings.append(current_ring)
                current_ring = []
            for _ in range(count):
                delta_x, cursor = _read_varint(payload, cursor)
                delta_y, cursor = _read_varint(payload, cursor)
                cursor_x += _zigzag_decode(delta_x)
                cursor_y += _zigzag_decode(delta_y)
                current_ring.append([cursor_x, cursor_y])
            continue
        if command_id == 2:
            for _ in range(count):
                delta_x, cursor = _read_varint(payload, cursor)
                delta_y, cursor = _read_varint(payload, cursor)
                cursor_x += _zigzag_decode(delta_x)
                cursor_y += _zigzag_decode(delta_y)
                current_ring.append([cursor_x, cursor_y])
            continue
        if command_id == 7:
            if current_ring:
                if current_ring[0] != current_ring[-1]:
                    current_ring.append(list(current_ring[0]))
                rings.append(current_ring)
                current_ring = []
            continue
        raise AssertionError(f"Unsupported geometry command id: {command_id}")
    if current_ring:
        rings.append(current_ring)
    return {"type": "Polygon", "coordinates": rings}


def _decode_feature_message(payload: bytes, keys: list[str], values: list[object]) -> dict[str, object]:
    cursor = 0
    properties: dict[str, object] = {}
    geometry = {"type": "Polygon", "coordinates": []}
    while cursor < len(payload):
        key, cursor = _read_varint(payload, cursor)
        field_number = key >> 3
        wire_type = key & 0x7
        if field_number == 2 and wire_type == 2:
            tags_payload, cursor = _read_length_delimited(payload, cursor)
            tags_cursor = 0
            tag_indexes: list[int] = []
            while tags_cursor < len(tags_payload):
                index_value, tags_cursor = _read_varint(tags_payload, tags_cursor)
                tag_indexes.append(index_value)
            for tag_index in range(0, len(tag_indexes), 2):
                key_index = tag_indexes[tag_index]
                value_index = tag_indexes[tag_index + 1]
                properties[keys[key_index]] = values[value_index]
            continue
        if field_number == 4 and wire_type == 2:
            geometry_payload, cursor = _read_length_delimited(payload, cursor)
            geometry = _decode_geometry(geometry_payload)
            continue
        if wire_type == 2:
            _, cursor = _read_length_delimited(payload, cursor)
            continue
        if wire_type == 0:
            _, cursor = _read_varint(payload, cursor)
            continue
        if wire_type == 1:
            cursor += 8
            continue
    return {"properties": properties, "geometry": geometry}


def _decode_layer_message(payload: bytes) -> dict[str, object]:
    cursor = 0
    name = ""
    feature_payloads: list[bytes] = []
    keys: list[str] = []
    values: list[object] = []
    while cursor < len(payload):
        key, cursor = _read_varint(payload, cursor)
        field_number = key >> 3
        wire_type = key & 0x7
        if wire_type == 2:
            field_payload, cursor = _read_length_delimited(payload, cursor)
            if field_number == 1:
                name = field_payload.decode("utf-8")
            elif field_number == 2:
                feature_payloads.append(field_payload)
            elif field_number == 3:
                keys.append(field_payload.decode("utf-8"))
            elif field_number == 4:
                values.append(_decode_value_message(field_payload))
            continue
        if wire_type == 0:
            _, cursor = _read_varint(payload, cursor)
            continue
        if wire_type == 1:
            cursor += 8
            continue
    return {
        "name": name,
        "features": [_decode_feature_message(feature_payload, keys, values) for feature_payload in feature_payloads],
    }


def _decode_vector_tile(payload: bytes) -> dict[str, dict[str, object]]:
    cursor = 0
    layers: dict[str, dict[str, object]] = {}
    while cursor < len(payload):
        key, cursor = _read_varint(payload, cursor)
        field_number = key >> 3
        wire_type = key & 0x7
        if field_number != 3 or wire_type != 2:
            raise AssertionError(f"Unsupported tile field {field_number} with wire type {wire_type}")
        layer_payload, cursor = _read_length_delimited(payload, cursor)
        layer = _decode_layer_message(layer_payload)
        layer_name = str(layer.get("name") or "")
        layers[layer_name] = layer
    return layers


class FineVectorPmtilesWorkerTests(TestCase):
    def tearDown(self) -> None:
        fine_worker._FINE_GRID_CONTEXTS.clear()

    def test_clip_polygon_to_extent_skips_degenerate_buffered_sliver(self) -> None:
        ring = [
            (4352.0, -256.0),
            (4352.0, -255.973),
            (4352.0, -256.0),
        ]

        clipped = fine_worker._clip_polygon_to_extent(ring)

        self.assertEqual(clipped, [])

    def test_integer_property_round_trips_without_doubling(self) -> None:
        from mapbox_vector_tile import encode as encode_vector_tile

        ring = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        feature = {
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {"resolution_m": 250, "cell_id": 7, "negative": -3},
        }
        encoded = encode_vector_tile(
            [{"name": "grid", "features": [feature]}],
            default_options={"extents": 4096},
        )

        decoded = _decode_vector_tile(encoded)
        decoded_props = decoded["grid"]["features"][0]["properties"]
        self.assertEqual(decoded_props["resolution_m"], 250)
        self.assertEqual(decoded_props["cell_id"], 7)
        self.assertEqual(decoded_props["negative"], -3)

    def test_z15_layer_emits_250_100_and_50m_features_and_skips_invalid_blocks(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            score_dir = tmp / "scores"
            shard_dir_shell = shell_dir / "shards"
            shard_dir_score = score_dir / "shards"
            shard_dir_shell.mkdir(parents=True, exist_ok=True)
            shard_dir_score.mkdir(parents=True, exist_ok=True)

            lon = -6.2603
            lat = 53.3498
            shard_size_m = 500
            tile_x, tile_y, shard_x, shard_y = _find_fixture_tile_and_shard(
                lon,
                lat,
                zoom=15,
                shard_size_m=shard_size_m,
            )
            shard_id = f"{shard_x}_{shard_y}"

            effective_area_ratio = np.ones((10, 10), dtype=np.float32)
            effective_area_ratio[8:10, 8:10] = 0.0
            total_score_50 = np.arange(100, dtype=np.float32).reshape(10, 10)

            np.savez_compressed(
                shard_dir_shell / f"{shard_id}.npz",
                effective_area_ratio=effective_area_ratio,
            )
            np.savez_compressed(
                shard_dir_score / f"{shard_id}.npz",
                total_score_50=total_score_50,
            )

            _write_manifest(
                shell_dir / "manifest.json",
                {
                    "status": "complete",
                    "schema_version": 1,
                    "base_resolution_m": 50,
                    "shard_size_m": shard_size_m,
                    "shard_inventory": [
                        {
                            "shard_id": shard_id,
                            "x_min_m": shard_x,
                            "y_min_m": shard_y,
                            "x_max_m": shard_x + shard_size_m,
                            "y_max_m": shard_y + shard_size_m,
                            "path": f"shards/{shard_id}.npz",
                        }
                    ],
                },
            )
            _write_manifest(
                score_dir / "manifest.json",
                {
                    "status": "complete",
                    "schema_version": 1,
                    "base_resolution_m": 50,
                    "shard_inventory": [
                        {
                            "shard_id": shard_id,
                            "path": f"shards/{shard_id}.npz",
                        }
                    ],
                },
            )

            context = fine_worker.FineGridTileContext(shell_dir=shell_dir, score_dir=score_dir)
            layer = context.build_grid_layer(z=15, x=tile_x, y=tile_y)

        self.assertIsNotNone(layer)
        self.assertEqual(layer["name"], "grid")
        features = layer["features"]
        counts_by_resolution: dict[int, int] = {}
        feature_by_cell_id = {}
        for feature in features:
            properties = feature["properties"]
            resolution_m = int(properties["resolution_m"])
            counts_by_resolution[resolution_m] = counts_by_resolution.get(resolution_m, 0) + 1
            feature_by_cell_id[str(properties["cell_id"])] = properties

        self.assertEqual(counts_by_resolution[250], 4)
        self.assertEqual(counts_by_resolution[100], 24)
        self.assertEqual(counts_by_resolution[50], 96)

        fifty_m_cell_id = f"50:{int(round((shard_x + 50) * 1000))}:{int(round(shard_y * 1000))}"
        self.assertEqual(feature_by_cell_id[fifty_m_cell_id]["total_score"], 1.0)

        two_hundred_fifty_m_cell_id = f"250:{int(round(shard_x * 1000))}:{int(round(shard_y * 1000))}"
        self.assertAlmostEqual(feature_by_cell_id[two_hundred_fifty_m_cell_id]["total_score"], 22.0)

        invalid_hundred_m_cell_id = f"100:{int(round((shard_x + 400) * 1000))}:{int(round((shard_y + 400) * 1000))}"
        self.assertNotIn(invalid_hundred_m_cell_id, feature_by_cell_id)

    def test_encoded_tile_bytes_keep_expected_resolutions_per_zoom(self) -> None:
        cases = (
            (12, 2500, {2500}),
            (13, 1000, {1000}),
            (14, 500, {500}),
            (15, 500, {50, 100, 250}),
        )

        for zoom, shard_size_m, expected_resolutions in cases:
            with self.subTest(zoom=zoom):
                with TemporaryDirectory() as tmp_name:
                    tmp = Path(tmp_name)
                    shell_dir, score_dir, tile_x, tile_y, _, _ = _write_single_shard_fixture(
                        tmp,
                        zoom=zoom,
                        shard_size_m=shard_size_m,
                    )
                    payload = fine_worker._fine_grid_tile_bytes(
                        config={"shell_dir": str(shell_dir), "score_dir": str(score_dir)},
                        z=zoom,
                        x=tile_x,
                        y=tile_y,
                    )

                features = _decoded_grid_features(payload)
                resolutions = {
                    int((feature.get("properties", {}) or {}).get("resolution_m", 0))
                    for feature in features
                }

                self.assertTrue(payload)
                self.assertEqual(resolutions, expected_resolutions)

    def test_adjacent_z15_tiles_share_buffered_border_cells(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "shell"
            score_dir = tmp / "scores"
            shard_dir_shell = shell_dir / "shards"
            shard_dir_score = score_dir / "shards"
            shard_dir_shell.mkdir(parents=True, exist_ok=True)
            shard_dir_score.mkdir(parents=True, exist_ok=True)

            lon = -6.2603
            lat = 53.3498
            shard_size_m = 500
            tile_x = fine_worker._lon_to_tile_x(lon, 15)
            tile_y = fine_worker._lat_to_tile_y(lat, 15)
            left_bounds = fine_worker._metric_tile_bounds(15, tile_x, tile_y)
            boundary_x = left_bounds[2]
            shard_x = int(math.floor(boundary_x / shard_size_m) * shard_size_m)
            shard_y = int(math.floor(left_bounds[1] / shard_size_m) * shard_size_m)
            shard_id = f"{shard_x}_{shard_y}"

            effective_area_ratio = np.ones((10, 10), dtype=np.float32)
            total_score_50 = np.ones((10, 10), dtype=np.float32)
            np.savez_compressed(
                shard_dir_shell / f"{shard_id}.npz",
                effective_area_ratio=effective_area_ratio,
            )
            np.savez_compressed(
                shard_dir_score / f"{shard_id}.npz",
                total_score_50=total_score_50,
            )
            _write_manifest(
                shell_dir / "manifest.json",
                {
                    "status": "complete",
                    "schema_version": 1,
                    "base_resolution_m": 50,
                    "shard_size_m": shard_size_m,
                    "shard_inventory": [
                        {
                            "shard_id": shard_id,
                            "x_min_m": shard_x,
                            "y_min_m": shard_y,
                            "x_max_m": shard_x + shard_size_m,
                            "y_max_m": shard_y + shard_size_m,
                            "path": f"shards/{shard_id}.npz",
                        }
                    ],
                },
            )
            _write_manifest(
                score_dir / "manifest.json",
                {
                    "status": "complete",
                    "schema_version": 1,
                    "base_resolution_m": 50,
                    "shard_inventory": [
                        {
                            "shard_id": shard_id,
                            "path": f"shards/{shard_id}.npz",
                        }
                    ],
                },
            )

            left_payload = fine_worker._fine_grid_tile_bytes(
                config={"shell_dir": str(shell_dir), "score_dir": str(score_dir)},
                z=15,
                x=tile_x,
                y=tile_y,
            )
            right_payload = fine_worker._fine_grid_tile_bytes(
                config={"shell_dir": str(shell_dir), "score_dir": str(score_dir)},
                z=15,
                x=tile_x + 1,
                y=tile_y,
            )

        left_features = _decoded_grid_features(left_payload)
        right_features = _decoded_grid_features(right_payload)
        left_by_cell_id = {
            str((feature.get("properties", {}) or {}).get("cell_id")): feature for feature in left_features
        }
        right_by_cell_id = {
            str((feature.get("properties", {}) or {}).get("cell_id")): feature for feature in right_features
        }

        crossing_col = int(math.floor(((boundary_x - shard_x) - 1e-6) / 50.0))
        crossing_cell_x_min = shard_x + (crossing_col * 50)
        crossing_cell_y_min = shard_y + 50
        crossing_cell_id = fine_worker._cell_id(50, crossing_cell_x_min, crossing_cell_y_min)
        self.assertLess(crossing_cell_x_min, boundary_x)
        self.assertGreater(crossing_cell_x_min + 50, boundary_x)

        self.assertIn(crossing_cell_id, left_by_cell_id)
        self.assertIn(crossing_cell_id, right_by_cell_id)
        self.assertGreater(max(_polygon_x_values(left_by_cell_id[crossing_cell_id])), fine_worker._TILE_EXTENT)
        self.assertLess(min(_polygon_x_values(right_by_cell_id[crossing_cell_id])), 0.0)

    def test_context_lru_caches_stay_within_limits(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir, score_dir, shard_ids = _build_surface_fixture(tmp, shard_count=10)
            context = fine_worker.FineGridTileContext(shell_dir=shell_dir, score_dir=score_dir)

            for shard_id in shard_ids:
                context._load_shell_shard(shard_id)
                context._load_score_shard(shard_id)
            for shard_id in shard_ids:
                for resolution_m in (50, 100):
                    context.aggregated_shard_surface(shard_id, resolution_m)

            sizes = context.cache_sizes()

        self.assertLessEqual(sizes["shell"], fine_worker._RAW_SHELL_CACHE_LIMIT)
        self.assertLessEqual(sizes["score"], fine_worker._RAW_SCORE_CACHE_LIMIT)
        self.assertLessEqual(sizes["aggregated"], fine_worker._AGGREGATED_CACHE_LIMIT)

    def test_bake_chunk_worker_resets_large_caches_but_keeps_manifest_state(self) -> None:
        class _FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                del exc_type, exc, tb

        class _FakeEngine:
            def connect(self):
                return _FakeConnection()

        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir, score_dir, shard_ids = _build_surface_fixture(tmp, shard_count=1)
            shard_id = shard_ids[0]
            context = fine_worker.FineGridTileContext(shell_dir=shell_dir, score_dir=score_dir)
            context.shard_wgs84_bbox(shard_id)
            cache_key = (str(shell_dir), str(score_dir))
            fine_worker._FINE_GRID_CONTEXTS.clear()
            fine_worker._FINE_GRID_CONTEXTS[cache_key] = context

            def _fake_tile_bytes(connection, *, build_key, z, x, y, layers, fine_grid_config):
                del connection, build_key, z, x, y, layers
                cached_context = fine_worker._fine_grid_context(fine_grid_config)
                self.assertIs(cached_context, context)
                cached_context._load_shell_shard(shard_id)
                cached_context._load_score_shard(shard_id)
                cached_context.aggregated_shard_surface(shard_id, 50)
                return b"tile"

            with (
                mock.patch.object(fine_worker, "_worker_get_engine", return_value=_FakeEngine()),
                mock.patch.object(
                    fine_worker,
                    "_tile_mvt_bytes_by_flags",
                    side_effect=_fake_tile_bytes,
                ),
            ):
                result = fine_worker._bake_chunk_worker(
                    [(15, 0, 0, fine_worker._LAYER_FINE_GRID)],
                    "build-key-123",
                    "postgresql://example",
                    {"shell_dir": str(shell_dir), "score_dir": str(score_dir)},
                )

            sizes = context.cache_sizes()
            fine_worker._FINE_GRID_CONTEXTS.clear()

        self.assertEqual(len(result), 1)
        self.assertEqual(sizes["shell"], 0)
        self.assertEqual(sizes["score"], 0)
        self.assertEqual(sizes["aggregated"], 0)
        self.assertEqual(sizes["wgs84_bbox"], 1)
        self.assertEqual(context.shard_count, 1)
