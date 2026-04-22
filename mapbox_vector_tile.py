from __future__ import annotations

from typing import Any, Iterable


_WIRE_VARINT = 0
_WIRE_LEN = 2

_GEOM_UNKNOWN = 0
_GEOM_POINT = 1
_GEOM_LINESTRING = 2
_GEOM_POLYGON = 3


def _varint(value: int) -> bytes:
    normalized = int(value)
    if normalized < 0:
        raise ValueError("varint cannot encode negative values")
    output = bytearray()
    while normalized >= 0x80:
        output.append((normalized & 0x7F) | 0x80)
        normalized >>= 7
    output.append(normalized)
    return bytes(output)


def _zigzag(value: int) -> int:
    normalized = int(value)
    return (normalized << 1) ^ (normalized >> 63)


def _field_key(field_number: int, wire_type: int) -> bytes:
    return _varint((int(field_number) << 3) | int(wire_type))


def _len_field(field_number: int, payload: bytes) -> bytes:
    return _field_key(field_number, _WIRE_LEN) + _varint(len(payload)) + payload


def _varint_field(field_number: int, value: int) -> bytes:
    return _field_key(field_number, _WIRE_VARINT) + _varint(value)


def _value_message(value: Any) -> bytes:
    payload = bytearray()
    if isinstance(value, bool):
        payload.extend(_varint_field(7, 1 if value else 0))
    elif isinstance(value, int):
        if value >= 0:
            payload.extend(_varint_field(5, value))
        else:
            payload.extend(_varint_field(6, _zigzag(value)))
    elif isinstance(value, float):
        import struct

        payload.extend(_field_key(3, 1))
        payload.extend(struct.pack("<d", float(value)))
    else:
        payload.extend(_len_field(1, str(value).encode("utf-8")))
    return bytes(payload)


def _command(command_id: int, count: int) -> int:
    return (int(count) << 3) | int(command_id)


def _normalize_ring(ring: Iterable[Iterable[int | float]]) -> list[tuple[int, int]]:
    points = [(int(round(point[0])), int(round(point[1]))) for point in ring]
    if len(points) < 4:
        raise ValueError("polygon rings require at least four coordinates")
    if points[0] == points[-1]:
        points = points[:-1]
    if len(points) < 3:
        raise ValueError("polygon rings require at least three unique vertices")
    return points


def _encode_polygon_geometry(geometry: dict[str, Any]) -> bytes:
    if str(geometry.get("type") or "") != "Polygon":
        raise ValueError("only Polygon geometry is supported")
    rings = geometry.get("coordinates") or []
    if not isinstance(rings, list) or not rings:
        raise ValueError("Polygon geometry requires coordinates")

    cursor_x = 0
    cursor_y = 0
    payload = bytearray()
    for ring in rings:
        normalized_ring = _normalize_ring(ring)
        payload.extend(_varint(_command(1, 1)))
        first_x, first_y = normalized_ring[0]
        payload.extend(_varint(_zigzag(first_x - cursor_x)))
        payload.extend(_varint(_zigzag(first_y - cursor_y)))
        cursor_x = first_x
        cursor_y = first_y

        if len(normalized_ring) > 1:
            payload.extend(_varint(_command(2, len(normalized_ring) - 1)))
            for point_x, point_y in normalized_ring[1:]:
                payload.extend(_varint(_zigzag(point_x - cursor_x)))
                payload.extend(_varint(_zigzag(point_y - cursor_y)))
                cursor_x = point_x
                cursor_y = point_y
        payload.extend(_varint(_command(7, 1)))
    return bytes(payload)


def _geometry_type(geometry: dict[str, Any]) -> int:
    geometry_name = str(geometry.get("type") or "")
    if geometry_name == "Polygon":
        return _GEOM_POLYGON
    if geometry_name == "LineString":
        return _GEOM_LINESTRING
    if geometry_name == "Point":
        return _GEOM_POINT
    return _GEOM_UNKNOWN


def _feature_message(
    feature: dict[str, Any],
    *,
    key_indexes: dict[str, int],
    value_indexes: dict[tuple[str, Any], int],
) -> bytes:
    payload = bytearray()
    feature_id = feature.get("id")
    if feature_id is not None:
        payload.extend(_varint_field(1, int(feature_id)))

    properties = feature.get("properties") or {}
    tags = bytearray()
    for key, value in properties.items():
        key_index = key_indexes[str(key)]
        value_key = _value_key(value)
        value_index = value_indexes[value_key]
        tags.extend(_varint(key_index))
        tags.extend(_varint(value_index))
    if tags:
        payload.extend(_len_field(2, bytes(tags)))

    geometry = feature.get("geometry") or {}
    payload.extend(_varint_field(3, _geometry_type(geometry)))
    payload.extend(_len_field(4, _encode_polygon_geometry(geometry)))
    return bytes(payload)


def _value_key(value: Any) -> tuple[str, Any]:
    if isinstance(value, bool):
        return ("bool", bool(value))
    if isinstance(value, int):
        return ("int", int(value))
    if isinstance(value, float):
        return ("float", float(value))
    return ("string", str(value))


def encode(
    layers: list[dict[str, Any]] | dict[str, Any],
    default_options: dict[str, Any] | None = None,
    **_: Any,
) -> bytes:
    if isinstance(layers, dict):
        normalized_layers = [layers]
    else:
        normalized_layers = list(layers)

    options = dict(default_options or {})
    extent = int(options.get("extents", 4096))
    tile_payload = bytearray()

    for layer in normalized_layers:
        layer_name = str(layer.get("name") or "")
        features = list(layer.get("features") or [])
        key_indexes: dict[str, int] = {}
        value_indexes: dict[tuple[str, Any], int] = {}
        keys: list[str] = []
        values: list[tuple[str, Any]] = []

        for feature in features:
            properties = feature.get("properties") or {}
            for raw_key, raw_value in properties.items():
                key = str(raw_key)
                if key not in key_indexes:
                    key_indexes[key] = len(keys)
                    keys.append(key)
                value_key = _value_key(raw_value)
                if value_key not in value_indexes:
                    value_indexes[value_key] = len(values)
                    values.append(value_key)

        layer_payload = bytearray()
        layer_payload.extend(_len_field(1, layer_name.encode("utf-8")))
        for feature in features:
            layer_payload.extend(
                _len_field(
                    2,
                    _feature_message(
                        feature,
                        key_indexes=key_indexes,
                        value_indexes=value_indexes,
                    ),
                )
            )
        for key in keys:
            layer_payload.extend(_len_field(3, key.encode("utf-8")))
        for _, raw_value in values:
            layer_payload.extend(_len_field(4, _value_message(raw_value)))
        layer_payload.extend(_varint_field(5, extent))
        layer_payload.extend(_varint_field(15, 2))
        tile_payload.extend(_len_field(3, bytes(layer_payload)))

    return bytes(tile_payload)


__all__ = ["encode"]
