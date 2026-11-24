"""Display-only service deserts, shared by the v3 and v4 publishers.

Inputs are explicit: this module never selects a build or reads pipeline state.
The legacy nearest-node / WALK_RADIUS_M rule is intentionally unchanged.
"""
from __future__ import annotations

from config import TO_WGS84, WALK_RADIUS_M
from shapely.ops import transform

from . import network
from .grid import materialize_cell_geometry


def cell_centre(cell):
    if "geometry_metric" in cell:
        point = cell["geometry_metric"].representative_point()
        lon, lat = TO_WGS84(point.x, point.y)
        return float(lat), float(lon)
    return cell["centre"]


def compute_service_desert_rows(
    graph, cells_by_resolution, reality_rows, *, hashes, reality_state,
    study_area_metric, created_at, cell_nodes_by_resolution=None,
):
    baseline = [row for row in reality_rows if str(row.get("source_status") or "gtfs_direct") == "gtfs_direct"]
    if not baseline:
        return []
    stop_points = {"transport": [(float(row["geom"].y), float(row["geom"].x)) for row in baseline]}
    stop_nodes = network.snap_amenities(graph, stop_points)
    nodes = network.nearest_nodes(graph, [row["geom"].x for row in baseline], [row["geom"].y for row in baseline])
    weights = {"public_departures_7d": [
        (int(node), int(row["public_departures_7d"]))
        for row, node in zip(baseline, nodes) if int(row["public_departures_7d"] or 0) > 0
    ]}
    if cell_nodes_by_resolution is None:
        cell_nodes_by_resolution = {}
        for resolution, cells in cells_by_resolution.items():
            centres = [cell_centre(cell) for cell in cells]
            cell_nodes_by_resolution[resolution] = network.nearest_nodes(
                graph, [lon for lat, lon in centres], [lat for lat, lon in centres],
            ) if centres else []
    origins = network.normalize_origin_node_ids(node for nodes in cell_nodes_by_resolution.values() for node in nodes)
    counts = network.precompute_walk_counts_by_origin_node(graph, stop_nodes, origins, cutoff=WALK_RADIUS_M, weight="length_m")
    totals = network.precompute_walk_weighted_totals_by_origin_node(graph, weights, origins, cutoff=WALK_RADIUS_M, weight="length_m")
    result = []
    for resolution, cells in cells_by_resolution.items():
        for cell, node in zip(cells, cell_nodes_by_resolution[resolution]):
            count = int(counts.get(int(node), {}).get("transport", 0))
            departures = int(totals.get(int(node), {}).get("public_departures_7d", 0))
            if count <= 0 or departures > 0:
                continue
            geometry = cell.get("geometry")
            if geometry is None:
                geometry = (transform(lambda x, y, z=None: TO_WGS84(x, y), cell["geometry_metric"])
                            if "geometry_metric" in cell else materialize_cell_geometry(cell, study_area_metric))
            result.append({
                "build_key": hashes.build_key, "import_fingerprint": hashes.import_fingerprint,
                "reality_fingerprint": reality_state.reality_fingerprint,
                "resolution_m": int(resolution), "cell_id": cell["cell_id"],
                "analysis_date": reality_state.analysis_date,
                "baseline_reachable_stop_count": count, "reachable_public_departures_7d": departures,
                "reason_codes_json": ["baseline_reachable_without_public_departures_7d"],
                "cell_geom": geometry, "created_at": created_at,
            })
    return result
