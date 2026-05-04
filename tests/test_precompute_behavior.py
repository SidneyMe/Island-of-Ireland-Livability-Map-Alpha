from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock

import numpy as np
from shapely.geometry import Point, Polygon, box

import config
import overture.merge as overture_merge
import precompute
import render_from_db
import study_area
from network.loader import WalkGraphIndex


class _FakeVertexSeq:
    def __init__(self, latitudes: list[float], longitudes: list[float]) -> None:
        self.indices = list(range(len(latitudes)))
        self._attrs = {
            "lat": list(latitudes),
            "lon": list(longitudes),
            "osmid": list(range(len(latitudes))),
        }

    def __getitem__(self, key: str):
        return self._attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self._attrs[key] = list(value)


class _FakeEdgeSeq:
    def __init__(self, length_m: list[float] | None = None) -> None:
        self._attrs = {"length_m": list(length_m or [1.0])}

    def __getitem__(self, key: str):
        return self._attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self._attrs[key] = list(value)


class _FakeGraph:
    def __init__(
        self,
        distance_lookup: dict[tuple[int, int], float],
        *,
        latitudes: list[float] | None = None,
        longitudes: list[float] | None = None,
        edge_lengths: list[float] | None = None,
    ) -> None:
        latitudes = latitudes or [53.0, 53.001, 53.002]
        longitudes = longitudes or [-6.0, -6.0, -6.0]
        self.vs = _FakeVertexSeq(latitudes, longitudes)
        self.es = _FakeEdgeSeq(edge_lengths)
        self._distance_lookup = {
            (int(src), int(dst)): float(distance)
            for (src, dst), distance in distance_lookup.items()
        }
        self._graph_attrs: dict[str, object] = {}

    def attributes(self) -> list[str]:
        return list(self._graph_attrs)

    def __getitem__(self, key: str):
        return self._graph_attrs[key]

    def __setitem__(self, key: str, value) -> None:
        self._graph_attrs[key] = value

    def vcount(self) -> int:
        return len(self.vs.indices)

    def ecount(self) -> int:
        return len(self.es["length_m"])

    def distances(self, source, target, weights=None, mode="out"):
        del weights, mode
        matrix = []
        for src in source:
            row = []
            for dst in target:
                row.append(self._distance_lookup.get((int(src), int(dst)), float("inf")))
            matrix.append(row)
        return matrix


def _tracker_mock() -> mock.Mock:
    tracker = mock.Mock()
    tracker.phase_callback.return_value = lambda *args, **kwargs: None
    return tracker


def _polygon_z(minx: float, miny: float, maxx: float, maxy: float, z: float = 7.0) -> Polygon:
    return Polygon(
        [
            (minx, miny, z),
            (maxx, miny, z),
            (maxx, maxy, z),
            (minx, maxy, z),
            (minx, miny, z),
        ]
    )


def _empty_amenity_data() -> dict[str, list[tuple[float, float]]]:
    return {category: [] for category in precompute.TAGS}


def _grid_cell(
    cell_id: str,
    *,
    geometry=None,
    centre: tuple[float, float] = (53.0, -6.0),
    metric_bounds: tuple[float, float, float, float] = (0.0, 0.0, 1.0, 1.0),
    include_geometry: bool = True,
    clip_required: bool = False,
    effective_area_m2: float | None = None,
    effective_area_ratio: float | None = None,
) -> dict[str, object]:
    raw_area_m2 = abs((metric_bounds[2] - metric_bounds[0]) * (metric_bounds[3] - metric_bounds[1]))
    if effective_area_m2 is None:
        effective_area_m2 = raw_area_m2
    if effective_area_ratio is None:
        effective_area_ratio = 0.0 if raw_area_m2 <= 0.0 else float(effective_area_m2) / raw_area_m2
    cell = {
        "cell_id": cell_id,
        "centre": centre,
        "metric_bounds": metric_bounds,
        "clip_required": clip_required,
        "effective_area_m2": float(effective_area_m2),
        "effective_area_ratio": float(effective_area_ratio),
        "counts": {},
        "cluster_counts": {},
        "effective_units": {},
        "scores": {},
        "total": 0.0,
    }
    if include_geometry:
        cell["geometry"] = geometry if geometry is not None else box(0.0, 0.0, 1.0, 1.0)
    return cell


def _clear_state_cache(cache_dir: Path) -> None:
    precompute._STATE.tier_valid.pop(cache_dir, None)
    precompute._STATE.tiers_building.discard(cache_dir)
    precompute._STATE.study_area_metric = None
    precompute._STATE.study_area_wgs84 = None


def _workflow_kwargs(**overrides):
    hashes = SimpleNamespace(
        build_profile="full",
        build_key="build-key-123",
        config_hash="config-hash-123",
        import_fingerprint="import-fingerprint-123",
    )
    source_state = SimpleNamespace(
        extract_path=Path("extract.osm.pbf"),
        extract_fingerprint="extract-fingerprint-123",
        import_fingerprint="import-fingerprint-123",
    )
    tracker = _tracker_mock()
    defaults = {
        "cache_dir": Path(".livability_cache"),
        "current_normalization_scope_hash": mock.Mock(return_value="norm-scope-123"),
        "build_engine": mock.Mock(return_value=mock.sentinel.engine),
        "ensure_database_ready": mock.Mock(),
        "resolve_source_state": mock.Mock(return_value=source_state),
        "activate_build_hashes": mock.Mock(),
        "print_cache_status": mock.Mock(),
        "validate_all_tiers": mock.Mock(),
        "phase_geometry": mock.Mock(return_value=(box(0.0, 0.0, 1.0, 1.0), box(0.0, 0.0, 1.0, 1.0))),
        "phase_amenities": mock.Mock(return_value=(_empty_amenity_data(), [])),
        "phase_grids": mock.Mock(return_value={1000: []}),
        "score_grid_fast_path_candidate": mock.Mock(return_value=False),
        "has_complete_build": mock.Mock(return_value=False),
        "import_payload_ready": mock.Mock(return_value=True),
        "ensure_local_osm_import": mock.Mock(),
        "tracker_factory": mock.Mock(return_value=tracker),
        "walk_rows": mock.Mock(return_value=[]),
        "amenity_rows": mock.Mock(return_value=[]),
        "transport_reality_rows": mock.Mock(return_value=[]),
        "service_desert_rows": mock.Mock(return_value=[]),
        "noise_rows": mock.Mock(return_value=[]),
        "compute_service_deserts": mock.Mock(),
        "publish_precomputed_artifacts": mock.Mock(),
        "summary_json": mock.Mock(return_value={"summary": True}),
        "package_snapshot": mock.Mock(return_value={"package": "1.0"}),
        "python_version": mock.Mock(return_value="3.12.0"),
        "get_hashes": mock.Mock(side_effect=lambda: hashes),
        "set_source_state": mock.Mock(),
    }
    defaults.update(overrides)
    return defaults


class OvertureAmenityMergeTests(TestCase):
    def _osm_row(
        self,
        *,
        category: str = "shops",
        lat: float = 53.35,
        lon: float = -6.26,
        source_ref: str = "node/1",
        name: str | None = "Corner Shop",
        tags_json: dict[str, object] | None = None,
        park_area_m2: float = 0.0,
        footprint_area_m2: float = 0.0,
    ) -> dict[str, object]:
        return {
            "category": category,
            "source": "osm_local_pbf",
            "source_ref": source_ref,
            "name": name,
            "tags_json": tags_json if tags_json is not None else {"name": name},
            "geom": Point(lon, lat),
            "park_area_m2": park_area_m2,
            "footprint_area_m2": footprint_area_m2,
        }

    def _overture_row(
        self,
        *,
        category: str = "shops",
        lat: float = 53.35005,
        lon: float = -6.26005,
        source_ref: str = "ovt-1",
        name: str | None = "Corner Shop",
        brand: str | None = None,
        raw_primary_category: str | None = None,
    ) -> dict[str, object]:
        return {
            "category": category,
            "source": "overture_places",
            "source_ref": source_ref,
            "name": name,
            "brand": brand,
            "raw_primary_category": raw_primary_category,
            "geom": Point(lon, lat),
            "park_area_m2": 0.0,
            "footprint_area_m2": 0.0,
        }

    def _candidate_pair(
        self,
        *,
        osm_row_id: int,
        overture_row_id: int,
        same_category: bool,
        aliases_agree: bool,
        distance_m: float,
    ) -> dict[str, object]:
        return {
            "osm_row_id": osm_row_id,
            "overture_row_id": overture_row_id,
            "same_category": same_category,
            "aliases_agree": aliases_agree,
            "distance_m": distance_m,
        }

    def test_same_category_nearby_rows_merge_into_source_agreement(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [self._osm_row()],
            [self._overture_row()],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["category"], "shops")
        self.assertEqual(merged[0]["source"], "osm_local_pbf")
        self.assertEqual(merged[0]["source_ref"], "node/1")
        self.assertEqual(merged[0]["conflict_class"], "source_agreement")

    def test_overture_only_rows_survive_merge(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [],
            [self._overture_row(source_ref="ovt-only", name="Late Addition")],
        )

        self.assertEqual(
            merged,
            [
                {
                    "category": "shops",
                    "lat": 53.35005,
                    "lon": -6.26005,
                    "source": "overture_places",
                    "source_ref": "ovt-only",
                    "name": "Late Addition",
                    "conflict_class": "overture_only",
                    "park_area_m2": 0.0,
                }
            ],
        )

    def test_category_conflicts_keep_osm_category_and_flag_conflict(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [self._osm_row(category="healthcare", source_ref="node/9", name="Town Pharmacy")],
            [self._overture_row(category="shops", source_ref="ovt-9", name="Town Pharmacy")],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["category"], "healthcare")
        self.assertEqual(merged[0]["source"], "osm_local_pbf")
        self.assertEqual(merged[0]["source_ref"], "node/9")
        self.assertEqual(merged[0]["conflict_class"], "source_conflict")

    def test_same_name_but_far_apart_rows_do_not_merge(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [self._osm_row(lat=53.35, lon=-6.26)],
            [self._overture_row(lat=53.36, lon=-6.26)],
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            sorted(row["conflict_class"] for row in merged),
            ["osm_only", "overture_only"],
        )

    def test_osm_park_area_survives_merge_with_overture_point(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [self._osm_row(category="parks", source_ref="way/7", name="Big Park", park_area_m2=125_000.0)],
            [self._overture_row(category="parks", source_ref="ovt-park", name="Big Park")],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["source_ref"], "way/7")
        self.assertEqual(merged[0]["park_area_m2"], 125_000.0)
        self.assertEqual(merged[0]["conflict_class"], "source_agreement")

    def test_alias_only_match_within_name_radius_merges(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(
                    source_ref="node/5",
                    name=None,
                    tags_json={"brand": "Corner Shop"},
                )
            ],
            [
                self._overture_row(
                    source_ref="ovt-5",
                    lat=53.3503,
                    lon=-6.2603,
                    name=None,
                    brand="Corner Shop",
                )
            ],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["source_ref"], "node/5")
        self.assertEqual(merged[0]["conflict_class"], "source_agreement")

    def test_missing_osm_name_uses_overture_name_when_matched(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [self._osm_row(source_ref="node/6", name=None, tags_json={})],
            [self._overture_row(source_ref="ovt-6", name="Rescue Name")],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["name"], "Rescue Name")

    def test_duplicate_osm_rows_with_same_alias_within_strict_radius_collapse_to_one(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(
                    source_ref="node/1",
                    name="ASICS",
                    tags_json={"name": "ASICS", "brand": "ASICS", "shop": "shoes"},
                ),
                self._osm_row(
                    source_ref="node/2",
                    name="ASICS",
                    lat=53.35005,
                    lon=-6.25995,
                    tags_json={"name": "ASICS", "brand": "ASICS", "shop": "shoes"},
                ),
            ],
            [],
        )

        self.assertEqual(
            merged,
            [
                {
                    "category": "shops",
                    "lat": 53.35,
                    "lon": -6.26,
                    "source": "osm_local_pbf",
                    "source_ref": "node/1",
                    "name": "ASICS",
                    "conflict_class": "osm_only",
                    "park_area_m2": 0.0,
                }
            ],
        )

    def test_same_name_rows_beyond_strict_osm_dedupe_radius_do_not_collapse(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(
                    source_ref="node/1",
                    name="ASICS",
                    tags_json={"name": "ASICS", "brand": "ASICS", "shop": "shoes"},
                ),
                self._osm_row(
                    source_ref="node/2",
                    name="ASICS",
                    lat=53.3502,
                    lon=-6.26,
                    tags_json={"name": "ASICS", "brand": "ASICS", "shop": "shoes"},
                ),
            ],
            [],
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            [row["source_ref"] for row in merged],
            ["node/1", "node/2"],
        )

    def test_nearby_different_named_osm_rows_do_not_collapse(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(
                    source_ref="node/1",
                    name="ASICS",
                    tags_json={"name": "ASICS", "brand": "ASICS", "shop": "shoes"},
                ),
                self._osm_row(
                    source_ref="node/2",
                    name="Nike",
                    lat=53.35005,
                    lon=-6.25995,
                    tags_json={"name": "Nike", "brand": "Nike", "shop": "shoes"},
                ),
            ],
            [],
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            sorted(row["name"] for row in merged),
            ["ASICS", "Nike"],
        )

    def test_unnamed_nearby_osm_rows_do_not_collapse_by_proximity_only(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(source_ref="node/1", name=None, tags_json={"shop": "clothes"}),
                self._osm_row(
                    source_ref="node/2",
                    name=None,
                    lat=53.35005,
                    lon=-6.25995,
                    tags_json={"shop": "clothes"},
                ),
            ],
            [],
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            [row["source_ref"] for row in merged],
            ["node/1", "node/2"],
        )

    def test_resolve_merge_categories_defaults_to_expected_baseline(self) -> None:
        self.assertEqual(
            overture_merge.resolve_merge_categories(),
            tuple(sorted(overture_merge.EXPECTED_BASELINE_MERGE_CATEGORIES)),
        )

    def test_candidate_pair_collapse_uses_or_or_min(self) -> None:
        collapsed = overture_merge._collapse_candidate_pairs(
            [
                self._candidate_pair(
                    osm_row_id=1,
                    overture_row_id=9,
                    same_category=True,
                    aliases_agree=False,
                    distance_m=12.0,
                ),
                self._candidate_pair(
                    osm_row_id=1,
                    overture_row_id=9,
                    same_category=True,
                    aliases_agree=True,
                    distance_m=22.0,
                ),
                self._candidate_pair(
                    osm_row_id=1,
                    overture_row_id=9,
                    same_category=False,
                    aliases_agree=False,
                    distance_m=18.0,
                ),
            ]
        )

        self.assertEqual(
            collapsed,
            [
                {
                    "osm_row_id": 1,
                    "overture_row_id": 9,
                    "same_category": True,
                    "aliases_agree": True,
                    "distance_m": 12.0,
                }
            ],
        )

    def test_same_category_candidates_are_tried_before_cross_category_candidates(self) -> None:
        osm_rows = [self._osm_row(category="shops", source_ref="node/7", name="Dual Match")]
        overture_rows = [
            self._overture_row(category="healthcare", source_ref="ovt-cross", name="Dual Match"),
            self._overture_row(category="shops", source_ref="ovt-same", name="Dual Match"),
        ]
        merged = overture_merge.merge_source_amenity_rows_from_candidate_pairs(
            osm_rows,
            overture_rows,
            [
                self._candidate_pair(
                    osm_row_id=1,
                    overture_row_id=1,
                    same_category=False,
                    aliases_agree=True,
                    distance_m=2.0,
                ),
                self._candidate_pair(
                    osm_row_id=1,
                    overture_row_id=2,
                    same_category=True,
                    aliases_agree=True,
                    distance_m=20.0,
                ),
            ],
            scoring_categories=["shops", "healthcare", "parks"],
        )

        self.assertEqual(len(merged), 2)
        osm_match = next(row for row in merged if row["source"] == "osm_local_pbf")
        overture_only = next(row for row in merged if row["source"] == "overture_places")
        self.assertEqual(osm_match["source_ref"], "node/7")
        self.assertEqual(osm_match["conflict_class"], "source_agreement")
        self.assertEqual(overture_only["source_ref"], "ovt-cross")

    def test_three_osm_rows_competing_for_one_overture_row_claim_only_once(self) -> None:
        osm_rows = [
            self._osm_row(source_ref="node/1", name="Shared Place"),
            self._osm_row(source_ref="node/2", name="Shared Place", lat=53.35012),
            self._osm_row(source_ref="node/3", name="Shared Place", lat=53.35024),
        ]
        overture_rows = [self._overture_row(source_ref="ovt-shared", name="Shared Place")]

        merged = overture_merge.merge_source_amenity_rows(osm_rows, overture_rows)

        self.assertEqual(
            [row["conflict_class"] for row in merged if row["source"] == "osm_local_pbf"],
            ["source_agreement", "osm_only", "osm_only"],
        )
        self.assertEqual(
            len([row for row in merged if row["source"] == "overture_places"]),
            0,
        )

    def test_named_osm_row_claims_overture_before_weaker_nearby_osm_row(self) -> None:
        merged = overture_merge.merge_source_amenity_rows(
            [
                self._osm_row(
                    source_ref="node/weak",
                    name=None,
                    tags_json={"shop": "clothes"},
                    lat=53.35002,
                    lon=-6.26002,
                ),
                self._osm_row(
                    source_ref="node/strong",
                    name="Penneys",
                    tags_json={"name": "Penneys", "brand": "Primark", "shop": "clothes"},
                    lat=53.35,
                    lon=-6.26,
                ),
            ],
            [
                self._overture_row(
                    source_ref="ovt-penneys",
                    name="Penneys",
                    brand="Primark",
                    lat=53.35003,
                    lon=-6.26003,
                )
            ],
        )

        self.assertEqual(len(merged), 2)
        strong_row = next(row for row in merged if row["source_ref"] == "node/strong")
        weak_row = next(row for row in merged if row["source_ref"] == "node/weak")
        self.assertEqual(strong_row["conflict_class"], "source_agreement")
        self.assertEqual(weak_row["conflict_class"], "osm_only")

    def test_one_osm_row_picks_best_overture_candidate_deterministically(self) -> None:
        osm_rows = [self._osm_row(source_ref="node/10", name="Match Me")]
        overture_rows = [
            self._overture_row(source_ref="ovt-b", name="Match Me", lat=53.3502, lon=-6.2602),
            self._overture_row(source_ref="ovt-a", name="Match Me", lat=53.3501, lon=-6.2601),
            self._overture_row(source_ref="ovt-c", name="Match Me", lat=53.3503, lon=-6.2603),
        ]

        merged = overture_merge.merge_source_amenity_rows(osm_rows, overture_rows)

        self.assertEqual(len(merged), 3)
        self.assertEqual(merged[0]["source_ref"], "node/10")
        self.assertEqual(merged[0]["conflict_class"], "source_agreement")
        self.assertEqual(
            sorted(row["source_ref"] for row in merged[1:]),
            ["ovt-b", "ovt-c"],
        )

    def test_merge_is_deterministic_when_inputs_are_shuffled(self) -> None:
        osm_rows = [
            self._osm_row(source_ref="node/1", name="A"),
            self._osm_row(source_ref="node/2", name="B"),
            self._osm_row(category="healthcare", source_ref="node/3", name="Clinic"),
        ]
        overture_rows = [
            self._overture_row(source_ref="ovt-1", name="A"),
            self._overture_row(source_ref="ovt-2", name="B"),
            self._overture_row(category="healthcare", source_ref="ovt-3", name="Clinic"),
        ]

        merged = overture_merge.merge_source_amenity_rows(osm_rows, overture_rows)
        shuffled_merged = overture_merge.merge_source_amenity_rows(
            list(reversed(osm_rows)),
            [overture_rows[1], overture_rows[2], overture_rows[0]],
        )

        self.assertEqual(merged, shuffled_merged)


class AmenityPhaseIntegrationTests(TestCase):
    def test_phase_amenities_uses_merged_rows_for_counts_and_cache(self) -> None:
        tracker = _tracker_mock()
        source_rows = [
            {
                "category": "shops",
                "source": "osm_local_pbf",
                "source_ref": "node/1",
                "name": "Corner Shop",
                "tags_json": {"name": "Corner Shop"},
                "geom": Point(-6.26, 53.35),
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            },
            {
                "category": "transport",
                "source": "gtfs_direct",
                "source_ref": "gtfs/nta/S1",
                "name": None,
                "conflict_class": "gtfs_direct",
                "geom": Point(-6.25, 53.35),
                "park_area_m2": 0.0,
            },
        ]
        merged_rows = [
            {
                "category": "shops",
                "lat": 53.35,
                "lon": -6.26,
                "source": "osm_local_pbf",
                "source_ref": "node/1",
                "name": "Corner Shop",
                "conflict_class": "source_agreement",
                "park_area_m2": 0.0,
            },
            {
                "category": "parks",
                "lat": 53.351,
                "lon": -6.251,
                "source": "overture_places",
                "source_ref": "ovt-park-1",
                "name": "Pocket Park",
                "conflict_class": "overture_only",
                "park_area_m2": 0.0,
            },
        ]

        cache: dict[str, object] = {}
        amenity_data, amenity_source_rows = precompute._phases.phase_amenities_impl(
            mock.sentinel.engine,
            box(-6.4, 53.2, -6.1, 53.5),
            tracker,
            tags=list(precompute.TAGS),
            cache_dir=Path("reach-cache"),
            reach_hash="reach-hash-123",
            import_fingerprint="import-fingerprint-123",
            cache_load=lambda key, cache_dir: cache.get(key),
            cache_save=lambda key, data, cache_dir: cache.__setitem__(key, data),
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            can_finalize_reach_tier=lambda amenity_data: True,
            load_source_amenity_rows=mock.Mock(return_value=source_rows),
            load_overture_amenity_rows=mock.Mock(return_value=[{"category": "parks"}]),
            merge_source_amenity_rows=mock.Mock(return_value=merged_rows),
            transit_reality_fingerprint="reality-123",
        )

        self.assertEqual(amenity_data["shops"], [(53.35, -6.26)])
        self.assertEqual(amenity_data["parks"], [(53.351, -6.251)])
        self.assertEqual(amenity_data["transport"], [(53.35, -6.25)])
        self.assertEqual(
            amenity_source_rows,
            [
                {
                    "category": "parks",
                    "lat": 53.351,
                    "lon": -6.251,
                    "source": "overture_places",
                    "source_ref": "ovt-park-1",
                    "name": "Pocket Park",
                    "conflict_class": "overture_only",
                    "park_area_m2": 0.0,
                    "footprint_area_m2": 0.0,
                    "tier": "neighbourhood",
                    "score_units": 2,
                },
                {
                    "category": "shops",
                    "lat": 53.35,
                    "lon": -6.26,
                    "source": "osm_local_pbf",
                    "source_ref": "node/1",
                    "name": "Corner Shop",
                    "conflict_class": "source_agreement",
                    "park_area_m2": 0.0,
                    "footprint_area_m2": 0.0,
                    "tier": "regular",
                    "score_units": 2,
                },
                {
                    "category": "transport",
                    "lat": 53.35,
                    "lon": -6.25,
                    "source": "gtfs_direct",
                    "source_ref": "gtfs/nta/S1",
                    "name": None,
                    "conflict_class": "gtfs_direct",
                    "park_area_m2": 0.0,
                    "footprint_area_m2": 0.0,
                    "tier": "stop",
                    "score_units": 1,
                },
            ],
        )
        self.assertEqual(cache["amenities"], amenity_source_rows)

    def test_phase_amenities_prefers_db_backed_merge_helper_when_available(self) -> None:
        tracker = _tracker_mock()
        source_rows = [
            {
                "category": "shops",
                "source": "osm_local_pbf",
                "source_ref": "node/1",
                "name": "Corner Shop",
                "tags_json": {"name": "Corner Shop"},
                "geom": Point(-6.26, 53.35),
                "park_area_m2": 0.0,
                "footprint_area_m2": 0.0,
            }
        ]
        merged_rows = [
            {
                "category": "shops",
                "lat": 53.35,
                "lon": -6.26,
                "source": "osm_local_pbf",
                "source_ref": "node/1",
                "name": "Corner Shop",
                "conflict_class": "source_agreement",
                "park_area_m2": 0.0,
            }
        ]
        merge_stats = {
            "candidate_pair_count": 1,
            "same_category_candidate_count": 1,
            "cross_category_candidate_count": 0,
            "excluded_non_operational_osm_rows": 0,
            "osm_duplicate_rows_removed": 0,
            "osm_duplicates_by_category": {},
            "candidate_pairs_by_osm_category": {"shops": 1},
            "stage_ms": {
                "filter_osm_rows": 0.0,
                "generate_osm_self_candidates": 1.0,
                "collapse_osm_duplicates": 1.0,
                "generate_overture_candidates": 1.0,
                "greedy_assignment": 1.0,
            },
        }

        db_merge_mock = mock.Mock(return_value=(merged_rows, merge_stats))
        python_merge_mock = mock.Mock(side_effect=AssertionError("DB helper should be preferred"))

        amenity_data, amenity_source_rows = precompute._phases.phase_amenities_impl(
            mock.sentinel.engine,
            box(-6.4, 53.2, -6.1, 53.5),
            tracker,
            tags=list(precompute.TAGS),
            cache_dir=Path("reach-cache"),
            reach_hash="reach-hash-123",
            import_fingerprint="import-fingerprint-123",
            cache_load=lambda key, cache_dir: None,
            cache_save=lambda key, data, cache_dir: None,
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            can_finalize_reach_tier=lambda amenity_data: True,
            load_source_amenity_rows=mock.Mock(return_value=source_rows),
            load_overture_amenity_rows=mock.Mock(return_value=[]),
            merge_source_amenity_rows=python_merge_mock,
            load_merged_source_amenity_rows=db_merge_mock,
            transit_reality_fingerprint="reality-123",
        )

        self.assertEqual(amenity_data["shops"], [(53.35, -6.26)])
        self.assertEqual(amenity_source_rows[0]["conflict_class"], "source_agreement")
        db_merge_mock.assert_called_once()

    def test_iter_amenity_rows_preserves_public_metadata_fields(self) -> None:
        hashes = SimpleNamespace(
            build_key="build-key-123",
            config_hash="config-hash-123",
            import_fingerprint="import-fingerprint-123",
        )

        rows = list(
            precompute._publish.iter_amenity_rows_impl(
                [
                    {
                        "category": "shops",
                        "lat": 53.35,
                        "lon": -6.26,
                        "source": "overture_places",
                        "source_ref": "ovt-1",
                        "name": "Late Addition",
                        "tier": "regular",
                        "conflict_class": "overture_only",
                    }
                ],
                datetime(2026, 4, 19, tzinfo=timezone.utc),
                hashes=hashes,
            )
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "overture_places")
        self.assertEqual(rows[0]["name"], "Late Addition")
        self.assertEqual(rows[0]["tier"], "regular")
        self.assertEqual(rows[0]["conflict_class"], "overture_only")

    def test_summary_json_includes_overture_dataset_traceability(self) -> None:
        summary = precompute._publish.summary_json_impl(
            box(-10.0, 50.0, -5.0, 55.0),
            {20_000: [_grid_cell("coarse-cell")]},
            _empty_amenity_data(),
            [
                {"category": "shops", "tier": "corner"},
                {"category": "shops", "tier": "regular"},
                {"category": "transport", "tier": "stop"},
                {"category": "parks", "tier": "district"},
            ],
            hashes=SimpleNamespace(
                build_key="build-key-dev",
                config_hash="config-hash-dev",
                import_fingerprint="import-fingerprint-dev",
            ),
            build_profile="dev",
            source_state=SimpleNamespace(extract_path=Path("extract.osm.pbf")),
            osm_extract_path=Path("extract.osm.pbf"),
            grid_sizes_m=[20_000, 10_000, 5_000],
            fine_resolutions_m=[],
            output_html="index.html",
            zoom_breaks=[(10, 5_000), (8, 10_000), (0, 20_000)],
            overture_dataset={"last_release": "2026-04-15.0", "file_size": 12345},
        )

        self.assertEqual(
            summary["overture_dataset"],
            {"last_release": "2026-04-15.0", "file_size": 12345},
        )
        self.assertEqual(
            summary["amenity_tier_counts"],
            {
                "shops": {"corner": 1, "regular": 1},
                "transport": {"stop": 1},
                "healthcare": {},
                "parks": {"district": 1},
            },
        )

    def test_summary_json_includes_transport_subtier_and_flag_counts(self) -> None:
        summary = precompute._publish.summary_json_impl(
            box(-10.0, 50.0, -5.0, 55.0),
            {20_000: [_grid_cell("coarse-cell")]},
            _empty_amenity_data(),
            [],
            transport_reality_rows=[
                {
                    "bus_service_subtier": "mon_sun",
                    "bus_frequency_tier": "frequent",
                    "is_unscheduled_stop": False,
                    "has_exception_only_service": False,
                    "has_any_bus_service": True,
                    "has_daily_bus_service": True,
                    "route_modes_json": ["bus"],
                },
                {
                    "bus_service_subtier": "weekdays_only",
                    "bus_frequency_tier": "moderate",
                    "is_unscheduled_stop": False,
                    "has_exception_only_service": True,
                    "has_any_bus_service": True,
                    "has_daily_bus_service": False,
                    "route_modes_json": ["bus", "rail", "tram", "tram"],
                },
                {
                    "bus_service_subtier": None,
                    "bus_frequency_tier": None,
                    "is_unscheduled_stop": True,
                    "has_exception_only_service": False,
                    "has_any_bus_service": False,
                    "has_daily_bus_service": False,
                    "route_modes_json": [],
                },
            ],
            hashes=SimpleNamespace(
                build_key="build-key-dev",
                config_hash="config-hash-dev",
                import_fingerprint="import-fingerprint-dev",
            ),
            build_profile="dev",
            source_state=SimpleNamespace(extract_path=Path("extract.osm.pbf")),
            osm_extract_path=Path("extract.osm.pbf"),
            grid_sizes_m=[20_000, 10_000, 5_000],
            fine_resolutions_m=[],
            output_html="index.html",
            zoom_breaks=[(10, 5_000), (8, 10_000), (0, 20_000)],
            transit_reality_state=SimpleNamespace(
                analysis_date=date(2026, 4, 14),
                reality_fingerprint="reality-123",
            ),
            transit_analysis_window_days=30,
            transit_service_desert_window_days=7,
            transport_reality_download_url="/exports/transport-reality.zip",
            service_deserts_enabled=True,
        )

        self.assertEqual(
            summary["transport_subtier_counts"],
            {"mon_sun": 1, "weekdays_only": 1},
        )
        self.assertEqual(
            summary["transport_bus_frequency_counts"],
            {"frequent": 1, "moderate": 1},
        )
        self.assertEqual(
            summary["transport_flag_counts"],
            {
                "is_unscheduled_stop": 1,
                "has_exception_only_service": 1,
                "has_any_bus_service": 2,
                "has_daily_bus_service": 1,
            },
        )
        self.assertEqual(
            summary["transport_mode_counts"],
            {"bus": 2, "rail": 1, "tram": 1},
        )

    def test_summary_json_includes_noise_counts(self) -> None:
        summary = precompute._publish.summary_json_impl(
            box(-10.0, 50.0, -5.0, 55.0),
            {20_000: [_grid_cell("coarse-cell")]},
            _empty_amenity_data(),
            [],
            noise_rows=[
                {
                    "jurisdiction": "roi",
                    "source_type": "road",
                    "metric": "Lden",
                    "db_value": "55-59",
                },
                {
                    "jurisdiction": "roi",
                    "source_type": "rail",
                    "metric": "Lnight",
                    "db_value": "50-54",
                },
                {
                    "jurisdiction": "ni",
                    "source_type": "road",
                    "metric": "Lden",
                    "db_value": "55-59",
                },
            ],
            hashes=SimpleNamespace(
                build_key="build-key-dev",
                config_hash="config-hash-dev",
                import_fingerprint="import-fingerprint-dev",
            ),
            build_profile="dev",
            source_state=SimpleNamespace(extract_path=Path("extract.osm.pbf")),
            osm_extract_path=Path("extract.osm.pbf"),
            grid_sizes_m=[20_000],
            fine_resolutions_m=[],
            output_html="index.html",
            zoom_breaks=[(0, 20_000)],
        )

        self.assertTrue(summary["noise_enabled"])
        self.assertEqual(summary["noise_counts"], {"roi": 2, "ni": 1})
        self.assertEqual(summary["noise_source_counts"], {"road": 2, "rail": 1})
        self.assertEqual(summary["noise_metric_counts"], {"Lden": 2, "Lnight": 1})
        self.assertEqual(summary["noise_band_counts"], {"55-59": 2, "50-54": 1})


class PrecomputeReachabilityTests(TestCase):
    def test_snap_amenities_uses_compact_vertex_ids(self) -> None:
        graph = _FakeGraph({})
        amenity_data = {
            "shops": [(53.0, -6.0), (53.0, -6.0)],
            "transport": [],
            "healthcare": [],
            "parks": [(53.002, -6.0)],
        }

        nodes_by_category = precompute.snap_amenities(graph, amenity_data)

        self.assertEqual(nodes_by_category["shops"], [0, 0])
        self.assertEqual(nodes_by_category["parks"], [2])

    def test_normalize_origin_node_ids_returns_same_list_for_pre_normalized_inputs(self) -> None:
        origin_nodes = [1, 3, 5]

        normalized = precompute.normalize_origin_node_ids(origin_nodes)

        self.assertIs(normalized, origin_nodes)

    def test_normalize_origin_node_ids_sorts_and_deduplicates_unsorted_iterable(self) -> None:
        normalized = precompute.normalize_origin_node_ids([5, None, 3, 5, 1, 3, None])

        self.assertEqual(normalized, [1, 3, 5])

    def test_normalize_origin_node_ids_handles_single_pass_iterable(self) -> None:
        normalized = precompute.normalize_origin_node_ids(
            value for value in [4, None, 2, 4, 3]
        )

        self.assertEqual(normalized, [2, 3, 4])

    def test_merge_normalized_origin_node_ids_unions_sorted_unique_lists(self) -> None:
        merged = precompute.merge_normalized_origin_node_ids(
            [1, 3, 5],
            [1, 2, 5, 8],
            [],
            [2, 9],
        )

        self.assertEqual(merged, [1, 2, 3, 5, 8, 9])

    def test_precompute_walk_counts_by_origin_node_counts_duplicate_amenities(self) -> None:
        graph = _FakeGraph(
            {
                (0, 0): 0.0,
                (0, 2): 2.0,
                (1, 0): 1.0,
                (1, 2): 1.0,
                (2, 0): 2.0,
                (2, 2): 0.0,
            }
        )
        nodes_by_category = {
            "shops": [0, 0],
            "transport": [],
            "healthcare": [],
            "parks": [2],
        }

        with mock.patch.object(precompute._network, "ig", object()):
            counts_by_node = precompute.precompute_walk_counts_by_origin_node(
                graph,
                nodes_by_category,
                [0, 1, 2],
                cutoff=1.0,
            )

        self.assertEqual(counts_by_node[0], {"shops": 2})
        self.assertEqual(counts_by_node[1], {"shops": 2, "parks": 1})
        self.assertEqual(counts_by_node[2], {"parks": 1})

    def test_precompute_walk_weighted_totals_by_origin_node_accumulates_integer_weights(self) -> None:
        graph = _FakeGraph(
            {
                (0, 2): 1.0,
                (1, 2): 0.0,
            }
        )
        park_weights = {"parks": [(2, 50_000), (2, 25_000)]}

        with mock.patch.object(precompute._network, "ig", object()):
            totals_by_node = precompute.precompute_walk_weighted_totals_by_origin_node(
                graph,
                park_weights,
                [0, 1],
                cutoff=1.0,
            )

        self.assertEqual(totals_by_node[0], {"parks": 75_000})
        self.assertEqual(totals_by_node[1], {"parks": 75_000})

    def test_build_amenity_clusters_collapse_same_category_points_and_pick_stable_representative(self) -> None:
        amenity_source_rows = [
            {
                "category": "shops",
                "lat": 53.34980,
                "lon": -6.26030,
                "source": "beta",
                "source_ref": "2",
                "name": "Beta",
                "score_units": 3,
            },
            {
                "category": "shops",
                "lat": 53.34986,
                "lon": -6.26026,
                "source": "alpha",
                "source_ref": "9",
                "name": "Alpha",
                "score_units": 3,
            },
            {
                "category": "shops",
                "lat": 53.34982,
                "lon": -6.26028,
                "source": "gamma",
                "source_ref": "1",
                "name": "",
                "score_units": 3,
            },
            {
                "category": "shops",
                "lat": 53.35100,
                "lon": -6.26030,
                "source": "delta",
                "source_ref": "4",
                "name": "Delta",
                "score_units": 2,
            },
        ]

        cluster_data, cluster_rows = precompute._amenity_clusters.build_amenity_clusters(
            amenity_source_rows,
            categories=["shops"],
        )

        self.assertEqual(len(cluster_data["shops"]), 2)
        self.assertEqual(len(cluster_rows), 2)
        self.assertEqual(cluster_rows[0]["source"], "alpha")
        self.assertEqual(cluster_rows[0]["source_ref"], "9")
        self.assertEqual(cluster_rows[0]["cluster_size"], 3)
        self.assertEqual(cluster_rows[0]["base_units"], 3)

    def test_precompute_walk_decayed_units_by_origin_node_applies_half_distance_formula(self) -> None:
        graph = _FakeGraph(
            {
                (0, 1): 150.0,
                (0, 2): 250.0,
            }
        )
        node_weights_by_category = {
            "shops": [(1, 2)],
            "transport": [(2, 1)],
        }

        with mock.patch.object(precompute._network, "ig", object()):
            decayed_units = precompute.precompute_walk_decayed_units_by_origin_node(
                graph,
                node_weights_by_category,
                [0],
                cutoff=500.0,
                half_distance_m_by_category={
                    "shops": 150.0,
                    "transport": 250.0,
                },
            )

        self.assertAlmostEqual(decayed_units[0]["shops"], 1.0, places=5)
        self.assertAlmostEqual(decayed_units[0]["transport"], 0.5, places=5)

    def test_score_cells_preserve_raw_counts_while_scoring_from_cluster_effective_units(self) -> None:
        cell = _grid_cell("clustered-shop-cell", effective_area_ratio=1.0)

        precompute.score_cells(
            [cell],
            {1: {"shops": 3}},
            {1: {"shops": 1}},
            [1],
            effective_units_by_node={1: {"shops": 1}},
        )

        self.assertEqual(cell["counts"], {"shops": 3})
        self.assertEqual(cell["cluster_counts"], {"shops": 1})
        self.assertEqual(cell["effective_units"], {"shops": 1.0})
        self.assertAlmostEqual(cell["scores"]["shops"], (1.0 / 6.0) * 25.0)

    def test_score_cells_missing_nodes_default_to_zero_counts(self) -> None:
        cells = [{"cell_id": "cell-1"}]

        precompute.score_cells(cells, {}, {}, ["missing-node"])

        self.assertEqual(cells[0]["counts"], {})
        self.assertEqual(cells[0]["cluster_counts"], {})
        self.assertEqual(cells[0]["effective_units"], {})
        self.assertEqual(cells[0]["scores"], {category: 0.0 for category in precompute.CAPS})
        self.assertEqual(cells[0]["total"], 0.0)

    def test_score_cells_preserve_inland_behavior_when_effective_area_is_full(self) -> None:
        cell = _grid_cell(
            "inland",
            effective_area_m2=1.0,
            effective_area_ratio=1.0,
        )

        precompute.score_cells(
            [cell],
            {1: {"shops": 1}},
            {1: {"shops": 1}},
            [1],
            effective_units_by_node={1: {"shops": 2}},
        )

        self.assertEqual(cell["counts"], {"shops": 1})
        self.assertEqual(cell["cluster_counts"], {"shops": 1})
        self.assertEqual(cell["effective_units"], {"shops": 2.0})
        self.assertAlmostEqual(cell["scores"]["shops"], (2.0 / 6.0) * 25.0)
        self.assertEqual(cell["scores"]["parks"], 0.0)
        self.assertAlmostEqual(cell["total"], (2.0 / 6.0) * 25.0)

    def test_score_cells_use_effective_park_units_for_inland_park_scoring(self) -> None:
        cell = _grid_cell(
            "inland-park",
            effective_area_m2=1.0,
            effective_area_ratio=1.0,
        )

        precompute.score_cells(
            [cell],
            {1: {"parks": 1}},
            {1: {"parks": 1}},
            [1],
            effective_units_by_node={1: {"parks": 4}},
        )

        self.assertEqual(cell["counts"], {"parks": 1})
        self.assertEqual(cell["cluster_counts"], {"parks": 1})
        self.assertEqual(cell["effective_units"], {"parks": 4.0})
        self.assertEqual(cell["scores"]["parks"], 20.0)
        self.assertEqual(cell["total"], 20.0)

    def test_score_cells_normalize_non_park_categories_by_effective_area(self) -> None:
        inland_cell = _grid_cell(
            "inland",
            effective_area_m2=1.0,
            effective_area_ratio=1.0,
        )
        coastal_cell = _grid_cell(
            "coastal",
            effective_area_m2=0.25,
            effective_area_ratio=0.25,
        )

        precompute.score_cells(
            [inland_cell, coastal_cell],
            {1: {"shops": 1}},
            {1: {"shops": 1}},
            [1, 1],
            effective_units_by_node={1: {"shops": 2}},
        )

        self.assertEqual(inland_cell["counts"], coastal_cell["counts"])
        self.assertEqual(inland_cell["counts"], {"shops": 1})
        self.assertGreater(coastal_cell["scores"]["shops"], inland_cell["scores"]["shops"])
        self.assertGreater(coastal_cell["total"], inland_cell["total"])

    def test_score_cells_clamp_density_normalization_floor_for_tiny_land_slivers(self) -> None:
        coastal_cell = _grid_cell(
            "coastal",
            effective_area_m2=0.01,
            effective_area_ratio=0.01,
        )

        precompute.score_cells(
            [coastal_cell],
            {1: {"healthcare": 2}},
            {1: {"healthcare": 2}},
            [1],
            effective_units_by_node={1: {"healthcare": 5}},
        )

        self.assertEqual(coastal_cell["counts"], {"healthcare": 2})
        self.assertEqual(coastal_cell["scores"]["healthcare"], 25.0)

    def test_score_cells_normalize_effective_park_units_by_effective_area(self) -> None:
        inland_cell = _grid_cell(
            "inland-park",
            effective_area_ratio=1.0,
        )
        coastal_cell = _grid_cell(
            "coastal-park",
            effective_area_ratio=0.5,
        )

        precompute.score_cells(
            [inland_cell, coastal_cell],
            {7: {"parks": 1}},
            {7: {"parks": 1}},
            [7, 7],
            effective_units_by_node={7: {"parks": 2}},
        )

        self.assertEqual(inland_cell["counts"], coastal_cell["counts"])
        self.assertEqual(inland_cell["counts"], {"parks": 1})
        self.assertLess(inland_cell["scores"]["parks"], coastal_cell["scores"]["parks"])

    def test_score_cells_clamp_effective_park_unit_normalization_floor_for_tiny_land_slivers(self) -> None:
        coastal_cell = _grid_cell(
            "tiny-coastal-park",
            effective_area_ratio=0.01,
        )

        precompute.score_cells(
            [coastal_cell],
            {3: {"parks": 1}},
            {3: {"parks": 1}},
            [3],
            effective_units_by_node={3: {"parks": 2}},
        )

        self.assertEqual(coastal_cell["counts"], {"parks": 1})
        self.assertEqual(coastal_cell["scores"]["parks"], 25.0)

    def test_score_cells_can_differ_for_cells_sharing_the_same_origin_node(self) -> None:
        inland_cell = _grid_cell(
            "inland",
            effective_area_ratio=1.0,
        )
        coastal_cell = _grid_cell(
            "coastal",
            effective_area_ratio=0.5,
        )

        precompute.score_cells(
            [inland_cell, coastal_cell],
            {7: {"transport": 2}},
            {7: {"transport": 2}},
            [7, 7],
            effective_units_by_node={7: {"transport": 2}},
        )

        self.assertEqual(inland_cell["counts"], coastal_cell["counts"])
        self.assertEqual(inland_cell["counts"], {"transport": 2})
        self.assertLess(inland_cell["scores"]["transport"], coastal_cell["scores"]["transport"])

    def test_score_cells_can_vary_park_scores_for_shared_origin_node(self) -> None:
        inland_cell = _grid_cell(
            "inland-park",
            effective_area_ratio=1.0,
        )
        coastal_cell = _grid_cell(
            "coastal-park",
            effective_area_ratio=0.5,
        )

        precompute.score_cells(
            [inland_cell, coastal_cell],
            {9: {"parks": 1}},
            {9: {"parks": 1}},
            [9, 9],
            effective_units_by_node={9: {"parks": 2}},
        )

        self.assertEqual(inland_cell["counts"], coastal_cell["counts"])
        self.assertEqual(inland_cell["counts"], {"parks": 1})
        self.assertLess(inland_cell["scores"]["parks"], coastal_cell["scores"]["parks"])

    def test_phase_reachability_reuses_cached_nodes_and_only_rebuilds_missing_walk_origins(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 3
        walk_nodes_by_category = {
            "shops": [],
            "transport": [0],
            "healthcare": [],
            "parks": [],
        }
        amenity_source_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "score_units": 1,
            }
        ]

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)
            precompute.cache_save(
                "amenity_clusters",
                [
                    {
                        "category": "transport",
                        "lat": 53.0,
                        "lon": -6.0,
                        "source_ref": "gtfs/1",
                        "base_units": 1,
                    }
                ],
                cache_dir,
            )
            precompute.cache_save("walk_cluster_nodes_by_cat", {"transport": [0]}, cache_dir)
            precompute.cache_save_large(
                "walk_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_cluster_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_effective_units_by_origin_node",
                {0: {"transport": 1.0}},
                cache_dir,
            )

            tracker = _tracker_mock()
            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    side_effect=AssertionError("cached amenity snaps should be reused"),
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=[{1: {"transport": 1}}, {1: {"transport": 1}}],
                ) as walk_counts_mock,
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    return_value={1: {"transport": 0.5}},
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=[0, 1],
                )
                cached_walk_counts = precompute.cache_load_large("walk_counts_by_origin_node", cache_dir)
                cached_cluster_counts = precompute.cache_load_large(
                    "walk_cluster_counts_by_origin_node",
                    cache_dir,
                )
                cached_effective_units = precompute.cache_load_large(
                    "walk_effective_units_by_origin_node",
                    cache_dir,
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(walk_counts_mock.call_count, 2)
        self.assertEqual(walk_counts_mock.call_args_list[0].args[2], (1,))
        self.assertEqual(walk_counts_mock.call_args_list[1].args[2], (1,))
        decayed_units_mock.assert_called_once()
        self.assertEqual(decayed_units_mock.call_args.args[2], (1,))
        self.assertEqual(walk_counts_by_node, {0: {"transport": 1}, 1: {"transport": 1}})
        self.assertEqual(walk_cluster_counts_by_node, {0: {"transport": 1}, 1: {"transport": 1}})
        self.assertEqual(walk_effective_units_by_node, {0: {"transport": 1.0}, 1: {"transport": 0.5}})
        self.assertEqual(cached_walk_counts, walk_counts_by_node)
        self.assertEqual(cached_cluster_counts, walk_cluster_counts_by_node)
        self.assertEqual(cached_effective_units, walk_effective_units_by_node)

    def test_phase_reachability_builds_and_caches_cluster_and_effective_unit_lookups(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 2
        walk_nodes_by_category = {
            "shops": [2],
            "transport": [],
            "healthcare": [],
            "parks": [],
        }
        amenity_source_rows = [
            {
                "category": "shops",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "node/1",
                "score_units": 3,
            }
        ]

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)

            tracker = _tracker_mock()
            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    return_value={"shops": [2]},
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=[{0: {"shops": 1}}, {0: {"shops": 1}}],
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    return_value={0: {"shops": 1.5}},
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=[0],
                )
                cached_cluster_counts = precompute.cache_load_large(
                    "walk_cluster_counts_by_origin_node",
                    cache_dir,
                )
                cached_effective_units = precompute.cache_load_large(
                    "walk_effective_units_by_origin_node",
                    cache_dir,
                )

            _clear_state_cache(cache_dir)

        decayed_units_mock.assert_called_once()
        self.assertEqual(walk_counts_by_node, {0: {"shops": 1}})
        self.assertEqual(walk_cluster_counts_by_node, {0: {"shops": 1}})
        self.assertEqual(walk_effective_units_by_node, {0: {"shops": 1.5}})
        self.assertEqual(cached_cluster_counts, {0: {"shops": 1}})
        self.assertEqual(cached_effective_units, {0: {"shops": 1.5}})

    def test_phase_reachability_checkpoint_callbacks_persist_chunk_only_large_caches(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 2
        amenity_source_rows = [
            {
                "category": "shops",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "node/1",
                "score_units": 3,
            }
        ]

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            tracker = _tracker_mock()

            def _checkpoint_counts(
                graph,
                nodes_by_category,
                origin_node_ids,
                *,
                save_chunk_cb,
                **kwargs,
            ):
                del graph, nodes_by_category, kwargs
                chunk = {int(node): {"shops": 1} for node in origin_node_ids}
                save_chunk_cb(chunk)
                return {}

            def _checkpoint_effective_units(
                graph,
                node_weights_by_category,
                origin_node_ids,
                *,
                save_chunk_cb,
                **kwargs,
            ):
                del graph, node_weights_by_category, kwargs
                chunk = {int(node): {"shops": 1.5} for node in origin_node_ids}
                save_chunk_cb(chunk)
                return {}

            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    return_value={"shops": [2]},
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=_checkpoint_counts,
                ) as walk_counts_mock,
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    side_effect=_checkpoint_effective_units,
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=[0, 1],
                )
                cached_walk_counts = precompute.cache_load_large(
                    "walk_counts_by_origin_node",
                    cache_dir,
                )
                cached_cluster_counts = precompute.cache_load_large(
                    "walk_cluster_counts_by_origin_node",
                    cache_dir,
                )
                cached_effective_units = precompute.cache_load_large(
                    "walk_effective_units_by_origin_node",
                    cache_dir,
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(walk_counts_mock.call_count, 2)
        self.assertEqual(decayed_units_mock.call_count, 1)
        self.assertEqual(walk_counts_by_node, {0: {"shops": 1}, 1: {"shops": 1}})
        self.assertEqual(walk_cluster_counts_by_node, {0: {"shops": 1}, 1: {"shops": 1}})
        self.assertEqual(walk_effective_units_by_node, {0: {"shops": 1.5}, 1: {"shops": 1.5}})
        self.assertEqual(cached_walk_counts, walk_counts_by_node)
        self.assertEqual(cached_cluster_counts, walk_cluster_counts_by_node)
        self.assertEqual(cached_effective_units, walk_effective_units_by_node)

    def test_phase_reachability_accepts_pre_normalized_origin_lists(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 3
        walk_nodes_by_category = {
            "shops": [],
            "transport": [0],
            "healthcare": [],
            "parks": [],
        }
        amenity_source_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "score_units": 1,
            }
        ]
        normalized_origin_nodes = precompute.normalize_origin_node_ids([0, 1])

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)
            precompute.cache_save(
                "amenity_clusters",
                [
                    {
                        "category": "transport",
                        "lat": 53.0,
                        "lon": -6.0,
                        "source_ref": "gtfs/1",
                        "base_units": 1,
                    }
                ],
                cache_dir,
            )
            precompute.cache_save("walk_cluster_nodes_by_cat", {"transport": [0]}, cache_dir)
            precompute.cache_save_large(
                "walk_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_cluster_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_effective_units_by_origin_node",
                {0: {"transport": 1.0}},
                cache_dir,
            )

            tracker = _tracker_mock()
            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    side_effect=AssertionError("cached amenity snaps should be reused"),
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=[{1: {"transport": 1}}, {1: {"transport": 1}}],
                ) as walk_counts_mock,
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    return_value={1: {"transport": 0.5}},
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=normalized_origin_nodes,
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(walk_counts_mock.call_count, 2)
        self.assertEqual(walk_counts_mock.call_args_list[0].args[2], (1,))
        self.assertEqual(walk_counts_mock.call_args_list[1].args[2], (1,))
        decayed_units_mock.assert_called_once()
        self.assertEqual(decayed_units_mock.call_args.args[2], (1,))
        self.assertEqual(walk_counts_by_node, {0: {"transport": 1}, 1: {"transport": 1}})
        self.assertEqual(walk_cluster_counts_by_node, {0: {"transport": 1}, 1: {"transport": 1}})
        self.assertEqual(walk_effective_units_by_node, {0: {"transport": 1.0}, 1: {"transport": 0.5}})

    def test_phase_reachability_recomputes_each_metric_only_for_its_own_missing_nodes(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 4
        walk_nodes_by_category = {
            "shops": [],
            "transport": [0],
            "healthcare": [],
            "parks": [],
        }
        amenity_cluster_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "base_units": 1,
            }
        ]
        amenity_source_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "score_units": 1,
            }
        ]

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)
            precompute.cache_save("amenity_clusters", amenity_cluster_rows, cache_dir)
            precompute.cache_save("walk_cluster_nodes_by_cat", {"transport": [0]}, cache_dir)
            precompute.cache_save_large(
                "walk_counts_by_origin_node",
                {
                    0: {"transport": 1},
                    2: {"transport": 1},
                    3: {"transport": 1},
                },
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_cluster_counts_by_origin_node",
                {
                    0: {"transport": 1},
                    1: {"transport": 1},
                    3: {"transport": 1},
                },
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_effective_units_by_origin_node",
                {
                    0: {"transport": 1.0},
                    1: {"transport": 1.0},
                    2: {"transport": 1.0},
                },
                cache_dir,
            )

            tracker = _tracker_mock()
            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    side_effect=AssertionError("cached amenity snaps should be reused"),
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=[{1: {"transport": 1}}, {2: {"transport": 1}}],
                ) as walk_counts_mock,
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    return_value={3: {"transport": 0.5}},
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=[0, 1, 2, 3],
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(walk_counts_mock.call_count, 2)
        self.assertEqual(walk_counts_mock.call_args_list[0].args[2], (1,))
        self.assertEqual(walk_counts_mock.call_args_list[1].args[2], (2,))
        decayed_units_mock.assert_called_once()
        self.assertEqual(decayed_units_mock.call_args.args[2], (3,))
        self.assertEqual(
            walk_counts_by_node,
            {
                0: {"transport": 1},
                1: {"transport": 1},
                2: {"transport": 1},
                3: {"transport": 1},
            },
        )
        self.assertEqual(
            walk_cluster_counts_by_node,
            {
                0: {"transport": 1},
                1: {"transport": 1},
                2: {"transport": 1},
                3: {"transport": 1},
            },
        )
        self.assertEqual(
            walk_effective_units_by_node,
            {
                0: {"transport": 1.0},
                1: {"transport": 1.0},
                2: {"transport": 1.0},
                3: {"transport": 0.5},
            },
        )

    def test_phase_reachability_salvages_legacy_blob_and_chunk_overlay_before_resuming(self) -> None:
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 3
        walk_nodes_by_category = {
            "shops": [],
            "transport": [0],
            "healthcare": [],
            "parks": [],
        }
        amenity_cluster_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "base_units": 1,
            }
        ]
        amenity_source_rows = [
            {
                "category": "transport",
                "lat": 53.0,
                "lon": -6.0,
                "source_ref": "gtfs/1",
                "score_units": 1,
            }
        ]

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", walk_nodes_by_category, cache_dir)
            precompute.cache_save("amenity_clusters", amenity_cluster_rows, cache_dir)
            precompute.cache_save("walk_cluster_nodes_by_cat", {"transport": [0]}, cache_dir)
            precompute.cache_save_large(
                "walk_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large_append_frame(
                "walk_counts_by_origin_node",
                {1: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_cluster_counts_by_origin_node",
                {0: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large_append_frame(
                "walk_cluster_counts_by_origin_node",
                {1: {"transport": 1}},
                cache_dir,
            )
            precompute.cache_save_large(
                "walk_effective_units_by_origin_node",
                {0: {"transport": 1.0}},
                cache_dir,
            )
            precompute.cache_save_large_append_frame(
                "walk_effective_units_by_origin_node",
                {1: {"transport": 0.75}},
                cache_dir,
            )

            tracker = _tracker_mock()
            with (
                mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir),
                mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False),
                mock.patch.object(
                    precompute,
                    "snap_amenities",
                    side_effect=AssertionError("cached amenity snaps should be reused"),
                ),
                mock.patch.object(
                    precompute,
                    "precompute_walk_counts_by_origin_node",
                    side_effect=[{2: {"transport": 1}}, {2: {"transport": 1}}],
                ) as walk_counts_mock,
                mock.patch.object(
                    precompute,
                    "precompute_walk_decayed_units_by_origin_node",
                    return_value={2: {"transport": 0.5}},
                ) as decayed_units_mock,
            ):
                (
                    _,
                    walk_counts_by_node,
                    walk_cluster_counts_by_node,
                    walk_effective_units_by_node,
                ) = precompute.phase_reachability(
                    walk_graph,
                    amenity_data={},
                    amenity_source_rows=amenity_source_rows,
                    tracker=tracker,
                    walk_origin_node_ids=[0, 1, 2],
                )
                cached_walk_counts = precompute.cache_load_large(
                    "walk_counts_by_origin_node",
                    cache_dir,
                )
                cached_cluster_counts = precompute.cache_load_large(
                    "walk_cluster_counts_by_origin_node",
                    cache_dir,
                )
                cached_effective_units = precompute.cache_load_large(
                    "walk_effective_units_by_origin_node",
                    cache_dir,
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(walk_counts_mock.call_count, 2)
        self.assertEqual(walk_counts_mock.call_args_list[0].args[2], (2,))
        self.assertEqual(walk_counts_mock.call_args_list[1].args[2], (2,))
        decayed_units_mock.assert_called_once()
        self.assertEqual(decayed_units_mock.call_args.args[2], (2,))
        self.assertEqual(
            walk_counts_by_node,
            {
                0: {"transport": 1},
                1: {"transport": 1},
                2: {"transport": 1},
            },
        )
        self.assertEqual(
            walk_cluster_counts_by_node,
            {
                0: {"transport": 1},
                1: {"transport": 1},
                2: {"transport": 1},
            },
        )
        self.assertEqual(
            walk_effective_units_by_node,
            {
                0: {"transport": 1.0},
                1: {"transport": 0.75},
                2: {"transport": 0.5},
            },
        )
        self.assertEqual(cached_walk_counts, walk_counts_by_node)
        self.assertEqual(cached_cluster_counts, walk_cluster_counts_by_node)
        self.assertEqual(cached_effective_units, walk_effective_units_by_node)

    def test_precompute_walk_counts_by_origin_node_uses_rust_bridge_for_walkgraph_index(self) -> None:
        graph = WalkGraphIndex(
            graph_dir=Path("cache/walk_graph"),
            meta={"node_count": 3, "edge_count": 2},
            node_latitudes=np.array([53.0, 53.1, 53.2], dtype=np.float64),
            node_longitudes=np.array([-6.3, -6.2, -6.1], dtype=np.float64),
        )
        nodes_by_category = {
            "shops": [0, 0],
            "transport": [1],
            "healthcare": [],
            "parks": [],
        }

        def _fake_rust_run(
            graph_dir,
            origins_bin,
            amenity_weights_bin,
            output_bin,
            *,
            category_count,
            cutoff_m,
            walkgraph_bin,
            progress_cb,
        ) -> None:
            del graph_dir, origins_bin, amenity_weights_bin, category_count, cutoff_m, walkgraph_bin, progress_cb
            np.asarray([2, 0, 2, 1], dtype=np.uint32).tofile(output_bin)

        with mock.patch.object(precompute._network, "run_walkgraph_reachability", side_effect=_fake_rust_run):
            counts_by_node = precompute.precompute_walk_counts_by_origin_node(
                graph,
                nodes_by_category,
                [0, 1],
                cutoff=500.0,
            )

        self.assertEqual(counts_by_node[0], {"shops": 2})
        self.assertEqual(counts_by_node[1], {"shops": 2, "transport": 1})

    def test_precompute_walk_decayed_units_by_origin_node_uses_rust_bridge_for_walkgraph_index(self) -> None:
        graph = WalkGraphIndex(
            graph_dir=Path("cache/walk_graph"),
            meta={"node_count": 3, "edge_count": 2},
            node_latitudes=np.array([53.0, 53.1, 53.2], dtype=np.float64),
            node_longitudes=np.array([-6.3, -6.2, -6.1], dtype=np.float64),
        )
        node_weights_by_category = {
            "shops": [(0, 2)],
            "transport": [(1, 1)],
        }

        def _fake_rust_run(
            graph_dir,
            origins_bin,
            amenity_weights_bin,
            output_bin,
            *,
            category_count,
            cutoff_m,
            walkgraph_bin,
            output_mode,
            half_distances_m,
            progress_cb,
        ) -> None:
            del graph_dir, origins_bin, amenity_weights_bin, category_count, cutoff_m, walkgraph_bin, progress_cb
            self.assertEqual(output_mode, "decayed-units")
            self.assertEqual(half_distances_m, [150.0, 250.0])
            np.asarray([1.0, 0.5, 0.25, 0.0], dtype=np.float32).tofile(output_bin)

        with mock.patch.object(precompute._network, "run_walkgraph_reachability", side_effect=_fake_rust_run):
            decayed_units = precompute.precompute_walk_decayed_units_by_origin_node(
                graph,
                node_weights_by_category,
                [0, 1],
                cutoff=500.0,
                half_distance_m_by_category={
                    "shops": 150.0,
                    "transport": 250.0,
                },
            )

        self.assertEqual(decayed_units[0], {"shops": 1.0, "transport": 0.5})
        self.assertEqual(decayed_units[1], {"shops": 0.25})

    def test_reach_tier_finalization_uses_walk_origin_count_cache(self) -> None:
        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("walk_nodes_by_cat", _empty_amenity_data(), cache_dir)
            precompute.cache_save("walk_cluster_nodes_by_cat", _empty_amenity_data(), cache_dir)
            precompute.cache_save_large("walk_counts_by_origin_node", {0: {}}, cache_dir)
            precompute.cache_save_large("walk_cluster_counts_by_origin_node", {0: {}}, cache_dir)
            precompute.cache_save_large("walk_effective_units_by_origin_node", {0: {}}, cache_dir)

            with mock.patch.object(precompute._STATE, "reach_cache_dir", cache_dir):
                can_finalize = precompute._can_finalize_reach_tier(_empty_amenity_data())

            _clear_state_cache(cache_dir)

        self.assertTrue(can_finalize)

    def test_validate_tier_reuses_recoverable_building_reach_cache(self) -> None:
        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            precompute.cache_save("amenities", [{"category": "shops", "lat": 53.0, "lon": -6.0}], cache_dir)
            precompute._tiers.write_tier_manifest(
                cache_dir,
                "reach",
                "reach-hash-123",
                "building",
                "amenities",
                manifest_name="manifest.json",
                cache_schema_version=config.CACHE_SCHEMA_VERSION,
                python_version=lambda: "3.12.0",
                package_snapshot=config.package_snapshot,
                render_hash="render-hash-123",
            )

            is_valid = precompute._tiers.validate_tier(
                cache_dir,
                "reach-hash-123",
                "reach",
                force_recompute=False,
                manifest_name="manifest.json",
                cache_schema_version=config.CACHE_SCHEMA_VERSION,
                recoverable_check=lambda tier_dir: precompute._tiers._has_recoverable_reach_artefacts(
                    tier_dir,
                    cache_load_for_finalize=precompute._cache_load_for_finalize,
                    cache_load_large_for_finalize=precompute._cache_load_large_for_finalize,
                ),
            )

            _clear_state_cache(cache_dir)

        self.assertTrue(is_valid)


class TransitServiceDesertTests(TestCase):
    def test_compute_service_deserts_skips_cells_with_upcoming_public_service(self) -> None:
        cell = _grid_cell("cell-1", geometry=box(0.0, 0.0, 1.0, 1.0))

        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            with (
                mock.patch.object(precompute._STATE, "transit_reality_state", SimpleNamespace(reality_fingerprint="reality-123", analysis_date=date(2026, 4, 14))),
                mock.patch.object(precompute._STATE, "study_area_metric", box(0.0, 0.0, 1.0, 1.0)),
                mock.patch.object(precompute._STATE, "hashes", SimpleNamespace(build_key="build-123", import_fingerprint="import-123")),
                mock.patch.object(precompute._STATE, "geo_cache_dir", cache_dir),
                mock.patch.object(precompute._STATE, "score_cache_dir", cache_dir),
                mock.patch.object(precompute, "load_walk_graph_index", return_value=mock.sentinel.walk_graph),
                mock.patch.object(precompute, "load_transport_reality_points", return_value=[{"geom": Point(-6.26, 53.35), "public_departures_7d": 3, "source_status": "gtfs_direct"}]),
                mock.patch.object(precompute, "snap_amenities", return_value={"transport": [101]}),
                mock.patch.object(precompute, "nearest_nodes", return_value=[101]),
                mock.patch.object(precompute, "cache_load", return_value=[101]),
                mock.patch.object(precompute, "normalize_origin_node_ids", return_value=[101]),
                mock.patch.object(precompute, "precompute_walk_counts_by_origin_node", return_value={101: {"transport": 1}}),
                mock.patch.object(precompute, "precompute_walk_weighted_totals_by_origin_node", return_value={101: {"public_departures_7d": 3}}),
                mock.patch.object(precompute, "replace_service_desert_rows") as replace_mock,
            ):
                precompute._compute_service_deserts(
                    mock.sentinel.engine,
                    {1000: [cell]},
                )

        replace_mock.assert_called_once_with(
            mock.sentinel.engine,
            build_key="build-123",
            desert_rows=[],
        )

    def test_compute_service_deserts_skips_cells_with_gtfs_direct_public_service(self) -> None:
        cell = _grid_cell("cell-1", geometry=box(0.0, 0.0, 1.0, 1.0))

        with TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name)
            with (
                mock.patch.object(precompute._STATE, "transit_reality_state", SimpleNamespace(reality_fingerprint="reality-123", analysis_date=date(2026, 4, 14))),
                mock.patch.object(precompute._STATE, "study_area_metric", box(0.0, 0.0, 1.0, 1.0)),
                mock.patch.object(precompute._STATE, "hashes", SimpleNamespace(build_key="build-123", import_fingerprint="import-123")),
                mock.patch.object(precompute._STATE, "geo_cache_dir", cache_dir),
                mock.patch.object(precompute._STATE, "score_cache_dir", cache_dir),
                mock.patch.object(precompute, "load_walk_graph_index", return_value=mock.sentinel.walk_graph),
                mock.patch.object(precompute, "load_transport_reality_points", return_value=[{"geom": Point(-6.26, 53.35), "public_departures_7d": 3, "source_status": "gtfs_direct"}]),
                mock.patch.object(precompute, "snap_amenities", return_value={"transport": [101]}),
                mock.patch.object(precompute, "nearest_nodes", return_value=[101]),
                mock.patch.object(precompute, "cache_load", return_value=[101]),
                mock.patch.object(precompute, "normalize_origin_node_ids", return_value=[101]),
                mock.patch.object(precompute, "precompute_walk_counts_by_origin_node", return_value={101: {"transport": 1}}),
                mock.patch.object(precompute, "precompute_walk_weighted_totals_by_origin_node", return_value={101: {"public_departures_7d": 3}}),
                mock.patch.object(precompute, "replace_service_desert_rows") as replace_mock,
            ):
                precompute._compute_service_deserts(
                    mock.sentinel.engine,
                    {1000: [cell]},
                )

        replace_mock.assert_called_once_with(
            mock.sentinel.engine,
            build_key="build-123",
            desert_rows=[],
        )


class WorkflowTests(TestCase):
    def test_precompute_runs_transit_preflight_before_geometry(self) -> None:
        kwargs = _workflow_kwargs(
            transit_preflight=mock.Mock(side_effect=RuntimeError("gtfs-refresh unavailable")),
        )

        with self.assertRaisesRegex(RuntimeError, "gtfs-refresh unavailable"):
            precompute._workflow.run_precompute_impl(**kwargs)

        kwargs["phase_geometry"].assert_not_called()

    def test_precompute_skips_after_geometry_when_complete_build_exists(self) -> None:
        kwargs = _workflow_kwargs(has_complete_build=mock.Mock(return_value=True))

        build_key = precompute._workflow.run_precompute_impl(**kwargs)

        self.assertEqual(build_key, "build-key-123")
        kwargs["phase_geometry"].assert_called_once()
        kwargs["ensure_local_osm_import"].assert_not_called()
        kwargs["tracker_factory"].assert_called_once()
        kwargs["print_cache_status"].assert_called_once()
        kwargs["validate_all_tiers"].assert_called_once()

    def test_precompute_skips_when_complete_build_and_pmtiles_present(self) -> None:
        with TemporaryDirectory() as tmp_name:
            pmtiles_path = Path(tmp_name) / "livability.pmtiles"
            pmtiles_path.write_bytes(b"PMTILESFAKE")
            bake_mock = mock.Mock()
            kwargs = _workflow_kwargs(has_complete_build=mock.Mock(return_value=True))

            build_key = precompute._workflow.run_precompute_impl(
                bake_pmtiles=bake_mock,
                pmtiles_output_path=pmtiles_path,
                **kwargs,
            )

        self.assertEqual(build_key, "build-key-123")
        bake_mock.assert_not_called()
        kwargs["phase_geometry"].assert_called_once()
        kwargs["publish_precomputed_artifacts"].assert_not_called()

    def test_precompute_rebakes_pmtiles_only_when_complete_build_but_archive_missing(self) -> None:
        with TemporaryDirectory() as tmp_name:
            pmtiles_path = Path(tmp_name) / "livability.pmtiles"
            self.assertFalse(pmtiles_path.exists())
            bake_mock = mock.Mock()
            kwargs = _workflow_kwargs(has_complete_build=mock.Mock(return_value=True))

            build_key = precompute._workflow.run_precompute_impl(
                bake_pmtiles=bake_mock,
                pmtiles_output_path=pmtiles_path,
                **kwargs,
            )

        self.assertEqual(build_key, "build-key-123")
        bake_mock.assert_called_once_with(
            mock.sentinel.engine,
            "build-key-123",
            pmtiles_path,
        )
        kwargs["phase_geometry"].assert_called_once()
        kwargs["publish_precomputed_artifacts"].assert_not_called()

    def test_precompute_propagates_pmtiles_bake_failure(self) -> None:
        with TemporaryDirectory() as tmp_name:
            pmtiles_path = Path(tmp_name) / "livability.pmtiles"
            bake_mock = mock.Mock(side_effect=RuntimeError("tippecanoe exploded"))
            kwargs = _workflow_kwargs(phase_grids=mock.Mock(return_value={1000: []}))

            with self.assertRaisesRegex(RuntimeError, "tippecanoe exploded"):
                precompute._workflow.run_precompute_impl(
                    bake_pmtiles=bake_mock,
                    pmtiles_output_path=pmtiles_path,
                    **kwargs,
                )

        bake_mock.assert_called_once()
        kwargs["publish_precomputed_artifacts"].assert_called_once()

    def test_precompute_fails_fast_when_import_is_missing_and_auto_refresh_is_disabled(self) -> None:
        kwargs = _workflow_kwargs(import_payload_ready=mock.Mock(return_value=False))

        with self.assertRaisesRegex(RuntimeError, "Run the import-refresh workflow first"):
            precompute._workflow.run_precompute_impl(**kwargs)

        kwargs["phase_geometry"].assert_not_called()
        kwargs["ensure_local_osm_import"].assert_not_called()

    def test_precompute_runs_import_when_auto_refresh_is_enabled_and_import_is_missing(self) -> None:
        kwargs = _workflow_kwargs(
            import_payload_ready=mock.Mock(return_value=False),
            phase_grids=mock.Mock(return_value={1000: []}),
        )

        build_key = precompute._workflow.run_precompute_impl(
            auto_refresh_import=True,
            **kwargs,
        )

        self.assertEqual(build_key, "build-key-123")
        kwargs["phase_geometry"].assert_called_once()
        kwargs["ensure_local_osm_import"].assert_called_once()
        self.assertFalse(kwargs["ensure_local_osm_import"].call_args.kwargs["force_refresh"])
        kwargs["publish_precomputed_artifacts"].assert_called_once()

    def test_successful_rebuild_preserves_walk_only_publish_payloads(self) -> None:
        walk_payload = [{"kind": "walk"}]
        amenity_payload = [{"kind": "amenity"}]
        noise_payload = [{"kind": "noise"}]
        summary_payload = {"summary": True}
        kwargs = _workflow_kwargs(
            phase_grids=mock.Mock(return_value={1000: []}),
            walk_rows=mock.Mock(return_value=walk_payload),
            amenity_rows=mock.Mock(return_value=amenity_payload),
            noise_rows=mock.Mock(return_value=noise_payload),
            summary_json=mock.Mock(return_value=summary_payload),
        )

        precompute._workflow.run_precompute_impl(force_precompute=True, **kwargs)

        publish_kwargs = kwargs["publish_precomputed_artifacts"].call_args.kwargs
        self.assertEqual(publish_kwargs["walk_rows"], walk_payload)
        self.assertEqual(publish_kwargs["amenity_rows"], amenity_payload)
        self.assertEqual(publish_kwargs["summary_json"], summary_payload)
        self.assertEqual(publish_kwargs["transport_reality_rows"], [])
        self.assertEqual(publish_kwargs["service_desert_rows"], [])
        self.assertEqual(publish_kwargs["noise_rows"], noise_payload)
        self.assertEqual(publish_kwargs["study_area_wgs84"], box(0.0, 0.0, 1.0, 1.0))
        self.assertIsNone(kwargs["summary_json"].call_args.kwargs["noise_rows"])
        self.assertNotIn("drive_rows", publish_kwargs)
        self.assertNotIn("hotspot_rows", publish_kwargs)

    def test_refresh_local_import_workflow_runs_import_without_full_scoring(self) -> None:
        kwargs = _workflow_kwargs(import_payload_ready=mock.Mock(return_value=False))

        build_key = precompute._workflow.run_import_refresh_impl(
            force_refresh=True,
            cache_dir=kwargs["cache_dir"],
            current_normalization_scope_hash=kwargs["current_normalization_scope_hash"],
            build_engine=kwargs["build_engine"],
            ensure_database_ready=kwargs["ensure_database_ready"],
            resolve_source_state=kwargs["resolve_source_state"],
            activate_build_hashes=kwargs["activate_build_hashes"],
            phase_geometry=kwargs["phase_geometry"],
            import_payload_ready=kwargs["import_payload_ready"],
            ensure_local_osm_import=kwargs["ensure_local_osm_import"],
            tracker_factory=kwargs["tracker_factory"],
            get_hashes=kwargs["get_hashes"],
            set_source_state=kwargs["set_source_state"],
        )

        self.assertEqual(build_key, "build-key-123")
        kwargs["phase_geometry"].assert_called_once()
        kwargs["ensure_local_osm_import"].assert_called_once()
        kwargs["tracker_factory"].assert_called_once()
        kwargs["publish_precomputed_artifacts"].assert_not_called()

    def _workflow_db_engine(self):
        engine = mock.Mock()
        conn = mock.Mock()
        connect_ctx = mock.MagicMock()
        connect_ctx.__enter__.return_value = conn
        connect_ctx.__exit__.return_value = False
        engine.connect.return_value = connect_ctx
        return engine

    def _workflow_db_engine_with_manifest_noise(
        self,
        *,
        stored_noise_hash: str | None,
        summary_json: dict | None = None,
    ):
        engine = self._workflow_db_engine()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.mappings.return_value.one_or_none.return_value = {
            "noise_processing_hash": stored_noise_hash,
            "summary_json": dict(summary_json or {}),
        }
        return engine

    def test_require_active_artifact_uses_mode_scoped_lookup_without_build(self) -> None:
        engine = self._workflow_db_engine()
        artifact = SimpleNamespace(artifact_hash="res-dev-123")
        kwargs = _workflow_kwargs(
            build_engine=mock.Mock(return_value=engine),
            has_complete_build=mock.Mock(return_value=True),
        )

        with (
            mock.patch("config.NOISE_MODE", "artifact"),
            mock.patch(
                "noise_artifacts.manifest.get_resolved_artifact_for_mode",
                return_value=artifact,
            ) as mode_lookup_mock,
            mock.patch(
                "noise_artifacts.runner.build_default_noise_artifact",
                side_effect=AssertionError("DevReuse must not build noise artifacts"),
            ),
            mock.patch("noise.loader.dataset_signature", side_effect=AssertionError("DevReuse must not compute source signatures")),
            mock.patch("noise_artifacts.ogr_ingest.discover_latest_rounds_by_group", side_effect=AssertionError("DevReuse must not discover rounds")),
            mock.patch("noise_artifacts.ingest.ingest_noise_normalized", side_effect=AssertionError("DevReuse must not ingest source rows")),
            mock.patch("builtins.print") as print_mock,
        ):
            build_key = precompute._workflow.run_precompute_impl(
                require_active_noise_artifact=True,
                **kwargs,
            )

        self.assertEqual(build_key, "build-key-123")
        mode_lookup_mock.assert_called_once_with(engine, "dev_fast")
        log_text = "\n".join(" ".join(str(arg) for arg in call.args) for call in print_mock.call_args_list)
        self.assertIn("active resolved artifact required and found", log_text)
        forbidden_markers = (
            "ingest start",
            "starting ogr2ogr import",
            "dev-fast: building road/rail coarse grid artifact",
            "computing raw noise source signature",
            "selected latest round",
            "loading ROI noise",
            "loading NI noise",
            "noise_datasets",
            "source hash:",
            "resolved hash:",
            "artifact build start",
            "ensuring artifact manifests",
        )
        log_lower = log_text.lower()
        for marker in forbidden_markers:
            self.assertNotIn(marker.lower(), log_lower)

    def test_complete_build_with_changed_noise_hash_refreshes_noise_only(self) -> None:
        from precompute._rows import _ArtifactNoiseReference

        engine = self._workflow_db_engine_with_manifest_noise(
            stored_noise_hash="res-old",
            summary_json={"summary": True},
        )
        artifact = SimpleNamespace(artifact_hash="res-new")
        noise_payload = _ArtifactNoiseReference("res-new", artifact)
        refresh_mock = mock.Mock()
        kwargs = _workflow_kwargs(
            build_engine=mock.Mock(return_value=engine),
            has_complete_build=mock.Mock(return_value=True),
            noise_processing_hash=mock.Mock(return_value="res-new"),
            noise_rows=mock.Mock(return_value=noise_payload),
            refresh_noise_overlay_for_build=refresh_mock,
        )

        with (
            mock.patch("config.NOISE_MODE", "artifact"),
            mock.patch(
                "noise_artifacts.manifest.get_resolved_artifact_for_mode",
                return_value=artifact,
            ),
            mock.patch(
                "noise_artifacts.runner.build_default_noise_artifact",
                side_effect=AssertionError("reuse mode must not rebuild artifacts"),
            ),
        ):
            build_key = precompute._workflow.run_precompute_impl(
                require_active_noise_artifact=True,
                **kwargs,
            )

        self.assertEqual(build_key, "build-key-123")
        kwargs["phase_amenities"].assert_not_called()
        kwargs["phase_grids"].assert_not_called()
        kwargs["summary_json"].assert_not_called()
        kwargs["publish_precomputed_artifacts"].assert_not_called()
        refresh_mock.assert_called_once()
        refresh_kwargs = refresh_mock.call_args.kwargs
        self.assertEqual(refresh_kwargs["summary_json"], {"summary": True})
        self.assertIs(refresh_kwargs["noise_rows"], noise_payload)
        self.assertEqual(refresh_kwargs["noise_processing_hash"], "res-new")
        self.assertEqual(refresh_kwargs["noise_artifact_hash"], "res-new")

    def test_require_active_artifact_missing_raises_fast_for_dev_fast(self) -> None:
        engine = self._workflow_db_engine()
        kwargs = _workflow_kwargs(
            build_engine=mock.Mock(return_value=engine),
            has_complete_build=mock.Mock(return_value=True),
        )

        with (
            mock.patch("config.NOISE_MODE", "artifact"),
            mock.patch(
                "noise_artifacts.manifest.get_resolved_artifact_for_mode",
                return_value=None,
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                r"Noise artifact missing for mode=dev_fast.*DevReuse never builds noise artifacts.*prepare_noise_artifact_dev\.cmd",
            ):
                precompute._workflow.run_precompute_impl(
                    require_active_noise_artifact=True,
                    **kwargs,
                )

    def test_require_active_artifact_missing_raises_fast_for_accurate(self) -> None:
        engine = self._workflow_db_engine()
        kwargs = _workflow_kwargs(
            build_engine=mock.Mock(return_value=engine),
            has_complete_build=mock.Mock(return_value=True),
        )

        with (
            mock.patch("config.NOISE_MODE", "artifact"),
            mock.patch(
                "noise_artifacts.manifest.get_resolved_artifact_for_mode",
                return_value=None,
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                r"Noise artifact missing for mode=accurate.*DevReuse never builds noise artifacts.*prepare_noise_artifact_accurate\.cmd",
            ):
                precompute._workflow.run_precompute_impl(
                    require_active_noise_artifact=True,
                    noise_accurate=True,
                    **kwargs,
                )

    def test_normal_mode_missing_artifact_builds(self) -> None:
        engine = self._workflow_db_engine()
        kwargs = _workflow_kwargs(
            build_engine=mock.Mock(return_value=engine),
            has_complete_build=mock.Mock(return_value=True),
        )
        build_result = {"status": "up_to_date", "artifact_hash": "res-x", "row_count": 0}
        built_artifact = SimpleNamespace(artifact_hash="res-x")

        with (
            mock.patch("config.NOISE_MODE", "artifact"),
            mock.patch(
                "noise_artifacts.manifest.get_resolved_artifact_for_mode",
                side_effect=[None, built_artifact],
            ),
            mock.patch(
                "noise_artifacts.runner.build_default_noise_artifact",
                return_value=build_result,
            ) as build_mock,
        ):
            build_key = precompute._workflow.run_precompute_impl(
                require_active_noise_artifact=False,
                refresh_noise_artifact=True,
                **kwargs,
            )

        self.assertEqual(build_key, "build-key-123")
        build_mock.assert_called_once()


class TransitRefreshPreflightTests(TestCase):
    def test_refresh_transit_fails_before_geometry_when_gtfs_preflight_fails(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")

        with (
            mock.patch.object(precompute, "build_engine", return_value=mock.sentinel.engine),
            mock.patch.object(precompute, "ensure_database_ready"),
            mock.patch.object(precompute, "resolve_source_state", return_value=source_state),
            mock.patch.object(precompute._STATE, "activate"),
            mock.patch.object(
                precompute,
                "_preflight_transit_rebuild",
                side_effect=RuntimeError("gtfs-refresh unavailable"),
            ),
            mock.patch.object(precompute, "phase_geometry") as phase_geometry_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "gtfs-refresh unavailable"):
                precompute.refresh_transit()

        phase_geometry_mock.assert_not_called()

    def test_refresh_transit_reuses_cached_manifest_without_geometry(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")
        reality_state = SimpleNamespace(reality_fingerprint="reality-123")
        tracker = _tracker_mock()

        with (
            mock.patch.object(precompute, "build_engine", return_value=mock.sentinel.engine),
            mock.patch.object(precompute, "ensure_database_ready"),
            mock.patch.object(precompute, "resolve_source_state", return_value=source_state),
            mock.patch.object(precompute._STATE, "activate"),
            mock.patch.object(
                precompute,
                "_preflight_transit_rebuild",
                return_value=(reality_state, False),
            ) as preflight_mock,
            mock.patch.object(
                precompute,
                "_ensure_transit_reality",
                return_value=reality_state,
            ) as ensure_transit_mock,
            mock.patch.object(precompute, "phase_geometry") as phase_geometry_mock,
            mock.patch.object(precompute, "PrecomputeProgressTracker", return_value=tracker),
        ):
            result = precompute.refresh_transit()

        self.assertEqual(result, "reality-123")
        preflight_mock.assert_called_once_with(
            mock.sentinel.engine,
            force_refresh=False,
            refresh_download=True,
            progress_cb=tracker.phase_callback.return_value,
        )
        ensure_transit_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            study_area_wgs84=None,
            force_refresh=False,
            refresh_download=False,
            progress_cb=tracker.phase_callback.return_value,
            reality_state=reality_state,
        )
        tracker.start_phase.assert_called_once_with(
            "transit",
            detail="checking GTFS feed availability and cache state",
        )
        tracker.finish_phase.assert_called_once_with(
            "transit",
            "cached",
            detail="transit reality ready (reality-123)",
        )
        phase_geometry_mock.assert_not_called()

    def test_refresh_transit_marks_transit_phase_completed_after_rebuild(self) -> None:
        source_state = SimpleNamespace(import_fingerprint="import-fingerprint-123")
        reality_state = SimpleNamespace(reality_fingerprint="reality-123")
        tracker = _tracker_mock()

        with (
            mock.patch.object(precompute, "build_engine", return_value=mock.sentinel.engine),
            mock.patch.object(precompute, "ensure_database_ready"),
            mock.patch.object(precompute, "resolve_source_state", return_value=source_state),
            mock.patch.object(precompute._STATE, "activate"),
            mock.patch.object(
                precompute,
                "_preflight_transit_rebuild",
                return_value=(reality_state, True),
            ),
            mock.patch.object(
                precompute,
                "phase_geometry",
                return_value=(mock.sentinel.study_area_metric, mock.sentinel.study_area_wgs84),
            ),
            mock.patch.object(precompute, "current_normalization_scope_hash", return_value="norm-scope-123"),
            mock.patch.object(precompute, "import_payload_ready", return_value=True),
            mock.patch.object(precompute, "ensure_local_osm_import"),
            mock.patch.object(
                precompute,
                "_ensure_transit_reality",
                return_value=reality_state,
            ) as ensure_transit_mock,
            mock.patch.object(precompute, "PrecomputeProgressTracker", return_value=tracker),
        ):
            result = precompute.refresh_transit()

        self.assertEqual(result, "reality-123")
        ensure_transit_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            study_area_wgs84=mock.sentinel.study_area_wgs84,
            refresh_download=False,
            force_refresh=False,
            progress_cb=tracker.phase_callback.return_value,
            reality_state=reality_state,
        )
        tracker.start_phase.assert_called_once_with(
            "transit",
            detail="checking GTFS feed availability and cache state",
        )
        tracker.finish_phase.assert_called_once_with(
            "transit",
            "completed",
            detail="transit reality ready (reality-123)",
        )

    def test_preflight_transit_rebuild_skips_walkgraph_when_transit_reality_is_cached(self) -> None:
        with (
            mock.patch.object(
                precompute._STATE,
                "source_state",
                SimpleNamespace(import_fingerprint="import-fingerprint-123"),
            ),
            mock.patch.object(
                precompute,
                "_transit_reality_refresh_required",
                return_value=(mock.sentinel.state, False),
            ) as refresh_required_mock,
            mock.patch.object(
                precompute,
                "ensure_walkgraph_subcommand_available",
            ) as ensure_subcommand_mock,
        ):
            state, refresh_required = precompute._preflight_transit_rebuild(mock.sentinel.engine)

        refresh_required_mock.assert_called_once_with(
            mock.sentinel.engine,
            import_fingerprint="import-fingerprint-123",
            refresh_download=False,
            force_refresh=False,
            progress_cb=None,
        )
        self.assertIs(state, mock.sentinel.state)
        self.assertFalse(refresh_required)
        ensure_subcommand_mock.assert_not_called()


class GridArtifactTests(TestCase):
    def test_load_or_build_walk_origin_nodes_reuses_cached_score_artifact(self) -> None:
        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            origin_key = precompute._phases._walk_origin_nodes_cache_key([1000])
            precompute.cache_save(origin_key, [7], cache_dir)

            with mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False):
                walk_origin_nodes, built = precompute._phases._load_or_build_walk_origin_nodes(
                    [1000],
                    {1000: [0, 1, 0]},
                    cache_dir=cache_dir,
                    cache_load=precompute.cache_load,
                    cache_save=precompute.cache_save,
                    normalize_origin_node_ids=precompute.normalize_origin_node_ids,
                )

            _clear_state_cache(cache_dir)

        self.assertFalse(built)
        self.assertEqual(walk_origin_nodes, [7])

    def test_phase_grids_deduplicates_walk_origins_across_sizes_and_caches_artifact(self) -> None:
        tracker = _tracker_mock()
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 3
        grid_1000 = [_grid_cell("1000-a"), _grid_cell("1000-b")]
        grid_500 = [_grid_cell("500-a")]
        amenity_source_rows = [{"category": "parks", "lat": 53.0, "lon": -6.0, "source_ref": "way/1"}]
        phase_reachability_mock = mock.Mock(
            return_value=(
                {},
                {
                    10: {"shops": 1},
                    20: {"parks": 1},
                },
                {
                    10: {"shops": 1},
                    20: {"parks": 1},
                },
                {
                    10: {},
                    20: {"parks": 0.5},
                },
            )
        )
        ensure_surface_shell_cache_mock = mock.Mock(return_value={})
        ensure_surface_score_cache_mock = mock.Mock(return_value={})

        def snap_side_effect(graph, cells, key, cache_dir):
            del graph, cells, cache_dir
            mapping = {
                "walk_cell_nodes_1000": [10, 10],
                "walk_cell_nodes_500": [20],
            }
            return list(mapping[key])

        with TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            with (
                mock.patch.object(precompute._STATE, "settings", SimpleNamespace(grid_sizes_m=[1000, 500])),
                mock.patch.object(precompute, "_active_fine_surface_enabled", return_value=False),
                mock.patch.object(precompute._STATE, "score_cache_dir", cache_dir),
                mock.patch.object(precompute, "phase_networks", return_value=walk_graph),
                mock.patch.object(precompute, "phase_reachability", phase_reachability_mock),
                mock.patch.object(precompute, "build_grid", side_effect=[grid_1000, grid_500]),
                mock.patch.object(precompute, "snap_cells_to_nodes", side_effect=snap_side_effect),
                mock.patch.object(precompute, "_mark_building"),
                mock.patch.object(precompute, "_mark_complete"),
            ):
                walk_grids = precompute.phase_grids(
                    mock.sentinel.engine,
                    box(0.0, 0.0, 1.0, 1.0),
                    _empty_amenity_data(),
                    amenity_source_rows,
                    tracker,
                )

            with mock.patch.dict(precompute._STATE.tier_valid, {cache_dir: True}, clear=False):
                cached_origin_nodes = precompute.cache_load(
                    "walk_origin_nodes__sizes_500_1000",
                    cache_dir,
                )

            _clear_state_cache(cache_dir)

        self.assertEqual(cached_origin_nodes, [10, 20])
        self.assertEqual(
            phase_reachability_mock.call_args.kwargs["walk_origin_node_ids"],
            [10, 20],
        )
        self.assertIs(phase_reachability_mock.call_args.args[2], amenity_source_rows)
        self.assertEqual(sorted(walk_grids), [500, 1000])

    def test_phase_grids_unions_coarse_and_surface_origins_without_using_all_graph_nodes(self) -> None:
        tracker = _tracker_mock()
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 999
        cache_store: dict[str, object] = {}
        phase_reachability_mock = mock.Mock(
            return_value=(
                {},
                {
                    10: {"shops": 1},
                    20: {"transport": 1},
                    30: {"parks": 1},
                },
                {
                    10: {"shops": 1},
                    20: {"transport": 1},
                    30: {"parks": 1},
                },
                {
                    10: {"shops": 0.5},
                    20: {"transport": 0.5},
                    30: {"parks": 1.0},
                },
            )
        )
        ensure_surface_shell_cache_mock = mock.Mock(return_value={})
        ensure_surface_score_cache_mock = mock.Mock(return_value={})

        def cache_load(key, cache_dir):
            del cache_dir
            return cache_store.get(key)

        def cache_save(key, data, cache_dir):
            del cache_dir
            cache_store[key] = data

        walk_grids = precompute._phases.phase_grids_impl(
            mock.sentinel.engine,
            box(0.0, 0.0, 1.0, 1.0),
            _empty_amenity_data(),
            [{"category": "parks", "lat": 53.0, "lon": -6.0, "source_ref": "way/1"}],
            tracker,
            grid_sizes_m=[1000],
            cache_dir=Path("score-cache"),
            score_hash="score-hash-123",
            tiers_building=set(),
            cache_exists=lambda key, cache_dir: key in cache_store,
            cache_load=cache_load,
            cache_save=cache_save,
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            grid_cells_are_2d=precompute._grid._grid_cells_are_2d,
            phase_networks=mock.Mock(return_value=walk_graph),
            phase_reachability=phase_reachability_mock,
            normalize_origin_node_ids=precompute.normalize_origin_node_ids,
            merge_normalized_origin_node_ids=precompute.merge_normalized_origin_node_ids,
            build_grid=mock.Mock(return_value=[_grid_cell("1000-a")]),
            elapsed=lambda started_at: "[0.0s]",
            clone_grid_shells=precompute._grid._clone_grid_shells,
            snap_cells_to_nodes=mock.Mock(return_value=[10]),
            score_cells=mock.Mock(),
            fine_surface_enabled=True,
            reach_hash="reach-hash-123",
            surface_shell_hash="shell-hash-123",
            surface_shell_dir=Path("surface-shell"),
            surface_score_dir=Path("surface-scores"),
            ensure_surface_shell_cache=ensure_surface_shell_cache_mock,
            ensure_surface_score_cache=ensure_surface_score_cache_mock,
            collect_surface_origin_nodes=mock.Mock(return_value=[20, 30]),
            surface_analysis_ready=mock.Mock(return_value=False),
            graph_dir=Path("graph-dir"),
            walkgraph_bin="walkgraph.exe",
            surface_threads=3,
        )

        self.assertEqual(
            phase_reachability_mock.call_args.kwargs["walk_origin_node_ids"],
            [10, 20, 30],
        )
        self.assertEqual(ensure_surface_shell_cache_mock.call_args.kwargs["threads"], 3)
        self.assertEqual(sorted(walk_grids), [1000])

    def test_phase_grids_skips_fine_surface_callbacks_when_disabled(self) -> None:
        tracker = _tracker_mock()
        walk_graph = mock.Mock()
        walk_graph.vcount.return_value = 42
        cache_store: dict[str, object] = {}
        phase_reachability_mock = mock.Mock(
            return_value=({}, {10: {"shops": 1}}, {10: {"shops": 1}}, {10: {"shops": 1.0}})
        )
        ensure_surface_shell_cache_mock = mock.Mock(
            side_effect=AssertionError("dev mode should not build fine surface shells")
        )
        ensure_surface_score_cache_mock = mock.Mock(
            side_effect=AssertionError("dev mode should not build fine surface scores")
        )

        walk_grids = precompute._phases.phase_grids_impl(
            mock.sentinel.engine,
            box(0.0, 0.0, 1.0, 1.0),
            _empty_amenity_data(),
            [],
            tracker,
            grid_sizes_m=[1000],
            cache_dir=Path("score-cache"),
            score_hash="score-hash-123",
            tiers_building=set(),
            cache_exists=lambda key, cache_dir: key in cache_store,
            cache_load=lambda key, cache_dir: cache_store.get(key),
            cache_save=lambda key, data, cache_dir: cache_store.__setitem__(key, data),
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            grid_cells_are_2d=precompute._grid._grid_cells_are_2d,
            phase_networks=mock.Mock(return_value=walk_graph),
            phase_reachability=phase_reachability_mock,
            normalize_origin_node_ids=precompute.normalize_origin_node_ids,
            merge_normalized_origin_node_ids=precompute.merge_normalized_origin_node_ids,
            build_grid=mock.Mock(return_value=[_grid_cell("1000-a")]),
            elapsed=lambda started_at: "[0.0s]",
            clone_grid_shells=precompute._grid._clone_grid_shells,
            snap_cells_to_nodes=mock.Mock(return_value=[10]),
            score_cells=mock.Mock(),
            fine_surface_enabled=False,
            reach_hash="reach-hash-123",
            surface_shell_hash="shell-hash-123",
            surface_shell_dir=Path("surface-shell"),
            surface_score_dir=Path("surface-scores"),
            ensure_surface_shell_cache=ensure_surface_shell_cache_mock,
            ensure_surface_score_cache=ensure_surface_score_cache_mock,
            collect_surface_origin_nodes=mock.Mock(return_value=[30]),
            surface_analysis_ready=mock.Mock(return_value=False),
            graph_dir=Path("graph-dir"),
            walkgraph_bin="walkgraph.exe",
            surface_threads=3,
        )

        self.assertEqual(sorted(walk_grids), [1000])
        self.assertEqual(phase_reachability_mock.call_args.kwargs["walk_origin_node_ids"], [10])
        ensure_surface_shell_cache_mock.assert_not_called()
        ensure_surface_score_cache_mock.assert_not_called()


class SurfaceCacheStatusTests(TestCase):
    def test_print_cache_status_shows_partial_surface_progress(self) -> None:
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            shell_dir = tmp / "surface-shell"
            score_dir = tmp / "surface-scores"
            tile_dir = tmp / "surface-tiles"
            shell_dir.mkdir()
            score_dir.mkdir()
            tile_dir.mkdir()
            (shell_dir / "shards").mkdir()
            for index in range(3):
                (shell_dir / "shards" / f"cached_{index}.npz").write_bytes(b"npz")

            precompute._surface.write_surface_manifest(
                shell_dir,
                {
                    "status": "building",
                    "schema_version": 1,
                    "surface_shell_hash": "shell-hash-123",
                    "reach_hash": "reach-hash-123",
                    "base_resolution_m": 50,
                    "shard_size_m": 20000,
                    "tile_size_px": 256,
                    "completed_shards": 63,
                    "total_shards": 278,
                    "shard_inventory": [{"shard_id": "0_0", "path": "shards/0_0.npz"}],
                },
            )
            precompute._surface.write_surface_manifest(
                score_dir,
                {
                    "status": "complete",
                    "schema_version": 1,
                    "score_hash": "score-hash-123",
                    "surface_shell_hash": "shell-hash-123",
                    "node_scores_file": "node_scores.npz",
                    "shard_inventory": [],
                },
            )
            precompute._surface.write_surface_manifest(
                tile_dir,
                {
                    "status": "ready",
                    "schema_version": 1,
                    "score_hash": "score-hash-123",
                    "render_hash": "render-hash-123",
                },
            )

            original_dirs = (
                precompute._STATE.surface_shell_dir,
                precompute._STATE.surface_score_dir,
                precompute._STATE.surface_tile_dir,
            )
            precompute._STATE.surface_shell_dir = shell_dir
            precompute._STATE.surface_score_dir = score_dir
            precompute._STATE.surface_tile_dir = tile_dir
            try:
                with (
                    mock.patch.object(precompute._tiers, "print_cache_status"),
                    mock.patch("builtins.print") as print_mock,
                ):
                    precompute.print_cache_status()
            finally:
                (
                    precompute._STATE.surface_shell_dir,
                    precompute._STATE.surface_score_dir,
                    precompute._STATE.surface_tile_dir,
                ) = original_dirs

        printed_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertTrue(any("surface_shell" in line for line in printed_lines))
        self.assertTrue(any("status=building" in line and "shards=63/278" in line for line in printed_lines))


class StudyAreaAndPublishTests(TestCase):
    def test_clean_union_strips_z_from_boundary_geometry(self) -> None:
        unioned = study_area.clean_union([_polygon_z(-1.0, -1.0, 1.0, 1.0)])

        self.assertFalse(unioned.has_z)

    def test_phase_geometry_uses_shared_study_area_loader(self) -> None:
        tracker = _tracker_mock()
        study_area_metric = box(0.0, 0.0, 2.0, 1.0)
        study_area_wgs84 = box(0.0, 0.0, 1.0, 0.5)
        saved = {}

        returned_metric, returned_wgs84 = precompute._phases.phase_geometry_impl(
            tracker,
            cache_dir=Path("geo-cache"),
            geo_hash="geo-hash-123",
            cache_load=lambda key, directory: None,
            cache_save=lambda key, data, directory: saved.__setitem__(key, data),
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            geometry_is_2d=precompute._grid._geometry_is_2d,
            can_finalize_geo_tier=mock.Mock(return_value=False),
            load_study_area_geometries=mock.Mock(
                return_value=(study_area_metric, study_area_wgs84)
            ),
            study_area_wgs84_from_metric=mock.Mock(),
        )

        self.assertEqual(returned_metric, study_area_metric)
        self.assertEqual(returned_wgs84, study_area_wgs84)
        self.assertEqual(saved["study_area_metric"], study_area_metric)
        self.assertEqual(saved["study_area_wgs84"], study_area_wgs84)

    def test_phase_geometry_cache_hit_reuses_cached_wgs84_geometry(self) -> None:
        tracker = _tracker_mock()
        cache_dir = Path("geo-cache")
        study_area_metric = box(0.0, 0.0, 2.0, 1.0)
        study_area_wgs84 = box(0.0, 0.0, 1.0, 0.5)
        cache = {
            "study_area_metric": study_area_metric,
            "study_area_wgs84": study_area_wgs84,
        }

        returned_metric, returned_wgs84 = precompute._phases.phase_geometry_impl(
            tracker,
            cache_dir=cache_dir,
            geo_hash="geo-hash-123",
            cache_load=lambda key, directory: cache.get(key),
            cache_save=mock.Mock(),
            mark_building=mock.Mock(),
            mark_complete=mock.Mock(),
            geometry_is_2d=precompute._grid._geometry_is_2d,
            can_finalize_geo_tier=mock.Mock(),
            load_study_area_geometries=mock.Mock(
                side_effect=AssertionError("cached geometry should be reused")
            ),
            study_area_wgs84_from_metric=mock.Mock(),
        )

        self.assertEqual(returned_metric, study_area_metric)
        self.assertEqual(returned_wgs84, study_area_wgs84)
        tracker.finish_phase.assert_called_with("geometry", "cached", detail="geometry cache hit")

    def test_phase_geometry_repairs_metric_only_cache_and_saves_wgs84(self) -> None:
        tracker = _tracker_mock()
        cache_dir = Path("geo-cache")
        study_area_metric = box(0.0, 0.0, 1.0, 1.0)
        study_area_wgs84 = box(0.0, 0.0, 1.0, 0.5)
        cache = {"study_area_metric": study_area_metric}
        saved = {}
        can_finalize = mock.Mock(return_value=True)
        mark_complete = mock.Mock()
        load_study_area_geometries = mock.Mock(
            side_effect=AssertionError("valid cached metric geometry should be reused")
        )

        returned_metric, returned_wgs84 = precompute._phases.phase_geometry_impl(
            tracker,
            cache_dir=cache_dir,
            geo_hash="geo-hash-123",
            cache_load=lambda key, directory: cache.get(key),
            cache_save=lambda key, data, directory: saved.__setitem__(key, data),
            mark_building=mock.Mock(),
            mark_complete=mark_complete,
            geometry_is_2d=precompute._grid._geometry_is_2d,
            can_finalize_geo_tier=can_finalize,
            load_study_area_geometries=load_study_area_geometries,
            study_area_wgs84_from_metric=mock.Mock(return_value=study_area_wgs84),
        )

        self.assertEqual(returned_metric, study_area_metric)
        self.assertEqual(returned_wgs84, study_area_wgs84)
        self.assertNotIn("study_area_metric", saved)
        self.assertEqual(saved["study_area_wgs84"], study_area_wgs84)
        can_finalize.assert_called_once_with(study_area_metric, study_area_wgs84)
        mark_complete.assert_called_once_with(cache_dir, "geo", "geo-hash-123", "geometry")

    def test_recoverable_geo_artifacts_require_both_cached_geometries(self) -> None:
        cache_dir = Path("geo-cache")
        artifacts = {"study_area_metric": box(0.0, 0.0, 1.0, 1.0)}

        def cache_load_for_finalize(key, directory):
            del directory
            return artifacts.get(key)

        self.assertFalse(
            precompute._tiers._has_recoverable_geo_artefacts(
                cache_dir,
                cache_load_for_finalize=cache_load_for_finalize,
            )
        )

        artifacts["study_area_wgs84"] = box(0.0, 0.0, 1.0, 1.0)

        self.assertTrue(
            precompute._tiers._has_recoverable_geo_artefacts(
                cache_dir,
                cache_load_for_finalize=cache_load_for_finalize,
            )
        )

    def test_load_study_area_metric_defaults_to_island_geometry(self) -> None:
        island_geom = box(0.0, 0.0, 2.0, 2.0)
        settings = SimpleNamespace(study_area_kind="ireland", study_area_county_name=None)

        with (
            mock.patch.object(study_area, "build_profile_settings", return_value=settings),
            mock.patch.object(study_area, "load_island_geometry_metric", return_value=island_geom),
            mock.patch.object(study_area, "load_m1_corridor_metric") as corridor_mock,
        ):
            returned = study_area.load_study_area_metric()

        self.assertEqual(returned, island_geom)
        corridor_mock.assert_not_called()

    def test_load_study_area_metric_keeps_corridor_mode_available(self) -> None:
        island_geom = box(0.0, 0.0, 2.0, 2.0)
        corridor_geom = box(0.5, 0.5, 1.5, 1.5)
        settings = SimpleNamespace(study_area_kind="m1_corridor", study_area_county_name=None)

        with (
            mock.patch.object(study_area, "build_profile_settings", return_value=settings),
            mock.patch.object(study_area, "load_island_geometry_metric", return_value=island_geom),
            mock.patch.object(study_area, "load_m1_corridor_metric", return_value=corridor_geom) as corridor_mock,
        ):
            returned = study_area.load_study_area_metric()

        self.assertEqual(returned, corridor_geom)
        corridor_mock.assert_called_once_with(island_geom)

    def test_load_study_area_metric_uses_bbox_geometry_for_test_profile(self) -> None:
        island_geom = box(0.0, 0.0, 2.0, 2.0)
        cork_geom = box(1.0, 1.0, 1.5, 1.5)
        settings = SimpleNamespace(
            study_area_kind="bbox",
            study_area_county_name=None,
            study_area_bbox_wgs84=(-8.55, 51.87, -8.41, 51.93),
        )

        with (
            mock.patch.object(study_area, "build_profile_settings", return_value=settings),
            mock.patch.object(study_area, "load_island_geometry_metric", return_value=island_geom) as island_mock,
            mock.patch.object(study_area, "load_bbox_geometry_metric", return_value=cork_geom) as bbox_mock,
        ):
            returned = study_area.load_study_area_metric(profile="test")

        self.assertEqual(returned, cork_geom)
        island_mock.assert_called_once_with(progress_cb=None)
        bbox_mock.assert_called_once_with(
            island_geom,
            (-8.55, 51.87, -8.41, 51.93),
            progress_cb=None,
        )

    def test_load_bbox_geometry_metric_clips_bbox_to_island_geometry(self) -> None:
        island_geom = box(100.0, 100.0, 200.0, 200.0)

        with mock.patch.object(study_area, "transform", return_value=box(120.0, 130.0, 170.0, 180.0)):
            returned = study_area.load_bbox_geometry_metric(
                island_geom,
                (-8.55, 51.87, -8.41, 51.93),
            )

        self.assertTrue(returned.equals(box(120.0, 130.0, 170.0, 180.0)))

    def test_load_bbox_geometry_metric_reports_empty_clip_clearly(self) -> None:
        island_geom = box(100.0, 100.0, 200.0, 200.0)

        with mock.patch.object(study_area, "transform", return_value=box(0.0, 0.0, 10.0, 10.0)):
            with self.assertRaises(RuntimeError) as ctx:
                study_area.load_bbox_geometry_metric(
                    island_geom,
                    (-8.55, 51.87, -8.41, 51.93),
                )

        self.assertIn("empty after clipping", str(ctx.exception))

    def test_load_bbox_geometry_metric_rejects_invalid_bbox_config(self) -> None:
        island_geom = box(100.0, 100.0, 200.0, 200.0)

        with self.assertRaises(RuntimeError) as ctx:
            study_area.load_bbox_geometry_metric(
                island_geom,
                (-8.41, 51.93, -8.55, 51.87),
            )

        self.assertIn("min_lon < max_lon", str(ctx.exception))

    def test_load_county_geometry_metric_selects_county_name_case_insensitively(self) -> None:
        county_rows = study_area.gpd.GeoDataFrame(
            {
                "ENG_NAME_VALUE": ["CORK", "Cork", "DUBLIN"],
                "geometry": [
                    box(0.0, 0.0, 1.0, 1.0),
                    box(1.0, 0.0, 2.0, 1.0),
                    box(10.0, 10.0, 11.0, 11.0),
                ],
            },
            crs=config.TARGET_CRS,
        )

        with mock.patch.object(study_area, "_read_boundary_file", return_value=county_rows):
            returned = study_area.load_county_geometry_metric("cork")

        self.assertFalse(returned.is_empty)
        self.assertFalse(returned.has_z)
        self.assertEqual(returned.bounds, (0.0, 0.0, 2.0, 1.0))

    def test_load_county_geometry_metric_reports_unknown_county_clearly(self) -> None:
        county_rows = study_area.gpd.GeoDataFrame(
            {
                "ENG_NAME_VALUE": ["DUBLIN"],
                "geometry": [box(10.0, 10.0, 11.0, 11.0)],
            },
            crs=config.TARGET_CRS,
        )

        with mock.patch.object(study_area, "_read_boundary_file", return_value=county_rows):
            with self.assertRaises(RuntimeError) as ctx:
                study_area.load_county_geometry_metric("CORK")

        self.assertIn("CORK", str(ctx.exception))

    def test_build_scoring_grid_emits_2d_geometry(self) -> None:
        with mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)):
            cells = precompute._grid.build_scoring_grid(1.0, box(0.0, 0.0, 2.0, 1.0))

        self.assertEqual(len(cells), 2)
        self.assertTrue(all("geometry" in cell for cell in cells))
        self.assertTrue(all(not cell["geometry"].has_z for cell in cells))

    def test_build_scoring_grid_records_full_effective_area_for_inland_cells(self) -> None:
        with mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)):
            cells = precompute._grid.build_scoring_grid(1.0, box(0.0, 0.0, 1.0, 1.0))

        self.assertEqual(len(cells), 1)
        self.assertEqual(cells[0]["effective_area_m2"], 1.0)
        self.assertEqual(cells[0]["effective_area_ratio"], 1.0)

    def test_build_scoring_grid_reduces_effective_area_for_boundary_cells(self) -> None:
        with mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)):
            cells = precompute._grid.build_scoring_grid(1.0, box(0.25, 0.25, 0.75, 0.75))

        self.assertEqual(len(cells), 1)
        self.assertAlmostEqual(cells[0]["effective_area_m2"], 0.25)
        self.assertAlmostEqual(cells[0]["effective_area_ratio"], 0.25)

    def test_clone_scoring_grid_shells_preserves_geometry_and_bounds(self) -> None:
        geometry = box(0.0, 0.0, 1.0, 1.0)
        original = _grid_cell(
            "walk-cell",
            geometry=geometry,
            metric_bounds=(10.0, 20.0, 30.0, 40.0),
            clip_required=True,
            effective_area_m2=123.0,
            effective_area_ratio=0.3075,
        )

        cloned = precompute._grid.clone_scoring_grid_shells([original])[0]

        self.assertTrue(cloned["geometry"].equals(geometry))
        self.assertEqual(cloned["metric_bounds"], (10.0, 20.0, 30.0, 40.0))
        self.assertTrue(cloned["clip_required"])
        self.assertEqual(cloned["effective_area_m2"], 123.0)
        self.assertEqual(cloned["effective_area_ratio"], 0.3075)

    def test_materialize_cell_geometry_clips_missing_boundary_geometry_by_rect(self) -> None:
        cell = _grid_cell(
            "boundary-cell",
            include_geometry=False,
            metric_bounds=(0.0, 0.0, 1.0, 1.0),
            clip_required=True,
        )

        with mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)):
            geometry = precompute._grid.materialize_cell_geometry(
                cell,
                box(0.25, 0.25, 0.75, 0.75),
            )

        self.assertTrue(geometry.equals(box(0.25, 0.25, 0.75, 0.75)))

    def test_walk_rows_preserve_2d_geometry_in_payloads(self) -> None:
        created_at = datetime.now(timezone.utc)
        walk_grids = {
            1000: [_grid_cell("walk-cell")],
        }

        walk_rows = list(precompute._walk_rows(walk_grids, created_at))

        self.assertFalse(walk_rows[0]["cell_geom"].has_z)
        self.assertFalse(walk_rows[0]["centre_geom"].has_z)
        self.assertEqual(walk_rows[0]["effective_area_m2"], 1.0)
        self.assertEqual(walk_rows[0]["effective_area_ratio"], 1.0)
        self.assertEqual(walk_rows[0]["cluster_counts_json"], {})
        self.assertEqual(walk_rows[0]["effective_units_json"], {})

        invalid_grids = {
            1000: [_grid_cell("bad-cell", geometry=_polygon_z(0.0, 0.0, 1.0, 1.0))],
        }
        with self.assertRaises(ValueError):
            list(precompute._walk_rows(invalid_grids, created_at))

    def test_walk_rows_materialize_missing_geometry(self) -> None:
        created_at = datetime.now(timezone.utc)
        walk_grids = {
            1000: [_grid_cell("walk-cell", include_geometry=False, metric_bounds=(0.0, 0.0, 1.0, 1.0))],
        }

        with (
            mock.patch.object(precompute._STATE, "study_area_metric", box(0.0, 0.0, 1.0, 1.0)),
            mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)),
        ):
            walk_rows = list(precompute._walk_rows(walk_grids, created_at))

        self.assertTrue(walk_rows[0]["cell_geom"].equals(box(0.0, 0.0, 1.0, 1.0)))

    def test_walk_rows_clip_boundary_cells_only_when_required(self) -> None:
        created_at = datetime.now(timezone.utc)
        walk_grids = {
            1000: [
                _grid_cell(
                    "inland-cell",
                    include_geometry=False,
                    metric_bounds=(0.0, 0.0, 1.0, 1.0),
                    clip_required=False,
                ),
                _grid_cell(
                    "boundary-cell",
                    include_geometry=False,
                    metric_bounds=(0.0, 0.0, 1.0, 1.0),
                    clip_required=True,
                ),
            ],
        }

        with (
            mock.patch.object(precompute._STATE, "study_area_metric", box(0.25, 0.25, 0.75, 0.75)),
            mock.patch.object(precompute._grid, "TO_WGS84", lambda x, y: (x, y)),
        ):
            walk_rows = list(precompute._walk_rows(walk_grids, created_at))

        self.assertTrue(walk_rows[0]["cell_geom"].equals(box(0.0, 0.0, 1.0, 1.0)))
        self.assertTrue(walk_rows[1]["cell_geom"].equals(box(0.25, 0.25, 0.75, 0.75)))

    def test_walk_rows_reuses_existing_geometry(self) -> None:
        created_at = datetime.now(timezone.utc)
        materialize_cell_geometry = mock.Mock(side_effect=AssertionError("should not materialize"))
        row_stream = precompute._publish.iter_walk_rows_impl(
            {1000: [_grid_cell("walk-cell")]},
            created_at,
            hashes=precompute._STATE.hashes,
            study_area_metric=box(0.0, 0.0, 1.0, 1.0),
            materialize_cell_geometry=materialize_cell_geometry,
        )

        walk_rows = list(row_stream)

        self.assertEqual(len(walk_rows), 1)
        materialize_cell_geometry.assert_not_called()

    def test_walk_rows_reports_cell_context_when_geometry_materialization_fails(self) -> None:
        created_at = datetime.now(timezone.utc)
        cell = _grid_cell(
            "bad-cell",
            include_geometry=False,
            metric_bounds=(0.0, 0.0, 1.0, 1.0),
            clip_required=True,
        )
        row_stream = precompute._publish.iter_walk_rows_impl(
            {1000: [cell]},
            created_at,
            hashes=precompute._STATE.hashes,
            study_area_metric=box(0.0, 0.0, 1.0, 1.0),
            materialize_cell_geometry=mock.Mock(side_effect=MemoryError("boom")),
        )

        with self.assertRaises(RuntimeError) as ctx:
            list(row_stream)

        message = str(ctx.exception)
        self.assertIn("resolution_m=1000", message)
        self.assertIn("cell_id='bad-cell'", message)
        self.assertIn("metric_bounds=(0.0, 0.0, 1.0, 1.0)", message)
        self.assertIn("clip_required=True", message)
        self.assertIsInstance(ctx.exception.__cause__, MemoryError)

    def test_grid_cells_are_2d_requires_clip_required_metadata(self) -> None:
        self.assertFalse(
            precompute._grid._grid_cells_are_2d(
                [
                    {
                        "cell_id": "missing-clip",
                        "centre": (53.0, -6.0),
                        "metric_bounds": (0.0, 0.0, 1.0, 1.0),
                        "effective_area_m2": 1.0,
                        "effective_area_ratio": 1.0,
                        "counts": {},
                        "scores": {},
                        "total": 0.0,
                    }
                ]
            )
        )

    def test_grid_cells_are_2d_requires_effective_area_metadata(self) -> None:
        self.assertFalse(
            precompute._grid._grid_cells_are_2d(
                [
                    {
                        "cell_id": "missing-effective-area",
                        "centre": (53.0, -6.0),
                        "metric_bounds": (0.0, 0.0, 1.0, 1.0),
                        "clip_required": False,
                        "counts": {},
                        "scores": {},
                        "total": 0.0,
                    }
                ]
            )
        )

    def test_iter_walk_rows_emits_progress_during_row_preparation(self) -> None:
        created_at = datetime.now(timezone.utc)
        progress = mock.Mock()
        row_stream = precompute._publish.iter_walk_rows_impl(
            {1000: [_grid_cell("walk-cell")]},
            created_at,
            hashes=precompute._STATE.hashes,
            study_area_metric=box(0.0, 0.0, 1.0, 1.0),
            materialize_cell_geometry=precompute._grid.materialize_cell_geometry,
            progress_cb=progress,
            progress_every=1,
        )

        rows = list(row_stream)

        self.assertEqual(len(rows), 1)
        progress.assert_any_call(
            "detail",
            detail="preparing grid_walk rows 1/1",
            force_log=True,
        )

    def test_summary_json_can_describe_coarse_only_dev_runtime(self) -> None:
        summary = precompute._publish.summary_json_impl(
            box(-10.0, 50.0, -5.0, 55.0),
            {20_000: [_grid_cell("coarse-cell")]},
            _empty_amenity_data(),
            hashes=SimpleNamespace(
                build_key="build-key-dev",
                config_hash="config-hash-dev",
                import_fingerprint="import-fingerprint-dev",
            ),
            build_profile="dev",
            source_state=SimpleNamespace(extract_path=Path("extract.osm.pbf")),
            osm_extract_path=Path("extract.osm.pbf"),
            grid_sizes_m=[20_000, 10_000, 5_000],
            fine_resolutions_m=[],
            output_html="index.html",
            zoom_breaks=[(10, 5_000), (8, 10_000), (0, 20_000)],
        )

        self.assertEqual(summary["build_profile"], "dev")
        self.assertEqual(summary["coarse_vector_resolutions_m"], [20_000, 10_000, 5_000])
        self.assertEqual(summary["fine_resolutions_m"], [])
        self.assertEqual(summary["surface_zoom_breaks"], [(10, 5_000), (8, 10_000), (0, 20_000)])
        self.assertEqual(
            summary["amenity_tier_counts"],
            {
                "shops": {},
                "transport": {},
                "healthcare": {},
                "parks": {},
            },
        )

    def test_render_from_db_delegates_to_local_server(self) -> None:
        with mock.patch.object(
            render_from_db,
            "serve_livability_app",
            return_value="http://127.0.0.1:8765/",
        ) as serve_mock:
            returned_url = render_from_db.run_render_from_db(host="127.0.0.1", port=8765)

        self.assertEqual(returned_url, "http://127.0.0.1:8765/")
        serve_mock.assert_called_once_with(host="127.0.0.1", port=8765, profile="full")


class ConfigTests(TestCase):
    def test_config_hashes_no_drive(self) -> None:
        config_source = Path(config.__file__).read_text(encoding="utf-8")

        self.assertFalse(hasattr(config, "DRIVE_TIME_S"))
        self.assertFalse(hasattr(config, "DRIVE_HOURS"))
        self.assertIn("WALKGRAPH_FORMAT_VERSION", config_source)
        self.assertIn("WALKGRAPH_BBOX_PADDING_M", config_source)
        self.assertNotIn("networkx", config_source)

        hashes = config.build_config_hashes()
        self.assertTrue(hashes.geo_hash)
        self.assertTrue(hashes.render_hash)

        with mock.patch.object(config, "WALKGRAPH_BIN", "walkgraph-from-another-path.exe"):
            alternate_hashes = config.build_config_hashes()

        self.assertEqual(hashes.geo_hash, alternate_hashes.geo_hash)
        self.assertEqual(hashes.config_hash, alternate_hashes.config_hash)

    def test_default_study_area_kind_is_ireland(self) -> None:
        self.assertEqual(config.STUDY_AREA_KIND, "ireland")
        self.assertEqual(config.build_profile_settings("full").study_area_kind, "ireland")
        self.assertEqual(config.build_profile_settings("test").study_area_kind, "bbox")
        self.assertEqual(
            config.build_profile_settings("test").study_area_bbox_wgs84,
            (-8.55, 51.87, -8.41, 51.93),
        )

    def test_config_hash_changes_when_profile_study_area_changes(self) -> None:
        island_hashes = config.build_config_hashes(profile="full")
        corridor_hashes = config.build_config_hashes(profile="test")

        self.assertNotEqual(island_hashes.geo_hash, corridor_hashes.geo_hash)
        self.assertNotEqual(island_hashes.config_hash, corridor_hashes.config_hash)


class CompactGraphArrayTests(TestCase):
    def test_vertex_coordinate_arrays_use_compact_graph_attrs(self) -> None:
        graph = _FakeGraph({})
        graph["_node_latitudes"] = [53.0, 53.1, 53.2]
        graph["_node_longitudes"] = [-6.3, -6.2, -6.1]

        latitudes, longitudes = precompute._network._vertex_coordinate_arrays(graph)

        self.assertEqual(latitudes.tolist(), [53.0, 53.1, 53.2])
        self.assertEqual(longitudes.tolist(), [-6.3, -6.2, -6.1])

    def test_edge_weights_use_compact_graph_attr_when_available(self) -> None:
        graph = _FakeGraph({}, edge_lengths=[1.0, 2.0])
        graph["_edge_length_m"] = [5.0, 6.0]

        weights = precompute._network._edge_weights(graph, "length_m")

        self.assertEqual(list(weights), [5.0, 6.0])
