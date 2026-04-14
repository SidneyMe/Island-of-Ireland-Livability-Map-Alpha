from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import BASE_DIR
from db_postgis import build_engine, ensure_database_ready, load_point_scores_for_build
from serve_from_db import RuntimeService


DEFAULT_FIXTURE_PATH = BASE_DIR / "fixtures" / "livability_sanity.v1.json"
EXPECTED_FIXTURE_VERSION = 1
EXPECTED_LOCATION_COUNT = 24
EXPECTED_SCORE_SCALE = "0-100"
VALID_TAGS = {
    "urban_core",
    "suburb",
    "rural",
    "coastal",
    "noise",
    "single_cluster",
    "ghost_stop",
    "control_town",
}
IRELAND_LAT_RANGE = (51.0, 55.6)
IRELAND_LON_RANGE = (-10.9, -5.2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or execute the livability sanity fixture against the current build.",
    )
    parser.add_argument(
        "--fixture",
        default=str(DEFAULT_FIXTURE_PATH),
        help=f"Fixture JSON path (default: {DEFAULT_FIXTURE_PATH}).",
    )
    parser.add_argument(
        "--profile",
        choices=("full", "dev"),
        default="full",
        help="Build profile used to resolve the current completed runtime manifest.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "Validate fixture structure only. "
            "CI uses this mode because the repository does not ship a completed Ireland precompute build."
        ),
    )
    return parser


def load_fixture(path: str | Path) -> dict[str, Any]:
    fixture_path = Path(path)
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def validate_fixture_payload(payload: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return ["fixture must be a JSON object"]

    if payload.get("fixture_version") != EXPECTED_FIXTURE_VERSION:
        errors.append(
            f"fixture_version must be {EXPECTED_FIXTURE_VERSION}, got {payload.get('fixture_version')!r}"
        )
    if payload.get("score_scale") != EXPECTED_SCORE_SCALE:
        errors.append(
            f"score_scale must be {EXPECTED_SCORE_SCALE!r}, got {payload.get('score_scale')!r}"
        )

    notes = payload.get("notes")
    if not isinstance(notes, list) or not notes or any(not isinstance(note, str) or not note.strip() for note in notes):
        errors.append("notes must be a non-empty list of strings")

    locations = payload.get("locations")
    if not isinstance(locations, list):
        return errors + ["locations must be a list"]
    if len(locations) != EXPECTED_LOCATION_COUNT:
        errors.append(
            f"locations must contain exactly {EXPECTED_LOCATION_COUNT} entries, got {len(locations)}"
        )

    seen_ids: set[str] = set()
    for index, location in enumerate(locations):
        prefix = f"locations[{index}]"
        if not isinstance(location, dict):
            errors.append(f"{prefix} must be an object")
            continue

        location_id = location.get("id")
        if not isinstance(location_id, str) or not location_id.strip():
            errors.append(f"{prefix}.id must be a non-empty string")
        elif location_id in seen_ids:
            errors.append(f"{prefix}.id {location_id!r} is duplicated")
        else:
            seen_ids.add(location_id)

        name = location.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{prefix}.name must be a non-empty string")

        rationale = location.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            errors.append(f"{prefix}.rationale must be a non-empty string")

        tags = location.get("tags")
        if not isinstance(tags, list) or not tags:
            errors.append(f"{prefix}.tags must be a non-empty list")
        else:
            normalized_tags: list[str] = []
            for tag in tags:
                if not isinstance(tag, str) or not tag.strip():
                    errors.append(f"{prefix}.tags must only contain non-empty strings")
                    continue
                normalized_tags.append(tag)
                if tag not in VALID_TAGS:
                    errors.append(f"{prefix}.tags contains unknown tag {tag!r}")
            if len(normalized_tags) != len(set(normalized_tags)):
                errors.append(f"{prefix}.tags must not contain duplicates")

        lat = location.get("lat")
        lon = location.get("lon")
        if not _is_finite_number(lat):
            errors.append(f"{prefix}.lat must be a finite number")
        else:
            latitude = float(lat)
            if latitude < IRELAND_LAT_RANGE[0] or latitude > IRELAND_LAT_RANGE[1]:
                errors.append(f"{prefix}.lat {latitude} is outside the Ireland fixture bounds")
        if not _is_finite_number(lon):
            errors.append(f"{prefix}.lon must be a finite number")
        else:
            longitude = float(lon)
            if longitude < IRELAND_LON_RANGE[0] or longitude > IRELAND_LON_RANGE[1]:
                errors.append(f"{prefix}.lon {longitude} is outside the Ireland fixture bounds")

        expected_total_score = location.get("expected_total_score")
        if not isinstance(expected_total_score, dict):
            errors.append(f"{prefix}.expected_total_score must be an object")
            continue
        minimum = expected_total_score.get("min")
        maximum = expected_total_score.get("max")
        if not _is_finite_number(minimum):
            errors.append(f"{prefix}.expected_total_score.min must be a finite number")
            continue
        if not _is_finite_number(maximum):
            errors.append(f"{prefix}.expected_total_score.max must be a finite number")
            continue
        minimum_value = float(minimum)
        maximum_value = float(maximum)
        if minimum_value < 0.0 or minimum_value > 100.0:
            errors.append(f"{prefix}.expected_total_score.min must be within 0..100")
        if maximum_value < 0.0 or maximum_value > 100.0:
            errors.append(f"{prefix}.expected_total_score.max must be within 0..100")
        if minimum_value > maximum_value:
            errors.append(f"{prefix}.expected_total_score.min must be <= max")

    return errors


def _lookup_descriptor(observed: dict[str, Any]) -> str:
    source = str(observed.get("lookup_source") or "unknown")
    resolution_m = observed.get("resolution_m")
    if isinstance(resolution_m, int):
        return f"{source}@{resolution_m}m"
    return source


def _load_fine_surface_scores(
    service: RuntimeService,
    locations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    observed_by_id: dict[str, dict[str, Any]] = {}
    for location in locations:
        payload = service.inspect(lat=float(location["lat"]), lon=float(location["lon"]), zoom=None)
        total_score = payload.get("total_score")
        observed_by_id[str(location["id"])] = {
            "lookup_source": "fine_surface",
            "resolution_m": (
                None
                if payload.get("resolution_m") is None
                else int(payload["resolution_m"])
            ),
            "total_score": None if total_score is None else float(total_score),
            "component_scores": dict(payload.get("component_scores") or {}),
            "counts": dict(payload.get("counts") or {}),
            "missing_reason": None
            if bool(payload.get("valid_land")) and total_score is not None
            else "fine surface inspection returned no valid land score",
        }
    return observed_by_id


def _load_grid_walk_scores(
    engine,
    *,
    build_key: str,
    locations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    observed_by_id: dict[str, dict[str, Any]] = {}
    for row in load_point_scores_for_build(
        engine,
        build_key=build_key,
        points=locations,
    ):
        observed_by_id[str(row["point_id"])] = {
            "lookup_source": "grid_walk",
            "resolution_m": int(row["resolution_m"]),
            "total_score": float(row["total_score"]),
            "component_scores": dict(row.get("scores_json") or {}),
            "counts": dict(row.get("counts_json") or {}),
            "missing_reason": None,
        }

    for location in locations:
        location_id = str(location["id"])
        if location_id in observed_by_id:
            continue
        observed_by_id[location_id] = {
            "lookup_source": "grid_walk",
            "resolution_m": None,
            "total_score": None,
            "component_scores": {},
            "counts": {},
            "missing_reason": "no grid_walk cell covered the fixture point",
        }
    return observed_by_id


def evaluate_fixture(
    locations: list[dict[str, Any]],
    observed_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    mismatches: list[str] = []
    for location in locations:
        location_id = str(location["id"])
        observed = observed_by_id.get(location_id)
        if observed is None:
            mismatches.append(
                f"MISSING {location_id}: no lookup result was produced. Rationale: {location['rationale']}"
            )
            continue

        total_score = observed.get("total_score")
        if total_score is None:
            mismatches.append(
                f"MISSING {location_id}: {observed.get('missing_reason') or 'no score available'} "
                f"via {_lookup_descriptor(observed)}. Rationale: {location['rationale']}"
            )
            continue

        minimum = float(location["expected_total_score"]["min"])
        maximum = float(location["expected_total_score"]["max"])
        score_value = float(total_score)
        if minimum <= score_value <= maximum:
            continue

        mismatches.append(
            f"MISMATCH {location_id}: expected {minimum:.1f}-{maximum:.1f}, "
            f"got {score_value:.1f} via {_lookup_descriptor(observed)}. "
            f"Rationale: {location['rationale']}"
        )
    return mismatches


def run_sanity_check(
    *,
    fixture_path: str | Path = DEFAULT_FIXTURE_PATH,
    profile: str = "full",
    validate_only: bool = False,
) -> int:
    resolved_fixture_path = Path(fixture_path)
    payload = load_fixture(resolved_fixture_path)
    errors = validate_fixture_payload(payload)
    if errors:
        print(f"Fixture validation failed for {resolved_fixture_path}:")
        for error in errors:
            print(f"  - {error}")
        return 1

    locations = list(payload["locations"])
    if validate_only:
        print(
            f"Fixture valid: {len(locations)} locations in {resolved_fixture_path} "
            f"(structure only; no build lookup performed)."
        )
        return 0

    engine = build_engine()
    ensure_database_ready(engine)
    service = RuntimeService(engine, profile=profile)
    state = service.state()
    if state.fine_surface_enabled:
        observed_by_id = _load_fine_surface_scores(service, locations)
        lookup_mode = "fine_surface"
    else:
        observed_by_id = _load_grid_walk_scores(
            engine,
            build_key=state.build_key,
            locations=locations,
        )
        lookup_mode = "grid_walk"

    mismatches = evaluate_fixture(locations, observed_by_id)
    if mismatches:
        print(
            f"Sanity fixture failed for build {state.build_key} "
            f"({lookup_mode}, profile={profile}):"
        )
        for mismatch in mismatches:
            print(f"  - {mismatch}")
        return 1

    print(
        f"Sanity fixture passed for build {state.build_key} "
        f"using {lookup_mode} lookups across {len(locations)} locations."
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_sanity_check(
            fixture_path=args.fixture,
            profile=args.profile,
            validate_only=args.validate_only,
        )
    except (FileNotFoundError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
