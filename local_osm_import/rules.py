from __future__ import annotations

from typing import Any


PRIVATE_VALUES = {"private", "no"}
WALK_EXCLUDED = {
    "construction",
    "motor",
    "motorway",
    "motorway_link",
    "planned",
    "proposed",
    "raceway",
    "trunk",
    "trunk_link",
}


def is_private_impl(
    tags: dict[str, Any],
    *keys: str,
    private_values: set[str],
) -> bool:
    for key in keys:
        value = str(tags.get(key) or "").lower()
        if value in private_values:
            return True
    return False


def is_walkable_impl(
    tags: dict[str, Any],
    *,
    is_private_fn,
    walk_excluded: set[str],
) -> bool:
    highway = str(tags.get("highway") or "")
    if not highway or highway in walk_excluded:
        return False
    if is_private_fn(tags, "access", "foot"):
        return False
    return True
