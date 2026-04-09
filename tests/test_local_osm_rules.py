from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from unittest import TestCase


def _load_rules_module() -> ModuleType:
    rules_path = Path(__file__).resolve().parents[1] / "local_osm_import" / "rules.py"
    spec = importlib.util.spec_from_file_location("local_osm_rules_under_test", rules_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load rules module from {rules_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rules = _load_rules_module()


def _is_private(tags: dict[str, object], *keys: str) -> bool:
    return rules.is_private_impl(tags, *keys, private_values=rules.PRIVATE_VALUES)


def _is_walkable(tags: dict[str, object]) -> bool:
    return rules.is_walkable_impl(
        tags,
        is_private_fn=_is_private,
        walk_excluded=rules.WALK_EXCLUDED,
    )


class LocalOsmWalkabilityRuleTests(TestCase):
    def test_walkable_highway_is_accepted(self) -> None:
        self.assertTrue(_is_walkable({"highway": "footway"}))

    def test_excluded_highways_are_rejected(self) -> None:
        for highway in rules.WALK_EXCLUDED:
            with self.subTest(highway=highway):
                self.assertFalse(_is_walkable({"highway": highway}))

    def test_private_or_no_access_is_rejected(self) -> None:
        for key in ("access", "foot"):
            for value in ("private", "no"):
                with self.subTest(key=key, value=value):
                    self.assertFalse(_is_walkable({"highway": "residential", key: value}))
