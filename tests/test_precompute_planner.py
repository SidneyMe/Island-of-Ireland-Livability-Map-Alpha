from __future__ import annotations

from unittest import TestCase


def _import_context(**overrides):
    from precompute._planning import ImportContext

    defaults = {
        "import_was_ready": True,
        "auto_refresh_import": False,
    }
    defaults.update(overrides)
    return ImportContext(**defaults)


def _build_context(**overrides):
    from precompute._planning import BuildContext

    defaults = {
        "force_precompute": False,
        "has_complete_build": True,
        "noise_hash_matches": True,
        "noise_refresh_requested": False,
        "can_refresh_noise_only": False,
        "pmtiles_missing": False,
        "noise_pmtiles_missing": False,
        "surface_missing": False,
    }
    defaults.update(overrides)
    return BuildContext(**defaults)


def _full_context(**overrides):
    from precompute._planning import BuildContext, ImportContext, NoiseArtifactContext, PrecomputeContext

    import_defaults = {
        "import_was_ready": True,
        "auto_refresh_import": False,
    }
    noise_defaults = {
        "noise_mode": "artifact",
        "noise_accuracy_mode": "dev_fast",
        "engine_has_connect": True,
        "require_active_noise_artifact": False,
        "active_noise_artifact_exists": True,
        "force_noise_artifact": False,
        "reimport_noise_source": False,
        "force_noise_all": False,
        "refresh_noise_artifact": False,
    }
    build_defaults = {
        "force_precompute": False,
        "has_complete_build": True,
        "noise_hash_matches": True,
        "noise_refresh_requested": False,
        "can_refresh_noise_only": False,
        "pmtiles_missing": False,
        "noise_pmtiles_missing": False,
        "surface_missing": False,
    }
    import_overrides = overrides.pop("import_context", {})
    noise_overrides = overrides.pop("noise_artifact_context", {})
    build_overrides = overrides.pop("build_context", {})
    import_defaults.update(import_overrides)
    noise_defaults.update(noise_overrides)
    build_defaults.update(build_overrides)
    return PrecomputeContext(
        import_context=ImportContext(**import_defaults),
        noise_artifact_context=NoiseArtifactContext(**noise_defaults),
        build_context=BuildContext(**build_defaults),
    )


class PrecomputePlannerTests(TestCase):
    def test_import_plan_refreshes_or_fails_explicitly(self) -> None:
        from precompute._planning import plan_import

        self.assertEqual(plan_import(_import_context(import_was_ready=True)).action, "skip")
        self.assertEqual(plan_import(_import_context(import_was_ready=False, auto_refresh_import=True)).action, "refresh")
        self.assertEqual(plan_import(_import_context(import_was_ready=False, auto_refresh_import=False)).action, "fail")

    def test_build_plan_skips_when_complete_build_is_clean(self) -> None:
        from precompute._planning import plan_build

        plan = plan_build(_build_context())
        self.assertEqual(plan.action, "skip")
        self.assertEqual(plan.reason, "complete PostGIS precompute already exists")

    def test_build_plan_refreshes_noise_overlay_when_selected_artifact_changes(self) -> None:
        from precompute._planning import plan_build

        plan = plan_build(
            _build_context(
                noise_hash_matches=False,
                can_refresh_noise_only=True,
            )
        )
        self.assertEqual(plan.action, "refresh_noise_overlay_only")
        self.assertTrue(plan.rebake_pmtiles_if_missing)
        self.assertTrue(plan.rebake_noise_pmtiles_always)
        self.assertIn("selected noise artifact changed", plan.reason)

    def test_build_plan_rebakes_missing_assets_without_full_rebuild(self) -> None:
        from precompute._planning import plan_build

        plan = plan_build(
            _build_context(
                pmtiles_missing=True,
                noise_pmtiles_missing=True,
            )
        )
        self.assertEqual(plan.action, "rebake_missing_assets")
        self.assertTrue(plan.rebake_pmtiles_if_missing)
        self.assertTrue(plan.rebake_noise_pmtiles_if_missing)
        self.assertFalse(plan.rebake_noise_pmtiles_always)
        self.assertIn("missing", plan.reason)

    def test_build_plan_rebuilds_fine_surface_when_surface_cache_missing(self) -> None:
        from precompute._planning import plan_build

        plan = plan_build(_build_context(surface_missing=True))
        self.assertEqual(plan.action, "rebuild_fine_surface")
        self.assertEqual(plan.reason, "fine surface cache is missing")

    def test_build_plan_rebuilds_full_pipeline_when_noise_cannot_be_isolated(self) -> None:
        from precompute._planning import plan_build

        plan = plan_build(
            _build_context(
                noise_hash_matches=False,
                can_refresh_noise_only=False,
            )
        )
        self.assertEqual(plan.action, "rebuild_full_pipeline")
        self.assertIn("rebuilding full pipeline", plan.reason)

    def test_precompute_plan_combines_subplans(self) -> None:
        from precompute._planning import plan_precompute

        plan = plan_precompute(
            _full_context(
                import_context={"import_was_ready": False, "auto_refresh_import": True},
                noise_artifact_context={"force_noise_artifact": True},
                build_context={"pmtiles_missing": True},
            )
        )
        self.assertEqual(plan.import_plan.action, "refresh")
        self.assertEqual(plan.noise_artifact_plan.action, "build")
        self.assertEqual(plan.build_plan.action, "rebake_missing_assets")
        self.assertIn("raw OSM import will be refreshed", plan.reasons[0])
