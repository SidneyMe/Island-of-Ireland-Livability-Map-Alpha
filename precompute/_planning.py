from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ImportPlan:
    action: Literal["skip", "refresh", "fail"]
    reason: str


@dataclass(frozen=True)
class ImportContext:
    import_was_ready: bool
    auto_refresh_import: bool


@dataclass(frozen=True)
class NoiseArtifactContext:
    noise_mode: str
    noise_accuracy_mode: str
    engine_has_connect: bool
    require_active_noise_artifact: bool
    active_noise_artifact_exists: bool
    force_noise_artifact: bool
    reimport_noise_source: bool
    force_noise_all: bool
    refresh_noise_artifact: bool


@dataclass(frozen=True)
class BuildContext:
    force_precompute: bool
    has_complete_build: bool
    noise_hash_matches: bool
    noise_refresh_requested: bool
    can_refresh_noise_only: bool
    pmtiles_missing: bool
    noise_pmtiles_missing: bool
    surface_missing: bool


@dataclass(frozen=True)
class NoiseArtifactPlan:
    action: Literal["skip", "reuse", "build", "require_active", "legacy"]
    reason: str
    force_resolved: bool = False
    reimport_source: bool = False


@dataclass(frozen=True)
class BuildPlan:
    action: Literal[
        "continue",
        "skip",
        "refresh_noise_overlay_only",
        "rebake_missing_assets",
        "rebuild_fine_surface",
        "rebuild_full_pipeline",
    ]
    reason: str
    rebake_pmtiles_if_missing: bool = False
    rebake_noise_pmtiles_if_missing: bool = False
    rebake_noise_pmtiles_always: bool = False


@dataclass(frozen=True)
class PrecomputeContext:
    import_context: ImportContext
    noise_artifact_context: NoiseArtifactContext
    build_context: BuildContext


@dataclass(frozen=True)
class PrecomputePlan:
    import_plan: ImportPlan
    noise_artifact_plan: NoiseArtifactPlan
    build_plan: BuildPlan

    @property
    def reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        reasons.append(self.import_plan.reason)
        reasons.append(self.noise_artifact_plan.reason)
        reasons.append(self.build_plan.reason)
        return tuple(reason for reason in reasons if reason)


def _surface_action(plan: PrecomputePlan) -> tuple[str, str]:
    if plan.build_plan.action == "rebuild_fine_surface":
        return "rebuild", plan.build_plan.reason
    if plan.build_plan.action == "skip":
        return "skip", "fine surface cache is current"
    if plan.build_plan.action == "rebake_missing_assets":
        return "skip", "fine surface cache is already present"
    if plan.build_plan.action == "refresh_noise_overlay_only":
        return "skip", "refresh does not rebuild surface"
    return "run", "full rebuild will materialize the surface"


def _pmtiles_action(plan: PrecomputePlan) -> tuple[str, str]:
    if plan.build_plan.action == "skip":
        return "skip", "PMTiles archives are current"
    if plan.build_plan.action == "rebake_missing_assets":
        missing = []
        if plan.build_plan.rebake_pmtiles_if_missing:
            missing.append("main")
        if plan.build_plan.rebake_noise_pmtiles_if_missing or plan.build_plan.rebake_noise_pmtiles_always:
            missing.append("noise")
        if not missing:
            return "skip", "PMTiles archives are current"
        return "rebake", f"missing {' and '.join(missing)} PMTiles archive(s)"
    if plan.build_plan.action == "refresh_noise_overlay_only":
        if plan.build_plan.rebake_pmtiles_if_missing or plan.build_plan.rebake_noise_pmtiles_always:
            return "rebake", "refresh will rebake missing PMTiles archives"
        return "skip", "refresh does not require PMTiles changes"
    if plan.build_plan.action in {"rebuild_full_pipeline", "rebuild_fine_surface", "continue"}:
        return "run", "full pipeline will handle PMTiles if needed"
    return "skip", "PMTiles decision not needed"


def _publish_action(plan: PrecomputePlan) -> tuple[str, str]:
    if plan.build_plan.action == "refresh_noise_overlay_only":
        return "refresh_noise_overlay_only", "refresh noise overlay without rebuilding the full pipeline"
    if plan.build_plan.action == "skip":
        return "skip", "complete build already exists"
    if plan.build_plan.action == "rebake_missing_assets":
        return "skip", "only missing PMTiles archives are rebaked"
    if plan.build_plan.action in {"rebuild_full_pipeline", "rebuild_fine_surface", "continue"}:
        return "run", "full publish will run"
    return "skip", "publish decision not needed"


def format_precompute_plan(plan: PrecomputePlan) -> str:
    surface_action, surface_reason = _surface_action(plan)
    pmtiles_action, pmtiles_reason = _pmtiles_action(plan)
    publish_action, publish_reason = _publish_action(plan)
    lines = [
        "Precompute plan",
        f"* import: {plan.import_plan.action}",
        f"  reason: {plan.import_plan.reason}",
        f"* noise artifact: {plan.noise_artifact_plan.action}",
        f"  reason: {plan.noise_artifact_plan.reason}",
        f"* build: {plan.build_plan.action}",
        f"  reason: {plan.build_plan.reason}",
        f"* surface: {surface_action}",
        f"  reason: {surface_reason}",
        f"* pmtiles: {pmtiles_action}",
        f"  reason: {pmtiles_reason}",
        f"* publish: {publish_action}",
        f"  reason: {publish_reason}",
    ]
    return "\n".join(lines)


def plan_import(context: PrecomputeContext) -> ImportPlan:
    if context.import_was_ready:
        return ImportPlan(action="skip", reason="raw OSM import already ready")
    if context.auto_refresh_import:
        return ImportPlan(action="refresh", reason="raw OSM import will be refreshed")
    return ImportPlan(action="fail", reason="raw OSM import is not ready")


def _noise_build_reason(context: PrecomputeContext) -> str:
    if not context.active_noise_artifact_exists:
        return "no active resolved artifact"
    if context.force_noise_all:
        return "--force-noise-all"
    if context.reimport_noise_source:
        return "--reimport-noise-source"
    if context.force_noise_artifact:
        return "--force-noise-artifact"
    return "--refresh-noise-artifact"


def plan_noise_artifact(context: PrecomputeContext) -> NoiseArtifactPlan:
    if context.noise_mode == "legacy":
        return NoiseArtifactPlan(
            action="legacy",
            reason="NOISE_MODE=legacy is the slow debug path",
        )
    if not context.engine_has_connect:
        if context.require_active_noise_artifact:
            return NoiseArtifactPlan(
                action="require_active",
                reason="engine does not expose connect()",
            )
        return NoiseArtifactPlan(
            action="skip",
            reason="engine does not expose connect()",
        )
    if context.require_active_noise_artifact:
        if context.active_noise_artifact_exists:
            return NoiseArtifactPlan(
                action="require_active",
                reason=f"active resolved artifact required and found (mode={context.noise_accuracy_mode})",
            )
        return NoiseArtifactPlan(
            action="require_active",
            reason=f"active resolved artifact required but missing (mode={context.noise_accuracy_mode})",
        )

    build_requested = (
        not context.active_noise_artifact_exists
        or context.refresh_noise_artifact
        or context.force_noise_artifact
        or context.force_noise_all
        or context.reimport_noise_source
    )
    if build_requested:
        force_resolved = bool(context.force_noise_artifact or context.force_noise_all)
        reimport_source = bool(context.reimport_noise_source or context.force_noise_all)
        if reimport_source:
            force_resolved = True
        return NoiseArtifactPlan(
            action="build",
            reason=_noise_build_reason(context),
            force_resolved=force_resolved,
            reimport_source=reimport_source,
        )
    return NoiseArtifactPlan(
        action="reuse",
        reason=f"active resolved artifact: (mode={context.noise_accuracy_mode})",
    )


def plan_build(context: PrecomputeContext) -> BuildPlan:
    if not context.has_complete_build or context.force_precompute:
        return BuildPlan(
            action="continue",
            reason="full pipeline will run",
        )

    if context.can_refresh_noise_only and (not context.noise_hash_matches or context.noise_refresh_requested):
        reason = (
            "refresh flags were used"
            if context.noise_refresh_requested
            else "selected noise artifact changed"
        )
        return BuildPlan(
            action="refresh_noise_overlay_only",
            reason=reason,
            rebake_pmtiles_if_missing=True,
            rebake_noise_pmtiles_always=True,
        )

    if not context.noise_hash_matches or context.noise_refresh_requested:
        return BuildPlan(
            action="rebuild_full_pipeline",
            reason="noise refresh cannot be isolated; rebuilding full pipeline.",
        )

    if not context.pmtiles_missing and not context.noise_pmtiles_missing and not context.surface_missing:
        return BuildPlan(
            action="skip",
            reason="complete PostGIS precompute already exists",
        )

    if not context.surface_missing:
        return BuildPlan(
            action="rebake_missing_assets",
            reason="PMTiles or noise PMTiles archive is missing",
            rebake_pmtiles_if_missing=context.pmtiles_missing,
            rebake_noise_pmtiles_if_missing=context.noise_pmtiles_missing,
        )

    return BuildPlan(
        action="rebuild_fine_surface",
        reason="fine surface cache is missing",
    )


def plan_precompute(context: PrecomputeContext) -> PrecomputePlan:
    import_plan = plan_import(context.import_context)
    noise_artifact_plan = plan_noise_artifact(context.noise_artifact_context)
    build_plan = plan_build(context.build_context)
    return PrecomputePlan(
        import_plan=import_plan,
        noise_artifact_plan=noise_artifact_plan,
        build_plan=build_plan,
    )
