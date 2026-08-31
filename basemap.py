"""Build and validate the locally served Protomaps basemap archive."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    BASEMAP_CACHE_DIR,
    BASEMAP_FONT_STACKS,
    BASE_DIR,
    OSM_EXTRACT_PATH,
    PROTOMAPS_ASSETS_COMMIT,
    PROTOMAPS_BASEMAPS_COMMIT,
    PROTOMAPS_TILE_SCHEMA_MAJOR,
    basemap_pmtiles_output_path,
)


BASEMAP_AREA_NAME = "ireland-and-northern-ireland-latest"
BASEMAP_MANIFEST_PATH = BASEMAP_CACHE_DIR / "manifest.json"
BASEMAP_STYLE_METADATA_PATH = BASE_DIR / "frontend" / "src" / "basemap_style_metadata.json"
BASEMAP_ASSETS_DIR = BASE_DIR / "static" / "basemap"
BASEMAP_COMPOSE_PATH = BASE_DIR / "basemap" / "docker-compose.yml"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _style_metadata() -> dict[str, Any]:
    try:
        return json.loads(BASEMAP_STYLE_METADATA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Basemap style metadata is unavailable at {BASEMAP_STYLE_METADATA_PATH}."
        ) from exc


def validate_vendored_assets() -> None:
    metadata = _style_metadata()
    if metadata.get("protomaps_basemaps_commit") != PROTOMAPS_BASEMAPS_COMMIT:
        raise RuntimeError("Basemap style metadata does not match PROTOMAPS_BASEMAPS_COMMIT.")
    if metadata.get("protomaps_assets_commit") != PROTOMAPS_ASSETS_COMMIT:
        raise RuntimeError("Basemap style metadata does not match PROTOMAPS_ASSETS_COMMIT.")
    if metadata.get("tile_schema_major") != PROTOMAPS_TILE_SCHEMA_MAJOR:
        raise RuntimeError("Basemap style metadata does not match the tile schema major version.")
    if tuple(metadata.get("font_stacks", ())) != BASEMAP_FONT_STACKS:
        raise RuntimeError("Basemap style metadata does not match the referenced font stacks.")

    expected_ranges = {f"{start}-{start + 255}.pbf" for start in range(0, 65536, 256)}
    for font_stack in BASEMAP_FONT_STACKS:
        ranges = {path.name for path in (BASEMAP_ASSETS_DIR / "fonts" / font_stack).glob("*.pbf")}
        if ranges != expected_ranges:
            raise RuntimeError(
                f"Basemap font stack {font_stack!r} is incomplete: expected all 256 glyph ranges, found {len(ranges)}."
            )
    for suffix in (".json", ".png", "@2x.json", "@2x.png"):
        sprite = BASEMAP_ASSETS_DIR / "sprites" / "v4" / f"light{suffix}"
        if not sprite.is_file():
            raise RuntimeError(f"Basemap sprite asset is missing: {sprite}")


def source_fingerprint(path: Path = OSM_EXTRACT_PATH) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"Basemap source PBF is missing: {path}")
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "modified_at_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "sha256": _sha256(path),
    }


def _manifest_input() -> dict[str, Any]:
    return {
        "source": source_fingerprint(),
        "protomaps_basemaps_commit": PROTOMAPS_BASEMAPS_COMMIT,
        "protomaps_assets_commit": PROTOMAPS_ASSETS_COMMIT,
        "tile_schema_major": PROTOMAPS_TILE_SCHEMA_MAJOR,
    }


def load_manifest() -> dict[str, Any] | None:
    try:
        payload = json.loads(BASEMAP_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def is_stale() -> bool:
    archive = basemap_pmtiles_output_path()
    manifest = load_manifest()
    if not archive.is_file() or manifest is None:
        return True
    try:
        output_checksums = manifest.get("output_checksums", {})
        return (
            manifest.get("input") != _manifest_input()
            or output_checksums.get("basemap.pmtiles") != _sha256(archive)
            or manifest.get("vendored_asset_sha256") != _vendored_asset_checksums()
        )
    except OSError:
        return True


def missing_basemap_message() -> str:
    return f"Basemap artifact missing. Run: python main.py basemap (expected {basemap_pmtiles_output_path()})."


def docker_command(*, force: bool = False) -> list[str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(BASEMAP_COMPOSE_PATH),
        "run",
        "--rm",
        "--build",
        "basemap",
        f"--area={BASEMAP_AREA_NAME}",
        "--download",
    ]
    if force:
        command.append("--force")
    return command


def _require_docker_engine() -> None:
    """Raise an actionable error before Compose emits a platform-specific pipe error."""
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Docker is required to build the local basemap. Install Docker Desktop, "
            "start it, then rerun: python main.py basemap"
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(
            "Docker Desktop's Linux engine is unavailable. Start Docker Desktop and wait "
            "until it reports 'Engine running' (use Linux containers), verify `docker info` "
            "works, then rerun: python main.py basemap"
        )


def _stage_pbf() -> Path:
    source = OSM_EXTRACT_PATH.resolve()
    target = BASEMAP_CACHE_DIR / "work" / "data" / "sources" / source.name
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and source_fingerprint(target)["sha256"] == source_fingerprint(source)["sha256"]:
        return target
    target.unlink(missing_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)
    return target


def _image_identity() -> str | None:
    image_name = f"livability-protomaps-basemap:{PROTOMAPS_BASEMAPS_COMMIT[:12]}"
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image_name],
        text=True,
        capture_output=True,
        check=False,
    )
    value = result.stdout.strip()
    return value or None


def _ancillary_checksums() -> dict[str, str]:
    source_root = BASEMAP_CACHE_DIR / "work" / "data" / "sources"
    if not source_root.is_dir():
        return {}
    pbf_name = OSM_EXTRACT_PATH.name
    return {
        str(path.relative_to(source_root).as_posix()): _sha256(path)
        for path in sorted(source_root.rglob("*"))
        if path.is_file() and path.name != pbf_name
    }


def _vendored_asset_checksums() -> dict[str, str]:
    return {
        str(path.relative_to(BASEMAP_ASSETS_DIR).as_posix()): _sha256(path)
        for path in sorted(BASEMAP_ASSETS_DIR.rglob("*"))
        if path.is_file()
    }


def build_basemap(*, force: bool = False) -> Path:
    validate_vendored_assets()
    if not force and not is_stale():
        return basemap_pmtiles_output_path()
    _require_docker_engine()
    _stage_pbf()
    BASEMAP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            docker_command(force=True),
            check=True,
            env={
                **os.environ,
                "PROTOMAPS_BASEMAPS_COMMIT": PROTOMAPS_BASEMAPS_COMMIT,
                "PROTOMAPS_BASEMAPS_COMMIT_SHORT": PROTOMAPS_BASEMAPS_COMMIT[:12],
            },
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker Compose is required to build the local basemap.") from exc
    generated = BASEMAP_CACHE_DIR / "work" / f"{BASEMAP_AREA_NAME}.pmtiles"
    if not generated.is_file():
        raise RuntimeError(f"Basemap builder completed without producing {generated}.")
    output = basemap_pmtiles_output_path()
    generated.replace(output)
    manifest = {
        "input": _manifest_input(),
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "docker_image_digest": _image_identity(),
        "ancillary_input_sha256": _ancillary_checksums(),
        "output_checksums": {"basemap.pmtiles": _sha256(output)},
        "vendored_asset_sha256": _vendored_asset_checksums(),
    }
    BASEMAP_MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return output


def build_if_stale() -> Path | None:
    if is_stale():
        return build_basemap()
    return None
