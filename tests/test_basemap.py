from __future__ import annotations

import json
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import basemap
import main
from config import (
    BASEMAP_FONT_STACKS,
    PROTOMAPS_ASSETS_COMMIT,
    PROTOMAPS_BASEMAPS_COMMIT,
    PROTOMAPS_TILE_SCHEMA_MAJOR,
)


def _font_stacks_in_exported_style(value: object) -> set[str]:
    if isinstance(value, str):
        return {value} if value.startswith("Noto Sans ") else set()
    if isinstance(value, list):
        return set().union(*(_font_stacks_in_exported_style(item) for item in value))
    if isinstance(value, dict):
        return set().union(*(_font_stacks_in_exported_style(item) for item in value.values()))
    return set()


class BasemapTests(TestCase):
    def test_style_metadata_and_vendored_assets_match_pins(self) -> None:
        basemap.validate_vendored_assets()
        metadata = json.loads(basemap.BASEMAP_STYLE_METADATA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(metadata["protomaps_basemaps_commit"], PROTOMAPS_BASEMAPS_COMMIT)
        self.assertEqual(metadata["protomaps_assets_commit"], PROTOMAPS_ASSETS_COMMIT)
        self.assertEqual(metadata["tile_schema_major"], PROTOMAPS_TILE_SCHEMA_MAJOR)
        self.assertEqual(tuple(metadata["font_stacks"]), BASEMAP_FONT_STACKS)
        exported_style = json.loads(
            (basemap.BASE_DIR / "frontend" / "src" / "basemap_style.generated.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(_font_stacks_in_exported_style(exported_style), set(BASEMAP_FONT_STACKS))

    def test_docker_command_uses_pinned_compose_service(self) -> None:
        self.assertEqual(
            basemap.docker_command(force=True)[-4:],
            ["basemap", f"--area={basemap.BASEMAP_AREA_NAME}", "--download", "--force"],
        )

    def test_missing_docker_engine_has_actionable_message(self) -> None:
        with mock.patch.object(
            basemap.subprocess,
            "run",
            return_value=mock.Mock(returncode=1),
        ):
            with self.assertRaisesRegex(RuntimeError, "Start Docker Desktop"):
                basemap._require_docker_engine()

    def test_stale_when_archive_or_manifest_is_missing(self) -> None:
        with mock.patch.object(basemap, "BASEMAP_MANIFEST_PATH", Path("missing-manifest.json")), mock.patch.object(
            basemap, "basemap_pmtiles_output_path", return_value=Path("missing-basemap.pmtiles")
        ):
            self.assertTrue(basemap.is_stale())

    def test_stale_when_manifest_inputs_or_checksums_change(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            archive = temp / "basemap.pmtiles"
            manifest_path = temp / "manifest.json"
            archive.write_bytes(b"archive")
            manifest_path.write_text(
                json.dumps(
                    {
                        "input": {"source": "current"},
                        "output_checksums": {"basemap.pmtiles": basemap._sha256(archive)},
                        "vendored_asset_sha256": {"fonts/a.pbf": "asset"},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(basemap, "BASEMAP_MANIFEST_PATH", manifest_path), mock.patch.object(
                basemap, "basemap_pmtiles_output_path", return_value=archive
            ), mock.patch.object(basemap, "_manifest_input", return_value={"source": "current"}), mock.patch.object(
                basemap, "_vendored_asset_checksums", return_value={"fonts/a.pbf": "asset"}
            ):
                self.assertFalse(basemap.is_stale())
                manifest_path.write_text(
                    json.dumps({"input": {"source": "stale"}}), encoding="utf-8"
                )
                self.assertTrue(basemap.is_stale())

    def test_missing_message_names_the_build_command(self) -> None:
        self.assertIn("python main.py basemap", basemap.missing_basemap_message())

    def test_cli_basemap_dispatches_force_flag(self) -> None:
        with mock.patch.object(basemap, "build_basemap", return_value=Path("basemap.pmtiles")) as build_mock:
            with mock.patch("sys.stdout", new_callable=io.StringIO):
                exit_code = main.main(["basemap", "--force"])
        self.assertEqual(exit_code, 0)
        build_mock.assert_called_once_with(force=True)
