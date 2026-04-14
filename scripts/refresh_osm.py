from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import OSM_DIR, OSM_EXTRACT_PATH

GEOFABRIK_URL = (
    "https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf"
)
GEOFABRIK_MD5_URL = GEOFABRIK_URL + ".md5"
EXTRACT_MANIFEST_NAME = "extract_manifest.json"

_DOWNLOAD_CHUNK_BYTES = 1024 * 1024  # 1 MB
_HASH_CHUNK_BYTES = 64 * 1024        # 64 KB


def load_extract_manifest(manifest_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _compute_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _fetch_md5(md5_url: str) -> str | None:
    try:
        with urllib.request.urlopen(md5_url, timeout=30) as response:
            content = response.read().decode("ascii", errors="replace").strip()
            return content.split()[0] if content else None
    except Exception:
        return None


def download_extract(
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    manifest_path = OSM_DIR / EXTRACT_MANIFEST_NAME
    existing_manifest = load_extract_manifest(manifest_path)

    request = urllib.request.Request(GEOFABRIK_URL)
    if not force and existing_manifest and existing_manifest.get("extract_date"):
        request.add_header("If-Modified-Since", existing_manifest["extract_date"])

    if dry_run:
        if existing_manifest and existing_manifest.get("extract_date") and not force:
            print(
                f"dry-run: would request {GEOFABRIK_URL} "
                f"with If-Modified-Since: {existing_manifest['extract_date']}"
            )
        else:
            print(f"dry-run: would download {GEOFABRIK_URL} to {OSM_EXTRACT_PATH}")
        return {"skipped": True, "reason": "dry-run", "manifest": existing_manifest}

    try:
        response = urllib.request.urlopen(request, timeout=600)
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return {"skipped": True, "reason": "not-modified", "manifest": existing_manifest}
        raise

    last_modified: str | None = response.headers.get("Last-Modified")
    downloaded_utc = datetime.now(timezone.utc).isoformat()

    tmp_path = OSM_EXTRACT_PATH.with_name(OSM_EXTRACT_PATH.name + ".tmp")
    try:
        with tmp_path.open("wb") as fh:
            while True:
                chunk = response.read(_DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                fh.write(chunk)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    sha256_hex = _compute_sha256(tmp_path)

    remote_md5 = _fetch_md5(GEOFABRIK_MD5_URL)
    if remote_md5 is not None:
        local_md5 = _compute_md5(tmp_path)
        if local_md5 != remote_md5:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise RuntimeError(
                f"Checksum mismatch for downloaded extract: "
                f"expected MD5 {remote_md5}, got {local_md5}. "
                f"The download may be corrupt; the existing extract was not replaced."
            )

    os.replace(tmp_path, OSM_EXTRACT_PATH)

    manifest: dict[str, Any] = {
        "extract_date": last_modified,
        "source_url": GEOFABRIK_URL,
        "downloaded_utc": downloaded_utc,
        "sha256": sha256_hex,
    }
    manifest_tmp = manifest_path.with_name(EXTRACT_MANIFEST_NAME + ".tmp")
    manifest_tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    os.replace(manifest_tmp, manifest_path)

    return {"skipped": False, "reason": "downloaded", "manifest": manifest}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download the latest Ireland + NI OSM extract from Geofabrik. "
            "Skips the download if the remote file has not changed since the last run."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without downloading or writing any files.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download unconditionally, ignoring any cached extract date.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = download_extract(force=args.force, dry_run=args.dry_run)
    except (OSError, RuntimeError, urllib.error.URLError) as exc:
        print(f"Error: {exc}")
        return 1

    reason = result["reason"]
    if reason == "downloaded":
        manifest = result["manifest"] or {}
        extract_date = manifest.get("extract_date") or "unknown date"
        print(f"Downloaded: {OSM_EXTRACT_PATH} (extract date: {extract_date})")
    elif reason == "not-modified":
        print(f"Already up to date: {OSM_EXTRACT_PATH}")
    # dry-run message is printed inside download_extract

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
