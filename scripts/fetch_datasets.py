"""Download every local input the pipeline needs.

Shared by setup.sh and setup.ps1 so the dataset list lives in one place. Uses
only the standard library: it runs before the project virtualenv exists.

Source order per dataset:
  1. an explicit per-dataset URL environment variable
  2. the dataset mirror (DATASET_MIRROR_BASE_URL, default: project release v2)
  3. the official upstream URL, when one is reliably downloadable

With --refresh, datasets that change upstream (OSM, GTFS) are re-downloaded and
their official source is tried before the mirror so the result is current.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIRROR_BASE_URL = "https://github.com/SidneyMe/livability-data/releases/download/v2"
USER_AGENT = "livability-setup/1.0 (+https://github.com/SidneyMe/Island-of-Ireland-Livability-Map-Alpha)"
_CHUNK_BYTES = 1024 * 1024
_PROGRESS_EVERY_BYTES = 64 * 1024 * 1024
_RETRIES = 3


@dataclass(frozen=True)
class Dataset:
    key: str
    label: str
    target: str
    mirror_name: str
    url_env: str
    official_urls: tuple[str, ...] = ()
    refreshable: bool = False
    is_zip: bool = False
    extract_to: str | None = None
    extracted_marker: str | None = None
    target_env: str | None = None

    def target_path(self, root: Path) -> Path:
        override = (os.getenv(self.target_env) or "").strip() if self.target_env else ""
        path = Path(override) if override else Path(self.target)
        return path if path.is_absolute() else root / path

    def ready_path(self, root: Path) -> Path:
        # Archives that are unpacked in place are "ready" once their marker exists.
        if self.extracted_marker:
            return root / self.extracted_marker
        return self.target_path(root)


DATASETS: tuple[Dataset, ...] = (
    Dataset(
        key="osm",
        label="OSM extract (Ireland + Northern Ireland)",
        target="osm/ireland-and-northern-ireland-latest.osm.pbf",
        mirror_name="ireland-and-northern-ireland-latest.osm.pbf",
        url_env="OSM_URL",
        official_urls=("https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf",),
        refreshable=True,
    ),
    Dataset(
        key="roi_boundary",
        label="Republic of Ireland boundary",
        target="boundaries/Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson",
        mirror_name="Counties_NationalStatutoryBoundaries_Ungeneralised_2024_-6732842875837866666.geojson",
        url_env="ROI_BOUNDARY_URL",
        official_urls=(
            "https://data-osi.opendata.arcgis.com/api/download/v1/items/dc24df2a5ce84ee9a38d9afe8431ee9b/geojson?layers=1",
        ),
    ),
    Dataset(
        key="ni_boundary",
        label="Northern Ireland boundary",
        target="boundaries/osni_open_data_largescale_boundaries_ni_outline.geojson",
        mirror_name="osni_open_data_largescale_boundaries_ni_outline.geojson",
        url_env="NI_BOUNDARY_URL",
        # OSNI portals reset/deny unattended downloads; the mirror is the
        # reliable source and ArcGIS Hub is a last resort.
        official_urls=(
            "https://hub.arcgis.com/api/v3/datasets/159c80fe1ad54140b429f8799f624962_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
        ),
    ),
    Dataset(
        key="gtfs_nta",
        label="NTA GTFS feed",
        target="gtfs/nta_gtfs.zip",
        mirror_name="nta_gtfs.zip",
        url_env="GTFS_NTA_URL",
        official_urls=("https://www.transportforireland.ie/transitData/Data/GTFS_All.zip",),
        refreshable=True,
        is_zip=True,
        target_env="GTFS_NTA_ZIP_PATH",
    ),
    Dataset(
        key="gtfs_translink",
        label="Translink GTFS feed",
        target="gtfs/translink_gtfs.zip",
        mirror_name="translink_gtfs.zip",
        url_env="GTFS_TRANSLINK_URL",
        refreshable=True,
        is_zip=True,
        target_env="GTFS_TRANSLINK_ZIP_PATH",
    ),
    Dataset(
        key="overture",
        label="Overture Places dataset",
        target="overture/ireland_places.geoparquet",
        mirror_name="ireland_places.geoparquet",
        url_env="OVERTURE_PLACES_URL",
    ),
    Dataset(
        key="overture_state",
        label="Overture Places release state",
        target="overture/ireland_places.geoparquet.state",
        mirror_name="ireland_places.geoparquet.state",
        url_env="OVERTURE_PLACES_STATE_URL",
    ),
    Dataset(
        key="main_island",
        label="Main-island coastline shapefile",
        target="ireland_main_island_shp/ireland_main_island.zip",
        mirror_name="ireland_main_island.zip",
        url_env="MAIN_ISLAND_BOUNDARY_ARCHIVE_URL",
        is_zip=True,
        extract_to="ireland_main_island_shp",
        extracted_marker="ireland_main_island_shp/ireland_main_island.shp",
    ),
    Dataset(
        key="noise_roi_round4",
        label="ROI Round 4 noise archive (EPA)",
        target="noise_datasets/NOISE_Round4.zip",
        mirror_name="NOISE_Round4.zip",
        url_env="NOISE_ROUND4_URL",
        is_zip=True,
    ),
    Dataset(
        key="noise_roi_round3",
        label="ROI Round 3 noise archive (EPA)",
        target="noise_datasets/NOISE_Round3.zip",
        mirror_name="NOISE_Round3.zip",
        url_env="NOISE_ROUND3_URL",
        is_zip=True,
    ),
    Dataset(
        key="noise_roi_round2",
        label="ROI Round 2 noise archive (EPA)",
        target="noise_datasets/NOISE_Round2.zip",
        mirror_name="NOISE_Round2.zip",
        url_env="NOISE_ROUND2_URL",
        is_zip=True,
    ),
    Dataset(
        key="noise_ni_round3",
        label="NI Round 3 noise archive (DAERA)",
        target="noise_datasets/end_noisedata_round3.zip",
        mirror_name="end_noisedata_round3.zip",
        url_env="NOISE_NI_ROUND3_URL",
        is_zip=True,
    ),
    Dataset(
        key="noise_ni_round2",
        label="NI Round 2 noise archive (DAERA)",
        target="noise_datasets/end_noisedata_round2.zip",
        mirror_name="end_noisedata_round2.zip",
        url_env="NOISE_NI_ROUND2_URL",
        is_zip=True,
    ),
    Dataset(
        key="noise_ni_round1",
        label="NI Round 1 noise archive (DAERA)",
        target="noise_datasets/end_noisedata_round1.zip",
        mirror_name="end_noisedata_round1.zip",
        url_env="NOISE_NI_ROUND1_URL",
        is_zip=True,
    ),
)


def mirror_base_url() -> str:
    raw = os.getenv("DATASET_MIRROR_BASE_URL")
    if raw is None:
        return DEFAULT_MIRROR_BASE_URL
    return raw.strip().rstrip("/")


def candidate_urls(dataset: Dataset, *, refresh: bool = False) -> list[str]:
    explicit = (os.getenv(dataset.url_env) or "").strip()
    if explicit:
        return [explicit]
    mirror = mirror_base_url()
    mirror_urls = [f"{mirror}/{dataset.mirror_name}"] if mirror else []
    official = list(dataset.official_urls)
    ordered = official + mirror_urls if refresh and dataset.refreshable else mirror_urls + official
    return list(dict.fromkeys(ordered))


def _default_download(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as handle:
        written = 0
        next_report = _PROGRESS_EVERY_BYTES
        while True:
            chunk = response.read(_CHUNK_BYTES)
            if not chunk:
                break
            handle.write(chunk)
            written += len(chunk)
            if written >= next_report:
                print(f"    ... {written // (1024 * 1024):,} MiB", flush=True)
                next_report += _PROGRESS_EVERY_BYTES


def _validate(dataset: Dataset, path: Path) -> None:
    if path.stat().st_size == 0:
        raise ValueError("downloaded file is empty")
    if dataset.is_zip and not zipfile.is_zipfile(path):
        raise ValueError("downloaded file is not a valid zip archive")


def _extract(dataset: Dataset, root: Path) -> None:
    archive = dataset.target_path(root)
    destination = root / (dataset.extract_to or "")
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(destination)
    if dataset.extracted_marker and not (root / dataset.extracted_marker).is_file():
        raise ValueError(f"{archive.name} did not contain {dataset.extracted_marker}")


def fetch_dataset(
    dataset: Dataset,
    *,
    root: Path = PROJECT_ROOT,
    refresh: bool = False,
    download: Callable[[str, Path], None] = _default_download,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Make one dataset present locally; return 'reused' or 'downloaded'.

    Raises RuntimeError when every configured source fails.
    """
    ready = dataset.ready_path(root)
    if ready.is_file() and ready.stat().st_size > 0 and not (refresh and dataset.refreshable):
        print(f"Reusing {dataset.label}: {ready.relative_to(root) if ready.is_relative_to(root) else ready}")
        return "reused"

    urls = candidate_urls(dataset, refresh=refresh)
    if not urls:
        raise RuntimeError(
            f"No download source for {dataset.label}. Set {dataset.url_env} or DATASET_MIRROR_BASE_URL, "
            f"or place the file at {dataset.target}."
        )

    target = dataset.target_path(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".part")
    errors: list[str] = []
    for url in urls:
        for attempt in range(1, _RETRIES + 1):
            print(f"Downloading {dataset.label} (attempt {attempt}/{_RETRIES}) from {url}", flush=True)
            try:
                download(url, partial)
                _validate(dataset, partial)
                os.replace(partial, target)
                if dataset.extract_to:
                    _extract(dataset, root)
                return "downloaded"
            except Exception as exc:  # network, HTTP, and validation errors all retry
                errors.append(f"{url}: {exc}")
                partial.unlink(missing_ok=True)
                if attempt < _RETRIES:
                    sleep(2.0 * attempt)
    raise RuntimeError(f"Download failed for {dataset.label}:\n  " + "\n  ".join(errors))


def select_datasets(keys: Sequence[str] | None) -> list[Dataset]:
    if not keys:
        return list(DATASETS)
    by_key = {dataset.key: dataset for dataset in DATASETS}
    unknown = [key for key in keys if key not in by_key]
    if unknown:
        raise SystemExit(f"Unknown dataset key(s): {', '.join(unknown)}. Known: {', '.join(by_key)}")
    return [by_key[key] for key in keys]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download all local datasets used by the livability pipeline.")
    parser.add_argument("--refresh", action="store_true", help="Re-download OSM and GTFS from their official sources.")
    parser.add_argument("--only", nargs="+", metavar="KEY", help="Fetch only these dataset keys.")
    parser.add_argument("--list", action="store_true", help="List dataset keys, targets, and sources, then exit.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    datasets = select_datasets(args.only)
    if args.list:
        for dataset in datasets:
            print(f"{dataset.key:18s} {dataset.target}")
            for url in candidate_urls(dataset, refresh=args.refresh):
                print(f"{'':18s}   <- {url}")
        return 0

    failures: list[str] = []
    for dataset in datasets:
        try:
            fetch_dataset(dataset, refresh=args.refresh)
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr, flush=True)
            failures.append(dataset.key)
    if failures:
        print(f"Missing datasets: {', '.join(failures)}", file=sys.stderr)
        return 1
    print("All datasets are present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
