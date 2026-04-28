from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


NOISE_DATA_DIR = Path(__file__).resolve().parent.parent / "noise_datasets"
NOISE_DATASET_VERSION = 2


@dataclass(frozen=True)
class _RoiSourceSpec:
    round_number: int
    zip_name: str
    source_type: str
    member: str
    file_format: str


ROI_SOURCE_SPECS: tuple[_RoiSourceSpec, ...] = (
    _RoiSourceSpec(4, "NOISE_Round4.zip", "airport", "Noise R4 DataDownload/Noise_R4_Airport.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "industry", "Noise R4 DataDownload/Noise_R4_Industry.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "rail", "Noise R4 DataDownload/Noise_R4_Rail.shp", "shp"),
    _RoiSourceSpec(4, "NOISE_Round4.zip", "road", "Noise R4 DataDownload/Noise_R4_Road.gdb", "gdb"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "airport", "Airport/NOISE_Rd3_Airport.shp", "shp"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "rail", "Rail/NOISE_Rd3_Rail.shp", "shp"),
    _RoiSourceSpec(3, "NOISE_Round3.zip", "road", "Road/NOISE_Rd3_Road.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "airport", "NOISE_Rd2_Airport.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "rail", "NOISE_Rd2_Rail.shp", "shp"),
    _RoiSourceSpec(2, "NOISE_Round2.zip", "road", "NOISE_Rd2_Road.shp", "shp"),
)

NI_ZIP_BY_ROUND = {
    3: "end_noisedata_round3.zip",
    2: "end_noisedata_round2.zip",
    1: "end_noisedata_round1.zip",
}


def _file_meta(path: Path) -> dict[str, int]:
    try:
        stat = path.stat()
        return {"mtime_ns": int(stat.st_mtime_ns), "size": int(stat.st_size)}
    except OSError:
        return {"mtime_ns": 0, "size": 0}


def _file_signature(path: Path) -> dict[str, Any]:
    meta = _file_meta(path)
    return {"path": str(path), "mtime_ns": meta["mtime_ns"], "size": meta["size"]}


def dataset_info(data_dir: Path = NOISE_DATA_DIR) -> dict[str, Any]:
    expected_files = sorted(
        {spec.zip_name for spec in ROI_SOURCE_SPECS}.union(NI_ZIP_BY_ROUND.values())
    )
    files = {}
    for file_name in expected_files:
        path = Path(data_dir) / file_name
        meta = _file_meta(path)
        files[file_name] = {
            "available": path.exists(),
            "file_size": meta["size"],
            "file_mtime_ns": meta["mtime_ns"],
        }
    return {
        "version": NOISE_DATASET_VERSION,
        "available": any(file_info["available"] for file_info in files.values()),
        "data_dir": str(Path(data_dir)),
        "files": files,
    }


def dataset_signature(data_dir: Path = NOISE_DATA_DIR) -> str:
    info = dataset_info(data_dir)
    signature_payload = {
        "version": info["version"],
        "files": info["files"],
    }
    return hashlib.sha256(
        json.dumps(signature_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]
