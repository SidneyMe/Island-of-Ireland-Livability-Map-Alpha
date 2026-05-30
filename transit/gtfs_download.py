from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from io import TextIOWrapper
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

from config import TransitFeedConfig, gtfs_static_feed_configs


USER_AGENT = "Island-of-Ireland-Livability-Map/gtfs-updater contact=local"
CONNECT_TIMEOUT_SECONDS = 15
READ_TIMEOUT_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = CONNECT_TIMEOUT_SECONDS + READ_TIMEOUT_SECONDS
MAX_ATTEMPTS = 3
FRESHNESS_SECONDS = 24 * 60 * 60
GTFS_STALE_IF_ENDS_WITHIN_DAYS = max(
    0,
    int(os.getenv("GTFS_STALE_IF_ENDS_WITHIN_DAYS", "7")),
)

REQUIRED_STATIC_GTFS_FILES = (
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
)
CALENDAR_FILES = ("calendar.txt", "calendar_dates.txt")
OPTIONAL_GTFS_FILES = ("feed_info.txt", "shapes.txt", "transfers.txt")


@dataclass(frozen=True)
class FeedRefreshResult:
    feed_id: str
    changed: bool
    sha256: str | None
    content_length: int | None
    zip_valid: bool
    calendar_min_date: str | None
    calendar_max_date: str | None
    path: Path
    reason: str


@dataclass(frozen=True)
class FeedRefreshStatus:
    feed_id: str
    path: Path
    sha256: str | None
    downloaded_at_utc: str | None
    checked_at_utc: str | None
    etag: str | None
    last_modified: str | None
    calendar_min_date: str | None
    calendar_max_date: str | None
    days_until_calendar_end: int | None
    cache_age_hours: float | None
    freshness_decision: str
    network_request: bool
    freshness_reason: str
    stale_by_calendar: bool
    stale_by_age: bool
    force_refresh: bool
    calendar_unknown: bool
    manifest_path: Path


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _emit(progress_cb, detail: str) -> None:
    if progress_cb is None:
        print(detail, flush=True)
        return
    progress_cb("detail", detail=detail, force_log=True)


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _iso_to_epoch_seconds(value: str | None) -> float | None:
    normalized = (value or "").strip()
    if not normalized:
        return None
    try:
        dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.timestamp()


def _http_date_to_iso(value: str | None) -> str | None:
    normalized = (value or "").strip()
    if not normalized:
        return None
    try:
        dt = parsedate_to_datetime(normalized)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_yyyymmdd(value: str | None) -> date | None:
    raw = (value or "").strip()
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _days_until_calendar_end(
    calendar_max_date: str | None,
    *,
    today_utc: date,
) -> int | None:
    parsed = _parse_yyyymmdd(calendar_max_date)
    if parsed is None:
        return None
    return (parsed - today_utc).days


def _cache_age_hours(manifest: dict[str, Any], *, now_epoch: float) -> float | None:
    checked_at_epoch = (
        _iso_to_epoch_seconds(str(manifest.get("checked_at_utc") or ""))
        or _iso_to_epoch_seconds(str(manifest.get("downloaded_at_utc") or ""))
    )
    if checked_at_epoch is None:
        return None
    return max(0.0, (now_epoch - checked_at_epoch) / 3600.0)


def _build_refresh_status(
    feed_config: TransitFeedConfig,
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
    force: bool,
    now_epoch: float,
    today_utc: date,
) -> FeedRefreshStatus:
    current_zip_exists = feed_config.zip_path.exists()
    has_manifest = bool(manifest)
    calendar_max_date = manifest.get("calendar_max_date") if has_manifest else None
    days_until_calendar_end = _days_until_calendar_end(calendar_max_date, today_utc=today_utc)
    stale_by_calendar_expired = days_until_calendar_end is not None and days_until_calendar_end < 0
    stale_by_calendar_near_expiry = (
        days_until_calendar_end is not None and 0 <= days_until_calendar_end <= GTFS_STALE_IF_ENDS_WITHIN_DAYS
    )
    stale_by_calendar = stale_by_calendar_expired or stale_by_calendar_near_expiry
    calendar_unknown = days_until_calendar_end is None
    cache_age_hours = _cache_age_hours(manifest, now_epoch=now_epoch) if has_manifest else None
    cache_fresh_by_age = (
        cache_age_hours is not None and cache_age_hours < (FRESHNESS_SECONDS / 3600.0)
    )

    if force:
        freshness_decision = "forced"
        freshness_reason = "forced_refresh"
        network_request = True
        stale_by_age = False
    elif not current_zip_exists:
        freshness_decision = "stale_by_age"
        freshness_reason = "cache_missing_current_zip"
        network_request = True
        stale_by_age = True
    elif not has_manifest:
        freshness_decision = "stale_by_age"
        freshness_reason = "cache_manifest_missing"
        network_request = True
        stale_by_age = True
    elif stale_by_calendar_expired:
        freshness_decision = "stale_by_calendar_expired"
        freshness_reason = "calendar_expired"
        network_request = True
        stale_by_age = False
    elif stale_by_calendar_near_expiry:
        freshness_decision = "stale_by_calendar_near_expiry"
        freshness_reason = "calendar_near_expiry"
        network_request = True
        stale_by_age = False
    elif cache_fresh_by_age:
        freshness_decision = "fresh"
        freshness_reason = (
            "cache_age_within_ttl_calendar_unknown" if calendar_unknown else "cache_age_within_ttl"
        )
        network_request = False
        stale_by_age = False
    else:
        freshness_decision = "stale_by_age"
        freshness_reason = "cache_age_exceeds_ttl_or_unknown"
        network_request = True
        stale_by_age = True

    return FeedRefreshStatus(
        feed_id=feed_config.feed_id,
        path=feed_config.zip_path,
        sha256=(manifest.get("sha256") or "").strip() or None,
        downloaded_at_utc=(manifest.get("downloaded_at_utc") or None),
        checked_at_utc=(manifest.get("checked_at_utc") or None),
        etag=(manifest.get("etag") or None),
        last_modified=(manifest.get("last_modified") or None),
        calendar_min_date=(manifest.get("calendar_min_date") or None),
        calendar_max_date=(calendar_max_date or None),
        days_until_calendar_end=days_until_calendar_end,
        cache_age_hours=cache_age_hours,
        freshness_decision=freshness_decision,
        network_request=network_request,
        freshness_reason=freshness_reason,
        stale_by_calendar=stale_by_calendar,
        stale_by_age=stale_by_age,
        force_refresh=bool(force),
        calendar_unknown=calendar_unknown,
        manifest_path=manifest_path,
    )


def _manifest_with_status_fields(
    manifest: dict[str, Any],
    *,
    status: FeedRefreshStatus,
    now_iso: str,
    changed: bool | None = None,
    last_network_check: bool = False,
    warning: str | None = None,
) -> dict[str, Any]:
    payload = dict(manifest)
    payload["days_until_calendar_end"] = status.days_until_calendar_end
    payload["freshness_reason"] = status.freshness_reason
    payload["stale_by_calendar"] = status.stale_by_calendar
    payload["stale_by_age"] = status.stale_by_age
    payload["force_refresh"] = status.force_refresh
    payload["last_refresh_decision"] = status.freshness_decision
    payload["calendar_unknown"] = status.calendar_unknown
    payload["checked_at_utc"] = now_iso
    if changed is not None:
        payload["changed"] = bool(changed)
    if last_network_check:
        payload["last_network_check_at_utc"] = now_iso
    if warning:
        payload["warning"] = warning
    elif "warning" in payload:
        payload.pop("warning", None)
    return payload


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _optional_member(zip_file: ZipFile, expected_name: str) -> str | None:
    expected = expected_name.lower()
    for member in zip_file.namelist():
        if member.lower().endswith(expected):
            return member
    return None


def _read_first_csv_row(zip_file: ZipFile, member_name: str) -> dict[str, str] | None:
    with zip_file.open(member_name, "r") as handle:
        wrapper = TextIOWrapper(handle, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(wrapper)
        for row in reader:
            return {
                str(key or "").strip(): (value or "").strip()
                for key, value in row.items()
            }
    return None


def _calendar_range_from_zip(zip_file: ZipFile) -> tuple[str | None, str | None]:
    min_date: str | None = None
    max_date: str | None = None

    calendar_member = _optional_member(zip_file, "calendar.txt")
    if calendar_member is not None:
        with zip_file.open(calendar_member, "r") as handle:
            wrapper = TextIOWrapper(handle, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(wrapper)
            for row in reader:
                start_date = (row.get("start_date") or "").strip()
                end_date = (row.get("end_date") or "").strip()
                for raw in (start_date, end_date):
                    if len(raw) != 8 or not raw.isdigit():
                        continue
                    if min_date is None or raw < min_date:
                        min_date = raw
                    if max_date is None or raw > max_date:
                        max_date = raw

    calendar_dates_member = _optional_member(zip_file, "calendar_dates.txt")
    if calendar_dates_member is not None:
        with zip_file.open(calendar_dates_member, "r") as handle:
            wrapper = TextIOWrapper(handle, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(wrapper)
            for row in reader:
                service_date = (row.get("date") or "").strip()
                if len(service_date) != 8 or not service_date.isdigit():
                    continue
                if min_date is None or service_date < min_date:
                    min_date = service_date
                if max_date is None or service_date > max_date:
                    max_date = service_date

    return min_date, max_date


def _validate_gtfs_zip(path: Path) -> dict[str, Any]:
    try:
        with ZipFile(path, "r") as zip_file:
            names = zip_file.namelist()
            basenames = sorted(
                {
                    Path(name).name
                    for name in names
                    if Path(name).name
                }
            )
            lower_name_set = {name.lower() for name in basenames}
            missing = [
                required
                for required in REQUIRED_STATIC_GTFS_FILES
                if required not in lower_name_set
            ]
            has_calendar = any(name in lower_name_set for name in CALENDAR_FILES)
            if not has_calendar:
                missing.append("calendar.txt|calendar_dates.txt")
            if missing:
                raise RuntimeError(
                    "GTFS zip is missing required files: " + ", ".join(missing)
                )

            feed_info_member = _optional_member(zip_file, "feed_info.txt")
            feed_info = (
                _read_first_csv_row(zip_file, feed_info_member)
                if feed_info_member is not None
                else None
            )
            feed_info_end_date = None
            if isinstance(feed_info, dict):
                candidate = (feed_info.get("feed_end_date") or "").strip()
                if candidate:
                    feed_info_end_date = candidate
            calendar_min_date, calendar_max_date = _calendar_range_from_zip(zip_file)
            total_uncompressed_size = sum(info.file_size for info in zip_file.infolist())

            return {
                "zip_valid": True,
                "gtfs_files": basenames,
                "feed_info": feed_info,
                "feed_info_end_date": feed_info_end_date,
                "calendar_min_date": calendar_min_date,
                "calendar_max_date": calendar_max_date,
                "total_uncompressed_size": int(total_uncompressed_size),
                "optional_files_present": [
                    file_name
                    for file_name in OPTIONAL_GTFS_FILES
                    if file_name in lower_name_set
                ],
            }
    except BadZipFile as exc:
        raise RuntimeError("Downloaded file is not a valid ZIP archive.") from exc


def _copy_to_downloads_cache(
    file_path: Path,
    *,
    downloads_dir: Path,
    sha256: str,
) -> None:
    downloads_dir.mkdir(parents=True, exist_ok=True)
    target = downloads_dir / f"{sha256}.zip"
    if target.exists():
        return
    temp_target = target.with_suffix(".zip.tmp")
    shutil.copy2(file_path, temp_target)
    temp_target.replace(target)


def _request_with_retries(
    request: urllib.request.Request,
    *,
    destination_tmp_path: Path,
) -> tuple[int, dict[str, Any]]:
    destination_tmp_path.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                status = int(getattr(response, "status", 200))
                with destination_tmp_path.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                headers = {key.lower(): value for key, value in response.headers.items()}
                return status, headers
        except urllib.error.HTTPError as exc:
            status_code = int(exc.code or 0)
            if status_code == 304:
                headers = {key.lower(): value for key, value in exc.headers.items()} if exc.headers else {}
                return 304, headers
            retryable = status_code == 429 or 500 <= status_code < 600
            last_error = exc
            if not retryable or attempt >= MAX_ATTEMPTS:
                raise
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt >= MAX_ATTEMPTS:
                raise
        sleep_seconds = 2 ** (attempt - 1)
        time.sleep(sleep_seconds)
    if last_error is None:
        raise RuntimeError("Unknown GTFS download failure.")
    raise last_error


def refresh_gtfs_feed(
    feed_config: TransitFeedConfig,
    *,
    force: bool = False,
    progress_cb=None,
) -> FeedRefreshResult:
    if not feed_config.url:
        if not feed_config.zip_path.exists():
            raise RuntimeError(
                f"GTFS feed zip for '{feed_config.feed_id}' was not found at '{feed_config.zip_path}'."
            )
        sha256 = _sha256_file(feed_config.zip_path)
        return FeedRefreshResult(
            feed_id=feed_config.feed_id,
            changed=False,
            sha256=sha256,
            content_length=feed_config.zip_path.stat().st_size,
            zip_valid=True,
            calendar_min_date=None,
            calendar_max_date=None,
            path=feed_config.zip_path,
            reason="local_path",
        )

    feed_root = feed_config.zip_path.parent
    manifest_path = feed_root / "manifest.json"
    sha_path = feed_root / "current.sha256"
    downloads_dir = feed_root / "downloads"
    current_zip_path = feed_config.zip_path
    temp_zip_path = current_zip_path.with_name(current_zip_path.name + ".tmp")

    manifest = _read_manifest(manifest_path)
    current_sha = (manifest.get("sha256") or "").strip() if isinstance(manifest, dict) else ""
    now_iso = _utc_now_iso()
    now_epoch = time.time()
    today_utc = datetime.now(UTC).date()
    status = _build_refresh_status(
        feed_config,
        manifest_path=manifest_path,
        manifest=manifest,
        force=force,
        now_epoch=now_epoch,
        today_utc=today_utc,
    )

    if not status.network_request:
        warning = None
        if status.calendar_unknown:
            warning = (
                "calendar_max_date is missing/invalid; freshness fallback used cache age only."
            )
            _emit(progress_cb, f"[gtfs] warning: {warning} ({feed_config.feed_id})")
        manifest_payload = _manifest_with_status_fields(
            manifest,
            status=status,
            now_iso=now_iso,
            changed=False,
            last_network_check=False,
            warning=warning,
        )
        _write_manifest(manifest_path, manifest_payload)
        _emit(progress_cb, f"[gtfs] cache fresh; skip download ({feed_config.feed_id})")
        return FeedRefreshResult(
            feed_id=feed_config.feed_id,
            changed=False,
            sha256=current_sha or None,
            content_length=int(manifest.get("content_length") or 0) or None,
            zip_valid=bool(manifest.get("zip_valid")),
            calendar_min_date=manifest.get("calendar_min_date"),
            calendar_max_date=manifest.get("calendar_max_date"),
            path=current_zip_path,
            reason="fresh_cache",
        )

    request = urllib.request.Request(
        feed_config.url,
        headers={"User-Agent": USER_AGENT},
    )
    etag = (manifest.get("etag") or "").strip()
    if etag:
        request.add_header("If-None-Match", etag)
    last_modified = (manifest.get("last_modified") or "").strip()
    if last_modified:
        request.add_header("If-Modified-Since", last_modified)

    _emit(progress_cb, f"[gtfs] checking {feed_config.feed_id}")
    try:
        status_code, headers = _request_with_retries(request, destination_tmp_path=temp_zip_path)
    except Exception as exc:
        temp_zip_path.unlink(missing_ok=True)
        manifest_payload = _manifest_with_status_fields(
            manifest,
            status=status,
            now_iso=now_iso,
            changed=False,
            last_network_check=True,
            warning=f"GTFS download failed: {exc}",
        )
        _write_manifest(manifest_path, manifest_payload)
        if current_zip_path.exists():
            _emit(progress_cb, f"[gtfs] download failed; keeping previous current.zip ({feed_config.feed_id})")
        raise RuntimeError(
            f"Unable to download GTFS feed '{feed_config.feed_id}' from {feed_config.url}: {exc}"
        ) from exc

    if status_code == 304:
        temp_zip_path.unlink(missing_ok=True)
        warning = None
        if status.freshness_decision in {"stale_by_calendar_expired", "stale_by_calendar_near_expiry"}:
            warning = "Server returned 304 but cached GTFS service dates are expired/near expiry."
            _emit(progress_cb, f"[gtfs] warning: {warning} ({feed_config.feed_id})")
        manifest_payload = _manifest_with_status_fields(
            manifest,
            status=status,
            now_iso=now_iso,
            changed=False,
            last_network_check=True,
            warning=warning,
        )
        manifest_payload["http_status"] = 304
        _write_manifest(manifest_path, manifest_payload)
        _emit(progress_cb, f"[gtfs] HTTP 304 not modified ({feed_config.feed_id})")
        return FeedRefreshResult(
            feed_id=feed_config.feed_id,
            changed=False,
            sha256=current_sha or None,
            content_length=int(manifest.get("content_length") or 0) or None,
            zip_valid=bool(manifest.get("zip_valid")),
            calendar_min_date=manifest.get("calendar_min_date"),
            calendar_max_date=manifest.get("calendar_max_date"),
            path=current_zip_path,
            reason="not_modified",
        )

    try:
        validation = _validate_gtfs_zip(temp_zip_path)
    except Exception as exc:
        temp_zip_path.unlink(missing_ok=True)
        _emit(progress_cb, f"[gtfs] invalid feed; keeping previous current.zip ({feed_config.feed_id})")
        raise RuntimeError(
            f"Downloaded GTFS feed '{feed_config.feed_id}' failed validation: {exc}"
        ) from exc

    new_sha = _sha256_file(temp_zip_path)
    content_length = int(temp_zip_path.stat().st_size)
    changed = (new_sha != current_sha) or (not current_zip_path.exists())
    previous_sha = current_sha or None

    try:
        _copy_to_downloads_cache(temp_zip_path, downloads_dir=downloads_dir, sha256=new_sha)
        feed_root.mkdir(parents=True, exist_ok=True)
        os.replace(temp_zip_path, current_zip_path)
        sha_path.write_text(new_sha + "\n", encoding="utf-8")
    finally:
        temp_zip_path.unlink(missing_ok=True)

    manifest_payload: dict[str, Any] = {
        "feed_id": feed_config.feed_id,
        "url": feed_config.url,
        "checked_at_utc": now_iso,
        "last_network_check_at_utc": now_iso,
        "downloaded_at_utc": now_iso,
        "http_status": int(status_code),
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "content_length": content_length,
        "sha256": new_sha,
        "zip_valid": bool(validation["zip_valid"]),
        "gtfs_files": validation["gtfs_files"],
        "feed_info": validation["feed_info"],
        "feed_info_end_date": validation["feed_info_end_date"],
        "calendar_min_date": validation["calendar_min_date"],
        "calendar_max_date": validation["calendar_max_date"],
        "days_until_calendar_end": _days_until_calendar_end(
            validation["calendar_max_date"],
            today_utc=today_utc,
        ),
        "total_uncompressed_size": validation["total_uncompressed_size"],
        "optional_files_present": validation["optional_files_present"],
        "previous_sha256": previous_sha,
        "changed": bool(changed),
        "source_etag_seen_at_utc": _http_date_to_iso(headers.get("date")),
        "freshness_reason": status.freshness_reason,
        "stale_by_calendar": status.stale_by_calendar,
        "stale_by_age": status.stale_by_age,
        "force_refresh": status.force_refresh,
        "last_refresh_decision": status.freshness_decision,
        "calendar_unknown": False,
    }
    _write_manifest(manifest_path, manifest_payload)
    _emit(progress_cb, f"[gtfs] downloaded {content_length} bytes sha256={new_sha} ({feed_config.feed_id})")
    _emit(progress_cb, "[gtfs] validation ok: agency/stops/routes/trips/stop_times present")

    return FeedRefreshResult(
        feed_id=feed_config.feed_id,
        changed=bool(changed),
        sha256=new_sha,
        content_length=content_length,
        zip_valid=True,
        calendar_min_date=validation["calendar_min_date"],
        calendar_max_date=validation["calendar_max_date"],
        path=current_zip_path,
        reason="downloaded" if changed else "unchanged_content",
    )


def describe_gtfs_feed_status(
    feed_config: TransitFeedConfig,
    *,
    force: bool = False,
) -> FeedRefreshStatus:
    manifest_path = feed_config.zip_path.parent / "manifest.json"
    manifest = _read_manifest(manifest_path)
    return _build_refresh_status(
        feed_config,
        manifest_path=manifest_path,
        manifest=manifest,
        force=force,
        now_epoch=time.time(),
        today_utc=datetime.now(UTC).date(),
    )


def refresh_gtfs_feeds(
    *,
    force: bool = False,
    progress_cb=None,
) -> list[FeedRefreshResult]:
    results: list[FeedRefreshResult] = []
    for feed_config in sorted(gtfs_static_feed_configs(), key=lambda feed: int(feed.priority)):
        if not feed_config.enabled:
            continue
        result = refresh_gtfs_feed(feed_config, force=force, progress_cb=progress_cb)
        results.append(result)
    return results


def describe_gtfs_feeds(
    *,
    force: bool = False,
) -> list[FeedRefreshStatus]:
    statuses: list[FeedRefreshStatus] = []
    for feed_config in sorted(gtfs_static_feed_configs(), key=lambda feed: int(feed.priority)):
        if not feed_config.enabled:
            continue
        statuses.append(describe_gtfs_feed_status(feed_config, force=force))
    return statuses


def ensure_transit_feed_available(
    feed_config: TransitFeedConfig,
    *,
    auto_refresh_gtfs: bool,
    force_gtfs_refresh: bool,
    progress_cb=None,
) -> Path:
    if force_gtfs_refresh or auto_refresh_gtfs:
        refresh_gtfs_feed(feed_config, force=force_gtfs_refresh, progress_cb=progress_cb)
    if feed_config.zip_path.exists():
        return feed_config.zip_path
    raise RuntimeError(
        "GTFS cache missing. Run --refresh-gtfs first."
    )
