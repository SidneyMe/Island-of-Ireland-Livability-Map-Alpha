from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path

from config import TransitFeedConfig


def _download_feed(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url)
    tmp_path = destination.with_name(destination.name + ".tmp")
    try:
        with urllib.request.urlopen(request, timeout=600) as response, tmp_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
        os.replace(tmp_path, destination)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def ensure_feed_zip(feed_config: TransitFeedConfig, *, refresh_download: bool) -> Path:
    if refresh_download and feed_config.url:
        _download_feed(feed_config.url, feed_config.zip_path)
    if feed_config.zip_path.exists():
        return feed_config.zip_path
    if feed_config.url:
        try:
            _download_feed(feed_config.url, feed_config.zip_path)
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"Unable to download GTFS feed '{feed_config.feed_id}' from {feed_config.url}: {exc}"
            ) from exc
        return feed_config.zip_path
    raise RuntimeError(
        f"GTFS feed zip for '{feed_config.feed_id}' was not found at '{feed_config.zip_path}'. "
        "Configure a local zip path or feed URL first."
    )
