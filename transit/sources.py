from __future__ import annotations

from pathlib import Path

from config import TransitFeedConfig

from .gtfs_download import (
    FeedRefreshResult,
    FeedRefreshStatus,
    describe_gtfs_feed_status,
    describe_gtfs_feeds,
    ensure_transit_feed_available,
    refresh_gtfs_feed,
    refresh_gtfs_feeds,
)


def ensure_feed_zip(
    feed_config: TransitFeedConfig,
    *,
    auto_refresh_gtfs: bool = False,
    force_gtfs_refresh: bool = False,
    progress_cb=None,
) -> Path:
    return ensure_transit_feed_available(
        feed_config,
        auto_refresh_gtfs=auto_refresh_gtfs,
        force_gtfs_refresh=force_gtfs_refresh,
        progress_cb=progress_cb,
    )


__all__ = [
    "FeedRefreshResult",
    "FeedRefreshStatus",
    "describe_gtfs_feed_status",
    "describe_gtfs_feeds",
    "ensure_feed_zip",
    "refresh_gtfs_feed",
    "refresh_gtfs_feeds",
]
