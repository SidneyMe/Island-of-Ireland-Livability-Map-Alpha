from __future__ import annotations

from .models import FeedDataset, ServiceClassification


def classify_services(
    dataset: FeedDataset,
    *,
    reality_fingerprint: str,
    service_windows: dict[str, dict[str, object]],
) -> dict[str, ServiceClassification]:
    classifications: dict[str, ServiceClassification] = {}

    for service_id in sorted(set(dataset.service_route_ids).union(dataset.service_time_buckets, service_windows)):
        window = service_windows.get(service_id) or {}
        bucket_counts = dict(
            dataset.service_time_buckets.get(
                service_id,
                {"morning": 0, "afternoon": 0, "offpeak": 0},
            )
        )
        total_events = sum(bucket_counts.values())
        school_bucket_events = bucket_counts.get("morning", 0) + bucket_counts.get("afternoon", 0)
        school_bucket_share = (school_bucket_events / total_events) if total_events else 0.0
        weekday_dates = int(window.get("weekday_dates", 0) or 0)
        weekend_dates = int(window.get("weekend_dates", 0) or 0)
        has_keyword = bool(dataset.service_keywords.get(service_id))
        route_ids = tuple(sorted(dataset.service_route_ids.get(service_id, set())))
        route_modes = tuple(sorted(dataset.service_route_modes.get(service_id, set())))
        reason_codes: list[str] = []

        if weekday_dates > 0:
            reason_codes.append("weekday_service_present")
        if weekend_dates > 0:
            reason_codes.append("weekend_service_present")
        if school_bucket_share >= 0.9 and total_events > 0:
            reason_codes.append("school_hour_concentration")
        if has_keyword:
            reason_codes.append("school_keyword")

        school_only_state = "no"
        if weekday_dates > 0 and weekend_dates == 0 and school_bucket_share >= 0.9:
            if has_keyword:
                school_only_state = "yes"
            else:
                school_only_state = "unknown"

        classifications[service_id] = ServiceClassification(
            reality_fingerprint=reality_fingerprint,
            feed_id=dataset.feed_id,
            service_id=service_id,
            school_only_state=school_only_state,
            route_ids=route_ids,
            route_modes=route_modes,
            reason_codes=tuple(sorted(set(reason_codes))),
            time_bucket_counts=bucket_counts,
        )

    return classifications
