from __future__ import annotations

import re
import unicodedata


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")
_ROUTE_REF_SPLIT_RE = re.compile(r"[/|;,]+")
_GENERIC_NAME_TOKENS = frozenset(
    {
        "bus",
        "halt",
        "metro",
        "platform",
        "rail",
        "railway",
        "station",
        "stop",
        "subway",
        "train",
        "tram",
    }
)


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_only.lower().replace("&", " and ")
    cleaned = _NON_ALNUM_RE.sub(" ", lowered)
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def token_set(value: str | None) -> frozenset[str]:
    normalized = normalize_name(value)
    if not normalized:
        return frozenset()
    return frozenset(token for token in normalized.split(" ") if token)


def split_route_refs(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    refs: set[str] = set()
    for raw_part in _ROUTE_REF_SPLIT_RE.split(str(value)):
        normalized = normalize_name(raw_part).replace(" ", "")
        if normalized:
            refs.add(normalized)
    if refs:
        return frozenset(refs)
    fallback = normalize_name(value).replace(" ", "")
    return frozenset({fallback}) if fallback else frozenset()


def _token_sequence(value: str | None) -> tuple[str, ...]:
    normalized = normalize_name(value)
    if not normalized:
        return ()
    return tuple(token for token in normalized.split(" ") if token)


def _core_name(value: str | None) -> str:
    return " ".join(token for token in _token_sequence(value) if token not in _GENERIC_NAME_TOKENS)


def _token_weight(token: str) -> float:
    return 0.25 if token in _GENERIC_NAME_TOKENS else 1.0


def _weighted_jaccard(left_tokens: frozenset[str], right_tokens: frozenset[str]) -> float:
    if not left_tokens or not right_tokens:
        return 0.0
    shared_tokens = left_tokens.intersection(right_tokens)
    union_tokens = left_tokens.union(right_tokens)
    if not union_tokens:
        return 0.0
    shared_weight = sum(_token_weight(token) for token in shared_tokens)
    union_weight = sum(_token_weight(token) for token in union_tokens)
    if union_weight <= 0.0:
        return 0.0
    return shared_weight / union_weight


def name_similarity(left: str | None, right: str | None) -> tuple[float, tuple[str, ...]]:
    left_normalized = normalize_name(left)
    right_normalized = normalize_name(right)
    reasons: list[str] = []
    if not left_normalized or not right_normalized:
        return 0.0, tuple(reasons)
    if left_normalized == right_normalized:
        return 1.0, ("name_exact",)

    left_tokens = token_set(left)
    right_tokens = token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0, tuple(reasons)

    shared_tokens = left_tokens.intersection(right_tokens)
    if shared_tokens:
        reasons.append("name_token_overlap")

    similarity_score = _weighted_jaccard(left_tokens, right_tokens)

    left_core = _core_name(left)
    right_core = _core_name(right)
    contains_bonus = 0.0
    if left_normalized.startswith(right_normalized) or right_normalized.startswith(left_normalized):
        contains_bonus = 0.1
        reasons.append("name_prefix")
    elif left_normalized in right_normalized or right_normalized in left_normalized:
        contains_bonus = 0.05
        reasons.append("name_contains")

    core_bonus = 0.0
    if left_core and right_core:
        left_core_tokens = frozenset(left_core.split(" "))
        right_core_tokens = frozenset(right_core.split(" "))
        if left_core_tokens.intersection(right_core_tokens):
            reasons.append("name_core_overlap")
        similarity_score = max(
            similarity_score,
            _weighted_jaccard(left_core_tokens, right_core_tokens),
        )
        if left_core == right_core:
            core_bonus = 0.2
            reasons.append("name_core_exact")
        elif left_core.startswith(right_core) or right_core.startswith(left_core):
            core_bonus = 0.15
            reasons.append("name_core_prefix")
        elif left_core in right_core or right_core in left_core:
            core_bonus = 0.1
            reasons.append("name_core_contains")

    return min(similarity_score + max(contains_bonus, core_bonus), 1.0), tuple(reasons)
