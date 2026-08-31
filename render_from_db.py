from __future__ import annotations

from config import DEFAULT_SERVER_HOST, DEFAULT_SERVER_PORT
from serve_from_db import (
    MISSING_PRECOMPUTE_MESSAGE,
    _missing_precompute_message,
    serve_livability_app,
)


def run_render_from_db(
    *,
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
    profile: str = "full",
    deployment: bool = False,
) -> str:
    serve_kwargs = dict(
        host=host,
        port=port,
        profile=profile,
    )
    if deployment:
        serve_kwargs["deployment"] = True
    return serve_livability_app(**serve_kwargs)


__all__ = [
    "MISSING_PRECOMPUTE_MESSAGE",
    "_missing_precompute_message",
    "run_render_from_db",
]
