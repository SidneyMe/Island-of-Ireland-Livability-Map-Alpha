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
) -> str:
    return serve_livability_app(host=host, port=port)


__all__ = [
    "MISSING_PRECOMPUTE_MESSAGE",
    "_missing_precompute_message",
    "run_render_from_db",
]
