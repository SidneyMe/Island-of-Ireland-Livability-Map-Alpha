from __future__ import annotations

from config import database_url

from ._dependencies import Engine, create_engine


def build_engine() -> Engine:
    return create_engine(database_url(), future=True, pool_pre_ping=True)
